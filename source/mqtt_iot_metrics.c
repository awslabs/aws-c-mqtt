/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/byte_buf.h>
#include <aws/common/system_info.h>
#include <aws/common/uri.h>
#include <aws/mqtt/mqtt.h>
#include <aws/mqtt/private/mqtt_iot_metrics.h>

#include <stdio.h>

// Use packet encoding limit for now: https://github.com/awslabs/aws-c-mqtt/blob/v0.13.3/source/packets.c#L26
const size_t AWS_IOT_MAX_USERNAME_SIZE = UINT16_MAX;
const size_t DEFAULT_QUERY_PARAM_COUNT = 10;

/*********************************************************************************************************************
 * Helper functions for parsing and merging key-value pairs
 ********************************************************************************************************************/

/**
 * Find a parameter by key in a list of aws_uri_param.
 *
 * @param params_list List of aws_uri_param to search
 * @param key The key to search for
 * @param out_index [Optional] If not NULL and key is found, will be set to the index of the found parameter
 * @return Pointer to the found parameter, or NULL if not found
 */
static struct aws_uri_param *s_find_param_by_key(
    const struct aws_array_list *params_list,
    const struct aws_byte_cursor key,
    size_t *out_index) {

    size_t params_count = aws_array_list_length(params_list);
    for (size_t i = 0; i < params_count; ++i) {
        struct aws_uri_param *param = NULL;
        aws_array_list_get_at_ptr(params_list, (void **)&param, i);
        if (aws_byte_cursor_eq(&param->key, &key)) {
            if (out_index) {
                *out_index = i;
            }
            return param;
        }
    }
    return NULL;
}

/**
 * Add a parameter to the list only if a parameter with the same key doesn't already exist.
 *
 * @param params_list List of aws_uri_param to add to
 * @param key The key for the new parameter
 * @param value The value for the new parameter
 * @return true if the parameter was added, false if a parameter with the same key already exists
 */
static bool s_add_param_if_not_exists(
    struct aws_array_list *params_list,
    const struct aws_byte_cursor key,
    const struct aws_byte_cursor value) {

    if (s_find_param_by_key(params_list, key, NULL) != NULL) {
        return false;
    }

    struct aws_uri_param param = {
        .key = key,
        .value = value,
    };
    aws_array_list_push_back(params_list, &param);
    return true;
}

/**
 * Parse key-value entries from a aws_byte_cursor using a specified delimiter.
 * Each entry is expected to be in the format "key=value" or just "key". (e.g., "key1=value1&key2=value2")
 *
 * @param content The content to parse
 * @param delimiter The delimiter cursor (e.g., ";" or "&"), length of delimiter must > 0.
 * @param out_entries List to populate with parsed aws_uri_param entries. Must be initialized by caller.
 * @return AWS_OP_SUCCESS on success, AWS_OP_ERR on failure
 */
static int s_parse_delimited_entries(
    const struct aws_byte_cursor content,
    const struct aws_byte_cursor delimiter,
    struct aws_array_list *out_entries) {

    AWS_ASSERT(delimiter.len > 0);

    struct aws_byte_cursor equals_delim = aws_byte_cursor_from_c_str("=");
    struct aws_byte_cursor entry_cursor;
    AWS_ZERO_STRUCT(entry_cursor);

    while (aws_byte_cursor_next_split_on_cursor(&content, delimiter, &entry_cursor)) {
        /* Skip empty entries */
        if (entry_cursor.len == 0) {
            continue;
        }

        /* Parse key=value from entry_cursor */
        struct aws_uri_param entry_param;
        AWS_ZERO_STRUCT(entry_param);

        struct aws_byte_cursor equals_pos;
        if (aws_byte_cursor_find_exact(&entry_cursor, &equals_delim, &equals_pos) == AWS_OP_SUCCESS) {
            entry_param.key.ptr = entry_cursor.ptr;
            entry_param.key.len = equals_pos.ptr - entry_cursor.ptr;
            entry_param.value = aws_byte_cursor_advance(&entry_cursor, entry_param.key.len + 1);
        } else {
            /* No equals sign, treat entire entry as key */
            entry_param.key = entry_cursor;
        }

        aws_array_list_push_back(out_entries, &entry_param);
    }

    return AWS_OP_SUCCESS;
}

/**
 * Merge new entries into an existing entries list without overwriting existing keys.
 *
 * @param existing_metadata_list List of existing aws_uri_param entries (will be modified)
 * @param new_entries Array of new entries to merge
 * @param new_entries_count Number of new entries
 */
static void s_merge_metadata_entries_no_overwrite(
    struct aws_array_list *existing_metadata_list,
    const struct aws_mqtt_metadata_entry *new_entries_list,
    const size_t new_entries_list_count) {

    for (size_t i = 0; i < new_entries_list_count; ++i) {
        const struct aws_mqtt_metadata_entry *new_entry = &new_entries_list[i];
        s_add_param_if_not_exists(existing_metadata_list, new_entry->key, new_entry->value);
    }
}

/**
 * Build a delimited key-value string from a list of parameters.
 * Can be used to either calculate the size, build the string, or both.
 *
 * Format: {prefix}{key1}={value1}{delimiter}{key2}={value2}...{suffix}
 * If params_list is empty, returns empty string (no prefix/suffix).
 *
 * @param params_list List of aws_uri_param entries
 * @param prefix [Optional] String to prepend (e.g., "?" for query strings, "(" for metadata)
 * @param suffix [Optional] String to append (e.g., ")" for metadata)
 * @param entry_delimiter Delimiter between entries (e.g., "&" for query strings, ";" for metadata)
 * @param output_buf [Optional] Buffer to write the result to. Must be initialized with sufficient capacity.
 * @param out_size [Optional] If not NULL, will be set to the size of the result string
 * @return AWS_OP_SUCCESS on success, AWS_OP_ERR on failure
 */
static int s_build_delimited_params(
    const struct aws_array_list *params_list,
    const struct aws_byte_cursor *prefix,
    const struct aws_byte_cursor *suffix,
    const struct aws_byte_cursor entry_delimiter,
    struct aws_byte_buf *output_buf,
    size_t *out_size) {

    AWS_ASSERT(entry_delimiter.len > 0);

    size_t local_out_size = 0;
    struct aws_byte_cursor key_value_delim = aws_byte_cursor_from_c_str("=");

    size_t params_count = aws_array_list_length(params_list);

    /* If params_list is empty, return empty string (no prefix/suffix) */
    if (params_count == 0) {
        if (out_size) {
            *out_size = 0;
        }
        return AWS_OP_SUCCESS;
    }

    if (prefix) {
        local_out_size += prefix->len;
    }
    if (suffix) {
        local_out_size += suffix->len;
    }

    if (output_buf && prefix && prefix->len > 0) {
        if (aws_byte_buf_append(output_buf, prefix)) {
            return AWS_OP_ERR;
        }
    }

    for (size_t i = 0; i < params_count; ++i) {
        struct aws_uri_param param;
        AWS_ZERO_STRUCT(param);
        aws_array_list_get_at(params_list, &param, i);

        /* Add delimiter between entries (not before the first one) */
        if (i > 0) {
            if (output_buf) {
                if (aws_byte_buf_append(output_buf, &entry_delimiter)) {
                    return AWS_OP_ERR;
                }
            }
            local_out_size += entry_delimiter.len;
        }

        /* Append key */
        if (output_buf && param.key.len > 0) {
            if (aws_byte_buf_append(output_buf, &param.key)) {
                return AWS_OP_ERR;
            }
        }
        local_out_size += param.key.len;

        /* Append =value if value exists */
        if (param.value.len > 0) {
            if (output_buf) {
                if (aws_byte_buf_append(output_buf, &key_value_delim) ||
                    aws_byte_buf_append(output_buf, &param.value)) {
                    return AWS_OP_ERR;
                }
            }
            local_out_size += 1 + param.value.len; /* '=' + value */
        }
    }

    if (output_buf && suffix && suffix->len > 0) {
        if (aws_byte_buf_append(output_buf, suffix)) {
            return AWS_OP_ERR;
        }
    }

    if (out_size) {
        *out_size = local_out_size;
    }

    return AWS_OP_SUCCESS;
}

/**
 * Build the metadata value string from entries: (key1=value1;key2=value2;...)
 * Can be used to either calculate the size, build the string, or both.
 *
 * @param entries List of aws_uri_param entries
 * @param output_buf [Optional] Buffer to write the metadata value to. Must be initialized with sufficient capacity.
 * @param out_size [Optional] If not NULL, will be set to the size of the metadata value string
 * @return AWS_OP_SUCCESS on success, AWS_OP_ERR on failure
 */
static int s_build_metadata_value(
    const struct aws_array_list *entries,
    struct aws_byte_buf *output_buf,
    size_t *out_size) {

    struct aws_byte_cursor open_paren = aws_byte_cursor_from_c_str("(");
    struct aws_byte_cursor close_paren = aws_byte_cursor_from_c_str(")");
    struct aws_byte_cursor semicolon_delim = aws_byte_cursor_from_c_str(";");

    return s_build_delimited_params(entries, &open_paren, &close_paren, semicolon_delim, output_buf, out_size);
}

/**
 * Builds final username with query parameters appended.
 *
 * @param base_username The original username cursor
 * @param base_username_length Length of base username to use (may differ from cursor length)
 * @param params_list List of query parameters to append
 * @param output_username [Optional] Buffer to write result. Caller must init/cleanup the buffer.
 * @param out_final_username_size [Optional] Outputs the final username size
 *
 * @return AWS_OP_SUCCESS on success, AWS_OP_ERR on failure
 */
static int s_build_username_query(
    const struct aws_byte_cursor base_username,
    const size_t base_username_length,
    const struct aws_array_list *params_list,
    struct aws_byte_buf *output_username,
    size_t *out_final_username_size) {

    /* Write base username first */
    if (output_username) {
        if (!aws_byte_buf_write(output_username, base_username.ptr, base_username_length)) {
            return AWS_OP_ERR;
        }
    }

    if (out_final_username_size) {
        *out_final_username_size = base_username_length;
    }

    /* Build query parameters using the generic helper */
    struct aws_byte_cursor query_prefix = aws_byte_cursor_from_c_str("?");
    struct aws_byte_cursor ampersand_delim = aws_byte_cursor_from_c_str("&");

    size_t params_size = 0;
    if (s_build_delimited_params(params_list, &query_prefix, NULL, ampersand_delim, output_username, &params_size)) {
        return AWS_OP_ERR;
    }

    if (out_final_username_size) {
        *out_final_username_size += params_size;
    }

    return AWS_OP_SUCCESS;
}

/**
 * Appends SDK metrics to an MQTT username for IoT service telemetry.
 *
 * This function transforms a username by appending SDK metrics as query parameters.
 * The IoT service uses the last '?' in the username to identify the metrics section.
 *
 * Step-by-step process:
 * 1. Parse existing username: Find the last '?' in the original username and parse query params into
 * username_params_list
 * 2. Add "SDK" and "Platform" parameters if not already present
 * 3. Handle Metadata entries if metadata entries are provided
 *  a. Check if existing "Metadata" parameter exists in username_params_list
 *  b. If existing Metadata found with valid format:
 *     - Parse existing entries into a key-value metadata_param_list
 *     - Remove existing Metadata from username_params_list (will be rebuilt later)
 *  c. If existing Metadata has an invalid format:
 *     - Log debug message and skip adding metrics metadata entries
 *     - Keep original Metadata value unchanged, skip (d)
 *  d. Append metrics metadata entries to metadata_param_list
 *     - Add new metadata entries into metadata_param_list (existing keys take precedence, won't be overwritten)
 *     - build metadata value string from metadata_param_list
 *     - Add Metadata parameter to username_params_list
 * 4. Build final username from username_params_list
 *
 * Example transformation:
 *   Input:  "myuser?existing=value"
 *   Metrics: {library_name="MySDK/1.0", metadata=[{key="ver", value="1.0"}]}
 *   Output: "myuser?existing=value&SDK=MySDK/1.0&Platform=Darwin&Metadata=(ver=1.0)"
 */
int aws_mqtt_append_sdk_metrics_to_username(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *original_username,
    const struct aws_mqtt_iot_metrics *metrics,
    struct aws_byte_buf *output_username,
    size_t *out_full_username_size) {
    AWS_PRECONDITION(
        output_username == NULL ||
        (aws_byte_buf_is_valid(output_username) && (output_username && output_username->buffer == NULL)));

    if (!allocator) {
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }
    const struct aws_byte_cursor local_original_username =
        original_username == NULL ? aws_byte_cursor_from_c_str("") : *original_username;

    if (!metrics) {
        if (out_full_username_size) {
            *out_full_username_size = local_original_username.len;
        }

        if (output_username) {
            return aws_byte_buf_init_copy_from_cursor(output_username, allocator, local_original_username);
        }

        return AWS_OP_SUCCESS;
    }

    if (aws_mqtt_validate_iot_metrics(metrics)) {
        return AWS_OP_ERR;
    }

    int result = AWS_OP_ERR;
    size_t base_username_length = 0;
    struct aws_byte_cursor question_mark_str = aws_byte_cursor_from_c_str("?");
    struct aws_byte_cursor sdk_str = aws_byte_cursor_from_c_str("SDK");
    struct aws_byte_cursor platform_str = aws_byte_cursor_from_c_str("Platform");
    struct aws_byte_cursor metadata_str = aws_byte_cursor_from_c_str("Metadata");

    struct aws_array_list username_params_list;
    aws_array_list_init_dynamic(
        &username_params_list, allocator, DEFAULT_QUERY_PARAM_COUNT, sizeof(struct aws_uri_param));

    struct aws_byte_buf metadata_value_buf;
    AWS_ZERO_STRUCT(metadata_value_buf);

    struct aws_array_list metadata_param_list;
    AWS_ZERO_STRUCT(metadata_param_list);

    /* Parse existing query parameters from the original username */
    if (local_original_username.len > 0) {
        struct aws_byte_cursor question_mark_find = local_original_username;

        bool found_query = false;
        /* Find the last question mark. The IoT service will trim string after last question mark and handle it as
         * metrics metadata */
        while (AWS_OP_SUCCESS ==
               aws_byte_cursor_find_exact(&question_mark_find, &question_mark_str, &question_mark_find)) {
            // Advance cursor to skip the "?" character
            aws_byte_cursor_advance(&question_mark_find, 1);
            found_query = true;
        }

        if (found_query) {
            // Trim out the query delimiter from the base username
            base_username_length = question_mark_find.ptr - 1 - local_original_username.ptr;
            aws_query_string_params(question_mark_find, &username_params_list);
        } else {
            base_username_length = local_original_username.len;
        }
    }

    /* Add SDK parameter if not already present */
    struct aws_byte_cursor sdk_value =
        metrics->library_name.len > 0 ? metrics->library_name : aws_byte_cursor_from_c_str("IoTDeviceSDK/C");
    s_add_param_if_not_exists(&username_params_list, sdk_str, sdk_value);

    /* Add Platform parameter if not already present */
    s_add_param_if_not_exists(&username_params_list, platform_str, aws_get_platform_build_os_string());

    /* Handle metadata entries: parse existing, merge with new, and rebuild */
    bool has_new_metadata = metrics->metadata_entries != NULL && metrics->metadata_count > 0;

    if (has_new_metadata) {
        struct aws_byte_cursor semicolon_delim = aws_byte_cursor_from_c_str(";");
        bool should_merge_metadata = true;

        aws_array_list_init_dynamic(
            &metadata_param_list, allocator, DEFAULT_QUERY_PARAM_COUNT, sizeof(struct aws_uri_param));

        /* Find existing Metadata parameter */
        size_t existing_metadata_index = 0;
        struct aws_uri_param *existing_metadata_param =
            s_find_param_by_key(&username_params_list, metadata_str, &existing_metadata_index);

        if (existing_metadata_param != NULL && existing_metadata_param->value.len > 0) {
            /* Extract content without parentheses: (key1=value1;key2=value2) -> key1=value1;key2=value2 */
            struct aws_byte_cursor existing_content = existing_metadata_param->value;
            if (existing_content.len >= 2 && existing_content.ptr[0] == '(' &&
                existing_content.ptr[existing_content.len - 1] == ')') {
                aws_byte_cursor_advance(&existing_content, 1);
                existing_content.len -= 1;

                /* Parse existing metadata entries */
                if (existing_content.len > 0) {
                    s_parse_delimited_entries(existing_content, semicolon_delim, &metadata_param_list);
                }

                /* Remove existing Metadata from username_params_list - we'll merge and re-add */
                aws_array_list_erase(&username_params_list, existing_metadata_index);
            } else {
                /* Wrong format: keep the original metadata value unchanged and don't append new metadata */
                AWS_LOGF_DEBUG(
                    AWS_LS_MQTT_GENERAL,
                    "Existing Metadata parameter has invalid format (expected parentheses). "
                    "Keeping original value and skipping new metadata entries.");
                should_merge_metadata = false;
            }
        }

        if (should_merge_metadata) {
            /* Merge new metadata entries (won't overwrite existing keys) */
            s_merge_metadata_entries_no_overwrite(
                &metadata_param_list, metrics->metadata_entries, metrics->metadata_count);

            /* Calculate size needed for metadata value string first */
            size_t metadata_value_size = 0;
            s_build_metadata_value(&metadata_param_list, NULL, &metadata_value_size);

            /* Initialize buffer and build the metadata value string */
            if (aws_byte_buf_init(&metadata_value_buf, allocator, metadata_value_size)) {
                goto cleanup;
            }

            if (s_build_metadata_value(&metadata_param_list, &metadata_value_buf, NULL)) {
                goto cleanup;
            }

            /* Add Metadata parameter to username_params_list */
            struct aws_uri_param metadata_params = {
                .key = metadata_str,
                .value = aws_byte_cursor_from_buf(&metadata_value_buf),
            };
            aws_array_list_push_back(&username_params_list, &metadata_params);
        }
    }

    /* Rebuild metrics string from username_params_list */
    // Calculate the final username size first.
    size_t total_size = 0;
    s_build_username_query(local_original_username, base_username_length, &username_params_list, NULL, &total_size);

    if (total_size > AWS_IOT_MAX_USERNAME_SIZE) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT_GENERAL,
            "Failed to append SDK metrics to username: resulting username exceeds max username size.");
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        goto cleanup;
    }

    if (output_username && aws_byte_buf_init(output_username, allocator, total_size)) {
        goto cleanup;
    }

    if (s_build_username_query(
            local_original_username,
            base_username_length,
            &username_params_list,
            output_username,
            out_full_username_size)) {
        goto cleanup;
    }

    result = AWS_OP_SUCCESS;

cleanup:
    aws_byte_buf_clean_up(&metadata_value_buf);
    aws_array_list_clean_up(&username_params_list);
    if (aws_array_list_is_valid(&metadata_param_list)) {
        aws_array_list_clean_up(&metadata_param_list);
    }

    if (result == AWS_OP_ERR) {
        aws_byte_buf_clean_up(output_username);
    }
    return result;
}

/*********************************************************************************************************************
 * IoT SDK Metrics
 ********************************************************************************************************************/

size_t aws_mqtt_iot_metrics_compute_storage_size(const struct aws_mqtt_iot_metrics *metrics) {
    if (metrics == NULL) {
        return 0;
    }

    size_t storage_size = 0;
    storage_size += metrics->library_name.len;

    /* Add storage for metadata entries */
    if (metrics->metadata_entries != NULL && metrics->metadata_count > 0) {
        for (size_t i = 0; i < metrics->metadata_count; ++i) {
            const struct aws_mqtt_metadata_entry *entry = &metrics->metadata_entries[i];
            storage_size += entry->key.len;
            storage_size += entry->value.len;
        }
    }

    return storage_size;
}

struct aws_mqtt_iot_metrics_storage *aws_mqtt_iot_metrics_storage_new(
    struct aws_allocator *allocator,
    const struct aws_mqtt_iot_metrics *metrics_options) {

    if (allocator == NULL || metrics_options == NULL) {
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        return NULL;
    }

    struct aws_mqtt_iot_metrics_storage *metrics_storage =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_iot_metrics_storage));

    size_t storage_capacity = aws_mqtt_iot_metrics_compute_storage_size(metrics_options);
    if (aws_byte_buf_init(&metrics_storage->storage, allocator, storage_capacity)) {
        goto cleanup_storage;
    }

    metrics_storage->allocator = allocator;

    struct aws_mqtt_iot_metrics *storage_view = &metrics_storage->storage_view;

    if (metrics_options->library_name.len > 0) {
        metrics_storage->library_name = metrics_options->library_name;
        if (aws_byte_buf_append_and_update(&metrics_storage->storage, &metrics_storage->library_name)) {
            goto cleanup_storage;
        }
        storage_view->library_name = metrics_storage->library_name;
    }

    /* Copy metadata entries */
    if (metrics_options->metadata_entries != NULL && metrics_options->metadata_count > 0) {
        if (aws_array_list_init_dynamic(
                &metrics_storage->metadata_entries,
                allocator,
                metrics_options->metadata_count,
                sizeof(struct aws_mqtt_metadata_entry))) {
            goto cleanup_storage;
        }

        for (size_t i = 0; i < metrics_options->metadata_count; ++i) {
            struct aws_mqtt_metadata_entry entry;
            entry.key = metrics_options->metadata_entries[i].key;
            entry.value = metrics_options->metadata_entries[i].value;

            /* Copy key into storage buffer and update cursor */
            if (entry.key.len > 0) {
                if (aws_byte_buf_append_and_update(&metrics_storage->storage, &entry.key)) {
                    goto cleanup_storage;
                }
            }

            /* Copy value into storage buffer and update cursor */
            if (entry.value.len > 0) {
                if (aws_byte_buf_append_and_update(&metrics_storage->storage, &entry.value)) {
                    goto cleanup_storage;
                }
            }

            aws_array_list_push_back(&metrics_storage->metadata_entries, &entry);
        }

        /* Set storage_view to point to the metadata entries array */
        storage_view->metadata_count = aws_array_list_length(&metrics_storage->metadata_entries);
        storage_view->metadata_entries = metrics_storage->metadata_entries.data;
    }

    return metrics_storage;

cleanup_storage:
    if (aws_array_list_is_valid(&metrics_storage->metadata_entries)) {
        aws_array_list_clean_up(&metrics_storage->metadata_entries);
    }

    aws_byte_buf_clean_up(&metrics_storage->storage);
    aws_mem_release(allocator, metrics_storage);
    return NULL;
}

void aws_mqtt_iot_metrics_storage_destroy(struct aws_mqtt_iot_metrics_storage *metrics_storage) {
    if (metrics_storage == NULL) {
        return;
    }

    if (aws_array_list_is_valid(&metrics_storage->metadata_entries)) {
        aws_array_list_clean_up(&metrics_storage->metadata_entries);
    }

    aws_byte_buf_clean_up(&metrics_storage->storage);

    aws_mem_release(metrics_storage->allocator, metrics_storage);
}

int aws_mqtt_validate_iot_metrics(const struct aws_mqtt_iot_metrics *metrics) {
    if (metrics == NULL) {
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    if (aws_mqtt_validate_utf8_text(metrics->library_name)) {
        return AWS_OP_ERR;
    }

    /* Validate metadata entries */
    if (metrics->metadata_entries != NULL && metrics->metadata_count > 0) {
        for (size_t i = 0; i < metrics->metadata_count; ++i) {
            if (aws_mqtt_validate_utf8_text(metrics->metadata_entries[i].key)) {
                return AWS_OP_ERR;
            }
            if (aws_mqtt_validate_utf8_text(metrics->metadata_entries[i].value)) {
                return AWS_OP_ERR;
            }
        }
    }

    return AWS_OP_SUCCESS;
}

bool aws_mqtt_has_non_empty_username(
    const struct aws_byte_cursor *username,
    const struct aws_mqtt_iot_metrics_storage *metrics_storage) {
    return (username != NULL && username->len > 0) || metrics_storage != NULL;
}
