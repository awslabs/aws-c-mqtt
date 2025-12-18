/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/byte_buf.h>
#include <aws/common/system_info.h>
#include <aws/common/uri.h>
#include <aws/mqtt/mqtt.h>
#include <aws/mqtt/private/client_impl_shared.h>

#include <stdio.h>

// MQTT payload size https://docs.aws.amazon.com/general/latest/gr/iot-core.html#thing-limits
const int AWS_IOT_MAX_CONTENT_SIZE = 128 * 1024;
const size_t DEFAULT_QUERY_PARAM_COUNT = 10;

// Build username query string from params_list, the caller is responsible to init and clean up output_username
// If output_username is NULL, the function will just calculate the full username size and return it in
// out_full_username_size
int s_build_username_query(
    const struct aws_byte_cursor *base_username,
    size_t base_username_length,
    const struct aws_array_list *params_list,
    struct aws_byte_buf *output_username,
    size_t *out_full_username_size) {

    if (output_username) {
        aws_byte_buf_write(output_username, base_username->ptr, base_username_length);
    }

    if (out_full_username_size) {
        *out_full_username_size = base_username_length;
    }

    struct aws_byte_cursor query_delim = aws_byte_cursor_from_c_str("?");
    struct aws_byte_cursor query_param_amp = aws_byte_cursor_from_c_str("&");
    struct aws_byte_cursor key_value_delim = aws_byte_cursor_from_c_str("=");

    size_t params_count = aws_array_list_length(params_list);
    for (size_t i = 0; i < params_count; ++i) {
        struct aws_uri_param param;
        AWS_ZERO_STRUCT(param);
        aws_array_list_get_at(params_list, &param, i);

        if (i == 0 && output_username) {
            aws_byte_buf_append(output_username, &query_delim);
        } else if (i > 0 && output_username) {
            aws_byte_buf_append(output_username, &query_param_amp);
        }

        if (out_full_username_size) {
            *out_full_username_size += 1;
        }

        if (output_username) {
            aws_byte_buf_append(output_username, &param.key);
            aws_byte_buf_append(output_username, &key_value_delim);
            aws_byte_buf_append(output_username, &param.value);
        }

        if (out_full_username_size) {
            *out_full_username_size += param.key.len + 1 + param.value.len;
        }
    }

    return AWS_OP_SUCCESS;
}

// TODO Future Work: we ignored the metadata field for now, will add them in future support
int aws_mqtt_append_sdk_metrics_to_username(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *original_username,
    const struct aws_mqtt_iot_sdk_metrics metrics,
    struct aws_byte_buf *output_username,
    size_t *out_full_username_size) {

    if (!allocator) {
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    if (aws_mqtt_validate_iot_sdk_metrics_utf8(&metrics)) {
        return AWS_OP_ERR;
    }

    /* Build metrics string */
    struct aws_byte_buf metrics_string;
    if (aws_byte_buf_init(&metrics_string, allocator, AWS_IOT_MAX_CONTENT_SIZE)) {
        return AWS_OP_ERR;
    }

    int result = AWS_OP_ERR;
    // The length of the base username part not including query parameters
    size_t base_username_length = 0;
    struct aws_byte_cursor question_mark_str = aws_byte_cursor_from_c_str("?");
    struct aws_byte_cursor sdk_str = aws_byte_cursor_from_c_str("SDK");
    struct aws_byte_cursor platform_str = aws_byte_cursor_from_c_str("Platform");

    struct aws_array_list params_list;
    aws_array_list_init_dynamic(&params_list, allocator, DEFAULT_QUERY_PARAM_COUNT, sizeof(struct aws_uri_param));

    if (original_username && original_username->len > 0) {
        struct aws_byte_cursor question_mark_find;

        if (AWS_OP_SUCCESS == aws_byte_cursor_find_exact(original_username, &question_mark_str, &question_mark_find)) {
            base_username_length = question_mark_find.ptr - original_username->ptr;
            // Advance cursor to skip the "?" character
            aws_byte_cursor_advance(&question_mark_find, 1);
            aws_byte_buf_append(&metrics_string, &question_mark_find);
            aws_query_string_params(question_mark_find, &params_list);
        } else {
            base_username_length = original_username->len;
        }
    }

    bool found_sdk = false;
    bool found_platform = false;

    size_t params_count = aws_array_list_length(&params_list);
    for (size_t i = 0; i < params_count; ++i) {
        struct aws_uri_param param;
        AWS_ZERO_STRUCT(param);
        aws_array_list_get_at(&params_list, &param, i);
        if (aws_byte_cursor_eq(&param.key, &sdk_str)) {
            found_sdk = true;
        } else if (aws_byte_cursor_eq(&param.key, &platform_str)) {
            found_platform = true;
        }
    }

    if (!found_sdk) {
        struct aws_uri_param sdk_params = {
            .key = sdk_str,
            .value = metrics.library_name.len > 0 ? metrics.library_name : aws_byte_cursor_from_c_str("IoTDeviceSDK/C"),
        };
        aws_array_list_push_back(&params_list, &sdk_params);
    }

    if (!found_platform) {
        struct aws_uri_param platform_params = {
            .key = platform_str,
            .value = aws_get_platform_build_os_string(),
        };
        aws_array_list_push_back(&params_list, &platform_params);
    }

    // Rebuild metrics string from params_list
    // First path to calculate total size
    size_t total_size = 0;
    s_build_username_query(original_username, base_username_length, &params_list, NULL, &total_size);

    if (output_username && aws_byte_buf_init(output_username, allocator, total_size)) {
        goto cleanup;
    }

    // build final output username
    s_build_username_query(
        original_username, base_username_length, &params_list, output_username, out_full_username_size);

    aws_byte_buf_clean_up(&metrics_string);
    result = AWS_OP_SUCCESS;

cleanup:
    if (aws_array_list_is_valid(&params_list)) {
        aws_array_list_clean_up(&params_list);
    }

    if (result == AWS_OP_ERR && aws_byte_buf_is_valid(output_username)) {
        aws_byte_buf_clean_up(output_username);
    }
    aws_byte_buf_clean_up(&metrics_string);
    return result;
}
