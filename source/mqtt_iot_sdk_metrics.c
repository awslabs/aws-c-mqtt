/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/byte_buf.h>
#include <aws/common/encoding.h>
#include <aws/common/string.h>
#include <aws/common/system_info.h>
#include <aws/mqtt/mqtt.h>

#include <stdio.h>

// Maximum MQTT5 Content Type size https://docs.aws.amazon.com/general/latest/gr/iot-core.html#thing-limits
const int AWS_IOT_MAX_CONTENT_SIZE = 256;

int aws_mqtt_append_sdk_metrics_to_username(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *original_username,
    const struct aws_mqtt_iot_sdk_metrics metrics,
    struct aws_byte_buf *output_username) {

    if (!allocator || !output_username) {
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    /* Build metrics string */
    struct aws_byte_buf metrics_string;
    if (aws_byte_buf_init(&metrics_string, allocator, AWS_IOT_MAX_CONTENT_SIZE)) {
        return AWS_OP_ERR;
    }

    /* Check if the attributes already exists in username */
    bool has_sdk = false;
    bool has_platform = false;
    bool has_query = false;

    struct aws_byte_cursor sdk_str = aws_byte_cursor_from_c_str("SDK=");
    struct aws_byte_cursor platform_str = aws_byte_cursor_from_c_str("Platform=");
    struct aws_byte_cursor question_mark_str = aws_byte_cursor_from_c_str("?");
    struct aws_byte_cursor amp = aws_byte_cursor_from_c_str("&");

    // TODO: we ignored the metadata field for now, need to add them later

    if (original_username && original_username->len > 0) {
        struct aws_byte_cursor question_mark_find;
        has_query =
            AWS_OP_SUCCESS == aws_byte_cursor_find_exact(original_username, &question_mark_str, &question_mark_find);
        if (has_query) {
            struct aws_byte_cursor temp_cursor;
            has_sdk = AWS_OP_SUCCESS == aws_byte_cursor_find_exact(&question_mark_find, &sdk_str, &temp_cursor);
            has_platform =
                AWS_OP_SUCCESS == aws_byte_cursor_find_exact(&question_mark_find, &platform_str, &temp_cursor);
        }
    }

    bool metrics_added = false;

    /* Add SDK if not present */
    if (!has_sdk) {
        if (aws_byte_buf_append(&metrics_string, &sdk_str)) {
            goto error;
        }

        struct aws_byte_cursor sdk_attr_value =
            metrics.library_name.len > 0 ? metrics.library_name : aws_byte_cursor_from_c_str("IoTDeviceSDK/C");
        if (aws_byte_buf_append(&metrics_string, &sdk_attr_value)) {
            goto error;
        }

        metrics_added = true;
    }

    /* Add Platform if not present */
    if (!has_platform) {
        struct aws_byte_cursor platform_cursor = aws_get_platform_build_os_string();
        if (platform_cursor.len > 0) {
            if (metrics_added) {
                if (aws_byte_buf_append(&metrics_string, &amp)) {
                    goto error;
                }
            }

            if (aws_byte_buf_append(&metrics_string, &platform_str) ||
                aws_byte_buf_append(&metrics_string, &platform_cursor)) {
                goto error;
            }
            metrics_added = true;
        }
    }

    /* Build final output */
    // TODO: we should consider the case where total size extceeds MQTT username limit
    size_t total_size = (original_username ? original_username->len : 0) + metrics_string.len + 1;

    if (aws_byte_buf_init(output_username, allocator, total_size)) {
        goto error;
    }

    /* Add original username */
    if (original_username && original_username->len > 0) {
        if (aws_byte_buf_append(output_username, original_username)) {
            goto error_output;
        }
    }

    /* Add metrics with separator */
    if (metrics_string.len > 0) {

        struct aws_byte_cursor metrics_cursor = aws_byte_cursor_from_buf(&metrics_string);
        if (aws_mqtt_validate_utf8_text(metrics_cursor) == AWS_OP_ERR) {
            goto error_output;
        }

        struct aws_byte_cursor separator = has_query ? amp : question_mark_str;
        if (aws_byte_buf_append(output_username, &separator)) {
            goto error_output;
        }

        if (aws_byte_buf_append(output_username, &metrics_cursor)) {
            goto error_output;
        }
    }

    aws_byte_buf_clean_up(&metrics_string);
    return AWS_OP_SUCCESS;

error_output:
    aws_byte_buf_clean_up(output_username);
error:
    aws_byte_buf_clean_up(&metrics_string);
    return AWS_OP_ERR;
}
