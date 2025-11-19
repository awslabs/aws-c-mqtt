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

int aws_mqtt_append_sdk_metrics_to_username(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *original_username,
    const struct aws_mqtt_iot_sdk_metrics *metrics,
    struct aws_byte_buf *output_username) {

    // WIP
    // if (!allocator || !metrics || !output_username) {
    //     return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    // }

    // /* Estimate buffer size needed */
    // size_t estimated_size = 256;
    // if (original_username && original_username->len > 0) {
    //     estimated_size += original_username->len;
    // }

    // if (aws_byte_buf_init(output_username, allocator, estimated_size)) {
    //     return AWS_OP_ERR;
    // }

    // /* Add original username if it exists */
    // if (original_username && original_username->len > 0) {
    //     if (aws_byte_buf_append(output_username, original_username)) {
    //         goto error;
    //     }

    //     /* Add separator */
    //     struct aws_byte_cursor separator = aws_byte_cursor_from_c_str("?");
    //     if (aws_byte_buf_append(output_username, &separator)) {
    //         goto error;
    //     }
    // }

    // /* Add SDK attribute */
    // if (metrics->library_name.len > 0) {
    //     struct aws_byte_cursor sdk_prefix =
    //         aws_byte_cursor_from_c_str(original_username && original_username->len > 0 ? "SDK=" : "?SDK=");
    //     if (aws_byte_buf_append(output_username, &sdk_prefix)) {
    //         goto error;
    //     }
    //     if (aws_byte_buf_append(output_username, &metrics->library_name)) {
    //         goto error;
    //     }
    // }

    // /* Add Version attribute */
    // if (metrics->library_version.len > 0) {
    //     struct aws_byte_cursor version_prefix = aws_byte_cursor_from_c_str("&Version=");
    //     if (aws_byte_buf_append(output_username, &version_prefix)) {
    //         goto error;
    //     }
    //     if (aws_byte_buf_append(output_username, &metrics->library_version)) {
    //         goto error;
    //     }
    // }

    // /* Add Platform attribute */
    // struct aws_byte_cursor platform_cursor = aws_get_platform_build_os_string();

    // struct aws_byte_cursor platform_prefix = aws_byte_cursor_from_c_str("&Platform=");
    // if (aws_byte_buf_append(output_username, &platform_prefix)) {
    //     goto error;
    // }
    // if (aws_byte_buf_append(output_username, &platform_cursor)) {
    //     goto error;
    // }

    // /* Add Metadata if present */
    // if (metrics->metadata_entries && metrics->metadata_count > 0) {
    //     struct aws_byte_cursor metadata_prefix = aws_byte_cursor_from_c_str("&Metadata=(");
    //     if (aws_byte_buf_append(output_username, &metadata_prefix)) {
    //         goto error;
    //     }

    //     for (size_t i = 0; i < metrics->metadata_count; ++i) {
    //         if (i > 0) {
    //             struct aws_byte_cursor semicolon = aws_byte_cursor_from_c_str(";");
    //             if (aws_byte_buf_append(output_username, &semicolon)) {
    //                 goto error;
    //             }
    //         }

    //         if (aws_byte_buf_append(output_username, &metrics->metadata_entries[i].key)) {
    //             goto error;
    //         }

    //         struct aws_byte_cursor equals = aws_byte_cursor_from_c_str("=");
    //         if (aws_byte_buf_append(output_username, &equals)) {
    //             goto error;
    //         }

    //         if (aws_byte_buf_append(output_username, &metrics->metadata_entries[i].value)) {
    //             goto error;
    //         }
    //     }

    //     struct aws_byte_cursor metadata_suffix = aws_byte_cursor_from_c_str(")");
    //     if (aws_byte_buf_append(output_username, &metadata_suffix)) {
    //         goto error;
    //     }
    // }

    return AWS_OP_SUCCESS;

error:
    aws_byte_buf_clean_up(output_username);
    return AWS_OP_ERR;
}
