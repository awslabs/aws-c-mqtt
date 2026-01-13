/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#ifndef AWS_MQTT_IOT_SDK_METRICS_H
#define AWS_MQTT_IOT_SDK_METRICS_H

/* Storage for `aws_mqtt_iot_sdk_metrics`. */
struct aws_mqtt_iot_sdk_metrics_storage {
    struct aws_allocator *allocator;

    struct aws_array_list metadata_entries;

    struct aws_byte_cursor library_name;

    struct aws_byte_buf storage;
};

AWS_MQTT_API struct aws_mqtt_iot_sdk_metrics_storage *aws_mqtt_iot_sdk_metrics_storage_new(
    struct aws_allocator *allocator,
    const struct aws_mqtt_iot_sdk_metrics *metrics_options);

AWS_MQTT_API void aws_mqtt_iot_sdk_metrics_storage_destroy(struct aws_mqtt_iot_sdk_metrics_storage *metrics_storage);

/**
 * Appends SDK metrics to the username
 *
 * @param original_username The original username
 * @param metrics_storage The metrics configuration
 * @param output_username Buffer to store the modified username. If the function succeed, caller is responsible to
 * release the memory for output_username.
 * @param out_full_username_size If not NULL, will be set to the full size of the username with metrics appended
 *
 * @return AWS_OP_SUCCESS on success, AWS_OP_ERR on failure
 */
AWS_MQTT_API
int aws_mqtt_append_sdk_metrics_to_username(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *original_username,
    const struct aws_mqtt_iot_sdk_metrics_storage *metrics_storage,
    struct aws_byte_buf *output_username,
    size_t *out_full_username_size);

/**
 * Validates that all string fields in aws_mqtt_iot_sdk_metrics
 *
 * @param metrics The metrics structure to validate
 * @return AWS_OP_SUCCESS if all strings are valid, AWS_OP_ERR otherwise
 */
AWS_MQTT_API
int aws_mqtt_validate_iot_sdk_metrics(const struct aws_mqtt_iot_sdk_metrics *metrics);

#endif /* AWS_MQTT_IOT_SDK_METRICS_H */
