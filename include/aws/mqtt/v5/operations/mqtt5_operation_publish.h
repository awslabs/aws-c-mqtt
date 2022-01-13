#ifndef AWS_MQTT_MQTT5_OPERATION_PUBLISH_H
#define AWS_MQTT_MQTT5_OPERATION_PUBLISH_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

struct aws_allocator;
struct aws_mqtt5_operation_publish;

AWS_EXTERN_C_BEGIN

AWS_MQTT_API struct aws_mqtt5_operation_publish *aws_mqtt5_operation_publish_new(struct aws_allocator *allocator);

AWS_MQTT_API struct aws_mqtt5_operation_publish *aws_mqtt5_operation_publish_acquire(
    struct aws_mqtt5_operation_publish *publish_operation);

AWS_MQTT_API struct aws_mqtt5_operation_publish *aws_mqtt5_operation_publish_release(
    struct aws_mqtt5_operation_publish *publish_operation);

AWS_MQTT_API int aws_mqtt5_operation_publish_set_payload_by_cursor(
    struct aws_mqtt5_operation_publish *publish_operation,
    struct aws_byte_cursor payload);

AWS_MQTT_API void aws_mqtt5_operation_publish_set_qos(
    struct aws_mqtt5_operation_publish *publish_operation,
    enum aws_mqtt5_qos qos);

AWS_MQTT_API void aws_mqtt5_operation_publish_set_retain(
    struct aws_mqtt5_operation_publish *publish_operation,
    bool retain);

AWS_MQTT_API int aws_mqtt5_operation_publish_set_topic(
    struct aws_mqtt5_operation_publish *publish_operation,
    struct aws_byte_cursor topic);

AWS_MQTT_API void aws_mqtt5_operation_publish_set_payload_format_indicator(
    struct aws_mqtt5_operation_publish *publish_operation,
    enum aws_mqtt5_payload_format_indicator payload_format);

AWS_MQTT_API void aws_mqtt5_operation_publish_set_message_expiry_interval(
    struct aws_mqtt5_operation_publish *publish_operation,
    uint32_t message_expiry_interval_seconds);

AWS_MQTT_API void aws_mqtt5_operation_publish_set_topic_alias(
    struct aws_mqtt5_operation_publish *publish_operation,
    uint16_t topic_alias);

AWS_MQTT_API int aws_mqtt5_operation_publish_set_response_topic(
    struct aws_mqtt5_operation_publish *publish_operation,
    struct aws_byte_cursor response_topic);

AWS_MQTT_API int aws_mqtt5_operation_publish_set_correlation_data(
    struct aws_mqtt5_operation_publish *publish_operation,
    struct aws_byte_cursor correlation_data);

AWS_MQTT_API int aws_mqtt5_operation_publish_set_subscription_identifier(
    struct aws_mqtt5_operation_publish *publish_operation,
    uint32_t subscription_identifier);

AWS_MQTT_API int aws_mqtt5_operation_publish_set_content_type(
    struct aws_mqtt5_operation_publish *publish_operation,
    struct aws_byte_cursor content_type);

AWS_MQTT_API int aws_mqtt5_operation_publish_add_user_property(
    struct aws_mqtt5_operation_publish *publish_operation,
    struct aws_mqtt5_user_property *property);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_MQTT5_OPERATION_PUBLISH_H */
