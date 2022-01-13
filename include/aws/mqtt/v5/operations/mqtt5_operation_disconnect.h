#ifndef AWS_MQTT_MQTT5_OPERATION_DISCONNECT_H
#define AWS_MQTT_MQTT5_OPERATION_DISCONNECT_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

#include <aws/mqtt/v5/mqtt5_types.h>

struct aws_allocator;
struct aws_byte_cursor;
struct aws_mqtt5_operation_disconnect;
struct aws_mqtt5_user_property;

AWS_EXTERN_C_BEGIN

AWS_MQTT_API struct aws_mqtt5_operation_disconnect *aws_mqtt5_operation_disconnect_new(struct aws_allocator *allocator);

AWS_MQTT_API struct aws_mqtt5_operation_disconnect *aws_mqtt5_operation_disconnect_acquire(
    struct aws_mqtt5_operation_disconnect *disconnect_operation);

AWS_MQTT_API struct aws_mqtt5_operation_disconnect *aws_mqtt5_operation_disconnect_release(
    struct aws_mqtt5_operation_disconnect *disconnect_operation);

AWS_MQTT_API void aws_mqtt5_operation_disconnect_set_reason_code(
    struct aws_mqtt5_operation_disconnect *disconnect_operation,
    enum aws_mqtt5_disconnect_reason_code reason_code);

AWS_MQTT_API void aws_mqtt5_operation_disconnect_set_session_expiry_interval(
    struct aws_mqtt5_operation_disconnect *disconnect_operation,
    uint32_t session_expiry_interval_seconds);

AWS_MQTT_API int aws_mqtt5_operation_disconnect_set_reason_string(
    struct aws_mqtt5_operation_disconnect *disconnect_operation,
    struct aws_byte_cursor reason);

AWS_MQTT_API int aws_mqtt5_operation_disconnect_add_user_property(
    struct aws_mqtt5_operation_disconnect *disconnect_operation,
    struct aws_mqtt5_user_property *property);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_MQTT5_OPERATION_CONNECT_H */
