#ifndef AWS_MQTT_MQTT5_OPERATION_UNSUBSCRIBE_H
#define AWS_MQTT_MQTT5_OPERATION_UNSUBSCRIBE_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

struct aws_allocator;
struct aws_byte_cursor;
struct aws_mqtt5_operation_unsubscribe;
struct aws_mqtt5_user_property;

AWS_EXTERN_C_BEGIN

AWS_MQTT_API struct aws_mqtt5_operation_unsubscribe *aws_mqtt5_operation_unsubscribe_new(
    struct aws_allocator *allocator);

AWS_MQTT_API struct aws_mqtt5_operation_unsubscribe *aws_mqtt5_operation_unsubscribe_acquire(
    struct aws_mqtt5_operation_unsubscribe *unsubscribe_operation);

AWS_MQTT_API struct aws_mqtt5_operation_unsubscribe *aws_mqtt5_operation_unsubscribe_release(
    struct aws_mqtt5_operation_unsubscribe *unsubscribe_operation);

AWS_MQTT_API int aws_mqtt5_operation_unsubscribe_add_topic(
    struct aws_mqtt5_operation_unsubscribe *unsubscribe_operation,
    struct aws_byte_cursor topic);

AWS_MQTT_API int aws_mqtt5_operation_unsubscribe_add_user_property(
    struct aws_mqtt5_operation_unsubscribe *unsubscribe_operation,
    struct aws_mqtt5_user_property *property);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_MQTT5_OPERATION_UNSUBSCRIBE_H */
