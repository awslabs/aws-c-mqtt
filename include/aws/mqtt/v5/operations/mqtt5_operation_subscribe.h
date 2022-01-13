#ifndef AWS_MQTT_MQTT5_OPERATION_SUBSCRIBE_H
#define AWS_MQTT_MQTT5_OPERATION_SUBSCRIBE_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

struct aws_allocator;
struct aws_mqtt5_operation_subscribe;

AWS_EXTERN_C_BEGIN

AWS_MQTT_API struct aws_mqtt5_operation_subscribe *aws_mqtt5_operation_subscribe_new(struct aws_allocator *allocator);

AWS_MQTT_API struct aws_mqtt5_operation_subscribe *aws_mqtt5_operation_subscribe_acquire(
    struct aws_mqtt5_operation_subscribe *subscribe_operation);

AWS_MQTT_API struct aws_mqtt5_operation_subscribe *aws_mqtt5_operation_subscribe_release(
    struct aws_mqtt5_operation_subscribe *subscribe_operation);

AWS_MQTT_API int aws_mqtt5_operation_subscribe_add_user_property(
    struct aws_mqtt5_operation_subscribe *subscribe_operation,
    struct aws_mqtt5_user_property *property);

AWS_MQTT_API int aws_mqtt5_operation_subscribe_add_subscription(
    struct aws_mqtt5_operation_subscribe *subscribe_operation,
    struct aws_mqtt5_subscripion_view *subscription_view);

AWS_MQTT_API int aws_mqtt5_operation_subscribe_set_subscription_identifier(
    struct aws_mqtt5_operation_subscribe *subscribe_operation,
    uint32_t subscription_identifier);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_MQTT5_OPERATION_SUBSCRIBE_H */
