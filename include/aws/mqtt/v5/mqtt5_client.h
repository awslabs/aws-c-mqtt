#ifndef AWS_MQTT_MQTT5_CLIENT_H
#define AWS_MQTT_MQTT5_CLIENT_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

struct aws_allocator;
struct aws_mqtt5_client;
struct aws_mqtt5_client_config;
struct aws_mqtt5_operation_disconnect;
struct aws_mqtt5_operation_publish;
struct aws_mqtt5_operation_subscribe;
struct aws_mqtt5_operation_unsubscribe;

AWS_EXTERN_C_BEGIN

AWS_MQTT_API
struct aws_mqtt5_client *aws_mqtt5_client_new(struct aws_allocator *allocator, struct aws_mqtt5_client_config *config);

AWS_MQTT_API
struct aws_mqtt5_client *aws_mqtt5_client_acquire(struct aws_mqtt5_client *client);

AWS_MQTT_API
void aws_mqtt5_client_release(struct aws_mqtt5_client *client);

AWS_MQTT_API
int aws_mqtt5_client_start(struct aws_mqtt5_client *client);

AWS_MQTT_API
int aws_mqtt5_client_stop(struct aws_mqtt5_client *client, struct aws_mqtt5_operation_disconnect *disconnect_operation);

AWS_MQTT_API
int aws_mqtt5_client_publish(struct aws_mqtt5_client *client, struct aws_mqtt5_operation_publish *publish_operation);

AWS_MQTT_API
int aws_mqtt5_client_subscribe(
    struct aws_mqtt5_client *client,
    struct aws_mqtt5_operation_subscribe *subscribe_operation);

AWS_MQTT_API
int aws_mqtt5_client_unsubscribe(
    struct aws_mqtt5_client *client,
    struct aws_mqtt5_operation_unsubscribe *unsubscribe_operation);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_MQTT5_CLIENT_H */
