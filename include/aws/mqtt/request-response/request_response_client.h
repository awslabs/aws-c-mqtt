#ifndef AWS_MQTT_REQUEST_RESPONSE_REQUEST_RESPONSE_CLIENT_H
#define AWS_MQTT_REQUEST_RESPONSE_REQUEST_RESPONSE_CLIENT_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/mqtt/mqtt.h"

struct aws_mqtt_request_response_client;
struct aws_mqtt_client_connection;
struct aws_mqtt5_client;

struct aws_mqtt_request_response_client_options {
    size_t max_subscriptions;
    uint32_t operation_timeout_seconds;
};

AWS_EXTERN_C_BEGIN

struct aws_mqtt_request_response_client *aws_mqtt_request_response_client_new_from_mqtt311_client(struct aws_allocator *allocator, struct aws_mqtt_client_connection *client, const struct aws_mqtt_request_response_client_options *options);

struct aws_mqtt_request_response_client *aws_mqtt_request_response_client_new_from_mqtt5_client(struct aws_allocator *allocator, struct aws_mqtt5_client *client, const struct aws_mqtt_request_response_client_options *options);

struct aws_mqtt_request_response_client *aws_mqtt_request_response_client_acquire(struct aws_mqtt_request_response_client *client);

struct aws_mqtt_request_response_client *aws_mqtt_request_response_client_release(struct aws_mqtt_request_response_client *client);


AWS_EXTERN_C_END

#endif /* AWS_MQTT_REQUEST_RESPONSE_REQUEST_RESPONSE_CLIENT_H */
