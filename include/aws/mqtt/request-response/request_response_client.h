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

typedef void(aws_mqtt_request_response_client_initialized_callback_fn)(void *user_data);
typedef void(aws_mqtt_request_response_client_terminated_callback_fn)(void *user_data);

struct aws_mqtt_request_response_client_options {
    size_t max_subscriptions;
    uint32_t operation_timeout_seconds;

    /* Do not bind the initialized callback; it exists mostly for tests and should not be exposed */
    aws_mqtt_request_response_client_initialized_callback_fn *initialized_callback;

    aws_mqtt_request_response_client_terminated_callback_fn *terminated_callback;
    void *user_data;
};

AWS_EXTERN_C_BEGIN

/*
 * Create a new request-response client that uses an MQTT311 client.
 */
struct aws_mqtt_request_response_client *aws_mqtt_request_response_client_new_from_mqtt311_client(
    struct aws_allocator *allocator,
    struct aws_mqtt_client_connection *client,
    const struct aws_mqtt_request_response_client_options *options);

/*
 * Create a new request-response client that uses an MQTT5 client.
 */
struct aws_mqtt_request_response_client *aws_mqtt_request_response_client_new_from_mqtt5_client(
    struct aws_allocator *allocator,
    struct aws_mqtt5_client *client,
    const struct aws_mqtt_request_response_client_options *options);

/*
 * Add a reference to a request-response client
 */
struct aws_mqtt_request_response_client *aws_mqtt_request_response_client_acquire(
    struct aws_mqtt_request_response_client *client);

/*
 * Remove a reference to a request-response client
 */
struct aws_mqtt_request_response_client *aws_mqtt_request_response_client_release(
    struct aws_mqtt_request_response_client *client);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_REQUEST_RESPONSE_REQUEST_RESPONSE_CLIENT_H */
