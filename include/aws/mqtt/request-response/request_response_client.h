#ifndef AWS_MQTT_REQUEST_RESPONSE_REQUEST_RESPONSE_CLIENT_H
#define AWS_MQTT_REQUEST_RESPONSE_REQUEST_RESPONSE_CLIENT_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

struct aws_mqtt_request_response_client;
struct aws_mqtt_client_connection;
struct aws_mqtt5_client;

struct aws_mqtt_request_operation_response_path {
    struct aws_byte_cursor topic;

    /* potential point of expansion into an abstract "extractor" if we ever need to support non-JSON payloads */
    struct aws_byte_cursor correlation_token_json_path;
};

typedef void(
    aws_mqtt_request_operation_completion_fn)(struct aws_byte_cursor *payload, int error_code, void *user_data);

struct aws_mqtt_request_operation_options {
    struct aws_byte_cursor subscription_topic_filter;

    struct aws_mqtt_request_operation_response_path *response_paths;
    size_t response_path_count;

    struct aws_byte_cursor publish_topic;
    struct aws_byte_cursor serialized_request;
    struct aws_byte_cursor correlation_token;

    aws_mqtt_request_operation_completion_fn *completion_callback;
    void *user_data;
};

struct aws_mqtt_request_operation_storage {
    struct aws_mqtt_request_operation_options options;

    struct aws_array_list operation_response_paths;

    struct aws_byte_buf operation_data;
};

/*
 * Describes a change to the state of a request operation subscription
 */
enum aws_rr_streaming_subscription_event_type {

    /*
     * The streaming operation is successfully subscribed to its topic (filter)
     */
    ARRSSET_SUBSCRIPTION_ESTABLISHED,

    /*
     * The streaming operation has temporarily lost its subscription to its topic (filter)
     */
    ARRSSET_SUBSCRIPTION_LOST,

    /*
     * The streaming operation has entered a terminal state where it has given up trying to subscribe
     * to its topic (filter).  This is always due to user error (bad topic filter or IoT Core permission policy).
     */
    ARRSSET_SUBSCRIPTION_HALTED,
};

typedef void(aws_mqtt_streaming_operation_subscription_status_fn)(
    enum aws_rr_streaming_subscription_event_type status,
    int error_code,
    void *user_data);
typedef void(aws_mqtt_streaming_operation_incoming_publish_fn)(struct aws_byte_cursor payload, void *user_data);
typedef void(aws_mqtt_streaming_operation_terminated_fn)(void *user_data);

struct aws_mqtt_streaming_operation_options {
    struct aws_byte_cursor topic_filter;

    aws_mqtt_streaming_operation_subscription_status_fn *subscription_status_callback;
    aws_mqtt_streaming_operation_incoming_publish_fn *incoming_publish_callback;
    aws_mqtt_streaming_operation_terminated_fn *terminated_callback;

    void *user_data;
};

struct aws_mqtt_streaming_operation_storage {
    struct aws_mqtt_streaming_operation_options options;

    struct aws_byte_buf operation_data;
};

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
AWS_MQTT_API struct aws_mqtt_request_response_client *aws_mqtt_request_response_client_new_from_mqtt311_client(
    struct aws_allocator *allocator,
    struct aws_mqtt_client_connection *client,
    const struct aws_mqtt_request_response_client_options *options);

/*
 * Create a new request-response client that uses an MQTT5 client.
 */
AWS_MQTT_API struct aws_mqtt_request_response_client *aws_mqtt_request_response_client_new_from_mqtt5_client(
    struct aws_allocator *allocator,
    struct aws_mqtt5_client *client,
    const struct aws_mqtt_request_response_client_options *options);

/*
 * Add a reference to a request-response client
 */
AWS_MQTT_API struct aws_mqtt_request_response_client *aws_mqtt_request_response_client_acquire(
    struct aws_mqtt_request_response_client *client);

/*
 * Remove a reference to a request-response client
 */
AWS_MQTT_API struct aws_mqtt_request_response_client *aws_mqtt_request_response_client_release(
    struct aws_mqtt_request_response_client *client);

AWS_MQTT_API int aws_mqtt_request_response_client_submit_request(
    struct aws_mqtt_request_response_client *client,
    const struct aws_mqtt_request_operation_options *request_options);

AWS_MQTT_API struct aws_mqtt_rr_client_operation *aws_mqtt_request_response_client_create_streaming_operation(
    struct aws_mqtt_request_response_client *client,
    const struct aws_mqtt_streaming_operation_options *streaming_options);

AWS_MQTT_API struct aws_mqtt_rr_client_operation *aws_mqtt_rr_client_operation_acquire(
    struct aws_mqtt_rr_client_operation *operation);

AWS_MQTT_API struct aws_mqtt_rr_client_operation *aws_mqtt_rr_client_operation_release(
    struct aws_mqtt_rr_client_operation *operation);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_REQUEST_RESPONSE_REQUEST_RESPONSE_CLIENT_H */
