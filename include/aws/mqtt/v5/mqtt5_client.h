#ifndef AWS_MQTT_MQTT5_CLIENT_H
#define AWS_MQTT_MQTT5_CLIENT_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

#include <aws/mqtt/v5/mqtt5_types.h>

struct aws_allocator;
struct aws_byte_cursor;
struct aws_client_bootstrap;
struct aws_http_proxy_strategy;
struct aws_mqtt5_client;
struct aws_tls_connection_options;
struct aws_socket_options;

/* ToDo: this is almost certainly the wrong value to use as a default */
#define AWS_MQTT5_DEFAULT_SESSION_EXPIRY_INTERVAL_SECONDS 0
#define AWS_MQTT5_DEFAULT_KEEP_ALIVE_INTERVAL_SECONDS 1200

struct aws_mqtt5_subscription_options {
    struct aws_byte_cursor topic;
    enum aws_mqtt5_qos qos;
    bool no_local;
    bool retain_as_published;
    enum aws_mqtt5_retain_handling_type retain_handling_type;
};

struct aws_mqtt5_operation_disconnect_options {
    enum aws_mqtt5_disconnect_reason_code reason_code;
    uint32_t *session_expiry_interval_seconds;
    struct aws_byte_cursor *reason_string;

    size_t user_property_count;
    struct aws_mqtt5_user_property *user_properties;
};

typedef struct aws_mqtt5_operation_disconnect_options aws_mqtt5_operation_disconnect_view;

struct aws_mqtt5_operation_subscribe_options {
    size_t subscription_count;
    struct aws_mqtt5_subscription_options *subscriptions;

    uint32_t *subscription_identifier;

    size_t user_property_count;
    struct aws_mqtt5_user_property *user_properties;
};

struct aws_mqtt5_operation_unsubscribe_options {
    size_t topic_count;
    struct aws_byte_cursor *topics;

    size_t user_property_count;
    struct aws_mqtt5_user_property *user_properties;
};

struct aws_mqtt5_operation_publish_options {
    struct aws_byte_cursor payload; /* possibly an input stream in the future */

    enum aws_mqtt5_qos qos;
    bool retain;
    struct aws_byte_cursor topic;
    enum aws_mqtt5_payload_format_indicator payload_format;
    uint32_t *message_expiry_interval_seconds;
    uint16_t *topic_alias;
    struct aws_byte_cursor *response_topic;
    struct aws_byte_cursor *correlation_data;
    uint32_t *subscription_identifier;
    struct aws_byte_cursor *content_type;

    size_t user_property_count;
    struct aws_mqtt5_user_property *user_properties;
};

struct aws_mqtt5_operation_connect_options {
    uint32_t keep_alive_interval_seconds;

    struct aws_byte_cursor client_id;

    struct aws_byte_cursor *username;
    struct aws_byte_cursor *password;

    uint32_t session_expiry_interval_seconds;
    enum aws_mqtt5_client_session_behavior_type session_behavior;

    bool request_response_information;
    bool request_problem_information;
    uint16_t receive_maximum;
    uint16_t topic_alias_maximum;
    uint32_t maximum_packet_size_bytes;

    uint32_t will_delay_interval_seconds;
    struct aws_mqtt5_operation_publish_options *will;

    size_t user_property_count;
    struct aws_mqtt5_user_property *user_properties;
};

struct aws_mqtt5_client_options {
    struct aws_byte_cursor host_name;
    uint16_t port;
    struct aws_client_bootstrap *bootstrap;
    struct aws_socket_options *socket_options;
    struct aws_tls_connection_options *tls_options;
    struct aws_http_proxy_options *http_proxy_options;

    aws_mqtt5_transform_websocket_handshake_fn *websocket_handshake_transform;
    void *websocket_handshake_transform_user_data;

    enum aws_mqtt5_client_reconnect_behavior_type reconnect_behavior;
    uint64_t min_reconnect_delay_ms;
    uint64_t max_reconnect_delay_ms;
    uint64_t min_connected_time_to_reset_reconnect_delay_ms;

    uint32_t ping_timeout_ms;

    struct aws_mqtt5_operation_connect_options *connect_options;

    aws_mqtt5_client_connection_event_callback_fn *lifecycle_event_handler;
    void *lifecycle_event_handler_user_data;
};

AWS_EXTERN_C_BEGIN

AWS_MQTT_API
struct aws_mqtt5_client *aws_mqtt5_client_new(
    struct aws_allocator *allocator,
    struct aws_mqtt5_client_options *options);

AWS_MQTT_API
struct aws_mqtt5_client *aws_mqtt5_client_acquire(struct aws_mqtt5_client *client);

AWS_MQTT_API
void aws_mqtt5_client_release(struct aws_mqtt5_client *client);

AWS_MQTT_API
int aws_mqtt5_client_start(struct aws_mqtt5_client *client);

AWS_MQTT_API
int aws_mqtt5_client_stop(
    struct aws_mqtt5_client *client,
    struct aws_mqtt5_operation_disconnect_options *disconnect_options);

AWS_MQTT_API
int aws_mqtt5_client_publish(
    struct aws_mqtt5_client *client,
    struct aws_mqtt5_operation_publish_options *publish_options);

AWS_MQTT_API
int aws_mqtt5_client_subscribe(
    struct aws_mqtt5_client *client,
    struct aws_mqtt5_operation_subscribe_options *subscribe_options);

AWS_MQTT_API
int aws_mqtt5_client_unsubscribe(
    struct aws_mqtt5_client *client,
    struct aws_mqtt5_operation_unsubscribe_options *unsubscribe_options);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_MQTT5_CLIENT_H */
