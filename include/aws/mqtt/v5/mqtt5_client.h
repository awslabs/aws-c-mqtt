#ifndef AWS_MQTT_MQTT5_CLIENT_H
#define AWS_MQTT_MQTT5_CLIENT_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

struct aws_client_bootstrap;
struct aws_http_header;
struct aws_http_message;
struct aws_http_proxy_options;
struct aws_socket_options;
struct aws_tls_connection_options;

struct aws_mqtt5_client;

/*
 * Fundamental Tenets:
 *
 * (1) All mqtt packet information must be surfaced to the user (previously many things just got consumed but
 * not surfaced; with user properties and dynamic negotiation we might as well tell the user about everything.
 * (2) Only keep mqtt spec level behavior (+ reconnect) in the base client and layer on a higher level client that
 * is responsible/extensible for additional behavior (queuing, etc...)
 * (3) config is done 100% at client creation time.
 */
/*
// Higher level client stuff, rejected from here

// queuing configuration

enum aws_mqtt5_operation_queuing_behavior_type {
    AWS_MQTT5_OQBT_NONE,
    AWS_MQTT5_OQBT_BOUNDED,
    AWS_MQTT5_OQBT_UNBOUNDED,
};

enum aws_mqtt5_operation_bounding_queue_limit_type {
    AWS_MQTT5_OBQLT_PAYLOAD_SIZE,
    AWS_MQTT5_OBQLT_QUEUE_SIZE,
};

// ??;
struct aws_mqtt5_client_queuing_options {
    enum aws_mqtt5_operation_queuing_behavior_type queuing_behavior;
    enum aws_mqtt5_operation_bounding_queue_limit_type limit_type;
    uint64_t limit_bound;
};

 */

// likely private impl detail
typedef uint64_t aws_mqtt5_op_id;
typedef uint16_t aws_mqtt5_packet_id;

// protocol types

enum aws_mqtt5_qos {
    AWS_MQTT5_QOS_AT_MOST_ONCE = 0x0,
    AWS_MQTT5_QOS_AT_LEAST_ONCE = 0x1,
    AWS_MQTT5_QOS_EXACTLY_ONCE = 0x2,
};

struct aws_mqtt5_reason {
    uint32_t reason_code;
    struct aws_byte_cursor reason_string;
};

// ephemeral kind of sucks, but we want to communicate "by-value"/"non-persistent" because we'll have
// matching persistent types in the implementation
struct aws_mqtt5_ephemeral_property {
    struct aws_byte_cursor name;
    struct aws_byte_cursor value;
};

struct aws_mqtt5_epehemeral_property_list {
    struct aws_mqtt5_ephemeral_property *properties;
    uint32_t property_count;
};

// implementation types

/* Timeout configuration */

struct aws_mqtt5_timeout_options {
    uint16_t keep_alive_time_secs;
    uint32_t ping_timeout_ms;
    uint32_t protocol_operation_timeout_ms; // consider removing or reworking or moving to higher level client
};

/* Session configuration */

struct aws_mqtt5_client_session_options {
    uint32_t session_expiry_interval;
    bool clean_start;

    // potentially rework into a strategy given that IoT core limits (8/sub) make this tricky
    bool resubscribe_on_clean_connect;
};



/* Connection event callback configuration */

/*
 alternatively, bind the events to actual packets (or pseudo packet for a socket-level disconnect):

enum aws_mqtt5_client_connection_event_type {
    // packet events with properties
    AWS_MQTT5_CCET_CONNACK,
    AWS_MQTT5_CCET_DISCONNECT,

    // socket disconnect, no properties/data
    AWS_MQTT5_CCET_CONNECTION_INTERRUPTED,

    // timeout during connect process
    AWS_MQTT5_CCET_CONNECT_TIMEOUT,

    // timeout due to keep alive
    AWS_MQTT5_CCET_KEEP_ALIVE_TIMEOUT,

    // client stop callback, cleanup/shutdown event
    AWS_MQTT5_CCET_STOPPED,
};

 */

enum aws_mqtt5_client_lifecycle_event_type {
    AWS_MQTT5_CLET_CONNECTION_ESTABLISHED,
    AWS_MQTT5_CLET_CONNECTION_FAILED,
    AWS_MQTT5_CLET_CONNECTION_INTERRUPTED,
    AWS_MQTT5_CLET_STOPPED,
};

// ??;
struct aws_mqtt5_connection_event {
    enum aws_mqtt5_client_connection_event_type event_type;
    int aws_error_code;
    struct aws_mqtt5_reason reason;
    void *connection_event_user_data;

    // additional properties (session, first-time, properties, etc...) ?
    // make this a union/algebraic type
};

typedef void (aws_mqtt5_client_connection_event_callback_fn)(struct aws_mqtt5_connection_event *event);

struct aws_mqtt5_client_connection_event_options {
    aws_mqtt5_client_connection_event_callback_fn *connection_event_callback;
    void *connection_event_user_data;
};

/* Will configuration */

/* Add a set-property API for all mqtt property structs that sets the flag and the value? */
enum aws_mqtt5_client_will_property_flags {
    AWS_MQTT5_CWPF_PAYLOAD_FORMAT       = 1 << 0,
    AWS_MQTT5_CWPF_MESSAGE_EXPIRY       = 1 << 1,
    AWS_MQTT5_CWPF_CONTENT_TYPE         = 1 << 2,
    AWS_MQTT5_CWPF_RESPONSE_TOPIC       = 1 << 3,
    AWS_MQTT5_CWPF_CORRELATION_DATA     = 1 << 4,
    AWS_MQTT5_CWPF_WILL_DELAY           = 1 << 5,
};

struct aws_mqtt5_client_will_property_options {
    enum aws_mqtt5_client_will_property_flags valid_properties;

    uint8_t payload_format;  // 1
    uint32_t message_expiry; // 2
    struct aws_byte_cursor content_type; // 3
    struct aws_byte_cursor response_topic; // 8
    struct aws_byte_cursor correlation_data; // 9
    uint32_t will_delay; // 24
};

struct aws_mqtt5_client_will_options {
    enum aws_mqtt5_qos qos;
    struct aws_byte_cursor topic;
    struct aws_byte_cursor payload;
    bool retained;

    struct aws_mqtt5_property_list *user_properties;
    struct aws_mqtt5_client_will_property_options properties;
};

/* Websockets configuration */

typedef void(aws_mqtt5_transform_websocket_handshake_complete_fn)(
    struct aws_http_message *request,
    int error_code,
    void *complete_ctx);

typedef void(aws_mqtt5_transform_websocket_handshake_fn)(
    struct aws_http_message *request,
    void *user_data,
    aws_mqtt_transform_websocket_handshake_complete_fn *complete_fn,
    void *complete_ctx);

struct aws_mqtt5_client_websocket_options {
    aws_mqtt_transform_websocket_handshake_fn *handshake_transformation;
    void *handshake_transformation_user_data;
};

/* Reconnect configuration */

enum aws_mqtt5_client_reconnect_behavior_type {
    AWS_MQTT5_CRBT_RECONNECT_IF_INITIAL_SUCCESS,
    AWS_MQTT5_CRBT_RECONNECT_ALWAYS,
    AWS_MQTT5_CRBT_RECONNECT_NEVER,
};

struct aws_mqtt5_client_reconnect_options {
    enum aws_mqtt5_client_reconnect_behavior_type reconnect_behavior;
    uint64_t min_reconnect_delay_ms;
    uint64_t max_reconnect_delay_ms;
    uint32_t min_connected_time_to_reset_reconnect_delay;
};

/* CONNECT packet configuration */

enum aws_mqtt5_client_connect_property_flags {
    AWS_MQTT5_CCPF_SESSION_EXPIRY_INTERVAL          = 1 << 0,
    AWS_MQTT5_CCPF_AUTHENTICATION_METHOD            = 1 << 1,
    AWS_MQTT5_CCPF_AUTHENTICATION_DATA              = 1 << 2,
    AWS_MQTT5_CCPF_REQUEST_PROBLEM_INFORMATION      = 1 << 3,
    AWS_MQTT5_CCPF_REQUEST_RESPONSE_INFORMATION     = 1 << 4,
    AWS_MQTT5_CCPF_RECEIVE_MAXIMUM                  = 1 << 5,
    AWS_MQTT5_CCPF_TOPIC_ALIAS_MAXIMUM              = 1 << 6,
    AWS_MQTT5_CCPF_MAXIMUM_PACKET_SIZE              = 1 << 7,
};

struct aws_mqtt5_client_connect_property_options {
    enum aws_mqtt5_client_connect_property_flags valid_properties;

    uint32_t session_expiry_interval; // 17
    struct aws_byte_cursor authentication_method; // 21
    struct aws_byte_cursor authentication_data; // 22
    bool request_problem_information; // 23
    bool request_response_information; // 25
    uint16_t receive_maximum; // 33
    uint16_t topic_aliax_maximum; // 34
    uint32_t maximum_packet_size; // 39
};

struct aws_mqtt5_client_connect_options {
    struct aws_byte_cursor client_id;
    struct aws_byte_cursor *username;
    struct aws_byte_cursor *password;

    struct aws_mqtt5_client_session_options *session_options;
    struct aws_mqtt5_client_will_options *will_options;
    struct aws_mqtt5_client_connect_property_options *connect_property_options;
};

/* Underlying transport (tcp/websocket/tls) configuration */

struct aws_mqtt5_client_transport_config_options {
    struct aws_byte_cursor host_name;
    uint16_t port;
    struct aws_client_bootstrap *bootstrap;
    struct aws_socket_options *socket_options;
    struct aws_tls_connection_options *tls_options;
    struct aws_http_proxy_options *http_proxy_options;
    struct aws_mqtt5_client_websocket_options *websocket_options;
};

/* Client configuration */

struct aws_mqtt5_client_config_options {
    struct aws_mqtt5_client_transport_config_options transport_options;
    struct aws_mqtt5_client_connect_options *connect_options;

    /*
     * Persistent client behavior and configuration
     */
    struct aws_mqtt5_timeout_options *timeout_options;
    struct aws_mqtt5_client_connection_event_options *connection_event_options;
    struct aws_mqtt5_client_reconnect_options *reconnect_options;
};



AWS_EXTERN_C_BEGIN

AWS_MQTT_API
int aws_mqtt5_client_config_options_validate(struct aws_mqtt5_client_config_options *config_options);

AWS_MQTT_API
struct aws_mqtt5_client *aws_mqtt5_client_new(struct aws_allocator *allocator, struct aws_mqtt5_client_config_options *config_options);

AWS_MQTT_API
struct aws_mqtt5_client *aws_mqtt5_client_acquire(struct aws_mqtt5_client *client);

AWS_MQTT_API
void aws_mqtt5_client_release(struct aws_mqtt5_client *client);

AWS_MQTT_API
int aws_mqtt5_client_start(struct aws_mqtt5_client *client);

AWS_MQTT_API
int aws_mqtt5_client_stop(struct aws_mqtt5_client *client, ?? on_stop_complete_callback, void *on_stop_complete_callback_userdata);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_MQTT5_CLIENT_H */
