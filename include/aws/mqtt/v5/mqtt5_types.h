#ifndef AWS_MQTT_MQTT5_TYPES_H
#define AWS_MQTT_MQTT5_TYPES_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

#include <aws/common/array_list.h>
#include <aws/common/byte_buf.h>

struct aws_http_message;

/**
 * Some artificial (non-MQTT spec specified) limits that we place on input packets (publish, subscribe, unsubscibe)
 * which lets us safely do the various packet size calculations with a bare minimum of checked arithmetic.
 *
 * I don't see any conceivable use cases why you'd want more than this, but they are relaxable to some degree.
 *
 * TODO: Add some static assert calculations that verify that we can't possibly overflow against the maximum value
 * of a variable length integer for relevant packet size encodings that are absolute worst-case against these limits.
 */
#define AWS_MQTT5_CLIENT_MAXIMUM_USER_PROPERTIES 1024
#define AWS_MQTT5_CLIENT_MAXIMUM_SUBSCRIPTIONS_PER_SUBSCRIBE 1024
#define AWS_MQTT5_CLIENT_MAXIMUM_TOPICS_PER_UNSUBSCRIBE 1024

/**
 * Over-the-wire packet id as defined in the mqtt spec.  Allocated at the point in time when the packet is
 * is next to go down the channel and about to be encoded into an io message buffer.
 *
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901026
 */
typedef uint16_t aws_mqtt5_packet_id_t;

/**
 * MQTT Message delivery quality of service.
 * Enum values match mqtt spec encoding values.
 *
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901234
 */
enum aws_mqtt5_qos {

    /** https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901235 */
    AWS_MQTT5_QOS_AT_MOST_ONCE = 0x0,

    /** https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901236 */
    AWS_MQTT5_QOS_AT_LEAST_ONCE = 0x1,

    /** https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901237 */
    AWS_MQTT5_QOS_EXACTLY_ONCE = 0x2,
};

/**
 * Server return code for CONNECT attempts.
 * Enum values match mqtt spec encoding values.
 *
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901079
 */
enum aws_mqtt5_connect_reason_code {
    AWS_MQTT5_CRC_SUCCESS = 0,
    AWS_MQTT5_CRC_UNSPECIFIED_ERROR = 128,
    AWS_MQTT5_CRC_MALFORMED_PACKET = 129,
    AWS_MQTT5_CRC_PROTOCOL_ERROR = 130,
    AWS_MQTT5_CRC_IMPLEMENTATION_SPECIFIC_ERROR = 131,
    AWS_MQTT5_CRC_UNSUPPORTED_PROTOCOL_VERSION = 132,
    AWS_MQTT5_CRC_CLIENT_IDENTIFIER_NOT_VALID = 133,
    AWS_MQTT5_CRC_BAD_USERNAME_OR_PASSWORD = 134,
    AWS_MQTT5_CRC_NOT_AUTHORIZED = 135,
    AWS_MQTT5_CRC_SERVER_UNAVAILABLE = 136,
    AWS_MQTT5_CRC_SERVER_BUSY = 137,
    AWS_MQTT5_CRC_BANNED = 138,
    AWS_MQTT5_CRC_BAD_AUTHENTICATION_METHOD = 140,
    AWS_MQTT5_CRC_TOPIC_NAME_INVALID = 144,
    AWS_MQTT5_CRC_PACKET_TOO_LARGE = 149,
    AWS_MQTT5_CRC_QUOTA_EXCEEDED = 151,
    AWS_MQTT5_CRC_PAYLOAD_FORMAT_INVALID = 153,
    AWS_MQTT5_CRC_RETAIN_NOT_SUPPORTED = 154,
    AWS_MQTT5_CRC_QOS_NOT_SUPPORTED = 155,
    AWS_MQTT5_CRC_USE_ANOTHER_SERVER = 156,
    AWS_MQTT5_CRC_SERVER_MOVED = 157,
    AWS_MQTT5_CRC_CONNECTION_RATE_EXCEEDED = 159,
};

/**
 * Reason code inside DISCONNECT packets.
 * Enum values match mqtt spec encoding values.
 *
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901208
 */
enum aws_mqtt5_disconnect_reason_code {
    AWS_MQTT5_DRC_NORMAL_DISCONNECTION = 0,
    AWS_MQTT5_DRC_DISCONNECT_WITH_WILL_MESSAGE = 4,
    AWS_MQTT5_DRC_UNSPECIFIED_ERROR = 128,
    AWS_MQTT5_DRC_MALFORMED_PACKET = 129,
    AWS_MQTT5_DRC_PROTOCOL_ERROR = 130,
    AWS_MQTT5_DRC_IMPLEMENTATION_SPECIFIC_ERROR = 131,
    AWS_MQTT5_DRC_NOT_AUTHORIZED = 135,
    AWS_MQTT5_DRC_SERVER_BUSY = 137,
    AWS_MQTT5_DRC_SERVER_SHUTTING_DOWN = 139,
    AWS_MQTT5_DRC_KEEP_ALIVE_TIMEOUT = 141,
    AWS_MQTT5_DRC_SESSION_TAKEN_OVER = 142,
    AWS_MQTT5_DRC_TOPIC_FILTER_INVALID = 143,
    AWS_MQTT5_DRC_TOPIC_NAME_INVALID = 144,
    AWS_MQTT5_DRC_RECEIVE_MAXIMUM_EXCEEDED = 147,
    AWS_MQTT5_DRC_TOPIC_ALIAS_INVALID = 148,
    AWS_MQTT5_DRC_PACKET_TOO_LARGE = 149,
    AWS_MQTT5_DRC_MESSAGE_RATE_TOO_HIGH = 150,
    AWS_MQTT5_DRC_QUOTA_EXCEEDED = 151,
    AWS_MQTT5_DRC_ADMINISTRATIVE_ACTION = 152,
    AWS_MQTT5_DRC_PAYLOAD_FORMAT_INVALID = 153,
    AWS_MQTT5_DRC_RETAIN_NOT_SUPPORTED = 154,
    AWS_MQTT5_DRC_QOS_NOT_SUPPORTED = 155,
    AWS_MQTT5_DRC_USE_ANOTHER_SERVER = 156,
    AWS_MQTT5_DRC_SERVER_MOVED = 157,
    AWS_MQTT5_DRC_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED = 158,
    AWS_MQTT5_DRC_CONNECTION_RATE_EXCEEDED = 159,
    AWS_MQTT5_DRC_MAXIMUM_CONNECT_TIME = 160,
    AWS_MQTT5_DRC_SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED = 161,
    AWS_MQTT5_DRC_WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED = 162,
};

/**
 * Reason code inside PUBACK packets.
 * Enum values match mqtt spec encoding values.
 *
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901124
 */
enum aws_mqtt5_puback_reason_code {
    AWS_MQTT5_PARC_SUCCESS = 0,
    AWS_MQTT5_PARC_NO_MATCHING_SUBSCRIBERS = 16,
    AWS_MQTT5_PARC_UNSPECIFIED_ERROR = 128,
    AWS_MQTT5_PARC_IMPLEMENTATION_SPECIFIC_ERROR = 131,
    AWS_MQTT5_PARC_NOT_AUTHORIZED = 135,
    AWS_MQTT5_PARC_TOPIC_NAME_INVALID = 144,
    AWS_MQTT5_PARC_PACKET_IDENTIFIER_IN_USE = 145,
    AWS_MQTT5_PARC_QUOTA_EXCEEDED = 151,
    AWS_MQTT5_PARC_PAYLOAD_FORMAT_INVALID = 153,
};

/**
 * Reason code inside SUBACK packet payloads.
 * Enum values match mqtt spec encoding values.
 *
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901178
 */
enum aws_mqtt5_suback_reason_code {
    AWS_MQTT5_SARC_GRANTED_QOS_0 = 0,
    AWS_MQTT5_SARC_GRANTED_QOS_1 = 1,
    AWS_MQTT5_SARC_GRANTED_QOS_2 = 2,
    AWS_MQTT5_SARC_UNSPECIFIED_ERROR = 128,
    AWS_MQTT5_SARC_IMPLEMENTATION_SPECIFIC_ERROR = 131,
    AWS_MQTT5_SARC_NOT_AUTHORIZED = 135,
    AWS_MQTT5_SARC_TOPIC_FILTER_INVALID = 143,
    AWS_MQTT5_SARC_PACKET_IDENTIFIER_IN_USE = 145,
    AWS_MQTT5_SARC_QUOTA_EXCEEDED = 151,
    AWS_MQTT5_SARC_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED = 158,
    AWS_MQTT5_SARC_SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED = 161,
    AWS_MQTT5_SARC_WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED = 162,
};

/**
 * Reason code inside UNSUBACK packet payloads.
 * Enum values match mqtt spec encoding values.
 *
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901194
 */
enum aws_mqtt5_unsuback_reason_code {
    AWS_MQTT5_UARC_SUCCESS = 0,
    AWS_MQTT5_UARC_NO_SUBSCRIPTION_EXISTED = 17,
    AWS_MQTT5_UARC_UNSPECIFIED_ERROR = 128,
    AWS_MQTT5_UARC_IMPLEMENTATION_SPECIFIC_ERROR = 131,
    AWS_MQTT5_UARC_NOT_AUTHORIZED = 135,
    AWS_MQTT5_UARC_TOPIC_NAME_INVALID = 144,
    AWS_MQTT5_UARC_PACKET_IDENTIFIER_IN_USE = 145,
};

/**
 * Type of mqtt packet.
 * Enum values match mqtt spec encoding values.
 *
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901022
 */
enum aws_mqtt5_packet_type {
    /** algebraic unions of packet data structs may use this value (0) to indicate no data */
    AWS_MQTT5_PT_RESERVED = 0,
    AWS_MQTT5_PT_CONNECT = 1,
    AWS_MQTT5_PT_CONNACK = 2,
    AWS_MQTT5_PT_PUBLISH = 3,
    AWS_MQTT5_PT_PUBACK = 4,
    AWS_MQTT5_PT_PUBREC = 5,
    AWS_MQTT5_PT_PUBREL = 6,
    AWS_MQTT5_PT_PUBCOMP = 7,
    AWS_MQTT5_PT_SUBSCRIBE = 8,
    AWS_MQTT5_PT_SUBACK = 9,
    AWS_MQTT5_PT_UNSUBSCRIBE = 10,
    AWS_MQTT5_PT_UNSUBACK = 11,
    AWS_MQTT5_PT_PINGREQ = 12,
    AWS_MQTT5_PT_PINGRESP = 13,
    AWS_MQTT5_PT_DISCONNECT = 14,
    AWS_MQTT5_PT_AUTH = 15,
};

/**
 * Signature of the continuation function to be called after user-code transforms a websocket handshake request
 */
typedef void(aws_mqtt5_transform_websocket_handshake_complete_fn)(
    struct aws_http_message *request,
    int error_code,
    void *complete_ctx);

/**
 * Signature of the websocket handshake request transformation function.  After transformation, the completion
 * function must be invoked to send the request.
 */
typedef void(aws_mqtt5_transform_websocket_handshake_fn)(
    struct aws_http_message *request,
    void *user_data,
    aws_mqtt5_transform_websocket_handshake_complete_fn *complete_fn,
    void *complete_ctx);

/**
 * Controls how the mqtt client should behave with respect to mqtt sessions.
 */
enum aws_mqtt5_client_session_behavior_type {
    /**
     * Always join a new, clean session
     */
    AWS_MQTT5_CSBT_CLEAN,

    /**
     * Attempt to rejoin an existing session.
     */
    AWS_MQTT5_CSBT_REJOIN,

    /* TODO: consider rejoin and resub support */
};

/*
 * Outbound aliasing behavior is controlled by this type.
 *
 * Topic alias behavior is described in https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901113
 *
 * Incoming topic aliases are handled opaquely by the client.  Alias id will appear without modification
 * in the received publish view, but the client will always inject the correct topic.
 *
 * If topic aliasing is not supported by the server, this setting has no affect and any attempts to directly
 * manipulate the topic alias id in outbound publishes will be ignored.
 */
enum aws_mqtt5_client_outbound_topic_alias_behavior_type {
    /*
     * Outbound aliasing is the user's responsibility.  Client will cache and auto-use
     * previously-established aliases if they fall within the negotiated limits of the connection.
     */
    AWS_MQTT5_COTABT_DUMB,

    /*
     * Client ignores any user-specified topic aliasing and acts on the outbound alias set as an LRU cache.
     */
    AWS_MQTT5_COTABT_LRU,
};

/**
 * Non-persistent representation of an mqtt5 user property.
 */
struct aws_mqtt5_user_property {
    struct aws_byte_cursor name;
    struct aws_byte_cursor value;
};

/**
 * Optional property describing a message's payload format.
 * Enum values match mqtt spec encoding values.
 *
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901063
 */
enum aws_mqtt5_payload_format_indicator {
    AWS_MQTT5_PFI_BYTES = 0,
    AWS_MQTT5_PFI_UTF8 = 1,
};

/**
 * Configures how retained messages should be handled when subscribing with a topic filter that matches topics with
 * associated retained messages.
 *
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901169
 */
enum aws_mqtt5_retain_handling_type {

    /**
     * Server should send all retained messages on topics that match the subscription's filter.
     */
    AWS_MQTT5_RHT_SEND_ON_SUBSCRIBE = 0x00,

    /**
     * Server should send all retained messages on topics that match the subscription's filter, where this is the
     * first (relative to connection) subscription filter that matches the topic with a retained message.
     */
    AWS_MQTT5_RHT_SEND_ON_SUBSCRIBE_IF_NEW = 0x01,

    /**
     * Subscribe must not trigger any retained message publishes from the server.
     */
    AWS_MQTT5_RHT_DONT_SEND = 0x02,
};

/**
 * Configures a single subscription within a Subscribe operation
 */
struct aws_mqtt5_subscription_view {
    /**
     * Topic filter to subscribe to
     */
    struct aws_byte_cursor topic_filter;

    /**
     * Maximum QOS that the subscriber will accept messages for.  Negotiated QoS may be different.
     */
    enum aws_mqtt5_qos qos;

    /**
     * Should the server not send publishes to a client when that client was the one who sent the publish?
     */
    bool no_local;

    /**
     * Should messages sent due to this subscription keep the retain flag preserved on the message?
     */
    bool retain_as_published;

    /**
     * Should retained messages on matching topics be sent in reaction to this subscription?
     */
    enum aws_mqtt5_retain_handling_type retain_handling_type;
};

/**
 * Read-only snapshot of a DISCONNECT packet
 *
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901205
 */
struct aws_mqtt5_packet_disconnect_view {
    enum aws_mqtt5_disconnect_reason_code reason_code;
    const uint32_t *session_expiry_interval_seconds;
    const struct aws_byte_cursor *reason_string;

    size_t user_property_count;
    const struct aws_mqtt5_user_property *user_properties;

    const struct aws_byte_cursor *server_reference;
};

/**
 * Read-only snapshot of a SUBSCRIBE packet
 *
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901161
 */
struct aws_mqtt5_packet_subscribe_view {
    aws_mqtt5_packet_id_t packet_id;

    size_t subscription_count;
    const struct aws_mqtt5_subscription_view *subscriptions;

    const uint32_t *subscription_identifier;

    size_t user_property_count;
    const struct aws_mqtt5_user_property *user_properties;
};

/**
 * Read-only snapshot of an UNSUBSCRIBE packet
 *
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901179
 */
struct aws_mqtt5_packet_unsubscribe_view {
    aws_mqtt5_packet_id_t packet_id;

    size_t topic_count;
    const struct aws_byte_cursor *topics;

    size_t user_property_count;
    const struct aws_mqtt5_user_property *user_properties;
};

/**
 * Read-only snapshot of a PUBLISH packet.  Used both in configuration of a publish operation and callback
 * data in message receipt.
 *
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901100
 */
struct aws_mqtt5_packet_publish_view {
    /* This field is always empty on received messages */
    struct aws_byte_cursor payload;

    /* packet_id is only set for QoS 1 and QoS 2 */
    aws_mqtt5_packet_id_t packet_id;

    enum aws_mqtt5_qos qos;
    /*
     * Used to set the duplicate flag on QoS 1+ re-delivery attempts.
     * Set to false on all first attempts or QoS 0. Set to true on any re-delivery.
     */
    bool duplicate;
    bool retain;
    struct aws_byte_cursor topic;
    enum aws_mqtt5_payload_format_indicator *payload_format;
    const uint32_t *message_expiry_interval_seconds;
    const uint16_t *topic_alias;
    const struct aws_byte_cursor *response_topic;
    const struct aws_byte_cursor *correlation_data;

    /* These are ignored when building publish operations */
    size_t subscription_identifier_count;
    const uint32_t *subscription_identifiers;

    const struct aws_byte_cursor *content_type;

    size_t user_property_count;
    const struct aws_mqtt5_user_property *user_properties;
};

/**
 * Read-only snapshot of a CONNECT packet
 *
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901033
 */
struct aws_mqtt5_packet_connect_view {
    uint16_t keep_alive_interval_seconds;

    struct aws_byte_cursor client_id;

    const struct aws_byte_cursor *username;
    const struct aws_byte_cursor *password;

    bool clean_start;

    const uint32_t *session_expiry_interval_seconds;

    const uint8_t *request_response_information;
    const uint8_t *request_problem_information;
    const uint16_t *receive_maximum;
    const uint16_t *topic_alias_maximum;
    const uint32_t *maximum_packet_size_bytes;

    const uint32_t *will_delay_interval_seconds;
    const struct aws_mqtt5_packet_publish_view *will;

    size_t user_property_count;
    const struct aws_mqtt5_user_property *user_properties;

    /* Do not bind these.  We don't support AUTH packets yet.  For decode/encade testing purposes only. */
    const struct aws_byte_cursor *authentication_method;
    const struct aws_byte_cursor *authentication_data;
};

/**
 * Read-only snapshot of a CONNACK packet.
 *
 * Important Note: The CONNACK view is *NOT* guaranteed to be the negotiated settings that the client connection
 * will be using (due to implicit defaults and optional properties).  The negotiated settings are communicated in
 * a separate structure.
 *
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901074
 */
struct aws_mqtt5_packet_connack_view {
    bool session_present;
    enum aws_mqtt5_connect_reason_code reason_code;

    const uint32_t *session_expiry_interval;
    const uint16_t *receive_maximum;
    const enum aws_mqtt5_qos *maximum_qos;
    const bool *retain_available;
    const uint32_t *maximum_packet_size;
    const struct aws_byte_cursor *assigned_client_identifier;
    const uint16_t *topic_alias_maximum;
    const struct aws_byte_cursor *reason_string;

    size_t user_property_count;
    const struct aws_mqtt5_user_property *user_properties;

    const bool *wildcard_subscriptions_available;
    const bool *subscription_identifiers_available;
    const bool *shared_subscriptions_available;

    const uint16_t *server_keep_alive;
    const struct aws_byte_cursor *response_information;
    const struct aws_byte_cursor *server_reference;
    const struct aws_byte_cursor *authentication_method;
    const struct aws_byte_cursor *authentication_data;
};

/**
 * Read-only snapshot of a PUBACK packet
 *
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901121
 */
struct aws_mqtt5_packet_puback_view {
    aws_mqtt5_packet_id_t packet_id;

    enum aws_mqtt5_puback_reason_code reason_code;
    const struct aws_byte_cursor *reason_string;

    size_t user_property_count;
    const struct aws_mqtt5_user_property *user_properties;
};

/**
 * Read-only snapshot of a SUBACK packet
 *
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901171
 */
struct aws_mqtt5_packet_suback_view {
    aws_mqtt5_packet_id_t packet_id;

    const struct aws_byte_cursor *reason_string;

    size_t user_property_count;
    const struct aws_mqtt5_user_property *user_properties;

    size_t reason_code_count;
    const enum aws_mqtt5_suback_reason_code *reason_codes;
};

/**
 * Read-only snapshot of an UNSUBACK packet
 *
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901187
 */
struct aws_mqtt5_packet_unsuback_view {
    aws_mqtt5_packet_id_t packet_id;

    const struct aws_byte_cursor *reason_string;

    size_t user_property_count;
    const struct aws_mqtt5_user_property *user_properties;

    size_t reason_code_count;
    const enum aws_mqtt5_unsuback_reason_code *reason_codes;
};

/*
 * Types related to client operation completion
 */

/**
 * Signature of callback to invoke on Publish success/failure.
 */
typedef void(aws_mqtt5_publish_completion_fn)(
    const struct aws_mqtt5_packet_puback_view *puback,
    int error_code,
    void *complete_ctx);

/**
 * Signature of callback to invoke on Subscribe success/failure.
 */
typedef void(aws_mqtt5_subscribe_completion_fn)(
    const struct aws_mqtt5_packet_suback_view *suback,
    int error_code,
    void *complete_ctx);

/**
 * Signature of callback to invoke on Unsubscribe success/failure.
 */
typedef void(aws_mqtt5_unsubscribe_completion_fn)(
    const struct aws_mqtt5_packet_unsuback_view *unsuback,
    int error_code,
    void *complete_ctx);

/**
 * Signature of callback to invoke when a DISCONNECT is fully written to the socket (or fails to be)
 */
typedef void(aws_mqtt5_disconnect_completion_fn)(int error_code, void *complete_ctx);

/**
 * Completion callback options for the Publish operation
 */
struct aws_mqtt5_publish_completion_options {
    aws_mqtt5_publish_completion_fn *completion_callback;
    void *completion_user_data;
};

/**
 * Completion callback options for the Subscribe operation
 */
struct aws_mqtt5_subscribe_completion_options {
    aws_mqtt5_subscribe_completion_fn *completion_callback;
    void *completion_user_data;
};

/**
 * Completion callback options for the Unsubscribe operation
 */
struct aws_mqtt5_unsubscribe_completion_options {
    aws_mqtt5_unsubscribe_completion_fn *completion_callback;
    void *completion_user_data;
};

/**
 * Public completion callback options for the a DISCONNECT operation
 */
struct aws_mqtt5_disconnect_completion_options {
    aws_mqtt5_disconnect_completion_fn *completion_callback;
    void *completion_user_data;
};

/**
 * Mqtt behavior settings that are dynamically negotiated as part of the CONNECT/CONNACK exchange.
 */
struct aws_mqtt5_negotiated_settings {
    enum aws_mqtt5_qos maximum_qos;

    uint32_t session_expiry_interval;
    uint16_t receive_maximum_from_server;
    uint32_t maximum_packet_size_to_server;
    uint16_t topic_alias_maximum_to_server;
    uint16_t topic_alias_maximum_to_client;
    uint16_t server_keep_alive;

    bool retain_available;
    bool wildcard_subscriptions_available;
    bool subscription_identifiers_available;
    bool shared_subscriptions_available;
    bool rejoined_session;
};

/**
 * Type of a client lifecycle event
 */
enum aws_mqtt5_client_lifecycle_event_type {
    /**
     * Emitted when the client begins an attempt to connect to the remote endpoint.
     *
     * Mandatory event fields: client, user_data
     */
    AWS_MQTT5_CLET_ATTEMPTING_CONNECT,

    /**
     * Emitted after the client connects to the remote endpoint and receives a successful CONNACK.
     * Every ATTEMPTING_CONNECT will be followed by exactly one CONNECTION_SUCCESS or one CONNECTION_FAILURE.
     *
     * Mandatory event fields: client, user_data, connack_data, settings
     */
    AWS_MQTT5_CLET_CONNECTION_SUCCESS,

    /**
     * Emitted at any point during the connection process when it has conclusively failed.
     * Every ATTEMPTING_CONNECT will be followed by exactly one CONNECTION_SUCCESS or one CONNECTION_FAILURE.
     *
     * Mandatory event fields: client, user_data, error_code
     * Conditional event fields: connack_data
     */
    AWS_MQTT5_CLET_CONNECTION_FAILURE,

    /**
     * Lifecycle event containing information about a disconnect.  Every CONNECTION_SUCCESS will eventually be
     * followed by one and only one DISCONNECTION.
     *
     * Mandatory event fields: client, user_data, error_code
     * Conditional event fields: disconnect_data
     */
    AWS_MQTT5_CLET_DISCONNECTION,

    /**
     * Lifecycle event notifying the user that the client has entered the STOPPED state.
     *
     * Mandatory event fields: client, user_data
     */
    AWS_MQTT5_CLET_STOPPED,
};

/**
 * Details about a client lifecycle event.
 */
struct aws_mqtt5_client_lifecycle_event {

    /**
     * Type of event this is.
     */
    enum aws_mqtt5_client_lifecycle_event_type event_type;

    /**
     * Client this event corresponds to.  Necessary (can't be replaced with user data) because the client
     * doesn't exist at the time the event callback user data is configured.
     */
    struct aws_mqtt5_client *client;

    /**
     * Aws-c-* error code associated with the event
     */
    int error_code;

    /**
     * User data associated with the client's lifecycle event handler.  Set with client configuration.
     */
    void *user_data;

    /**
     * If this event was caused by receiving a CONNACK, this will be a view of that packet.
     */
    const struct aws_mqtt5_packet_connack_view *connack_data;

    /**
     * If this is a successful connection establishment, this will contain the negotiated mqtt5 behavioral settings
     */
    const struct aws_mqtt5_negotiated_settings *settings;

    /**
     * If this event was caused by receiving a DISCONNECT, this will be a view of that packet.
     */
    const struct aws_mqtt5_packet_disconnect_view *disconnect_data;
};

/**
 * Callback signature for mqtt5 client lifecycle events.
 */
typedef void(aws_mqtt5_client_connection_event_callback_fn)(const struct aws_mqtt5_client_lifecycle_event *event);

/*
 * Types related to incoming message delivery.
 *
 * We include the message view on the payload callbacks to let the bindings avoid having to persist the message view
 * when they want to deliver the payload in a single buffer.  This implies we need to persist the view and its backing
 * contents in the client/decoder until the message has been fully processed.
 */

/**
 * Invoked by the client when there is additional payload data ready to be read by the receiver.
 */
typedef int(aws_mqtt5_payload_delivery_on_stream_data_callback_fn)(
    struct aws_mqtt5_packet_publish_view *message_view,
    struct aws_byte_cursor payload_data,
    void *user_data);

/**
 * Invoked by the client when the message payload has been completely streamed.  Message delivery is considered
 * successful and finished at this point.
 */
typedef void(aws_mqtt5_payload_delivery_on_stream_complete_callback_fn)(
    struct aws_mqtt5_packet_publish_view *message_view,
    void *user_data);

/**
 * Invoked by the client when there's an error during payload stream processing.  User should consider delivery to
 * have failed and a failing PUBACK will be sent to the server if the connection is still good.
 */
typedef void(aws_mqtt5_payload_delivery_on_stream_error_callback_fn)(
    struct aws_mqtt5_packet_publish_view *message_view,
    int error_code,
    void *user_data);

/**
 * Configuration options for streaming delivery of an mqtt message payload
 */
struct aws_mqtt5_publish_payload_delivery_options {
    aws_mqtt5_payload_delivery_on_stream_data_callback_fn *on_data;
    aws_mqtt5_payload_delivery_on_stream_complete_callback_fn *on_complete;
    aws_mqtt5_payload_delivery_on_stream_error_callback_fn *on_error;
    void *user_data;
};

/**
 * Fundamental message delivery user callback.
 *
 * Receiver must set all delivery_options_out callback members in order to receive the payload.
 */
typedef int(aws_mqtt5_on_message_received_callback_fn)(
    struct aws_mqtt5_packet_publish_view *message_view,
    struct aws_mqtt5_publish_payload_delivery_options *delivery_options_out,
    void *user_data);

/**
 * Configuration options for incoming message delivery.
 *
 * TODO: Associate with subscribe.  Should this be per-subscription (struct aws_mqtt5_subscription) or
 * per-subscribe-call (add to aws_mqtt5_client_subscribe, operation, etc...)?
 */
struct aws_mqtt5_message_receive_options {
    aws_mqtt5_on_message_received_callback_fn *on_received_fn;
    void *user_data;
};

#endif /* AWS_MQTT_MQTT5_TYPES_H */
