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
    AWS_MQTT5_PARC_NO_MATCHING_SUSCRIBERS = 16,
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
    /** algebraic unions of packet data structs use this value to indicate no data */
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
 * Enum governing how the mqtt client should behave in reconnect scenarios.
 */
enum aws_mqtt5_client_reconnect_behavior_type {
    /**
     * Default legacy behavior: reconnect if the initial connection attempt was successful, otherwise do nothing.
     */
    AWS_MQTT5_CRBT_RECONNECT_IF_INITIAL_SUCCESS,

    /**
     * New default: always reconnect, regardless of what happened on the initial connection attempt
     */
    AWS_MQTT5_CRBT_RECONNECT_ALWAYS,

    /**
     * Never attempt to reconnect, just go into the stopped state.
     * TODO: does this have any value whatsoever?
     */
    AWS_MQTT5_CRBT_RECONNECT_NEVER,
};

/**
 * Controls how the mqtt client should behave with respect to mqtt sessions.
 */
enum aws_mqtt5_client_session_behavior_type {
    /**
     * Always join a new, clean session
     */
    AWS_MQTT5_CSBT_CLEAN,

    /**
     * Attempt to rejoin an existing session.  If rejoining fails, do nothing else.
     */
    AWS_MQTT5_CSBT_REJOIN,

    /* TODO: rejoin and resub support */
};

/*
 * Incoming topic aliases are handled opaquely by the client.  Alias id will appear without modification
 * in the received publish view, but the client will always inject the correct topic.
 *
 * Outbound aliasing behavior is controlled by this type.
 */
enum aws_mqtt5_client_outbound_topic_alias_behavior_type {
    /*
     * Outbound aliasing is the user's responsibility.  Client will cache and auto-use
     * previously-established aliases.
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
    /* not in the spec, indicates no value supplied */
    AWS_MQTT5_PFI_NOT_SET = -1,
    AWS_MQTT5_PFI_BYTES = 0,
    AWS_MQTT5_PFI_UTF8 = 1,
};

enum aws_mqtt5_retain_handling_type {
    AWS_MQTT5_RHT_SEND_ON_SUBSCRIBE = 0x00,
    AWS_MQTT5_RHT_SEND_ON_SUBSCRIBE_IF_NEW = 0x01,
    AWS_MQTT5_RHT_DONT_SEND = 0x02,
};

struct aws_mqtt5_subscription_view {
    struct aws_byte_cursor topic;
    enum aws_mqtt5_qos qos;
    bool no_local;
    bool retain_as_published;
    enum aws_mqtt5_retain_handling_type retain_handling_type;
};

struct aws_mqtt5_packet_disconnect_view {
    enum aws_mqtt5_disconnect_reason_code reason_code;
    uint32_t *session_expiry_interval_seconds;
    struct aws_byte_cursor *reason_string;

    size_t user_property_count;
    struct aws_mqtt5_user_property *user_properties;
};

struct aws_mqtt5_packet_subscribe_view {
    size_t subscription_count;
    struct aws_mqtt5_subscription_view *subscriptions;

    uint32_t *subscription_identifier;

    size_t user_property_count;
    struct aws_mqtt5_user_property *user_properties;
};

struct aws_mqtt5_packet_unsubscribe_view {
    size_t topic_count;
    struct aws_byte_cursor *topics;

    size_t user_property_count;
    struct aws_mqtt5_user_property *user_properties;
};

struct aws_mqtt5_packet_publish_view {
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

struct aws_mqtt5_packet_connect_view {
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
    struct aws_mqtt5_packet_publish_view *will;

    size_t user_property_count;
    struct aws_mqtt5_user_property *user_properties;
};

/*
 * Important Note: The connack view is *NOT* guaranteed to be the negotiated settings that the client connection
 * will be using (due to implicit defaults and optional properties).  The negotiated settings are communicated in
 * a separate structure.
 */
struct aws_mqtt5_packet_connack_view {
    bool session_present;
    enum aws_mqtt5_connect_reason_code reason_code;

    uint32_t *session_expiry_interval;
    uint16_t *receive_maximum;
    enum aws_mqtt5_qos *maximum_qos;
    bool *retain_available;
    uint32_t *maximum_packet_size;
    struct aws_byte_cursor *assigned_client_identifier;
    uint16_t *topic_alias_maximum;
    struct aws_byte_cursor *reason_string;

    size_t user_property_count;
    struct aws_mqtt5_user_property *user_properties;

    bool *wildcard_subscriptions_available;
    bool *subscription_identifiers_available;
    bool *shared_subscriptions_available;

    uint16_t *server_keep_alive;
    struct aws_byte_cursor *response_information;
    struct aws_byte_cursor *server_reference;
    struct aws_byte_cursor *authentication_method;
    struct aws_byte_cursor *authentication_data;
};

struct aws_mqtt5_packet_puback_view {
    enum aws_mqtt5_puback_reason_code reason_code;
    struct aws_byte_cursor *reason_string;

    size_t user_property_count;
    struct aws_mqtt5_user_property *user_properties;
};

struct aws_mqtt5_packet_suback_view {
    struct aws_byte_cursor *reason_string;

    size_t user_property_count;
    struct aws_mqtt5_user_property *user_properties;

    size_t reason_code_count;
    enum aws_mqtt5_suback_reason_code *reason_codes;
};

struct aws_mqtt5_packet_unsuback_view {
    struct aws_byte_cursor *reason_string;

    size_t user_property_count;
    struct aws_mqtt5_user_property *user_properties;

    size_t reason_code_count;
    enum aws_mqtt5_unsuback_reason_code *reason_codes;
};

typedef void(
    aws_mqtt5_publish_complete_fn)(struct aws_mqtt5_packet_puback_view *puback, int error_code, void *complete_ctx);

typedef void(
    aws_mqtt5_subscribe_complete_fn)(struct aws_mqtt5_packet_suback_view *suback, int error_code, void *complete_ctx);

typedef void(aws_mqtt5_unsubscribe_complete_fn)(
    struct aws_mqtt5_packet_unsuback_view *unsuback,
    int error_code,
    void *complete_ctx);

struct aws_mqtt5_publish_completion_options {
    aws_mqtt5_publish_complete_fn *completion_callback;
    void *completion_user_data;
};

struct aws_mqtt5_subscribe_completion_options {
    aws_mqtt5_subscribe_complete_fn *completion_callback;
    void *completion_user_data;
};

struct aws_mqtt5_unsubscribe_completion_options {
    aws_mqtt5_unsubscribe_complete_fn *completion_callback;
    void *completion_user_data;
};

struct aws_mqtt5_negotiated_settings {
    enum aws_mqtt5_qos maximum_qos;

    uint32_t session_expiry_interval;
    uint16_t receive_maximum;
    uint32_t maximum_packet_size;
    uint16_t topic_alias_maximum;
    uint16_t server_keep_alive;

    bool retain_available;
    bool wildcard_subscriptions_available;
    bool subscription_identifiers_available;
    bool shared_subscriptions_available;
};

/**
 * Type of a lifecycle event
 */
enum aws_mqtt5_client_lifecycle_event_type {
    /**
     * Lifecycle event containing the result of a connection attempt
     */
    AWS_MQTT5_CLET_CONNECTION_RESULT,

    /**
     * Lifecycle event containing information about a disconnect
     */
    AWS_MQTT5_CLET_DISCONNECTION,

    /**
     * Lifecycle event notifying the user that the client has entered the STOPPED state.
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
     * Aws-c-* error code associated with the event
     */
    int aws_error_code;

    /**
     * User data associated with the client's lifecycle event handler.  Set during configuration.
     */
    void *user_data;

    /* If this event was caused by receiving a CONNACK, this will be set */
    struct aws_mqtt5_packet_connack_view *connack_data;

    /* If this is a successful connection establishment, this will be set */
    struct aws_mqtt5_negotiated_settings *settings;

    /* If this event was caused by receiving a DISCONNECT, this will be set */
    struct aws_mqtt5_packet_disconnect_view *disconnect_data;
};

/**
 * Callback signature for mqtt5 client lifecycle events.
 */
typedef void(aws_mqtt5_client_connection_event_callback_fn)(struct aws_mqtt5_client_lifecycle_event *event);

AWS_EXTERN_C_BEGIN

/**
 * Converts a disconnect reason code into the Reason Code Name, as it appears in the mqtt5 spec.
 *
 * @param reason_code a disconnect reason code
 * @return name associated with the reason code
 */
AWS_MQTT_API const char *aws_mqtt5_disconnect_reason_code_to_c_string(
    enum aws_mqtt5_disconnect_reason_code reason_code);

/**
 * Converts a connect reason code into the Reason Code Name, as it appears in the mqtt5 spec.
 *
 * @param reason_code a connect reason code
 * @return name associated with the reason code
 */
AWS_MQTT_API const char *aws_mqtt5_connect_reason_code_to_c_string(enum aws_mqtt5_connect_reason_code reason_code);

/**
 * Converts a publish reason code into the Reason Code Name, as it appears in the mqtt5 spec.
 *
 * @param reason_code a publish reason code
 * @return name associated with the reason code
 */
AWS_MQTT_API const char *aws_mqtt5_puback_reason_code_to_c_string(enum aws_mqtt5_puback_reason_code reason_code);

/**
 * Converts a subscribe reason code into the Reason Code Name, as it appears in the mqtt5 spec.
 *
 * @param reason_code a subscribe reason code
 * @return name associated with the reason code
 */
AWS_MQTT_API const char *aws_mqtt5_suback_reason_code_to_c_string(enum aws_mqtt5_suback_reason_code reason_code);

/**
 * Converts a unsubscribe reason code into the Reason Code Name, as it appears in the mqtt5 spec.
 *
 * @param reason_code an unsubscribe reason code
 * @return name associated with the reason code
 */
AWS_MQTT_API const char *aws_mqtt5_unsuback_reason_code_to_c_string(enum aws_mqtt5_unsuback_reason_code reason_code);

/**
 * Converts a reconnect behavior type value to a readable description.
 *
 * @param reconnect_behavior type of reconnect behavior
 * @return short string describing the reconnect behavior
 */
AWS_MQTT_API const char *aws_mqtt5_client_reconnect_behavior_type_to_c_string(
    enum aws_mqtt5_client_reconnect_behavior_type reconnect_behavior);

/**
 * Converts a session behavior type value to a readable description.
 *
 * @param session_behavior type of session behavior
 * @return short string describing the session behavior
 */
AWS_MQTT_API const char *aws_mqtt5_client_session_behavior_type_to_c_string(
    enum aws_mqtt5_client_session_behavior_type session_behavior);

/**
 * Converts an outbound topic aliasing behavior type value to a readable description.
 *
 * @param session_behavior type of outbound topic aliasing behavior
 * @return short string describing the outbound topic aliasing behavior
 */
AWS_MQTT_API const char *aws_mqtt5_outbound_topic_alias_behavior_type_to_c_string(
    enum aws_mqtt5_client_outbound_topic_alias_behavior_type outbound_aliasing_behavior);

/**
 * Converts a lifecycle event type value to a readable description.
 *
 * @param lifecycle_event type of lifecycle event
 * @return short string describing the lifecycle event type
 */
AWS_MQTT_API const char *aws_mqtt5_client_lifecycle_event_type_to_c_string(
    enum aws_mqtt5_client_lifecycle_event_type lifecycle_event);

/**
 * Converts a payload format indicator value to a readable description.
 *
 * @param format_indicator type of payload format indicator
 * @return short string describing the payload format indicator
 */
AWS_MQTT_API const char *aws_mqtt5_payload_format_indicator_to_c_string(
    enum aws_mqtt5_payload_format_indicator format_indicator);

/**
 * Converts a retain handling type value to a readable description.
 *
 * @param retain_handling_type type of retain handling
 * @return short string describing the retain handling type
 */
AWS_MQTT_API const char *aws_mqtt5_retain_handling_type_to_c_string(
    enum aws_mqtt5_retain_handling_type retain_handling_type);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_MQTT5_TYPES_H */
