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

typedef uint64_t aws_mqtt5_op_id;

/* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901026 */
typedef uint16_t aws_mqtt5_packet_id;

/* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901234 */
enum aws_mqtt5_qos {

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901235 */
    AWS_MQTT5_QOS_AT_MOST_ONCE = 0x0,

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901236 */
    AWS_MQTT5_QOS_AT_LEAST_ONCE = 0x1,

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901237 */
    AWS_MQTT5_QOS_EXACTLY_ONCE = 0x2,
};

/* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901079 */
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

/* properties allowed in a CONNACK packet */
enum aws_mqtt5_client_connack_property_flags {

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901082 */
    AWS_MQTT5_CCAPF_SESSION_EXPIRY_INTERVAL = 1 << 0,

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901087 */
    AWS_MQTT5_CCAPF_ASSIGNED_CLIENT_IDENTIFIER = 1 << 1,

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901094 */
    AWS_MQTT5_CCAPF_SERVER_KEEP_ALIVE = 1 << 2,

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901097 */
    AWS_MQTT5_CCAPF_AUTHENTICATION_METHOD = 1 << 3,

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901098 */
    AWS_MQTT5_CCAPF_AUTHENTICATION_DATA = 1 << 4,

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901095 */
    AWS_MQTT5_CCAPF_RESPONSE_INFORMATION = 1 << 5,

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901096 */
    AWS_MQTT5_CCAPF_SERVER_REFERENCE = 1 << 6,

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901089 */
    AWS_MQTT5_CCAPF_REASON_STRING = 1 << 7,

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901083 */
    AWS_MQTT5_CCAPF_RECEIVE_MAXIMUM = 1 << 8,

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901088 */
    AWS_MQTT5_CCAPF_TOPIC_ALIAS_MAXIMUM = 1 << 9,

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901084 */
    AWS_MQTT5_CCAPF_MAXIMUM_QOS = 1 << 10,

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901085 */
    AWS_MQTT5_CCAPF_RETAIN_AVAILABLE = 1 << 11,

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901086 */
    AWS_MQTT5_CCAPF_MAXIMUM_PACKET_SIZE = 1 << 12,

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901091 */
    AWS_MQTT5_CCAPF_WILDCARD_SUBSCRIPTION_AVAILABLE = 1 << 13,

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901092 */
    AWS_MQTT5_CCAPF_SUBSCRIPTION_IDENTIFIERS_AVAILABLE = 1 << 14,

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901093 */
    AWS_MQTT5_CCAPF_SHARED_SUBSCRIPTION_AVAILABLE = 1 << 15,
};

/* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901080 */
struct aws_mqtt5_client_connack_property_set {
    enum aws_mqtt5_client_connack_property_flags valid_properties;

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901082 */
    uint32_t session_expiry_interval; /* 17 */

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901087 */
    struct aws_byte_cursor assigned_client_identifier;

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901094 */
    uint16_t server_keep_alive; // 19

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901097 */
    struct aws_byte_cursor authentication_method; // 21

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901098 */
    struct aws_byte_cursor authentication_data; // 22

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901095 */
    struct aws_byte_cursor response_information; // 26;

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901096 */
    struct aws_byte_cursor server_reference; // 28;

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901089 */
    struct aws_byte_cursor reason_string; // 31;

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901083 */
    uint16_t receive_maximum; // 33

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901088 */
    uint16_t topic_alias_maximum; // 34

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901084 */
    enum aws_mqtt5_qos maximum_qos; // 36

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901085 */
    bool retain_available; // 37

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901086 */
    uint32_t maximum_packet_size; // 39

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901091 */
    bool wildcard_subscription_available; // 41

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901092 */
    bool subscription_identifiers_available; // 42

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901093 */
    bool shared_subscription_available; // 43
};

/* Non-persistent view of the data in a CONNACK packet.
 * Cursors are backed by the raw packet data that will exist for the duration of the callback only.
 */
struct aws_mqtt5_connack_packet_data {
    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901078 */
    bool session_present;

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901079 */
    enum aws_mqtt5_connect_reason_code reason_code;

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901080 */
    struct aws_mqtt5_client_connack_property_set properties;

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901090 */
    struct aws_array_list user_properties;
};

/* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901208 */
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

/* properties allowed in a DISCONNECT packet */
enum aws_mqtt5_client_disconnect_property_flags {
    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901211 */
    AWS_MQTT5_CDPF_SESSION_EXPIRY_INTERVAL = 1 << 0,

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901214 */
    AWS_MQTT5_CDPF_SERVER_REFERENCE = 1 << 1,

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901212 */
    AWS_MQTT5_CDPF_REASON_STRING = 1 << 2,
};

/* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901209 */
struct aws_mqtt5_client_disconnect_property_set {
    enum aws_mqtt5_client_disconnect_property_flags valid_properties;

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901211 */
    uint32_t session_expiry_interval; /* 17 */

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901214 */
    struct aws_byte_cursor server_reference; /* 28 */

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901212 */
    struct aws_byte_cursor reason_string; /* 31 */
};

/* Non-persistent view of the data in a DISCONNECT packet.
 * Cursors are backed by the raw packet data that will exist for the duration of the callback only.
 */
struct aws_mqtt5_disconnect_packet_data {
    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901208 */
    enum aws_mqtt5_disconnect_reason_code reason_code;

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901209 */
    struct aws_mqtt5_client_disconnect_property_set properties;

    /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901213 */
    struct aws_array_list user_properties;
};

/* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901022 */
enum aws_mqtt5_packet_type {
    AWS_MQTT5_PT_RESERVED = 0, /* algebraic unions of packet data structs use this to indicate no data */
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

typedef void(aws_mqtt5_transform_websocket_handshake_complete_fn)(
    struct aws_http_message *request,
    int error_code,
    void *complete_ctx);

typedef void(aws_mqtt5_transform_websocket_handshake_fn)(
    struct aws_http_message *request,
    void *user_data,
    aws_mqtt5_transform_websocket_handshake_complete_fn *complete_fn,
    void *complete_ctx);

enum aws_mqtt5_client_reconnect_behavior_type {
    AWS_MQTT5_CRBT_RECONNECT_IF_INITIAL_SUCCESS,
    AWS_MQTT5_CRBT_RECONNECT_ALWAYS,
    AWS_MQTT5_CRBT_RECONNECT_NEVER,
};

enum aws_mqtt5_client_session_behavior_type {
    AWS_MQTT5_CSBT_CLEAN,
    AWS_MQTT5_CSBT_REJOIN,
    AWS_MQTT5_CSBT_REJOIN_AND_RESUB_ON_CLEAN,
};

struct aws_mqtt5_user_property {
    struct aws_byte_cursor name;
    struct aws_byte_cursor value;
};

enum aws_mqtt5_client_lifecycle_event_type {
    AWS_MQTT5_CLET_CONNECTION_RESULT,
    AWS_MQTT5_CLET_DISCONNECTION,
    AWS_MQTT5_CLET_STOPPED,
};

struct aws_mqtt5_client_lifecycle_event {
    enum aws_mqtt5_client_lifecycle_event_type event_type;
    int aws_error_code;
    void *user_data;

    enum aws_mqtt5_packet_type packet_type;
    union {
        struct aws_mqtt5_connack_packet_data connack_data;
        struct aws_mqtt5_disconnect_packet_data disconnect_data;
    } packet_data;
};

typedef void(aws_mqtt5_client_connection_event_callback_fn)(struct aws_mqtt5_connection_event *event);

/* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901063 */
enum aws_mqtt5_payload_format_indicator {
    AWS_MQTT5_PFI_NOT_SET = -1, /* not in the spec, indicates no value supplied */
    AWS_MQTT5_PFI_BYTES = 0,
    AWS_MQTT5_PFI_UTF8 = 1,
};

AWS_EXTERN_C_BEGIN

AWS_MQTT_API const char *aws_mqtt5_disconnect_reason_code_to_c_string(
    enum aws_mqtt5_disconnect_reason_code reason_code);

AWS_MQTT_API const char *aws_mqtt5_connect_reason_code_to_c_string(enum aws_mqtt5_connect_reason_code reason_code);

AWS_MQTT_API const char *aws_mqtt5_client_reconnect_behavior_type_to_c_string(
    enum aws_mqtt5_client_reconnect_behavior_type reconnect_behavior);

AWS_MQTT_API const char *aws_mqtt5_client_session_behavior_type_to_c_string(
    enum aws_mqtt5_client_session_behavior_type session_behavior);

AWS_MQTT_API const char *aws_mqtt5_client_lifecycle_event_type_to_c_string(
    enum aws_mqtt5_client_lifecycle_event_type lifecycle_event);

AWS_MQTT_API const char *aws_mqtt5_payload_format_indicator_to_c_string(
    enum aws_mqtt5_payload_format_indicator format_indicator);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_MQTT5_TYPES_H */
