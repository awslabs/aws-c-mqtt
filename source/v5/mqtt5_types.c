/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/v5/mqtt5_types.h>

/* disconnect and shared reason codes */
static const char *s_normal_disconnection = "Normal Disconnection";
static const char *s_disconnect_with_will_message = "Disconnect With Will Message";
static const char *s_unspecified_error = "Unspecified Error";
static const char *s_malformed_packet = "Malformed Packet";
static const char *s_protocol_error = "Protocol Error";
static const char *s_implementation_specific_error = "Implementation Specific Error";
static const char *s_not_authorized = "Not Authorized";
static const char *s_server_busy = "Server Busy";
static const char *s_server_shutting_down = "Server Shutting Down";
static const char *s_keep_alive_timeout = "Keep Alive Timeout";
static const char *s_session_taken_over = "Session Taken Over";
static const char *s_topic_filter_invalid = "Topic Filter Invalid";
static const char *s_topic_name_invalid = "Topic Name Invalid";
static const char *s_receive_maximum_exceeded = "Receive Maximum Exceeded";
static const char *s_topic_alias_invalid = "Topic Alias Invalid";
static const char *s_packet_too_large = "Packet Too Large";
static const char *s_message_rate_too_high = "Message Rate Too High";
static const char *s_quota_exceeded = "Quota Exceeded";
static const char *s_administrative_action = "Administrative Action";
static const char *s_payload_format_invalid = "Payload Format Invalid";
static const char *s_retain_not_supported = "Retain Not Supported";
static const char *s_qos_not_supported = "QoS Not Supported";
static const char *s_use_another_server = "Use Another Server";
static const char *s_server_moved = "Server Moved";
static const char *s_shared_subscriptions_not_supported = "Shared Subscriptions Not Supported";
static const char *s_connection_rate_exceeded = "Connection Rate Exceeded";
static const char *s_maximum_connect_time = "Maximum Connect Time";
static const char *s_subscription_identifiers_not_supported = "Subscription Identifiers Not Supported";
static const char *s_wildcard_subscriptions_not_supported = "Wildcard Subscriptions Not Supported";

/* connect reason code only */
static const char *s_success = "Success";
static const char *s_unsupported_protocol_version = "Unsupported Protocol Version";
static const char *s_client_identifier_not_valid = "Client Identifier Not Valid";
static const char *s_bad_username_or_password = "Bad Username Or Password";
static const char *s_server_unavailable = "Server Unavailable";
static const char *s_banned = "Banned";
static const char *s_bad_authentication_method = "Bad Authentication Method";

const char *aws_mqtt5_connect_reason_code_to_c_string(enum aws_mqtt5_connect_reason_code reason_code) {
    switch (reason_code) {
        case AWS_MQTT5_CRC_SUCCESS:
            return s_success;
        case AWS_MQTT5_CRC_UNSPECIFIED_ERROR:
            return s_unspecified_error;
        case AWS_MQTT5_CRC_MALFORMED_PACKET:
            return s_malformed_packet;
        case AWS_MQTT5_CRC_PROTOCOL_ERROR:
            return s_protocol_error;
        case AWS_MQTT5_CRC_IMPLEMENTATION_SPECIFIC_ERROR:
            return s_implementation_specific_error;
        case AWS_MQTT5_CRC_UNSUPPORTED_PROTOCOL_VERSION:
            return s_unsupported_protocol_version;
        case AWS_MQTT5_CRC_CLIENT_IDENTIFIER_NOT_VALID:
            return s_client_identifier_not_valid;
        case AWS_MQTT5_CRC_BAD_USERNAME_OR_PASSWORD:
            return s_bad_username_or_password;
        case AWS_MQTT5_CRC_NOT_AUTHORIZED:
            return s_not_authorized;
        case AWS_MQTT5_CRC_SERVER_UNAVAILABLE:
            return s_server_unavailable;
        case AWS_MQTT5_CRC_SERVER_BUSY:
            return s_server_busy;
        case AWS_MQTT5_CRC_BANNED:
            return s_banned;
        case AWS_MQTT5_CRC_BAD_AUTHENTICATION_METHOD:
            return s_bad_authentication_method;
        case AWS_MQTT5_CRC_TOPIC_NAME_INVALID:
            return s_topic_name_invalid;
        case AWS_MQTT5_CRC_PACKET_TOO_LARGE:
            return s_packet_too_large;
        case AWS_MQTT5_CRC_QUOTA_EXCEEDED:
            return s_quota_exceeded;
        case AWS_MQTT5_CRC_PAYLOAD_FORMAT_INVALID:
            return s_payload_format_invalid;
        case AWS_MQTT5_CRC_RETAIN_NOT_SUPPORTED:
            return s_retain_not_supported;
        case AWS_MQTT5_CRC_QOS_NOT_SUPPORTED:
            return s_qos_not_supported;
        case AWS_MQTT5_CRC_USE_ANOTHER_SERVER:
            return s_use_another_server;
        case AWS_MQTT5_CRC_SERVER_MOVED:
            return s_server_moved;
        case AWS_MQTT5_CRC_CONNECTION_RATE_EXCEEDED:
            return s_connection_rate_exceeded;
    }

    return "Unknown Reason";
}

const char *aws_mqtt5_disconnect_reason_code_to_c_string(enum aws_mqtt5_disconnect_reason_code reason_code) {
    switch (reason_code) {
        case AWS_MQTT5_DRC_NORMAL_DISCONNECTION:
            return s_normal_disconnection;
        case AWS_MQTT5_DRC_DISCONNECT_WITH_WILL_MESSAGE:
            return s_disconnect_with_will_message;
        case AWS_MQTT5_DRC_UNSPECIFIED_ERROR:
            return s_unspecified_error;
        case AWS_MQTT5_DRC_MALFORMED_PACKET:
            return s_malformed_packet;
        case AWS_MQTT5_DRC_PROTOCOL_ERROR:
            return s_protocol_error;
        case AWS_MQTT5_DRC_IMPLEMENTATION_SPECIFIC_ERROR:
            return s_implementation_specific_error;
        case AWS_MQTT5_DRC_NOT_AUTHORIZED:
            return s_not_authorized;
        case AWS_MQTT5_DRC_SERVER_BUSY:
            return s_server_busy;
        case AWS_MQTT5_DRC_SERVER_SHUTTING_DOWN:
            return s_server_shutting_down;
        case AWS_MQTT5_DRC_KEEP_ALIVE_TIMEOUT:
            return s_keep_alive_timeout;
        case AWS_MQTT5_DRC_SESSION_TAKEN_OVER:
            return s_session_taken_over;
        case AWS_MQTT5_DRC_TOPIC_FILTER_INVALID:
            return s_topic_filter_invalid;
        case AWS_MQTT5_DRC_TOPIC_NAME_INVALID:
            return s_topic_name_invalid;
        case AWS_MQTT5_DRC_RECEIVE_MAXIMUM_EXCEEDED:
            return s_receive_maximum_exceeded;
        case AWS_MQTT5_DRC_TOPIC_ALIAS_INVALID:
            return s_topic_alias_invalid;
        case AWS_MQTT5_DRC_PACKET_TOO_LARGE:
            return s_packet_too_large;
        case AWS_MQTT5_DRC_MESSAGE_RATE_TOO_HIGH:
            return s_message_rate_too_high;
        case AWS_MQTT5_DRC_QUOTA_EXCEEDED:
            return s_quota_exceeded;
        case AWS_MQTT5_DRC_ADMINISTRATIVE_ACTION:
            return s_administrative_action;
        case AWS_MQTT5_DRC_PAYLOAD_FORMAT_INVALID:
            return s_payload_format_invalid;
        case AWS_MQTT5_DRC_RETAIN_NOT_SUPPORTED:
            return s_retain_not_supported;
        case AWS_MQTT5_DRC_QOS_NOT_SUPPORTED:
            return s_qos_not_supported;
        case AWS_MQTT5_DRC_USE_ANOTHER_SERVER:
            return s_use_another_server;
        case AWS_MQTT5_DRC_SERVER_MOVED:
            return s_server_moved;
        case AWS_MQTT5_DRC_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED:
            return s_shared_subscriptions_not_supported;
        case AWS_MQTT5_DRC_CONNECTION_RATE_EXCEEDED:
            return s_connection_rate_exceeded;
        case AWS_MQTT5_DRC_MAXIMUM_CONNECT_TIME:
            return s_maximum_connect_time;
        case AWS_MQTT5_DRC_SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED:
            return s_subscription_identifiers_not_supported;
        case AWS_MQTT5_DRC_WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED:
            return s_wildcard_subscriptions_not_supported;
    }

    return "Unknown Reason";
}

const char *aws_mqtt5_client_reconnect_behavior_type_to_c_string(
    enum aws_mqtt5_client_reconnect_behavior_type reconnect_behavior) {
    switch (reconnect_behavior) {
        case AWS_MQTT5_CRBT_RECONNECT_IF_INITIAL_SUCCESS:
            return "Reconnect if and only if initial connection attempt succeeded";
        case AWS_MQTT5_CRBT_RECONNECT_ALWAYS:
            return "Reconnect always";
        case AWS_MQTT5_CRBT_RECONNECT_NEVER:
            return "Reconnect never";
    }

    return "Unknown reconnect behavior";
}

const char *aws_mqtt5_client_session_behavior_type_to_c_string(
    enum aws_mqtt5_client_session_behavior_type session_behavior) {
    switch (session_behavior) {
        case AWS_MQTT5_CSBT_CLEAN:
            return "Clean session always";
        case AWS_MQTT5_CSBT_REJOIN:
            return "Rejoin session always";
        case AWS_MQTT5_CSBT_REJOIN_AND_RESUB_ON_CLEAN:
            return "Rejoin session always and resubscribe to client-tracked topics if no session could be rejoined";
    }

    return "Unknown session behavior";
}

const char *aws_mqtt5_client_lifecycle_event_type_to_c_string(
    enum aws_mqtt5_client_lifecycle_event_type lifecycle_event) {
    switch (lifecycle_event) {
        case AWS_MQTT5_CLET_CONNECTION_RESULT:
            return "Connection establishment result";
        case AWS_MQTT5_CLET_DISCONNECTION:
            return "Disconnection";
        case AWS_MQTT5_CLET_STOPPED:
            return "Client stopped";
    }

    return "Unknown lifecycle event";
}
