#ifndef AWS_MQTT_MQTT5_UTILS_H
#define AWS_MQTT_MQTT5_UTILS_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

#include <aws/mqtt/v5/mqtt5_client.h>
#include <aws/mqtt/v5/mqtt5_types.h>

struct aws_byte_buf;
struct aws_mqtt5_negotiated_settings;

#define AWS_MQTT5_MAXIMUM_VARIABLE_LENGTH_INTEGER 268435455
#define AWS_MQTT5_MAXIMUM_PACKET_SIZE (5 + AWS_MQTT5_MAXIMUM_VARIABLE_LENGTH_INTEGER)
#define AWS_MQTT5_RECEIVE_MAXIMUM 65535
#define AWS_MQTT5_PINGREQ_ENCODED_SIZE 2

/* property type codes */
#define AWS_MQTT5_PROPERTY_TYPE_PAYLOAD_FORMAT_INDICATOR ((uint8_t)1)
#define AWS_MQTT5_PROPERTY_TYPE_MESSAGE_EXPIRY_INTERVAL ((uint8_t)2)
#define AWS_MQTT5_PROPERTY_TYPE_CONTENT_TYPE ((uint8_t)3)
#define AWS_MQTT5_PROPERTY_TYPE_RESPONSE_TOPIC ((uint8_t)8)
#define AWS_MQTT5_PROPERTY_TYPE_CORRELATION_DATA ((uint8_t)9)
#define AWS_MQTT5_PROPERTY_TYPE_SUBSCRIPTION_IDENTIFIER ((uint8_t)11)
#define AWS_MQTT5_PROPERTY_TYPE_SESSION_EXPIRY_INTERVAL ((uint8_t)17)
#define AWS_MQTT5_PROPERTY_TYPE_ASSIGNED_CLIENT_IDENTIFIER ((uint8_t)18)
#define AWS_MQTT5_PROPERTY_TYPE_SERVER_KEEP_ALIVE ((uint8_t)19)
#define AWS_MQTT5_PROPERTY_TYPE_AUTHENTICATION_METHOD ((uint8_t)21)
#define AWS_MQTT5_PROPERTY_TYPE_AUTHENTICATION_DATA ((uint8_t)22)
#define AWS_MQTT5_PROPERTY_TYPE_REQUEST_PROBLEM_INFORMATION ((uint8_t)23)
#define AWS_MQTT5_PROPERTY_TYPE_WILL_DELAY_INTERVAL ((uint8_t)24)
#define AWS_MQTT5_PROPERTY_TYPE_REQUEST_RESPONSE_INFORMATION ((uint8_t)25)
#define AWS_MQTT5_PROPERTY_TYPE_RESPONSE_INFORMATION ((uint8_t)26)
#define AWS_MQTT5_PROPERTY_TYPE_SERVER_REFERENCE ((uint8_t)28)
#define AWS_MQTT5_PROPERTY_TYPE_REASON_STRING ((uint8_t)31)
#define AWS_MQTT5_PROPERTY_TYPE_RECEIVE_MAXIMUM ((uint8_t)33)
#define AWS_MQTT5_PROPERTY_TYPE_TOPIC_ALIAS_MAXIMUM ((uint8_t)34)
#define AWS_MQTT5_PROPERTY_TYPE_TOPIC_ALIAS ((uint8_t)35)
#define AWS_MQTT5_PROPERTY_TYPE_MAXIMUM_QOS ((uint8_t)36)
#define AWS_MQTT5_PROPERTY_TYPE_RETAIN_AVAILABLE ((uint8_t)37)
#define AWS_MQTT5_PROPERTY_TYPE_USER_PROPERTY ((uint8_t)38)
#define AWS_MQTT5_PROPERTY_TYPE_MAXIMUM_PACKET_SIZE ((uint8_t)39)
#define AWS_MQTT5_PROPERTY_TYPE_WILDCARD_SUBSCRIPTIONS_AVAILABLE ((uint8_t)40)
#define AWS_MQTT5_PROPERTY_TYPE_SUBSCRIPTION_IDENTIFIERS_AVAILABLE ((uint8_t)41)
#define AWS_MQTT5_PROPERTY_TYPE_SHARED_SUBSCRIPTIONS_AVAILABLE ((uint8_t)42)

/* decode/encode bit masks and positions */
#define AWS_MQTT5_CONNECT_FLAGS_WILL_BIT (1U << 2)
#define AWS_MQTT5_CONNECT_FLAGS_CLEAN_START_BIT (1U << 1)
#define AWS_MQTT5_CONNECT_FLAGS_USER_NAME_BIT (1U << 7)
#define AWS_MQTT5_CONNECT_FLAGS_PASSWORD_BIT (1U << 6)
#define AWS_MQTT5_CONNECT_FLAGS_WILL_RETAIN_BIT (1U << 5)

#define AWS_MQTT5_CONNECT_FLAGS_WILL_QOS_BIT_POSITION 3
#define AWS_MQTT5_CONNECT_FLAGS_WILL_QOS_BIT_MASK 0x03

#define AWS_MQTT5_SUBSCRIBE_FLAGS_NO_LOCAL (1U << 2)
#define AWS_MQTT5_SUBSCRIBE_FLAGS_RETAIN_AS_PUBLISHED (1U << 3)

#define AWS_MQTT5_SUBSCRIBE_FLAGS_RETAIN_HANDLING_TYPE_BIT_POSITION 4
#define AWS_MQTT5_SUBSCRIBE_FLAGS_RETAIN_HANDLING_TYPE_BIT_MASK 0x03
#define AWS_MQTT5_SUBSCRIBE_FLAGS_QOS_BIT_POSITION 0
#define AWS_MQTT5_SUBSCRIBE_FLAGS_QOS_BIT_MASK 0x03

/* Static AWS IoT Core Limit/Quota Values */
#define AWS_IOT_CORE_MAXIMUM_CLIENT_ID_LENGTH 128
#define AWS_IOT_CORE_MAXIMUM_TOPIC_LENGTH 256
#define AWS_IOT_CORE_MAXIMUM_TOPIC_SEGMENTS 8
#define AWS_IOT_CORE_MAXIMUM_SUSBCRIPTIONS_PER_SUBSCRIBE 8

/* Dynamic IoT Core Limits */
#define AWS_IOT_CORE_PUBLISH_PER_SECOND_LIMIT 100
#define AWS_IOT_CORE_THROUGHPUT_LIMIT (512 * 1024)

AWS_EXTERN_C_BEGIN

/**
 * CONNECT packet MQTT5 prefix which includes "MQTT" encoded as a utf-8 string followed by the protocol number (5)
 *
 * {0x00, 0x04, "MQTT", 0x05}
 */
AWS_MQTT_API extern struct aws_byte_cursor g_aws_mqtt5_connect_protocol_cursor;

/**
 * Simple helper function to compute the first byte of an MQTT packet encoding as a function of 4 bit flags
 * and the packet type.
 *
 * @param packet_type type of MQTT packet
 * @param flags 4-bit wide flags, specific to each packet type, 0-valued for most
 * @return the expected/required first byte of a packet of that type with flags set
 */
AWS_MQTT_API uint8_t aws_mqtt5_compute_fixed_header_byte1(enum aws_mqtt5_packet_type packet_type, uint8_t flags);

void aws_mqtt5_negotiated_settings_log(
    struct aws_mqtt5_negotiated_settings *negotiated_settings,
    enum aws_log_level level);

/**
 * Resets negotiated_settings to defaults reconciled with client set properties.
 * Called on init of mqtt5 Client and just prior to a CONNECT.
 *
 * @param negotiated_settings struct containing settings to be set
 * @param packet_connect_view Read-only snapshot of a CONNECT packet
 * @return void
 */
void aws_mqtt5_negotiated_settings_reset(
    struct aws_mqtt5_negotiated_settings *negotiated_settings,
    const struct aws_mqtt5_packet_connect_view *packet_connect_view);

/**
 * Checks properties received from Server CONNACK and reconcile with negotiated_settings
 *
 * @param negotiated_settings struct containing settings to be set
 * @param connack_data Read-only snapshot of a CONNACK packet
 * @return void
 */
void aws_mqtt5_negotiated_settings_apply_connack(
    struct aws_mqtt5_negotiated_settings *negotiated_settings,
    const struct aws_mqtt5_packet_connack_view *connack_data);

/**
 * Converts a disconnect reason code into the Reason Code Name, as it appears in the mqtt5 spec.
 *
 * @param reason_code a disconnect reason code
 * @return name associated with the reason code
 */
AWS_MQTT_API const char *aws_mqtt5_disconnect_reason_code_to_c_string(
    enum aws_mqtt5_disconnect_reason_code reason_code,
    bool *is_valid);

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

/**
 * Converts a packet type value to a readable description.
 *
 * @param packet_type type of packet
 * @return short string describing the packet type
 */
AWS_MQTT_API const char *aws_mqtt5_packet_type_to_c_string(enum aws_mqtt5_packet_type packet_type);

/**
 *
 * @param mqtt_error type of error
 * @return short string describing the mqtt error
 */
AWS_MQTT_API const char *aws_mqtt_error_to_c_string(enum aws_mqtt_error mqtt_error);

/**
 * Computes a uniformly-distributed random number in the specified range.  Not intended for cryptographic purposes.
 *
 * @param from one end of the range to sample from
 * @param to other end of the range to sample from
 * @return a random number from the supplied range, with roughly a uniform distribution
 */
AWS_MQTT_API uint64_t aws_mqtt5_client_random_in_range(uint64_t from, uint64_t to);

/**
 * Utility function to skip the "$aws/rules/<rule-name>/" prefix of a topic.  Technically this works for topic
 * filters too.
 *
 * @param topic_cursor topic to get the non-rules suffix for
 * @return remaining part of the topic after the leading AWS IoT Rules prefix has been skipped, if present
 */
AWS_MQTT_API struct aws_byte_cursor aws_mqtt5_topic_skip_aws_iot_rules_prefix(struct aws_byte_cursor topic_cursor);

/**
 * Computes the number of topic segments in a topic or topic filter
 * @param topic_cursor topic or topic filter
 * @return number of topic segments in the topic or topic filter
 */
AWS_MQTT_API size_t aws_mqtt5_topic_get_segment_count(const struct aws_byte_cursor topic_cursor);

/**
 * Checks a topic filter for validity against AWS IoT Core rules
 * @param topic_filter_cursor topic filter to check
 * @return true if valid, false otherwise
 */
AWS_MQTT_API bool aws_mqtt_is_valid_topic_filter_for_iot_core(struct aws_byte_cursor topic_filter_cursor);

/**
 * Checks a topic for validity against AWS IoT Core rules
 * @param topic_cursor topic to check
 * @return true if valid, false otherwise
 */
AWS_MQTT_API bool aws_mqtt_is_valid_topic_for_iot_core(struct aws_byte_cursor topic_cursor);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_MQTT5_UTILS_H */
