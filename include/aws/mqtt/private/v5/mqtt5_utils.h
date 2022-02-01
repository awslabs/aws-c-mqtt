#ifndef AWS_MQTT_MQTT5_UTILS_H
#define AWS_MQTT_MQTT5_UTILS_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

#include <aws/mqtt/v5/mqtt5_types.h>

struct aws_byte_buf;

#define AWS_MQTT5_MAXIMUM_VARIABLE_LENGTH_INTEGER 268435455

/* property type codes */
#define AWS_MQTT5_PROPERTY_TYPE_PAYLOAD_FORMAT_INDICATOR ((uint8_t)1)
#define AWS_MQTT5_PROPERTY_TYPE_MESSAGE_EXPIRY_INTERVAL ((uint8_t)2)
#define AWS_MQTT5_PROPERTY_TYPE_CONTENT_TYPE ((uint8_t)3)
#define AWS_MQTT5_PROPERTY_TYPE_RESPONSE_TOPIC ((uint8_t)8)
#define AWS_MQTT5_PROPERTY_TYPE_CORRELATION_DATA ((uint8_t)9)
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
#define AWS_MQTT5_CONNECT_FLAGS_WILL_RETAIN_BIT (1U << 6)

#define AWS_MQTT5_CONNECT_FLAGS_WILL_QOS_BIT_POSITION 3
#define AWS_MQTT5_CONNECT_FLAGS_WILL_QOS_BIT_MASK 0x03

AWS_EXTERN_C_BEGIN

extern struct aws_byte_cursor g_aws_mqtt5_connect_protocol_cursor;

AWS_MQTT_API int aws_mqtt5_encode_variable_length_integer(struct aws_byte_buf *buf, uint32_t value);

AWS_MQTT_API int aws_mqtt5_get_variable_length_encode_size(size_t value, size_t *encode_size);

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

#endif /* AWS_MQTT_MQTT5_UTILS_H */
