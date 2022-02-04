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

AWS_EXTERN_C_BEGIN

int aws_mqtt5_encode_variable_length_integer(struct aws_byte_buf *buf, uint32_t value);

int aws_mqtt5_get_variable_length_encode_size(size_t value, size_t *encode_size);

void aws_mqtt5_negotiated_settings_log(
    struct aws_mqtt5_negotiated_settings *negotiated_settings,
    enum aws_log_level level); 

void aws_mqtt5_negotiated_settings_reset(
    struct aws_mqtt5_negotiated_settings *negotiated_settings,
    struct aws_mqtt5_packet_connect_view *packet_connect_view);

int aws_mqtt5_negotiated_settings_apply_connack(
    struct aws_mqtt5_negotiated_settings *negotiated_settings,
    struct aws_mqtt5_packet_connack_view *connack_data);

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
