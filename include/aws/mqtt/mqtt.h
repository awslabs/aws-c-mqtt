#ifndef AWS_MQTT_MQTT_H
#define AWS_MQTT_MQTT_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/byte_buf.h>
#include <aws/common/logging.h>

#include <aws/mqtt/exports.h>

AWS_PUSH_SANE_WARNING_LEVEL

#define AWS_C_MQTT_PACKAGE_ID 5

/* Quality of Service associated with a publish action or subscription [MQTT-4.3]. */
enum aws_mqtt_qos {
    AWS_MQTT_QOS_AT_MOST_ONCE = 0x0,
    AWS_MQTT_QOS_AT_LEAST_ONCE = 0x1,
    AWS_MQTT_QOS_EXACTLY_ONCE = 0x2,
    /* reserved = 3 */
    AWS_MQTT_QOS_FAILURE = 0x80, /* Only used in SUBACK packets */
};

/* Result of a connect request [MQTT-3.2.2.3]. */
enum aws_mqtt_connect_return_code {
    AWS_MQTT_CONNECT_ACCEPTED,
    AWS_MQTT_CONNECT_UNACCEPTABLE_PROTOCOL_VERSION,
    AWS_MQTT_CONNECT_IDENTIFIER_REJECTED,
    AWS_MQTT_CONNECT_SERVER_UNAVAILABLE,
    AWS_MQTT_CONNECT_BAD_USERNAME_OR_PASSWORD,
    AWS_MQTT_CONNECT_NOT_AUTHORIZED,
    /* reserved = 6 - 255 */
};

enum aws_mqtt_error {
    AWS_ERROR_MQTT_INVALID_RESERVED_BITS = AWS_ERROR_ENUM_BEGIN_RANGE(AWS_C_MQTT_PACKAGE_ID),
    AWS_ERROR_MQTT_BUFFER_TOO_BIG,
    AWS_ERROR_MQTT_INVALID_REMAINING_LENGTH,
    AWS_ERROR_MQTT_UNSUPPORTED_PROTOCOL_NAME,
    AWS_ERROR_MQTT_UNSUPPORTED_PROTOCOL_LEVEL,
    AWS_ERROR_MQTT_INVALID_CREDENTIALS,
    AWS_ERROR_MQTT_INVALID_QOS,
    AWS_ERROR_MQTT_INVALID_PACKET_TYPE,
    AWS_ERROR_MQTT_INVALID_TOPIC,
    AWS_ERROR_MQTT_TIMEOUT,
    AWS_ERROR_MQTT_PROTOCOL_ERROR,
    AWS_ERROR_MQTT_NOT_CONNECTED,
    AWS_ERROR_MQTT_ALREADY_CONNECTED,
    AWS_ERROR_MQTT_BUILT_WITHOUT_WEBSOCKETS,
    AWS_ERROR_MQTT_UNEXPECTED_HANGUP,
    AWS_ERROR_MQTT_CONNECTION_SHUTDOWN,
    AWS_ERROR_MQTT_CONNECTION_DESTROYED,
    AWS_ERROR_MQTT_CONNECTION_DISCONNECTING,
    AWS_ERROR_MQTT_CANCELLED_FOR_CLEAN_SESSION,
    AWS_ERROR_MQTT_QUEUE_FULL,
    AWS_ERROR_MQTT5_CLIENT_OPTIONS_VALIDATION,
    AWS_ERROR_MQTT5_CONNECT_OPTIONS_VALIDATION,
    AWS_ERROR_MQTT5_DISCONNECT_OPTIONS_VALIDATION,
    AWS_ERROR_MQTT5_PUBLISH_OPTIONS_VALIDATION,
    AWS_ERROR_MQTT5_SUBSCRIBE_OPTIONS_VALIDATION,
    AWS_ERROR_MQTT5_UNSUBSCRIBE_OPTIONS_VALIDATION,
    AWS_ERROR_MQTT5_USER_PROPERTY_VALIDATION,
    AWS_ERROR_MQTT5_PACKET_VALIDATION,
    AWS_ERROR_MQTT5_ENCODE_FAILURE,
    AWS_ERROR_MQTT5_DECODE_PROTOCOL_ERROR,
    AWS_ERROR_MQTT5_CONNACK_CONNECTION_REFUSED,
    AWS_ERROR_MQTT5_CONNACK_TIMEOUT,
    AWS_ERROR_MQTT5_PING_RESPONSE_TIMEOUT,
    AWS_ERROR_MQTT5_USER_REQUESTED_STOP,
    AWS_ERROR_MQTT5_DISCONNECT_RECEIVED,
    AWS_ERROR_MQTT5_CLIENT_TERMINATED,
    AWS_ERROR_MQTT5_OPERATION_FAILED_DUE_TO_OFFLINE_QUEUE_POLICY,
    AWS_ERROR_MQTT5_ENCODE_SIZE_UNSUPPORTED_PACKET_TYPE,
    AWS_ERROR_MQTT5_OPERATION_PROCESSING_FAILURE,
    AWS_ERROR_MQTT5_INVALID_INBOUND_TOPIC_ALIAS,
    AWS_ERROR_MQTT5_INVALID_OUTBOUND_TOPIC_ALIAS,
    AWS_ERROR_MQTT5_INVALID_UTF8_STRING,
    AWS_ERROR_MQTT_CONNECTION_RESET_FOR_ADAPTER_CONNECT,
    AWS_ERROR_MQTT_CONNECTION_RESUBSCRIBE_NO_TOPICS,
    AWS_ERROR_MQTT_CONNECTION_SUBSCRIBE_FAILURE,
    AWS_ERROR_MQTT_PROTOCOL_ADAPTER_FAILING_REASON_CODE,

    AWS_ERROR_END_MQTT_RANGE = AWS_ERROR_ENUM_END_RANGE(AWS_C_MQTT_PACKAGE_ID),
};

enum aws_mqtt_log_subject {
    AWS_LS_MQTT_GENERAL = AWS_LOG_SUBJECT_BEGIN_RANGE(AWS_C_MQTT_PACKAGE_ID),
    AWS_LS_MQTT_CLIENT,
    AWS_LS_MQTT_TOPIC_TREE,
    AWS_LS_MQTT5_GENERAL,
    AWS_LS_MQTT5_CLIENT,
    AWS_LS_MQTT5_CANARY,
    AWS_LS_MQTT5_TO_MQTT3_ADAPTER,
};

/** Function called on cleanup of a userdata. */
typedef void(aws_mqtt_userdata_cleanup_fn)(void *userdata);

AWS_EXTERN_C_BEGIN

AWS_MQTT_API
bool aws_mqtt_is_valid_topic(const struct aws_byte_cursor *topic);

AWS_MQTT_API
bool aws_mqtt_is_valid_topic_filter(const struct aws_byte_cursor *topic_filter);

/**
 * Validate utf-8 string under mqtt specs
 *
 * @param text
 * @return AWS_OP_SUCCESS if the text is validate, otherwise AWS_OP_ERR
 */
AWS_MQTT_API int aws_mqtt_validate_utf8_text(struct aws_byte_cursor text);

/**
 * Initializes internal datastructures used by aws-c-mqtt.
 * Must be called before using any functionality in aws-c-mqtt.
 */
AWS_MQTT_API
void aws_mqtt_library_init(struct aws_allocator *allocator);

/**
 * Shuts down the internal datastructures used by aws-c-mqtt.
 */
AWS_MQTT_API
void aws_mqtt_library_clean_up(void);

AWS_MQTT_API
void aws_mqtt_fatal_assert_library_initialized(void);

AWS_EXTERN_C_END
AWS_POP_SANE_WARNING_LEVEL

#endif /* AWS_MQTT_MQTT_H */
