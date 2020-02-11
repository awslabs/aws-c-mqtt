#ifndef AWS_MQTT_MQTT_H
#define AWS_MQTT_MQTT_H

/*
 * Copyright 2010-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

#include <aws/common/byte_buf.h>
#include <aws/common/logging.h>

#include <aws/mqtt/exports.h>

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
    AWS_ERROR_MQTT_NO_TOPICS_FOR_RESUBSCRIBE,
    AWS_ERROR_MQTT_CONNECTION_SHUTDOWN,

    AWS_ERROR_END_MQTT_RANGE = AWS_ERROR_ENUM_END_RANGE(AWS_C_MQTT_PACKAGE_ID),
};

enum aws_mqtt_log_subject {
    AWS_LS_MQTT_GENERAL = AWS_LOG_SUBJECT_BEGIN_RANGE(AWS_C_MQTT_PACKAGE_ID),
    AWS_LS_MQTT_CLIENT,
    AWS_LS_MQTT_TOPIC_TREE,
};

/** Function called on cleanup of a userdata. */
typedef void(aws_mqtt_userdata_cleanup_fn)(void *userdata);

AWS_EXTERN_C_BEGIN

AWS_MQTT_API
bool aws_mqtt_is_valid_topic(const struct aws_byte_cursor *topic);
AWS_MQTT_API
bool aws_mqtt_is_valid_topic_filter(const struct aws_byte_cursor *topic_filter);

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

#endif /* AWS_MQTT_MQTT_H */
