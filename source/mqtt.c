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

#include <aws/mqtt/mqtt.h>

/*******************************************************************************
 * Topic Validation
 ******************************************************************************/

static bool s_is_valid_topic(const struct aws_byte_cursor *topic, bool is_filter) {

    /* [MQTT-4.7.3-1] Check existance and length */
    if (!topic->ptr || !topic->len) {
        return false;
    }

    /* [MQTT-4.7.3-2] Check for the null character */
    if (memchr(topic->ptr, 0, topic->len)) {
        return false;
    }

    /* [MQTT-4.7.3-3] Topic must not be too long */
    if (topic->len > 65535) {
        return false;
    }

    bool saw_hash = false;

    struct aws_byte_cursor topic_part;
    AWS_ZERO_STRUCT(topic_part);
    while (aws_byte_cursor_next_split(topic, '/', &topic_part)) {

        if (saw_hash) {
            /* [MQTT-4.7.1-2] If last part was a '#' and there's still another part, it's an invalid topic */
            return false;
        }

        if (topic_part.len == 0) {
            /* 0 length parts are fine */
            continue;
        }

        /* Check single level wildcard */
        if (memchr(topic_part.ptr, '+', topic_part.len)) {
            if (!is_filter) {
                /* [MQTT-4.7.1-3] + only allowed on filters */
                return false;
            }
            if (topic_part.len > 1) {
                /* topic part must be 1 character long */
                return false;
            }
        }

        /* Check multi level wildcard */
        if (memchr(topic_part.ptr, '#', topic_part.len)) {
            if (!is_filter) {
                /* [MQTT-4.7.1-2] # only allowed on filters */
                return false;
            }
            if (topic_part.len > 1) {
                /* topic part must be 1 character long */
                return false;
            }
            saw_hash = true;
        }
    }

    return true;
}

bool aws_mqtt_is_valid_topic(const struct aws_byte_cursor *topic) {

    return s_is_valid_topic(topic, false);
}
bool aws_mqtt_is_valid_topic_filter(const struct aws_byte_cursor *topic_filter) {

    return s_is_valid_topic(topic_filter, true);
}

/*******************************************************************************
 * Load Error String
 ******************************************************************************/

void aws_mqtt_load_error_strings() {

    static bool s_error_strings_loaded = false;
    if (!s_error_strings_loaded) {

        s_error_strings_loaded = true;

#define AWS_DEFINE_ERROR_INFO_MQTT(C, ES) AWS_DEFINE_ERROR_INFO(C, ES, "libaws-c-mqtt")
        /* clang-format off */
        static struct aws_error_info s_errors[] = {
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_INVALID_RESERVED_BITS,
                "Bits marked as reserved in the MQTT spec were incorrectly set."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_BUFFER_TOO_BIG,
                "[MQTT-1.5.3] Encoded UTF-8 buffers may be no bigger than 65535 bytes."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_INVALID_REMAINING_LENGTH,
                "[MQTT-2.2.3] Encoded remaining length field is malformed."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_UNSUPPORTED_PROTOCOL_NAME,
                "[MQTT-3.1.2-1] Protocol name specified is unsupported."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_UNSUPPORTED_PROTOCOL_LEVEL,
                "[MQTT-3.1.2-2] Protocol level specified is unsupported."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_INVALID_CREDENTIALS,
                "[MQTT-3.1.2-21] Connect packet may not include password when no username is present."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_INVALID_QOS,
                "Both bits in a QoS field must not be set."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_INVALID_PACKET_TYPE,
                "Packet type in packet fixed header is invalid."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_INVALID_TOPIC,
                "Topic or filter is invalid."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_TIMEOUT,
                "Time limit between request and response has been exceeded."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_PROTOCOL_ERROR,
                "Protocol error occured."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_NOT_CONNECTED,
                "The requested operation is invalid as the connection is not open."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_ALREADY_CONNECTED,
                "The requested operation is invalid as the connection is already open."),
        };
        /* clang-format on */
#undef AWS_DEFINE_ERROR_INFO_MQTT

        static struct aws_error_info_list s_list = {
            .error_list = s_errors,
            .count = AWS_ARRAY_SIZE(s_errors),
        };
        aws_register_error_info(&s_list);
    }
}
