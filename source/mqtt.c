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

static bool s_error_strings_loaded = false;

#define AWS_DEFINE_ERROR_INFO_MQTT(C, ES) AWS_DEFINE_ERROR_INFO(C, ES, "libaws-c-mqtt")
/* clang-format off */
static struct aws_error_info errors[] = {
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
        AWS_ERROR_MQTT_PROTOCOL_ERROR,
        "Protocol error occured."),
};
/* clang-format on */
#undef AWS_DEFINE_ERROR_INFO_MQTT

static struct aws_error_info_list s_list = {
    .error_list = errors,
    .count = AWS_ARRAY_SIZE(errors),
};

void aws_mqtt_load_error_strings() {

    if (!s_error_strings_loaded) {

        s_error_strings_loaded = true;
        aws_register_error_info(&s_list);
    }
}
