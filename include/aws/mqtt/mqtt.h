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

#include <aws/mqtt/exports.h>

#include <aws/common/byte_buf.h>

/* Represents the types of the MQTT control packets [MQTT-2.2.1]. */
enum aws_mqtt_packet_type {
    /* reserved = 0, */
    AWS_MQTT_PACKET_CONNECT = 1,
    AWS_MQTT_PACKET_CONNACK,
    AWS_MQTT_PACKET_PUBLISH,
    AWS_MQTT_PACKET_PUBACK,
    AWS_MQTT_PACKET_PUBREC,
    AWS_MQTT_PACKET_PUBREL,
    AWS_MQTT_PACKET_PUBCOMP,
    AWS_MQTT_PACKET_SUBSCRIBE,
    AWS_MQTT_PACKET_SUBACK,
    AWS_MQTT_PACKET_UNSUBSCRIBE,
    AWS_MQTT_PACKET_UNSUBACK,
    AWS_MQTT_PACKET_PINGREQ,
    AWS_MQTT_PACKET_PINGRESP,
    AWS_MQTT_PACKET_DISCONNECT,
    /* reserved = 15, */
};

/* Quality of Service associated with a publish action or subscription [MQTT-4.3]. */
enum aws_mqtt_qos {
    AWS_MQTT_QOS_AT_MOST_ONCE = 0,
    AWS_MQTT_QOS_AT_LEAST_ONCE = 1,
    AWS_MQTT_QOS_EXACTLY_ONCE = 2,
    /* reserved = 3 */
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

struct aws_mqtt_subscription {
    /* Topic filte to subscribe to [MQTT-4.7]. */
    struct aws_byte_cursor topic_filter;
    /* Maximum QoS of messages to receive [MQTT-4.3]. */
    enum aws_mqtt_qos qos;
};

#endif /* AWS_MQTT_MQTT_H */
