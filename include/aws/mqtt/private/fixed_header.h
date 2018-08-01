#ifndef AWS_MQTT_PRIVATE_FIXED_HEADER_H
#define AWS_MQTT_PRIVATE_FIXED_HEADER_H

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

#include <aws/mqtt/mqtt.h>

struct aws_mqtt_fixed_header {
    uint8_t flags;
    enum aws_mqtt_packet_type packet_type;
    size_t remaining_length;
};

enum aws_mqtt_has_payload {
    PACKET_PAYLOAD_NONE,
    PACKET_PAYLOAD_OPTIONAL,
    PACKET_PAYLOAD_REQUIRED,
};

struct packet_traits {
    bool has_flags;
    bool has_variable_header;
    enum aws_mqtt_has_payload has_payload;
};

/**
 * Get traits describing a packet described by header.
 */
struct packet_traits aws_mqtt_get_packet_type_traits(struct aws_mqtt_fixed_header *header);

/**
 * Write a fixed header to a byte stream.
 */
int aws_mqtt_fixed_header_encode(struct aws_byte_cursor *cur, struct aws_mqtt_fixed_header *header);

/**
 * Read a fixed header from a byte stream.
 */
int aws_mqtt_fixed_header_decode(struct aws_byte_cursor *cur, struct aws_mqtt_fixed_header *header);

#endif /* AWS_MQTT_PRIVATE_FIXED_HEADER_H */
