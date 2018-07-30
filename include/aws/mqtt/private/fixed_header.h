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
    unsigned flags : 4;
    unsigned packet_type : 4;
    uint32_t remaining_length;
};

/**
 * Cast a aws_mqtt_fixed_header * to a aws_mqtt_fixed_header_flags * to write
 * individual flags.
 *
 * Add more structs to support flags in different packet types.
 * \note No struct my exceed 4 bits in size without causing damage.
 */
union aws_mqtt_fixed_header_flags {
    struct aws_mqtt_fixed_header header;

    struct {
        bool retain : 1;
        unsigned qos : 2;
        bool dup : 1;
    } publish;
};

enum aws_mqtt_has_payload {
    PACKET_PAYLOAD_NONE,
    PACKET_PAYLOAD_OPTIONAL,
    PACKET_PAYLOAD_REQUIRED,
};

struct packet_traits {
    bool has_flags : 1;
    bool has_variable_header : 1;
    bool has_id : 1; /* Special form of variable_header */
    unsigned has_payload : 2;
};

/**
 * Get traits describing a packet described by header.
 */
struct packet_traits aws_mqtt_get_packet_type_traits(struct aws_mqtt_fixed_header *header);

/**
 * Write a fixed header to a byte stream.
 */
int aws_mqtt_encode_fixed_header(struct aws_byte_cursor *cur, struct aws_mqtt_fixed_header *header);

/**
 * Read a fixed header from a byte stream.
 */
int aws_mqtt_decode_fixed_header(struct aws_byte_cursor *cur, struct aws_mqtt_fixed_header *header);

#endif /* AWS_MQTT_PRIVATE_FIXED_HEADER_H */
