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

#include <aws/mqtt/private/fixed_header.h>

#include <assert.h>

static int s_encode_remaining_length(struct aws_byte_cursor *cur, size_t remaining_length) {

    assert(cur);
    assert(remaining_length < UINT32_MAX);

    do {
        uint8_t encoded_byte = remaining_length % 128;
        remaining_length /= 128;
        if (remaining_length) {
            encoded_byte |= 128;
        }
        if (!aws_byte_cursor_write_u8(cur, encoded_byte)) {
            return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
        }
    } while (remaining_length);

    return AWS_OP_SUCCESS;
}

static int s_decode_remaining_length(struct aws_byte_cursor *cur, size_t *remaining_length) {

    assert(cur);

    /* Read remaining_length */
    size_t multiplier = 1;
    *remaining_length = 0;
    while (true) {
        uint8_t encoded_byte;
        if (!aws_byte_cursor_read_u8(cur, &encoded_byte)) {
            return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
        }

        *remaining_length += (encoded_byte & 127) * multiplier;
        multiplier *= 128;

        if (!(encoded_byte & 128)) {
            break;
        }
        if (multiplier > 128 * 128 * 128) {
            /* If high order bit is set on last byte, value is malformed */
            return aws_raise_error(INT32_MAX);
        }
    }

    return AWS_OP_SUCCESS;
}

struct packet_traits aws_mqtt_get_packet_type_traits(struct aws_mqtt_fixed_header *header) {

    struct packet_traits traits = {
        .has_flags = false,
        .has_variable_header = false,
        .has_payload = PACKET_PAYLOAD_NONE,
    };

    /* Parse attributes based on packet type */
    switch (header->packet_type) {
        case AWS_MQTT_PACKET_CONNECT:
            traits.has_variable_header = true;
            traits.has_payload = PACKET_PAYLOAD_REQUIRED;
            break;

        case AWS_MQTT_PACKET_PUBLISH:
            traits.has_flags = true;
            traits.has_payload = PACKET_PAYLOAD_OPTIONAL;
            break;

        case AWS_MQTT_PACKET_PUBREL:
            traits.has_flags = true;
            break;

        case AWS_MQTT_PACKET_CONNACK:
        case AWS_MQTT_PACKET_PUBACK:
        case AWS_MQTT_PACKET_PUBREC:
        case AWS_MQTT_PACKET_PUBCOMP:
        case AWS_MQTT_PACKET_UNSUBACK:
        case AWS_MQTT_PACKET_PINGREQ:
        case AWS_MQTT_PACKET_PINGRESP:
        case AWS_MQTT_PACKET_DISCONNECT:
            break;

        case AWS_MQTT_PACKET_SUBSCRIBE:
        case AWS_MQTT_PACKET_UNSUBSCRIBE:
            traits.has_flags = true;
            /* fallthrough */

        case AWS_MQTT_PACKET_SUBACK:
            traits.has_payload = PACKET_PAYLOAD_REQUIRED;
            break;
    }

    return traits;
}

int aws_mqtt_fixed_header_encode(struct aws_byte_cursor *cur, struct aws_mqtt_fixed_header *header) {

    assert(cur);
    assert(header);

    struct packet_traits traits = aws_mqtt_get_packet_type_traits(header);

    /* Check that flags are 0 if they must not be present */
    if (!traits.has_flags && header->flags != 0) {
        return aws_raise_error(INT32_MAX);
    }

    /* Write packet type and flags */
    uint8_t byte_1 = (uint8_t)(header->packet_type) << 4 | (header->flags & 0xF);
    if (!aws_byte_cursor_write_u8(cur, byte_1)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    /* Write remaining length */
    if (s_encode_remaining_length(cur, header->remaining_length)) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

int aws_mqtt_fixed_header_decode(struct aws_byte_cursor *cur, struct aws_mqtt_fixed_header *header) {

    assert(cur);
    assert(header);

    /* Read packet type and flags */
    uint8_t byte_1 = 0;
    if (!aws_byte_cursor_read_u8(cur, &byte_1)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }
    header->packet_type = byte_1 >> 4;
    header->flags = byte_1 & 0xF;

    /* Read remaining length */
    if (s_decode_remaining_length(cur, &header->remaining_length)) {
        return AWS_OP_ERR;
    }
    if (cur->len < header->remaining_length) {
        return aws_raise_error(INT32_MAX);
    }

    struct packet_traits traits = aws_mqtt_get_packet_type_traits(header);

    /* Check that flags are 0 if they must not be present */
    if (!traits.has_flags && header->flags != 0) {
        return aws_raise_error(INT32_MAX);
    }

    return AWS_OP_SUCCESS;
}
