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

#include <aws/mqtt/private/packets.h>

#include <assert.h>

enum { S_PROTOCOL_LEVEL = 4 };
enum { S_BIT_1_FLAGS = 0x2 };

static struct aws_byte_cursor s_protocol_name = {
    .ptr = (uint8_t *)"MQTT",
    .len = 4,
};

static size_t s_sizeof_encoded_buffer(struct aws_byte_cursor *buf) {
    return sizeof(uint16_t) + buf->len;
}

static int s_encode_buffer(struct aws_byte_cursor *cur, const struct aws_byte_cursor buf) {

    assert(cur);

    /* Make sure the buffer isn't too big */
    if (buf.len > UINT16_MAX) {
        return aws_raise_error(INT32_MAX);
    }

    /* Write the length */
    if (!aws_byte_cursor_write_be16(cur, (uint16_t)buf.len)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    /* Write the data */
    if (!aws_byte_cursor_write(cur, buf.ptr, buf.len)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    return AWS_OP_SUCCESS;
}

static int s_decode_buffer(struct aws_byte_cursor *cur, struct aws_byte_cursor *buf) {

    assert(cur);
    assert(buf);

    /* Read the length */
    uint16_t len;
    if (!aws_byte_cursor_read_be16(cur, &len)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    /* Store the data */
    *buf = aws_byte_cursor_advance(cur, len);

    return AWS_OP_SUCCESS;
}

/*****************************************************************************/
/* Ack                                                                       */

static void s_ack_init(struct aws_mqtt_packet_ack *packet, enum aws_mqtt_packet_type type, uint16_t packet_identifier) {

    assert(packet);

    AWS_ZERO_STRUCT(*packet);

    packet->fixed_header.packet_type = type;
    packet->fixed_header.remaining_length = sizeof(uint16_t);

    packet->packet_identifier = packet_identifier;
}

int aws_mqtt_packet_ack_encode(struct aws_byte_cursor *cur, const struct aws_mqtt_packet_ack *packet) {

    assert(cur);
    assert(packet);

    /*************************************************************************/
    /* Fixed Header */

    if (aws_mqtt_fixed_header_encode(cur, &packet->fixed_header)) {
        return AWS_OP_ERR;
    }

    /*************************************************************************/
    /* Variable Header                                                       */

    /* Write packet identifier */
    if (!aws_byte_cursor_write_be16(cur, packet->packet_identifier)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    return AWS_OP_SUCCESS;
}

int aws_mqtt_packet_ack_decode(struct aws_byte_cursor *cur, struct aws_mqtt_packet_ack *packet) {

    assert(cur);
    assert(packet);

    /*************************************************************************/
    /* Fixed Header */

    if (aws_mqtt_fixed_header_decode(cur, &packet->fixed_header)) {
        return AWS_OP_ERR;
    }

    /* Validate flags */
    if (packet->fixed_header.flags != (aws_mqtt_packet_has_flags(&packet->fixed_header) ? S_BIT_1_FLAGS : 0u)) {

        return aws_raise_error(INT32_MAX);
    }

    /*************************************************************************/
    /* Variable Header                                                       */

    /* Read packet identifier */
    if (!aws_byte_cursor_read_be16(cur, &packet->packet_identifier)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    return AWS_OP_SUCCESS;
}

/*****************************************************************************/
/* Connect                                                                   */

int aws_mqtt_packet_connect_init(
    struct aws_mqtt_packet_connect *packet,
    struct aws_byte_cursor client_identifier,
    bool clean_session,
    uint16_t keep_alive) {

    assert(packet);
    assert(client_identifier.len > 0);

    AWS_ZERO_STRUCT(*packet);

    packet->fixed_header.packet_type = AWS_MQTT_PACKET_CONNECT;
    /* [MQTT-3.1.1] */
    packet->fixed_header.remaining_length = 10 + s_sizeof_encoded_buffer(&client_identifier);

    packet->client_identifier = client_identifier;
    packet->clean_session = clean_session;
    packet->keep_alive_timeout = keep_alive;

    return AWS_OP_SUCCESS;
}

int aws_mqtt_packet_connect_add_credentials(
    struct aws_mqtt_packet_connect *packet,
    struct aws_byte_cursor username,
    struct aws_byte_cursor password) {

    assert(packet);
    assert(username.len > 0);

    if (!packet->has_username) {
        /* If not already username, add size of length field */
        packet->fixed_header.remaining_length += 2;
    }

    /* Add change in size to remaining_length */
    packet->fixed_header.remaining_length += username.len - packet->username.len;
    packet->has_username = true;

    packet->username = username;

    if (password.len > 0) {

        if (!packet->has_password) {
            /* If not already password, add size of length field */
            packet->fixed_header.remaining_length += 2;
        }

        /* Add change in size to remaining_length */
        packet->fixed_header.remaining_length += password.len - packet->password.len;
        packet->has_password = true;

        packet->password = password;
    }

    return AWS_OP_SUCCESS;
}

int aws_mqtt_packet_connect_encode(struct aws_byte_cursor *cur, const struct aws_mqtt_packet_connect *packet) {

    assert(cur);
    assert(packet);

    /* Do validation */
    if (packet->has_password && !packet->has_username) {

        return aws_raise_error(INT32_MAX);
    }

    /*************************************************************************/
    /* Fixed Header */

    if (aws_mqtt_fixed_header_encode(cur, &packet->fixed_header)) {
        return AWS_OP_ERR;
    }

    /*************************************************************************/
    /* Variable Header                                                       */

    /* Write protocol name */
    if (s_encode_buffer(cur, s_protocol_name)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    /* Write protocol level */
    if (!aws_byte_cursor_write_u8(cur, S_PROTOCOL_LEVEL)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    /* Write connect flags [MQTT-3.1.2.3] */
    uint8_t connect_flags = packet->clean_session << 1 | packet->has_will << 2 | (uint8_t)packet->will_qos << 3 |
                            packet->will_retain << 5 | packet->has_password << 6 | packet->has_username << 7;

    if (!aws_byte_cursor_write_u8(cur, connect_flags)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    /* Write keep alive */
    if (!aws_byte_cursor_write_be16(cur, packet->keep_alive_timeout)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    /*************************************************************************/
    /* Payload                                                               */

    /* Client identifier is required, write it */
    if (s_encode_buffer(cur, packet->client_identifier)) {
        return AWS_OP_ERR;
    }

    /* Write will */
    if (packet->has_will) {
        if (s_encode_buffer(cur, packet->will_topic)) {
            return AWS_OP_ERR;
        }
        if (s_encode_buffer(cur, packet->will_message)) {
            return AWS_OP_ERR;
        }
    }

    /* Write username */
    if (packet->has_username) {
        if (s_encode_buffer(cur, packet->username)) {
            return AWS_OP_ERR;
        }
    }

    /* Write password */
    if (packet->has_password) {
        if (s_encode_buffer(cur, packet->password)) {
            return AWS_OP_ERR;
        }
    }

    return AWS_OP_SUCCESS;
}

int aws_mqtt_packet_connect_decode(struct aws_byte_cursor *cur, struct aws_mqtt_packet_connect *packet) {

    assert(cur);
    assert(packet);

    /*************************************************************************/
    /* Fixed Header                                                          */

    if (aws_mqtt_fixed_header_decode(cur, &packet->fixed_header)) {
        return AWS_OP_ERR;
    }

    /*************************************************************************/
    /* Variable Header                                                       */

    /* Check protocol name */
    struct aws_byte_cursor protocol_name = {
        .ptr = NULL,
        .len = 0,
    };
    if (s_decode_buffer(cur, &protocol_name)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }
    assert(protocol_name.ptr && protocol_name.len);
    if (protocol_name.len != s_protocol_name.len) {
        return aws_raise_error(INT32_MAX);
    }
    if (memcmp(protocol_name.ptr, s_protocol_name.ptr, s_protocol_name.len) != 0) {
        return aws_raise_error(INT32_MAX);
    }

    /* Check protocol level */
    struct aws_byte_cursor protocol_level = aws_byte_cursor_advance(cur, 1);
    if (protocol_level.len == 0) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }
    if (*protocol_level.ptr != S_PROTOCOL_LEVEL) {
        return aws_raise_error(INT32_MAX);
    }

    /* Read connect flags [MQTT-3.1.2.3] */
    uint8_t connect_flags = 0;
    if (!aws_byte_cursor_read_u8(cur, &connect_flags)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }
    packet->clean_session = (connect_flags >> 1) & 0x1;
    packet->has_will = (connect_flags >> 2) & 0x1;
    packet->will_qos = (connect_flags >> 3) & 0x3;
    packet->will_retain = (connect_flags >> 5) & 0x1;
    packet->has_password = (connect_flags >> 6) & 0x1;
    packet->has_username = (connect_flags >> 7) & 0x1;

    /* Read keep alive */
    if (!aws_byte_cursor_read_be16(cur, &packet->keep_alive_timeout)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    /*************************************************************************/
    /* Payload                                                               */

    /* Client identifier is required, Read it */
    if (s_decode_buffer(cur, &packet->client_identifier)) {
        return AWS_OP_ERR;
    }

    /* Read will */
    if (packet->has_will) {
        if (s_decode_buffer(cur, &packet->will_topic)) {
            return AWS_OP_ERR;
        }
        if (s_decode_buffer(cur, &packet->will_message)) {
            return AWS_OP_ERR;
        }
    }

    /* Read username */
    if (packet->has_username) {
        if (s_decode_buffer(cur, &packet->username)) {
            return AWS_OP_ERR;
        }
    }

    /* Read password */
    if (packet->has_password) {
        if (s_decode_buffer(cur, &packet->password)) {
            return AWS_OP_ERR;
        }
    }

    /* Do validation */
    if (packet->has_password && !packet->has_username) {

        return aws_raise_error(INT32_MAX);
    }

    return AWS_OP_SUCCESS;
}

/*****************************************************************************/
/* Connack                                                                   */

int aws_mqtt_packet_connack_init(
    struct aws_mqtt_packet_connack *packet,
    bool session_present,
    enum aws_mqtt_connect_return_code return_code) {

    assert(packet);

    AWS_ZERO_STRUCT(*packet);

    packet->fixed_header.packet_type = AWS_MQTT_PACKET_CONNACK;
    packet->fixed_header.remaining_length = 1 + sizeof(packet->connect_return_code);

    packet->session_present = session_present;
    packet->connect_return_code = return_code;

    return AWS_OP_SUCCESS;
}

int aws_mqtt_packet_connack_encode(struct aws_byte_cursor *cur, const struct aws_mqtt_packet_connack *packet) {

    assert(cur);
    assert(packet);

    /*************************************************************************/
    /* Fixed Header */

    if (aws_mqtt_fixed_header_encode(cur, &packet->fixed_header)) {
        return AWS_OP_ERR;
    }

    /*************************************************************************/
    /* Variable Header                                                       */

    /* Read connack flags */
    uint8_t connack_flags = packet->session_present & 0x1;
    if (!aws_byte_cursor_write_u8(cur, connack_flags)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    /* Read return code */
    if (!aws_byte_cursor_write_u8(cur, packet->connect_return_code)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    return AWS_OP_SUCCESS;
}

int aws_mqtt_packet_connack_decode(struct aws_byte_cursor *cur, struct aws_mqtt_packet_connack *packet) {

    assert(cur);
    assert(packet);

    /*************************************************************************/
    /* Fixed Header                                                          */

    if (aws_mqtt_fixed_header_decode(cur, &packet->fixed_header)) {
        return AWS_OP_ERR;
    }

    /*************************************************************************/
    /* Variable Header                                                       */

    /* Read connack flags */
    uint8_t connack_flags = 0;
    if (!aws_byte_cursor_read_u8(cur, &connack_flags)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }
    packet->session_present = connack_flags & 0x1;

    /* Read return code */
    if (!aws_byte_cursor_read_u8(cur, &packet->connect_return_code)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    return AWS_OP_SUCCESS;
}

/*****************************************************************************/
/* Publish                                                                   */

int aws_mqtt_packet_publish_init(
    struct aws_mqtt_packet_publish *packet,
    bool retain,
    enum aws_mqtt_qos qos,
    bool dup,
    struct aws_byte_cursor topic_name,
    uint16_t packet_identifier,
    struct aws_byte_cursor payload) {

    assert(packet);
    assert(topic_name.len > 0);

    AWS_ZERO_STRUCT(*packet);

    packet->fixed_header.packet_type = AWS_MQTT_PACKET_PUBLISH;
    packet->fixed_header.remaining_length =
        s_sizeof_encoded_buffer(&topic_name) + sizeof(packet->packet_identifier) + payload.len;

    /* [MQTT-2.2.2] */
    uint8_t publish_flags = (retain & 0x1) | (qos & 0x3) << 1 | (dup & 0x1) << 3;
    packet->fixed_header.flags = publish_flags;

    packet->topic_name = topic_name;
    packet->packet_identifier = packet_identifier;
    packet->payload = payload;

    return AWS_OP_SUCCESS;
}

int aws_mqtt_packet_publish_encode(struct aws_byte_cursor *cur, const struct aws_mqtt_packet_publish *packet) {

    assert(cur);
    assert(packet);

    /*************************************************************************/
    /* Fixed Header */

    if (aws_mqtt_fixed_header_encode(cur, &packet->fixed_header)) {
        return AWS_OP_ERR;
    }

    /*************************************************************************/
    /* Variable Header                                                       */

    /* Write topic name */
    if (s_encode_buffer(cur, packet->topic_name)) {
        return AWS_OP_ERR;
    }

    /* Write packet identifier */
    if (!aws_byte_cursor_write_be16(cur, packet->packet_identifier)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    /*************************************************************************/
    /* Payload                                                               */

    if (!aws_byte_cursor_write(cur, packet->payload.ptr, packet->payload.len)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    return AWS_OP_SUCCESS;
}

int aws_mqtt_packet_publish_decode(struct aws_byte_cursor *cur, struct aws_mqtt_packet_publish *packet) {

    assert(cur);
    assert(packet);

    /*************************************************************************/
    /* Fixed Header */

    if (aws_mqtt_fixed_header_decode(cur, &packet->fixed_header)) {
        return AWS_OP_ERR;
    }

    /*************************************************************************/
    /* Variable Header                                                       */

    /* Read topic name */
    if (s_decode_buffer(cur, &packet->topic_name)) {
        return AWS_OP_ERR;
    }

    /* Read packet identifier */
    if (!aws_byte_cursor_read_be16(cur, &packet->packet_identifier)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    /*************************************************************************/
    /* Payload                                                               */

    size_t payload_size = packet->fixed_header.remaining_length - s_sizeof_encoded_buffer(&packet->topic_name) -
                          sizeof(packet->packet_identifier);
    packet->payload = aws_byte_cursor_advance(cur, payload_size);
    if (packet->payload.len == 0) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    return AWS_OP_SUCCESS;
}

/*****************************************************************************/
/* Puback                                                                    */

int aws_mqtt_packet_puback_init(struct aws_mqtt_packet_ack *packet, uint16_t packet_identifier) {

    s_ack_init(packet, AWS_MQTT_PACKET_PUBACK, packet_identifier);

    return AWS_OP_SUCCESS;
}

/*****************************************************************************/
/* Pubrec                                                                    */

int aws_mqtt_packet_pubrec_init(struct aws_mqtt_packet_ack *packet, uint16_t packet_identifier) {

    s_ack_init(packet, AWS_MQTT_PACKET_PUBREC, packet_identifier);

    return AWS_OP_SUCCESS;
}

/*****************************************************************************/
/* Pubrel                                                                    */

int aws_mqtt_packet_pubrel_init(struct aws_mqtt_packet_ack *packet, uint16_t packet_identifier) {

    s_ack_init(packet, AWS_MQTT_PACKET_PUBREL, packet_identifier);
    packet->fixed_header.flags = S_BIT_1_FLAGS;

    return AWS_OP_SUCCESS;
}

/*****************************************************************************/
/* Pubcomp                                                                   */

int aws_mqtt_packet_pubcomp_init(struct aws_mqtt_packet_ack *packet, uint16_t packet_identifier) {

    s_ack_init(packet, AWS_MQTT_PACKET_PUBCOMP, packet_identifier);

    return AWS_OP_SUCCESS;
}

/*****************************************************************************/
/* Subscribe                                                                 */

int aws_mqtt_packet_subscribe_init(
    struct aws_mqtt_packet_subscribe *packet,
    struct aws_allocator *allocator,
    uint16_t packet_identifier) {

    assert(packet);

    AWS_ZERO_STRUCT(*packet);

    packet->fixed_header.packet_type = AWS_MQTT_PACKET_SUBSCRIBE;
    packet->fixed_header.flags = S_BIT_1_FLAGS;
    packet->fixed_header.remaining_length = sizeof(uint16_t);

    packet->packet_identifier = packet_identifier;

    if (aws_array_list_init_dynamic(&packet->topic_filters, allocator, 1, sizeof(struct aws_mqtt_subscription))) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

void aws_mqtt_packet_subscribe_clean_up(struct aws_mqtt_packet_subscribe *packet) {

    assert(packet);

    aws_array_list_clean_up(&packet->topic_filters);

    AWS_ZERO_STRUCT(*packet);
}

int aws_mqtt_packet_subscribe_add_topic(
    struct aws_mqtt_packet_subscribe *packet,
    struct aws_byte_cursor topic_filter,
    enum aws_mqtt_qos qos) {

    assert(packet);

    /* Add to the array list */
    struct aws_mqtt_subscription subscription;
    subscription.topic_filter = topic_filter;
    subscription.qos = qos;
    if (aws_array_list_push_back(&packet->topic_filters, &subscription)) {
        return AWS_OP_ERR;
    }

    /* Add to the remaining length */
    packet->fixed_header.remaining_length += s_sizeof_encoded_buffer(&topic_filter) + 1;

    return AWS_OP_SUCCESS;
}

int aws_mqtt_packet_subscribe_encode(struct aws_byte_cursor *cur, const struct aws_mqtt_packet_subscribe *packet) {

    assert(cur);
    assert(packet);

    /*************************************************************************/
    /* Fixed Header */

    if (aws_mqtt_fixed_header_encode(cur, &packet->fixed_header)) {
        return AWS_OP_ERR;
    }

    /*************************************************************************/
    /* Variable Header                                                       */

    /* Write packet identifier */
    if (!aws_byte_cursor_write_be16(cur, packet->packet_identifier)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    /* Write topic filters */
    const size_t num_filters = aws_array_list_length(&packet->topic_filters);
    for (size_t i = 0; i < num_filters; ++i) {

        struct aws_mqtt_subscription *subscription;
        if (aws_array_list_get_at_ptr(&packet->topic_filters, (void **)&subscription, i)) {

            return AWS_OP_ERR;
        }
        s_encode_buffer(cur, subscription->topic_filter);

        uint8_t eos_byte = subscription->qos & 0x3;
        if (!aws_byte_cursor_write_u8(cur, eos_byte)) {
            return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
        }
    }

    return AWS_OP_SUCCESS;
}

int aws_mqtt_packet_subscribe_decode(struct aws_byte_cursor *cur, struct aws_mqtt_packet_subscribe *packet) {

    assert(cur);
    assert(packet);

    /*************************************************************************/
    /* Fixed Header */

    if (aws_mqtt_fixed_header_decode(cur, &packet->fixed_header)) {
        return AWS_OP_ERR;
    }

    /*************************************************************************/
    /* Variable Header                                                       */

    /* Read packet identifier */
    if (!aws_byte_cursor_read_be16(cur, &packet->packet_identifier)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    /* Read topic filters */
    size_t remaining_length = packet->fixed_header.remaining_length - sizeof(uint16_t);
    while (remaining_length) {

        struct aws_mqtt_subscription subscription = {
            .topic_filter = {.ptr = NULL, .len = 0},
            .qos = 0,
        };
        if (s_decode_buffer(cur, &subscription.topic_filter)) {
            return AWS_OP_ERR;
        }

        uint8_t eos_byte = 0;
        if (!aws_byte_cursor_read_u8(cur, &eos_byte)) {
            return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
        }
        if ((eos_byte >> 2) != 0) {
            return aws_raise_error(INT32_MAX);
        }
        subscription.qos = eos_byte & 0x3;

        aws_array_list_push_back(&packet->topic_filters, &subscription);

        remaining_length -= s_sizeof_encoded_buffer(&subscription.topic_filter) + 1;
    }

    return AWS_OP_SUCCESS;
}

/*****************************************************************************/
/* Suback                                                                    */

int aws_mqtt_packet_suback_init(struct aws_mqtt_packet_ack *packet, uint16_t packet_identifier) {

    s_ack_init(packet, AWS_MQTT_PACKET_SUBACK, packet_identifier);

    return AWS_OP_SUCCESS;
}

/*****************************************************************************/
/* Unubscribe                                                                */

int aws_mqtt_packet_unsubscribe_init(
    struct aws_mqtt_packet_unsubscribe *packet,
    struct aws_allocator *allocator,
    uint16_t packet_identifier) {

    assert(packet);
    assert(allocator);

    AWS_ZERO_STRUCT(*packet);

    packet->fixed_header.packet_type = AWS_MQTT_PACKET_SUBSCRIBE;
    packet->fixed_header.flags = S_BIT_1_FLAGS;
    packet->fixed_header.remaining_length = sizeof(uint16_t);

    packet->packet_identifier = packet_identifier;

    if (aws_array_list_init_dynamic(&packet->topic_filters, allocator, 1, sizeof(struct aws_byte_cursor))) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

void aws_mqtt_packet_unsubscribe_clean_up(struct aws_mqtt_packet_unsubscribe *packet) {

    assert(packet);

    aws_array_list_clean_up(&packet->topic_filters);

    AWS_ZERO_STRUCT(*packet);
}

int aws_mqtt_packet_unsubscribe_add_topic(
    struct aws_mqtt_packet_unsubscribe *packet,
    struct aws_byte_cursor topic_filter) {

    assert(packet);

    /* Add to the array list */
    if (aws_array_list_push_back(&packet->topic_filters, &topic_filter)) {
        return AWS_OP_ERR;
    }

    /* Add to the remaining length */
    packet->fixed_header.remaining_length += s_sizeof_encoded_buffer(&topic_filter);

    return AWS_OP_SUCCESS;
}

int aws_mqtt_packet_unsubscribe_encode(struct aws_byte_cursor *cur, const struct aws_mqtt_packet_unsubscribe *packet) {

    assert(cur);
    assert(packet);

    /*************************************************************************/
    /* Fixed Header */

    if (aws_mqtt_fixed_header_encode(cur, &packet->fixed_header)) {
        return AWS_OP_ERR;
    }

    /*************************************************************************/
    /* Variable Header                                                       */

    /* Write packet identifier */
    if (!aws_byte_cursor_write_be16(cur, packet->packet_identifier)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    /* Write topic filters */
    const size_t num_filters = aws_array_list_length(&packet->topic_filters);
    for (size_t i = 0; i < num_filters; ++i) {

        struct aws_byte_cursor topic_filter;
        if (aws_array_list_get_at(&packet->topic_filters, (void *)&topic_filter, i)) {

            return AWS_OP_ERR;
        }
        s_encode_buffer(cur, topic_filter);
    }

    return AWS_OP_SUCCESS;
}

int aws_mqtt_packet_unsubscribe_decode(struct aws_byte_cursor *cur, struct aws_mqtt_packet_unsubscribe *packet) {

    assert(cur);
    assert(packet);

    /*************************************************************************/
    /* Fixed Header */

    if (aws_mqtt_fixed_header_decode(cur, &packet->fixed_header)) {
        return AWS_OP_ERR;
    }

    /*************************************************************************/
    /* Variable Header                                                       */

    /* Read packet identifier */
    if (!aws_byte_cursor_read_be16(cur, &packet->packet_identifier)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    /* Read topic filters */
    size_t remaining_length = packet->fixed_header.remaining_length - sizeof(uint16_t);
    while (remaining_length) {

        struct aws_byte_cursor topic_filter;
        AWS_ZERO_STRUCT(topic_filter);
        if (s_decode_buffer(cur, &topic_filter)) {
            return AWS_OP_ERR;
        }

        aws_array_list_push_back(&packet->topic_filters, &topic_filter);

        remaining_length -= s_sizeof_encoded_buffer(&topic_filter);
    }

    return AWS_OP_SUCCESS;
}

/*****************************************************************************/
/* Unsuback                                                                  */

int aws_mqtt_packet_unsuback_init(struct aws_mqtt_packet_ack *packet, uint16_t packet_identifier) {

    s_ack_init(packet, AWS_MQTT_PACKET_UNSUBACK, packet_identifier);

    return AWS_OP_SUCCESS;
}

/*****************************************************************************/
/* Ping request/response                                                     */

static void s_connection_init(struct aws_mqtt_packet_connection *packet, enum aws_mqtt_packet_type type) {

    assert(packet);

    AWS_ZERO_STRUCT(*packet);
    packet->fixed_header.packet_type = type;
}

int aws_mqtt_packet_pingreq_init(struct aws_mqtt_packet_connection *packet) {

    s_connection_init(packet, AWS_MQTT_PACKET_PINGREQ);

    return AWS_OP_SUCCESS;
}

int aws_mqtt_packet_pingresp_init(struct aws_mqtt_packet_connection *packet) {

    s_connection_init(packet, AWS_MQTT_PACKET_PINGRESP);

    return AWS_OP_SUCCESS;
}

int aws_mqtt_packet_disconnect_init(struct aws_mqtt_packet_connection *packet) {

    s_connection_init(packet, AWS_MQTT_PACKET_DISCONNECT);

    return AWS_OP_SUCCESS;
}

int aws_mqtt_packet_connection_encode(struct aws_byte_cursor *cur, const struct aws_mqtt_packet_connection *packet) {

    assert(cur);
    assert(packet);

    /*************************************************************************/
    /* Fixed Header */

    if (aws_mqtt_fixed_header_encode(cur, &packet->fixed_header)) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

int aws_mqtt_packet_connection_decode(struct aws_byte_cursor *cur, struct aws_mqtt_packet_connection *packet) {

    assert(cur);
    assert(packet);

    /*************************************************************************/
    /* Fixed Header */

    if (aws_mqtt_fixed_header_decode(cur, &packet->fixed_header)) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}
