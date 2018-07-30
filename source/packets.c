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

enum { PROTOCOL_LEVEL = 4 };
enum { PROTOCOL_NAME_LEN = 4 };
static uint8_t s_protocol_name[PROTOCOL_NAME_LEN] = "MQTT";
enum { BIT_1_FLAGS = 0x2 };

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

static void s_ack_init(
    struct aws_mqtt_packet_ack *packet,
    enum aws_mqtt_packet_type type,
    uint16_t packet_identifier) {

    assert(packet);

    AWS_ZERO_STRUCT(*packet);

    packet->fixed_header.packet_type = type;
    packet->fixed_header.remaining_length = sizeof(uint16_t);

    packet->packet_identifier = packet_identifier;
}

int aws_mqtt_packet_ack_encode(struct aws_byte_cursor *cur, struct aws_mqtt_packet_ack *packet) {

    assert(cur);
    assert(packet);

    /*************************************************************************/
    /* Fixed Header */

    if (aws_mqtt_encode_fixed_header(cur, &packet->fixed_header)) {
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

    if (aws_mqtt_decode_fixed_header(cur, &packet->fixed_header)) {
        return AWS_OP_ERR;
    }

    /* Validate flags */
    struct packet_traits traits = aws_mqtt_get_packet_type_traits(&packet->fixed_header);
    if (packet->fixed_header.flags != (traits.has_flags ? BIT_1_FLAGS : 0)) {

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

void aws_mqtt_packet_connect_init(
    struct aws_mqtt_packet_connect *packet,
    struct aws_byte_cursor client_identifier,
    bool clean_session,
    uint16_t keep_alive) {

    assert(packet);
    assert(client_identifier.len > 0);

    AWS_ZERO_STRUCT(*packet);

    packet->fixed_header.packet_type = AWS_MQTT_PACKET_CONNECT;
    packet->fixed_header.remaining_length =
        1 + sizeof(packet->keep_alive_timeout) + client_identifier.len;

    packet->client_identifier = client_identifier;
    packet->connect_flags.flags.clean_session = clean_session;
    packet->keep_alive_timeout = keep_alive;
}

void aws_mqtt_packet_connect_add_credentials(
    struct aws_mqtt_packet_connect *packet,
    struct aws_byte_cursor username,
    struct aws_byte_cursor password) {

    assert(packet);
    assert(username.len > 0);

    /* Add change in size to remaining_length */
    packet->fixed_header.remaining_length += username.len - packet->username.len;
    packet->connect_flags.flags.has_username = true;

    packet->username = username;

    if (password.len > 0) {

        /* Add change in size to remaining_length */
        packet->fixed_header.remaining_length += password.len - packet->password.len;
        packet->connect_flags.flags.has_password = true;

        packet->password = password;
    }
}

int aws_mqtt_packet_connect_encode(struct aws_byte_cursor *cur, struct aws_mqtt_packet_connect *packet) {

    assert(cur);
    assert(packet);

    /* Do validation */
    if (packet->connect_flags.flags.has_password && !packet->connect_flags.flags.has_username) {

        return aws_raise_error(INT32_MAX);
    }

    /*************************************************************************/
    /* Fixed Header */

    if (aws_mqtt_encode_fixed_header(cur, &packet->fixed_header)) {
        return AWS_OP_ERR;
    }

    /*************************************************************************/
    /* Variable Header                                                       */

    /* Write protocol name */
    if (!aws_byte_cursor_write_be16(cur, PROTOCOL_NAME_LEN)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }
    if (!aws_byte_cursor_write(cur, s_protocol_name, PROTOCOL_NAME_LEN)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    /* Write protocol level */
    if (!aws_byte_cursor_write_u8(cur, PROTOCOL_LEVEL)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    /* Write connect flags */
    if (!aws_byte_cursor_write_u8(cur, packet->connect_flags.all)) {
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
    if (packet->connect_flags.flags.has_will) {
        if (s_encode_buffer(cur, packet->will_topic)) {
            return AWS_OP_ERR;
        }
        if (s_encode_buffer(cur, packet->will_message)) {
            return AWS_OP_ERR;
        }
    }

    /* Write username */
    if (packet->connect_flags.flags.has_username) {
        if (s_encode_buffer(cur, packet->username)) {
            return AWS_OP_ERR;
        }
    }

    /* Write password */
    if (packet->connect_flags.flags.has_password) {
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

    if (aws_mqtt_decode_fixed_header(cur, &packet->fixed_header)) {
        return AWS_OP_ERR;
    }

    /*************************************************************************/
    /* Variable Header                                                       */

    /* Check protocol name length */
    uint16_t protocol_name_len;
    if (!aws_byte_cursor_read_be16(cur, &protocol_name_len)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }
    if (protocol_name_len != PROTOCOL_NAME_LEN) {
        return aws_raise_error(INT32_MAX);
    }

    /* Check protocol name */
    struct aws_byte_cursor protocol_name = aws_byte_cursor_advance(cur, PROTOCOL_NAME_LEN);
    if (protocol_name.len == 0) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }
    if (memcmp(protocol_name.ptr, s_protocol_name, PROTOCOL_NAME_LEN) != 0) {
        return aws_raise_error(INT32_MAX);
    }

    /* Check protocol level */
    struct aws_byte_cursor protocol_level = aws_byte_cursor_advance(cur, 1);
    if (protocol_level.len == 0) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }
    if (*protocol_level.ptr != PROTOCOL_LEVEL) {
        return aws_raise_error(INT32_MAX);
    }

    /* Read connect flags */
    if (!aws_byte_cursor_read_u8(cur, &packet->connect_flags.all)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

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
    if (packet->connect_flags.flags.has_will) {
        if (s_decode_buffer(cur, &packet->will_topic)) {
            return AWS_OP_ERR;
        }
        if (s_decode_buffer(cur, &packet->will_message)) {
            return AWS_OP_ERR;
        }
    }

    /* Read username */
    if (packet->connect_flags.flags.has_username) {
        if (s_decode_buffer(cur, &packet->username)) {
            return AWS_OP_ERR;
        }
    }

    /* Read password */
    if (packet->connect_flags.flags.has_password) {
        if (s_decode_buffer(cur, &packet->password)) {
            return AWS_OP_ERR;
        }
    }

    /* Do validation */
    if (packet->connect_flags.flags.has_password && !packet->connect_flags.flags.has_username) {

        return aws_raise_error(INT32_MAX);
    }

    return AWS_OP_SUCCESS;
}

/*****************************************************************************/
/* Connack                                                                   */

void aws_mqtt_packet_connack_init(
    struct aws_mqtt_packet_connack *packet,
    bool session_present,
    enum aws_mqtt_connect_return_code return_code) {

    assert(packet);

    AWS_ZERO_STRUCT(*packet);

    packet->fixed_header.packet_type = AWS_MQTT_PACKET_CONNACK;
    packet->fixed_header.remaining_length = sizeof(packet->connack_flags) + sizeof(packet->connect_return_code);

    packet->connack_flags.flags.session_present = session_present;
    packet->connect_return_code = return_code;
}

int aws_mqtt_packet_connack_encode(struct aws_byte_cursor *cur, struct aws_mqtt_packet_connack *packet) {

    assert(cur);
    assert(packet);

    /*************************************************************************/
    /* Fixed Header */

    if (aws_mqtt_encode_fixed_header(cur, &packet->fixed_header)) {
        return AWS_OP_ERR;
    }

    /*************************************************************************/
    /* Variable Header                                                       */

    /* Read connack flags */
    if (!aws_byte_cursor_write_u8(cur, packet->connack_flags.all)) {
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

    if (aws_mqtt_decode_fixed_header(cur, &packet->fixed_header)) {
        return AWS_OP_ERR;
    }

    /*************************************************************************/
    /* Variable Header                                                       */

    /* Read connack flags */
    if (!aws_byte_cursor_read_u8(cur, &packet->connack_flags.all)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    /* Read return code */
    if (!aws_byte_cursor_read_u8(cur, &packet->connect_return_code)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    return AWS_OP_SUCCESS;
}

/*****************************************************************************/
/* Publish                                                                   */

void aws_mqtt_packet_publish_init(
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

    union aws_mqtt_fixed_header_flags *flags = (union aws_mqtt_fixed_header_flags *)&packet->fixed_header;
    flags->publish.retain = retain;
    flags->publish.qos = qos;
    flags->publish.dup = dup;

    packet->topic_name = topic_name;
    packet->packet_identifier = packet_identifier;
    packet->payload = payload;
}

int aws_mqtt_packet_publish_encode(struct aws_byte_cursor *cur, struct aws_mqtt_packet_publish *packet) {

    assert(cur);
    assert(packet);

    /*************************************************************************/
    /* Fixed Header */

    if (aws_mqtt_encode_fixed_header(cur, &packet->fixed_header)) {
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

    if (aws_mqtt_decode_fixed_header(cur, &packet->fixed_header)) {
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

    uint32_t payload_size = packet->fixed_header.remaining_length - s_sizeof_encoded_buffer(&packet->topic_name) -
                            sizeof(packet->packet_identifier);
    packet->payload = aws_byte_cursor_advance(cur, payload_size);
    if (packet->payload.len == 0) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    return AWS_OP_SUCCESS;
}

/*****************************************************************************/
/* Puback                                                                    */

void aws_mqtt_packet_puback_init(struct aws_mqtt_packet_ack *packet, uint16_t packet_identifier) {

    s_ack_init(packet, AWS_MQTT_PACKET_PUBACK, packet_identifier);
}

/*****************************************************************************/
/* Pubrec                                                                    */

void aws_mqtt_packet_pubrec_init(struct aws_mqtt_packet_ack *packet, uint16_t packet_identifier) {

    s_ack_init(packet, AWS_MQTT_PACKET_PUBREC, packet_identifier);
}

/*****************************************************************************/
/* Pubrel                                                                    */

void aws_mqtt_packet_pubrel_init(struct aws_mqtt_packet_ack *packet, uint16_t packet_identifier) {

    s_ack_init(packet, AWS_MQTT_PACKET_PUBREL, packet_identifier);
    packet->fixed_header.flags = BIT_1_FLAGS;
}

/*****************************************************************************/
/* Pubcomp                                                                   */

void aws_mqtt_packet_pubcomp_init(struct aws_mqtt_packet_ack *packet, uint16_t packet_identifier) {

    s_ack_init(packet, AWS_MQTT_PACKET_PUBCOMP, packet_identifier);
}

/*****************************************************************************/
/* Subscribe                                                                 */

void aws_mqtt_packet_subscribe_init(
    struct aws_mqtt_packet_subscribe *packet,
    struct aws_allocator *allocator,
    uint16_t packet_identifier) {

    assert(packet);

    AWS_ZERO_STRUCT(*packet);

    packet->fixed_header.packet_type = AWS_MQTT_PACKET_SUBSCRIBE;
    packet->fixed_header.flags = BIT_1_FLAGS;
    packet->fixed_header.remaining_length = sizeof(uint16_t);

    packet->packet_identifier = packet_identifier;

    aws_array_list_init_dynamic(&packet->topic_filters, allocator, 1, sizeof(struct aws_mqtt_subscription));
}

void aws_mqtt_packet_subscribe_clean_up(struct aws_mqtt_packet_subscribe *packet) {

    assert(packet);

    aws_array_list_clean_up(&packet->topic_filters);

    AWS_ZERO_STRUCT(*packet);
}

void aws_mqtt_packet_subscribe_add_topic(
    struct aws_mqtt_packet_subscribe *packet,
    struct aws_byte_cursor topic_filter,
    enum aws_mqtt_qos qos) {

    assert(packet);

    /* Add to the remaining length */
    packet->fixed_header.remaining_length += topic_filter.len + 1;

    /* Add to the array list */
    struct aws_mqtt_subscription subscription = {
        subscription.filter = topic_filter,
        subscription.qos = qos,
    };
    aws_array_list_push_back(&packet->topic_filters, &subscription);
}

/* For reading/writing qos bits */
typedef union s_subscribe_eos {
    struct {
        uint8_t qos : 2;
        uint8_t reserved : 6;
    } flags;
    uint8_t byte;
} s_subscribe_eos_t;

int aws_mqtt_packet_subscribe_encode(struct aws_byte_cursor *cur, struct aws_mqtt_packet_subscribe *packet) {

    assert(cur);
    assert(packet);

    /*************************************************************************/
    /* Fixed Header */

    if (aws_mqtt_encode_fixed_header(cur, &packet->fixed_header)) {
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
        s_encode_buffer(cur, subscription->filter);

        s_subscribe_eos_t qos;
        AWS_ZERO_STRUCT(qos);
        qos.flags.qos = subscription->qos;
        if (!aws_byte_cursor_write_u8(cur, qos.byte)) {
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

    if (aws_mqtt_decode_fixed_header(cur, &packet->fixed_header)) {
        return AWS_OP_ERR;
    }

    /*************************************************************************/
    /* Variable Header                                                       */

    /* Read packet identifier */
    if (!aws_byte_cursor_read_be16(cur, &packet->packet_identifier)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    /* Read topic filters */
    uint32_t remaining_length = packet->fixed_header.remaining_length - sizeof(uint16_t);
    while (remaining_length) {

        struct aws_mqtt_subscription subscription = {
            .filter = {.ptr = NULL, .len = 0},
            .qos = 0,
        };
        if (s_decode_buffer(cur, &subscription.filter)) {
            return AWS_OP_ERR;
        }

        s_subscribe_eos_t qos;
        AWS_ZERO_STRUCT(qos);
        if (!aws_byte_cursor_read_u8(cur, &qos.byte)) {
            return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
        }
        if (qos.flags.reserved != 0) {
            return aws_raise_error(INT32_MAX);
        }
        subscription.qos = qos.flags.qos;

        aws_array_list_push_back(&packet->topic_filters, &subscription);

        remaining_length -= subscription.filter.len + 1;
    }

    return AWS_OP_SUCCESS;
}

/*****************************************************************************/
/* Suback                                                                    */

void aws_mqtt_packet_suback_init(struct aws_mqtt_packet_ack *packet, uint16_t packet_identifier) {

    s_ack_init(packet, AWS_MQTT_PACKET_SUBACK, packet_identifier);
}

/*****************************************************************************/
/* Unubscribe                                                                */

void aws_mqtt_packet_unsubscribe_init(
    struct aws_mqtt_packet_unsubscribe *packet,
    struct aws_allocator *allocator,
    uint16_t packet_identifier) {

    assert(packet);
    assert(allocator);

    AWS_ZERO_STRUCT(*packet);

    packet->fixed_header.packet_type = AWS_MQTT_PACKET_SUBSCRIBE;
    packet->fixed_header.flags = BIT_1_FLAGS;
    packet->fixed_header.remaining_length = sizeof(uint16_t);

    packet->packet_identifier = packet_identifier;

    aws_array_list_init_dynamic(&packet->topic_filters, allocator, 1, sizeof(struct aws_byte_cursor));
}

void aws_mqtt_packet_unsubscribe_clean_up(struct aws_mqtt_packet_unsubscribe *packet) {

    assert(packet);

    aws_array_list_clean_up(&packet->topic_filters);

    AWS_ZERO_STRUCT(*packet);
}

void aws_mqtt_packet_unsubscribe_add_topic(
    struct aws_mqtt_packet_unsubscribe *packet,
    struct aws_byte_cursor topic_filter) {

    assert(packet);

    /* Add to the remaining length */
    packet->fixed_header.remaining_length += topic_filter.len;

    /* Add to the array list */
    aws_array_list_push_back(&packet->topic_filters, &topic_filter);
}

int aws_mqtt_packet_unsubscribe_encode(struct aws_byte_cursor *cur, struct aws_mqtt_packet_unsubscribe *packet) {

    assert(cur);
    assert(packet);

    /*************************************************************************/
    /* Fixed Header */

    if (aws_mqtt_encode_fixed_header(cur, &packet->fixed_header)) {
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

        struct aws_byte_cursor filter;
        if (aws_array_list_get_at(&packet->topic_filters, (void *)&filter, i)) {

            return AWS_OP_ERR;
        }
        s_encode_buffer(cur, filter);
    }

    return AWS_OP_SUCCESS;
}

int aws_mqtt_packet_unsubscribe_decode(struct aws_byte_cursor *cur, struct aws_mqtt_packet_unsubscribe *packet) {

    assert(cur);
    assert(packet);

    /*************************************************************************/
    /* Fixed Header */

    if (aws_mqtt_decode_fixed_header(cur, &packet->fixed_header)) {
        return AWS_OP_ERR;
    }

    /*************************************************************************/
    /* Variable Header                                                       */

    /* Read packet identifier */
    if (!aws_byte_cursor_read_be16(cur, &packet->packet_identifier)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    /* Read topic filters */
    uint32_t remaining_length = packet->fixed_header.remaining_length - sizeof(uint16_t);
    while (remaining_length) {

        struct aws_byte_cursor filter;
        AWS_ZERO_STRUCT(filter);
        if (s_decode_buffer(cur, &filter)) {
            return AWS_OP_ERR;
        }

        aws_array_list_push_back(&packet->topic_filters, &filter);

        remaining_length -= filter.len;
    }

    return AWS_OP_SUCCESS;
}

/*****************************************************************************/
/* Unsuback                                                                  */

void aws_mqtt_packet_unsuback_init(struct aws_mqtt_packet_ack *packet, uint16_t packet_identifier) {

    s_ack_init(packet, AWS_MQTT_PACKET_UNSUBACK, packet_identifier);
}

/*****************************************************************************/
/* Ping request/response                                                     */

static void s_connection_init(struct aws_mqtt_packet_connection *packet, enum aws_mqtt_packet_type type) {

    assert(packet);

    AWS_ZERO_STRUCT(*packet);
    packet->fixed_header.packet_type = type;
}

void aws_mqtt_packet_pingreq_init(struct aws_mqtt_packet_connection *packet) {

    s_connection_init(packet, AWS_MQTT_PACKET_PINGREQ);
}

void aws_mqtt_packet_pingresp_init(struct aws_mqtt_packet_connection *packet) {

    s_connection_init(packet, AWS_MQTT_PACKET_PINGRESP);
}

void aws_mqtt_packet_disconnect_init(struct aws_mqtt_packet_connection *packet) {

    s_connection_init(packet, AWS_MQTT_PACKET_DISCONNECT);
}

int aws_mqtt_packet_connection_encode(struct aws_byte_cursor *cur, struct aws_mqtt_packet_connection *packet) {

    assert(cur);
    assert(packet);

    /*************************************************************************/
    /* Fixed Header */

    if (aws_mqtt_encode_fixed_header(cur, &packet->fixed_header)) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

int aws_mqtt_packet_connection_decode(struct aws_byte_cursor *cur, struct aws_mqtt_packet_connection *packet) {

    assert(cur);
    assert(packet);

    /*************************************************************************/
    /* Fixed Header */

    if (aws_mqtt_decode_fixed_header(cur, &packet->fixed_header)) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}
