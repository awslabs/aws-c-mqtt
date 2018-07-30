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

#include <aws/testing/aws_test_harness.h>

#include <aws/mqtt/private/packets.h>

enum { BUFFER_SIZE = 128 };

struct packet_test_fixture;

/* Function type used to init and cleanup a fixture */
typedef void(packet_init_fn)(struct packet_test_fixture *, struct aws_byte_cursor *);
/* Function used to encode and decode a packet (this should be set to a function from packets.h) */
typedef int(packet_code_fn)(struct aws_byte_cursor *, void *);
/* Function used to check if two packets are equal */
typedef bool(packet_eq_fn)(void *, void *, size_t);

/* Helper for comparing the fixed headers of packets */
static bool mqtt_fixed_header_eq(struct aws_mqtt_fixed_header *l, struct aws_mqtt_fixed_header *r) {

    return l->packet_type == r->packet_type && l->flags == r->flags && l->remaining_length == r->remaining_length;
}

/* Default packet compare function, checks headers then memcmps the rest */
static bool mqtt_packet_eq_default(void *a, void *b, size_t size) {

    static const size_t HEADER_SIZE = sizeof(struct aws_mqtt_fixed_header);

    return mqtt_fixed_header_eq(a, b) &&
           memcmp((uint8_t *)a + HEADER_SIZE, (uint8_t *)b + HEADER_SIZE, size - HEADER_SIZE) == 0;
}

/* Compares the data inside 2 byte cursors */
static bool byte_cursor_eq(struct aws_byte_cursor l, struct aws_byte_cursor r) {

    if (l.len == r.len) {
        if (l.len == 0) {
            return true;
        }

        return memcmp(l.ptr, r.ptr, l.len) == 0;
    }

    return false;
}

/* Contains all of the information required to run a packet's test case */
struct packet_test_fixture {
    enum aws_mqtt_packet_type type;
    size_t size;
    packet_init_fn *init;
    packet_code_fn *encode;
    packet_code_fn *decode;
    packet_init_fn *teardown;
    packet_eq_fn *equal;
    struct aws_allocator *allocator;

    void *in_packet;
    void *out_packet;
    struct aws_byte_buf buffer;
};

static void mqtt_packet_test_before(struct aws_allocator *allocator, void *ctx) {

    struct packet_test_fixture *fixture = ctx;
    fixture->allocator = allocator;

    /* Setup the fixture */
    fixture->in_packet = aws_mem_acquire(allocator, fixture->size);
    assert(fixture->in_packet);
    memset(fixture->in_packet, 0, fixture->size);

    fixture->out_packet = aws_mem_acquire(allocator, fixture->size);
    assert(fixture->out_packet);
    memset(fixture->out_packet, 0, fixture->size);

    aws_byte_buf_init(allocator, &fixture->buffer, BUFFER_SIZE);

    /* Init the in_packet & buffer */
    struct aws_byte_cursor cursor = {
        .ptr = fixture->buffer.buffer,
        .len = fixture->buffer.capacity,
    };
    fixture->init(fixture, &cursor);
    fixture->buffer.len = fixture->buffer.capacity - cursor.len;
}

static int mqtt_packet_test_run(struct aws_allocator *allocator, void *ctx) {

    struct packet_test_fixture *fixture = ctx;

    /* Encode */

    /* Create the output buffer */
    struct aws_byte_buf output_buffer;
    ASSERT_SUCCESS(aws_byte_buf_init(allocator, &output_buffer, BUFFER_SIZE));
    struct aws_byte_cursor output_cursor = {
        .ptr = output_buffer.buffer,
        .len = output_buffer.capacity,
    };

    /* Encode the packet */
    ASSERT_SUCCESS(fixture->encode(&output_cursor, fixture->in_packet));

    output_buffer.len = output_buffer.capacity - output_cursor.len;

    /* Compare the buffers */
    ASSERT_BIN_ARRAYS_EQUALS(fixture->buffer.buffer, fixture->buffer.len, output_buffer.buffer, output_buffer.len);

    aws_byte_buf_clean_up(&output_buffer);

    /* Decode */

    /* Decode the buffer */
    struct aws_byte_cursor cursor = aws_byte_cursor_from_buf(&fixture->buffer);
    ASSERT_SUCCESS(fixture->decode(&cursor, fixture->out_packet));

    /* Compare the packets */
    if (fixture->equal) {
        ASSERT_TRUE(fixture->equal(fixture->out_packet, fixture->in_packet, fixture->size));
    } else {
        ASSERT_TRUE(mqtt_packet_eq_default(fixture->out_packet, fixture->in_packet, fixture->size));
    }

    return AWS_OP_SUCCESS;
}

static void mqtt_packet_test_after(struct aws_allocator *allocator, void *ctx) {

    struct packet_test_fixture *fixture = ctx;

    /* Tear down the packet & buffer */
    if (fixture->teardown) {
        struct aws_byte_cursor cursor = aws_byte_cursor_from_buf(&fixture->buffer);
        fixture->teardown(fixture, &cursor);
    }

    /* Tear down the fixture */
    aws_mem_release(allocator, fixture->in_packet);
    aws_mem_release(allocator, fixture->out_packet);
    aws_byte_buf_clean_up(&fixture->buffer);
}

#define PACKET_TEST_NAME(e_type, t_name, s_name, i, t, e)                                                              \
    static struct packet_test_fixture mqtt_packet_##t_name##_fixture = {                                               \
        .type = AWS_MQTT_PACKET_##e_type,                                                                              \
        .size = sizeof(struct aws_mqtt_packet_##s_name),                                                               \
        .init = (i),                                                                                                   \
        .encode = (packet_code_fn *)&aws_mqtt_packet_##s_name##_encode,                                                \
        .decode = (packet_code_fn *)&aws_mqtt_packet_##s_name##_decode,                                                \
        .teardown = (t),                                                                                               \
        .equal = (e),                                                                                                  \
    };                                                                                                                 \
    AWS_TEST_CASE_FIXTURE(                                                                                             \
        mqtt_packet_##t_name,                                                                                          \
        mqtt_packet_test_before,                                                                                       \
        mqtt_packet_test_run,                                                                                          \
        mqtt_packet_test_after,                                                                                        \
        &mqtt_packet_##t_name##_fixture)

#define PACKET_TEST(e_type, s_name, i, t, e) PACKET_TEST_NAME(e_type, s_name, s_name, i, t, e)

static uint8_t s_client_id[] = "Test Client ID";
enum { CLIEN_ID_LEN = sizeof(s_client_id) };
static uint8_t s_topic_name[] = "test/topic";
enum { TOPIC_NAME_LEN = sizeof(s_topic_name) };
static uint8_t s_payload[] = "This s_payload contains data. It is some good ol' fashioned data.";
enum { PAYLOAD_LEN = sizeof(s_payload) };

/*****************************************************************************/
/* Ack                                                                       */

static void mqtt_test_ack_init(struct packet_test_fixture *fixture, struct aws_byte_cursor *buffer) {

    /* Init buffer */
    uint8_t packet_id = (uint8_t)(fixture->type + 7);

    /* clang-format off */
    uint8_t header[] = {
        (uint8_t)(fixture->type << 4),  /* Packet type */
        2,                              /* Remaining length */
        0, packet_id,                   /* Packet identifier */
    };

    /* Init packet */
    switch (fixture->type) {
        case AWS_MQTT_PACKET_PUBACK:
            aws_mqtt_packet_puback_init(fixture->in_packet, packet_id);
            break;
        case AWS_MQTT_PACKET_PUBREC:
            aws_mqtt_packet_pubrec_init(fixture->in_packet, packet_id);
            break;
        case AWS_MQTT_PACKET_PUBREL:
            aws_mqtt_packet_pubrel_init(fixture->in_packet, packet_id);
            /* if pubrel, bit 1 in flags must be set */
            header[0] |= 0x2;
            break;
        case AWS_MQTT_PACKET_PUBCOMP:
            aws_mqtt_packet_pubcomp_init(fixture->in_packet, packet_id);
            break;
        case AWS_MQTT_PACKET_SUBACK:
            aws_mqtt_packet_suback_init(fixture->in_packet, packet_id);
            break;
        case AWS_MQTT_PACKET_UNSUBACK:
            aws_mqtt_packet_unsuback_init(fixture->in_packet, packet_id);
            break;
        default:
            assert(false);
            break;
    }

    aws_byte_cursor_write(buffer, header, sizeof(header));
}
#define PACKET_TEST_ACK(e_type, name)                   \
    PACKET_TEST_NAME(                                   \
    e_type, name, ack, &mqtt_test_ack_init, NULL, NULL)
PACKET_TEST_ACK(PUBACK, puback)
PACKET_TEST_ACK(PUBREC, pubrec)
PACKET_TEST_ACK(PUBREL, pubrel)
PACKET_TEST_ACK(PUBCOMP, pubcomp)
PACKET_TEST_ACK(SUBACK, suback)
PACKET_TEST_ACK(UNSUBACK, unsuback)
#undef PACKET_TEST_ACK

/*****************************************************************************/
/* Connect                                                                   */

static void mqtt_test_connect_init(
    struct packet_test_fixture *fixture,
    struct aws_byte_cursor *buffer) {

    /* Init packet */
    aws_mqtt_packet_connect_init(
        fixture->in_packet,
        aws_byte_cursor_from_array(s_client_id, CLIEN_ID_LEN),
        false,
        0);

    /* Init buffer */
    /* clang-format off */
    uint8_t header[] = {
        AWS_MQTT_PACKET_CONNECT << 4,   /* Packet type */
        18,                             /* Remaining length */
        0, 4, 'M', 'Q', 'T', 'T',       /* Protocol name */
        4,                              /* Protocol level */
        0,                              /* Connect Flags */
        0, 0,                           /* Keep alive */
    };
    /* clang-format on */

    aws_byte_cursor_write(buffer, header, sizeof(header));
    aws_byte_cursor_write_u8(buffer, 0);
    aws_byte_cursor_write_u8(buffer, CLIEN_ID_LEN);
    aws_byte_cursor_write(buffer, s_client_id, CLIEN_ID_LEN);
}
static bool mqtt_test_connect_eq(void *a, void *b, size_t size) {

    (void)size;

    struct aws_mqtt_packet_connect *l = a;
    struct aws_mqtt_packet_connect *r = b;

    return mqtt_fixed_header_eq(&l->fixed_header, &r->fixed_header) && l->connect_flags.all == r->connect_flags.all &&
           l->keep_alive_timeout == r->keep_alive_timeout &&
           byte_cursor_eq(l->client_identifier, r->client_identifier) && byte_cursor_eq(l->will_topic, r->will_topic) &&
           byte_cursor_eq(l->username, r->username) && byte_cursor_eq(l->password, r->password);
}
PACKET_TEST(CONNECT, connect, &mqtt_test_connect_init, NULL, &mqtt_test_connect_eq)

/*****************************************************************************/
/* Connect                                                                   */

static void mqtt_test_connack_init(struct packet_test_fixture *fixture, struct aws_byte_cursor *buffer) {

    /* Init packet */
    aws_mqtt_packet_connack_init(fixture->in_packet, true, AWS_MQTT_CONNECT_ACCEPTED);

    /* Init buffer */
    /* clang-format off */
    uint8_t header[] = {
        AWS_MQTT_PACKET_CONNACK << 4,   /* Packet type */
        2,                              /* Remaining length */
        1,                              /* Acknowledge flags */
        AWS_MQTT_CONNECT_ACCEPTED,      /* Return code */
    };
    /* clang-format on */

    aws_byte_cursor_write(buffer, header, sizeof(header));
}
PACKET_TEST(CONNACK, connack, &mqtt_test_connack_init, NULL, NULL)

/*****************************************************************************/
/* Publish                                                                   */

static void mqtt_test_publish_init(struct packet_test_fixture *fixture, struct aws_byte_cursor *buffer) {

    /* Init packet */
    aws_mqtt_packet_publish_init(
        fixture->in_packet,
        false,
        AWS_MQTT_QOS_EXACTLY_ONCE,
        false,
        aws_byte_cursor_from_array(s_topic_name, TOPIC_NAME_LEN),
        7,
        aws_byte_cursor_from_array(s_payload, PAYLOAD_LEN));

    /* Init buffer */
    /* clang-format off */
    aws_byte_cursor_write_u8(
        buffer, (AWS_MQTT_PACKET_PUBLISH << 4) | (AWS_MQTT_QOS_EXACTLY_ONCE << 1)); /* Packet type */
    aws_byte_cursor_write_u8(
        buffer, 4 + TOPIC_NAME_LEN + PAYLOAD_LEN); /* Remaining length */
    aws_byte_cursor_write_u8(
        buffer, 0); /* Topic name len byte 1 */
    aws_byte_cursor_write_u8(
        buffer, TOPIC_NAME_LEN); /* Topic name len byte 2 */
    aws_byte_cursor_write(
        buffer, s_topic_name, TOPIC_NAME_LEN); /* Topic name */
    aws_byte_cursor_write_u8(
        buffer, 0);       /* Packet id byte 1 */
    aws_byte_cursor_write_u8(
        buffer, 7);       /* Packet id byte 2 */
    aws_byte_cursor_write(
        buffer, s_payload, PAYLOAD_LEN); /* payload */
    /* clang-format on */
}
static bool mqtt_test_publish_eq(void *a, void *b, size_t size) {

    (void)size;

    struct aws_mqtt_packet_publish *l = a;
    struct aws_mqtt_packet_publish *r = b;

    return mqtt_fixed_header_eq(&l->fixed_header, &r->fixed_header) && l->packet_identifier == r->packet_identifier &&
           byte_cursor_eq(l->topic_name, r->topic_name) && byte_cursor_eq(l->payload, r->payload);
}
PACKET_TEST(PUBLISH, publish, &mqtt_test_publish_init, NULL, &mqtt_test_publish_eq)

/*****************************************************************************/
/* Subscribe                                                                 */

static void mqtt_test_subscribe_init(struct packet_test_fixture *fixture, struct aws_byte_cursor *buffer) {

    /* Init packets */
    aws_mqtt_packet_subscribe_init(fixture->in_packet, fixture->allocator, 7);
    aws_mqtt_packet_subscribe_init(fixture->out_packet, fixture->allocator, 0);

    aws_mqtt_packet_subscribe_add_topic(
        fixture->in_packet, aws_byte_cursor_from_array(s_topic_name, TOPIC_NAME_LEN), AWS_MQTT_QOS_EXACTLY_ONCE);

    /* Init buffer */ /* clang-format off */
    aws_byte_cursor_write_u8(
        buffer, (AWS_MQTT_PACKET_SUBSCRIBE << 4) | 0x2); /* Packet type & flags */
    aws_byte_cursor_write_u8(
        buffer, 2 + TOPIC_NAME_LEN + 1); /* Remaining length */
    aws_byte_cursor_write_u8(
        buffer, 0);
    aws_byte_cursor_write_u8(
        buffer, 7);
    aws_byte_cursor_write_u8(
        buffer, 0);
    aws_byte_cursor_write_u8(
        buffer, TOPIC_NAME_LEN);
    aws_byte_cursor_write(
        buffer, s_topic_name, TOPIC_NAME_LEN);
    aws_byte_cursor_write_u8(
        buffer, AWS_MQTT_QOS_EXACTLY_ONCE);
    /* clang-format on */
}
static void mqtt_test_subscribe_clean_up(struct packet_test_fixture *fixture, struct aws_byte_cursor *buffer) {

    (void)buffer;

    aws_mqtt_packet_subscribe_clean_up(fixture->in_packet);
    aws_mqtt_packet_subscribe_clean_up(fixture->out_packet);
}
static bool mqtt_test_subscribe_eq(void *a, void *b, size_t size) {

    (void)size;

    struct aws_mqtt_packet_subscribe *l = a;
    struct aws_mqtt_packet_subscribe *r = b;

    if (!mqtt_fixed_header_eq(&l->fixed_header, &r->fixed_header) || l->packet_identifier != r->packet_identifier) {
        return false;
    }

    const size_t length = aws_array_list_length(&l->topic_filters);
    if (length != aws_array_list_length(&r->topic_filters)) {
        return false;
    }

    for (size_t i = 0; i < length; ++i) {
        struct aws_mqtt_subscription *lt;
        aws_array_list_get_at_ptr(&l->topic_filters, (void **)&lt, i);
        struct aws_mqtt_subscription *rt;
        aws_array_list_get_at_ptr(&r->topic_filters, (void **)&rt, i);

        if (lt->qos != rt->qos) {
            return false;
        }
        if (!byte_cursor_eq(lt->filter, rt->filter)) {
            return false;
        }
    }

    return true;
}
PACKET_TEST(SUBSCRIBE, subscribe, &mqtt_test_subscribe_init, &mqtt_test_subscribe_clean_up, &mqtt_test_subscribe_eq)

/*****************************************************************************/
/* Unsubscribe                                                               */

static void mqtt_test_unsubscribe_init(struct packet_test_fixture *fixture, struct aws_byte_cursor *buffer) {

    /* Init packet */
    aws_mqtt_packet_unsubscribe_init(fixture->in_packet, fixture->allocator, 7);
    aws_mqtt_packet_unsubscribe_init(fixture->out_packet, fixture->allocator, 0);

    aws_mqtt_packet_unsubscribe_add_topic(fixture->in_packet, aws_byte_cursor_from_array(s_topic_name, TOPIC_NAME_LEN));

    /* Init buffer */
    /* clang-format off */
    aws_byte_cursor_write_u8(
        buffer, (AWS_MQTT_PACKET_SUBSCRIBE << 4) | 0x2); /* Packet type & flags */
    aws_byte_cursor_write_u8(
        buffer, 2 + TOPIC_NAME_LEN); /* Remaining length */
    aws_byte_cursor_write_u8(
        buffer, 0);
    aws_byte_cursor_write_u8(
        buffer, 7);
    aws_byte_cursor_write_u8(
        buffer, 0);
    aws_byte_cursor_write_u8(
        buffer, TOPIC_NAME_LEN);
    aws_byte_cursor_write(
        buffer, s_topic_name, TOPIC_NAME_LEN);
    /* clang-format on */
}
static void mqtt_test_unsubscribe_clean_up(struct packet_test_fixture *fixture, struct aws_byte_cursor *buffer) {

    (void)buffer;

    aws_mqtt_packet_unsubscribe_clean_up(fixture->in_packet);
    aws_mqtt_packet_unsubscribe_clean_up(fixture->out_packet);
}
static bool mqtt_test_unsubscribe_eq(void *a, void *b, size_t size) {

    (void)size;

    struct aws_mqtt_packet_unsubscribe *l = a;
    struct aws_mqtt_packet_unsubscribe *r = b;

    if (!mqtt_fixed_header_eq(&l->fixed_header, &r->fixed_header) || l->packet_identifier != r->packet_identifier) {
        return false;
    }

    const size_t length = aws_array_list_length(&l->topic_filters);
    if (length != aws_array_list_length(&r->topic_filters)) {
        return false;
    }

    for (size_t i = 0; i < length; ++i) {
        struct aws_byte_cursor *lt;
        aws_array_list_get_at_ptr(&l->topic_filters, (void **)&lt, i);
        struct aws_byte_cursor *rt;
        aws_array_list_get_at_ptr(&r->topic_filters, (void **)&rt, i);

        if (!byte_cursor_eq(*lt, *rt)) {
            return false;
        }
    }

    return true;
}
PACKET_TEST(
    UNSUBSCRIBE,
    unsubscribe,
    &mqtt_test_unsubscribe_init,
    &mqtt_test_unsubscribe_clean_up,
    &mqtt_test_unsubscribe_eq)

/*****************************************************************************/
/* Connection                                                                */

static void mqtt_test_connection_init(struct packet_test_fixture *fixture, struct aws_byte_cursor *buffer) {

    /* Init packet */
    switch (fixture->type) {
        case AWS_MQTT_PACKET_PINGREQ:
            aws_mqtt_packet_pingreq_init(fixture->in_packet);
            break;
        case AWS_MQTT_PACKET_PINGRESP:
            aws_mqtt_packet_pingresp_init(fixture->in_packet);
            break;
        case AWS_MQTT_PACKET_DISCONNECT:
            aws_mqtt_packet_disconnect_init(fixture->in_packet);
            break;
        default:
            assert(false);
            break;
    }

    /* Init buffer */
    /* clang-format off */
    uint8_t header[] = {
        (uint8_t)(fixture->type << 4),  /* Packet type */
        0,                              /* Remaining length */
    };
    /* clang-format on */

    aws_byte_cursor_write(buffer, header, sizeof(header));
}
#define PACKET_TEST_CONNETION(e_type, name)                                                                            \
    PACKET_TEST_NAME(e_type, name, connection, &mqtt_test_connection_init, NULL, NULL)
PACKET_TEST_CONNETION(PINGREQ, pingreq)
PACKET_TEST_CONNETION(PINGRESP, pingresp)
PACKET_TEST_CONNETION(DISCONNECT, disconnect)
#undef PACKET_TEST_CONNETION
