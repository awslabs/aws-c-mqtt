/**
* Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
* SPDX-License-Identifier: Apache-2.0.
*/

#include <aws/testing/aws_test_harness.h>

#include <aws/mqtt/private/fixed_header.h>
#include <aws/mqtt/private/mqtt311_decoder.h>
#include <aws/mqtt/private/packets.h>

struct mqtt_311_decoding_test_context {
    struct aws_allocator *allocator;
    struct aws_mqtt311_decoder decoder;

    void *expected_packet;
    size_t packet_count;
};

static int s_compare_fixed_header(struct aws_mqtt_fixed_header *expected_header, struct aws_mqtt_fixed_header *actual_header) {
    ASSERT_INT_EQUALS(expected_header->packet_type, actual_header->packet_type);
    ASSERT_INT_EQUALS(expected_header->remaining_length, actual_header->remaining_length);
    ASSERT_INT_EQUALS(expected_header->flags, actual_header->flags);

    return AWS_OP_SUCCESS;
}

static int s_decoding_test_handle_publish(struct aws_byte_cursor message_cursor, void *user_data) {

    struct mqtt_311_decoding_test_context *context = user_data;
    (void)context;

    struct aws_mqtt_packet_publish publish;
    if (aws_mqtt_packet_publish_decode(&message_cursor, &publish)) {
        return AWS_OP_ERR;
    }

    struct aws_mqtt_packet_publish *expected_publish = context->expected_packet;

    ASSERT_SUCCESS(s_compare_fixed_header(&expected_publish->fixed_header, &publish.fixed_header));

    ASSERT_INT_EQUALS(expected_publish->packet_identifier, publish.packet_identifier);

    ASSERT_BIN_ARRAYS_EQUALS(expected_publish->topic_name.ptr, expected_publish->topic_name.len, publish.topic_name.ptr, publish.topic_name.len);
    ASSERT_BIN_ARRAYS_EQUALS(expected_publish->payload.ptr, expected_publish->payload.len, publish.payload.ptr, publish.payload.len);

    ++context->packet_count;

    return AWS_OP_SUCCESS;
}

static struct aws_mqtt_client_connection_packet_handlers s_decoding_test_packet_handlers = {
    .handlers_by_packet_type = {
        [AWS_MQTT_PACKET_PUBLISH] = &s_decoding_test_handle_publish,
    }
};

static void s_init_decoding_test_context(struct mqtt_311_decoding_test_context *context, struct aws_allocator *allocator) {
    AWS_ZERO_STRUCT(*context);

    context->allocator = allocator;

    struct aws_mqtt311_decoder_options config = {
        .packet_handlers = &s_decoding_test_packet_handlers,
        .handler_user_data = context,
    };

    aws_mqtt311_decoder_init(&context->decoder, allocator, &config);
}

static void s_clean_up_decoding_test_context(struct mqtt_311_decoding_test_context *context) {

    aws_mqtt311_decoder_clean_up(&context->decoder);
}

#define TEST_ADJACENT_PACKET_COUNT 4

static int s_mqtt_decode_publish_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    /*
     * For completeness, run the test with payload sizes that lead to a remaining length VLI encoding of 1, 2, 3, and
     * 4 bytes.
     */
    size_t payload_sizes[] = {35, 1234, 1 << 16, 1 << 21};

    for (size_t i = 0; i < AWS_ARRAY_SIZE(payload_sizes); ++i) {
        struct mqtt_311_decoding_test_context test_context;
        s_init_decoding_test_context(&test_context, allocator);

        struct aws_mqtt311_decoder *decoder = &test_context.decoder;

        size_t publish_payload_size = payload_sizes[i];
        AWS_VARIABLE_LENGTH_ARRAY(uint8_t, payload_buffer, publish_payload_size);

        struct aws_mqtt_packet_publish publish_packet;
        ASSERT_SUCCESS(aws_mqtt_packet_publish_init(
            &publish_packet,
            true,
            AWS_MQTT_QOS_AT_LEAST_ONCE,
            false,
            aws_byte_cursor_from_c_str("Hello/World"),
            12,
            aws_byte_cursor_from_array(payload_buffer, publish_payload_size)));

        test_context.expected_packet = &publish_packet;

        struct aws_byte_buf encoded_buffer;
        aws_byte_buf_init(&encoded_buffer, allocator, (publish_payload_size + 100) * TEST_ADJACENT_PACKET_COUNT );

        for (size_t j = 0; j < TEST_ADJACENT_PACKET_COUNT; ++j) {
            ASSERT_SUCCESS(aws_mqtt_packet_publish_encode(&encoded_buffer, &publish_packet));
        }

        size_t fragment_lengths[] = {1, 2, 3, 5, 7, 11, 23, 37, 67, 131};

        for (size_t j = 0; j < AWS_ARRAY_SIZE(fragment_lengths); ++j) {
            size_t fragment_length = fragment_lengths[j];

            struct aws_byte_cursor packet_cursor = aws_byte_cursor_from_buf(&encoded_buffer);
            while (packet_cursor.len > 0) {
                size_t advance = aws_min_size(packet_cursor.len, fragment_length);
                struct aws_byte_cursor fragment_cursor = aws_byte_cursor_advance(&packet_cursor, advance);

                ASSERT_SUCCESS(aws_mqtt311_decoder_on_bytes_received(decoder, fragment_cursor));
            }
        }

        ASSERT_INT_EQUALS(4 * AWS_ARRAY_SIZE(fragment_lengths), test_context.packet_count);

        aws_byte_buf_clean_up(&encoded_buffer);

        s_clean_up_decoding_test_context(&test_context);
    }

    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt_decode_publish, s_mqtt_decode_publish_fn)
