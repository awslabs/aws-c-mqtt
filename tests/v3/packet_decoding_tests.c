/**
* Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
* SPDX-License-Identifier: Apache-2.0.
*/

#include <aws/testing/aws_test_harness.h>

#include <aws/io/channel_bootstrap.h>
#include <aws/io/socket.h>
#include <aws/mqtt/client.h>
#include <aws/mqtt/private/client_impl.h>

struct mqtt_311_decoding_test_context {
    struct aws_allocator *allocator;
    struct aws_client_bootstrap *client_bootstrap;
    struct aws_event_loop_group *el_group;
    struct aws_host_resolver *host_resolver;
    struct aws_mqtt_client *mqtt_client;
    struct aws_mqtt_client_connection *mqtt_connection;
    struct aws_socket_options socket_options;

    void *expected_packet;
    size_t packet_count;

    struct aws_mqtt_client_connection_packet_handlers test_handlers;
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

static void s_init_decoding_test_context(struct mqtt_311_decoding_test_context *context, struct aws_allocator *allocator) {
    AWS_ZERO_STRUCT(*context);

    aws_mqtt_library_init(allocator);

    context->allocator = allocator;

    struct aws_socket_options socket_options = {
        .connect_timeout_ms = 1000,
        .domain = AWS_SOCKET_LOCAL,
    };

    context->socket_options = socket_options;
    context->el_group = aws_event_loop_group_new_default(allocator, 1, NULL);
    struct aws_host_resolver_default_options resolver_options = {
        .el_group = context->el_group,
        .max_entries = 1,
    };
    context->host_resolver = aws_host_resolver_new_default(allocator, &resolver_options);

    struct aws_client_bootstrap_options bootstrap_options = {
        .event_loop_group = context->el_group,
        .user_data = context,
        .host_resolver = context->host_resolver,
    };

    context->client_bootstrap = aws_client_bootstrap_new(allocator, &bootstrap_options);

    context->mqtt_client = aws_mqtt_client_new(allocator, context->client_bootstrap);
    context->mqtt_connection = aws_mqtt_client_connection_new(context->mqtt_client);

    struct aws_mqtt_client_connection_311_impl *connection = context->mqtt_connection->impl;
    connection->synced_data.state = AWS_MQTT_CLIENT_STATE_CONNECTED;

    context->test_handlers.handlers_by_packet_type[AWS_MQTT_PACKET_PUBLISH] = s_decoding_test_handle_publish;

    connection->thread_data.decoder.packet_handlers = &context->test_handlers;
}

static void s_clean_up_decoding_test_context(struct mqtt_311_decoding_test_context *context) {
    struct aws_mqtt_client_connection_311_impl *connection = context->mqtt_connection->impl;
    connection->synced_data.state = AWS_MQTT_CLIENT_STATE_DISCONNECTED;

    aws_mqtt_client_connection_release(context->mqtt_connection);
    aws_mqtt_client_release(context->mqtt_client);

    aws_client_bootstrap_release(context->client_bootstrap);
    aws_host_resolver_release(context->host_resolver);
    aws_event_loop_group_release(context->el_group);

    aws_thread_join_all_managed();

    aws_mqtt_library_clean_up();
}

static int s_mqtt_decode_publish_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct mqtt_311_decoding_test_context test_context;
    s_init_decoding_test_context(&test_context, allocator);

    struct aws_mqtt_client_connection_311_impl *connection = test_context.mqtt_connection->impl;
    struct aws_mqtt311_decoder *decoder = &connection->thread_data.decoder;

    struct aws_mqtt_packet_publish publish_packet;
    ASSERT_SUCCESS(aws_mqtt_packet_publish_init(&publish_packet, true, AWS_MQTT_QOS_AT_LEAST_ONCE, false, aws_byte_cursor_from_c_str("Hello/World"), 12, aws_byte_cursor_from_c_str("Payyyyyyyyyyyyload")));

    test_context.expected_packet = &publish_packet;
    
    struct aws_byte_buf encoded_buffer;
    aws_byte_buf_init(&encoded_buffer, allocator, 16384);

    ASSERT_SUCCESS(aws_mqtt_packet_publish_encode(&encoded_buffer, &publish_packet));
    ASSERT_SUCCESS(aws_mqtt_packet_publish_encode(&encoded_buffer, &publish_packet));
    ASSERT_SUCCESS(aws_mqtt_packet_publish_encode(&encoded_buffer, &publish_packet));
    ASSERT_SUCCESS(aws_mqtt_packet_publish_encode(&encoded_buffer, &publish_packet));

    size_t fragment_lengths[] = { 1, 2, 3, 5, 7, 11, 23, 37, 67, 128};

    for (size_t i = 0; i < AWS_ARRAY_SIZE(fragment_lengths); ++i) {
        size_t fragment_length = fragment_lengths[i];

        struct aws_byte_cursor packet_cursor = aws_byte_cursor_from_buf(&encoded_buffer);
        while (packet_cursor.len > 0) {
            size_t advance = aws_min_size(packet_cursor.len, fragment_length);
            struct aws_byte_cursor fragment_cursor = aws_byte_cursor_advance(&packet_cursor, advance);

            ASSERT_SUCCESS(aws_mqtt311_decoder_on_bytes_received(decoder, fragment_cursor, connection));
        }
    }

    ASSERT_INT_EQUALS(4 * AWS_ARRAY_SIZE(fragment_lengths), test_context.packet_count);

    s_clean_up_decoding_test_context(&test_context);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt_decode_publish, s_mqtt_decode_publish_fn)
