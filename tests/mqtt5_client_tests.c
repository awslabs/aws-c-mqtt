/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "mqtt5_testing_utils.h"

#include <aws/common/string.h>
#include <aws/mqtt/mqtt.h>
#include <aws/mqtt/v5/mqtt5_client.h>

#include <aws/testing/aws_test_harness.h>

#define TEST_IO_MESSAGE_LENGTH 4096

static int s_aws_mqtt5_mock_server_send_packet(
    struct aws_mqtt5_server_mock_connection_context *connection,
    enum aws_mqtt5_packet_type packet_type,
    void *packet) {
    aws_mqtt5_encoder_append_packet_encoding(&connection->encoder, packet_type, packet);

    struct aws_io_message *message = aws_channel_acquire_message_from_pool(
        connection->slot->channel, AWS_IO_MESSAGE_APPLICATION_DATA, TEST_IO_MESSAGE_LENGTH);
    if (message == NULL) {
        return AWS_OP_ERR;
    }

    enum aws_mqtt5_encoding_result result =
        aws_mqtt5_encoder_encode_to_buffer(&connection->encoder, &message->message_data);
    AWS_FATAL_ASSERT(result == AWS_MQTT5_ER_FINISHED);

    if (aws_channel_slot_send_message(connection->slot, message, AWS_CHANNEL_DIR_WRITE)) {
        aws_mem_release(message->allocator, message);
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

static int s_aws_mqtt5_mock_server_handle_connect_always_succeed(
    void *packet,
    struct aws_mqtt5_server_mock_connection_context *connection,
    void *user_data) {
    (void)packet;
    (void)user_data;

    struct aws_mqtt5_packet_connack_view connack_view;
    AWS_ZERO_STRUCT(connack_view);

    connack_view.reason_code = AWS_MQTT5_CRC_SUCCESS;

    return s_aws_mqtt5_mock_server_send_packet(connection, AWS_MQTT5_PT_CONNACK, &connack_view);
}

static int s_aws_mqtt5_mock_server_handle_pingreq_always_respond(
    void *packet,
    struct aws_mqtt5_server_mock_connection_context *connection,
    void *user_data) {
    (void)packet;
    (void)user_data;

    return s_aws_mqtt5_mock_server_send_packet(connection, AWS_MQTT5_PT_PINGRESP, NULL);
}

static int s_aws_mqtt5_mock_server_handle_disconnect(
    void *packet,
    struct aws_mqtt5_server_mock_connection_context *connection,
    void *user_data) {
    (void)packet;
    (void)connection;
    (void)user_data;

    return AWS_OP_SUCCESS;
}

void s_lifecycle_event_callback(const struct aws_mqtt5_client_lifecycle_event *event) {
    (void)event;
}

AWS_STATIC_STRING_FROM_LITERAL(s_client_id, "HelloWorld");

static void s_mqtt5_client_test_init_default_options(
    struct aws_mqtt5_packet_connect_view *connect_options,
    struct aws_mqtt5_client_options *client_options,
    struct aws_mqtt5_mock_server_vtable *server_function_table) {
    struct aws_mqtt5_packet_connect_view local_connect_options = {
        .keep_alive_interval_seconds = 30,
        .client_id = aws_byte_cursor_from_string(s_client_id),
        .clean_start = true,
    };

    *connect_options = local_connect_options;

    struct aws_mqtt5_client_options local_client_options = {
        .connect_options = connect_options,
        .session_behavior = AWS_MQTT5_CSBT_CLEAN,
        .outbound_topic_aliasing_behavior = AWS_MQTT5_COTABT_LRU,
        .lifecycle_event_handler = s_lifecycle_event_callback,
        .lifecycle_event_handler_user_data = NULL,
        .max_reconnect_delay_ms = 120000,
        .min_connected_time_to_reset_reconnect_delay_ms = 30000,
        .min_reconnect_delay_ms = 1000,
        .ping_timeout_ms = 10000,
    };

    *client_options = local_client_options;

    struct aws_mqtt5_mock_server_vtable local_server_function_table = {
        .packet_handlers = {
            NULL,                                                   /* RESERVED = 0 */
            &s_aws_mqtt5_mock_server_handle_connect_always_succeed, /* CONNECT */
            NULL,                                                   /* CONNACK */
            NULL,                                                   /* PUBLISH */
            NULL,                                                   /* PUBACK */
            NULL,                                                   /* PUBREC */
            NULL,                                                   /* PUBREL */
            NULL,                                                   /* PUBCOMP */
            NULL,                                                   /* SUBSCRIBE */
            NULL,                                                   /* SUBACK */
            NULL,                                                   /* UNSUBSCRIBE */
            NULL,                                                   /* UNSUBACK */
            &s_aws_mqtt5_mock_server_handle_pingreq_always_respond, /* PINGREQ */
            NULL,                                                   /* PINGRESP */
            &s_aws_mqtt5_mock_server_handle_disconnect,             /* DISCONNECT */
            NULL                                                    /* AUTH */
        }};

    *server_function_table = local_server_function_table;
}

static bool s_last_life_cycle_event_is(
    struct aws_mqtt5_client_mock_test_fixture *test_fixture,
    enum aws_mqtt5_client_lifecycle_event_type event_type) {
    size_t event_count = aws_array_list_length(&test_fixture->lifecycle_events);
    if (event_count == 0) {
        return false;
    }

    struct aws_mqtt5_lifecycle_event_record *record = NULL;
    aws_array_list_get_at(&test_fixture->lifecycle_events, &record, event_count - 1);

    return record->event.event_type == event_type;
}

static bool s_last_lifecycle_event_is_connected(void *arg) {
    struct aws_mqtt5_client_mock_test_fixture *test_fixture = arg;

    return s_last_life_cycle_event_is(test_fixture, AWS_MQTT5_CLET_CONNECTION_SUCCESS);
}

static void s_wait_for_connected_lifecycle_event(struct aws_mqtt5_client_mock_test_fixture *test_context) {
    aws_mutex_lock(&test_context->lock);
    aws_condition_variable_wait_pred(
        &test_context->signal, &test_context->lock, s_last_lifecycle_event_is_connected, test_context);
    aws_mutex_unlock(&test_context->lock);
}

static bool s_last_lifecycle_event_is_stopped(void *arg) {
    struct aws_mqtt5_client_mock_test_fixture *test_fixture = arg;

    return s_last_life_cycle_event_is(test_fixture, AWS_MQTT5_CLET_STOPPED);
}

static void s_wait_for_stopped_lifecycle_event(struct aws_mqtt5_client_mock_test_fixture *test_context) {
    aws_mutex_lock(&test_context->lock);
    aws_condition_variable_wait_pred(
        &test_context->signal, &test_context->lock, s_last_lifecycle_event_is_stopped, test_context);
    aws_mutex_unlock(&test_context->lock);
}

static int s_mqtt5_client_simple_connect_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct aws_mqtt5_packet_connect_view connect_options;
    struct aws_mqtt5_client_options client_options;
    struct aws_mqtt5_mock_server_vtable server_function_table;
    s_mqtt5_client_test_init_default_options(&connect_options, &client_options, &server_function_table);

    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options test_fixture_options = {
        .client_options = &client_options,
        .server_function_table = &server_function_table,
    };

    struct aws_mqtt5_client_mock_test_fixture test_context;
    aws_mqtt5_client_mock_test_fixture_init(&test_context, allocator, &test_fixture_options);

    struct aws_mqtt5_client *client = test_context.client;
    aws_mqtt5_client_start(client);

    s_wait_for_connected_lifecycle_event(&test_context);

    aws_mqtt5_client_stop(client, NULL);

    s_wait_for_stopped_lifecycle_event(&test_context);

    aws_mqtt5_client_mock_test_fixture_clean_up(&test_context);

    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_client_simple_connect, s_mqtt5_client_simple_connect_fn)
