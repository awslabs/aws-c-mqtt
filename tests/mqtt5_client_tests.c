/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "mqtt5_testing_utils.h"

#include <aws/common/string.h>
#include <aws/http/websocket.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
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

static int s_aws_mqtt5_client_test_init_default_connect_storage(
    struct aws_mqtt5_packet_connect_storage *storage,
    struct aws_allocator *allocator) {
    struct aws_mqtt5_packet_connect_view connect_view = {
        .keep_alive_interval_seconds = 30,
        .client_id = aws_byte_cursor_from_string(s_client_id),
        .clean_start = true,
    };

    return aws_mqtt5_packet_connect_storage_init(storage, allocator, &connect_view);
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

static bool s_has_lifecycle_event(
    struct aws_mqtt5_client_mock_test_fixture *test_fixture,
    enum aws_mqtt5_client_lifecycle_event_type event_type) {
    size_t event_count = aws_array_list_length(&test_fixture->lifecycle_events);
    if (event_count == 0) {
        return false;
    }

    size_t record_count = aws_array_list_length(&test_fixture->lifecycle_events);
    for (size_t i = 0; i < record_count; ++i) {
        struct aws_mqtt5_lifecycle_event_record *record = NULL;
        aws_array_list_get_at(&test_fixture->lifecycle_events, &record, i);
        if (record->event.event_type == event_type) {
            return true;
        }
    }

    return false;
}

static bool s_has_connection_failure_event(void *arg) {
    struct aws_mqtt5_client_mock_test_fixture *test_fixture = arg;

    return s_has_lifecycle_event(test_fixture, AWS_MQTT5_CLET_CONNECTION_FAILURE);
}

static void s_wait_for_connection_failure_lifecycle_event(struct aws_mqtt5_client_mock_test_fixture *test_context) {
    aws_mutex_lock(&test_context->lock);
    aws_condition_variable_wait_pred(
        &test_context->signal, &test_context->lock, s_has_connection_failure_event, test_context);
    aws_mutex_unlock(&test_context->lock);
}

static bool s_has_disconnect_event(void *arg) {
    struct aws_mqtt5_client_mock_test_fixture *test_fixture = arg;

    return s_has_lifecycle_event(test_fixture, AWS_MQTT5_CLET_DISCONNECTION);
}

static void s_wait_for_disconnection_lifecycle_event(struct aws_mqtt5_client_mock_test_fixture *test_context) {
    aws_mutex_lock(&test_context->lock);
    aws_condition_variable_wait_pred(&test_context->signal, &test_context->lock, s_has_disconnect_event, test_context);
    aws_mutex_unlock(&test_context->lock);
}

static int s_verify_client_state_sequence(
    struct aws_mqtt5_client_mock_test_fixture *test_context,
    enum aws_mqtt5_client_state *expected_states,
    size_t expected_states_count) {
    aws_mutex_lock(&test_context->lock);

    size_t actual_states_count = aws_array_list_length(&test_context->client_states);
    ASSERT_TRUE(actual_states_count >= expected_states_count);

    for (size_t i = 0; i < expected_states_count; ++i) {
        enum aws_mqtt5_client_state state = AWS_MCS_STOPPED;
        aws_array_list_get_at(&test_context->client_states, &state, i);

        ASSERT_INT_EQUALS(expected_states[i], state);
    }

    aws_mutex_unlock(&test_context->lock);

    return AWS_OP_SUCCESS;
}

static int s_verify_simple_lifecycle_event_sequence(
    struct aws_mqtt5_client_mock_test_fixture *test_context,
    struct aws_mqtt5_client_lifecycle_event *expected_events,
    size_t expected_events_count) {
    aws_mutex_lock(&test_context->lock);

    size_t actual_events_count = aws_array_list_length(&test_context->lifecycle_events);
    ASSERT_TRUE(actual_events_count >= expected_events_count);

    for (size_t i = 0; i < expected_events_count; ++i) {
        struct aws_mqtt5_lifecycle_event_record *lifecycle_event = NULL;
        aws_array_list_get_at(&test_context->lifecycle_events, &lifecycle_event, i);

        struct aws_mqtt5_client_lifecycle_event *expected_event = &expected_events[i];
        ASSERT_INT_EQUALS(expected_event->event_type, lifecycle_event->event.event_type);
        ASSERT_INT_EQUALS(expected_event->error_code, lifecycle_event->event.error_code);
    }

    aws_mutex_unlock(&test_context->lock);

    return AWS_OP_SUCCESS;
}

static int s_verify_received_packet_sequence(
    struct aws_mqtt5_client_mock_test_fixture *test_context,
    struct aws_mqtt5_mock_server_packet_record *expected_packets,
    size_t expected_packets_count) {
    aws_mutex_lock(&test_context->lock);

    size_t actual_packets_count = aws_array_list_length(&test_context->server_received_packets);
    ASSERT_TRUE(actual_packets_count == expected_packets_count);

    for (size_t i = 0; i < expected_packets_count; ++i) {
        struct aws_mqtt5_mock_server_packet_record *actual_packet = NULL;
        aws_array_list_get_at_ptr(&test_context->server_received_packets, (void **)&actual_packet, i);

        struct aws_mqtt5_mock_server_packet_record *expected_packet = &expected_packets[i];

        ASSERT_INT_EQUALS(expected_packet->packet_type, actual_packet->packet_type);

        ASSERT_TRUE(aws_mqtt5_client_test_are_packets_equal(
            expected_packet->packet_type, expected_packet->packet_storage, actual_packet->packet_storage));
    }

    aws_mutex_unlock(&test_context->lock);

    return AWS_OP_SUCCESS;
}

static int s_mqtt5_client_direct_connect_success_fn(struct aws_allocator *allocator, void *ctx) {
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
    ASSERT_SUCCESS(aws_mqtt5_client_mock_test_fixture_init(&test_context, allocator, &test_fixture_options));

    struct aws_mqtt5_client *client = test_context.client;
    ASSERT_SUCCESS(aws_mqtt5_client_start(client));

    s_wait_for_connected_lifecycle_event(&test_context);

    ASSERT_SUCCESS(aws_mqtt5_client_stop(client, NULL));

    s_wait_for_stopped_lifecycle_event(&test_context);

    struct aws_mqtt5_client_lifecycle_event expected_events[] = {
        {
            .event_type = AWS_MQTT5_CLET_ATTEMPTING_CONNECT,
        },
        {
            .event_type = AWS_MQTT5_CLET_CONNECTION_SUCCESS,
        },
        {
            .event_type = AWS_MQTT5_CLET_DISCONNECTION,
            .error_code = AWS_ERROR_MQTT5_USER_REQUESTED_STOP,
        },
        {
            .event_type = AWS_MQTT5_CLET_STOPPED,
        },
    };
    ASSERT_SUCCESS(
        s_verify_simple_lifecycle_event_sequence(&test_context, expected_events, AWS_ARRAY_SIZE(expected_events)));

    enum aws_mqtt5_client_state expected_states[] = {
        AWS_MCS_CONNECTING,
        AWS_MCS_MQTT_CONNECT,
        AWS_MCS_CONNECTED,
        AWS_MCS_CHANNEL_SHUTDOWN,
        AWS_MCS_STOPPED,
    };
    ASSERT_SUCCESS(s_verify_client_state_sequence(&test_context, expected_states, AWS_ARRAY_SIZE(expected_states)));

    struct aws_mqtt5_packet_connect_storage expected_connect_storage;
    ASSERT_SUCCESS(s_aws_mqtt5_client_test_init_default_connect_storage(&expected_connect_storage, allocator));

    struct aws_mqtt5_mock_server_packet_record expected_packets[] = {
        {
            .packet_type = AWS_MQTT5_PT_CONNECT,
            .packet_storage = &expected_connect_storage,
        },
    };
    ASSERT_SUCCESS(
        s_verify_received_packet_sequence(&test_context, expected_packets, AWS_ARRAY_SIZE(expected_packets)));

    aws_mqtt5_packet_connect_storage_clean_up(&expected_connect_storage);

    aws_mqtt5_client_mock_test_fixture_clean_up(&test_context);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_client_direct_connect_success, s_mqtt5_client_direct_connect_success_fn)

static int s_mqtt5_client_simple_failure_test_fn(
    struct aws_allocator *allocator,
    void (*change_client_test_config_fn)(struct aws_mqtt5_client_mqtt5_mock_test_fixture_options *config),
    void (*change_client_vtable_fn)(struct aws_mqtt5_client_vtable *)) {

    aws_mqtt_library_init(allocator);

    struct aws_mqtt5_packet_connect_view connect_options;
    struct aws_mqtt5_client_options client_options;
    struct aws_mqtt5_mock_server_vtable server_function_table;
    s_mqtt5_client_test_init_default_options(&connect_options, &client_options, &server_function_table);

    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options test_fixture_options = {
        .client_options = &client_options,
        .server_function_table = &server_function_table,
    };

    if (change_client_test_config_fn != NULL) {
        (*change_client_test_config_fn)(&test_fixture_options);
    }

    struct aws_mqtt5_client_mock_test_fixture test_context;
    ASSERT_SUCCESS(aws_mqtt5_client_mock_test_fixture_init(&test_context, allocator, &test_fixture_options));

    struct aws_mqtt5_client *client = test_context.client;
    struct aws_mqtt5_client_vtable vtable = *client->vtable;
    if (change_client_vtable_fn != NULL) {
        (*change_client_vtable_fn)(&vtable);
        aws_mqtt5_client_set_vtable(client, &vtable);
    }

    ASSERT_SUCCESS(aws_mqtt5_client_start(client));

    s_wait_for_connection_failure_lifecycle_event(&test_context);

    ASSERT_SUCCESS(aws_mqtt5_client_stop(client, NULL));

    s_wait_for_stopped_lifecycle_event(&test_context);

    struct aws_mqtt5_client_lifecycle_event expected_events[] = {
        {
            .event_type = AWS_MQTT5_CLET_ATTEMPTING_CONNECT,
        },
        {
            .event_type = AWS_MQTT5_CLET_CONNECTION_FAILURE,
            .error_code = AWS_ERROR_INVALID_STATE,
        },
    };
    ASSERT_SUCCESS(
        s_verify_simple_lifecycle_event_sequence(&test_context, expected_events, AWS_ARRAY_SIZE(expected_events)));

    enum aws_mqtt5_client_state expected_states[] = {
        AWS_MCS_CONNECTING,
        AWS_MCS_PENDING_RECONNECT,
    };
    ASSERT_SUCCESS(s_verify_client_state_sequence(&test_context, expected_states, AWS_ARRAY_SIZE(expected_states)));

    aws_mqtt5_client_mock_test_fixture_clean_up(&test_context);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

static int s_mqtt5_client_test_synchronous_socket_channel_failure_fn(
    struct aws_socket_channel_bootstrap_options *options) {
    (void)options;

    return aws_raise_error(AWS_ERROR_INVALID_STATE);
}

static void s_change_client_vtable_synchronous_direct_failure(struct aws_mqtt5_client_vtable *vtable) {
    vtable->client_bootstrap_new_socket_channel_fn = s_mqtt5_client_test_synchronous_socket_channel_failure_fn;
}

static int s_mqtt5_client_direct_connect_sync_channel_failure_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(
        s_mqtt5_client_simple_failure_test_fn(allocator, NULL, s_change_client_vtable_synchronous_direct_failure));

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_client_direct_connect_sync_channel_failure, s_mqtt5_client_direct_connect_sync_channel_failure_fn)

struct socket_channel_failure_wrapper {
    struct aws_socket_channel_bootstrap_options bootstrap_options;
    struct aws_task task;
};

static struct socket_channel_failure_wrapper s_socket_channel_failure_wrapper;

void s_socket_channel_async_failure_task_fn(struct aws_task *task, void *arg, enum aws_task_status status) {
    (void)task;
    if (status != AWS_TASK_STATUS_RUN_READY) {
        return;
    }

    struct socket_channel_failure_wrapper *wrapper = arg;
    struct aws_socket_channel_bootstrap_options *options = &wrapper->bootstrap_options;

    (*wrapper->bootstrap_options.setup_callback)(options->bootstrap, AWS_ERROR_INVALID_STATE, NULL, options->user_data);
}

static int s_mqtt5_client_test_asynchronous_socket_channel_failure_fn(
    struct aws_socket_channel_bootstrap_options *options) {
    aws_task_init(
        &s_socket_channel_failure_wrapper.task,
        s_socket_channel_async_failure_task_fn,
        &s_socket_channel_failure_wrapper,
        "asynchronous_socket_channel_failure");
    s_socket_channel_failure_wrapper.bootstrap_options = *options;

    struct aws_mqtt5_client *client = options->user_data;
    aws_event_loop_schedule_task_now(client->loop, &s_socket_channel_failure_wrapper.task);

    return AWS_OP_SUCCESS;
}

static void s_change_client_vtable_asynchronous_direct_failure(struct aws_mqtt5_client_vtable *vtable) {
    vtable->client_bootstrap_new_socket_channel_fn = s_mqtt5_client_test_asynchronous_socket_channel_failure_fn;
}

static int s_mqtt5_client_direct_connect_async_channel_failure_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(
        s_mqtt5_client_simple_failure_test_fn(allocator, NULL, s_change_client_vtable_asynchronous_direct_failure));

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_client_direct_connect_async_channel_failure, s_mqtt5_client_direct_connect_async_channel_failure_fn)

static int s_mqtt5_client_test_synchronous_websocket_failure_fn(
    const struct aws_websocket_client_connection_options *options) {
    (void)options;

    return aws_raise_error(AWS_ERROR_INVALID_STATE);
}

static void s_change_client_vtable_synchronous_websocket_failure(struct aws_mqtt5_client_vtable *vtable) {
    vtable->websocket_connect_fn = s_mqtt5_client_test_synchronous_websocket_failure_fn;
}

static void s_mqtt5_client_test_websocket_successful_transform(
    struct aws_http_message *request,
    void *user_data,
    aws_mqtt5_transform_websocket_handshake_complete_fn *complete_fn,
    void *complete_ctx) {

    (*complete_fn)(request, AWS_ERROR_SUCCESS, complete_ctx);
}

static void s_change_client_options_to_websockets(struct aws_mqtt5_client_mqtt5_mock_test_fixture_options *config) {
    config->client_options->websocket_handshake_transform = s_mqtt5_client_test_websocket_successful_transform;
}

static int s_mqtt5_client_websocket_connect_sync_channel_failure_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_mqtt5_client_simple_failure_test_fn(
        allocator, s_change_client_options_to_websockets, s_change_client_vtable_synchronous_websocket_failure));

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(
    mqtt5_client_websocket_connect_sync_channel_failure,
    s_mqtt5_client_websocket_connect_sync_channel_failure_fn)

struct websocket_channel_failure_wrapper {
    struct aws_websocket_client_connection_options websocket_options;
    struct aws_task task;
};

static struct websocket_channel_failure_wrapper s_websocket_channel_failure_wrapper;

void s_websocket_channel_async_failure_task_fn(struct aws_task *task, void *arg, enum aws_task_status status) {
    (void)task;
    if (status != AWS_TASK_STATUS_RUN_READY) {
        return;
    }

    struct websocket_channel_failure_wrapper *wrapper = arg;
    struct aws_websocket_client_connection_options *options = &wrapper->websocket_options;

    (*wrapper->websocket_options.on_connection_setup)(NULL, AWS_ERROR_INVALID_STATE, 0, NULL, 0, options->user_data);
}

static int s_mqtt5_client_test_asynchronous_websocket_failure_fn(
    const struct aws_websocket_client_connection_options *options) {
    aws_task_init(
        &s_websocket_channel_failure_wrapper.task,
        s_websocket_channel_async_failure_task_fn,
        &s_websocket_channel_failure_wrapper,
        "asynchronous_websocket_channel_failure");
    s_websocket_channel_failure_wrapper.websocket_options = *options;

    struct aws_mqtt5_client *client = options->user_data;
    aws_event_loop_schedule_task_now(client->loop, &s_websocket_channel_failure_wrapper.task);

    return AWS_OP_SUCCESS;
}

static void s_change_client_vtable_asynchronous_websocket_failure(struct aws_mqtt5_client_vtable *vtable) {
    vtable->websocket_connect_fn = s_mqtt5_client_test_asynchronous_websocket_failure_fn;
}

static int s_mqtt5_client_websocket_connect_async_channel_failure_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_mqtt5_client_simple_failure_test_fn(
        allocator, s_change_client_options_to_websockets, s_change_client_vtable_asynchronous_websocket_failure));

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(
    mqtt5_client_websocket_connect_async_channel_failure,
    s_mqtt5_client_websocket_connect_async_channel_failure_fn)

static void s_mqtt5_client_test_websocket_failed_transform(
    struct aws_http_message *request,
    void *user_data,
    aws_mqtt5_transform_websocket_handshake_complete_fn *complete_fn,
    void *complete_ctx) {

    (*complete_fn)(request, AWS_ERROR_INVALID_STATE, complete_ctx);
}

static void s_change_client_options_to_failed_websocket_transform(
    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options *config) {
    config->client_options->websocket_handshake_transform = s_mqtt5_client_test_websocket_failed_transform;
}

static int s_mqtt5_client_websocket_connect_handshake_failure_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(
        s_mqtt5_client_simple_failure_test_fn(allocator, s_change_client_options_to_failed_websocket_transform, NULL));

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_client_websocket_connect_handshake_failure, s_mqtt5_client_websocket_connect_handshake_failure_fn)

static int s_aws_mqtt5_mock_server_handle_connect_always_fail(
    void *packet,
    struct aws_mqtt5_server_mock_connection_context *connection,
    void *user_data) {
    (void)packet;
    (void)user_data;

    struct aws_mqtt5_packet_connack_view connack_view;
    AWS_ZERO_STRUCT(connack_view);

    connack_view.reason_code = AWS_MQTT5_CRC_BANNED;

    return s_aws_mqtt5_mock_server_send_packet(connection, AWS_MQTT5_PT_CONNACK, &connack_view);
}

static int s_mqtt5_client_direct_connect_connack_refusal_fn(struct aws_allocator *allocator, void *ctx) {

    aws_mqtt_library_init(allocator);

    struct aws_mqtt5_packet_connect_view connect_options;
    struct aws_mqtt5_client_options client_options;
    struct aws_mqtt5_mock_server_vtable server_function_table;
    s_mqtt5_client_test_init_default_options(&connect_options, &client_options, &server_function_table);

    server_function_table.packet_handlers[AWS_MQTT5_PT_CONNECT] = s_aws_mqtt5_mock_server_handle_connect_always_fail;

    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options test_fixture_options = {
        .client_options = &client_options,
        .server_function_table = &server_function_table,
    };

    struct aws_mqtt5_client_mock_test_fixture test_context;
    ASSERT_SUCCESS(aws_mqtt5_client_mock_test_fixture_init(&test_context, allocator, &test_fixture_options));

    struct aws_mqtt5_client *client = test_context.client;

    ASSERT_SUCCESS(aws_mqtt5_client_start(client));

    s_wait_for_connection_failure_lifecycle_event(&test_context);

    ASSERT_SUCCESS(aws_mqtt5_client_stop(client, NULL));

    s_wait_for_stopped_lifecycle_event(&test_context);

    struct aws_mqtt5_client_lifecycle_event expected_events[] = {
        {
            .event_type = AWS_MQTT5_CLET_ATTEMPTING_CONNECT,
        },
        {
            .event_type = AWS_MQTT5_CLET_CONNECTION_FAILURE,
            .error_code = AWS_ERROR_MQTT5_CONNACK_CONNECTION_REFUSED,
        },
    };
    ASSERT_SUCCESS(
        s_verify_simple_lifecycle_event_sequence(&test_context, expected_events, AWS_ARRAY_SIZE(expected_events)));

    enum aws_mqtt5_client_state expected_states[] = {
        AWS_MCS_CONNECTING,
        AWS_MCS_MQTT_CONNECT,
        AWS_MCS_CHANNEL_SHUTDOWN,
    };
    ASSERT_SUCCESS(s_verify_client_state_sequence(&test_context, expected_states, AWS_ARRAY_SIZE(expected_states)));

    aws_mqtt5_client_mock_test_fixture_clean_up(&test_context);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_client_direct_connect_connack_refusal, s_mqtt5_client_direct_connect_connack_refusal_fn)

static int s_mqtt5_client_direct_connect_connack_timeout_fn(struct aws_allocator *allocator, void *ctx) {

    aws_mqtt_library_init(allocator);

    struct aws_mqtt5_packet_connect_view connect_options;
    struct aws_mqtt5_client_options client_options;
    struct aws_mqtt5_mock_server_vtable server_function_table;
    s_mqtt5_client_test_init_default_options(&connect_options, &client_options, &server_function_table);

    /* fast CONNACK timeout and don't response to the CONNECT packet */
    client_options.connack_timeout_ms = 2000;
    server_function_table.packet_handlers[AWS_MQTT5_PT_CONNECT] = NULL;

    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options test_fixture_options = {
        .client_options = &client_options,
        .server_function_table = &server_function_table,
    };

    struct aws_mqtt5_client_mock_test_fixture test_context;
    ASSERT_SUCCESS(aws_mqtt5_client_mock_test_fixture_init(&test_context, allocator, &test_fixture_options));

    struct aws_mqtt5_client *client = test_context.client;

    ASSERT_SUCCESS(aws_mqtt5_client_start(client));

    s_wait_for_connection_failure_lifecycle_event(&test_context);

    ASSERT_SUCCESS(aws_mqtt5_client_stop(client, NULL));

    s_wait_for_stopped_lifecycle_event(&test_context);

    struct aws_mqtt5_client_lifecycle_event expected_events[] = {
        {
            .event_type = AWS_MQTT5_CLET_ATTEMPTING_CONNECT,
        },
        {
            .event_type = AWS_MQTT5_CLET_CONNECTION_FAILURE,
            .error_code = AWS_ERROR_MQTT5_CONNACK_TIMEOUT,
        },
    };
    ASSERT_SUCCESS(
        s_verify_simple_lifecycle_event_sequence(&test_context, expected_events, AWS_ARRAY_SIZE(expected_events)));

    enum aws_mqtt5_client_state expected_states[] = {
        AWS_MCS_CONNECTING,
        AWS_MCS_MQTT_CONNECT,
        AWS_MCS_CHANNEL_SHUTDOWN,
    };
    ASSERT_SUCCESS(s_verify_client_state_sequence(&test_context, expected_states, AWS_ARRAY_SIZE(expected_states)));

    aws_mqtt5_client_mock_test_fixture_clean_up(&test_context);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_client_direct_connect_connack_timeout, s_mqtt5_client_direct_connect_connack_timeout_fn)

struct aws_mqtt5_server_disconnect_test_context {
    struct aws_mqtt5_client_mock_test_fixture *test_fixture;
    bool disconnect_sent;
    bool connack_sent;
};

static void s_server_disconnect_service_fn(
    struct aws_mqtt5_server_mock_connection_context *mock_server,
    void *user_data) {

    struct aws_mqtt5_server_disconnect_test_context *test_context = user_data;
    if (test_context->disconnect_sent || !test_context->connack_sent) {
        return;
    }

    test_context->disconnect_sent = true;

    struct aws_mqtt5_packet_disconnect_view disconnect = {
        .reason_code = AWS_MQTT5_DRC_PACKET_TOO_LARGE,
    };

    s_aws_mqtt5_mock_server_send_packet(mock_server, AWS_MQTT5_PT_DISCONNECT, &disconnect);
}

static int s_aws_mqtt5_server_disconnect_on_connect(
    void *packet,
    struct aws_mqtt5_server_mock_connection_context *connection,
    void *user_data) {

    /*
     * We intercept the CONNECT in order to correctly set the connack_sent test state.  Otherwise we risk sometimes
     * sending the DISCONNECT before the CONNACK
     */
    int result = s_aws_mqtt5_mock_server_handle_connect_always_succeed(packet, connection, user_data);

    struct aws_mqtt5_server_disconnect_test_context *test_context = user_data;
    test_context->connack_sent = true;

    return result;
}

static int s_mqtt5_client_direct_connect_from_server_disconnect_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct aws_mqtt5_packet_connect_view connect_options;
    struct aws_mqtt5_client_options client_options;
    struct aws_mqtt5_mock_server_vtable server_function_table;
    s_mqtt5_client_test_init_default_options(&connect_options, &client_options, &server_function_table);

    /* mock server sends a DISCONNECT packet back to the client after a successful CONNECTION establishment */
    server_function_table.service_task_fn = s_server_disconnect_service_fn;
    server_function_table.packet_handlers[AWS_MQTT5_PT_CONNECT] = s_aws_mqtt5_server_disconnect_on_connect;

    struct aws_mqtt5_client_mock_test_fixture test_context;

    struct aws_mqtt5_server_disconnect_test_context disconnect_context = {
        .test_fixture = &test_context,
        .disconnect_sent = false,
    };

    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options test_fixture_options = {
        .client_options = &client_options,
        .server_function_table = &server_function_table,
        .mock_server_user_data = &disconnect_context,
    };

    ASSERT_SUCCESS(aws_mqtt5_client_mock_test_fixture_init(&test_context, allocator, &test_fixture_options));

    struct aws_mqtt5_client *client = test_context.client;
    ASSERT_SUCCESS(aws_mqtt5_client_start(client));

    s_wait_for_disconnection_lifecycle_event(&test_context);

    ASSERT_SUCCESS(aws_mqtt5_client_stop(client, NULL));

    s_wait_for_stopped_lifecycle_event(&test_context);

    struct aws_mqtt5_client_lifecycle_event expected_events[] = {
        {
            .event_type = AWS_MQTT5_CLET_ATTEMPTING_CONNECT,
        },
        {
            .event_type = AWS_MQTT5_CLET_CONNECTION_SUCCESS,
        },
        {
            .event_type = AWS_MQTT5_CLET_DISCONNECTION,
            .error_code = AWS_ERROR_MQTT5_DISCONNECT_RECEIVED,
        },
    };
    ASSERT_SUCCESS(
        s_verify_simple_lifecycle_event_sequence(&test_context, expected_events, AWS_ARRAY_SIZE(expected_events)));

    enum aws_mqtt5_client_state expected_states[] = {
        AWS_MCS_CONNECTING,
        AWS_MCS_MQTT_CONNECT,
        AWS_MCS_CONNECTED,
        AWS_MCS_CHANNEL_SHUTDOWN,
    };
    ASSERT_SUCCESS(s_verify_client_state_sequence(&test_context, expected_states, AWS_ARRAY_SIZE(expected_states)));

    aws_mqtt5_client_mock_test_fixture_clean_up(&test_context);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(
    mqtt5_client_direct_connect_from_server_disconnect,
    s_mqtt5_client_direct_connect_from_server_disconnect_fn)
