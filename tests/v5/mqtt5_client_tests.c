/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "mqtt5_testing_utils.h"

#include <aws/common/clock.h>
#include <aws/common/string.h>
#include <aws/http/websocket.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/mqtt/mqtt.h>
#include <aws/mqtt/private/v5/mqtt5_utils.h>
#include <aws/mqtt/v5/mqtt5_client.h>

#include <aws/testing/aws_test_harness.h>

#include <inttypes.h>
#include <math.h>

#define TEST_IO_MESSAGE_LENGTH 4096

static bool s_is_within_percentage_of(uint64_t expected_time, uint64_t actual_time, double percentage) {
    double actual_percent = 1.0 - (double)actual_time / (double)expected_time;
    return fabs(actual_percent) <= percentage;
}

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

void s_publish_received_callback(const struct aws_mqtt5_packet_publish_view *publish, void *user_data) {
    (void)publish;
    (void)user_data;
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
        .timout_seconds = 0,
        .publish_received = s_publish_received_callback,
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

static int s_aws_mqtt5_client_test_init_default_disconnect_storage(
    struct aws_mqtt5_packet_disconnect_storage *storage,
    struct aws_allocator *allocator) {
    struct aws_mqtt5_packet_disconnect_view disconnect_view = {
        .reason_code = AWS_MQTT5_DRC_NORMAL_DISCONNECTION,
    };

    return aws_mqtt5_packet_disconnect_storage_init(storage, allocator, &disconnect_view);
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

static bool s_disconnect_completion_invoked(void *arg) {
    struct aws_mqtt5_client_mock_test_fixture *test_fixture = arg;

    return test_fixture->disconnect_completion_callback_invoked;
}

static void s_on_disconnect_completion(int error_code, void *user_data) {
    struct aws_mqtt5_client_mock_test_fixture *test_fixture = user_data;

    aws_mutex_lock(&test_fixture->lock);
    test_fixture->disconnect_completion_callback_invoked = true;
    aws_mutex_unlock(&test_fixture->lock);
    aws_condition_variable_notify_all(&test_fixture->signal);
}

static void s_wait_for_disconnect_completion(struct aws_mqtt5_client_mock_test_fixture *test_context) {
    aws_mutex_lock(&test_context->lock);
    aws_condition_variable_wait_pred(
        &test_context->signal, &test_context->lock, s_disconnect_completion_invoked, test_context);
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
    ASSERT_TRUE(actual_packets_count >= expected_packets_count);

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

/*
 * Basic successful connect/disconnect test.  We check expected lifecycle events, internal client state changes,
 * and server received packets.
 */
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

    struct aws_mqtt5_packet_disconnect_view disconnect_options = {
        .reason_code = AWS_MQTT5_DRC_DISCONNECT_WITH_WILL_MESSAGE,
    };

    struct aws_mqtt5_disconnect_completion_options completion_options = {
        .completion_callback = s_on_disconnect_completion,
        .completion_user_data = &test_context,
    };

    ASSERT_SUCCESS(aws_mqtt5_client_stop(client, &disconnect_options, &completion_options));

    s_wait_for_stopped_lifecycle_event(&test_context);
    s_wait_for_disconnect_completion(&test_context);

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
        AWS_MCS_CLEAN_DISCONNECT,
        AWS_MCS_CHANNEL_SHUTDOWN,
        AWS_MCS_STOPPED,
    };
    ASSERT_SUCCESS(s_verify_client_state_sequence(&test_context, expected_states, AWS_ARRAY_SIZE(expected_states)));

    struct aws_mqtt5_packet_connect_storage expected_connect_storage;
    ASSERT_SUCCESS(s_aws_mqtt5_client_test_init_default_connect_storage(&expected_connect_storage, allocator));

    struct aws_mqtt5_packet_disconnect_storage expected_disconnect_storage;
    ASSERT_SUCCESS(s_aws_mqtt5_client_test_init_default_disconnect_storage(&expected_disconnect_storage, allocator));
    expected_disconnect_storage.storage_view.reason_code = AWS_MQTT5_DRC_DISCONNECT_WITH_WILL_MESSAGE;

    struct aws_mqtt5_mock_server_packet_record expected_packets[] = {
        {
            .packet_type = AWS_MQTT5_PT_CONNECT,
            .packet_storage = &expected_connect_storage,
        },
        {
            .packet_type = AWS_MQTT5_PT_DISCONNECT,
            .packet_storage = &expected_disconnect_storage,
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

/*
 * Connection failure test infrastructure.  Supplied callbacks are used to modify the way in which the connection
 * establishment fails.
 */
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

    ASSERT_SUCCESS(aws_mqtt5_client_stop(client, NULL, NULL));

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

/* Connection failure test where direct MQTT channel establishment fails synchronously */
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

/* Connection failure test where direct MQTT channel establishment fails asynchronously */
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

/* Connection failure test where websocket MQTT channel establishment fails synchronously */
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

/* Connection failure test where websocket MQTT channel establishment fails asynchronously */
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

/* Connection failure test where websocket MQTT channel establishment fails due to handshake transform failure */
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

/* Connection failure test where overall connection fails due to a CONNACK error code */
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

    ASSERT_SUCCESS(aws_mqtt5_client_stop(client, NULL, NULL));

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

/* Connection failure test where overall connection fails because there's no response to the CONNECT packet */
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

    ASSERT_SUCCESS(aws_mqtt5_client_stop(client, NULL, NULL));

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

/* Connection test where we succeed and then the server sends a DISCONNECT */
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

    ASSERT_SUCCESS(aws_mqtt5_client_stop(client, NULL, NULL));

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

struct aws_mqtt5_client_test_wait_for_n_context {
    size_t required_event_count;
    struct aws_mqtt5_client_mock_test_fixture *test_fixture;
};

static bool s_received_at_least_n_pingreqs(void *arg) {
    struct aws_mqtt5_client_test_wait_for_n_context *ping_context = arg;
    struct aws_mqtt5_client_mock_test_fixture *test_fixture = ping_context->test_fixture;

    size_t ping_count = 0;
    size_t packet_count = aws_array_list_length(&test_fixture->server_received_packets);
    for (size_t i = 0; i < packet_count; ++i) {
        struct aws_mqtt5_mock_server_packet_record *record = NULL;
        aws_array_list_get_at_ptr(&test_fixture->server_received_packets, (void **)&record, i);

        if (record->packet_type == AWS_MQTT5_PT_PINGREQ) {
            ping_count++;
        }
    }

    return ping_count >= ping_context->required_event_count;
}

static void s_wait_for_n_pingreqs(struct aws_mqtt5_client_test_wait_for_n_context *ping_context) {

    struct aws_mqtt5_client_mock_test_fixture *test_context = ping_context->test_fixture;
    aws_mutex_lock(&test_context->lock);
    aws_condition_variable_wait_pred(
        &test_context->signal, &test_context->lock, s_received_at_least_n_pingreqs, ping_context);
    aws_mutex_unlock(&test_context->lock);
}

#define TEST_PING_INTERVAL_MS 2000

static int s_verify_ping_sequence_timing(struct aws_mqtt5_client_mock_test_fixture *test_context) {
    aws_mutex_lock(&test_context->lock);

    uint64_t last_packet_time = 0;

    size_t packet_count = aws_array_list_length(&test_context->server_received_packets);
    for (size_t i = 0; i < packet_count; ++i) {
        struct aws_mqtt5_mock_server_packet_record *record = NULL;
        aws_array_list_get_at_ptr(&test_context->server_received_packets, (void **)&record, i);

        if (i == 0) {
            ASSERT_INT_EQUALS(record->packet_type, AWS_MQTT5_PT_CONNECT);
            last_packet_time = record->timestamp;
        } else {
            if (record->packet_type == AWS_MQTT5_PT_PINGREQ) {
                uint64_t time_delta_ns = record->timestamp - last_packet_time;
                uint64_t time_delta_millis =
                    aws_timestamp_convert(time_delta_ns, AWS_TIMESTAMP_NANOS, AWS_TIMESTAMP_MILLIS, NULL);

                ASSERT_TRUE(s_is_within_percentage_of(TEST_PING_INTERVAL_MS, time_delta_millis, .1));

                last_packet_time = record->timestamp;
            }
        }
    }
    aws_mutex_unlock(&test_context->lock);

    return AWS_OP_SUCCESS;
}

static int s_aws_mqtt5_client_test_init_ping_test_connect_storage(
    struct aws_mqtt5_packet_connect_storage *storage,
    struct aws_allocator *allocator) {
    struct aws_mqtt5_packet_connect_view connect_view = {
        .keep_alive_interval_seconds =
            (uint32_t)aws_timestamp_convert(TEST_PING_INTERVAL_MS, AWS_TIMESTAMP_MILLIS, AWS_TIMESTAMP_SECS, NULL),
        .client_id = aws_byte_cursor_from_string(s_client_id),
        .clean_start = true,
    };

    return aws_mqtt5_packet_connect_storage_init(storage, allocator, &connect_view);
}

/*
 * Test that the client sends pings at regular intervals to the server
 *
 * This is a low-keep-alive variant of the basic success test that waits for N pingreqs to be received by the server
 * and validates the approximate time intervals between them.
 */
static int s_mqtt5_client_ping_sequence_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct aws_mqtt5_packet_connect_view connect_options;
    struct aws_mqtt5_client_options client_options;
    struct aws_mqtt5_mock_server_vtable server_function_table;
    s_mqtt5_client_test_init_default_options(&connect_options, &client_options, &server_function_table);

    /* fast keep alive in order keep tests reasonably short */
    uint32_t keep_alive_seconds =
        aws_timestamp_convert(TEST_PING_INTERVAL_MS, AWS_TIMESTAMP_MILLIS, AWS_TIMESTAMP_SECS, NULL);
    connect_options.keep_alive_interval_seconds = keep_alive_seconds;

    /* faster ping timeout */
    client_options.ping_timeout_ms = 750;

    struct aws_mqtt5_client_mock_test_fixture test_context;
    struct aws_mqtt5_client_test_wait_for_n_context ping_context = {
        .required_event_count = 5,
        .test_fixture = &test_context,
    };

    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options test_fixture_options = {
        .client_options = &client_options,
        .server_function_table = &server_function_table,
        .mock_server_user_data = &ping_context,
    };

    ASSERT_SUCCESS(aws_mqtt5_client_mock_test_fixture_init(&test_context, allocator, &test_fixture_options));

    struct aws_mqtt5_client *client = test_context.client;
    ASSERT_SUCCESS(aws_mqtt5_client_start(client));

    s_wait_for_connected_lifecycle_event(&test_context);
    s_wait_for_n_pingreqs(&ping_context);

    struct aws_mqtt5_packet_disconnect_view disconnect_view = {
        .reason_code = AWS_MQTT5_DRC_NORMAL_DISCONNECTION,
    };

    ASSERT_SUCCESS(aws_mqtt5_client_stop(client, &disconnect_view, NULL));

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
        AWS_MCS_CLEAN_DISCONNECT,
        AWS_MCS_CHANNEL_SHUTDOWN,
        AWS_MCS_STOPPED,
    };
    ASSERT_SUCCESS(s_verify_client_state_sequence(&test_context, expected_states, AWS_ARRAY_SIZE(expected_states)));

    struct aws_mqtt5_packet_connect_storage expected_connect_storage;
    ASSERT_SUCCESS(s_aws_mqtt5_client_test_init_ping_test_connect_storage(&expected_connect_storage, allocator));

    struct aws_mqtt5_packet_disconnect_storage expected_disconnect_storage;
    ASSERT_SUCCESS(s_aws_mqtt5_client_test_init_default_disconnect_storage(&expected_disconnect_storage, allocator));

    struct aws_mqtt5_mock_server_packet_record expected_packets[] = {
        {
            .packet_type = AWS_MQTT5_PT_CONNECT,
            .packet_storage = &expected_connect_storage,
        },
        {
            .packet_type = AWS_MQTT5_PT_PINGREQ,
        },
        {
            .packet_type = AWS_MQTT5_PT_PINGREQ,
        },
        {
            .packet_type = AWS_MQTT5_PT_PINGREQ,
        },
        {
            .packet_type = AWS_MQTT5_PT_PINGREQ,
        },
        {
            .packet_type = AWS_MQTT5_PT_PINGREQ,
        },
        {
            .packet_type = AWS_MQTT5_PT_DISCONNECT,
            .packet_storage = &expected_disconnect_storage,
        },
    };
    ASSERT_SUCCESS(
        s_verify_received_packet_sequence(&test_context, expected_packets, AWS_ARRAY_SIZE(expected_packets)));

    ASSERT_SUCCESS(s_verify_ping_sequence_timing(&test_context));

    aws_mqtt5_packet_connect_storage_clean_up(&expected_connect_storage);

    aws_mqtt5_client_mock_test_fixture_clean_up(&test_context);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_client_ping_sequence, s_mqtt5_client_ping_sequence_fn)

/*
 * Test that sending other data to the server pushes out the client ping timer
 *
 * This is a low-keep-alive variant of the basic success test that writes UNSUBSCRIBEs to the server at fast intervals.
 * Verify the server doesn't receive any PINGREQs until the right amount of time after we stop sending the CONNECTs to
 * it.
 *
 * TODO: we can't write this test until we have proper operation handling during CONNECTED state
 */
static int s_mqtt5_client_ping_write_pushout_fn(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    (void)ctx;

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_client_ping_write_pushout, s_mqtt5_client_ping_write_pushout_fn)

#define TIMEOUT_TEST_PING_INTERVAL_MS 10000

static int s_verify_ping_timeout_interval(struct aws_mqtt5_client_mock_test_fixture *test_context) {
    aws_mutex_lock(&test_context->lock);

    uint64_t connected_time = 0;
    uint64_t disconnected_time = 0;

    size_t event_count = aws_array_list_length(&test_context->lifecycle_events);
    for (size_t i = 0; i < event_count; ++i) {
        struct aws_mqtt5_lifecycle_event_record *record = NULL;
        aws_array_list_get_at(&test_context->lifecycle_events, &record, i);
        if (connected_time == 0 && record->event.event_type == AWS_MQTT5_CLET_CONNECTION_SUCCESS) {
            connected_time = record->timestamp;
        }

        if (disconnected_time == 0 && record->event.event_type == AWS_MQTT5_CLET_DISCONNECTION) {
            disconnected_time = record->timestamp;
        }
    }

    aws_mutex_unlock(&test_context->lock);

    ASSERT_TRUE(connected_time > 0 && disconnected_time > 0 && disconnected_time > connected_time);

    uint64_t connected_interval_ms =
        aws_timestamp_convert(disconnected_time - connected_time, AWS_TIMESTAMP_NANOS, AWS_TIMESTAMP_MILLIS, NULL);
    uint64_t expected_connected_time_ms = TIMEOUT_TEST_PING_INTERVAL_MS + test_context->client->config->ping_timeout_ms;

    ASSERT_TRUE(s_is_within_percentage_of(expected_connected_time_ms, connected_interval_ms, .1));

    return AWS_OP_SUCCESS;
}

/*
 * Test that not receiving a PINGRESP causes a disconnection
 *
 * This is a low-keep-alive variant of the basic success test where the mock server does not respond to a PINGREQ.
 */
static int s_mqtt5_client_ping_timeout_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct aws_mqtt5_packet_connect_view connect_options;
    struct aws_mqtt5_client_options client_options;
    struct aws_mqtt5_mock_server_vtable server_function_table;
    s_mqtt5_client_test_init_default_options(&connect_options, &client_options, &server_function_table);

    /* fast keep alive in order keep tests reasonably short */
    uint32_t keep_alive_seconds =
        aws_timestamp_convert(TIMEOUT_TEST_PING_INTERVAL_MS, AWS_TIMESTAMP_MILLIS, AWS_TIMESTAMP_SECS, NULL);
    connect_options.keep_alive_interval_seconds = keep_alive_seconds;

    /* don't response to PINGREQs */
    server_function_table.packet_handlers[AWS_MQTT5_PT_PINGREQ] = NULL;

    /* faster ping timeout */
    client_options.ping_timeout_ms = 5000;

    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options test_fixture_options = {
        .client_options = &client_options,
        .server_function_table = &server_function_table,
    };

    struct aws_mqtt5_client_mock_test_fixture test_context;
    ASSERT_SUCCESS(aws_mqtt5_client_mock_test_fixture_init(&test_context, allocator, &test_fixture_options));

    struct aws_mqtt5_client *client = test_context.client;
    ASSERT_SUCCESS(aws_mqtt5_client_start(client));

    s_wait_for_connected_lifecycle_event(&test_context);
    s_wait_for_disconnection_lifecycle_event(&test_context);

    ASSERT_SUCCESS(aws_mqtt5_client_stop(client, NULL, NULL));

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
            .error_code = AWS_ERROR_MQTT5_PING_RESPONSE_TIMEOUT,
        },
        {
            .event_type = AWS_MQTT5_CLET_STOPPED,
        },
    };
    ASSERT_SUCCESS(
        s_verify_simple_lifecycle_event_sequence(&test_context, expected_events, AWS_ARRAY_SIZE(expected_events)));

    ASSERT_SUCCESS(s_verify_ping_timeout_interval(&test_context));

    enum aws_mqtt5_client_state expected_states[] = {
        AWS_MCS_CONNECTING,
        AWS_MCS_MQTT_CONNECT,
        AWS_MCS_CONNECTED,
        AWS_MCS_CLEAN_DISCONNECT,
        AWS_MCS_CHANNEL_SHUTDOWN,
        AWS_MCS_STOPPED,
    };
    ASSERT_SUCCESS(s_verify_client_state_sequence(&test_context, expected_states, AWS_ARRAY_SIZE(expected_states)));

    aws_mqtt5_client_mock_test_fixture_clean_up(&test_context);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_client_ping_timeout, s_mqtt5_client_ping_timeout_fn)

struct aws_connection_failure_wait_context {
    size_t number_of_failures;
    struct aws_mqtt5_client_mock_test_fixture *test_fixture;
};

static bool s_received_at_least_n_connection_failures(void *arg) {
    struct aws_connection_failure_wait_context *context = arg;
    struct aws_mqtt5_client_mock_test_fixture *test_fixture = context->test_fixture;

    size_t failure_count = 0;
    size_t event_count = aws_array_list_length(&test_fixture->lifecycle_events);
    for (size_t i = 0; i < event_count; ++i) {
        struct aws_mqtt5_lifecycle_event_record *record = NULL;
        aws_array_list_get_at(&test_fixture->lifecycle_events, &record, i);

        if (record->event.event_type == AWS_MQTT5_CLET_CONNECTION_FAILURE) {
            failure_count++;
        }
    }

    return failure_count >= context->number_of_failures;
}

static void s_wait_for_n_connection_failure_lifecycle_events(
    struct aws_mqtt5_client_mock_test_fixture *test_context,
    size_t failure_count) {
    struct aws_connection_failure_wait_context context = {
        .number_of_failures = failure_count,
        .test_fixture = test_context,
    };

    aws_mutex_lock(&test_context->lock);
    aws_condition_variable_wait_pred(
        &test_context->signal, &test_context->lock, s_received_at_least_n_connection_failures, &context);
    aws_mutex_unlock(&test_context->lock);
}

#define RECONNECT_TEST_MIN_BACKOFF 500
#define RECONNECT_TEST_MAX_BACKOFF 5000
#define RECONNECT_TEST_BACKOFF_RESET_DELAY 5000

static int s_verify_reconnection_exponential_backoff_timestamps(
    struct aws_mqtt5_client_mock_test_fixture *test_fixture) {
    aws_mutex_lock(&test_fixture->lock);

    size_t event_count = aws_array_list_length(&test_fixture->lifecycle_events);
    uint64_t last_timestamp = 0;
    uint64_t expected_backoff = RECONNECT_TEST_MIN_BACKOFF;

    for (size_t i = 0; i < event_count; ++i) {
        struct aws_mqtt5_lifecycle_event_record *record = NULL;
        aws_array_list_get_at(&test_fixture->lifecycle_events, &record, i);

        if (record->event.event_type == AWS_MQTT5_CLET_CONNECTION_FAILURE) {
            if (last_timestamp == 0) {
                last_timestamp = record->timestamp;
            } else {
                uint64_t time_diff = aws_timestamp_convert(
                    record->timestamp - last_timestamp, AWS_TIMESTAMP_NANOS, AWS_TIMESTAMP_MILLIS, NULL);

                if (!s_is_within_percentage_of(expected_backoff, time_diff, .1)) {
                    return AWS_OP_ERR;
                }

                expected_backoff = aws_min_u64(expected_backoff * 2, RECONNECT_TEST_MAX_BACKOFF);
                last_timestamp = record->timestamp;
            }
        }
    }

    aws_mutex_unlock(&test_fixture->lock);

    return AWS_OP_SUCCESS;
}

/*
 * Always-fail variant that waits for 6 connection failures and then checks the timestamps between them against
 * what we'd expect with exponential backoff and no jitter
 */
static int s_mqtt5_client_reconnect_failure_backoff_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct aws_mqtt5_packet_connect_view connect_options;
    struct aws_mqtt5_client_options client_options;
    struct aws_mqtt5_mock_server_vtable server_function_table;
    s_mqtt5_client_test_init_default_options(&connect_options, &client_options, &server_function_table);

    /* backoff delay sequence: 500, 1000, 2000, 4000, 5000, ... */
    client_options.retry_jitter_mode = AWS_EXPONENTIAL_BACKOFF_JITTER_NONE;
    client_options.min_reconnect_delay_ms = RECONNECT_TEST_MIN_BACKOFF;
    client_options.max_reconnect_delay_ms = RECONNECT_TEST_MAX_BACKOFF;
    client_options.min_connected_time_to_reset_reconnect_delay_ms = RECONNECT_TEST_BACKOFF_RESET_DELAY;

    server_function_table.packet_handlers[AWS_MQTT5_PT_CONNECT] = s_aws_mqtt5_mock_server_handle_connect_always_fail;

    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options test_fixture_options = {
        .client_options = &client_options,
        .server_function_table = &server_function_table,
    };

    struct aws_mqtt5_client_mock_test_fixture test_context;
    ASSERT_SUCCESS(aws_mqtt5_client_mock_test_fixture_init(&test_context, allocator, &test_fixture_options));

    struct aws_mqtt5_client *client = test_context.client;

    ASSERT_SUCCESS(aws_mqtt5_client_start(client));

    s_wait_for_n_connection_failure_lifecycle_events(&test_context, 6);

    ASSERT_SUCCESS(aws_mqtt5_client_stop(client, NULL, NULL));

    s_wait_for_stopped_lifecycle_event(&test_context);

    /* 6 (connecting, connection failure) pairs */
    struct aws_mqtt5_client_lifecycle_event expected_events[] = {
        {
            .event_type = AWS_MQTT5_CLET_ATTEMPTING_CONNECT,
        },
        {
            .event_type = AWS_MQTT5_CLET_CONNECTION_FAILURE,
            .error_code = AWS_ERROR_MQTT5_CONNACK_CONNECTION_REFUSED,
        },
        {
            .event_type = AWS_MQTT5_CLET_ATTEMPTING_CONNECT,
        },
        {
            .event_type = AWS_MQTT5_CLET_CONNECTION_FAILURE,
            .error_code = AWS_ERROR_MQTT5_CONNACK_CONNECTION_REFUSED,
        },
        {
            .event_type = AWS_MQTT5_CLET_ATTEMPTING_CONNECT,
        },
        {
            .event_type = AWS_MQTT5_CLET_CONNECTION_FAILURE,
            .error_code = AWS_ERROR_MQTT5_CONNACK_CONNECTION_REFUSED,
        },
        {
            .event_type = AWS_MQTT5_CLET_ATTEMPTING_CONNECT,
        },
        {
            .event_type = AWS_MQTT5_CLET_CONNECTION_FAILURE,
            .error_code = AWS_ERROR_MQTT5_CONNACK_CONNECTION_REFUSED,
        },
        {
            .event_type = AWS_MQTT5_CLET_ATTEMPTING_CONNECT,
        },
        {
            .event_type = AWS_MQTT5_CLET_CONNECTION_FAILURE,
            .error_code = AWS_ERROR_MQTT5_CONNACK_CONNECTION_REFUSED,
        },
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

    ASSERT_SUCCESS(s_verify_reconnection_exponential_backoff_timestamps(&test_context));

    /* 6 (connecting, mqtt_connect, channel_shutdown, pending_reconnect) tuples (minus the final pending_reconnect) */
    enum aws_mqtt5_client_state expected_states[] = {
        AWS_MCS_CONNECTING, AWS_MCS_MQTT_CONNECT, AWS_MCS_CHANNEL_SHUTDOWN, AWS_MCS_PENDING_RECONNECT,
        AWS_MCS_CONNECTING, AWS_MCS_MQTT_CONNECT, AWS_MCS_CHANNEL_SHUTDOWN, AWS_MCS_PENDING_RECONNECT,
        AWS_MCS_CONNECTING, AWS_MCS_MQTT_CONNECT, AWS_MCS_CHANNEL_SHUTDOWN, AWS_MCS_PENDING_RECONNECT,
        AWS_MCS_CONNECTING, AWS_MCS_MQTT_CONNECT, AWS_MCS_CHANNEL_SHUTDOWN, AWS_MCS_PENDING_RECONNECT,
        AWS_MCS_CONNECTING, AWS_MCS_MQTT_CONNECT, AWS_MCS_CHANNEL_SHUTDOWN, AWS_MCS_PENDING_RECONNECT,
        AWS_MCS_CONNECTING, AWS_MCS_MQTT_CONNECT, AWS_MCS_CHANNEL_SHUTDOWN,
    };
    ASSERT_SUCCESS(s_verify_client_state_sequence(&test_context, expected_states, AWS_ARRAY_SIZE(expected_states)));

    aws_mqtt5_client_mock_test_fixture_clean_up(&test_context);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_client_reconnect_failure_backoff, s_mqtt5_client_reconnect_failure_backoff_fn)

struct aws_mqtt5_mock_server_reconnect_state {
    size_t required_connection_failure_count;

    size_t connection_attempts;
    uint64_t connect_timestamp;

    uint64_t successful_connection_disconnect_delay_ms;
};

static int s_aws_mqtt5_mock_server_handle_connect_succeed_on_nth(
    void *packet,
    struct aws_mqtt5_server_mock_connection_context *connection,
    void *user_data) {
    (void)packet;

    struct aws_mqtt5_mock_server_reconnect_state *context = user_data;

    struct aws_mqtt5_packet_connack_view connack_view;
    AWS_ZERO_STRUCT(connack_view);

    if (context->connection_attempts == context->required_connection_failure_count) {
        connack_view.reason_code = AWS_MQTT5_CRC_SUCCESS;
        aws_high_res_clock_get_ticks(&context->connect_timestamp);
    } else {
        connack_view.reason_code = AWS_MQTT5_CRC_NOT_AUTHORIZED;
    }

    ++context->connection_attempts;

    return s_aws_mqtt5_mock_server_send_packet(connection, AWS_MQTT5_PT_CONNACK, &connack_view);
}

static void s_aws_mqtt5_mock_server_disconnect_after_n_ms(
    struct aws_mqtt5_server_mock_connection_context *mock_server,
    void *user_data) {

    struct aws_mqtt5_mock_server_reconnect_state *context = user_data;
    if (context->connect_timestamp == 0) {
        return;
    }

    uint64_t now = 0;
    aws_high_res_clock_get_ticks(&now);

    if (now < context->connect_timestamp) {
        return;
    }

    uint64_t elapsed_ms =
        aws_timestamp_convert(now - context->connect_timestamp, AWS_TIMESTAMP_NANOS, AWS_TIMESTAMP_MILLIS, NULL);
    if (elapsed_ms > context->successful_connection_disconnect_delay_ms) {

        struct aws_mqtt5_packet_disconnect_view disconnect = {
            .reason_code = AWS_MQTT5_DRC_PACKET_TOO_LARGE,
        };

        s_aws_mqtt5_mock_server_send_packet(mock_server, AWS_MQTT5_PT_DISCONNECT, &disconnect);
        context->connect_timestamp = 0;
    }
}

static int s_verify_reconnection_after_success_used_backoff(
    struct aws_mqtt5_client_mock_test_fixture *test_fixture,
    uint64_t expected_reconnect_delay_ms) {

    aws_mutex_lock(&test_fixture->lock);

    size_t event_count = aws_array_list_length(&test_fixture->lifecycle_events);

    uint64_t disconnect_after_success_timestamp = 0;
    uint64_t reconnect_failure_after_disconnect_timestamp = 0;

    for (size_t i = 0; i < event_count; ++i) {
        struct aws_mqtt5_lifecycle_event_record *record = NULL;
        aws_array_list_get_at(&test_fixture->lifecycle_events, &record, i);

        if (record->event.event_type == AWS_MQTT5_CLET_DISCONNECTION) {
            ASSERT_INT_EQUALS(0, disconnect_after_success_timestamp);
            disconnect_after_success_timestamp = record->timestamp;
        } else if (record->event.event_type == AWS_MQTT5_CLET_CONNECTION_FAILURE) {
            if (reconnect_failure_after_disconnect_timestamp == 0 && disconnect_after_success_timestamp > 0) {
                reconnect_failure_after_disconnect_timestamp = record->timestamp;
            }
        }
    }

    aws_mutex_unlock(&test_fixture->lock);

    ASSERT_TRUE(disconnect_after_success_timestamp > 0 && reconnect_failure_after_disconnect_timestamp > 0);

    uint64_t post_success_reconnect_time_ms = aws_timestamp_convert(
        reconnect_failure_after_disconnect_timestamp - disconnect_after_success_timestamp,
        AWS_TIMESTAMP_NANOS,
        AWS_TIMESTAMP_MILLIS,
        NULL);

    if (!s_is_within_percentage_of(expected_reconnect_delay_ms, post_success_reconnect_time_ms, .1)) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

/*
 * Fail-until-max-backoff variant, followed by a success that then quickly disconnects.  Verify the next reconnect
 * attempt still uses the maximum backoff because we weren't connected long enough to reset it.
 */
static int s_mqtt5_client_reconnect_backoff_insufficient_reset_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct aws_mqtt5_packet_connect_view connect_options;
    struct aws_mqtt5_client_options client_options;
    struct aws_mqtt5_mock_server_vtable server_function_table;
    s_mqtt5_client_test_init_default_options(&connect_options, &client_options, &server_function_table);

    struct aws_mqtt5_mock_server_reconnect_state mock_server_state = {
        .required_connection_failure_count = 6,
        /* quick disconnect should not reset reconnect delay */
        .successful_connection_disconnect_delay_ms = RECONNECT_TEST_BACKOFF_RESET_DELAY / 5,
    };

    /* backoff delay sequence: 500, 1000, 2000, 4000, 5000, ... */
    client_options.retry_jitter_mode = AWS_EXPONENTIAL_BACKOFF_JITTER_NONE;
    client_options.min_reconnect_delay_ms = RECONNECT_TEST_MIN_BACKOFF;
    client_options.max_reconnect_delay_ms = RECONNECT_TEST_MAX_BACKOFF;
    client_options.min_connected_time_to_reset_reconnect_delay_ms = RECONNECT_TEST_BACKOFF_RESET_DELAY;

    server_function_table.packet_handlers[AWS_MQTT5_PT_CONNECT] = s_aws_mqtt5_mock_server_handle_connect_succeed_on_nth;
    server_function_table.service_task_fn = s_aws_mqtt5_mock_server_disconnect_after_n_ms;

    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options test_fixture_options = {
        .client_options = &client_options,
        .server_function_table = &server_function_table,
        .mock_server_user_data = &mock_server_state,
    };

    struct aws_mqtt5_client_mock_test_fixture test_context;
    ASSERT_SUCCESS(aws_mqtt5_client_mock_test_fixture_init(&test_context, allocator, &test_fixture_options));

    struct aws_mqtt5_client *client = test_context.client;

    ASSERT_SUCCESS(aws_mqtt5_client_start(client));

    s_wait_for_n_connection_failure_lifecycle_events(&test_context, 7);

    ASSERT_SUCCESS(aws_mqtt5_client_stop(client, NULL, NULL));

    s_wait_for_stopped_lifecycle_event(&test_context);

    /* 6 (connecting, connection failure) pairs, followed by a successful connection, then a disconnect and reconnect */
    struct aws_mqtt5_client_lifecycle_event expected_events[] = {
        {
            .event_type = AWS_MQTT5_CLET_ATTEMPTING_CONNECT,
        },
        {
            .event_type = AWS_MQTT5_CLET_CONNECTION_FAILURE,
            .error_code = AWS_ERROR_MQTT5_CONNACK_CONNECTION_REFUSED,
        },
        {
            .event_type = AWS_MQTT5_CLET_ATTEMPTING_CONNECT,
        },
        {
            .event_type = AWS_MQTT5_CLET_CONNECTION_FAILURE,
            .error_code = AWS_ERROR_MQTT5_CONNACK_CONNECTION_REFUSED,
        },
        {
            .event_type = AWS_MQTT5_CLET_ATTEMPTING_CONNECT,
        },
        {
            .event_type = AWS_MQTT5_CLET_CONNECTION_FAILURE,
            .error_code = AWS_ERROR_MQTT5_CONNACK_CONNECTION_REFUSED,
        },
        {
            .event_type = AWS_MQTT5_CLET_ATTEMPTING_CONNECT,
        },
        {
            .event_type = AWS_MQTT5_CLET_CONNECTION_FAILURE,
            .error_code = AWS_ERROR_MQTT5_CONNACK_CONNECTION_REFUSED,
        },
        {
            .event_type = AWS_MQTT5_CLET_ATTEMPTING_CONNECT,
        },
        {
            .event_type = AWS_MQTT5_CLET_CONNECTION_FAILURE,
            .error_code = AWS_ERROR_MQTT5_CONNACK_CONNECTION_REFUSED,
        },
        {
            .event_type = AWS_MQTT5_CLET_ATTEMPTING_CONNECT,
        },
        {
            .event_type = AWS_MQTT5_CLET_CONNECTION_FAILURE,
            .error_code = AWS_ERROR_MQTT5_CONNACK_CONNECTION_REFUSED,
        },
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

    ASSERT_SUCCESS(s_verify_reconnection_after_success_used_backoff(&test_context, RECONNECT_TEST_MAX_BACKOFF));

    aws_mqtt5_client_mock_test_fixture_clean_up(&test_context);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_client_reconnect_backoff_insufficient_reset, s_mqtt5_client_reconnect_backoff_insufficient_reset_fn)

/*
 * Fail-until-max-backoff variant, followed by a success that disconnects after enough time has passed that the backoff
 * should be reset.  Verify that the next reconnect is back to using the minimum backoff value.
 */
static int s_mqtt5_client_reconnect_backoff_sufficient_reset_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    aws_mqtt_library_init(allocator);

    struct aws_mqtt5_packet_connect_view connect_options;
    struct aws_mqtt5_client_options client_options;
    struct aws_mqtt5_mock_server_vtable server_function_table;
    s_mqtt5_client_test_init_default_options(&connect_options, &client_options, &server_function_table);

    struct aws_mqtt5_mock_server_reconnect_state mock_server_state = {
        .required_connection_failure_count = 6,
        /* slow disconnect should reset reconnect delay */
        .successful_connection_disconnect_delay_ms = RECONNECT_TEST_BACKOFF_RESET_DELAY * 2,
    };

    /* backoff delay sequence: 500, 1000, 2000, 4000, 5000, ... */
    client_options.retry_jitter_mode = AWS_EXPONENTIAL_BACKOFF_JITTER_NONE;
    client_options.min_reconnect_delay_ms = RECONNECT_TEST_MIN_BACKOFF;
    client_options.max_reconnect_delay_ms = RECONNECT_TEST_MAX_BACKOFF;
    client_options.min_connected_time_to_reset_reconnect_delay_ms = RECONNECT_TEST_BACKOFF_RESET_DELAY;

    server_function_table.packet_handlers[AWS_MQTT5_PT_CONNECT] = s_aws_mqtt5_mock_server_handle_connect_succeed_on_nth;
    server_function_table.service_task_fn = s_aws_mqtt5_mock_server_disconnect_after_n_ms;

    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options test_fixture_options = {
        .client_options = &client_options,
        .server_function_table = &server_function_table,
        .mock_server_user_data = &mock_server_state,
    };

    struct aws_mqtt5_client_mock_test_fixture test_context;
    ASSERT_SUCCESS(aws_mqtt5_client_mock_test_fixture_init(&test_context, allocator, &test_fixture_options));

    struct aws_mqtt5_client *client = test_context.client;

    ASSERT_SUCCESS(aws_mqtt5_client_start(client));

    s_wait_for_n_connection_failure_lifecycle_events(&test_context, 7);

    ASSERT_SUCCESS(aws_mqtt5_client_stop(client, NULL, NULL));

    s_wait_for_stopped_lifecycle_event(&test_context);

    /* 6 (connecting, connection failure) pairs, followed by a successful connection, then a disconnect and reconnect */
    struct aws_mqtt5_client_lifecycle_event expected_events[] = {
        {
            .event_type = AWS_MQTT5_CLET_ATTEMPTING_CONNECT,
        },
        {
            .event_type = AWS_MQTT5_CLET_CONNECTION_FAILURE,
            .error_code = AWS_ERROR_MQTT5_CONNACK_CONNECTION_REFUSED,
        },
        {
            .event_type = AWS_MQTT5_CLET_ATTEMPTING_CONNECT,
        },
        {
            .event_type = AWS_MQTT5_CLET_CONNECTION_FAILURE,
            .error_code = AWS_ERROR_MQTT5_CONNACK_CONNECTION_REFUSED,
        },
        {
            .event_type = AWS_MQTT5_CLET_ATTEMPTING_CONNECT,
        },
        {
            .event_type = AWS_MQTT5_CLET_CONNECTION_FAILURE,
            .error_code = AWS_ERROR_MQTT5_CONNACK_CONNECTION_REFUSED,
        },
        {
            .event_type = AWS_MQTT5_CLET_ATTEMPTING_CONNECT,
        },
        {
            .event_type = AWS_MQTT5_CLET_CONNECTION_FAILURE,
            .error_code = AWS_ERROR_MQTT5_CONNACK_CONNECTION_REFUSED,
        },
        {
            .event_type = AWS_MQTT5_CLET_ATTEMPTING_CONNECT,
        },
        {
            .event_type = AWS_MQTT5_CLET_CONNECTION_FAILURE,
            .error_code = AWS_ERROR_MQTT5_CONNACK_CONNECTION_REFUSED,
        },
        {
            .event_type = AWS_MQTT5_CLET_ATTEMPTING_CONNECT,
        },
        {
            .event_type = AWS_MQTT5_CLET_CONNECTION_FAILURE,
            .error_code = AWS_ERROR_MQTT5_CONNACK_CONNECTION_REFUSED,
        },
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

    ASSERT_SUCCESS(s_verify_reconnection_after_success_used_backoff(&test_context, RECONNECT_TEST_MIN_BACKOFF));

    aws_mqtt5_client_mock_test_fixture_clean_up(&test_context);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_client_reconnect_backoff_sufficient_reset, s_mqtt5_client_reconnect_backoff_sufficient_reset_fn)

static const char s_topic_filter1[] = "some/topic/but/letsmakeit/longer/soIcanfailpacketsizetests/+";

static struct aws_mqtt5_subscription_view s_subscriptions[] = {
    {
        .topic_filter =
            {
                .ptr = (uint8_t *)s_topic_filter1,
                .len = AWS_ARRAY_SIZE(s_topic_filter1) - 1,
            },
        .qos = AWS_MQTT5_QOS_AT_LEAST_ONCE,
        .no_local = false,
        .retain_as_published = false,
        .retain_handling_type = AWS_MQTT5_RHT_SEND_ON_SUBSCRIBE,
    },
};

static enum aws_mqtt5_suback_reason_code s_suback_reason_codes[] = {
    AWS_MQTT5_SARC_GRANTED_QOS_1,
};

void s_aws_mqtt5_subscribe_complete_fn(
    const struct aws_mqtt5_packet_suback_view *suback,
    int error_code,
    void *complete_ctx) {

    (void)suback;
    (void)error_code;

    struct aws_mqtt5_client_mock_test_fixture *test_context = complete_ctx;

    aws_mutex_lock(&test_context->lock);
    test_context->subscribe_complete = true;
    aws_mutex_unlock(&test_context->lock);
    aws_condition_variable_notify_all(&test_context->signal);
}

static bool s_received_suback(void *arg) {
    struct aws_mqtt5_client_mock_test_fixture *test_context = arg;

    return test_context->subscribe_complete;
}

static void s_wait_for_suback_received(struct aws_mqtt5_client_mock_test_fixture *test_context) {
    aws_mutex_lock(&test_context->lock);
    aws_condition_variable_wait_pred(&test_context->signal, &test_context->lock, s_received_suback, test_context);
    aws_mutex_unlock(&test_context->lock);
}

static int s_aws_mqtt5_server_send_suback_on_subscribe(
    void *packet,
    struct aws_mqtt5_server_mock_connection_context *connection,
    void *user_data) {
    (void)packet;
    (void)user_data;

    struct aws_mqtt5_packet_subscribe_view *subscribe_view = packet;

    struct aws_mqtt5_packet_suback_view suback_view = {
        .packet_id = subscribe_view->packet_id,
        .reason_code_count = AWS_ARRAY_SIZE(s_suback_reason_codes),
        .reason_codes = s_suback_reason_codes,
    };

    return s_aws_mqtt5_mock_server_send_packet(connection, AWS_MQTT5_PT_SUBACK, &suback_view);
}

/* Connection test where we succeed, send a SUBSCRIBE, and wait for a SUBACK */
static int s_mqtt5_client_subscribe_success_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct aws_mqtt5_packet_connect_view connect_options;
    struct aws_mqtt5_client_options client_options;
    struct aws_mqtt5_mock_server_vtable server_function_table;
    s_mqtt5_client_test_init_default_options(&connect_options, &client_options, &server_function_table);

    server_function_table.packet_handlers[AWS_MQTT5_PT_SUBSCRIBE] = s_aws_mqtt5_server_send_suback_on_subscribe;

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

    s_wait_for_connected_lifecycle_event(&test_context);

    struct aws_mqtt5_packet_subscribe_view subscribe_view = {
        .subscriptions = s_subscriptions,
        .subscription_count = AWS_ARRAY_SIZE(s_subscriptions),
    };

    struct aws_mqtt5_subscribe_completion_options completion_options = {
        .completion_callback = s_aws_mqtt5_subscribe_complete_fn,
        .completion_user_data = &test_context,
    };
    aws_mqtt5_client_subscribe(client, &subscribe_view, &completion_options);

    s_wait_for_suback_received(&test_context);

    ASSERT_SUCCESS(aws_mqtt5_client_stop(client, NULL, NULL));

    s_wait_for_stopped_lifecycle_event(&test_context);

    aws_mqtt5_client_mock_test_fixture_clean_up(&test_context);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_client_subscribe_success, s_mqtt5_client_subscribe_success_fn)

static int s_aws_mqtt5_mock_server_handle_connect_succeed_maximum_packet_size(
    void *packet,
    struct aws_mqtt5_server_mock_connection_context *connection,
    void *user_data) {
    (void)packet;
    (void)user_data;

    struct aws_mqtt5_packet_connack_view connack_view;
    AWS_ZERO_STRUCT(connack_view);

    uint32_t maximum_packet_size = 50;

    connack_view.reason_code = AWS_MQTT5_CRC_SUCCESS;
    connack_view.maximum_packet_size = &maximum_packet_size;

    return s_aws_mqtt5_mock_server_send_packet(connection, AWS_MQTT5_PT_CONNACK, &connack_view);
}

void s_aws_mqtt5_subscribe_complete_packet_size_too_small_fn(
    const struct aws_mqtt5_packet_suback_view *suback,
    int error_code,
    void *complete_ctx) {

    AWS_FATAL_ASSERT(suback == NULL);
    AWS_FATAL_ASSERT(AWS_ERROR_MQTT5_PACKET_VALIDATION == error_code);

    struct aws_mqtt5_client_mock_test_fixture *test_context = complete_ctx;

    aws_mutex_lock(&test_context->lock);
    test_context->subscribe_complete = true;
    aws_mutex_unlock(&test_context->lock);
    aws_condition_variable_notify_all(&test_context->signal);
}

static int s_mqtt5_client_subscribe_fail_packet_too_big_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct aws_mqtt5_packet_connect_view connect_options;
    struct aws_mqtt5_client_options client_options;
    struct aws_mqtt5_mock_server_vtable server_function_table;
    s_mqtt5_client_test_init_default_options(&connect_options, &client_options, &server_function_table);

    server_function_table.packet_handlers[AWS_MQTT5_PT_CONNECT] =
        s_aws_mqtt5_mock_server_handle_connect_succeed_maximum_packet_size;

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

    s_wait_for_connected_lifecycle_event(&test_context);

    struct aws_mqtt5_packet_subscribe_view subscribe_view = {
        .subscriptions = s_subscriptions,
        .subscription_count = AWS_ARRAY_SIZE(s_subscriptions),
    };

    struct aws_mqtt5_subscribe_completion_options completion_options = {
        .completion_callback = s_aws_mqtt5_subscribe_complete_packet_size_too_small_fn,
        .completion_user_data = &test_context,
    };
    aws_mqtt5_client_subscribe(client, &subscribe_view, &completion_options);

    s_wait_for_suback_received(&test_context);

    ASSERT_SUCCESS(aws_mqtt5_client_stop(client, NULL, NULL));

    s_wait_for_stopped_lifecycle_event(&test_context);

    aws_mqtt5_client_mock_test_fixture_clean_up(&test_context);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_client_subscribe_fail_packet_too_big, s_mqtt5_client_subscribe_fail_packet_too_big_fn)

static void s_aws_mqtt5_disconnect_failure_completion_fn(int error_code, void *complete_ctx) {
    AWS_FATAL_ASSERT(error_code == AWS_ERROR_MQTT5_PACKET_VALIDATION);

    s_on_disconnect_completion(error_code, complete_ctx);
}

static int s_mqtt5_client_disconnect_fail_packet_too_big_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct aws_mqtt5_packet_connect_view connect_options;
    struct aws_mqtt5_client_options client_options;
    struct aws_mqtt5_mock_server_vtable server_function_table;
    s_mqtt5_client_test_init_default_options(&connect_options, &client_options, &server_function_table);

    server_function_table.packet_handlers[AWS_MQTT5_PT_CONNECT] =
        s_aws_mqtt5_mock_server_handle_connect_succeed_maximum_packet_size;

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

    s_wait_for_connected_lifecycle_event(&test_context);

    struct aws_byte_cursor long_reason_string_cursor = aws_byte_cursor_from_c_str(
        "Not valid because it includes the 0-terminator but we don't check utf-8 so who cares");

    struct aws_mqtt5_packet_disconnect_view disconnect_view = {
        .reason_string = &long_reason_string_cursor,
    };

    struct aws_mqtt5_disconnect_completion_options completion_options = {
        .completion_callback = s_aws_mqtt5_disconnect_failure_completion_fn,
        .completion_user_data = &test_context,
    };

    ASSERT_SUCCESS(aws_mqtt5_client_stop(client, &disconnect_view, &completion_options));

    s_wait_for_disconnect_completion(&test_context);
    s_wait_for_stopped_lifecycle_event(&test_context);

    aws_mqtt5_client_mock_test_fixture_clean_up(&test_context);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_client_disconnect_fail_packet_too_big, s_mqtt5_client_disconnect_fail_packet_too_big_fn)

static uint8_t s_topic[] = "Hello/world";

#define RECEIVE_MAXIMUM_PUBLISH_COUNT 30
#define TEST_RECEIVE_MAXIMUM 3

static int s_aws_mqtt5_mock_server_handle_connect_succeed_receive_maximum(
    void *packet,
    struct aws_mqtt5_server_mock_connection_context *connection,
    void *user_data) {
    (void)packet;
    (void)user_data;

    struct aws_mqtt5_packet_connack_view connack_view;
    AWS_ZERO_STRUCT(connack_view);

    uint16_t receive_maximum = TEST_RECEIVE_MAXIMUM;

    connack_view.reason_code = AWS_MQTT5_CRC_SUCCESS;
    connack_view.receive_maximum = &receive_maximum;

    return s_aws_mqtt5_mock_server_send_packet(connection, AWS_MQTT5_PT_CONNACK, &connack_view);
}

struct send_puback_task {
    struct aws_allocator *allocator;
    struct aws_task task;
    struct aws_mqtt5_server_mock_connection_context *connection;
    uint16_t packet_id;
};

void send_puback_fn(struct aws_task *task, void *arg, enum aws_task_status status) {
    struct send_puback_task *puback_response_task = arg;
    if (status == AWS_TASK_STATUS_CANCELED) {
        goto done;
    }

    struct aws_mqtt5_client_mock_test_fixture *test_fixture = puback_response_task->connection->test_fixture;

    aws_mutex_lock(&test_fixture->lock);
    --test_fixture->server_current_inflight_publishes;
    aws_mutex_unlock(&test_fixture->lock);

    struct aws_mqtt5_packet_puback_view puback_view = {
        .packet_id = puback_response_task->packet_id,
    };

    s_aws_mqtt5_mock_server_send_packet(puback_response_task->connection, AWS_MQTT5_PT_PUBACK, &puback_view);

done:

    aws_mem_release(puback_response_task->allocator, puback_response_task);
}

static int s_aws_mqtt5_mock_server_handle_publish_delayed_puback(
    void *packet,
    struct aws_mqtt5_server_mock_connection_context *connection,
    void *user_data) {

    (void)user_data;

    struct aws_mqtt5_client_mock_test_fixture *test_fixture = connection->test_fixture;

    aws_mutex_lock(&test_fixture->lock);
    ++test_fixture->server_current_inflight_publishes;
    test_fixture->server_maximum_inflight_publishes =
        aws_max_u32(test_fixture->server_current_inflight_publishes, test_fixture->server_maximum_inflight_publishes);
    aws_mutex_unlock(&test_fixture->lock);

    struct aws_mqtt5_packet_publish_view *publish_view = packet;

    struct send_puback_task *puback_response_task =
        aws_mem_calloc(connection->allocator, 1, sizeof(struct send_puback_task));
    puback_response_task->allocator = connection->allocator;
    puback_response_task->connection = connection;
    puback_response_task->packet_id = publish_view->packet_id;

    aws_task_init(&puback_response_task->task, send_puback_fn, puback_response_task, "delayed_puback_response");

    struct aws_event_loop *event_loop = aws_channel_get_event_loop(connection->slot->channel);

    uint64_t now = 0;
    aws_event_loop_current_clock_time(event_loop, &now);

    uint64_t min_delay = aws_timestamp_convert(250, AWS_TIMESTAMP_MILLIS, AWS_TIMESTAMP_NANOS, NULL);
    uint64_t max_delay = aws_timestamp_convert(500, AWS_TIMESTAMP_MILLIS, AWS_TIMESTAMP_NANOS, NULL);
    uint64_t delay_nanos = aws_mqtt5_client_random_in_range(min_delay, max_delay);

    uint64_t puback_time = aws_add_u64_saturating(now, delay_nanos);
    aws_event_loop_schedule_task_future(event_loop, &puback_response_task->task, puback_time);

    return AWS_OP_SUCCESS;
}

static void s_receive_maximum_publish_completion_fn(
    const struct aws_mqtt5_packet_puback_view *puback,
    int error_code,
    void *complete_ctx) {

    (void)puback;
    (void)error_code;

    struct aws_mqtt5_client_mock_test_fixture *test_context = complete_ctx;

    uint64_t now = 0;
    aws_high_res_clock_get_ticks(&now);

    aws_mutex_lock(&test_context->lock);

    ++test_context->total_pubacks_received;
    if (error_code == AWS_ERROR_SUCCESS && puback->reason_code < 128) {
        ++test_context->successful_pubacks_received;
    }

    aws_mutex_unlock(&test_context->lock);
    aws_condition_variable_notify_all(&test_context->signal);
}

static bool s_received_n_successful_publishes(void *arg) {
    struct aws_mqtt5_client_test_wait_for_n_context *context = arg;
    struct aws_mqtt5_client_mock_test_fixture *test_fixture = context->test_fixture;

    return test_fixture->successful_pubacks_received >= context->required_event_count;
}

static void s_wait_for_n_successful_publishes(struct aws_mqtt5_client_test_wait_for_n_context *context) {

    struct aws_mqtt5_client_mock_test_fixture *test_context = context->test_fixture;
    aws_mutex_lock(&test_context->lock);
    aws_condition_variable_wait_pred(
        &test_context->signal, &test_context->lock, s_received_n_successful_publishes, context);
    aws_mutex_unlock(&test_context->lock);
}

static int s_mqtt5_client_flow_control_receive_maximum_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct aws_mqtt5_packet_connect_view connect_options;
    struct aws_mqtt5_client_options client_options;
    struct aws_mqtt5_mock_server_vtable server_function_table;
    s_mqtt5_client_test_init_default_options(&connect_options, &client_options, &server_function_table);

    /* send delayed pubacks */
    server_function_table.packet_handlers[AWS_MQTT5_PT_PUBLISH] = s_aws_mqtt5_mock_server_handle_publish_delayed_puback;

    /* establish a low receive maximum */
    server_function_table.packet_handlers[AWS_MQTT5_PT_CONNECT] =
        s_aws_mqtt5_mock_server_handle_connect_succeed_receive_maximum;

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

    s_wait_for_connected_lifecycle_event(&test_context);

    /* send a bunch of publishes */
    for (size_t i = 0; i < RECEIVE_MAXIMUM_PUBLISH_COUNT; ++i) {
        struct aws_mqtt5_packet_publish_view qos1_publish_view = {
            .qos = AWS_MQTT5_QOS_AT_LEAST_ONCE,
            .topic =
                {
                    .ptr = s_topic,
                    .len = AWS_ARRAY_SIZE(s_topic) - 1,
                },
        };

        struct aws_mqtt5_publish_completion_options completion_options = {
            .completion_callback = s_receive_maximum_publish_completion_fn,
            .completion_user_data = &test_context,
        };

        ASSERT_SUCCESS(aws_mqtt5_client_publish(client, &qos1_publish_view, &completion_options));
    }

    /* wait for all publishes to succeed */
    struct aws_mqtt5_client_test_wait_for_n_context wait_context = {
        .test_fixture = &test_context,
        .required_event_count = RECEIVE_MAXIMUM_PUBLISH_COUNT,
    };
    s_wait_for_n_successful_publishes(&wait_context);

    ASSERT_SUCCESS(aws_mqtt5_client_stop(client, NULL, NULL));

    s_wait_for_stopped_lifecycle_event(&test_context);

    /*
     * verify that the maximum number of in-progress qos1 publishes on the server was never more than what the
     * server said its maximum was
     */
    aws_mutex_lock(&test_context.lock);
    uint32_t max_inflight_publishes = test_context.server_maximum_inflight_publishes;
    aws_mutex_unlock(&test_context.lock);

    ASSERT_TRUE(max_inflight_publishes <= TEST_RECEIVE_MAXIMUM);

    aws_mqtt5_client_mock_test_fixture_clean_up(&test_context);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_client_flow_control_receive_maximum, s_mqtt5_client_flow_control_receive_maximum_fn)

static void s_publish_timeout_publish_completion_fn(
    const struct aws_mqtt5_packet_puback_view *puback,
    int error_code,
    void *complete_ctx) {
    (void)puback;

    struct aws_mqtt5_client_mock_test_fixture *test_context = complete_ctx;

    aws_mutex_lock(&test_context->lock);

    if (error_code == AWS_ERROR_MQTT_TIMEOUT) {
        ++test_context->timeouts_received;
    }

    aws_mutex_unlock(&test_context->lock);
    aws_condition_variable_notify_all(&test_context->signal);
}

static bool s_received_n_publish_timeouts(void *arg) {
    struct aws_mqtt5_client_test_wait_for_n_context *context = arg;
    struct aws_mqtt5_client_mock_test_fixture *test_fixture = context->test_fixture;

    return test_fixture->timeouts_received >= context->required_event_count;
}

static void s_wait_for_n_successful_publish_timeouts(struct aws_mqtt5_client_test_wait_for_n_context *context) {
    struct aws_mqtt5_client_mock_test_fixture *test_context = context->test_fixture;
    aws_mutex_lock(&test_context->lock);
    aws_condition_variable_wait_pred(
        &test_context->signal, &test_context->lock, s_received_n_publish_timeouts, context);
    aws_mutex_unlock(&test_context->lock);
}

static bool s_received_n_timeout_publish_packets(void *arg) {
    struct aws_mqtt5_client_test_wait_for_n_context *context = arg;
    struct aws_mqtt5_client_mock_test_fixture *test_fixture = context->test_fixture;

    return test_fixture->publishes_received >= context->required_event_count;
}

static int s_aws_mqtt5_mock_server_handle_timeout_publish(
    void *packet,
    struct aws_mqtt5_server_mock_connection_context *connection,
    void *user_data) {

    (void)user_data;
    ++connection->test_fixture->publishes_received;

    return AWS_OP_SUCCESS;
}

static void s_wait_for_n_successful_server_timeout_publishes(struct aws_mqtt5_client_test_wait_for_n_context *context) {
    struct aws_mqtt5_client_mock_test_fixture *test_context = context->test_fixture;
    aws_mutex_lock(&test_context->lock);
    aws_condition_variable_wait_pred(
        &test_context->signal, &test_context->lock, s_received_n_timeout_publish_packets, context);
    aws_mutex_unlock(&test_context->lock);
}

/*
 * Test that not receiving a PUBACK causes the PUBLISH waiting for the PUBACK to timeout
 */
static int s_mqtt5_client_publish_timeout_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct aws_mqtt5_packet_connect_view connect_options;
    struct aws_mqtt5_client_options client_options;
    struct aws_mqtt5_mock_server_vtable server_function_table;
    s_mqtt5_client_test_init_default_options(&connect_options, &client_options, &server_function_table);

    server_function_table.packet_handlers[AWS_MQTT5_PT_PUBLISH] = s_aws_mqtt5_mock_server_handle_timeout_publish;

    /* fast publish timeout */
    client_options.timout_seconds = 5;

    struct aws_mqtt5_client_mock_test_fixture test_context;

    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options test_fixture_options = {
        .client_options = &client_options,
        .server_function_table = &server_function_table,
    };

    ASSERT_SUCCESS(aws_mqtt5_client_mock_test_fixture_init(&test_context, allocator, &test_fixture_options));

    struct aws_mqtt5_client *client = test_context.client;
    ASSERT_SUCCESS(aws_mqtt5_client_start(client));

    s_wait_for_connected_lifecycle_event(&test_context);

    struct aws_mqtt5_client_test_wait_for_n_context wait_context = {
        .test_fixture = &test_context,
        .required_event_count = aws_mqtt5_client_random_in_range(3, 20),
    };

    struct aws_mqtt5_publish_completion_options completion_options = {
        .completion_callback = &s_publish_timeout_publish_completion_fn,
        .completion_user_data = &test_context,
    };

    struct aws_mqtt5_packet_publish_view packet_publish_view = {
        .qos = AWS_MQTT5_QOS_AT_LEAST_ONCE,
        .topic =
            {
                .ptr = s_topic,
                .len = AWS_ARRAY_SIZE(s_topic) - 1,
            },
    };

    /* Send semi-random number of publishes that will not be acked */
    for (size_t publish_count = 0; publish_count < wait_context.required_event_count; ++publish_count) {
        ASSERT_SUCCESS(aws_mqtt5_client_publish(client, &packet_publish_view, &completion_options));
    }

    s_wait_for_n_successful_server_timeout_publishes(&wait_context);

    size_t unacked_count = 0;
    unacked_count = aws_hash_table_get_entry_count(&client->operational_state.unacked_operations_table);
    ASSERT_INT_EQUALS(wait_context.required_event_count, unacked_count);

    s_wait_for_n_successful_publish_timeouts(&wait_context);

    unacked_count = aws_hash_table_get_entry_count(&client->operational_state.unacked_operations_table);
    ASSERT_INT_EQUALS(0, unacked_count);

    ASSERT_SUCCESS(aws_mqtt5_client_stop(client, NULL, NULL));

    s_wait_for_stopped_lifecycle_event(&test_context);

    aws_mqtt5_client_mock_test_fixture_clean_up(&test_context);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_client_publish_timeout, s_mqtt5_client_publish_timeout_fn)

static int s_aws_mqtt5_mock_server_handle_publish_puback(
    void *packet,
    struct aws_mqtt5_server_mock_connection_context *connection,
    void *user_data) {

    (void)user_data;

    struct aws_mqtt5_packet_publish_view *publish_view = packet;
    if (publish_view->qos != AWS_MQTT5_QOS_AT_LEAST_ONCE) {
        return AWS_OP_SUCCESS;
    }

    struct aws_mqtt5_packet_puback_view puback_view = {
        .packet_id = publish_view->packet_id,
    };

    return s_aws_mqtt5_mock_server_send_packet(connection, AWS_MQTT5_PT_PUBACK, &puback_view);
}

#define IOT_CORE_THROUGHPUT_PACKETS 21

static int s_do_iot_core_throughput_test(struct aws_allocator *allocator, bool use_iot_core_limits) {

    struct aws_mqtt5_packet_connect_view connect_options;
    struct aws_mqtt5_client_options client_options;
    struct aws_mqtt5_mock_server_vtable server_function_table;
    s_mqtt5_client_test_init_default_options(&connect_options, &client_options, &server_function_table);

    /* send pubacks */
    server_function_table.packet_handlers[AWS_MQTT5_PT_PUBLISH] = s_aws_mqtt5_mock_server_handle_publish_puback;

    if (use_iot_core_limits) {
        client_options.extended_validation_and_flow_control_options = AWS_MQTT5_EVAFCO_AWS_IOT_CORE_DEFAULTS;
    }

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

    s_wait_for_connected_lifecycle_event(&test_context);

    /* send a bunch of large publishes */
    uint8_t packet_payload[127 * 1024];
    aws_secure_zero(packet_payload, AWS_ARRAY_SIZE(packet_payload));

    for (size_t i = 0; i < IOT_CORE_THROUGHPUT_PACKETS; ++i) {
        struct aws_mqtt5_packet_publish_view qos1_publish_view = {
            .qos = AWS_MQTT5_QOS_AT_LEAST_ONCE,
            .topic =
                {
                    .ptr = s_topic,
                    .len = AWS_ARRAY_SIZE(s_topic) - 1,
                },
            .payload = aws_byte_cursor_from_array(packet_payload, AWS_ARRAY_SIZE(packet_payload)),
        };

        struct aws_mqtt5_publish_completion_options completion_options = {
            .completion_callback = s_receive_maximum_publish_completion_fn, /* can reuse receive_maximum callback */
            .completion_user_data = &test_context,
        };

        ASSERT_SUCCESS(aws_mqtt5_client_publish(client, &qos1_publish_view, &completion_options));
    }

    /* wait for all publishes to succeed */
    struct aws_mqtt5_client_test_wait_for_n_context wait_context = {
        .test_fixture = &test_context,
        .required_event_count = IOT_CORE_THROUGHPUT_PACKETS,
    };
    s_wait_for_n_successful_publishes(&wait_context);

    ASSERT_SUCCESS(aws_mqtt5_client_stop(client, NULL, NULL));

    s_wait_for_stopped_lifecycle_event(&test_context);

    aws_mqtt5_client_mock_test_fixture_clean_up(&test_context);

    return AWS_OP_SUCCESS;
}

static int s_mqtt5_client_flow_control_iot_core_throughput_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    uint64_t start_time1 = 0;
    aws_high_res_clock_get_ticks(&start_time1);

    ASSERT_SUCCESS(s_do_iot_core_throughput_test(allocator, false));

    uint64_t end_time1 = 0;
    aws_high_res_clock_get_ticks(&end_time1);

    uint64_t test_time1 = end_time1 - start_time1;

    uint64_t start_time2 = 0;
    aws_high_res_clock_get_ticks(&start_time2);

    ASSERT_SUCCESS(s_do_iot_core_throughput_test(allocator, true));

    uint64_t end_time2 = 0;
    aws_high_res_clock_get_ticks(&end_time2);

    uint64_t test_time2 = end_time2 - start_time2;

    /* We expect the unthrottled test to complete quickly */
    ASSERT_TRUE(test_time1 < AWS_TIMESTAMP_NANOS);

    /*
     * We expect the throttled version to take around 5 seconds, since we're sending 21 almost-max size (127k) packets
     * against a limit of 512KB/s.  Since the packets are submitted immediately on CONNACK, the rate limiter
     * token bucket is starting at zero and so will give us immediate throttling.
     */
    ASSERT_TRUE(test_time2 > 5 * (uint64_t)AWS_TIMESTAMP_NANOS);

    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_client_flow_control_iot_core_throughput, s_mqtt5_client_flow_control_iot_core_throughput_fn)

#define IOT_CORE_PUBLISH_TPS_PACKETS 650

static int s_do_iot_core_publish_tps_test(struct aws_allocator *allocator, bool use_iot_core_limits) {

    struct aws_mqtt5_packet_connect_view connect_options;
    struct aws_mqtt5_client_options client_options;
    struct aws_mqtt5_mock_server_vtable server_function_table;
    s_mqtt5_client_test_init_default_options(&connect_options, &client_options, &server_function_table);

    /* send pubacks */
    server_function_table.packet_handlers[AWS_MQTT5_PT_PUBLISH] = s_aws_mqtt5_mock_server_handle_publish_puback;

    if (use_iot_core_limits) {
        client_options.extended_validation_and_flow_control_options = AWS_MQTT5_EVAFCO_AWS_IOT_CORE_DEFAULTS;
    }

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

    s_wait_for_connected_lifecycle_event(&test_context);

    /* send a bunch of tiny publishes */
    for (size_t i = 0; i < IOT_CORE_PUBLISH_TPS_PACKETS; ++i) {
        struct aws_mqtt5_packet_publish_view qos1_publish_view = {
            .qos = AWS_MQTT5_QOS_AT_LEAST_ONCE,
            .topic =
                {
                    .ptr = s_topic,
                    .len = AWS_ARRAY_SIZE(s_topic) - 1,
                },
        };

        struct aws_mqtt5_publish_completion_options completion_options = {
            .completion_callback = s_receive_maximum_publish_completion_fn, /* can reuse receive_maximum callback */
            .completion_user_data = &test_context,
        };

        ASSERT_SUCCESS(aws_mqtt5_client_publish(client, &qos1_publish_view, &completion_options));
    }

    /* wait for all publishes to succeed */
    struct aws_mqtt5_client_test_wait_for_n_context wait_context = {
        .test_fixture = &test_context,
        .required_event_count = IOT_CORE_PUBLISH_TPS_PACKETS,
    };
    s_wait_for_n_successful_publishes(&wait_context);

    ASSERT_SUCCESS(aws_mqtt5_client_stop(client, NULL, NULL));

    s_wait_for_stopped_lifecycle_event(&test_context);

    aws_mqtt5_client_mock_test_fixture_clean_up(&test_context);

    return AWS_OP_SUCCESS;
}

static int s_mqtt5_client_flow_control_iot_core_publish_tps_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    uint64_t start_time1 = 0;
    aws_high_res_clock_get_ticks(&start_time1);

    ASSERT_SUCCESS(s_do_iot_core_publish_tps_test(allocator, false));

    uint64_t end_time1 = 0;
    aws_high_res_clock_get_ticks(&end_time1);

    uint64_t test_time1 = end_time1 - start_time1;

    uint64_t start_time2 = 0;
    aws_high_res_clock_get_ticks(&start_time2);

    ASSERT_SUCCESS(s_do_iot_core_publish_tps_test(allocator, true));

    uint64_t end_time2 = 0;
    aws_high_res_clock_get_ticks(&end_time2);

    uint64_t test_time2 = end_time2 - start_time2;

    /* We expect the unthrottled test to complete quickly */
    ASSERT_TRUE(test_time1 < AWS_TIMESTAMP_NANOS);

    /*
     * We expect the throttled version to take over 6 seconds, since we're sending over 650 tiny publish packets
     * against a limit of 100TPS.  Since the packets are submitted immediately on CONNACK, the rate limiter
     * token bucket is starting at zero and so will give us immediate throttling.
     */
    ASSERT_TRUE(test_time2 > 6 * (uint64_t)AWS_TIMESTAMP_NANOS);

    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_client_flow_control_iot_core_publish_tps, s_mqtt5_client_flow_control_iot_core_publish_tps_fn)

static int s_aws_mqtt5_mock_server_handle_connect_honor_session(
    void *packet,
    struct aws_mqtt5_server_mock_connection_context *connection,
    void *user_data) {
    (void)packet;
    (void)user_data;

    struct aws_mqtt5_packet_connect_view *connect_packet = packet;

    struct aws_mqtt5_packet_connack_view connack_view;
    AWS_ZERO_STRUCT(connack_view);

    connack_view.reason_code = AWS_MQTT5_CRC_SUCCESS;
    connack_view.session_present = !connect_packet->clean_start;

    return s_aws_mqtt5_mock_server_send_packet(connection, AWS_MQTT5_PT_CONNACK, &connack_view);
}

struct aws_mqtt5_wait_for_n_lifecycle_events_context {
    struct aws_mqtt5_client_mock_test_fixture *test_fixture;
    enum aws_mqtt5_client_lifecycle_event_type event_type;
    size_t expected_event_count;
};

static bool s_received_n_lifecycle_events(void *arg) {
    struct aws_mqtt5_wait_for_n_lifecycle_events_context *context = arg;
    struct aws_mqtt5_client_mock_test_fixture *test_fixture = context->test_fixture;

    size_t matching_events = 0;
    size_t event_count = aws_array_list_length(&test_fixture->lifecycle_events);
    for (size_t i = 0; i < event_count; ++i) {
        struct aws_mqtt5_lifecycle_event_record *record = NULL;
        aws_array_list_get_at(&test_fixture->lifecycle_events, &record, i);

        if (record->event.event_type == context->event_type) {
            ++matching_events;
        }
    }

    return matching_events >= context->expected_event_count;
}

static void s_wait_for_n_lifecycle_events(
    struct aws_mqtt5_client_mock_test_fixture *test_fixture,
    enum aws_mqtt5_client_lifecycle_event_type event_type,
    size_t expected_event_count) {
    struct aws_mqtt5_wait_for_n_lifecycle_events_context wait_context = {
        .test_fixture = test_fixture,
        .event_type = event_type,
        .expected_event_count = expected_event_count,
    };

    aws_mutex_lock(&test_fixture->lock);
    aws_condition_variable_wait_pred(
        &test_fixture->signal, &test_fixture->lock, s_received_n_lifecycle_events, &wait_context);
    aws_mutex_unlock(&test_fixture->lock);
}

static bool s_compute_expected_rejoined_session(
    enum aws_mqtt5_client_session_behavior_type session_behavior,
    size_t connect_index) {
    switch (session_behavior) {
        case AWS_MQTT5_CSBT_CLEAN:
            return false;

        case AWS_MQTT5_CSBT_REJOIN_POST_SUCCESS:
            return connect_index > 0;

        case AWS_MQTT5_CSBT_REJOIN_ALWAYS:
        default:
            return true;
    }
}

static int s_aws_mqtt5_client_test_init_resume_session_connect_storage(
    struct aws_mqtt5_packet_connect_storage *storage,
    struct aws_allocator *allocator) {
    struct aws_mqtt5_packet_connect_view connect_view = {
        .keep_alive_interval_seconds = 30,
        .client_id = aws_byte_cursor_from_string(s_client_id),
        .clean_start = false,
    };

    return aws_mqtt5_packet_connect_storage_init(storage, allocator, &connect_view);
}

#define SESSION_RESUMPTION_CONNECT_COUNT 10

static int s_do_mqtt5_client_session_resumption_test(
    struct aws_allocator *allocator,
    enum aws_mqtt5_client_session_behavior_type session_behavior) {
    aws_mqtt_library_init(allocator);

    struct aws_mqtt5_packet_connect_view connect_options;
    struct aws_mqtt5_client_options client_options;
    struct aws_mqtt5_mock_server_vtable server_function_table;
    s_mqtt5_client_test_init_default_options(&connect_options, &client_options, &server_function_table);

    client_options.session_behavior = session_behavior;

    server_function_table.packet_handlers[AWS_MQTT5_PT_CONNECT] = s_aws_mqtt5_mock_server_handle_connect_honor_session;

    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options test_fixture_options = {
        .client_options = &client_options,
        .server_function_table = &server_function_table,
    };

    struct aws_mqtt5_client_mock_test_fixture test_context;
    ASSERT_SUCCESS(aws_mqtt5_client_mock_test_fixture_init(&test_context, allocator, &test_fixture_options));

    struct aws_mqtt5_client *client = test_context.client;

    for (size_t i = 0; i < SESSION_RESUMPTION_CONNECT_COUNT; ++i) {
        ASSERT_SUCCESS(aws_mqtt5_client_start(client));
        s_wait_for_n_lifecycle_events(&test_context, AWS_MQTT5_CLET_CONNECTION_SUCCESS, i + 1);

        /* not technically truly safe to query depending on memory model.  Remove if it becomes a problem. */
        bool expected_rejoined_session = s_compute_expected_rejoined_session(session_behavior, i);
        ASSERT_INT_EQUALS(expected_rejoined_session, client->negotiated_settings.rejoined_session);

        ASSERT_SUCCESS(aws_mqtt5_client_stop(client, NULL, NULL));
        s_wait_for_n_lifecycle_events(&test_context, AWS_MQTT5_CLET_STOPPED, i + 1);
    }

    struct aws_mqtt5_packet_connect_storage clean_start_connect_storage;
    ASSERT_SUCCESS(s_aws_mqtt5_client_test_init_default_connect_storage(&clean_start_connect_storage, allocator));

    struct aws_mqtt5_packet_connect_storage resume_session_connect_storage;
    ASSERT_SUCCESS(
        s_aws_mqtt5_client_test_init_resume_session_connect_storage(&resume_session_connect_storage, allocator));

    struct aws_mqtt5_mock_server_packet_record expected_packets[SESSION_RESUMPTION_CONNECT_COUNT];
    for (size_t i = 0; i < SESSION_RESUMPTION_CONNECT_COUNT; ++i) {
        expected_packets[i].packet_type = AWS_MQTT5_PT_CONNECT;
        if (s_compute_expected_rejoined_session(session_behavior, i)) {
            expected_packets[i].packet_storage = &resume_session_connect_storage;
        } else {
            expected_packets[i].packet_storage = &clean_start_connect_storage;
        }
    }

    ASSERT_SUCCESS(
        s_verify_received_packet_sequence(&test_context, expected_packets, AWS_ARRAY_SIZE(expected_packets)));

    aws_mqtt5_packet_connect_storage_clean_up(&clean_start_connect_storage);
    aws_mqtt5_packet_connect_storage_clean_up(&resume_session_connect_storage);
    aws_mqtt5_client_mock_test_fixture_clean_up(&test_context);

    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

static int s_mqtt5_client_session_resumption_clean_start_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_do_mqtt5_client_session_resumption_test(allocator, AWS_MQTT5_CSBT_CLEAN));

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_client_session_resumption_clean_start, s_mqtt5_client_session_resumption_clean_start_fn)

static int s_mqtt5_client_session_resumption_always_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_do_mqtt5_client_session_resumption_test(allocator, AWS_MQTT5_CSBT_REJOIN_ALWAYS));

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_client_session_resumption_always, s_mqtt5_client_session_resumption_always_fn)

static int s_mqtt5_client_session_resumption_post_success_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_do_mqtt5_client_session_resumption_test(allocator, AWS_MQTT5_CSBT_REJOIN_POST_SUCCESS));

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_client_session_resumption_post_success, s_mqtt5_client_session_resumption_post_success_fn)
