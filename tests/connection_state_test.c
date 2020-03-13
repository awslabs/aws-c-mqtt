/*
 * Copyright 2010-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

#include <aws/mqtt/private/client_impl.h>

#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/io/host_resolver.h>

#include <aws/common/clock.h>
#include <aws/common/condition_variable.h>

#include <aws/testing/aws_test_harness.h>

#include "mqtt_mock_server_handler.h"

#ifdef _WIN32
#    define LOCAL_SOCK_TEST_PATTERN "\\\\.\\pipe\\testsock%llu"
#else
#    define LOCAL_SOCK_TEST_PATTERN "testsock%llu.sock"
#endif

static const int TEST_LOG_SUBJECT = 60000;

struct mqtt_connection_state_test {
    struct aws_allocator *allocator;
    struct aws_channel *server_channel;
    struct aws_channel_handler *test_channel_handler;
    struct aws_client_bootstrap *client_bootstrap;
    struct aws_server_bootstrap *server_bootstrap;
    struct aws_event_loop_group el_group;
    struct aws_host_resolver host_resolver;
    struct aws_socket_endpoint endpoint;
    struct aws_socket *listener;
    struct aws_mqtt_client mqtt_client;
    struct aws_mqtt_client_connection *mqtt_connection;
    struct aws_socket_options socket_options;
    bool session_present;
    bool connection_completed;
    bool client_disconnect_completed;
    bool server_disconnect_completed;
    bool connection_interrupted;
    bool connection_resumed;
    bool subscribe_completed;
    bool listener_destroyed;
    int interruption_error;
    enum aws_mqtt_connect_return_code mqtt_return_code;
    int error;
    struct aws_condition_variable cvar;
    struct aws_mutex lock;
    struct aws_array_list published_messages;
    size_t publishes_received;
    size_t expected_publishes;
    size_t ops_completed;
    size_t expected_ops_completed;
};

static struct mqtt_connection_state_test test_data = {0};

static void s_on_incoming_channel_setup_fn(
    struct aws_server_bootstrap *bootstrap,
    int error_code,
    struct aws_channel *channel,
    void *user_data) {
    (void)bootstrap;
    struct mqtt_connection_state_test *state_test_data = user_data;

    state_test_data->error = error_code;

    if (!error_code) {
        aws_mutex_lock(&state_test_data->lock);
        state_test_data->server_disconnect_completed = false;
        aws_mutex_unlock(&state_test_data->lock);
        AWS_LOGF_DEBUG(TEST_LOG_SUBJECT, "server channel setup completed");

        state_test_data->server_channel = channel;
        struct aws_channel_slot *test_handler_slot = aws_channel_slot_new(channel);
        aws_channel_slot_insert_end(channel, test_handler_slot);
        s_mqtt_mock_server_handler_update_slot(state_test_data->test_channel_handler, test_handler_slot);
        aws_channel_slot_set_handler(test_handler_slot, state_test_data->test_channel_handler);
    }
}

static void s_on_incoming_channel_shutdown_fn(
    struct aws_server_bootstrap *bootstrap,
    int error_code,
    struct aws_channel *channel,
    void *user_data) {
    (void)bootstrap;
    (void)error_code;
    (void)channel;
    struct mqtt_connection_state_test *state_test_data = user_data;
    aws_mutex_lock(&state_test_data->lock);
    state_test_data->server_disconnect_completed = true;
    AWS_LOGF_DEBUG(TEST_LOG_SUBJECT, "server channel shutdown completed");
    aws_mutex_unlock(&state_test_data->lock);
    aws_condition_variable_notify_one(&state_test_data->cvar);
}

static void s_on_listener_destroy(struct aws_server_bootstrap *bootstrap, void *user_data) {
    (void)bootstrap;
    struct mqtt_connection_state_test *state_test_data = user_data;
    aws_mutex_lock(&state_test_data->lock);
    state_test_data->listener_destroyed = true;
    aws_mutex_unlock(&state_test_data->lock);
    aws_condition_variable_notify_one(&state_test_data->cvar);
}

static bool s_is_listener_destroyed(void *arg) {
    struct mqtt_connection_state_test *state_test_data = arg;
    return state_test_data->listener_destroyed;
}

static void s_wait_on_listener_cleanup(struct mqtt_connection_state_test *state_test_data) {
    aws_mutex_lock(&state_test_data->lock);
    aws_condition_variable_wait_pred(
        &state_test_data->cvar, &state_test_data->lock, s_is_listener_destroyed, state_test_data);
    aws_mutex_unlock(&state_test_data->lock);
}

static void s_on_connection_interrupted(struct aws_mqtt_client_connection *connection, int error_code, void *userdata) {
    (void)connection;
    (void)error_code;
    struct mqtt_connection_state_test *state_test_data = userdata;

    aws_mutex_lock(&state_test_data->lock);
    state_test_data->connection_interrupted = true;
    state_test_data->interruption_error = error_code;
    aws_mutex_unlock(&state_test_data->lock);
    AWS_LOGF_DEBUG(TEST_LOG_SUBJECT, "connection interrupted");
    aws_condition_variable_notify_one(&state_test_data->cvar);
}

static bool s_is_connection_interrupted(void *arg) {
    struct mqtt_connection_state_test *state_test_data = arg;
    return state_test_data->connection_interrupted;
}

static void s_wait_for_interrupt_to_complete(struct mqtt_connection_state_test *state_test_data) {
    aws_mutex_lock(&state_test_data->lock);
    aws_condition_variable_wait_pred(
        &state_test_data->cvar, &state_test_data->lock, s_is_connection_interrupted, state_test_data);
    state_test_data->connection_interrupted = false;
    aws_mutex_unlock(&state_test_data->lock);
}

static void s_on_connection_resumed(
    struct aws_mqtt_client_connection *connection,
    enum aws_mqtt_connect_return_code return_code,
    bool session_present,
    void *userdata) {
    (void)connection;
    (void)return_code;
    (void)session_present;
    AWS_LOGF_DEBUG(TEST_LOG_SUBJECT, "reconnect completed");

    struct mqtt_connection_state_test *state_test_data = userdata;

    aws_mutex_lock(&state_test_data->lock);
    state_test_data->connection_resumed = true;
    aws_mutex_unlock(&state_test_data->lock);
    aws_condition_variable_notify_one(&state_test_data->cvar);
}

static bool s_is_connection_resumed(void *arg) {
    struct mqtt_connection_state_test *state_test_data = arg;
    return state_test_data->connection_resumed;
}

static void s_wait_for_reconnect_to_complete(struct mqtt_connection_state_test *state_test_data) {
    aws_mutex_lock(&state_test_data->lock);
    aws_condition_variable_wait_pred(
        &state_test_data->cvar, &state_test_data->lock, s_is_connection_resumed, state_test_data);
    aws_mutex_unlock(&state_test_data->lock);
}

/** sets up a unix domain socket server and socket options. Creates an mqtt connection configured to use
 * the domain socket.
 */
static int s_setup_mqtt_server_fn(struct aws_allocator *allocator, void *ctx) {
    aws_mqtt_library_init(allocator);

    struct mqtt_connection_state_test *state_test_data = ctx;

    AWS_ZERO_STRUCT(*state_test_data);

    state_test_data->allocator = allocator;

    ASSERT_SUCCESS(aws_event_loop_group_default_init(&state_test_data->el_group, allocator, 1));

    state_test_data->test_channel_handler = s_new_mqtt_mock_server(allocator);
    ASSERT_NOT_NULL(state_test_data->test_channel_handler);

    state_test_data->server_bootstrap = aws_server_bootstrap_new(allocator, &state_test_data->el_group);
    ASSERT_NOT_NULL(state_test_data->server_bootstrap);

    struct aws_socket_options socket_options = {
        .connect_timeout_ms = 100,
        .domain = AWS_SOCKET_LOCAL,
    };

    state_test_data->socket_options = socket_options;
    ASSERT_SUCCESS(aws_condition_variable_init(&state_test_data->cvar));
    ASSERT_SUCCESS(aws_mutex_init(&state_test_data->lock));

    uint64_t timestamp = 0;
    ASSERT_SUCCESS(aws_sys_clock_get_ticks(&timestamp));

    snprintf(
        state_test_data->endpoint.address,
        sizeof(state_test_data->endpoint.address),
        LOCAL_SOCK_TEST_PATTERN,
        (long long unsigned)timestamp);

    struct aws_server_socket_channel_bootstrap_options server_bootstrap_options = {
        .bootstrap = state_test_data->server_bootstrap,
        .host_name = state_test_data->endpoint.address,
        .port = state_test_data->endpoint.port,
        .socket_options = &state_test_data->socket_options,
        .incoming_callback = s_on_incoming_channel_setup_fn,
        .shutdown_callback = s_on_incoming_channel_shutdown_fn,
        .destroy_callback = s_on_listener_destroy,
        .user_data = state_test_data,
    };
    state_test_data->listener = aws_server_bootstrap_new_socket_listener(&server_bootstrap_options);

    ASSERT_NOT_NULL(state_test_data->listener);

    ASSERT_SUCCESS(
        aws_host_resolver_init_default(&state_test_data->host_resolver, allocator, 1, &state_test_data->el_group));

    struct aws_client_bootstrap_options bootstrap_options = {
        .event_loop_group = &state_test_data->el_group,
        .user_data = state_test_data,
        .host_resolver = &state_test_data->host_resolver,
    };

    state_test_data->client_bootstrap = aws_client_bootstrap_new(allocator, &bootstrap_options);

    ASSERT_SUCCESS(aws_mqtt_client_init(&state_test_data->mqtt_client, allocator, state_test_data->client_bootstrap));
    state_test_data->mqtt_connection = aws_mqtt_client_connection_new(&state_test_data->mqtt_client);
    ASSERT_NOT_NULL(state_test_data->mqtt_connection);

    aws_mqtt_client_connection_set_connection_interruption_handlers(
        state_test_data->mqtt_connection,
        s_on_connection_interrupted,
        state_test_data,
        s_on_connection_resumed,
        state_test_data);

    aws_array_list_init_dynamic(&state_test_data->published_messages, allocator, 4, sizeof(struct aws_byte_buf));
    return AWS_OP_SUCCESS;
}

static int s_clean_up_mqtt_server_fn(struct aws_allocator *allocator, int setup_result, void *ctx) {
    (void)allocator;

    if (!setup_result) {
        struct mqtt_connection_state_test *state_test_data = ctx;

        for (size_t i = 0; i < aws_array_list_length(&state_test_data->published_messages); ++i) {
            struct aws_byte_buf *buf_ptr = NULL;
            aws_array_list_get_at_ptr(&state_test_data->published_messages, (void **)&buf_ptr, i);
            aws_byte_buf_clean_up(buf_ptr);
        }

        aws_array_list_clean_up(&state_test_data->published_messages);
        aws_mqtt_client_connection_destroy(state_test_data->mqtt_connection);
        aws_mqtt_client_clean_up(&state_test_data->mqtt_client);
        aws_client_bootstrap_release(state_test_data->client_bootstrap);
        aws_host_resolver_clean_up(&state_test_data->host_resolver);
        aws_server_bootstrap_destroy_socket_listener(state_test_data->server_bootstrap, state_test_data->listener);
        s_wait_on_listener_cleanup(state_test_data);
        aws_server_bootstrap_release(state_test_data->server_bootstrap);
        aws_event_loop_group_clean_up(&state_test_data->el_group);
        s_destroy_mqtt_mock_server(state_test_data->test_channel_handler);
    }

    aws_mqtt_library_clean_up();
    return AWS_OP_SUCCESS;
}

static void s_on_connection_complete_fn(
    struct aws_mqtt_client_connection *connection,
    int error_code,
    enum aws_mqtt_connect_return_code return_code,
    bool session_present,
    void *userdata) {
    (void)connection;
    struct mqtt_connection_state_test *state_test_data = userdata;
    aws_mutex_lock(&state_test_data->lock);

    state_test_data->session_present = session_present;
    state_test_data->mqtt_return_code = return_code;
    state_test_data->error = error_code;
    state_test_data->connection_completed = true;
    aws_mutex_unlock(&state_test_data->lock);

    aws_condition_variable_notify_one(&state_test_data->cvar);
}

static bool s_is_connection_completed(void *arg) {
    struct mqtt_connection_state_test *state_test_data = arg;
    return state_test_data->connection_completed;
}

static void s_wait_for_connection_to_complete(struct mqtt_connection_state_test *state_test_data) {
    aws_mutex_lock(&state_test_data->lock);
    aws_condition_variable_wait_pred(
        &state_test_data->cvar, &state_test_data->lock, s_is_connection_completed, state_test_data);
    aws_mutex_unlock(&state_test_data->lock);
}

void s_on_disconnect_fn(struct aws_mqtt_client_connection *connection, void *userdata) {
    (void)connection;
    struct mqtt_connection_state_test *state_test_data = userdata;
    aws_mutex_lock(&state_test_data->lock);
    state_test_data->client_disconnect_completed = true;
    aws_mutex_unlock(&state_test_data->lock);

    aws_condition_variable_notify_one(&state_test_data->cvar);
}

static bool s_is_disconnect_completed(void *arg) {
    struct mqtt_connection_state_test *state_test_data = arg;
    return state_test_data->client_disconnect_completed && state_test_data->server_disconnect_completed;
}

static void s_wait_for_disconnect_to_complete(struct mqtt_connection_state_test *state_test_data) {
    aws_mutex_lock(&state_test_data->lock);
    aws_condition_variable_wait_pred(
        &state_test_data->cvar, &state_test_data->lock, s_is_disconnect_completed, state_test_data);
    state_test_data->client_disconnect_completed = false;
    state_test_data->server_disconnect_completed = false;
    aws_mutex_unlock(&state_test_data->lock);
}

static void s_on_publish_received(
    struct aws_mqtt_client_connection *connection,
    const struct aws_byte_cursor *topic,
    const struct aws_byte_cursor *payload,
    void *userdata) {
    (void)connection;
    (void)topic;
    struct mqtt_connection_state_test *state_test_data = userdata;

    struct aws_byte_buf payload_cp;
    aws_byte_buf_init_copy_from_cursor(&payload_cp, state_test_data->allocator, *payload);

    aws_mutex_lock(&state_test_data->lock);
    aws_array_list_push_back(&state_test_data->published_messages, &payload_cp);
    state_test_data->publishes_received++;
    aws_mutex_unlock(&state_test_data->lock);
    aws_condition_variable_notify_one(&state_test_data->cvar);
}

static bool s_is_publish_received(void *arg) {
    struct mqtt_connection_state_test *state_test_data = arg;
    return state_test_data->publishes_received == state_test_data->expected_publishes;
}

static void s_wait_for_publish(struct mqtt_connection_state_test *state_test_data) {
    aws_mutex_lock(&state_test_data->lock);
    aws_condition_variable_wait_pred(
        &state_test_data->cvar, &state_test_data->lock, s_is_publish_received, state_test_data);
    state_test_data->publishes_received = 0;
    state_test_data->expected_publishes = 0;
    aws_mutex_unlock(&state_test_data->lock);
}

static void s_on_suback(
    struct aws_mqtt_client_connection *connection,
    uint16_t packet_id,
    const struct aws_byte_cursor *topic,
    enum aws_mqtt_qos qos,
    int error_code,
    void *userdata) {
    (void)connection;
    (void)packet_id;
    (void)topic;
    (void)qos;
    (void)error_code;

    struct mqtt_connection_state_test *state_test_data = userdata;

    aws_mutex_lock(&state_test_data->lock);
    state_test_data->subscribe_completed = true;
    aws_mutex_unlock(&state_test_data->lock);
    aws_condition_variable_notify_one(&state_test_data->cvar);
}

static bool s_is_subscribe_completed(void *arg) {
    struct mqtt_connection_state_test *state_test_data = arg;
    return state_test_data->subscribe_completed;
}

static void s_wait_for_subscribe_to_complete(struct mqtt_connection_state_test *state_test_data) {
    aws_mutex_lock(&state_test_data->lock);
    aws_condition_variable_wait_pred(
        &state_test_data->cvar, &state_test_data->lock, s_is_subscribe_completed, state_test_data);
    aws_mutex_unlock(&state_test_data->lock);
}

/*
 * Makes an Mqtt connect call, then a disconnect. Then verifies a CONNECT and DISCONNECT were sent.
 */
static int s_test_mqtt_connect_disconnect_fn(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    struct mqtt_connection_state_test *state_test_data = ctx;

    struct aws_mqtt_connection_options connection_options = {
        .user_data = state_test_data,
        .clean_session = false,
        .client_id = aws_byte_cursor_from_c_str("client1234"),
        .host_name = aws_byte_cursor_from_c_str(state_test_data->endpoint.address),
        .socket_options = &state_test_data->socket_options,
        .on_connection_complete = s_on_connection_complete_fn,
    };

    ASSERT_SUCCESS(aws_mqtt_client_connection_connect(state_test_data->mqtt_connection, &connection_options));
    s_wait_for_connection_to_complete(state_test_data);
    ASSERT_SUCCESS(
        aws_mqtt_client_connection_disconnect(state_test_data->mqtt_connection, s_on_disconnect_fn, state_test_data));
    s_wait_for_disconnect_to_complete(state_test_data);

    struct aws_array_list *received_messages =
        s_mqtt_mock_server_get_received_messages(state_test_data->test_channel_handler);
    ASSERT_NOT_NULL(received_messages);
    ASSERT_UINT_EQUALS(2, aws_array_list_length(received_messages));

    struct aws_byte_buf received_message = {0};
    ASSERT_SUCCESS(aws_array_list_get_at(received_messages, &received_message, 0));
    struct aws_byte_cursor message_cur = aws_byte_cursor_from_buf(&received_message);

    struct aws_mqtt_packet_connect connect_packet;
    ASSERT_SUCCESS(aws_mqtt_packet_connect_decode(&message_cur, &connect_packet));
    ASSERT_UINT_EQUALS(connection_options.clean_session, connect_packet.clean_session);
    ASSERT_BIN_ARRAYS_EQUALS(
        connection_options.client_id.ptr,
        connection_options.client_id.len,
        connect_packet.client_identifier.ptr,
        connect_packet.client_identifier.len);

    ASSERT_SUCCESS(aws_array_list_get_at(received_messages, &received_message, 1));
    message_cur = aws_byte_cursor_from_buf(&received_message);

    struct aws_mqtt_packet_connection packet;
    ASSERT_SUCCESS(aws_mqtt_packet_connection_decode(&message_cur, &packet));
    ASSERT_INT_EQUALS(AWS_MQTT_PACKET_DISCONNECT, packet.fixed_header.packet_type);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    mqtt_connect_disconnect,
    s_setup_mqtt_server_fn,
    s_test_mqtt_connect_disconnect_fn,
    s_clean_up_mqtt_server_fn,
    &test_data)

/*
 * Makes a CONNECT, then the server hangs up, tests that the client reconnects on its own, then sends a DISCONNECT.
 */
static int s_test_mqtt_connection_interrupted_fn(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    struct mqtt_connection_state_test *state_test_data = ctx;

    struct aws_mqtt_connection_options connection_options = {
        .user_data = state_test_data,
        .clean_session = true,
        .client_id = aws_byte_cursor_from_c_str("client1234"),
        .host_name = aws_byte_cursor_from_c_str(state_test_data->endpoint.address),
        .socket_options = &state_test_data->socket_options,
        .on_connection_complete = s_on_connection_complete_fn,
    };

    ASSERT_SUCCESS(aws_mqtt_client_connection_connect(state_test_data->mqtt_connection, &connection_options));
    s_wait_for_connection_to_complete(state_test_data);

    /* shut it down and make sure the client automatically reconnects.*/
    aws_channel_shutdown(state_test_data->server_channel, AWS_OP_SUCCESS);
    s_wait_for_reconnect_to_complete(state_test_data);

    ASSERT_SUCCESS(
        aws_mqtt_client_connection_disconnect(state_test_data->mqtt_connection, s_on_disconnect_fn, state_test_data));
    s_wait_for_disconnect_to_complete(state_test_data);

    struct aws_array_list *received_messages =
        s_mqtt_mock_server_get_received_messages(state_test_data->test_channel_handler);
    ASSERT_NOT_NULL(received_messages);
    ASSERT_UINT_EQUALS(3, aws_array_list_length(received_messages));

    struct aws_byte_buf received_message = {0};
    ASSERT_SUCCESS(aws_array_list_get_at(received_messages, &received_message, 0));
    struct aws_byte_cursor message_cur = aws_byte_cursor_from_buf(&received_message);

    struct aws_mqtt_packet_connect connect_packet;
    ASSERT_SUCCESS(aws_mqtt_packet_connect_decode(&message_cur, &connect_packet));
    ASSERT_UINT_EQUALS(connection_options.clean_session, connect_packet.clean_session);
    ASSERT_BIN_ARRAYS_EQUALS(
        connection_options.client_id.ptr,
        connection_options.client_id.len,
        connect_packet.client_identifier.ptr,
        connect_packet.client_identifier.len);

    ASSERT_SUCCESS(aws_array_list_get_at(received_messages, &received_message, 1));
    message_cur = aws_byte_cursor_from_buf(&received_message);

    ASSERT_SUCCESS(aws_mqtt_packet_connect_decode(&message_cur, &connect_packet));
    ASSERT_UINT_EQUALS(connection_options.clean_session, connect_packet.clean_session);
    ASSERT_BIN_ARRAYS_EQUALS(
        connection_options.client_id.ptr,
        connection_options.client_id.len,
        connect_packet.client_identifier.ptr,
        connect_packet.client_identifier.len);

    ASSERT_SUCCESS(aws_array_list_get_at(received_messages, &received_message, 2));
    message_cur = aws_byte_cursor_from_buf(&received_message);

    struct aws_mqtt_packet_connection packet;
    ASSERT_SUCCESS(aws_mqtt_packet_connection_decode(&message_cur, &packet));
    ASSERT_INT_EQUALS(AWS_MQTT_PACKET_DISCONNECT, packet.fixed_header.packet_type);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    mqtt_connection_interrupted,
    s_setup_mqtt_server_fn,
    s_test_mqtt_connection_interrupted_fn,
    s_clean_up_mqtt_server_fn,
    &test_data)

/* Makes a CONNECT, with a 1 second keep alive ping interval, the mock is configured to not reply to the PING,
 * this should cause a timeout, then the PING responses are turned back on, and the client should automatically
 * reconnect. Then send a DISCONNECT. */
static int s_test_mqtt_connection_timeout_fn(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    struct mqtt_connection_state_test *state_test_data = ctx;

    struct aws_mqtt_connection_options connection_options = {
        .user_data = state_test_data,
        .clean_session = true,
        .client_id = aws_byte_cursor_from_c_str("client1234"),
        .host_name = aws_byte_cursor_from_c_str(state_test_data->endpoint.address),
        .socket_options = &state_test_data->socket_options,
        .on_connection_complete = s_on_connection_complete_fn,
        .keep_alive_time_secs = 1,
        .ping_timeout_ms = 100,
    };

    s_mqtt_mock_server_set_max_ping_resp(state_test_data->test_channel_handler, 0);
    ASSERT_SUCCESS(aws_mqtt_client_connection_connect(state_test_data->mqtt_connection, &connection_options));
    s_wait_for_connection_to_complete(state_test_data);

    /* this should take about 1.1 seconds for the timeout and reconnect.*/
    s_wait_for_reconnect_to_complete(state_test_data);

    ASSERT_SUCCESS(
        aws_mqtt_client_connection_disconnect(state_test_data->mqtt_connection, s_on_disconnect_fn, state_test_data));
    s_wait_for_disconnect_to_complete(state_test_data);

    ASSERT_INT_EQUALS(AWS_ERROR_MQTT_TIMEOUT, state_test_data->interruption_error);

    struct aws_array_list *received_messages =
        s_mqtt_mock_server_get_received_messages(state_test_data->test_channel_handler);
    ASSERT_NOT_NULL(received_messages);
    ASSERT_UINT_EQUALS(4, aws_array_list_length(received_messages));

    struct aws_byte_buf received_message = {0};
    ASSERT_SUCCESS(aws_array_list_get_at(received_messages, &received_message, 0));
    struct aws_byte_cursor message_cur = aws_byte_cursor_from_buf(&received_message);

    struct aws_mqtt_packet_connect connect_packet;
    ASSERT_SUCCESS(aws_mqtt_packet_connect_decode(&message_cur, &connect_packet));
    ASSERT_UINT_EQUALS(connection_options.clean_session, connect_packet.clean_session);
    ASSERT_BIN_ARRAYS_EQUALS(
        connection_options.client_id.ptr,
        connection_options.client_id.len,
        connect_packet.client_identifier.ptr,
        connect_packet.client_identifier.len);

    ASSERT_SUCCESS(aws_array_list_get_at(received_messages, &received_message, 1));
    message_cur = aws_byte_cursor_from_buf(&received_message);

    struct aws_mqtt_packet_connection ping;
    ASSERT_SUCCESS(aws_mqtt_packet_connection_decode(&message_cur, &ping));
    ASSERT_INT_EQUALS(AWS_MQTT_PACKET_PINGREQ, ping.fixed_header.packet_type);

    ASSERT_SUCCESS(aws_array_list_get_at(received_messages, &received_message, 2));
    message_cur = aws_byte_cursor_from_buf(&received_message);

    ASSERT_SUCCESS(aws_mqtt_packet_connect_decode(&message_cur, &connect_packet));
    ASSERT_UINT_EQUALS(connection_options.clean_session, connect_packet.clean_session);
    ASSERT_BIN_ARRAYS_EQUALS(
        connection_options.client_id.ptr,
        connection_options.client_id.len,
        connect_packet.client_identifier.ptr,
        connect_packet.client_identifier.len);

    ASSERT_SUCCESS(aws_array_list_get_at(received_messages, &received_message, 3));
    message_cur = aws_byte_cursor_from_buf(&received_message);

    struct aws_mqtt_packet_connection packet;
    ASSERT_SUCCESS(aws_mqtt_packet_connection_decode(&message_cur, &packet));
    ASSERT_INT_EQUALS(AWS_MQTT_PACKET_DISCONNECT, packet.fixed_header.packet_type);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    mqtt_connection_timeout,
    s_setup_mqtt_server_fn,
    s_test_mqtt_connection_timeout_fn,
    s_clean_up_mqtt_server_fn,
    &test_data)

/* Makes a CONNECT, channel is successfully setup, but the server never sends a connack, make sure we timeout. */
static int s_test_mqtt_connection_connack_timeout_fn(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    struct mqtt_connection_state_test *state_test_data = ctx;

    struct aws_mqtt_connection_options connection_options = {
        .user_data = state_test_data,
        .clean_session = true,
        .client_id = aws_byte_cursor_from_c_str("client1234"),
        .host_name = aws_byte_cursor_from_c_str(state_test_data->endpoint.address),
        .socket_options = &state_test_data->socket_options,
        .on_connection_complete = s_on_connection_complete_fn,
        .keep_alive_time_secs = 1,
        .ping_timeout_ms = 100,
    };

    s_mqtt_mock_server_set_max_connack(state_test_data->test_channel_handler, 0);
    ASSERT_SUCCESS(aws_mqtt_client_connection_connect(state_test_data->mqtt_connection, &connection_options));
    s_wait_for_connection_to_complete(state_test_data);

    ASSERT_INT_EQUALS(AWS_ERROR_MQTT_TIMEOUT, state_test_data->error);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    mqtt_connection_connack_timeout,
    s_setup_mqtt_server_fn,
    s_test_mqtt_connection_connack_timeout_fn,
    s_clean_up_mqtt_server_fn,
    &test_data)

/* Subscribe to a topic prior to connection, make a CONNECT, have the server send PUBLISH messages,
 * make sure they're received, then send a DISCONNECT. */
static int s_test_mqtt_subscribe_fn(struct aws_allocator *allocator, void *ctx) {
    struct mqtt_connection_state_test *state_test_data = ctx;

    struct aws_mqtt_connection_options connection_options = {
        .user_data = state_test_data,
        .clean_session = false,
        .client_id = aws_byte_cursor_from_c_str("client1234"),
        .host_name = aws_byte_cursor_from_c_str(state_test_data->endpoint.address),
        .socket_options = &state_test_data->socket_options,
        .on_connection_complete = s_on_connection_complete_fn,
    };

    struct aws_byte_cursor sub_topic = aws_byte_cursor_from_c_str("/test/topic");

    ASSERT_TRUE(
        aws_mqtt_client_connection_subscribe(
            state_test_data->mqtt_connection,
            &sub_topic,
            AWS_MQTT_QOS_AT_LEAST_ONCE,
            s_on_publish_received,
            state_test_data,
            NULL,
            s_on_suback,
            state_test_data) > 0);

    ASSERT_SUCCESS(aws_mqtt_client_connection_connect(state_test_data->mqtt_connection, &connection_options));
    s_wait_for_connection_to_complete(state_test_data);

    s_wait_for_subscribe_to_complete(state_test_data);

    state_test_data->expected_publishes = 2;
    struct aws_byte_cursor payload_1 = aws_byte_cursor_from_c_str("Test Message 1");
    ASSERT_SUCCESS(s_mqtt_mock_server_send_publish(
        state_test_data->test_channel_handler, &sub_topic, &payload_1, AWS_MQTT_QOS_AT_LEAST_ONCE));
    struct aws_byte_cursor payload_2 = aws_byte_cursor_from_c_str("Test Message 2");
    ASSERT_SUCCESS(s_mqtt_mock_server_send_publish(
        state_test_data->test_channel_handler, &sub_topic, &payload_2, AWS_MQTT_QOS_AT_LEAST_ONCE));

    s_wait_for_publish(state_test_data);
    s_mqtt_mock_server_wait_for_pubacks(state_test_data->test_channel_handler, 2);

    ASSERT_SUCCESS(
        aws_mqtt_client_connection_disconnect(state_test_data->mqtt_connection, s_on_disconnect_fn, state_test_data));
    s_wait_for_disconnect_to_complete(state_test_data);

    struct aws_array_list *received_messages =
        s_mqtt_mock_server_get_received_messages(state_test_data->test_channel_handler);
    ASSERT_NOT_NULL(received_messages);
    ASSERT_UINT_EQUALS(5, aws_array_list_length(received_messages));

    struct aws_byte_buf received_message = {0};
    ASSERT_SUCCESS(aws_array_list_get_at(received_messages, &received_message, 0));
    struct aws_byte_cursor message_cur = aws_byte_cursor_from_buf(&received_message);

    struct aws_mqtt_packet_connect connect_packet;
    ASSERT_SUCCESS(aws_mqtt_packet_connect_decode(&message_cur, &connect_packet));
    ASSERT_UINT_EQUALS(connection_options.clean_session, connect_packet.clean_session);
    ASSERT_BIN_ARRAYS_EQUALS(
        connection_options.client_id.ptr,
        connection_options.client_id.len,
        connect_packet.client_identifier.ptr,
        connect_packet.client_identifier.len);

    ASSERT_SUCCESS(aws_array_list_get_at(received_messages, &received_message, 1));
    message_cur = aws_byte_cursor_from_buf(&received_message);

    struct aws_mqtt_packet_subscribe subscribe_packet;
    ASSERT_SUCCESS(aws_mqtt_packet_subscribe_init(&subscribe_packet, allocator, 0));
    ASSERT_SUCCESS(aws_mqtt_packet_subscribe_decode(&message_cur, &subscribe_packet));
    aws_mqtt_packet_subscribe_clean_up(&subscribe_packet);

    ASSERT_SUCCESS(aws_array_list_get_at(received_messages, &received_message, 2));
    message_cur = aws_byte_cursor_from_buf(&received_message);

    struct aws_mqtt_packet_ack puback;
    ASSERT_SUCCESS(aws_mqtt_packet_ack_decode(&message_cur, &puback));
    ASSERT_INT_EQUALS(AWS_MQTT_PACKET_PUBACK, puback.fixed_header.packet_type);

    ASSERT_SUCCESS(aws_array_list_get_at(received_messages, &received_message, 3));
    message_cur = aws_byte_cursor_from_buf(&received_message);

    ASSERT_SUCCESS(aws_mqtt_packet_ack_decode(&message_cur, &puback));
    ASSERT_INT_EQUALS(AWS_MQTT_PACKET_PUBACK, puback.fixed_header.packet_type);

    ASSERT_SUCCESS(aws_array_list_get_at(received_messages, &received_message, 4));
    message_cur = aws_byte_cursor_from_buf(&received_message);

    struct aws_mqtt_packet_connection packet;
    ASSERT_SUCCESS(aws_mqtt_packet_connection_decode(&message_cur, &packet));
    ASSERT_INT_EQUALS(AWS_MQTT_PACKET_DISCONNECT, packet.fixed_header.packet_type);

    ASSERT_UINT_EQUALS(2, aws_array_list_length(&state_test_data->published_messages));

    struct aws_byte_buf *publish_msg = NULL;
    ASSERT_SUCCESS(aws_array_list_get_at_ptr(&state_test_data->published_messages, (void **)&publish_msg, 0));
    ASSERT_BIN_ARRAYS_EQUALS(payload_1.ptr, payload_1.len, publish_msg->buffer, publish_msg->len);
    ASSERT_SUCCESS(aws_array_list_get_at_ptr(&state_test_data->published_messages, (void **)&publish_msg, 1));
    ASSERT_BIN_ARRAYS_EQUALS(payload_2.ptr, payload_2.len, publish_msg->buffer, publish_msg->len);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    mqtt_connect_subscribe,
    s_setup_mqtt_server_fn,
    s_test_mqtt_subscribe_fn,
    s_clean_up_mqtt_server_fn,
    &test_data)

static void s_on_op_complete(
    struct aws_mqtt_client_connection *connection,
    uint16_t packet_id,
    int error_code,
    void *userdata) {
    (void)connection;
    (void)packet_id;
    (void)error_code;

    struct mqtt_connection_state_test *state_test_data = userdata;
    AWS_LOGF_DEBUG(TEST_LOG_SUBJECT, "pub op completed");
    aws_mutex_lock(&state_test_data->lock);
    state_test_data->ops_completed++;
    aws_mutex_unlock(&state_test_data->lock);
    aws_condition_variable_notify_one(&state_test_data->cvar);
}

static bool s_is_ops_completed(void *arg) {
    struct mqtt_connection_state_test *state_test_data = arg;
    return state_test_data->ops_completed == state_test_data->expected_ops_completed;
}

static void s_wait_for_ops_completed(struct mqtt_connection_state_test *state_test_data) {
    aws_mutex_lock(&state_test_data->lock);
    aws_condition_variable_wait_for_pred(
        &state_test_data->cvar, &state_test_data->lock, 10000000000, s_is_ops_completed, state_test_data);
    aws_mutex_unlock(&state_test_data->lock);
}

/* Make a CONNECT, PUBLISH to a topic, make sure server received, then send a DISCONNECT. */
static int s_test_mqtt_publish_fn(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    struct mqtt_connection_state_test *state_test_data = ctx;

    struct aws_mqtt_connection_options connection_options = {
        .user_data = state_test_data,
        .clean_session = false,
        .client_id = aws_byte_cursor_from_c_str("client1234"),
        .host_name = aws_byte_cursor_from_c_str(state_test_data->endpoint.address),
        .socket_options = &state_test_data->socket_options,
        .on_connection_complete = s_on_connection_complete_fn,
    };

    struct aws_byte_cursor pub_topic = aws_byte_cursor_from_c_str("/test/topic");
    struct aws_byte_cursor payload_1 = aws_byte_cursor_from_c_str("Test Message 1");
    struct aws_byte_cursor payload_2 = aws_byte_cursor_from_c_str("Test Message 2");

    ASSERT_SUCCESS(aws_mqtt_client_connection_connect(state_test_data->mqtt_connection, &connection_options));
    s_wait_for_connection_to_complete(state_test_data);

    state_test_data->expected_ops_completed = 2;
    ASSERT_TRUE(
        aws_mqtt_client_connection_publish(
            state_test_data->mqtt_connection,
            &pub_topic,
            AWS_MQTT_QOS_AT_LEAST_ONCE,
            false,
            &payload_1,
            s_on_op_complete,
            state_test_data) > 0);
    ASSERT_TRUE(
        aws_mqtt_client_connection_publish(
            state_test_data->mqtt_connection,
            &pub_topic,
            AWS_MQTT_QOS_AT_LEAST_ONCE,
            false,
            &payload_2,
            s_on_op_complete,
            state_test_data) > 0);

    s_wait_for_ops_completed(state_test_data);

    ASSERT_SUCCESS(
        aws_mqtt_client_connection_disconnect(state_test_data->mqtt_connection, s_on_disconnect_fn, state_test_data));
    s_wait_for_disconnect_to_complete(state_test_data);

    struct aws_array_list *received_messages =
        s_mqtt_mock_server_get_received_messages(state_test_data->test_channel_handler);
    ASSERT_NOT_NULL(received_messages);
    ASSERT_UINT_EQUALS(4, aws_array_list_length(received_messages));

    struct aws_byte_buf received_message = {0};
    ASSERT_SUCCESS(aws_array_list_get_at(received_messages, &received_message, 0));
    struct aws_byte_cursor message_cur = aws_byte_cursor_from_buf(&received_message);

    struct aws_mqtt_packet_connect connect_packet;
    ASSERT_SUCCESS(aws_mqtt_packet_connect_decode(&message_cur, &connect_packet));
    ASSERT_UINT_EQUALS(connection_options.clean_session, connect_packet.clean_session);
    ASSERT_BIN_ARRAYS_EQUALS(
        connection_options.client_id.ptr,
        connection_options.client_id.len,
        connect_packet.client_identifier.ptr,
        connect_packet.client_identifier.len);

    ASSERT_SUCCESS(aws_array_list_get_at(received_messages, &received_message, 1));
    message_cur = aws_byte_cursor_from_buf(&received_message);

    struct aws_mqtt_packet_publish publish_packet;
    ASSERT_SUCCESS(aws_mqtt_packet_publish_decode(&message_cur, &publish_packet));
    ASSERT_BIN_ARRAYS_EQUALS(
        pub_topic.ptr, pub_topic.len, publish_packet.topic_name.ptr, publish_packet.topic_name.len);
    ASSERT_BIN_ARRAYS_EQUALS(payload_1.ptr, payload_1.len, publish_packet.payload.ptr, publish_packet.payload.len);

    ASSERT_SUCCESS(aws_array_list_get_at(received_messages, &received_message, 2));
    message_cur = aws_byte_cursor_from_buf(&received_message);

    ASSERT_SUCCESS(aws_mqtt_packet_publish_decode(&message_cur, &publish_packet));
    ASSERT_BIN_ARRAYS_EQUALS(
        pub_topic.ptr, pub_topic.len, publish_packet.topic_name.ptr, publish_packet.topic_name.len);
    ASSERT_BIN_ARRAYS_EQUALS(payload_2.ptr, payload_2.len, publish_packet.payload.ptr, publish_packet.payload.len);

    ASSERT_SUCCESS(aws_array_list_get_at(received_messages, &received_message, 3));
    message_cur = aws_byte_cursor_from_buf(&received_message);

    struct aws_mqtt_packet_connection packet;
    ASSERT_SUCCESS(aws_mqtt_packet_connection_decode(&message_cur, &packet));
    ASSERT_INT_EQUALS(AWS_MQTT_PACKET_DISCONNECT, packet.fixed_header.packet_type);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    mqtt_connect_publish,
    s_setup_mqtt_server_fn,
    s_test_mqtt_publish_fn,
    s_clean_up_mqtt_server_fn,
    &test_data)

/*
 * CONNECT, force the server to hang up after a successful connection and block all CONNACKS, send PUBLISH messages
 * let the server send CONNACKS, make sure when the client reconnects automatically, it sends the PUBLISH messages
 * that were sent during offline mode. Then send a DISCONNECT.
 */
static int s_test_mqtt_connection_offline_publish_fn(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    struct mqtt_connection_state_test *state_test_data = ctx;

    struct aws_mqtt_connection_options connection_options = {
        .user_data = state_test_data,
        .clean_session = true,
        .client_id = aws_byte_cursor_from_c_str("client1234"),
        .host_name = aws_byte_cursor_from_c_str(state_test_data->endpoint.address),
        .socket_options = &state_test_data->socket_options,
        .on_connection_complete = s_on_connection_complete_fn,
        .ping_timeout_ms = 10,
        .keep_alive_time_secs = 1,
    };

    ASSERT_SUCCESS(aws_mqtt_client_connection_connect(state_test_data->mqtt_connection, &connection_options));
    s_wait_for_connection_to_complete(state_test_data);

    s_mqtt_mock_server_set_max_connack(state_test_data->test_channel_handler, 0);

    /* shut it down and make sure the client automatically reconnects.*/
    aws_channel_shutdown(state_test_data->server_channel, AWS_OP_SUCCESS);
    s_wait_for_interrupt_to_complete(state_test_data);

    state_test_data->server_disconnect_completed = false;

    struct aws_byte_cursor pub_topic = aws_byte_cursor_from_c_str("/test/topic");
    struct aws_byte_cursor payload_1 = aws_byte_cursor_from_c_str("Test Message 1");
    struct aws_byte_cursor payload_2 = aws_byte_cursor_from_c_str("Test Message 2");

    aws_mutex_lock(&state_test_data->lock);
    state_test_data->expected_ops_completed = 2;
    aws_mutex_unlock(&state_test_data->lock);

    ASSERT_TRUE(
        aws_mqtt_client_connection_publish(
            state_test_data->mqtt_connection,
            &pub_topic,
            AWS_MQTT_QOS_AT_LEAST_ONCE,
            false,
            &payload_1,
            s_on_op_complete,
            state_test_data) > 0);
    ASSERT_TRUE(
        aws_mqtt_client_connection_publish(
            state_test_data->mqtt_connection,
            &pub_topic,
            AWS_MQTT_QOS_AT_LEAST_ONCE,
            false,
            &payload_2,
            s_on_op_complete,
            state_test_data) > 0);

    aws_mutex_lock(&state_test_data->lock);
    ASSERT_FALSE(state_test_data->connection_resumed);
    aws_mutex_unlock(&state_test_data->lock);
    s_mqtt_mock_server_set_max_connack(state_test_data->test_channel_handler, SIZE_MAX);
    s_wait_for_ops_completed(state_test_data);
    aws_mutex_lock(&state_test_data->lock);
    ASSERT_TRUE(state_test_data->connection_resumed);
    aws_mutex_unlock(&state_test_data->lock);

    ASSERT_SUCCESS(
        aws_mqtt_client_connection_disconnect(state_test_data->mqtt_connection, s_on_disconnect_fn, state_test_data));
    s_wait_for_disconnect_to_complete(state_test_data);

    struct aws_array_list *received_messages =
        s_mqtt_mock_server_get_received_messages(state_test_data->test_channel_handler);
    ASSERT_NOT_NULL(received_messages);
    ASSERT_TRUE(aws_array_list_length(received_messages) >= 5 && aws_array_list_length(received_messages) <= 6);

    size_t message_count = aws_array_list_length(received_messages);

    struct aws_byte_buf received_message = {0};
    ASSERT_SUCCESS(aws_array_list_get_at(received_messages, &received_message, 0));
    struct aws_byte_cursor message_cur = aws_byte_cursor_from_buf(&received_message);

    struct aws_mqtt_packet_connect connect_packet;
    ASSERT_SUCCESS(aws_mqtt_packet_connect_decode(&message_cur, &connect_packet));
    ASSERT_UINT_EQUALS(connection_options.clean_session, connect_packet.clean_session);
    ASSERT_BIN_ARRAYS_EQUALS(
        connection_options.client_id.ptr,
        connection_options.client_id.len,
        connect_packet.client_identifier.ptr,
        connect_packet.client_identifier.len);

    ASSERT_SUCCESS(aws_array_list_get_at(received_messages, &received_message, 1));
    message_cur = aws_byte_cursor_from_buf(&received_message);

    ASSERT_SUCCESS(aws_mqtt_packet_connect_decode(&message_cur, &connect_packet));
    ASSERT_UINT_EQUALS(connection_options.clean_session, connect_packet.clean_session);
    ASSERT_BIN_ARRAYS_EQUALS(
        connection_options.client_id.ptr,
        connection_options.client_id.len,
        connect_packet.client_identifier.ptr,
        connect_packet.client_identifier.len);

    /* if message count is 6 there was an extra connect message due to the automatic reconnect behavior and timing. */
    size_t index = 2;
    if (message_count == 6) {
        ASSERT_SUCCESS(aws_array_list_get_at(received_messages, &received_message, index++));
        message_cur = aws_byte_cursor_from_buf(&received_message);
        ASSERT_SUCCESS(aws_mqtt_packet_connect_decode(&message_cur, &connect_packet));
        ASSERT_UINT_EQUALS(connection_options.clean_session, connect_packet.clean_session);
        ASSERT_BIN_ARRAYS_EQUALS(
            connection_options.client_id.ptr,
            connection_options.client_id.len,
            connect_packet.client_identifier.ptr,
            connect_packet.client_identifier.len);
    }

    ASSERT_SUCCESS(aws_array_list_get_at(received_messages, &received_message, index++));
    message_cur = aws_byte_cursor_from_buf(&received_message);

    struct aws_mqtt_packet_publish publish_packet;
    ASSERT_SUCCESS(aws_mqtt_packet_publish_decode(&message_cur, &publish_packet));
    ASSERT_BIN_ARRAYS_EQUALS(
        pub_topic.ptr, pub_topic.len, publish_packet.topic_name.ptr, publish_packet.topic_name.len);
    ASSERT_BIN_ARRAYS_EQUALS(payload_1.ptr, payload_1.len, publish_packet.payload.ptr, publish_packet.payload.len);

    ASSERT_SUCCESS(aws_array_list_get_at(received_messages, &received_message, index++));
    message_cur = aws_byte_cursor_from_buf(&received_message);

    ASSERT_SUCCESS(aws_mqtt_packet_publish_decode(&message_cur, &publish_packet));
    ASSERT_BIN_ARRAYS_EQUALS(
        pub_topic.ptr, pub_topic.len, publish_packet.topic_name.ptr, publish_packet.topic_name.len);
    ASSERT_BIN_ARRAYS_EQUALS(payload_2.ptr, payload_2.len, publish_packet.payload.ptr, publish_packet.payload.len);

    ASSERT_SUCCESS(aws_array_list_get_at(received_messages, &received_message, index++));
    message_cur = aws_byte_cursor_from_buf(&received_message);

    struct aws_mqtt_packet_connection packet;
    ASSERT_SUCCESS(aws_mqtt_packet_connection_decode(&message_cur, &packet));
    ASSERT_INT_EQUALS(AWS_MQTT_PACKET_DISCONNECT, packet.fixed_header.packet_type);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    mqtt_connection_offline_publish,
    s_setup_mqtt_server_fn,
    s_test_mqtt_connection_offline_publish_fn,
    s_clean_up_mqtt_server_fn,
    &test_data)
