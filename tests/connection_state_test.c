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
#include <aws/io/host_resolver.h>
#include <aws/io/event_loop.h>

#include <aws/testing/aws_test_harness.h>
#include <aws/testing/io_testing_channel.h>
#include <aws/common/condition_variable.h>

#include "mqtt_mock_server_handler.h"

#ifdef _WIN32
#    define LOCAL_SOCK_TEST_PATTERN "\\\\.\\pipe\\testsock%llu"
#else
#    define LOCAL_SOCK_TEST_PATTERN "testsock%llu.sock"
#endif

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
    bool disconnect_completed;
    enum aws_mqtt_connect_return_code mqtt_return_code;
    int error;
    struct aws_condition_variable cvar;
    struct aws_mutex lock;
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
        state_test_data->server_channel = channel;
        struct aws_channel_slot *test_handler_slot = aws_channel_slot_new(channel);
        aws_channel_slot_insert_end(channel, test_handler_slot);
        aws_channel_slot_set_handler(test_handler_slot, state_test_data->test_channel_handler);
    }
}

static void s_on_incoming_channel_shutdown_fn(
        struct aws_server_bootstrap *bootstrap,
        int error_code,
        struct aws_channel *channel,
        void *user_data) {

}

static void s_on_listener_destroy(struct aws_server_bootstrap *bootstrap, void *user_data) {

}

static int s_setup_mqtt_server_fn(struct aws_allocator *allocator, void *ctx) {
    aws_mqtt_library_init(allocator);

    struct mqtt_connection_state_test *state_test_data = ctx;

    ASSERT_SUCCESS(aws_event_loop_group_default_init(&state_test_data->el_group, allocator, 1));

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

    snprintf(state_test_data->endpoint.address, sizeof(state_test_data->endpoint.address), LOCAL_SOCK_TEST_PATTERN, (long long unsigned)timestamp);

    state_test_data->listener = aws_server_bootstrap_new_socket_listener(state_test_data->server_bootstrap, &state_test_data->endpoint,
                                                                         &socket_options, s_on_incoming_channel_setup_fn,
                                                                         s_on_incoming_channel_shutdown_fn, s_on_listener_destroy, state_test_data);

    ASSERT_NOT_NULL(state_test_data->listener);

    ASSERT_SUCCESS(aws_host_resolver_init_default(&state_test_data->host_resolver, allocator, 1, &state_test_data->el_group));

    struct aws_client_bootstrap_options bootstrap_options = {
            .event_loop_group = &state_test_data->el_group,
            .user_data = state_test_data,
            .host_resolver = &state_test_data->host_resolver,
    };

    state_test_data->client_bootstrap = aws_client_bootstrap_new(allocator, &bootstrap_options);

    ASSERT_SUCCESS(aws_mqtt_client_init(&state_test_data->mqtt_client, allocator, state_test_data->client_bootstrap));
    state_test_data->mqtt_connection = aws_mqtt_client_connection_new(&state_test_data->mqtt_client);
    ASSERT_NOT_NULL(state_test_data->mqtt_connection);

    return AWS_OP_SUCCESS;
}

static int s_clean_up_mqtt_server_fn(struct aws_allocator *allocator, void *ctx) {
    struct mqtt_connection_state_test *state_test_data = ctx;

    aws_mqtt_client_connection_destroy(state_test_data->mqtt_connection);
    aws_mqtt_client_clean_up(&state_test_data->mqtt_client);
    aws_client_bootstrap_release(state_test_data->client_bootstrap);
    aws_host_resolver_clean_up(&state_test_data->host_resolver);
    aws_server_bootstrap_destroy_socket_listener(state_test_data->server_bootstrap, state_test_data->listener);
    aws_server_bootstrap_release(state_test_data->server_bootstrap);
    aws_event_loop_group_clean_up(&state_test_data->el_group);

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
    state_test_data->session_present = session_present;
    state_test_data->mqtt_return_code = return_code;
    state_test_data->error = error_code;
    state_test_data->connection_completed = true;
    aws_condition_variable_notify_one(&state_test_data->cvar);
}

static bool s_is_connection_completed(void *arg) {
    struct mqtt_connection_state_test *state_test_data = arg;
    return state_test_data->connection_completed;
}

static void s_wait_for_connection_to_complete(struct mqtt_connection_state_test *state_test_data) {
    aws_mutex_lock(&state_test_data->lock);
    aws_condition_variable_wait_pred(&state_test_data->cvar, &state_test_data->lock, s_is_connection_completed, state_test_data);
    aws_mutex_unlock(&state_test_data->lock);
}

void s_on_disconnect_fn(struct aws_mqtt_client_connection *connection, void *userdata) {
    struct mqtt_connection_state_test *state_test_data = userdata;

    state_test_data->disconnect_completed = true;
    aws_condition_variable_notify_one(&state_test_data->cvar);
}

static bool s_is_disconnect_completed(void *arg) {
    struct mqtt_connection_state_test *state_test_data = arg;
    return state_test_data->disconnect_completed;
}

static void s_wait_for_disconnect_to_complete(struct mqtt_connection_state_test *state_test_data) {
    aws_mutex_lock(&state_test_data->lock);
    aws_condition_variable_wait_pred(&state_test_data->cvar, &state_test_data->lock, s_is_disconnect_completed, state_test_data);
    aws_mutex_unlock(&state_test_data->lock);
}

static int s_test_mqtt_connect_disconnect_fn(struct aws_allocator *allocator, void *ctx) {
    struct mqtt_connection_state_test *state_test_data = ctx;

    uint8_t connack_raw[256] = {0};
    struct aws_byte_buf connack_buf = aws_byte_buf_from_empty_array(connack_raw, sizeof(connack_raw));

    struct aws_mqtt_packet_connack conn_ack;
    ASSERT_SUCCESS(aws_mqtt_packet_connack_init(&conn_ack, false, AWS_MQTT_CONNECT_ACCEPTED));
    ASSERT_SUCCESS(aws_mqtt_packet_connack_encode(&connack_buf, &conn_ack));

    struct aws_io_message *connack_msg = aws_mem_calloc(allocator, 1, sizeof(struct aws_io_message));
    ASSERT_NOT_NULL(connack_msg);
    connack_msg->allocator = allocator;
    connack_msg->message_type = AWS_IO_MESSAGE_APPLICATION_DATA;
    connack_msg->message_data = connack_buf;

    state_test_data->test_channel_handler = s_new_mqtt_mock_server(allocator);
    ASSERT_NOT_NULL(state_test_data->test_channel_handler);
    struct mqtt_mock_server_handler *mock_handler = state_test_data->test_channel_handler->impl;
    aws_linked_list_push_back(&mock_handler->response_messages, &connack_msg->queueing_handle);

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
    ASSERT_SUCCESS(aws_mqtt_client_connection_disconnect(state_test_data->mqtt_connection, s_on_disconnect_fn, state_test_data));
    s_wait_for_disconnect_to_complete(state_test_data);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(mqtt_connect_disconnect, s_setup_mqtt_server_fn,
        s_test_mqtt_connect_disconnect_fn, s_clean_up_mqtt_server_fn, &test_data)

