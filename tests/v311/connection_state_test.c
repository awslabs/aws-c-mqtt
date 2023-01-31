/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "mqtt_mock_server_handler.h"

#include <aws/mqtt/private/client_impl.h>

#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/io/host_resolver.h>

#include <aws/common/clock.h>
#include <aws/common/condition_variable.h>

#include <aws/testing/aws_test_harness.h>

#ifdef _WIN32
#    define LOCAL_SOCK_TEST_PATTERN "\\\\.\\pipe\\testsock%llu"
#else
#    define LOCAL_SOCK_TEST_PATTERN "testsock%llu.sock"
#endif

static const int TEST_LOG_SUBJECT = 60000;
static const int ONE_SEC = 1000000000;

struct received_publish_packet {
    struct aws_byte_buf topic;
    struct aws_byte_buf payload;
    bool dup;
    enum aws_mqtt_qos qos;
    bool retain;
};

struct mqtt_connection_state_test {
    struct aws_allocator *allocator;
    struct aws_channel *server_channel;
    struct aws_channel_handler *mock_server;
    struct aws_client_bootstrap *client_bootstrap;
    struct aws_server_bootstrap *server_bootstrap;
    struct aws_event_loop_group *el_group;
    struct aws_host_resolver *host_resolver;
    struct aws_socket_endpoint endpoint;
    struct aws_socket *listener;
    struct aws_mqtt_client *mqtt_client;
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
    int subscribe_complete_error;
    int op_complete_error;
    enum aws_mqtt_connect_return_code mqtt_return_code;
    int error;
    struct aws_condition_variable cvar;
    struct aws_mutex lock;
    /* any published messages from mock server, that you may not subscribe to. (Which should not happen in real life) */
    struct aws_array_list any_published_messages; /* list of struct received_publish_packet */
    size_t any_publishes_received;
    size_t expected_any_publishes;
    /* the published messages from mock server, that you did subscribe to. */
    struct aws_array_list published_messages; /* list of struct received_publish_packet */
    size_t publishes_received;
    size_t expected_publishes;
    /* The returned QoS from mock server */
    struct aws_array_list qos_returned; /* list of uint_8 */
    size_t ops_completed;
    size_t expected_ops_completed;
};

static struct mqtt_connection_state_test test_data = {0};

static void s_on_any_publish_received(
    struct aws_mqtt_client_connection *connection,
    const struct aws_byte_cursor *topic,
    const struct aws_byte_cursor *payload,
    bool dup,
    enum aws_mqtt_qos qos,
    bool retain,
    void *userdata);

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
        mqtt_mock_server_handler_update_slot(state_test_data->mock_server, test_handler_slot);
        aws_channel_slot_set_handler(test_handler_slot, state_test_data->mock_server);
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
    state_test_data->el_group = aws_event_loop_group_new_default(allocator, 1, NULL);

    state_test_data->mock_server = new_mqtt_mock_server(allocator);
    ASSERT_NOT_NULL(state_test_data->mock_server);

    state_test_data->server_bootstrap = aws_server_bootstrap_new(allocator, state_test_data->el_group);
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

    struct aws_host_resolver_default_options resolver_options = {
        .el_group = state_test_data->el_group,
        .max_entries = 1,
    };
    state_test_data->host_resolver = aws_host_resolver_new_default(allocator, &resolver_options);

    struct aws_client_bootstrap_options bootstrap_options = {
        .event_loop_group = state_test_data->el_group,
        .user_data = state_test_data,
        .host_resolver = state_test_data->host_resolver,
    };

    state_test_data->client_bootstrap = aws_client_bootstrap_new(allocator, &bootstrap_options);

    state_test_data->mqtt_client = aws_mqtt_client_new(allocator, state_test_data->client_bootstrap);
    state_test_data->mqtt_connection = aws_mqtt_client_connection_new(state_test_data->mqtt_client);
    ASSERT_NOT_NULL(state_test_data->mqtt_connection);

    ASSERT_SUCCESS(aws_mqtt_client_connection_set_connection_interruption_handlers(
        state_test_data->mqtt_connection,
        s_on_connection_interrupted,
        state_test_data,
        s_on_connection_resumed,
        state_test_data));

    ASSERT_SUCCESS(aws_mqtt_client_connection_set_on_any_publish_handler(
        state_test_data->mqtt_connection, s_on_any_publish_received, state_test_data));

    ASSERT_SUCCESS(aws_array_list_init_dynamic(
        &state_test_data->published_messages, allocator, 4, sizeof(struct received_publish_packet)));
    ASSERT_SUCCESS(aws_array_list_init_dynamic(
        &state_test_data->any_published_messages, allocator, 4, sizeof(struct received_publish_packet)));
    ASSERT_SUCCESS(aws_array_list_init_dynamic(&state_test_data->qos_returned, allocator, 2, sizeof(uint8_t)));
    return AWS_OP_SUCCESS;
}

static void s_received_publish_packet_list_clean_up(struct aws_array_list *list) {
    for (size_t i = 0; i < aws_array_list_length(list); ++i) {
        struct received_publish_packet *val_ptr = NULL;
        aws_array_list_get_at_ptr(list, (void **)&val_ptr, i);
        aws_byte_buf_clean_up(&val_ptr->payload);
        aws_byte_buf_clean_up(&val_ptr->topic);
    }
    aws_array_list_clean_up(list);
}

static int s_clean_up_mqtt_server_fn(struct aws_allocator *allocator, int setup_result, void *ctx) {
    (void)allocator;

    if (!setup_result) {
        struct mqtt_connection_state_test *state_test_data = ctx;

        s_received_publish_packet_list_clean_up(&state_test_data->published_messages);
        s_received_publish_packet_list_clean_up(&state_test_data->any_published_messages);
        aws_array_list_clean_up(&state_test_data->qos_returned);
        aws_mqtt_client_connection_release(state_test_data->mqtt_connection);
        aws_mqtt_client_release(state_test_data->mqtt_client);
        aws_client_bootstrap_release(state_test_data->client_bootstrap);
        aws_host_resolver_release(state_test_data->host_resolver);
        aws_server_bootstrap_destroy_socket_listener(state_test_data->server_bootstrap, state_test_data->listener);
        s_wait_on_listener_cleanup(state_test_data);
        aws_server_bootstrap_release(state_test_data->server_bootstrap);
        aws_event_loop_group_release(state_test_data->el_group);
        destroy_mqtt_mock_server(state_test_data->mock_server);
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
    state_test_data->connection_completed = false;
    aws_mutex_unlock(&state_test_data->lock);
}

void s_on_disconnect_fn(struct aws_mqtt_client_connection *connection, void *userdata) {
    (void)connection;
    struct mqtt_connection_state_test *state_test_data = userdata;
    AWS_LOGF_DEBUG(TEST_LOG_SUBJECT, "disconnect completed");
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

static void s_on_any_publish_received(
    struct aws_mqtt_client_connection *connection,
    const struct aws_byte_cursor *topic,
    const struct aws_byte_cursor *payload,
    bool dup,
    enum aws_mqtt_qos qos,
    bool retain,
    void *userdata) {
    (void)connection;
    struct mqtt_connection_state_test *state_test_data = userdata;

    struct aws_byte_buf payload_cp;
    aws_byte_buf_init_copy_from_cursor(&payload_cp, state_test_data->allocator, *payload);
    struct aws_byte_buf topic_cp;
    aws_byte_buf_init_copy_from_cursor(&topic_cp, state_test_data->allocator, *topic);
    struct received_publish_packet received_packet = {
        .payload = payload_cp,
        .topic = topic_cp,
        .dup = dup,
        .qos = qos,
        .retain = retain,
    };

    aws_mutex_lock(&state_test_data->lock);
    aws_array_list_push_back(&state_test_data->any_published_messages, &received_packet);
    state_test_data->any_publishes_received++;
    aws_mutex_unlock(&state_test_data->lock);
    aws_condition_variable_notify_one(&state_test_data->cvar);
}

static bool s_is_any_publish_received(void *arg) {
    struct mqtt_connection_state_test *state_test_data = arg;
    return state_test_data->any_publishes_received == state_test_data->expected_any_publishes;
}

static void s_wait_for_any_publish(struct mqtt_connection_state_test *state_test_data) {
    aws_mutex_lock(&state_test_data->lock);
    aws_condition_variable_wait_pred(
        &state_test_data->cvar, &state_test_data->lock, s_is_any_publish_received, state_test_data);
    state_test_data->any_publishes_received = 0;
    state_test_data->expected_any_publishes = 0;
    aws_mutex_unlock(&state_test_data->lock);
}

static void s_on_publish_received(
    struct aws_mqtt_client_connection *connection,
    const struct aws_byte_cursor *topic,
    const struct aws_byte_cursor *payload,
    bool dup,
    enum aws_mqtt_qos qos,
    bool retain,
    void *userdata) {

    (void)connection;
    (void)topic;
    struct mqtt_connection_state_test *state_test_data = userdata;

    struct aws_byte_buf payload_cp;
    aws_byte_buf_init_copy_from_cursor(&payload_cp, state_test_data->allocator, *payload);
    struct aws_byte_buf topic_cp;
    aws_byte_buf_init_copy_from_cursor(&topic_cp, state_test_data->allocator, *topic);
    struct received_publish_packet received_packet = {
        .payload = payload_cp,
        .topic = topic_cp,
        .dup = dup,
        .qos = qos,
        .retain = retain,
    };

    aws_mutex_lock(&state_test_data->lock);
    aws_array_list_push_back(&state_test_data->published_messages, &received_packet);
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

    struct mqtt_connection_state_test *state_test_data = userdata;

    aws_mutex_lock(&state_test_data->lock);
    if (!error_code) {
        aws_array_list_push_back(&state_test_data->qos_returned, &qos);
    }
    state_test_data->subscribe_completed = true;
    state_test_data->subscribe_complete_error = error_code;
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
    state_test_data->subscribe_completed = false;
    aws_mutex_unlock(&state_test_data->lock);
}

static void s_on_multi_suback(
    struct aws_mqtt_client_connection *connection,
    uint16_t packet_id,
    const struct aws_array_list *topic_subacks, /* contains aws_mqtt_topic_subscription pointers */
    int error_code,
    void *userdata) {
    (void)connection;
    (void)packet_id;
    (void)topic_subacks;
    (void)error_code;

    struct mqtt_connection_state_test *state_test_data = userdata;

    aws_mutex_lock(&state_test_data->lock);
    state_test_data->subscribe_completed = true;
    if (!error_code) {
        size_t length = aws_array_list_length(topic_subacks);
        for (size_t i = 0; i < length; ++i) {
            struct aws_mqtt_topic_subscription *subscription = NULL;
            aws_array_list_get_at(topic_subacks, &subscription, i);
            aws_array_list_push_back(&state_test_data->qos_returned, &subscription->qos);
        }
    }
    aws_mutex_unlock(&state_test_data->lock);
    aws_condition_variable_notify_one(&state_test_data->cvar);
}

static void s_on_op_complete(
    struct aws_mqtt_client_connection *connection,
    uint16_t packet_id,
    int error_code,
    void *userdata) {
    (void)connection;
    (void)packet_id;

    struct mqtt_connection_state_test *state_test_data = userdata;
    AWS_LOGF_DEBUG(TEST_LOG_SUBJECT, "pub op completed");
    aws_mutex_lock(&state_test_data->lock);
    state_test_data->ops_completed++;
    state_test_data->op_complete_error = error_code;
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

    /* Decode all received packets by mock server */
    ASSERT_SUCCESS(mqtt_mock_server_decode_packets(state_test_data->mock_server));

    ASSERT_UINT_EQUALS(2, mqtt_mock_server_decoded_packets_count(state_test_data->mock_server));
    struct mqtt_decoded_packet *received_packet =
        mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 0);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_CONNECT, received_packet->type);
    ASSERT_UINT_EQUALS(connection_options.clean_session, received_packet->clean_session);
    ASSERT_TRUE(aws_byte_cursor_eq(&received_packet->client_identifier, &connection_options.client_id));

    received_packet = mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 1);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_DISCONNECT, received_packet->type);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    mqtt_connect_disconnect,
    s_setup_mqtt_server_fn,
    s_test_mqtt_connect_disconnect_fn,
    s_clean_up_mqtt_server_fn,
    &test_data)

/*
 * Makes an Mqtt connect call, and set will and login information for the connection. Validate the those information are
 * correctly included in the CONNECT package.
 */
static int s_test_mqtt_connect_set_will_login_fn(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    struct mqtt_connection_state_test *state_test_data = ctx;

    struct aws_byte_cursor will_payload = aws_byte_cursor_from_c_str("this is a will.");
    struct aws_byte_cursor topic = aws_byte_cursor_from_c_str("test_topic");
    struct aws_byte_cursor username = aws_byte_cursor_from_c_str("user name");
    struct aws_byte_cursor password = aws_byte_cursor_from_c_str("password");
    enum aws_mqtt_qos will_qos = AWS_MQTT_QOS_AT_LEAST_ONCE;

    ASSERT_SUCCESS(aws_mqtt_client_connection_set_will(
        state_test_data->mqtt_connection, &topic, will_qos, true /*retain*/, &will_payload));

    ASSERT_SUCCESS(aws_mqtt_client_connection_set_login(state_test_data->mqtt_connection, &username, &password));

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

    /* Decode all received packets by mock server */
    ASSERT_SUCCESS(mqtt_mock_server_decode_packets(state_test_data->mock_server));

    ASSERT_UINT_EQUALS(2, mqtt_mock_server_decoded_packets_count(state_test_data->mock_server));

    /* CONNECT packet */
    struct mqtt_decoded_packet *received_packet =
        mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 0);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_CONNECT, received_packet->type);
    ASSERT_UINT_EQUALS(connection_options.clean_session, received_packet->clean_session);
    ASSERT_TRUE(aws_byte_cursor_eq(&received_packet->client_identifier, &connection_options.client_id));
    /* validate the received will */
    ASSERT_TRUE(aws_byte_cursor_eq(&received_packet->will_message, &will_payload));
    ASSERT_TRUE(aws_byte_cursor_eq(&received_packet->will_topic, &topic));
    ASSERT_UINT_EQUALS(will_qos, received_packet->will_qos);
    ASSERT_TRUE(true == received_packet->will_retain);
    /* validate the received login information */
    ASSERT_TRUE(aws_byte_cursor_eq(&received_packet->username, &username));
    ASSERT_TRUE(aws_byte_cursor_eq(&received_packet->password, &password));

    /* DISCONNECT packet */
    received_packet = mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 1);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_DISCONNECT, received_packet->type);

    /* Connect to the mock server again. If set will&loggin message is not called before the next connect, the
     * will&loggin message will still be there and be sent to the server again */
    ASSERT_SUCCESS(aws_mqtt_client_connection_connect(state_test_data->mqtt_connection, &connection_options));
    s_wait_for_connection_to_complete(state_test_data);
    ASSERT_SUCCESS(mqtt_mock_server_decode_packets(state_test_data->mock_server));

    /* The second CONNECT packet */
    received_packet = mqtt_mock_server_get_latest_decoded_packet(state_test_data->mock_server);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_CONNECT, received_packet->type);
    ASSERT_UINT_EQUALS(connection_options.clean_session, received_packet->clean_session);
    ASSERT_TRUE(aws_byte_cursor_eq(&received_packet->client_identifier, &connection_options.client_id));
    /* validate the received will */
    ASSERT_TRUE(aws_byte_cursor_eq(&received_packet->will_message, &will_payload));
    ASSERT_TRUE(aws_byte_cursor_eq(&received_packet->will_topic, &topic));
    ASSERT_UINT_EQUALS(will_qos, received_packet->will_qos);
    ASSERT_TRUE(true == received_packet->will_retain);
    /* validate the received login information */
    ASSERT_TRUE(aws_byte_cursor_eq(&received_packet->username, &username));
    ASSERT_TRUE(aws_byte_cursor_eq(&received_packet->password, &password));

    /* disconnect */
    ASSERT_SUCCESS(
        aws_mqtt_client_connection_disconnect(state_test_data->mqtt_connection, s_on_disconnect_fn, state_test_data));
    s_wait_for_disconnect_to_complete(state_test_data);

    /* set new will & loggin message, before next connect, the next CONNECT packet will contain the new information */
    struct aws_byte_cursor new_will_payload = aws_byte_cursor_from_c_str("this is a new will.");
    struct aws_byte_cursor new_topic = aws_byte_cursor_from_c_str("test_topic_New");
    struct aws_byte_cursor new_username = aws_byte_cursor_from_c_str("new user name");
    struct aws_byte_cursor new_password = aws_byte_cursor_from_c_str("new password");
    enum aws_mqtt_qos new_will_qos = AWS_MQTT_QOS_AT_MOST_ONCE;

    ASSERT_SUCCESS(aws_mqtt_client_connection_set_will(
        state_test_data->mqtt_connection, &new_topic, new_will_qos, true /*retain*/, &new_will_payload));

    ASSERT_SUCCESS(
        aws_mqtt_client_connection_set_login(state_test_data->mqtt_connection, &new_username, &new_password));

    /* connect again */
    ASSERT_SUCCESS(aws_mqtt_client_connection_connect(state_test_data->mqtt_connection, &connection_options));
    s_wait_for_connection_to_complete(state_test_data);
    ASSERT_SUCCESS(mqtt_mock_server_decode_packets(state_test_data->mock_server));

    /* The third CONNECT packet */
    received_packet = mqtt_mock_server_get_latest_decoded_packet(state_test_data->mock_server);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_CONNECT, received_packet->type);
    ASSERT_UINT_EQUALS(connection_options.clean_session, received_packet->clean_session);
    ASSERT_TRUE(aws_byte_cursor_eq(&received_packet->client_identifier, &connection_options.client_id));
    /* validate the received will */
    ASSERT_TRUE(aws_byte_cursor_eq(&received_packet->will_message, &new_will_payload));
    ASSERT_TRUE(aws_byte_cursor_eq(&received_packet->will_topic, &new_topic));
    ASSERT_UINT_EQUALS(new_will_qos, received_packet->will_qos);
    ASSERT_TRUE(true == received_packet->will_retain);
    /* validate the received login information */
    ASSERT_TRUE(aws_byte_cursor_eq(&received_packet->username, &new_username));
    ASSERT_TRUE(aws_byte_cursor_eq(&received_packet->password, &new_password));

    /* disconnect. FINISHED */
    ASSERT_SUCCESS(
        aws_mqtt_client_connection_disconnect(state_test_data->mqtt_connection, s_on_disconnect_fn, state_test_data));
    s_wait_for_disconnect_to_complete(state_test_data);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    mqtt_connect_set_will_login,
    s_setup_mqtt_server_fn,
    s_test_mqtt_connect_set_will_login_fn,
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

    /* Decode all received packets by mock server */
    ASSERT_SUCCESS(mqtt_mock_server_decode_packets(state_test_data->mock_server));

    ASSERT_UINT_EQUALS(3, mqtt_mock_server_decoded_packets_count(state_test_data->mock_server));
    struct mqtt_decoded_packet *received_packet =
        mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 0);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_CONNECT, received_packet->type);
    ASSERT_UINT_EQUALS(connection_options.clean_session, received_packet->clean_session);
    ASSERT_TRUE(aws_byte_cursor_eq(&received_packet->client_identifier, &connection_options.client_id));

    received_packet = mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 1);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_CONNECT, received_packet->type);
    ASSERT_UINT_EQUALS(connection_options.clean_session, received_packet->clean_session);
    ASSERT_TRUE(aws_byte_cursor_eq(&received_packet->client_identifier, &connection_options.client_id));

    received_packet = mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 2);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_DISCONNECT, received_packet->type);

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

    mqtt_mock_server_set_max_ping_resp(state_test_data->mock_server, 0);
    ASSERT_SUCCESS(aws_mqtt_client_connection_connect(state_test_data->mqtt_connection, &connection_options));
    s_wait_for_connection_to_complete(state_test_data);

    /* this should take about 1.1 seconds for the timeout and reconnect.*/
    s_wait_for_reconnect_to_complete(state_test_data);

    ASSERT_SUCCESS(
        aws_mqtt_client_connection_disconnect(state_test_data->mqtt_connection, s_on_disconnect_fn, state_test_data));
    s_wait_for_disconnect_to_complete(state_test_data);

    ASSERT_INT_EQUALS(AWS_ERROR_MQTT_TIMEOUT, state_test_data->interruption_error);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    mqtt_connection_timeout,
    s_setup_mqtt_server_fn,
    s_test_mqtt_connection_timeout_fn,
    s_clean_up_mqtt_server_fn,
    &test_data)

/* Test set on_any_publish handler. User can set on_any_publish handler to be called whenever any publish packet is
 * received */
static int s_test_mqtt_connection_any_publish_fn(struct aws_allocator *allocator, void *ctx) {
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

    struct aws_byte_cursor topic_1 = aws_byte_cursor_from_c_str("/test/topic1");
    struct aws_byte_cursor topic_2 = aws_byte_cursor_from_c_str("/test/topic2");

    ASSERT_SUCCESS(aws_mqtt_client_connection_connect(state_test_data->mqtt_connection, &connection_options));
    s_wait_for_connection_to_complete(state_test_data);

    /* NOTE: mock server sends to client with no subscription at all, which should not happen in the real world! */
    state_test_data->expected_any_publishes = 2;
    struct aws_byte_cursor payload_1 = aws_byte_cursor_from_c_str("Test Message 1");
    ASSERT_SUCCESS(mqtt_mock_server_send_publish(
        state_test_data->mock_server,
        &topic_1,
        &payload_1,
        false /*dup*/,
        AWS_MQTT_QOS_AT_LEAST_ONCE,
        false /*retain*/));
    struct aws_byte_cursor payload_2 = aws_byte_cursor_from_c_str("Test Message 2");
    ASSERT_SUCCESS(mqtt_mock_server_send_publish(
        state_test_data->mock_server,
        &topic_2,
        &payload_2,
        false /*dup*/,
        AWS_MQTT_QOS_AT_LEAST_ONCE,
        false /*retain*/));

    s_wait_for_any_publish(state_test_data);
    mqtt_mock_server_wait_for_pubacks(state_test_data->mock_server, 2);

    ASSERT_SUCCESS(
        aws_mqtt_client_connection_disconnect(state_test_data->mqtt_connection, s_on_disconnect_fn, state_test_data));
    s_wait_for_disconnect_to_complete(state_test_data);

    /* Decode all received packets by mock server */
    ASSERT_SUCCESS(mqtt_mock_server_decode_packets(state_test_data->mock_server));

    /* CONNECT two PUBACK DISCONNECT */
    ASSERT_UINT_EQUALS(4, mqtt_mock_server_decoded_packets_count(state_test_data->mock_server));
    struct mqtt_decoded_packet *received_packet =
        mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 0);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_CONNECT, received_packet->type);
    ASSERT_UINT_EQUALS(connection_options.clean_session, received_packet->clean_session);
    ASSERT_TRUE(aws_byte_cursor_eq(&received_packet->client_identifier, &connection_options.client_id));

    received_packet = mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 1);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_PUBACK, received_packet->type);

    received_packet = mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 2);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_PUBACK, received_packet->type);

    received_packet = mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 3);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_DISCONNECT, received_packet->type);

    /* Check the received publish packet from the client side */
    ASSERT_UINT_EQUALS(2, aws_array_list_length(&state_test_data->any_published_messages));
    struct received_publish_packet *publish_msg = NULL;
    ASSERT_SUCCESS(aws_array_list_get_at_ptr(&state_test_data->any_published_messages, (void **)&publish_msg, 0));
    ASSERT_TRUE(aws_byte_cursor_eq_byte_buf(&topic_1, &publish_msg->topic));
    ASSERT_TRUE(aws_byte_cursor_eq_byte_buf(&payload_1, &publish_msg->payload));
    ASSERT_SUCCESS(aws_array_list_get_at_ptr(&state_test_data->any_published_messages, (void **)&publish_msg, 1));
    ASSERT_TRUE(aws_byte_cursor_eq_byte_buf(&topic_2, &publish_msg->topic));
    ASSERT_TRUE(aws_byte_cursor_eq_byte_buf(&payload_2, &publish_msg->payload));

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    mqtt_connection_any_publish,
    s_setup_mqtt_server_fn,
    s_test_mqtt_connection_any_publish_fn,
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

    mqtt_mock_server_set_max_connack(state_test_data->mock_server, 0);
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

    struct aws_byte_cursor sub_topic = aws_byte_cursor_from_c_str("/test/topic");

    uint16_t packet_id = aws_mqtt_client_connection_subscribe(
        state_test_data->mqtt_connection,
        &sub_topic,
        AWS_MQTT_QOS_AT_LEAST_ONCE,
        s_on_publish_received,
        state_test_data,
        NULL,
        s_on_suback,
        state_test_data);
    ASSERT_TRUE(packet_id > 0);

    ASSERT_SUCCESS(aws_mqtt_client_connection_connect(state_test_data->mqtt_connection, &connection_options));
    s_wait_for_connection_to_complete(state_test_data);

    s_wait_for_subscribe_to_complete(state_test_data);

    state_test_data->expected_publishes = 2;
    state_test_data->expected_any_publishes = 2;
    struct aws_byte_cursor payload_1 = aws_byte_cursor_from_c_str("Test Message 1");
    ASSERT_SUCCESS(mqtt_mock_server_send_publish(
        state_test_data->mock_server,
        &sub_topic,
        &payload_1,
        false /*dup*/,
        AWS_MQTT_QOS_AT_LEAST_ONCE,
        true /*retain*/));
    struct aws_byte_cursor payload_2 = aws_byte_cursor_from_c_str("Test Message 2");
    ASSERT_SUCCESS(mqtt_mock_server_send_publish(
        state_test_data->mock_server,
        &sub_topic,
        &payload_2,
        true /*dup*/,
        AWS_MQTT_QOS_AT_LEAST_ONCE,
        false /*retain*/));

    s_wait_for_publish(state_test_data);
    s_wait_for_any_publish(state_test_data);
    mqtt_mock_server_wait_for_pubacks(state_test_data->mock_server, 2);

    ASSERT_SUCCESS(
        aws_mqtt_client_connection_disconnect(state_test_data->mqtt_connection, s_on_disconnect_fn, state_test_data));
    s_wait_for_disconnect_to_complete(state_test_data);

    /* Decode all received packets by mock server */
    ASSERT_SUCCESS(mqtt_mock_server_decode_packets(state_test_data->mock_server));

    ASSERT_UINT_EQUALS(5, mqtt_mock_server_decoded_packets_count(state_test_data->mock_server));
    struct mqtt_decoded_packet *received_packet =
        mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 0);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_CONNECT, received_packet->type);
    ASSERT_UINT_EQUALS(connection_options.clean_session, received_packet->clean_session);
    ASSERT_TRUE(aws_byte_cursor_eq(&received_packet->client_identifier, &connection_options.client_id));

    received_packet = mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 1);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_SUBSCRIBE, received_packet->type);
    ASSERT_UINT_EQUALS(1, aws_array_list_length(&received_packet->sub_topic_filters));
    struct aws_mqtt_subscription val;
    ASSERT_SUCCESS(aws_array_list_front(&received_packet->sub_topic_filters, &val));
    ASSERT_TRUE(aws_byte_cursor_eq(&val.topic_filter, &sub_topic));
    ASSERT_UINT_EQUALS(AWS_MQTT_QOS_AT_LEAST_ONCE, val.qos);
    ASSERT_UINT_EQUALS(packet_id, received_packet->packet_identifier);

    received_packet = mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 2);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_PUBACK, received_packet->type);

    received_packet = mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 3);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_PUBACK, received_packet->type);

    received_packet = mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 4);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_DISCONNECT, received_packet->type);

    /* Check PUBLISH packets received via subscription callback */
    ASSERT_UINT_EQUALS(2, aws_array_list_length(&state_test_data->published_messages));

    struct received_publish_packet *publish_msg = NULL;
    ASSERT_SUCCESS(aws_array_list_get_at_ptr(&state_test_data->published_messages, (void **)&publish_msg, 0));
    ASSERT_TRUE(aws_byte_cursor_eq_byte_buf(&sub_topic, &publish_msg->topic));
    ASSERT_TRUE(aws_byte_cursor_eq_byte_buf(&payload_1, &publish_msg->payload));
    ASSERT_FALSE(publish_msg->dup);
    ASSERT_TRUE(publish_msg->retain);

    ASSERT_SUCCESS(aws_array_list_get_at_ptr(&state_test_data->published_messages, (void **)&publish_msg, 1));
    ASSERT_TRUE(aws_byte_cursor_eq_byte_buf(&sub_topic, &publish_msg->topic));
    ASSERT_TRUE(aws_byte_cursor_eq_byte_buf(&payload_2, &publish_msg->payload));
    ASSERT_TRUE(publish_msg->dup);
    ASSERT_FALSE(publish_msg->retain);

    /* Check PUBLISH packets received via on_any_publish callback */
    ASSERT_UINT_EQUALS(2, aws_array_list_length(&state_test_data->any_published_messages));

    ASSERT_SUCCESS(aws_array_list_get_at_ptr(&state_test_data->any_published_messages, (void **)&publish_msg, 0));
    ASSERT_TRUE(aws_byte_cursor_eq_byte_buf(&sub_topic, &publish_msg->topic));
    ASSERT_TRUE(aws_byte_cursor_eq_byte_buf(&payload_1, &publish_msg->payload));
    ASSERT_FALSE(publish_msg->dup);
    ASSERT_TRUE(publish_msg->retain);

    ASSERT_SUCCESS(aws_array_list_get_at_ptr(&state_test_data->any_published_messages, (void **)&publish_msg, 1));
    ASSERT_TRUE(aws_byte_cursor_eq_byte_buf(&sub_topic, &publish_msg->topic));
    ASSERT_TRUE(aws_byte_cursor_eq_byte_buf(&payload_2, &publish_msg->payload));
    ASSERT_TRUE(publish_msg->dup);
    ASSERT_FALSE(publish_msg->retain);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    mqtt_connect_subscribe,
    s_setup_mqtt_server_fn,
    s_test_mqtt_subscribe_fn,
    s_clean_up_mqtt_server_fn,
    &test_data)

/* Subscribe to a topic and broker returns a SUBACK with failure return code, the subscribe should fail */
static int s_test_mqtt_connect_subscribe_fail_from_broker_fn(struct aws_allocator *allocator, void *ctx) {
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

    /* Disable the auto ACK packets sent by the server, we will send failure SUBACK */
    mqtt_mock_server_disable_auto_ack(state_test_data->mock_server);

    struct aws_byte_cursor sub_topic = aws_byte_cursor_from_c_str("/test/topic");

    uint16_t packet_id = aws_mqtt_client_connection_subscribe(
        state_test_data->mqtt_connection,
        &sub_topic,
        AWS_MQTT_QOS_AT_LEAST_ONCE,
        s_on_publish_received,
        state_test_data,
        NULL,
        s_on_suback,
        state_test_data);
    ASSERT_TRUE(packet_id > 0);

    ASSERT_SUCCESS(mqtt_mock_server_send_single_suback(state_test_data->mock_server, packet_id, AWS_MQTT_QOS_FAILURE));
    s_wait_for_subscribe_to_complete(state_test_data);
    /* Check the subscribe returned QoS is failure */
    size_t length = aws_array_list_length(&state_test_data->qos_returned);
    ASSERT_UINT_EQUALS(1, length);
    uint8_t qos = 0;
    ASSERT_SUCCESS(aws_array_list_get_at(&state_test_data->qos_returned, &qos, 0));
    ASSERT_UINT_EQUALS(AWS_MQTT_QOS_FAILURE, qos);

    ASSERT_SUCCESS(
        aws_mqtt_client_connection_disconnect(state_test_data->mqtt_connection, s_on_disconnect_fn, state_test_data));
    s_wait_for_disconnect_to_complete(state_test_data);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    mqtt_connect_subscribe_fail_from_broker,
    s_setup_mqtt_server_fn,
    s_test_mqtt_connect_subscribe_fail_from_broker_fn,
    s_clean_up_mqtt_server_fn,
    &test_data)

/* Subscribe to multiple topics prior to connection, make a CONNECT, have the server send PUBLISH messages,
 * make sure they're received, then send a DISCONNECT. */
static int s_test_mqtt_subscribe_multi_fn(struct aws_allocator *allocator, void *ctx) {
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

    struct aws_byte_cursor sub_topic_1 = aws_byte_cursor_from_c_str("/test/topic1");
    struct aws_byte_cursor sub_topic_2 = aws_byte_cursor_from_c_str("/test/topic2");

    struct aws_mqtt_topic_subscription sub1 = {
        .topic = sub_topic_1,
        .qos = AWS_MQTT_QOS_AT_LEAST_ONCE,
        .on_publish = s_on_publish_received,
        .on_cleanup = NULL,
        .on_publish_ud = state_test_data,
    };
    struct aws_mqtt_topic_subscription sub2 = {
        .topic = sub_topic_2,
        .qos = AWS_MQTT_QOS_AT_LEAST_ONCE,
        .on_publish = s_on_publish_received,
        .on_cleanup = NULL,
        .on_publish_ud = state_test_data,
    };

    struct aws_array_list topic_filters;
    size_t list_len = 2;
    AWS_VARIABLE_LENGTH_ARRAY(uint8_t, static_buf, list_len * sizeof(struct aws_mqtt_topic_subscription));
    aws_array_list_init_static(&topic_filters, static_buf, list_len, sizeof(struct aws_mqtt_topic_subscription));

    aws_array_list_push_back(&topic_filters, &sub1);
    aws_array_list_push_back(&topic_filters, &sub2);

    uint16_t packet_id = aws_mqtt_client_connection_subscribe_multiple(
        state_test_data->mqtt_connection, &topic_filters, s_on_multi_suback, state_test_data);
    ASSERT_TRUE(packet_id > 0);

    ASSERT_SUCCESS(aws_mqtt_client_connection_connect(state_test_data->mqtt_connection, &connection_options));
    s_wait_for_connection_to_complete(state_test_data);

    s_wait_for_subscribe_to_complete(state_test_data);
    /* Check the subscribe returned QoS is expected */
    size_t length = aws_array_list_length(&state_test_data->qos_returned);
    ASSERT_UINT_EQUALS(2, length);
    uint8_t qos = 0;
    ASSERT_SUCCESS(aws_array_list_get_at(&state_test_data->qos_returned, &qos, 0));
    ASSERT_UINT_EQUALS(AWS_MQTT_QOS_EXACTLY_ONCE, qos);
    ASSERT_SUCCESS(aws_array_list_get_at(&state_test_data->qos_returned, &qos, 1));
    ASSERT_UINT_EQUALS(AWS_MQTT_QOS_EXACTLY_ONCE, qos);

    state_test_data->expected_publishes = 2;
    struct aws_byte_cursor payload_1 = aws_byte_cursor_from_c_str("Test Message 1");
    ASSERT_SUCCESS(mqtt_mock_server_send_publish(
        state_test_data->mock_server,
        &sub_topic_1,
        &payload_1,
        false /*dup*/,
        AWS_MQTT_QOS_AT_LEAST_ONCE,
        false /*retain*/));
    struct aws_byte_cursor payload_2 = aws_byte_cursor_from_c_str("Test Message 2");
    ASSERT_SUCCESS(mqtt_mock_server_send_publish(
        state_test_data->mock_server,
        &sub_topic_2,
        &payload_2,
        false /*dup*/,
        AWS_MQTT_QOS_AT_LEAST_ONCE,
        false /*retain*/));
    s_wait_for_publish(state_test_data);

    /* Let's do another publish on a topic that is not subscribed by client.
     * This can happen if the Server automatically assigned a subscription to the Client */
    state_test_data->expected_any_publishes = 3;
    struct aws_byte_cursor payload_3 = aws_byte_cursor_from_c_str("Test Message 3");
    struct aws_byte_cursor topic_3 = aws_byte_cursor_from_c_str("/test/topic3");
    ASSERT_SUCCESS(mqtt_mock_server_send_publish(
        state_test_data->mock_server,
        &topic_3,
        &payload_3,
        false /*dup*/,
        AWS_MQTT_QOS_AT_LEAST_ONCE,
        false /*retain*/));
    s_wait_for_any_publish(state_test_data);

    mqtt_mock_server_wait_for_pubacks(state_test_data->mock_server, 3);

    ASSERT_SUCCESS(
        aws_mqtt_client_connection_disconnect(state_test_data->mqtt_connection, s_on_disconnect_fn, state_test_data));
    s_wait_for_disconnect_to_complete(state_test_data);

    /* Decode all received packets by mock server */
    ASSERT_SUCCESS(mqtt_mock_server_decode_packets(state_test_data->mock_server));

    ASSERT_UINT_EQUALS(6, mqtt_mock_server_decoded_packets_count(state_test_data->mock_server));
    struct mqtt_decoded_packet *received_packet =
        mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 0);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_CONNECT, received_packet->type);

    received_packet = mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 1);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_SUBSCRIBE, received_packet->type);
    ASSERT_UINT_EQUALS(2, aws_array_list_length(&received_packet->sub_topic_filters));
    struct aws_mqtt_subscription val;
    ASSERT_SUCCESS(aws_array_list_front(&received_packet->sub_topic_filters, &val));
    ASSERT_TRUE(aws_byte_cursor_eq(&val.topic_filter, &sub_topic_1));
    ASSERT_UINT_EQUALS(AWS_MQTT_QOS_AT_LEAST_ONCE, val.qos);
    ASSERT_SUCCESS(aws_array_list_back(&received_packet->sub_topic_filters, &val));
    ASSERT_TRUE(aws_byte_cursor_eq(&val.topic_filter, &sub_topic_2));
    ASSERT_UINT_EQUALS(AWS_MQTT_QOS_AT_LEAST_ONCE, val.qos);
    ASSERT_UINT_EQUALS(packet_id, received_packet->packet_identifier);

    received_packet = mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 2);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_PUBACK, received_packet->type);
    received_packet = mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 3);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_PUBACK, received_packet->type);
    received_packet = mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 4);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_PUBACK, received_packet->type);

    received_packet = mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 5);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_DISCONNECT, received_packet->type);

    /* Only two packets should be recorded by the published_messages, but all the three packets will be recorded by
     * any_published_messages */
    ASSERT_UINT_EQUALS(2, aws_array_list_length(&state_test_data->published_messages));
    ASSERT_UINT_EQUALS(3, aws_array_list_length(&state_test_data->any_published_messages));

    struct received_publish_packet *publish_msg = NULL;
    ASSERT_SUCCESS(aws_array_list_get_at_ptr(&state_test_data->published_messages, (void **)&publish_msg, 0));
    ASSERT_TRUE(aws_byte_cursor_eq_byte_buf(&sub_topic_1, &publish_msg->topic));
    ASSERT_TRUE(aws_byte_cursor_eq_byte_buf(&payload_1, &publish_msg->payload));
    ASSERT_SUCCESS(aws_array_list_get_at_ptr(&state_test_data->published_messages, (void **)&publish_msg, 1));
    ASSERT_TRUE(aws_byte_cursor_eq_byte_buf(&sub_topic_2, &publish_msg->topic));
    ASSERT_TRUE(aws_byte_cursor_eq_byte_buf(&payload_2, &publish_msg->payload));

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    mqtt_connect_subscribe_multi,
    s_setup_mqtt_server_fn,
    s_test_mqtt_subscribe_multi_fn,
    s_clean_up_mqtt_server_fn,
    &test_data)

/* Subscribe to multiple topics prior to connection, make a CONNECT, have the server send PUBLISH messages, unsubscribe
 * to a topic, have the server send PUBLISH messages again, make sure the unsubscribed topic callback will not be fired
 */
static int s_test_mqtt_unsubscribe_fn(struct aws_allocator *allocator, void *ctx) {
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

    struct aws_byte_cursor sub_topic_1 = aws_byte_cursor_from_c_str("/test/topic1");
    struct aws_byte_cursor sub_topic_2 = aws_byte_cursor_from_c_str("/test/topic2");

    struct aws_mqtt_topic_subscription sub1 = {
        .topic = sub_topic_1,
        .qos = AWS_MQTT_QOS_AT_LEAST_ONCE,
        .on_publish = s_on_publish_received,
        .on_cleanup = NULL,
        .on_publish_ud = state_test_data,
    };
    struct aws_mqtt_topic_subscription sub2 = {
        .topic = sub_topic_2,
        .qos = AWS_MQTT_QOS_AT_LEAST_ONCE,
        .on_publish = s_on_publish_received,
        .on_cleanup = NULL,
        .on_publish_ud = state_test_data,
    };

    struct aws_array_list topic_filters;
    size_t list_len = 2;
    AWS_VARIABLE_LENGTH_ARRAY(uint8_t, static_buf, list_len * sizeof(struct aws_mqtt_topic_subscription));
    aws_array_list_init_static(&topic_filters, static_buf, list_len, sizeof(struct aws_mqtt_topic_subscription));

    aws_array_list_push_back(&topic_filters, &sub1);
    aws_array_list_push_back(&topic_filters, &sub2);

    uint16_t sub_packet_id = aws_mqtt_client_connection_subscribe_multiple(
        state_test_data->mqtt_connection, &topic_filters, s_on_multi_suback, state_test_data);
    ASSERT_TRUE(sub_packet_id > 0);

    ASSERT_SUCCESS(aws_mqtt_client_connection_connect(state_test_data->mqtt_connection, &connection_options));
    s_wait_for_connection_to_complete(state_test_data);

    s_wait_for_subscribe_to_complete(state_test_data);

    state_test_data->expected_any_publishes = 2;
    struct aws_byte_cursor payload_1 = aws_byte_cursor_from_c_str("Test Message 1");
    ASSERT_SUCCESS(mqtt_mock_server_send_publish(
        state_test_data->mock_server,
        &sub_topic_1,
        &payload_1,
        false /*dup*/,
        AWS_MQTT_QOS_AT_LEAST_ONCE,
        false /*retain*/));
    struct aws_byte_cursor payload_2 = aws_byte_cursor_from_c_str("Test Message 2");
    ASSERT_SUCCESS(mqtt_mock_server_send_publish(
        state_test_data->mock_server,
        &sub_topic_2,
        &payload_2,
        false /*dup*/,
        AWS_MQTT_QOS_AT_LEAST_ONCE,
        false /*retain*/));
    s_wait_for_any_publish(state_test_data);
    mqtt_mock_server_wait_for_pubacks(state_test_data->mock_server, 2);

    aws_mutex_lock(&state_test_data->lock);
    state_test_data->expected_ops_completed = 1;
    aws_mutex_unlock(&state_test_data->lock);
    /* unsubscribe to the first topic */
    uint16_t unsub_packet_id = aws_mqtt_client_connection_unsubscribe(
        state_test_data->mqtt_connection, &sub_topic_1, s_on_op_complete, state_test_data);
    ASSERT_TRUE(unsub_packet_id > 0);
    /* Even when the UNSUBACK has not received, the client will not invoke the on_pub callback for that topic */
    ASSERT_SUCCESS(mqtt_mock_server_send_publish(
        state_test_data->mock_server,
        &sub_topic_1,
        &payload_1,
        false /*dup*/,
        AWS_MQTT_QOS_AT_LEAST_ONCE,
        false /*retain*/));
    ASSERT_SUCCESS(mqtt_mock_server_send_publish(
        state_test_data->mock_server,
        &sub_topic_2,
        &payload_2,
        false /*dup*/,
        AWS_MQTT_QOS_AT_LEAST_ONCE,
        false /*retain*/));
    state_test_data->expected_any_publishes = 2;
    s_wait_for_any_publish(state_test_data);
    mqtt_mock_server_wait_for_pubacks(state_test_data->mock_server, 2);
    s_wait_for_ops_completed(state_test_data);

    ASSERT_SUCCESS(
        aws_mqtt_client_connection_disconnect(state_test_data->mqtt_connection, s_on_disconnect_fn, state_test_data));
    s_wait_for_disconnect_to_complete(state_test_data);

    /* Decode all received packets by mock server */
    ASSERT_SUCCESS(mqtt_mock_server_decode_packets(state_test_data->mock_server));

    struct mqtt_decoded_packet *received_packet =
        mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 0);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_CONNECT, received_packet->type);

    received_packet = mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 1);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_SUBSCRIBE, received_packet->type);
    ASSERT_UINT_EQUALS(2, aws_array_list_length(&received_packet->sub_topic_filters));
    struct aws_mqtt_subscription val;
    ASSERT_SUCCESS(aws_array_list_front(&received_packet->sub_topic_filters, &val));
    ASSERT_TRUE(aws_byte_cursor_eq(&val.topic_filter, &sub_topic_1));
    ASSERT_UINT_EQUALS(AWS_MQTT_QOS_AT_LEAST_ONCE, val.qos);
    ASSERT_SUCCESS(aws_array_list_back(&received_packet->sub_topic_filters, &val));
    ASSERT_TRUE(aws_byte_cursor_eq(&val.topic_filter, &sub_topic_2));
    ASSERT_UINT_EQUALS(AWS_MQTT_QOS_AT_LEAST_ONCE, val.qos);
    ASSERT_UINT_EQUALS(sub_packet_id, received_packet->packet_identifier);

    received_packet = mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 2);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_PUBACK, received_packet->type);
    received_packet = mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 3);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_PUBACK, received_packet->type);

    received_packet = mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 4);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_UNSUBSCRIBE, received_packet->type);
    ASSERT_UINT_EQUALS(1, aws_array_list_length(&received_packet->unsub_topic_filters));
    struct aws_byte_cursor val_cur;
    ASSERT_SUCCESS(aws_array_list_front(&received_packet->unsub_topic_filters, &val_cur));
    ASSERT_TRUE(aws_byte_cursor_eq(&val_cur, &sub_topic_1));
    ASSERT_UINT_EQUALS(unsub_packet_id, received_packet->packet_identifier);

    received_packet = mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 5);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_PUBACK, received_packet->type);
    received_packet = mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 6);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_PUBACK, received_packet->type);

    received_packet = mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 7);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_DISCONNECT, received_packet->type);

    /* Only three packets should be recorded by the published_messages, but all the four packets will be recorded by
     * any_published_messages */
    ASSERT_UINT_EQUALS(3, aws_array_list_length(&state_test_data->published_messages));
    ASSERT_UINT_EQUALS(4, aws_array_list_length(&state_test_data->any_published_messages));

    struct received_publish_packet *publish_msg = NULL;
    ASSERT_SUCCESS(aws_array_list_get_at_ptr(&state_test_data->published_messages, (void **)&publish_msg, 0));
    ASSERT_TRUE(aws_byte_cursor_eq_byte_buf(&sub_topic_1, &publish_msg->topic));
    ASSERT_TRUE(aws_byte_cursor_eq_byte_buf(&payload_1, &publish_msg->payload));
    ASSERT_SUCCESS(aws_array_list_get_at_ptr(&state_test_data->published_messages, (void **)&publish_msg, 1));
    ASSERT_TRUE(aws_byte_cursor_eq_byte_buf(&sub_topic_2, &publish_msg->topic));
    ASSERT_TRUE(aws_byte_cursor_eq_byte_buf(&payload_2, &publish_msg->payload));
    ASSERT_SUCCESS(aws_array_list_get_at_ptr(&state_test_data->published_messages, (void **)&publish_msg, 2));
    ASSERT_TRUE(aws_byte_cursor_eq_byte_buf(&sub_topic_2, &publish_msg->topic));
    ASSERT_TRUE(aws_byte_cursor_eq_byte_buf(&payload_2, &publish_msg->payload));

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    mqtt_connect_unsubscribe,
    s_setup_mqtt_server_fn,
    s_test_mqtt_unsubscribe_fn,
    s_clean_up_mqtt_server_fn,
    &test_data)

/**
 * Subscribe to multiple topics prior to connection, make a CONNECT, have the server send PUBLISH messages, unsubscribe
 * to a topic, disconnect with the broker, make a connection with clean_session true, then call resubscribe, client will
 * successfully resubscribe to the old topics.
 */
static int s_test_mqtt_resubscribe_fn(struct aws_allocator *allocator, void *ctx) {
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

    struct aws_byte_cursor sub_topic_1 = aws_byte_cursor_from_c_str("/test/topic1");
    struct aws_byte_cursor sub_topic_2 = aws_byte_cursor_from_c_str("/test/topic2");
    struct aws_byte_cursor sub_topic_3 = aws_byte_cursor_from_c_str("/test/topic3");

    struct aws_mqtt_topic_subscription sub1 = {
        .topic = sub_topic_1,
        .qos = AWS_MQTT_QOS_AT_LEAST_ONCE,
        .on_publish = s_on_publish_received,
        .on_cleanup = NULL,
        .on_publish_ud = state_test_data,
    };
    struct aws_mqtt_topic_subscription sub2 = {
        .topic = sub_topic_2,
        .qos = AWS_MQTT_QOS_AT_LEAST_ONCE,
        .on_publish = s_on_publish_received,
        .on_cleanup = NULL,
        .on_publish_ud = state_test_data,
    };
    struct aws_mqtt_topic_subscription sub3 = {
        .topic = sub_topic_3,
        .qos = AWS_MQTT_QOS_AT_LEAST_ONCE,
        .on_publish = s_on_publish_received,
        .on_cleanup = NULL,
        .on_publish_ud = state_test_data,
    };

    struct aws_array_list topic_filters;
    size_t list_len = 3;
    AWS_VARIABLE_LENGTH_ARRAY(uint8_t, static_buf, list_len * sizeof(struct aws_mqtt_topic_subscription));
    aws_array_list_init_static(&topic_filters, static_buf, list_len, sizeof(struct aws_mqtt_topic_subscription));

    aws_array_list_push_back(&topic_filters, &sub1);
    aws_array_list_push_back(&topic_filters, &sub2);
    aws_array_list_push_back(&topic_filters, &sub3);

    uint16_t sub_packet_id = aws_mqtt_client_connection_subscribe_multiple(
        state_test_data->mqtt_connection, &topic_filters, s_on_multi_suback, state_test_data);
    ASSERT_TRUE(sub_packet_id > 0);

    ASSERT_SUCCESS(aws_mqtt_client_connection_connect(state_test_data->mqtt_connection, &connection_options));
    s_wait_for_connection_to_complete(state_test_data);

    s_wait_for_subscribe_to_complete(state_test_data);
    aws_mutex_lock(&state_test_data->lock);
    state_test_data->expected_ops_completed = 1;
    aws_mutex_unlock(&state_test_data->lock);
    /* unsubscribe to the first topic */
    uint16_t unsub_packet_id = aws_mqtt_client_connection_unsubscribe(
        state_test_data->mqtt_connection, &sub_topic_1, s_on_op_complete, state_test_data);
    ASSERT_TRUE(unsub_packet_id > 0);

    s_wait_for_ops_completed(state_test_data);
    /* client still subscribes to topic_2 & topic_3 */

    ASSERT_SUCCESS(
        aws_mqtt_client_connection_disconnect(state_test_data->mqtt_connection, s_on_disconnect_fn, state_test_data));
    s_wait_for_disconnect_to_complete(state_test_data);

    /* reconnection to the same server */
    ASSERT_SUCCESS(aws_mqtt_client_connection_connect(state_test_data->mqtt_connection, &connection_options));
    s_wait_for_connection_to_complete(state_test_data);

    /* Get all the packets out of the way */
    ASSERT_SUCCESS(mqtt_mock_server_decode_packets(state_test_data->mock_server));
    size_t packets_count = mqtt_mock_server_decoded_packets_count(state_test_data->mock_server);
    struct mqtt_decoded_packet *t_received_packet =
        mqtt_mock_server_get_latest_decoded_packet(state_test_data->mock_server);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_CONNECT, t_received_packet->type);

    /* resubscribes to topic_2 & topic_3 */
    uint16_t resub_packet_id =
        aws_mqtt_resubscribe_existing_topics(state_test_data->mqtt_connection, s_on_multi_suback, state_test_data);
    ASSERT_TRUE(resub_packet_id > 0);
    s_wait_for_subscribe_to_complete(state_test_data);

    ASSERT_SUCCESS(
        aws_mqtt_client_connection_disconnect(state_test_data->mqtt_connection, s_on_disconnect_fn, state_test_data));
    s_wait_for_disconnect_to_complete(state_test_data);

    ASSERT_SUCCESS(mqtt_mock_server_decode_packets(state_test_data->mock_server));
    struct mqtt_decoded_packet *received_packet =
        mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, packets_count);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_SUBSCRIBE, received_packet->type);

    ASSERT_UINT_EQUALS(2, aws_array_list_length(&received_packet->sub_topic_filters));
    struct aws_mqtt_subscription val;
    ASSERT_SUCCESS(aws_array_list_front(&received_packet->sub_topic_filters, &val));
    ASSERT_TRUE(aws_byte_cursor_eq(&val.topic_filter, &sub_topic_3));
    ASSERT_UINT_EQUALS(AWS_MQTT_QOS_AT_LEAST_ONCE, val.qos);
    ASSERT_SUCCESS(aws_array_list_back(&received_packet->sub_topic_filters, &val));
    ASSERT_TRUE(aws_byte_cursor_eq(&val.topic_filter, &sub_topic_2));
    ASSERT_UINT_EQUALS(AWS_MQTT_QOS_AT_LEAST_ONCE, val.qos);
    ASSERT_UINT_EQUALS(resub_packet_id, received_packet->packet_identifier);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    mqtt_connect_resubscribe,
    s_setup_mqtt_server_fn,
    s_test_mqtt_resubscribe_fn,
    s_clean_up_mqtt_server_fn,
    &test_data)

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

    aws_mutex_lock(&state_test_data->lock);
    state_test_data->expected_ops_completed = 2;
    aws_mutex_unlock(&state_test_data->lock);
    uint16_t packet_id_1 = aws_mqtt_client_connection_publish(
        state_test_data->mqtt_connection,
        &pub_topic,
        AWS_MQTT_QOS_AT_LEAST_ONCE,
        false,
        &payload_1,
        s_on_op_complete,
        state_test_data);
    ASSERT_TRUE(packet_id_1 > 0);
    uint16_t packet_id_2 = aws_mqtt_client_connection_publish(
        state_test_data->mqtt_connection,
        &pub_topic,
        AWS_MQTT_QOS_AT_LEAST_ONCE,
        false,
        &payload_2,
        s_on_op_complete,
        state_test_data);
    ASSERT_TRUE(packet_id_2 > 0);

    s_wait_for_ops_completed(state_test_data);

    ASSERT_SUCCESS(
        aws_mqtt_client_connection_disconnect(state_test_data->mqtt_connection, s_on_disconnect_fn, state_test_data));
    s_wait_for_disconnect_to_complete(state_test_data);

    /* Decode all received packets by mock server */
    ASSERT_SUCCESS(mqtt_mock_server_decode_packets(state_test_data->mock_server));

    ASSERT_UINT_EQUALS(4, mqtt_mock_server_decoded_packets_count(state_test_data->mock_server));
    struct mqtt_decoded_packet *received_packet =
        mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 0);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_CONNECT, received_packet->type);
    ASSERT_TRUE(aws_byte_cursor_eq(&received_packet->client_identifier, &connection_options.client_id));

    received_packet = mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 1);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_PUBLISH, received_packet->type);
    ASSERT_TRUE(aws_byte_cursor_eq(&received_packet->topic_name, &pub_topic));
    ASSERT_TRUE(aws_byte_cursor_eq(&received_packet->publish_payload, &payload_1));

    received_packet = mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 2);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_PUBLISH, received_packet->type);
    ASSERT_TRUE(aws_byte_cursor_eq(&received_packet->topic_name, &pub_topic));
    ASSERT_TRUE(aws_byte_cursor_eq(&received_packet->publish_payload, &payload_2));

    received_packet = mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 3);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_DISCONNECT, received_packet->type);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    mqtt_connect_publish,
    s_setup_mqtt_server_fn,
    s_test_mqtt_publish_fn,
    s_clean_up_mqtt_server_fn,
    &test_data)

/* Make a CONNECT, PUBLISH to a topic, free the payload before the publish completes to make sure it's safe */
static int s_test_mqtt_publish_payload_fn(struct aws_allocator *allocator, void *ctx) {
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
    struct aws_byte_buf buf_payload;
    struct aws_byte_cursor ori_payload = aws_byte_cursor_from_c_str("Test Message 1");
    ASSERT_SUCCESS(aws_byte_buf_init_copy_from_cursor(&buf_payload, allocator, ori_payload));
    struct aws_byte_cursor payload_curser = aws_byte_cursor_from_buf(&buf_payload);

    ASSERT_SUCCESS(aws_mqtt_client_connection_connect(state_test_data->mqtt_connection, &connection_options));
    s_wait_for_connection_to_complete(state_test_data);

    aws_mutex_lock(&state_test_data->lock);
    state_test_data->expected_ops_completed = 1;
    aws_mutex_unlock(&state_test_data->lock);
    uint16_t packet_id = aws_mqtt_client_connection_publish(
        state_test_data->mqtt_connection,
        &pub_topic,
        AWS_MQTT_QOS_AT_LEAST_ONCE,
        false,
        &payload_curser,
        s_on_op_complete,
        state_test_data);
    ASSERT_TRUE(packet_id > 0);

    /* clean up the payload buf, as user don't need to manage the buf and keep it valid until publish completes */
    aws_byte_buf_clean_up(&buf_payload);
    s_wait_for_ops_completed(state_test_data);

    ASSERT_SUCCESS(
        aws_mqtt_client_connection_disconnect(state_test_data->mqtt_connection, s_on_disconnect_fn, state_test_data));
    s_wait_for_disconnect_to_complete(state_test_data);

    /* Decode all received packets by mock server */
    ASSERT_SUCCESS(mqtt_mock_server_decode_packets(state_test_data->mock_server));

    ASSERT_UINT_EQUALS(3, mqtt_mock_server_decoded_packets_count(state_test_data->mock_server));
    struct mqtt_decoded_packet *received_packet =
        mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 0);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_CONNECT, received_packet->type);
    ASSERT_TRUE(aws_byte_cursor_eq(&received_packet->client_identifier, &connection_options.client_id));

    received_packet = mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 1);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_PUBLISH, received_packet->type);
    ASSERT_TRUE(aws_byte_cursor_eq(&received_packet->topic_name, &pub_topic));
    ASSERT_TRUE(aws_byte_cursor_eq(&received_packet->publish_payload, &ori_payload));

    received_packet = mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 2);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_DISCONNECT, received_packet->type);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    mqtt_connect_publish_payload,
    s_setup_mqtt_server_fn,
    s_test_mqtt_publish_payload_fn,
    s_clean_up_mqtt_server_fn,
    &test_data)

/**
 * CONNECT, force the server to hang up after a successful connection and block all CONNACKS, send PUBLISH messages
 * let the server send CONNACKS, make sure when the client reconnects automatically, it sends the PUBLISH messages
 * that were sent during offline mode. Then send a DISCONNECT.
 */
static int s_test_mqtt_connection_offline_publish_fn(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    struct mqtt_connection_state_test *state_test_data = ctx;

    struct aws_mqtt_connection_options connection_options = {
        .user_data = state_test_data,
        .clean_session = false,
        .client_id = aws_byte_cursor_from_c_str("client1234"),
        .host_name = aws_byte_cursor_from_c_str(state_test_data->endpoint.address),
        .socket_options = &state_test_data->socket_options,
        .on_connection_complete = s_on_connection_complete_fn,
        .ping_timeout_ms = 10,
        .keep_alive_time_secs = 1,
    };

    ASSERT_SUCCESS(aws_mqtt_client_connection_connect(state_test_data->mqtt_connection, &connection_options));
    s_wait_for_connection_to_complete(state_test_data);

    mqtt_mock_server_set_max_connack(state_test_data->mock_server, 0);

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
    mqtt_mock_server_set_max_connack(state_test_data->mock_server, SIZE_MAX);
    s_wait_for_ops_completed(state_test_data);
    aws_mutex_lock(&state_test_data->lock);
    ASSERT_TRUE(state_test_data->connection_resumed);
    aws_mutex_unlock(&state_test_data->lock);

    ASSERT_SUCCESS(
        aws_mqtt_client_connection_disconnect(state_test_data->mqtt_connection, s_on_disconnect_fn, state_test_data));
    s_wait_for_disconnect_to_complete(state_test_data);

    /* Decode all received packets by mock server */
    ASSERT_SUCCESS(mqtt_mock_server_decode_packets(state_test_data->mock_server));
    size_t packets_count = mqtt_mock_server_decoded_packets_count(state_test_data->mock_server);
    ASSERT_TRUE(packets_count >= 5 && packets_count <= 6);

    struct mqtt_decoded_packet *received_packet =
        mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 0);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_CONNECT, received_packet->type);
    ASSERT_UINT_EQUALS(connection_options.clean_session, received_packet->clean_session);
    ASSERT_TRUE(aws_byte_cursor_eq(&received_packet->client_identifier, &connection_options.client_id));

    received_packet = mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, 1);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_CONNECT, received_packet->type);
    ASSERT_UINT_EQUALS(connection_options.clean_session, received_packet->clean_session);
    ASSERT_TRUE(aws_byte_cursor_eq(&received_packet->client_identifier, &connection_options.client_id));

    /* if message count is 6 there was an extra connect message due to the automatic reconnect behavior and timing. */
    size_t index = 2;
    if (packets_count == 6) {
        received_packet = mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, index++);
        ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_CONNECT, received_packet->type);
        ASSERT_UINT_EQUALS(connection_options.clean_session, received_packet->clean_session);
        ASSERT_TRUE(aws_byte_cursor_eq(&received_packet->client_identifier, &connection_options.client_id));
    }

    received_packet = mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, index++);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_PUBLISH, received_packet->type);
    ASSERT_TRUE(aws_byte_cursor_eq(&received_packet->topic_name, &pub_topic));
    ASSERT_TRUE(aws_byte_cursor_eq(&received_packet->publish_payload, &payload_1));

    received_packet = mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, index++);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_PUBLISH, received_packet->type);
    ASSERT_TRUE(aws_byte_cursor_eq(&received_packet->topic_name, &pub_topic));
    ASSERT_TRUE(aws_byte_cursor_eq(&received_packet->publish_payload, &payload_2));

    received_packet = mqtt_mock_server_get_decoded_packet_by_index(state_test_data->mock_server, index++);
    ASSERT_UINT_EQUALS(AWS_MQTT_PACKET_DISCONNECT, received_packet->type);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    mqtt_connection_offline_publish,
    s_setup_mqtt_server_fn,
    s_test_mqtt_connection_offline_publish_fn,
    s_clean_up_mqtt_server_fn,
    &test_data)

/**
 * CONNECT, force the server to hang up after a successful connection and block all CONNACKS, DISCONNECT while client is
 * reconnecting. Resource and pending requests are cleaned up correctly
 */
static int s_test_mqtt_connection_disconnect_while_reconnecting(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    struct mqtt_connection_state_test *state_test_data = ctx;

    struct aws_mqtt_connection_options connection_options = {
        .user_data = state_test_data,
        .clean_session = false,
        .client_id = aws_byte_cursor_from_c_str("client1234"),
        .host_name = aws_byte_cursor_from_c_str(state_test_data->endpoint.address),
        .socket_options = &state_test_data->socket_options,
        .on_connection_complete = s_on_connection_complete_fn,
        .ping_timeout_ms = 10,
        .keep_alive_time_secs = 1,
    };

    ASSERT_SUCCESS(aws_mqtt_client_connection_connect(state_test_data->mqtt_connection, &connection_options));
    s_wait_for_connection_to_complete(state_test_data);

    mqtt_mock_server_set_max_connack(state_test_data->mock_server, 0);

    /* shut it down and the client automatically reconnects.*/
    aws_channel_shutdown(state_test_data->server_channel, AWS_OP_SUCCESS);
    s_wait_for_interrupt_to_complete(state_test_data);

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

    ASSERT_SUCCESS(
        aws_mqtt_client_connection_disconnect(state_test_data->mqtt_connection, s_on_disconnect_fn, state_test_data));
    s_wait_for_disconnect_to_complete(state_test_data);
    aws_mqtt_client_connection_release(state_test_data->mqtt_connection);
    state_test_data->mqtt_connection = NULL;
    s_wait_for_ops_completed(state_test_data);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    mqtt_connection_disconnect_while_reconnecting,
    s_setup_mqtt_server_fn,
    s_test_mqtt_connection_disconnect_while_reconnecting,
    s_clean_up_mqtt_server_fn,
    &test_data)

/**
 * Regression test: Once upon a time there was a bug caused by race condition on the state of connection.
 * The scenario is the server/broker closes the connection, while the client is making requests.
 * The race condition between the eventloop thread closes the connection and the main thread makes request could cause a
 * bug.
 * Solution: put a lock for the state of connection, protect it from accessing by multiple threads at the same time.
 */

static int s_test_mqtt_connection_closes_while_making_requests_fn(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    struct mqtt_connection_state_test *state_test_data = ctx;

    struct aws_mqtt_connection_options connection_options = {
        .user_data = state_test_data,
        .clean_session = false,
        .client_id = aws_byte_cursor_from_c_str("client1234"),
        .host_name = aws_byte_cursor_from_c_str(state_test_data->endpoint.address),
        .socket_options = &state_test_data->socket_options,
        .on_connection_complete = s_on_connection_complete_fn,
        .ping_timeout_ms = 10,
        .keep_alive_time_secs = 1,
    };

    struct aws_byte_cursor pub_topic = aws_byte_cursor_from_c_str("/test/topic");
    struct aws_byte_cursor payload_1 = aws_byte_cursor_from_c_str("Test Message 1");

    ASSERT_SUCCESS(aws_mqtt_client_connection_connect(state_test_data->mqtt_connection, &connection_options));
    s_wait_for_connection_to_complete(state_test_data);

    aws_mutex_lock(&state_test_data->lock);
    state_test_data->expected_ops_completed = 1;
    aws_mutex_unlock(&state_test_data->lock);

    /* shutdown the channel for some error */
    aws_channel_shutdown(state_test_data->server_channel, AWS_ERROR_INVALID_STATE);

    /* While the shutdown is still in process, making a publish request */
    /* It may not 100% trigger the crash, the crash only happens when the s_mqtt_client_shutdown and mqtt_create_request
     * happen at the same time. Like the slot is removed by shutdown, and the create request still think the slot is
     * there and try to access it. Crash happens. It's not possible to trigger the crash 100% without changing the
     * implementation. */
    uint16_t packet_id_1 = aws_mqtt_client_connection_publish(
        state_test_data->mqtt_connection,
        &pub_topic,
        AWS_MQTT_QOS_AT_LEAST_ONCE,
        false,
        &payload_1,
        s_on_op_complete,
        state_test_data);
    ASSERT_TRUE(packet_id_1 > 0);

    s_wait_for_reconnect_to_complete(state_test_data);
    s_wait_for_ops_completed(state_test_data);

    ASSERT_SUCCESS(
        aws_mqtt_client_connection_disconnect(state_test_data->mqtt_connection, s_on_disconnect_fn, state_test_data));
    s_wait_for_disconnect_to_complete(state_test_data);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    mqtt_connection_closes_while_making_requests,
    s_setup_mqtt_server_fn,
    s_test_mqtt_connection_closes_while_making_requests_fn,
    s_clean_up_mqtt_server_fn,
    &test_data)

/* helper to make sure ID 1 is received earlier than ID 2 and ID 2 is earlier than ID 3 */
static int s_check_packets_received_order(
    struct aws_channel_handler *handler,
    size_t search_start_idx,
    uint16_t packet_id_1,
    uint16_t packet_id_2,
    uint16_t packet_id_3) {
    ASSERT_SUCCESS(mqtt_mock_server_decode_packets(handler));
    size_t packet_idx_1 = 0;
    size_t packet_idx_2 = 0;
    size_t packet_idx_3 = 0;
    ASSERT_NOT_NULL(mqtt_mock_server_find_decoded_packet_by_id(handler, search_start_idx, packet_id_1, &packet_idx_1));
    ASSERT_NOT_NULL(mqtt_mock_server_find_decoded_packet_by_id(handler, search_start_idx, packet_id_2, &packet_idx_2));
    ASSERT_NOT_NULL(mqtt_mock_server_find_decoded_packet_by_id(handler, search_start_idx, packet_id_3, &packet_idx_3));
    ASSERT_TRUE(packet_idx_3 > packet_idx_2 && packet_idx_2 > packet_idx_1);
    return AWS_OP_SUCCESS;
}

/**
 * Test that when the response is not back from the server, and the connection lost, the sent packets will be retried
 * in the same order as they are sent before.
 */
static int s_test_mqtt_connection_resend_packets_fn(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    struct mqtt_connection_state_test *state_test_data = ctx;

    struct aws_mqtt_connection_options connection_options = {
        .user_data = state_test_data,
        .clean_session = false,
        .client_id = aws_byte_cursor_from_c_str("client1234"),
        .host_name = aws_byte_cursor_from_c_str(state_test_data->endpoint.address),
        .socket_options = &state_test_data->socket_options,
        .on_connection_complete = s_on_connection_complete_fn,
        .ping_timeout_ms = 10,
        .keep_alive_time_secs = 1,
    };

    struct aws_byte_cursor pub_topic = aws_byte_cursor_from_c_str("/test/topic");
    struct aws_byte_cursor payload_1 = aws_byte_cursor_from_c_str("Test Message 1");
    struct aws_byte_cursor payload_2 = aws_byte_cursor_from_c_str("Test Message 2");
    struct aws_byte_cursor payload_3 = aws_byte_cursor_from_c_str("Test Message 3");

    ASSERT_SUCCESS(aws_mqtt_client_connection_connect(state_test_data->mqtt_connection, &connection_options));
    s_wait_for_connection_to_complete(state_test_data);

    /* Disable the auto ACK packets sent by the server, which blocks the requests to complete */
    mqtt_mock_server_disable_auto_ack(state_test_data->mock_server);
    uint16_t packet_id_1 = aws_mqtt_client_connection_publish(
        state_test_data->mqtt_connection, &pub_topic, AWS_MQTT_QOS_AT_LEAST_ONCE, false, &payload_1, NULL, NULL);
    ASSERT_TRUE(packet_id_1 > 0);
    uint16_t packet_id_2 = aws_mqtt_client_connection_publish(
        state_test_data->mqtt_connection, &pub_topic, AWS_MQTT_QOS_AT_LEAST_ONCE, false, &payload_2, NULL, NULL);
    ASSERT_TRUE(packet_id_2 > 0);
    uint16_t packet_id_3 = aws_mqtt_client_connection_publish(
        state_test_data->mqtt_connection, &pub_topic, AWS_MQTT_QOS_AT_LEAST_ONCE, false, &payload_3, NULL, NULL);
    ASSERT_TRUE(packet_id_3 > 0);
    /* Wait for 1 sec. ensure all the publishes have been received by the server */
    aws_thread_current_sleep(ONE_SEC);
    ASSERT_SUCCESS(
        s_check_packets_received_order(state_test_data->mock_server, 0, packet_id_1, packet_id_2, packet_id_3));
    size_t packet_count = mqtt_mock_server_decoded_packets_count(state_test_data->mock_server);

    /* shutdown the channel for some error */
    aws_channel_shutdown(state_test_data->server_channel, AWS_ERROR_INVALID_STATE);
    s_wait_for_reconnect_to_complete(state_test_data);
    /* Wait again, and ensure the publishes have been resent */
    aws_thread_current_sleep(ONE_SEC);
    ASSERT_SUCCESS(s_check_packets_received_order(
        state_test_data->mock_server, packet_count, packet_id_1, packet_id_2, packet_id_3));

    ASSERT_SUCCESS(
        aws_mqtt_client_connection_disconnect(state_test_data->mqtt_connection, s_on_disconnect_fn, state_test_data));
    s_wait_for_disconnect_to_complete(state_test_data);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    mqtt_connection_resend_packets,
    s_setup_mqtt_server_fn,
    s_test_mqtt_connection_resend_packets_fn,
    s_clean_up_mqtt_server_fn,
    &test_data)

/**
 * Test that connection lost before the publish QoS 0 ever sent, publish QoS 0 will not be retried.
 */
static int s_test_mqtt_connection_not_retry_publish_QoS_0_fn(struct aws_allocator *allocator, void *ctx) {
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
        .keep_alive_time_secs = 16960, /* basically stop automatically sending PINGREQ */
    };

    struct aws_byte_cursor pub_topic = aws_byte_cursor_from_c_str("/test/topic");
    struct aws_byte_cursor payload_1 = aws_byte_cursor_from_c_str("Test Message 1");

    ASSERT_SUCCESS(aws_mqtt_client_connection_connect(state_test_data->mqtt_connection, &connection_options));
    s_wait_for_connection_to_complete(state_test_data);

    /* kill the connection */
    aws_channel_shutdown(state_test_data->server_channel, AWS_ERROR_INVALID_STATE);
    /* TODO: only one eventloop thread in the el group. Most likely the test will fail, the outgoing task will not be
     * cancelled because of connection lost */
    /* make a publish with QoS 0 immediate. */
    aws_mutex_lock(&state_test_data->lock);
    state_test_data->expected_ops_completed = 1;
    aws_mutex_unlock(&state_test_data->lock);
    uint16_t packet_id_1 = aws_mqtt_client_connection_publish(
        state_test_data->mqtt_connection,
        &pub_topic,
        AWS_MQTT_QOS_AT_MOST_ONCE,
        false,
        &payload_1,
        s_on_op_complete,
        state_test_data);
    ASSERT_TRUE(packet_id_1 > 0);

    /* publish should complete after the shutdown */
    s_wait_for_ops_completed(state_test_data);
    /* wait for reconnect */
    s_wait_for_reconnect_to_complete(state_test_data);

    /* Check all received packets, no publish packets ever received by the server. Because the connection lost before it
     * ever get sent. */
    ASSERT_SUCCESS(mqtt_mock_server_decode_packets(state_test_data->mock_server));
    ASSERT_NULL(
        mqtt_mock_server_find_decoded_packet_by_type(state_test_data->mock_server, 0, AWS_MQTT_PACKET_PUBLISH, NULL));
    ASSERT_SUCCESS(
        aws_mqtt_client_connection_disconnect(state_test_data->mqtt_connection, s_on_disconnect_fn, state_test_data));
    s_wait_for_disconnect_to_complete(state_test_data);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    mqtt_connection_not_retry_publish_QoS_0,
    s_setup_mqtt_server_fn,
    s_test_mqtt_connection_not_retry_publish_QoS_0_fn,
    s_clean_up_mqtt_server_fn,
    &test_data)

/**
 * Test that the retry policy is consistent, which means either the request has been sent or not, when the connection
 * lost/resume, the request will be retried.
 */
static int s_test_mqtt_connection_consistent_retry_policy_fn(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    struct mqtt_connection_state_test *state_test_data = ctx;

    struct aws_mqtt_connection_options connection_options = {
        .user_data = state_test_data,
        .clean_session = false,
        .client_id = aws_byte_cursor_from_c_str("client1234"),
        .host_name = aws_byte_cursor_from_c_str(state_test_data->endpoint.address),
        .socket_options = &state_test_data->socket_options,
        .on_connection_complete = s_on_connection_complete_fn,
        .ping_timeout_ms = 10,
        .protocol_operation_timeout_ms = 3000,
        .keep_alive_time_secs = 16960, /* basically stop automatically sending PINGREQ */
    };

    struct aws_byte_cursor pub_topic = aws_byte_cursor_from_c_str("/test/topic");
    struct aws_byte_cursor payload_1 = aws_byte_cursor_from_c_str("Test Message 1");
    struct aws_byte_cursor sub_topic = aws_byte_cursor_from_c_str("/test/topic");

    ASSERT_SUCCESS(aws_mqtt_client_connection_connect(state_test_data->mqtt_connection, &connection_options));
    s_wait_for_connection_to_complete(state_test_data);
    struct aws_channel_handler *handler = state_test_data->mock_server;

    /* Disable the auto ACK packets sent by the server, which blocks the requests to complete */
    mqtt_mock_server_disable_auto_ack(handler);

    /* kill the connection */
    aws_channel_shutdown(state_test_data->server_channel, AWS_ERROR_INVALID_STATE);
    /* There is a hidden race condition between channel shutdown from eventloop and publish/subscribe from main thread,
     * but either way, it should work as they are retried in the end.  */
    /* make a publish with QoS 1 immediate. */
    uint16_t packet_id_1 = aws_mqtt_client_connection_publish(
        state_test_data->mqtt_connection,
        &pub_topic,
        AWS_MQTT_QOS_AT_LEAST_ONCE,
        false,
        &payload_1,
        s_on_op_complete,
        state_test_data);
    ASSERT_TRUE(packet_id_1 > 0);
    /* make another subscribe */
    uint16_t packet_id_2 = aws_mqtt_client_connection_subscribe(
        state_test_data->mqtt_connection,
        &sub_topic,
        AWS_MQTT_QOS_AT_LEAST_ONCE,
        NULL,
        NULL,
        NULL,
        s_on_suback,
        state_test_data);
    ASSERT_TRUE(packet_id_2 > 0);

    /* wait for reconnect */
    s_wait_for_reconnect_to_complete(state_test_data);
    /* Wait for 1 sec. ensure all the requests have been received by the server */
    aws_thread_current_sleep(ONE_SEC);

    /* Check all received packets, subscribe and publish has been received */
    ASSERT_SUCCESS(mqtt_mock_server_decode_packets(handler));
    size_t packet_count = mqtt_mock_server_decoded_packets_count(handler);
    /* the latest packet should be subscribe */
    ASSERT_NOT_NULL(mqtt_mock_server_find_decoded_packet_by_type(handler, 0, AWS_MQTT_PACKET_SUBSCRIBE, NULL));
    ASSERT_NOT_NULL(mqtt_mock_server_find_decoded_packet_by_type(handler, 0, AWS_MQTT_PACKET_PUBLISH, NULL));

    /* Re-enable the auto ack to finish the requests */
    mqtt_mock_server_enable_auto_ack(handler);
    /* Kill the connection again, the requests will be retried since the response has not been received yet. */
    aws_channel_shutdown(state_test_data->server_channel, AWS_ERROR_INVALID_STATE);
    s_wait_for_reconnect_to_complete(state_test_data);
    /* subscribe should be able to completed now */
    s_wait_for_subscribe_to_complete(state_test_data);

    /* Check all received packets, subscribe and publish has been resent */
    ASSERT_SUCCESS(mqtt_mock_server_decode_packets(handler));
    ASSERT_TRUE(mqtt_mock_server_decoded_packets_count(handler) > packet_count);
    /* the latest packet should be subscribe */
    ASSERT_NOT_NULL(
        mqtt_mock_server_find_decoded_packet_by_type(handler, packet_count, AWS_MQTT_PACKET_SUBSCRIBE, NULL));
    ASSERT_NOT_NULL(mqtt_mock_server_find_decoded_packet_by_type(handler, packet_count, AWS_MQTT_PACKET_PUBLISH, NULL));

    ASSERT_SUCCESS(
        aws_mqtt_client_connection_disconnect(state_test_data->mqtt_connection, s_on_disconnect_fn, state_test_data));
    s_wait_for_disconnect_to_complete(state_test_data);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    mqtt_connection_consistent_retry_policy,
    s_setup_mqtt_server_fn,
    s_test_mqtt_connection_consistent_retry_policy_fn,
    s_clean_up_mqtt_server_fn,
    &test_data)

/**
 * Test that the request will not be retried even the response has not received for a while.
 */
static int s_test_mqtt_connection_not_resend_packets_on_healthy_connection_fn(
    struct aws_allocator *allocator,
    void *ctx) {
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

    struct aws_byte_cursor pub_topic = aws_byte_cursor_from_c_str("/test/topic");
    struct aws_byte_cursor payload_1 = aws_byte_cursor_from_c_str("Test Message 1");
    struct aws_byte_cursor sub_topic = aws_byte_cursor_from_c_str("/test/topic");

    ASSERT_SUCCESS(aws_mqtt_client_connection_connect(state_test_data->mqtt_connection, &connection_options));
    s_wait_for_connection_to_complete(state_test_data);
    struct aws_channel_handler *handler = state_test_data->mock_server;

    /* Disable the auto ACK packets sent by the server, which blocks the requests to complete */
    mqtt_mock_server_disable_auto_ack(handler);

    /* make a publish with QoS 1 */
    aws_mutex_lock(&state_test_data->lock);
    state_test_data->expected_ops_completed = 1;
    aws_mutex_unlock(&state_test_data->lock);
    uint16_t packet_id_1 = aws_mqtt_client_connection_publish(
        state_test_data->mqtt_connection,
        &pub_topic,
        AWS_MQTT_QOS_AT_LEAST_ONCE,
        false,
        &payload_1,
        s_on_op_complete,
        state_test_data);
    ASSERT_TRUE(packet_id_1 > 0);
    /* make another subscribe */
    uint16_t packet_id_2 = aws_mqtt_client_connection_subscribe(
        state_test_data->mqtt_connection,
        &sub_topic,
        AWS_MQTT_QOS_AT_LEAST_ONCE,
        NULL,
        NULL,
        NULL,
        s_on_suback,
        state_test_data);
    ASSERT_TRUE(packet_id_2 > 0);

    /* Wait for 3 sec. ensure no duplicate requests will be sent */
    aws_thread_current_sleep((uint64_t)ONE_SEC * 3);
    /* Check all received packets, only one publish and subscribe received */
    ASSERT_SUCCESS(mqtt_mock_server_decode_packets(state_test_data->mock_server));
    size_t pre_index = SIZE_MAX;
    ASSERT_NOT_NULL(mqtt_mock_server_find_decoded_packet_by_type(handler, 0, AWS_MQTT_PACKET_PUBLISH, &pre_index));
    if (pre_index + 1 < mqtt_mock_server_decoded_packets_count(handler)) {
        /* If it's not the last packet, search again, and result should be NULL. */
        ASSERT_NULL(
            mqtt_mock_server_find_decoded_packet_by_type(handler, pre_index + 1, AWS_MQTT_PACKET_PUBLISH, NULL));
    }
    ASSERT_NOT_NULL(mqtt_mock_server_find_decoded_packet_by_type(handler, 0, AWS_MQTT_PACKET_SUBSCRIBE, &pre_index));
    if (pre_index + 1 < mqtt_mock_server_decoded_packets_count(handler)) {
        ASSERT_NULL(
            mqtt_mock_server_find_decoded_packet_by_type(handler, pre_index + 1, AWS_MQTT_PACKET_SUBSCRIBE, NULL));
    }

    ASSERT_SUCCESS(
        aws_mqtt_client_connection_disconnect(state_test_data->mqtt_connection, s_on_disconnect_fn, state_test_data));
    s_wait_for_disconnect_to_complete(state_test_data);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    mqtt_connection_not_resend_packets_on_healthy_connection,
    s_setup_mqtt_server_fn,
    s_test_mqtt_connection_not_resend_packets_on_healthy_connection_fn,
    s_clean_up_mqtt_server_fn,
    &test_data)

/* Make requests during offline, and destory the connection before ever online, the resource should be cleaned up
 * properly */
static int s_test_mqtt_connection_destory_pending_requests_fn(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    struct mqtt_connection_state_test *state_test_data = ctx;

    struct aws_byte_cursor topic = aws_byte_cursor_from_c_str("/test/topic");
    struct aws_byte_cursor payload = aws_byte_cursor_from_c_str("Test Message 1");

    ASSERT_TRUE(
        aws_mqtt_client_connection_publish(
            state_test_data->mqtt_connection,
            &topic,
            AWS_MQTT_QOS_AT_LEAST_ONCE,
            false,
            &payload,
            s_on_op_complete,
            state_test_data) > 0);
    ASSERT_TRUE(
        aws_mqtt_client_connection_subscribe(
            state_test_data->mqtt_connection,
            &topic,
            AWS_MQTT_QOS_AT_LEAST_ONCE,
            s_on_publish_received,
            state_test_data,
            NULL,
            s_on_suback,
            state_test_data) > 0);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    mqtt_connection_destory_pending_requests,
    s_setup_mqtt_server_fn,
    s_test_mqtt_connection_destory_pending_requests_fn,
    s_clean_up_mqtt_server_fn,
    &test_data)

/* Make a clean session connection, all the request will not be retried when the connection lost */
static int s_test_mqtt_clean_session_not_retry_fn(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    struct mqtt_connection_state_test *state_test_data = ctx;

    struct aws_mqtt_connection_options connection_options = {
        .user_data = state_test_data,
        .clean_session = true, /* make a clean_session connection */
        .client_id = aws_byte_cursor_from_c_str("client1234"),
        .host_name = aws_byte_cursor_from_c_str(state_test_data->endpoint.address),
        .socket_options = &state_test_data->socket_options,
        .on_connection_complete = s_on_connection_complete_fn,
        .ping_timeout_ms = 10,
        .keep_alive_time_secs = 1,
    };

    ASSERT_SUCCESS(aws_mqtt_client_connection_connect(state_test_data->mqtt_connection, &connection_options));
    s_wait_for_connection_to_complete(state_test_data);
    struct aws_channel_handler *handler = state_test_data->mock_server;
    mqtt_mock_server_disable_auto_ack(handler);

    struct aws_byte_cursor topic = aws_byte_cursor_from_c_str("/test/topic");
    struct aws_byte_cursor payload = aws_byte_cursor_from_c_str("Test Message 1");

    ASSERT_TRUE(
        aws_mqtt_client_connection_publish(
            state_test_data->mqtt_connection,
            &topic,
            AWS_MQTT_QOS_AT_LEAST_ONCE,
            false,
            &payload,
            s_on_op_complete,
            state_test_data) > 0);
    ASSERT_TRUE(
        aws_mqtt_client_connection_subscribe(
            state_test_data->mqtt_connection,
            &topic,
            AWS_MQTT_QOS_AT_LEAST_ONCE,
            s_on_publish_received,
            state_test_data,
            NULL,
            s_on_suback,
            state_test_data) > 0);
    aws_mutex_lock(&state_test_data->lock);
    state_test_data->expected_ops_completed = 1;
    aws_mutex_unlock(&state_test_data->lock);
    /* Shutdown the connection */
    aws_channel_shutdown(state_test_data->server_channel, AWS_OP_SUCCESS);
    s_wait_for_interrupt_to_complete(state_test_data);

    /* Once the connection lost, the requests will fail */
    ASSERT_UINT_EQUALS(state_test_data->op_complete_error, AWS_ERROR_MQTT_CANCELLED_FOR_CLEAN_SESSION);
    ASSERT_UINT_EQUALS(state_test_data->subscribe_complete_error, AWS_ERROR_MQTT_CANCELLED_FOR_CLEAN_SESSION);

    /* Disconnect */
    ASSERT_SUCCESS(
        aws_mqtt_client_connection_disconnect(state_test_data->mqtt_connection, s_on_disconnect_fn, state_test_data));
    s_wait_for_disconnect_to_complete(state_test_data);
    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    mqtt_clean_session_not_retry,
    s_setup_mqtt_server_fn,
    s_test_mqtt_clean_session_not_retry_fn,
    s_clean_up_mqtt_server_fn,
    &test_data)

/* Make a clean session connection, the previous session will be discard when a new connection with clean session true
 * is created */
static int s_test_mqtt_clean_session_discard_previous_fn(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    struct mqtt_connection_state_test *state_test_data = ctx;

    struct aws_mqtt_connection_options connection_options = {
        .user_data = state_test_data,
        .clean_session = true, /* make a clean_session connection */
        .client_id = aws_byte_cursor_from_c_str("client1234"),
        .host_name = aws_byte_cursor_from_c_str(state_test_data->endpoint.address),
        .socket_options = &state_test_data->socket_options,
        .on_connection_complete = s_on_connection_complete_fn,
        .ping_timeout_ms = 10,
        .keep_alive_time_secs = 16960, /* basically stop automatically sending PINGREQ */
    };

    struct aws_byte_cursor topic = aws_byte_cursor_from_c_str("/test/topic");
    struct aws_byte_cursor payload = aws_byte_cursor_from_c_str("Test Message 1");
    aws_mutex_lock(&state_test_data->lock);
    state_test_data->expected_ops_completed = 1;
    aws_mutex_unlock(&state_test_data->lock);
    /* Requests made now will be considered as the previous session. */
    ASSERT_TRUE(
        aws_mqtt_client_connection_publish(
            state_test_data->mqtt_connection,
            &topic,
            AWS_MQTT_QOS_AT_LEAST_ONCE,
            false,
            &payload,
            s_on_op_complete,
            state_test_data) > 0);
    ASSERT_TRUE(
        aws_mqtt_client_connection_subscribe(
            state_test_data->mqtt_connection,
            &topic,
            AWS_MQTT_QOS_AT_LEAST_ONCE,
            s_on_publish_received,
            state_test_data,
            NULL,
            s_on_suback,
            state_test_data) > 0);

    ASSERT_SUCCESS(aws_mqtt_client_connection_connect(state_test_data->mqtt_connection, &connection_options));
    s_wait_for_connection_to_complete(state_test_data);
    struct aws_channel_handler *handler = state_test_data->mock_server;

    s_wait_for_ops_completed(state_test_data);
    s_wait_for_subscribe_to_complete(state_test_data);
    ASSERT_UINT_EQUALS(state_test_data->op_complete_error, AWS_ERROR_MQTT_CANCELLED_FOR_CLEAN_SESSION);
    ASSERT_UINT_EQUALS(state_test_data->subscribe_complete_error, AWS_ERROR_MQTT_CANCELLED_FOR_CLEAN_SESSION);

    /* Check no request is received by the server */
    ASSERT_SUCCESS(mqtt_mock_server_decode_packets(handler));
    ASSERT_NULL(mqtt_mock_server_find_decoded_packet_by_type(handler, 0, AWS_MQTT_PACKET_PUBLISH, NULL));
    ASSERT_NULL(mqtt_mock_server_find_decoded_packet_by_type(handler, 0, AWS_MQTT_PACKET_SUBSCRIBE, NULL));

    /* Disconnect */
    ASSERT_SUCCESS(
        aws_mqtt_client_connection_disconnect(state_test_data->mqtt_connection, s_on_disconnect_fn, state_test_data));
    s_wait_for_disconnect_to_complete(state_test_data);
    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    mqtt_clean_session_discard_previous,
    s_setup_mqtt_server_fn,
    s_test_mqtt_clean_session_discard_previous_fn,
    s_clean_up_mqtt_server_fn,
    &test_data)

/* Make a clean session connection, the requests after the connect function will be considered as the next session */
static int s_test_mqtt_clean_session_keep_next_session_fn(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    struct mqtt_connection_state_test *state_test_data = ctx;

    struct aws_mqtt_connection_options connection_options = {
        .user_data = state_test_data,
        .clean_session = true, /* make a clean_session connection */
        .client_id = aws_byte_cursor_from_c_str("client1234"),
        .host_name = aws_byte_cursor_from_c_str(state_test_data->endpoint.address),
        .socket_options = &state_test_data->socket_options,
        .on_connection_complete = s_on_connection_complete_fn,
        .ping_timeout_ms = 10,
        .keep_alive_time_secs = 16960, /* basically stop automatically sending PINGREQ */
    };

    struct aws_byte_cursor topic = aws_byte_cursor_from_c_str("/test/topic");
    struct aws_byte_cursor payload = aws_byte_cursor_from_c_str("Test Message 1");
    aws_mutex_lock(&state_test_data->lock);
    state_test_data->expected_ops_completed = 1;
    aws_mutex_unlock(&state_test_data->lock);

    ASSERT_SUCCESS(aws_mqtt_client_connection_connect(state_test_data->mqtt_connection, &connection_options));
    /* Requests made after the connect function will be considered as the next session, and will be sent eventually */
    ASSERT_TRUE(
        aws_mqtt_client_connection_publish(
            state_test_data->mqtt_connection,
            &topic,
            AWS_MQTT_QOS_AT_LEAST_ONCE,
            false,
            &payload,
            s_on_op_complete,
            state_test_data) > 0);
    ASSERT_TRUE(
        aws_mqtt_client_connection_subscribe(
            state_test_data->mqtt_connection,
            &topic,
            AWS_MQTT_QOS_AT_LEAST_ONCE,
            s_on_publish_received,
            state_test_data,
            NULL,
            s_on_suback,
            state_test_data) > 0);
    s_wait_for_connection_to_complete(state_test_data);
    struct aws_channel_handler *handler = state_test_data->mock_server;

    s_wait_for_ops_completed(state_test_data);
    s_wait_for_subscribe_to_complete(state_test_data);
    ASSERT_UINT_EQUALS(state_test_data->op_complete_error, AWS_ERROR_SUCCESS);
    ASSERT_UINT_EQUALS(state_test_data->subscribe_complete_error, AWS_ERROR_SUCCESS);

    /* Check no request is received by the server */
    ASSERT_SUCCESS(mqtt_mock_server_decode_packets(handler));
    ASSERT_NOT_NULL(mqtt_mock_server_find_decoded_packet_by_type(handler, 0, AWS_MQTT_PACKET_PUBLISH, NULL));
    ASSERT_NOT_NULL(mqtt_mock_server_find_decoded_packet_by_type(handler, 0, AWS_MQTT_PACKET_SUBSCRIBE, NULL));

    /* Disconnect */
    ASSERT_SUCCESS(
        aws_mqtt_client_connection_disconnect(state_test_data->mqtt_connection, s_on_disconnect_fn, state_test_data));
    s_wait_for_disconnect_to_complete(state_test_data);
    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    mqtt_clean_session_keep_next_session,
    s_setup_mqtt_server_fn,
    s_test_mqtt_clean_session_keep_next_session_fn,
    s_clean_up_mqtt_server_fn,
    &test_data)

/**
 * Test that connection is healthy, user set the timeout for request, and timeout happens and the publish failed.
 */
static int s_test_mqtt_connection_publish_QoS1_timeout_fn(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    struct mqtt_connection_state_test *state_test_data = ctx;

    struct aws_mqtt_connection_options connection_options = {
        .user_data = state_test_data,
        .clean_session = false,
        .client_id = aws_byte_cursor_from_c_str("client1234"),
        .host_name = aws_byte_cursor_from_c_str(state_test_data->endpoint.address),
        .socket_options = &state_test_data->socket_options,
        .on_connection_complete = s_on_connection_complete_fn,
        .ping_timeout_ms = 10,
        .protocol_operation_timeout_ms = 3000,
        .keep_alive_time_secs = 16960, /* basically stop automatically sending PINGREQ */
    };

    struct aws_byte_cursor pub_topic = aws_byte_cursor_from_c_str("/test/topic");
    struct aws_byte_cursor payload_1 = aws_byte_cursor_from_c_str("Test Message 1");

    ASSERT_SUCCESS(aws_mqtt_client_connection_connect(state_test_data->mqtt_connection, &connection_options));
    s_wait_for_connection_to_complete(state_test_data);

    /* Disable the auto ACK packets sent by the server, which blocks the requests to complete */
    mqtt_mock_server_disable_auto_ack(state_test_data->mock_server);

    /* make a publish with QoS 1 immediate. */
    aws_mutex_lock(&state_test_data->lock);
    state_test_data->expected_ops_completed = 1;
    aws_mutex_unlock(&state_test_data->lock);
    uint16_t packet_id_1 = aws_mqtt_client_connection_publish(
        state_test_data->mqtt_connection,
        &pub_topic,
        AWS_MQTT_QOS_AT_LEAST_ONCE,
        false,
        &payload_1,
        s_on_op_complete,
        state_test_data);
    ASSERT_TRUE(packet_id_1 > 0);

    /* publish should complete after the shutdown */
    s_wait_for_ops_completed(state_test_data);
    /* Check the publish has been completed with timeout error */
    ASSERT_UINT_EQUALS(state_test_data->op_complete_error, AWS_ERROR_MQTT_TIMEOUT);
    ASSERT_SUCCESS(
        aws_mqtt_client_connection_disconnect(state_test_data->mqtt_connection, s_on_disconnect_fn, state_test_data));
    s_wait_for_disconnect_to_complete(state_test_data);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    mqtt_connection_publish_QoS1_timeout,
    s_setup_mqtt_server_fn,
    s_test_mqtt_connection_publish_QoS1_timeout_fn,
    s_clean_up_mqtt_server_fn,
    &test_data)

/**
 * Test that connection is healthy, user set the timeout for request, and timeout happens and the unsubscribe failed.
 */
static int s_test_mqtt_connection_unsub_timeout_fn(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    struct mqtt_connection_state_test *state_test_data = ctx;

    struct aws_mqtt_connection_options connection_options = {
        .user_data = state_test_data,
        .clean_session = false,
        .client_id = aws_byte_cursor_from_c_str("client1234"),
        .host_name = aws_byte_cursor_from_c_str(state_test_data->endpoint.address),
        .socket_options = &state_test_data->socket_options,
        .on_connection_complete = s_on_connection_complete_fn,
        .ping_timeout_ms = 10,
        .protocol_operation_timeout_ms = 3000,
        .keep_alive_time_secs = 16960, /* basically stop automatically sending PINGREQ */
    };

    struct aws_byte_cursor pub_topic = aws_byte_cursor_from_c_str("/test/topic");

    ASSERT_SUCCESS(aws_mqtt_client_connection_connect(state_test_data->mqtt_connection, &connection_options));
    s_wait_for_connection_to_complete(state_test_data);

    /* Disable the auto ACK packets sent by the server, which blocks the requests to complete */
    mqtt_mock_server_disable_auto_ack(state_test_data->mock_server);

    aws_mutex_lock(&state_test_data->lock);
    state_test_data->expected_ops_completed = 1;
    aws_mutex_unlock(&state_test_data->lock);

    /* unsubscribe to the first topic */
    uint16_t unsub_packet_id = aws_mqtt_client_connection_unsubscribe(
        state_test_data->mqtt_connection, &pub_topic, s_on_op_complete, state_test_data);
    ASSERT_TRUE(unsub_packet_id > 0);

    /* publish should complete after the shutdown */
    s_wait_for_ops_completed(state_test_data);
    /* Check the publish has been completed with timeout error */
    ASSERT_UINT_EQUALS(state_test_data->op_complete_error, AWS_ERROR_MQTT_TIMEOUT);
    ASSERT_SUCCESS(
        aws_mqtt_client_connection_disconnect(state_test_data->mqtt_connection, s_on_disconnect_fn, state_test_data));
    s_wait_for_disconnect_to_complete(state_test_data);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    mqtt_connection_unsub_timeout,
    s_setup_mqtt_server_fn,
    s_test_mqtt_connection_unsub_timeout_fn,
    s_clean_up_mqtt_server_fn,
    &test_data)

/**
 * Test that connection is healthy, user set the timeout for request, and connection lost will reset timeout.
 */
static int s_test_mqtt_connection_publish_QoS1_timeout_connection_lost_reset_time_fn(
    struct aws_allocator *allocator,
    void *ctx) {
    (void)allocator;
    struct mqtt_connection_state_test *state_test_data = ctx;

    struct aws_mqtt_connection_options connection_options = {
        .user_data = state_test_data,
        .clean_session = false,
        .client_id = aws_byte_cursor_from_c_str("client1234"),
        .host_name = aws_byte_cursor_from_c_str(state_test_data->endpoint.address),
        .socket_options = &state_test_data->socket_options,
        .on_connection_complete = s_on_connection_complete_fn,
        .ping_timeout_ms = 10,
        .protocol_operation_timeout_ms = 3000,
        .keep_alive_time_secs = 16960, /* basically stop automatically sending PINGREQ */
    };

    struct aws_byte_cursor pub_topic = aws_byte_cursor_from_c_str("/test/topic");
    struct aws_byte_cursor payload_1 = aws_byte_cursor_from_c_str("Test Message 1");

    ASSERT_SUCCESS(aws_mqtt_client_connection_connect(state_test_data->mqtt_connection, &connection_options));
    s_wait_for_connection_to_complete(state_test_data);

    /* Disable the auto ACK packets sent by the server, which blocks the requests to complete */
    mqtt_mock_server_disable_auto_ack(state_test_data->mock_server);

    /* make a publish with QoS 1 immediate. */
    aws_mutex_lock(&state_test_data->lock);
    state_test_data->expected_ops_completed = 1;
    aws_mutex_unlock(&state_test_data->lock);
    uint16_t packet_id_1 = aws_mqtt_client_connection_publish(
        state_test_data->mqtt_connection,
        &pub_topic,
        AWS_MQTT_QOS_AT_LEAST_ONCE,
        false,
        &payload_1,
        s_on_op_complete,
        state_test_data);
    ASSERT_TRUE(packet_id_1 > 0);

    /* sleep for 2 sec, close the connection */
    aws_thread_current_sleep((uint64_t)ONE_SEC * 2);
    /* Kill the connection, the requests will be retried and the timeout will be reset. */
    aws_channel_shutdown(state_test_data->server_channel, AWS_ERROR_INVALID_STATE);
    s_wait_for_reconnect_to_complete(state_test_data);
    /* sleep for 2 sec again, in total the response has not received for more than 4 sec, timeout should happen if the
     * lost of connection not reset the timeout */
    aws_thread_current_sleep((uint64_t)ONE_SEC * 2);
    /* send a puback */
    ASSERT_SUCCESS(mqtt_mock_server_send_puback(state_test_data->mock_server, packet_id_1));

    /* publish should complete after the shutdown */
    s_wait_for_ops_completed(state_test_data);
    /* Check the publish has been completed successfully since the lost of the connection reset the timeout */
    ASSERT_UINT_EQUALS(state_test_data->op_complete_error, AWS_ERROR_SUCCESS);
    ASSERT_SUCCESS(
        aws_mqtt_client_connection_disconnect(state_test_data->mqtt_connection, s_on_disconnect_fn, state_test_data));
    s_wait_for_disconnect_to_complete(state_test_data);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    mqtt_connection_publish_QoS1_timeout_connection_lost_reset_time,
    s_setup_mqtt_server_fn,
    s_test_mqtt_connection_publish_QoS1_timeout_connection_lost_reset_time_fn,
    s_clean_up_mqtt_server_fn,
    &test_data)
