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

/* ========== HELPER FUNCTIONS FOR THE TEST ========== */

static void s_operation_statistics_on_any_publish_received(
    struct aws_mqtt_client_connection *connection,
    const struct aws_byte_cursor *topic,
    const struct aws_byte_cursor *payload,
    bool dup,
    enum aws_mqtt_qos qos,
    bool retain,
    void *userdata);

static void s_operation_statistics_on_incoming_channel_setup_fn(
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

static void s_operation_statistics_on_incoming_channel_shutdown_fn(
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

static void s_operation_statistics_on_listener_destroy(struct aws_server_bootstrap *bootstrap, void *user_data) {
    (void)bootstrap;
    struct mqtt_connection_state_test *state_test_data = user_data;
    aws_mutex_lock(&state_test_data->lock);
    state_test_data->listener_destroyed = true;
    aws_mutex_unlock(&state_test_data->lock);
    aws_condition_variable_notify_one(&state_test_data->cvar);
}

static bool s_operation_statistics_is_listener_destroyed(void *arg) {
    struct mqtt_connection_state_test *state_test_data = arg;
    return state_test_data->listener_destroyed;
}

static void s_operation_statistics_wait_on_listener_cleanup(struct mqtt_connection_state_test *state_test_data) {
    aws_mutex_lock(&state_test_data->lock);
    aws_condition_variable_wait_pred(
        &state_test_data->cvar, &state_test_data->lock, s_operation_statistics_is_listener_destroyed, state_test_data);
    aws_mutex_unlock(&state_test_data->lock);
}

static void s_operation_statistics_on_connection_interrupted(
    struct aws_mqtt_client_connection *connection,
    int error_code,
    void *userdata) {
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

static void s_operation_statistics_on_connection_resumed(
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

/** sets up a unix domain socket server and socket options. Creates an mqtt connection configured to use
 * the domain socket.
 */
static int s_operation_statistics_setup_mqtt_server_fn(struct aws_allocator *allocator, void *ctx) {
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
        .incoming_callback = s_operation_statistics_on_incoming_channel_setup_fn,
        .shutdown_callback = s_operation_statistics_on_incoming_channel_shutdown_fn,
        .destroy_callback = s_operation_statistics_on_listener_destroy,
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
        s_operation_statistics_on_connection_interrupted,
        state_test_data,
        s_operation_statistics_on_connection_resumed,
        state_test_data));

    ASSERT_SUCCESS(aws_mqtt_client_connection_set_on_any_publish_handler(
        state_test_data->mqtt_connection, s_operation_statistics_on_any_publish_received, state_test_data));

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

static int s_operation_statistics_clean_up_mqtt_server_fn(
    struct aws_allocator *allocator,
    int setup_result,
    void *ctx) {
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
        s_operation_statistics_wait_on_listener_cleanup(state_test_data);
        aws_server_bootstrap_release(state_test_data->server_bootstrap);
        aws_event_loop_group_release(state_test_data->el_group);
        destroy_mqtt_mock_server(state_test_data->mock_server);
    }

    aws_mqtt_library_clean_up();
    return AWS_OP_SUCCESS;
}

static void s_operation_statistics_on_connection_complete_fn(
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

static bool s_operation_statistics_is_connection_completed(void *arg) {
    struct mqtt_connection_state_test *state_test_data = arg;
    return state_test_data->connection_completed;
}

static void s_operation_statistics_wait_for_connection_to_complete(struct mqtt_connection_state_test *state_test_data) {
    aws_mutex_lock(&state_test_data->lock);
    aws_condition_variable_wait_pred(
        &state_test_data->cvar,
        &state_test_data->lock,
        s_operation_statistics_is_connection_completed,
        state_test_data);
    state_test_data->connection_completed = false;
    aws_mutex_unlock(&state_test_data->lock);
}

void s_operation_statistics_on_disconnect_fn(struct aws_mqtt_client_connection *connection, void *userdata) {
    (void)connection;
    struct mqtt_connection_state_test *state_test_data = userdata;
    AWS_LOGF_DEBUG(TEST_LOG_SUBJECT, "disconnect completed");
    aws_mutex_lock(&state_test_data->lock);
    state_test_data->client_disconnect_completed = true;
    aws_mutex_unlock(&state_test_data->lock);

    aws_condition_variable_notify_one(&state_test_data->cvar);
}

static bool s_operation_statistics_is_disconnect_completed(void *arg) {
    struct mqtt_connection_state_test *state_test_data = arg;
    return state_test_data->client_disconnect_completed && state_test_data->server_disconnect_completed;
}

static void s_operation_statistics_wait_for_disconnect_to_complete(struct mqtt_connection_state_test *state_test_data) {
    aws_mutex_lock(&state_test_data->lock);
    aws_condition_variable_wait_pred(
        &state_test_data->cvar,
        &state_test_data->lock,
        s_operation_statistics_is_disconnect_completed,
        state_test_data);
    state_test_data->client_disconnect_completed = false;
    state_test_data->server_disconnect_completed = false;
    aws_mutex_unlock(&state_test_data->lock);
}

static void s_operation_statistics_on_any_publish_received(
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

static void s_operation_statistics_on_publish_received(
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

static void s_operation_statistics_on_suback(
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

/* ========== PUBLISH TESTS ========== */

/**
 * Make a connection, tell the server NOT to send Acks, publish and immediately check the statistics to make sure
 * it is incomplete, then wait a little bit and check that it was properly marked as UnAcked, then send the PubAck
 * confirm statistics are zero, and then disconnect
 */
static int s_test_mqtt_operation_statistics_simple_publish(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    struct mqtt_connection_state_test *state_test_data = ctx;

    struct aws_mqtt_connection_options connection_options = {
        .user_data = state_test_data,
        .clean_session = false,
        .client_id = aws_byte_cursor_from_c_str("client1234"),
        .host_name = aws_byte_cursor_from_c_str(state_test_data->endpoint.address),
        .socket_options = &state_test_data->socket_options,
        .on_connection_complete = s_operation_statistics_on_connection_complete_fn,
    };

    struct aws_byte_cursor pub_topic = aws_byte_cursor_from_c_str("/test/topic");
    struct aws_byte_cursor payload_1 = aws_byte_cursor_from_c_str("Test Message");

    // Connect
    ASSERT_SUCCESS(aws_mqtt_client_connection_connect(state_test_data->mqtt_connection, &connection_options));
    s_operation_statistics_wait_for_connection_to_complete(state_test_data);

    // Stop ACKS so we make sure the operation statistics correctly identify we sent a packet
    mqtt_mock_server_disable_auto_ack(state_test_data->mock_server);

    // We want to wait for 1 operation to complete
    aws_mutex_lock(&state_test_data->lock);
    state_test_data->expected_ops_completed = 1;
    aws_mutex_unlock(&state_test_data->lock);

    // Publish a packet
    uint16_t packet_id_1 = aws_mqtt_client_connection_publish(
        state_test_data->mqtt_connection,
        &pub_topic,
        AWS_MQTT_QOS_AT_LEAST_ONCE,
        false,
        &payload_1,
        s_on_op_complete,
        state_test_data);
    ASSERT_TRUE(packet_id_1 > 0);

    // Wait just a little bit to let it go to be moved into the queue/socket to be un-acked
    // If we do it too soon, it might be in between (but it is inconsistent!)
    aws_thread_current_sleep((uint64_t)ONE_SEC * 3);

    // Make sure the sizes are correct and there is only one operation waiting
    // (The size of the topic, the size of the payload, 2 for the header, 2 for the packet ID)
    uint64_t expected_packet_size = pub_topic.len + payload_1.len + 4;
    struct aws_mqtt_connection_operation_statistics operation_statistics;
    ASSERT_SUCCESS(aws_mqtt_client_connection_get_stats(state_test_data->mqtt_connection, &operation_statistics));
    ASSERT_INT_EQUALS(1, operation_statistics.incomplete_operation_count);
    ASSERT_INT_EQUALS(expected_packet_size, operation_statistics.incomplete_operation_size);
    ASSERT_INT_EQUALS(1, operation_statistics.unacked_operation_count);
    ASSERT_INT_EQUALS(expected_packet_size, operation_statistics.unacked_operation_size);

    // Send the PubAck
    mqtt_mock_server_send_puback(state_test_data->mock_server, packet_id_1);

    // Make sure the publish finished
    s_wait_for_ops_completed(state_test_data);

    // Make sure the operation values are back to zero now that the publish went out
    ASSERT_SUCCESS(aws_mqtt_client_connection_get_stats(state_test_data->mqtt_connection, &operation_statistics));
    ASSERT_INT_EQUALS(0, operation_statistics.incomplete_operation_count);
    ASSERT_INT_EQUALS(0, operation_statistics.incomplete_operation_size);
    ASSERT_INT_EQUALS(0, operation_statistics.unacked_operation_count);
    ASSERT_INT_EQUALS(0, operation_statistics.unacked_operation_size);

    // Disconnect
    ASSERT_SUCCESS(aws_mqtt_client_connection_disconnect(
        state_test_data->mqtt_connection, s_operation_statistics_on_disconnect_fn, state_test_data));
    s_operation_statistics_wait_for_disconnect_to_complete(state_test_data);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    mqtt_operation_statistics_simple_publish,
    s_operation_statistics_setup_mqtt_server_fn,
    s_test_mqtt_operation_statistics_simple_publish,
    s_operation_statistics_clean_up_mqtt_server_fn,
    &test_data)

/* ========== SUBSCRIBE TESTS ========== */

/**
 * Make a connection, tell the server NOT to send Acks, subscribe and check the statistics to make sure
 * it is incomplete, then wait a little bit and check that it was properly marked as UnAcked, then send the SubAck
 * confirm statistics are zero, and then disconnect
 */
static int s_test_mqtt_operation_statistics_simple_subscribe(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    struct mqtt_connection_state_test *state_test_data = ctx;

    struct aws_mqtt_connection_options connection_options = {
        .user_data = state_test_data,
        .clean_session = false,
        .client_id = aws_byte_cursor_from_c_str("client1234"),
        .host_name = aws_byte_cursor_from_c_str(state_test_data->endpoint.address),
        .socket_options = &state_test_data->socket_options,
        .on_connection_complete = s_operation_statistics_on_connection_complete_fn,
    };

    struct aws_byte_cursor sub_topic = aws_byte_cursor_from_c_str("/test/topic");

    // Connect
    ASSERT_SUCCESS(aws_mqtt_client_connection_connect(state_test_data->mqtt_connection, &connection_options));
    s_operation_statistics_wait_for_connection_to_complete(state_test_data);

    // We want to wait for 1 operation
    aws_mutex_lock(&state_test_data->lock);
    state_test_data->expected_ops_completed = 1;
    aws_mutex_unlock(&state_test_data->lock);

    // Stop ACKS so we make sure the operation statistics correctly identify we sent a packet
    mqtt_mock_server_disable_auto_ack(state_test_data->mock_server);

    // Send a subscribe packet
    uint16_t packet_id_1 = aws_mqtt_client_connection_subscribe(
        state_test_data->mqtt_connection,
        &sub_topic,
        AWS_MQTT_QOS_AT_LEAST_ONCE,
        s_operation_statistics_on_publish_received,
        state_test_data,
        NULL,
        s_operation_statistics_on_suback,
        state_test_data);
    ASSERT_TRUE(packet_id_1 > 0);

    // Wait just a little bit to let it go to be moved into the queue/socket to be un-acked
    // If we do it too soon, it might be in between (but it is inconsistent!)
    aws_thread_current_sleep((uint64_t)ONE_SEC * 3);

    // Make sure the sizes are correct and there is only one operation waiting
    // (The size of the topic + 3 (QoS, MSB, LSB), 2 for the header, 2 for the packet ID)
    uint64_t expected_packet_size = sub_topic.len + 7;
    struct aws_mqtt_connection_operation_statistics operation_statistics;
    ASSERT_SUCCESS(aws_mqtt_client_connection_get_stats(state_test_data->mqtt_connection, &operation_statistics));
    ASSERT_INT_EQUALS(1, operation_statistics.incomplete_operation_count);
    ASSERT_INT_EQUALS(expected_packet_size, operation_statistics.incomplete_operation_size);
    ASSERT_INT_EQUALS(1, operation_statistics.unacked_operation_count);
    ASSERT_INT_EQUALS(expected_packet_size, operation_statistics.unacked_operation_size);

    // Send the SubAck
    mqtt_mock_server_send_single_suback(state_test_data->mock_server, packet_id_1, AWS_MQTT_QOS_AT_LEAST_ONCE);

    // Wait for the ACK
    s_wait_for_ops_completed(state_test_data);

    // Make sure the operation statistics are empty
    ASSERT_SUCCESS(aws_mqtt_client_connection_get_stats(state_test_data->mqtt_connection, &operation_statistics));
    ASSERT_INT_EQUALS(0, operation_statistics.incomplete_operation_count);
    ASSERT_INT_EQUALS(0, operation_statistics.incomplete_operation_size);
    ASSERT_INT_EQUALS(0, operation_statistics.unacked_operation_count);
    ASSERT_INT_EQUALS(0, operation_statistics.unacked_operation_size);

    // Disconnect
    ASSERT_SUCCESS(aws_mqtt_client_connection_disconnect(
        state_test_data->mqtt_connection, s_operation_statistics_on_disconnect_fn, state_test_data));
    s_operation_statistics_wait_for_disconnect_to_complete(state_test_data);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    mqtt_operation_statistics_simple_subscribe,
    s_operation_statistics_setup_mqtt_server_fn,
    s_test_mqtt_operation_statistics_simple_subscribe,
    s_operation_statistics_clean_up_mqtt_server_fn,
    &test_data)

/* ========== UNSUBSCRIBE TESTS ========== */

/**
 * Make a connection, tell the server NOT to send Acks, publish and immediately check the statistics to make sure
 * it is incomplete, then wait a little bit and check that it was properly marked as UnAcked, then send the UnsubAck
 * confirm statistics are zero, and then disconnect
 */
static int s_test_mqtt_operation_statistics_simple_unsubscribe(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    struct mqtt_connection_state_test *state_test_data = ctx;

    struct aws_mqtt_connection_options connection_options = {
        .user_data = state_test_data,
        .clean_session = false,
        .client_id = aws_byte_cursor_from_c_str("client1234"),
        .host_name = aws_byte_cursor_from_c_str(state_test_data->endpoint.address),
        .socket_options = &state_test_data->socket_options,
        .on_connection_complete = s_operation_statistics_on_connection_complete_fn,
    };

    struct aws_byte_cursor unsub_topic = aws_byte_cursor_from_c_str("/test/topic");

    // Connect
    ASSERT_SUCCESS(aws_mqtt_client_connection_connect(state_test_data->mqtt_connection, &connection_options));
    s_operation_statistics_wait_for_connection_to_complete(state_test_data);

    // We want to wait for 1 operation
    aws_mutex_lock(&state_test_data->lock);
    state_test_data->expected_ops_completed = 1;
    aws_mutex_unlock(&state_test_data->lock);

    // Stop ACKS so we make sure the operation statistics correctly identify we sent a packet
    mqtt_mock_server_disable_auto_ack(state_test_data->mock_server);

    // Send a subscribe packet
    uint16_t packet_id_1 = aws_mqtt_client_connection_unsubscribe(
        state_test_data->mqtt_connection,
        &unsub_topic,
        s_on_op_complete,
        state_test_data);
    ASSERT_TRUE(packet_id_1 > 0);

    // Wait just a little bit to let it go to be moved into the queue/socket to be un-acked
    // If we do it too soon, it might be in between (but it is inconsistent!)
    aws_thread_current_sleep((uint64_t)ONE_SEC * 3);

    // Make sure the sizes are correct and there is only one operation waiting
    // (The size of the topic, 2 for the header, 2 for the packet ID)
    uint64_t expected_packet_size = unsub_topic.len + 4;
    struct aws_mqtt_connection_operation_statistics operation_statistics;
    ASSERT_SUCCESS(aws_mqtt_client_connection_get_stats(state_test_data->mqtt_connection, &operation_statistics));
    ASSERT_INT_EQUALS(1, operation_statistics.incomplete_operation_count);
    ASSERT_INT_EQUALS(expected_packet_size, operation_statistics.incomplete_operation_size);
    ASSERT_INT_EQUALS(1, operation_statistics.unacked_operation_count);
    ASSERT_INT_EQUALS(expected_packet_size, operation_statistics.unacked_operation_size);

    // Send the UnsubAck
    mqtt_mock_server_send_unsuback(state_test_data->mock_server, packet_id_1);

    // Wait for the ACK
    s_wait_for_ops_completed(state_test_data);

    // Make sure the operation statistics are empty
    ASSERT_SUCCESS(aws_mqtt_client_connection_get_stats(state_test_data->mqtt_connection, &operation_statistics));
    ASSERT_INT_EQUALS(0, operation_statistics.incomplete_operation_count);
    ASSERT_INT_EQUALS(0, operation_statistics.incomplete_operation_size);
    ASSERT_INT_EQUALS(0, operation_statistics.unacked_operation_count);
    ASSERT_INT_EQUALS(0, operation_statistics.unacked_operation_size);

    // Disconnect
    ASSERT_SUCCESS(aws_mqtt_client_connection_disconnect(
        state_test_data->mqtt_connection, s_operation_statistics_on_disconnect_fn, state_test_data));
    s_operation_statistics_wait_for_disconnect_to_complete(state_test_data);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    mqtt_operation_statistics_simple_unsubscribe,
    s_operation_statistics_setup_mqtt_server_fn,
    s_test_mqtt_operation_statistics_simple_unsubscribe,
    s_operation_statistics_clean_up_mqtt_server_fn,
    &test_data)

/* ========== OTHER TESTS ========== */

// TODO: Add Resubscribe operation tests

// TODO: Add test that adds a bunch of publishes offline, confirms they are there with correct size in incomplete

// TODO: Add test that makes 10 publishes with no ack from server, confirm they are there and unacked, have server
// disconnect, confirm publishes are now in incomplete only

// TODO: Add operation statistics callback tests
