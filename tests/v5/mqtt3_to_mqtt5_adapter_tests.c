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
#include <aws/mqtt/client.h>
#include <aws/mqtt/mqtt.h>
#include <aws/mqtt/private/v5/mqtt5_utils.h>
#include <aws/mqtt/v5/mqtt5_client.h>
#include <aws/mqtt/v5/mqtt5_listener.h>

#include <aws/testing/aws_test_harness.h>

#include <inttypes.h>
#include <math.h>

enum aws_mqtt3_lifecycle_event_type {
    AWS_MQTT3_LET_CONNECTION_COMPLETE,
    AWS_MQTT3_LET_INTERRUPTED,
    AWS_MQTT3_LET_RESUMED,
    AWS_MQTT3_LET_CLOSED,
    AWS_MQTT3_LET_DISCONNECTION_COMPLETE,
};

struct aws_mqtt3_lifecycle_event {
    enum aws_mqtt3_lifecycle_event_type type;

    uint64_t timestamp;
    int error_code;
    enum aws_mqtt_connect_return_code return_code;
    bool session_present;

    bool skip_error_code_equality;
};

struct aws_mqtt3_to_mqtt5_adapter_test_fixture_config {
    aws_mqtt_client_on_connection_interrupted_fn *on_interrupted;
    aws_mqtt_client_on_connection_resumed_fn *on_resumed;
    aws_mqtt_client_on_connection_closed_fn *on_closed;

    void *callback_user_data;
};

struct aws_mqtt3_to_mqtt5_adapter_test_fixture {
    struct aws_mqtt5_client_mock_test_fixture mqtt5_fixture;

    struct aws_mqtt_client_connection *connection;

    struct aws_array_list lifecycle_events;

    struct aws_mutex lock;
    struct aws_condition_variable signal;

    struct aws_mqtt3_to_mqtt5_adapter_test_fixture_config config;
};

static void s_init_adapter_connection_options_from_fixture(
    struct aws_mqtt_connection_options *connection_options,
    struct aws_mqtt3_to_mqtt5_adapter_test_fixture *fixture) {
    AWS_ZERO_STRUCT(*connection_options);

    connection_options->host_name = aws_byte_cursor_from_c_str(fixture->mqtt5_fixture.endpoint.address);
    connection_options->port = fixture->mqtt5_fixture.endpoint.port;
    connection_options->socket_options = &fixture->mqtt5_fixture.socket_options;
    connection_options->keep_alive_time_secs = 30;
    connection_options->ping_timeout_ms = 10000;
    connection_options->clean_session = true;
}

struct n_lifeycle_event_wait_context {
    struct aws_mqtt3_to_mqtt5_adapter_test_fixture *fixture;
    enum aws_mqtt3_lifecycle_event_type type;
    size_t count;
};

static bool s_wait_for_n_adapter_lifecycle_events_predicate(void *context) {
    struct n_lifeycle_event_wait_context *wait_context = context;
    struct aws_mqtt3_to_mqtt5_adapter_test_fixture *fixture = wait_context->fixture;

    size_t actual_count = 0;
    size_t event_count = aws_array_list_length(&fixture->lifecycle_events);
    for (size_t i = 0; i < event_count; ++i) {
        struct aws_mqtt3_lifecycle_event *actual_event = NULL;
        aws_array_list_get_at_ptr(&fixture->lifecycle_events, (void **)(&actual_event), i);
        if (actual_event->type == wait_context->type) {
            ++actual_count;
        }
    }

    return actual_count >= wait_context->count;
}

static void s_wait_for_n_adapter_lifecycle_events(
    struct aws_mqtt3_to_mqtt5_adapter_test_fixture *fixture,
    enum aws_mqtt3_lifecycle_event_type type,
    size_t count) {
    struct n_lifeycle_event_wait_context wait_context = {
        .fixture = fixture,
        .type = type,
        .count = count,
    };

    aws_mutex_lock(&fixture->lock);
    aws_condition_variable_wait_pred(
        &fixture->signal, &fixture->lock, s_wait_for_n_adapter_lifecycle_events_predicate, &wait_context);
    aws_mutex_unlock(&fixture->lock);
}

static int s_aws_mqtt3_to_mqtt5_adapter_test_fixture_verify_lifecycle_event(
    struct aws_mqtt3_lifecycle_event *expected_event,
    struct aws_mqtt3_lifecycle_event *actual_event) {
    ASSERT_INT_EQUALS(actual_event->type, expected_event->type);
    if (expected_event->skip_error_code_equality) {
        /* some error scenarios lead to different values cross-platform, so just verify yes/no in that case */
        ASSERT_TRUE((actual_event->error_code != 0) == (expected_event->error_code != 0));
    } else {
        ASSERT_INT_EQUALS(actual_event->error_code, expected_event->error_code);
    }

    ASSERT_INT_EQUALS(actual_event->return_code, expected_event->return_code);
    ASSERT_TRUE(actual_event->session_present == expected_event->session_present);

    return AWS_OP_SUCCESS;
}

static int s_aws_mqtt3_to_mqtt5_adapter_test_fixture_verify_lifecycle_sequence(
    struct aws_mqtt3_to_mqtt5_adapter_test_fixture *fixture,
    size_t expected_event_count,
    struct aws_mqtt3_lifecycle_event *expected_events,
    size_t maximum_event_count) {

    aws_mutex_lock(&fixture->lock);

    size_t actual_event_count = aws_array_list_length(&fixture->lifecycle_events);
    ASSERT_TRUE(expected_event_count <= actual_event_count);
    ASSERT_TRUE(actual_event_count <= maximum_event_count);

    for (size_t i = 0; i < expected_event_count; ++i) {
        struct aws_mqtt3_lifecycle_event *expected_event = expected_events + i;
        struct aws_mqtt3_lifecycle_event *actual_event = NULL;
        aws_array_list_get_at_ptr(&fixture->lifecycle_events, (void **)(&actual_event), i);

        ASSERT_SUCCESS(s_aws_mqtt3_to_mqtt5_adapter_test_fixture_verify_lifecycle_event(expected_event, actual_event));
    }

    aws_mutex_unlock(&fixture->lock);

    return AWS_OP_SUCCESS;
}

static int s_aws_mqtt3_to_mqtt5_adapter_test_fixture_verify_lifecycle_sequence_starts_with(
    struct aws_mqtt3_to_mqtt5_adapter_test_fixture *fixture,
    size_t expected_event_count,
    struct aws_mqtt3_lifecycle_event *expected_events) {

    aws_mutex_lock(&fixture->lock);

    size_t actual_event_count = aws_array_list_length(&fixture->lifecycle_events);
    ASSERT_TRUE(expected_event_count <= actual_event_count);

    for (size_t i = 0; i < expected_event_count; ++i) {
        struct aws_mqtt3_lifecycle_event *expected_event = expected_events + i;
        struct aws_mqtt3_lifecycle_event *actual_event = NULL;
        aws_array_list_get_at_ptr(&fixture->lifecycle_events, (void **)(&actual_event), i);

        ASSERT_SUCCESS(s_aws_mqtt3_to_mqtt5_adapter_test_fixture_verify_lifecycle_event(expected_event, actual_event));
    }

    aws_mutex_unlock(&fixture->lock);

    return AWS_OP_SUCCESS;
}

static int s_aws_mqtt3_to_mqtt5_adapter_test_fixture_verify_lifecycle_sequence_ends_with(
    struct aws_mqtt3_to_mqtt5_adapter_test_fixture *fixture,
    size_t expected_event_count,
    struct aws_mqtt3_lifecycle_event *expected_events) {

    aws_mutex_lock(&fixture->lock);

    size_t actual_event_count = aws_array_list_length(&fixture->lifecycle_events);
    ASSERT_TRUE(expected_event_count <= actual_event_count);

    for (size_t i = 0; i < expected_event_count; ++i) {
        struct aws_mqtt3_lifecycle_event *expected_event = expected_events + i;

        size_t actual_index = i + (actual_event_count - expected_event_count);
        struct aws_mqtt3_lifecycle_event *actual_event = NULL;
        aws_array_list_get_at_ptr(&fixture->lifecycle_events, (void **)(&actual_event), actual_index);

        ASSERT_SUCCESS(s_aws_mqtt3_to_mqtt5_adapter_test_fixture_verify_lifecycle_event(expected_event, actual_event));
    }

    aws_mutex_unlock(&fixture->lock);

    return AWS_OP_SUCCESS;
}

static void s_aws_mqtt3_to_mqtt5_adapter_test_fixture_closed_handler(
    struct aws_mqtt_client_connection *connection,
    struct on_connection_closed_data *data,
    void *userdata) {
    struct aws_mqtt3_to_mqtt5_adapter_test_fixture *fixture = userdata;

    /* record the event */
    struct aws_mqtt3_lifecycle_event event;
    AWS_ZERO_STRUCT(event);

    event.type = AWS_MQTT3_LET_CLOSED;
    aws_high_res_clock_get_ticks(&event.timestamp);

    aws_mutex_lock(&fixture->lock);
    aws_array_list_push_back(&fixture->lifecycle_events, &event);
    aws_mutex_unlock(&fixture->lock);
    aws_condition_variable_notify_all(&fixture->signal);

    /* invoke user callback if registered */
    if (fixture->config.on_closed) {
        (*fixture->config.on_closed)(connection, data, fixture->config.callback_user_data);
    }
}

static void s_aws_mqtt3_to_mqtt5_adapter_test_fixture_interrupted_handler(
    struct aws_mqtt_client_connection *connection,
    int error_code,
    void *userdata) {
    struct aws_mqtt3_to_mqtt5_adapter_test_fixture *fixture = userdata;

    /* record the event */
    struct aws_mqtt3_lifecycle_event event;
    AWS_ZERO_STRUCT(event);

    event.type = AWS_MQTT3_LET_INTERRUPTED;
    aws_high_res_clock_get_ticks(&event.timestamp);
    event.error_code = error_code;

    aws_mutex_lock(&fixture->lock);
    aws_array_list_push_back(&fixture->lifecycle_events, &event);
    aws_mutex_unlock(&fixture->lock);
    aws_condition_variable_notify_all(&fixture->signal);

    /* invoke user callback if registered */
    if (fixture->config.on_interrupted) {
        (*fixture->config.on_interrupted)(connection, error_code, fixture->config.callback_user_data);
    }
}

static void s_aws_mqtt3_to_mqtt5_adapter_test_fixture_resumed_handler(
    struct aws_mqtt_client_connection *connection,
    enum aws_mqtt_connect_return_code return_code,
    bool session_present,
    void *userdata) {
    struct aws_mqtt3_to_mqtt5_adapter_test_fixture *fixture = userdata;

    /* record the event */
    struct aws_mqtt3_lifecycle_event event;
    AWS_ZERO_STRUCT(event);

    event.type = AWS_MQTT3_LET_RESUMED;
    aws_high_res_clock_get_ticks(&event.timestamp);
    event.return_code = return_code;
    event.session_present = session_present;

    aws_mutex_lock(&fixture->lock);
    aws_array_list_push_back(&fixture->lifecycle_events, &event);
    aws_mutex_unlock(&fixture->lock);
    aws_condition_variable_notify_all(&fixture->signal);

    /* invoke user callback if registered */
    if (fixture->config.on_resumed) {
        (*fixture->config.on_resumed)(connection, return_code, session_present, fixture->config.callback_user_data);
    }
}

static void s_aws_mqtt3_to_mqtt5_adapter_test_fixture_record_connection_complete(
    struct aws_mqtt_client_connection *connection,
    int error_code,
    enum aws_mqtt_connect_return_code return_code,
    bool session_present,
    void *user_data) {
    (void)connection;

    struct aws_mqtt3_to_mqtt5_adapter_test_fixture *fixture = user_data;

    struct aws_mqtt3_lifecycle_event event;
    AWS_ZERO_STRUCT(event);

    event.type = AWS_MQTT3_LET_CONNECTION_COMPLETE;
    aws_high_res_clock_get_ticks(&event.timestamp);
    event.error_code = error_code;
    event.return_code = return_code;
    event.session_present = session_present;

    aws_mutex_lock(&fixture->lock);
    aws_array_list_push_back(&fixture->lifecycle_events, &event);
    aws_mutex_unlock(&fixture->lock);
    aws_condition_variable_notify_all(&fixture->signal);
}

static void s_aws_mqtt3_to_mqtt5_adapter_test_fixture_record_disconnection_complete(
    struct aws_mqtt_client_connection *connection,
    void *user_data) {
    (void)connection;

    struct aws_mqtt3_to_mqtt5_adapter_test_fixture *fixture = user_data;

    struct aws_mqtt3_lifecycle_event event;
    AWS_ZERO_STRUCT(event);

    event.type = AWS_MQTT3_LET_DISCONNECTION_COMPLETE;
    aws_high_res_clock_get_ticks(&event.timestamp);

    aws_mutex_lock(&fixture->lock);
    aws_array_list_push_back(&fixture->lifecycle_events, &event);
    aws_mutex_unlock(&fixture->lock);
    aws_condition_variable_notify_all(&fixture->signal);
}

int aws_mqtt3_to_mqtt5_adapter_test_fixture_init(
    struct aws_mqtt3_to_mqtt5_adapter_test_fixture *fixture,
    struct aws_allocator *allocator,
    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options *mqtt5_fixture_config,
    struct aws_mqtt3_to_mqtt5_adapter_test_fixture_config *config) {
    AWS_ZERO_STRUCT(*fixture);

    if (aws_mqtt5_client_mock_test_fixture_init(&fixture->mqtt5_fixture, allocator, mqtt5_fixture_config)) {
        return AWS_OP_ERR;
    }

    fixture->connection = aws_mqtt_client_connection_new_from_mqtt5_client(fixture->mqtt5_fixture.client);
    if (fixture->connection == NULL) {
        return AWS_OP_ERR;
    }

    aws_array_list_init_dynamic(&fixture->lifecycle_events, allocator, 10, sizeof(struct aws_mqtt3_lifecycle_event));

    aws_mutex_init(&fixture->lock);
    aws_condition_variable_init(&fixture->signal);

    if (config) {
        fixture->config = *config;
    }

    aws_mqtt_client_connection_set_connection_closed_handler(
        fixture->connection, s_aws_mqtt3_to_mqtt5_adapter_test_fixture_closed_handler, fixture);
    aws_mqtt_client_connection_set_connection_interruption_handlers(
        fixture->connection,
        s_aws_mqtt3_to_mqtt5_adapter_test_fixture_interrupted_handler,
        fixture,
        s_aws_mqtt3_to_mqtt5_adapter_test_fixture_resumed_handler,
        fixture);

    return AWS_OP_SUCCESS;
}

void aws_mqtt3_to_mqtt5_adapter_test_fixture_clean_up(struct aws_mqtt3_to_mqtt5_adapter_test_fixture *fixture) {
    aws_mqtt_client_connection_release(fixture->connection);

    aws_mqtt5_client_mock_test_fixture_clean_up(&fixture->mqtt5_fixture);

    aws_array_list_clean_up(&fixture->lifecycle_events);

    aws_mutex_clean_up(&fixture->lock);
    aws_condition_variable_clean_up(&fixture->signal);
}

void s_mqtt3to5_lifecycle_event_callback(const struct aws_mqtt5_client_lifecycle_event *event) {
    (void)event;
}

void s_mqtt3to5_publish_received_callback(const struct aws_mqtt5_packet_publish_view *publish, void *user_data) {
    (void)publish;
    (void)user_data;
}

static int s_do_mqtt3to5_adapter_create_destroy(struct aws_allocator *allocator, uint64_t sleep_nanos) {
    aws_mqtt_library_init(allocator);

    struct aws_mqtt5_packet_connect_view local_connect_options = {
        .keep_alive_interval_seconds = 30,
        .clean_start = true,
    };

    struct aws_mqtt5_client_options client_options = {
        .connect_options = &local_connect_options,
        .lifecycle_event_handler = s_mqtt3to5_lifecycle_event_callback,
        .lifecycle_event_handler_user_data = NULL,
        .publish_received_handler = s_mqtt3to5_publish_received_callback,
        .publish_received_handler_user_data = NULL,
        .ping_timeout_ms = 10000,
    };

    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options test_fixture_config = {
        .client_options = &client_options,
    };

    struct aws_mqtt5_client_mock_test_fixture test_fixture;
    AWS_ZERO_STRUCT(test_fixture);

    ASSERT_SUCCESS(aws_mqtt5_client_mock_test_fixture_init(&test_fixture, allocator, &test_fixture_config));

    struct aws_mqtt_client_connection *connection =
        aws_mqtt_client_connection_new_from_mqtt5_client(test_fixture.client);

    if (sleep_nanos > 0) {
        /* sleep a little just to let the listener attachment resolve */
        aws_thread_current_sleep(aws_timestamp_convert(1, AWS_TIMESTAMP_SECS, AWS_TIMESTAMP_NANOS, NULL));
    }

    aws_mqtt_client_connection_release(connection);

    if (sleep_nanos > 0) {
        /* sleep a little just to let the listener detachment resolve */
        aws_thread_current_sleep(aws_timestamp_convert(1, AWS_TIMESTAMP_SECS, AWS_TIMESTAMP_NANOS, NULL));
    }

    aws_mqtt5_client_mock_test_fixture_clean_up(&test_fixture);

    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

static int s_mqtt3to5_adapter_create_destroy_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_do_mqtt3to5_adapter_create_destroy(allocator, 0));

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt3to5_adapter_create_destroy, s_mqtt3to5_adapter_create_destroy_fn)

static int s_mqtt3to5_adapter_create_destroy_delayed_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_do_mqtt3to5_adapter_create_destroy(
        allocator, aws_timestamp_convert(1, AWS_TIMESTAMP_SECS, AWS_TIMESTAMP_NANOS, NULL)));

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt3to5_adapter_create_destroy_delayed, s_mqtt3to5_adapter_create_destroy_delayed_fn)

typedef int (*mqtt3to5_adapter_config_test_setup_fn)(
    struct aws_allocator *allocator,
    struct aws_mqtt_client_connection *adapter,
    struct aws_mqtt5_packet_connect_storage *expected_connect);

static int s_do_mqtt3to5_adapter_config_test(
    struct aws_allocator *allocator,
    mqtt3to5_adapter_config_test_setup_fn setup_fn) {
    aws_mqtt_library_init(allocator);

    struct mqtt5_client_test_options test_options;
    aws_mqtt5_client_test_init_default_options(&test_options);

    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options test_fixture_options = {
        .client_options = &test_options.client_options,
        .server_function_table = &test_options.server_function_table,
    };

    struct aws_mqtt3_to_mqtt5_adapter_test_fixture test_fixture;
    ASSERT_SUCCESS(aws_mqtt3_to_mqtt5_adapter_test_fixture_init(&test_fixture, allocator, &test_fixture_options, NULL));

    struct aws_mqtt5_client *client = test_fixture.mqtt5_fixture.client;

    struct aws_mqtt_client_connection *adapter = test_fixture.connection;

    struct aws_mqtt5_packet_connect_storage expected_connect_storage;
    ASSERT_SUCCESS((*setup_fn)(allocator, adapter, &expected_connect_storage));

    ASSERT_SUCCESS(aws_mqtt5_client_start(client));

    aws_wait_for_connected_lifecycle_event(&test_fixture.mqtt5_fixture);

    ASSERT_SUCCESS(aws_mqtt5_client_stop(client, NULL, NULL));

    aws_wait_for_stopped_lifecycle_event(&test_fixture.mqtt5_fixture);

    struct aws_mqtt5_mock_server_packet_record expected_packets[] = {
        {
            .packet_type = AWS_MQTT5_PT_CONNECT,
            .packet_storage = &expected_connect_storage,
        },
    };
    ASSERT_SUCCESS(aws_verify_received_packet_sequence(
        &test_fixture.mqtt5_fixture, expected_packets, AWS_ARRAY_SIZE(expected_packets)));

    aws_mqtt5_packet_connect_storage_clean_up(&expected_connect_storage);

    aws_mqtt3_to_mqtt5_adapter_test_fixture_clean_up(&test_fixture);

    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_STATIC_STRING_FROM_LITERAL(s_simple_topic, "Hello/World");
AWS_STATIC_STRING_FROM_LITERAL(s_simple_payload, "A Payload");

static int s_mqtt3to5_adapter_set_will_setup(
    struct aws_allocator *allocator,
    struct aws_mqtt_client_connection *adapter,
    struct aws_mqtt5_packet_connect_storage *expected_connect) {

    struct aws_byte_cursor topic_cursor = aws_byte_cursor_from_string(s_simple_topic);
    struct aws_byte_cursor payload_cursor = aws_byte_cursor_from_string(s_simple_payload);

    ASSERT_SUCCESS(
        aws_mqtt_client_connection_set_will(adapter, &topic_cursor, AWS_MQTT_QOS_AT_LEAST_ONCE, true, &payload_cursor));

    struct aws_mqtt5_packet_publish_view expected_will = {
        .payload = payload_cursor,
        .qos = AWS_MQTT5_QOS_AT_LEAST_ONCE,
        .retain = true,
        .topic = topic_cursor,
    };

    struct aws_mqtt5_packet_connect_view expected_connect_view = {
        .client_id = aws_byte_cursor_from_string(g_default_client_id),
        .keep_alive_interval_seconds = 30,
        .clean_start = true,
        .will = &expected_will,
    };

    ASSERT_SUCCESS(aws_mqtt5_packet_connect_storage_init(expected_connect, allocator, &expected_connect_view));

    return AWS_OP_SUCCESS;
}

static int s_mqtt3to5_adapter_set_will_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_do_mqtt3to5_adapter_config_test(allocator, s_mqtt3to5_adapter_set_will_setup));

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt3to5_adapter_set_will, s_mqtt3to5_adapter_set_will_fn)

AWS_STATIC_STRING_FROM_LITERAL(s_username, "MyUsername");
AWS_STATIC_STRING_FROM_LITERAL(s_password, "TopTopSecret");

static int s_mqtt3to5_adapter_set_login_setup(
    struct aws_allocator *allocator,
    struct aws_mqtt_client_connection *adapter,
    struct aws_mqtt5_packet_connect_storage *expected_connect) {

    struct aws_byte_cursor username_cursor = aws_byte_cursor_from_string(s_username);
    struct aws_byte_cursor password_cursor = aws_byte_cursor_from_string(s_password);

    ASSERT_SUCCESS(aws_mqtt_client_connection_set_login(adapter, &username_cursor, &password_cursor));

    struct aws_mqtt5_packet_connect_view expected_connect_view = {
        .client_id = aws_byte_cursor_from_string(g_default_client_id),
        .keep_alive_interval_seconds = 30,
        .clean_start = true,
        .username = &username_cursor,
        .password = &password_cursor,
    };

    ASSERT_SUCCESS(aws_mqtt5_packet_connect_storage_init(expected_connect, allocator, &expected_connect_view));

    return AWS_OP_SUCCESS;
}

static int s_mqtt3to5_adapter_set_login_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_do_mqtt3to5_adapter_config_test(allocator, s_mqtt3to5_adapter_set_login_setup));

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt3to5_adapter_set_login, s_mqtt3to5_adapter_set_login_fn)

static int s_mqtt3to5_adapter_set_reconnect_timeout_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    /*
     * This is a variant of the mqtt5_client_reconnect_failure_backoff test.
     *
     * The primary change is that we configure the mqtt5 client with "wrong" (fast) reconnect delays and then use
     * the adapter API to configure with the "right" ones that will let the test pass.
     */
    aws_mqtt_library_init(allocator);

    struct mqtt5_client_test_options test_options;
    aws_mqtt5_client_test_init_default_options(&test_options);

    /* backoff delay sequence: 500, 1000, 2000, 4000, 5000, ... */
    test_options.client_options.retry_jitter_mode = AWS_EXPONENTIAL_BACKOFF_JITTER_NONE;
    test_options.client_options.min_reconnect_delay_ms = 10;
    test_options.client_options.max_reconnect_delay_ms = 50;
    test_options.client_options.min_connected_time_to_reset_reconnect_delay_ms = RECONNECT_TEST_BACKOFF_RESET_DELAY;

    test_options.server_function_table.packet_handlers[AWS_MQTT5_PT_CONNECT] =
        aws_mqtt5_mock_server_handle_connect_always_fail;

    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options test_fixture_options = {
        .client_options = &test_options.client_options,
        .server_function_table = &test_options.server_function_table,
    };

    struct aws_mqtt5_client_mock_test_fixture test_context;
    ASSERT_SUCCESS(aws_mqtt5_client_mock_test_fixture_init(&test_context, allocator, &test_fixture_options));

    struct aws_mqtt5_client *client = test_context.client;

    struct aws_mqtt_client_connection *adapter = aws_mqtt_client_connection_new_from_mqtt5_client(client);

    aws_mqtt_client_connection_set_reconnect_timeout(adapter, RECONNECT_TEST_MIN_BACKOFF, RECONNECT_TEST_MAX_BACKOFF);

    ASSERT_SUCCESS(aws_mqtt5_client_start(client));

    aws_mqtt5_wait_for_n_lifecycle_events(&test_context, AWS_MQTT5_CLET_CONNECTION_FAILURE, 6);

    ASSERT_SUCCESS(aws_mqtt5_client_stop(client, NULL, NULL));

    aws_wait_for_stopped_lifecycle_event(&test_context);

    ASSERT_SUCCESS(aws_verify_reconnection_exponential_backoff_timestamps(&test_context));

    /* 6 (connecting, mqtt_connect, channel_shutdown, pending_reconnect) tuples (minus the final pending_reconnect) */
    enum aws_mqtt5_client_state expected_states[] = {
        AWS_MCS_CONNECTING, AWS_MCS_MQTT_CONNECT, AWS_MCS_CHANNEL_SHUTDOWN, AWS_MCS_PENDING_RECONNECT,
        AWS_MCS_CONNECTING, AWS_MCS_MQTT_CONNECT, AWS_MCS_CHANNEL_SHUTDOWN, AWS_MCS_PENDING_RECONNECT,
        AWS_MCS_CONNECTING, AWS_MCS_MQTT_CONNECT, AWS_MCS_CHANNEL_SHUTDOWN, AWS_MCS_PENDING_RECONNECT,
        AWS_MCS_CONNECTING, AWS_MCS_MQTT_CONNECT, AWS_MCS_CHANNEL_SHUTDOWN, AWS_MCS_PENDING_RECONNECT,
        AWS_MCS_CONNECTING, AWS_MCS_MQTT_CONNECT, AWS_MCS_CHANNEL_SHUTDOWN, AWS_MCS_PENDING_RECONNECT,
        AWS_MCS_CONNECTING, AWS_MCS_MQTT_CONNECT, AWS_MCS_CHANNEL_SHUTDOWN,
    };
    ASSERT_SUCCESS(aws_verify_client_state_sequence(&test_context, expected_states, AWS_ARRAY_SIZE(expected_states)));

    aws_mqtt_client_connection_release(adapter);

    aws_mqtt5_client_mock_test_fixture_clean_up(&test_context);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt3to5_adapter_set_reconnect_timeout, s_mqtt3to5_adapter_set_reconnect_timeout_fn)

/*
 * Basic successful connection test
 */
static int s_mqtt3to5_adapter_connect_success_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct mqtt5_client_test_options test_options;
    aws_mqtt5_client_test_init_default_options(&test_options);

    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options test_fixture_options = {
        .client_options = &test_options.client_options,
        .server_function_table = &test_options.server_function_table,
    };

    struct aws_mqtt3_to_mqtt5_adapter_test_fixture fixture;
    ASSERT_SUCCESS(aws_mqtt3_to_mqtt5_adapter_test_fixture_init(&fixture, allocator, &test_fixture_options, NULL));

    struct aws_mqtt_client_connection *adapter = fixture.connection;

    struct aws_mqtt_connection_options connection_options;
    s_init_adapter_connection_options_from_fixture(&connection_options, &fixture);

    connection_options.on_connection_complete = s_aws_mqtt3_to_mqtt5_adapter_test_fixture_record_connection_complete;
    connection_options.user_data = &fixture;

    aws_mqtt_client_connection_connect(adapter, &connection_options);

    s_wait_for_n_adapter_lifecycle_events(&fixture, AWS_MQTT3_LET_CONNECTION_COMPLETE, 1);

    struct aws_mqtt3_lifecycle_event expected_events[] = {
        {
            .type = AWS_MQTT3_LET_CONNECTION_COMPLETE,
        },
    };
    ASSERT_SUCCESS(s_aws_mqtt3_to_mqtt5_adapter_test_fixture_verify_lifecycle_sequence(
        &fixture, AWS_ARRAY_SIZE(expected_events), expected_events, AWS_ARRAY_SIZE(expected_events)));

    aws_mqtt3_to_mqtt5_adapter_test_fixture_clean_up(&fixture);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt3to5_adapter_connect_success, s_mqtt3to5_adapter_connect_success_fn)

static int s_do_mqtt3to5_adapter_connect_success_disconnect_success_cycle(
    struct aws_allocator *allocator,
    size_t iterations) {
    aws_mqtt_library_init(allocator);

    struct mqtt5_client_test_options test_options;
    aws_mqtt5_client_test_init_default_options(&test_options);

    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options test_fixture_options = {
        .client_options = &test_options.client_options,
        .server_function_table = &test_options.server_function_table,
    };

    struct aws_mqtt3_to_mqtt5_adapter_test_fixture fixture;
    ASSERT_SUCCESS(aws_mqtt3_to_mqtt5_adapter_test_fixture_init(&fixture, allocator, &test_fixture_options, NULL));

    struct aws_mqtt_client_connection *adapter = fixture.connection;

    for (size_t i = 0; i < iterations; ++i) {
        struct aws_mqtt_connection_options connection_options;
        s_init_adapter_connection_options_from_fixture(&connection_options, &fixture);

        connection_options.on_connection_complete =
            s_aws_mqtt3_to_mqtt5_adapter_test_fixture_record_connection_complete;
        connection_options.user_data = &fixture;

        aws_mqtt_client_connection_connect(adapter, &connection_options);

        s_wait_for_n_adapter_lifecycle_events(&fixture, AWS_MQTT3_LET_CONNECTION_COMPLETE, i + 1);

        aws_mqtt_client_connection_disconnect(
            adapter, s_aws_mqtt3_to_mqtt5_adapter_test_fixture_record_disconnection_complete, &fixture);

        s_wait_for_n_adapter_lifecycle_events(&fixture, AWS_MQTT3_LET_DISCONNECTION_COMPLETE, i + 1);
        s_wait_for_n_adapter_lifecycle_events(&fixture, AWS_MQTT3_LET_CLOSED, i + 1);

        struct aws_mqtt3_lifecycle_event expected_event_sequence[] = {
            {
                .type = AWS_MQTT3_LET_CONNECTION_COMPLETE,
            },
            {
                .type = AWS_MQTT3_LET_DISCONNECTION_COMPLETE,
            },
            {
                .type = AWS_MQTT3_LET_CLOSED,
            },
        };

        size_t expected_event_count = (i + 1) * 3;
        struct aws_mqtt3_lifecycle_event *expected_events =
            aws_mem_calloc(allocator, expected_event_count, sizeof(struct aws_mqtt3_lifecycle_event));
        for (size_t j = 0; j < i + 1; ++j) {
            for (size_t k = 0; k < 3; ++k) {
                *(expected_events + j * 3 + k) = expected_event_sequence[k];
            }
        }

        ASSERT_SUCCESS(s_aws_mqtt3_to_mqtt5_adapter_test_fixture_verify_lifecycle_sequence(
            &fixture, expected_event_count, expected_events, expected_event_count));

        aws_mem_release(allocator, expected_events);
    }

    aws_mqtt3_to_mqtt5_adapter_test_fixture_clean_up(&fixture);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

/*
 * A couple of simple connect-disconnect cycle tests.  The first does a single cycle while the second does several.
 * Verifies proper lifecycle event sequencing.
 */
static int s_mqtt3to5_adapter_connect_success_disconnect_success_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_do_mqtt3to5_adapter_connect_success_disconnect_success_cycle(allocator, 1));

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(
    mqtt3to5_adapter_connect_success_disconnect_success,
    s_mqtt3to5_adapter_connect_success_disconnect_success_fn)

static int s_mqtt3to5_adapter_connect_success_disconnect_success_thrice_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_do_mqtt3to5_adapter_connect_success_disconnect_success_cycle(allocator, 3));

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(
    mqtt3to5_adapter_connect_success_disconnect_success_thrice,
    s_mqtt3to5_adapter_connect_success_disconnect_success_thrice_fn)

/*
 * Verifies that calling connect() while connected yields a connection completion callback with the
 * appropriate already-connected error code.  Note that in the mqtt311 impl, this error is synchronous.
 */
static int s_mqtt3to5_adapter_connect_success_connect_failure_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct mqtt5_client_test_options test_options;
    aws_mqtt5_client_test_init_default_options(&test_options);

    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options test_fixture_options = {
        .client_options = &test_options.client_options,
        .server_function_table = &test_options.server_function_table,
    };

    struct aws_mqtt3_to_mqtt5_adapter_test_fixture fixture;
    ASSERT_SUCCESS(aws_mqtt3_to_mqtt5_adapter_test_fixture_init(&fixture, allocator, &test_fixture_options, NULL));

    struct aws_mqtt_client_connection *adapter = fixture.connection;

    struct aws_mqtt_connection_options connection_options;
    s_init_adapter_connection_options_from_fixture(&connection_options, &fixture);

    connection_options.on_connection_complete = s_aws_mqtt3_to_mqtt5_adapter_test_fixture_record_connection_complete;
    connection_options.user_data = &fixture;

    aws_mqtt_client_connection_connect(adapter, &connection_options);

    s_wait_for_n_adapter_lifecycle_events(&fixture, AWS_MQTT3_LET_CONNECTION_COMPLETE, 1);

    aws_mqtt_client_connection_connect(adapter, &connection_options);

    s_wait_for_n_adapter_lifecycle_events(&fixture, AWS_MQTT3_LET_CONNECTION_COMPLETE, 2);

    struct aws_mqtt3_lifecycle_event expected_events[] = {
        {
            .type = AWS_MQTT3_LET_CONNECTION_COMPLETE,
        },
        {
            .type = AWS_MQTT3_LET_CONNECTION_COMPLETE,
            .error_code = AWS_ERROR_MQTT_ALREADY_CONNECTED,
        },
    };
    ASSERT_SUCCESS(s_aws_mqtt3_to_mqtt5_adapter_test_fixture_verify_lifecycle_sequence(
        &fixture, AWS_ARRAY_SIZE(expected_events), expected_events, AWS_ARRAY_SIZE(expected_events)));

    aws_mqtt3_to_mqtt5_adapter_test_fixture_clean_up(&fixture);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt3to5_adapter_connect_success_connect_failure, s_mqtt3to5_adapter_connect_success_connect_failure_fn)

/*
 * A non-deterministic test that starts the connect process and immediately drops the last external adapter
 * reference.  Intended to stochastically shake out shutdown race conditions.
 */
static int s_mqtt3to5_adapter_connect_success_sloppy_shutdown_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct mqtt5_client_test_options test_options;
    aws_mqtt5_client_test_init_default_options(&test_options);

    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options test_fixture_options = {
        .client_options = &test_options.client_options,
        .server_function_table = &test_options.server_function_table,
    };

    struct aws_mqtt3_to_mqtt5_adapter_test_fixture fixture;
    ASSERT_SUCCESS(aws_mqtt3_to_mqtt5_adapter_test_fixture_init(&fixture, allocator, &test_fixture_options, NULL));

    struct aws_mqtt_client_connection *adapter = fixture.connection;

    struct aws_mqtt_connection_options connection_options;
    s_init_adapter_connection_options_from_fixture(&connection_options, &fixture);

    connection_options.on_connection_complete = s_aws_mqtt3_to_mqtt5_adapter_test_fixture_record_connection_complete;
    connection_options.user_data = &fixture;

    aws_mqtt_client_connection_connect(adapter, &connection_options);

    aws_mqtt3_to_mqtt5_adapter_test_fixture_clean_up(&fixture);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt3to5_adapter_connect_success_sloppy_shutdown, s_mqtt3to5_adapter_connect_success_sloppy_shutdown_fn)

static int s_aws_mqtt5_server_disconnect_after_connect(
    void *packet,
    struct aws_mqtt5_server_mock_connection_context *connection,
    void *user_data) {
    aws_mqtt5_mock_server_handle_connect_always_succeed(packet, connection, user_data);

    struct aws_mqtt5_packet_disconnect_view disconnect = {
        .reason_code = AWS_MQTT5_DRC_SERVER_SHUTTING_DOWN,
    };

    int result = aws_mqtt5_mock_server_send_packet(connection, AWS_MQTT5_PT_DISCONNECT, &disconnect);

    return result;
}

static int s_verify_bad_connectivity_callbacks(struct aws_mqtt3_to_mqtt5_adapter_test_fixture *fixture) {
    struct aws_mqtt3_lifecycle_event expected_events[] = {
        {
            .type = AWS_MQTT3_LET_CONNECTION_COMPLETE,
        },
        {
            .type = AWS_MQTT3_LET_INTERRUPTED,
            .error_code = AWS_ERROR_MQTT5_DISCONNECT_RECEIVED,
        },
        {
            .type = AWS_MQTT3_LET_RESUMED,
        },
        {
            .type = AWS_MQTT3_LET_INTERRUPTED,
            .error_code = AWS_ERROR_MQTT5_DISCONNECT_RECEIVED,
        },
        {
            .type = AWS_MQTT3_LET_RESUMED,
        },
        {
            .type = AWS_MQTT3_LET_INTERRUPTED,
            .error_code = AWS_ERROR_MQTT5_DISCONNECT_RECEIVED,
        },
        {
            .type = AWS_MQTT3_LET_DISCONNECTION_COMPLETE,
        },
        {
            .type = AWS_MQTT3_LET_CLOSED,
        },
    };
    ASSERT_SUCCESS(s_aws_mqtt3_to_mqtt5_adapter_test_fixture_verify_lifecycle_sequence(
        fixture, AWS_ARRAY_SIZE(expected_events), expected_events, AWS_ARRAY_SIZE(expected_events)));

    return AWS_OP_SUCCESS;
}

static int s_do_bad_connectivity_basic_test(struct aws_mqtt3_to_mqtt5_adapter_test_fixture *fixture) {
    struct aws_mqtt_client_connection *adapter = fixture->connection;

    struct aws_mqtt_connection_options connection_options;
    s_init_adapter_connection_options_from_fixture(&connection_options, fixture);

    connection_options.on_connection_complete = s_aws_mqtt3_to_mqtt5_adapter_test_fixture_record_connection_complete;
    connection_options.user_data = fixture;

    aws_mqtt_client_connection_connect(adapter, &connection_options);

    s_wait_for_n_adapter_lifecycle_events(fixture, AWS_MQTT3_LET_INTERRUPTED, 3);

    aws_mqtt_client_connection_disconnect(
        adapter, s_aws_mqtt3_to_mqtt5_adapter_test_fixture_record_disconnection_complete, fixture);

    s_wait_for_n_adapter_lifecycle_events(fixture, AWS_MQTT3_LET_CLOSED, 1);

    ASSERT_SUCCESS(s_verify_bad_connectivity_callbacks(fixture));

    return AWS_OP_SUCCESS;
}

/*
 * A test where each successful connection is immediately dropped after the connack is sent.  Allows us to verify
 * proper interrupt/resume sequencing.
 */
static int s_mqtt3to5_adapter_connect_bad_connectivity_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct mqtt5_client_test_options test_options;
    aws_mqtt5_client_test_init_default_options(&test_options);

    /* So that the test doesn't get excessively slow due to all the reconnects with backoff */
    test_options.client_options.min_reconnect_delay_ms = 500;
    test_options.client_options.max_reconnect_delay_ms = 1000;

    test_options.server_function_table.packet_handlers[AWS_MQTT5_PT_CONNECT] =
        s_aws_mqtt5_server_disconnect_after_connect;

    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options test_fixture_options = {
        .client_options = &test_options.client_options,
        .server_function_table = &test_options.server_function_table,
    };

    struct aws_mqtt3_to_mqtt5_adapter_test_fixture fixture;
    ASSERT_SUCCESS(aws_mqtt3_to_mqtt5_adapter_test_fixture_init(&fixture, allocator, &test_fixture_options, NULL));

    ASSERT_SUCCESS(s_do_bad_connectivity_basic_test(&fixture));

    aws_mqtt3_to_mqtt5_adapter_test_fixture_clean_up(&fixture);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt3to5_adapter_connect_bad_connectivity, s_mqtt3to5_adapter_connect_bad_connectivity_fn)

/*
 * A variant of the bad connectivity test where we restart the mqtt5 client after the main test is over and verify
 * we don't get any interrupt/resume callbacks.
 */
static int s_mqtt3to5_adapter_connect_bad_connectivity_with_mqtt5_restart_fn(
    struct aws_allocator *allocator,
    void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct mqtt5_client_test_options test_options;
    aws_mqtt5_client_test_init_default_options(&test_options);

    /* So that the test doesn't get excessively slow due to all the reconnects with backoff */
    test_options.client_options.min_reconnect_delay_ms = 500;
    test_options.client_options.max_reconnect_delay_ms = 1000;

    test_options.server_function_table.packet_handlers[AWS_MQTT5_PT_CONNECT] =
        s_aws_mqtt5_server_disconnect_after_connect;

    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options test_fixture_options = {
        .client_options = &test_options.client_options,
        .server_function_table = &test_options.server_function_table,
    };

    struct aws_mqtt3_to_mqtt5_adapter_test_fixture fixture;
    ASSERT_SUCCESS(aws_mqtt3_to_mqtt5_adapter_test_fixture_init(&fixture, allocator, &test_fixture_options, NULL));

    ASSERT_SUCCESS(s_do_bad_connectivity_basic_test(&fixture));

    /*
     * Now restart the 5 client, wait for a few more connection success/disconnect cycles, and then verify that no
     * further adapter callbacks were invoked because of this.
     */
    aws_mqtt5_client_start(fixture.mqtt5_fixture.client);

    aws_mqtt5_wait_for_n_lifecycle_events(&fixture.mqtt5_fixture, AWS_MQTT5_CLET_CONNECTION_SUCCESS, 6);

    aws_thread_current_sleep(aws_timestamp_convert(2, AWS_TIMESTAMP_SECS, AWS_TIMESTAMP_NANOS, NULL));

    ASSERT_SUCCESS(s_verify_bad_connectivity_callbacks(&fixture));

    aws_mqtt3_to_mqtt5_adapter_test_fixture_clean_up(&fixture);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(
    mqtt3to5_adapter_connect_bad_connectivity_with_mqtt5_restart,
    s_mqtt3to5_adapter_connect_bad_connectivity_with_mqtt5_restart_fn)

int aws_mqtt5_mock_server_handle_connect_succeed_on_or_after_nth(
    void *packet,
    struct aws_mqtt5_server_mock_connection_context *connection,
    void *user_data) {
    (void)packet;

    struct aws_mqtt5_mock_server_reconnect_state *context = user_data;

    struct aws_mqtt5_packet_connack_view connack_view;
    AWS_ZERO_STRUCT(connack_view);

    if (context->connection_attempts >= context->required_connection_failure_count) {
        connack_view.reason_code = AWS_MQTT5_CRC_SUCCESS;
        aws_high_res_clock_get_ticks(&context->connect_timestamp);
    } else {
        connack_view.reason_code = AWS_MQTT5_CRC_NOT_AUTHORIZED;
    }

    ++context->connection_attempts;

    return aws_mqtt5_mock_server_send_packet(connection, AWS_MQTT5_PT_CONNACK, &connack_view);
}

/*
 * Test where the initial connect is rejected, which should put the adapter to sleep.  Meanwhile followup attempts
 * are successful and the mqtt5 client itself becomes connected.
 */
static int s_mqtt3to5_adapter_connect_failure_connect_success_via_mqtt5_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct aws_mqtt5_mock_server_reconnect_state mock_server_state = {
        .required_connection_failure_count = 1,
    };

    struct mqtt5_client_test_options test_options;
    aws_mqtt5_client_test_init_default_options(&test_options);

    test_options.server_function_table.packet_handlers[AWS_MQTT5_PT_CONNECT] =
        aws_mqtt5_mock_server_handle_connect_succeed_on_or_after_nth;

    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options test_fixture_options = {
        .client_options = &test_options.client_options,
        .server_function_table = &test_options.server_function_table,
        .mock_server_user_data = &mock_server_state,
    };

    struct aws_mqtt3_to_mqtt5_adapter_test_fixture fixture;
    ASSERT_SUCCESS(aws_mqtt3_to_mqtt5_adapter_test_fixture_init(&fixture, allocator, &test_fixture_options, NULL));

    struct aws_mqtt_client_connection *adapter = fixture.connection;

    struct aws_mqtt_connection_options connection_options;
    s_init_adapter_connection_options_from_fixture(&connection_options, &fixture);

    connection_options.on_connection_complete = s_aws_mqtt3_to_mqtt5_adapter_test_fixture_record_connection_complete;
    connection_options.user_data = &fixture;

    aws_mqtt_client_connection_connect(adapter, &connection_options);

    // wait for and verify a connection failure
    s_wait_for_n_adapter_lifecycle_events(&fixture, AWS_MQTT3_LET_CONNECTION_COMPLETE, 1);

    struct aws_mqtt3_lifecycle_event expected_events[] = {
        {
            .type = AWS_MQTT3_LET_CONNECTION_COMPLETE,
            .error_code = AWS_ERROR_MQTT5_CONNACK_CONNECTION_REFUSED,
        },
    };
    ASSERT_SUCCESS(s_aws_mqtt3_to_mqtt5_adapter_test_fixture_verify_lifecycle_sequence(
        &fixture, AWS_ARRAY_SIZE(expected_events), expected_events, AWS_ARRAY_SIZE(expected_events)));

    // wait for the mqtt5 client to successfully connect on the second try
    aws_mqtt5_wait_for_n_lifecycle_events(&fixture.mqtt5_fixture, AWS_MQTT5_CLET_CONNECTION_SUCCESS, 1);

    // verify we didn't get any callbacks on the adapter
    ASSERT_SUCCESS(s_aws_mqtt3_to_mqtt5_adapter_test_fixture_verify_lifecycle_sequence(
        &fixture, AWS_ARRAY_SIZE(expected_events), expected_events, AWS_ARRAY_SIZE(expected_events)));

    // "connect" on the adapter, wait for and verify success
    aws_mqtt_client_connection_connect(adapter, &connection_options);
    s_wait_for_n_adapter_lifecycle_events(&fixture, AWS_MQTT3_LET_CONNECTION_COMPLETE, 2);

    struct aws_mqtt3_lifecycle_event expected_reconnect_events[] = {
        {
            .type = AWS_MQTT3_LET_CONNECTION_COMPLETE,
            .error_code = AWS_ERROR_MQTT5_CONNACK_CONNECTION_REFUSED,
        },
        {
            .type = AWS_MQTT3_LET_CONNECTION_COMPLETE,
        },
    };
    ASSERT_SUCCESS(s_aws_mqtt3_to_mqtt5_adapter_test_fixture_verify_lifecycle_sequence(
        &fixture,
        AWS_ARRAY_SIZE(expected_reconnect_events),
        expected_reconnect_events,
        AWS_ARRAY_SIZE(expected_reconnect_events)));

    aws_mqtt3_to_mqtt5_adapter_test_fixture_clean_up(&fixture);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(
    mqtt3to5_adapter_connect_failure_connect_success_via_mqtt5,
    s_mqtt3to5_adapter_connect_failure_connect_success_via_mqtt5_fn)

AWS_STATIC_STRING_FROM_LITERAL(s_bad_host_name, "derpity_derp");

/*
 * Fails to connect with a bad config.  Follow up with a good config.  Verifies that config is re-evaluated with
 * each connect() invocation.
 */
static int s_mqtt3to5_adapter_connect_failure_bad_config_success_good_config_fn(
    struct aws_allocator *allocator,
    void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct mqtt5_client_test_options test_options;
    aws_mqtt5_client_test_init_default_options(&test_options);

    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options test_fixture_options = {
        .client_options = &test_options.client_options,
        .server_function_table = &test_options.server_function_table,
    };

    struct aws_mqtt3_to_mqtt5_adapter_test_fixture fixture;
    ASSERT_SUCCESS(aws_mqtt3_to_mqtt5_adapter_test_fixture_init(&fixture, allocator, &test_fixture_options, NULL));

    struct aws_mqtt_client_connection *adapter = fixture.connection;

    struct aws_mqtt_connection_options connection_options;
    s_init_adapter_connection_options_from_fixture(&connection_options, &fixture);
    struct aws_byte_cursor good_host_name = connection_options.host_name;
    connection_options.host_name = aws_byte_cursor_from_string(s_bad_host_name);

    connection_options.on_connection_complete = s_aws_mqtt3_to_mqtt5_adapter_test_fixture_record_connection_complete;
    connection_options.user_data = &fixture;

    aws_mqtt_client_connection_connect(adapter, &connection_options);

    // wait for and verify a connection failure
    s_wait_for_n_adapter_lifecycle_events(&fixture, AWS_MQTT3_LET_CONNECTION_COMPLETE, 1);

    struct aws_mqtt3_lifecycle_event expected_events[] = {
        {
            .type = AWS_MQTT3_LET_CONNECTION_COMPLETE,
            .error_code = AWS_ERROR_FILE_INVALID_PATH,
            .skip_error_code_equality = true, /* the error code here is platform-dependent */
        },
    };
    ASSERT_SUCCESS(s_aws_mqtt3_to_mqtt5_adapter_test_fixture_verify_lifecycle_sequence(
        &fixture, AWS_ARRAY_SIZE(expected_events), expected_events, AWS_ARRAY_SIZE(expected_events)));

    // reconnect with a good host the adapter, wait for and verify success
    connection_options.host_name = good_host_name;
    aws_mqtt_client_connection_connect(adapter, &connection_options);
    s_wait_for_n_adapter_lifecycle_events(&fixture, AWS_MQTT3_LET_CONNECTION_COMPLETE, 2);

    struct aws_mqtt3_lifecycle_event expected_reconnect_events[] = {
        {
            .type = AWS_MQTT3_LET_CONNECTION_COMPLETE,
            .error_code = AWS_ERROR_FILE_INVALID_PATH,
            .skip_error_code_equality = true, /* the error code here is platform-dependent */
        },
        {
            .type = AWS_MQTT3_LET_CONNECTION_COMPLETE,
        },
    };
    ASSERT_SUCCESS(s_aws_mqtt3_to_mqtt5_adapter_test_fixture_verify_lifecycle_sequence(
        &fixture,
        AWS_ARRAY_SIZE(expected_reconnect_events),
        expected_reconnect_events,
        AWS_ARRAY_SIZE(expected_reconnect_events)));

    aws_mqtt3_to_mqtt5_adapter_test_fixture_clean_up(&fixture);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(
    mqtt3to5_adapter_connect_failure_bad_config_success_good_config,
    s_mqtt3to5_adapter_connect_failure_bad_config_success_good_config_fn)

/*
 * Connect successfully then disconnect followed by a connect with no intervening wait.  Verifies simple reliable
 * action and event sequencing.
 */
static int s_mqtt3to5_adapter_connect_success_disconnect_connect_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct mqtt5_client_test_options test_options;
    aws_mqtt5_client_test_init_default_options(&test_options);

    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options test_fixture_options = {
        .client_options = &test_options.client_options,
        .server_function_table = &test_options.server_function_table,
    };

    struct aws_mqtt3_to_mqtt5_adapter_test_fixture fixture;
    ASSERT_SUCCESS(aws_mqtt3_to_mqtt5_adapter_test_fixture_init(&fixture, allocator, &test_fixture_options, NULL));

    struct aws_mqtt_client_connection *adapter = fixture.connection;

    struct aws_mqtt_connection_options connection_options;
    s_init_adapter_connection_options_from_fixture(&connection_options, &fixture);

    connection_options.on_connection_complete = s_aws_mqtt3_to_mqtt5_adapter_test_fixture_record_connection_complete;
    connection_options.user_data = &fixture;

    aws_mqtt_client_connection_connect(adapter, &connection_options);

    s_wait_for_n_adapter_lifecycle_events(&fixture, AWS_MQTT3_LET_CONNECTION_COMPLETE, 1);

    aws_mqtt_client_connection_disconnect(
        adapter, s_aws_mqtt3_to_mqtt5_adapter_test_fixture_record_disconnection_complete, &fixture);

    aws_mqtt_client_connection_connect(adapter, &connection_options);

    s_wait_for_n_adapter_lifecycle_events(&fixture, AWS_MQTT3_LET_DISCONNECTION_COMPLETE, 1);
    s_wait_for_n_adapter_lifecycle_events(&fixture, AWS_MQTT3_LET_CONNECTION_COMPLETE, 2);

    /*
     * depending on timing there may or may not be a closed event in between, so just check beginning and end for
     * expected events
     */

    struct aws_mqtt3_lifecycle_event expected_sequence_beginning[] = {
        {
            .type = AWS_MQTT3_LET_CONNECTION_COMPLETE,
        },
        {
            .type = AWS_MQTT3_LET_DISCONNECTION_COMPLETE,
        },
    };

    ASSERT_SUCCESS(s_aws_mqtt3_to_mqtt5_adapter_test_fixture_verify_lifecycle_sequence_starts_with(
        &fixture, AWS_ARRAY_SIZE(expected_sequence_beginning), expected_sequence_beginning));

    struct aws_mqtt3_lifecycle_event expected_sequence_ending[] = {
        {
            .type = AWS_MQTT3_LET_CONNECTION_COMPLETE,
        },
    };

    ASSERT_SUCCESS(s_aws_mqtt3_to_mqtt5_adapter_test_fixture_verify_lifecycle_sequence_ends_with(
        &fixture, AWS_ARRAY_SIZE(expected_sequence_ending), expected_sequence_ending));

    aws_mqtt3_to_mqtt5_adapter_test_fixture_clean_up(&fixture);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(
    mqtt3to5_adapter_connect_success_disconnect_connect,
    s_mqtt3to5_adapter_connect_success_disconnect_connect_fn)

/*
 * Calls disconnect() on an adapter that successfully connected but then had the mqtt5 client stopped behind the
 * adapter's back.  Verifies that we still get a completion callback.
 */
static int s_mqtt3to5_adapter_connect_success_stop_mqtt5_disconnect_success_fn(
    struct aws_allocator *allocator,
    void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct mqtt5_client_test_options test_options;
    aws_mqtt5_client_test_init_default_options(&test_options);

    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options test_fixture_options = {
        .client_options = &test_options.client_options,
        .server_function_table = &test_options.server_function_table,
    };

    struct aws_mqtt3_to_mqtt5_adapter_test_fixture fixture;
    ASSERT_SUCCESS(aws_mqtt3_to_mqtt5_adapter_test_fixture_init(&fixture, allocator, &test_fixture_options, NULL));

    struct aws_mqtt_client_connection *adapter = fixture.connection;

    struct aws_mqtt_connection_options connection_options;
    s_init_adapter_connection_options_from_fixture(&connection_options, &fixture);

    connection_options.on_connection_complete = s_aws_mqtt3_to_mqtt5_adapter_test_fixture_record_connection_complete;
    connection_options.user_data = &fixture;

    aws_mqtt_client_connection_connect(adapter, &connection_options);

    s_wait_for_n_adapter_lifecycle_events(&fixture, AWS_MQTT3_LET_CONNECTION_COMPLETE, 1);

    aws_mqtt5_client_stop(fixture.mqtt5_fixture.client, NULL, NULL);

    aws_wait_for_stopped_lifecycle_event(&fixture.mqtt5_fixture);

    aws_mqtt_client_connection_disconnect(
        adapter, s_aws_mqtt3_to_mqtt5_adapter_test_fixture_record_disconnection_complete, &fixture);

    s_wait_for_n_adapter_lifecycle_events(&fixture, AWS_MQTT3_LET_DISCONNECTION_COMPLETE, 1);

    aws_mqtt3_to_mqtt5_adapter_test_fixture_clean_up(&fixture);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(
    mqtt3to5_adapter_connect_success_stop_mqtt5_disconnect_success,
    s_mqtt3to5_adapter_connect_success_stop_mqtt5_disconnect_success_fn)

/*
 * Call disconnect on a newly-created adapter.  Verifies that we get a completion callback.
 */
static int s_mqtt3to5_adapter_disconnect_success_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct mqtt5_client_test_options test_options;
    aws_mqtt5_client_test_init_default_options(&test_options);

    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options test_fixture_options = {
        .client_options = &test_options.client_options,
        .server_function_table = &test_options.server_function_table,
    };

    struct aws_mqtt3_to_mqtt5_adapter_test_fixture fixture;
    ASSERT_SUCCESS(aws_mqtt3_to_mqtt5_adapter_test_fixture_init(&fixture, allocator, &test_fixture_options, NULL));

    struct aws_mqtt_client_connection *adapter = fixture.connection;

    aws_mqtt_client_connection_disconnect(
        adapter, s_aws_mqtt3_to_mqtt5_adapter_test_fixture_record_disconnection_complete, &fixture);

    s_wait_for_n_adapter_lifecycle_events(&fixture, AWS_MQTT3_LET_DISCONNECTION_COMPLETE, 1);

    struct aws_mqtt3_lifecycle_event expected_events[] = {
        {
            .type = AWS_MQTT3_LET_DISCONNECTION_COMPLETE,
        },
    };

    ASSERT_SUCCESS(s_aws_mqtt3_to_mqtt5_adapter_test_fixture_verify_lifecycle_sequence(
        &fixture, AWS_ARRAY_SIZE(expected_events), expected_events, AWS_ARRAY_SIZE(expected_events)));

    aws_mqtt3_to_mqtt5_adapter_test_fixture_clean_up(&fixture);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt3to5_adapter_disconnect_success, s_mqtt3to5_adapter_disconnect_success_fn)

/*
 * Use the adapter to successfully connect then call disconnect multiple times.  Verify that all disconnect
 * invocations generate expected lifecycle events.  Verifies that disconnects after a disconnect are properly handled.
 */
static int s_mqtt3to5_adapter_connect_success_disconnect_success_disconnect_success_fn(
    struct aws_allocator *allocator,
    void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct mqtt5_client_test_options test_options;
    aws_mqtt5_client_test_init_default_options(&test_options);

    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options test_fixture_options = {
        .client_options = &test_options.client_options,
        .server_function_table = &test_options.server_function_table,
    };

    struct aws_mqtt3_to_mqtt5_adapter_test_fixture fixture;
    ASSERT_SUCCESS(aws_mqtt3_to_mqtt5_adapter_test_fixture_init(&fixture, allocator, &test_fixture_options, NULL));

    struct aws_mqtt_client_connection *adapter = fixture.connection;

    struct aws_mqtt_connection_options connection_options;
    s_init_adapter_connection_options_from_fixture(&connection_options, &fixture);

    connection_options.on_connection_complete = s_aws_mqtt3_to_mqtt5_adapter_test_fixture_record_connection_complete;
    connection_options.user_data = &fixture;

    aws_mqtt_client_connection_connect(adapter, &connection_options);

    s_wait_for_n_adapter_lifecycle_events(&fixture, AWS_MQTT3_LET_CONNECTION_COMPLETE, 1);

    aws_mqtt5_client_stop(fixture.mqtt5_fixture.client, NULL, NULL);

    aws_wait_for_stopped_lifecycle_event(&fixture.mqtt5_fixture);

    aws_mqtt_client_connection_disconnect(
        adapter, s_aws_mqtt3_to_mqtt5_adapter_test_fixture_record_disconnection_complete, &fixture);

    s_wait_for_n_adapter_lifecycle_events(&fixture, AWS_MQTT3_LET_DISCONNECTION_COMPLETE, 1);

    aws_mqtt_client_connection_disconnect(
        adapter, s_aws_mqtt3_to_mqtt5_adapter_test_fixture_record_disconnection_complete, &fixture);

    aws_mqtt_client_connection_disconnect(
        adapter, s_aws_mqtt3_to_mqtt5_adapter_test_fixture_record_disconnection_complete, &fixture);

    s_wait_for_n_adapter_lifecycle_events(&fixture, AWS_MQTT3_LET_DISCONNECTION_COMPLETE, 3);
    s_wait_for_n_adapter_lifecycle_events(&fixture, AWS_MQTT3_LET_CLOSED, 1);

    aws_mqtt3_to_mqtt5_adapter_test_fixture_clean_up(&fixture);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(
    mqtt3to5_adapter_connect_success_disconnect_success_disconnect_success,
    s_mqtt3to5_adapter_connect_success_disconnect_success_disconnect_success_fn)
