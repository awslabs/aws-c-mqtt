/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/private/request-response/protocol_adapter.h>
#include <aws/mqtt/request-response/request_response_client.h>

#include <aws/testing/aws_test_harness.h>

#include "../v3/mqtt311_testing_utils.h"
#include "../v5/mqtt5_testing_utils.h"

enum rr_test_client_protocol {
    RRCP_MQTT311,
    RRCP_MQTT5,
};

struct aws_rr_client_test_fixture {
    struct aws_allocator *allocator;

    struct aws_mqtt_request_response_client *rr_client;

    enum rr_test_client_protocol test_protocol;
    union {
        struct aws_mqtt5_client_mock_test_fixture mqtt5_test_fixture;
        struct mqtt_connection_state_test mqtt311_test_fixture;
    } client_test_fixture;

    void *test_context;

    struct aws_mutex lock;
    struct aws_condition_variable signal;

    bool client_initialized;
    bool client_destroyed;
};

static void s_aws_rr_client_test_fixture_on_initialized(void *user_data) {
    struct aws_rr_client_test_fixture *fixture = user_data;

    aws_mutex_lock(&fixture->lock);
    fixture->client_initialized = true;
    aws_mutex_unlock(&fixture->lock);
    aws_condition_variable_notify_all(&fixture->signal);
}

static bool s_rr_client_test_fixture_initialized(void *context) {
    struct aws_rr_client_test_fixture *fixture = context;

    return fixture->client_initialized;
}

static void s_aws_rr_client_test_fixture_wait_for_initialized(struct aws_rr_client_test_fixture *fixture) {
    aws_mutex_lock(&fixture->lock);
    aws_condition_variable_wait_pred(&fixture->signal, &fixture->lock, s_rr_client_test_fixture_initialized, fixture);
    aws_mutex_unlock(&fixture->lock);
}

static void s_aws_rr_client_test_fixture_on_terminated(void *user_data) {
    struct aws_rr_client_test_fixture *fixture = user_data;

    aws_mutex_lock(&fixture->lock);
    fixture->client_destroyed = true;
    aws_mutex_unlock(&fixture->lock);
    aws_condition_variable_notify_all(&fixture->signal);
}

static int s_aws_rr_client_test_fixture_init_from_mqtt5(
    struct aws_rr_client_test_fixture *fixture,
    struct aws_allocator *allocator,
    struct aws_mqtt_request_response_client_options *rr_client_options,
    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options *client_test_fixture_options,
    void *test_context) {
    AWS_ZERO_STRUCT(*fixture);
    fixture->allocator = allocator;
    fixture->test_protocol = RRCP_MQTT5;

    aws_mutex_init(&fixture->lock);
    aws_condition_variable_init(&fixture->signal);
    fixture->test_context = test_context;

    if (aws_mqtt5_client_mock_test_fixture_init(
            &fixture->client_test_fixture.mqtt5_test_fixture, allocator, client_test_fixture_options)) {
        return AWS_OP_ERR;
    }

    struct aws_mqtt_request_response_client_options client_options = {
        .max_subscriptions = 3,
        .operation_timeout_seconds = 5,
    };

    if (rr_client_options != NULL) {
        client_options = *rr_client_options;
    }

    client_options.initialized_callback = s_aws_rr_client_test_fixture_on_initialized;
    client_options.terminated_callback = s_aws_rr_client_test_fixture_on_terminated;
    client_options.user_data = fixture;

    fixture->rr_client = aws_mqtt_request_response_client_new_from_mqtt5_client(
        allocator, fixture->client_test_fixture.mqtt5_test_fixture.client, &client_options);
    AWS_FATAL_ASSERT(fixture->rr_client != NULL);

    aws_mqtt5_client_start(fixture->client_test_fixture.mqtt5_test_fixture.client);

    aws_wait_for_connected_lifecycle_event(&fixture->client_test_fixture.mqtt5_test_fixture);
    s_aws_rr_client_test_fixture_wait_for_initialized(fixture);

    return AWS_OP_SUCCESS;
}

static int s_aws_rr_client_test_fixture_init_from_mqtt311(
    struct aws_rr_client_test_fixture *fixture,
    struct aws_allocator *allocator,
    struct aws_mqtt_request_response_client_options *rr_client_options,
    void *test_context) {
    AWS_ZERO_STRUCT(*fixture);
    fixture->allocator = allocator;
    fixture->test_protocol = RRCP_MQTT311;

    aws_mutex_init(&fixture->lock);
    aws_condition_variable_init(&fixture->signal);
    fixture->test_context = test_context;

    aws_test311_setup_mqtt_server_fn(allocator, &fixture->client_test_fixture.mqtt311_test_fixture);

    struct aws_mqtt_request_response_client_options client_options = {
        .max_subscriptions = 3,
        .operation_timeout_seconds = 5,
    };

    if (rr_client_options != NULL) {
        client_options = *rr_client_options;
    }

    client_options.initialized_callback = s_aws_rr_client_test_fixture_on_initialized;
    client_options.terminated_callback = s_aws_rr_client_test_fixture_on_terminated;
    client_options.user_data = fixture;

    struct aws_mqtt_client_connection *mqtt_client = fixture->client_test_fixture.mqtt311_test_fixture.mqtt_connection;

    fixture->rr_client =
        aws_mqtt_request_response_client_new_from_mqtt311_client(allocator, mqtt_client, &client_options);
    AWS_FATAL_ASSERT(fixture->rr_client != NULL);

    struct aws_mqtt_connection_options connection_options = {
        .user_data = &fixture->client_test_fixture.mqtt311_test_fixture,
        .clean_session = false,
        .client_id = aws_byte_cursor_from_c_str("client1234"),
        .host_name = aws_byte_cursor_from_c_str(fixture->client_test_fixture.mqtt311_test_fixture.endpoint.address),
        .socket_options = &fixture->client_test_fixture.mqtt311_test_fixture.socket_options,
        .on_connection_complete = aws_test311_on_connection_complete_fn,
        .ping_timeout_ms = DEFAULT_TEST_PING_TIMEOUT_MS,
        .keep_alive_time_secs = 16960,
    };

    ASSERT_SUCCESS(aws_mqtt_client_connection_connect(mqtt_client, &connection_options));
    aws_test311_wait_for_connection_to_complete(&fixture->client_test_fixture.mqtt311_test_fixture);

    s_aws_rr_client_test_fixture_wait_for_initialized(fixture);

    return AWS_OP_SUCCESS;
}

static bool s_rr_client_test_fixture_terminated(void *context) {
    struct aws_rr_client_test_fixture *fixture = context;

    return fixture->client_destroyed;
}

static void s_aws_rr_client_test_fixture_clean_up(struct aws_rr_client_test_fixture *fixture) {
    aws_mqtt_request_response_client_release(fixture->rr_client);

    aws_mutex_lock(&fixture->lock);
    aws_condition_variable_wait_pred(&fixture->signal, &fixture->lock, s_rr_client_test_fixture_terminated, fixture);
    aws_mutex_unlock(&fixture->lock);

    if (fixture->test_protocol == RRCP_MQTT5) {
        aws_mqtt5_client_mock_test_fixture_clean_up(&fixture->client_test_fixture.mqtt5_test_fixture);
    } else {
        struct mqtt_connection_state_test *mqtt311_test_fixture = &fixture->client_test_fixture.mqtt311_test_fixture;
        aws_mqtt_client_connection_disconnect(
            mqtt311_test_fixture->mqtt_connection, aws_test311_on_disconnect_fn, mqtt311_test_fixture);
        aws_test311_clean_up_mqtt_server_fn(
            fixture->allocator, AWS_OP_SUCCESS, &fixture->client_test_fixture.mqtt311_test_fixture);
    }

    aws_mutex_clean_up(&fixture->lock);
    aws_condition_variable_clean_up(&fixture->signal);
}

static int s_rrc_mqtt5_create_destroy_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct mqtt5_client_test_options client_test_options;
    aws_mqtt5_client_test_init_default_options(&client_test_options);

    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options client_test_fixture_options = {
        .client_options = &client_test_options.client_options,
        .server_function_table = &client_test_options.server_function_table,
    };

    struct aws_rr_client_test_fixture fixture;
    ASSERT_SUCCESS(
        s_aws_rr_client_test_fixture_init_from_mqtt5(&fixture, allocator, NULL, &client_test_fixture_options, NULL));

    s_aws_rr_client_test_fixture_clean_up(&fixture);

    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(rrc_mqtt5_create_destroy, s_rrc_mqtt5_create_destroy_fn)

static int s_rrc_mqtt311_create_destroy_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct aws_rr_client_test_fixture fixture;
    ASSERT_SUCCESS(s_aws_rr_client_test_fixture_init_from_mqtt311(&fixture, allocator, NULL, NULL));

    s_aws_rr_client_test_fixture_clean_up(&fixture);

    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(rrc_mqtt311_create_destroy, s_rrc_mqtt311_create_destroy_fn)