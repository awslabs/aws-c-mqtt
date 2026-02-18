/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "mqtt5_testing_utils.h"

#include <aws/common/clock.h>
#include <aws/common/string.h>
#include <aws/mqtt/mqtt.h>
#include <aws/mqtt/private/v5/mqtt5_utils.h>
#include <aws/mqtt/v5/mqtt5_client.h>
#include <aws/mqtt/v5/mqtt5_listener.h>

#include <aws/testing/aws_test_harness.h>

/**
 * Test context for manual PUBACK tests
 */
struct aws_mqtt5_manual_puback_test_context {
    struct aws_mqtt5_client_mock_test_fixture *test_fixture;

    /* Control IDs for manual puback */
    uint64_t puback_control_ids[10];
    size_t puback_control_count;

    /* Results from manual puback operations */
    enum aws_mqtt5_manual_puback_result puback_results[10];
    size_t puback_result_count;

    /* Tracking flags */
    bool publish_received;
    size_t publishes_received_count;
    bool puback_callback_invoked;
    size_t puback_callbacks_invoked_count;

    /* For tracking publish packet IDs */
    uint16_t received_packet_ids[10];
    size_t received_packet_id_count;
};

static void s_manual_puback_test_context_init(struct aws_mqtt5_manual_puback_test_context *context) {
    AWS_ZERO_STRUCT(*context);
}

static void s_manual_puback_completion_fn(enum aws_mqtt5_manual_puback_result puback_result, void *complete_ctx) {

    struct aws_mqtt5_manual_puback_test_context *context = complete_ctx;
    struct aws_mqtt5_client_mock_test_fixture *test_fixture = context->test_fixture;

    aws_mutex_lock(&test_fixture->lock);

    if (context->puback_result_count < AWS_ARRAY_SIZE(context->puback_results)) {
        context->puback_results[context->puback_result_count++] = puback_result;
    }

    context->puback_callback_invoked = true;
    context->puback_callbacks_invoked_count++;

    aws_mutex_unlock(&test_fixture->lock);
    aws_condition_variable_notify_all(&test_fixture->signal);
}

static bool s_manual_puback_publish_received_handler(
    const struct aws_mqtt5_packet_publish_view *publish,
    void *user_data) {

    struct aws_mqtt5_manual_puback_test_context *context = user_data;
    struct aws_mqtt5_client_mock_test_fixture *test_fixture = context->test_fixture;

    if (publish->qos == AWS_MQTT5_QOS_AT_LEAST_ONCE) {
        aws_mutex_lock(&test_fixture->lock);

        /* Acquire manual puback control */
        uint64_t control_id = aws_mqtt5_client_acquire_puback(test_fixture->client, publish);

        if (context->puback_control_count < AWS_ARRAY_SIZE(context->puback_control_ids)) {
            context->puback_control_ids[context->puback_control_count++] = control_id;
        }

        if (context->received_packet_id_count < AWS_ARRAY_SIZE(context->received_packet_ids)) {
            context->received_packet_ids[context->received_packet_id_count++] = publish->packet_id;
        }

        context->publish_received = true;
        context->publishes_received_count++;

        aws_mutex_unlock(&test_fixture->lock);
        aws_condition_variable_notify_all(&test_fixture->signal);

        return true; /* Signal we handled it manually */
    }

    return false;
}

static bool s_manual_puback_callback_invoked(void *arg) {
    struct aws_mqtt5_manual_puback_test_context *context = arg;
    return context->puback_callback_invoked;
}

static void s_wait_for_manual_puback_callback(struct aws_mqtt5_manual_puback_test_context *context) {
    struct aws_mqtt5_client_mock_test_fixture *test_fixture = context->test_fixture;

    aws_mutex_lock(&test_fixture->lock);
    aws_condition_variable_wait_pred(
        &test_fixture->signal, &test_fixture->lock, s_manual_puback_callback_invoked, context);
    aws_mutex_unlock(&test_fixture->lock);
}

static bool s_manual_puback_publish_received(void *arg) {
    struct aws_mqtt5_manual_puback_test_context *context = arg;
    return context->publish_received;
}

static void s_wait_for_manual_puback_publish(struct aws_mqtt5_manual_puback_test_context *context) {
    struct aws_mqtt5_client_mock_test_fixture *test_fixture = context->test_fixture;

    aws_mutex_lock(&test_fixture->lock);
    aws_condition_variable_wait_pred(
        &test_fixture->signal, &test_fixture->lock, s_manual_puback_publish_received, context);
    aws_mutex_unlock(&test_fixture->lock);
}

static void s_wait_for_n_manual_puback_publishes(struct aws_mqtt5_manual_puback_test_context *context, size_t count) {
    struct aws_mqtt5_client_mock_test_fixture *test_fixture = context->test_fixture;

    aws_mutex_lock(&test_fixture->lock);
    while (context->publishes_received_count < count) {
        aws_condition_variable_wait(&test_fixture->signal, &test_fixture->lock);
    }
    aws_mutex_unlock(&test_fixture->lock);
}

static void s_wait_for_n_manual_puback_callbacks(struct aws_mqtt5_manual_puback_test_context *context, size_t count) {
    struct aws_mqtt5_client_mock_test_fixture *test_fixture = context->test_fixture;

    aws_mutex_lock(&test_fixture->lock);
    while (context->puback_callbacks_invoked_count < count) {
        aws_condition_variable_wait(&test_fixture->signal, &test_fixture->lock);
    }
    aws_mutex_unlock(&test_fixture->lock);
}

/* Mock server handler that sends a QoS1 PUBLISH after CONNACK */
static uint8_t s_test_topic[] = "test/manual/puback";
static uint8_t s_test_payload[] = "test_payload";

struct aws_mqtt5_server_manual_puback_context {
    struct aws_mqtt5_client_mock_test_fixture *test_fixture;
    bool publish_sent;
    bool connack_sent;
    size_t publishes_to_send;
};

static void s_aws_mqtt5_mock_server_send_qos1_publish(
    struct aws_mqtt5_server_mock_connection_context *mock_server,
    void *user_data) {

    struct aws_mqtt5_server_manual_puback_context *context = user_data;

    if (context->publish_sent || !context->connack_sent) {
        return;
    }

    context->publish_sent = true;

    size_t count = context->publishes_to_send > 0 ? context->publishes_to_send : 1;

    for (size_t i = 0; i < count; i++) {
        struct aws_mqtt5_packet_publish_view publish_view = {
            .packet_id = (uint16_t)(i + 1),
            .qos = AWS_MQTT5_QOS_AT_LEAST_ONCE,
            .topic =
                {
                    .ptr = s_test_topic,
                    .len = AWS_ARRAY_SIZE(s_test_topic) - 1,
                },
            .payload =
                {
                    .ptr = s_test_payload,
                    .len = AWS_ARRAY_SIZE(s_test_payload) - 1,
                },
        };

        aws_mqtt5_mock_server_send_packet(mock_server, AWS_MQTT5_PT_PUBLISH, &publish_view);
    }
}

static int s_aws_mqtt5_server_send_qos1_publish_on_connect(
    void *packet,
    struct aws_mqtt5_server_mock_connection_context *connection,
    void *user_data) {

    int result = aws_mqtt5_mock_server_handle_connect_always_succeed(packet, connection, user_data);

    struct aws_mqtt5_server_manual_puback_context *context = user_data;
    context->connack_sent = true;

    return result;
}

/**
 * Test 1: Basic manual PUBACK success
 * - Server sends QoS1 PUBLISH
 * - Client acquires manual puback control
 * - Client invokes puback
 * - Verify success result and server receives PUBACK
 */
static int s_mqtt5_client_manual_puback_basic_success_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct mqtt5_client_test_options test_options;
    aws_mqtt5_client_test_init_default_options(&test_options);

    struct aws_mqtt5_manual_puback_test_context puback_context;
    s_manual_puback_test_context_init(&puback_context);

    struct aws_mqtt5_server_manual_puback_context server_context = {
        .publish_sent = false,
        .connack_sent = false,
        .publishes_to_send = 1,
    };

    test_options.server_function_table.service_task_fn = s_aws_mqtt5_mock_server_send_qos1_publish;
    test_options.server_function_table.packet_handlers[AWS_MQTT5_PT_CONNECT] =
        s_aws_mqtt5_server_send_qos1_publish_on_connect;

    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options test_fixture_options = {
        .client_options = &test_options.client_options,
        .server_function_table = &test_options.server_function_table,
        .mock_server_user_data = &server_context,
    };

    struct aws_mqtt5_client_mock_test_fixture test_context;
    ASSERT_SUCCESS(aws_mqtt5_client_mock_test_fixture_init(&test_context, allocator, &test_fixture_options));

    puback_context.test_fixture = &test_context;
    server_context.test_fixture = &test_context;

    /* Create listener to intercept publishes */
    struct aws_mqtt5_listener_config listener_config = {
        .client = test_context.client,
        .listener_callbacks = {
            .listener_publish_received_handler = s_manual_puback_publish_received_handler,
            .listener_publish_received_handler_user_data = &puback_context,
        }};

    struct aws_mqtt5_listener *listener = aws_mqtt5_listener_new(allocator, &listener_config);
    ASSERT_NOT_NULL(listener);

    /* Start client */
    ASSERT_SUCCESS(aws_mqtt5_client_start(test_context.client));
    aws_wait_for_connected_lifecycle_event(&test_context);

    /* Wait for publish to be received */
    s_wait_for_manual_puback_publish(&puback_context);

    /* Verify we got a control ID */
    aws_mutex_lock(&test_context.lock);
    ASSERT_TRUE(puback_context.puback_control_count == 1);
    ASSERT_TRUE(puback_context.puback_control_ids[0] != 0);
    uint64_t control_id = puback_context.puback_control_ids[0];
    aws_mutex_unlock(&test_context.lock);

    /* Invoke manual puback */
    struct aws_mqtt5_manual_puback_completion_options completion_options = {
        .completion_callback = s_manual_puback_completion_fn,
        .completion_user_data = &puback_context,
    };

    ASSERT_SUCCESS(aws_mqtt5_client_invoke_puback(test_context.client, control_id, &completion_options));

    /* Wait for completion callback */
    s_wait_for_manual_puback_callback(&puback_context);

    /* Verify success result */
    aws_mutex_lock(&test_context.lock);
    ASSERT_INT_EQUALS(1, puback_context.puback_result_count);
    ASSERT_INT_EQUALS(AWS_MQTT5_MPR_SUCCESS, puback_context.puback_results[0]);
    aws_mutex_unlock(&test_context.lock);

    /* Clean up */
    aws_mqtt5_listener_release(listener);
    ASSERT_SUCCESS(aws_mqtt5_client_stop(test_context.client, NULL, NULL));
    aws_wait_for_stopped_lifecycle_event(&test_context);

    aws_mqtt5_client_mock_test_fixture_clean_up(&test_context);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_client_manual_puback_basic_success, s_mqtt5_client_manual_puback_basic_success_fn)

/**
 * Test 2: Verify no automatic PUBACK when manual control taken
 * - Server sends QoS1 PUBLISH
 * - Client acquires manual puback control
 * - Wait to verify no automatic PUBACK sent
 * - Client invokes manual puback
 * - Verify PUBACK is now sent
 */
static int s_mqtt5_client_manual_puback_no_auto_puback_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct mqtt5_client_test_options test_options;
    aws_mqtt5_client_test_init_default_options(&test_options);

    struct aws_mqtt5_manual_puback_test_context puback_context;
    s_manual_puback_test_context_init(&puback_context);

    struct aws_mqtt5_server_manual_puback_context server_context = {
        .publish_sent = false,
        .connack_sent = false,
        .publishes_to_send = 1,
    };

    test_options.server_function_table.service_task_fn = s_aws_mqtt5_mock_server_send_qos1_publish;
    test_options.server_function_table.packet_handlers[AWS_MQTT5_PT_CONNECT] =
        s_aws_mqtt5_server_send_qos1_publish_on_connect;

    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options test_fixture_options = {
        .client_options = &test_options.client_options,
        .server_function_table = &test_options.server_function_table,
        .mock_server_user_data = &server_context,
    };

    struct aws_mqtt5_client_mock_test_fixture test_context;
    ASSERT_SUCCESS(aws_mqtt5_client_mock_test_fixture_init(&test_context, allocator, &test_fixture_options));

    puback_context.test_fixture = &test_context;
    server_context.test_fixture = &test_context;

    /* Create listener */
    struct aws_mqtt5_listener_config listener_config = {
        .client = test_context.client,
        .listener_callbacks = {
            .listener_publish_received_handler = s_manual_puback_publish_received_handler,
            .listener_publish_received_handler_user_data = &puback_context,
        }};

    struct aws_mqtt5_listener *listener = aws_mqtt5_listener_new(allocator, &listener_config);

    ASSERT_SUCCESS(aws_mqtt5_client_start(test_context.client));
    aws_wait_for_connected_lifecycle_event(&test_context);

    s_wait_for_manual_puback_publish(&puback_context);

    aws_mutex_lock(&test_context.lock);
    uint64_t control_id = puback_context.puback_control_ids[0];
    aws_mutex_unlock(&test_context.lock);

    /* Wait a bit to ensure no automatic PUBACK is sent */
    aws_thread_current_sleep(aws_timestamp_convert(1, AWS_TIMESTAMP_SECS, AWS_TIMESTAMP_NANOS, NULL));

    /* Verify no PUBACK received by server yet */
    aws_mutex_lock(&test_context.lock);
    size_t packet_count = aws_array_list_length(&test_context.server_received_packets);
    bool found_puback = false;
    for (size_t i = 0; i < packet_count; i++) {
        struct aws_mqtt5_mock_server_packet_record *record = NULL;
        aws_array_list_get_at_ptr(&test_context.server_received_packets, (void **)&record, i);
        if (record->packet_type == AWS_MQTT5_PT_PUBACK) {
            found_puback = true;
            break;
        }
    }
    ASSERT_FALSE(found_puback);
    aws_mutex_unlock(&test_context.lock);

    /* Now invoke manual puback */
    struct aws_mqtt5_manual_puback_completion_options completion_options = {
        .completion_callback = s_manual_puback_completion_fn,
        .completion_user_data = &puback_context,
    };

    ASSERT_SUCCESS(aws_mqtt5_client_invoke_puback(test_context.client, control_id, &completion_options));
    s_wait_for_manual_puback_callback(&puback_context);

    /* Verify PUBACK now received by server */
    aws_thread_current_sleep(aws_timestamp_convert(100, AWS_TIMESTAMP_MILLIS, AWS_TIMESTAMP_NANOS, NULL));

    aws_mutex_lock(&test_context.lock);
    packet_count = aws_array_list_length(&test_context.server_received_packets);
    found_puback = false;
    for (size_t i = 0; i < packet_count; i++) {
        struct aws_mqtt5_mock_server_packet_record *record = NULL;
        aws_array_list_get_at_ptr(&test_context.server_received_packets, (void **)&record, i);
        if (record->packet_type == AWS_MQTT5_PT_PUBACK) {
            found_puback = true;
            break;
        }
    }
    ASSERT_TRUE(found_puback);
    aws_mutex_unlock(&test_context.lock);

    aws_mqtt5_listener_release(listener);
    ASSERT_SUCCESS(aws_mqtt5_client_stop(test_context.client, NULL, NULL));
    aws_wait_for_stopped_lifecycle_event(&test_context);

    aws_mqtt5_client_mock_test_fixture_clean_up(&test_context);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_client_manual_puback_no_auto_puback, s_mqtt5_client_manual_puback_no_auto_puback_fn)

/**
 * Test 3: Invalid control ID
 * - Invoke puback with invalid control ID
 * - Verify AWS_MQTT5_MPR_PUBACK_INVALID result
 */
static int s_mqtt5_client_manual_puback_invalid_control_id_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct mqtt5_client_test_options test_options;
    aws_mqtt5_client_test_init_default_options(&test_options);

    struct aws_mqtt5_manual_puback_test_context puback_context;
    s_manual_puback_test_context_init(&puback_context);

    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options test_fixture_options = {
        .client_options = &test_options.client_options,
        .server_function_table = &test_options.server_function_table,
    };

    struct aws_mqtt5_client_mock_test_fixture test_context;
    ASSERT_SUCCESS(aws_mqtt5_client_mock_test_fixture_init(&test_context, allocator, &test_fixture_options));

    puback_context.test_fixture = &test_context;

    ASSERT_SUCCESS(aws_mqtt5_client_start(test_context.client));
    aws_wait_for_connected_lifecycle_event(&test_context);

    /* Try to invoke puback with invalid control ID */
    struct aws_mqtt5_manual_puback_completion_options completion_options = {
        .completion_callback = s_manual_puback_completion_fn,
        .completion_user_data = &puback_context,
    };

    uint64_t invalid_control_id = 999999;
    ASSERT_SUCCESS(aws_mqtt5_client_invoke_puback(test_context.client, invalid_control_id, &completion_options));

    s_wait_for_manual_puback_callback(&puback_context);

    /* Verify invalid result */
    aws_mutex_lock(&test_context.lock);
    ASSERT_INT_EQUALS(1, puback_context.puback_result_count);
    ASSERT_INT_EQUALS(AWS_MQTT5_MPR_PUBACK_INVALID, puback_context.puback_results[0]);
    aws_mutex_unlock(&test_context.lock);

    ASSERT_SUCCESS(aws_mqtt5_client_stop(test_context.client, NULL, NULL));
    aws_wait_for_stopped_lifecycle_event(&test_context);

    aws_mqtt5_client_mock_test_fixture_clean_up(&test_context);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_client_manual_puback_invalid_control_id, s_mqtt5_client_manual_puback_invalid_control_id_fn)

/**
 * Test 4: Multiple PUBLISHes with manual PUBACK
 * - Server sends 3 QoS1 PUBLISHes
 * - Client acquires control for all 3
 * - Client invokes puback for them (in reverse order)
 * - Verify all succeed
 */
static int s_mqtt5_client_manual_puback_multiple_publishes_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct mqtt5_client_test_options test_options;
    aws_mqtt5_client_test_init_default_options(&test_options);

    struct aws_mqtt5_manual_puback_test_context puback_context;
    s_manual_puback_test_context_init(&puback_context);

    struct aws_mqtt5_server_manual_puback_context server_context = {
        .publish_sent = false,
        .connack_sent = false,
        .publishes_to_send = 3,
    };

    test_options.server_function_table.service_task_fn = s_aws_mqtt5_mock_server_send_qos1_publish;
    test_options.server_function_table.packet_handlers[AWS_MQTT5_PT_CONNECT] =
        s_aws_mqtt5_server_send_qos1_publish_on_connect;

    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options test_fixture_options = {
        .client_options = &test_options.client_options,
        .server_function_table = &test_options.server_function_table,
        .mock_server_user_data = &server_context,
    };

    struct aws_mqtt5_client_mock_test_fixture test_context;
    ASSERT_SUCCESS(aws_mqtt5_client_mock_test_fixture_init(&test_context, allocator, &test_fixture_options));

    puback_context.test_fixture = &test_context;
    server_context.test_fixture = &test_context;

    struct aws_mqtt5_listener_config listener_config = {
        .client = test_context.client,
        .listener_callbacks = {
            .listener_publish_received_handler = s_manual_puback_publish_received_handler,
            .listener_publish_received_handler_user_data = &puback_context,
        }};

    struct aws_mqtt5_listener *listener = aws_mqtt5_listener_new(allocator, &listener_config);

    ASSERT_SUCCESS(aws_mqtt5_client_start(test_context.client));
    aws_wait_for_connected_lifecycle_event(&test_context);

    /* Wait for all 3 publishes */
    s_wait_for_n_manual_puback_publishes(&puback_context, 3);

    /* Verify we got 3 control IDs */
    aws_mutex_lock(&test_context.lock);
    ASSERT_INT_EQUALS(3, puback_context.puback_control_count);
    aws_mutex_unlock(&test_context.lock);

    /* Invoke pubacks in reverse order */
    struct aws_mqtt5_manual_puback_completion_options completion_options = {
        .completion_callback = s_manual_puback_completion_fn,
        .completion_user_data = &puback_context,
    };

    for (int i = 2; i >= 0; i--) {
        ASSERT_SUCCESS(aws_mqtt5_client_invoke_puback(
            test_context.client, puback_context.puback_control_ids[i], &completion_options));
    }

    /* Wait for all callbacks */
    s_wait_for_n_manual_puback_callbacks(&puback_context, 3);

    /* Verify all succeeded */
    aws_mutex_lock(&test_context.lock);
    ASSERT_INT_EQUALS(3, puback_context.puback_result_count);
    for (size_t i = 0; i < 3; i++) {
        ASSERT_INT_EQUALS(AWS_MQTT5_MPR_SUCCESS, puback_context.puback_results[i]);
    }
    aws_mutex_unlock(&test_context.lock);

    aws_mqtt5_listener_release(listener);
    ASSERT_SUCCESS(aws_mqtt5_client_stop(test_context.client, NULL, NULL));
    aws_wait_for_stopped_lifecycle_event(&test_context);

    aws_mqtt5_client_mock_test_fixture_clean_up(&test_context);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_client_manual_puback_multiple_publishes, s_mqtt5_client_manual_puback_multiple_publishes_fn)

/**
 * Test 5: Disconnect cancels pending manual PUBACKs
 * - Server sends QoS1 PUBLISH
 * - Client acquires manual puback control
 * - Disconnect before invoking puback
 * - Invoke puback after disconnect
 * - Verify AWS_MQTT5_MPR_PUBACK_CANCELLED result
 */
static int s_mqtt5_client_manual_puback_disconnect_cancellation_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct mqtt5_client_test_options test_options;
    aws_mqtt5_client_test_init_default_options(&test_options);

    struct aws_mqtt5_manual_puback_test_context puback_context;
    s_manual_puback_test_context_init(&puback_context);

    struct aws_mqtt5_server_manual_puback_context server_context = {
        .publish_sent = false,
        .connack_sent = false,
        .publishes_to_send = 1,
    };

    test_options.server_function_table.service_task_fn = s_aws_mqtt5_mock_server_send_qos1_publish;
    test_options.server_function_table.packet_handlers[AWS_MQTT5_PT_CONNECT] =
        s_aws_mqtt5_server_send_qos1_publish_on_connect;

    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options test_fixture_options = {
        .client_options = &test_options.client_options,
        .server_function_table = &test_options.server_function_table,
        .mock_server_user_data = &server_context,
    };

    struct aws_mqtt5_client_mock_test_fixture test_context;
    ASSERT_SUCCESS(aws_mqtt5_client_mock_test_fixture_init(&test_context, allocator, &test_fixture_options));

    puback_context.test_fixture = &test_context;
    server_context.test_fixture = &test_context;

    struct aws_mqtt5_listener_config listener_config = {
        .client = test_context.client,
        .listener_callbacks = {
            .listener_publish_received_handler = s_manual_puback_publish_received_handler,
            .listener_publish_received_handler_user_data = &puback_context,
        }};

    struct aws_mqtt5_listener *listener = aws_mqtt5_listener_new(allocator, &listener_config);

    ASSERT_SUCCESS(aws_mqtt5_client_start(test_context.client));
    aws_wait_for_connected_lifecycle_event(&test_context);

    s_wait_for_manual_puback_publish(&puback_context);

    aws_mutex_lock(&test_context.lock);
    uint64_t control_id = puback_context.puback_control_ids[0];
    aws_mutex_unlock(&test_context.lock);

    /* Disconnect before invoking puback */
    ASSERT_SUCCESS(aws_mqtt5_client_stop(test_context.client, NULL, NULL));
    aws_wait_for_stopped_lifecycle_event(&test_context);

    /* Now try to invoke puback after disconnect */
    struct aws_mqtt5_manual_puback_completion_options completion_options = {
        .completion_callback = s_manual_puback_completion_fn,
        .completion_user_data = &puback_context,
    };

    ASSERT_SUCCESS(aws_mqtt5_client_invoke_puback(test_context.client, control_id, &completion_options));
    s_wait_for_manual_puback_callback(&puback_context);

    /* Verify cancelled result */
    aws_mutex_lock(&test_context.lock);
    ASSERT_INT_EQUALS(1, puback_context.puback_result_count);
    ASSERT_INT_EQUALS(AWS_MQTT5_MPR_PUBACK_CANCELLED, puback_context.puback_results[0]);
    aws_mutex_unlock(&test_context.lock);

    aws_mqtt5_listener_release(listener);
    aws_mqtt5_client_mock_test_fixture_clean_up(&test_context);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(
    mqtt5_client_manual_puback_disconnect_cancellation,
    s_mqtt5_client_manual_puback_disconnect_cancellation_fn)