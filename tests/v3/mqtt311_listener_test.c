/**
* Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
* SPDX-License-Identifier: Apache-2.0.
*/

#include <aws/mqtt/mqtt.h>

#include <aws/mqtt/private/mqtt311_listener.h>

#include "mqtt311_testing_utils.h"
#include "mqtt_mock_server_handler.h"

#include <aws/testing/aws_test_harness.h>

struct mqtt311_listener_resumption_record {
    bool rejoined_session;
};

struct mqtt311_listener_publish_record {
    struct aws_byte_buf topic;
    struct aws_byte_buf payload;
};

struct mqtt311_listener_test_context {
    struct aws_allocator *allocator;

    struct aws_mqtt311_listener *listener;

    struct mqtt_connection_state_test *mqtt311_test_context;
    int mqtt311_test_context_setup_result;

    struct aws_array_list resumption_events;
    struct aws_array_list publish_events;
    bool terminated;

    struct aws_mutex lock;
    struct aws_condition_variable signal;
};

static void s_311_listener_test_on_publish_received(
    struct aws_mqtt_client_connection *connection,
    const struct aws_byte_cursor *topic,
    const struct aws_byte_cursor *payload,
    bool dup,
    enum aws_mqtt_qos qos,
    bool retain,
    void *userdata) {

}

static void s_311_listener_test_on_connection_resumed(
    struct aws_mqtt_client_connection *connection,
    enum aws_mqtt_connect_return_code return_code,
    bool session_present,
    void *userdata) {

}

static void s_311_listener_test_on_termination(void *complete_ctx) {

}

static int mqtt311_listener_test_context_init(struct mqtt311_listener_test_context *context, struct aws_allocator *allocator, struct mqtt_connection_state_test *mqtt311_test_context) {
    AWS_ZERO_STRUCT(*context);

    context->allocator = allocator;
    context->mqtt311_test_context = mqtt311_test_context;

    aws_array_list_init_dynamic(&context->resumption_events, allocator, 10, sizeof(struct mqtt311_listener_resumption_record));
    aws_array_list_init_dynamic(&context->publish_events, allocator, 10, sizeof(struct mqtt311_listener_publish_record));

    aws_mutex_init(&context->lock);
    aws_condition_variable_init(&context->signal);

    context->mqtt311_test_context_setup_result = aws_test311_setup_mqtt_server_fn(allocator, &mqtt311_test_context);
    ASSERT_SUCCESS(context->mqtt311_test_context_setup_result);

    struct aws_mqtt311_listener_config listener_config = {
        .connection = mqtt311_test_context->mqtt_connection,
        .listener_callbacks = {
            .publish_received_handler = s_311_listener_test_on_publish_received,
            .connection_resumed_handler = s_311_listener_test_on_connection_resumed,
            .user_data = context
        },
        .termination_callback = s_311_listener_test_on_termination,
        .termination_callback_user_data = context,
    };

    context->listener = aws_mqtt311_listener_new(allocator, &listener_config);

    return AWS_OP_SUCCESS;
}

static void s_wait_for_listener_termination_callback(struct mqtt311_listener_test_context *context) {

}

static void mqtt311_listener_test_context_clean_up(struct mqtt311_listener_test_context *context) {

    aws_mqtt311_listener_release(context->listener);
    s_wait_for_listener_termination_callback(context);

    aws_test311_clean_up_mqtt_server_fn(context->allocator, context->mqtt311_test_context_setup_result, context->mqtt311_test_context);

    aws_mutex_clean_up(&context->lock);
    aws_condition_variable_clean_up(&context->signal);

    aws_array_list_clean_up(&context->resumption_events);

    for (size_t i = 0; i < aws_array_list_length(&context->publish_events); ++i) {
        struct mqtt311_listener_publish_record publish_record;
        AWS_ZERO_STRUCT(publish_record);

        aws_array_list_get_at(&context->publish_events, &publish_record, i);

        aws_byte_buf_clean_up(&publish_record.topic);
        aws_byte_buf_clean_up(&publish_record.payload);
    }

    aws_array_list_clean_up(&context->publish_events);
}

static int s_mqtt311_listener_session_events_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct mqtt_connection_state_test mqtt311_context;
    AWS_ZERO_STRUCT(mqtt311_context);

    struct mqtt311_listener_test_context test_context;
    ASSERT_SUCCESS(mqtt311_listener_test_context_init(&test_context, allocator, &mqtt311_context));

    struct aws_mqtt_connection_options connection_options = {
        .user_data = &mqtt311_context,
        .clean_session = false,
        .client_id = aws_byte_cursor_from_c_str("client1234"),
        .host_name = aws_byte_cursor_from_c_str(mqtt311_context.endpoint.address),
        .socket_options = &mqtt311_context.socket_options,
        .on_connection_complete = aws_test311_on_connection_complete_fn,
    };

    ASSERT_SUCCESS(aws_mqtt_client_connection_connect(mqtt311_context.mqtt_connection, &connection_options));
    aws_test311_wait_for_connection_to_complete(&mqtt311_context);

    ASSERT_SUCCESS(aws_mqtt_client_connection_disconnect(
        mqtt311_context.mqtt_connection, aws_test311_on_disconnect_fn, &mqtt311_context));
    aws_test311_wait_for_disconnect_to_complete(&mqtt311_context);

    mqtt311_listener_test_context_clean_up(&test_context);

    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt311_listener_session_events, s_mqtt311_listener_session_events_fn)