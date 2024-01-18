/**
* Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
* SPDX-License-Identifier: Apache-2.0.
*/

#include "v5/mqtt5_testing_utils.h"
#include <aws/common/common.h>

#include "aws/mqtt/private/request-response/protocol_adapter.h"

#include <aws/testing/aws_test_harness.h>

struct request_response_protocol_adapter_incoming_publish_event_record {
    struct aws_byte_buf topic;
    struct aws_byte_buf payload;
};

static void s_request_response_protocol_adapter_incoming_publish_event_record_init(
    struct request_response_protocol_adapter_incoming_publish_event_record *record,
    struct aws_allocator *allocator,
    struct aws_byte_cursor topic,
    struct aws_byte_cursor payload) {

    aws_byte_buf_init_copy_from_cursor(&record->topic, allocator, topic);
    aws_byte_buf_init_copy_from_cursor(&record->payload, allocator, payload);
}

static void s_request_response_protocol_adapter_incoming_publish_event_record_cleanup(struct request_response_protocol_adapter_incoming_publish_event_record *record) {
    aws_byte_buf_clean_up(&record->topic);
    aws_byte_buf_clean_up(&record->payload);
}

struct request_response_protocol_adapter_connection_event_record {
    enum aws_protocol_adapter_connection_event_type event_type;
    bool rejoined_session;
};

struct request_response_protocol_adapter_subscription_event_record {
    enum aws_protocol_adapter_subscription_event_type event_type;
    struct aws_byte_buf topic_filter;
};

static void s_request_response_protocol_adapter_subscription_event_record_init(
    struct request_response_protocol_adapter_subscription_event_record *record,
    struct aws_allocator *allocator,
    struct aws_byte_cursor topic_filter) {

    aws_byte_buf_init_copy_from_cursor(&record->topic_filter, allocator, topic_filter);
}

static void s_request_response_protocol_adapter_subscription_event_record_cleanup(struct request_response_protocol_adapter_subscription_event_record *record) {
    aws_byte_buf_clean_up(&record->topic_filter);
}

struct aws_request_response_mqtt5_adapter_test_fixture {
    struct aws_allocator *allocator;
    struct aws_mqtt5_client_mock_test_fixture mqtt5_fixture;

    struct aws_mqtt_protocol_adapter *protocol_adapter;

    struct aws_array_list incoming_publish_events;
    struct aws_array_list connection_events;
    struct aws_array_list subscription_events;

    bool adapter_terminated;

    struct aws_mutex lock;
    struct aws_condition_variable signal;
};


static void s_rr_mqtt5_protocol_adapter_test_on_subscription_event(struct aws_protocol_adapter_subscription_event *event, void *user_data) {
    struct aws_request_response_mqtt5_adapter_test_fixture *fixture = user_data;

    struct request_response_protocol_adapter_subscription_event_record record = {
        .event_type = event->event_type
    };
    s_request_response_protocol_adapter_subscription_event_record_init(&record, fixture->allocator, event->topic_filter);

    aws_mutex_lock(&fixture->lock);
    aws_array_list_push_back(&fixture->subscription_events, &record);
    aws_mutex_unlock(&fixture->lock);
    aws_condition_variable_notify_all(&fixture->signal);
}

static void s_rr_mqtt5_protocol_adapter_test_on_incoming_publish(struct aws_protocol_adapter_incoming_publish_event *publish, void *user_data) {
    struct aws_request_response_mqtt5_adapter_test_fixture *fixture = user_data;

    struct request_response_protocol_adapter_incoming_publish_event_record record;
    AWS_ZERO_STRUCT(record);
    s_request_response_protocol_adapter_incoming_publish_event_record_init(&record, fixture->allocator, publish->topic, publish->payload);

    aws_mutex_lock(&fixture->lock);
    aws_array_list_push_back(&fixture->incoming_publish_events, &record);
    aws_mutex_unlock(&fixture->lock);
    aws_condition_variable_notify_all(&fixture->signal);
}

static void s_rr_mqtt5_protocol_adapter_test_on_terminate_callback(void *user_data) {
    struct aws_request_response_mqtt5_adapter_test_fixture *fixture = user_data;

    aws_mutex_lock(&fixture->lock);
    fixture->adapter_terminated = true;
    aws_mutex_unlock(&fixture->lock);
    aws_condition_variable_notify_all(&fixture->signal);
}

static void s_rr_mqtt5_protocol_adapter_test_on_connection_event(struct aws_protocol_adapter_connection_event *event, void *user_data) {
    struct aws_request_response_mqtt5_adapter_test_fixture *fixture = user_data;

    struct request_response_protocol_adapter_connection_event_record record = {
        .event_type = event->event_type,
        .rejoined_session = event->rejoined_session
    };

    aws_mutex_lock(&fixture->lock);
    aws_array_list_push_back(&fixture->connection_events, &record);
    aws_mutex_unlock(&fixture->lock);
    aws_condition_variable_notify_all(&fixture->signal);
}

static int s_aws_request_response_mqtt5_adapter_test_fixture_init(
    struct aws_request_response_mqtt5_adapter_test_fixture *fixture,
    struct aws_allocator *allocator,
    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options *mqtt5_fixture_config) {

    AWS_ZERO_STRUCT(*fixture);

    fixture->allocator = allocator;

    if (aws_mqtt5_client_mock_test_fixture_init(&fixture->mqtt5_fixture, allocator, mqtt5_fixture_config)) {
        return AWS_OP_ERR;
    }

    struct aws_mqtt_protocol_adapter_options protocol_adapter_options = {
        .subscription_event_callback = s_rr_mqtt5_protocol_adapter_test_on_subscription_event,
        .incoming_publish_callback = s_rr_mqtt5_protocol_adapter_test_on_incoming_publish,
        .terminate_callback = s_rr_mqtt5_protocol_adapter_test_on_terminate_callback,
        .connection_event_callback = s_rr_mqtt5_protocol_adapter_test_on_connection_event,
        .user_data = fixture
    };

    fixture->protocol_adapter = aws_mqtt_protocol_adapter_new_from_5(allocator, &protocol_adapter_options, fixture->mqtt5_fixture.client);
    AWS_FATAL_ASSERT(fixture->protocol_adapter != NULL);

    aws_array_list_init_dynamic(&fixture->incoming_publish_events, allocator, 10, sizeof(struct request_response_protocol_adapter_incoming_publish_event_record));
    aws_array_list_init_dynamic(&fixture->connection_events, allocator, 10, sizeof(struct request_response_protocol_adapter_connection_event_record));
    aws_array_list_init_dynamic(&fixture->subscription_events, allocator, 10, sizeof(struct request_response_protocol_adapter_subscription_event_record));

    aws_mutex_init(&fixture->lock);
    aws_condition_variable_init(&fixture->signal);

    return AWS_OP_SUCCESS;
}

static bool s_is_adapter_terminated(void *context) {
    struct aws_request_response_mqtt5_adapter_test_fixture *fixture = context;

    return fixture->adapter_terminated;
}

static void s_aws_request_response_mqtt5_adapter_test_fixture_clean_up(struct aws_request_response_mqtt5_adapter_test_fixture *fixture) {
    aws_mqtt_protocol_adapter_delete(fixture->protocol_adapter);

    aws_mutex_lock(&fixture->lock);
    aws_condition_variable_wait_pred(&fixture->signal, &fixture->lock, s_is_adapter_terminated, fixture);
    aws_mutex_unlock(&fixture->lock);

    aws_mqtt5_client_mock_test_fixture_clean_up(&fixture->mqtt5_fixture);

    for (size_t i = 0; i < aws_array_list_length(&fixture->subscription_events); ++i) {
        struct request_response_protocol_adapter_subscription_event_record record;
        aws_array_list_get_at(&fixture->subscription_events, &record, i);
        s_request_response_protocol_adapter_subscription_event_record_cleanup(&record);
    }
    aws_array_list_clean_up(&fixture->subscription_events);

    for (size_t i = 0; i < aws_array_list_length(&fixture->incoming_publish_events); ++i) {
        struct request_response_protocol_adapter_incoming_publish_event_record record;
        aws_array_list_get_at(&fixture->incoming_publish_events, &record, i);
        s_request_response_protocol_adapter_incoming_publish_event_record_cleanup(&record);
    }
    aws_array_list_clean_up(&fixture->incoming_publish_events);

    aws_array_list_clean_up(&fixture->connection_events);

    aws_mutex_clean_up(&fixture->lock);
    aws_condition_variable_clean_up(&fixture->signal);
}

struct test_subscription_event_wait_context {
    struct request_response_protocol_adapter_subscription_event_record *expected_event;
    size_t expected_count;
    struct aws_request_response_mqtt5_adapter_test_fixture *fixture;
};

static bool s_do_subscription_events_contain(void *context) {
    struct test_subscription_event_wait_context *wait_context = context;

    size_t found = 0;

    size_t num_events = aws_array_list_length(&wait_context->fixture->subscription_events);
    for (size_t i = 0; i < num_events; ++i) {
        struct request_response_protocol_adapter_subscription_event_record record;
        aws_array_list_get_at(&wait_context->fixture->subscription_events, &record, i);

        if (record.event_type == wait_context->expected_event->event_type) {
            struct aws_byte_cursor record_topic_filter = aws_byte_cursor_from_buf(&record.topic_filter);
            struct aws_byte_cursor expected_topic_filter = aws_byte_cursor_from_buf(&wait_context->expected_event->topic_filter);
            if (aws_byte_cursor_eq(&record_topic_filter, &expected_topic_filter)) {
                ++found;
            }
        }
    }

    return found >= wait_context->expected_count;
}

static void s_wait_for_subscription_events_contains(struct aws_request_response_mqtt5_adapter_test_fixture *fixture,
                                                    struct request_response_protocol_adapter_subscription_event_record *expected_event,
                                                    size_t expected_count) {

    struct test_subscription_event_wait_context context = {
        .expected_event = expected_event,
        .expected_count = expected_count,
        .fixture = fixture,
    };

    aws_mutex_lock(&fixture->lock);
    aws_condition_variable_wait_pred(&fixture->signal, &fixture->lock, s_do_subscription_events_contain, &context);
    aws_mutex_unlock(&fixture->lock);
}

struct test_connection_event_wait_context {
    struct request_response_protocol_adapter_connection_event_record *expected_event;
    size_t expected_count;
    struct aws_request_response_mqtt5_adapter_test_fixture *fixture;
};

static bool s_do_connection_events_contain(void *context) {
    struct test_connection_event_wait_context *wait_context = context;

    size_t found = 0;

    size_t num_events = aws_array_list_length(&wait_context->fixture->connection_events);
    for (size_t i = 0; i < num_events; ++i) {
        struct request_response_protocol_adapter_connection_event_record record;
        aws_array_list_get_at(&wait_context->fixture->connection_events, &record, i);

        if (record.event_type == wait_context->expected_event->event_type && record.rejoined_session == wait_context->expected_event->rejoined_session) {
            ++found;
        }
    }

    return found >= wait_context->expected_count;
}

static void s_wait_for_connection_events_contains(struct aws_request_response_mqtt5_adapter_test_fixture *fixture,
                                                    struct request_response_protocol_adapter_connection_event_record *expected_event,
                                                    size_t expected_count) {

    struct test_connection_event_wait_context context = {
        .expected_event = expected_event,
        .expected_count = expected_count,
        .fixture = fixture,
    };

    aws_mutex_lock(&fixture->lock);
    aws_condition_variable_wait_pred(&fixture->signal, &fixture->lock, s_do_connection_events_contain, &context);
    aws_mutex_unlock(&fixture->lock);
}

struct test_incoming_publish_event_wait_context {
    struct request_response_protocol_adapter_incoming_publish_event_record *expected_event;
    size_t expected_count;
    struct aws_request_response_mqtt5_adapter_test_fixture *fixture;
};

static bool s_do_incoming_publish_events_contain(void *context) {
    struct test_incoming_publish_event_wait_context *wait_context = context;

    size_t found = 0;

    size_t num_events = aws_array_list_length(&wait_context->fixture->incoming_publish_events);
    for (size_t i = 0; i < num_events; ++i) {
        struct request_response_protocol_adapter_incoming_publish_event_record record;
        aws_array_list_get_at(&wait_context->fixture->incoming_publish_events, &record, i);

        struct aws_byte_cursor record_topic = aws_byte_cursor_from_buf(&record.topic);
        struct aws_byte_cursor expected_topic = aws_byte_cursor_from_buf(&wait_context->expected_event->topic);
        if (!aws_byte_cursor_eq(&record_topic, &expected_topic)) {
            continue;
        }

        struct aws_byte_cursor record_payload = aws_byte_cursor_from_buf(&record.payload);
        struct aws_byte_cursor expected_payload = aws_byte_cursor_from_buf(&wait_context->expected_event->payload);
        if (!aws_byte_cursor_eq(&record_payload, &expected_payload)) {
            continue;
        }

        ++found;
    }

    return found >= wait_context->expected_count;
}

static void s_wait_for_incoming_publish_events_contains(struct aws_request_response_mqtt5_adapter_test_fixture *fixture,
                                                  struct request_response_protocol_adapter_incoming_publish_event_record *expected_event,
                                                  size_t expected_count) {

    struct test_incoming_publish_event_wait_context context = {
        .expected_event = expected_event,
        .expected_count = expected_count,
        .fixture = fixture,
    };

    aws_mutex_lock(&fixture->lock);
    aws_condition_variable_wait_pred(&fixture->signal, &fixture->lock, s_do_incoming_publish_events_contain, &context);
    aws_mutex_unlock(&fixture->lock);
}

static int s_request_response_mqtt5_protocol_adapter_subscribe_success_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct mqtt5_client_test_options test_options;
    aws_mqtt5_client_test_init_default_options(&test_options);

    test_options.server_function_table.packet_handlers[AWS_MQTT5_PT_SUBSCRIBE] =
        aws_mqtt5_server_send_suback_on_subscribe;

    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options mqtt5_test_fixture_options = {
        .client_options = &test_options.client_options,
        .server_function_table = &test_options.server_function_table,
    };

    struct aws_request_response_mqtt5_adapter_test_fixture fixture;
    ASSERT_SUCCESS(s_aws_request_response_mqtt5_adapter_test_fixture_init(&fixture, allocator, &mqtt5_test_fixture_options));

    struct aws_mqtt5_client *client = fixture.mqtt5_fixture.client;
    ASSERT_SUCCESS(aws_mqtt5_client_start(client));

    aws_wait_for_connected_lifecycle_event(&fixture.mqtt5_fixture);

    struct request_response_protocol_adapter_subscription_event_record expected_outcome = {
        .event_type = AWS_PASET_SUBSCRIBE_SUCCESS,
    };

    aws_byte_buf_init_copy_from_cursor(&expected_outcome.topic_filter, allocator, aws_byte_cursor_from_c_str("hello/world"));

    struct aws_protocol_adapter_subscribe_options subscribe_options = {
        .topic_filter = aws_byte_cursor_from_buf(&expected_outcome.topic_filter),
        .ack_timeout_seconds = 5,
    };

    aws_mqtt_protocol_adapter_subscribe(fixture.protocol_adapter, &subscribe_options);

    s_wait_for_subscription_events_contains(&fixture, &expected_outcome, 1);

    s_request_response_protocol_adapter_subscription_event_record_cleanup(&expected_outcome);

    s_aws_request_response_mqtt5_adapter_test_fixture_clean_up(&fixture);

    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(
    request_response_mqtt5_protocol_adapter_subscribe_success,
    s_request_response_mqtt5_protocol_adapter_subscribe_success_fn)