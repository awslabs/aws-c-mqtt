/**
* Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
* SPDX-License-Identifier: Apache-2.0.
*/

#include "mqtt5_testing_utils.h"
#include <aws/mqtt/private/request-response/protocol_adapter.h>

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

static void s_request_response_protocol_adapter_incoming_subscription_event_record_init(
    struct request_response_protocol_adapter_subscription_event_record *record,
    struct aws_allocator *allocator,
    struct aws_byte_cursor topic_filter) {

    aws_byte_buf_init_copy_from_cursor(&record->topic_filter, allocator, topic_filter);
}

static void s_request_response_protocol_adapter_incoming_subscription_event_record_cleanup(struct request_response_protocol_adapter_subscription_event_record *record) {
    aws_byte_buf_clean_up(&record->topic_filter);
}

struct aws_request_response_mqtt5_adapter_test_fixture {
    struct aws_allocator *allocator;
    struct aws_mqtt5_client_mock_test_fixture mqtt5_fixture;

    struct aws_mqtt_protocol_adapter *protocol_adapter;

    struct aws_array_list incoming_publish_events;
    struct aws_array_list connection_events;
    struct aws_array_list subscription_events;

    struct aws_mutex lock;
    struct aws_condition_variable signal;
};


static void s_rr_mqtt5_protocol_adapter_subscription_event(struct aws_protocol_adapter_subscription_event *event, void *user_data) {
    struct aws_request_response_mqtt5_adapter_test_fixture *fixture = user_data;

    struct request_response_protocol_adapter_subscription_event_record record = {
        .event_type = event->event_type
    };
    s_request_response_protocol_adapter_incoming_subscription_event_record_init(&record, fixture->allocator, event->topic_filter);

    aws_mutex_lock(&fixture->lock);
    aws_array_list_push_back(&fixture->subscription_events, &record);
    aws_mutex_unlock(&fixture->lock);
    aws_condition_variable_notify_all(&fixture->signal);
}

static void s_rr_mqtt5_protocol_adapter_incoming_publish(struct aws_protocol_adapter_incoming_publish_event *publish, void *user_data) {
    struct aws_request_response_mqtt5_adapter_test_fixture *fixture = user_data;

}

static void s_rr_mqtt5_protocol_adapter_terminate_callback(void *user_data) {
    struct aws_request_response_mqtt5_adapter_test_fixture *fixture = user_data;
}

static void s_rr_mqtt5_protocol_adapter_connection_event(struct aws_protocol_adapter_connection_event *event, void *user_data) {
    struct aws_request_response_mqtt5_adapter_test_fixture *fixture = user_data;
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
        .subscription_event_callback = s_rr_mqtt5_protocol_adapter_subscription_event,
        .incoming_publish_callback = s_rr_mqtt5_protocol_adapter_incoming_publish,
        .terminate_callback = s_rr_mqtt5_protocol_adapter_terminate_callback,
        .connection_event_callback = s_rr_mqtt5_protocol_adapter_connection_event,
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

static void s_aws_mqtt5_to_mqtt3_adapter_test_fixture_clean_up(struct aws_request_response_mqtt5_adapter_test_fixture *fixture) {

    aws_mqtt5_client_mock_test_fixture_clean_up(&fixture->mqtt5_fixture);

    aws_mutex_clean_up(&fixture->lock);
    aws_condition_variable_clean_up(&fixture->signal);
}
