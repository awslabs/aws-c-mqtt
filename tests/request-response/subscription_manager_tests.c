/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/array_list.h>
#include <aws/mqtt/private/request-response/protocol_adapter.h>
#include <aws/mqtt/private/request-response/subscription_manager.h>

#include <aws/testing/aws_test_harness.h>

enum aws_protocol_adapter_api_type {
    PAAT_SUBSCRIBE,
    PAAT_UNSUBSCRIBE,
};

struct aws_protocol_adapter_api_record {
    enum aws_protocol_adapter_api_type type;

    struct aws_byte_buf topic_filter;
    struct aws_byte_cursor topic_filter_cursor;

    uint32_t timeout;
};

static void s_aws_protocol_adapter_api_record_init_from_subscribe(
    struct aws_protocol_adapter_api_record *record,
    struct aws_allocator *allocator,
    struct aws_byte_cursor topic_filter,
    uint32_t timeout) {
    AWS_ZERO_STRUCT(*record);
    record->type = PAAT_SUBSCRIBE;
    record->timeout = timeout;

    aws_byte_buf_init_copy_from_cursor(&record->topic_filter, allocator, topic_filter);
    record->topic_filter_cursor = aws_byte_cursor_from_buf(&record->topic_filter);
}

static void s_aws_protocol_adapter_api_record_init_from_unsubscribe(
    struct aws_protocol_adapter_api_record *record,
    struct aws_allocator *allocator,
    struct aws_byte_cursor topic_filter,
    uint32_t timeout) {
    AWS_ZERO_STRUCT(*record);
    record->type = PAAT_UNSUBSCRIBE;
    record->timeout = timeout;

    aws_byte_buf_init_copy_from_cursor(&record->topic_filter, allocator, topic_filter);
    record->topic_filter_cursor = aws_byte_cursor_from_buf(&record->topic_filter);
}

static void s_aws_protocol_adapter_api_record_clean_up(struct aws_protocol_adapter_api_record *record) {
    aws_byte_buf_clean_up(&record->topic_filter);
}

struct aws_mqtt_protocol_adapter_mock_impl {
    struct aws_allocator *allocator;
    struct aws_mqtt_protocol_adapter base;

    struct aws_array_list api_records;
    bool is_connected;
};

static void s_aws_mqtt_protocol_adapter_mock_destroy(void *impl) {
    struct aws_mqtt_protocol_adapter_mock_impl *adapter = impl;

    size_t record_count = aws_array_list_length(&adapter->api_records);
    for (size_t i = 0; i < record_count; ++i) {
        struct aws_protocol_adapter_api_record *record = NULL;
        aws_array_list_get_at_ptr(&adapter->api_records, (void **)&record, i);

        s_aws_protocol_adapter_api_record_clean_up(record);
    }

    aws_array_list_clean_up(&adapter->api_records);
}

int s_aws_mqtt_protocol_adapter_mock_subscribe(void *impl, struct aws_protocol_adapter_subscribe_options *options) {
    struct aws_mqtt_protocol_adapter_mock_impl *adapter = impl;

    struct aws_protocol_adapter_api_record record;
    s_aws_protocol_adapter_api_record_init_from_subscribe(
        &record, adapter->allocator, options->topic_filter, options->ack_timeout_seconds);

    aws_array_list_push_back(&adapter->api_records, &record);

    return AWS_OP_SUCCESS;
}

int s_aws_mqtt_protocol_adapter_mock_unsubscribe(void *impl, struct aws_protocol_adapter_unsubscribe_options *options) {
    struct aws_mqtt_protocol_adapter_mock_impl *adapter = impl;

    struct aws_protocol_adapter_api_record record;
    s_aws_protocol_adapter_api_record_init_from_unsubscribe(
        &record, adapter->allocator, options->topic_filter, options->ack_timeout_seconds);

    aws_array_list_push_back(&adapter->api_records, &record);

    return AWS_OP_SUCCESS;
}

static bool s_aws_mqtt_protocol_adapter_mqtt_is_connected(void *impl) {
    struct aws_mqtt_protocol_adapter_mock_impl *adapter = impl;

    return adapter->is_connected;
}

static struct aws_mqtt_protocol_adapter_vtable s_protocol_adapter_mock_vtable = {
    .aws_mqtt_protocol_adapter_destroy_fn = s_aws_mqtt_protocol_adapter_mock_destroy,
    .aws_mqtt_protocol_adapter_subscribe_fn = s_aws_mqtt_protocol_adapter_mock_subscribe,
    .aws_mqtt_protocol_adapter_unsubscribe_fn = s_aws_mqtt_protocol_adapter_mock_unsubscribe,
    .aws_mqtt_protocol_adapter_publish_fn = NULL,
    .aws_mqtt_protocol_adapter_is_connected_fn = s_aws_mqtt_protocol_adapter_mqtt_is_connected,
};

static struct aws_protocol_adapter *s_aws_mqtt_mock_protocol_adapter_new(
    struct aws_allocator *allocator,
    bool is_connected) {
    struct aws_mqtt_protocol_adapter_mock_impl *adapter =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_protocol_adapter_mock_impl));

    adapter->allocator = allocator;
    adapter->base.impl = adapter;
    adapter->base.vtable = &s_protocol_adapter_mock_vtable;
    adapter->is_connected = is_connected;
    aws_array_list_init_dynamic(&adapter->api_records, allocator, 10, sizeof(struct aws_protocol_adapter_api_record));

    return &adapter->base;
}

static bool s_api_records_contains_range(
    struct aws_mqtt_protocol_adapter *protocol_adapter,
    size_t range_start,
    size_t range_end,
    struct aws_protocol_adapter_api_record *expected_records) {
    struct aws_mqtt_protocol_adapter_mock_impl *adapter = protocol_adapter->impl;

    size_t record_count = aws_array_list_length(&adapter->api_records);
    if (record_count < range_end) {
        return false;
    }

    for (size_t i = range_start; i < range_end; ++i) {
        struct aws_protocol_adapter_api_record *expected_record = &expected_records[i - range_start];

        struct aws_protocol_adapter_api_record *actual_record = NULL;
        aws_array_list_get_at_ptr(&adapter->api_records, (void **)&actual_record, i);

        if (expected_record->type != actual_record->type) {
            return false;
        }

        if (expected_record->timeout != actual_record->timeout) {
            return false;
        }

        if (!aws_byte_cursor_eq(&expected_record->topic_filter_cursor, &actual_record->topic_filter_cursor)) {
            return false;
        }
    }

    return true;
}

static bool s_api_records_equals(
    struct aws_mqtt_protocol_adapter *protocol_adapter,
    size_t record_count,
    struct aws_protocol_adapter_api_record *expected_records) {
    struct aws_mqtt_protocol_adapter_mock_impl *adapter = protocol_adapter->impl;

    return aws_array_list_length(&adapter->api_records) == record_count &&
           s_api_records_contains_range(protocol_adapter, 0, record_count, expected_records);
}

//////////////////////////////////////////////////////////////

struct aws_subscription_status_record {
    enum aws_rr_subscription_event_type type;
    struct aws_byte_buf topic_filter;
    struct aws_byte_cursor topic_filter_cursor;
    uint64_t operation_id;
};

static void s_aws_subscription_status_record_init_from_event(
    struct aws_subscription_status_record *record,
    struct aws_allocator *allocator,
    struct aws_rr_subscription_status_event *event) {
    AWS_ZERO_STRUCT(*record);
    record->type = event->type;
    record->operation_id = event->operation_id;

    aws_byte_buf_init_copy_from_cursor(&record->topic_filter, allocator, event->topic_filter);
    record->topic_filter_cursor = aws_byte_cursor_from_buf(&record->topic_filter);
}

static void s_aws_subscription_status_record_clean_up(struct aws_subscription_status_record *record) {
    aws_byte_buf_clean_up(&record->topic_filter);
}

//////////////////////////////////////////////////////////////

struct aws_subscription_manager_test_fixture {
    struct aws_allocator *allocator;

    struct aws_protocol_adapter *mock_protocol_adapter;
    struct aws_rr_subscription_manager subscription_manager;

    struct aws_array_list subscription_status_records;
};

static void s_aws_rr_subscription_status_event_test_callback_fn(
    const struct aws_rr_subscription_status_event *event,
    void *userdata) {
    struct aws_subscription_manager_test_fixture *fixture = userdata;

    struct aws_subscription_status_record record;
    s_aws_subscription_status_record_init_from_event(&record, fixture->allocator, event);

    aws_array_list_push_back(&fixture->subscription_status_records, &record);
}

struct aws_subscription_manager_test_fixture_options {
    uint32_t operation_timeout_seconds;
    size_t max_subscriptions;
    bool start_connected;
};

static int s_aws_subscription_manager_test_fixture_init(
    struct aws_subscription_manager_test_fixture *fixture,
    struct aws_allocator *allocator,
    const struct aws_subscription_manager_test_fixture_options *options) {
    AWS_ZERO_STRUCT(*fixture);

    fixture->allocator = allocator;
    fixture->mock_protocol_adapter = s_aws_mqtt_mock_protocol_adapter_new(allocator, options->start_connected);

    aws_array_list_init_dynamic(
        &fixture->subscription_status_records, allocator, 10, sizeof(struct aws_subscription_status_record));

    struct aws_rr_subscription_manager_options subscription_manager_options = {
        .max_subscriptions = options->max_subscriptions,
        .operation_timeout_seconds = options->operation_timeout_seconds,
        .subscription_status_callback = s_aws_rr_subscription_status_event_test_callback_fn,
        .userdata = fixture};
    ASSERT_SUCCESS(aws_rr_subscription_manager_init(
        &fixture->subscription_manager, allocator, fixture->mock_protocol_adapter, &subscription_manager_options));

    return AWS_OP_SUCCESS;
}

static void s_aws_subscription_manager_test_fixture_clean_up(struct aws_subscription_manager_test_fixture *fixture) {

    aws_rr_subscription_manager_clean_up(&fixture->subscription_manager);
    aws_mqtt_protocol_adapter_destroy(fixture->mock_protocol_adapter);

    size_t record_count = aws_array_list_length(&fixture->subscription_status_records);
    for (size_t i = 0; i < record_count; ++i) {
        struct aws_subscription_status_record *record = NULL;
        aws_array_list_get_at_ptr(&fixture->subscription_status_records, (void **)&record, i);

        s_aws_subscription_status_record_clean_up(record);
    }

    aws_array_list_clean_up(&fixture->subscription_status_records);
}

static bool s_find_subscription_event_record(
    struct aws_subscription_manager_test_fixture *fixture,
    struct aws_subscription_status_record *expected_record,
    size_t *index) {

    size_t record_count = aws_array_list_length(&fixture->subscription_status_records);
    for (size_t i = 0; i < record_count; ++i) {

        struct aws_subscription_status_record *actual_record = NULL;
        aws_array_list_get_at_ptr(&fixture->subscription_status_records, (void **)&actual_record, i);

        if (expected_record->type != actual_record->type) {
            continue;
        }

        if (expected_record->operation_id != actual_record->operation_id) {
            continue;
        }

        if (!aws_byte_cursor_eq(&expected_record->topic_filter_cursor, &actual_record->topic_filter_cursor)) {
            continue;
        }

        *index = i;
        return true;
    }

    return false;
}

static bool s_contains_subscription_event_record(
    struct aws_subscription_manager_test_fixture *fixture,
    struct aws_subscription_status_record *record) {
    size_t index = 0;
    return s_find_subscription_event_record(fixture, record, &index);
}

static bool s_contains_subscription_event_records(
    struct aws_subscription_manager_test_fixture *fixture,
    size_t record_count,
    struct aws_subscription_status_record *records) {
    for (size_t i = 0; i < record_count; ++i) {
        if (!s_contains_subscription_event_record(fixture, &records[i])) {
            return false;
        }
    }

    return true;
}

static bool s_contains_subscription_event_sequential_records(
    struct aws_subscription_manager_test_fixture *fixture,
    size_t record_count,
    struct aws_subscription_status_record *records) {
    size_t previous_index = 0;
    for (size_t i = 0; i < record_count; ++i) {
        size_t index = 0;
        if (!s_find_subscription_event_record(fixture, &records[i], &index)) {
            return false;
        }

        if (i > 0) {
            if (index <= previous_index) {
                return false;
            }
        }
        previous_index = index;
    }

    return true;
}
