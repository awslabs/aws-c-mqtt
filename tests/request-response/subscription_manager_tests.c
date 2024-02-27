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

    aws_mem_release(adapter->allocator, adapter);
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

static struct aws_mqtt_protocol_adapter *s_aws_mqtt_mock_protocol_adapter_new(
    struct aws_allocator *allocator,
    const struct aws_mqtt_protocol_adapter_vtable *vtable,
    bool is_connected) {
    struct aws_mqtt_protocol_adapter_mock_impl *adapter =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_protocol_adapter_mock_impl));

    adapter->allocator = allocator;
    adapter->base.impl = adapter;
    if (vtable != NULL) {
        adapter->base.vtable = vtable;
    } else {
        adapter->base.vtable = &s_protocol_adapter_mock_vtable;
    }
    adapter->is_connected = is_connected;
    aws_array_list_init_dynamic(&adapter->api_records, allocator, 10, sizeof(struct aws_protocol_adapter_api_record));

    return &adapter->base;
}

static bool s_protocol_adapter_api_records_equal(
    struct aws_protocol_adapter_api_record *record1,
    struct aws_protocol_adapter_api_record *record2) {
    if (record1->type != record2->type) {
        return false;
    }

    if (record1->timeout != record2->timeout) {
        return false;
    }

    if (!aws_byte_cursor_eq(&record1->topic_filter_cursor, &record2->topic_filter_cursor)) {
        return false;
    }

    return true;
}

static bool s_api_records_contains_record(
    struct aws_mqtt_protocol_adapter *protocol_adapter,
    struct aws_protocol_adapter_api_record *expected_record) {

    struct aws_mqtt_protocol_adapter_mock_impl *adapter = protocol_adapter->impl;

    size_t record_count = aws_array_list_length(&adapter->api_records);
    for (size_t i = 0; i < record_count; ++i) {
        struct aws_protocol_adapter_api_record *actual_record = NULL;
        aws_array_list_get_at_ptr(&adapter->api_records, (void **)&actual_record, i);

        if (s_protocol_adapter_api_records_equal(expected_record, actual_record)) {
            return true;
        }
    }

    return false;
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

        if (!s_protocol_adapter_api_records_equal(expected_record, actual_record)) {
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
    const struct aws_rr_subscription_status_event *event) {
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

    struct aws_mqtt_protocol_adapter *mock_protocol_adapter;
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
    const struct aws_mqtt_protocol_adapter_vtable *adapter_vtable;
};

static const uint32_t DEFAULT_SM_TEST_TIMEOUT = 5;

static int s_aws_subscription_manager_test_fixture_init(
    struct aws_subscription_manager_test_fixture *fixture,
    struct aws_allocator *allocator,
    const struct aws_subscription_manager_test_fixture_options *options) {
    AWS_ZERO_STRUCT(*fixture);

    struct aws_subscription_manager_test_fixture_options default_options = {
        .max_subscriptions = 3,
        .operation_timeout_seconds = DEFAULT_SM_TEST_TIMEOUT,
        .start_connected = true,
    };

    if (options == NULL) {
        options = &default_options;
    }

    fixture->allocator = allocator;
    fixture->mock_protocol_adapter =
        s_aws_mqtt_mock_protocol_adapter_new(allocator, options->adapter_vtable, options->start_connected);

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

static int s_rrsm_acquire_subscribing_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct aws_subscription_manager_test_fixture fixture;
    ASSERT_SUCCESS(s_aws_subscription_manager_test_fixture_init(&fixture, allocator, NULL));

    struct aws_protocol_adapter_api_record expected_subscribes[] = {
        {
            .type = PAAT_SUBSCRIBE,
            .topic_filter_cursor = aws_byte_cursor_from_c_str("hello/world1"),
            .timeout = DEFAULT_SM_TEST_TIMEOUT,
        },
        {
            .type = PAAT_SUBSCRIBE,
            .topic_filter_cursor = aws_byte_cursor_from_c_str("hello/world2"),
            .timeout = DEFAULT_SM_TEST_TIMEOUT,
        },
        {
            .type = PAAT_SUBSCRIBE,
            .topic_filter_cursor = aws_byte_cursor_from_c_str("hello/world3"),
            .timeout = DEFAULT_SM_TEST_TIMEOUT,
        },
    };

    struct aws_rr_subscription_manager *manager = &fixture.subscription_manager;

    struct aws_rr_acquire_subscription_options acquire1_options = {
        .type = ARRST_REQUEST_RESPONSE,
        .topic_filter = aws_byte_cursor_from_c_str("hello/world1"),
        .operation_id = 1,
    };
    ASSERT_INT_EQUALS(AASRT_SUBSCRIBING, aws_rr_subscription_manager_acquire_subscription(manager, &acquire1_options));
    ASSERT_TRUE(s_api_records_equals(fixture.mock_protocol_adapter, 1, expected_subscribes));

    struct aws_rr_acquire_subscription_options acquire2_options = {
        .type = ARRST_EVENT_STREAM,
        .topic_filter = aws_byte_cursor_from_c_str("hello/world2"),
        .operation_id = 2,
    };
    ASSERT_INT_EQUALS(AASRT_SUBSCRIBING, aws_rr_subscription_manager_acquire_subscription(manager, &acquire2_options));
    ASSERT_TRUE(s_api_records_equals(fixture.mock_protocol_adapter, 2, expected_subscribes));

    struct aws_rr_acquire_subscription_options acquire3_options = {
        .type = ARRST_REQUEST_RESPONSE,
        .topic_filter = aws_byte_cursor_from_c_str("hello/world3"),
        .operation_id = 3,
    };
    ASSERT_INT_EQUALS(AASRT_SUBSCRIBING, aws_rr_subscription_manager_acquire_subscription(manager, &acquire3_options));
    ASSERT_TRUE(s_api_records_equals(fixture.mock_protocol_adapter, 3, expected_subscribes));

    s_aws_subscription_manager_test_fixture_clean_up(&fixture);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(rrsm_acquire_subscribing, s_rrsm_acquire_subscribing_fn)

// duplicate acquires while subscribing should not generate additional subscribe requests
static int s_rrsm_acquire_existing_subscribing_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct aws_subscription_manager_test_fixture fixture;
    ASSERT_SUCCESS(s_aws_subscription_manager_test_fixture_init(&fixture, allocator, NULL));

    struct aws_protocol_adapter_api_record expected_subscribes[] = {
        {
            .type = PAAT_SUBSCRIBE,
            .topic_filter_cursor = aws_byte_cursor_from_c_str("hello/world1"),
            .timeout = DEFAULT_SM_TEST_TIMEOUT,
        },
        {
            .type = PAAT_SUBSCRIBE,
            .topic_filter_cursor = aws_byte_cursor_from_c_str("hello/world2"),
            .timeout = DEFAULT_SM_TEST_TIMEOUT,
        },
    };

    struct aws_rr_subscription_manager *manager = &fixture.subscription_manager;

    struct aws_rr_acquire_subscription_options acquire1_options = {
        .type = ARRST_REQUEST_RESPONSE,
        .topic_filter = aws_byte_cursor_from_c_str("hello/world1"),
        .operation_id = 1,
    };
    ASSERT_INT_EQUALS(AASRT_SUBSCRIBING, aws_rr_subscription_manager_acquire_subscription(manager, &acquire1_options));
    ASSERT_TRUE(s_api_records_equals(fixture.mock_protocol_adapter, 1, expected_subscribes));

    struct aws_rr_acquire_subscription_options acquire2_options = {
        .type = ARRST_EVENT_STREAM,
        .topic_filter = aws_byte_cursor_from_c_str("hello/world2"),
        .operation_id = 2,
    };
    ASSERT_INT_EQUALS(AASRT_SUBSCRIBING, aws_rr_subscription_manager_acquire_subscription(manager, &acquire2_options));
    ASSERT_TRUE(s_api_records_equals(fixture.mock_protocol_adapter, 2, expected_subscribes));

    struct aws_rr_acquire_subscription_options reacquire1_options = {
        .type = ARRST_REQUEST_RESPONSE,
        .topic_filter = aws_byte_cursor_from_c_str("hello/world1"),
        .operation_id = 3,
    };
    ASSERT_INT_EQUALS(
        AASRT_SUBSCRIBING, aws_rr_subscription_manager_acquire_subscription(manager, &reacquire1_options));
    ASSERT_TRUE(s_api_records_equals(fixture.mock_protocol_adapter, 2, expected_subscribes));

    struct aws_rr_acquire_subscription_options reacquire2_options = {
        .type = ARRST_EVENT_STREAM,
        .topic_filter = aws_byte_cursor_from_c_str("hello/world2"),
        .operation_id = 4,
    };
    ASSERT_INT_EQUALS(
        AASRT_SUBSCRIBING, aws_rr_subscription_manager_acquire_subscription(manager, &reacquire2_options));
    ASSERT_TRUE(s_api_records_equals(fixture.mock_protocol_adapter, 2, expected_subscribes));

    s_aws_subscription_manager_test_fixture_clean_up(&fixture);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(rrsm_acquire_existing_subscribing, s_rrsm_acquire_existing_subscribing_fn)

// calling acquire while subscribed returns subscribed, also checks subscription event emission
static int s_rrsm_acquire_existing_subscribed_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct aws_subscription_manager_test_fixture fixture;
    ASSERT_SUCCESS(s_aws_subscription_manager_test_fixture_init(&fixture, allocator, NULL));

    struct aws_protocol_adapter_api_record expected_subscribes[] = {
        {
            .type = PAAT_SUBSCRIBE,
            .topic_filter_cursor = aws_byte_cursor_from_c_str("hello/world1"),
            .timeout = DEFAULT_SM_TEST_TIMEOUT,
        },
        {
            .type = PAAT_SUBSCRIBE,
            .topic_filter_cursor = aws_byte_cursor_from_c_str("hello/world2"),
            .timeout = DEFAULT_SM_TEST_TIMEOUT,
        },
    };

    struct aws_rr_subscription_manager *manager = &fixture.subscription_manager;

    struct aws_rr_acquire_subscription_options acquire1_options = {
        .type = ARRST_REQUEST_RESPONSE,
        .topic_filter = aws_byte_cursor_from_c_str("hello/world1"),
        .operation_id = 1,
    };
    ASSERT_INT_EQUALS(AASRT_SUBSCRIBING, aws_rr_subscription_manager_acquire_subscription(manager, &acquire1_options));
    ASSERT_TRUE(s_api_records_equals(fixture.mock_protocol_adapter, 1, expected_subscribes));

    struct aws_rr_acquire_subscription_options acquire2_options = {
        .type = ARRST_EVENT_STREAM,
        .topic_filter = aws_byte_cursor_from_c_str("hello/world2"),
        .operation_id = 2,
    };
    ASSERT_INT_EQUALS(AASRT_SUBSCRIBING, aws_rr_subscription_manager_acquire_subscription(manager, &acquire2_options));
    ASSERT_TRUE(s_api_records_equals(fixture.mock_protocol_adapter, 2, expected_subscribes));

    struct aws_protocol_adapter_subscription_event successful_subscription1_event = {
        .topic_filter = aws_byte_cursor_from_c_str("hello/world1"),
        .event_type = AWS_PASET_SUBSCRIBE,
        .error_code = AWS_ERROR_SUCCESS,
    };
    aws_rr_subscription_manager_on_protocol_adapter_subscription_event(manager, &successful_subscription1_event);

    struct aws_subscription_status_record expected_subscription_events[] = {
        {
            .type = ARRSET_SUBSCRIPTION_SUBSCRIBE_SUCCESS,
            .topic_filter_cursor = aws_byte_cursor_from_c_str("hello/world1"),
            .operation_id = 1,
        },
        {
            .type = ARRSET_SUBSCRIPTION_SUBSCRIBE_SUCCESS,
            .topic_filter_cursor = aws_byte_cursor_from_c_str("hello/world2"),
            .operation_id = 2,
        }};
    ASSERT_TRUE(s_contains_subscription_event_sequential_records(&fixture, 1, expected_subscription_events));

    struct aws_protocol_adapter_subscription_event successful_subscription2_event = {
        .topic_filter = aws_byte_cursor_from_c_str("hello/world2"),
        .event_type = AWS_PASET_SUBSCRIBE,
        .error_code = AWS_ERROR_SUCCESS,
    };
    aws_rr_subscription_manager_on_protocol_adapter_subscription_event(manager, &successful_subscription2_event);

    ASSERT_TRUE(s_contains_subscription_event_sequential_records(&fixture, 2, expected_subscription_events));

    struct aws_rr_acquire_subscription_options reacquire1_options = {
        .type = ARRST_REQUEST_RESPONSE,
        .topic_filter = aws_byte_cursor_from_c_str("hello/world1"),
        .operation_id = 3,
    };
    ASSERT_INT_EQUALS(AASRT_SUBSCRIBED, aws_rr_subscription_manager_acquire_subscription(manager, &reacquire1_options));
    ASSERT_TRUE(s_api_records_equals(fixture.mock_protocol_adapter, 2, expected_subscribes));

    struct aws_rr_acquire_subscription_options reacquire2_options = {
        .type = ARRST_EVENT_STREAM,
        .topic_filter = aws_byte_cursor_from_c_str("hello/world2"),
        .operation_id = 4,
    };
    ASSERT_INT_EQUALS(AASRT_SUBSCRIBED, aws_rr_subscription_manager_acquire_subscription(manager, &reacquire2_options));
    ASSERT_TRUE(s_api_records_equals(fixture.mock_protocol_adapter, 2, expected_subscribes));

    s_aws_subscription_manager_test_fixture_clean_up(&fixture);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(rrsm_acquire_existing_subscribed, s_rrsm_acquire_existing_subscribed_fn)

static int s_do_acquire_blocked_test(
    struct aws_subscription_manager_test_fixture *fixture,
    enum aws_rr_subscription_type subscription_type) {
    struct aws_rr_subscription_manager *manager = &fixture->subscription_manager;

    struct aws_rr_acquire_subscription_options acquire1_options = {
        .type = ARRST_REQUEST_RESPONSE,
        .topic_filter = aws_byte_cursor_from_c_str("hello/world1"),
        .operation_id = 1,
    };
    ASSERT_INT_EQUALS(AASRT_SUBSCRIBING, aws_rr_subscription_manager_acquire_subscription(manager, &acquire1_options));

    // no room, but it could potentially free up in the future
    struct aws_rr_acquire_subscription_options acquire2_options = {
        .type = subscription_type,
        .topic_filter = aws_byte_cursor_from_c_str("hello/world2"),
        .operation_id = 2,
    };
    ASSERT_INT_EQUALS(AASRT_BLOCKED, aws_rr_subscription_manager_acquire_subscription(manager, &acquire2_options));

    return AWS_OP_SUCCESS;
}

static int s_rrsm_acquire_blocked_rr_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct aws_subscription_manager_test_fixture_options fixture_config = {
        .max_subscriptions = 1,
        .operation_timeout_seconds = 30,
    };
    struct aws_subscription_manager_test_fixture fixture;
    ASSERT_SUCCESS(s_aws_subscription_manager_test_fixture_init(&fixture, allocator, &fixture_config));

    ASSERT_SUCCESS(s_do_acquire_blocked_test(&fixture, ARRST_REQUEST_RESPONSE));

    s_aws_subscription_manager_test_fixture_clean_up(&fixture);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(rrsm_acquire_blocked_rr, s_rrsm_acquire_blocked_rr_fn)

static int s_rrsm_acquire_blocked_eventstream_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct aws_subscription_manager_test_fixture_options fixture_config = {
        .max_subscriptions = 2,
        .operation_timeout_seconds = 30,
    };
    struct aws_subscription_manager_test_fixture fixture;
    ASSERT_SUCCESS(s_aws_subscription_manager_test_fixture_init(&fixture, allocator, &fixture_config));

    struct aws_rr_acquire_subscription_options acquire1_options = {
        .type = ARRST_REQUEST_RESPONSE,
        .topic_filter = aws_byte_cursor_from_c_str("hello/world3"),
        .operation_id = 3,
    };
    ASSERT_INT_EQUALS(
        AASRT_SUBSCRIBING,
        aws_rr_subscription_manager_acquire_subscription(&fixture.subscription_manager, &acquire1_options));

    ASSERT_SUCCESS(s_do_acquire_blocked_test(&fixture, ARRST_EVENT_STREAM));

    s_aws_subscription_manager_test_fixture_clean_up(&fixture);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(rrsm_acquire_blocked_eventstream, s_rrsm_acquire_blocked_eventstream_fn)

static int s_do_acquire_no_capacity_test(struct aws_subscription_manager_test_fixture *fixture) {
    struct aws_rr_subscription_manager *manager = &fixture->subscription_manager;

    struct aws_rr_acquire_subscription_options acquire_options = {
        .type = ARRST_EVENT_STREAM,
        .topic_filter = aws_byte_cursor_from_c_str("hello/world"),
        .operation_id = 1,
    };
    ASSERT_INT_EQUALS(AASRT_NO_CAPACITY, aws_rr_subscription_manager_acquire_subscription(manager, &acquire_options));

    return AWS_OP_SUCCESS;
}

static int s_rrsm_acquire_no_capacity_max1_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct aws_subscription_manager_test_fixture_options fixture_config = {
        .max_subscriptions = 1,
        .operation_timeout_seconds = 30,
    };
    struct aws_subscription_manager_test_fixture fixture;
    ASSERT_SUCCESS(s_aws_subscription_manager_test_fixture_init(&fixture, allocator, &fixture_config));

    ASSERT_SUCCESS(s_do_acquire_no_capacity_test(&fixture));

    s_aws_subscription_manager_test_fixture_clean_up(&fixture);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(rrsm_acquire_no_capacity_max1, s_rrsm_acquire_no_capacity_max1_fn)

static int s_rrsm_acquire_no_capacity_too_many_event_stream_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct aws_subscription_manager_test_fixture_options fixture_config = {
        .max_subscriptions = 5,
        .operation_timeout_seconds = 30,
    };
    struct aws_subscription_manager_test_fixture fixture;
    ASSERT_SUCCESS(s_aws_subscription_manager_test_fixture_init(&fixture, allocator, &fixture_config));

    // N - 1 event stream subscriptions
    for (size_t i = 0; i < 4; ++i) {
        char topic_filter_buffer[256];
        snprintf(topic_filter_buffer, AWS_ARRAY_SIZE(topic_filter_buffer), "hello/world/%d", (int)(i + 1));

        struct aws_rr_acquire_subscription_options acquire_options = {
            .type = ARRST_EVENT_STREAM,
            .topic_filter = aws_byte_cursor_from_c_str(topic_filter_buffer),
            .operation_id = i + 2,
        };
        ASSERT_INT_EQUALS(
            AASRT_SUBSCRIBING,
            aws_rr_subscription_manager_acquire_subscription(&fixture.subscription_manager, &acquire_options));
    }

    // 1 more, should fail
    ASSERT_SUCCESS(s_do_acquire_no_capacity_test(&fixture));

    s_aws_subscription_manager_test_fixture_clean_up(&fixture);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(rrsm_acquire_no_capacity_too_many_event_stream, s_rrsm_acquire_no_capacity_too_many_event_stream_fn)

static int s_rrsm_acquire_failure_mixed_subscription_types_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct aws_subscription_manager_test_fixture fixture;
    ASSERT_SUCCESS(s_aws_subscription_manager_test_fixture_init(&fixture, allocator, NULL));

    struct aws_rr_acquire_subscription_options acquire1_options = {
        .type = ARRST_REQUEST_RESPONSE,
        .topic_filter = aws_byte_cursor_from_c_str("hello/world"),
        .operation_id = 1,
    };
    ASSERT_INT_EQUALS(
        AASRT_SUBSCRIBING,
        aws_rr_subscription_manager_acquire_subscription(&fixture.subscription_manager, &acquire1_options));

    struct aws_rr_acquire_subscription_options acquire2_options = {
        .type = ARRST_EVENT_STREAM,
        .topic_filter = aws_byte_cursor_from_c_str("hello/world"),
        .operation_id = 2,
    };
    ASSERT_INT_EQUALS(
        AASRT_FAILURE,
        aws_rr_subscription_manager_acquire_subscription(&fixture.subscription_manager, &acquire2_options));

    s_aws_subscription_manager_test_fixture_clean_up(&fixture);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(rrsm_acquire_failure_mixed_subscription_types, s_rrsm_acquire_failure_mixed_subscription_types_fn)

static int s_rrsm_release_unsubscribes_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct aws_subscription_manager_test_fixture fixture;
    ASSERT_SUCCESS(s_aws_subscription_manager_test_fixture_init(&fixture, allocator, NULL));

    struct aws_rr_subscription_manager *manager = &fixture.subscription_manager;

    struct aws_rr_acquire_subscription_options acquire1_options = {
        .type = ARRST_REQUEST_RESPONSE,
        .topic_filter = aws_byte_cursor_from_c_str("hello/world"),
        .operation_id = 1,
    };
    ASSERT_INT_EQUALS(AASRT_SUBSCRIBING, aws_rr_subscription_manager_acquire_subscription(manager, &acquire1_options));

    struct aws_rr_acquire_subscription_options acquire2_options = {
        .type = ARRST_REQUEST_RESPONSE,
        .topic_filter = aws_byte_cursor_from_c_str("hello/world"),
        .operation_id = 2,
    };
    ASSERT_INT_EQUALS(AASRT_SUBSCRIBING, aws_rr_subscription_manager_acquire_subscription(manager, &acquire2_options));

    struct aws_protocol_adapter_subscription_event successful_subscription_event = {
        .topic_filter = aws_byte_cursor_from_c_str("hello/world"),
        .event_type = AWS_PASET_SUBSCRIBE,
        .error_code = AWS_ERROR_SUCCESS,
    };
    aws_rr_subscription_manager_on_protocol_adapter_subscription_event(manager, &successful_subscription_event);

    // verify two success callbacks
    struct aws_subscription_status_record expected_subscription_events[] = {
        {
            .type = ARRSET_SUBSCRIPTION_SUBSCRIBE_SUCCESS,
            .topic_filter_cursor = aws_byte_cursor_from_c_str("hello/world"),
            .operation_id = 1,
        },
        {
            .type = ARRSET_SUBSCRIPTION_SUBSCRIBE_SUCCESS,
            .topic_filter_cursor = aws_byte_cursor_from_c_str("hello/world"),
            .operation_id = 2,
        }};
    ASSERT_TRUE(s_contains_subscription_event_records(&fixture, 2, expected_subscription_events));

    // verify no unsubscribes
    struct aws_protocol_adapter_api_record expected_unsubscribe = {
        .type = PAAT_UNSUBSCRIBE,
        .topic_filter_cursor = aws_byte_cursor_from_c_str("hello/world"),
        .timeout = DEFAULT_SM_TEST_TIMEOUT,
    };
    ASSERT_FALSE(s_api_records_contains_record(fixture.mock_protocol_adapter, &expected_unsubscribe));

    // release once, verify no unsubscribe
    struct aws_rr_release_subscription_options release1_options = {
        .topic_filter = aws_byte_cursor_from_c_str("hello/world"),
        .operation_id = 1,
    };
    aws_rr_subscription_manager_release_subscription(manager, &release1_options);
    ASSERT_FALSE(s_api_records_contains_record(fixture.mock_protocol_adapter, &expected_unsubscribe));

    // release second
    struct aws_rr_release_subscription_options release2_options = {
        .topic_filter = aws_byte_cursor_from_c_str("hello/world"),
        .operation_id = 2,
    };
    aws_rr_subscription_manager_release_subscription(manager, &release2_options);
    ASSERT_FALSE(s_api_records_contains_record(fixture.mock_protocol_adapter, &expected_unsubscribe));

    // unsubscribe is lazy, so we need to trigger it by acquiring something else
    struct aws_rr_acquire_subscription_options acquire3_options = {
        .type = ARRST_REQUEST_RESPONSE,
        .topic_filter = aws_byte_cursor_from_c_str("hello/world2"),
        .operation_id = 3,
    };
    ASSERT_INT_EQUALS(AASRT_SUBSCRIBING, aws_rr_subscription_manager_acquire_subscription(manager, &acquire3_options));

    // now the unsubscribe should be present
    ASSERT_TRUE(s_api_records_contains_record(fixture.mock_protocol_adapter, &expected_unsubscribe));

    s_aws_subscription_manager_test_fixture_clean_up(&fixture);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(rrsm_release_unsubscribes, s_rrsm_release_unsubscribes_fn)

static int s_rrsm_do_unsubscribe_test(struct aws_allocator *allocator, bool should_succeed) {
    aws_mqtt_library_init(allocator);

    struct aws_subscription_manager_test_fixture_options fixture_config = {
        .max_subscriptions = 1,
        .operation_timeout_seconds = DEFAULT_SM_TEST_TIMEOUT,
        .start_connected = true,
    };

    struct aws_subscription_manager_test_fixture fixture;
    ASSERT_SUCCESS(s_aws_subscription_manager_test_fixture_init(&fixture, allocator, &fixture_config));

    struct aws_rr_subscription_manager *manager = &fixture.subscription_manager;

    struct aws_rr_acquire_subscription_options acquire1_options = {
        .type = ARRST_REQUEST_RESPONSE,
        .topic_filter = aws_byte_cursor_from_c_str("hello/world"),
        .operation_id = 1,
    };
    ASSERT_INT_EQUALS(AASRT_SUBSCRIBING, aws_rr_subscription_manager_acquire_subscription(manager, &acquire1_options));

    // budget of 1, so new acquires should be blocked
    struct aws_rr_acquire_subscription_options acquire2_options = {
        .type = ARRST_REQUEST_RESPONSE,
        .topic_filter = aws_byte_cursor_from_c_str("hello/world2"),
        .operation_id = 2,
    };
    ASSERT_INT_EQUALS(AASRT_BLOCKED, aws_rr_subscription_manager_acquire_subscription(manager, &acquire2_options));

    // complete the subscribe
    struct aws_protocol_adapter_subscription_event successful_subscription_event = {
        .topic_filter = aws_byte_cursor_from_c_str("hello/world"),
        .event_type = AWS_PASET_SUBSCRIBE,
        .error_code = AWS_ERROR_SUCCESS,
    };
    aws_rr_subscription_manager_on_protocol_adapter_subscription_event(manager, &successful_subscription_event);

    // still blocked
    ASSERT_INT_EQUALS(AASRT_BLOCKED, aws_rr_subscription_manager_acquire_subscription(manager, &acquire2_options));

    // release
    struct aws_rr_release_subscription_options release1_options = {
        .topic_filter = aws_byte_cursor_from_c_str("hello/world"),
        .operation_id = 1,
    };
    aws_rr_subscription_manager_release_subscription(manager, &release1_options);

    // unsubscribe should be visible, but we're still blocked because it hasn't completed
    ASSERT_INT_EQUALS(AASRT_BLOCKED, aws_rr_subscription_manager_acquire_subscription(manager, &acquire2_options));
    struct aws_protocol_adapter_api_record expected_unsubscribe = {
        .type = PAAT_UNSUBSCRIBE,
        .topic_filter_cursor = aws_byte_cursor_from_c_str("hello/world"),
        .timeout = DEFAULT_SM_TEST_TIMEOUT,
    };
    ASSERT_TRUE(s_api_records_contains_record(fixture.mock_protocol_adapter, &expected_unsubscribe));

    // complete the unsubscribe
    struct aws_protocol_adapter_subscription_event successful_unsubscribe_event = {
        .topic_filter = aws_byte_cursor_from_c_str("hello/world"),
        .event_type = AWS_PASET_UNSUBSCRIBE,
        .error_code = should_succeed ? AWS_ERROR_SUCCESS : AWS_ERROR_MQTT5_UNSUBSCRIBE_OPTIONS_VALIDATION,
    };
    aws_rr_subscription_manager_on_protocol_adapter_subscription_event(manager, &successful_unsubscribe_event);

    // a successful unsubscribe should clear space, a failed one should not
    ASSERT_INT_EQUALS(
        (should_succeed ? AASRT_SUBSCRIBING : AASRT_BLOCKED),
        aws_rr_subscription_manager_acquire_subscription(manager, &acquire2_options));

    s_aws_subscription_manager_test_fixture_clean_up(&fixture);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

static int s_rrsm_release_unsubscribe_success_clears_space_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    return s_rrsm_do_unsubscribe_test(allocator, true);
}

AWS_TEST_CASE(rrsm_release_unsubscribe_success_clears_space, s_rrsm_release_unsubscribe_success_clears_space_fn)

static int s_rrsm_release_unsubscribe_failure_blocked_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    return s_rrsm_do_unsubscribe_test(allocator, false);
}

AWS_TEST_CASE(rrsm_release_unsubscribe_failure_blocked, s_rrsm_release_unsubscribe_failure_blocked_fn)

static int s_aws_mqtt_protocol_adapter_mock_subscribe_fails(
    void *impl,
    struct aws_protocol_adapter_subscribe_options *options) {
    (void)impl;
    (void)options;

    return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
}

static int s_rrsm_acquire_failure_subscribe_sync_failure_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct aws_mqtt_protocol_adapter_vtable failing_vtable = s_protocol_adapter_mock_vtable;
    failing_vtable.aws_mqtt_protocol_adapter_subscribe_fn = s_aws_mqtt_protocol_adapter_mock_subscribe_fails;

    struct aws_subscription_manager_test_fixture_options fixture_config = {
        .max_subscriptions = 3,
        .operation_timeout_seconds = DEFAULT_SM_TEST_TIMEOUT,
        .start_connected = true,
        .adapter_vtable = &failing_vtable,
    };

    struct aws_subscription_manager_test_fixture fixture;
    ASSERT_SUCCESS(s_aws_subscription_manager_test_fixture_init(&fixture, allocator, &fixture_config));

    struct aws_rr_subscription_manager *manager = &fixture.subscription_manager;

    struct aws_rr_acquire_subscription_options acquire_options = {
        .type = ARRST_REQUEST_RESPONSE,
        .topic_filter = aws_byte_cursor_from_c_str("hello/world"),
        .operation_id = 1,
    };
    ASSERT_INT_EQUALS(AASRT_FAILURE, aws_rr_subscription_manager_acquire_subscription(manager, &acquire_options));

    s_aws_subscription_manager_test_fixture_clean_up(&fixture);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(rrsm_acquire_failure_subscribe_sync_failure, s_rrsm_acquire_failure_subscribe_sync_failure_fn)

static int s_rrsm_acquire_subscribe_failure_event_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct aws_subscription_manager_test_fixture fixture;
    ASSERT_SUCCESS(s_aws_subscription_manager_test_fixture_init(&fixture, allocator, NULL));

    struct aws_rr_subscription_manager *manager = &fixture.subscription_manager;

    struct aws_rr_acquire_subscription_options acquire1_options = {
        .type = ARRST_REQUEST_RESPONSE,
        .topic_filter = aws_byte_cursor_from_c_str("hello/world"),
        .operation_id = 1,
    };
    ASSERT_INT_EQUALS(AASRT_SUBSCRIBING, aws_rr_subscription_manager_acquire_subscription(manager, &acquire1_options));

    // complete the subscribe with a failure
    struct aws_protocol_adapter_subscription_event failed_subscription_event = {
        .topic_filter = aws_byte_cursor_from_c_str("hello/world"),
        .event_type = AWS_PASET_SUBSCRIBE,
        .error_code = AWS_ERROR_MQTT_PROTOCOL_ADAPTER_FAILING_REASON_CODE,
    };
    aws_rr_subscription_manager_on_protocol_adapter_subscription_event(manager, &failed_subscription_event);

    // verify subscribe failure event emission
    struct aws_subscription_status_record expected_subscription_event = {
        .type = ARRSET_SUBSCRIPTION_SUBSCRIBE_FAILURE,
        .topic_filter_cursor = aws_byte_cursor_from_c_str("hello/world"),
        .operation_id = 1,
    };

    ASSERT_TRUE(s_contains_subscription_event_record(&fixture, &expected_subscription_event));

    s_aws_subscription_manager_test_fixture_clean_up(&fixture);
    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(rrsm_acquire_subscribe_failure_event, s_rrsm_acquire_subscribe_failure_event_fn)
