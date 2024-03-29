/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/clock.h>
#include <aws/mqtt/private/client_impl_shared.h>
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

    struct aws_hash_table request_response_records;
    struct aws_hash_table streaming_records;
};

struct aws_rr_client_fixture_request_response_record {
    struct aws_allocator *allocator;

    struct aws_rr_client_test_fixture *fixture;

    struct aws_byte_cursor payload_cursor;

    struct aws_byte_buf payload;

    bool completed;
    int error_code;
    struct aws_byte_buf response;
};

struct aws_rr_client_fixture_request_response_record *s_aws_rr_client_fixture_request_response_record_new(
    struct aws_allocator *allocator,
    struct aws_rr_client_test_fixture *fixture,
    struct aws_byte_cursor request_payload) {
    struct aws_rr_client_fixture_request_response_record *record =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_rr_client_fixture_request_response_record));

    record->allocator = allocator;
    record->fixture = fixture;

    aws_byte_buf_init_copy_from_cursor(&record->payload, allocator, request_payload);
    record->payload_cursor = aws_byte_cursor_from_buf(&record->payload);

    return record;
}

void s_aws_rr_client_fixture_request_response_record_delete(
    struct aws_rr_client_fixture_request_response_record *record) {
    aws_byte_buf_clean_up(&record->payload);
    aws_byte_buf_clean_up(&record->response);

    aws_mem_release(record->allocator, record);
}

static void s_aws_rr_client_fixture_request_response_record_hash_destroy(void *element) {
    struct aws_rr_client_fixture_request_response_record *record = element;

    s_aws_rr_client_fixture_request_response_record_delete(record);
}

static void s_rrc_fixture_request_completion_callback(
    struct aws_byte_cursor *payload,
    int error_code,
    void *user_data) {
    struct aws_rr_client_fixture_request_response_record *record = user_data;
    struct aws_rr_client_test_fixture *fixture = record->fixture;

    aws_mutex_lock(&fixture->lock);

    if (payload != NULL) {
        AWS_FATAL_ASSERT(error_code == AWS_ERROR_SUCCESS);

        aws_byte_buf_init_copy_from_cursor(&record->response, fixture->allocator, *payload);
    } else {
        AWS_FATAL_ASSERT(error_code != AWS_ERROR_SUCCESS);
        record->error_code = error_code;
    }

    record->completed = true;

    aws_mutex_unlock(&fixture->lock);
    aws_condition_variable_notify_all(&fixture->signal);
}

static struct aws_rr_client_fixture_request_response_record *s_rrc_fixture_add_request_record(
    struct aws_rr_client_test_fixture *fixture,
    struct aws_byte_cursor request_payload) {
    struct aws_rr_client_fixture_request_response_record *record =
        s_aws_rr_client_fixture_request_response_record_new(fixture->allocator, fixture, request_payload);

    aws_hash_table_put(&fixture->request_response_records, &record->payload_cursor, record, NULL);

    return record;
}

struct rrc_operation_completion_context {
    struct aws_byte_cursor key;
    struct aws_rr_client_test_fixture *fixture;
};

static bool s_is_request_complete(void *context) {
    struct rrc_operation_completion_context *completion_context = context;

    struct aws_hash_element *element = NULL;
    aws_hash_table_find(&completion_context->fixture->request_response_records, &completion_context->key, &element);

    AWS_FATAL_ASSERT(element != NULL && element->value != NULL);

    struct aws_rr_client_fixture_request_response_record *record = element->value;

    return record->completed;
}

static void s_rrc_wait_on_request_completion(
    struct aws_rr_client_test_fixture *fixture,
    struct aws_byte_cursor request_payload) {
    struct rrc_operation_completion_context context = {
        .key = request_payload,
        .fixture = fixture,
    };

    aws_mutex_lock(&fixture->lock);
    aws_condition_variable_wait_pred(&fixture->signal, &fixture->lock, s_is_request_complete, &context);
    aws_mutex_unlock(&fixture->lock);
}

static int s_rrc_verify_request_completion(
    struct aws_rr_client_test_fixture *fixture,
    struct aws_byte_cursor request_payload,
    int expected_error_code,
    struct aws_byte_cursor *expected_response) {
    aws_mutex_lock(&fixture->lock);

    struct aws_hash_element *element = NULL;
    aws_hash_table_find(&fixture->request_response_records, &request_payload, &element);

    AWS_FATAL_ASSERT(element != NULL && element->value != NULL);

    struct aws_rr_client_fixture_request_response_record *record = element->value;

    ASSERT_INT_EQUALS(expected_error_code, record->error_code);

    if (expected_response != NULL) {
        struct aws_byte_cursor actual_payload = aws_byte_cursor_from_buf(&record->response);
        ASSERT_TRUE(aws_byte_cursor_eq(expected_response, &actual_payload));
    } else {
        ASSERT_INT_EQUALS(0, record->response.len);
    }

    aws_mutex_unlock(&fixture->lock);

    return AWS_OP_SUCCESS;
}

struct aws_rr_client_fixture_streaming_record {
    struct aws_allocator *allocator;

    struct aws_rr_client_test_fixture *fixture;

    struct aws_byte_cursor record_key_cursor;
    struct aws_byte_buf record_key;

    struct aws_array_list publishes;
    struct aws_array_list subscription_events;

    bool terminated;
};

struct aws_rr_client_fixture_streaming_record_subscription_event {
    enum aws_rr_streaming_subscription_event_type status;
    int error_code;
};

struct aws_rr_client_fixture_streaming_record *s_aws_rr_client_fixture_streaming_record_new(
    struct aws_allocator *allocator,
    struct aws_rr_client_test_fixture *fixture,
    struct aws_byte_cursor record_key) {
    struct aws_rr_client_fixture_streaming_record *record =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_rr_client_fixture_streaming_record));

    record->allocator = allocator;
    record->fixture = fixture;

    aws_byte_buf_init_copy_from_cursor(&record->record_key, allocator, record_key);
    record->record_key_cursor = aws_byte_cursor_from_buf(&record->record_key);

    aws_array_list_init_dynamic(&record->publishes, allocator, 10, sizeof(struct aws_byte_buf));
    aws_array_list_init_dynamic(
        &record->subscription_events,
        allocator,
        10,
        sizeof(struct aws_rr_client_fixture_streaming_record_subscription_event));

    return record;
}

void s_aws_rr_client_fixture_streaming_record_delete(struct aws_rr_client_fixture_streaming_record *record) {
    aws_byte_buf_clean_up(&record->record_key);

    size_t publish_count = aws_array_list_length(&record->publishes);
    for (size_t i = 0; i < publish_count; ++i) {
        struct aws_byte_buf publish_payload;
        aws_array_list_get_at(&record->publishes, &publish_payload, i);

        aws_byte_buf_clean_up(&publish_payload);
    }

    aws_array_list_clean_up(&record->publishes);
    aws_array_list_clean_up(&record->subscription_events);

    aws_mem_release(record->allocator, record);
}

static void s_aws_rr_client_fixture_streaming_record_hash_destroy(void *element) {
    struct aws_rr_client_fixture_streaming_record *record = element;

    s_aws_rr_client_fixture_streaming_record_delete(record);
}

static void s_rrc_fixture_streaming_operation_subscription_status_callback(
    enum aws_rr_streaming_subscription_event_type status,
    int error_code,
    void *user_data) {

    struct aws_rr_client_fixture_streaming_record *record = user_data;
    struct aws_rr_client_test_fixture *fixture = record->fixture;

    aws_mutex_lock(&fixture->lock);

    struct aws_rr_client_fixture_streaming_record_subscription_event event = {
        .status = status,
        .error_code = error_code,
    };
    aws_array_list_push_back(&record->subscription_events, &event);

    aws_mutex_unlock(&fixture->lock);
    aws_condition_variable_notify_all(&fixture->signal);
}

static void s_rrc_fixture_streaming_operation_incoming_publish_callback(
    struct aws_byte_cursor payload,
    void *user_data) {
    struct aws_rr_client_fixture_streaming_record *record = user_data;
    struct aws_rr_client_test_fixture *fixture = record->fixture;

    aws_mutex_lock(&fixture->lock);

    struct aws_byte_buf payload_buffer;
    aws_byte_buf_init_copy_from_cursor(&payload_buffer, fixture->allocator, payload);

    aws_array_list_push_back(&record->publishes, &payload_buffer);

    aws_mutex_unlock(&fixture->lock);
    aws_condition_variable_notify_all(&fixture->signal);
}

static void s_rrc_fixture_streaming_operation_terminated_callback(void *user_data) {
    struct aws_rr_client_fixture_streaming_record *record = user_data;
    struct aws_rr_client_test_fixture *fixture = record->fixture;

    aws_mutex_lock(&fixture->lock);

    record->terminated = true;

    aws_mutex_unlock(&fixture->lock);
    aws_condition_variable_notify_all(&fixture->signal);
}

static struct aws_rr_client_fixture_streaming_record *s_rrc_fixture_add_streaming_record(
    struct aws_rr_client_test_fixture *fixture,
    struct aws_byte_cursor key) {
    struct aws_rr_client_fixture_streaming_record *record =
        s_aws_rr_client_fixture_streaming_record_new(fixture->allocator, fixture, key);

    aws_hash_table_put(&fixture->streaming_records, &record->record_key, record, NULL);

    return record;
}

static bool s_is_stream_terminated(void *context) {
    struct rrc_operation_completion_context *completion_context = context;

    struct aws_hash_element *element = NULL;
    aws_hash_table_find(&completion_context->fixture->streaming_records, &completion_context->key, &element);

    AWS_FATAL_ASSERT(element != NULL && element->value != NULL);

    struct aws_rr_client_fixture_streaming_record *record = element->value;

    return record->terminated;
}

static void s_rrc_wait_on_streaming_termination(
    struct aws_rr_client_test_fixture *fixture,
    struct aws_byte_cursor key) {
    struct rrc_operation_completion_context context = {
        .key = key,
        .fixture = fixture,
    };

    aws_mutex_lock(&fixture->lock);
    aws_condition_variable_wait_pred(&fixture->signal, &fixture->lock, s_is_stream_terminated, &context);
    aws_mutex_unlock(&fixture->lock);
}

struct rrc_streaming_event_wait_context {
    struct aws_byte_cursor operation_key;
    struct aws_rr_client_test_fixture *fixture;
    size_t event_count;
};

static bool s_streaming_operation_has_n_publishes(void *context) {
    struct rrc_streaming_event_wait_context *streaming_publish_context = context;

    struct aws_hash_element *element = NULL;
    aws_hash_table_find(
        &streaming_publish_context->fixture->streaming_records, &streaming_publish_context->operation_key, &element);

    AWS_FATAL_ASSERT(element != NULL && element->value != NULL);

    struct aws_rr_client_fixture_streaming_record *record = element->value;

    return aws_array_list_length(&record->publishes) >= streaming_publish_context->event_count;
}

static void s_rrc_wait_for_n_streaming_publishes(
    struct aws_rr_client_test_fixture *fixture,
    struct aws_byte_cursor key,
    size_t count) {
    struct rrc_streaming_event_wait_context context = {
        .operation_key = key,
        .fixture = fixture,
        .event_count = count,
    };

    aws_mutex_lock(&fixture->lock);
    aws_condition_variable_wait_pred(&fixture->signal, &fixture->lock, s_streaming_operation_has_n_publishes, &context);
    aws_mutex_unlock(&fixture->lock);
}

static int s_rrc_verify_streaming_publishes(
    struct aws_rr_client_test_fixture *fixture,
    struct aws_byte_cursor key,
    size_t expected_publish_count,
    struct aws_byte_cursor *expected_publishes) {

    aws_mutex_lock(&fixture->lock);

    struct aws_hash_element *element = NULL;
    aws_hash_table_find(&fixture->streaming_records, &key, &element);

    AWS_FATAL_ASSERT(element != NULL && element->value != NULL);

    struct aws_rr_client_fixture_streaming_record *record = element->value;

    size_t actual_publish_count = aws_array_list_length(&record->publishes);
    ASSERT_INT_EQUALS(expected_publish_count, actual_publish_count);

    for (size_t i = 0; i < actual_publish_count; ++i) {
        struct aws_byte_buf actual_payload;
        aws_array_list_get_at(&record->publishes, &actual_payload, i);

        struct aws_byte_cursor *expected_payload = &expected_publishes[i];

        ASSERT_BIN_ARRAYS_EQUALS(
            expected_payload->ptr, expected_payload->len, actual_payload.buffer, actual_payload.len);
    }

    aws_mutex_unlock(&fixture->lock);

    return AWS_OP_SUCCESS;
}

static bool s_streaming_operation_has_n_subscription_events(void *context) {
    struct rrc_streaming_event_wait_context *streaming_publish_context = context;

    struct aws_hash_element *element = NULL;
    aws_hash_table_find(
        &streaming_publish_context->fixture->streaming_records, &streaming_publish_context->operation_key, &element);

    AWS_FATAL_ASSERT(element != NULL && element->value != NULL);

    struct aws_rr_client_fixture_streaming_record *record = element->value;

    return aws_array_list_length(&record->subscription_events) >= streaming_publish_context->event_count;
}

static void s_rrc_wait_for_n_streaming_subscription_events(
    struct aws_rr_client_test_fixture *fixture,
    struct aws_byte_cursor key,
    size_t count) {
    struct rrc_streaming_event_wait_context context = {
        .operation_key = key,
        .fixture = fixture,
        .event_count = count,
    };

    aws_mutex_lock(&fixture->lock);
    aws_condition_variable_wait_pred(
        &fixture->signal, &fixture->lock, s_streaming_operation_has_n_subscription_events, &context);
    aws_mutex_unlock(&fixture->lock);
}

static int s_rrc_verify_streaming_record_subscription_events(
    struct aws_rr_client_test_fixture *fixture,
    struct aws_byte_cursor key,
    size_t expected_subscription_event_count,
    struct aws_rr_client_fixture_streaming_record_subscription_event *expected_subscription_events) {
    aws_mutex_lock(&fixture->lock);

    struct aws_hash_element *element = NULL;
    aws_hash_table_find(&fixture->streaming_records, &key, &element);

    AWS_FATAL_ASSERT(element != NULL && element->value != NULL);

    struct aws_rr_client_fixture_streaming_record *record = element->value;

    size_t actual_subscription_event_count = aws_array_list_length(&record->subscription_events);
    ASSERT_INT_EQUALS(expected_subscription_event_count, actual_subscription_event_count);

    for (size_t i = 0; i < actual_subscription_event_count; ++i) {
        struct aws_rr_client_fixture_streaming_record_subscription_event actual_event;
        aws_array_list_get_at(&record->subscription_events, &actual_event, i);

        struct aws_rr_client_fixture_streaming_record_subscription_event *expected_event =
            &expected_subscription_events[i];

        ASSERT_INT_EQUALS(expected_event->status, actual_event.status);
        ASSERT_INT_EQUALS(expected_event->error_code, actual_event.error_code);
    }

    aws_mutex_unlock(&fixture->lock);

    return AWS_OP_SUCCESS;
}

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

    aws_hash_table_init(
        &fixture->request_response_records,
        allocator,
        10,
        aws_hash_byte_cursor_ptr,
        aws_mqtt_byte_cursor_hash_equality,
        NULL,
        s_aws_rr_client_fixture_request_response_record_hash_destroy);

    aws_hash_table_init(
        &fixture->streaming_records,
        allocator,
        10,
        aws_hash_byte_cursor_ptr,
        aws_mqtt_byte_cursor_hash_equality,
        NULL,
        s_aws_rr_client_fixture_streaming_record_hash_destroy);

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

    aws_hash_table_init(
        &fixture->request_response_records,
        allocator,
        10,
        aws_hash_byte_cursor_ptr,
        aws_mqtt_byte_cursor_hash_equality,
        NULL,
        s_aws_rr_client_fixture_request_response_record_hash_destroy);

    aws_hash_table_init(
        &fixture->streaming_records,
        allocator,
        10,
        aws_hash_byte_cursor_ptr,
        aws_mqtt_byte_cursor_hash_equality,
        NULL,
        s_aws_rr_client_fixture_streaming_record_hash_destroy);

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

    aws_hash_table_clean_up(&fixture->request_response_records);
    aws_hash_table_clean_up(&fixture->streaming_records);
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

static int s_rrc_do_submit_request_operation_failure_test(
    struct aws_allocator *allocator,
    void (*request_mutator_fn)(struct aws_mqtt_request_operation_options *)) {
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

    struct aws_mqtt_request_operation_response_path response_paths[] = {
        {
            .topic = aws_byte_cursor_from_c_str("response/filter/accepted"),
            .correlation_token_json_path = aws_byte_cursor_from_c_str("client_token"),
        },
        {
            .topic = aws_byte_cursor_from_c_str("response/filter/rejected"),
            .correlation_token_json_path = aws_byte_cursor_from_c_str("client_token"),
        },
    };
    struct aws_mqtt_request_operation_options good_request = {
        .subscription_topic_filter = aws_byte_cursor_from_c_str("response/filter/+"),
        .response_paths = response_paths,
        .response_path_count = AWS_ARRAY_SIZE(response_paths),
        .publish_topic = aws_byte_cursor_from_c_str("get/shadow"),
        .serialized_request = aws_byte_cursor_from_c_str("{}"),
        .correlation_token = aws_byte_cursor_from_c_str("MyRequest#1"),
    };
    ASSERT_SUCCESS(aws_mqtt_request_response_client_submit_request(fixture.rr_client, &good_request));

    struct aws_mqtt_request_operation_options bad_request = good_request;
    (*request_mutator_fn)(&bad_request);

    ASSERT_FAILS(aws_mqtt_request_response_client_submit_request(fixture.rr_client, &bad_request));

    s_aws_rr_client_test_fixture_clean_up(&fixture);

    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

static void s_no_response_paths_mutator(struct aws_mqtt_request_operation_options *request_options) {
    request_options->response_path_count = 0;
    request_options->response_paths = NULL;
}

static int s_rrc_submit_request_operation_failure_no_response_paths_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    return s_rrc_do_submit_request_operation_failure_test(allocator, s_no_response_paths_mutator);
}

AWS_TEST_CASE(
    rrc_submit_request_operation_failure_no_response_paths,
    s_rrc_submit_request_operation_failure_no_response_paths_fn)

static void s_invalid_response_topic_mutator(struct aws_mqtt_request_operation_options *request_options) {
    request_options->response_paths[0].topic = aws_byte_cursor_from_c_str("a/b/#");
}

static int s_rrc_submit_request_operation_failure_invalid_response_topic_fn(
    struct aws_allocator *allocator,
    void *ctx) {
    (void)ctx;

    return s_rrc_do_submit_request_operation_failure_test(allocator, s_invalid_response_topic_mutator);
}

AWS_TEST_CASE(
    rrc_submit_request_operation_failure_invalid_response_topic,
    s_rrc_submit_request_operation_failure_invalid_response_topic_fn)

static void s_invalid_response_correlation_token_path_mutator(
    struct aws_mqtt_request_operation_options *request_options) {
    request_options->response_paths[0].correlation_token_json_path = aws_byte_cursor_from_c_str("");
}

static int s_rrc_submit_request_operation_failure_invalid_response_correlation_token_path_fn(
    struct aws_allocator *allocator,
    void *ctx) {
    (void)ctx;

    return s_rrc_do_submit_request_operation_failure_test(allocator, s_invalid_response_correlation_token_path_mutator);
}

AWS_TEST_CASE(
    rrc_submit_request_operation_failure_invalid_response_correlation_token_path,
    s_rrc_submit_request_operation_failure_invalid_response_correlation_token_path_fn)

static void s_no_correlation_token_mutator(struct aws_mqtt_request_operation_options *request_options) {
    request_options->correlation_token = aws_byte_cursor_from_c_str("");
}

static int s_rrc_submit_request_operation_failure_no_correlation_token_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    return s_rrc_do_submit_request_operation_failure_test(allocator, s_no_correlation_token_mutator);
}

AWS_TEST_CASE(
    rrc_submit_request_operation_failure_no_correlation_token,
    s_rrc_submit_request_operation_failure_no_correlation_token_fn)

static void s_invalid_publish_topic_mutator(struct aws_mqtt_request_operation_options *request_options) {
    request_options->publish_topic = aws_byte_cursor_from_c_str("a/b/#");
}

static int s_rrc_submit_request_operation_failure_invalid_publish_topic_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    return s_rrc_do_submit_request_operation_failure_test(allocator, s_invalid_publish_topic_mutator);
}

AWS_TEST_CASE(
    rrc_submit_request_operation_failure_invalid_publish_topic,
    s_rrc_submit_request_operation_failure_invalid_publish_topic_fn)

static void s_empty_request_mutator(struct aws_mqtt_request_operation_options *request_options) {
    request_options->serialized_request = aws_byte_cursor_from_c_str("");
}

static int s_rrc_submit_request_operation_failure_empty_request_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    return s_rrc_do_submit_request_operation_failure_test(allocator, s_empty_request_mutator);
}

AWS_TEST_CASE(
    rrc_submit_request_operation_failure_empty_request,
    s_rrc_submit_request_operation_failure_empty_request_fn)

static int s_rrc_submit_streaming_operation_failure_invalid_subscription_topic_filter_fn(
    struct aws_allocator *allocator,
    void *ctx) {
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

    struct aws_mqtt_streaming_operation_options good_options = {
        .topic_filter = aws_byte_cursor_from_c_str("a/b"),
    };

    struct aws_mqtt_rr_client_operation *good_operation =
        aws_mqtt_request_response_client_create_streaming_operation(fixture.rr_client, &good_options);
    ASSERT_NOT_NULL(good_operation);

    aws_mqtt_rr_client_operation_release(good_operation);

    struct aws_mqtt_streaming_operation_options bad_options = good_options;
    bad_options.topic_filter = aws_byte_cursor_from_c_str("");

    struct aws_mqtt_rr_client_operation *bad_operation =
        aws_mqtt_request_response_client_create_streaming_operation(fixture.rr_client, &bad_options);
    ASSERT_NULL(bad_operation);

    s_aws_rr_client_test_fixture_clean_up(&fixture);

    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(
    rrc_submit_streaming_operation_failure_invalid_subscription_topic_filter,
    s_rrc_submit_streaming_operation_failure_invalid_subscription_topic_filter_fn)

static int s_do_rrc_single_request_operation_test_fn(
    struct aws_allocator *allocator,
    struct aws_mqtt_request_response_client_options *rr_client_options,
    struct aws_mqtt_request_operation_options *request_options,
    int expected_error_code,
    struct aws_byte_cursor *expected_payload,
    bool shutdown_after_submit) {
    aws_mqtt_library_init(allocator);

    struct mqtt5_client_test_options client_test_options;
    aws_mqtt5_client_test_init_default_options(&client_test_options);

    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options client_test_fixture_options = {
        .client_options = &client_test_options.client_options,
        .server_function_table = &client_test_options.server_function_table,
    };

    struct aws_rr_client_test_fixture fixture;
    ASSERT_SUCCESS(s_aws_rr_client_test_fixture_init_from_mqtt5(
        &fixture, allocator, rr_client_options, &client_test_fixture_options, NULL));

    struct aws_rr_client_fixture_request_response_record *record =
        s_rrc_fixture_add_request_record(&fixture, request_options->serialized_request);

    request_options->completion_callback = s_rrc_fixture_request_completion_callback;
    request_options->user_data = record;

    ASSERT_SUCCESS(aws_mqtt_request_response_client_submit_request(fixture.rr_client, request_options));

    if (shutdown_after_submit) {
        aws_mqtt_request_response_client_release(fixture.rr_client);
        fixture.rr_client = NULL;
    }

    s_rrc_wait_on_request_completion(&fixture, request_options->serialized_request);

    ASSERT_SUCCESS(s_rrc_verify_request_completion(
        &fixture, request_options->serialized_request, expected_error_code, expected_payload));

    s_aws_rr_client_test_fixture_clean_up(&fixture);

    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

static int s_rrc_submit_request_operation_failure_by_shutdown_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_mqtt_request_operation_response_path response_paths[] = {
        {
            .topic = aws_byte_cursor_from_c_str("response/filter/accepted"),
            .correlation_token_json_path = aws_byte_cursor_from_c_str("client_token"),
        },
    };

    struct aws_mqtt_request_operation_options request = {
        .subscription_topic_filter = aws_byte_cursor_from_c_str("response/filter/+"),
        .response_paths = response_paths,
        .response_path_count = AWS_ARRAY_SIZE(response_paths),
        .publish_topic = aws_byte_cursor_from_c_str("get/shadow"),
        .serialized_request = aws_byte_cursor_from_c_str("request1"),
        .correlation_token = aws_byte_cursor_from_c_str("MyRequest#1"),
    };

    return s_do_rrc_single_request_operation_test_fn(
        allocator, NULL, &request, AWS_ERROR_MQTT_REQUEST_RESPONSE_CLIENT_SHUT_DOWN, NULL, true);
}

AWS_TEST_CASE(rrc_submit_request_operation_failure_by_shutdown, s_rrc_submit_request_operation_failure_by_shutdown_fn)

static int s_do_rrc_single_streaming_operation_test_fn(
    struct aws_allocator *allocator,
    struct aws_mqtt_request_response_client_options *rr_client_options,
    struct aws_mqtt_streaming_operation_options *streaming_options,
    size_t expected_subscription_event_count,
    struct aws_rr_client_fixture_streaming_record_subscription_event *expected_subscription_events,
    bool shutdown_after_submit) {
    aws_mqtt_library_init(allocator);

    struct mqtt5_client_test_options client_test_options;
    aws_mqtt5_client_test_init_default_options(&client_test_options);

    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options client_test_fixture_options = {
        .client_options = &client_test_options.client_options,
        .server_function_table = &client_test_options.server_function_table,
    };

    struct aws_rr_client_test_fixture fixture;
    ASSERT_SUCCESS(s_aws_rr_client_test_fixture_init_from_mqtt5(
        &fixture, allocator, rr_client_options, &client_test_fixture_options, NULL));

    struct aws_byte_cursor streaming_id = aws_byte_cursor_from_c_str("streaming1");
    struct aws_rr_client_fixture_streaming_record *record = s_rrc_fixture_add_streaming_record(&fixture, streaming_id);

    streaming_options->incoming_publish_callback = s_rrc_fixture_streaming_operation_incoming_publish_callback;
    streaming_options->subscription_status_callback = s_rrc_fixture_streaming_operation_subscription_status_callback;
    streaming_options->terminated_callback = s_rrc_fixture_streaming_operation_terminated_callback;
    streaming_options->user_data = record;

    struct aws_mqtt_rr_client_operation *streaming_operation =
        aws_mqtt_request_response_client_create_streaming_operation(fixture.rr_client, streaming_options);
    ASSERT_NOT_NULL(streaming_operation);

    if (shutdown_after_submit) {
        aws_mqtt_request_response_client_release(fixture.rr_client);
        fixture.rr_client = NULL;

        /*
         * Extremely awkward sleep:
         *
         * We've submitted the operation and we've decref'd the client to zero.  When the operation submit task
         * is processed, if the release in the succeeding line has happened-before the client external destroy task
         * has run, then the operation's destory will be scheduled in-thread and run ahead of the client external
         * destroy.  This doesn't break correctness, but it does prevent the client from emitting a HALTED event
         * on the subscription because the subscription/operation will be gone before the client external destroy
         * task runs.
         *
         * So we add a nice, fat sleep to guarantee that the client external destroy task runs before the operation
         * destroy task.
         */
        aws_thread_current_sleep(aws_timestamp_convert(1, AWS_TIMESTAMP_SECS, AWS_TIMESTAMP_NANOS, NULL));
        aws_mqtt_rr_client_operation_release(streaming_operation);
    }

    s_rrc_wait_on_streaming_termination(&fixture, streaming_id);

    ASSERT_SUCCESS(s_rrc_verify_streaming_record_subscription_events(
        &fixture, streaming_id, expected_subscription_event_count, expected_subscription_events));

    s_aws_rr_client_test_fixture_clean_up(&fixture);

    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

static int s_rrc_submit_streaming_operation_and_shutdown_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_rr_client_fixture_streaming_record_subscription_event expected_events[] = {
        {
            .status = ARRSSET_SUBSCRIPTION_HALTED,
            .error_code = AWS_ERROR_MQTT_REQUEST_RESPONSE_CLIENT_SHUT_DOWN,
        },
    };

    struct aws_mqtt_streaming_operation_options streaming_options = {
        .topic_filter = aws_byte_cursor_from_c_str("derp/filter"),
    };

    return s_do_rrc_single_streaming_operation_test_fn(
        allocator, NULL, &streaming_options, AWS_ARRAY_SIZE(expected_events), expected_events, true);
}

AWS_TEST_CASE(rrc_submit_streaming_operation_and_shutdown, s_rrc_submit_streaming_operation_and_shutdown_fn)

static int s_rrc_submit_request_operation_failure_by_timeout_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_mqtt_request_operation_response_path response_paths[] = {
        {
            .topic = aws_byte_cursor_from_c_str("response/filter/accepted"),
            .correlation_token_json_path = aws_byte_cursor_from_c_str("client_token"),
        },
    };

    struct aws_mqtt_request_operation_options request = {
        .subscription_topic_filter = aws_byte_cursor_from_c_str("response/filter/+"),
        .response_paths = response_paths,
        .response_path_count = AWS_ARRAY_SIZE(response_paths),
        .publish_topic = aws_byte_cursor_from_c_str("get/shadow"),
        .serialized_request = aws_byte_cursor_from_c_str("request1"),
        .correlation_token = aws_byte_cursor_from_c_str("MyRequest#1"),
    };

    struct aws_mqtt_request_response_client_options rr_client_options = {
        .max_subscriptions = 2,
        .operation_timeout_seconds = 2,
    };

    return s_do_rrc_single_request_operation_test_fn(
        allocator, &rr_client_options, &request, AWS_ERROR_MQTT_REQUEST_RESPONSE_TIMEOUT, NULL, false);
}

AWS_TEST_CASE(rrc_submit_request_operation_failure_by_timeout, s_rrc_submit_request_operation_failure_by_timeout_fn)

static struct aws_mqtt_rr_client_operation *s_create_streaming_operation(struct aws_rr_client_test_fixture *fixture, struct aws_byte_cursor record_key, struct aws_byte_cursor topic_filter) {
    struct aws_rr_client_fixture_streaming_record *record = s_rrc_fixture_add_streaming_record(&fixture, record_key);

    struct aws_mqtt_streaming_operation_options streaming_options = {
        .topic_filter = topic_filter,
    };
    streaming_options.incoming_publish_callback = s_rrc_fixture_streaming_operation_incoming_publish_callback;
    streaming_options.subscription_status_callback = s_rrc_fixture_streaming_operation_subscription_status_callback;
    streaming_options.terminated_callback = s_rrc_fixture_streaming_operation_terminated_callback;
    streaming_options.user_data = record;

    struct aws_mqtt_rr_client_operation *streaming_operation =
        aws_mqtt_request_response_client_create_streaming_operation(fixture->rr_client, &streaming_options);
    ASSERT_NOT_NULL(streaming_operation);

    return streaming_operation;
}

static int s_rrc_publish_5(struct aws_mqtt5_client *client, struct aws_byte_cursor topic, struct aws_byte_cursor payload) {


    return AWS_OP_SUCCESS;
}

static int s_rrc_publish_311(struct aws_mqtt_client_connection *connection, struct aws_byte_cursor topic, struct aws_byte_cursor payload) {

    
    return AWS_OP_SUCCESS;
}

static int s_rrc_protocol_client_publish(struct aws_rr_client_test_fixture *fixture, struct aws_byte_cursor topic, struct aws_byte_cursor payload) {

    if (fixture->test_protocol == RRCP_MQTT311) {
        return s_rrc_publish_5(fixture->client_test_fixture.mqtt311_test_fixture.mqtt_connection, topic, payload);
    } else {
        return s_rrc_publish_5(fixture->client_test_fixture.mqtt5_test_fixture.client, topic, payload);
    }
}

static int s_rrc_streaming_operation_success_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct mqtt5_client_test_options client_test_options;
    aws_mqtt5_client_test_init_default_options(&client_test_options);

    client_test_options.server_function_table.packet_handlers[AWS_MQTT5_PT_SUBSCRIBE] = ??;
    client_test_options.server_function_table.packet_handlers[AWS_MQTT5_PT_PUBLISH] = ??;

    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options client_test_fixture_options = {
        .client_options = &client_test_options.client_options,
        .server_function_table = &client_test_options.server_function_table,
    };

    struct aws_mqtt_request_response_client_options rr_client_options = {
        .max_subscriptions = 2,
        .operation_timeout_seconds = 2,
    };

    struct aws_rr_client_test_fixture fixture;
    ASSERT_SUCCESS(s_aws_rr_client_test_fixture_init_from_mqtt5(
        &fixture, allocator, &rr_client_options, &client_test_fixture_options, NULL));

    struct aws_byte_cursor record_key1 = aws_byte_cursor_from_c_str("key1");
    struct aws_byte_cursor topic_filter1 = aws_byte_cursor_from_c_str("topic/1");
    struct aws_mqtt_rr_client_operation *operation = s_create_streaming_operation(&fixture, record_key1, topic_filter1);

    s_rrc_wait_for_n_streaming_subscription_events(&fixture, record_key1, 1);

    struct aws_rr_client_fixture_streaming_record_subscription_event expected_events[] = {
        {
            .status = ARRSSET_SUBSCRIPTION_ESTABLISHED,
            .error_code = AWS_ERROR_SUCCESS,
        },
    };
    s_rrc_verify_streaming_record_subscription_events(&fixture, record_key1, AWS_ARRAY_SIZE(expected_events), expected_events);

    // two publishes on the mqtt client that get reflected into our subscription topic
    struct aws_byte_cursor payload1 = aws_byte_cursor_from_c_str("Payload1");
    struct aws_byte_cursor payload2 = aws_byte_cursor_from_c_str("Payload1");
    s_rrc_protocol_client_publish(&fixture, topic_filter1, payload1);
    s_rrc_protocol_client_publish(&fixture, topic_filter1, payload2);

    s_rrc_wait_for_n_streaming_publishes(&fixture, record_key1, 2);

    struct aws_byte_cursor expected_publishes[] = {
        payload1,
        payload2,
    };
    s_rrc_verify_streaming_publishes(&fixture, record_key1, AWS_ARRAY_SIZE(expected_publishes), expected_publishes);

    aws_mqtt_rr_client_operation_release(operation);

    s_aws_rr_client_test_fixture_clean_up(&fixture);

    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(rrc_streaming_operation_success, s_rrc_streaming_operation_success_fn)
