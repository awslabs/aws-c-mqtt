/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/testing/aws_test_harness.h>

#include <aws/common/clock.h>
#include <aws/mqtt/client.h>

#include "mqtt5_testing_utils.h"

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
