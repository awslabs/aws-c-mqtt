/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>
#include <aws/mqtt/private/v5/mqtt5_topic_alias.h>

#include <aws/common/byte_buf.h>
#include <aws/common/string.h>

#include <aws/testing/aws_test_harness.h>

AWS_STATIC_STRING_FROM_LITERAL(s_topic1, "hello/world");
AWS_STATIC_STRING_FROM_LITERAL(s_topic2, "this/is/a/longer/topic");

static int s_mqtt5_inbound_topic_alias_register_failure_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_mqtt5_inbound_topic_alias_manager inbound_alias_manager;
    ASSERT_SUCCESS(aws_mqtt5_inbound_topic_alias_manager_init(&inbound_alias_manager, allocator));

    ASSERT_SUCCESS(aws_mqtt5_inbound_topic_alias_manager_reset(&inbound_alias_manager, 10));

    ASSERT_FAILS(aws_mqtt5_inbound_topic_alias_manager_register_alias(
        &inbound_alias_manager, 0, aws_byte_cursor_from_string(s_topic1)));
    ASSERT_FAILS(aws_mqtt5_inbound_topic_alias_manager_register_alias(
        &inbound_alias_manager, 11, aws_byte_cursor_from_string(s_topic1)));

    aws_mqtt5_inbound_topic_alias_manager_clean_up(&inbound_alias_manager);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_inbound_topic_alias_register_failure, s_mqtt5_inbound_topic_alias_register_failure_fn)

static int s_mqtt5_inbound_topic_alias_resolve_success_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_mqtt5_inbound_topic_alias_manager inbound_alias_manager;
    ASSERT_SUCCESS(aws_mqtt5_inbound_topic_alias_manager_init(&inbound_alias_manager, allocator));

    ASSERT_SUCCESS(aws_mqtt5_inbound_topic_alias_manager_reset(&inbound_alias_manager, 10));

    ASSERT_SUCCESS(aws_mqtt5_inbound_topic_alias_manager_register_alias(
        &inbound_alias_manager, 1, aws_byte_cursor_from_string(s_topic1)));
    ASSERT_SUCCESS(aws_mqtt5_inbound_topic_alias_manager_register_alias(
        &inbound_alias_manager, 10, aws_byte_cursor_from_string(s_topic2)));

    struct aws_byte_cursor topic1;
    ASSERT_SUCCESS(aws_mqtt5_inbound_topic_alias_manager_resolve_alias(&inbound_alias_manager, 1, &topic1));
    ASSERT_BIN_ARRAYS_EQUALS(s_topic1->bytes, s_topic1->len, topic1.ptr, topic1.len);

    struct aws_byte_cursor topic2;
    ASSERT_SUCCESS(aws_mqtt5_inbound_topic_alias_manager_resolve_alias(&inbound_alias_manager, 10, &topic2));
    ASSERT_BIN_ARRAYS_EQUALS(s_topic2->bytes, s_topic2->len, topic2.ptr, topic2.len);

    /* overwrite an existing alias to verify memory is cleaned up */
    ASSERT_SUCCESS(aws_mqtt5_inbound_topic_alias_manager_register_alias(
        &inbound_alias_manager, 10, aws_byte_cursor_from_string(s_topic1)));

    struct aws_byte_cursor topic3;
    ASSERT_SUCCESS(aws_mqtt5_inbound_topic_alias_manager_resolve_alias(&inbound_alias_manager, 10, &topic3));
    ASSERT_BIN_ARRAYS_EQUALS(s_topic1->bytes, s_topic1->len, topic3.ptr, topic3.len);

    aws_mqtt5_inbound_topic_alias_manager_clean_up(&inbound_alias_manager);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_inbound_topic_alias_resolve_success, s_mqtt5_inbound_topic_alias_resolve_success_fn)

static int s_mqtt5_inbound_topic_alias_resolve_failure_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_mqtt5_inbound_topic_alias_manager inbound_alias_manager;
    ASSERT_SUCCESS(aws_mqtt5_inbound_topic_alias_manager_init(&inbound_alias_manager, allocator));

    ASSERT_SUCCESS(aws_mqtt5_inbound_topic_alias_manager_reset(&inbound_alias_manager, 10));

    struct aws_byte_cursor topic;
    ASSERT_FAILS(aws_mqtt5_inbound_topic_alias_manager_resolve_alias(&inbound_alias_manager, 0, &topic));
    ASSERT_FAILS(aws_mqtt5_inbound_topic_alias_manager_resolve_alias(&inbound_alias_manager, 11, &topic));
    ASSERT_FAILS(aws_mqtt5_inbound_topic_alias_manager_resolve_alias(&inbound_alias_manager, 10, &topic));

    aws_mqtt5_inbound_topic_alias_manager_clean_up(&inbound_alias_manager);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_inbound_topic_alias_resolve_failure, s_mqtt5_inbound_topic_alias_resolve_failure_fn)

static int s_mqtt5_inbound_topic_alias_reset_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_mqtt5_inbound_topic_alias_manager inbound_alias_manager;
    ASSERT_SUCCESS(aws_mqtt5_inbound_topic_alias_manager_init(&inbound_alias_manager, allocator));

    ASSERT_SUCCESS(aws_mqtt5_inbound_topic_alias_manager_reset(&inbound_alias_manager, 10));

    ASSERT_SUCCESS(aws_mqtt5_inbound_topic_alias_manager_register_alias(
        &inbound_alias_manager, 1, aws_byte_cursor_from_string(s_topic1)));

    struct aws_byte_cursor topic;
    ASSERT_SUCCESS(aws_mqtt5_inbound_topic_alias_manager_resolve_alias(&inbound_alias_manager, 1, &topic));
    ASSERT_BIN_ARRAYS_EQUALS(s_topic1->bytes, s_topic1->len, topic.ptr, topic.len);

    ASSERT_SUCCESS(aws_mqtt5_inbound_topic_alias_manager_reset(&inbound_alias_manager, 10));

    ASSERT_FAILS(aws_mqtt5_inbound_topic_alias_manager_resolve_alias(&inbound_alias_manager, 1, &topic));

    aws_mqtt5_inbound_topic_alias_manager_clean_up(&inbound_alias_manager);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_inbound_topic_alias_reset, s_mqtt5_inbound_topic_alias_reset_fn)
