/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/string.h>

#include "mqtt5_testing_utils.h"
#include <aws/io/stream.h>
#include <aws/mqtt/mqtt.h>
#include <aws/mqtt/private/v5/mqtt5_options_storage.h>
#include <aws/mqtt/private/v5/mqtt5_utils.h>
#include <aws/mqtt/v5/mqtt5_types.h>

#include <aws/testing/aws_test_harness.h>

static int s_verify_user_properties(
    struct aws_mqtt5_user_property_set *property_set,
    size_t expected_count,
    const struct aws_mqtt5_user_property *expected_properties) {

    return aws_mqtt5_test_verify_user_properties_raw(
        aws_mqtt5_user_property_set_size(property_set),
        property_set->properties.data,
        expected_count,
        expected_properties);
}

static bool s_is_cursor_in_buffer(const struct aws_byte_buf *buffer, struct aws_byte_cursor cursor) {
    if (cursor.ptr < buffer->buffer) {
        return false;
    }

    if (cursor.ptr + cursor.len > buffer->buffer + buffer->len) {
        return false;
    }

    return true;
}

/*
 * a bunch of macros to simplify the verification of required and optional properties being properly propagated
 * and referenced within packet storage and packet views
 */

#define AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULL(storage_ptr, field_name)                                             \
    ASSERT_NULL((storage_ptr)->field_name##_ptr);                                                                      \
    ASSERT_NULL((storage_ptr)->storage_view.field_name);

#define AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_CURSOR(storage_ptr, view_ptr, field_name)                        \
    ASSERT_BIN_ARRAYS_EQUALS(                                                                                          \
        (view_ptr)->field_name->ptr,                                                                                   \
        (view_ptr)->field_name->len,                                                                                   \
        (storage_ptr)->field_name.ptr,                                                                                 \
        (storage_ptr)->field_name.len);                                                                                \
    ASSERT_BIN_ARRAYS_EQUALS(                                                                                          \
        (view_ptr)->field_name->ptr,                                                                                   \
        (view_ptr)->field_name->len,                                                                                   \
        (storage_ptr)->storage_view.field_name->ptr,                                                                   \
        (storage_ptr)->storage_view.field_name->len);                                                                  \
    ASSERT_TRUE(s_is_cursor_in_buffer(&(storage_ptr)->storage, ((storage_ptr)->field_name)));                          \
    ASSERT_TRUE(s_is_cursor_in_buffer(&(storage_ptr)->storage, *((storage_ptr)->storage_view.field_name)));            \
    ASSERT_TRUE((view_ptr)->field_name->ptr != (storage_ptr)->field_name.ptr);

#define AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_CURSOR(storage_ptr, view_ptr, field_name)                                 \
    ASSERT_BIN_ARRAYS_EQUALS(                                                                                          \
        (view_ptr)->field_name.ptr,                                                                                    \
        (view_ptr)->field_name.len,                                                                                    \
        (storage_ptr)->field_name.ptr,                                                                                 \
        (storage_ptr)->field_name.len);                                                                                \
    ASSERT_BIN_ARRAYS_EQUALS(                                                                                          \
        (view_ptr)->field_name.ptr,                                                                                    \
        (view_ptr)->field_name.len,                                                                                    \
        (storage_ptr)->storage_view.field_name.ptr,                                                                    \
        (storage_ptr)->storage_view.field_name.len);                                                                   \
    ASSERT_TRUE(s_is_cursor_in_buffer(&(storage_ptr)->storage, (storage_ptr)->field_name));                            \
    ASSERT_TRUE(s_is_cursor_in_buffer(&(storage_ptr)->storage, (storage_ptr)->storage_view.field_name));               \
    ASSERT_TRUE((view_ptr)->field_name.ptr != (storage_ptr)->field_name.ptr);

#define AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_EMPTY_CURSOR(storage_ptr, view_ptr, field_name)                           \
    ASSERT_UINT_EQUALS(0, (storage_ptr)->field_name.len);                                                              \
    ASSERT_UINT_EQUALS(0, (storage_ptr)->storage_view.field_name.len);

#define AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_UINT(storage_ptr, view_ptr, field_name)                                   \
    ASSERT_UINT_EQUALS((view_ptr)->field_name, (storage_ptr)->field_name);                                             \
    ASSERT_UINT_EQUALS((view_ptr)->field_name, (storage_ptr)->storage_view.field_name);

#define AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_UINT(storage_ptr, view_ptr, field_name)                          \
    ASSERT_PTR_EQUALS(&(storage_ptr)->field_name, (storage_ptr)->field_name##_ptr);                                    \
    ASSERT_UINT_EQUALS(*(view_ptr)->field_name, (storage_ptr)->field_name);                                            \
    ASSERT_PTR_EQUALS((storage_ptr)->storage_view.field_name, (storage_ptr)->field_name##_ptr);

static const char *PUBLISH_PAYLOAD = "hello-world";
static const char *PUBLISH_TOPIC = "greetings/friendly";

static int s_verify_publish_operation_required_fields(
    struct aws_mqtt5_packet_publish_storage *publish_storage,
    struct aws_mqtt5_packet_publish_view *original_view) {
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_EMPTY_CURSOR(publish_storage, original_view, payload);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_UINT(publish_storage, original_view, qos);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_UINT(publish_storage, original_view, retain);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_CURSOR(publish_storage, original_view, topic);

    return AWS_OP_SUCCESS;
}

static int s_mqtt5_publish_operation_new_set_no_optional_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_mqtt5_packet_publish_view publish_options = {
        .qos = AWS_MQTT5_QOS_AT_LEAST_ONCE,
        .retain = true,
        .topic = aws_byte_cursor_from_c_str(PUBLISH_TOPIC),
    };

    struct aws_mqtt5_operation_publish *publish_op = aws_mqtt5_operation_publish_new(allocator, &publish_options, NULL);

    ASSERT_NOT_NULL(publish_op);

    /* This test will check both the values in storage as well as the embedded view.  They should be in sync. */
    struct aws_mqtt5_packet_publish_storage *publish_storage = &publish_op->options_storage;
    struct aws_mqtt5_packet_publish_view *stored_view = &publish_storage->storage_view;

    /* required fields */
    ASSERT_SUCCESS(s_verify_publish_operation_required_fields(publish_storage, &publish_options));

    /* optional fields */
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULL(publish_storage, payload_format);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULL(publish_storage, message_expiry_interval_seconds);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULL(publish_storage, topic_alias);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULL(publish_storage, response_topic);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULL(publish_storage, correlation_data);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULL(publish_storage, content_type);

    ASSERT_SUCCESS(s_verify_user_properties(&publish_storage->user_properties, 0, NULL));
    ASSERT_SUCCESS(aws_mqtt5_test_verify_user_properties_raw(
        stored_view->user_property_count, stored_view->user_properties, 0, NULL));

    ASSERT_NULL(publish_op->completion_options.completion_callback);
    ASSERT_NULL(publish_op->completion_options.completion_user_data);

    aws_mqtt5_packet_publish_view_log(stored_view, AWS_LL_DEBUG);

    aws_mqtt5_operation_release(&publish_op->base);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_publish_operation_new_set_no_optional, s_mqtt5_publish_operation_new_set_no_optional_fn)

static const uint32_t s_message_expiry_interval_seconds = 60;
static const uint16_t s_topic_alias = 2;
static const char *s_response_topic = "A-response-topic";
static const char *s_correlation_data = "CorrelationData";
static const char *s_content_type = "JSON";

static char s_user_prop1_name[] = "Property1";
static char s_user_prop1_value[] = "Value1";
static char s_user_prop2_name[] = "Property2";
static char s_user_prop2_value[] = "Value2";
static const struct aws_mqtt5_user_property s_user_properties[] = {
    {
        .name =
            {
                .ptr = (uint8_t *)s_user_prop1_name,
                .len = AWS_ARRAY_SIZE(s_user_prop1_name),
            },
        .value =
            {
                .ptr = (uint8_t *)s_user_prop1_value,
                .len = AWS_ARRAY_SIZE(s_user_prop1_value),
            },
    },
    {
        .name =
            {
                .ptr = (uint8_t *)s_user_prop2_name,
                .len = AWS_ARRAY_SIZE(s_user_prop2_name),
            },
        .value =
            {
                .ptr = (uint8_t *)s_user_prop2_value,
                .len = AWS_ARRAY_SIZE(s_user_prop2_value),
            },
    },
};

static void s_aws_mqtt5_publish_completion_fn(
    const struct aws_mqtt5_packet_puback_view *puback,
    int error_code,
    void *complete_ctx) {
    (void)puback;
    (void)error_code;
    (void)complete_ctx;
}

static int s_mqtt5_publish_operation_new_set_all_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_byte_cursor response_topic = aws_byte_cursor_from_c_str(s_response_topic);
    struct aws_byte_cursor correlation_data = aws_byte_cursor_from_c_str(s_correlation_data);
    struct aws_byte_cursor content_type = aws_byte_cursor_from_c_str(s_content_type);
    enum aws_mqtt5_payload_format_indicator payload_format = AWS_MQTT5_PFI_UTF8;
    struct aws_byte_cursor payload_cursor = aws_byte_cursor_from_c_str(PUBLISH_PAYLOAD);

    struct aws_mqtt5_packet_publish_view publish_options = {
        .payload = payload_cursor,
        .qos = AWS_MQTT5_QOS_AT_MOST_ONCE,
        .retain = false,
        .topic = aws_byte_cursor_from_c_str(PUBLISH_TOPIC),
        .payload_format = &payload_format,
        .message_expiry_interval_seconds = &s_message_expiry_interval_seconds,
        .topic_alias = &s_topic_alias,
        .response_topic = &response_topic,
        .correlation_data = &correlation_data,
        .subscription_identifier_count = 0,
        .subscription_identifiers = NULL,
        .content_type = &content_type,
        .user_property_count = AWS_ARRAY_SIZE(s_user_properties),
        .user_properties = s_user_properties,
    };

    struct aws_mqtt5_publish_completion_options completion_options = {
        .completion_callback = &s_aws_mqtt5_publish_completion_fn,
        .completion_user_data = (void *)0xFFFF,
    };

    struct aws_mqtt5_operation_publish *publish_op =
        aws_mqtt5_operation_publish_new(allocator, &publish_options, &completion_options);

    ASSERT_NOT_NULL(publish_op);

    struct aws_mqtt5_packet_publish_storage *publish_storage = &publish_op->options_storage;
    struct aws_mqtt5_packet_publish_view *stored_view = &publish_storage->storage_view;

    /* required fields */
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_CURSOR(publish_storage, &publish_options, payload);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_UINT(publish_storage, &publish_options, qos);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_UINT(publish_storage, &publish_options, retain);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_CURSOR(publish_storage, &publish_options, topic);

    /* optional fields */
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_UINT(publish_storage, &publish_options, payload_format);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_UINT(
        publish_storage, &publish_options, message_expiry_interval_seconds);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_UINT(publish_storage, &publish_options, topic_alias);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_CURSOR(publish_storage, &publish_options, response_topic);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_CURSOR(publish_storage, &publish_options, correlation_data);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_CURSOR(publish_storage, &publish_options, content_type);

    ASSERT_SUCCESS(s_verify_user_properties(
        &publish_storage->user_properties, AWS_ARRAY_SIZE(s_user_properties), s_user_properties));
    ASSERT_SUCCESS(aws_mqtt5_test_verify_user_properties_raw(
        stored_view->user_property_count,
        stored_view->user_properties,
        AWS_ARRAY_SIZE(s_user_properties),
        s_user_properties));

    ASSERT_PTR_EQUALS(completion_options.completion_callback, publish_op->completion_options.completion_callback);
    ASSERT_PTR_EQUALS(completion_options.completion_user_data, publish_op->completion_options.completion_user_data);

    aws_mqtt5_packet_publish_view_log(stored_view, AWS_LL_DEBUG);

    aws_mqtt5_operation_release(&publish_op->base);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_publish_operation_new_set_all, s_mqtt5_publish_operation_new_set_all_fn)

static const char s_topic_filter1[] = "some/topic/+";
static const char s_topic_filter2[] = "another/topic/*";

static struct aws_mqtt5_subscription_view s_subscriptions[] = {
    {
        .topic_filter =
            {
                .ptr = (uint8_t *)s_topic_filter1,
                .len = AWS_ARRAY_SIZE(s_topic_filter1) - 1,
            },
        .qos = AWS_MQTT5_QOS_AT_MOST_ONCE,
        .no_local = true,
        .retain_as_published = false,
        .retain_handling_type = AWS_MQTT5_RHT_SEND_ON_SUBSCRIBE,
    },
    {
        .topic_filter =
            {
                .ptr = (uint8_t *)s_topic_filter2,
                .len = AWS_ARRAY_SIZE(s_topic_filter2) - 1,
            },
        .qos = AWS_MQTT5_QOS_AT_LEAST_ONCE,
        .no_local = false,
        .retain_as_published = true,
        .retain_handling_type = AWS_MQTT5_RHT_SEND_ON_SUBSCRIBE_IF_NEW,
    },
};

static int s_verify_subscriptions_raw(
    size_t expected_subscription_count,
    const struct aws_mqtt5_subscription_view *expected_subscriptions,
    size_t actual_subscription_count,
    const struct aws_mqtt5_subscription_view *actual_subscriptions) {
    ASSERT_INT_EQUALS(expected_subscription_count, actual_subscription_count);

    for (size_t i = 0; i < expected_subscription_count; ++i) {
        const struct aws_mqtt5_subscription_view *expected_view = &expected_subscriptions[i];
        const struct aws_mqtt5_subscription_view *actual_view = &actual_subscriptions[i];

        ASSERT_BIN_ARRAYS_EQUALS(
            expected_view->topic_filter.ptr,
            expected_view->topic_filter.len,
            actual_view->topic_filter.ptr,
            actual_view->topic_filter.len);
        ASSERT_INT_EQUALS(expected_view->qos, actual_view->qos);
        ASSERT_INT_EQUALS(expected_view->no_local, actual_view->no_local);
        ASSERT_INT_EQUALS(expected_view->retain_as_published, actual_view->retain_as_published);
        ASSERT_INT_EQUALS(expected_view->retain_handling_type, actual_view->retain_handling_type);
    }

    return AWS_OP_SUCCESS;
}

static int s_verify_subscriptions(
    size_t expected_subscription_count,
    const struct aws_mqtt5_subscription_view *expected_subscriptions,
    struct aws_array_list *storage_subscriptions) {
    return s_verify_subscriptions_raw(
        expected_subscription_count,
        expected_subscriptions,
        aws_array_list_length(storage_subscriptions),
        storage_subscriptions->data);
}

static int s_aws_mqtt5_subcribe_operation_verify_required_properties(
    struct aws_mqtt5_operation_subscribe *subscribe_op,
    struct aws_mqtt5_packet_subscribe_view *original_view,
    struct aws_mqtt5_subscribe_completion_options *original_completion_options) {
    (void)original_view;

    ASSERT_NOT_NULL(subscribe_op);

    struct aws_mqtt5_packet_subscribe_storage *subscribe_storage = &subscribe_op->options_storage;
    struct aws_mqtt5_packet_subscribe_view *stored_view = &subscribe_storage->storage_view;

    ASSERT_SUCCESS(
        s_verify_subscriptions(AWS_ARRAY_SIZE(s_subscriptions), s_subscriptions, &subscribe_storage->subscriptions));
    ASSERT_SUCCESS(s_verify_subscriptions_raw(
        AWS_ARRAY_SIZE(s_subscriptions), s_subscriptions, stored_view->subscription_count, stored_view->subscriptions));

    ASSERT_PTR_EQUALS(
        original_completion_options->completion_callback, subscribe_op->completion_options.completion_callback);
    ASSERT_PTR_EQUALS(
        original_completion_options->completion_user_data, subscribe_op->completion_options.completion_user_data);

    return AWS_OP_SUCCESS;
}

static void s_aws_mqtt5_subscribe_completion_fn(
    const struct aws_mqtt5_packet_suback_view *suback,
    int error_code,
    void *complete_ctx) {
    (void)suback;
    (void)error_code;
    (void)complete_ctx;
}

static int s_mqtt5_subscribe_operation_new_set_no_optional_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_mqtt5_packet_subscribe_view subscribe_options = {
        .subscriptions = s_subscriptions,
        .subscription_count = AWS_ARRAY_SIZE(s_subscriptions),
        .user_property_count = 0,
        .user_properties = NULL,
    };

    struct aws_mqtt5_subscribe_completion_options completion_options = {
        .completion_callback = &s_aws_mqtt5_subscribe_completion_fn,
        .completion_user_data = (void *)0xFFFF,
    };

    struct aws_mqtt5_operation_subscribe *subscribe_op =
        aws_mqtt5_operation_subscribe_new(allocator, &subscribe_options, &completion_options);

    ASSERT_SUCCESS(s_aws_mqtt5_subcribe_operation_verify_required_properties(
        subscribe_op, &subscribe_options, &completion_options));

    struct aws_mqtt5_packet_subscribe_view *stored_view = &subscribe_op->options_storage.storage_view;
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULL(&subscribe_op->options_storage, subscription_identifier);

    aws_mqtt5_packet_subscribe_view_log(stored_view, AWS_LL_DEBUG);

    aws_mqtt5_operation_release(&subscribe_op->base);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_subscribe_operation_new_set_no_optional, s_mqtt5_subscribe_operation_new_set_no_optional_fn)

static int s_mqtt5_subscribe_operation_new_set_all_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_mqtt5_packet_subscribe_view subscribe_options = {
        .subscriptions = s_subscriptions,
        .subscription_count = AWS_ARRAY_SIZE(s_subscriptions),
        .user_property_count = AWS_ARRAY_SIZE(s_user_properties),
        .user_properties = s_user_properties,
    };

    uint32_t sub_id = 5;
    subscribe_options.subscription_identifier = &sub_id;

    struct aws_mqtt5_subscribe_completion_options completion_options = {
        .completion_callback = &s_aws_mqtt5_subscribe_completion_fn,
        .completion_user_data = (void *)0xFFFF,
    };

    struct aws_mqtt5_operation_subscribe *subscribe_op =
        aws_mqtt5_operation_subscribe_new(allocator, &subscribe_options, &completion_options);

    ASSERT_SUCCESS(s_aws_mqtt5_subcribe_operation_verify_required_properties(
        subscribe_op, &subscribe_options, &completion_options));

    struct aws_mqtt5_packet_subscribe_storage *subscribe_storage = &subscribe_op->options_storage;
    struct aws_mqtt5_packet_subscribe_view *stored_view = &subscribe_storage->storage_view;

    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_UINT(subscribe_storage, &subscribe_options, subscription_identifier);

    ASSERT_SUCCESS(s_verify_user_properties(
        &subscribe_storage->user_properties, AWS_ARRAY_SIZE(s_user_properties), s_user_properties));
    ASSERT_SUCCESS(aws_mqtt5_test_verify_user_properties_raw(
        stored_view->user_property_count,
        stored_view->user_properties,
        AWS_ARRAY_SIZE(s_user_properties),
        s_user_properties));

    aws_mqtt5_packet_subscribe_view_log(stored_view, AWS_LL_DEBUG);

    aws_mqtt5_operation_release(&subscribe_op->base);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_subscribe_operation_new_set_all, s_mqtt5_subscribe_operation_new_set_all_fn)

static void s_aws_mqtt5_unsubscribe_completion_fn(
    const struct aws_mqtt5_packet_unsuback_view *unsuback,
    int error_code,
    void *complete_ctx) {
    (void)unsuback;
    (void)error_code;
    (void)complete_ctx;
}

static const char s_unsub_topic_filter1[] = "a/topic";
static const char s_unsub_topic_filter2[] = "another/*";
static const char s_unsub_topic_filter3[] = "hello/+/world";

static const struct aws_byte_cursor s_topics[] = {
    {
        .ptr = (uint8_t *)s_unsub_topic_filter1,
        .len = AWS_ARRAY_SIZE(s_unsub_topic_filter1) - 1,
    },
    {
        .ptr = (uint8_t *)s_unsub_topic_filter2,
        .len = AWS_ARRAY_SIZE(s_unsub_topic_filter2) - 1,
    },
    {
        .ptr = (uint8_t *)s_unsub_topic_filter3,
        .len = AWS_ARRAY_SIZE(s_unsub_topic_filter3) - 1,
    },
};

static int s_mqtt5_unsubscribe_operation_new_set_all_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_mqtt5_packet_unsubscribe_view unsubscribe_options = {
        .topics = s_topics,
        .topic_count = AWS_ARRAY_SIZE(s_topics),
        .user_property_count = AWS_ARRAY_SIZE(s_user_properties),
        .user_properties = s_user_properties,
    };

    struct aws_mqtt5_unsubscribe_completion_options completion_options = {
        .completion_callback = &s_aws_mqtt5_unsubscribe_completion_fn,
        .completion_user_data = (void *)0xFFFF,
    };

    struct aws_mqtt5_operation_unsubscribe *unsubscribe_op =
        aws_mqtt5_operation_unsubscribe_new(allocator, &unsubscribe_options, &completion_options);

    struct aws_mqtt5_packet_unsubscribe_storage *unsubscribe_storage = &unsubscribe_op->options_storage;
    struct aws_mqtt5_packet_unsubscribe_view *stored_view = &unsubscribe_storage->storage_view;

    ASSERT_UINT_EQUALS(stored_view->topic_count, unsubscribe_options.topic_count);
    for (size_t i = 0; i < stored_view->topic_count; ++i) {
        const struct aws_byte_cursor *expected_topic = &unsubscribe_options.topics[i];
        const struct aws_byte_cursor *actual_topic = &stored_view->topics[i];

        ASSERT_UINT_EQUALS(expected_topic->len, actual_topic->len);
        ASSERT_TRUE(expected_topic->ptr != actual_topic->ptr);

        ASSERT_BIN_ARRAYS_EQUALS(expected_topic->ptr, expected_topic->len, actual_topic->ptr, actual_topic->len);
    }

    ASSERT_SUCCESS(s_verify_user_properties(
        &unsubscribe_storage->user_properties, AWS_ARRAY_SIZE(s_user_properties), s_user_properties));
    ASSERT_SUCCESS(aws_mqtt5_test_verify_user_properties_raw(
        stored_view->user_property_count,
        stored_view->user_properties,
        AWS_ARRAY_SIZE(s_user_properties),
        s_user_properties));

    aws_mqtt5_packet_unsubscribe_view_log(stored_view, AWS_LL_DEBUG);

    aws_mqtt5_operation_release(&unsubscribe_op->base);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_unsubscribe_operation_new_set_all, s_mqtt5_unsubscribe_operation_new_set_all_fn)

static int s_aws_mqtt5_connect_storage_verify_required_properties(
    struct aws_mqtt5_packet_connect_storage *connect_storage,
    struct aws_mqtt5_packet_connect_view *connect_options) {

    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_UINT(connect_storage, connect_options, keep_alive_interval_seconds);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_CURSOR(connect_storage, connect_options, client_id);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_UINT(connect_storage, connect_options, clean_start);

    return AWS_OP_SUCCESS;
}

AWS_STATIC_STRING_FROM_LITERAL(s_client_id, "MyClientId");

static int s_mqtt5_connect_storage_new_set_no_optional_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_mqtt5_packet_connect_view connect_options = {
        .keep_alive_interval_seconds = 50,
        .client_id = aws_byte_cursor_from_string(s_client_id),
        .clean_start = true,
    };

    struct aws_mqtt5_packet_connect_storage connect_storage;
    AWS_ZERO_STRUCT(connect_storage);

    ASSERT_SUCCESS(aws_mqtt5_packet_connect_storage_init(&connect_storage, allocator, &connect_options));

    ASSERT_SUCCESS(s_aws_mqtt5_connect_storage_verify_required_properties(&connect_storage, &connect_options));

    struct aws_mqtt5_packet_connect_view *stored_view = &connect_storage.storage_view;

    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULL(&connect_storage, username);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULL(&connect_storage, password);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULL(&connect_storage, session_expiry_interval_seconds);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULL(&connect_storage, request_response_information);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULL(&connect_storage, request_problem_information);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULL(&connect_storage, receive_maximum);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULL(&connect_storage, topic_alias_maximum);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULL(&connect_storage, maximum_packet_size_bytes);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULL(&connect_storage, will_delay_interval_seconds);

    ASSERT_NULL(connect_storage.will);
    ASSERT_NULL(stored_view->will);

    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULL(&connect_storage, authentication_method);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULL(&connect_storage, authentication_data);

    ASSERT_SUCCESS(s_verify_user_properties(&connect_storage.user_properties, 0, NULL));
    ASSERT_SUCCESS(aws_mqtt5_test_verify_user_properties_raw(
        stored_view->user_property_count, stored_view->user_properties, 0, NULL));

    aws_mqtt5_packet_connect_view_log(stored_view, AWS_LL_DEBUG);

    aws_mqtt5_packet_connect_storage_clean_up(&connect_storage);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_connect_storage_new_set_no_optional, s_mqtt5_connect_storage_new_set_no_optional_fn)

static const char s_username[] = "SomeUser";
static const struct aws_byte_cursor s_username_cursor = {
    .ptr = (uint8_t *)s_username,
    .len = AWS_ARRAY_SIZE(s_username) - 1,
};

static const char s_password[] = "CantBeGuessed";
static const struct aws_byte_cursor s_password_cursor = {
    .ptr = (uint8_t *)s_password,
    .len = AWS_ARRAY_SIZE(s_password) - 1,
};

static const uint32_t s_session_expiry_interval_seconds = 60;
static const uint8_t s_request_response_information = true;
static const uint8_t s_request_problem_information = true;
static const uint16_t s_connect_receive_maximum = 10;
static const uint16_t s_connect_topic_alias_maximum = 15;
static const uint32_t s_connect_maximum_packet_size_bytes = 128 * 1024 * 1024;
static const uint32_t s_will_delay_interval_seconds = 30;

static const char s_authentication_method[] = "ECDSA-DH-RSA-EVP-SEKRTI";
static const struct aws_byte_cursor s_authentication_method_cursor = {
    .ptr = (uint8_t *)s_authentication_method,
    .len = AWS_ARRAY_SIZE(s_authentication_method) - 1,
};

static const char s_authentication_data[] = "SomeSignature";
static const struct aws_byte_cursor s_authentication_data_cursor = {
    .ptr = (uint8_t *)s_authentication_data,
    .len = AWS_ARRAY_SIZE(s_authentication_data) - 1,
};

static int s_mqtt5_connect_storage_new_set_all_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_mqtt5_packet_publish_view publish_options = {
        .qos = AWS_MQTT5_QOS_AT_LEAST_ONCE,
        .retain = true,
        .topic = aws_byte_cursor_from_c_str(PUBLISH_TOPIC),
    };

    struct aws_mqtt5_packet_connect_view connect_options = {
        .keep_alive_interval_seconds = 50,
        .client_id = aws_byte_cursor_from_string(s_client_id),
        .username = &s_username_cursor,
        .password = &s_password_cursor,
        .clean_start = true,
        .session_expiry_interval_seconds = &s_session_expiry_interval_seconds,
        .request_response_information = &s_request_response_information,
        .request_problem_information = &s_request_problem_information,
        .receive_maximum = &s_connect_receive_maximum,
        .topic_alias_maximum = &s_connect_topic_alias_maximum,
        .maximum_packet_size_bytes = &s_connect_maximum_packet_size_bytes,
        .will_delay_interval_seconds = &s_will_delay_interval_seconds,
        .will = &publish_options,
        .user_property_count = AWS_ARRAY_SIZE(s_user_properties),
        .user_properties = s_user_properties,
        .authentication_method = &s_authentication_method_cursor,
        .authentication_data = &s_authentication_data_cursor,
    };

    struct aws_mqtt5_packet_connect_storage connect_storage;
    AWS_ZERO_STRUCT(connect_storage);

    ASSERT_SUCCESS(aws_mqtt5_packet_connect_storage_init(&connect_storage, allocator, &connect_options));

    ASSERT_SUCCESS(s_aws_mqtt5_connect_storage_verify_required_properties(&connect_storage, &connect_options));

    struct aws_mqtt5_packet_connect_view *stored_view = &connect_storage.storage_view;

    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_CURSOR(&connect_storage, &connect_options, username);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_CURSOR(&connect_storage, &connect_options, password);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_UINT(
        &connect_storage, &connect_options, session_expiry_interval_seconds);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_UINT(
        &connect_storage, &connect_options, request_response_information);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_UINT(&connect_storage, &connect_options, request_problem_information);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_UINT(&connect_storage, &connect_options, receive_maximum);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_UINT(&connect_storage, &connect_options, topic_alias_maximum);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_UINT(&connect_storage, &connect_options, maximum_packet_size_bytes);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_UINT(&connect_storage, &connect_options, will_delay_interval_seconds);

    ASSERT_NOT_NULL(connect_storage.will);
    ASSERT_NOT_NULL(stored_view->will);
    ASSERT_SUCCESS(s_verify_publish_operation_required_fields(connect_storage.will, &publish_options));

    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_CURSOR(&connect_storage, &connect_options, authentication_method);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_CURSOR(&connect_storage, &connect_options, authentication_data);

    ASSERT_SUCCESS(s_verify_user_properties(
        &connect_storage.user_properties, AWS_ARRAY_SIZE(s_user_properties), s_user_properties));
    ASSERT_SUCCESS(aws_mqtt5_test_verify_user_properties_raw(
        stored_view->user_property_count,
        stored_view->user_properties,
        AWS_ARRAY_SIZE(s_user_properties),
        s_user_properties));

    aws_mqtt5_packet_connect_view_log(stored_view, AWS_LL_DEBUG);

    aws_mqtt5_packet_connect_storage_clean_up(&connect_storage);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_connect_storage_new_set_all, s_mqtt5_connect_storage_new_set_all_fn)

static int s_aws_mqtt5_connack_storage_verify_required_properties(
    struct aws_mqtt5_packet_connack_storage *connack_storage,
    struct aws_mqtt5_packet_connack_view *connack_view) {
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_UINT(connack_storage, connack_view, session_present);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_UINT(connack_storage, connack_view, reason_code);

    return AWS_OP_SUCCESS;
}

static int s_mqtt5_connack_storage_new_set_no_optional_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct aws_mqtt5_packet_connack_view connack_options = {
        .session_present = true,
        .reason_code = AWS_MQTT5_CRC_BANNED,
    };

    struct aws_mqtt5_packet_connack_storage connack_storage;
    AWS_ZERO_STRUCT(connack_storage);

    ASSERT_SUCCESS(aws_mqtt5_packet_connack_storage_init(&connack_storage, allocator, &connack_options));

    ASSERT_SUCCESS(s_aws_mqtt5_connack_storage_verify_required_properties(&connack_storage, &connack_options));

    struct aws_mqtt5_packet_connack_view *stored_view = &connack_storage.storage_view;

    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULL(&connack_storage, session_expiry_interval);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULL(&connack_storage, receive_maximum);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULL(&connack_storage, maximum_qos);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULL(&connack_storage, retain_available);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULL(&connack_storage, maximum_packet_size);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULL(&connack_storage, assigned_client_identifier);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULL(&connack_storage, topic_alias_maximum);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULL(&connack_storage, reason_string);

    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULL(&connack_storage, wildcard_subscriptions_available);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULL(&connack_storage, subscription_identifiers_available);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULL(&connack_storage, shared_subscriptions_available);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULL(&connack_storage, server_keep_alive);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULL(&connack_storage, response_information);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULL(&connack_storage, server_reference);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULL(&connack_storage, authentication_method);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULL(&connack_storage, authentication_data);

    ASSERT_SUCCESS(s_verify_user_properties(&connack_storage.user_properties, 0, NULL));
    ASSERT_SUCCESS(aws_mqtt5_test_verify_user_properties_raw(
        stored_view->user_property_count, stored_view->user_properties, 0, NULL));

    aws_mqtt5_packet_connack_view_log(stored_view, AWS_LL_DEBUG);

    aws_mqtt5_packet_connack_storage_clean_up(&connack_storage);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_connack_storage_new_set_no_optional, s_mqtt5_connack_storage_new_set_no_optional_fn)

static const uint32_t s_connack_session_expiry_interval = 300;
static const uint16_t s_connack_receive_maximum = 15;
static const enum aws_mqtt5_qos s_connack_maximum_qos = AWS_MQTT5_QOS_EXACTLY_ONCE;
static const bool s_connack_retain_available = true;
static const uint32_t s_connack_maximum_packet_size = 256 * 1024 * 1024;

static const char s_assigned_client_identifier[] = "ThisIsYourClientId";
static const struct aws_byte_cursor s_assigned_client_identifier_cursor = {
    .ptr = (uint8_t *)s_assigned_client_identifier,
    .len = AWS_ARRAY_SIZE(s_assigned_client_identifier) - 1,
};
static const uint16_t s_connack_topic_alias_maximum = 32;

static const char s_reason_string[] = "Very Bad Behavior";
static const struct aws_byte_cursor s_reason_string_cursor = {
    .ptr = (uint8_t *)s_reason_string,
    .len = AWS_ARRAY_SIZE(s_reason_string) - 1,
};

static const bool s_connack_wildcard_subscriptions_available = true;
static const bool s_connack_subscription_identifiers_available = true;
static const bool s_connack_shared_subscriptions_available = true;
static const uint16_t s_connack_server_keep_alive = 3600;

static const char s_response_information[] = "Everything worked great!";
static const struct aws_byte_cursor s_response_information_cursor = {
    .ptr = (uint8_t *)s_response_information,
    .len = AWS_ARRAY_SIZE(s_response_information) - 1,
};

static const char s_server_reference[] = "no-dont-leave.com";
static const struct aws_byte_cursor s_server_reference_cursor = {
    .ptr = (uint8_t *)s_server_reference,
    .len = AWS_ARRAY_SIZE(s_server_reference) - 1,
};

static int s_mqtt5_connack_storage_new_set_all_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct aws_mqtt5_packet_connack_view connack_options = {
        .session_present = true,
        .reason_code = AWS_MQTT5_CRC_BANNED,
        .session_expiry_interval = &s_connack_session_expiry_interval,
        .receive_maximum = &s_connack_receive_maximum,
        .maximum_qos = &s_connack_maximum_qos,
        .retain_available = &s_connack_retain_available,
        .maximum_packet_size = &s_connack_maximum_packet_size,
        .assigned_client_identifier = &s_assigned_client_identifier_cursor,
        .topic_alias_maximum = &s_connack_topic_alias_maximum,
        .reason_string = &s_reason_string_cursor,
        .user_property_count = AWS_ARRAY_SIZE(s_user_properties),
        .user_properties = s_user_properties,
        .wildcard_subscriptions_available = &s_connack_wildcard_subscriptions_available,
        .subscription_identifiers_available = &s_connack_subscription_identifiers_available,
        .shared_subscriptions_available = &s_connack_shared_subscriptions_available,
        .server_keep_alive = &s_connack_server_keep_alive,
        .response_information = &s_response_information_cursor,
        .server_reference = &s_server_reference_cursor,
        .authentication_method = &s_authentication_method_cursor,
        .authentication_data = &s_authentication_data_cursor,
    };

    struct aws_mqtt5_packet_connack_storage connack_storage;
    AWS_ZERO_STRUCT(connack_storage);

    ASSERT_SUCCESS(aws_mqtt5_packet_connack_storage_init(&connack_storage, allocator, &connack_options));

    ASSERT_SUCCESS(s_aws_mqtt5_connack_storage_verify_required_properties(&connack_storage, &connack_options));

    struct aws_mqtt5_packet_connack_view *stored_view = &connack_storage.storage_view;

    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_UINT(&connack_storage, &connack_options, session_expiry_interval);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_UINT(&connack_storage, &connack_options, receive_maximum);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_UINT(&connack_storage, &connack_options, maximum_qos);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_UINT(&connack_storage, &connack_options, retain_available);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_UINT(&connack_storage, &connack_options, maximum_packet_size);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_CURSOR(
        &connack_storage, &connack_options, assigned_client_identifier);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_UINT(&connack_storage, &connack_options, topic_alias_maximum);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_CURSOR(&connack_storage, &connack_options, reason_string);

    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_UINT(
        &connack_storage, &connack_options, wildcard_subscriptions_available);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_UINT(
        &connack_storage, &connack_options, subscription_identifiers_available);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_UINT(
        &connack_storage, &connack_options, shared_subscriptions_available);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_UINT(&connack_storage, &connack_options, server_keep_alive);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_CURSOR(&connack_storage, &connack_options, response_information);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_CURSOR(&connack_storage, &connack_options, server_reference);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_CURSOR(&connack_storage, &connack_options, authentication_method);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_CURSOR(&connack_storage, &connack_options, authentication_data);

    ASSERT_SUCCESS(s_verify_user_properties(
        &connack_storage.user_properties, AWS_ARRAY_SIZE(s_user_properties), s_user_properties));
    ASSERT_SUCCESS(aws_mqtt5_test_verify_user_properties_raw(
        stored_view->user_property_count,
        stored_view->user_properties,
        AWS_ARRAY_SIZE(s_user_properties),
        s_user_properties));

    aws_mqtt5_packet_connack_view_log(stored_view, AWS_LL_DEBUG);

    aws_mqtt5_packet_connack_storage_clean_up(&connack_storage);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_connack_storage_new_set_all, s_mqtt5_connack_storage_new_set_all_fn)

static int s_mqtt5_disconnect_storage_new_set_no_optional_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct aws_mqtt5_packet_disconnect_view disconnect_options = {
        .reason_code = AWS_MQTT5_DRC_ADMINISTRATIVE_ACTION,
    };

    struct aws_mqtt5_packet_disconnect_storage disconnect_storage;
    AWS_ZERO_STRUCT(disconnect_storage);

    ASSERT_SUCCESS(aws_mqtt5_packet_disconnect_storage_init(&disconnect_storage, allocator, &disconnect_options));

    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_UINT(&disconnect_storage, &disconnect_options, reason_code);

    struct aws_mqtt5_packet_disconnect_view *stored_view = &disconnect_storage.storage_view;

    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULL(&disconnect_storage, session_expiry_interval_seconds);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULL(&disconnect_storage, reason_string);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULL(&disconnect_storage, server_reference);

    ASSERT_SUCCESS(s_verify_user_properties(&disconnect_storage.user_properties, 0, NULL));
    ASSERT_SUCCESS(aws_mqtt5_test_verify_user_properties_raw(
        stored_view->user_property_count, stored_view->user_properties, 0, NULL));

    aws_mqtt5_packet_disconnect_view_log(stored_view, AWS_LL_DEBUG);

    aws_mqtt5_packet_disconnect_storage_clean_up(&disconnect_storage);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_disconnect_storage_new_set_no_optional, s_mqtt5_disconnect_storage_new_set_no_optional_fn)

static int s_mqtt5_disconnect_storage_new_set_all_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_mqtt5_packet_disconnect_view disconnect_options = {
        .reason_code = AWS_MQTT5_DRC_ADMINISTRATIVE_ACTION,
        .session_expiry_interval_seconds = &s_session_expiry_interval_seconds,
        .reason_string = &s_reason_string_cursor,
        .user_property_count = AWS_ARRAY_SIZE(s_user_properties),
        .user_properties = s_user_properties,
        .server_reference = &s_server_reference_cursor,
    };

    struct aws_mqtt5_packet_disconnect_storage disconnect_storage;
    AWS_ZERO_STRUCT(disconnect_storage);

    ASSERT_SUCCESS(aws_mqtt5_packet_disconnect_storage_init(&disconnect_storage, allocator, &disconnect_options));

    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_UINT(&disconnect_storage, &disconnect_options, reason_code);

    struct aws_mqtt5_packet_disconnect_view *stored_view = &disconnect_storage.storage_view;

    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_UINT(
        &disconnect_storage, &disconnect_options, session_expiry_interval_seconds);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_CURSOR(&disconnect_storage, &disconnect_options, reason_string);
    AWS_VERIFY_VIEW_STORAGE_RELATIONSHIP_NULLABLE_CURSOR(&disconnect_storage, &disconnect_options, server_reference);

    ASSERT_SUCCESS(s_verify_user_properties(
        &disconnect_storage.user_properties, AWS_ARRAY_SIZE(s_user_properties), s_user_properties));
    ASSERT_SUCCESS(aws_mqtt5_test_verify_user_properties_raw(
        stored_view->user_property_count,
        stored_view->user_properties,
        AWS_ARRAY_SIZE(s_user_properties),
        s_user_properties));

    aws_mqtt5_packet_disconnect_view_log(stored_view, AWS_LL_DEBUG);

    aws_mqtt5_packet_disconnect_storage_clean_up(&disconnect_storage);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_disconnect_storage_new_set_all, s_mqtt5_disconnect_storage_new_set_all_fn)

static const enum aws_mqtt5_qos s_maximum_qos = AWS_MQTT5_QOS_AT_LEAST_ONCE;
static const uint16_t s_keep_alive_interval_seconds = 999;
static const uint32_t s_session_expiry_interval = 999;
static const uint16_t s_receive_maximum = 999;
static const uint32_t s_maximum_packet_size = 999;
static const uint16_t s_topic_alias_maximum_to_server = 999;
static const uint16_t s_topic_alias_maximum = 999;
static const uint16_t s_server_keep_alive = 999;
static const bool s_retain_available = false;
static const bool s_wildcard_subscriptions_available = false;
static const bool s_subscription_identifiers_available = false;
static const bool s_shared_subscriptions_available = false;

static int mqtt5_negotiated_settings_reset_test_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    (void)allocator;

    /* aws_mqtt5_negotiated_settings used for testing */
    struct aws_mqtt5_negotiated_settings negotiated_settings;
    AWS_ZERO_STRUCT(negotiated_settings);

    /* Simulate an aws_mqtt5_packet_connect_view with no user set settings  */
    struct aws_mqtt5_packet_connect_view connect_view = {
        .keep_alive_interval_seconds = 0,
    };

    /* Apply no client settings to a reset of negotiated_settings */
    aws_mqtt5_negotiated_settings_reset(&negotiated_settings, &connect_view);

    /* Check that all settings are the expected default values */
    ASSERT_TRUE(negotiated_settings.maximum_qos == AWS_MQTT5_QOS_AT_LEAST_ONCE);

    ASSERT_UINT_EQUALS(negotiated_settings.session_expiry_interval, 0);
    ASSERT_UINT_EQUALS(negotiated_settings.receive_maximum_from_server, AWS_MQTT5_RECEIVE_MAXIMUM);
    ASSERT_UINT_EQUALS(negotiated_settings.maximum_packet_size, AWS_MQTT5_MAXIMUM_PACKET_SIZE);
    ASSERT_UINT_EQUALS(negotiated_settings.topic_alias_maximum_to_server, 0);
    ASSERT_UINT_EQUALS(negotiated_settings.topic_alias_maximum_to_client, 0);
    ASSERT_UINT_EQUALS(negotiated_settings.server_keep_alive, 0);

    ASSERT_TRUE(negotiated_settings.retain_available);
    ASSERT_TRUE(negotiated_settings.wildcard_subscriptions_available);
    ASSERT_TRUE(negotiated_settings.subscription_identifiers_available);
    ASSERT_TRUE(negotiated_settings.shared_subscriptions_available);
    ASSERT_FALSE(negotiated_settings.rejoined_session);

    /* Set client modifiable CONNECT settings */
    connect_view.keep_alive_interval_seconds = s_keep_alive_interval_seconds;
    connect_view.session_expiry_interval_seconds = &s_session_expiry_interval;
    connect_view.receive_maximum = &s_receive_maximum;
    connect_view.maximum_packet_size_bytes = &s_maximum_packet_size;
    connect_view.topic_alias_maximum = &s_topic_alias_maximum;

    /* Apply client settings to a reset of negotiated settings */
    aws_mqtt5_negotiated_settings_reset(&negotiated_settings, &connect_view);

    /* Check that all settings are the expected values with client settings */
    ASSERT_TRUE(negotiated_settings.maximum_qos == AWS_MQTT5_QOS_AT_LEAST_ONCE);

    ASSERT_UINT_EQUALS(negotiated_settings.server_keep_alive, connect_view.keep_alive_interval_seconds);
    ASSERT_UINT_EQUALS(negotiated_settings.session_expiry_interval, *connect_view.session_expiry_interval_seconds);
    ASSERT_UINT_EQUALS(negotiated_settings.receive_maximum_from_server, AWS_MQTT5_RECEIVE_MAXIMUM);
    ASSERT_UINT_EQUALS(negotiated_settings.maximum_packet_size, *connect_view.maximum_packet_size_bytes);
    ASSERT_UINT_EQUALS(negotiated_settings.topic_alias_maximum_to_server, 0);
    ASSERT_UINT_EQUALS(negotiated_settings.topic_alias_maximum_to_client, *connect_view.topic_alias_maximum);

    ASSERT_TRUE(negotiated_settings.retain_available);
    ASSERT_TRUE(negotiated_settings.wildcard_subscriptions_available);
    ASSERT_TRUE(negotiated_settings.subscription_identifiers_available);
    ASSERT_TRUE(negotiated_settings.shared_subscriptions_available);
    ASSERT_FALSE(negotiated_settings.rejoined_session);

    /* Reset connect view to clean defaults */

    connect_view.keep_alive_interval_seconds = 0;
    connect_view.session_expiry_interval_seconds = NULL;
    connect_view.receive_maximum = NULL;
    connect_view.maximum_packet_size_bytes = NULL;
    connect_view.topic_alias_maximum = NULL;

    /* Change remaining default properties on negotiated_settings to non-default values */
    negotiated_settings.maximum_qos = AWS_MQTT5_QOS_EXACTLY_ONCE;

    negotiated_settings.topic_alias_maximum_to_server = s_topic_alias_maximum_to_server;
    negotiated_settings.topic_alias_maximum_to_client = s_topic_alias_maximum_to_server;

    negotiated_settings.retain_available = s_retain_available;
    negotiated_settings.wildcard_subscriptions_available = s_wildcard_subscriptions_available;
    negotiated_settings.subscription_identifiers_available = s_subscription_identifiers_available;
    negotiated_settings.shared_subscriptions_available = s_shared_subscriptions_available;

    /* Apply no client settings to a reset of negotiated_settings */
    aws_mqtt5_negotiated_settings_reset(&negotiated_settings, &connect_view);

    /* Check that all settings are the expected default values */
    ASSERT_TRUE(negotiated_settings.maximum_qos == AWS_MQTT5_QOS_AT_LEAST_ONCE);

    ASSERT_UINT_EQUALS(negotiated_settings.session_expiry_interval, 0);
    ASSERT_UINT_EQUALS(negotiated_settings.receive_maximum_from_server, AWS_MQTT5_RECEIVE_MAXIMUM);
    ASSERT_UINT_EQUALS(negotiated_settings.maximum_packet_size, AWS_MQTT5_MAXIMUM_PACKET_SIZE);
    ASSERT_UINT_EQUALS(negotiated_settings.topic_alias_maximum_to_server, 0);
    ASSERT_UINT_EQUALS(negotiated_settings.topic_alias_maximum_to_client, 0);
    ASSERT_UINT_EQUALS(negotiated_settings.server_keep_alive, 0);

    ASSERT_TRUE(negotiated_settings.retain_available);
    ASSERT_TRUE(negotiated_settings.wildcard_subscriptions_available);
    ASSERT_TRUE(negotiated_settings.subscription_identifiers_available);
    ASSERT_TRUE(negotiated_settings.shared_subscriptions_available);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_negotiated_settings_reset_test, mqtt5_negotiated_settings_reset_test_fn)

static int mqtt5_negotiated_settings_apply_connack_test_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    (void)allocator;

    /* aws_mqtt5_negotiated_settings used for testing */
    struct aws_mqtt5_negotiated_settings negotiated_settings;
    AWS_ZERO_STRUCT(negotiated_settings);

    /* An aws_mqtt5_packet_connect_view with no user set settings to reset negotiated_settings */
    struct aws_mqtt5_packet_connect_view connect_view = {
        .keep_alive_interval_seconds = 0,
    };

    /* reset negotiated_settings to default values */
    aws_mqtt5_negotiated_settings_reset(&negotiated_settings, &connect_view);

    /* Simulate an aws_mqtt5_packet_connack_view with no user set settings  */
    struct aws_mqtt5_packet_connack_view connack_view = {
        .session_present = false,
    };

    /* Check if everything defaults appropriately if no properties are set in either direction */
    aws_mqtt5_negotiated_settings_apply_connack(&negotiated_settings, &connack_view);

    ASSERT_TRUE(negotiated_settings.maximum_qos == AWS_MQTT5_QOS_AT_LEAST_ONCE);

    ASSERT_UINT_EQUALS(negotiated_settings.session_expiry_interval, 0);
    ASSERT_UINT_EQUALS(negotiated_settings.receive_maximum_from_server, AWS_MQTT5_RECEIVE_MAXIMUM);
    ASSERT_UINT_EQUALS(negotiated_settings.maximum_packet_size, AWS_MQTT5_MAXIMUM_PACKET_SIZE);
    ASSERT_UINT_EQUALS(negotiated_settings.topic_alias_maximum_to_server, 0);
    ASSERT_UINT_EQUALS(negotiated_settings.topic_alias_maximum_to_client, 0);
    ASSERT_UINT_EQUALS(negotiated_settings.server_keep_alive, 0);

    ASSERT_TRUE(negotiated_settings.retain_available);
    ASSERT_TRUE(negotiated_settings.wildcard_subscriptions_available);
    ASSERT_TRUE(negotiated_settings.subscription_identifiers_available);
    ASSERT_TRUE(negotiated_settings.shared_subscriptions_available);
    ASSERT_FALSE(negotiated_settings.rejoined_session);

    /* Apply server settings to properties in connack_view */
    connack_view.session_present = true;
    connack_view.maximum_qos = &s_maximum_qos;
    connack_view.session_expiry_interval = &s_session_expiry_interval;
    connack_view.receive_maximum = &s_receive_maximum;
    connack_view.retain_available = &s_retain_available;
    connack_view.maximum_packet_size = &s_maximum_packet_size;
    connack_view.topic_alias_maximum = &s_topic_alias_maximum_to_server;
    connack_view.wildcard_subscriptions_available = &s_wildcard_subscriptions_available;
    connack_view.subscription_identifiers_available = &s_subscription_identifiers_available;
    connack_view.shared_subscriptions_available = &s_shared_subscriptions_available;
    connack_view.server_keep_alive = &s_server_keep_alive;

    aws_mqtt5_negotiated_settings_apply_connack(&negotiated_settings, &connack_view);

    ASSERT_TRUE(negotiated_settings.rejoined_session);
    ASSERT_TRUE(negotiated_settings.maximum_qos == s_maximum_qos);
    ASSERT_UINT_EQUALS(negotiated_settings.session_expiry_interval, *connack_view.session_expiry_interval);
    ASSERT_UINT_EQUALS(negotiated_settings.receive_maximum_from_server, *connack_view.receive_maximum);
    ASSERT_UINT_EQUALS(negotiated_settings.maximum_packet_size, *connack_view.maximum_packet_size);
    ASSERT_UINT_EQUALS(negotiated_settings.server_keep_alive, *connack_view.server_keep_alive);
    ASSERT_UINT_EQUALS(negotiated_settings.topic_alias_maximum_to_server, *connack_view.topic_alias_maximum);
    ASSERT_UINT_EQUALS(negotiated_settings.topic_alias_maximum_to_client, 0);

    ASSERT_FALSE(negotiated_settings.retain_available);
    ASSERT_FALSE(negotiated_settings.wildcard_subscriptions_available);
    ASSERT_FALSE(negotiated_settings.subscription_identifiers_available);
    ASSERT_FALSE(negotiated_settings.shared_subscriptions_available);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_negotiated_settings_apply_connack_test, mqtt5_negotiated_settings_apply_connack_test_fn)

static const struct aws_byte_cursor s_topic = {
    .ptr = (uint8_t *)s_unsub_topic_filter1,
    .len = AWS_ARRAY_SIZE(s_unsub_topic_filter1) - 1,
};

static const char s_payload[] = "ThePayload";

static const struct aws_byte_cursor s_payload_cursor = {
    .ptr = (uint8_t *)s_payload,
    .len = AWS_ARRAY_SIZE(s_payload) - 1,
};

/* test that allocates packet ids from an empty table. */
static int s_mqtt5_operation_bind_packet_id_empty_table_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    (void)allocator;

    struct aws_mqtt5_packet_publish_view publish_view = {
        .qos = AWS_MQTT5_QOS_AT_LEAST_ONCE,
        .topic = s_topic,
        .payload = s_payload_cursor,
    };

    struct aws_mqtt5_operation_publish *publish_operation =
        aws_mqtt5_operation_publish_new(allocator, &publish_view, NULL);

    struct aws_mqtt5_client_operational_state operational_state;
    aws_mqtt5_client_operational_state_init(&operational_state, allocator, NULL);
    operational_state.next_mqtt_packet_id = 1;

    ASSERT_SUCCESS(aws_mqtt5_operation_bind_packet_id(&publish_operation->base, &operational_state));
    ASSERT_UINT_EQUALS(1, aws_mqtt5_operation_get_packet_id(&publish_operation->base));
    ASSERT_UINT_EQUALS(2, operational_state.next_mqtt_packet_id);

    aws_mqtt5_operation_set_packet_id(&publish_operation->base, 0);
    operational_state.next_mqtt_packet_id = 5;

    ASSERT_SUCCESS(aws_mqtt5_operation_bind_packet_id(&publish_operation->base, &operational_state));
    ASSERT_UINT_EQUALS(5, aws_mqtt5_operation_get_packet_id(&publish_operation->base));
    ASSERT_UINT_EQUALS(6, operational_state.next_mqtt_packet_id);

    aws_mqtt5_operation_set_packet_id(&publish_operation->base, 0);
    operational_state.next_mqtt_packet_id = 65535;

    ASSERT_SUCCESS(aws_mqtt5_operation_bind_packet_id(&publish_operation->base, &operational_state));
    ASSERT_UINT_EQUALS(65535, aws_mqtt5_operation_get_packet_id(&publish_operation->base));
    ASSERT_UINT_EQUALS(1, operational_state.next_mqtt_packet_id);

    aws_mqtt5_client_operational_state_clean_up(&operational_state);

    aws_mqtt5_operation_release(&publish_operation->base);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_operation_bind_packet_id_empty_table, s_mqtt5_operation_bind_packet_id_empty_table_fn)

static void s_create_operations(
    struct aws_allocator *allocator,
    struct aws_mqtt5_operation_publish **publish_op,
    struct aws_mqtt5_operation_subscribe **subscribe_op,
    struct aws_mqtt5_operation_unsubscribe **unsubscribe_op) {

    struct aws_mqtt5_packet_publish_view publish_view = {
        .qos = AWS_MQTT5_QOS_AT_LEAST_ONCE,
        .topic = s_topic,
        .payload = s_payload_cursor,
    };

    *publish_op = aws_mqtt5_operation_publish_new(allocator, &publish_view, NULL);

    struct aws_mqtt5_packet_subscribe_view subscribe_view = {
        .subscriptions = s_subscriptions,
        .subscription_count = AWS_ARRAY_SIZE(s_subscriptions),
    };

    *subscribe_op = aws_mqtt5_operation_subscribe_new(allocator, &subscribe_view, NULL);

    struct aws_mqtt5_packet_unsubscribe_view unsubscribe_view = {
        .topics = s_topics,
        .topic_count = AWS_ARRAY_SIZE(s_topics),
    };

    *unsubscribe_op = aws_mqtt5_operation_unsubscribe_new(allocator, &unsubscribe_view, NULL);
}

static void s_seed_unacked_operations(
    struct aws_mqtt5_client_operational_state *operational_state,
    struct aws_mqtt5_operation_publish *pending_publish,
    struct aws_mqtt5_operation_subscribe *pending_subscribe,
    struct aws_mqtt5_operation_unsubscribe *pending_unsubscribe) {
    aws_hash_table_put(
        &operational_state->unacked_operations_table,
        &pending_publish->options_storage.storage_view.packet_id,
        &pending_publish->base,
        NULL);
    aws_linked_list_push_back(&operational_state->unacked_operations, &pending_publish->base.node);
    aws_hash_table_put(
        &operational_state->unacked_operations_table,
        &pending_subscribe->options_storage.storage_view.packet_id,
        &pending_subscribe->base,
        NULL);
    aws_linked_list_push_back(&operational_state->unacked_operations, &pending_subscribe->base.node);
    aws_hash_table_put(
        &operational_state->unacked_operations_table,
        &pending_unsubscribe->options_storage.storage_view.packet_id,
        &pending_unsubscribe->base,
        NULL);
    aws_linked_list_push_back(&operational_state->unacked_operations, &pending_unsubscribe->base.node);
}

/* test that allocates packet ids from a table with entries that overlap the next id space */
static int s_mqtt5_operation_bind_packet_id_multiple_with_existing_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_mqtt5_operation_publish *pending_publish = NULL;
    struct aws_mqtt5_operation_subscribe *pending_subscribe = NULL;
    struct aws_mqtt5_operation_unsubscribe *pending_unsubscribe = NULL;
    s_create_operations(allocator, &pending_publish, &pending_subscribe, &pending_unsubscribe);

    aws_mqtt5_operation_set_packet_id(&pending_publish->base, 1);
    aws_mqtt5_operation_set_packet_id(&pending_subscribe->base, 3);
    aws_mqtt5_operation_set_packet_id(&pending_unsubscribe->base, 5);

    struct aws_mqtt5_client_operational_state operational_state;
    aws_mqtt5_client_operational_state_init(&operational_state, allocator, NULL);

    s_seed_unacked_operations(&operational_state, pending_publish, pending_subscribe, pending_unsubscribe);

    struct aws_mqtt5_operation_publish *new_publish = NULL;
    struct aws_mqtt5_operation_subscribe *new_subscribe = NULL;
    struct aws_mqtt5_operation_unsubscribe *new_unsubscribe = NULL;
    s_create_operations(allocator, &new_publish, &new_subscribe, &new_unsubscribe);

    ASSERT_SUCCESS(aws_mqtt5_operation_bind_packet_id(&new_publish->base, &operational_state));
    ASSERT_UINT_EQUALS(2, aws_mqtt5_operation_get_packet_id(&new_publish->base));
    ASSERT_UINT_EQUALS(3, operational_state.next_mqtt_packet_id);

    ASSERT_SUCCESS(aws_mqtt5_operation_bind_packet_id(&new_subscribe->base, &operational_state));
    ASSERT_UINT_EQUALS(4, aws_mqtt5_operation_get_packet_id(&new_subscribe->base));
    ASSERT_UINT_EQUALS(5, operational_state.next_mqtt_packet_id);

    ASSERT_SUCCESS(aws_mqtt5_operation_bind_packet_id(&new_unsubscribe->base, &operational_state));
    ASSERT_UINT_EQUALS(6, aws_mqtt5_operation_get_packet_id(&new_unsubscribe->base));
    ASSERT_UINT_EQUALS(7, operational_state.next_mqtt_packet_id);

    aws_mqtt5_client_operational_state_clean_up(&operational_state);
    aws_mqtt5_operation_release(&new_publish->base);
    aws_mqtt5_operation_release(&new_subscribe->base);
    aws_mqtt5_operation_release(&new_unsubscribe->base);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(
    mqtt5_operation_bind_packet_id_multiple_with_existing,
    s_mqtt5_operation_bind_packet_id_multiple_with_existing_fn)

/* test that allocates packet ids from a table where the next id forces an id wraparound */
static int s_mqtt5_operation_bind_packet_id_multiple_with_wrap_around_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_mqtt5_operation_publish *pending_publish = NULL;
    struct aws_mqtt5_operation_subscribe *pending_subscribe = NULL;
    struct aws_mqtt5_operation_unsubscribe *pending_unsubscribe = NULL;
    s_create_operations(allocator, &pending_publish, &pending_subscribe, &pending_unsubscribe);

    aws_mqtt5_operation_set_packet_id(&pending_publish->base, 65533);
    aws_mqtt5_operation_set_packet_id(&pending_subscribe->base, 65535);
    aws_mqtt5_operation_set_packet_id(&pending_unsubscribe->base, 1);

    struct aws_mqtt5_client_operational_state operational_state;
    aws_mqtt5_client_operational_state_init(&operational_state, allocator, NULL);
    operational_state.next_mqtt_packet_id = 65532;

    s_seed_unacked_operations(&operational_state, pending_publish, pending_subscribe, pending_unsubscribe);

    struct aws_mqtt5_operation_publish *new_publish = NULL;
    struct aws_mqtt5_operation_subscribe *new_subscribe = NULL;
    struct aws_mqtt5_operation_unsubscribe *new_unsubscribe = NULL;
    s_create_operations(allocator, &new_publish, &new_subscribe, &new_unsubscribe);

    ASSERT_SUCCESS(aws_mqtt5_operation_bind_packet_id(&new_publish->base, &operational_state));
    ASSERT_UINT_EQUALS(65532, aws_mqtt5_operation_get_packet_id(&new_publish->base));
    ASSERT_UINT_EQUALS(65533, operational_state.next_mqtt_packet_id);

    ASSERT_SUCCESS(aws_mqtt5_operation_bind_packet_id(&new_subscribe->base, &operational_state));
    ASSERT_UINT_EQUALS(65534, aws_mqtt5_operation_get_packet_id(&new_subscribe->base));
    ASSERT_UINT_EQUALS(65535, operational_state.next_mqtt_packet_id);

    ASSERT_SUCCESS(aws_mqtt5_operation_bind_packet_id(&new_unsubscribe->base, &operational_state));
    ASSERT_UINT_EQUALS(2, aws_mqtt5_operation_get_packet_id(&new_unsubscribe->base));
    ASSERT_UINT_EQUALS(3, operational_state.next_mqtt_packet_id);

    aws_mqtt5_client_operational_state_clean_up(&operational_state);
    aws_mqtt5_operation_release(&new_publish->base);
    aws_mqtt5_operation_release(&new_subscribe->base);
    aws_mqtt5_operation_release(&new_unsubscribe->base);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(
    mqtt5_operation_bind_packet_id_multiple_with_wrap_around,
    s_mqtt5_operation_bind_packet_id_multiple_with_wrap_around_fn)

/* test that fails to allocate packet ids from a full table */
static int s_mqtt5_operation_bind_packet_id_full_table_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_mqtt5_packet_publish_view publish_view = {
        .qos = AWS_MQTT5_QOS_AT_LEAST_ONCE,
        .topic = s_topic,
        .payload = s_payload_cursor,
    };

    struct aws_mqtt5_client_operational_state operational_state;
    aws_mqtt5_client_operational_state_init(&operational_state, allocator, NULL);

    for (uint16_t i = 0; i < UINT16_MAX; ++i) {
        struct aws_mqtt5_operation_publish *publish_op =
            aws_mqtt5_operation_publish_new(allocator, &publish_view, NULL);
        aws_mqtt5_operation_set_packet_id(&publish_op->base, i + 1);

        aws_hash_table_put(
            &operational_state.unacked_operations_table,
            &publish_op->options_storage.storage_view.packet_id,
            &publish_op->base,
            NULL);
        aws_linked_list_push_back(&operational_state.unacked_operations, &publish_op->base.node);
    }

    struct aws_mqtt5_operation_publish *new_publish = aws_mqtt5_operation_publish_new(allocator, &publish_view, NULL);

    ASSERT_FAILS(aws_mqtt5_operation_bind_packet_id(&new_publish->base, &operational_state));
    ASSERT_UINT_EQUALS(1, operational_state.next_mqtt_packet_id);

    aws_mqtt5_client_operational_state_clean_up(&operational_state);
    aws_mqtt5_operation_release(&new_publish->base);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_operation_bind_packet_id_full_table, s_mqtt5_operation_bind_packet_id_full_table_fn)

/* test that skips allocation because the packet is not a QOS1+PUBLISH */
static int s_mqtt5_operation_bind_packet_id_not_valid_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_mqtt5_packet_publish_view publish_view = {
        .qos = AWS_MQTT5_QOS_AT_MOST_ONCE,
        .topic = s_topic,
        .payload = s_payload_cursor,
    };

    struct aws_mqtt5_operation_publish *new_publish = aws_mqtt5_operation_publish_new(allocator, &publish_view, NULL);

    struct aws_mqtt5_client_operational_state operational_state;
    aws_mqtt5_client_operational_state_init(&operational_state, allocator, NULL);

    ASSERT_SUCCESS(aws_mqtt5_operation_bind_packet_id(&new_publish->base, &operational_state));
    ASSERT_UINT_EQUALS(0, aws_mqtt5_operation_get_packet_id(&new_publish->base));
    ASSERT_UINT_EQUALS(1, operational_state.next_mqtt_packet_id);

    aws_mqtt5_client_operational_state_clean_up(&operational_state);
    aws_mqtt5_operation_release(&new_publish->base);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_operation_bind_packet_id_not_valid, s_mqtt5_operation_bind_packet_id_not_valid_fn)

/* test that skips allocation because the packet already has an id bound */
static int s_mqtt5_operation_bind_packet_id_already_bound_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct aws_mqtt5_packet_publish_view publish_view = {
        .qos = AWS_MQTT5_QOS_AT_LEAST_ONCE,
        .topic = s_topic,
        .payload = s_payload_cursor,
    };

    struct aws_mqtt5_operation_publish *new_publish = aws_mqtt5_operation_publish_new(allocator, &publish_view, NULL);
    aws_mqtt5_operation_set_packet_id(&new_publish->base, 2);

    struct aws_mqtt5_client_operational_state operational_state;
    aws_mqtt5_client_operational_state_init(&operational_state, allocator, NULL);

    ASSERT_SUCCESS(aws_mqtt5_operation_bind_packet_id(&new_publish->base, &operational_state));
    ASSERT_UINT_EQUALS(2, aws_mqtt5_operation_get_packet_id(&new_publish->base));
    ASSERT_UINT_EQUALS(1, operational_state.next_mqtt_packet_id);

    aws_mqtt5_client_operational_state_clean_up(&operational_state);
    aws_mqtt5_operation_release(&new_publish->base);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_operation_bind_packet_id_already_bound, s_mqtt5_operation_bind_packet_id_already_bound_fn)

struct aws_mqtt5_operation_processing_test_context {
    struct aws_allocator *allocator;
    struct aws_mqtt5_client dummy_client;
    struct aws_mqtt5_client_vtable vtable;
    struct aws_channel_slot dummy_slot;

    struct aws_mqtt5_encoder verification_encoder;
    struct aws_array_list output_io_messages;
};

static struct aws_io_message *s_aws_channel_acquire_message_from_pool_success_fn(
    struct aws_channel *channel,
    enum aws_io_message_type message_type,
    size_t size_hint,
    void *user_data) {

    struct aws_mqtt5_operation_processing_test_context *test_context = user_data;
    struct aws_allocator *allocator = test_context->allocator;

    struct aws_io_message *new_message = aws_mem_calloc(allocator, 1, sizeof(struct aws_io_message));
    new_message->allocator = allocator;
    aws_byte_buf_init(&new_message->message_data, allocator, size_hint);

    return new_message;
}

static int s_aws_channel_slot_send_message_success_fn(
    struct aws_channel_slot *slot,
    struct aws_io_message *message,
    enum aws_channel_direction dir,
    void *user_data) {

    struct aws_mqtt5_operation_processing_test_context *test_context = user_data;

    aws_array_list_push_back(&test_context->output_io_messages, &message);

    return AWS_OP_SUCCESS;
}

static struct aws_io_message *s_aws_channel_acquire_message_from_pool_failure_fn(
    struct aws_channel *channel,
    enum aws_io_message_type message_type,
    size_t size_hint,
    void *user_data) {
    (void)channel;
    (void)message_type;
    (void)size_hint;
    (void)user_data;

    aws_raise_error(AWS_ERROR_INVALID_STATE);
    return NULL;
}

static int s_aws_channel_slot_send_message_failure_fn(
    struct aws_channel_slot *slot,
    struct aws_io_message *message,
    enum aws_channel_direction dir,
    void *user_data) {

    (void)slot;
    (void)message;
    (void)dir;
    (void)user_data;

    return aws_raise_error(AWS_ERROR_INVALID_STATE);
}

static void s_aws_mqtt5_operation_processing_test_context_init(
    struct aws_mqtt5_operation_processing_test_context *test_context,
    struct aws_allocator *allocator) {
    AWS_ZERO_STRUCT(*test_context);

    test_context->allocator = allocator;
    aws_mqtt5_client_operational_state_init(
        &test_context->dummy_client.operational_state, allocator, &test_context->dummy_client);

    struct aws_mqtt5_encoder_options encoder_options = {
        .client = &test_context->dummy_client,
    };

    aws_mqtt5_encoder_init(&test_context->dummy_client.encoder, allocator, &encoder_options);

    test_context->vtable = *aws_mqtt5_client_get_default_vtable();
    test_context->vtable.aws_channel_acquire_message_from_pool_fn = s_aws_channel_acquire_message_from_pool_success_fn;
    test_context->vtable.aws_channel_slot_send_message_fn = s_aws_channel_slot_send_message_success_fn;
    test_context->vtable.vtable_user_data = test_context;

    test_context->dummy_client.vtable = &test_context->vtable;

    /* this keeps the operation processing logic from crashing when dereferencing client->slot->channel */
    test_context->dummy_client.slot = &test_context->dummy_slot;

    aws_array_list_init_dynamic(&test_context->output_io_messages, allocator, 0, sizeof(struct aws_io_message *));

    struct aws_mqtt5_encoder_options verification_encoder_options = {
        .client = NULL,
    };

    aws_mqtt5_encoder_init(&test_context->verification_encoder, allocator, &verification_encoder_options);
}

static void s_aws_mqtt5_operation_processing_test_context_clean_up(
    struct aws_mqtt5_operation_processing_test_context *test_context) {
    for (size_t i = 0; i < aws_array_list_length(&test_context->output_io_messages); ++i) {
        struct aws_io_message *message = NULL;
        aws_array_list_get_at(&test_context->output_io_messages, &message, i);

        aws_byte_buf_clean_up(&message->message_data);
        aws_mem_release(message->allocator, message);
    }

    aws_array_list_clean_up(&test_context->output_io_messages);
    aws_mqtt5_encoder_clean_up(&test_context->verification_encoder);

    aws_mqtt5_encoder_clean_up(&test_context->dummy_client.encoder);
    aws_mqtt5_client_operational_state_clean_up(&test_context->dummy_client.operational_state);
}

static void s_aws_mqtt5_operation_processing_test_context_enqueue_op(
    struct aws_mqtt5_operation_processing_test_context *test_context,
    struct aws_mqtt5_operation *operation) {
    aws_linked_list_push_back(&test_context->dummy_client.operational_state.queued_operations, &operation->node);
}

static int s_mqtt5_operation_processing_nothing_empty_queue_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_mqtt5_operation_processing_test_context test_context;
    s_aws_mqtt5_operation_processing_test_context_init(&test_context, allocator);

    for (int32_t i = 0; i <= AWS_MCS_TERMINATED; ++i) {
        enum aws_mqtt5_client_state state = i;

        test_context.dummy_client.current_state = state;
        ASSERT_SUCCESS(aws_mqtt5_client_service_operational_state(&test_context.dummy_client.operational_state));
        ASSERT_UINT_EQUALS(0, aws_array_list_length(&test_context.output_io_messages));
    }

    s_aws_mqtt5_operation_processing_test_context_clean_up(&test_context);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_operation_processing_nothing_empty_queue, s_mqtt5_operation_processing_nothing_empty_queue_fn)

static struct aws_mqtt5_operation_subscribe *s_make_simple_subscribe_operation(struct aws_allocator *allocator) {
    struct aws_mqtt5_packet_subscribe_view subscribe_view = {
        .subscriptions = s_subscriptions,
        .subscription_count = AWS_ARRAY_SIZE(s_subscriptions),
    };

    return aws_mqtt5_operation_subscribe_new(allocator, &subscribe_view, NULL);
}

static int s_mqtt5_operation_processing_nothing_mqtt_connect_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_mqtt5_operation_processing_test_context test_context;
    s_aws_mqtt5_operation_processing_test_context_init(&test_context, allocator);

    test_context.dummy_client.current_state = AWS_MCS_MQTT_CONNECT;

    struct aws_mqtt5_operation_subscribe *subscribe_op = s_make_simple_subscribe_operation(allocator);
    s_aws_mqtt5_operation_processing_test_context_enqueue_op(&test_context, &subscribe_op->base);

    ASSERT_SUCCESS(aws_mqtt5_client_service_operational_state(&test_context.dummy_client.operational_state));
    ASSERT_UINT_EQUALS(0, aws_array_list_length(&test_context.output_io_messages));

    s_aws_mqtt5_operation_processing_test_context_clean_up(&test_context);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_operation_processing_nothing_mqtt_connect, s_mqtt5_operation_processing_nothing_mqtt_connect_fn)

static int s_mqtt5_operation_processing_nothing_clean_disconnect_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_mqtt5_operation_processing_test_context test_context;
    s_aws_mqtt5_operation_processing_test_context_init(&test_context, allocator);

    test_context.dummy_client.current_state = AWS_MCS_CLEAN_DISCONNECT;

    struct aws_mqtt5_operation_subscribe *subscribe_op = s_make_simple_subscribe_operation(allocator);
    s_aws_mqtt5_operation_processing_test_context_enqueue_op(&test_context, &subscribe_op->base);

    ASSERT_SUCCESS(aws_mqtt5_client_service_operational_state(&test_context.dummy_client.operational_state));
    ASSERT_UINT_EQUALS(0, aws_array_list_length(&test_context.output_io_messages));

    s_aws_mqtt5_operation_processing_test_context_clean_up(&test_context);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(
    mqtt5_operation_processing_nothing_clean_disconnect,
    s_mqtt5_operation_processing_nothing_clean_disconnect_fn)

static struct aws_mqtt5_operation_connect *s_make_simple_connect_operation(struct aws_allocator *allocator) {
    struct aws_mqtt5_packet_connect_view connect_view = {
        .keep_alive_interval_seconds = 0,
    };

    return aws_mqtt5_operation_connect_new(allocator, &connect_view);
}

static int s_mqtt5_operation_processing_nothing_pending_write_completion_mqtt_connect_fn(
    struct aws_allocator *allocator,
    void *ctx) {
    (void)ctx;

    struct aws_mqtt5_operation_processing_test_context test_context;
    s_aws_mqtt5_operation_processing_test_context_init(&test_context, allocator);

    test_context.dummy_client.current_state = AWS_MCS_MQTT_CONNECT;

    struct aws_mqtt5_operation_connect *connect_op = s_make_simple_connect_operation(allocator);
    s_aws_mqtt5_operation_processing_test_context_enqueue_op(&test_context, &connect_op->base);
    test_context.dummy_client.operational_state.pending_write_completion = true;

    ASSERT_SUCCESS(aws_mqtt5_client_service_operational_state(&test_context.dummy_client.operational_state));
    ASSERT_UINT_EQUALS(0, aws_array_list_length(&test_context.output_io_messages));

    s_aws_mqtt5_operation_processing_test_context_clean_up(&test_context);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(
    mqtt5_operation_processing_nothing_pending_write_completion_mqtt_connect,
    s_mqtt5_operation_processing_nothing_pending_write_completion_mqtt_connect_fn)

static int s_mqtt5_operation_processing_nothing_pending_write_completion_connected_fn(
    struct aws_allocator *allocator,
    void *ctx) {
    (void)ctx;

    struct aws_mqtt5_operation_processing_test_context test_context;
    s_aws_mqtt5_operation_processing_test_context_init(&test_context, allocator);

    test_context.dummy_client.current_state = AWS_MCS_CONNECTED;

    struct aws_mqtt5_operation_subscribe *subscribe_op = s_make_simple_subscribe_operation(allocator);
    s_aws_mqtt5_operation_processing_test_context_enqueue_op(&test_context, &subscribe_op->base);
    test_context.dummy_client.operational_state.pending_write_completion = true;

    ASSERT_SUCCESS(aws_mqtt5_client_service_operational_state(&test_context.dummy_client.operational_state));
    ASSERT_UINT_EQUALS(0, aws_array_list_length(&test_context.output_io_messages));

    s_aws_mqtt5_operation_processing_test_context_clean_up(&test_context);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(
    mqtt5_operation_processing_nothing_pending_write_completion_connected,
    s_mqtt5_operation_processing_nothing_pending_write_completion_connected_fn)

static struct aws_mqtt5_operation_disconnect *s_make_simple_disconnect_operation(struct aws_allocator *allocator) {
    struct aws_mqtt5_packet_disconnect_view disconnect_view = {
        .reason_code = AWS_MQTT5_DRC_ADMINISTRATIVE_ACTION,
    };

    return aws_mqtt5_operation_disconnect_new(allocator, &disconnect_view);
}

static int s_mqtt5_operation_processing_nothing_pending_write_completion_clean_disconnect_fn(
    struct aws_allocator *allocator,
    void *ctx) {
    (void)ctx;

    struct aws_mqtt5_operation_processing_test_context test_context;
    s_aws_mqtt5_operation_processing_test_context_init(&test_context, allocator);

    test_context.dummy_client.current_state = AWS_MCS_CLEAN_DISCONNECT;

    struct aws_mqtt5_operation_disconnect *disconnect_op = s_make_simple_disconnect_operation(allocator);
    s_aws_mqtt5_operation_processing_test_context_enqueue_op(&test_context, &disconnect_op->base);
    test_context.dummy_client.operational_state.pending_write_completion = true;

    ASSERT_SUCCESS(aws_mqtt5_client_service_operational_state(&test_context.dummy_client.operational_state));
    ASSERT_UINT_EQUALS(0, aws_array_list_length(&test_context.output_io_messages));

    s_aws_mqtt5_operation_processing_test_context_clean_up(&test_context);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(
    mqtt5_operation_processing_nothing_pending_write_completion_clean_disconnect,
    s_mqtt5_operation_processing_nothing_pending_write_completion_clean_disconnect_fn)

static int s_mqtt5_operation_processing_failure_message_allocation_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_mqtt5_operation_processing_test_context test_context;
    s_aws_mqtt5_operation_processing_test_context_init(&test_context, allocator);
    test_context.vtable.aws_channel_acquire_message_from_pool_fn = s_aws_channel_acquire_message_from_pool_failure_fn;

    test_context.dummy_client.current_state = AWS_MCS_CONNECTED;

    struct aws_mqtt5_operation_subscribe *subscribe_op = s_make_simple_subscribe_operation(allocator);
    s_aws_mqtt5_operation_processing_test_context_enqueue_op(&test_context, &subscribe_op->base);

    ASSERT_FAILS(aws_mqtt5_client_service_operational_state(&test_context.dummy_client.operational_state));
    ASSERT_UINT_EQUALS(0, aws_array_list_length(&test_context.output_io_messages));

    s_aws_mqtt5_operation_processing_test_context_clean_up(&test_context);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(
    mqtt5_operation_processing_failure_message_allocation,
    s_mqtt5_operation_processing_failure_message_allocation_fn)

static int s_mqtt5_operation_processing_failure_message_send_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_mqtt5_operation_processing_test_context test_context;
    s_aws_mqtt5_operation_processing_test_context_init(&test_context, allocator);
    test_context.vtable.aws_channel_slot_send_message_fn = s_aws_channel_slot_send_message_failure_fn;

    test_context.dummy_client.current_state = AWS_MCS_CONNECTED;

    struct aws_mqtt5_operation_subscribe *subscribe_op = s_make_simple_subscribe_operation(allocator);
    s_aws_mqtt5_operation_processing_test_context_enqueue_op(&test_context, &subscribe_op->base);

    ASSERT_FAILS(aws_mqtt5_client_service_operational_state(&test_context.dummy_client.operational_state));
    ASSERT_UINT_EQUALS(0, aws_array_list_length(&test_context.output_io_messages));

    s_aws_mqtt5_operation_processing_test_context_clean_up(&test_context);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_operation_processing_failure_message_send, s_mqtt5_operation_processing_failure_message_send_fn)

static int s_verify_operation_list_versus_expected(
    struct aws_linked_list *operation_list,
    struct aws_mqtt5_operation *expected_operations[],
    size_t expected_operations_size) {
    struct aws_linked_list_node *node = aws_linked_list_begin(operation_list);
    for (size_t i = 0; i < expected_operations_size; ++i) {
        ASSERT_TRUE(node != aws_linked_list_end(operation_list));
        struct aws_mqtt5_operation *operation = expected_operations[i];
        struct aws_mqtt5_operation *queued_operation = AWS_CONTAINER_OF(node, struct aws_mqtt5_operation, node);

        ASSERT_PTR_EQUALS(operation, queued_operation);
        node = aws_linked_list_next(node);
    }
    ASSERT_TRUE(node == aws_linked_list_end(operation_list));

    return AWS_OP_SUCCESS;
}

static int s_do_simple_operation_processing_io_message_write_test(
    struct aws_allocator *allocator,
    struct aws_mqtt5_operation_processing_test_context *test_context,
    struct aws_mqtt5_operation *expected_writes[],
    size_t expected_writes_size,
    struct aws_mqtt5_operation *expected_queued[],
    size_t expected_queued_size) {
    for (size_t i = 0; i < expected_writes_size; ++i) {
        s_aws_mqtt5_operation_processing_test_context_enqueue_op(test_context, expected_writes[i]);
    }

    for (size_t i = 0; i < expected_queued_size; ++i) {
        s_aws_mqtt5_operation_processing_test_context_enqueue_op(test_context, expected_queued[i]);
    }

    ASSERT_SUCCESS(aws_mqtt5_client_service_operational_state(&test_context->dummy_client.operational_state));
    ASSERT_UINT_EQUALS(1, aws_array_list_length(&test_context->output_io_messages));

    struct aws_io_message *message = NULL;
    aws_array_list_get_at(&test_context->output_io_messages, &message, 0);

    struct aws_byte_buf verification_buffer;
    aws_byte_buf_init(&verification_buffer, allocator, 4096);

    for (size_t i = 0; i < expected_writes_size; ++i) {
        struct aws_mqtt5_operation *operation = expected_writes[i];

        aws_mqtt5_encoder_append_packet_encoding(
            &test_context->verification_encoder, operation->packet_type, operation->packet_view);
        aws_mqtt5_encoder_encode_to_buffer(&test_context->verification_encoder, &verification_buffer);
    }

    ASSERT_BIN_ARRAYS_EQUALS(
        verification_buffer.buffer, verification_buffer.len, message->message_data.buffer, message->message_data.len);

    aws_byte_buf_clean_up(&verification_buffer);

    ASSERT_SUCCESS(s_verify_operation_list_versus_expected(
        &test_context->dummy_client.operational_state.queued_operations, expected_queued, expected_queued_size));
    ASSERT_SUCCESS(s_verify_operation_list_versus_expected(
        &test_context->dummy_client.operational_state.write_completion_operations,
        expected_writes,
        expected_writes_size));

    return AWS_OP_SUCCESS;
}

static int s_mqtt5_operation_processing_something_mqtt_connect_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_mqtt5_operation_processing_test_context test_context;
    s_aws_mqtt5_operation_processing_test_context_init(&test_context, allocator);

    test_context.dummy_client.current_state = AWS_MCS_MQTT_CONNECT;

    struct aws_mqtt5_operation *write_operations[] = {
        &s_make_simple_connect_operation(allocator)->base,
    };

    struct aws_mqtt5_operation *queued_operations[] = {
        &s_make_simple_subscribe_operation(allocator)->base,
    };

    ASSERT_SUCCESS(s_do_simple_operation_processing_io_message_write_test(
        allocator,
        &test_context,
        write_operations,
        AWS_ARRAY_SIZE(write_operations),
        queued_operations,
        AWS_ARRAY_SIZE(queued_operations)));

    s_aws_mqtt5_operation_processing_test_context_clean_up(&test_context);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_operation_processing_something_mqtt_connect, s_mqtt5_operation_processing_something_mqtt_connect_fn)

static int s_mqtt5_operation_processing_something_clean_disconnect_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_mqtt5_operation_processing_test_context test_context;
    s_aws_mqtt5_operation_processing_test_context_init(&test_context, allocator);

    test_context.dummy_client.current_state = AWS_MCS_CLEAN_DISCONNECT;

    struct aws_mqtt5_operation *write_operations[] = {
        &s_make_simple_disconnect_operation(allocator)->base,
    };

    struct aws_mqtt5_operation *queued_operations[] = {
        &s_make_simple_subscribe_operation(allocator)->base,
    };

    ASSERT_SUCCESS(s_do_simple_operation_processing_io_message_write_test(
        allocator,
        &test_context,
        write_operations,
        AWS_ARRAY_SIZE(write_operations),
        queued_operations,
        AWS_ARRAY_SIZE(queued_operations)));

    s_aws_mqtt5_operation_processing_test_context_clean_up(&test_context);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(
    mqtt5_operation_processing_something_clean_disconnect,
    s_mqtt5_operation_processing_something_clean_disconnect_fn)

static int s_mqtt5_operation_processing_something_connected_multi_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_mqtt5_operation_processing_test_context test_context;
    s_aws_mqtt5_operation_processing_test_context_init(&test_context, allocator);

    test_context.dummy_client.current_state = AWS_MCS_CONNECTED;

    /* TODO: make this a mixture of different ops, but currently, we only have encode for disconnect */
    struct aws_mqtt5_operation *write_operations[] = {
        &s_make_simple_disconnect_operation(allocator)->base,
        &s_make_simple_disconnect_operation(allocator)->base,
        &s_make_simple_disconnect_operation(allocator)->base,
    };

    ASSERT_SUCCESS(s_do_simple_operation_processing_io_message_write_test(
        allocator, &test_context, write_operations, AWS_ARRAY_SIZE(write_operations), NULL, 0));

    s_aws_mqtt5_operation_processing_test_context_clean_up(&test_context);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(
    mqtt5_operation_processing_something_connected_multi,
    s_mqtt5_operation_processing_something_connected_multi_fn)
