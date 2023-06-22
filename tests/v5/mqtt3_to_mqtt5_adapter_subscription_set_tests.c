/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>
#include <aws/mqtt/private/client_impl_shared.h>
#include <aws/mqtt/private/v5/mqtt3_to_mqtt5_adapter_subscription_set.h>

#include <aws/testing/aws_test_harness.h>

struct subscription_test_context_callback_record {
    struct aws_allocator *allocator;

    struct aws_byte_cursor topic;
    struct aws_byte_buf topic_buffer;

    size_t callback_count;
};

static struct subscription_test_context_callback_record *s_subscription_test_context_callback_record_new(
    struct aws_allocator *allocator,
    struct aws_byte_cursor topic) {
    struct subscription_test_context_callback_record *record =
        aws_mem_calloc(allocator, 1, sizeof(struct subscription_test_context_callback_record));
    record->allocator = allocator;
    record->callback_count = 1;

    aws_byte_buf_init_copy_from_cursor(&record->topic_buffer, allocator, topic);
    record->topic = aws_byte_cursor_from_buf(&record->topic_buffer);

    return record;
}

static void s_subscription_test_context_callback_record_destroy(
    struct subscription_test_context_callback_record *record) {
    if (record == NULL) {
        return;
    }

    aws_byte_buf_clean_up(&record->topic_buffer);

    aws_mem_release(record->allocator, record);
}

static void s_destroy_callback_record(void *element) {
    struct subscription_test_context_callback_record *record = element;

    s_subscription_test_context_callback_record_destroy(record);
}

struct aws_mqtt_adapter_subscription_set_test_context {
    struct aws_allocator *allocator;

    struct aws_hash_table callbacks;
};

static void s_aws_mqtt_adapter_subscription_set_test_context_init(
    struct aws_mqtt_adapter_subscription_set_test_context *context,
    struct aws_allocator *allocator) {
    context->allocator = allocator;

    aws_hash_table_init(
        &context->callbacks,
        allocator,
        10,
        aws_hash_byte_cursor_ptr,
        aws_mqtt_byte_cursor_hash_equality,
        NULL,
        s_destroy_callback_record);
}

static void s_aws_mqtt_adapter_subscription_set_test_context_clean_up(
    struct aws_mqtt_adapter_subscription_set_test_context *context) {
    aws_hash_table_clean_up(&context->callbacks);
}

static void s_aws_mqtt_adapter_subscription_set_test_context_record_callback(
    struct aws_mqtt_adapter_subscription_set_test_context *context,
    struct aws_byte_cursor topic) {

    struct aws_hash_element *element = NULL;
    aws_hash_table_find(&context->callbacks, &topic, &element);

    if (element == NULL) {
        struct subscription_test_context_callback_record *record =
            s_subscription_test_context_callback_record_new(context->allocator, topic);
        aws_hash_table_put(&context->callbacks, &record->topic, record, NULL);
    } else {
        struct subscription_test_context_callback_record *record = element->value;
        ++record->callback_count;
    }
}

static int s_aws_mqtt_adapter_subscription_set_test_context_validate_callbacks(
    struct aws_mqtt_adapter_subscription_set_test_context *context,
    struct subscription_test_context_callback_record *expected_records,
    size_t expected_record_count) {
    ASSERT_INT_EQUALS(expected_record_count, aws_hash_table_get_entry_count(&context->callbacks));

    for (size_t i = 0; i < expected_record_count; ++i) {
        struct subscription_test_context_callback_record *expected_record = expected_records + i;

        struct aws_hash_element *element = NULL;
        aws_hash_table_find(&context->callbacks, &expected_record->topic, &element);

        ASSERT_TRUE(element != NULL);
        ASSERT_TRUE(element->value != NULL);

        struct subscription_test_context_callback_record *actual_record = element->value;

        ASSERT_INT_EQUALS(expected_record->callback_count, actual_record->callback_count);
    }

    return AWS_OP_SUCCESS;
}

static void s_subscription_set_test_on_publish_received(
    struct aws_mqtt_client_connection *connection,
    const struct aws_byte_cursor *topic,
    const struct aws_byte_cursor *payload,
    bool dup,
    enum aws_mqtt_qos qos,
    bool retain,
    void *userdata) {
    (void)connection;
    (void)payload;
    (void)qos;
    (void)dup;
    (void)retain;

    struct aws_mqtt_adapter_subscription_set_test_context *context = userdata;

    s_aws_mqtt_adapter_subscription_set_test_context_record_callback(context, *topic);
}

enum subscription_set_operation_type {
    SSOT_ADD,
    SSOT_REMOVE,
    SSOT_PUBLISH,
};

struct subscription_set_operation {
    enum subscription_set_operation_type type;

    const char *topic_filter;

    const char *topic;
};

static void s_subscription_set_perform_operations(
    struct aws_mqtt_adapter_subscription_set_test_context *context,
    struct aws_mqtt3_to_mqtt5_adapter_subscription_set *subscription_set,
    struct subscription_set_operation *operations,
    size_t operation_count) {
    for (size_t i = 0; i < operation_count; ++i) {
        struct subscription_set_operation *operation = operations + i;

        switch (operation->type) {
            case SSOT_ADD: {
                struct aws_mqtt3_to_mqtt5_adapter_subscription_options subscription_options = {
                    .topic_filter = aws_byte_cursor_from_c_str(operation->topic_filter),
                    .callback_user_data = context,
                    .on_publish_received = s_subscription_set_test_on_publish_received,
                };
                aws_mqtt3_to_mqtt5_adapter_subscription_set_add_subscription(subscription_set, &subscription_options);
                break;
            }

            case SSOT_REMOVE:
                aws_mqtt3_to_mqtt5_adapter_subscription_set_remove_subscription(
                    subscription_set, aws_byte_cursor_from_c_str(operation->topic_filter));
                break;

            case SSOT_PUBLISH: {
                struct aws_mqtt3_to_mqtt5_adapter_publish_received_options publish_options = {
                    .topic = aws_byte_cursor_from_c_str(operation->topic),
                };
                aws_mqtt3_to_mqtt5_adapter_subscription_set_on_publish_received(subscription_set, &publish_options);
                break;
            }
        }
    }
}

static int s_mqtt3to5_adapter_subscription_set_ph_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct aws_mqtt_adapter_subscription_set_test_context context;
    s_aws_mqtt_adapter_subscription_set_test_context_init(&context, allocator);

    struct aws_mqtt3_to_mqtt5_adapter_subscription_set *subscription_set =
        aws_mqtt3_to_mqtt5_adapter_subscription_set_new(allocator);

    struct subscription_set_operation operations[] = {
        {
            .type = SSOT_ADD,
            .topic_filter = "a/b/#",
        },
        {
            .type = SSOT_PUBLISH,
            .topic = "a/b/c",
        },
    };

    s_subscription_set_perform_operations(&context, subscription_set, operations, AWS_ARRAY_SIZE(operations));

    struct subscription_test_context_callback_record expected_callback_records[] = {{
        .topic = aws_byte_cursor_from_c_str("a/b/c"),
        .callback_count = 1,
    }};
    ASSERT_SUCCESS(s_aws_mqtt_adapter_subscription_set_test_context_validate_callbacks(
        &context, expected_callback_records, AWS_ARRAY_SIZE(expected_callback_records)));

    aws_mqtt3_to_mqtt5_adapter_subscription_set_destroy(subscription_set);

    s_aws_mqtt_adapter_subscription_set_test_context_clean_up(&context);

    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt3to5_adapter_subscription_set_ph, s_mqtt3to5_adapter_subscription_set_ph_fn)
