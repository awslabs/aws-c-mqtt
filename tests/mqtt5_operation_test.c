/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/string.h>

#include <aws/io/stream.h>
#include <aws/mqtt/mqtt.h>
#include <aws/mqtt/private/v5/mqtt5_options_storage.h>
#include <aws/mqtt/v5/mqtt5_types.h>

#include <aws/testing/aws_test_harness.h>

static int s_verify_user_properties(
    struct aws_mqtt5_user_property_set *property_set,
    size_t expected_count,
    const struct aws_mqtt5_user_property *expected_properties) {
    ASSERT_UINT_EQUALS(expected_count, aws_mqtt5_user_property_set_size(property_set));

    for (size_t i = 0; i < expected_count; ++i) {
        const struct aws_mqtt5_user_property *expected_property = &expected_properties[i];
        struct aws_byte_cursor expected_name = expected_property->name;
        struct aws_byte_cursor expected_value = expected_property->value;

        bool found = false;
        for (size_t j = 0; j < expected_count; ++j) {
            struct aws_mqtt5_user_property nv_pair;
            if (aws_mqtt5_user_property_set_get_property(property_set, j, &nv_pair)) {
                return AWS_OP_ERR;
            }

            if (aws_byte_cursor_compare_lexical(&expected_name, &nv_pair.name) == 0 &&
                aws_byte_cursor_compare_lexical(&expected_value, &nv_pair.value) == 0) {
                found = true;
                break;
            }
        }

        if (!found) {
            return AWS_OP_ERR;
        }
    }

    return AWS_OP_SUCCESS;
}

static const char *PUBLISH_PAYLOAD = "hello-world";
static const char *PUBLISH_TOPIC = "greetings/friendly";

static int s_mqtt5_publish_operation_new_set_no_optional_fn(struct aws_allocator *allocator, void *ctx) {
    struct aws_mqtt5_packet_publish_view publish_options = {
        .qos = AWS_MQTT5_QOS_AT_LEAST_ONCE,
        .retain = true,
        .topic = aws_byte_cursor_from_c_str(PUBLISH_TOPIC),
        .payload_format = AWS_MQTT5_PFI_NOT_SET,
        .message_expiry_interval_seconds = NULL,
        .topic_alias = NULL,
        .response_topic = NULL,
        .correlation_data = NULL,
        .subscription_identifier_count = 0,
        .subscription_identifiers = NULL,
        .content_type = NULL,
        .user_property_count = 0,
        .user_properties = NULL,
    };

    struct aws_mqtt5_operation_publish *publish_op =
        aws_mqtt5_operation_publish_new(allocator, &publish_options, NULL, NULL);

    ASSERT_NULL(publish_op->payload);
    ASSERT_UINT_EQUALS((uint32_t)publish_options.qos, (uint32_t)publish_op->qos);
    ASSERT_TRUE(publish_op->retain);
    ASSERT_NOT_NULL(publish_op->topic);
    ASSERT_BIN_ARRAYS_EQUALS(
        publish_options.topic.ptr, publish_options.topic.len, publish_op->topic->bytes, publish_op->topic->len);
    ASSERT_UINT_EQUALS((uint32_t)publish_options.payload_format, (uint32_t)publish_op->payload_format);
    ASSERT_NULL(publish_op->message_expiry_interval_seconds_ptr);
    ASSERT_NULL(publish_op->topic_alias_ptr);
    ASSERT_NULL(publish_op->response_topic);
    ASSERT_NULL(publish_op->correlation_data_ptr);
    ASSERT_NULL(publish_op->content_type);
    ASSERT_SUCCESS(s_verify_user_properties(&publish_op->user_properties, 0, NULL));
    ASSERT_NULL(publish_op->completion_options.completion_callback);
    ASSERT_NULL(publish_op->completion_options.completion_user_data);

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
    struct aws_byte_cursor response_topic = aws_byte_cursor_from_c_str(s_response_topic);
    struct aws_byte_cursor correlation_data = aws_byte_cursor_from_c_str(s_correlation_data);
    struct aws_byte_cursor content_type = aws_byte_cursor_from_c_str(s_content_type);

    struct aws_mqtt5_packet_publish_view publish_options = {
        .qos = AWS_MQTT5_QOS_EXACTLY_ONCE,
        .retain = false,
        .topic = aws_byte_cursor_from_c_str(PUBLISH_TOPIC),
        .payload_format = AWS_MQTT5_PFI_UTF8,
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

    struct aws_byte_cursor payload_cursor = aws_byte_cursor_from_c_str(PUBLISH_PAYLOAD);
    struct aws_input_stream *payload_stream = aws_input_stream_new_from_cursor(allocator, &payload_cursor);

    struct aws_mqtt5_operation_publish *publish_op =
        aws_mqtt5_operation_publish_new(allocator, &publish_options, payload_stream, &completion_options);

    ASSERT_PTR_EQUALS(payload_stream, publish_op->payload);
    ASSERT_UINT_EQUALS((uint32_t)publish_options.qos, (uint32_t)publish_op->qos);
    ASSERT_FALSE(publish_op->retain);
    ASSERT_NOT_NULL(publish_op->topic);
    ASSERT_BIN_ARRAYS_EQUALS(
        publish_options.topic.ptr, publish_options.topic.len, publish_op->topic->bytes, publish_op->topic->len);
    ASSERT_UINT_EQUALS((uint32_t)publish_options.payload_format, (uint32_t)publish_op->payload_format);

    ASSERT_PTR_EQUALS(&publish_op->message_expiry_interval_seconds, publish_op->message_expiry_interval_seconds_ptr);
    ASSERT_UINT_EQUALS(*publish_options.message_expiry_interval_seconds, publish_op->message_expiry_interval_seconds);

    ASSERT_PTR_EQUALS(&publish_op->topic_alias, publish_op->topic_alias_ptr);
    ASSERT_UINT_EQUALS(*publish_options.topic_alias, publish_op->topic_alias);

    ASSERT_FALSE(publish_options.response_topic->ptr == publish_op->response_topic->bytes);
    ASSERT_BIN_ARRAYS_EQUALS(
        publish_options.response_topic->ptr,
        publish_options.response_topic->len,
        publish_op->response_topic->bytes,
        publish_op->response_topic->len);

    ASSERT_FALSE(publish_options.correlation_data->ptr == publish_op->correlation_data_ptr->buffer);
    ASSERT_PTR_EQUALS(&publish_op->correlation_data, publish_op->correlation_data_ptr);
    ASSERT_BIN_ARRAYS_EQUALS(
        publish_options.correlation_data->ptr,
        publish_options.correlation_data->len,
        publish_op->correlation_data.buffer,
        publish_op->correlation_data.len);

    ASSERT_FALSE(publish_options.content_type->ptr == publish_op->content_type->bytes);
    ASSERT_BIN_ARRAYS_EQUALS(
        publish_options.content_type->ptr,
        publish_options.content_type->len,
        publish_op->content_type->bytes,
        publish_op->content_type->len);

    ASSERT_SUCCESS(
        s_verify_user_properties(&publish_op->user_properties, AWS_ARRAY_SIZE(s_user_properties), s_user_properties));

    ASSERT_PTR_EQUALS(completion_options.completion_callback, publish_op->completion_options.completion_callback);
    ASSERT_PTR_EQUALS(completion_options.completion_user_data, publish_op->completion_options.completion_user_data);

    aws_mqtt5_operation_release(&publish_op->base);

    aws_input_stream_destroy(payload_stream);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_publish_operation_new_set_all, s_mqtt5_publish_operation_new_set_all_fn)

/* Utility functions that will be needed for client config testing */

#ifdef WHEN_NEEDED

static struct aws_client_bootstrap *s_new_bootstrap(struct aws_allocator *allocator) {
    struct aws_event_loop_group *elg = aws_event_loop_group_new_default(allocator, 1, NULL);

    struct aws_host_resolver_default_options hr_options = {
        .max_entries = 16,
        .el_group = elg,
    };

    struct aws_host_resolver *hr = aws_host_resolver_new_default(allocator, &hr_options);

    struct aws_client_bootstrap_options options = {
        .event_loop_group = elg,
        .host_resolver = hr,
    };

    struct aws_client_bootstrap *bootstrap = aws_client_bootstrap_new(allocator, &options);
    aws_event_loop_group_release(elg);
    aws_host_resolver_release(hr);

    return bootstrap;
}

static int s_init_tls_options(
    struct aws_allocator *allocator,
    const char *server_name,
    const char *alpn_list,
    struct aws_tls_connection_options *options_out) {
    struct aws_tls_ctx_options tls_ctx_options;
    AWS_ZERO_STRUCT(tls_ctx_options);
    aws_tls_ctx_options_init_default_client(&tls_ctx_options, allocator);

    struct aws_tls_ctx *tls_context = aws_tls_client_ctx_new(allocator, &tls_ctx_options);

    aws_tls_connection_options_init_from_ctx(options_out, tls_context);
    struct aws_byte_cursor server_name_cursor = aws_byte_cursor_from_c_str(server_name);
    ASSERT_SUCCESS(aws_tls_connection_options_set_server_name(options_out, allocator, &server_name_cursor));
    ASSERT_SUCCESS(aws_tls_connection_options_set_alpn_list(options_out, allocator, alpn_list));

    aws_tls_ctx_release(tls_context);

    return AWS_OP_SUCCESS;
}

#endif /* WHEN_NEEDED */
