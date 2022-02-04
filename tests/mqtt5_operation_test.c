/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/string.h>

#include <aws/io/stream.h>
#include <aws/mqtt/mqtt.h>
#include <aws/mqtt/private/v5/mqtt5_options_storage.h>
#include <aws/mqtt/private/v5/mqtt5_utils.h>
#include <aws/mqtt/v5/mqtt5_types.h>

#include <aws/testing/aws_test_harness.h>

static int s_verify_user_properties_raw(
    size_t property_count,
    const struct aws_mqtt5_user_property *properties,
    size_t expected_count,
    const struct aws_mqtt5_user_property *expected_properties) {

    ASSERT_UINT_EQUALS(expected_count, property_count);

    for (size_t i = 0; i < expected_count; ++i) {
        const struct aws_mqtt5_user_property *expected_property = &expected_properties[i];
        struct aws_byte_cursor expected_name = expected_property->name;
        struct aws_byte_cursor expected_value = expected_property->value;

        bool found = false;
        for (size_t j = 0; j < property_count; ++j) {
            const struct aws_mqtt5_user_property *nv_pair = &properties[j];

            if (aws_byte_cursor_compare_lexical(&expected_name, &nv_pair->name) == 0 &&
                aws_byte_cursor_compare_lexical(&expected_value, &nv_pair->value) == 0) {
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

static int s_verify_user_properties(
    struct aws_mqtt5_user_property_set *property_set,
    size_t expected_count,
    const struct aws_mqtt5_user_property *expected_properties) {

    return s_verify_user_properties_raw(
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

static const char *PUBLISH_PAYLOAD = "hello-world";
static const char *PUBLISH_TOPIC = "greetings/friendly";

static int s_mqtt5_publish_operation_new_set_no_optional_fn(struct aws_allocator *allocator, void *ctx) {
    struct aws_mqtt5_packet_publish_view publish_options = {
        .payload = NULL,
        .qos = AWS_MQTT5_QOS_AT_LEAST_ONCE,
        .retain = true,
        .topic = aws_byte_cursor_from_c_str(PUBLISH_TOPIC),
        .payload_format = NULL,
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

    struct aws_mqtt5_operation_publish *publish_op = aws_mqtt5_operation_publish_new(allocator, &publish_options, NULL);

    ASSERT_NOT_NULL(publish_op);

    /* This test will check both the values in storage as well as the embedded view.  They should be in sync. */
    struct aws_mqtt5_packet_publish_storage *publish_storage = &publish_op->options_storage;
    struct aws_mqtt5_packet_publish_view *stored_view = &publish_storage->storage_view;

    ASSERT_NULL(publish_storage->payload);
    ASSERT_NULL(stored_view->payload);

    ASSERT_UINT_EQUALS((uint32_t)publish_options.qos, (uint32_t)publish_storage->qos);
    ASSERT_UINT_EQUALS((uint32_t)publish_options.qos, (uint32_t)stored_view->qos);

    ASSERT_TRUE(publish_storage->retain);
    ASSERT_TRUE(stored_view->retain);

    ASSERT_NOT_NULL(publish_storage->topic.ptr);
    ASSERT_BIN_ARRAYS_EQUALS(
        publish_options.topic.ptr, publish_options.topic.len, publish_storage->topic.ptr, publish_storage->topic.len);
    ASSERT_BIN_ARRAYS_EQUALS(
        publish_options.topic.ptr, publish_options.topic.len, stored_view->topic.ptr, stored_view->topic.len);
    ASSERT_TRUE(s_is_cursor_in_buffer(&publish_storage->storage, publish_storage->topic));
    ASSERT_TRUE(s_is_cursor_in_buffer(&publish_storage->storage, stored_view->topic));

    ASSERT_NULL(publish_storage->payload_format_ptr);
    ASSERT_NULL(stored_view->payload_format);

    ASSERT_NULL(publish_storage->message_expiry_interval_seconds_ptr);
    ASSERT_NULL(stored_view->message_expiry_interval_seconds);

    ASSERT_NULL(publish_storage->topic_alias_ptr);
    ASSERT_NULL(stored_view->topic_alias);

    ASSERT_NULL(publish_storage->response_topic_ptr);
    ASSERT_NULL(stored_view->response_topic);

    ASSERT_NULL(publish_storage->correlation_data_ptr);
    ASSERT_NULL(stored_view->correlation_data);

    ASSERT_NULL(publish_storage->content_type_ptr);
    ASSERT_NULL(stored_view->content_type);

    ASSERT_SUCCESS(s_verify_user_properties(&publish_storage->user_properties, 0, NULL));
    ASSERT_SUCCESS(
        s_verify_user_properties_raw(stored_view->user_property_count, stored_view->user_properties, 0, NULL));

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
    struct aws_byte_cursor response_topic = aws_byte_cursor_from_c_str(s_response_topic);
    struct aws_byte_cursor correlation_data = aws_byte_cursor_from_c_str(s_correlation_data);
    struct aws_byte_cursor content_type = aws_byte_cursor_from_c_str(s_content_type);
    enum aws_mqtt5_payload_format_indicator payload_format = AWS_MQTT5_PFI_UTF8;
    struct aws_byte_cursor payload_cursor = aws_byte_cursor_from_c_str(PUBLISH_PAYLOAD);
    struct aws_input_stream *payload_stream = aws_input_stream_new_from_cursor(allocator, &payload_cursor);

    struct aws_mqtt5_packet_publish_view publish_options = {
        .payload = payload_stream,
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

    ASSERT_PTR_EQUALS(payload_stream, publish_storage->payload);
    ASSERT_PTR_EQUALS(payload_stream, stored_view->payload);

    ASSERT_UINT_EQUALS((uint32_t)publish_options.qos, (uint32_t)publish_storage->qos);
    ASSERT_UINT_EQUALS((uint32_t)publish_options.qos, (uint32_t)stored_view->qos);

    ASSERT_FALSE(publish_storage->retain);
    ASSERT_FALSE(stored_view->retain);

    ASSERT_NOT_NULL(publish_storage->topic.ptr);
    ASSERT_BIN_ARRAYS_EQUALS(
        publish_options.topic.ptr, publish_options.topic.len, publish_storage->topic.ptr, publish_storage->topic.len);
    ASSERT_BIN_ARRAYS_EQUALS(
        publish_options.topic.ptr, publish_options.topic.len, stored_view->topic.ptr, stored_view->topic.len);
    ASSERT_TRUE(s_is_cursor_in_buffer(&publish_storage->storage, publish_storage->topic));
    ASSERT_TRUE(s_is_cursor_in_buffer(&publish_storage->storage, stored_view->topic));

    ASSERT_PTR_EQUALS(&publish_storage->payload_format, publish_storage->payload_format_ptr);
    ASSERT_UINT_EQUALS(*publish_options.payload_format, publish_storage->payload_format);
    ASSERT_PTR_EQUALS(stored_view->payload_format, publish_storage->payload_format_ptr);

    ASSERT_PTR_EQUALS(
        &publish_storage->message_expiry_interval_seconds, publish_storage->message_expiry_interval_seconds_ptr);
    ASSERT_UINT_EQUALS(
        *publish_options.message_expiry_interval_seconds, publish_storage->message_expiry_interval_seconds);
    ASSERT_PTR_EQUALS(
        stored_view->message_expiry_interval_seconds, publish_storage->message_expiry_interval_seconds_ptr);

    ASSERT_PTR_EQUALS(&publish_storage->topic_alias, publish_storage->topic_alias_ptr);
    ASSERT_UINT_EQUALS(*publish_options.topic_alias, publish_storage->topic_alias);
    ASSERT_PTR_EQUALS(stored_view->topic_alias, publish_storage->topic_alias_ptr);

    ASSERT_FALSE(publish_options.response_topic->ptr == publish_storage->response_topic.ptr);
    ASSERT_BIN_ARRAYS_EQUALS(
        publish_options.response_topic->ptr,
        publish_options.response_topic->len,
        publish_storage->response_topic.ptr,
        publish_storage->response_topic.len);
    ASSERT_PTR_EQUALS(stored_view->response_topic, &publish_storage->response_topic);
    ASSERT_BIN_ARRAYS_EQUALS(
        publish_options.response_topic->ptr,
        publish_options.response_topic->len,
        stored_view->response_topic->ptr,
        stored_view->response_topic->len);
    ASSERT_TRUE(s_is_cursor_in_buffer(&publish_storage->storage, publish_storage->response_topic));
    ASSERT_TRUE(s_is_cursor_in_buffer(&publish_storage->storage, *stored_view->response_topic));

    ASSERT_FALSE(publish_options.correlation_data->ptr == publish_storage->correlation_data.ptr);
    ASSERT_PTR_EQUALS(publish_storage->correlation_data_ptr, &publish_storage->correlation_data);
    ASSERT_BIN_ARRAYS_EQUALS(
        publish_options.correlation_data->ptr,
        publish_options.correlation_data->len,
        publish_storage->correlation_data.ptr,
        publish_storage->correlation_data.len);
    ASSERT_PTR_EQUALS(stored_view->correlation_data, publish_storage->correlation_data_ptr);
    ASSERT_BIN_ARRAYS_EQUALS(
        publish_options.correlation_data->ptr,
        publish_options.correlation_data->len,
        stored_view->correlation_data->ptr,
        stored_view->correlation_data->len);
    ASSERT_TRUE(s_is_cursor_in_buffer(&publish_storage->storage, publish_storage->correlation_data));
    ASSERT_TRUE(s_is_cursor_in_buffer(&publish_storage->storage, *stored_view->correlation_data));

    ASSERT_FALSE(publish_options.content_type->ptr == publish_storage->content_type.ptr);
    ASSERT_PTR_EQUALS(publish_storage->content_type_ptr, &publish_storage->content_type);
    ASSERT_BIN_ARRAYS_EQUALS(
        publish_options.content_type->ptr,
        publish_options.content_type->len,
        publish_storage->content_type.ptr,
        publish_storage->content_type.len);
    ASSERT_PTR_EQUALS(stored_view->content_type, publish_storage->content_type_ptr);
    ASSERT_BIN_ARRAYS_EQUALS(
        publish_options.content_type->ptr,
        publish_options.content_type->len,
        stored_view->content_type->ptr,
        stored_view->content_type->len);
    ASSERT_TRUE(s_is_cursor_in_buffer(&publish_storage->storage, publish_storage->content_type));
    ASSERT_TRUE(s_is_cursor_in_buffer(&publish_storage->storage, *stored_view->content_type));

    ASSERT_SUCCESS(s_verify_user_properties(
        &publish_storage->user_properties, AWS_ARRAY_SIZE(s_user_properties), s_user_properties));
    ASSERT_SUCCESS(s_verify_user_properties_raw(
        stored_view->user_property_count,
        stored_view->user_properties,
        AWS_ARRAY_SIZE(s_user_properties),
        s_user_properties));

    ASSERT_PTR_EQUALS(completion_options.completion_callback, publish_op->completion_options.completion_callback);
    ASSERT_PTR_EQUALS(completion_options.completion_user_data, publish_op->completion_options.completion_user_data);

    aws_mqtt5_packet_publish_view_log(stored_view, AWS_LL_DEBUG);

    aws_mqtt5_operation_release(&publish_op->base);

    aws_input_stream_destroy(payload_stream);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_publish_operation_new_set_all, s_mqtt5_publish_operation_new_set_all_fn)

static const uint16_t s_keep_alive_interval_seconds = 999;
static const uint32_t s_session_expiry_interval = 999;
static const uint16_t s_receive_maximum = 999;
static const uint32_t s_maximum_packet_size = 999;
static const uint16_t s_to_server_topic_alias_maximum = 999;
static const uint16_t s_topic_alias_maximum = 999;
static const uint16_t s_server_keep_alive = 999;
static const bool s_retain_available = false;
static const bool s_wildcard_subscriptions_available = false;
static const bool s_subscription_identifiers_available = false;
static const bool s_shared_subscriptions_available = false;

static int mqtt5_negotiated_settings_reset_default_fn(struct aws_allocator *allocator, void *ctx) {

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
    ASSERT_UINT_EQUALS(negotiated_settings.receive_maximum, 65535);
    ASSERT_UINT_EQUALS(negotiated_settings.maximum_packet_size, 0);
    ASSERT_UINT_EQUALS(negotiated_settings.to_server_topic_alias_maximum, 0);
    ASSERT_UINT_EQUALS(negotiated_settings.to_client_topic_alias_maximum, 0);
    ASSERT_UINT_EQUALS(negotiated_settings.server_keep_alive, 0);

    ASSERT_TRUE(negotiated_settings.retain_available);
    ASSERT_TRUE(negotiated_settings.wildcard_subscriptions_available);
    ASSERT_TRUE(negotiated_settings.subscription_identifiers_available);
    ASSERT_TRUE(negotiated_settings.shared_subscriptions_available);

    /* Set client modifiable CONNECT settings then apply them to negotiated_settings */

    connect_view.keep_alive_interval_seconds = s_keep_alive_interval_seconds;
    connect_view.session_expiry_interval_seconds = &s_session_expiry_interval;
    connect_view.receive_maximum = &s_receive_maximum;
    connect_view.maximum_packet_size_bytes = &s_maximum_packet_size;
    connect_view.topic_alias_maximum = &s_topic_alias_maximum;

    aws_mqtt5_negotiated_settings_reset(&negotiated_settings, &connect_view);

    /* Check that all settings are the expected values with client settings */

    ASSERT_TRUE(negotiated_settings.maximum_qos == AWS_MQTT5_QOS_AT_LEAST_ONCE);

    ASSERT_UINT_EQUALS(negotiated_settings.server_keep_alive, connect_view.keep_alive_interval_seconds);
    ASSERT_UINT_EQUALS(negotiated_settings.session_expiry_interval, *connect_view.session_expiry_interval_seconds);
    ASSERT_UINT_EQUALS(negotiated_settings.receive_maximum, *connect_view.receive_maximum);
    ASSERT_UINT_EQUALS(negotiated_settings.maximum_packet_size, *connect_view.maximum_packet_size_bytes);
    ASSERT_UINT_EQUALS(negotiated_settings.to_server_topic_alias_maximum, 0);
    ASSERT_UINT_EQUALS(negotiated_settings.to_client_topic_alias_maximum, *connect_view.topic_alias_maximum);

    ASSERT_TRUE(negotiated_settings.retain_available);
    ASSERT_TRUE(negotiated_settings.wildcard_subscriptions_available);
    ASSERT_TRUE(negotiated_settings.subscription_identifiers_available);
    ASSERT_TRUE(negotiated_settings.shared_subscriptions_available);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_negotiated_settings_reset_default, mqtt5_negotiated_settings_reset_default_fn)

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
