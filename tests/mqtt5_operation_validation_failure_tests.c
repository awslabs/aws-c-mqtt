/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/io/host_resolver.h>
#include <aws/mqtt/private/v5/mqtt5_options_storage.h>
#include <aws/mqtt/private/v5/mqtt5_utils.h>
#include <aws/mqtt/v5/mqtt5_client.h>
#include <aws/mqtt/v5/mqtt5_types.h>

#include <aws/testing/aws_test_harness.h>

static uint8_t s_server_reference[] = "derp.com";
static struct aws_byte_cursor s_server_reference_cursor = {
    .ptr = s_server_reference,
    .len = AWS_ARRAY_SIZE(s_server_reference),
};

static uint8_t s_binary_data[] = "binary data";
static struct aws_byte_cursor s_binary_data_cursor = {
    .ptr = s_binary_data,
    .len = AWS_ARRAY_SIZE(s_binary_data),
};

static uint8_t s_too_long_for_uint16[UINT16_MAX + 1];
static struct aws_byte_cursor s_too_long_for_uint16_cursor = {
    .ptr = s_too_long_for_uint16,
    .len = AWS_ARRAY_SIZE(s_too_long_for_uint16),
};

static uint8_t s_user_prop_name[] = "name";
static uint8_t s_user_prop_value[] = "value";

static const struct aws_mqtt5_user_property s_bad_user_properties_name[] = {
    {
        .name =
            {
                .ptr = s_too_long_for_uint16,
                .len = AWS_ARRAY_SIZE(s_too_long_for_uint16),
            },
        .value =
            {
                .ptr = (uint8_t *)s_user_prop_value,
                .len = AWS_ARRAY_SIZE(s_user_prop_value),
            },
    },
};

static const struct aws_mqtt5_user_property s_bad_user_properties_value[] = {
    {
        .name =
            {
                .ptr = s_user_prop_name,
                .len = AWS_ARRAY_SIZE(s_user_prop_name),
            },
        .value =
            {
                .ptr = s_too_long_for_uint16,
                .len = AWS_ARRAY_SIZE(s_too_long_for_uint16),
            },
    },
};

static const struct aws_mqtt5_user_property
    s_bad_user_properties_too_many[AWS_MQTT5_CLIENT_MAXIMUM_USER_PROPERTIES + 1];

static void s_no_init(void) {}

#define AWS_VALIDATION_FAILURE_PREFIX(packet_type, failure_reason, view_name, init_function)                           \
    static int s_mqtt5_operation_##packet_type##_validation_failure_##failure_reason##_fn(                             \
        struct aws_allocator *allocator, void *ctx) {                                                                  \
        (void)ctx;                                                                                                     \
        (*init_function)();                                                                                            \
        ASSERT_FAILS(aws_mqtt5_packet_##packet_type##_view_validate(&view_name));

#define AWS_VALIDATION_FAILURE_SUFFIX(packet_type, failure_reason, view_name)                                          \
    return AWS_OP_SUCCESS;                                                                                             \
    }                                                                                                                  \
                                                                                                                       \
    AWS_TEST_CASE(                                                                                                     \
        mqtt5_operation_##packet_type##_validation_failure_##failure_reason,                                           \
        s_mqtt5_operation_##packet_type##_validation_failure_##failure_reason##_fn)

#define AWS_VALIDATION_FAILURE_TEST4(packet_type, failure_reason, view_name)                                           \
    AWS_VALIDATION_FAILURE_PREFIX(packet_type, failure_reason, view_name, &s_no_init)                                  \
    ASSERT_NULL(aws_mqtt5_operation_##packet_type##_new(allocator, &view_name, NULL, NULL));                           \
    AWS_VALIDATION_FAILURE_SUFFIX(packet_type, failure_reason, view_name)

#define AWS_VALIDATION_FAILURE_TEST3(packet_type, failure_reason, view_name)                                           \
    AWS_VALIDATION_FAILURE_PREFIX(packet_type, failure_reason, view_name, &s_no_init)                                  \
    ASSERT_NULL(aws_mqtt5_operation_##packet_type##_new(allocator, &view_name, NULL));                                 \
    AWS_VALIDATION_FAILURE_SUFFIX(packet_type, failure_reason, view_name)

#define AWS_VALIDATION_FAILURE_TEST_WITH_INIT3(packet_type, failure_reason, view_name, init_function)                  \
    AWS_VALIDATION_FAILURE_PREFIX(packet_type, failure_reason, view_name, init_function)                               \
    ASSERT_NULL(aws_mqtt5_operation_##packet_type##_new(allocator, &view_name, NULL));                                 \
    AWS_VALIDATION_FAILURE_SUFFIX(packet_type, failure_reason, view_name)

#define AWS_VALIDATION_FAILURE_TEST2(packet_type, failure_reason, view_name)                                           \
    AWS_VALIDATION_FAILURE_PREFIX(packet_type, failure_reason, view_name, &s_no_init)                                  \
    ASSERT_NULL(aws_mqtt5_operation_##packet_type##_new(allocator, &view_name));                                       \
    AWS_VALIDATION_FAILURE_SUFFIX(packet_type, failure_reason, view_name)

static struct aws_mqtt5_packet_disconnect_view s_server_reference_disconnect_view = {
    .server_reference = &s_server_reference_cursor,
};

AWS_VALIDATION_FAILURE_TEST4(disconnect, server_reference, s_server_reference_disconnect_view)

static struct aws_mqtt5_packet_disconnect_view s_bad_reason_code_disconnect_view = {
    .reason_code = -1,
};

AWS_VALIDATION_FAILURE_TEST4(disconnect, bad_reason_code, s_bad_reason_code_disconnect_view)

static struct aws_mqtt5_packet_disconnect_view s_reason_string_too_long_disconnect_view = {
    .reason_string = &s_too_long_for_uint16_cursor,
};

AWS_VALIDATION_FAILURE_TEST4(disconnect, reason_string_too_long, s_reason_string_too_long_disconnect_view)

static struct aws_mqtt5_packet_disconnect_view s_user_properties_name_too_long_disconnect_view = {
    .user_property_count = AWS_ARRAY_SIZE(s_bad_user_properties_name),
    .user_properties = s_bad_user_properties_name,
};

AWS_VALIDATION_FAILURE_TEST4(disconnect, user_properties_name_too_long, s_user_properties_name_too_long_disconnect_view)

static struct aws_mqtt5_packet_disconnect_view s_user_properties_value_too_long_disconnect_view = {
    .user_property_count = AWS_ARRAY_SIZE(s_bad_user_properties_value),
    .user_properties = s_bad_user_properties_value,
};

AWS_VALIDATION_FAILURE_TEST4(
    disconnect,
    user_properties_value_too_long,
    s_user_properties_value_too_long_disconnect_view)

static struct aws_mqtt5_packet_disconnect_view s_user_properties_too_many_disconnect_view = {
    .user_property_count = AWS_ARRAY_SIZE(s_bad_user_properties_too_many),
    .user_properties = s_bad_user_properties_too_many,
};

AWS_VALIDATION_FAILURE_TEST4(disconnect, user_properties_too_many, s_user_properties_too_many_disconnect_view)

static struct aws_mqtt5_packet_connect_view s_client_id_too_long_connect_view = {
    .client_id = {
        .ptr = s_too_long_for_uint16,
        .len = AWS_ARRAY_SIZE(s_too_long_for_uint16),
    }};

AWS_VALIDATION_FAILURE_TEST2(connect, client_id_too_long, s_client_id_too_long_connect_view)

static struct aws_mqtt5_packet_connect_view s_username_too_long_connect_view = {
    .username = &s_too_long_for_uint16_cursor,
};

AWS_VALIDATION_FAILURE_TEST2(connect, username_too_long, s_username_too_long_connect_view)

static struct aws_mqtt5_packet_connect_view s_password_too_long_connect_view = {
    .password = &s_too_long_for_uint16_cursor,
};

AWS_VALIDATION_FAILURE_TEST2(connect, password_too_long, s_password_too_long_connect_view)

static const uint16_t s_zero_receive_maximum = 0;
static struct aws_mqtt5_packet_connect_view s_receive_maximum_zero_connect_view = {
    .receive_maximum = &s_zero_receive_maximum,
};

AWS_VALIDATION_FAILURE_TEST2(connect, receive_maximum_zero, s_receive_maximum_zero_connect_view)

static const uint32_t s_maximum_packet_size_zero = 0;
static struct aws_mqtt5_packet_connect_view s_maximum_packet_size_zero_connect_view = {
    .maximum_packet_size_bytes = &s_maximum_packet_size_zero,
};

AWS_VALIDATION_FAILURE_TEST2(connect, maximum_packet_size_zero, s_maximum_packet_size_zero_connect_view)

static struct aws_mqtt5_packet_connect_view s_auth_method_unsupported_connect_view = {
    .authentication_method = &s_binary_data_cursor,
};

AWS_VALIDATION_FAILURE_TEST2(connect, auth_method_unsupported, s_auth_method_unsupported_connect_view)

static struct aws_mqtt5_packet_connect_view s_auth_data_unsupported_connect_view = {
    .authentication_data = &s_binary_data_cursor,
};

AWS_VALIDATION_FAILURE_TEST2(connect, auth_data_unsupported, s_auth_data_unsupported_connect_view)

static uint8_t s_bad_boolean = 2;
static struct aws_mqtt5_packet_connect_view s_request_problem_information_invalid_connect_view = {
    .request_problem_information = &s_bad_boolean,
};

AWS_VALIDATION_FAILURE_TEST2(
    connect,
    request_problem_information_invalid,
    s_request_problem_information_invalid_connect_view)

static struct aws_mqtt5_packet_connect_view s_request_response_information_invalid_connect_view = {
    .request_response_information = &s_bad_boolean,
};

AWS_VALIDATION_FAILURE_TEST2(
    connect,
    request_response_information_invalid,
    s_request_response_information_invalid_connect_view)

static struct aws_mqtt5_packet_connect_view s_user_properties_name_too_long_connect_view = {
    .user_property_count = AWS_ARRAY_SIZE(s_bad_user_properties_name),
    .user_properties = s_bad_user_properties_name,
};

AWS_VALIDATION_FAILURE_TEST2(connect, user_properties_name_too_long, s_user_properties_name_too_long_connect_view)

static struct aws_mqtt5_packet_connect_view s_user_properties_value_too_long_connect_view = {
    .user_property_count = AWS_ARRAY_SIZE(s_bad_user_properties_value),
    .user_properties = s_bad_user_properties_value,
};

AWS_VALIDATION_FAILURE_TEST2(connect, user_properties_value_too_long, s_user_properties_value_too_long_connect_view)

static struct aws_mqtt5_packet_connect_view s_user_properties_too_many_connect_view = {
    .user_property_count = AWS_ARRAY_SIZE(s_bad_user_properties_too_many),
    .user_properties = s_bad_user_properties_too_many,
};

AWS_VALIDATION_FAILURE_TEST2(connect, user_properties_too_many, s_user_properties_too_many_connect_view)

/* no-topic and no-topic-alias is invalid */
static struct aws_mqtt5_packet_publish_view s_will_invalid_publish_view = {};

static struct aws_mqtt5_packet_connect_view s_will_invalid_connect_view = {
    .will = &s_will_invalid_publish_view,
};

AWS_VALIDATION_FAILURE_TEST2(connect, will_invalid, s_will_invalid_connect_view)

static struct aws_mqtt5_packet_publish_view s_will_payload_too_long_publish_view = {
    .topic =
        {
            .ptr = s_user_prop_name,
            .len = AWS_ARRAY_SIZE(s_user_prop_name),
        },
    .payload = {
        .ptr = s_too_long_for_uint16,
        .len = AWS_ARRAY_SIZE(s_too_long_for_uint16),
    }};

static struct aws_mqtt5_packet_connect_view s_will_payload_too_long_connect_view = {
    .will = &s_will_payload_too_long_publish_view,
};

AWS_VALIDATION_FAILURE_TEST2(connect, will_payload_too_long, s_will_payload_too_long_connect_view)

static struct aws_mqtt5_packet_subscribe_view s_no_subscriptions_subscribe_view = {};

AWS_VALIDATION_FAILURE_TEST3(subscribe, no_subscriptions, s_no_subscriptions_subscribe_view)

static struct aws_mqtt5_subscription_view
    s_too_many_subscriptions[AWS_MQTT5_CLIENT_MAXIMUM_SUBSCRIPTIONS_PER_SUBSCRIBE + 1] = {};

static void s_init_too_many_subscriptions(void) {
    for (size_t i = 0; i < AWS_ARRAY_SIZE(s_too_many_subscriptions); ++i) {
        struct aws_mqtt5_subscription_view *view = &s_too_many_subscriptions[i];

        view->topic_filter = aws_byte_cursor_from_c_str("a/topic");
        view->qos = AWS_MQTT5_QOS_AT_MOST_ONCE;
        view->no_local = false;
        view->retain_handling_type = AWS_MQTT5_RHT_SEND_ON_SUBSCRIBE;
        view->retain_as_published = false;
    }
}

static struct aws_mqtt5_packet_subscribe_view s_too_many_subscriptions_subscribe_view = {
    .subscriptions = s_too_many_subscriptions,
    .subscription_count = AWS_ARRAY_SIZE(s_too_many_subscriptions),
};

AWS_VALIDATION_FAILURE_TEST_WITH_INIT3(
    subscribe,
    too_many_subscriptions,
    s_too_many_subscriptions_subscribe_view,
    s_init_too_many_subscriptions)

static uint8_t s_good_topic[] = "hello/world";

static struct aws_mqtt5_subscription_view s_good_subscription[] = {
    {
        .topic_filter =
            {
                .ptr = s_good_topic,
                .len = AWS_ARRAY_SIZE(s_good_topic),
            },
        .qos = AWS_MQTT5_QOS_AT_MOST_ONCE,
        .no_local = false,
        .retain_handling_type = AWS_MQTT5_RHT_SEND_ON_SUBSCRIBE,
        .retain_as_published = false,
    },
};

static const uint32_t s_invalid_subscription_identifier = 0;

static struct aws_mqtt5_packet_subscribe_view s_invalid_subscription_identifier_subscribe_view = {
    .subscriptions = s_good_subscription,
    .subscription_count = AWS_ARRAY_SIZE(s_good_subscription),
    .subscription_identifier = &s_invalid_subscription_identifier,
};

AWS_VALIDATION_FAILURE_TEST3(
    subscribe,
    invalid_subscription_identifier,
    s_invalid_subscription_identifier_subscribe_view)

static uint8_t s_bad_topic_filter[] = "hello/#/world";

static struct aws_mqtt5_subscription_view s_invalid_topic_filter_subscription[] = {
    {
        .topic_filter =
            {
                .ptr = s_bad_topic_filter,
                .len = AWS_ARRAY_SIZE(s_bad_topic_filter),
            },
        .qos = AWS_MQTT5_QOS_AT_MOST_ONCE,
        .no_local = false,
        .retain_handling_type = AWS_MQTT5_RHT_SEND_ON_SUBSCRIBE,
        .retain_as_published = false,
    },
};

static struct aws_mqtt5_packet_subscribe_view s_invalid_topic_filter_subscribe_view = {
    .subscriptions = s_invalid_topic_filter_subscription,
    .subscription_count = AWS_ARRAY_SIZE(s_invalid_topic_filter_subscription),
};

AWS_VALIDATION_FAILURE_TEST3(subscribe, invalid_topic_filter, s_invalid_topic_filter_subscribe_view)

static struct aws_mqtt5_subscription_view s_invalid_qos_subscription[] = {
    {
        .topic_filter =
            {
                .ptr = s_good_topic,
                .len = AWS_ARRAY_SIZE(s_good_topic),
            },
        .qos = 3,
        .no_local = false,
        .retain_handling_type = AWS_MQTT5_RHT_SEND_ON_SUBSCRIBE,
        .retain_as_published = false,
    },
};

static struct aws_mqtt5_packet_subscribe_view s_invalid_qos_subscribe_view = {
    .subscriptions = s_invalid_qos_subscription,
    .subscription_count = AWS_ARRAY_SIZE(s_invalid_qos_subscription),
};

AWS_VALIDATION_FAILURE_TEST3(subscribe, invalid_qos, s_invalid_qos_subscribe_view)

static struct aws_mqtt5_subscription_view s_invalid_retain_type_subscription[] = {
    {
        .topic_filter =
            {
                .ptr = s_good_topic,
                .len = AWS_ARRAY_SIZE(s_good_topic),
            },
        .qos = AWS_MQTT5_QOS_AT_MOST_ONCE,
        .no_local = false,
        .retain_handling_type = 5,
        .retain_as_published = false,
    },
};

static struct aws_mqtt5_packet_subscribe_view s_invalid_retain_type_subscribe_view = {
    .subscriptions = s_invalid_retain_type_subscription,
    .subscription_count = AWS_ARRAY_SIZE(s_invalid_retain_type_subscription),
};

AWS_VALIDATION_FAILURE_TEST3(subscribe, invalid_retain_type, s_invalid_retain_type_subscribe_view)

static struct aws_mqtt5_packet_subscribe_view s_user_properties_name_too_long_subscribe_view = {
    .subscriptions = s_good_subscription,
    .subscription_count = AWS_ARRAY_SIZE(s_good_subscription),
    .user_property_count = AWS_ARRAY_SIZE(s_bad_user_properties_name),
    .user_properties = s_bad_user_properties_name,
};

AWS_VALIDATION_FAILURE_TEST3(subscribe, user_properties_name_too_long, s_user_properties_name_too_long_subscribe_view)

static struct aws_mqtt5_packet_subscribe_view s_user_properties_value_too_long_subscribe_view = {
    .subscriptions = s_good_subscription,
    .subscription_count = AWS_ARRAY_SIZE(s_good_subscription),
    .user_property_count = AWS_ARRAY_SIZE(s_bad_user_properties_value),
    .user_properties = s_bad_user_properties_value,
};

AWS_VALIDATION_FAILURE_TEST3(subscribe, user_properties_value_too_long, s_user_properties_value_too_long_subscribe_view)

static struct aws_mqtt5_packet_subscribe_view s_user_properties_too_many_subscribe_view = {
    .subscriptions = s_good_subscription,
    .subscription_count = AWS_ARRAY_SIZE(s_good_subscription),
    .user_property_count = AWS_ARRAY_SIZE(s_bad_user_properties_too_many),
    .user_properties = s_bad_user_properties_too_many,
};

AWS_VALIDATION_FAILURE_TEST3(subscribe, user_properties_too_many, s_user_properties_too_many_subscribe_view)

static struct aws_mqtt5_packet_unsubscribe_view s_no_topic_filters_unsubscribe_view = {};

AWS_VALIDATION_FAILURE_TEST3(unsubscribe, no_topic_filters, s_no_topic_filters_unsubscribe_view)

static struct aws_byte_cursor s_too_many_topic_filters[AWS_MQTT5_CLIENT_MAXIMUM_TOPIC_FILTERS_PER_UNSUBSCRIBE + 1] = {};

static void s_init_too_many_topic_filters(void) {
    for (size_t i = 0; i < AWS_ARRAY_SIZE(s_too_many_topic_filters); ++i) {
        struct aws_byte_cursor *cursor = &s_too_many_topic_filters[i];

        cursor->ptr = s_good_topic;
        cursor->len = AWS_ARRAY_SIZE(s_good_topic);
    }
}

static struct aws_mqtt5_packet_unsubscribe_view s_too_many_topic_filters_unsubscribe_view = {
    .topic_filters = s_too_many_topic_filters,
    .topic_filter_count = AWS_ARRAY_SIZE(s_too_many_topic_filters),
};

AWS_VALIDATION_FAILURE_TEST_WITH_INIT3(
    unsubscribe,
    too_many_topic_filters,
    s_too_many_topic_filters_unsubscribe_view,
    s_init_too_many_topic_filters)

static struct aws_byte_cursor s_invalid_topic_filter[] = {
    {
        .ptr = s_bad_topic_filter,
        .len = AWS_ARRAY_SIZE(s_bad_topic_filter),
    },
};

static struct aws_mqtt5_packet_unsubscribe_view s_invalid_topic_filter_unsubscribe_view = {
    .topic_filters = s_invalid_topic_filter,
    .topic_filter_count = AWS_ARRAY_SIZE(s_invalid_topic_filter),
};

AWS_VALIDATION_FAILURE_TEST3(unsubscribe, invalid_topic_filter, s_invalid_topic_filter_unsubscribe_view)

static struct aws_byte_cursor s_valid_topic_filter[] = {
    {
        .ptr = s_good_topic,
        .len = AWS_ARRAY_SIZE(s_good_topic),
    },
};

static struct aws_mqtt5_packet_unsubscribe_view s_user_properties_name_too_long_unsubscribe_view = {
    .topic_filters = s_valid_topic_filter,
    .topic_filter_count = AWS_ARRAY_SIZE(s_valid_topic_filter),
    .user_property_count = AWS_ARRAY_SIZE(s_bad_user_properties_name),
    .user_properties = s_bad_user_properties_name,
};

AWS_VALIDATION_FAILURE_TEST3(
    unsubscribe,
    user_properties_name_too_long,
    s_user_properties_name_too_long_unsubscribe_view)

static struct aws_mqtt5_packet_unsubscribe_view s_user_properties_value_too_long_unsubscribe_view = {
    .topic_filters = s_valid_topic_filter,
    .topic_filter_count = AWS_ARRAY_SIZE(s_valid_topic_filter),
    .user_property_count = AWS_ARRAY_SIZE(s_bad_user_properties_value),
    .user_properties = s_bad_user_properties_value,
};

AWS_VALIDATION_FAILURE_TEST3(
    unsubscribe,
    user_properties_value_too_long,
    s_user_properties_value_too_long_unsubscribe_view)

static struct aws_mqtt5_packet_unsubscribe_view s_user_properties_too_many_unsubscribe_view = {
    .topic_filters = s_valid_topic_filter,
    .topic_filter_count = AWS_ARRAY_SIZE(s_valid_topic_filter),
    .user_property_count = AWS_ARRAY_SIZE(s_bad_user_properties_too_many),
    .user_properties = s_bad_user_properties_too_many,
};

AWS_VALIDATION_FAILURE_TEST3(unsubscribe, user_properties_too_many, s_user_properties_too_many_unsubscribe_view)

static uint8_t s_invalid_topic[] = "hello/#";
static struct aws_mqtt5_packet_publish_view s_invalid_topic_publish_view = {.topic = {
                                                                                .ptr = s_invalid_topic,
                                                                                .len = AWS_ARRAY_SIZE(s_invalid_topic),
                                                                            }};

AWS_VALIDATION_FAILURE_TEST3(publish, invalid_topic, s_invalid_topic_publish_view)

static struct aws_mqtt5_packet_publish_view s_no_topic_publish_view = {};

AWS_VALIDATION_FAILURE_TEST3(publish, no_topic, s_no_topic_publish_view)

static enum aws_mqtt5_payload_format_indicator s_invalid_payload_format = 3;

static struct aws_mqtt5_packet_publish_view s_invalid_payload_format_publish_view = {
    .topic =
        {
            .ptr = s_good_topic,
            .len = AWS_ARRAY_SIZE(s_good_topic),
        },
    .payload_format = &s_invalid_payload_format,
};

AWS_VALIDATION_FAILURE_TEST3(publish, invalid_payload_format, s_invalid_payload_format_publish_view)

static struct aws_mqtt5_packet_publish_view s_response_topic_too_long_publish_view = {
    .topic =
        {
            .ptr = s_good_topic,
            .len = AWS_ARRAY_SIZE(s_good_topic),
        },
    .response_topic = &s_too_long_for_uint16_cursor,
};

AWS_VALIDATION_FAILURE_TEST3(publish, response_topic_too_long, s_response_topic_too_long_publish_view)

static struct aws_mqtt5_packet_publish_view s_correlation_data_too_long_publish_view = {
    .topic =
        {
            .ptr = s_good_topic,
            .len = AWS_ARRAY_SIZE(s_good_topic),
        },
    .correlation_data = &s_too_long_for_uint16_cursor,
};

AWS_VALIDATION_FAILURE_TEST3(publish, correlation_data_too_long, s_correlation_data_too_long_publish_view)

static const uint32_t s_subscription_identifiers[] = {1, 2};

static struct aws_mqtt5_packet_publish_view s_subscription_identifier_exists_publish_view = {
    .topic =
        {
            .ptr = s_good_topic,
            .len = AWS_ARRAY_SIZE(s_good_topic),
        },
    .subscription_identifiers = s_subscription_identifiers,
    .subscription_identifier_count = AWS_ARRAY_SIZE(s_subscription_identifiers),
};

AWS_VALIDATION_FAILURE_TEST3(publish, subscription_identifier_exists, s_subscription_identifier_exists_publish_view)

static struct aws_mqtt5_packet_publish_view s_content_type_too_long_publish_view = {
    .topic =
        {
            .ptr = s_good_topic,
            .len = AWS_ARRAY_SIZE(s_good_topic),
        },
    .content_type = &s_too_long_for_uint16_cursor,
};

AWS_VALIDATION_FAILURE_TEST3(publish, content_type_too_long, s_content_type_too_long_publish_view)

static const uint16_t s_topic_alias_zero = 0;
static struct aws_mqtt5_packet_publish_view s_topic_alias_zero_publish_view = {
    .topic =
        {
            .ptr = s_good_topic,
            .len = AWS_ARRAY_SIZE(s_good_topic),
        },
    .topic_alias = &s_topic_alias_zero,
};

AWS_VALIDATION_FAILURE_TEST3(publish, topic_alias_zero, s_topic_alias_zero_publish_view)

static struct aws_mqtt5_packet_publish_view s_user_properties_name_too_long_publish_view = {
    .topic =
        {
            .ptr = s_good_topic,
            .len = AWS_ARRAY_SIZE(s_good_topic),
        },
    .user_property_count = AWS_ARRAY_SIZE(s_bad_user_properties_name),
    .user_properties = s_bad_user_properties_name,
};

AWS_VALIDATION_FAILURE_TEST3(publish, user_properties_name_too_long, s_user_properties_name_too_long_publish_view)

static struct aws_mqtt5_packet_publish_view s_user_properties_value_too_long_publish_view = {
    .topic =
        {
            .ptr = s_good_topic,
            .len = AWS_ARRAY_SIZE(s_good_topic),
        },
    .user_property_count = AWS_ARRAY_SIZE(s_bad_user_properties_value),
    .user_properties = s_bad_user_properties_value,
};

AWS_VALIDATION_FAILURE_TEST3(publish, user_properties_value_too_long, s_user_properties_value_too_long_publish_view)

static struct aws_mqtt5_packet_publish_view s_user_properties_too_many_publish_view = {
    .topic =
        {
            .ptr = s_good_topic,
            .len = AWS_ARRAY_SIZE(s_good_topic),
        },
    .user_property_count = AWS_ARRAY_SIZE(s_bad_user_properties_too_many),
    .user_properties = s_bad_user_properties_too_many,
};

AWS_VALIDATION_FAILURE_TEST3(publish, user_properties_too_many, s_user_properties_too_many_publish_view)

#define AWS_CLIENT_CREATION_VALIDATION_FAILURE(failure_reason, options_name, set_bootstrap)                            \
    static int s_mqtt5_client_options_validation_failure_##failure_reason##_fn(                                        \
        struct aws_allocator *allocator, void *ctx) {                                                                  \
        (void)ctx;                                                                                                     \
        aws_mqtt_library_init(allocator);                                                                              \
        struct aws_event_loop_group *elg = NULL;                                                                       \
        struct aws_host_resolver *hr = NULL;                                                                           \
        struct aws_client_bootstrap *bootstrap = NULL;                                                                 \
        if (set_bootstrap) {                                                                                           \
            elg = aws_event_loop_group_new_default(allocator, 1, NULL);                                                \
                                                                                                                       \
            struct aws_host_resolver_default_options hr_options = {                                                    \
                .el_group = elg,                                                                                       \
                .max_entries = 1,                                                                                      \
            };                                                                                                         \
            hr = aws_host_resolver_new_default(allocator, &hr_options);                                                \
                                                                                                                       \
            struct aws_client_bootstrap_options bootstrap_options = {                                                  \
                .event_loop_group = elg,                                                                               \
                .host_resolver = hr,                                                                                   \
            };                                                                                                         \
            bootstrap = aws_client_bootstrap_new(allocator, &bootstrap_options);                                       \
        }                                                                                                              \
        options_name.bootstrap = bootstrap;                                                                            \
                                                                                                                       \
        ASSERT_FAILS(aws_mqtt5_client_options_validate(&options_name));                                                \
        ASSERT_NULL(aws_mqtt5_client_new(allocator, &options_name));                                                   \
        aws_client_bootstrap_release(bootstrap);                                                                       \
        aws_host_resolver_release(hr);                                                                                 \
        aws_event_loop_group_release(elg);                                                                             \
                                                                                                                       \
        aws_mqtt_library_clean_up();                                                                                   \
        return AWS_OP_SUCCESS;                                                                                         \
    }                                                                                                                  \
                                                                                                                       \
    AWS_TEST_CASE(                                                                                                     \
        mqtt5_client_options_validation_failure_##failure_reason,                                                      \
        s_mqtt5_client_options_validation_failure_##failure_reason##_fn)

#ifdef NEVER

struct aws_byte_cursor host_name;
struct aws_client_bootstrap *bootstrap;
const struct aws_socket_options *socket_options;
const struct aws_mqtt5_packet_connect_view *connect_options;
uint32_t ping_timeout_ms;
aws_mqtt5_client_connection_event_callback_fn *lifecycle_event_handler;

#endif

static struct aws_socket_options s_good_socket_options = {
    .type = AWS_SOCKET_STREAM,
    .domain = AWS_SOCKET_IPV4,
};

static struct aws_mqtt5_packet_connect_view s_good_connect = {};

void s_lifecycle_event_handler(const struct aws_mqtt5_client_lifecycle_event *event) {
    (void)event;
}

static struct aws_mqtt5_client_options s_no_host_client_options = {
    .socket_options = &s_good_socket_options,
    .connect_options = &s_good_connect,
    .ping_timeout_ms = 30000,
    .lifecycle_event_handler = &s_lifecycle_event_handler,
};

AWS_CLIENT_CREATION_VALIDATION_FAILURE(no_host, s_no_host_client_options, true)

static struct aws_mqtt5_client_options s_no_bootstrap_client_options = {
    .host_name =
        {
            .ptr = s_server_reference,
            .len = AWS_ARRAY_SIZE(s_server_reference),
        },
    .socket_options = &s_good_socket_options,
    .connect_options = &s_good_connect,
    .ping_timeout_ms = 30000,
    .lifecycle_event_handler = &s_lifecycle_event_handler,
};

AWS_CLIENT_CREATION_VALIDATION_FAILURE(no_bootstrap, s_no_bootstrap_client_options, false)

static struct aws_mqtt5_client_options s_invalid_socket_options_client_options = {
    .host_name =
        {
            .ptr = s_server_reference,
            .len = AWS_ARRAY_SIZE(s_server_reference),
        },
    .connect_options = &s_good_connect,
    .ping_timeout_ms = 30000,
    .lifecycle_event_handler = &s_lifecycle_event_handler,
};

AWS_CLIENT_CREATION_VALIDATION_FAILURE(invalid_socket_options, s_invalid_socket_options_client_options, true)

static struct aws_mqtt5_client_options s_invalid_connect_client_options = {
    .host_name =
        {
            .ptr = s_server_reference,
            .len = AWS_ARRAY_SIZE(s_server_reference),
        },
    .socket_options = &s_good_socket_options,
    .connect_options = &s_client_id_too_long_connect_view,
    .ping_timeout_ms = 30000,
    .lifecycle_event_handler = &s_lifecycle_event_handler,
};

AWS_CLIENT_CREATION_VALIDATION_FAILURE(invalid_connect, s_invalid_connect_client_options, true)

static struct aws_mqtt5_packet_connect_view s_short_keep_alive_connect_view = {
    .keep_alive_interval_seconds = 20,
};

static struct aws_mqtt5_client_options s_invalid_keep_alive_client_options = {
    .host_name =
        {
            .ptr = s_server_reference,
            .len = AWS_ARRAY_SIZE(s_server_reference),
        },
    .socket_options = &s_good_socket_options,
    .connect_options = &s_short_keep_alive_connect_view,
    .ping_timeout_ms = 30000,
    .lifecycle_event_handler = &s_lifecycle_event_handler,
};

AWS_CLIENT_CREATION_VALIDATION_FAILURE(invalid_keep_alive, s_invalid_keep_alive_client_options, true)
