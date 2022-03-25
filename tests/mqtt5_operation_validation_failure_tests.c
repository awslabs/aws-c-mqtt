/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>
#include <aws/mqtt/private/v5/mqtt5_options_storage.h>
#include <aws/mqtt/private/v5/mqtt5_utils.h>
#include <aws/mqtt/v5/mqtt5_types.h>

#include <aws/testing/aws_test_harness.h>

static uint8_t s_server_reference[] = "derp.com";
static struct aws_byte_cursor s_server_reference_cursor = {
    .ptr = s_server_reference,
    .len = AWS_ARRAY_SIZE(s_server_reference),
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

#define AWS_VALIDATION_FAILURE_TEST(packet_type, failure_reason, view_name)                                            \
    static int s_mqtt5_operation_##packet_type##_validation_failure_##failure_reason##_fn(                             \
        struct aws_allocator *allocator, void *ctx) {                                                                  \
        (void)ctx;                                                                                                     \
                                                                                                                       \
        ASSERT_FAILS(aws_mqtt5_packet_##packet_type##_view_validate(&view_name));                                      \
        ASSERT_NULL(aws_mqtt5_operation_##packet_type##_new(allocator, &view_name, NULL, NULL));                       \
        return AWS_OP_SUCCESS;                                                                                         \
    }                                                                                                                  \
                                                                                                                       \
    AWS_TEST_CASE(                                                                                                     \
        mqtt5_operation_##packet_type##_validation_failure_##failure_reason,                                           \
        s_mqtt5_operation_##packet_type##_validation_failure_##failure_reason##_fn)

static struct aws_mqtt5_packet_disconnect_view s_server_reference_disconnect_view = {
    .server_reference = &s_server_reference_cursor,
};

AWS_VALIDATION_FAILURE_TEST(disconnect, server_reference, s_server_reference_disconnect_view)

static struct aws_mqtt5_packet_disconnect_view s_bad_reason_code_disconnect_view = {
    .reason_code = -1,
};

AWS_VALIDATION_FAILURE_TEST(disconnect, bad_reason_code, s_bad_reason_code_disconnect_view)

static struct aws_mqtt5_packet_disconnect_view s_reason_string_too_long_disconnect_view = {
    .reason_string = &s_too_long_for_uint16_cursor,
};

AWS_VALIDATION_FAILURE_TEST(disconnect, reason_string_too_long, s_reason_string_too_long_disconnect_view)

static struct aws_mqtt5_packet_disconnect_view s_bad_user_properties_name_disconnect_view = {
    .user_property_count = AWS_ARRAY_SIZE(s_bad_user_properties_name),
    .user_properties = s_bad_user_properties_name,
};

AWS_VALIDATION_FAILURE_TEST(disconnect, bad_user_properties_name, s_bad_user_properties_name_disconnect_view)

static struct aws_mqtt5_packet_disconnect_view s_bad_user_properties_value_disconnect_view = {
    .user_property_count = AWS_ARRAY_SIZE(s_bad_user_properties_value),
    .user_properties = s_bad_user_properties_value,
};

AWS_VALIDATION_FAILURE_TEST(disconnect, bad_user_properties_value, s_bad_user_properties_value_disconnect_view)

static struct aws_mqtt5_packet_disconnect_view s_bad_user_properties_too_many_disconnect_view = {
    .user_property_count = AWS_ARRAY_SIZE(s_bad_user_properties_too_many),
    .user_properties = s_bad_user_properties_too_many,
};

AWS_VALIDATION_FAILURE_TEST(disconnect, bad_user_properties_too_many, s_bad_user_properties_too_many_disconnect_view)
