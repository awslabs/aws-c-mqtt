/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/byte_buf.h>
#include <aws/common/string.h>
#include <aws/mqtt/mqtt.h>
#include <aws/mqtt/private/client_impl_shared.h>
#include <aws/testing/aws_test_harness.h>

static const struct aws_byte_cursor QUESTION_MARK = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("?");
static const struct aws_byte_cursor PLATFORM_ATT_STR = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("&Platform=");

static void s_get_expected_string(
    struct aws_allocator *allocator,
    struct aws_byte_cursor *original_username,
    struct aws_byte_cursor sdk,
    struct aws_byte_cursor *platform,
    struct aws_byte_buf *expected_buf) {
    struct aws_byte_cursor platform_to_use = platform ? *platform : aws_get_platform_build_os_string();
    if (original_username) {
        aws_byte_buf_init_copy_from_cursor(expected_buf, allocator, *original_username);
    } else {
        aws_byte_buf_init(expected_buf, allocator, 0);
    }
    aws_byte_buf_append_dynamic(expected_buf, &QUESTION_MARK);
    aws_byte_buf_append_dynamic(expected_buf, &sdk);
    aws_byte_buf_append_dynamic(expected_buf, &PLATFORM_ATT_STR);
    aws_byte_buf_append_dynamic(expected_buf, &platform_to_use);
}

static int s_test_mqtt_append_sdk_metrics_empty(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_mqtt_iot_sdk_metrics metrics = {0};

    struct aws_byte_buf output_username;
    AWS_ZERO_STRUCT(output_username);

    struct aws_byte_cursor original_username = aws_byte_cursor_from_c_str("");

    ASSERT_SUCCESS(aws_mqtt_append_sdk_metrics_to_username(allocator, &original_username, metrics, &output_username));

    struct aws_byte_cursor output_cursor = aws_byte_cursor_from_buf(&output_username);

    /* Expected string: ?SDK=IoTDeviceSDK/C&Platform=<OS> */
    struct aws_byte_buf expected_buf;
    AWS_ZERO_STRUCT(expected_buf);
    s_get_expected_string(allocator, NULL, aws_byte_cursor_from_c_str("SDK=IoTDeviceSDK/C"), NULL, &expected_buf);

    ASSERT_TRUE(aws_byte_cursor_eq_byte_buf(&output_cursor, &expected_buf));

    aws_byte_buf_clean_up(&output_username);
    aws_byte_buf_clean_up(&expected_buf);

    return AWS_OP_SUCCESS;
}

static int s_test_mqtt_append_sdk_metrics_basic(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_mqtt_iot_sdk_metrics metrics = {
        .library_name = aws_byte_cursor_from_c_str("TEST_SDK_STRING"),
        .metadata_entries = NULL,
        .metadata_count = 0,
    };

    struct aws_byte_buf output_username;
    AWS_ZERO_STRUCT(output_username);

    struct aws_byte_cursor original_username = aws_byte_cursor_from_c_str("TEST_USERNAME");

    ASSERT_SUCCESS(aws_mqtt_append_sdk_metrics_to_username(allocator, &original_username, metrics, &output_username));

    struct aws_byte_cursor output_cursor = aws_byte_cursor_from_buf(&output_username);

    /* Expected string: TEST_USERNAME?SDK=TEST_SDK_STRING&Platform=<OS> */
    struct aws_byte_buf expected_buf;
    AWS_ZERO_STRUCT(expected_buf);
    s_get_expected_string(
        allocator, &original_username, aws_byte_cursor_from_c_str("SDK=TEST_SDK_STRING"), NULL, &expected_buf);

    ASSERT_TRUE(aws_byte_cursor_eq_byte_buf(&output_cursor, &expected_buf));

    aws_byte_buf_clean_up(&output_username);
    aws_byte_buf_clean_up(&expected_buf);

    return AWS_OP_SUCCESS;
}

static int s_test_mqtt_append_sdk_metrics_existing_attributes(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_mqtt_iot_sdk_metrics metrics = {
        .library_name = aws_byte_cursor_from_c_str("NewSDK"),
        .metadata_entries = NULL,
        .metadata_count = 0,
    };

    struct aws_byte_buf output_username;
    AWS_ZERO_STRUCT(output_username);

    struct aws_byte_cursor original_username =
        aws_byte_cursor_from_c_str("user?SDK=ExistingSDK&Platform=ExistingPlatform");

    ASSERT_SUCCESS(aws_mqtt_append_sdk_metrics_to_username(allocator, &original_username, metrics, &output_username));

    struct aws_byte_cursor output_cursor = aws_byte_cursor_from_buf(&output_username);

    ASSERT_TRUE(aws_byte_cursor_eq(&output_cursor, &original_username));

    aws_byte_buf_clean_up(&output_username);

    return AWS_OP_SUCCESS;
}

static int s_test_mqtt_append_sdk_metrics_special_chars(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_mqtt_iot_sdk_metrics metrics = {
        .library_name = aws_byte_cursor_from_c_str("SDK/Test-1.0"),
        .metadata_entries = NULL,
        .metadata_count = 0,
    };

    struct aws_byte_buf output_username;
    AWS_ZERO_STRUCT(output_username);

    struct aws_byte_cursor original_username = aws_byte_cursor_from_c_str("user@domain.com");

    ASSERT_SUCCESS(aws_mqtt_append_sdk_metrics_to_username(allocator, &original_username, metrics, &output_username));

    struct aws_byte_cursor output_cursor = aws_byte_cursor_from_buf(&output_username);

    /* Expected string: user@domain.com?SDK=SDK/Test-1.0&Platform=<OS> */
    struct aws_byte_buf expected_buf;
    AWS_ZERO_STRUCT(expected_buf);
    s_get_expected_string(
        allocator, &original_username, aws_byte_cursor_from_c_str("SDK=SDK/Test-1.0"), NULL, &expected_buf);

    ASSERT_TRUE(aws_byte_cursor_eq_byte_buf(&output_cursor, &expected_buf));

    aws_byte_buf_clean_up(&output_username);
    aws_byte_buf_clean_up(&expected_buf);

    return AWS_OP_SUCCESS;
}

static int s_test_mqtt_append_sdk_metrics_long_strings(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    char long_username[1024];
    char long_sdk_name[512];

    memset(long_username, 'A', sizeof(long_username) - 1);
    long_username[sizeof(long_username) - 1] = '\0';

    memset(long_sdk_name, 'B', sizeof(long_sdk_name) - 1);
    long_sdk_name[sizeof(long_sdk_name) - 1] = '\0';

    struct aws_mqtt_iot_sdk_metrics metrics = {
        .library_name = aws_byte_cursor_from_c_str(long_sdk_name),
        .metadata_entries = NULL,
        .metadata_count = 0,
    };

    struct aws_mqtt_iot_sdk_metrics short_metrics = {
        .library_name = aws_byte_cursor_from_c_str("SHORT_SDK_NAME"),
        .metadata_entries = NULL,
        .metadata_count = 0,
    };

    struct aws_byte_buf output_username;
    AWS_ZERO_STRUCT(output_username);

    struct aws_byte_cursor original_username = aws_byte_cursor_from_c_str(long_username);

    // aws_mqtt_append_sdk_metrics_to_username does not valid original username length
    ASSERT_SUCCESS(
        aws_mqtt_append_sdk_metrics_to_username(allocator, &original_username, short_metrics, &output_username));
    // aws_mqtt_append_sdk_metrics_to_username fails when the extra metrics string exceeds buffer limit
    ASSERT_FAILS(aws_mqtt_append_sdk_metrics_to_username(allocator, &original_username, metrics, &output_username));
    ASSERT_INT_EQUALS(aws_last_error(), AWS_ERROR_DEST_COPY_TOO_SMALL);

    aws_byte_buf_clean_up(&output_username);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt_append_sdk_metrics_empty, s_test_mqtt_append_sdk_metrics_empty)
AWS_TEST_CASE(mqtt_append_sdk_metrics_basic, s_test_mqtt_append_sdk_metrics_basic)
AWS_TEST_CASE(mqtt_append_sdk_metrics_existing_attributes, s_test_mqtt_append_sdk_metrics_existing_attributes)
AWS_TEST_CASE(mqtt_append_sdk_metrics_special_chars, s_test_mqtt_append_sdk_metrics_special_chars)
AWS_TEST_CASE(mqtt_append_sdk_metrics_long_strings, s_test_mqtt_append_sdk_metrics_long_strings)