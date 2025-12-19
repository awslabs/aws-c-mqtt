/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/encoding.h>
#include <aws/mqtt/mqtt.h>
#include <aws/mqtt/private/mqtt_iot_sdk_metrics.h>
#include <v3/mqtt311_testing_utils.h>

#include <aws/testing/aws_test_harness.h>

struct utf8_example {
    const char *name;
    struct aws_byte_cursor text;
};

static struct utf8_example s_valid_mqtt_utf8_examples[] = {
    {
        .name = "1 letter",
        .text = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("a"),
    },
    {
        .name = "Several ascii letters",
        .text = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ascii word"),
    },
    {
        .name = "empty string",
        .text = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(""),
    },
    {
        .name = "2 byte codepoint",
        .text = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("\xC2\xA3"),
    },
    {
        .name = "3 byte codepoint",
        .text = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("\xE2\x82\xAC"),
    },
    {
        .name = "4 byte codepoint",
        .text = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("\xF0\x90\x8D\x88"),
    },
    {
        .name = "A variety of different length codepoints",
        .text = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(
            "\xF0\x90\x8D\x88\xE2\x82\xAC\xC2\xA3\x24\xC2\xA3\xE2\x82\xAC\xF0\x90\x8D\x88"),
    },
    {
        .name = "UTF8 BOM",
        .text = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("\xEF\xBB\xBF"),
    },
    {
        .name = "UTF8 BOM plus extra",
        .text = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("\xEF\xBB\xBF\x24\xC2\xA3"),
    },
    {
        .name = "First possible 3 byte codepoint",
        .text = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("\xE0\xA0\x80"),
    },
    {
        .name = "First possible 4 byte codepoint",
        .text = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("\xF0\x90\x80\x80"),
    },
    {
        .name = "Last possible 2 byte codepoint",
        .text = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("\xDF\xBF"),
    },
    {
        .name = "Last valid codepoint before prohibited range U+D800 - U+DFFF",
        .text = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("\xED\x9F\xBF"),
    },
    {
        .name = "Next valid codepoint after prohibited range U+D800 - U+DFFF",
        .text = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("\xEE\x80\x80"),
    },
    {
        .name = "Boundary condition",
        .text = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("\xEF\xBF\xBD"),
    },
    {
        .name = "Boundary condition",
        .text = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("\xF4\x90\x80\x80"),
    },
};

static struct utf8_example s_illegal_mqtt_utf8_examples[] = {
    {
        .name = "non character U+0000",
        .text = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("\x00"),
    },
    {
        .name = "Codepoint in prohibited range U+0001 - U+001F (in the middle)",
        .text = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("\x04"),
    },
    {
        .name = "Codepoint in prohibited range U+0001 - U+001F (boundary)",
        .text = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("\x1F"),
    },
    {
        .name = "Codepoint in prohibited range U+007F - U+009F (min: U+7F)",
        .text = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("\x7F"),
    },
    {
        .name = "Codepoint in prohibited range U+007F - U+009F (in the middle u+8F)",
        .text = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("\xC2\x8F"),
    },
    {
        .name = "Codepoint in prohibited range U+007F - U+009F (boundary U+9F)",
        .text = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("\xC2\x9F"),
    },
    {
        .name = "non character end with U+FFFF",
        .text = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("\xEF\xBF\xBF"),
    },
    {
        .name = "non character end with U+FFFE",
        .text = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("\xF7\xBF\xBF\xBE"),
    },
    {
        .name = "non character in  U+FDD0 - U+FDEF (lower bound)",
        .text = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("\xEF\xB7\x90"),
    },
    {
        .name = "non character in  U+FDD0 - U+FDEF (in middle)",
        .text = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("\xEF\xB7\xA1"),
    },
    {
        .name = "non character in  U+FDD0 - U+FDEF (upper bound)",
        .text = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("\xEF\xB7\xAF"),
    }};

static int s_mqtt_utf8_encoded_string_test(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    /* Check the valid test cases */
    for (size_t i = 0; i < AWS_ARRAY_SIZE(s_valid_mqtt_utf8_examples); ++i) {
        struct utf8_example example = s_valid_mqtt_utf8_examples[i];
        printf("valid example [%zu]: %s\n", i, example.name);
        ASSERT_SUCCESS(aws_mqtt_validate_utf8_text(example.text));
    }

    /* Glue all the valid test cases together, they ought to pass */
    struct aws_byte_buf all_good_text;
    aws_byte_buf_init(&all_good_text, allocator, 1024);
    for (size_t i = 0; i < AWS_ARRAY_SIZE(s_valid_mqtt_utf8_examples); ++i) {
        aws_byte_buf_append_dynamic(&all_good_text, &s_valid_mqtt_utf8_examples[i].text);
    }
    ASSERT_SUCCESS(aws_mqtt_validate_utf8_text(aws_byte_cursor_from_buf(&all_good_text)));
    aws_byte_buf_clean_up(&all_good_text);

    /* Check the illegal test cases */
    for (size_t i = 0; i < AWS_ARRAY_SIZE(s_illegal_mqtt_utf8_examples); ++i) {
        struct utf8_example example = s_illegal_mqtt_utf8_examples[i];
        printf("illegal example [%zu]: %s\n", i, example.name);
        ASSERT_FAILS(aws_mqtt_validate_utf8_text(example.text));
    }

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt_utf8_encoded_string_test, s_mqtt_utf8_encoded_string_test)

static int s_test_mqtt_append_sdk_metrics_empty(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_mqtt_iot_sdk_metrics metrics = {{0}};

    struct aws_byte_buf output_username;
    AWS_ZERO_STRUCT(output_username);

    struct aws_byte_cursor original_username = aws_byte_cursor_from_c_str("");

    ASSERT_SUCCESS(
        aws_mqtt_append_sdk_metrics_to_username(allocator, &original_username, &metrics, &output_username, NULL));

    struct aws_byte_cursor output_cursor = aws_byte_cursor_from_buf(&output_username);

    /* Expected string: ?SDK=IoTDeviceSDK/C&Platform=<OS> */
    struct aws_byte_buf expected_buf;
    AWS_ZERO_STRUCT(expected_buf);
    aws_test_mqtt_build_expected_metrics(
        allocator, NULL, aws_byte_cursor_from_c_str("IoTDeviceSDK/C"), NULL, &expected_buf);

    ASSERT_TRUE(aws_byte_cursor_eq_byte_buf(&output_cursor, &expected_buf));

    aws_byte_buf_clean_up(&output_username);
    aws_byte_buf_clean_up(&expected_buf);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt_append_sdk_metrics_empty, s_test_mqtt_append_sdk_metrics_empty)

static int s_test_mqtt_append_sdk_metrics_basic(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_mqtt_iot_sdk_metrics metrics = {
        .library_name = aws_byte_cursor_from_c_str("TEST_SDK_STRING"),
        // TODO: add metadata entries when enabled
        // .metadata_entries = NULL,
        // .metadata_count = 0,
    };

    struct aws_byte_buf output_username;
    AWS_ZERO_STRUCT(output_username);

    struct aws_byte_cursor original_username = aws_byte_cursor_from_c_str("TEST_USERNAME");

    ASSERT_SUCCESS(
        aws_mqtt_append_sdk_metrics_to_username(allocator, &original_username, &metrics, &output_username, NULL));

    struct aws_byte_cursor output_cursor = aws_byte_cursor_from_buf(&output_username);

    /* Expected string: TEST_USERNAME?SDK=TEST_SDK_STRING&Platform=<OS> */
    struct aws_byte_buf expected_buf;
    AWS_ZERO_STRUCT(expected_buf);
    aws_test_mqtt_build_expected_metrics(
        allocator, &original_username, aws_byte_cursor_from_c_str("TEST_SDK_STRING"), NULL, &expected_buf);

    ASSERT_TRUE(aws_byte_cursor_eq_byte_buf(&output_cursor, &expected_buf));

    aws_byte_buf_clean_up(&output_username);
    aws_byte_buf_clean_up(&expected_buf);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt_append_sdk_metrics_basic, s_test_mqtt_append_sdk_metrics_basic)

static int s_test_mqtt_append_sdk_metrics_existing_attributes(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_mqtt_iot_sdk_metrics metrics = {
        .library_name = aws_byte_cursor_from_c_str("NewSDK"),
        // TODO: add metadata entries when enabled
        // .metadata_entries = NULL,
        // .metadata_count = 0,
    };

    struct aws_byte_buf output_username;
    AWS_ZERO_STRUCT(output_username);

    struct aws_byte_cursor original_username =
        aws_byte_cursor_from_c_str("user?SDK=ExistingSDK&Platform=ExistingPlatform");

    ASSERT_SUCCESS(
        aws_mqtt_append_sdk_metrics_to_username(allocator, &original_username, &metrics, &output_username, NULL));

    struct aws_byte_cursor output_cursor = aws_byte_cursor_from_buf(&output_username);

    ASSERT_TRUE(aws_byte_cursor_eq(&output_cursor, &original_username));

    aws_byte_buf_clean_up(&output_username);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt_append_sdk_metrics_existing_attributes, s_test_mqtt_append_sdk_metrics_existing_attributes)

static int s_test_mqtt_append_sdk_metrics_special_chars(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_mqtt_iot_sdk_metrics metrics = {.library_name = aws_byte_cursor_from_c_str("SDK/Test-1.0")};

    struct aws_byte_buf output_username;
    AWS_ZERO_STRUCT(output_username);

    struct aws_byte_cursor original_username = aws_byte_cursor_from_c_str("user@domain.com");

    ASSERT_SUCCESS(
        aws_mqtt_append_sdk_metrics_to_username(allocator, &original_username, &metrics, &output_username, NULL));

    struct aws_byte_cursor output_cursor = aws_byte_cursor_from_buf(&output_username);

    /* Expected string: user@domain.com?SDK=SDK/Test-1.0&Platform=<OS> */
    struct aws_byte_buf expected_buf;
    AWS_ZERO_STRUCT(expected_buf);
    aws_test_mqtt_build_expected_metrics(
        allocator, &original_username, aws_byte_cursor_from_c_str("SDK/Test-1.0"), NULL, &expected_buf);

    ASSERT_TRUE(aws_byte_cursor_eq_byte_buf(&output_cursor, &expected_buf));

    aws_byte_buf_clean_up(&output_username);
    aws_byte_buf_clean_up(&expected_buf);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt_append_sdk_metrics_special_chars, s_test_mqtt_append_sdk_metrics_special_chars)

static int s_test_mqtt_append_sdk_metrics_long_strings(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    char long_username[1024];
    char long_sdk_name[2 * 1024 * 128];

    memset(long_username, 'A', sizeof(long_username) - 1);
    long_username[sizeof(long_username) - 1] = '\0';

    memset(long_sdk_name, 'B', sizeof(long_sdk_name) - 1);
    long_sdk_name[sizeof(long_sdk_name) - 1] = '\0';

    struct aws_mqtt_iot_sdk_metrics metrics = {.library_name = aws_byte_cursor_from_c_str(long_sdk_name)};

    struct aws_mqtt_iot_sdk_metrics short_metrics = {.library_name = aws_byte_cursor_from_c_str("SHORT_SDK_NAME")};

    struct aws_byte_buf output_username;
    AWS_ZERO_STRUCT(output_username);

    struct aws_byte_cursor original_username = aws_byte_cursor_from_c_str(long_username);

    // aws_mqtt_append_sdk_metrics_to_username does not valid original username length
    ASSERT_SUCCESS(
        aws_mqtt_append_sdk_metrics_to_username(allocator, &original_username, &short_metrics, &output_username, NULL));
    // aws_mqtt_append_sdk_metrics_to_username fails when the extra metrics string exceeds buffer limit
    ASSERT_FAILS(
        aws_mqtt_append_sdk_metrics_to_username(allocator, &original_username, &metrics, &output_username, NULL));

    aws_byte_buf_clean_up(&output_username);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt_append_sdk_metrics_long_strings, s_test_mqtt_append_sdk_metrics_long_strings)

static int s_test_mqtt_append_sdk_metrics_invalid_utf8(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    /* Invalid UTF-8 sequence in library name */
    struct aws_byte_cursor invalid_utf8_library = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("TestSDK\xFF\xFE");

    struct aws_mqtt_iot_sdk_metrics metrics = {.library_name = invalid_utf8_library};

    struct aws_byte_buf output_username;
    AWS_ZERO_STRUCT(output_username);

    struct aws_byte_cursor original_username = aws_byte_cursor_from_c_str("testuser");

    /* Should fail due to invalid UTF-8 */
    ASSERT_FAILS(
        aws_mqtt_append_sdk_metrics_to_username(allocator, &original_username, &metrics, &output_username, NULL));
    ASSERT_INT_EQUALS(aws_last_error(), AWS_ERROR_INVALID_UTF8);

    aws_byte_buf_clean_up(&output_username);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt_append_sdk_metrics_invalid_utf8, s_test_mqtt_append_sdk_metrics_invalid_utf8)
