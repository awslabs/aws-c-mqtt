/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/string.h>
#include <aws/io/stream.h>
#include <aws/mqtt/mqtt.h>
#include <aws/mqtt/private/v5/mqtt5_encoder.h>
#include <aws/mqtt/v5/mqtt5_types.h>

#include <aws/testing/aws_test_harness.h>

static uint8_t s_expected_ping_encoding[2] = {0xC0, 0x00};

static int s_mqtt5_packet_ping_encode_fn(struct aws_allocator *allocator, void *ctx) {
    struct aws_byte_buf dest;
    aws_byte_buf_init(&dest, allocator, 256);

    struct aws_mqtt5_encoder encoder;
    ASSERT_SUCCESS(aws_mqtt5_encoder_init(&encoder, allocator, NULL));

    ASSERT_SUCCESS(aws_mqtt5_encoder_begin_pingreq(&encoder));
    enum aws_mqtt5_encoding_result result = aws_mqtt5_encoder_encode_to_buffer(&encoder, &dest);

    ASSERT_INT_EQUALS(AWS_MQTT5_ER_FINISHED, result);
    ASSERT_INT_EQUALS(2, dest.len);
    ASSERT_BIN_ARRAYS_EQUALS(
        &s_expected_ping_encoding[0], AWS_ARRAY_SIZE(s_expected_ping_encoding), dest.buffer, dest.len);

    aws_byte_buf_clean_up(&dest);
    aws_mqtt5_encoder_clean_up(&encoder);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_packet_ping_encode, s_mqtt5_packet_ping_encode_fn)

static uint8_t s_expected_minimal_disconnect_encoding[4] = {0xE0, 0x02, 0x83, 0x00};

static int s_mqtt5_packet_disconnect_encode_minimal_fn(struct aws_allocator *allocator, void *ctx) {
    struct aws_byte_buf dest;
    aws_byte_buf_init(&dest, allocator, 256);

    struct aws_mqtt5_encoder encoder;
    ASSERT_SUCCESS(aws_mqtt5_encoder_init(&encoder, allocator, NULL));

    struct aws_mqtt5_packet_disconnect_view disconnect_view = {
        .reason_code = AWS_MQTT5_DRC_IMPLEMENTATION_SPECIFIC_ERROR,
    };

    ASSERT_SUCCESS(aws_mqtt5_encoder_begin_disconnect(&encoder, &disconnect_view));
    enum aws_mqtt5_encoding_result result = aws_mqtt5_encoder_encode_to_buffer(&encoder, &dest);

    ASSERT_INT_EQUALS(AWS_MQTT5_ER_FINISHED, result);
    ASSERT_INT_EQUALS(4, dest.len);
    ASSERT_BIN_ARRAYS_EQUALS(
        &s_expected_minimal_disconnect_encoding[0],
        AWS_ARRAY_SIZE(s_expected_minimal_disconnect_encoding),
        dest.buffer,
        dest.len);

    aws_byte_buf_clean_up(&dest);
    aws_mqtt5_encoder_clean_up(&encoder);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_packet_disconnect_encode_minimal, s_mqtt5_packet_disconnect_encode_minimal_fn)

static char s_user_prop1_name[] = "Property1";
static char s_user_prop1_value[] = "Value1";
static char s_user_prop2_name[] = "Property2";
static char s_user_prop2_value[] = "Value2";
static const struct aws_mqtt5_user_property s_user_properties[] = {
    {
        .name =
            {
                .ptr = (uint8_t *)s_user_prop1_name,
                .len = AWS_ARRAY_SIZE(s_user_prop1_name) - 1,
            },
        .value =
            {
                .ptr = (uint8_t *)s_user_prop1_value,
                .len = AWS_ARRAY_SIZE(s_user_prop1_value) - 1,
            },
    },
    {
        .name =
            {
                .ptr = (uint8_t *)s_user_prop2_name,
                .len = AWS_ARRAY_SIZE(s_user_prop2_name) - 1,
            },
        .value =
            {
                .ptr = (uint8_t *)s_user_prop2_value,
                .len = AWS_ARRAY_SIZE(s_user_prop2_value) - 1,
            },
    },
};

static const char *s_reason_string = "This is why I'm disconnecting";
static const char *s_server_reference = "connect-here-instead.com";

static int s_mqtt5_packet_disconnect_encode_all_fn(struct aws_allocator *allocator, void *ctx) {
    struct aws_byte_buf dest;
    aws_byte_buf_init(&dest, allocator, 512);

    struct aws_mqtt5_encoder encoder;
    ASSERT_SUCCESS(aws_mqtt5_encoder_init(&encoder, allocator, NULL));

    uint32_t session_expiry_interval_seconds = 333;
    struct aws_byte_cursor reason_string_cursor = aws_byte_cursor_from_c_str(s_reason_string);
    struct aws_byte_cursor server_reference_cursor = aws_byte_cursor_from_c_str(s_server_reference);

    struct aws_mqtt5_packet_disconnect_view disconnect_view = {
        .reason_code = AWS_MQTT5_DRC_DISCONNECT_WITH_WILL_MESSAGE,
        .session_expiry_interval_seconds = &session_expiry_interval_seconds,
        .reason_string = &reason_string_cursor,
        .user_property_count = AWS_ARRAY_SIZE(s_user_properties),
        .user_properties = &s_user_properties[0],
        .server_reference = &server_reference_cursor,
    };

    ASSERT_SUCCESS(aws_mqtt5_encoder_begin_disconnect(&encoder, &disconnect_view));
    enum aws_mqtt5_encoding_result result = aws_mqtt5_encoder_encode_to_buffer(&encoder, &dest);

    /* nothing to assert other than success until we can do round tripping */
    ASSERT_INT_EQUALS(AWS_MQTT5_ER_FINISHED, result);

    aws_byte_buf_clean_up(&dest);
    aws_mqtt5_encoder_clean_up(&encoder);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_packet_disconnect_encode_all, s_mqtt5_packet_disconnect_encode_all_fn)

static int s_mqtt5_packet_connect_encode_minimal_fn(struct aws_allocator *allocator, void *ctx) {
    struct aws_byte_buf dest;
    aws_byte_buf_init(&dest, allocator, 256);

    struct aws_mqtt5_encoder encoder;
    ASSERT_SUCCESS(aws_mqtt5_encoder_init(&encoder, allocator, NULL));

    struct aws_mqtt5_packet_connect_view connect_view = {
        .keep_alive_interval_seconds = 1200,
        .clean_start = true,
    };

    ASSERT_SUCCESS(aws_mqtt5_encoder_begin_connect(&encoder, &connect_view));
    enum aws_mqtt5_encoding_result result = aws_mqtt5_encoder_encode_to_buffer(&encoder, &dest);

    ASSERT_INT_EQUALS(AWS_MQTT5_ER_FINISHED, result);

    aws_byte_buf_clean_up(&dest);
    aws_mqtt5_encoder_clean_up(&encoder);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_packet_connect_encode_minimal, s_mqtt5_packet_connect_encode_minimal_fn)

static const char *s_will_payload = "{\"content\":\"This is the payload of a will message\"}";
static const char *s_will_topic = "zomg/where-did/my/connection-go";
static const char *s_will_response_topic = "TheWillResponseTopic";
static const char *s_will_correlation_data = "ThisAndThat";
static const char *s_will_content_type = "Json";
static const char *s_client_id = "DeviceNumber47";
static const char *s_username = "MyUser";
static const char *s_password = "SuprSekritDontRead";

static int s_mqtt5_packet_connect_encode_all_fn(struct aws_allocator *allocator, void *ctx) {
    struct aws_byte_buf dest;
    aws_byte_buf_init(&dest, allocator, 1024);

    struct aws_mqtt5_encoder encoder;
    ASSERT_SUCCESS(aws_mqtt5_encoder_init(&encoder, allocator, NULL));

    struct aws_byte_cursor will_payload_cursor = aws_byte_cursor_from_c_str(s_will_payload);
    enum aws_mqtt5_payload_format_indicator payload_format = AWS_MQTT5_PFI_UTF8;
    uint32_t message_expiry_interval_seconds = 65537;
    uint16_t topic_alias = 1;
    struct aws_byte_cursor will_response_topic = aws_byte_cursor_from_c_str(s_will_response_topic);
    struct aws_byte_cursor will_correlation_data = aws_byte_cursor_from_c_str(s_will_correlation_data);
    struct aws_byte_cursor will_content_type = aws_byte_cursor_from_c_str(s_will_content_type);
    struct aws_byte_cursor username = aws_byte_cursor_from_c_str(s_username);
    struct aws_byte_cursor password = aws_byte_cursor_from_c_str(s_password);
    uint32_t session_expiry_interval_seconds = 3600;
    uint8_t request_response_information = 1;
    uint8_t request_problem_information = 1;
    uint16_t receive_maximum = 50;
    uint16_t topic_alias_maximum = 16;
    uint32_t maximum_packet_size_bytes = 1ULL << 24;
    uint32_t will_delay_interval_seconds = 30;

    struct aws_mqtt5_packet_publish_view will_view = {
        .payload = will_payload_cursor,
        .qos = AWS_MQTT5_QOS_AT_LEAST_ONCE,
        .retain = true,
        .topic = aws_byte_cursor_from_c_str(s_will_topic),
        .payload_format = &payload_format,
        .message_expiry_interval_seconds = &message_expiry_interval_seconds,
        .topic_alias = &topic_alias,
        .response_topic = &will_response_topic,
        .correlation_data = &will_correlation_data,
        .content_type = &will_content_type,
        .user_property_count = AWS_ARRAY_SIZE(s_user_properties),
        .user_properties = &s_user_properties[0],
    };

    struct aws_mqtt5_packet_connect_view connect_view = {
        .keep_alive_interval_seconds = 1200,
        .client_id = aws_byte_cursor_from_c_str(s_client_id),
        .username = &username,
        .password = &password,
        .clean_start = true,
        .session_expiry_interval_seconds = &session_expiry_interval_seconds,
        .request_response_information = &request_response_information,
        .request_problem_information = &request_problem_information,
        .receive_maximum = &receive_maximum,
        .topic_alias_maximum = &topic_alias_maximum,
        .maximum_packet_size_bytes = &maximum_packet_size_bytes,
        .will_delay_interval_seconds = &will_delay_interval_seconds,
        .will = &will_view,
        .user_property_count = AWS_ARRAY_SIZE(s_user_properties),
        .user_properties = &s_user_properties[0],
    };

    ASSERT_SUCCESS(aws_mqtt5_encoder_begin_connect(&encoder, &connect_view));
    enum aws_mqtt5_encoding_result result = aws_mqtt5_encoder_encode_to_buffer(&encoder, &dest);

    ASSERT_INT_EQUALS(AWS_MQTT5_ER_FINISHED, result);

    aws_byte_buf_clean_up(&dest);
    aws_mqtt5_encoder_clean_up(&encoder);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_packet_connect_encode_all, s_mqtt5_packet_connect_encode_all_fn)
