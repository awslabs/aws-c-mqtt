/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "mqtt5_testing_utils.h"
#include <aws/common/string.h>
#include <aws/io/stream.h>
#include <aws/mqtt/mqtt.h>
#include <aws/mqtt/private/v5/mqtt5_decoder.h>
#include <aws/mqtt/private/v5/mqtt5_encoder.h>
#include <aws/mqtt/private/v5/mqtt5_utils.h>
#include <aws/mqtt/v5/mqtt5_types.h>

#include <aws/testing/aws_test_harness.h>

/*
 * AWS_MQTT_API enum aws_mqtt5_decode_result_type aws_mqtt5_decode_vli(struct aws_byte_cursor *cursor, uint32_t *dest);
 * AWS_MQTT_API int aws_mqtt5_encode_variable_length_integer(struct aws_byte_buf *buf, uint32_t value);
 * AWS_MQTT_API int aws_mqtt5_get_variable_length_encode_size(size_t value, size_t *encode_size);
 */

static int s_mqtt5_vli_size_fn(struct aws_allocator *allocator, void *ctx) {

    size_t encode_size = 0;

    ASSERT_SUCCESS(aws_mqtt5_get_variable_length_encode_size(0, &encode_size));
    ASSERT_INT_EQUALS(1, encode_size);

    ASSERT_SUCCESS(aws_mqtt5_get_variable_length_encode_size(1, &encode_size));
    ASSERT_INT_EQUALS(1, encode_size);

    ASSERT_SUCCESS(aws_mqtt5_get_variable_length_encode_size(127, &encode_size));
    ASSERT_INT_EQUALS(1, encode_size);

    ASSERT_SUCCESS(aws_mqtt5_get_variable_length_encode_size(128, &encode_size));
    ASSERT_INT_EQUALS(2, encode_size);

    ASSERT_SUCCESS(aws_mqtt5_get_variable_length_encode_size(256, &encode_size));
    ASSERT_INT_EQUALS(2, encode_size);

    ASSERT_SUCCESS(aws_mqtt5_get_variable_length_encode_size(16383, &encode_size));
    ASSERT_INT_EQUALS(2, encode_size);

    ASSERT_SUCCESS(aws_mqtt5_get_variable_length_encode_size(16384, &encode_size));
    ASSERT_INT_EQUALS(3, encode_size);

    ASSERT_SUCCESS(aws_mqtt5_get_variable_length_encode_size(16384, &encode_size));
    ASSERT_INT_EQUALS(3, encode_size);

    ASSERT_SUCCESS(aws_mqtt5_get_variable_length_encode_size(16385, &encode_size));
    ASSERT_INT_EQUALS(3, encode_size);

    ASSERT_SUCCESS(aws_mqtt5_get_variable_length_encode_size(2097151, &encode_size));
    ASSERT_INT_EQUALS(3, encode_size);

    ASSERT_SUCCESS(aws_mqtt5_get_variable_length_encode_size(2097152, &encode_size));
    ASSERT_INT_EQUALS(4, encode_size);

    ASSERT_SUCCESS(aws_mqtt5_get_variable_length_encode_size(AWS_MQTT5_MAXIMUM_VARIABLE_LENGTH_INTEGER, &encode_size));
    ASSERT_INT_EQUALS(4, encode_size);

    ASSERT_FAILS(
        aws_mqtt5_get_variable_length_encode_size(AWS_MQTT5_MAXIMUM_VARIABLE_LENGTH_INTEGER + 1, &encode_size));
    ASSERT_FAILS(aws_mqtt5_get_variable_length_encode_size(0xFFFFFFFF, &encode_size));

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_vli_size, s_mqtt5_vli_size_fn)

static int s_do_success_round_trip_vli_test(uint32_t value, struct aws_allocator *allocator) {
    struct aws_byte_buf buffer;
    aws_byte_buf_init(&buffer, allocator, 4);

    ASSERT_SUCCESS(aws_mqtt5_encode_variable_length_integer(&buffer, value));

    size_t encoded_size = 0;
    ASSERT_SUCCESS(aws_mqtt5_get_variable_length_encode_size(value, &encoded_size));
    ASSERT_INT_EQUALS(encoded_size, buffer.len);

    uint32_t decoded_value = 0;
    for (size_t i = 1; i < encoded_size; ++i) {
        struct aws_byte_cursor partial_cursor = aws_byte_cursor_from_buf(&buffer);
        partial_cursor.len = i;

        enum aws_mqtt5_decode_result_type result = aws_mqtt5_decode_vli(&partial_cursor, &decoded_value);
        ASSERT_INT_EQUALS(AWS_MQTT5_DRT_MORE_DATA, result);
    }

    struct aws_byte_cursor full_cursor = aws_byte_cursor_from_buf(&buffer);

    enum aws_mqtt5_decode_result_type result = aws_mqtt5_decode_vli(&full_cursor, &decoded_value);
    ASSERT_INT_EQUALS(AWS_MQTT5_DRT_SUCCESS, result);
    ASSERT_INT_EQUALS(decoded_value, value);

    aws_byte_buf_clean_up(&buffer);

    return AWS_OP_SUCCESS;
}

static int s_mqtt5_vli_success_round_trip_fn(struct aws_allocator *allocator, void *ctx) {

    ASSERT_SUCCESS(s_do_success_round_trip_vli_test(0, allocator));
    ASSERT_SUCCESS(s_do_success_round_trip_vli_test(1, allocator));
    ASSERT_SUCCESS(s_do_success_round_trip_vli_test(47, allocator));
    ASSERT_SUCCESS(s_do_success_round_trip_vli_test(127, allocator));
    ASSERT_SUCCESS(s_do_success_round_trip_vli_test(128, allocator));
    ASSERT_SUCCESS(s_do_success_round_trip_vli_test(129, allocator));
    ASSERT_SUCCESS(s_do_success_round_trip_vli_test(511, allocator));
    ASSERT_SUCCESS(s_do_success_round_trip_vli_test(8000, allocator));
    ASSERT_SUCCESS(s_do_success_round_trip_vli_test(16383, allocator));
    ASSERT_SUCCESS(s_do_success_round_trip_vli_test(16384, allocator));
    ASSERT_SUCCESS(s_do_success_round_trip_vli_test(16385, allocator));
    ASSERT_SUCCESS(s_do_success_round_trip_vli_test(100000, allocator));
    ASSERT_SUCCESS(s_do_success_round_trip_vli_test(4200000, allocator));
    ASSERT_SUCCESS(s_do_success_round_trip_vli_test(34200000, allocator));
    ASSERT_SUCCESS(s_do_success_round_trip_vli_test(AWS_MQTT5_MAXIMUM_VARIABLE_LENGTH_INTEGER, allocator));

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_vli_success_round_trip, s_mqtt5_vli_success_round_trip_fn)

static int s_mqtt5_vli_encode_failures_fn(struct aws_allocator *allocator, void *ctx) {

    struct aws_byte_buf buffer;
    aws_byte_buf_init(&buffer, allocator, 4);

    ASSERT_FAILS(aws_mqtt5_encode_variable_length_integer(&buffer, AWS_MQTT5_MAXIMUM_VARIABLE_LENGTH_INTEGER + 1));
    ASSERT_FAILS(aws_mqtt5_encode_variable_length_integer(&buffer, 0x80000000));
    ASSERT_FAILS(aws_mqtt5_encode_variable_length_integer(&buffer, 0xFFFFFFFF));

    aws_byte_buf_clean_up(&buffer);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_vli_encode_failures, s_mqtt5_vli_encode_failures_fn)

static uint8_t bad_buffers0[] = {0x80, 0x80, 0x80, 0x80};
static uint8_t bad_buffers1[] = {0x81, 0x81, 0x81, 0xFF};

static int s_mqtt5_vli_decode_failures_fn(struct aws_allocator *allocator, void *ctx) {

    uint32_t value = 0;

    struct aws_byte_cursor cursor = aws_byte_cursor_from_array(&bad_buffers0[0], AWS_ARRAY_SIZE(bad_buffers0));
    ASSERT_INT_EQUALS(AWS_MQTT5_DRT_ERROR, aws_mqtt5_decode_vli(&cursor, &value));

    cursor = aws_byte_cursor_from_array(&bad_buffers1[0], AWS_ARRAY_SIZE(bad_buffers1));
    ASSERT_INT_EQUALS(AWS_MQTT5_DRT_ERROR, aws_mqtt5_decode_vli(&cursor, &value));

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_vli_decode_failures, s_mqtt5_vli_decode_failures_fn)

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
static const char *s_will_payload = "{\"content\":\"This is the payload of a will message\"}";
static const char *s_will_topic = "zomg/where-did/my/connection-go";
static const char *s_will_response_topic = "TheWillResponseTopic";
static const char *s_will_correlation_data = "ThisAndThat";
static const char *s_will_content_type = "Json";
static const char *s_client_id = "DeviceNumber47";
static const char *s_username = "MyUser";
static const char *s_password = "SuprSekritDontRead";

struct aws_mqtt5_encode_decode_tester {
    struct aws_mqtt5_encoder_function_table encoder_function_table;
    struct aws_mqtt5_decoder_function_table decoder_function_table;

    size_t view_count;

    size_t expected_view_count;
    void *expected_views;
};

static void s_aws_mqtt5_encode_decoder_tester_init_single_view(
    struct aws_mqtt5_encode_decode_tester *tester,
    void *expected_view) {
    tester->view_count = 0;
    tester->expected_view_count = 1;
    tester->expected_views = expected_view;

    aws_mqtt5_encode_init_testing_function_table(&tester->encoder_function_table);
    aws_mqtt5_decode_init_testing_function_table(&tester->decoder_function_table);
}

static int s_aws_mqtt5_on_disconnect_received_fn(enum aws_mqtt5_packet_type type, void *packet_view, void *user_data) {
    struct aws_mqtt5_encode_decode_tester *tester = user_data;

    ++tester->view_count;

    ASSERT_INT_EQUALS((uint32_t)AWS_MQTT5_PT_DISCONNECT, (uint32_t)type);

    struct aws_mqtt5_packet_disconnect_view *disconnect_view = packet_view;
    struct aws_mqtt5_packet_disconnect_view *expected_view = tester->expected_views;

    ASSERT_INT_EQUALS((uint32_t)expected_view->reason_code, (uint32_t)disconnect_view->reason_code);
    ASSERT_INT_EQUALS(
        *expected_view->session_expiry_interval_seconds, *disconnect_view->session_expiry_interval_seconds);
    ASSERT_BIN_ARRAYS_EQUALS(
        expected_view->reason_string->ptr,
        expected_view->reason_string->len,
        disconnect_view->reason_string->ptr,
        disconnect_view->reason_string->len);
    ASSERT_BIN_ARRAYS_EQUALS(
        expected_view->server_reference->ptr,
        expected_view->server_reference->len,
        disconnect_view->server_reference->ptr,
        disconnect_view->server_reference->len);
    ASSERT_SUCCESS(aws_mqtt5_test_verify_user_properties_raw(
        disconnect_view->user_property_count,
        disconnect_view->user_properties,
        expected_view->user_property_count,
        expected_view->user_properties));

    return AWS_OP_SUCCESS;
}

static int s_mqtt5_packet_disconnect_round_trip_fn(struct aws_allocator *allocator, void *ctx) {
    struct aws_byte_buf dest;
    aws_byte_buf_init(&dest, allocator, 512);

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

    struct aws_mqtt5_encode_decode_tester tester;
    s_aws_mqtt5_encode_decoder_tester_init_single_view(&tester, &disconnect_view);

    struct aws_mqtt5_encoder_options encoder_options = {
        .encoders = &tester.encoder_function_table,
    };
    struct aws_mqtt5_encoder encoder;
    ASSERT_SUCCESS(aws_mqtt5_encoder_init(&encoder, allocator, &encoder_options));

    ASSERT_SUCCESS(aws_mqtt5_encoder_append_packet_encoding(&encoder, AWS_MQTT5_PT_DISCONNECT, &disconnect_view));
    enum aws_mqtt5_encoding_result result = aws_mqtt5_encoder_encode_to_buffer(&encoder, &dest);

    ASSERT_INT_EQUALS(AWS_MQTT5_ER_FINISHED, result);

    struct aws_mqtt5_decoder_options decoder_options = {
        .on_packet_received = s_aws_mqtt5_on_disconnect_received_fn,
        .callback_user_data = &tester,
        .decoder_table = &tester.decoder_function_table,
    };

    struct aws_mqtt5_decoder decoder;
    ASSERT_SUCCESS(aws_mqtt5_decoder_init(&decoder, allocator, &decoder_options));

    ASSERT_SUCCESS(aws_mqtt5_decoder_on_data_received(&decoder, aws_byte_cursor_from_buf(&dest)));

    ASSERT_INT_EQUALS(1, tester.view_count);

    aws_byte_buf_clean_up(&dest);
    aws_mqtt5_encoder_clean_up(&encoder);
    aws_mqtt5_decoder_clean_up(&decoder);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_packet_disconnect_round_trip, s_mqtt5_packet_disconnect_round_trip_fn)

static int s_aws_mqtt5_on_pingreq_received_fn(enum aws_mqtt5_packet_type type, void *packet_view, void *user_data) {
    struct aws_mqtt5_encode_decode_tester *tester = user_data;

    ++tester->view_count;

    ASSERT_INT_EQUALS((uint32_t)AWS_MQTT5_PT_PINGREQ, (uint32_t)type);
    ASSERT_NULL(packet_view);

    return AWS_OP_SUCCESS;
}

static int s_mqtt5_packet_pingreq_round_trip_fn(struct aws_allocator *allocator, void *ctx) {
    struct aws_byte_buf dest;
    aws_byte_buf_init(&dest, allocator, 512);

    struct aws_mqtt5_encode_decode_tester tester;
    s_aws_mqtt5_encode_decoder_tester_init_single_view(&tester, NULL);

    struct aws_mqtt5_encoder_options encoder_options = {
        .encoders = &tester.encoder_function_table,
    };

    struct aws_mqtt5_encoder encoder;
    ASSERT_SUCCESS(aws_mqtt5_encoder_init(&encoder, allocator, &encoder_options));

    ASSERT_SUCCESS(aws_mqtt5_encoder_append_packet_encoding(&encoder, AWS_MQTT5_PT_PINGREQ, NULL));
    enum aws_mqtt5_encoding_result result = aws_mqtt5_encoder_encode_to_buffer(&encoder, &dest);

    ASSERT_INT_EQUALS(AWS_MQTT5_ER_FINISHED, result);

    struct aws_mqtt5_decoder_options decoder_options = {
        .on_packet_received = s_aws_mqtt5_on_pingreq_received_fn,
        .callback_user_data = &tester,
        .decoder_table = &tester.decoder_function_table,
    };

    struct aws_mqtt5_decoder decoder;
    ASSERT_SUCCESS(aws_mqtt5_decoder_init(&decoder, allocator, &decoder_options));

    ASSERT_SUCCESS(aws_mqtt5_decoder_on_data_received(&decoder, aws_byte_cursor_from_buf(&dest)));

    ASSERT_INT_EQUALS(1, tester.view_count);

    aws_byte_buf_clean_up(&dest);
    aws_mqtt5_encoder_clean_up(&encoder);
    aws_mqtt5_decoder_clean_up(&decoder);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_packet_pingreq_round_trip, s_mqtt5_packet_pingreq_round_trip_fn)

static int s_aws_mqtt5_on_pingresp_received_fn(enum aws_mqtt5_packet_type type, void *packet_view, void *user_data) {
    struct aws_mqtt5_encode_decode_tester *tester = user_data;

    ++tester->view_count;

    ASSERT_INT_EQUALS((uint32_t)AWS_MQTT5_PT_PINGRESP, (uint32_t)type);
    ASSERT_NULL(packet_view);

    return AWS_OP_SUCCESS;
}

static int s_mqtt5_packet_pingresp_round_trip_fn(struct aws_allocator *allocator, void *ctx) {
    struct aws_byte_buf dest;
    aws_byte_buf_init(&dest, allocator, 512);

    struct aws_mqtt5_encode_decode_tester tester;
    s_aws_mqtt5_encode_decoder_tester_init_single_view(&tester, NULL);

    struct aws_mqtt5_encoder_options encoder_options = {
        .encoders = &tester.encoder_function_table,
    };

    struct aws_mqtt5_encoder encoder;
    ASSERT_SUCCESS(aws_mqtt5_encoder_init(&encoder, allocator, &encoder_options));

    ASSERT_SUCCESS(aws_mqtt5_encoder_append_packet_encoding(&encoder, AWS_MQTT5_PT_PINGRESP, NULL));
    enum aws_mqtt5_encoding_result result = aws_mqtt5_encoder_encode_to_buffer(&encoder, &dest);

    ASSERT_INT_EQUALS(AWS_MQTT5_ER_FINISHED, result);

    struct aws_mqtt5_decoder_options decoder_options = {
        .on_packet_received = s_aws_mqtt5_on_pingresp_received_fn,
        .callback_user_data = &tester,
        .decoder_table = &tester.decoder_function_table,
    };

    struct aws_mqtt5_decoder decoder;
    ASSERT_SUCCESS(aws_mqtt5_decoder_init(&decoder, allocator, &decoder_options));

    ASSERT_SUCCESS(aws_mqtt5_decoder_on_data_received(&decoder, aws_byte_cursor_from_buf(&dest)));

    ASSERT_INT_EQUALS(1, tester.view_count);

    aws_byte_buf_clean_up(&dest);
    aws_mqtt5_encoder_clean_up(&encoder);
    aws_mqtt5_decoder_clean_up(&decoder);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_packet_pingresp_round_trip, s_mqtt5_packet_pingresp_round_trip_fn)

static int s_aws_mqtt5_on_connect_received_fn(enum aws_mqtt5_packet_type type, void *packet_view, void *user_data) {
    struct aws_mqtt5_encode_decode_tester *tester = user_data;

    ++tester->view_count;

    ASSERT_INT_EQUALS((uint32_t)AWS_MQTT5_PT_CONNECT, (uint32_t)type);

    struct aws_mqtt5_packet_connect_view *connect_view = packet_view;
    struct aws_mqtt5_packet_connect_view *expected_view = tester->expected_views;

    ASSERT_INT_EQUALS(expected_view->keep_alive_interval_seconds, connect_view->keep_alive_interval_seconds);
    ASSERT_BIN_ARRAYS_EQUALS(
        expected_view->client_id.ptr,
        expected_view->client_id.len,
        connect_view->client_id.ptr,
        connect_view->client_id.len);
    ASSERT_BIN_ARRAYS_EQUALS(
        expected_view->username->ptr,
        expected_view->username->len,
        connect_view->username->ptr,
        connect_view->username->len);
    ASSERT_BIN_ARRAYS_EQUALS(
        expected_view->password->ptr,
        expected_view->password->len,
        connect_view->password->ptr,
        connect_view->password->len);
    ASSERT_TRUE(expected_view->clean_start == connect_view->clean_start);
    ASSERT_INT_EQUALS(*expected_view->session_expiry_interval_seconds, *connect_view->session_expiry_interval_seconds);

    ASSERT_INT_EQUALS(*expected_view->request_response_information, *connect_view->request_response_information);
    ASSERT_INT_EQUALS(*expected_view->request_problem_information, *connect_view->request_problem_information);
    ASSERT_INT_EQUALS(*expected_view->receive_maximum, *connect_view->receive_maximum);
    ASSERT_INT_EQUALS(*expected_view->topic_alias_maximum, *connect_view->topic_alias_maximum);
    ASSERT_INT_EQUALS(*expected_view->maximum_packet_size_bytes, *connect_view->maximum_packet_size_bytes);

    ASSERT_INT_EQUALS(*expected_view->will_delay_interval_seconds, *connect_view->will_delay_interval_seconds);

    const struct aws_mqtt5_packet_publish_view *expected_will_view = expected_view->will;
    const struct aws_mqtt5_packet_publish_view *will_view = connect_view->will;

    ASSERT_BIN_ARRAYS_EQUALS(
        expected_will_view->payload.ptr,
        expected_will_view->payload.len,
        will_view->payload.ptr,
        will_view->payload.len);
    ASSERT_INT_EQUALS(expected_will_view->qos, will_view->qos);
    ASSERT_INT_EQUALS(expected_will_view->retain, will_view->retain);
    ASSERT_BIN_ARRAYS_EQUALS(
        expected_will_view->topic.ptr, expected_will_view->topic.len, will_view->topic.ptr, will_view->topic.len);
    ASSERT_INT_EQUALS(*expected_will_view->payload_format, *will_view->payload_format);
    ASSERT_INT_EQUALS(
        *expected_will_view->message_expiry_interval_seconds, *will_view->message_expiry_interval_seconds);
    ASSERT_BIN_ARRAYS_EQUALS(
        expected_will_view->response_topic->ptr,
        expected_will_view->response_topic->len,
        will_view->response_topic->ptr,
        will_view->response_topic->len);
    ASSERT_BIN_ARRAYS_EQUALS(
        expected_will_view->correlation_data->ptr,
        expected_will_view->correlation_data->len,
        will_view->correlation_data->ptr,
        will_view->correlation_data->len);
    ASSERT_BIN_ARRAYS_EQUALS(
        expected_will_view->content_type->ptr,
        expected_will_view->content_type->len,
        will_view->content_type->ptr,
        will_view->content_type->len);

    ASSERT_SUCCESS(aws_mqtt5_test_verify_user_properties_raw(
        expected_will_view->user_property_count,
        expected_will_view->user_properties,
        will_view->user_property_count,
        will_view->user_properties));

    ASSERT_SUCCESS(aws_mqtt5_test_verify_user_properties_raw(
        expected_view->user_property_count,
        expected_view->user_properties,
        connect_view->user_property_count,
        connect_view->user_properties));

    ASSERT_BIN_ARRAYS_EQUALS(
        expected_view->authentication_method->ptr,
        expected_view->authentication_method->len,
        connect_view->authentication_method->ptr,
        connect_view->authentication_method->len);
    ASSERT_BIN_ARRAYS_EQUALS(
        expected_view->authentication_data->ptr,
        expected_view->authentication_data->len,
        connect_view->authentication_data->ptr,
        connect_view->authentication_data->len);

    return AWS_OP_SUCCESS;
}

static int s_mqtt5_packet_connect_round_trip_fn(struct aws_allocator *allocator, void *ctx) {
    struct aws_byte_buf dest;
    aws_byte_buf_init(&dest, allocator, 512);

    struct aws_byte_cursor will_payload_cursor = aws_byte_cursor_from_c_str(s_will_payload);
    enum aws_mqtt5_payload_format_indicator payload_format = AWS_MQTT5_PFI_UTF8;
    uint32_t message_expiry_interval_seconds = 65537;
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
    struct aws_byte_cursor authentication_method = aws_byte_cursor_from_c_str("AuthMethod");
    struct aws_byte_cursor authentication_data = aws_byte_cursor_from_c_str("SuperSecret");

    struct aws_mqtt5_packet_publish_view will_view = {
        .payload = will_payload_cursor,
        .qos = AWS_MQTT5_QOS_AT_LEAST_ONCE,
        .retain = true,
        .topic = aws_byte_cursor_from_c_str(s_will_topic),
        .payload_format = &payload_format,
        .message_expiry_interval_seconds = &message_expiry_interval_seconds,
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
        .authentication_method = &authentication_method,
        .authentication_data = &authentication_data,
    };

    struct aws_mqtt5_encode_decode_tester tester;
    s_aws_mqtt5_encode_decoder_tester_init_single_view(&tester, &connect_view);

    struct aws_mqtt5_encoder_options encoder_options = {
        .encoders = &tester.encoder_function_table,
    };

    struct aws_mqtt5_encoder encoder;
    ASSERT_SUCCESS(aws_mqtt5_encoder_init(&encoder, allocator, &encoder_options));

    ASSERT_SUCCESS(aws_mqtt5_encoder_append_packet_encoding(&encoder, AWS_MQTT5_PT_CONNECT, &connect_view));
    enum aws_mqtt5_encoding_result result = aws_mqtt5_encoder_encode_to_buffer(&encoder, &dest);

    ASSERT_INT_EQUALS(AWS_MQTT5_ER_FINISHED, result);

    struct aws_mqtt5_decoder_options decoder_options = {
        .on_packet_received = s_aws_mqtt5_on_connect_received_fn,
        .callback_user_data = &tester,
        .decoder_table = &tester.decoder_function_table,
    };

    struct aws_mqtt5_decoder decoder;
    ASSERT_SUCCESS(aws_mqtt5_decoder_init(&decoder, allocator, &decoder_options));

    ASSERT_SUCCESS(aws_mqtt5_decoder_on_data_received(&decoder, aws_byte_cursor_from_buf(&dest)));

    ASSERT_INT_EQUALS(1, tester.view_count);

    aws_byte_buf_clean_up(&dest);
    aws_mqtt5_encoder_clean_up(&encoder);
    aws_mqtt5_decoder_clean_up(&decoder);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_packet_connect_round_trip, s_mqtt5_packet_connect_round_trip_fn)

static int s_aws_mqtt5_on_connack_received_fn(enum aws_mqtt5_packet_type type, void *packet_view, void *user_data) {
    struct aws_mqtt5_encode_decode_tester *tester = user_data;

    ++tester->view_count;

    ASSERT_INT_EQUALS((uint32_t)AWS_MQTT5_PT_CONNACK, (uint32_t)type);

    struct aws_mqtt5_packet_connack_view *connack_view = packet_view;
    struct aws_mqtt5_packet_connack_view *expected_view = tester->expected_views;

    ASSERT_INT_EQUALS(expected_view->session_present, connack_view->session_present);
    ASSERT_INT_EQUALS(expected_view->reason_code, connack_view->reason_code);
    ASSERT_INT_EQUALS(*expected_view->session_expiry_interval, *connack_view->session_expiry_interval);
    ASSERT_INT_EQUALS(*expected_view->receive_maximum, *connack_view->receive_maximum);
    ASSERT_INT_EQUALS(*expected_view->maximum_qos, *connack_view->maximum_qos);
    ASSERT_INT_EQUALS(*expected_view->retain_available, *connack_view->retain_available);
    ASSERT_INT_EQUALS(*expected_view->maximum_packet_size, *connack_view->maximum_packet_size);
    ASSERT_BIN_ARRAYS_EQUALS(
        expected_view->assigned_client_identifier->ptr,
        expected_view->assigned_client_identifier->len,
        connack_view->assigned_client_identifier->ptr,
        connack_view->assigned_client_identifier->len);
    ASSERT_INT_EQUALS(*expected_view->topic_alias_maximum, *connack_view->topic_alias_maximum);
    ASSERT_BIN_ARRAYS_EQUALS(
        expected_view->reason_string->ptr,
        expected_view->reason_string->len,
        connack_view->reason_string->ptr,
        connack_view->reason_string->len);

    ASSERT_SUCCESS(aws_mqtt5_test_verify_user_properties_raw(
        expected_view->user_property_count,
        expected_view->user_properties,
        connack_view->user_property_count,
        connack_view->user_properties));

    ASSERT_INT_EQUALS(
        *expected_view->wildcard_subscriptions_available, *connack_view->wildcard_subscriptions_available);
    ASSERT_INT_EQUALS(
        *expected_view->subscription_identifiers_available, *connack_view->subscription_identifiers_available);
    ASSERT_INT_EQUALS(*expected_view->shared_subscriptions_available, *connack_view->shared_subscriptions_available);
    ASSERT_INT_EQUALS(*expected_view->server_keep_alive, *connack_view->server_keep_alive);

    ASSERT_BIN_ARRAYS_EQUALS(
        expected_view->response_information->ptr,
        expected_view->response_information->len,
        connack_view->response_information->ptr,
        connack_view->response_information->len);
    ASSERT_BIN_ARRAYS_EQUALS(
        expected_view->server_reference->ptr,
        expected_view->server_reference->len,
        connack_view->server_reference->ptr,
        connack_view->server_reference->len);
    ASSERT_BIN_ARRAYS_EQUALS(
        expected_view->authentication_method->ptr,
        expected_view->authentication_method->len,
        connack_view->authentication_method->ptr,
        connack_view->authentication_method->len);
    ASSERT_BIN_ARRAYS_EQUALS(
        expected_view->authentication_data->ptr,
        expected_view->authentication_data->len,
        connack_view->authentication_data->ptr,
        connack_view->authentication_data->len);

    return AWS_OP_SUCCESS;
}

static int s_mqtt5_packet_connack_round_trip_fn(struct aws_allocator *allocator, void *ctx) {
    struct aws_byte_buf dest;
    aws_byte_buf_init(&dest, allocator, 512);

    uint32_t session_expiry_interval = 3600;
    uint16_t receive_maximum = 20;
    enum aws_mqtt5_qos maximum_qos = AWS_MQTT5_QOS_AT_LEAST_ONCE;
    bool retain_available = true;
    uint32_t maximum_packet_size = 1U << 18;
    struct aws_byte_cursor assigned_client_identifier = aws_byte_cursor_from_c_str("server-assigned-client-id-01");
    uint16_t topic_alias_maximum = 10;
    struct aws_byte_cursor reason_string = aws_byte_cursor_from_c_str("You've been banned from this server");
    bool wildcard_subscriptions_available = true;
    bool subscription_identifiers_available = true;
    bool shared_subscriptions_available = true;
    uint16_t server_keep_alive = 1200;
    struct aws_byte_cursor response_information = aws_byte_cursor_from_c_str("some/topic-prefix");
    struct aws_byte_cursor server_reference = aws_byte_cursor_from_c_str("www.somewhere-else.com:1883");
    struct aws_byte_cursor authentication_method = aws_byte_cursor_from_c_str("GSSAPI");
    struct aws_byte_cursor authentication_data = aws_byte_cursor_from_c_str("TOP-SECRET");

    struct aws_mqtt5_packet_connack_view connack_view = {
        .session_present = true,
        .reason_code = AWS_MQTT5_CRC_BANNED,
        .session_expiry_interval = &session_expiry_interval,
        .receive_maximum = &receive_maximum,
        .maximum_qos = &maximum_qos,
        .retain_available = &retain_available,
        .maximum_packet_size = &maximum_packet_size,
        .assigned_client_identifier = &assigned_client_identifier,
        .topic_alias_maximum = &topic_alias_maximum,
        .reason_string = &reason_string,
        .user_property_count = AWS_ARRAY_SIZE(s_user_properties),
        .user_properties = &s_user_properties[0],
        .wildcard_subscriptions_available = &wildcard_subscriptions_available,
        .subscription_identifiers_available = &subscription_identifiers_available,
        .shared_subscriptions_available = &shared_subscriptions_available,
        .server_keep_alive = &server_keep_alive,
        .response_information = &response_information,
        .server_reference = &server_reference,
        .authentication_method = &authentication_method,
        .authentication_data = &authentication_data,
    };

    struct aws_mqtt5_encode_decode_tester tester;
    s_aws_mqtt5_encode_decoder_tester_init_single_view(&tester, &connack_view);

    struct aws_mqtt5_encoder_options encoder_options = {
        .encoders = &tester.encoder_function_table,
    };

    struct aws_mqtt5_encoder encoder;
    ASSERT_SUCCESS(aws_mqtt5_encoder_init(&encoder, allocator, &encoder_options));

    ASSERT_SUCCESS(aws_mqtt5_encoder_append_packet_encoding(&encoder, AWS_MQTT5_PT_CONNACK, &connack_view));
    enum aws_mqtt5_encoding_result result = aws_mqtt5_encoder_encode_to_buffer(&encoder, &dest);

    ASSERT_INT_EQUALS(AWS_MQTT5_ER_FINISHED, result);

    struct aws_mqtt5_decoder_options decoder_options = {
        .on_packet_received = s_aws_mqtt5_on_connack_received_fn,
        .callback_user_data = &tester,
        .decoder_table = &tester.decoder_function_table,
    };

    struct aws_mqtt5_decoder decoder;
    ASSERT_SUCCESS(aws_mqtt5_decoder_init(&decoder, allocator, &decoder_options));

    ASSERT_SUCCESS(aws_mqtt5_decoder_on_data_received(&decoder, aws_byte_cursor_from_buf(&dest)));

    ASSERT_INT_EQUALS(1, tester.view_count);

    aws_byte_buf_clean_up(&dest);
    aws_mqtt5_encoder_clean_up(&encoder);
    aws_mqtt5_decoder_clean_up(&decoder);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_packet_connack_round_trip, s_mqtt5_packet_connack_round_trip_fn)
