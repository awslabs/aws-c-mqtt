/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "mqtt5_testing_utils.h"

#include <aws/common/clock.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/io/host_resolver.h>
#include <aws/mqtt/private/v5/mqtt5_decoder.h>
#include <aws/mqtt/private/v5/mqtt5_encoder.h>
#include <aws/mqtt/private/v5/mqtt5_utils.h>
#include <aws/mqtt/v5/mqtt5_client.h>

#include <aws/testing/aws_test_harness.h>

#include <inttypes.h>

int aws_mqtt5_test_verify_user_properties_raw(
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

static int s_compute_connack_variable_length_fields(
    const struct aws_mqtt5_packet_connack_view *connack_view,
    uint32_t *total_remaining_length,
    uint32_t *property_length) {

    size_t local_property_length =
        aws_mqtt5_compute_user_property_encode_length(connack_view->user_properties, connack_view->user_property_count);

    ADD_OPTIONAL_U32_PROPERTY_LENGTH(connack_view->session_expiry_interval, local_property_length);
    ADD_OPTIONAL_U16_PROPERTY_LENGTH(connack_view->receive_maximum, local_property_length);
    ADD_OPTIONAL_U8_PROPERTY_LENGTH(connack_view->maximum_qos, local_property_length);
    ADD_OPTIONAL_U8_PROPERTY_LENGTH(connack_view->retain_available, local_property_length);
    ADD_OPTIONAL_U32_PROPERTY_LENGTH(connack_view->maximum_packet_size, local_property_length);
    ADD_OPTIONAL_CURSOR_PROPERTY_LENGTH(connack_view->assigned_client_identifier, local_property_length);
    ADD_OPTIONAL_U16_PROPERTY_LENGTH(connack_view->topic_alias_maximum, local_property_length);
    ADD_OPTIONAL_CURSOR_PROPERTY_LENGTH(connack_view->reason_string, local_property_length);
    ADD_OPTIONAL_U8_PROPERTY_LENGTH(connack_view->wildcard_subscriptions_available, local_property_length);
    ADD_OPTIONAL_U8_PROPERTY_LENGTH(connack_view->subscription_identifiers_available, local_property_length);
    ADD_OPTIONAL_U8_PROPERTY_LENGTH(connack_view->shared_subscriptions_available, local_property_length);
    ADD_OPTIONAL_U16_PROPERTY_LENGTH(connack_view->server_keep_alive, local_property_length);
    ADD_OPTIONAL_CURSOR_PROPERTY_LENGTH(connack_view->response_information, local_property_length);
    ADD_OPTIONAL_CURSOR_PROPERTY_LENGTH(connack_view->server_reference, local_property_length);
    ADD_OPTIONAL_CURSOR_PROPERTY_LENGTH(connack_view->authentication_method, local_property_length);
    ADD_OPTIONAL_CURSOR_PROPERTY_LENGTH(connack_view->authentication_data, local_property_length);

    *property_length = (uint32_t)local_property_length;

    size_t property_length_encoding_length = 0;
    if (aws_mqtt5_get_variable_length_encode_size(local_property_length, &property_length_encoding_length)) {
        return AWS_OP_ERR;
    }

    /* reason code (1 byte) + flags (1 byte) */
    *total_remaining_length = *property_length + property_length_encoding_length + 2;

    return AWS_OP_SUCCESS;
}

int aws_mqtt5_encoder_begin_connack(struct aws_mqtt5_encoder *encoder, const void *packet_view) {

    const struct aws_mqtt5_packet_connack_view *connack_view = packet_view;

    uint32_t total_remaining_length = 0;
    uint32_t property_length = 0;
    if (s_compute_connack_variable_length_fields(connack_view, &total_remaining_length, &property_length)) {
        int error_code = aws_last_error();
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_GENERAL,
            "(%p) mqtt5 client encoder - failed to compute variable length values for CONNACK packet with error "
            "%d(%s)",
            (void *)encoder->config.client,
            error_code,
            aws_error_debug_str(error_code));
        return AWS_OP_ERR;
    }

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_GENERAL,
        "(%p) mqtt5 client encoder - setting up encode for a CONNACK packet with remaining length %" PRIu32,
        (void *)encoder->config.client,
        total_remaining_length);

    uint8_t flags = connack_view->session_present ? 1 : 0;

    ADD_ENCODE_STEP_U8(encoder, aws_mqtt5_compute_fixed_header_byte1(AWS_MQTT5_PT_CONNACK, 0));
    ADD_ENCODE_STEP_VLI(encoder, total_remaining_length);
    ADD_ENCODE_STEP_U8(encoder, flags);
    ADD_ENCODE_STEP_U8(encoder, (uint8_t)connack_view->reason_code);
    ADD_ENCODE_STEP_VLI(encoder, property_length);

    if (property_length > 0) {
        ADD_ENCODE_STEP_OPTIONAL_U32_PROPERTY(
            encoder, AWS_MQTT5_PROPERTY_TYPE_SESSION_EXPIRY_INTERVAL, connack_view->session_expiry_interval);
        ADD_ENCODE_STEP_OPTIONAL_U16_PROPERTY(
            encoder, AWS_MQTT5_PROPERTY_TYPE_RECEIVE_MAXIMUM, connack_view->receive_maximum);
        ADD_ENCODE_STEP_OPTIONAL_U8_PROPERTY(encoder, AWS_MQTT5_PROPERTY_TYPE_MAXIMUM_QOS, connack_view->maximum_qos);
        ADD_ENCODE_STEP_OPTIONAL_U8_PROPERTY(
            encoder, AWS_MQTT5_PROPERTY_TYPE_RETAIN_AVAILABLE, connack_view->retain_available);
        ADD_ENCODE_STEP_OPTIONAL_U32_PROPERTY(
            encoder, AWS_MQTT5_PROPERTY_TYPE_MAXIMUM_PACKET_SIZE, connack_view->maximum_packet_size);
        ADD_ENCODE_STEP_OPTIONAL_CURSOR_PROPERTY(
            encoder, AWS_MQTT5_PROPERTY_TYPE_ASSIGNED_CLIENT_IDENTIFIER, connack_view->assigned_client_identifier);
        ADD_ENCODE_STEP_OPTIONAL_U16_PROPERTY(
            encoder, AWS_MQTT5_PROPERTY_TYPE_TOPIC_ALIAS_MAXIMUM, connack_view->topic_alias_maximum);
        ADD_ENCODE_STEP_OPTIONAL_CURSOR_PROPERTY(
            encoder, AWS_MQTT5_PROPERTY_TYPE_REASON_STRING, connack_view->reason_string);
        ADD_ENCODE_STEP_OPTIONAL_U8_PROPERTY(
            encoder,
            AWS_MQTT5_PROPERTY_TYPE_WILDCARD_SUBSCRIPTIONS_AVAILABLE,
            connack_view->wildcard_subscriptions_available);
        ADD_ENCODE_STEP_OPTIONAL_U8_PROPERTY(
            encoder,
            AWS_MQTT5_PROPERTY_TYPE_SUBSCRIPTION_IDENTIFIERS_AVAILABLE,
            connack_view->subscription_identifiers_available);
        ADD_ENCODE_STEP_OPTIONAL_U8_PROPERTY(
            encoder,
            AWS_MQTT5_PROPERTY_TYPE_SHARED_SUBSCRIPTIONS_AVAILABLE,
            connack_view->shared_subscriptions_available);
        ADD_ENCODE_STEP_OPTIONAL_U16_PROPERTY(
            encoder, AWS_MQTT5_PROPERTY_TYPE_SERVER_KEEP_ALIVE, connack_view->server_keep_alive);
        ADD_ENCODE_STEP_OPTIONAL_CURSOR_PROPERTY(
            encoder, AWS_MQTT5_PROPERTY_TYPE_RESPONSE_INFORMATION, connack_view->response_information);
        ADD_ENCODE_STEP_OPTIONAL_CURSOR_PROPERTY(
            encoder, AWS_MQTT5_PROPERTY_TYPE_SERVER_REFERENCE, connack_view->server_reference);
        ADD_ENCODE_STEP_OPTIONAL_CURSOR_PROPERTY(
            encoder, AWS_MQTT5_PROPERTY_TYPE_AUTHENTICATION_METHOD, connack_view->authentication_method);
        ADD_ENCODE_STEP_OPTIONAL_CURSOR_PROPERTY(
            encoder, AWS_MQTT5_PROPERTY_TYPE_AUTHENTICATION_DATA, connack_view->authentication_data);

        aws_mqtt5_add_user_property_encoding_steps(
            encoder, connack_view->user_properties, connack_view->user_property_count);
    }

    return AWS_OP_SUCCESS;
}

int aws_mqtt5_encoder_begin_pingresp(struct aws_mqtt5_encoder *encoder, const void *packet_view) {
    (void)packet_view;

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_GENERAL,
        "(%p) mqtt5 client encoder - setting up encode for a PINGRESP packet",
        (void *)encoder->config.client);

    /* A ping response is just a fixed header with a 0-valued remaining length which we encode as a 0 u8 */
    ADD_ENCODE_STEP_U8(encoder, aws_mqtt5_compute_fixed_header_byte1(AWS_MQTT5_PT_PINGRESP, 0));
    ADD_ENCODE_STEP_U8(encoder, 0);

    return AWS_OP_SUCCESS;
}

void aws_mqtt5_encode_init_testing_function_table(struct aws_mqtt5_encoder_function_table *function_table) {
    *function_table = *g_aws_mqtt5_encoder_default_function_table;
    function_table->encoders_by_packet_type[AWS_MQTT5_PT_PINGRESP] = &aws_mqtt5_encoder_begin_pingresp;
    function_table->encoders_by_packet_type[AWS_MQTT5_PT_CONNACK] = &aws_mqtt5_encoder_begin_connack;
}

static int s_aws_mqtt5_decoder_decode_pingreq(struct aws_mqtt5_decoder *decoder) {
    if (decoder->packet_cursor.len != 0) {
        goto error;
    }

    uint8_t expected_first_byte = aws_mqtt5_compute_fixed_header_byte1(AWS_MQTT5_PT_PINGREQ, 0);
    if (decoder->packet_first_byte != expected_first_byte || decoder->remaining_length != 0) {
        goto error;
    }

    if (decoder->options.on_packet_received != NULL) {
        (*decoder->options.on_packet_received)(AWS_MQTT5_PT_PINGREQ, NULL, decoder->options.callback_user_data);
    }

    return AWS_OP_SUCCESS;

error:

    AWS_LOGF_ERROR(
        AWS_LS_MQTT5_GENERAL, "(%p) aws_mqtt5_decoder - PINGREQ decode failure", decoder->options.callback_user_data);
    return aws_raise_error(AWS_ERROR_MQTT5_DECODE_PROTOCOL_ERROR);
}

/* decode function for all CONNECT properties.  Movable to test-only code if we switched to a decoding function table */
static int s_read_connect_property(
    struct aws_mqtt5_packet_connect_storage *storage,
    struct aws_byte_cursor *packet_cursor) {
    int result = AWS_OP_ERR;

    uint8_t property_type = 0;
    AWS_MQTT5_DECODE_U8(packet_cursor, &property_type, done);

    switch (property_type) {
        case AWS_MQTT5_PROPERTY_TYPE_SESSION_EXPIRY_INTERVAL:
            AWS_MQTT5_DECODE_U32_OPTIONAL(
                packet_cursor,
                &storage->session_expiry_interval_seconds,
                &storage->session_expiry_interval_seconds_ptr,
                done);
            break;

        case AWS_MQTT5_PROPERTY_TYPE_RECEIVE_MAXIMUM:
            AWS_MQTT5_DECODE_U16_OPTIONAL(
                packet_cursor, &storage->receive_maximum, &storage->receive_maximum_ptr, done);
            break;

        case AWS_MQTT5_PROPERTY_TYPE_MAXIMUM_PACKET_SIZE:
            AWS_MQTT5_DECODE_U32_OPTIONAL(
                packet_cursor, &storage->maximum_packet_size_bytes, &storage->maximum_packet_size_bytes_ptr, done);
            break;

        case AWS_MQTT5_PROPERTY_TYPE_TOPIC_ALIAS_MAXIMUM:
            AWS_MQTT5_DECODE_U16_OPTIONAL(
                packet_cursor, &storage->topic_alias_maximum, &storage->topic_alias_maximum_ptr, done);
            break;

        case AWS_MQTT5_PROPERTY_TYPE_REQUEST_RESPONSE_INFORMATION:
            AWS_MQTT5_DECODE_U8_OPTIONAL(
                packet_cursor,
                &storage->request_response_information,
                &storage->request_response_information_ptr,
                done);
            break;

        case AWS_MQTT5_PROPERTY_TYPE_REQUEST_PROBLEM_INFORMATION:
            AWS_MQTT5_DECODE_U8_OPTIONAL(
                packet_cursor, &storage->request_problem_information, &storage->request_problem_information_ptr, done);
            break;

        case AWS_MQTT5_PROPERTY_TYPE_AUTHENTICATION_METHOD:
            AWS_MQTT5_DECODE_LENGTH_PREFIXED_CURSOR_OPTIONAL(
                packet_cursor, &storage->authentication_method, &storage->authentication_method_ptr, done);
            break;

        case AWS_MQTT5_PROPERTY_TYPE_AUTHENTICATION_DATA:
            AWS_MQTT5_DECODE_LENGTH_PREFIXED_CURSOR_OPTIONAL(
                packet_cursor, &storage->authentication_data, &storage->authentication_data_ptr, done);
            break;

        case AWS_MQTT5_PROPERTY_TYPE_USER_PROPERTY:
            if (aws_mqtt5_decode_user_property(packet_cursor, &storage->user_properties)) {
                goto done;
            }
            break;

        default:
            goto done;
    }

    result = AWS_OP_SUCCESS;

done:

    if (result != AWS_OP_SUCCESS) {
        aws_raise_error(AWS_ERROR_MQTT5_DECODE_PROTOCOL_ERROR);
    }

    return result;
}

/* decode function for all will properties.  Movable to test-only code if we switched to a decoding function table */
static int s_read_will_property(
    struct aws_mqtt5_packet_connect_storage *connect_storage,
    struct aws_mqtt5_packet_publish_storage *will_storage,
    struct aws_byte_cursor *packet_cursor) {
    int result = AWS_OP_ERR;

    uint8_t property_type = 0;
    AWS_MQTT5_DECODE_U8(packet_cursor, &property_type, done);

    switch (property_type) {
        case AWS_MQTT5_PROPERTY_TYPE_WILL_DELAY_INTERVAL:
            AWS_MQTT5_DECODE_U32_OPTIONAL(
                packet_cursor,
                &connect_storage->will_delay_interval_seconds,
                &connect_storage->will_delay_interval_seconds_ptr,
                done);
            break;

        case AWS_MQTT5_PROPERTY_TYPE_PAYLOAD_FORMAT_INDICATOR:
            AWS_MQTT5_DECODE_U8_OPTIONAL(
                packet_cursor, &will_storage->payload_format, &will_storage->payload_format_ptr, done);
            break;

        case AWS_MQTT5_PROPERTY_TYPE_MESSAGE_EXPIRY_INTERVAL:
            AWS_MQTT5_DECODE_U32_OPTIONAL(
                packet_cursor,
                &will_storage->message_expiry_interval_seconds,
                &will_storage->message_expiry_interval_seconds_ptr,
                done);
            break;

        case AWS_MQTT5_PROPERTY_TYPE_CONTENT_TYPE:
            AWS_MQTT5_DECODE_LENGTH_PREFIXED_CURSOR_OPTIONAL(
                packet_cursor, &will_storage->content_type, &will_storage->content_type_ptr, done);
            break;

        case AWS_MQTT5_PROPERTY_TYPE_RESPONSE_TOPIC:
            AWS_MQTT5_DECODE_LENGTH_PREFIXED_CURSOR_OPTIONAL(
                packet_cursor, &will_storage->response_topic, &will_storage->response_topic_ptr, done);
            break;

        case AWS_MQTT5_PROPERTY_TYPE_CORRELATION_DATA:
            AWS_MQTT5_DECODE_LENGTH_PREFIXED_CURSOR_OPTIONAL(
                packet_cursor, &will_storage->correlation_data, &will_storage->correlation_data_ptr, done);
            break;

        case AWS_MQTT5_PROPERTY_TYPE_USER_PROPERTY:
            if (aws_mqtt5_decode_user_property(packet_cursor, &will_storage->user_properties)) {
                goto done;
            }
            break;

        default:
            goto done;
    }

    result = AWS_OP_SUCCESS;

done:

    if (result != AWS_OP_SUCCESS) {
        aws_raise_error(AWS_ERROR_MQTT5_DECODE_PROTOCOL_ERROR);
    }

    return result;
}

/*
 * Decodes a CONNECT packet whose data must be in the scratch buffer.
 * Could be moved to test-only if we used a function table for per-packet-type decoding.
 */
static int s_aws_mqtt5_decoder_decode_connect(struct aws_mqtt5_decoder *decoder) {
    struct aws_mqtt5_packet_connect_storage connect_storage;
    struct aws_mqtt5_packet_publish_storage publish_storage;
    int result = AWS_OP_ERR;
    bool has_will = false;

    if (aws_mqtt5_packet_connect_storage_init_from_external_storage(&connect_storage, decoder->allocator)) {
        return AWS_OP_ERR;
    }

    if (aws_mqtt5_packet_publish_storage_init_from_external_storage(&publish_storage, decoder->allocator)) {
        goto done;
    }

    uint8_t first_byte = decoder->packet_first_byte;
    /* CONNECT flags must be zero by protocol */
    if ((first_byte & 0x0F) != 0) {
        goto done;
    }

    struct aws_byte_cursor packet_cursor = decoder->packet_cursor;
    if (decoder->remaining_length != (uint32_t)packet_cursor.len) {
        goto done;
    }

    struct aws_byte_cursor protocol_cursor = aws_byte_cursor_advance(&packet_cursor, 7);
    if (!aws_byte_cursor_eq(&protocol_cursor, &g_aws_mqtt5_connect_protocol_cursor)) {
        goto done;
    }

    uint8_t connect_flags = 0;
    AWS_MQTT5_DECODE_U8(&packet_cursor, &connect_flags, done);

    connect_storage.clean_start = (connect_flags & AWS_MQTT5_CONNECT_FLAGS_CLEAN_START_BIT) != 0;

    AWS_MQTT5_DECODE_U16(&packet_cursor, &connect_storage.keep_alive_interval_seconds, done);

    uint32_t connect_property_length = 0;
    AWS_MQTT5_DECODE_VLI(&packet_cursor, &connect_property_length, done);
    if (connect_property_length > packet_cursor.len) {
        goto done;
    }

    struct aws_byte_cursor connect_property_cursor = aws_byte_cursor_advance(&packet_cursor, connect_property_length);
    while (connect_property_cursor.len > 0) {
        if (s_read_connect_property(&connect_storage, &connect_property_cursor)) {
            goto done;
        }
    }

    AWS_MQTT5_DECODE_LENGTH_PREFIXED_CURSOR(&packet_cursor, &connect_storage.client_id, done);

    has_will = (connect_flags & AWS_MQTT5_CONNECT_FLAGS_WILL_BIT) != 0;
    if (has_will) {
        uint32_t will_property_length = 0;
        AWS_MQTT5_DECODE_VLI(&packet_cursor, &will_property_length, done);
        if (will_property_length > packet_cursor.len) {
            goto done;
        }

        struct aws_byte_cursor will_property_cursor = aws_byte_cursor_advance(&packet_cursor, will_property_length);
        while (will_property_cursor.len > 0) {
            if (s_read_will_property(&connect_storage, &publish_storage, &will_property_cursor)) {
                goto done;
            }
        }

        AWS_MQTT5_DECODE_LENGTH_PREFIXED_CURSOR(&packet_cursor, &publish_storage.topic, done);
        AWS_MQTT5_DECODE_LENGTH_PREFIXED_CURSOR(&packet_cursor, &publish_storage.payload, done);

        /* apply will flags from the connect flags to the will's storage */
        publish_storage.qos = (enum aws_mqtt5_qos)(
            (connect_flags >> AWS_MQTT5_CONNECT_FLAGS_WILL_QOS_BIT_POSITION) &
            AWS_MQTT5_CONNECT_FLAGS_WILL_QOS_BIT_MASK);
        publish_storage.retain = (connect_flags & AWS_MQTT5_CONNECT_FLAGS_WILL_RETAIN_BIT) != 0;
    }

    if ((connect_flags & AWS_MQTT5_CONNECT_FLAGS_USER_NAME_BIT) != 0) {
        AWS_MQTT5_DECODE_LENGTH_PREFIXED_CURSOR_OPTIONAL(
            &packet_cursor, &connect_storage.username, &connect_storage.username_ptr, done);
    }

    if ((connect_flags & AWS_MQTT5_CONNECT_FLAGS_PASSWORD_BIT) != 0) {
        AWS_MQTT5_DECODE_LENGTH_PREFIXED_CURSOR_OPTIONAL(
            &packet_cursor, &connect_storage.password, &connect_storage.password_ptr, done);
    }

    if (packet_cursor.len == 0) {
        result = AWS_OP_SUCCESS;
    }

done:

    if (result == AWS_OP_SUCCESS) {
        if (decoder->options.on_packet_received != NULL) {
            aws_mqtt5_packet_connect_view_init_from_storage(&connect_storage.storage_view, &connect_storage);
            if (has_will) {
                aws_mqtt5_packet_publish_view_init_from_storage(&publish_storage.storage_view, &publish_storage);
                connect_storage.storage_view.will = &publish_storage.storage_view;
            }

            result = (*decoder->options.on_packet_received)(
                AWS_MQTT5_PT_CONNECT, &connect_storage.storage_view, decoder->options.callback_user_data);
        }
    } else {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_GENERAL,
            "(%p) aws_mqtt5_decoder - CONNECT decode failure",
            decoder->options.callback_user_data);
        aws_raise_error(AWS_ERROR_MQTT5_DECODE_PROTOCOL_ERROR);
    }

    aws_mqtt5_packet_publish_storage_clean_up(&publish_storage);
    aws_mqtt5_packet_connect_storage_clean_up(&connect_storage);

    return result;
}

void aws_mqtt5_decode_init_testing_function_table(struct aws_mqtt5_decoder_function_table *function_table) {
    *function_table = *g_aws_mqtt5_default_decoder_table;
    function_table->decoders_by_packet_type[AWS_MQTT5_PT_PINGREQ] = &s_aws_mqtt5_decoder_decode_pingreq;
    function_table->decoders_by_packet_type[AWS_MQTT5_PT_CONNECT] = &s_aws_mqtt5_decoder_decode_connect;
}

/*******************************************************************************************************************/

static int s_aws_mqtt5_mock_test_fixture_on_packet_received_fn(
    enum aws_mqtt5_packet_type type,
    void *packet_view,
    void *decoder_callback_user_data) {

    struct aws_mqtt5_mock_server_packet_record packet_record;
    AWS_ZERO_STRUCT(packet_record);

    struct aws_mqtt5_server_mock_connection_context *server_connection = decoder_callback_user_data;
    struct aws_mqtt5_client_mock_test_fixture *test_fixture = server_connection->test_fixture;

    switch (type) {
        case AWS_MQTT5_PT_CONNECT:
            packet_record.packet_storage =
                aws_mem_calloc(test_fixture->allocator, 1, sizeof(struct aws_mqtt5_packet_connect_storage));
            aws_mqtt5_packet_connect_storage_init(packet_record.packet_storage, test_fixture->allocator, packet_view);
            break;

        case AWS_MQTT5_PT_DISCONNECT:
            packet_record.packet_storage =
                aws_mem_calloc(test_fixture->allocator, 1, sizeof(struct aws_mqtt5_packet_disconnect_storage));
            aws_mqtt5_packet_disconnect_storage_init(
                packet_record.packet_storage, test_fixture->allocator, packet_view);
            break;

        case AWS_MQTT5_PT_SUBSCRIBE:
            packet_record.packet_storage =
                aws_mem_calloc(test_fixture->allocator, 1, sizeof(struct aws_mqtt5_packet_subscribe_storage));
            aws_mqtt5_packet_subscribe_storage_init(packet_record.packet_storage, test_fixture->allocator, packet_view);
            break;

        case AWS_MQTT5_PT_UNSUBSCRIBE:
            packet_record.packet_storage =
                aws_mem_calloc(test_fixture->allocator, 1, sizeof(struct aws_mqtt5_packet_unsubscribe_storage));
            aws_mqtt5_packet_unsubscribe_storage_init(
                packet_record.packet_storage, test_fixture->allocator, packet_view);
            break;

        case AWS_MQTT5_PT_PUBLISH:
            packet_record.packet_storage =
                aws_mem_calloc(test_fixture->allocator, 1, sizeof(struct aws_mqtt5_packet_publish_storage));
            aws_mqtt5_packet_publish_storage_init(packet_record.packet_storage, test_fixture->allocator, packet_view);
            break;

        case AWS_MQTT5_PT_PUBACK:
            /* TODO */
            break;

        default:
            break;
    }

    int result = AWS_OP_SUCCESS;
    on_server_packet_received_fn packet_handler = test_fixture->server_function_table->packet_handlers[type];
    if (packet_handler != NULL) {
        result = (*packet_handler)(packet_view, server_connection, test_fixture->server_packet_received_user_data);
    }

    packet_record.storage_allocator = test_fixture->allocator;
    packet_record.timestamp = (*test_fixture->client_vtable.get_current_time_fn)();
    packet_record.packet_type = type;

    aws_mutex_lock(&test_fixture->lock);
    aws_array_list_push_back(&test_fixture->server_received_packets, &packet_record);
    aws_mutex_unlock(&test_fixture->lock);
    aws_condition_variable_notify_all(&test_fixture->signal);

    return result;
}

#ifdef _WIN32
#    define LOCAL_SOCK_TEST_PATTERN "\\\\.\\pipe\\testsock%llu"
#else
#    define LOCAL_SOCK_TEST_PATTERN "testsock%llu.sock"
#endif

static int s_process_read_message(
    struct aws_channel_handler *handler,
    struct aws_channel_slot *slot,
    struct aws_io_message *message) {

    struct aws_mqtt5_server_mock_connection_context *server_connection = handler->impl;

    if (message->message_type != AWS_IO_MESSAGE_APPLICATION_DATA) {
        return AWS_OP_ERR;
    }

    struct aws_byte_cursor message_cursor = aws_byte_cursor_from_buf(&message->message_data);

    int result = aws_mqtt5_decoder_on_data_received(&server_connection->decoder, message_cursor);
    if (result != AWS_OP_SUCCESS) {
        aws_channel_shutdown(server_connection->channel, aws_last_error());
        return AWS_OP_SUCCESS;
    }

    aws_channel_slot_increment_read_window(slot, message->message_data.len);
    aws_mem_release(message->allocator, message);

    return AWS_OP_SUCCESS;
}

static int s_shutdown(
    struct aws_channel_handler *handler,
    struct aws_channel_slot *slot,
    enum aws_channel_direction dir,
    int error_code,
    bool free_scarce_resources_immediately) {

    (void)handler;

    return aws_channel_slot_on_handler_shutdown_complete(slot, dir, error_code, free_scarce_resources_immediately);
}

static size_t s_initial_window_size(struct aws_channel_handler *handler) {
    (void)handler;

    return SIZE_MAX;
}

static void s_destroy(struct aws_channel_handler *handler) {
    struct aws_mqtt5_server_mock_connection_context *server_connection = handler->impl;

    aws_mqtt5_decoder_clean_up(&server_connection->decoder);
    aws_mqtt5_encoder_clean_up(&server_connection->encoder);

    aws_mem_release(server_connection->allocator, server_connection);
}

static size_t s_message_overhead(struct aws_channel_handler *handler) {
    (void)handler;

    return 0;
}

static struct aws_channel_handler_vtable s_mqtt5_mock_server_channel_handler_vtable = {
    .process_read_message = &s_process_read_message,
    .process_write_message = NULL,
    .increment_read_window = NULL,
    .shutdown = &s_shutdown,
    .initial_window_size = &s_initial_window_size,
    .message_overhead = &s_message_overhead,
    .destroy = &s_destroy,
};

static void s_on_incoming_channel_setup_fn(
    struct aws_server_bootstrap *bootstrap,
    int error_code,
    struct aws_channel *channel,
    void *user_data) {
    (void)bootstrap;
    struct aws_mqtt5_client_mock_test_fixture *test_fixture = user_data;

    if (!error_code) {
        struct aws_channel_slot *test_handler_slot = aws_channel_slot_new(channel);
        aws_channel_slot_insert_end(channel, test_handler_slot);

        struct aws_mqtt5_server_mock_connection_context *server_connection =
            aws_mem_calloc(test_fixture->allocator, 1, sizeof(struct aws_mqtt5_server_mock_connection_context));
        server_connection->allocator = test_fixture->allocator;
        server_connection->channel = channel;
        server_connection->test_fixture = test_fixture;
        server_connection->slot = test_handler_slot;
        server_connection->handler.alloc = server_connection->allocator;
        server_connection->handler.vtable = &s_mqtt5_mock_server_channel_handler_vtable;
        server_connection->handler.impl = server_connection;

        aws_channel_slot_set_handler(server_connection->slot, &server_connection->handler);

        aws_mqtt5_encode_init_testing_function_table(&server_connection->encoding_table);

        struct aws_mqtt5_encoder_options encoder_options = {
            .client = NULL,
            .encoders = &server_connection->encoding_table,
        };

        aws_mqtt5_encoder_init(&server_connection->encoder, server_connection->allocator, &encoder_options);

        aws_mqtt5_decode_init_testing_function_table(&server_connection->decoding_table);

        struct aws_mqtt5_decoder_options decoder_options = {
            .callback_user_data = server_connection,
            .on_packet_received = s_aws_mqtt5_mock_test_fixture_on_packet_received_fn,
            .on_publish_payload_data = NULL, /* TODO */
            .decoder_table = &server_connection->decoding_table,
        };

        aws_mqtt5_decoder_init(&server_connection->decoder, server_connection->allocator, &decoder_options);
    }
}

static void s_on_incoming_channel_shutdown_fn(
    struct aws_server_bootstrap *bootstrap,
    int error_code,
    struct aws_channel *channel,
    void *user_data) {
    (void)bootstrap;
    (void)error_code;
    (void)channel;
    (void)user_data;
}

static void s_on_listener_destroy(struct aws_server_bootstrap *bootstrap, void *user_data) {
    (void)bootstrap;
    struct aws_mqtt5_client_mock_test_fixture *test_fixture = user_data;
    aws_mutex_lock(&test_fixture->lock);
    test_fixture->listener_destroyed = true;
    aws_mutex_unlock(&test_fixture->lock);
    aws_condition_variable_notify_one(&test_fixture->signal);
}

static bool s_is_listener_destroyed(void *arg) {
    struct aws_mqtt5_client_mock_test_fixture *test_fixture = arg;
    return test_fixture->listener_destroyed;
}

static void s_wait_on_listener_cleanup(struct aws_mqtt5_client_mock_test_fixture *test_fixture) {
    aws_mutex_lock(&test_fixture->lock);
    aws_condition_variable_wait_pred(&test_fixture->signal, &test_fixture->lock, s_is_listener_destroyed, test_fixture);
    aws_mutex_unlock(&test_fixture->lock);
}

void s_aws_mqtt5_test_fixture_lifecycle_event_handler(const struct aws_mqtt5_client_lifecycle_event *event) {
    struct aws_mqtt5_client_mock_test_fixture *test_fixture = event->user_data;

    struct aws_mqtt5_lifecycle_event_record *record =
        aws_mem_calloc(test_fixture->allocator, 1, sizeof(struct aws_mqtt5_lifecycle_event_record));
    record->allocator = test_fixture->allocator;
    aws_high_res_clock_get_ticks(&record->timestamp);
    record->event = *event;

    if (event->settings != NULL) {
        record->settings_storage = *event->settings;
        record->event.settings = &record->settings_storage;
    }

    if (event->disconnect_data != NULL) {
        aws_mqtt5_packet_disconnect_storage_init(
            &record->disconnect_storage, record->allocator, event->disconnect_data);
        record->event.disconnect_data = &record->disconnect_storage.storage_view;
    }

    if (event->connack_data != NULL) {
        aws_mqtt5_packet_connack_storage_init(&record->connack_storage, record->allocator, event->connack_data);
        record->event.connack_data = &record->connack_storage.storage_view;
    }

    aws_mutex_lock(&test_fixture->lock);
    aws_array_list_push_back(&test_fixture->lifecycle_events, &record);
    aws_mutex_unlock(&test_fixture->lock);
    aws_condition_variable_notify_all(&test_fixture->signal);

    aws_mqtt5_client_connection_event_callback_fn *event_callback = test_fixture->original_lifecycle_event_handler;
    if (event_callback != NULL) {
        struct aws_mqtt5_client_lifecycle_event event_copy = *event;
        event_copy.user_data = test_fixture->original_lifecycle_event_handler_user_data;

        (*event_callback)(&event_copy);
    }
}

void s_aws_mqtt5_test_fixture_state_changed_callback(
    struct aws_mqtt5_client *client,
    enum aws_mqtt5_client_state old_state,
    enum aws_mqtt5_client_state new_state,
    void *vtable_user_data) {

    struct aws_mqtt5_client_mock_test_fixture *test_fixture = vtable_user_data;

    aws_mutex_lock(&test_fixture->lock);
    aws_array_list_push_back(&test_fixture->client_states, &new_state);
    aws_mutex_unlock(&test_fixture->lock);
}

int aws_mqtt5_client_mock_test_fixture_init(
    struct aws_mqtt5_client_mock_test_fixture *test_fixture,
    struct aws_allocator *allocator,
    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options *options) {

    AWS_ZERO_STRUCT(*test_fixture);

    test_fixture->allocator = allocator;

    aws_mutex_init(&test_fixture->lock);
    aws_condition_variable_init(&test_fixture->signal);

    test_fixture->server_function_table = options->server_function_table;
    test_fixture->server_packet_received_user_data = options->server_packet_received_user_data;

    struct aws_socket_options socket_options = {
        .connect_timeout_ms = 100,
        .domain = AWS_SOCKET_LOCAL,
    };

    test_fixture->socket_options = socket_options;
    test_fixture->elg = aws_event_loop_group_new_default(allocator, 4, NULL);
    test_fixture->server_bootstrap = aws_server_bootstrap_new(allocator, test_fixture->elg);

    struct aws_host_resolver_default_options resolver_options = {
        .el_group = test_fixture->elg,
        .max_entries = 1,
    };
    test_fixture->host_resolver = aws_host_resolver_new_default(allocator, &resolver_options);

    struct aws_client_bootstrap_options bootstrap_options = {
        .event_loop_group = test_fixture->elg,
        .user_data = test_fixture,
        .host_resolver = test_fixture->host_resolver,
    };

    test_fixture->client_bootstrap = aws_client_bootstrap_new(allocator, &bootstrap_options);

    uint64_t timestamp = 0;
    ASSERT_SUCCESS(aws_sys_clock_get_ticks(&timestamp));

    snprintf(
        test_fixture->endpoint.address,
        sizeof(test_fixture->endpoint.address),
        LOCAL_SOCK_TEST_PATTERN,
        (long long unsigned)timestamp);

    struct aws_server_socket_channel_bootstrap_options server_bootstrap_options = {
        .bootstrap = test_fixture->server_bootstrap,
        .host_name = test_fixture->endpoint.address,
        .port = test_fixture->endpoint.port,
        .socket_options = &test_fixture->socket_options,
        .incoming_callback = s_on_incoming_channel_setup_fn,
        .shutdown_callback = s_on_incoming_channel_shutdown_fn,
        .destroy_callback = s_on_listener_destroy,
        .user_data = test_fixture,
    };
    test_fixture->listener = aws_server_bootstrap_new_socket_listener(&server_bootstrap_options);

    test_fixture->original_lifecycle_event_handler = options->client_options->lifecycle_event_handler;
    test_fixture->original_lifecycle_event_handler_user_data =
        options->client_options->lifecycle_event_handler_user_data;
    options->client_options->lifecycle_event_handler = &s_aws_mqtt5_test_fixture_lifecycle_event_handler;
    options->client_options->lifecycle_event_handler_user_data = test_fixture;

    options->client_options->host_name = aws_byte_cursor_from_c_str(test_fixture->endpoint.address);
    options->client_options->port = test_fixture->endpoint.port;
    options->client_options->socket_options = &test_fixture->socket_options;
    options->client_options->bootstrap = test_fixture->client_bootstrap;

    test_fixture->client = aws_mqtt5_client_new(allocator, options->client_options);

    test_fixture->client_vtable = *aws_mqtt5_client_get_default_vtable();
    test_fixture->client_vtable.on_client_state_change_callback_fn = s_aws_mqtt5_test_fixture_state_changed_callback;
    test_fixture->client_vtable.vtable_user_data = test_fixture;

    aws_mqtt5_client_set_vtable(test_fixture->client, &test_fixture->client_vtable);

    aws_array_list_init_dynamic(
        &test_fixture->server_received_packets, allocator, 10, sizeof(struct aws_mqtt5_mock_server_packet_record));

    aws_array_list_init_dynamic(
        &test_fixture->lifecycle_events, allocator, 10, sizeof(struct aws_mqtt5_lifecycle_event_record *));

    aws_array_list_init_dynamic(&test_fixture->client_states, allocator, 10, sizeof(enum aws_mqtt5_client_state));

    return AWS_OP_SUCCESS;
}

static void s_destroy_packet_storage(
    void *packet_storage,
    struct aws_allocator *allocator,
    enum aws_mqtt5_packet_type packet_type) {
    switch (packet_type) {
        case AWS_MQTT5_PT_CONNECT:
            aws_mqtt5_packet_connect_storage_clean_up(packet_storage);
            break;

        case AWS_MQTT5_PT_DISCONNECT:
            aws_mqtt5_packet_disconnect_storage_clean_up(packet_storage);
            break;

        case AWS_MQTT5_PT_SUBSCRIBE:
            aws_mqtt5_packet_subscribe_storage_clean_up(packet_storage);
            break;

        case AWS_MQTT5_PT_UNSUBSCRIBE:
            aws_mqtt5_packet_unsubscribe_storage_clean_up(packet_storage);
            break;

        case AWS_MQTT5_PT_PUBLISH:
            aws_mqtt5_packet_publish_storage_clean_up(packet_storage);
            break;

        case AWS_MQTT5_PT_PUBACK:
            /* TODO */
            break;

        case AWS_MQTT5_PT_PINGREQ:
            AWS_FATAL_ASSERT(packet_storage == NULL);
            break;

        default:
            AWS_FATAL_ASSERT(false);
    }

    if (packet_storage != NULL) {
        aws_mem_release(allocator, packet_storage);
    }
}

static void s_destroy_lifecycle_event_storage(struct aws_mqtt5_lifecycle_event_record *event) {
    aws_mqtt5_packet_connack_storage_clean_up(&event->connack_storage);
    aws_mqtt5_packet_disconnect_storage_clean_up(&event->disconnect_storage);

    aws_mem_release(event->allocator, event);
}

void aws_mqtt5_client_mock_test_fixture_clean_up(struct aws_mqtt5_client_mock_test_fixture *test_fixture) {
    aws_mqtt5_client_release(test_fixture->client);
    aws_client_bootstrap_release(test_fixture->client_bootstrap);
    aws_host_resolver_release(test_fixture->host_resolver);
    aws_server_bootstrap_destroy_socket_listener(test_fixture->server_bootstrap, test_fixture->listener);

    s_wait_on_listener_cleanup(test_fixture);

    aws_server_bootstrap_release(test_fixture->server_bootstrap);
    aws_event_loop_group_release(test_fixture->elg);

    aws_thread_join_all_managed();

    for (size_t i = 0; i < aws_array_list_length(&test_fixture->server_received_packets); ++i) {
        struct aws_mqtt5_mock_server_packet_record *packet = NULL;
        aws_array_list_get_at_ptr(&test_fixture->server_received_packets, (void **)&packet, i);

        s_destroy_packet_storage(packet->packet_storage, packet->storage_allocator, packet->packet_type);
    }

    aws_array_list_clean_up(&test_fixture->server_received_packets);

    for (size_t i = 0; i < aws_array_list_length(&test_fixture->lifecycle_events); ++i) {
        struct aws_mqtt5_lifecycle_event_record *event = NULL;
        aws_array_list_get_at(&test_fixture->lifecycle_events, &event, i);

        s_destroy_lifecycle_event_storage(event);
    }

    aws_array_list_clean_up(&test_fixture->lifecycle_events);
    aws_array_list_clean_up(&test_fixture->client_states);

    aws_mutex_clean_up(&test_fixture->lock);
    aws_condition_variable_clean_up(&test_fixture->signal);
}
