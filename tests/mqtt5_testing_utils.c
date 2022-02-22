/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "mqtt5_testing_utils.h"

#include <aws/mqtt/private/v5/mqtt5_decoder.h>
#include <aws/mqtt/private/v5/mqtt5_encoder.h>
#include <aws/mqtt/private/v5/mqtt5_utils.h>

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
