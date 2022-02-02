/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/private/v5/mqtt5_decoder.h>

#include <aws/mqtt/private/v5/mqtt5_utils.h>

#define AWS_MQTT5_DECODER_BUFFER_START_SIZE 2048

static void s_reset_decoder_for_new_packet(struct aws_mqtt5_decoder *decoder) {
    aws_byte_buf_reset(&decoder->scratch_space, false);
    decoder->current_step_scratch_offset = 0;

    decoder->packet_type = AWS_MQTT5_PT_RESERVED;
    decoder->remaining_length = 0;
    decoder->total_length = 0;
}

int aws_mqtt5_decoder_init(
    struct aws_mqtt5_decoder *decoder,
    struct aws_allocator *allocator,
    struct aws_mqtt5_decoder_options *options) {
    AWS_ZERO_STRUCT(*decoder);

    decoder->options = *options;
    decoder->allocator = allocator;

    decoder->state = AWS_MQTT5_DS_READ_PACKET_TYPE;

    if (aws_byte_buf_init(&decoder->scratch_space, allocator, AWS_MQTT5_DECODER_BUFFER_START_SIZE)) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

void aws_mqtt5_decoder_reset(struct aws_mqtt5_decoder *decoder) {
    s_reset_decoder_for_new_packet(decoder);
}

void aws_mqtt5_decoder_clean_up(struct aws_mqtt5_decoder *decoder) {
    aws_byte_buf_clean_up(&decoder->scratch_space);
}

static void s_enter_state(struct aws_mqtt5_decoder *decoder, enum aws_mqtt5_decoder_state state) {
    decoder->state = state;

    if (state == AWS_MQTT5_DS_READ_PACKET_TYPE) {
        s_reset_decoder_for_new_packet(decoder);
    }

    decoder->current_step_scratch_offset = decoder->scratch_space.len;
}

static bool s_is_decodable_packet_type(enum aws_mqtt5_packet_type type) {
    switch (type) {
        case AWS_MQTT5_PT_CONNECT:
        case AWS_MQTT5_PT_CONNACK:
        case AWS_MQTT5_PT_PUBLISH:
        case AWS_MQTT5_PT_PUBACK:
        case AWS_MQTT5_PT_SUBSCRIBE:
        case AWS_MQTT5_PT_SUBACK:
        case AWS_MQTT5_PT_UNSUBSCRIBE:
        case AWS_MQTT5_PT_UNSUBACK:
        case AWS_MQTT5_PT_PINGREQ:
        case AWS_MQTT5_PT_PINGRESP:
        case AWS_MQTT5_PT_DISCONNECT:
            return true;

        default:
            return false;
    }
}

/*
 * "streaming" step of one byte.  Every mqtt packet has a first byte that, amongst other things, determines the
 * packet type
 */
static int s_aws_mqtt5_decoder_read_packet_type_on_data(
    struct aws_mqtt5_decoder *decoder,
    struct aws_byte_cursor *data) {

    if (data->len == 0) {
        return AWS_MQTT5_DRT_MORE_DATA;
    }

    uint8_t byte = *data->ptr;
    aws_byte_cursor_advance(data, 1);
    aws_byte_buf_append_byte_dynamic(&decoder->scratch_space, byte);

    enum aws_mqtt5_packet_type packet_type = (byte >> 4);

    if (!s_is_decodable_packet_type(packet_type)) {
        aws_raise_error(AWS_ERROR_MQTT5_DECODE_INVALID_PACKET_TYPE);
        return AWS_MQTT5_DRT_ERROR;
    }

    decoder->packet_type = packet_type;

    s_enter_state(decoder, AWS_MQTT5_DS_READ_REMAINING_LENGTH);

    return AWS_MQTT5_DRT_SUCCESS;
}

/* non-streaming variable length integer decode */
enum aws_mqtt5_decode_result_type aws_mqtt5_decode_vli(struct aws_byte_cursor *cursor, uint32_t *dest) {
    uint32_t value = 0;
    bool more_data = false;
    size_t bytes_used = 0;

    uint32_t shift = 0;

    for (; bytes_used < 4; ++bytes_used) {
        if (bytes_used >= cursor->len) {
            return AWS_MQTT5_DRT_MORE_DATA;
        }

        uint8_t byte = *(cursor->ptr + bytes_used);
        value |= ((byte & 0x7F) << shift);
        shift += 7;

        more_data = (byte & 0x80) != 0;
        if (!more_data) {
            break;
        }
    }

    if (more_data) {
        /* A variable length integer with the 4th byte high bit set is not valid */
        return AWS_MQTT5_DRT_ERROR;
    }

    aws_byte_cursor_advance(cursor, bytes_used + 1);
    *dest = value;

    return AWS_MQTT5_DRT_SUCCESS;
}

/* "streaming" variable length integer decode */
static enum aws_mqtt5_decode_result_type s_aws_mqtt5_decoder_read_vli_on_data(
    struct aws_mqtt5_decoder *decoder,
    uint32_t *vli_dest,
    struct aws_byte_cursor *data) {

    enum aws_mqtt5_decode_result_type decode_vli_result = AWS_MQTT5_DRT_MORE_DATA;

    /* try to decode the vli integer one byte at a time */
    while (data->len > 0 && decode_vli_result == AWS_MQTT5_DRT_MORE_DATA) {
        /* append a single byte to the scratch buffer */
        struct aws_byte_cursor byte_cursor = aws_byte_cursor_advance(data, 1);
        aws_byte_buf_append_dynamic(&decoder->scratch_space, &byte_cursor);

        /* now try and decode a vli integer based on the range implied by the offset into the buffer */
        struct aws_byte_cursor vli_cursor = {
            .ptr = decoder->scratch_space.buffer + decoder->current_step_scratch_offset,
            .len = decoder->scratch_space.len - decoder->current_step_scratch_offset,
        };

        decode_vli_result = aws_mqtt5_decode_vli(&vli_cursor, vli_dest);
    }

    return decode_vli_result;
}

/* attempts to read the variable length integer that is always the second piece of data in an mqtt packet */
static enum aws_mqtt5_decode_result_type s_aws_mqtt5_decoder_read_remaining_length_on_data(
    struct aws_mqtt5_decoder *decoder,
    struct aws_byte_cursor *data) {

    enum aws_mqtt5_decode_result_type result =
        s_aws_mqtt5_decoder_read_vli_on_data(decoder, &decoder->remaining_length, data);

    if (result == AWS_MQTT5_DRT_ERROR) {
        aws_raise_error(AWS_ERROR_MQTT5_DECODE_INVALID_VARIABLE_LENGTH_INTEGER);
    }

    if (result != AWS_MQTT5_DRT_SUCCESS) {
        return result;
    }

    decoder->total_length = decoder->remaining_length + decoder->scratch_space.len;

    s_enter_state(decoder, AWS_MQTT5_DS_READ_PACKET);

    return AWS_MQTT5_DRT_SUCCESS;
}

/* non-streaming decode of a user property; failure implies connection termination */
int aws_mqtt5_decode_user_property(
    struct aws_byte_cursor *packet_cursor,
    struct aws_mqtt5_user_property_set *properties) {
    struct aws_mqtt5_user_property property;

    AWS_MQTT5_DECODE_LENGTH_PREFIXED_CURSOR(packet_cursor, &property.name, error);
    AWS_MQTT5_DECODE_LENGTH_PREFIXED_CURSOR(packet_cursor, &property.value, error);

    if (aws_array_list_push_back(&properties->properties, &property)) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;

error:

    return AWS_OP_ERR;
}

/* decode function for all CONNACK properties */
static int s_read_connack_property(
    struct aws_mqtt5_packet_connack_storage *storage,
    struct aws_byte_cursor *packet_cursor) {
    int result = AWS_OP_ERR;

    uint8_t property_type = 0;
    AWS_MQTT5_DECODE_U8(packet_cursor, &property_type, done);

    switch (property_type) {
        case AWS_MQTT5_PROPERTY_TYPE_SESSION_EXPIRY_INTERVAL:
            AWS_MQTT5_DECODE_U32_OPTIONAL(
                packet_cursor, &storage->session_expiry_interval, &storage->session_expiry_interval_ptr, done);
            break;

        case AWS_MQTT5_PROPERTY_TYPE_RECEIVE_MAXIMUM:
            AWS_MQTT5_DECODE_U16_OPTIONAL(
                packet_cursor, &storage->receive_maximum, &storage->receive_maximum_ptr, done);
            break;

        case AWS_MQTT5_PROPERTY_TYPE_MAXIMUM_QOS:
            AWS_MQTT5_DECODE_U8_OPTIONAL(packet_cursor, &storage->maximum_qos, &storage->maximum_qos_ptr, done);
            break;

        case AWS_MQTT5_PROPERTY_TYPE_RETAIN_AVAILABLE:
            AWS_MQTT5_DECODE_U8_OPTIONAL(
                packet_cursor, &storage->retain_available, &storage->retain_available_ptr, done);
            break;

        case AWS_MQTT5_PROPERTY_TYPE_MAXIMUM_PACKET_SIZE:
            AWS_MQTT5_DECODE_U32_OPTIONAL(
                packet_cursor, &storage->maximum_packet_size, &storage->maximum_packet_size_ptr, done);
            break;

        case AWS_MQTT5_PROPERTY_TYPE_ASSIGNED_CLIENT_IDENTIFIER:
            AWS_MQTT5_DECODE_LENGTH_PREFIXED_CURSOR_OPTIONAL(
                packet_cursor, &storage->assigned_client_identifier, &storage->assigned_client_identifier_ptr, done);
            break;

        case AWS_MQTT5_PROPERTY_TYPE_TOPIC_ALIAS_MAXIMUM:
            AWS_MQTT5_DECODE_U16_OPTIONAL(
                packet_cursor, &storage->topic_alias_maximum, &storage->topic_alias_maximum_ptr, done);
            break;

        case AWS_MQTT5_PROPERTY_TYPE_REASON_STRING:
            AWS_MQTT5_DECODE_LENGTH_PREFIXED_CURSOR_OPTIONAL(
                packet_cursor, &storage->reason_string, &storage->reason_string_ptr, done);
            break;

        case AWS_MQTT5_PROPERTY_TYPE_WILDCARD_SUBSCRIPTIONS_AVAILABLE:
            AWS_MQTT5_DECODE_U8_OPTIONAL(
                packet_cursor,
                &storage->wildcard_subscriptions_available,
                &storage->wildcard_subscriptions_available_ptr,
                done);
            break;

        case AWS_MQTT5_PROPERTY_TYPE_SUBSCRIPTION_IDENTIFIERS_AVAILABLE:
            AWS_MQTT5_DECODE_U8_OPTIONAL(
                packet_cursor,
                &storage->subscription_identifiers_available,
                &storage->subscription_identifiers_available_ptr,
                done);
            break;

        case AWS_MQTT5_PROPERTY_TYPE_SHARED_SUBSCRIPTIONS_AVAILABLE:
            AWS_MQTT5_DECODE_U8_OPTIONAL(
                packet_cursor,
                &storage->shared_subscriptions_available,
                &storage->shared_subscriptions_available_ptr,
                done);
            break;

        case AWS_MQTT5_PROPERTY_TYPE_SERVER_KEEP_ALIVE:
            AWS_MQTT5_DECODE_U16_OPTIONAL(
                packet_cursor, &storage->server_keep_alive, &storage->server_keep_alive_ptr, done);
            break;

        case AWS_MQTT5_PROPERTY_TYPE_RESPONSE_INFORMATION:
            AWS_MQTT5_DECODE_LENGTH_PREFIXED_CURSOR_OPTIONAL(
                packet_cursor, &storage->response_information, &storage->response_information_ptr, done);
            break;

        case AWS_MQTT5_PROPERTY_TYPE_SERVER_REFERENCE:
            AWS_MQTT5_DECODE_LENGTH_PREFIXED_CURSOR_OPTIONAL(
                packet_cursor, &storage->server_reference, &storage->server_reference_ptr, done);
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

    return result;
}

/* decodes a CONNACK packet whose data must be in the scratch buffer */
static int s_aws_mqtt5_decoder_decode_connack_from_scratch_buffer(struct aws_mqtt5_decoder *decoder) {
    struct aws_mqtt5_packet_connack_storage storage;
    if (aws_mqtt5_packet_connack_storage_init_from_external_storage(&storage, decoder->allocator)) {
        return AWS_OP_ERR;
    }

    int result = AWS_OP_ERR;
    struct aws_byte_cursor packet_cursor = aws_byte_cursor_from_buf(&decoder->scratch_space);

    uint8_t first_byte = 0;
    AWS_MQTT5_DECODE_U8(&packet_cursor, &first_byte, done);

    /* CONNACK flags must be zero by protocol */
    if ((first_byte & 0x0F) != 0) {
        aws_raise_error(AWS_ERROR_MQTT5_DECODE_PROTOCOL_ERROR);
        goto done;
    }

    uint32_t remaining_length = 0;
    AWS_MQTT5_DECODE_VLI(&packet_cursor, &remaining_length, done);
    if (remaining_length != decoder->remaining_length || remaining_length != (uint32_t)packet_cursor.len) {
        aws_raise_error(AWS_ERROR_MQTT5_DECODE_PROTOCOL_ERROR);
        goto done;
    }

    uint8_t connect_flags = 0;
    AWS_MQTT5_DECODE_U8(&packet_cursor, &connect_flags, done);

    /* everything but the 0-bit must be 0 */
    if ((connect_flags & 0xFE) != 0) {
        aws_raise_error(AWS_ERROR_MQTT5_DECODE_PROTOCOL_ERROR);
        goto done;
    }

    storage.session_present = (connect_flags & 0x01) != 0;

    uint8_t reason_code = 0;
    AWS_MQTT5_DECODE_U8(&packet_cursor, &reason_code, done);
    storage.reason_code = reason_code;

    uint32_t property_length = 0;
    AWS_MQTT5_DECODE_VLI(&packet_cursor, &property_length, done);
    if (property_length != (uint32_t)packet_cursor.len) {
        aws_raise_error(AWS_ERROR_MQTT5_DECODE_PROTOCOL_ERROR);
        goto done;
    }

    while (packet_cursor.len > 0) {
        if (s_read_connack_property(&storage, &packet_cursor)) {
            goto done;
        }
    }

    result = AWS_OP_SUCCESS;

done:

    if (result == AWS_OP_SUCCESS) {
        if (decoder->options.on_packet_received != NULL) {
            aws_mqtt5_packet_connack_view_init_from_storage(&storage.storage_view, &storage);
            result = (*decoder->options.on_packet_received)(
                AWS_MQTT5_PT_CONNACK, &storage.storage_view, decoder->options.callback_user_data);
        }
    }

    aws_mqtt5_packet_connack_storage_clean_up(&storage);

    return result;
}

/* decode function for all DISCONNECT properties */
static int s_read_disconnect_property(
    struct aws_mqtt5_packet_disconnect_storage *storage,
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

        case AWS_MQTT5_PROPERTY_TYPE_SERVER_REFERENCE:
            AWS_MQTT5_DECODE_LENGTH_PREFIXED_CURSOR_OPTIONAL(
                packet_cursor, &storage->server_reference, &storage->server_reference_ptr, done);
            break;

        case AWS_MQTT5_PROPERTY_TYPE_REASON_STRING:
            AWS_MQTT5_DECODE_LENGTH_PREFIXED_CURSOR_OPTIONAL(
                packet_cursor, &storage->reason_string, &storage->reason_string_ptr, done);
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

    return result;
}

/* decodes a DISCONNECT packet whose data must be in the scratch buffer */
static int s_aws_mqtt5_decoder_decode_disconnect_from_scratch_buffer(struct aws_mqtt5_decoder *decoder) {
    struct aws_mqtt5_packet_disconnect_storage storage;
    if (aws_mqtt5_packet_disconnect_storage_init_from_external_storage(&storage, decoder->allocator)) {
        return AWS_OP_ERR;
    }

    int result = AWS_OP_ERR;
    struct aws_byte_cursor packet_cursor = aws_byte_cursor_from_buf(&decoder->scratch_space);

    uint8_t first_byte = 0;
    AWS_MQTT5_DECODE_U8(&packet_cursor, &first_byte, done);

    /* DISCONNECT flags must be zero by protocol */
    if ((first_byte & 0x0F) != 0) {
        aws_raise_error(AWS_ERROR_MQTT5_DECODE_PROTOCOL_ERROR);
        goto done;
    }

    uint32_t remaining_length = 0;
    AWS_MQTT5_DECODE_VLI(&packet_cursor, &remaining_length, done);
    if (remaining_length != decoder->remaining_length || remaining_length != (uint32_t)packet_cursor.len) {
        aws_raise_error(AWS_ERROR_MQTT5_DECODE_PROTOCOL_ERROR);
        goto done;
    }

    if (remaining_length > 0) {
        uint8_t reason_code = 0;
        AWS_MQTT5_DECODE_U8(&packet_cursor, &reason_code, done);
        storage.reason_code = reason_code;
        if (packet_cursor.len == 0) {
            result = AWS_OP_SUCCESS;
            goto done;
        }

        uint32_t property_length = 0;
        AWS_MQTT5_DECODE_VLI(&packet_cursor, &property_length, done);
        if (property_length != (uint32_t)packet_cursor.len) {
            aws_raise_error(AWS_ERROR_MQTT5_DECODE_PROTOCOL_ERROR);
            goto done;
        }

        while (packet_cursor.len > 0) {
            if (s_read_disconnect_property(&storage, &packet_cursor)) {
                goto done;
            }
        }
    }

    result = AWS_OP_SUCCESS;

done:

    if (result == AWS_OP_SUCCESS) {
        if (decoder->options.on_packet_received != NULL) {
            aws_mqtt5_packet_disconnect_view_init_from_storage(&storage.storage_view, &storage);
            result = (*decoder->options.on_packet_received)(
                AWS_MQTT5_PT_DISCONNECT, &storage.storage_view, decoder->options.callback_user_data);
        }
    }

    aws_mqtt5_packet_disconnect_storage_clean_up(&storage);

    return result;
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

    return result;
}

/*
 * Decodes a CONNECT packet whose data must be in the scratch buffer.
 * Could be moved to test-only if we used a function table for per-packet-type decoding.
 */
static int s_aws_mqtt5_decoder_decode_connect_from_scratch_buffer(struct aws_mqtt5_decoder *decoder) {
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

    struct aws_byte_cursor packet_cursor = aws_byte_cursor_from_buf(&decoder->scratch_space);
    uint8_t first_byte = 0;
    AWS_MQTT5_DECODE_U8(&packet_cursor, &first_byte, done);

    /* CONNECT flags must be zero by protocol */
    if ((first_byte & 0x0F) != 0) {
        aws_raise_error(AWS_ERROR_MQTT5_DECODE_PROTOCOL_ERROR);
        goto done;
    }

    uint32_t remaining_length = 0;
    AWS_MQTT5_DECODE_VLI(&packet_cursor, &remaining_length, done);
    if (remaining_length != decoder->remaining_length || remaining_length != (uint32_t)packet_cursor.len) {
        aws_raise_error(AWS_ERROR_MQTT5_DECODE_PROTOCOL_ERROR);
        goto done;
    }

    struct aws_byte_cursor protocol_cursor = aws_byte_cursor_advance(&packet_cursor, 7);
    if (!aws_byte_cursor_eq(&protocol_cursor, &g_aws_mqtt5_connect_protocol_cursor)) {
        aws_raise_error(AWS_ERROR_MQTT5_DECODE_PROTOCOL_ERROR);
        goto done;
    }

    uint8_t connect_flags = 0;
    AWS_MQTT5_DECODE_U8(&packet_cursor, &connect_flags, done);

    connect_storage.clean_start = (connect_flags & AWS_MQTT5_CONNECT_FLAGS_CLEAN_START_BIT) != 0;

    AWS_MQTT5_DECODE_U16(&packet_cursor, &connect_storage.keep_alive_interval_seconds, done);

    uint32_t connect_property_length = 0;
    AWS_MQTT5_DECODE_VLI(&packet_cursor, &connect_property_length, done);
    if (connect_property_length > packet_cursor.len) {
        aws_raise_error(AWS_ERROR_MQTT5_DECODE_PROTOCOL_ERROR);
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
            aws_raise_error(AWS_ERROR_MQTT5_DECODE_PROTOCOL_ERROR);
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
    }

    aws_mqtt5_packet_publish_storage_clean_up(&publish_storage);
    aws_mqtt5_packet_connect_storage_clean_up(&connect_storage);

    return result;
}

/*
 * TODO: consider making an array of function pointers indexed by packet type.  Then the server-decode-only
 * packets that we support (for testing purposes) could be moved into test-only in the same way that encoding
 * server-encode-only functionality can be moved there already.
 */
static int s_aws_mqtt5_decoder_decode_packet_from_scratch_buffer(struct aws_mqtt5_decoder *decoder) {
    switch (decoder->packet_type) {
        case AWS_MQTT5_PT_PINGREQ:
        case AWS_MQTT5_PT_PINGRESP:
            /* TODO: validate flags */
            if (decoder->remaining_length != 0) {
                return aws_raise_error(AWS_ERROR_INVALID_STATE);
            }

            if (decoder->options.on_packet_received != NULL) {
                (*decoder->options.on_packet_received)(decoder->packet_type, NULL, decoder->options.callback_user_data);
            }

            return AWS_OP_SUCCESS;

        case AWS_MQTT5_PT_CONNACK:
            return s_aws_mqtt5_decoder_decode_connack_from_scratch_buffer(decoder);

        case AWS_MQTT5_PT_DISCONNECT:
            return s_aws_mqtt5_decoder_decode_disconnect_from_scratch_buffer(decoder);

        case AWS_MQTT5_PT_CONNECT:
            return s_aws_mqtt5_decoder_decode_connect_from_scratch_buffer(decoder);

        default:
            return aws_raise_error(AWS_ERROR_INVALID_STATE);
    }
}

/*
 * (Streaming) Given a packet type and a variable length integer specifying the packet length, this state reads the
 * entire packet, and only that packet, into the scratch buffer
 */
static enum aws_mqtt5_decode_result_type s_aws_mqtt5_decoder_read_packet_on_data(
    struct aws_mqtt5_decoder *decoder,
    struct aws_byte_cursor *data) {

    size_t unread_length = decoder->total_length - decoder->scratch_space.len;
    size_t copy_length = aws_min_size(unread_length, data->len);

    struct aws_byte_cursor copy_cursor = {
        .ptr = data->ptr,
        .len = copy_length,
    };

    if (aws_byte_buf_append_dynamic(&decoder->scratch_space, &copy_cursor)) {
        return AWS_MQTT5_DRT_ERROR;
    }

    aws_byte_cursor_advance(data, copy_length);

    if (copy_length < unread_length) {
        return AWS_MQTT5_DRT_MORE_DATA;
    }

    if (s_aws_mqtt5_decoder_decode_packet_from_scratch_buffer(decoder)) {
        return AWS_MQTT5_DRT_ERROR;
    }

    s_enter_state(decoder, AWS_MQTT5_DS_READ_PACKET_TYPE);

    return AWS_MQTT5_DRT_SUCCESS;
}

/* top-level entry function for all new data received from the remote mqtt endpoint */
int aws_mqtt5_decoder_on_data_received(struct aws_mqtt5_decoder *decoder, struct aws_byte_cursor data) {
    enum aws_mqtt5_decode_result_type result = AWS_MQTT5_DRT_SUCCESS;
    while (result == AWS_MQTT5_DRT_SUCCESS) {

        switch (decoder->state) {
            case AWS_MQTT5_DS_READ_PACKET_TYPE:
                result = s_aws_mqtt5_decoder_read_packet_type_on_data(decoder, &data);
                break;

            case AWS_MQTT5_DS_READ_REMAINING_LENGTH:
                result = s_aws_mqtt5_decoder_read_remaining_length_on_data(decoder, &data);
                break;

            case AWS_MQTT5_DS_READ_PACKET:
                result = s_aws_mqtt5_decoder_read_packet_on_data(decoder, &data);
                break;

            default:
                result = aws_raise_error(AWS_ERROR_INVALID_STATE);
                break;
        }
    }

    if (result == AWS_MQTT5_DRT_ERROR) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}
