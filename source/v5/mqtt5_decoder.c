/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/private/v5/mqtt5_decoder.h>

#include <aws/mqtt/private/v5/mqtt5_utils.h>

#define AWS_MQTT5_DECODER_BUFFER_START_SIZE 2048

static void s_reset_decoder_for_new_packet(struct aws_mqtt5_decoder *decoder) {
    aws_byte_buf_reset(&decoder->scratch_space, false);

    decoder->packet_first_byte = 0;
    decoder->remaining_length = 0;
    AWS_ZERO_STRUCT(decoder->packet_cursor);
}

static void s_enter_state(struct aws_mqtt5_decoder *decoder, enum aws_mqtt5_decoder_state state) {
    decoder->state = state;

    if (state == AWS_MQTT5_DS_READ_PACKET_TYPE) {
        s_reset_decoder_for_new_packet(decoder);
    } else {
        aws_byte_buf_reset(&decoder->scratch_space, false);
    }
}

static bool s_is_decodable_packet_type(struct aws_mqtt5_decoder *decoder, enum aws_mqtt5_packet_type type) {
    return type < AWS_ARRAY_SIZE(decoder->options.decoder_table->decoders_by_packet_type) &&
           decoder->options.decoder_table->decoders_by_packet_type[type] != NULL;
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

    if (!s_is_decodable_packet_type(decoder, packet_type)) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_GENERAL,
            "(%p) aws_mqtt5_decoder - unsupported or illegal packet type value: %d",
            decoder->options.callback_user_data,
            (int)packet_type);
        return AWS_MQTT5_DRT_ERROR;
    }

    decoder->packet_first_byte = byte;

    s_enter_state(decoder, AWS_MQTT5_DS_READ_REMAINING_LENGTH);

    return AWS_MQTT5_DRT_SUCCESS;
}

/*
 * non-streaming variable length integer decode.  cursor is updated only if the value was successfully read
 */
enum aws_mqtt5_decode_result_type aws_mqtt5_decode_vli(struct aws_byte_cursor *cursor, uint32_t *dest) {
    uint32_t value = 0;
    bool more_data = false;
    size_t bytes_used = 0;

    uint32_t shift = 0;

    struct aws_byte_cursor cursor_copy = *cursor;
    for (; bytes_used < 4; ++bytes_used) {
        uint8_t byte = 0;
        if (!aws_byte_cursor_read_u8(&cursor_copy, &byte)) {
            return AWS_MQTT5_DRT_MORE_DATA;
        }

        value |= ((byte & 0x7F) << shift);
        shift += 7;

        more_data = (byte & 0x80) != 0;
        if (!more_data) {
            break;
        }
    }

    if (more_data) {
        /* A variable length integer with the 4th byte high bit set is not valid */
        AWS_LOGF_ERROR(AWS_LS_MQTT5_GENERAL, "(static) aws_mqtt5_decoder - illegal variable length integer encoding");
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
            .ptr = decoder->scratch_space.buffer,
            .len = decoder->scratch_space.len,
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
    if (result != AWS_MQTT5_DRT_SUCCESS) {
        return result;
    }

    /* TODO: branch state based on PUBLISH packet vs non-PUBLISH */

    s_enter_state(decoder, AWS_MQTT5_DS_READ_NON_PUBLISH_PACKET);

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

    if (result != AWS_OP_SUCCESS) {
        aws_raise_error(AWS_ERROR_MQTT5_DECODE_PROTOCOL_ERROR);
    }

    return result;
}

/* decodes a CONNACK packet whose data must be in the scratch buffer */
static int s_aws_mqtt5_decoder_decode_connack(struct aws_mqtt5_decoder *decoder) {
    struct aws_mqtt5_packet_connack_storage storage;
    if (aws_mqtt5_packet_connack_storage_init_from_external_storage(&storage, decoder->allocator)) {
        return AWS_OP_ERR;
    }

    int result = AWS_OP_ERR;

    uint8_t first_byte = decoder->packet_first_byte;
    /* CONNACK flags must be zero by protocol */
    if ((first_byte & 0x0F) != 0) {
        goto done;
    }

    struct aws_byte_cursor packet_cursor = decoder->packet_cursor;
    uint32_t remaining_length = decoder->remaining_length;
    if (remaining_length != (uint32_t)packet_cursor.len) {
        goto done;
    }

    uint8_t connect_flags = 0;
    AWS_MQTT5_DECODE_U8(&packet_cursor, &connect_flags, done);

    /* everything but the 0-bit must be 0 */
    if ((connect_flags & 0xFE) != 0) {
        goto done;
    }

    storage.session_present = (connect_flags & 0x01) != 0;

    uint8_t reason_code = 0;
    AWS_MQTT5_DECODE_U8(&packet_cursor, &reason_code, done);
    storage.reason_code = reason_code;

    uint32_t property_length = 0;
    AWS_MQTT5_DECODE_VLI(&packet_cursor, &property_length, done);
    if (property_length != (uint32_t)packet_cursor.len) {
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
    } else {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_GENERAL,
            "(%p) aws_mqtt5_decoder - CONNACK decode failure",
            decoder->options.callback_user_data);
        aws_raise_error(AWS_ERROR_MQTT5_DECODE_PROTOCOL_ERROR);
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

    if (result == AWS_OP_ERR) {
        aws_raise_error(AWS_ERROR_MQTT5_DECODE_PROTOCOL_ERROR);
    }

    return result;
}

/* decodes a DISCONNECT packet whose data must be in the scratch buffer */
static int s_aws_mqtt5_decoder_decode_disconnect(struct aws_mqtt5_decoder *decoder) {
    struct aws_mqtt5_packet_disconnect_storage storage;
    if (aws_mqtt5_packet_disconnect_storage_init_from_external_storage(&storage, decoder->allocator)) {
        return AWS_OP_ERR;
    }

    int result = AWS_OP_ERR;

    uint8_t first_byte = decoder->packet_first_byte;
    /* DISCONNECT flags must be zero by protocol */
    if ((first_byte & 0x0F) != 0) {
        goto done;
    }

    struct aws_byte_cursor packet_cursor = decoder->packet_cursor;
    uint32_t remaining_length = decoder->remaining_length;
    if (remaining_length != (uint32_t)packet_cursor.len) {
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
    } else {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_GENERAL,
            "(%p) aws_mqtt5_decoder - DISCONNECT decode failure",
            decoder->options.callback_user_data);
        aws_raise_error(AWS_ERROR_MQTT5_DECODE_PROTOCOL_ERROR);
    }

    aws_mqtt5_packet_disconnect_storage_clean_up(&storage);

    return result;
}

static int s_aws_mqtt5_decoder_decode_pingresp(struct aws_mqtt5_decoder *decoder) {
    if (decoder->packet_cursor.len != 0) {
        goto error;
    }

    uint8_t expected_first_byte = aws_mqtt5_compute_fixed_header_byte1(AWS_MQTT5_PT_PINGRESP, 0);
    if (decoder->packet_first_byte != expected_first_byte || decoder->remaining_length != 0) {
        goto error;
    }

    int result = AWS_OP_SUCCESS;
    if (decoder->options.on_packet_received != NULL) {
        result =
            (*decoder->options.on_packet_received)(AWS_MQTT5_PT_PINGRESP, NULL, decoder->options.callback_user_data);
    }

    return result;

error:

    AWS_LOGF_ERROR(
        AWS_LS_MQTT5_GENERAL, "(%p) aws_mqtt5_decoder - PINGRESP decode failure", decoder->options.callback_user_data);
    return aws_raise_error(AWS_ERROR_MQTT5_DECODE_PROTOCOL_ERROR);
}

static int s_aws_mqtt5_decoder_decode_packet(struct aws_mqtt5_decoder *decoder) {
    enum aws_mqtt5_packet_type packet_type = (enum aws_mqtt5_packet_type)(decoder->packet_first_byte >> 4);
    aws_mqtt5_decoding_fn *decoder_fn = decoder->options.decoder_table->decoders_by_packet_type[packet_type];
    if (decoder_fn == NULL) {
        return aws_raise_error(AWS_ERROR_MQTT5_DECODE_PROTOCOL_ERROR);
    }

    return (*decoder_fn)(decoder);
}

/*
 * (Streaming) Given a packet type and a variable length integer specifying the packet length, this state either
 *    (1) decodes directly from the cursor if possible
 *    (2) reads the packet into the scratch buffer and then decodes it once it is completely present
 *
 */
static enum aws_mqtt5_decode_result_type s_aws_mqtt5_decoder_read_packet_on_data(
    struct aws_mqtt5_decoder *decoder,
    struct aws_byte_cursor *data) {

    /* Are we able to decode directly from the channel message data buffer? */
    if (decoder->scratch_space.len == 0 && decoder->remaining_length <= data->len) {
        /* The cursor contains the entire packet, so decode directly from the backing io message buffer */
        decoder->packet_cursor = aws_byte_cursor_advance(data, decoder->remaining_length);
    } else {
        /* If the packet is fragmented across multiple io messages, then we buffer it internally */
        size_t unread_length = decoder->remaining_length - decoder->scratch_space.len;
        size_t copy_length = aws_min_size(unread_length, data->len);

        struct aws_byte_cursor copy_cursor = aws_byte_cursor_advance(data, copy_length);
        if (aws_byte_buf_append_dynamic(&decoder->scratch_space, &copy_cursor)) {
            return AWS_MQTT5_DRT_ERROR;
        }

        if (copy_length < unread_length) {
            return AWS_MQTT5_DRT_MORE_DATA;
        }

        decoder->packet_cursor = aws_byte_cursor_from_buf(&decoder->scratch_space);
    }

    if (s_aws_mqtt5_decoder_decode_packet(decoder)) {
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

            case AWS_MQTT5_DS_READ_NON_PUBLISH_PACKET:
                result = s_aws_mqtt5_decoder_read_packet_on_data(decoder, &data);
                break;

            default:
                result = AWS_MQTT5_DRT_ERROR;
                break;
        }
    }

    if (result == AWS_MQTT5_DRT_ERROR) {
        aws_raise_error(AWS_ERROR_MQTT5_DECODE_PROTOCOL_ERROR);
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

static struct aws_mqtt5_decoder_function_table s_aws_mqtt5_decoder_default_function_table = {
    .decoders_by_packet_type =
        {
            NULL,                                   /* RESERVED = 0 */
            NULL,                                   /* CONNECT */
            &s_aws_mqtt5_decoder_decode_connack,    /* CONNACK */
            NULL,                                   /* PUBLISH */
            NULL,                                   /* PUBACK */
            NULL,                                   /* PUBREC */
            NULL,                                   /* PUBREL */
            NULL,                                   /* PUBCOMP */
            NULL,                                   /* SUBSCRIBE */
            NULL,                                   /* SUBACK */
            NULL,                                   /* UNSUBSCRIBE */
            NULL,                                   /* UNSUBACK */
            NULL,                                   /* PINGREQ */
            &s_aws_mqtt5_decoder_decode_pingresp,   /* PINGRESP */
            &s_aws_mqtt5_decoder_decode_disconnect, /* DISCONNECT */
            NULL                                    /* AUTH */
        },
};

const struct aws_mqtt5_decoder_function_table *g_aws_mqtt5_default_decoder_table =
    &s_aws_mqtt5_decoder_default_function_table;

int aws_mqtt5_decoder_init(
    struct aws_mqtt5_decoder *decoder,
    struct aws_allocator *allocator,
    struct aws_mqtt5_decoder_options *options) {
    AWS_ZERO_STRUCT(*decoder);

    decoder->options = *options;

    if (decoder->options.decoder_table == NULL) {
        decoder->options.decoder_table = g_aws_mqtt5_default_decoder_table;
    }

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
