/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/private/v5/mqtt5_decoder.h>

#define AWS_MQTT5_DECODER_STACK_START_SIZE 8
#define AWS_MQTT5_DECODER_BUFFER_START_SIZE 2048

static void s_reset_decoder_for_new_packet(struct aws_mqtt5_decoder *decoder) {
    aws_byte_buf_reset(&decoder->scratch_space, false);
    decoder->current_step_scratch_offset = 0;

    decoder->packet_type = AWS_MQTT5_PT_RESERVED;
    decoder->remaining_length = 0;
    decoder->publish_topic_length = 0;
    AWS_ZERO_STRUCT(decoder->publish_topic);
    decoder->publish_properties_remaining_length = 0;
}

int aws_mqtt5_decoder_init(
    struct aws_mqtt5_decoder *decoder,
    struct aws_allocator *allocator,
    struct aws_mqtt5_client *client) {
    AWS_ZERO_STRUCT(*decoder);

    decoder->client = client;
    decoder->state = AWS_MQTT5_DS_READ_PACKET_TYPE;

    if (aws_byte_buf_init(&decoder->scratch_space, allocator, AWS_MQTT5_DECODER_BUFFER_START_SIZE)) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
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

static int s_aws_mqtt5_decoder_read_packet_type_on_data(
    struct aws_mqtt5_decoder *decoder,
    struct aws_byte_cursor *data) {

    uint8_t packet_type = *data->ptr;
    aws_byte_cursor_advance(data, 1);

    if (!s_is_decodable_packet_type(packet_type)) {
        return aws_raise_error(AWS_ERROR_MQTT5_DECODE_INVALID_PACKET_TYPE);
    }

    decoder->packet_type = packet_type;

    s_enter_state(decoder, AWS_MQTT5_DS_READ_REMAINING_LENGTH);

    return AWS_OP_SUCCESS;
}

enum aws_mqtt5_decode_vli_result_type {
    AWS_MQTT5_DVRT_MORE_DATA,
    AWS_MQTT5_DVRT_SUCCESS,
    AWS_MQTT5_DVRT_ERROR,
};

static enum aws_mqtt5_decode_vli_result_type s_decode_vli(struct aws_byte_cursor *cursor, uint32_t *dest) {
    uint32_t value = 0;
    bool more_data = false;
    size_t bytes_used = 0;

    for (; bytes_used < 4; ++bytes_used) {
        if (bytes_used >= cursor->len) {
            return AWS_MQTT5_DVRT_MORE_DATA;
        }

        value <<= 7;

        uint8_t byte = *(cursor->ptr + bytes_used);
        value |= (byte & 0x7F);

        more_data = (byte & 0x80) != 0;
        if (!more_data) {
            break;
        }
    }

    if (more_data) {
        /* A variable length integer with the 4th byte high bit set is not valid */
        return AWS_MQTT5_DVRT_ERROR;
    }

    aws_byte_cursor_advance(cursor, bytes_used);
    *dest = value;

    return AWS_MQTT5_DVRT_SUCCESS;
}

static int s_aws_mqtt5_decoder_read_vli_on_data(
    struct aws_mqtt5_decoder *decoder,
    uint32_t *vli_dest,
    struct aws_byte_cursor *data) {

    enum aws_mqtt5_decode_vli_result_type decode_vli_result = AWS_MQTT5_DVRT_MORE_DATA;

    while (data->len > 0 && decode_vli_result == AWS_MQTT5_DVRT_MORE_DATA) {
        struct aws_byte_cursor byte_cursor = *data;
        byte_cursor.len = 1;

        aws_byte_buf_append_dynamic(&decoder->scratch_space, &byte_cursor);
        aws_byte_cursor_advance(data, 1);

        struct aws_byte_cursor vli_cursor = {
            .ptr = decoder->scratch_space.buffer + decoder->current_step_scratch_offset,
            .len = decoder->scratch_space.len - decoder->current_step_scratch_offset,
        };

        decode_vli_result = s_decode_vli(&vli_cursor, vli_dest);
    }

    return decode_vli_result;
}

static int s_aws_mqtt5_decoder_read_remaining_length_on_data(
    struct aws_mqtt5_decoder *decoder,
    struct aws_byte_cursor *data) {

    enum aws_mqtt5_decode_vli_result_type result =
        s_aws_mqtt5_decoder_read_vli_on_data(decoder, &decoder->remaining_length, data);

    if (result == AWS_MQTT5_DVRT_MORE_DATA) {
        AWS_FATAL_ASSERT(data->len == 0);
        return AWS_OP_SUCCESS;
    }

    if (result == AWS_MQTT5_DVRT_ERROR) {
        return aws_raise_error(AWS_ERROR_MQTT5_DECODE_INVALID_VARIABLE_LENGTH_INTEGER);
    }

    decoder->total_length = decoder->remaining_length + decoder->scratch_space.len;

    s_enter_state(decoder, AWS_MQTT5_DS_READ_PACKET);

    return AWS_OP_SUCCESS;
}

static int s_aws_mqtt5_decoder_decode_packet_from_scratch_buffer(struct aws_mqtt5_decoder *decoder) {
    (void)decoder;

    /* TODO */
    return AWS_OP_SUCCESS;
}

static int s_aws_mqtt5_decoder_read_packet_on_data(struct aws_mqtt5_decoder *decoder, struct aws_byte_cursor *data) {

    size_t unread_length = decoder->total_length - decoder->scratch_space.len;
    size_t copy_length = aws_min_size(unread_length, data->len);

    struct aws_byte_cursor copy_cursor = {
        .ptr = data->ptr,
        .len = copy_length,
    };

    if (aws_byte_buf_append_dynamic(&decoder->scratch_space, &copy_cursor)) {
        return AWS_OP_ERR;
    }

    aws_byte_cursor_advance(data, copy_length);

    if (copy_length < unread_length) {
        return AWS_OP_SUCCESS;
    }

    if (s_aws_mqtt5_decoder_decode_packet_from_scratch_buffer(decoder)) {
        return AWS_OP_ERR;
    }

    s_enter_state(decoder, AWS_MQTT5_DS_READ_PACKET_TYPE);

    return AWS_OP_SUCCESS;
}

int aws_mqtt5_decoder_on_data_received(struct aws_mqtt5_decoder *decoder, struct aws_byte_cursor data) {
    int result = AWS_OP_SUCCESS;
    while (data.len > 0 && result == AWS_OP_SUCCESS) {

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

    return result;
}
