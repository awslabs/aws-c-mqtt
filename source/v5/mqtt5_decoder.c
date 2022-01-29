/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/private/v5/mqtt5_decoder.h>

#define AWS_MQTT5_DECODER_BUFFER_START_SIZE 2048

static void s_reset_decoder_for_new_packet(struct aws_mqtt5_decoder *decoder) {
    aws_byte_buf_reset(&decoder->pre_decode_buffer, false);
    decoder->pre_decode_cursor = aws_byte_cursor_from_buf(&decoder->pre_decode_buffer);

    aws_mqtt5_packet_publish_storage_clean_up(&decoder->packet_state.publish_storage);
    AWS_ZERO_STRUCT(decoder->packet_state);
}

int aws_mqtt5_decoder_init(
    struct aws_mqtt5_decoder *decoder,
    struct aws_allocator *allocator,
    struct aws_mqtt5_decoder_options *options) {
    AWS_ZERO_STRUCT(*decoder);

    decoder->config = *options;
    decoder->state = AWS_MQTT5_DS_READ_PACKET_TYPE;

    if (aws_byte_buf_init(&decoder->pre_decode_buffer, allocator, AWS_MQTT5_DECODER_BUFFER_START_SIZE)) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

void aws_mqtt5_decoder_clean_up(struct aws_mqtt5_decoder *decoder) {
    aws_byte_buf_clean_up(&decoder->pre_decode_buffer);
}

static void s_enter_state(struct aws_mqtt5_decoder *decoder, enum aws_mqtt5_decoder_state state) {
    switch (state) {
        case AWS_MQTT5_DS_READ_PACKET_TYPE:
            s_reset_decoder_for_new_packet(decoder);
            break;

        case AWS_MQTT5_DS_READ_REMAINING_LENGTH:
            decoder->packet_state.type =
                (enum aws_mqtt5_packet_type)((decoder->pre_decode_buffer.buffer[0] & 0xF0) >> 4);
            break;

        case AWS_MQTT5_DS_READ_PACKET:
            break;

        case AWS_MQTT5_DS_DECODE_PACKET:
            break;

        default:
            break;
    }

    decoder->state = state;
}

static int s_aws_mqtt5_decoder_read_packet_type_on_data(
    struct aws_mqtt5_decoder *decoder,
    struct aws_byte_cursor *data) {
    AWS_ASSERT(decoder->pre_decode_buffer.len == 0);
    AWS_ASSERT(data.len > 0);

    struct aws_byte_cursor byte_cursor = *data;
    byte_cursor.len = 1;

    aws_byte_buf_append_dynamic(&decoder->pre_decode_buffer, &byte_cursor);
    s_enter_state(decoder, AWS_MQTT5_DS_READ_REMAINING_LENGTH);
}

static bool s_can_decode_vli(struct aws_byte_cursor cursor) {
    return cursor.len > 0 && (cursor.ptr[cursor.len - 1] & 0x80) == 0;
}

static uint32_t s_decode_vli(struct aws_byte_cursor cursor) {
    AWS_PRECONDITION(cursor.len <= 4 && cursor.len > 0);
}

static int s_aws_mqtt5_decoder_read_remaining_length_on_data(
    struct aws_mqtt5_decoder *decoder,
    struct aws_byte_cursor *data) {
    struct aws_byte_cursor vli_cursor = {.ptr = decoder->pre_decode_buffer.buffer + 1,
                                         .len = decoder->pre_decode_buffer.len - 1};

    bool enough_data = s_can_decode_vli(vli_cursor);

    while (data->len > 0 && !enough_data && vli_cursor.len < 4) {
        struct aws_byte_cursor byte_cursor = *data;
        byte_cursor.len = 1;

        aws_byte_buf_append_dynamic(&decoder->pre_decode_buffer, &byte_cursor);
        aws_byte_cursor_advance(data, 1);
        ++vli_cursor.len;

        enough_data = s_can_decode_vli(vli_cursor);
    }

    if (!enough_data) {
        return AWS_OP_SUCCESS;
    }

    decoder->packet_state.remaining_length = s_decode_vli(vli_cursor);
    s_enter_state(decoder, AWS_MQTT5_DS_READ_PACKET);

    return AWS_OP_SUCCESS;
}

static int s_aws_mqtt5_decoder_read_packet_on_data(struct aws_mqtt5_decoder *decoder, struct aws_byte_cursor *data) {}

static int s_aws_mqtt5_decoder_decode_packet_type_on_data(
    struct aws_mqtt5_decoder *decoder,
    struct aws_byte_cursor *data) {}

int aws_mqtt5_decoder_on_data_received(struct aws_mqtt5_decoder *decoder, struct aws_byte_cursor data) {
    int result = AWS_OP_SUCCESS;
    while (data.len > 0 && result == AWS_OP_SUCCESS) {

        switch (decoder->state) {
            case AWS_MQTT5_DS_READ_PACKET_TYPE:
                result = s_aws_mqtt5_decoder_read_packet_type_on_data(decoder, &data);

            case AWS_MQTT5_DS_READ_REMAINING_LENGTH:
                result = s_aws_mqtt5_decoder_read_remaining_length_on_data(decoder, &data);

            case AWS_MQTT5_DS_READ_PACKET:
                result = s_aws_mqtt5_decoder_read_packet_on_data(decoder, &data);

            case AWS_MQTT5_DS_DECODE_PACKET:
                result = s_aws_mqtt5_decoder_decode_packet_type_on_data(decoder, &data);

            default:
                result = aws_raise_error(AWS_ERROR_INVALID_STATE);
        }
    }

    return result;
}
