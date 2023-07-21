/**
* Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
* SPDX-License-Identifier: Apache-2.0.
*/

#include <aws/mqtt/private/mqtt311_decoder.h>

#include <aws/mqtt/private/fixed_header.h>

void aws_mqtt311_decoder_init(struct aws_mqtt311_decoder *decoder, struct aws_allocator *allocator, const struct aws_mqtt311_decoder_options *options) {
    decoder->state = AWS_MDST_READ_FIRST_BYTE;
    decoder->total_packet_length = 0;
    aws_byte_buf_init(&decoder->packet_buffer, allocator, 5);
    decoder->config = *options;
}

void aws_mqtt311_decoder_clean_up(struct aws_mqtt311_decoder *decoder) {
    aws_byte_buf_clean_up(&decoder->packet_buffer);
}

static void s_aws_mqtt311_decoder_reset(struct aws_mqtt311_decoder *decoder) {
    decoder->state = AWS_MDST_READ_FIRST_BYTE;
    decoder->total_packet_length = 0;
    aws_byte_buf_reset(&decoder->packet_buffer, false);
}

static void s_aws_mqtt311_decoder_reset_for_new_packet(struct aws_mqtt311_decoder *decoder) {
    if (decoder->state != AWS_MDST_PROTOCOL_ERROR) {
        s_aws_mqtt311_decoder_reset(decoder);
    }
}

static int s_handle_decoder_read_first_byte(struct aws_mqtt311_decoder *decoder, struct aws_byte_cursor *data) {
    AWS_FATAL_ASSERT(decoder->packet_buffer.len == 0);

    /*
     * Do a greedy check to see if the whole MQTT packet is contained within the received data.  If it is, decode it
     * directly from the incoming data cursor without buffering it first.  Otherwise, the packet is fragmented
     * across multiple received data calls, and so we must use the packet buffer and copy everything first via the
     * full decoder state machine.
     *
     * A corollary of this is that the decoder is only ever in the AWS_MDST_READ_REMAINING_LENGTH or AWS_MDST_READ_BODY
     * states if the current MQTT packet was received in a fragmented manner.
     */
    struct aws_byte_cursor temp_cursor = *data;
    struct aws_mqtt_fixed_header packet_header;
    AWS_ZERO_STRUCT(packet_header);
    if (!aws_mqtt_fixed_header_decode(&temp_cursor, &packet_header) && temp_cursor.len >= packet_header.remaining_length) {
        /* figure out the cursor that spans the full packet */
        size_t fixed_header_length = temp_cursor.ptr - data->ptr;
        struct aws_byte_cursor whole_packet_cursor = *data;
        whole_packet_cursor.len = fixed_header_length + packet_header.remaining_length;

        /* advance the external, mutable data cursor to the start of the next packet */
        aws_byte_cursor_advance(data, whole_packet_cursor.len);

        /*
         * if this fails, the decoder goes into an error state.  If it succeeds we'll loop again into the same state
         * because we'll be back at the beginning of the next packet (if it exists).
         */
        enum aws_mqtt_packet_type packet_type = aws_mqtt_get_packet_type(whole_packet_cursor.ptr);
        return decoder->config.packet_handlers->handlers_by_packet_type[packet_type](whole_packet_cursor, decoder->config.handler_user_data);
    }

    uint8_t byte = *data->ptr;
    aws_byte_cursor_advance(data, 1);
    aws_byte_buf_append_byte_dynamic(&decoder->packet_buffer, byte);

    decoder->state = AWS_MDST_READ_REMAINING_LENGTH;

    return AWS_OP_SUCCESS;
}

static int s_handle_decoder_read_remaining_length(struct aws_mqtt311_decoder *decoder, struct aws_byte_cursor *data) {
    AWS_FATAL_ASSERT(decoder->total_packet_length == 0);

    uint8_t byte = *data->ptr;
    aws_byte_cursor_advance(data, 1);
    aws_byte_buf_append_byte_dynamic(&decoder->packet_buffer, byte);

    struct aws_byte_cursor vli_cursor = aws_byte_cursor_from_buf(&decoder->packet_buffer);
    aws_byte_cursor_advance(&vli_cursor, 1);

    size_t remaining_length = 0;
    int vli_result = aws_mqtt311_decode_remaining_length(&vli_cursor, &remaining_length);
    if (vli_result == AWS_OP_ERR && aws_last_error() != AWS_ERROR_SHORT_BUFFER) {
        return aws_raise_error(AWS_ERROR_MQTT_PROTOCOL_ERROR);
    }

    if (vli_result == AWS_OP_SUCCESS) {
        decoder->total_packet_length = remaining_length + decoder->packet_buffer.len;
        AWS_FATAL_ASSERT(decoder->total_packet_length > 0);
        decoder->state = AWS_MDST_READ_BODY;
    }

    return AWS_OP_SUCCESS;
}

static int s_handle_decoder_read_body(struct aws_mqtt311_decoder *decoder, struct aws_byte_cursor *data) {
    AWS_FATAL_ASSERT(decoder->total_packet_length > 0);

    size_t buffer_length = decoder->packet_buffer.len;
    size_t amount_to_read = aws_min_size(decoder->total_packet_length - buffer_length, data->len);

    struct aws_byte_cursor copy_cursor = aws_byte_cursor_advance(data, amount_to_read);
    if (aws_byte_buf_append_dynamic(&decoder->packet_buffer, &copy_cursor)) {
        return aws_raise_error(AWS_ERROR_MQTT_PROTOCOL_ERROR);
    }

    if (decoder->packet_buffer.len == decoder->total_packet_length) {
        struct aws_byte_cursor packet_data = aws_byte_cursor_from_buf(&decoder->packet_buffer);
        enum aws_mqtt_packet_type packet_type = aws_mqtt_get_packet_type(packet_data.ptr);
        if (decoder->config.packet_handlers->handlers_by_packet_type[packet_type](packet_data, decoder->config.handler_user_data) == AWS_OP_ERR) {
            return AWS_OP_ERR;
        }

        s_aws_mqtt311_decoder_reset_for_new_packet(decoder);
    }

    return AWS_OP_SUCCESS;
}

int aws_mqtt311_decoder_on_bytes_received(struct aws_mqtt311_decoder *decoder, struct aws_byte_cursor data) {
    struct aws_byte_cursor data_cursor = data;

    while (data_cursor.len > 0) {
        int result = AWS_OP_ERR;
        switch (decoder->state) {
            case AWS_MDST_READ_FIRST_BYTE:
                result = s_handle_decoder_read_first_byte(decoder, &data_cursor);
                break;

            case AWS_MDST_READ_REMAINING_LENGTH:
                result = s_handle_decoder_read_remaining_length(decoder, &data_cursor);
                break;

            case AWS_MDST_READ_BODY:
                result = s_handle_decoder_read_body(decoder, &data_cursor);
                break;

            default:
                break;
        }

        if (result == AWS_OP_ERR) {
            decoder->state = AWS_MDST_PROTOCOL_ERROR;
            return aws_raise_error(AWS_ERROR_MQTT_PROTOCOL_ERROR);
        }
    }

    return AWS_OP_SUCCESS;
}

void aws_mqtt311_decoder_reset_for_new_connection(struct aws_mqtt311_decoder *decoder) {
    s_aws_mqtt311_decoder_reset(decoder);
}
