/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/private/v5/mqtt5_encoder.h>

#include <aws/io/stream.h>
#include <aws/mqtt/private/v5/mqtt5_utils.h>
#include <aws/mqtt/v5/mqtt5_types.h>

#define INITIAL_ENCODING_STEP_COUNT 64

/* TODO: move to shared private header as decoder will need too */
#define AWS_MQTT5_PROPERTY_TYPE_PAYLOAD_FORMAT_INDICATOR ((uint8_t)1)
#define AWS_MQTT5_PROPERTY_TYPE_MESSAGE_EXPIRY_INTERVAL ((uint8_t)2)
#define AWS_MQTT5_PROPERTY_TYPE_CONTENT_TYPE ((uint8_t)3)
#define AWS_MQTT5_PROPERTY_TYPE_RESPONSE_TOPIC ((uint8_t)8)
#define AWS_MQTT5_PROPERTY_TYPE_CORRELATION_DATA ((uint8_t)9)
#define AWS_MQTT5_PROPERTY_TYPE_SESSION_EXPIRY_INTERVAL ((uint8_t)17)
#define AWS_MQTT5_PROPERTY_TYPE_REQUEST_PROBLEM_INFORMATION ((uint8_t)23)
#define AWS_MQTT5_PROPERTY_TYPE_WILL_DELAY_INTERVAL ((uint8_t)24)
#define AWS_MQTT5_PROPERTY_TYPE_REQUEST_RESPONSE_INFORMATION ((uint8_t)25)
#define AWS_MQTT5_PROPERTY_TYPE_SERVER_REFERENCE ((uint8_t)28)
#define AWS_MQTT5_PROPERTY_TYPE_REASON_STRING ((uint8_t)31)
#define AWS_MQTT5_PROPERTY_TYPE_RECEIVE_MAXIMUM ((uint8_t)33)
#define AWS_MQTT5_PROPERTY_TYPE_TOPIC_ALIAS_MAXIMUM ((uint8_t)34)
#define AWS_MQTT5_PROPERTY_TYPE_USER_PROPERTY ((uint8_t)38)
#define AWS_MQTT5_PROPERTY_TYPE_MAXIMUM_PACKET_SIZE ((uint8_t)39)

int aws_mqtt5_encoder_init(struct aws_mqtt5_encoder *encoder, struct aws_allocator *allocator) {
    AWS_ZERO_STRUCT(*encoder);

    if (aws_array_list_init_dynamic(
            &encoder->encoding_steps, allocator, INITIAL_ENCODING_STEP_COUNT, sizeof(struct aws_mqtt5_encoding_step))) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

void aws_mqtt5_encoder_clean_up(struct aws_mqtt5_encoder *encoder) {
    aws_array_list_clean_up(&encoder->encoding_steps);
}

static int s_aws_mqtt5_encoder_push_step_u8(struct aws_mqtt5_encoder *encoder, uint8_t value) {
    struct aws_mqtt5_encoding_step step;
    AWS_ZERO_STRUCT(step);

    step.type = AWS_MQTT5_EST_U8;
    step.value.value_u8 = value;

    return aws_array_list_push_back(&encoder->encoding_steps, &step);
}

#define ADD_ENCODE_STEP_U8(encoder, value)                                                                             \
    if (s_aws_mqtt5_encoder_push_step_u8(encoder, (value))) {                                                          \
        return AWS_OP_ERR;                                                                                             \
    }

static int s_aws_mqtt5_encoder_push_step_u16(struct aws_mqtt5_encoder *encoder, uint16_t value) {
    struct aws_mqtt5_encoding_step step;
    AWS_ZERO_STRUCT(step);

    step.type = AWS_MQTT5_EST_U16;
    step.value.value_u16 = value;

    return aws_array_list_push_back(&encoder->encoding_steps, &step);
}

#define ADD_ENCODE_STEP_U16(encoder, value)                                                                            \
    if (s_aws_mqtt5_encoder_push_step_u16(encoder, (value))) {                                                         \
        return AWS_OP_ERR;                                                                                             \
    }

static int s_aws_mqtt5_encoder_push_step_u32(struct aws_mqtt5_encoder *encoder, uint32_t value) {
    struct aws_mqtt5_encoding_step step;
    AWS_ZERO_STRUCT(step);

    step.type = AWS_MQTT5_EST_U32;
    step.value.value_u32 = value;

    return aws_array_list_push_back(&encoder->encoding_steps, &step);
}

#define ADD_ENCODE_STEP_U32(encoder, value)                                                                            \
    if (s_aws_mqtt5_encoder_push_step_u32(encoder, (value))) {                                                         \
        return AWS_OP_ERR;                                                                                             \
    }

static int s_aws_mqtt5_encoder_push_step_vli(struct aws_mqtt5_encoder *encoder, uint32_t value) {
    struct aws_mqtt5_encoding_step step;
    AWS_ZERO_STRUCT(step);

    step.type = AWS_MQTT5_EST_VLI;
    step.value.value_u32 = value;

    return aws_array_list_push_back(&encoder->encoding_steps, &step);
}

#define ADD_ENCODE_STEP_VLI(encoder, value)                                                                            \
    if (s_aws_mqtt5_encoder_push_step_vli(encoder, (value))) {                                                         \
        return AWS_OP_ERR;                                                                                             \
    }

static int s_aws_mqtt5_encoder_push_step_cursor(struct aws_mqtt5_encoder *encoder, struct aws_byte_cursor value) {
    struct aws_mqtt5_encoding_step step;
    AWS_ZERO_STRUCT(step);

    step.type = AWS_MQTT5_EST_CURSOR;
    step.value.value_cursor = value;

    return aws_array_list_push_back(&encoder->encoding_steps, &step);
}

#define ADD_ENCODE_STEP_CURSOR(encoder, cursor)                                                                        \
    if (s_aws_mqtt5_encoder_push_step_cursor(encoder, (cursor))) {                                                     \
        return AWS_OP_ERR;                                                                                             \
    }

#define ADD_ENCODE_STEP_LENGTH_PREFIXED_CURSOR(encoder, cursor)                                                        \
    if (s_aws_mqtt5_encoder_push_step_u16(encoder, (cursor).len) ||                                                    \
        s_aws_mqtt5_encoder_push_step_cursor(encoder, (cursor))) {                                                     \
        return AWS_OP_ERR;                                                                                             \
    }

#define ADD_ENCODE_STEP_OPTIONAL_LENGTH_PREFIXED_CURSOR(encoder, cursor_ptr)                                           \
    if (cursor_ptr != NULL) {                                                                                          \
        if (s_aws_mqtt5_encoder_push_step_u16(encoder, (cursor_ptr)->len) ||                                           \
            s_aws_mqtt5_encoder_push_step_cursor(encoder, (*cursor_ptr))) {                                            \
            return AWS_OP_ERR;                                                                                         \
        }                                                                                                              \
    }

static int s_aws_mqtt5_encoder_push_step_stream(struct aws_mqtt5_encoder *encoder, struct aws_input_stream *value) {
    struct aws_mqtt5_encoding_step step;
    AWS_ZERO_STRUCT(step);

    step.type = AWS_MQTT5_EST_STREAM;
    step.value.value_stream = value;

    return aws_array_list_push_back(&encoder->encoding_steps, &step);
}

#define ADD_ENCODE_STEP_STREAM(encoder, value)                                                                         \
    if (s_aws_mqtt5_encoder_push_step_stream(encoder, (value))) {                                                      \
        return AWS_OP_ERR;                                                                                             \
    }

/* optional properties complicate packet size calculations.  Add some macros that simplify */

#define ADD_OPTIONAL_U8_PROPERTY_LENGTH(property_ptr, length)                                                          \
    if ((property_ptr) != NULL) {                                                                                      \
        (length) += 2;                                                                                                 \
    }

#define ADD_OPTIONAL_U16_PROPERTY_LENGTH(property_ptr, length)                                                         \
    if ((property_ptr) != NULL) {                                                                                      \
        (length) += 3;                                                                                                 \
    }

#define ADD_OPTIONAL_U32_PROPERTY_LENGTH(property_ptr, length)                                                         \
    if ((property_ptr) != NULL) {                                                                                      \
        (length) += 5;                                                                                                 \
    }

#define ADD_OPTIONAL_CURSOR_PROPERTY_LENGTH(property_ptr, length)                                                      \
    if ((property_ptr) != NULL) {                                                                                      \
        (length) += 3 + ((property_ptr)->len);                                                                         \
    }

#define ADD_ENCODE_STEP_OPTIONAL_U8_PROPERTY(encoder, property_value, value_ptr)                                       \
    if ((value_ptr) != NULL) {                                                                                         \
        ADD_ENCODE_STEP_U8(encoder, property_value);                                                                   \
        ADD_ENCODE_STEP_U8(encoder, *(value_ptr));                                                                     \
    }

#define ADD_ENCODE_STEP_OPTIONAL_U16_PROPERTY(encoder, property_value, value_ptr)                                      \
    if ((value_ptr) != NULL) {                                                                                         \
        ADD_ENCODE_STEP_U8(encoder, property_value);                                                                   \
        ADD_ENCODE_STEP_U16(encoder, *(value_ptr));                                                                    \
    }

#define ADD_ENCODE_STEP_OPTIONAL_U32_PROPERTY(encoder, property_value, value_ptr)                                      \
    if ((value_ptr) != NULL) {                                                                                         \
        ADD_ENCODE_STEP_U8(encoder, property_value);                                                                   \
        ADD_ENCODE_STEP_U32(encoder, *(value_ptr));                                                                    \
    }

#define ADD_ENCODE_STEP_OPTIONAL_CURSOR_PROPERTY(encoder, property_value, cursor_ptr)                                  \
    if ((cursor_ptr) != NULL) {                                                                                        \
        ADD_ENCODE_STEP_U8(encoder, property_value);                                                                   \
        ADD_ENCODE_STEP_U16(encoder, (cursor_ptr)->len);                                                               \
        ADD_ENCODE_STEP_CURSOR(encoder, *(cursor_ptr));                                                                \
    }

static size_t s_compute_user_property_encode_length(
    const struct aws_mqtt5_user_property *properties,
    size_t user_property_count) {
    /*
     * for each user property, in addition to the raw name-value bytes, we also have 5 bytes of prefix required:
     *  1 byte for the property type
     *  2 bytes for the name length
     *  2 bytes for the value length
     */
    size_t length = 5 * user_property_count;

    for (size_t i = 0; i < user_property_count; ++i) {
        const struct aws_mqtt5_user_property *property = &properties[i];

        length += property->name.len;
        length += property->value.len;
    }

    return length;
}

static int s_add_user_property_encoding_steps(
    struct aws_mqtt5_encoder *encoder,
    const struct aws_mqtt5_user_property *user_properties,
    size_t user_property_count) {
    for (size_t i = 0; i < user_property_count; ++i) {
        const struct aws_mqtt5_user_property *property = &user_properties[i];

        ADD_ENCODE_STEP_U8(encoder, AWS_MQTT5_PROPERTY_TYPE_USER_PROPERTY);
        ADD_ENCODE_STEP_U16(encoder, (uint16_t)property->name.len);
        ADD_ENCODE_STEP_CURSOR(encoder, property->name);
        ADD_ENCODE_STEP_U16(encoder, (uint16_t)property->value.len);
        ADD_ENCODE_STEP_CURSOR(encoder, property->value);
    }

    return AWS_OP_SUCCESS;
}

static uint8_t s_aws_mqtt5_fixed_header_byte1(enum aws_mqtt5_packet_type packet_type, uint8_t flags) {
    return flags | ((uint8_t)packet_type << 4);
}

int aws_mqtt5_encoder_begin_pingreq(struct aws_mqtt5_encoder *encoder) {
    /* A ping is just a fixed header with a 0-valued remaining length which we encode as a 0 u8 rather than a 0 vli */
    ADD_ENCODE_STEP_U8(encoder, s_aws_mqtt5_fixed_header_byte1(AWS_MQTT5_PT_PINGREQ, 0));
    ADD_ENCODE_STEP_U8(encoder, 0);

    return AWS_OP_SUCCESS;
}

static int s_compute_disconnect_variable_length_fields(
    struct aws_mqtt5_packet_disconnect_view *disconnect_view,
    uint32_t *total_remaining_length,
    uint32_t *property_length) {
    size_t local_property_length =
        s_compute_user_property_encode_length(disconnect_view->user_properties, disconnect_view->user_property_count);

    ADD_OPTIONAL_U32_PROPERTY_LENGTH(disconnect_view->session_expiry_interval_seconds, local_property_length);
    ADD_OPTIONAL_CURSOR_PROPERTY_LENGTH(disconnect_view->server_reference, local_property_length);
    ADD_OPTIONAL_CURSOR_PROPERTY_LENGTH(disconnect_view->reason_string, local_property_length);

    *property_length = (uint32_t)local_property_length;

    size_t property_length_encoding_length = 0;
    if (aws_mqtt5_get_variable_length_encode_size(local_property_length, &property_length_encoding_length)) {
        return AWS_OP_ERR;
    }

    /* reason code is the only other thing to worry about */
    *total_remaining_length = 1 + *property_length + (uint32_t)property_length_encoding_length;

    return AWS_OP_SUCCESS;
}

int aws_mqtt5_encoder_begin_disconnect(
    struct aws_mqtt5_encoder *encoder,
    struct aws_mqtt5_packet_disconnect_view *disconnect_view) {
    uint32_t total_remaining_length = 0;
    uint32_t property_length = 0;
    if (s_compute_disconnect_variable_length_fields(disconnect_view, &total_remaining_length, &property_length)) {
        return AWS_OP_ERR;
    }

    ADD_ENCODE_STEP_U8(encoder, s_aws_mqtt5_fixed_header_byte1(AWS_MQTT5_PT_DISCONNECT, 0));
    ADD_ENCODE_STEP_VLI(encoder, total_remaining_length);
    ADD_ENCODE_STEP_U8(encoder, (uint8_t)disconnect_view->reason_code);
    ADD_ENCODE_STEP_VLI(encoder, property_length);

    if (property_length > 0) {
        ADD_ENCODE_STEP_OPTIONAL_U32_PROPERTY(
            encoder, AWS_MQTT5_PROPERTY_TYPE_SESSION_EXPIRY_INTERVAL, disconnect_view->session_expiry_interval_seconds);
        ADD_ENCODE_STEP_OPTIONAL_CURSOR_PROPERTY(
            encoder, AWS_MQTT5_PROPERTY_TYPE_REASON_STRING, disconnect_view->reason_string);
        ADD_ENCODE_STEP_OPTIONAL_CURSOR_PROPERTY(
            encoder, AWS_MQTT5_PROPERTY_TYPE_SERVER_REFERENCE, disconnect_view->server_reference);

        if (s_add_user_property_encoding_steps(
                encoder, disconnect_view->user_properties, disconnect_view->user_property_count)) {
            return AWS_OP_ERR;
        }
    }

    return AWS_OP_SUCCESS;
}

static int s_compute_connect_variable_length_fields(
    struct aws_mqtt5_packet_connect_view *connect_view,
    size_t will_payload_length,
    size_t *total_remaining_length,
    size_t *connect_property_length,
    size_t *will_property_length) {
    size_t connect_property_section_length =
        s_compute_user_property_encode_length(connect_view->user_properties, connect_view->user_property_count);

    ADD_OPTIONAL_U32_PROPERTY_LENGTH(connect_view->session_expiry_interval_seconds, connect_property_section_length);
    ADD_OPTIONAL_U16_PROPERTY_LENGTH(connect_view->receive_maximum, connect_property_section_length);
    ADD_OPTIONAL_U32_PROPERTY_LENGTH(connect_view->maximum_packet_size_bytes, connect_property_section_length);
    ADD_OPTIONAL_U16_PROPERTY_LENGTH(connect_view->topic_alias_maximum, connect_property_section_length);
    ADD_OPTIONAL_U8_PROPERTY_LENGTH(connect_view->request_response_information, connect_property_section_length);
    ADD_OPTIONAL_U8_PROPERTY_LENGTH(connect_view->request_problem_information, connect_property_section_length);

    *connect_property_length = (uint32_t)connect_property_section_length;

    /* variable header length =
     *    10 bytes (6 for mqtt string, 1 for protocol version, 1 for flags, 2 for keep alive)
     *  + # bytes(variable_length_encoding(connect_property_section_length))
     *  + connect_property_section_length
     */
    size_t variable_header_length = 0;
    if (aws_mqtt5_get_variable_length_encode_size(connect_property_section_length, &variable_header_length)) {
        return AWS_OP_ERR;
    }

    variable_header_length += 10 + connect_property_section_length;

    size_t payload_length = 2 + connect_view->client_id.len;

    *will_property_length = 0;
    if (connect_view->will != NULL) {
        const struct aws_mqtt5_packet_publish_view *publish_view = connect_view->will;

        *will_property_length =
            s_compute_user_property_encode_length(publish_view->user_properties, publish_view->user_property_count);
        ;

        ADD_OPTIONAL_U32_PROPERTY_LENGTH(connect_view->will_delay_interval_seconds, *will_property_length);
        ADD_OPTIONAL_U8_PROPERTY_LENGTH(publish_view->payload_format, *will_property_length);
        ADD_OPTIONAL_U32_PROPERTY_LENGTH(publish_view->message_expiry_interval_seconds, *will_property_length);
        ADD_OPTIONAL_CURSOR_PROPERTY_LENGTH(publish_view->content_type, *will_property_length);
        ADD_OPTIONAL_CURSOR_PROPERTY_LENGTH(publish_view->response_topic, *will_property_length);
        ADD_OPTIONAL_CURSOR_PROPERTY_LENGTH(publish_view->correlation_data, *will_property_length);

        size_t will_properties_length_encode_size = 0;
        if (aws_mqtt5_get_variable_length_encode_size(
                (uint32_t)*will_property_length, &will_properties_length_encode_size)) {
            return AWS_OP_ERR;
        }

        payload_length += *will_property_length;
        payload_length += will_properties_length_encode_size;

        payload_length += 2 + publish_view->topic.len;
        payload_length += 2 + will_payload_length;
    }

    /* Can't use the optional property macros because these don't have a leading property type byte */
    if (connect_view->username != NULL) {
        payload_length += connect_view->username->len + 2;
    }

    if (connect_view->password != NULL) {
        payload_length += connect_view->password->len + 2;
    }

    *total_remaining_length = payload_length + variable_header_length;

    return AWS_OP_SUCCESS;
}

static uint8_t s_aws_mqtt5_connect_compute_connect_flags(const struct aws_mqtt5_packet_connect_view *connect_view) {
    uint8_t flags = 0;

    if (connect_view->clean_start) {
        flags |= 1 << 1;
    }

    const struct aws_mqtt5_packet_publish_view *will = connect_view->will;
    if (will != NULL) {
        flags |= 1 << 2;
        flags |= ((uint8_t)will->qos) << 3;

        if (will->retain) {
            flags |= 1 << 5;
        }
    }

    if (connect_view->password != NULL) {
        flags |= 1 << 6;
    }

    if (connect_view->username != NULL) {
        flags |= 1 << 7;
    }

    return flags;
}

/* encodes a utf8-string (2 byte length + "MQTT") + the version value (5) */
static uint8_t s_connect_variable_length_header_prefix[7] = {0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x05};

static struct aws_byte_cursor s_variable_header_prefix_cursor = {
    .ptr = &s_connect_variable_length_header_prefix[0],
    .len = AWS_ARRAY_SIZE(s_connect_variable_length_header_prefix),
};

int aws_mqtt5_encoder_begin_connect(
    struct aws_mqtt5_encoder *encoder,
    struct aws_mqtt5_packet_connect_view *connect_view) {

    const struct aws_mqtt5_packet_publish_view *will = connect_view->will;
    int64_t will_payload_length = 0;
    if (will != NULL && will->payload != NULL) {
        if (aws_input_stream_seek(will->payload, AWS_SSB_BEGIN, 0)) {
            return AWS_OP_ERR;
        }

        if (aws_input_stream_get_length(will->payload, &will_payload_length)) {
            return AWS_OP_ERR;
        }
    }

    size_t total_remaining_length = 0;
    size_t connect_property_length = 0;
    size_t will_property_length = 0;
    if (s_compute_connect_variable_length_fields(
            connect_view,
            (size_t)will_payload_length,
            &total_remaining_length,
            &connect_property_length,
            &will_property_length)) {
        return AWS_OP_ERR;
    }

    uint32_t total_remaining_length_u32 = (uint32_t)total_remaining_length;
    uint32_t connect_property_length_u32 = (uint32_t)connect_property_length;
    uint32_t will_property_length_u32 = (uint32_t)will_property_length;

    ADD_ENCODE_STEP_U8(encoder, s_aws_mqtt5_fixed_header_byte1(AWS_MQTT5_PT_CONNECT, 0));
    ADD_ENCODE_STEP_VLI(encoder, total_remaining_length_u32);
    ADD_ENCODE_STEP_CURSOR(encoder, s_variable_header_prefix_cursor);
    ADD_ENCODE_STEP_U8(encoder, s_aws_mqtt5_connect_compute_connect_flags(connect_view));
    ADD_ENCODE_STEP_U16(encoder, connect_view->keep_alive_interval_seconds);

    ADD_ENCODE_STEP_VLI(encoder, connect_property_length_u32);
    ADD_ENCODE_STEP_OPTIONAL_U32_PROPERTY(
        encoder, AWS_MQTT5_PROPERTY_TYPE_SESSION_EXPIRY_INTERVAL, connect_view->session_expiry_interval_seconds);
    ADD_ENCODE_STEP_OPTIONAL_U16_PROPERTY(
        encoder, AWS_MQTT5_PROPERTY_TYPE_RECEIVE_MAXIMUM, connect_view->receive_maximum);
    ADD_ENCODE_STEP_OPTIONAL_U32_PROPERTY(
        encoder, AWS_MQTT5_PROPERTY_TYPE_MAXIMUM_PACKET_SIZE, connect_view->maximum_packet_size_bytes);
    ADD_ENCODE_STEP_OPTIONAL_U16_PROPERTY(
        encoder, AWS_MQTT5_PROPERTY_TYPE_TOPIC_ALIAS_MAXIMUM, connect_view->topic_alias_maximum);
    ADD_ENCODE_STEP_OPTIONAL_U8_PROPERTY(
        encoder, AWS_MQTT5_PROPERTY_TYPE_REQUEST_RESPONSE_INFORMATION, connect_view->request_response_information);
    ADD_ENCODE_STEP_OPTIONAL_U8_PROPERTY(
        encoder, AWS_MQTT5_PROPERTY_TYPE_REQUEST_PROBLEM_INFORMATION, connect_view->request_problem_information);

    if (s_add_user_property_encoding_steps(encoder, connect_view->user_properties, connect_view->user_property_count)) {
        return AWS_OP_ERR;
    }

    ADD_ENCODE_STEP_LENGTH_PREFIXED_CURSOR(encoder, connect_view->client_id);

    if (will != NULL) {
        ADD_ENCODE_STEP_VLI(encoder, will_property_length_u32);
        ADD_ENCODE_STEP_OPTIONAL_U32_PROPERTY(
            encoder, AWS_MQTT5_PROPERTY_TYPE_WILL_DELAY_INTERVAL, connect_view->will_delay_interval_seconds);
        ADD_ENCODE_STEP_OPTIONAL_U8_PROPERTY(
            encoder, AWS_MQTT5_PROPERTY_TYPE_PAYLOAD_FORMAT_INDICATOR, will->payload_format);
        ADD_ENCODE_STEP_OPTIONAL_U32_PROPERTY(
            encoder, AWS_MQTT5_PROPERTY_TYPE_MESSAGE_EXPIRY_INTERVAL, will->message_expiry_interval_seconds);
        ADD_ENCODE_STEP_OPTIONAL_CURSOR_PROPERTY(encoder, AWS_MQTT5_PROPERTY_TYPE_CONTENT_TYPE, will->content_type);
        ADD_ENCODE_STEP_OPTIONAL_CURSOR_PROPERTY(encoder, AWS_MQTT5_PROPERTY_TYPE_RESPONSE_TOPIC, will->response_topic);
        ADD_ENCODE_STEP_OPTIONAL_CURSOR_PROPERTY(
            encoder, AWS_MQTT5_PROPERTY_TYPE_CORRELATION_DATA, will->correlation_data);

        if (s_add_user_property_encoding_steps(encoder, will->user_properties, will->user_property_count)) {
            return AWS_OP_ERR;
        }

        ADD_ENCODE_STEP_LENGTH_PREFIXED_CURSOR(encoder, will->topic);
        ADD_ENCODE_STEP_U16(encoder, (uint16_t)will_payload_length);
        ADD_ENCODE_STEP_STREAM(encoder, will->payload);
    }

    ADD_ENCODE_STEP_OPTIONAL_LENGTH_PREFIXED_CURSOR(encoder, connect_view->username);
    ADD_ENCODE_STEP_OPTIONAL_LENGTH_PREFIXED_CURSOR(encoder, connect_view->password);

    return AWS_OP_SUCCESS;
}

static enum aws_mqtt5_encoding_result s_execute_encode_step(
    struct aws_mqtt5_encoding_step *step,
    struct aws_byte_buf *buffer) {
    size_t buffer_room = buffer->capacity - buffer->len;

    switch (step->type) {
        case AWS_MQTT5_EST_U8:
            if (buffer_room < 1) {
                return AWS_MQTT5_ER_OUT_OF_ROOM;
            }

            if (!aws_byte_buf_write_u8(buffer, step->value.value_u8)) {
                return AWS_MQTT5_ER_ERROR;
            }

            return AWS_MQTT5_ER_FINISHED;

        case AWS_MQTT5_EST_U16:
            if (buffer_room < 2) {
                return AWS_MQTT5_ER_OUT_OF_ROOM;
            }

            if (!aws_byte_buf_write_be16(buffer, step->value.value_u16)) {
                return AWS_MQTT5_ER_ERROR;
            }

            return AWS_MQTT5_ER_FINISHED;

        case AWS_MQTT5_EST_U32:
            if (buffer_room < 4) {
                return AWS_MQTT5_ER_OUT_OF_ROOM;
            }

            if (!aws_byte_buf_write_be32(buffer, step->value.value_u32)) {
                return AWS_MQTT5_ER_ERROR;
            }

            return AWS_MQTT5_ER_FINISHED;

        case AWS_MQTT5_EST_VLI:
            /* being lazy here and just assuming the worst case */
            if (buffer_room < 4) {
                return AWS_MQTT5_ER_OUT_OF_ROOM;
            }

            if (aws_mqtt5_encode_variable_length_integer(buffer, step->value.value_u32)) {
                return AWS_MQTT5_ER_ERROR;
            }

            return AWS_MQTT5_ER_FINISHED;

        case AWS_MQTT5_EST_CURSOR:
            if (buffer_room < 1) {
                return AWS_MQTT5_ER_OUT_OF_ROOM;
            }

            aws_byte_buf_write_to_capacity(buffer, &step->value.value_cursor);

            return (step->value.value_cursor.len == 0) ? AWS_MQTT5_ER_FINISHED : AWS_MQTT5_ER_OUT_OF_ROOM;

        case AWS_MQTT5_EST_STREAM: {
            if (buffer_room < 1) {
                return AWS_MQTT5_ER_OUT_OF_ROOM;
            }

            bool stream_finished = false;
            int result = AWS_OP_SUCCESS;
            do {
                result = aws_input_stream_read(step->value.value_stream, buffer);
                if (result == AWS_OP_SUCCESS) {
                    struct aws_stream_status status;
                    if (aws_input_stream_get_status(step->value.value_stream, &status)) {
                        return AWS_MQTT5_ER_ERROR;
                    }

                    stream_finished = status.is_end_of_stream;
                }
            } while (buffer->len < buffer->capacity && !stream_finished && result == AWS_OP_SUCCESS);

            if (stream_finished) {
                return AWS_MQTT5_ER_FINISHED;
            } else if (result != AWS_OP_SUCCESS) {
                return AWS_MQTT5_ER_ERROR;
            } else if (buffer->len == buffer->capacity) {
                return AWS_MQTT5_ER_OUT_OF_ROOM;
            }

            /* shouldn't be reachable */
            return AWS_MQTT5_ER_ERROR;
        }
    }

    return AWS_MQTT5_ER_ERROR;
}

enum aws_mqtt5_encoding_result aws_mqtt5_encoder_encode_to_buffer(
    struct aws_mqtt5_encoder *encoder,
    struct aws_byte_buf *buffer) {

    enum aws_mqtt5_encoding_result result = AWS_MQTT5_ER_FINISHED;
    size_t step_count = aws_array_list_length(&encoder->encoding_steps);
    while (result == AWS_MQTT5_ER_FINISHED && encoder->current_encoding_step_index < step_count) {
        struct aws_mqtt5_encoding_step *step = NULL;
        if (aws_array_list_get_at_ptr(&encoder->encoding_steps, (void **)&step, encoder->current_encoding_step_index)) {
            return AWS_MQTT5_ER_ERROR;
        }

        result = s_execute_encode_step(step, buffer);
        if (result == AWS_MQTT5_ER_FINISHED) {
            encoder->current_encoding_step_index++;
        }
    }

    if (result == AWS_MQTT5_ER_FINISHED) {
        aws_array_list_clear(&encoder->encoding_steps);
        encoder->current_encoding_step_index = 0;
    }

    return result;
}
