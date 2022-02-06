/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/private/v5/mqtt5_encoder.h>

#include <aws/io/stream.h>
#include <aws/mqtt/private/v5/mqtt5_utils.h>
#include <aws/mqtt/v5/mqtt5_types.h>

#include <inttypes.h>

#define INITIAL_ENCODING_STEP_COUNT 64

int aws_mqtt5_encode_variable_length_integer(struct aws_byte_buf *buf, uint32_t value) {
    AWS_PRECONDITION(buf);

    if (value > AWS_MQTT5_MAXIMUM_VARIABLE_LENGTH_INTEGER) {
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    do {
        uint8_t encoded_byte = value % 128;
        value /= 128;
        if (value) {
            encoded_byte |= 128;
        }
        if (!aws_byte_buf_write_u8(buf, encoded_byte)) {
            return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
        }
    } while (value);

    return AWS_OP_SUCCESS;
}

int aws_mqtt5_get_variable_length_encode_size(size_t value, size_t *encode_size) {
    if (value > AWS_MQTT5_MAXIMUM_VARIABLE_LENGTH_INTEGER) {
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    if (value < 128) {
        *encode_size = 1;
    } else if (value < 16384) {
        *encode_size = 2;
    } else if (value < 2097152) {
        *encode_size = 3;
    } else {
        *encode_size = 4;
    }

    return AWS_OP_SUCCESS;
}

/* helper functions that add a single type of encoding step to the list of steps in an encoder */

void aws_mqtt5_encoder_push_step_u8(struct aws_mqtt5_encoder *encoder, uint8_t value) {
    struct aws_mqtt5_encoding_step step;
    AWS_ZERO_STRUCT(step);

    step.type = AWS_MQTT5_EST_U8;
    step.value.value_u8 = value;

    aws_array_list_push_back(&encoder->encoding_steps, &step);
}

void aws_mqtt5_encoder_push_step_u16(struct aws_mqtt5_encoder *encoder, uint16_t value) {
    struct aws_mqtt5_encoding_step step;
    AWS_ZERO_STRUCT(step);

    step.type = AWS_MQTT5_EST_U16;
    step.value.value_u16 = value;

    aws_array_list_push_back(&encoder->encoding_steps, &step);
}

void aws_mqtt5_encoder_push_step_u32(struct aws_mqtt5_encoder *encoder, uint32_t value) {
    struct aws_mqtt5_encoding_step step;
    AWS_ZERO_STRUCT(step);

    step.type = AWS_MQTT5_EST_U32;
    step.value.value_u32 = value;

    aws_array_list_push_back(&encoder->encoding_steps, &step);
}

int aws_mqtt5_encoder_push_step_vli(struct aws_mqtt5_encoder *encoder, uint32_t value) {
    if (value > AWS_MQTT5_MAXIMUM_VARIABLE_LENGTH_INTEGER) {
        return aws_raise_error(AWS_ERROR_MQTT5_ENCODE_INVALID_VARIABLE_LENGTH_INTEGER);
    }

    struct aws_mqtt5_encoding_step step;
    AWS_ZERO_STRUCT(step);

    step.type = AWS_MQTT5_EST_VLI;
    step.value.value_u32 = value;

    aws_array_list_push_back(&encoder->encoding_steps, &step);

    return AWS_OP_SUCCESS;
}

void aws_mqtt5_encoder_push_step_cursor(struct aws_mqtt5_encoder *encoder, struct aws_byte_cursor value) {
    struct aws_mqtt5_encoding_step step;
    AWS_ZERO_STRUCT(step);

    step.type = AWS_MQTT5_EST_CURSOR;
    step.value.value_cursor = value;

    aws_array_list_push_back(&encoder->encoding_steps, &step);
}

/*
 * All size calculations are done with size_t.  We assume that view validation will catch and fail all packets
 * that violate length constraints either from the MQTT5 spec or additional constraints that we impose on packets
 * to ensure that the size calculations do not need to perform checked arithmetic.  The only place where we need
 * to use checked arithmetic is a PUBLISH packet when combining the payload size and "sizeof everything else"
 *
 * The additional beyond-spec constraints we apply to view validation ensure our results actually fit in 32 bits.
 *
 * TODO: view validation does not currently check the total length of the packet.
 */
size_t aws_mqtt5_compute_user_property_encode_length(
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

void aws_mqtt5_add_user_property_encoding_steps(
    struct aws_mqtt5_encoder *encoder,
    const struct aws_mqtt5_user_property *user_properties,
    size_t user_property_count) {
    for (size_t i = 0; i < user_property_count; ++i) {
        const struct aws_mqtt5_user_property *property = &user_properties[i];

        /* https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901054 */
        ADD_ENCODE_STEP_U8(encoder, AWS_MQTT5_PROPERTY_TYPE_USER_PROPERTY);
        ADD_ENCODE_STEP_U16(encoder, (uint16_t)property->name.len);
        ADD_ENCODE_STEP_CURSOR(encoder, property->name);
        ADD_ENCODE_STEP_U16(encoder, (uint16_t)property->value.len);
        ADD_ENCODE_STEP_CURSOR(encoder, property->value);
    }
}

static int s_aws_mqtt5_encoder_begin_pingreq(struct aws_mqtt5_encoder *encoder, void *view) {
    (void)view;

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_GENERAL,
        "(%p) mqtt5 client encoder - setting up encode for a PINGREQ packet",
        (void *)encoder->client);

    /* A ping is just a fixed header with a 0-valued remaining length which we encode as a 0 u8 rather than a 0 vli */
    ADD_ENCODE_STEP_U8(encoder, aws_mqtt5_compute_fixed_header_byte1(AWS_MQTT5_PT_PINGREQ, 0));
    ADD_ENCODE_STEP_U8(encoder, 0);

    return AWS_OP_SUCCESS;
}

static int s_compute_disconnect_variable_length_fields(
    struct aws_mqtt5_packet_disconnect_view *disconnect_view,
    uint32_t *total_remaining_length,
    uint32_t *property_length) {
    size_t local_property_length = aws_mqtt5_compute_user_property_encode_length(
        disconnect_view->user_properties, disconnect_view->user_property_count);

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

static int s_aws_mqtt5_encoder_begin_disconnect(struct aws_mqtt5_encoder *encoder, void *view) {

    struct aws_mqtt5_packet_disconnect_view *disconnect_view = view;

    uint32_t total_remaining_length = 0;
    uint32_t property_length = 0;
    if (s_compute_disconnect_variable_length_fields(disconnect_view, &total_remaining_length, &property_length)) {
        int error_code = aws_last_error();
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_GENERAL,
            "(%p) mqtt5 client encoder - failed to compute variable length values for DISCONNECT packet with error "
            "%d(%s)",
            (void *)encoder->client,
            error_code,
            aws_error_debug_str(error_code));
        return AWS_OP_ERR;
    }

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_GENERAL,
        "(%p) mqtt5 client encoder - setting up encode for a DISCONNECT packet with remaining length %" PRIu32,
        (void *)encoder->client,
        total_remaining_length);

    ADD_ENCODE_STEP_U8(encoder, aws_mqtt5_compute_fixed_header_byte1(AWS_MQTT5_PT_DISCONNECT, 0));
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

        aws_mqtt5_add_user_property_encoding_steps(
            encoder, disconnect_view->user_properties, disconnect_view->user_property_count);
    }

    return AWS_OP_SUCCESS;
}

static int s_compute_connect_variable_length_fields(
    struct aws_mqtt5_packet_connect_view *connect_view,
    size_t *total_remaining_length,
    size_t *connect_property_length,
    size_t *will_property_length) {

    size_t connect_property_section_length =
        aws_mqtt5_compute_user_property_encode_length(connect_view->user_properties, connect_view->user_property_count);

    ADD_OPTIONAL_U32_PROPERTY_LENGTH(connect_view->session_expiry_interval_seconds, connect_property_section_length);
    ADD_OPTIONAL_U16_PROPERTY_LENGTH(connect_view->receive_maximum, connect_property_section_length);
    ADD_OPTIONAL_U32_PROPERTY_LENGTH(connect_view->maximum_packet_size_bytes, connect_property_section_length);
    ADD_OPTIONAL_U16_PROPERTY_LENGTH(connect_view->topic_alias_maximum, connect_property_section_length);
    ADD_OPTIONAL_U8_PROPERTY_LENGTH(connect_view->request_response_information, connect_property_section_length);
    ADD_OPTIONAL_U8_PROPERTY_LENGTH(connect_view->request_problem_information, connect_property_section_length);
    ADD_OPTIONAL_CURSOR_PROPERTY_LENGTH(connect_view->authentication_method, connect_property_section_length);
    ADD_OPTIONAL_CURSOR_PROPERTY_LENGTH(connect_view->authentication_data, connect_property_section_length);

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

        *will_property_length = aws_mqtt5_compute_user_property_encode_length(
            publish_view->user_properties, publish_view->user_property_count);

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
        payload_length += 2 + publish_view->payload.len;
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

static int s_aws_mqtt5_encoder_begin_connect(struct aws_mqtt5_encoder *encoder, void *view) {

    struct aws_mqtt5_packet_connect_view *connect_view = view;
    const struct aws_mqtt5_packet_publish_view *will = connect_view->will;

    size_t total_remaining_length = 0;
    size_t connect_property_length = 0;
    size_t will_property_length = 0;
    if (s_compute_connect_variable_length_fields(
            connect_view, &total_remaining_length, &connect_property_length, &will_property_length)) {
        int error_code = aws_last_error();
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_GENERAL,
            "(%p) mqtt5 client encoder - failed to compute variable length values for CONNECT packet with error %d(%s)",
            (void *)encoder->client,
            error_code,
            aws_error_debug_str(error_code));
        return AWS_OP_ERR;
    }

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_GENERAL,
        "(%p) mqtt5 client encoder - setting up encode for a CONNECT packet with remaining length %zu",
        (void *)encoder->client,
        total_remaining_length);

    uint32_t total_remaining_length_u32 = (uint32_t)total_remaining_length;
    uint32_t connect_property_length_u32 = (uint32_t)connect_property_length;
    uint32_t will_property_length_u32 = (uint32_t)will_property_length;

    ADD_ENCODE_STEP_U8(encoder, aws_mqtt5_compute_fixed_header_byte1(AWS_MQTT5_PT_CONNECT, 0));
    ADD_ENCODE_STEP_VLI(encoder, total_remaining_length_u32);
    ADD_ENCODE_STEP_CURSOR(encoder, g_aws_mqtt5_connect_protocol_cursor);
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
    ADD_ENCODE_STEP_OPTIONAL_CURSOR_PROPERTY(
        encoder, AWS_MQTT5_PROPERTY_TYPE_AUTHENTICATION_METHOD, connect_view->authentication_method);
    ADD_ENCODE_STEP_OPTIONAL_CURSOR_PROPERTY(
        encoder, AWS_MQTT5_PROPERTY_TYPE_AUTHENTICATION_DATA, connect_view->authentication_data);

    aws_mqtt5_add_user_property_encoding_steps(
        encoder, connect_view->user_properties, connect_view->user_property_count);

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

        aws_mqtt5_add_user_property_encoding_steps(encoder, will->user_properties, will->user_property_count);

        ADD_ENCODE_STEP_LENGTH_PREFIXED_CURSOR(encoder, will->topic);
        ADD_ENCODE_STEP_U16(encoder, (uint16_t)will->payload.len);
        ADD_ENCODE_STEP_CURSOR(encoder, will->payload);
    }

    ADD_ENCODE_STEP_OPTIONAL_LENGTH_PREFIXED_CURSOR(encoder, connect_view->username);
    ADD_ENCODE_STEP_OPTIONAL_LENGTH_PREFIXED_CURSOR(encoder, connect_view->password);

    return AWS_OP_SUCCESS;
}

static enum aws_mqtt5_encoding_result s_execute_encode_step(
    struct aws_mqtt5_encoder *encoder,
    struct aws_mqtt5_encoding_step *step,
    struct aws_byte_buf *buffer) {
    size_t buffer_room = buffer->capacity - buffer->len;

    switch (step->type) {
        case AWS_MQTT5_EST_U8:
            if (buffer_room < 1) {
                return AWS_MQTT5_ER_OUT_OF_ROOM;
            }

            aws_byte_buf_write_u8(buffer, step->value.value_u8);

            return AWS_MQTT5_ER_FINISHED;

        case AWS_MQTT5_EST_U16:
            if (buffer_room < 2) {
                return AWS_MQTT5_ER_OUT_OF_ROOM;
            }

            aws_byte_buf_write_be16(buffer, step->value.value_u16);

            return AWS_MQTT5_ER_FINISHED;

        case AWS_MQTT5_EST_U32:
            if (buffer_room < 4) {
                return AWS_MQTT5_ER_OUT_OF_ROOM;
            }

            aws_byte_buf_write_be32(buffer, step->value.value_u32);

            return AWS_MQTT5_ER_FINISHED;

        case AWS_MQTT5_EST_VLI:
            /* being lazy here and just assuming the worst case */
            if (buffer_room < 4) {
                return AWS_MQTT5_ER_OUT_OF_ROOM;
            }

            /* This can't fail.  We've already validated the vli value when we made the step */
            aws_mqtt5_encode_variable_length_integer(buffer, step->value.value_u32);

            return AWS_MQTT5_ER_FINISHED;

        case AWS_MQTT5_EST_CURSOR:
            if (buffer_room < 1) {
                return AWS_MQTT5_ER_OUT_OF_ROOM;
            }

            aws_byte_buf_write_to_capacity(buffer, &step->value.value_cursor);

            return (step->value.value_cursor.len == 0) ? AWS_MQTT5_ER_FINISHED : AWS_MQTT5_ER_OUT_OF_ROOM;

        case AWS_MQTT5_EST_STREAM:
            while (buffer->len < buffer->capacity) {
                if (aws_input_stream_read(step->value.value_stream, buffer)) {
                    int error_code = aws_last_error();
                    AWS_LOGF_ERROR(
                        AWS_LS_MQTT5_GENERAL,
                        "(%p) mqtt5 client encoder - failed to read from stream with error %d(%s)",
                        (void *)encoder->client,
                        error_code,
                        aws_error_debug_str(error_code));
                    return AWS_MQTT5_ER_ERROR;
                }

                struct aws_stream_status status;
                if (aws_input_stream_get_status(step->value.value_stream, &status)) {
                    int error_code = aws_last_error();
                    AWS_LOGF_ERROR(
                        AWS_LS_MQTT5_GENERAL,
                        "(%p) mqtt5 client encoder - failed to query stream status with error %d(%s)",
                        (void *)encoder->client,
                        error_code,
                        aws_error_debug_str(error_code));
                    return AWS_MQTT5_ER_ERROR;
                }

                if (status.is_end_of_stream) {
                    return AWS_MQTT5_ER_FINISHED;
                }
            }

            if (buffer->len == buffer->capacity) {
                return AWS_MQTT5_ER_OUT_OF_ROOM;
            }

            /* fall through intentional */
    }

    /* shouldn't be reachable */
    AWS_LOGF_ERROR(
        AWS_LS_MQTT5_GENERAL, "(%p) mqtt5 client encoder - reached an unreachable state", (void *)encoder->client);
    aws_raise_error(AWS_ERROR_INVALID_STATE);
    return AWS_MQTT5_ER_ERROR;
}

enum aws_mqtt5_encoding_result aws_mqtt5_encoder_encode_to_buffer(
    struct aws_mqtt5_encoder *encoder,
    struct aws_byte_buf *buffer) {

    enum aws_mqtt5_encoding_result result = AWS_MQTT5_ER_FINISHED;
    size_t step_count = aws_array_list_length(&encoder->encoding_steps);
    while (result == AWS_MQTT5_ER_FINISHED && encoder->current_encoding_step_index < step_count) {
        struct aws_mqtt5_encoding_step *step = NULL;
        aws_array_list_get_at_ptr(&encoder->encoding_steps, (void **)&step, encoder->current_encoding_step_index);

        result = s_execute_encode_step(encoder, step, buffer);
        if (result == AWS_MQTT5_ER_FINISHED) {
            encoder->current_encoding_step_index++;
        }
    }

    if (result == AWS_MQTT5_ER_FINISHED) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_GENERAL,
            "(%p) mqtt5 client encoder - finished encoding current operation",
            (void *)encoder->client);
        aws_mqtt5_encoder_reset(encoder);
    }

    return result;
}

static struct aws_mqtt5_encoder_vtable s_aws_mqtt5_encoder_default_vtable = {
    .encoders_by_packet_type =
        {
            NULL,                                  /* RESERVED = 0 */
            &s_aws_mqtt5_encoder_begin_connect,    /* CONNECT */
            NULL,                                  /* CONNACK */
            NULL,                                  /* PUBLISH */
            NULL,                                  /* PUBACK */
            NULL,                                  /* PUBREC */
            NULL,                                  /* PUBREL */
            NULL,                                  /* PUBCOMP */
            NULL,                                  /* SUBSCRIBE */
            NULL,                                  /* SUBACK */
            NULL,                                  /* UNSUBSCRIBE */
            NULL,                                  /* UNSUBACK */
            &s_aws_mqtt5_encoder_begin_pingreq,    /* PINGREQ */
            NULL,                                  /* PINGRESP */
            &s_aws_mqtt5_encoder_begin_disconnect, /* DISCONNECT */
            NULL                                   /* AUTH */
        },
};

const struct aws_mqtt5_encoder_vtable *g_aws_mqtt5_encoder_default_vtable = &s_aws_mqtt5_encoder_default_vtable;

int aws_mqtt5_encoder_init(
    struct aws_mqtt5_encoder *encoder,
    struct aws_allocator *allocator,
    struct aws_mqtt5_client *client) {
    AWS_ZERO_STRUCT(*encoder);

    encoder->client = client;
    encoder->vtable = g_aws_mqtt5_encoder_default_vtable;

    if (aws_array_list_init_dynamic(
            &encoder->encoding_steps, allocator, INITIAL_ENCODING_STEP_COUNT, sizeof(struct aws_mqtt5_encoding_step))) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

int aws_mqtt5_encoder_init_with_vtable(
    struct aws_mqtt5_encoder *encoder,
    struct aws_allocator *allocator,
    struct aws_mqtt5_client *client,
    struct aws_mqtt5_encoder_vtable *vtable) {
    AWS_ZERO_STRUCT(*encoder);

    encoder->client = client;
    encoder->vtable = vtable;

    if (aws_array_list_init_dynamic(
            &encoder->encoding_steps, allocator, INITIAL_ENCODING_STEP_COUNT, sizeof(struct aws_mqtt5_encoding_step))) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

void aws_mqtt5_encoder_clean_up(struct aws_mqtt5_encoder *encoder) {
    aws_array_list_clean_up(&encoder->encoding_steps);
}

void aws_mqtt5_encoder_reset(struct aws_mqtt5_encoder *encoder) {
    aws_array_list_clear(&encoder->encoding_steps);
    encoder->current_encoding_step_index = 0;
}

int aws_mqtt5_encoder_append_packet_encoding(
    struct aws_mqtt5_encoder *encoder,
    enum aws_mqtt5_packet_type packet_type,
    void *packet_view) {
    aws_mqtt5_encode_begin_packet_type_fn *encoding_fn = encoder->vtable->encoders_by_packet_type[packet_type];
    if (encoding_fn == NULL) {
        /* TODO: I think the right error for this is in another branch atm, fix after merging */
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    return (*encoding_fn)(encoder, packet_view);
}
