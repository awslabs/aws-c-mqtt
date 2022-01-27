/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#ifndef AWS_MQTT_MQTT5_ENCODER_H
#define AWS_MQTT_MQTT5_ENCODER_H

#include <aws/mqtt/mqtt.h>

#include <aws/common/array_list.h>
#include <aws/common/byte_buf.h>

struct aws_mqtt5_packet_connect_view;
struct aws_mqtt5_packet_disconnect_view;

enum aws_mqtt5_encoding_step_type {
    /* encode a single byte */
    AWS_MQTT5_EST_U8,

    /* encode a 16 bit unsigned integer in network order */
    AWS_MQTT5_EST_U16,

    /* encode a 32 bit unsigned integer in network order */
    AWS_MQTT5_EST_U32,

    /* encode a 32 bit unsigned integer using variable length encoding */
    AWS_MQTT5_EST_VLI,

    /* encode an array of bytes as referenced by a cursor */
    AWS_MQTT5_EST_CURSOR,

    /* encode a stream of bytes */
    AWS_MQTT5_EST_STREAM,
};

struct aws_mqtt5_encoding_step {
    enum aws_mqtt5_encoding_step_type type;
    union {
        uint8_t value_u8;
        uint16_t value_u16;
        uint32_t value_u32;
        struct aws_byte_cursor value_cursor;
        struct aws_input_stream *value_stream;
    } value;
};

struct aws_mqtt5_encoder {
    struct aws_array_list encoding_steps;
    size_t current_encoding_step_index;
};

enum aws_mqtt5_encoding_result {
    AWS_MQTT5_ER_ERROR,
    AWS_MQTT5_ER_FINISHED,
    AWS_MQTT5_ER_OUT_OF_ROOM,
};

AWS_EXTERN_C_BEGIN

AWS_MQTT_API int aws_mqtt5_encoder_init(struct aws_mqtt5_encoder *encoder, struct aws_allocator *allocator);

AWS_MQTT_API void aws_mqtt5_encoder_clean_up(struct aws_mqtt5_encoder *encoder);

AWS_MQTT_API int aws_mqtt5_encoder_begin_connect(
    struct aws_mqtt5_encoder *encoder,
    struct aws_mqtt5_packet_connect_view *connect_view);

AWS_MQTT_API int aws_mqtt5_encoder_begin_disconnect(
    struct aws_mqtt5_encoder *encoder,
    struct aws_mqtt5_packet_disconnect_view *disconnect_view);

AWS_MQTT_API int aws_mqtt5_encoder_begin_ping(struct aws_mqtt5_encoder *encoder);

AWS_MQTT_API enum aws_mqtt5_encoding_result aws_mqtt5_encoder_encode_to_buffer(
    struct aws_mqtt5_encoder *encoder,
    struct aws_byte_buf *buffer);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_MQTT5_ENCODER_H */
