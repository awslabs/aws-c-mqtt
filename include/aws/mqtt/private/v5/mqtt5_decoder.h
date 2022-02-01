/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#ifndef AWS_MQTT_MQTT5_DECODER_H
#define AWS_MQTT_MQTT5_DECODER_H

#include <aws/mqtt/mqtt.h>

#include <aws/common/byte_buf.h>
#include <aws/mqtt/private/v5/mqtt5_options_storage.h>
#include <aws/mqtt/v5/mqtt5_types.h>

struct aws_mqtt5_client;

enum aws_mqtt5_decoder_state {
    AWS_MQTT5_DS_READ_PACKET_TYPE,
    AWS_MQTT5_DS_READ_REMAINING_LENGTH,
    AWS_MQTT5_DS_READ_PACKET,

    /* NYI: publish-specific to support streaming payloads */
    AWS_MQTT5_DS_READ_PUBLISH_TOPIC_LENGTH,
    AWS_MQTT5_DS_READ_PUBLISH_TOPIC,
    AWS_MQTT5_DS_READ_PUBLISH_PROPERTIES_REMAINING_LENGTH,
    AWS_MQTT5_DS_READ_PUBLISH_PRE_PAYLOAD,
    AWS_MQTT5_DS_READ_PUBLISH_PAYLOAD,
};

typedef int(aws_mqtt5_on_packet_received_fn)(enum aws_mqtt5_packet_type type, void *packet_view);
typedef void(aws_mqtt5_on_publish_payload_data_fn)(void *packet_view, struct aws_byte_cursor payload);

struct aws_mqtt5_decoder_options {
    struct aws_mqtt5_client *client;

    aws_mqtt5_on_packet_received_fn *on_packet_received;
    aws_mqtt5_on_publish_payload_data_fn *on_publish_payload_data;
};

struct aws_mqtt5_decoder {
    struct aws_mqtt5_decoder_options options;
    struct aws_allocator *allocator;

    enum aws_mqtt5_decoder_state state;

    /* decode output/scratch space */
    struct aws_byte_buf scratch_space;
    size_t current_step_scratch_offset;

    enum aws_mqtt5_packet_type packet_type;
    uint32_t total_length;
    uint32_t remaining_length;

    /* publish-specific to support streaming payloads */
    uint32_t publish_topic_length;
    struct aws_byte_cursor publish_topic;
    uint32_t publish_properties_remaining_length;
};

AWS_EXTERN_C_BEGIN

AWS_MQTT_API int aws_mqtt5_decoder_init(
    struct aws_mqtt5_decoder *decoder,
    struct aws_allocator *allocator,
    struct aws_mqtt5_decoder_options *options);

AWS_MQTT_API void aws_mqtt5_decoder_clean_up(struct aws_mqtt5_decoder *decoder);

AWS_MQTT_API int aws_mqtt5_decoder_on_data_received(struct aws_mqtt5_decoder *decoder, struct aws_byte_cursor data);

AWS_EXTERN_C_END

/* Decode helpers */

enum aws_mqtt5_decode_vli_result_type {
    AWS_MQTT5_DVRT_MORE_DATA,
    AWS_MQTT5_DVRT_SUCCESS,
    AWS_MQTT5_DVRT_ERROR,
};

AWS_EXTERN_C_BEGIN

AWS_MQTT_API enum aws_mqtt5_decode_vli_result_type aws_mqtt5_decode_vli(struct aws_byte_cursor *cursor, uint32_t *dest);

AWS_MQTT_API int aws_mqtt5_decode_user_property(
    struct aws_byte_cursor *packet_cursor,
    struct aws_mqtt5_user_property_set *properties);

AWS_EXTERN_C_END

/* Decode macros */

/*
 * u8 and u16 decode are a little different in order to support encoded values that are widened to larger storage.
 * To make that safe, we decode to a local and then assign the local to the final spot.  There should be no
 * complaints as long as the implicit conversion is the same size or wider.
 *
 * Some u8 examples include qos (one byte encode -> int-based enum) and various reason codes
 * Some u16 examples include cursor lengths decoded directly into a cursor's len field (u16 -> size_t)
 */
#define AWS_MQTT5_DECODE_U8(cursor_ptr, u8_ptr, error_label)                                                           \
    {                                                                                                                  \
        uint8_t decoded_value = 0;                                                                                     \
        if (!aws_byte_cursor_read_u8((cursor_ptr), (&decoded_value))) {                                                \
            goto error_label;                                                                                          \
        }                                                                                                              \
        *u8_ptr = decoded_value;                                                                                       \
    }

#define AWS_MQTT5_DECODE_U8_OPTIONAL(cursor_ptr, u8_ptr, u8_ptr_ptr, error_label)                                      \
    AWS_MQTT5_DECODE_U8(cursor_ptr, u8_ptr, error_label);                                                              \
    *(u8_ptr_ptr) = (u8_ptr);

#define AWS_MQTT5_DECODE_U16(cursor_ptr, u16_ptr, error_label)                                                         \
    {                                                                                                                  \
        uint16_t decoded_value = 0;                                                                                    \
        if (!aws_byte_cursor_read_be16((cursor_ptr), (&decoded_value))) {                                              \
            goto error_label;                                                                                          \
        }                                                                                                              \
        *u16_ptr = decoded_value;                                                                                      \
    }

#define AWS_MQTT5_DECODE_U16_PREFIX(cursor_ptr, u16_ptr, error_label)                                                  \
    AWS_MQTT5_DECODE_U16((cursor_ptr), (u16_ptr), error_label);                                                        \
    if (cursor_ptr->len < *(u16_ptr)) {                                                                                \
        aws_raise_error(AWS_ERROR_MQTT5_DECODE_PROTOCOL_ERROR);                                                        \
        goto error_label;                                                                                              \
    }

#define AWS_MQTT5_DECODE_U16_OPTIONAL(cursor_ptr, u16_ptr, u16_ptr_ptr, error_label)                                   \
    AWS_MQTT5_DECODE_U16((cursor_ptr), u16_ptr, error_label);                                                          \
    *(u16_ptr_ptr) = (u16_ptr);

#define AWS_MQTT5_DECODE_U32(cursor_ptr, u32_ptr, error_label)                                                         \
    if (!aws_byte_cursor_read_be32((cursor_ptr), (u32_ptr))) {                                                         \
        goto error_label;                                                                                              \
    }

#define AWS_MQTT5_DECODE_U32_OPTIONAL(cursor_ptr, u32_ptr, u32_ptr_ptr, error_label)                                   \
    AWS_MQTT5_DECODE_U32((cursor_ptr), u32_ptr, error_label);                                                          \
    *(u32_ptr_ptr) = (u32_ptr);

#define AWS_MQTT5_DECODE_VLI(cursor_ptr, u32_ptr, error_label)                                                         \
    if (AWS_MQTT5_DVRT_SUCCESS != aws_mqtt5_decode_vli((cursor_ptr), (u32_ptr))) {                                     \
        goto error_label;                                                                                              \
    }

#define AWS_MQTT5_DECODE_LENGTH_PREFIXED_CURSOR(cursor_ptr, dest_cursor_ptr, error_label)                              \
    {                                                                                                                  \
        uint16_t prefix_length = 0;                                                                                    \
        AWS_MQTT5_DECODE_U16_PREFIX((cursor_ptr), &prefix_length, error_label)                                         \
                                                                                                                       \
        *(dest_cursor_ptr) = aws_byte_cursor_advance((cursor_ptr), prefix_length);                                     \
    }

#define AWS_MQTT5_DECODE_LENGTH_PREFIXED_CURSOR_OPTIONAL(                                                              \
    cursor_ptr, dest_cursor_ptr, dest_cursor_ptr_ptr, error_label)                                                     \
    AWS_MQTT5_DECODE_LENGTH_PREFIXED_CURSOR((cursor_ptr), (dest_cursor_ptr), error_label)                              \
    *(dest_cursor_ptr_ptr) = (dest_cursor_ptr);

#endif /* AWS_MQTT_MQTT5_DECODER_H */
