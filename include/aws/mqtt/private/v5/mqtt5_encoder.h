/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#ifndef AWS_MQTT_MQTT5_ENCODER_H
#define AWS_MQTT_MQTT5_ENCODER_H

#include <aws/mqtt/mqtt.h>

#include <aws/common/array_list.h>
#include <aws/common/byte_buf.h>

#include <aws/mqtt/v5/mqtt5_types.h>

struct aws_mqtt5_client;

/**
 * We encode packets by looking at all of the packet's values/properties and building a sequence of encoding steps.
 * Each encoding step is a simple, primitive operation of which there are two types:
 *   (1) encode an integer in some fashion (fixed width or variable length)
 *   (2) encode a raw sequence of bytes (either a cursor or a stream)
 *
 * Once the encoding step sequence is constructed, we do the actual encoding by iterating the sequence, performing
 * the steps.  This is interruptible/resumable, so we can perform encodings that span multiple buffers easily.
 */
enum aws_mqtt5_encoding_step_type {
    /* encode a single byte */
    AWS_MQTT5_EST_U8,

    /* encode a 16 bit unsigned integer in network order */
    AWS_MQTT5_EST_U16,

    /* encode a 32 bit unsigned integer in network order */
    AWS_MQTT5_EST_U32,

    /*
     * encode a 32 bit unsigned integer using MQTT variable length encoding. It is assumed that the 32 bit value has
     * already been checked against the maximum allowed value for variable length encoding.
     */
    AWS_MQTT5_EST_VLI,

    /*
     * encode an array of bytes as referenced by a cursor.  Most of the time this step is paired with either a prefix
     * specifying the number of bytes or a preceding variable length integer from which the data length can be
     * computed.
     */
    AWS_MQTT5_EST_CURSOR,

    /* encode a stream of bytes.  The same context that applies to cursor encoding above also applies here. */
    AWS_MQTT5_EST_STREAM,
};

/**
 * Elemental unit of packet encoding.
 */
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

/**
 * An encoder is just a list of steps and a current location for the encoding process within that list.
 */
struct aws_mqtt5_encoder {
    struct aws_mqtt5_client *client;
    struct aws_array_list encoding_steps;
    size_t current_encoding_step_index;
};

/**
 * Encoding proceeds until either
 *  (1) a fatal error is reached
 *  (2) the steps are done
 *  (3) no room is left in the buffer
 */
enum aws_mqtt5_encoding_result {
    /*
     * A fatal error state was reached during encoding.  This forces a connection shut down with no DISCONNECT.
     * An error can arise from several sources:
     *   (1) Bug in the encoder (length calculations, step calculations)
     *   (2) Bug in the view validation logic that is assumed to have caught any illegal/forbidden situations like
     *       values-too-big, etc...
     *   (3) System error when reading from a stream that is more than just a memory buffer
     *
     *  Regardless of the origin, the connection is in an unusable state once this happens.
     *
     *  If the encode function returns this value, aws last error will have an error value in it
     */
    AWS_MQTT5_ER_ERROR,

    /* All encoding steps in the encoder have been completed.  The encoder is ready for a new packet. */
    AWS_MQTT5_ER_FINISHED,

    /*
     * The buffer has been filled as closely to full as possible and there are still encoding steps remaining that
     * have not been completed.  It is technically possible to hit a permanent out-of-room state if the buffer size
     * is less than 4.  Don't do that.
     */
    AWS_MQTT5_ER_OUT_OF_ROOM,
};

AWS_EXTERN_C_BEGIN

/**
 * Initializes an mqtt5 encoder
 *
 * @param encoder encoder to initialize
 * @param allocator allocator to use for all memory allocation
 * @param client mqtt5 client this decoder belongs to.  Needed for logging.
 * @return
 */
AWS_MQTT_API int aws_mqtt5_encoder_init(
    struct aws_mqtt5_encoder *encoder,
    struct aws_allocator *allocator,
    struct aws_mqtt5_client *client);

/**
 * Cleans up an mqtt5 encoder
 *
 * @param encoder encoder to free up all resources for
 */
AWS_MQTT_API void aws_mqtt5_encoder_clean_up(struct aws_mqtt5_encoder *encoder);

/**
 * Resets the state on an mqtt5 encoder.  Ok to call after a failure to a packet _begin_packet() function.  Not ok to
 * call after a failed call to aws_mqtt5_encoder_encode_to_buffer()
 *
 * @param encoder encoder to reset
 * @return
 */
AWS_MQTT_API void aws_mqtt5_encoder_reset(struct aws_mqtt5_encoder *encoder);

/*
 * We intend that the client implementation only submits one packet at a time to the encoder, corresponding to the
 * current operation of the client.  This is an important property to maintain to allow us to correlate socket
 * completions with packets/operations sent.  It's the client's responsibility though; the encoder is dumb.
 *
 * The client will greedily use as much of an iomsg's buffer as it can if there are multiple operations (packets)
 * queued and there is sufficient room.
 */

/**
 * Asks the encoder to encode as much as it possibly can into the supplied buffer.
 *
 * @param encoder encoder to do the encoding
 * @param buffer where to encode into
 * @return result of the encoding process.  aws last error will be set appropriately.
 */
AWS_MQTT_API enum aws_mqtt5_encoding_result aws_mqtt5_encoder_encode_to_buffer(
    struct aws_mqtt5_encoder *encoder,
    struct aws_byte_buf *buffer);

/**
 * Prepares the encoder to encode a CONNECT packet by building the sequence of primitive encoding steps necessary
 * to do so.
 *
 * @param encoder encoder to add encoding steps to
 * @param connect_view view of the CONNECT packet to encode
 * @return success/failure
 */
AWS_MQTT_API int aws_mqtt5_encoder_begin_connect(
    struct aws_mqtt5_encoder *encoder,
    struct aws_mqtt5_packet_connect_view *connect_view);

/**
 * Prepares the encoder to encode a DISCONNECT packet by building the sequence of primitive encoding steps necessary
 * to do so.
 *
 * @param encoder encoder to add encoding steps to
 * @param disconnect_view view of the DISCONNECT packet to encode
 * @return success/failure
 */
AWS_MQTT_API int aws_mqtt5_encoder_begin_disconnect(
    struct aws_mqtt5_encoder *encoder,
    struct aws_mqtt5_packet_disconnect_view *disconnect_view);

/**
 * Prepares the encoder to encode a PINGREQ packet by building the sequence of primitive encoding steps necessary
 * to do so.
 *
 * @param encoder encoder to add encoding steps to
 * @return success/failure
 */
AWS_MQTT_API int aws_mqtt5_encoder_begin_pingreq(struct aws_mqtt5_encoder *encoder);

/* TODO: all the other packets save AUTH, PUBREC, PUBREL, PUBCOMP */

AWS_EXTERN_C_END

/******************************************************************************************************************
 *  Encoding helper functions and macros - placed in header so that test-only encoding has access
 ******************************************************************************************************************/

AWS_EXTERN_C_BEGIN

AWS_MQTT_API void aws_mqtt5_encoder_push_step_u8(struct aws_mqtt5_encoder *encoder, uint8_t value);

AWS_MQTT_API void aws_mqtt5_encoder_push_step_u16(struct aws_mqtt5_encoder *encoder, uint16_t value);

AWS_MQTT_API void aws_mqtt5_encoder_push_step_u32(struct aws_mqtt5_encoder *encoder, uint32_t value);

AWS_MQTT_API int aws_mqtt5_encoder_push_step_vli(struct aws_mqtt5_encoder *encoder, uint32_t value);

AWS_MQTT_API void aws_mqtt5_encoder_push_step_cursor(struct aws_mqtt5_encoder *encoder, struct aws_byte_cursor value);

AWS_MQTT_API size_t aws_mqtt5_compute_user_property_encode_length(
    const struct aws_mqtt5_user_property *properties,
    size_t user_property_count);

AWS_MQTT_API void aws_mqtt5_add_user_property_encoding_steps(
    struct aws_mqtt5_encoder *encoder,
    const struct aws_mqtt5_user_property *user_properties,
    size_t user_property_count);

AWS_EXTERN_C_END

/* macros to simplify encoding step list construction */

#define ADD_ENCODE_STEP_U8(encoder, value) aws_mqtt5_encoder_push_step_u8(encoder, (uint8_t)(value))
#define ADD_ENCODE_STEP_U16(encoder, value) aws_mqtt5_encoder_push_step_u16(encoder, (uint16_t)(value))
#define ADD_ENCODE_STEP_U32(encoder, value) aws_mqtt5_encoder_push_step_u32(encoder, (uint32_t)(value))
#define ADD_ENCODE_STEP_CURSOR(encoder, cursor) aws_mqtt5_encoder_push_step_cursor(encoder, (cursor))

#define ADD_ENCODE_STEP_VLI(encoder, value)                                                                            \
    if (aws_mqtt5_encoder_push_step_vli(encoder, (value))) {                                                           \
        return AWS_OP_ERR;                                                                                             \
    }

#define ADD_ENCODE_STEP_LENGTH_PREFIXED_CURSOR(encoder, cursor)                                                        \
    {                                                                                                                  \
        aws_mqtt5_encoder_push_step_u16(encoder, (cursor).len);                                                        \
        aws_mqtt5_encoder_push_step_cursor(encoder, (cursor));                                                         \
    }

#define ADD_ENCODE_STEP_OPTIONAL_LENGTH_PREFIXED_CURSOR(encoder, cursor_ptr)                                           \
    if (cursor_ptr != NULL) {                                                                                          \
        ADD_ENCODE_STEP_LENGTH_PREFIXED_CURSOR(encoder, *cursor_ptr);                                                  \
    }

/* Property-oriented macros for encode steps.  Properties have an additional prefix byte saying what their type is. */

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

#define ADD_ENCODE_STEP_OPTIONAL_CURSOR_PROPERTY(encoder, property_type, cursor_ptr)                                   \
    if ((cursor_ptr) != NULL) {                                                                                        \
        ADD_ENCODE_STEP_U8(encoder, property_type);                                                                    \
        ADD_ENCODE_STEP_U16(encoder, (cursor_ptr)->len);                                                               \
        ADD_ENCODE_STEP_CURSOR(encoder, *(cursor_ptr));                                                                \
    }

/*
 * Macros to simplify packet size calculations, which are significantly complicated by mqtt5's many optional
 * properties.
 */

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

#endif /* AWS_MQTT_MQTT5_ENCODER_H */
