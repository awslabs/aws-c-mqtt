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
 * @return
 */
AWS_MQTT_API int aws_mqtt5_encoder_init(struct aws_mqtt5_encoder *encoder, struct aws_allocator *allocator);

/**
 * Cleans up an mqtt5 encoder
 *
 * @param encoder encoder to free up all resources for
 */
AWS_MQTT_API void aws_mqtt5_encoder_clean_up(struct aws_mqtt5_encoder *encoder);

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

#endif /* AWS_MQTT_MQTT5_ENCODER_H */
