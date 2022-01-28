/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#ifndef AWS_MQTT_MQTT5_DECODER_H
#define AWS_MQTT_MQTT5_DECODER_H

#include <aws/mqtt/mqtt.h>

#include <aws/mqtt/v5/mqtt5_types.h>

struct aws_mqtt5_client;

enum aws_mqtt5_decoder_state {
    AWS_MQTT5_DS_READ_PACKET_TYPE,
    AWS_MQTT5_DS_READ_REMAINING_LENGTH,
    AWS_MQTT5_DS_READ_PACKET,
    AWS_MQTT5_DS_DECODE_PACKET,
};

struct aws_mqtt5_decoder_packet_state {
    enum aws_mqtt5_packet_type type;
    size_t total_length;
    size_t remaining_length;
};

typedef int(
    mqtt5_client_packet_callback_fn)(struct aws_mqtt5_client *client, enum aws_mqtt5_packet_type, void *packet_storage);

typedef int(mqtt5_client_publish_payload_callback_fn)(
    struct aws_mqtt5_client *client,
    struct aws_mqtt5_packet_publish_view *publish_view,
    struct aws_byte_cursor payload_bytes);

struct aws_mqtt5_decoder_options {
    struct aws_mqtt5_client *client;
    mqtt5_client_packet_callback_fn *packet_callback;
    mqtt5_client_publish_payload_callback_fn *publish_payload_callback;
};

struct aws_mqtt5_decoder {
    struct aws_mqtt5_decoder_options config;

    enum aws_mqtt5_decoder_state state;
    struct aws_mqtt5_decoder_packet_state packet_state;

    struct aws_byte_buf pre_decode_buffer;
    struct aws_byte_cursor pre_decode_cursor;
};

AWS_EXTERN_C_BEGIN

AWS_MQTT_API int aws_mqtt5_decoder_init(struct aws_mqtt5_decoder *decoder, struct aws_allocator *allocator, struct aws_mqtt5_decoder_options *options);

AWS_MQTT_API void aws_mqtt5_decoder_clean_up(struct aws_mqtt5_decoder *decoder);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_MQTT5_DECODER_H */
