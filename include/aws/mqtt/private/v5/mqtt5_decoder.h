/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#ifndef AWS_MQTT_MQTT5_DECODER_H
#define AWS_MQTT_MQTT5_DECODER_H

#include <aws/mqtt/mqtt.h>

#include <aws/mqtt/private/v5/mqtt5_options_storage.h>
#include <aws/mqtt/v5/mqtt5_types.h>

struct aws_mqtt5_client;

enum aws_mqtt5_decoder_state {
    AWS_MQTT5_DS_READ_PACKET_TYPE,
    AWS_MQTT5_DS_READ_REMAINING_LENGTH,
    AWS_MQTT5_DS_READ_PACKET,

    /* publish-specific to support streaming payloads */
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

#endif /* AWS_MQTT_MQTT5_DECODER_H */
