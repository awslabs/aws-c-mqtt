#ifndef AWS_MQTT_PRIVATE_MQTT311_DECODER_H
#define AWS_MQTT_PRIVATE_MQTT311_DECODER_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

#include <aws/common/byte_buf.h>

typedef int(
    packet_handler_fn)(struct aws_byte_cursor message_cursor, void *user_data);

struct aws_mqtt_client_connection_packet_handlers {
    packet_handler_fn *handlers_by_packet_type[16];
};

enum aws_mqtt_311_decoder_state_type {
    AWS_MDST_READ_FIRST_BYTE,
    AWS_MDST_READ_REMAINING_LENGTH,
    AWS_MDST_READ_BODY,
    AWS_MDST_PROTOCOL_ERROR,
};

struct aws_mqtt311_decoder_options {
    const struct aws_mqtt_client_connection_packet_handlers *packet_handlers;
    void *handler_user_data;
};

struct aws_mqtt311_decoder {
    struct aws_mqtt311_decoder_options config;

    enum aws_mqtt_311_decoder_state_type state;
    size_t total_packet_length;
    struct aws_byte_buf packet_buffer;
};

AWS_EXTERN_C_BEGIN

AWS_MQTT_API void aws_mqtt311_decoder_init(struct aws_mqtt311_decoder *decoder, struct aws_allocator *allocator, const struct aws_mqtt311_decoder_options *options);

AWS_MQTT_API void aws_mqtt311_decoder_clean_up(struct aws_mqtt311_decoder *decoder);

AWS_MQTT_API int aws_mqtt311_decoder_on_bytes_received(struct aws_mqtt311_decoder *decoder, struct aws_byte_cursor data);

AWS_MQTT_API void aws_mqtt311_decoder_reset_for_new_connection(struct aws_mqtt311_decoder *decoder);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_PRIVATE_MQTT311_DECODER_H */

