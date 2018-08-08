/*
 * Copyright 2010-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

#include <aws/mqtt/private/client_channel_handler.h>

#include <aws/io/channel.h>

#include <aws/mqtt/private/packets.h>

/**
 * Handles incoming messages from the server.
 */
static int s_process_read_message(
    struct aws_channel_handler *handler,
    struct aws_channel_slot *slot,
    struct aws_io_message *message) {

    struct aws_mqtt_client_impl *impl = handler->impl;

    if (message->message_type != AWS_IO_MESSAGE_APPLICATION_DATA || message->message_data.len < 1) {
        return AWS_OP_ERR;
    }

    struct aws_byte_cursor message_cursor = aws_byte_cursor_from_buf(&message->message_data);

    enum aws_mqtt_packet_type type = aws_mqtt_get_packet_type(message->message_data.buffer);
    switch (type) {
        case AWS_MQTT_PACKET_CONNACK: {

            struct aws_mqtt_packet_connack connack;
            aws_mqtt_packet_connack_decode(&message_cursor, &connack);

            if (impl->client->on_connect) {
                impl->client->on_connect(connack.connect_return_code, connack.session_present, impl->user_data);
            } else if (connack.connect_return_code != AWS_MQTT_CONNECT_ACCEPTED) {
                /* TODO: aws_mqtt_disconnect() */
            }

            break;
        }

        case AWS_MQTT_PACKET_CONNECT:
        default:
            /* These packets should never be sent by the server */
            return AWS_OP_ERR;
    }

    /* Do cleanup */
    aws_channel_slot_increment_read_window(slot, message->message_data.len);
    aws_channel_release_message_to_pool(slot->channel, message);

    return AWS_OP_SUCCESS;
}

static int s_shutdown(
    struct aws_channel_handler *handler,
    struct aws_channel_slot *slot,
    enum aws_channel_direction dir,
    int error_code,
    bool free_scarce_resources_immediately) {

    struct aws_mqtt_client_impl *client_impl = handler->impl;
    struct aws_mqtt_client *client = client_impl->client;

    if (dir == AWS_CHANNEL_DIR_WRITE) {
        if (!free_scarce_resources_immediately) {

            /* Send the disconnect message */
            struct aws_mqtt_packet_connection disconnect;
            aws_mqtt_packet_disconnect_init(&disconnect);

            struct aws_io_message *message = aws_channel_acquire_message_from_pool(
                slot->channel, AWS_IO_MESSAGE_APPLICATION_DATA, disconnect.fixed_header.remaining_length + 3);
            if (!message) {
                return AWS_OP_ERR;
            }
            struct aws_byte_cursor message_cursor = {
                .ptr = message->message_data.buffer,
                .len = message->message_data.capacity,
            };

            if (aws_mqtt_packet_connection_encode(&message_cursor, &disconnect)) {
                return AWS_OP_ERR;
            }
            message->message_data.len = message->message_data.capacity - message_cursor.len;

            if (aws_channel_slot_send_message(slot, message, AWS_CHANNEL_DIR_WRITE)) {
                return AWS_OP_ERR;
            }
        }

        /* Alert the client we've shutdown */
        if (client->on_disconnect) {
            client->on_disconnect(error_code, client_impl->user_data);
        }
    }

    return aws_channel_slot_on_handler_shutdown_complete(slot, dir, error_code, free_scarce_resources_immediately);
}

static size_t s_initial_window_size(struct aws_channel_handler *handler) {

    (void)handler;

    return SIZE_MAX;
}

static void s_destroy(struct aws_channel_handler *handler) {

    /* This function should not do anything, as all shutdown will be handled by the channel shutdown routine. */
    (void)handler;

    struct aws_mqtt_client_impl *client_impl = handler->impl;
    struct aws_mqtt_client *client = client_impl->client;

    /* Clear all state from the connection */
    aws_hash_table_clear(&client_impl->subscriptions);

    /* Free all of the active subscriptions */
    aws_hash_table_clean_up(&client_impl->subscriptions);

    /* Frees all allocated memory */
    aws_mem_release(client_impl->allocator, client);
}

struct aws_channel_handler_vtable vtable = {
    .process_read_message = &s_process_read_message,
    .process_write_message = NULL,
    .increment_read_window = NULL,
    .shutdown = &s_shutdown,
    .initial_window_size = &s_initial_window_size,
    .destroy = &s_destroy,
};

int aws_mqtt_client_channel_handler_init(
    struct aws_channel_handler *handler,
    struct aws_mqtt_client_impl *client_impl) {

    AWS_ZERO_STRUCT(*handler);

    handler->alloc = client_impl->allocator;
    handler->vtable = vtable;
    handler->impl = client_impl;

    return AWS_OP_SUCCESS;
}
