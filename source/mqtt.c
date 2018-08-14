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

#include <aws/mqtt/mqtt.h>

#include <aws/mqtt/private/client_channel_handler.h>
#include <aws/mqtt/private/packets.h>

#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/io/socket.h>
#include <aws/io/tls_channel_handler.h>

#include <assert.h>

static int s_mqtt_client_init(
    struct aws_client_bootstrap *bootstrap,
    int error_code,
    struct aws_channel *channel,
    void *user_data) {

    (void)bootstrap;

    if (error_code != AWS_OP_SUCCESS) {
        return AWS_OP_ERR;
    }

    struct aws_mqtt_client_connection *connection = user_data;

    /* Create the slot and handler */
    connection->slot = aws_channel_slot_new(channel);
    aws_channel_slot_insert_end(channel, connection->slot);
    aws_channel_slot_set_handler(connection->slot, &connection->handler);

    /* Send the connect packet */
    struct aws_mqtt_packet_connect connect;
    aws_mqtt_packet_connect_init(
        &connect, connection->client_id, connection->clean_session, connection->keep_alive_time);

    struct aws_io_message *message = aws_channel_acquire_message_from_pool(
        channel, AWS_IO_MESSAGE_APPLICATION_DATA, connect.fixed_header.remaining_length + 3);
    if (!message) {
        return AWS_OP_ERR;
    }
    struct aws_byte_cursor message_cursor = {
        .ptr = message->message_data.buffer,
        .len = message->message_data.capacity,
    };
    if (aws_mqtt_packet_connect_encode(&message_cursor, &connect)) {
        return AWS_OP_ERR;
    }
    message->message_data.len = message->message_data.capacity - message_cursor.len;

    if (aws_channel_slot_send_message(connection->slot, message, AWS_CHANNEL_DIR_WRITE)) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

static int s_mqtt_client_shutdown(
    struct aws_client_bootstrap *bootstrap,
    int error_code,
    struct aws_channel *channel,
    void *user_data) {

    (void)bootstrap;
    (void)channel;

    struct aws_mqtt_client_connection *connection = user_data;

    /* Alert the connection we've shutdown */
    MQTT_CALL_CALLBACK(connection, on_disconnect, error_code);

    return AWS_OP_SUCCESS;
}

struct aws_mqtt_client_connection *aws_mqtt_client_connection_new(
    struct aws_allocator *allocator,
    struct aws_mqtt_client *client,
    struct aws_mqtt_client_connection_callbacks callbacks,
    struct aws_socket_endpoint *endpoint,
    struct aws_tls_connection_options *tls_options,
    struct aws_byte_cursor client_id,
    bool clean_session,
    uint16_t keep_alive_time) {

    assert(allocator);
    assert(client);
    assert(!tls_options || client->client_bootstrap->tls_ctx);

    struct aws_mqtt_client_connection *connection =
        aws_mem_acquire(allocator, sizeof(struct aws_mqtt_client_connection));

    if (!client) {

        return NULL;
    }

    /* Initialize the client */
    AWS_ZERO_STRUCT(*connection);
    connection->allocator = allocator;
    connection->callbacks = callbacks;
    connection->state = AWS_MQTT_CLIENT_STATE_CONNECTING;
    connection->client_id = client_id;
    connection->clean_session = clean_session;
    connection->keep_alive_time = keep_alive_time;

    /* Initialize the handler */
    connection->handler.alloc = allocator;
    connection->handler.vtable = aws_mqtt_get_client_channel_vtable();
    connection->handler.impl = connection;

    if (aws_hash_table_init(
            &connection->subscriptions,
            connection->allocator,
            0,
            &aws_hash_string,
            &aws_string_eq,
            &aws_string_destroy,
            NULL)) {

        aws_mem_release(allocator, connection);
        connection = NULL;
    }

    if (tls_options) {

        if (aws_client_bootstrap_new_tls_socket_channel(
                client->client_bootstrap,
                endpoint,
                client->socket_options,
                tls_options,
                &s_mqtt_client_init,
                &s_mqtt_client_shutdown,
                connection)) {

            aws_hash_table_clean_up(&connection->subscriptions);
            aws_mem_release(allocator, connection);
            connection = NULL;
        }
    } else {

        if (aws_client_bootstrap_new_socket_channel(
                client->client_bootstrap,
                endpoint,
                client->socket_options,
                &s_mqtt_client_init,
                &s_mqtt_client_shutdown,
                connection)) {

            aws_hash_table_clean_up(&connection->subscriptions);
            aws_mem_release(allocator, connection);
            connection = NULL;
        }
    }

    return connection;
}

int aws_mqtt_client_connection_disconnect(struct aws_mqtt_client_connection *connection) {

    assert(connection);
    assert(connection && connection->slot);

    connection->state = AWS_MQTT_CLIENT_STATE_DISCONNECTING;

    if (aws_channel_shutdown(connection->slot->channel, AWS_OP_SUCCESS)) {
        return AWS_OP_ERR;
    }

    connection->slot = NULL;

    return AWS_OP_SUCCESS;
}
void aws_mqtt_load_error_strings() {

    static bool s_error_strings_loaded = false;
    if (!s_error_strings_loaded) {

        s_error_strings_loaded = true;

#define AWS_DEFINE_ERROR_INFO_MQTT(C, ES) AWS_DEFINE_ERROR_INFO(C, ES, "libaws-c-mqtt")
        /* clang-format off */
        static struct aws_error_info s_errors[] = {
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_INVALID_RESERVED_BITS,
                "Bits marked as reserved in the MQTT spec were incorrectly set."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_BUFFER_TOO_BIG,
                "[MQTT-1.5.3] Encoded UTF-8 buffers may be no bigger than 65535 bytes."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_INVALID_REMAINING_LENGTH,
                "[MQTT-2.2.3] Encoded remaining length field is malformed."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_UNSUPPORTED_PROTOCOL_NAME,
                "[MQTT-3.1.2-1] Protocol name specified is unsupported."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_UNSUPPORTED_PROTOCOL_LEVEL,
                "[MQTT-3.1.2-2] Protocol level specified is unsupported."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_INVALID_CREDENTIALS,
                "[MQTT-3.1.2-21] Connect packet may not include password when no username is present."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_INVALID_QOS,
                "Both bits in a QoS field must not be set."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_INVALID_PACKET_TYPE,
                "Packet type in packet fixed header is invalid."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_PROTOCOL_ERROR,
                "Protocol error occured."),
        };
        /* clang-format on */
#undef AWS_DEFINE_ERROR_INFO_MQTT

        static struct aws_error_info_list s_list = {
            .error_list = s_errors,
            .count = AWS_ARRAY_SIZE(s_errors),
        };
        aws_register_error_info(&s_list);
    }
}
