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

static void s_mqtt_subscription_impl_destroy(void *value) {
    struct aws_mqtt_subscription_impl *impl = value;

    aws_string_destroy((void *)impl->filter);
    aws_mem_release(impl->connection->allocator, impl);
}

/**
 * Channel has be initialized callback. Sets up channel handler and sends out CONNECT packet.
 * The on_connack callback is called with the CONNACK packet is received from the server.
 */
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

    if (!connection->slot) {
        MQTT_CALL_CALLBACK(connection, on_connection_failed, aws_last_error());
        return AWS_OP_ERR;
    }

    aws_channel_slot_insert_end(channel, connection->slot);
    aws_channel_slot_set_handler(connection->slot, &connection->handler);

    /* Send the connect packet */
    struct aws_mqtt_packet_connect connect;
    aws_mqtt_packet_connect_init(
        &connect,
        aws_byte_cursor_from_buf(&connection->client_id),
        connection->clean_session,
        connection->keep_alive_time);

    struct aws_io_message *message = mqtt_get_message_for_packet(connection, &connect.fixed_header);
    if (!message) {
        MQTT_CALL_CALLBACK(connection, on_connection_failed, aws_last_error());
        return AWS_OP_ERR;
    }
    struct aws_byte_cursor message_cursor = {
        .ptr = message->message_data.buffer,
        .len = message->message_data.capacity,
    };
    if (aws_mqtt_packet_connect_encode(&message_cursor, &connect)) {
        MQTT_CALL_CALLBACK(connection, on_connection_failed, aws_last_error());
        return AWS_OP_ERR;
    }
    message->message_data.len = message->message_data.capacity - message_cursor.len;

    if (aws_channel_slot_send_message(connection->slot, message, AWS_CHANNEL_DIR_WRITE)) {
        MQTT_CALL_CALLBACK(connection, on_connection_failed, aws_last_error());
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

static uint64_t s_hash_uint16_t(const void *item) {
    return *(uint16_t *)item;
}

static bool s_uint16_t_eq(const void *a, const void *b) {
    return *(uint16_t *)a == *(uint16_t *)b;
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

    if (!connection) {

        return NULL;
    }

    /* Initialize the client */
    AWS_ZERO_STRUCT(*connection);
    connection->allocator = allocator;
    connection->callbacks = callbacks;
    connection->state = AWS_MQTT_CLIENT_STATE_CONNECTING;
    connection->clean_session = clean_session;
    connection->keep_alive_time = keep_alive_time;

    struct aws_byte_buf client_id_buf = {
        .buffer = client_id.ptr,
        .len = client_id.len,
        .capacity = client_id.len,
    };
    aws_byte_buf_init_copy(allocator, &connection->client_id, &client_id_buf);

    /* Initialize the handler */
    connection->handler.alloc = allocator;
    connection->handler.vtable = aws_mqtt_get_client_channel_vtable();
    connection->handler.impl = connection;

    if (aws_hash_table_init(
            &connection->subscriptions,
            allocator,
            0,
            &aws_hash_string,
            &aws_string_eq,
            NULL,
            &s_mqtt_subscription_impl_destroy)) {

        goto handle_error;
    }

    if (aws_memory_pool_init(&connection->requests_pool, allocator, 32, sizeof(struct aws_mqtt_outstanding_request))) {

        goto handle_error;
    }
    if (aws_hash_table_init(
            &connection->outstanding_requests,
            allocator,
            sizeof(struct aws_mqtt_outstanding_request *),
            s_hash_uint16_t,
            s_uint16_t_eq,
            NULL,
            NULL)) {

        goto handle_error;
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

            goto handle_error;
        }
    } else {
        if (aws_client_bootstrap_new_socket_channel(
                client->client_bootstrap,
                endpoint,
                client->socket_options,
                &s_mqtt_client_init,
                &s_mqtt_client_shutdown,
                connection)) {

            goto handle_error;
        }
    }

    return connection;

handle_error:

    MQTT_CALL_CALLBACK(connection, on_connection_failed, aws_last_error());

    if (connection->subscriptions.p_impl) {
        aws_hash_table_clean_up(&connection->subscriptions);
    }

    if (connection->requests_pool.data_ptr) {
        aws_memory_pool_clean_up(&connection->requests_pool);
    }

    if (connection->outstanding_requests.p_impl) {
        aws_hash_table_clean_up(&connection->outstanding_requests);
    }

    aws_mem_release(allocator, connection);
    return NULL;
}

int aws_mqtt_client_connection_disconnect(struct aws_mqtt_client_connection *connection) {

    assert(connection);
    assert(connection->slot);

    connection->state = AWS_MQTT_CLIENT_STATE_DISCONNECTING;

    if (aws_channel_shutdown(connection->slot->channel, AWS_OP_SUCCESS)) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

int aws_mqtt_client_subscribe(
    struct aws_mqtt_client_connection *connection,
    const struct aws_mqtt_subscription *subscription,
    aws_mqtt_publish_recieved_fn *callback,
    void *user_data) {

    assert(connection);

    struct aws_io_message *message = NULL;
    int was_created = 0;

    struct aws_mqtt_subscription_impl *subscription_impl =
        aws_mem_acquire(connection->allocator, sizeof(struct aws_mqtt_subscription_impl));
    if (!subscription_impl) {
        goto handle_error;
    }

    subscription_impl->connection = connection;
    subscription_impl->callback = callback;
    subscription_impl->user_data = user_data;

    subscription_impl->filter = aws_string_new_from_array(
        connection->allocator, subscription->topic_filter.ptr, subscription->topic_filter.len);
    if (!subscription_impl->filter) {
        goto handle_error;
    }

    subscription_impl->subscription.qos = subscription->qos;
    subscription_impl->subscription.topic_filter = aws_byte_cursor_from_string(subscription_impl->filter);

    struct aws_hash_element *elem;
    aws_hash_table_create(&connection->subscriptions, subscription_impl->filter, &elem, &was_created);
    elem->value = subscription_impl;

    /* Send the subscribe packet */
    uint16_t packet_id = mqtt_get_next_packet_id(connection);
    struct aws_mqtt_packet_subscribe subscribe;
    if (aws_mqtt_packet_subscribe_init(&subscribe, connection->allocator, packet_id)) {
        goto handle_error;
    }
    if (aws_mqtt_packet_subscribe_add_topic(
            &subscribe, subscription_impl->subscription.topic_filter, subscription->qos)) {
        goto handle_error;
    }

    message = mqtt_get_message_for_packet(connection, &subscribe.fixed_header);
    if (!message) {
        goto handle_error;
    }
    struct aws_byte_cursor message_cursor = {
        .ptr = message->message_data.buffer,
        .len = message->message_data.capacity,
    };
    if (aws_mqtt_packet_subscribe_encode(&message_cursor, &subscribe)) {
        goto handle_error;
    }
    message->message_data.len = message->message_data.capacity - message_cursor.len;

    aws_mqtt_packet_subscribe_clean_up(&subscribe);

    if (aws_channel_slot_send_message(connection->slot, message, AWS_CHANNEL_DIR_WRITE)) {
        goto handle_error;
    }
    return AWS_OP_SUCCESS;

handle_error:

    if (subscription_impl) {
        if (subscription_impl->filter) {
            aws_string_destroy((void *)subscription_impl->filter);
        }
        aws_mem_release(connection->allocator, subscription_impl);
    }
    if (was_created) {
        aws_hash_table_remove(&connection->subscriptions, subscription_impl->filter, NULL, NULL);
    }
    if (message) {
        aws_channel_release_message_to_pool(connection->slot->channel, message);
    }
    return AWS_OP_ERR;
}

int aws_mqtt_client_publish(
    struct aws_mqtt_client_connection *connection,
    struct aws_byte_cursor topic,
    enum aws_mqtt_qos qos,
    bool retain,
    struct aws_byte_cursor payload) {

    assert(connection);

    uint16_t packet_id = 0;
    if (qos > AWS_MQTT_QOS_AT_MOST_ONCE) {
        packet_id = mqtt_get_next_packet_id(connection);
    }

    struct aws_mqtt_packet_publish publish;
    aws_mqtt_packet_publish_init(&publish, retain, qos, false, topic, packet_id, payload);

    struct aws_io_message *message = mqtt_get_message_for_packet(connection, &publish.fixed_header);
    if (!message) {
        goto handle_error;
    }
    struct aws_byte_cursor message_cursor = {
        .ptr = message->message_data.buffer,
        .len = message->message_data.capacity,
    };
    if (aws_mqtt_packet_publish_encode(&message_cursor, &publish)) {
        goto handle_error;
    }
    message->message_data.len = message->message_data.capacity - message_cursor.len;

    if (aws_channel_slot_send_message(connection->slot, message, AWS_CHANNEL_DIR_WRITE)) {

        goto handle_error;
    }

    return AWS_OP_SUCCESS;

handle_error:
    if (message) {
        aws_channel_release_message_to_pool(connection->slot->channel, message);
    }
    return AWS_OP_ERR;
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
