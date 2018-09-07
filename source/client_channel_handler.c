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

#include <aws/mqtt/private/packets.h>
#include <aws/mqtt/private/utils.h>

#include <aws/common/task_scheduler.h>

static const uint64_t s_timeout = 3000000000;

typedef int(packet_handler_fn)(struct aws_mqtt_client_connection *connection, struct aws_byte_cursor message_cursor);

static int s_packet_handler_default(
    struct aws_mqtt_client_connection *connection,
    struct aws_byte_cursor message_cursor) {
    (void)connection;
    (void)message_cursor;

    return aws_raise_error(AWS_ERROR_MQTT_INVALID_PACKET_TYPE);
}

static int s_packet_handler_connack(
    struct aws_mqtt_client_connection *connection,
    struct aws_byte_cursor message_cursor) {

    struct aws_mqtt_packet_connack connack;
    if (aws_mqtt_packet_connack_decode(&message_cursor, &connack)) {
        return AWS_OP_ERR;
    }

    connection->state = AWS_MQTT_CLIENT_STATE_CONNECTED;

    MQTT_CALL_CALLBACK(connection, on_connack, connack.connect_return_code, connack.session_present);

    if (connack.connect_return_code != AWS_MQTT_CONNECT_ACCEPTED) {
        aws_mqtt_client_connection_disconnect(connection);
    }

    return AWS_OP_SUCCESS;
}

static int s_packet_handler_publish(
    struct aws_mqtt_client_connection *connection,
    struct aws_byte_cursor message_cursor) {

    struct aws_mqtt_packet_publish publish;
    if (aws_mqtt_packet_publish_decode(&message_cursor, &publish)) {
        return AWS_OP_ERR;
    }

    const struct aws_string *topic =
        aws_string_new_from_array(connection->allocator, publish.topic_name.ptr, publish.topic_name.len);

    /* Attempt lazy search of just topic name, no wildcard matching */
    struct aws_hash_element *elem;
    aws_hash_table_find(&connection->subscriptions, topic, &elem);
    aws_string_destroy((void *)topic);

    struct aws_mqtt_subscription_impl *sub = NULL;
    if (elem) {
        sub = elem->value;
    } else {
        for (struct aws_hash_iter iter = aws_hash_iter_begin(&connection->subscriptions); !aws_hash_iter_done(&iter);
             aws_hash_iter_next(&iter)) {

            struct aws_mqtt_subscription_impl *test = iter.element.value;

            if (aws_mqtt_subscription_matches_publish(connection->allocator, test, &publish)) {
                sub = test;
                break;
            }
        }
    }

    if (sub) {

        sub->callback(connection, &sub->subscription, publish.payload, sub->user_data);
    }

    struct aws_mqtt_packet_ack puback;
    AWS_ZERO_STRUCT(puback);

    /* Switch on QoS flags (bits 1 & 2) */
    switch ((publish.fixed_header.flags >> 1) & 0x3) {
        case AWS_MQTT_QOS_AT_MOST_ONCE:
            /* No more communication necessary */
            break;
        case AWS_MQTT_QOS_AT_LEAST_ONCE:
            aws_mqtt_packet_puback_init(&puback, publish.packet_identifier);
            break;
        case AWS_MQTT_QOS_EXACTLY_ONCE:
            aws_mqtt_packet_pubrec_init(&puback, publish.packet_identifier);
            break;
    }

    if (puback.packet_identifier) {

        struct aws_io_message *message = mqtt_get_message_for_packet(connection, &puback.fixed_header);
        if (!message) {
            return AWS_OP_ERR;
        }

        struct aws_byte_cursor message_cursor = {
            .ptr = message->message_data.buffer,
            .len = message->message_data.capacity,
        };
        if (aws_mqtt_packet_ack_encode(&message_cursor, &puback)) {
            return AWS_OP_ERR;
        }
        message->message_data.len = message->message_data.capacity - message_cursor.len;

        if (aws_channel_slot_send_message(connection->slot, message, AWS_CHANNEL_DIR_WRITE)) {
            return AWS_OP_ERR;
        }
    }

    return AWS_OP_SUCCESS;
}

static int s_packet_handler_ack(struct aws_mqtt_client_connection *connection, struct aws_byte_cursor message_cursor) {

    struct aws_mqtt_packet_ack ack;
    if (aws_mqtt_packet_ack_decode(&message_cursor, &ack)) {
        return AWS_OP_ERR;
    }

    mqtt_request_complete(connection, ack.packet_identifier);

    return AWS_OP_SUCCESS;
}

static int s_packet_handler_pubrec(
    struct aws_mqtt_client_connection *connection,
    struct aws_byte_cursor message_cursor) {

    struct aws_mqtt_packet_ack ack;
    if (aws_mqtt_packet_ack_decode(&message_cursor, &ack)) {
        return AWS_OP_ERR;
    }

    /* TODO: When sending PUBLISH with QoS 3, we should be storing the data until this packet is recieved, at which
     * point we may discard it. */

    /* Send PUBREL */
    aws_mqtt_packet_pubrel_init(&ack, ack.packet_identifier);
    struct aws_io_message *message = mqtt_get_message_for_packet(connection, &ack.fixed_header);
    if (!message) {
        return AWS_OP_ERR;
    }

    struct aws_byte_cursor out_message_cursor = {
        .ptr = message->message_data.buffer,
        .len = message->message_data.capacity,
    };
    if (aws_mqtt_packet_ack_encode(&out_message_cursor, &ack)) {
        return AWS_OP_ERR;
    }
    message->message_data.len = message->message_data.capacity - out_message_cursor.len;

    if (aws_channel_slot_send_message(connection->slot, message, AWS_CHANNEL_DIR_WRITE)) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

static int s_packet_handler_pubrel(
    struct aws_mqtt_client_connection *connection,
    struct aws_byte_cursor message_cursor) {

    struct aws_mqtt_packet_ack ack;
    if (aws_mqtt_packet_ack_decode(&message_cursor, &ack)) {
        return AWS_OP_ERR;
    }

    /* Send PUBCOMP */
    aws_mqtt_packet_pubcomp_init(&ack, ack.packet_identifier);
    struct aws_io_message *message = mqtt_get_message_for_packet(connection, &ack.fixed_header);
    if (!message) {
        return AWS_OP_ERR;
    }

    struct aws_byte_cursor out_message_cursor = {
        .ptr = message->message_data.buffer,
        .len = message->message_data.capacity,
    };
    if (aws_mqtt_packet_ack_encode(&out_message_cursor, &ack)) {
        return AWS_OP_ERR;
    }
    message->message_data.len = message->message_data.capacity - out_message_cursor.len;

    if (aws_channel_slot_send_message(connection->slot, message, AWS_CHANNEL_DIR_WRITE)) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

static int s_packet_handler_pingresp(
    struct aws_mqtt_client_connection *connection,
    struct aws_byte_cursor message_cursor) {

    (void)connection;
    (void)message_cursor;

    /* Don't need to do anything on PINGRESP */

    return AWS_OP_SUCCESS;
}

/* Bake up a big ol' function table just like Gramma used to make */
static packet_handler_fn *s_packet_handlers[] = {
    [AWS_MQTT_PACKET_CONNECT] = &s_packet_handler_default,
    [AWS_MQTT_PACKET_CONNACK] = &s_packet_handler_connack,
    [AWS_MQTT_PACKET_PUBLISH] = &s_packet_handler_publish,
    [AWS_MQTT_PACKET_PUBACK] = &s_packet_handler_ack,
    [AWS_MQTT_PACKET_PUBREC] = &s_packet_handler_pubrec,
    [AWS_MQTT_PACKET_PUBREL] = &s_packet_handler_pubrel,
    [AWS_MQTT_PACKET_PUBCOMP] = &s_packet_handler_ack,
    [AWS_MQTT_PACKET_SUBSCRIBE] = &s_packet_handler_default,
    [AWS_MQTT_PACKET_SUBACK] = &s_packet_handler_ack,
    [AWS_MQTT_PACKET_UNSUBSCRIBE] = &s_packet_handler_default,
    [AWS_MQTT_PACKET_UNSUBACK] = &s_packet_handler_ack,
    [AWS_MQTT_PACKET_PINGREQ] = &s_packet_handler_default,
    [AWS_MQTT_PACKET_PINGRESP] = &s_packet_handler_pingresp,
    [AWS_MQTT_PACKET_DISCONNECT] = &s_packet_handler_default,
};

/**
 * Handles incoming messages from the server.
 */
static int s_process_read_message(
    struct aws_channel_handler *handler,
    struct aws_channel_slot *slot,
    struct aws_io_message *message) {

    struct aws_mqtt_client_connection *connection = handler->impl;

    if (message->message_type != AWS_IO_MESSAGE_APPLICATION_DATA || message->message_data.len < 1) {
        return AWS_OP_ERR;
    }

    enum aws_mqtt_packet_type type = aws_mqtt_get_packet_type(message->message_data.buffer);

    /* [MQTT-3.2.0-1] The first packet sent from the Server to the Client MUST be a CONNACK Packet */
    if (connection->state == AWS_MQTT_CLIENT_STATE_CONNECTING && type != AWS_MQTT_PACKET_CONNACK) {

        aws_mqtt_client_connection_disconnect(connection);
        return aws_raise_error(AWS_ERROR_MQTT_PROTOCOL_ERROR);
    }

    struct aws_byte_cursor message_cursor = aws_byte_cursor_from_buf(&message->message_data);

    if (AWS_UNLIKELY(type > AWS_MQTT_PACKET_DISCONNECT || type < AWS_MQTT_PACKET_CONNECT)) {
        return aws_raise_error(AWS_ERROR_MQTT_INVALID_PACKET_TYPE);
    }

    /* Handle the packet */
    int result = s_packet_handlers[type](connection, message_cursor);

    /* Do cleanup */
    aws_channel_slot_increment_read_window(slot, message->message_data.len);
    if (result == AWS_OP_SUCCESS) {
        aws_channel_release_message_to_pool(slot->channel, message);
    }

    return result;
}

static int s_shutdown(
    struct aws_channel_handler *handler,
    struct aws_channel_slot *slot,
    enum aws_channel_direction dir,
    int error_code,
    bool free_scarce_resources_immediately) {

    struct aws_mqtt_client_connection *connection = handler->impl;

    if (dir == AWS_CHANNEL_DIR_WRITE) {
        /* On closing write direction, send out disconnect packet before closing connection. */

        if (!free_scarce_resources_immediately) {

            if (error_code == AWS_OP_SUCCESS) {
                /* On clean shutdown, send the disconnect message */
                struct aws_mqtt_packet_connection disconnect;
                aws_mqtt_packet_disconnect_init(&disconnect);

                struct aws_io_message *message = mqtt_get_message_for_packet(connection, &disconnect.fixed_header);
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
        }
    }

    return aws_channel_slot_on_handler_shutdown_complete(slot, dir, error_code, free_scarce_resources_immediately);
}

static size_t s_initial_window_size(struct aws_channel_handler *handler) {

    (void)handler;

    return SIZE_MAX;
}

static void s_destroy(struct aws_channel_handler *handler) {

    struct aws_mqtt_client_connection *connection = handler->impl;

    /* Clear the client_id */
    aws_byte_buf_clean_up(&connection->client_id);

    /* Free all of the active subscriptions */
    aws_hash_table_clean_up(&connection->subscriptions);

    /* Cleanup outstanding requests */
    aws_memory_pool_clean_up(&connection->requests_pool);
    aws_hash_table_clean_up(&connection->outstanding_requests);

    /* Frees all allocated memory */
    aws_mem_release(connection->allocator, connection);
}

struct aws_channel_handler_vtable aws_mqtt_get_client_channel_vtable() {

    static struct aws_channel_handler_vtable s_vtable = {
        .process_read_message = &s_process_read_message,
        .process_write_message = NULL,
        .increment_read_window = NULL,
        .shutdown = &s_shutdown,
        .initial_window_size = &s_initial_window_size,
        .destroy = &s_destroy,
    };

    return s_vtable;
}

struct aws_io_message *mqtt_get_message_for_packet(
    struct aws_mqtt_client_connection *connection,
    struct aws_mqtt_fixed_header *header) {

    return aws_channel_acquire_message_from_pool(
        connection->slot->channel, AWS_IO_MESSAGE_APPLICATION_DATA, 3 + header->remaining_length);
}

static void s_request_timeout_task(void *arg, enum aws_task_status status) {
    if (status == AWS_TASK_STATUS_RUN_READY) {
        struct aws_mqtt_outstanding_request *request = arg;

        if (!request->completed) {
            /* If not complete, attempt retry */
            if (request->send_request(request->message_id, false, request->userdata)) {
                /* If the send_request function reports the request is complete,
                   remove from the hash table and call the callback. */
                request->completed = true;
                request->on_complete(request->userdata);
            }
        }

        if (request->completed) {
            /* If complete, remove request from outstanding list and return to pool */

            struct aws_hash_element elem;
            int was_present = 0;
            aws_hash_table_remove(&request->connection->outstanding_requests, &request->message_id, &elem, &was_present);
            assert(was_present);

            aws_memory_pool_release(&request->connection->requests_pool, elem.value);
        } else {
            /* If not complete, schedule retry task */

            struct aws_task retry_task;
            retry_task.fn = &s_request_timeout_task;
            retry_task.arg = arg;

            uint64_t ttr = 0;
            aws_channel_current_clock_time(request->connection->slot->channel, &ttr);
            ttr += s_timeout;

            aws_channel_schedule_task(request->connection->slot->channel, &retry_task, ttr);
        }
    }
}

uint16_t mqtt_create_request(
    struct aws_mqtt_client_connection *connection,
    aws_mqtt_send_request_fn *send_request,
    aws_mqtt_complete_fn *on_complete,
    void *userdata) {

    assert(connection);
    assert(send_request);

    struct aws_mqtt_outstanding_request *next_request = aws_memory_pool_acquire(&connection->requests_pool);
    if (!next_request) {
        return 0;
    }

    /* If this is a new node that doesn't have an id, generate one */
    if (next_request->message_id == 0) {

        struct aws_hash_element *elem = NULL;
        uint16_t next_id = 0;
        do {

            ++next_id;
            aws_hash_table_find(&connection->outstanding_requests, &next_id, &elem);

        } while (elem);

        assert(next_id); /* Somehow have UINT16_MAX outstanding requests, definitely a bug */
        next_request->message_id = next_id;
    }

    next_request->connection = connection;
    next_request->completed = false;
    next_request->send_request = send_request;
    next_request->on_complete = on_complete;
    next_request->userdata = userdata;

    /* Store the request by message_id */
    if (aws_hash_table_put(&connection->outstanding_requests, &next_request->message_id, next_request, NULL)) {

        aws_memory_pool_release(&connection->requests_pool, next_request);
        return 0;
    }

    /* Send the request now if on channel's thread, otherwise schedule a task */
    if (aws_channel_thread_is_callers_thread(connection->slot->channel)) {
        s_request_timeout_task(next_request, AWS_TASK_STATUS_RUN_READY);
    } else {
        struct aws_task send_task;
        send_task.fn = &s_request_timeout_task;
        send_task.arg = next_request;
        aws_channel_schedule_task(connection->slot->channel, &send_task, 0);
    }

    return next_request->message_id;
}

void mqtt_request_complete(struct aws_mqtt_client_connection *connection, uint16_t message_id) {

    struct aws_hash_element *elem = NULL;
    aws_hash_table_find(&connection->outstanding_requests, &message_id, &elem);
    assert(elem);

    struct aws_mqtt_outstanding_request *request = elem->value;

    /* Mark as complete for the cleanup task */
    request->completed = true;

    /* Alert the user */
    if (request->on_complete) {
        request->on_complete(request->userdata);
    }
}
