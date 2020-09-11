/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/private/client_impl.h>

#include <aws/mqtt/private/packets.h>
#include <aws/mqtt/private/topic_tree.h>

#include <aws/io/logging.h>

#include <aws/common/clock.h>
#include <aws/common/math.h>
#include <aws/common/task_scheduler.h>

#include <inttypes.h>

#ifdef _MSC_VER
#    pragma warning(disable : 4204)
#endif

/*******************************************************************************
 * Packet State Machine
 ******************************************************************************/

typedef int(packet_handler_fn)(struct aws_mqtt_client_connection *connection, struct aws_byte_cursor message_cursor);

static int s_packet_handler_default(
    struct aws_mqtt_client_connection *connection,
    struct aws_byte_cursor message_cursor) {
    (void)connection;
    (void)message_cursor;

    AWS_LOGF_ERROR(AWS_LS_MQTT_CLIENT, "id=%p: Unhandled packet type received", (void *)connection);
    return aws_raise_error(AWS_ERROR_MQTT_INVALID_PACKET_TYPE);
}

static void s_on_time_to_ping(struct aws_channel_task *channel_task, void *arg, enum aws_task_status status);
static void s_schedule_ping(struct aws_mqtt_client_connection *connection) {
    aws_channel_task_init(&connection->ping_task, s_on_time_to_ping, connection, "mqtt_ping");

    uint64_t schedule_time = 0;
    aws_channel_current_clock_time(connection->slot->channel, &schedule_time);
    AWS_LOGF_TRACE(
        AWS_LS_MQTT_CLIENT, "id=%p: Scheduling PING. current timestamp is %" PRIu64, (void *)connection, schedule_time);

    schedule_time +=
        aws_timestamp_convert(connection->keep_alive_time_secs, AWS_TIMESTAMP_SECS, AWS_TIMESTAMP_NANOS, NULL);

    AWS_LOGF_TRACE(
        AWS_LS_MQTT_CLIENT,
        "id=%p: The next ping will be run at timestamp %" PRIu64,
        (void *)connection,
        schedule_time);
    aws_channel_schedule_task_future(connection->slot->channel, &connection->ping_task, schedule_time);
}

static void s_on_time_to_ping(struct aws_channel_task *channel_task, void *arg, enum aws_task_status status) {
    (void)channel_task;

    if (status == AWS_TASK_STATUS_RUN_READY) {
        struct aws_mqtt_client_connection *connection = arg;
        AWS_LOGF_TRACE(AWS_LS_MQTT_CLIENT, "id=%p: Sending PING", (void *)connection);
        aws_mqtt_client_connection_ping(connection);
        s_schedule_ping(connection);
    }
}
static int s_packet_handler_connack(
    struct aws_mqtt_client_connection *connection,
    struct aws_byte_cursor message_cursor) {

    AWS_LOGF_TRACE(AWS_LS_MQTT_CLIENT, "id=%p: CONNACK received", (void *)connection);

    struct aws_mqtt_packet_connack connack;
    if (aws_mqtt_packet_connack_decode(&message_cursor, &connack)) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT_CLIENT, "id=%p: error %d parsing CONNACK packet", (void *)connection, aws_last_error());

        return AWS_OP_ERR;
    }
    bool was_reconnecting;
    struct aws_linked_list requests;
    aws_linked_list_init(&requests);
    { /* BEGIN CRITICAL SECTION */
        mqtt_connection_lock_synced_data(connection);
        /* User requested disconnect, don't do anything */
        if (connection->synced_data.state >= AWS_MQTT_CLIENT_STATE_DISCONNECTING) {
            mqtt_connection_unlock_synced_data(connection);
            AWS_LOGF_TRACE(
                AWS_LS_MQTT_CLIENT, "id=%p: User has requested disconnect, dropping connection", (void *)connection);
            return AWS_OP_SUCCESS;
        }

        was_reconnecting = connection->synced_data.state == AWS_MQTT_CLIENT_STATE_RECONNECTING;
        connection->synced_data.state = AWS_MQTT_CLIENT_STATE_CONNECTED;
        if (connack.connect_return_code == AWS_MQTT_CONNECT_ACCEPTED) {
            aws_linked_list_swap_contents(&connection->synced_data.pending_requests_list, &requests);
        }
        mqtt_connection_unlock_synced_data(connection);
    } /* END CRITICAL SECTION */
    connection->connection_count++;

    /* Reset the current timeout timer */
    connection->reconnect_timeouts.current = connection->reconnect_timeouts.min;

    /* It is possible for a connection to complete, and a hangup to occur before the
     * CONNECT/CONNACK cycle completes. In that case, we must deliver on_connection_complete
     * on the first successful CONNACK or user code will never think it's connected */
    if (was_reconnecting && connection->connection_count > 1) {

        AWS_LOGF_TRACE(
            AWS_LS_MQTT_CLIENT,
            "id=%p: connection is a resumed connection, invoking on_resumed callback",
            (void *)connection);

        MQTT_CLIENT_CALL_CALLBACK_ARGS(connection, on_resumed, connack.connect_return_code, connack.session_present);
    } else {

        aws_create_reconnect_task(connection);

        AWS_LOGF_TRACE(
            AWS_LS_MQTT_CLIENT,
            "id=%p: connection is a new connection, invoking on_connection_complete callback",
            (void *)connection);
        MQTT_CLIENT_CALL_CALLBACK_ARGS(
            connection, on_connection_complete, AWS_OP_SUCCESS, connack.connect_return_code, connack.session_present);
    }

    AWS_LOGF_TRACE(AWS_LS_MQTT_CLIENT, "id=%p: connection callback completed", (void *)connection);

    if (connack.connect_return_code == AWS_MQTT_CONNECT_ACCEPTED) {
        /* If successfully connected, schedule all pending tasks */
        AWS_LOGF_TRACE(
            AWS_LS_MQTT_CLIENT, "id=%p: connection was accepted processing offline requests.", (void *)connection);
        /**
         * TODO: if the clean_session is true, the pending requests should be cleaned up.
         */
        if (!aws_linked_list_empty(&requests)) {
            struct aws_linked_list_node *current = aws_linked_list_front(&requests);
            const struct aws_linked_list_node *end = aws_linked_list_end(&requests);

            do {

                struct aws_mqtt_outstanding_request *request =
                    AWS_CONTAINER_OF(current, struct aws_mqtt_outstanding_request, list_node);
                AWS_LOGF_TRACE(
                    AWS_LS_MQTT_CLIENT,
                    "id=%p: processing offline request %" PRIu16,
                    (void *)connection,
                    request->packet_id);
                aws_channel_schedule_task_now(connection->slot->channel, &request->timeout_task);

                current = current->next;
            } while (current != end);
        }
    } else {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT_CLIENT,
            "id=%p: invalid connect return code %d, disconnecting",
            (void *)connection,
            connack.connect_return_code);
        /* If error code returned, disconnect */
        aws_channel_shutdown(connection->slot->channel, AWS_ERROR_MQTT_PROTOCOL_ERROR);
    }

    s_schedule_ping(connection);
    return AWS_OP_SUCCESS;
}

static int s_packet_handler_publish(
    struct aws_mqtt_client_connection *connection,
    struct aws_byte_cursor message_cursor) {

    /* TODO: need to handle the QoS 2 message to avoid processing the message a second time */
    struct aws_mqtt_packet_publish publish;
    if (aws_mqtt_packet_publish_decode(&message_cursor, &publish)) {
        return AWS_OP_ERR;
    }

    aws_mqtt_topic_tree_publish(&connection->thread_data.subscriptions, &publish);

    MQTT_CLIENT_CALL_CALLBACK_ARGS(connection, on_any_publish, &publish.topic_name, &publish.payload);

    AWS_LOGF_TRACE(
        AWS_LS_MQTT_CLIENT,
        "id=%p: publish received with message id %" PRIu16,
        (void *)connection,
        publish.packet_identifier);
    struct aws_mqtt_packet_ack puback;
    AWS_ZERO_STRUCT(puback);

    /* Switch on QoS flags (bits 1 & 2) */
    switch ((publish.fixed_header.flags >> 1) & 0x3) {
        case AWS_MQTT_QOS_AT_MOST_ONCE:
            AWS_LOGF_TRACE(
                AWS_LS_MQTT_CLIENT, "id=%p: received publish QOS is 0, not sending puback", (void *)connection);
            /* No more communication necessary */
            break;
        case AWS_MQTT_QOS_AT_LEAST_ONCE:
            AWS_LOGF_TRACE(AWS_LS_MQTT_CLIENT, "id=%p: received publish QOS is 1, sending puback", (void *)connection);
            aws_mqtt_packet_puback_init(&puback, publish.packet_identifier);
            break;
        case AWS_MQTT_QOS_EXACTLY_ONCE:
            AWS_LOGF_TRACE(AWS_LS_MQTT_CLIENT, "id=%p: received publish QOS is 2, sending pubrec", (void *)connection);
            aws_mqtt_packet_pubrec_init(&puback, publish.packet_identifier);
            break;
    }

    if (puback.packet_identifier) {
        struct aws_io_message *message = mqtt_get_message_for_packet(connection, &puback.fixed_header);
        if (!message) {
            return AWS_OP_ERR;
        }

        if (aws_mqtt_packet_ack_encode(&message->message_data, &puback)) {
            aws_mem_release(message->allocator, message);
            return AWS_OP_ERR;
        }

        if (aws_channel_slot_send_message(connection->slot, message, AWS_CHANNEL_DIR_WRITE)) {
            aws_mem_release(message->allocator, message);
            return AWS_OP_ERR;
        }
    }

    return AWS_OP_SUCCESS;
}

static int s_packet_handler_ack(struct aws_mqtt_client_connection *connection, struct aws_byte_cursor message_cursor) {
    /* TODO: we need more detail from ack packets, eg: return code from suback */
    struct aws_mqtt_packet_ack ack;
    if (aws_mqtt_packet_ack_decode(&message_cursor, &ack)) {
        return AWS_OP_ERR;
    }

    AWS_LOGF_TRACE(
        AWS_LS_MQTT_CLIENT, "id=%p: received ack for message id %" PRIu16, (void *)connection, ack.packet_identifier);

    mqtt_request_complete(connection, AWS_OP_SUCCESS, ack.packet_identifier);

    return AWS_OP_SUCCESS;
}

static int s_packet_handler_pubrec(
    struct aws_mqtt_client_connection *connection,
    struct aws_byte_cursor message_cursor) {

    struct aws_mqtt_packet_ack ack;
    if (aws_mqtt_packet_ack_decode(&message_cursor, &ack)) {
        return AWS_OP_ERR;
    }

    /* TODO: When sending PUBLISH with QoS 2, we should be storing the data until this packet is received, at which
     * point we may discard it. */

    /* Send PUBREL */
    aws_mqtt_packet_pubrel_init(&ack, ack.packet_identifier);
    struct aws_io_message *message = mqtt_get_message_for_packet(connection, &ack.fixed_header);
    if (!message) {
        return AWS_OP_ERR;
    }

    if (aws_mqtt_packet_ack_encode(&message->message_data, &ack)) {
        goto on_error;
    }

    if (aws_channel_slot_send_message(connection->slot, message, AWS_CHANNEL_DIR_WRITE)) {
        goto on_error;
    }

    return AWS_OP_SUCCESS;

on_error:

    if (message) {
        aws_mem_release(message->allocator, message);
    }

    return AWS_OP_ERR;
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

    if (aws_mqtt_packet_ack_encode(&message->message_data, &ack)) {
        goto on_error;
    }

    if (aws_channel_slot_send_message(connection->slot, message, AWS_CHANNEL_DIR_WRITE)) {
        goto on_error;
    }

    return AWS_OP_SUCCESS;

on_error:

    if (message) {
        aws_mem_release(message->allocator, message);
    }

    return AWS_OP_ERR;
}

static int s_packet_handler_pingresp(
    struct aws_mqtt_client_connection *connection,
    struct aws_byte_cursor message_cursor) {

    (void)message_cursor;

    AWS_LOGF_TRACE(AWS_LS_MQTT_CLIENT, "id=%p: PINGRESP received", (void *)connection);

    connection->thread_data.waiting_on_ping_response = false;

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

/*******************************************************************************
 * Channel Handler
 ******************************************************************************/

static int s_process_mqtt_packet(
    struct aws_mqtt_client_connection *connection,
    enum aws_mqtt_packet_type packet_type,
    struct aws_byte_cursor packet) {
    { /* BEGIN CRITICAL SECTION */
        mqtt_connection_lock_synced_data(connection);
        /* [MQTT-3.2.0-1] The first packet sent from the Server to the Client MUST be a CONNACK Packet */
        if (connection->synced_data.state == AWS_MQTT_CLIENT_STATE_CONNECTING &&
            packet_type != AWS_MQTT_PACKET_CONNACK) {
            mqtt_connection_unlock_synced_data(connection);
            AWS_LOGF_ERROR(
                AWS_LS_MQTT_CLIENT,
                "id=%p: First message received from the server was not a CONNACK. Terminating connection.",
                (void *)connection);
            aws_channel_shutdown(connection->slot->channel, AWS_ERROR_MQTT_PROTOCOL_ERROR);
            return aws_raise_error(AWS_ERROR_MQTT_PROTOCOL_ERROR);
        }
        mqtt_connection_unlock_synced_data(connection);
    } /* END CRITICAL SECTION */

    if (AWS_UNLIKELY(packet_type > AWS_MQTT_PACKET_DISCONNECT || packet_type < AWS_MQTT_PACKET_CONNECT)) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT_CLIENT,
            "id=%p: Invalid packet type received %d. Terminating connection.",
            (void *)connection,
            packet_type);
        return aws_raise_error(AWS_ERROR_MQTT_INVALID_PACKET_TYPE);
    }

    /* Handle the packet */
    return s_packet_handlers[packet_type](connection, packet);
}

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

    AWS_LOGF_TRACE(
        AWS_LS_MQTT_CLIENT,
        "id=%p: precessing read message of size %zu",
        (void *)connection,
        message->message_data.len);

    /* This cursor will be updated as we read through the message. */
    struct aws_byte_cursor message_cursor = aws_byte_cursor_from_buf(&message->message_data);

    /* If there's pending packet left over from last time, attempt to complete it. */
    if (connection->thread_data.pending_packet.len) {
        int result = AWS_OP_SUCCESS;

        /* This determines how much to read from the message (min(expected_remaining, message.len)) */
        size_t to_read = connection->thread_data.pending_packet.capacity - connection->thread_data.pending_packet.len;
        /* This will be set to false if this message still won't complete the packet object. */
        bool packet_complete = true;
        if (to_read > message_cursor.len) {
            to_read = message_cursor.len;
            packet_complete = false;
        }

        /* Write the chunk to the buffer.
         * This will either complete the packet, or be the entirety of message if more data is required. */
        struct aws_byte_cursor chunk = aws_byte_cursor_advance(&message_cursor, to_read);
        AWS_ASSERT(chunk.ptr); /* Guaranteed to be in bounds */
        result = (int)aws_byte_buf_write_from_whole_cursor(&connection->thread_data.pending_packet, chunk) - 1;
        if (result) {
            goto handle_error;
        }

        /* If the packet is still incomplete, don't do anything with the data. */
        if (!packet_complete) {
            AWS_LOGF_TRACE(
                AWS_LS_MQTT_CLIENT,
                "id=%p: partial message is still incomplete, waiting on another read.",
                (void *)connection);

            goto cleanup;
        }

        /* Handle the completed pending packet */
        struct aws_byte_cursor packet_data = aws_byte_cursor_from_buf(&connection->thread_data.pending_packet);
        AWS_LOGF_TRACE(AWS_LS_MQTT_CLIENT, "id=%p: full mqtt packet re-assembled, dispatching.", (void *)connection);
        result = s_process_mqtt_packet(connection, aws_mqtt_get_packet_type(packet_data.ptr), packet_data);

    handle_error:
        /* Clean up the pending packet */
        aws_byte_buf_clean_up(&connection->thread_data.pending_packet);
        AWS_ZERO_STRUCT(connection->thread_data.pending_packet);

        if (result) {
            return AWS_OP_ERR;
        }
    }

    while (message_cursor.len) {

        /* Temp byte cursor so we can decode the header without advancing message_cursor. */
        struct aws_byte_cursor header_decode = message_cursor;

        struct aws_mqtt_fixed_header packet_header;
        AWS_ZERO_STRUCT(packet_header);
        int result = aws_mqtt_fixed_header_decode(&header_decode, &packet_header);

        /* Calculate how much data was read. */
        const size_t fixed_header_size = message_cursor.len - header_decode.len;

        if (result) {
            if (aws_last_error() == AWS_ERROR_SHORT_BUFFER) {
                /* Message data too short, store data and come back later. */
                AWS_LOGF_TRACE(
                    AWS_LS_MQTT_CLIENT, "id=%p: message is incomplete, waiting on another read.", (void *)connection);
                if (aws_byte_buf_init(
                        &connection->thread_data.pending_packet,
                        connection->allocator,
                        fixed_header_size + packet_header.remaining_length)) {

                    return AWS_OP_ERR;
                }

                /* Write the partial packet. */
                if (!aws_byte_buf_write_from_whole_cursor(&connection->thread_data.pending_packet, message_cursor)) {
                    aws_byte_buf_clean_up(&connection->thread_data.pending_packet);
                    return AWS_OP_ERR;
                }

                aws_reset_error();
                goto cleanup;
            } else {
                return AWS_OP_ERR;
            }
        }

        struct aws_byte_cursor packet_data =
            aws_byte_cursor_advance(&message_cursor, fixed_header_size + packet_header.remaining_length);
        AWS_LOGF_TRACE(AWS_LS_MQTT_CLIENT, "id=%p: full mqtt packet read, dispatching.", (void *)connection);
        s_process_mqtt_packet(connection, packet_header.packet_type, packet_data);
    }

cleanup:
    /* Do cleanup */
    aws_channel_slot_increment_read_window(slot, message->message_data.len);
    aws_mem_release(message->allocator, message);

    return AWS_OP_SUCCESS;
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
                AWS_LOGF_INFO(
                    AWS_LS_MQTT_CLIENT,
                    "id=%p: sending disconnect message as part of graceful shutdown.",
                    (void *)connection);
                /* On clean shutdown, send the disconnect message */
                struct aws_mqtt_packet_connection disconnect;
                aws_mqtt_packet_disconnect_init(&disconnect);

                struct aws_io_message *message = mqtt_get_message_for_packet(connection, &disconnect.fixed_header);
                if (!message) {
                    goto done;
                }

                if (aws_mqtt_packet_connection_encode(&message->message_data, &disconnect)) {
                    AWS_LOGF_DEBUG(
                        AWS_LS_MQTT_CLIENT,
                        "id=%p: failed to encode courteous disconnect io message",
                        (void *)connection);
                    aws_mem_release(message->allocator, message);
                    goto done;
                }

                if (aws_channel_slot_send_message(slot, message, AWS_CHANNEL_DIR_WRITE)) {
                    AWS_LOGF_DEBUG(
                        AWS_LS_MQTT_CLIENT,
                        "id=%p: failed to send courteous disconnect io message",
                        (void *)connection);
                    aws_mem_release(message->allocator, message);
                    goto done;
                }
            }
        }
    }

done:
    return aws_channel_slot_on_handler_shutdown_complete(slot, dir, error_code, free_scarce_resources_immediately);
}

static size_t s_initial_window_size(struct aws_channel_handler *handler) {

    (void)handler;

    return SIZE_MAX;
}

static void s_destroy(struct aws_channel_handler *handler) {

    struct aws_mqtt_client_connection *connection = handler->impl;
    (void)connection;
}

static size_t s_message_overhead(struct aws_channel_handler *handler) {
    (void)handler;
    return 0;
}

struct aws_channel_handler_vtable *aws_mqtt_get_client_channel_vtable(void) {

    static struct aws_channel_handler_vtable s_vtable = {
        .process_read_message = &s_process_read_message,
        .process_write_message = NULL,
        .increment_read_window = NULL,
        .shutdown = &s_shutdown,
        .initial_window_size = &s_initial_window_size,
        .message_overhead = &s_message_overhead,
        .destroy = &s_destroy,
    };

    return &s_vtable;
}

/*******************************************************************************
 * Helpers
 ******************************************************************************/

struct aws_io_message *mqtt_get_message_for_packet(
    struct aws_mqtt_client_connection *connection,
    struct aws_mqtt_fixed_header *header) {

    const size_t required_length = 3 + header->remaining_length;

    struct aws_io_message *message = aws_channel_acquire_message_from_pool(
        connection->slot->channel, AWS_IO_MESSAGE_APPLICATION_DATA, required_length);

    AWS_LOGF_TRACE(
        AWS_LS_MQTT_CLIENT,
        "id=%p: Acquiring memory from pool of required_length %zu",
        (void *)connection,
        required_length);

    return message;
}

/*******************************************************************************
 * Requests
 ******************************************************************************/

static void s_request_timeout_task(struct aws_channel_task *task, void *arg, enum aws_task_status status) {

    struct aws_mqtt_outstanding_request *request = arg;

    if (status == AWS_TASK_STATUS_CANCELED) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT_CLIENT,
            "static: task id %p, was canceled due to the channel shutting down. Canceling request for packet id "
            "%" PRIu16 ".",
            (void *)task,
            request->packet_id);
        /**
         * TODO:
         * 1. If the request doesn't need to retry, we should cancel the request and complete it with error, since we
         * never really send it, in this case.
         * 2. If the request need to retry, it should not be just cancelled and completed. Probably needs to be pushed
         * into pending list, not sure it will break relues like "When it re-sends any PUBLISH packets, it MUST re-send
         * them in the order in which the original PUBLISH packets were sent (this applies to QoS 1 and QoS 2 messages)
         * [MQTT-4.6.0-1]" or not.
         */

        /*
         * If the task is cancelled, assume safe shutdown is in progress.
         * There are two distinct situations where requests end up with a cancelled task:
         *   (1) The request has been completed but disconnect was begun before the task had a chance to run
         *       normally.  We need a way to ensure that aws_memory_pool_release is called for this request.
         *
         *       We can do that by setting request->cancelled to true.  Then the hash table's value removal function
         *       will ensure aws_memory_pool_release is called when the table is cleared during final disconnect.
         *
         *   (2) The request is incomplete.  In this case, we need to call complete with an error and also
         *       ensure that the hash table removal function calls the memory pool release, by setting
         *       cancelled to true.
         */
        if (!request->cancelled) {
            request->cancelled = true;
            if (!request->completed) {
                request->completed = true; /* logically we are completing the request, just not successfully */
                AWS_LOGF_DEBUG(
                    AWS_LS_MQTT_CLIENT,
                    "static: task id %p, completing with interrupt request for packet id "
                    "%" PRIu16 ".",
                    (void *)task,
                    request->packet_id);
                if (request->on_complete != NULL) {
                    request->on_complete(
                        request->connection,
                        request->packet_id,
                        AWS_ERROR_MQTT_CONNECTION_SHUTDOWN,
                        request->on_complete_ud);
                }
            }
        }
        return;
    }

    if (request->cancelled) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT_CLIENT,
            "id=%p: request was canceled. Canceling request for packet id %" PRIu16 ".",
            (void *)request->connection,
            request->packet_id);
        /* If the request was cancelled, assume all containers are gone and just free */
        AWS_LOGF_TRACE(
            AWS_LS_MQTT_CLIENT,
            "id=%p: (timeout task run of cancelled request) Releasing request %" PRIu16 " to connection memory pool",
            (void *)(request->connection),
            request->packet_id);

        struct aws_mqtt_client_connection *connection = request->connection;
        { /* BEGIN CRITICAL SECTION */
            mqtt_connection_lock_synced_data(connection);
            aws_memory_pool_release(&connection->synced_data.requests_pool, request);
            mqtt_connection_unlock_synced_data(connection);
        } /* END CRITICAL SECTION */
        return;
    }

    if (status == AWS_TASK_STATUS_RUN_READY) {
        if (!request->completed) {
            /* If not complete, attempt retry */
            enum aws_mqtt_client_request_state state =
                request->send_request(request->packet_id, !request->initiated, request->send_request_ud);

            int error_code = AWS_ERROR_SUCCESS;
            switch (state) {
                case AWS_MQTT_CLIENT_REQUEST_ERROR:
                    error_code = aws_last_error();
                    AWS_LOGF_ERROR(
                        AWS_LS_MQTT_CLIENT,
                        "id=%p: sending request %" PRIu16 " failed with error %d.",
                        (void *)request->connection,
                        request->packet_id,
                        error_code);
                    /* fall-thru */

                case AWS_MQTT_CLIENT_REQUEST_COMPLETE:
                    AWS_LOGF_TRACE(
                        AWS_LS_MQTT_CLIENT,
                        "id=%p: sending request %" PRIu16 " complete, invoking on_complete callback.",
                        (void *)request->connection,
                        request->packet_id);
                    /* If the send_request function reports the request is complete,
                    remove from the hash table and call the callback. */
                    request->completed = true;
                    if (request->on_complete) {
                        request->on_complete(
                            request->connection, request->packet_id, error_code, request->on_complete_ud);
                    }
                    AWS_LOGF_TRACE(
                        AWS_LS_MQTT_CLIENT, "id=%p: on_complete callback completed.", (void *)request->connection);
                    break;

                case AWS_MQTT_CLIENT_REQUEST_ONGOING:
                    AWS_LOGF_TRACE(
                        AWS_LS_MQTT_CLIENT,
                        "id=%p: request %" PRIu16 " sent, but waiting on an acknowledgement from peer.",
                        (void *)request->connection,
                        request->packet_id);
                    /* Nothing to do here, just continue */
                    break;
            }
        }
        request->initiated = true;

        struct aws_mqtt_client_connection *connection = request->connection;
        if (request->completed) {
            /* If complete, remove request from outstanding list and return to pool */

            struct aws_hash_element elem;
            int was_present = 0;

            AWS_LOGF_TRACE(
                AWS_LS_MQTT_CLIENT,
                "id=%p: removing message id %" PRIu16 " from the outstanding requests list.",
                (void *)connection,
                request->packet_id);

            { /* BEGIN CRITICAL SECTION */
                mqtt_connection_lock_synced_data(connection);
                aws_hash_table_remove(
                    &connection->synced_data.outstanding_requests_table, &request->packet_id, &elem, &was_present);
                /* If the request is not in outstanding_requests_table, warning user about possible error */
                if (!was_present) {
                    AWS_LOGF_WARN(
                        AWS_LS_MQTT_CLIENT,
                        "id=%p: Request %" PRIu16 " was not stored in the outstanding_requests_table.",
                        (void *)(connection),
                        request->packet_id);
                }

                AWS_LOGF_TRACE(
                    AWS_LS_MQTT_CLIENT,
                    "id=%p: (timeout task run of now-completed request) Releasing request %" PRIu16
                    " to connection memory pool",
                    (void *)(connection),
                    request->packet_id);
                aws_memory_pool_release(&connection->synced_data.requests_pool, elem.value);
                mqtt_connection_unlock_synced_data(connection);
            } /* END CRITICAL SECTION */

            return;
        }
        { /* BEGIN CRITICAL SECTION */
            mqtt_connection_lock_synced_data(connection);
            if (connection->synced_data.state != AWS_MQTT_CLIENT_STATE_CONNECTED) {
                /* TODO: if the request is publish with QoS 0, it's more reasonable to complete the publish with error
                 * (or without error, since it's QoS 0). I know it's not against the law to resend them as what we are
                 * doing now, but the user may just don't want those QoS 0 messages stored in memory, occupying the
                 * limited resource. */
                AWS_LOGF_TRACE(
                    AWS_LS_MQTT_CLIENT,
                    "id=%p: not currently connected, adding message id %" PRIu16 " to the pending requests list.",
                    (void *)request->connection,
                    request->packet_id);
                aws_linked_list_push_back(&connection->synced_data.pending_requests_list, &request->list_node);
            } else {
                /* If not complete and online, schedule retry task */
                uint64_t ttr = 0;
                aws_channel_current_clock_time(request->connection->slot->channel, &ttr);
                ttr += request->connection->request_timeout_ns;
                AWS_LOGF_TRACE(
                    AWS_LS_MQTT_CLIENT,
                    "id=%p: scheduling timeout task for message id %" PRIu16 " to run at %" PRIu64,
                    (void *)request->connection,
                    request->packet_id,
                    ttr);

                aws_channel_schedule_task_future(request->connection->slot->channel, task, ttr);
            }
            mqtt_connection_unlock_synced_data(connection);
        } /* END CRITICAL SECTION */
    }
}

uint16_t mqtt_create_request(
    struct aws_mqtt_client_connection *connection,
    aws_mqtt_send_request_fn *send_request,
    void *send_request_ud,
    aws_mqtt_op_complete_fn *on_complete,
    void *on_complete_ud) {

    AWS_ASSERT(connection);
    AWS_ASSERT(send_request);
    struct aws_mqtt_outstanding_request *next_request = NULL;
    { /* BEGIN CRITICAL SECTION */
        mqtt_connection_lock_synced_data(connection);

        if (connection->synced_data.state == AWS_MQTT_CLIENT_STATE_DISCONNECTING) {
            mqtt_connection_unlock_synced_data(connection);
            /* User requested disconnecting, ensure no new requests are made until the channel finished shutting down.
             */
            AWS_LOGF_ERROR(
                AWS_LS_MQTT_CLIENT, "id=%p: Disconnect requested, stop creating any new request.", (void *)connection);
            aws_raise_error(AWS_ERROR_MQTT_CONNECTION_DISCONNECTING);
            return 0;
        }

        next_request = aws_memory_pool_acquire(&connection->synced_data.requests_pool);
        if (!next_request) {
            mqtt_connection_unlock_synced_data(connection);
            return 0;
        }
        memset(next_request, 0, sizeof(struct aws_mqtt_outstanding_request));
        /* TODO: don't put publish QoS 0 into the table or find the unused packet id for it. It's just a waste of
         * resource */
        struct aws_hash_element *elem = NULL;
        uint16_t next_id = 0;
        do {
            /* find the next unused packet id for the new request */
            ++next_id;
            aws_hash_table_find(&connection->synced_data.outstanding_requests_table, &next_id, &elem);

        } while (elem);
        AWS_ASSERT(next_id); /* Somehow have UINT16_MAX outstanding requests, definitely a bug */
        next_request->packet_id = next_id;

        if (aws_hash_table_put(
                &connection->synced_data.outstanding_requests_table, &next_request->packet_id, next_request, NULL)) {
            /* failed to put the next request into the table */
            aws_memory_pool_release(&connection->synced_data.requests_pool, next_request);
            mqtt_connection_unlock_synced_data(connection);
            return 0;
        }
        /* Store the request by packet_id */
        next_request->allocator = connection->allocator;
        next_request->connection = connection;
        next_request->initiated = false;
        next_request->completed = false;
        next_request->send_request = send_request;
        next_request->send_request_ud = send_request_ud;
        next_request->on_complete = on_complete;
        next_request->on_complete_ud = on_complete_ud;
        aws_channel_task_init(
            &next_request->timeout_task, s_request_timeout_task, next_request, "mqtt_request_timeout");
        if (connection->synced_data.state != AWS_MQTT_CLIENT_STATE_CONNECTED) {
            AWS_LOGF_TRACE(
                AWS_LS_MQTT_CLIENT,
                "id=%p: not currently connected, added message id %" PRIu16 " to the pending requests list.",
                (void *)connection,
                next_request->packet_id);
            aws_linked_list_push_back(&connection->synced_data.pending_requests_list, &next_request->list_node);
        } else {
            AWS_LOGF_TRACE(
                AWS_LS_MQTT_CLIENT,
                "id=%p: Currently not in the event-loop thread, scheduling a task to send message id %" PRIu16 ".",
                (void *)connection,
                next_request->packet_id);
            aws_channel_schedule_task_now(connection->slot->channel, &next_request->timeout_task);
        }
        mqtt_connection_unlock_synced_data(connection);
    } /* END CRITICAL SECTION */

    return next_request->packet_id;
}

void mqtt_request_complete(struct aws_mqtt_client_connection *connection, int error_code, uint16_t packet_id) {

    struct aws_hash_element *elem = NULL;

    AWS_LOGF_TRACE(
        AWS_LS_MQTT_CLIENT,
        "id=%p: message id %" PRIu16 " completed with error code %d, removing from outstanding requests list.",
        (void *)connection,
        packet_id,
        error_code);
    { /* BEGIN CRITICAL SECTION */
        mqtt_connection_lock_synced_data(connection);
        aws_hash_table_find(&connection->synced_data.outstanding_requests_table, &packet_id, &elem);
        mqtt_connection_unlock_synced_data(connection);
    } /* END CRITICAL SECTION */

    if (elem == NULL) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT_CLIENT,
            "id=%p: received completion for message id %" PRIu16
            " but no outstanding request exists.  Assuming this is an ack of a resend when the first request has "
            "already completed.",
            (void *)connection,
            packet_id);
        return;
    }

    struct aws_mqtt_outstanding_request *request = elem->value;

    if (request->completed) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT_CLIENT,
            "id=%p: received duplicate completion for message id  %" PRIu16,
            (void *)connection,
            packet_id);
        return;
    }

    AWS_LOGF_TRACE(AWS_LS_MQTT_CLIENT, "id=%p: invoking on_complete callback.", (void *)connection);
    /* Alert the user */
    if (request->on_complete) {
        request->on_complete(request->connection, request->packet_id, error_code, request->on_complete_ud);
    }
    AWS_LOGF_TRACE(AWS_LS_MQTT_CLIENT, "id=%p: on_complete callback completed.", (void *)connection);

    /* Mark as complete for the cleanup task */
    request->completed = true;
}

struct mqtt_shutdown_task {
    int error_code;
    struct aws_channel_task task;
};

static void s_mqtt_disconnect_task(struct aws_channel_task *channel_task, void *arg, enum aws_task_status status) {

    (void)status;

    struct mqtt_shutdown_task *task = AWS_CONTAINER_OF(channel_task, struct mqtt_shutdown_task, task);
    struct aws_mqtt_client_connection *connection = arg;

    AWS_LOGF_TRACE(AWS_LS_MQTT_CLIENT, "id=%p: Doing disconnect", (void *)connection);
    { /* BEGIN CRITICAL SECTION */
        mqtt_connection_lock_synced_data(connection);
        /* If there is an outstanding reconnect task, cancel it */
        if (connection->synced_data.state == AWS_MQTT_CLIENT_STATE_DISCONNECTING && connection->reconnect_task) {
            aws_atomic_store_ptr(&connection->reconnect_task->connection_ptr, NULL);
            /* If the reconnect_task isn't scheduled, free it */
            if (connection->reconnect_task && !connection->reconnect_task->task.timestamp) {
                aws_mem_release(connection->reconnect_task->allocator, connection->reconnect_task);
            }
            connection->reconnect_task = NULL;
        }
        mqtt_connection_unlock_synced_data(connection);
    } /* END CRITICAL SECTION */

    if (connection->slot && connection->slot->channel) {
        aws_channel_shutdown(connection->slot->channel, task->error_code);
    }

    aws_mem_release(connection->allocator, task);
}

void mqtt_disconnect_impl(struct aws_mqtt_client_connection *connection, int error_code) {
    if (connection->slot) {
        struct mqtt_shutdown_task *shutdown_task =
            aws_mem_calloc(connection->allocator, 1, sizeof(struct mqtt_shutdown_task));
        shutdown_task->error_code = error_code;
        aws_channel_task_init(&shutdown_task->task, s_mqtt_disconnect_task, connection, "mqtt_disconnect");
        aws_channel_schedule_task_now(connection->slot->channel, &shutdown_task->task);
    }
}
