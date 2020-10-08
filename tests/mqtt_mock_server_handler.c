/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "mqtt_mock_server_handler.h"
#include <aws/testing/aws_test_harness.h>

static int s_mqtt_decoded_packet_init(struct mqtt_decoded_packet *packet, struct aws_allocator *alloc) {
    AWS_ZERO_STRUCT(*packet);
    if (aws_array_list_init_dynamic(&packet->sub_topic_filters, alloc, 1, sizeof(struct aws_mqtt_subscription))) {
        return AWS_OP_ERR;
    }
    if (aws_array_list_init_dynamic(&packet->unsub_topic_filters, alloc, 1, sizeof(struct aws_byte_cursor))) {
        return AWS_OP_ERR;
    }
    return AWS_OP_SUCCESS;
}

static void s_mqtt_decoded_packet_clean_up(struct mqtt_decoded_packet *packet) {
    aws_array_list_clean_up(&packet->sub_topic_filters);
    aws_array_list_clean_up(&packet->unsub_topic_filters);
}

static int s_mqtt_mock_server_handler_process_packet(
    struct mqtt_mock_server_handler *testing_handler,
    struct aws_byte_cursor *message_cur) {
    struct aws_byte_buf received_message;
    aws_byte_buf_init_copy_from_cursor(&received_message, testing_handler->handler.alloc, *message_cur);
    aws_mutex_lock(&testing_handler->lock);
    aws_array_list_push_back(&testing_handler->synced_data.received_messages, &received_message);
    aws_mutex_unlock(&testing_handler->lock);

    struct aws_byte_cursor message_cur_cpy = *message_cur;
    int err = 0;

    enum aws_mqtt_packet_type packet_type = aws_mqtt_get_packet_type(message_cur_cpy.ptr);
    switch (packet_type) {
        case AWS_MQTT_PACKET_CONNECT: {
            size_t connacks_available = 0;
            err |= aws_mutex_lock(&testing_handler->lock);
            AWS_LOGF_DEBUG(
                MOCK_LOG_SUBJECT,
                "server, CONNECT received, %llu available connacks.",
                (long long unsigned)testing_handler->connacks_avail);
            connacks_available = testing_handler->connacks_avail > 0 ? testing_handler->connacks_avail-- : 0;
            err |= aws_mutex_unlock(&testing_handler->lock);

            if (connacks_available) {
                struct aws_io_message *connack_msg = aws_channel_acquire_message_from_pool(
                    testing_handler->slot->channel, AWS_IO_MESSAGE_APPLICATION_DATA, 256);

                struct aws_mqtt_packet_connack conn_ack;
                err |= aws_mqtt_packet_connack_init(&conn_ack, false, AWS_MQTT_CONNECT_ACCEPTED);
                err |= aws_mqtt_packet_connack_encode(&connack_msg->message_data, &conn_ack);
                if (aws_channel_slot_send_message(testing_handler->slot, connack_msg, AWS_CHANNEL_DIR_WRITE)) {
                    err |= 1;
                    AWS_LOGF_DEBUG(MOCK_LOG_SUBJECT, "Failed to send connack with error %d", aws_last_error());
                }
            }
            break;
        }

        case AWS_MQTT_PACKET_DISCONNECT:
            AWS_LOGF_DEBUG(MOCK_LOG_SUBJECT, "server, DISCONNECT received");

            err |= aws_channel_shutdown(testing_handler->slot->channel, AWS_OP_SUCCESS);
            break;

        case AWS_MQTT_PACKET_PINGREQ: {
            AWS_LOGF_DEBUG(MOCK_LOG_SUBJECT, "server, PINGREQ received");

            size_t ping_resp_available = 0;
            err |= aws_mutex_lock(&testing_handler->lock);
            ping_resp_available = testing_handler->ping_resp_avail > 0 ? testing_handler->ping_resp_avail-- : 0;
            err |= aws_mutex_unlock(&testing_handler->lock);

            if (ping_resp_available) {
                struct aws_io_message *ping_resp = aws_channel_acquire_message_from_pool(
                    testing_handler->slot->channel, AWS_IO_MESSAGE_APPLICATION_DATA, 256);
                struct aws_mqtt_packet_connection packet;
                err |= aws_mqtt_packet_pingresp_init(&packet);
                err |= aws_mqtt_packet_connection_encode(&ping_resp->message_data, &packet);
                err |= aws_channel_slot_send_message(testing_handler->slot, ping_resp, AWS_CHANNEL_DIR_WRITE);
            }
            break;
        }

        case AWS_MQTT_PACKET_SUBSCRIBE: {
            AWS_LOGF_DEBUG(MOCK_LOG_SUBJECT, "server, SUBSCRIBE received");

            struct aws_mqtt_packet_subscribe subscribe_packet;
            err |= aws_mqtt_packet_subscribe_init(&subscribe_packet, testing_handler->handler.alloc, 0);
            err |= aws_mqtt_packet_subscribe_decode(message_cur, &subscribe_packet);

            if (testing_handler->auto_ack) {
                struct aws_io_message *suback_msg = aws_channel_acquire_message_from_pool(
                    testing_handler->slot->channel, AWS_IO_MESSAGE_APPLICATION_DATA, 256);
                struct aws_mqtt_packet_ack suback;
                err |= aws_mqtt_packet_suback_init(&suback, subscribe_packet.packet_identifier);
                err |= aws_mqtt_packet_ack_encode(&suback_msg->message_data, &suback);
                err |= aws_channel_slot_send_message(testing_handler->slot, suback_msg, AWS_CHANNEL_DIR_WRITE);
            }
            aws_mqtt_packet_subscribe_clean_up(&subscribe_packet);
            break;
        }

        case AWS_MQTT_PACKET_UNSUBSCRIBE: {
            AWS_LOGF_DEBUG(MOCK_LOG_SUBJECT, "server, UNSUBSCRIBE received");

            struct aws_mqtt_packet_unsubscribe unsubscribe_packet;
            err |= aws_mqtt_packet_unsubscribe_init(&unsubscribe_packet, testing_handler->handler.alloc, 0);
            err |= aws_mqtt_packet_unsubscribe_decode(message_cur, &unsubscribe_packet);

            if (testing_handler->auto_ack) {
                struct aws_io_message *unsuback_msg = aws_channel_acquire_message_from_pool(
                    testing_handler->slot->channel, AWS_IO_MESSAGE_APPLICATION_DATA, 256);
                struct aws_mqtt_packet_ack unsuback;
                err |= aws_mqtt_packet_unsuback_init(&unsuback, unsubscribe_packet.packet_identifier);
                err |= aws_mqtt_packet_ack_encode(&unsuback_msg->message_data, &unsuback);
                err |= aws_channel_slot_send_message(testing_handler->slot, unsuback_msg, AWS_CHANNEL_DIR_WRITE);
            }
            aws_mqtt_packet_unsubscribe_clean_up(&unsubscribe_packet);
            break;
        }

        case AWS_MQTT_PACKET_PUBLISH: {
            AWS_LOGF_DEBUG(MOCK_LOG_SUBJECT, "server, PUBLISH received");

            struct aws_mqtt_packet_publish publish_packet;
            err |= aws_mqtt_packet_publish_decode(message_cur, &publish_packet);

            if (testing_handler->auto_ack) {
                struct aws_io_message *puback_msg = aws_channel_acquire_message_from_pool(
                    testing_handler->slot->channel, AWS_IO_MESSAGE_APPLICATION_DATA, 256);
                struct aws_mqtt_packet_ack puback;
                err |= aws_mqtt_packet_puback_init(&puback, publish_packet.packet_identifier);
                err |= aws_mqtt_packet_ack_encode(&puback_msg->message_data, &puback);
                err |= aws_channel_slot_send_message(testing_handler->slot, puback_msg, AWS_CHANNEL_DIR_WRITE);
            }
            break;
        }

        case AWS_MQTT_PACKET_PUBACK:
            AWS_LOGF_DEBUG(MOCK_LOG_SUBJECT, "server, PUBACK received");

            err |= aws_mutex_lock(&testing_handler->lock);
            testing_handler->pubacks_received++;
            err |= aws_mutex_unlock(&testing_handler->lock);
            err |= aws_condition_variable_notify_one(&testing_handler->cvar);
            break;

        default:
            aws_mutex_lock(&testing_handler->lock);
            if (aws_array_list_length(&testing_handler->response_messages)) {
                struct aws_byte_buf response_message;
                err |= aws_array_list_front(&testing_handler->response_messages, &response_message);
                err |= aws_array_list_pop_front(&testing_handler->response_messages);

                struct aws_io_message *reply = aws_channel_acquire_message_from_pool(
                    testing_handler->slot->channel, AWS_IO_MESSAGE_APPLICATION_DATA, response_message.len);
                err |= !aws_byte_buf_write_from_whole_buffer(&reply->message_data, response_message);
                err |= aws_channel_slot_send_message(testing_handler->slot, reply, AWS_CHANNEL_DIR_WRITE);
            }
            aws_mutex_unlock(&testing_handler->lock);
            break;
    }
    if (err) {
        AWS_LOGF_DEBUG(MOCK_LOG_SUBJECT, "server, process packet failed, the package type is %d", packet_type);
        /* crash */
        AWS_FATAL_ASSERT(!err);
    }
    return AWS_OP_SUCCESS;
}

static int s_mqtt_mock_server_handler_process_read_message(
    struct aws_channel_handler *handler,
    struct aws_channel_slot *slot,
    struct aws_io_message *message) {

    struct mqtt_mock_server_handler *testing_handler = handler->impl;

    struct aws_byte_cursor message_cursor = aws_byte_cursor_from_buf(&message->message_data);

    if (testing_handler->pending_packet.len) {
        size_t to_read = testing_handler->pending_packet.capacity - testing_handler->pending_packet.len;

        bool packet_complete = true;
        if (to_read > message_cursor.len) {
            to_read = message_cursor.len;
            packet_complete = false;
        }

        struct aws_byte_cursor chunk = aws_byte_cursor_advance(&message_cursor, to_read);
        aws_byte_buf_write_from_whole_cursor(&testing_handler->pending_packet, chunk);

        if (!packet_complete) {
            goto cleanup;
        }

        struct aws_byte_cursor packet_data = aws_byte_cursor_from_buf(&testing_handler->pending_packet);
        s_mqtt_mock_server_handler_process_packet(testing_handler, &packet_data);
    }

    while (message_cursor.len) {
        struct aws_byte_cursor header_decode = message_cursor;
        struct aws_mqtt_fixed_header packet_header;
        AWS_ZERO_STRUCT(packet_header);
        int result = aws_mqtt_fixed_header_decode(&header_decode, &packet_header);

        const size_t fixed_header_size = message_cursor.len - header_decode.len;

        if (result) {
            if (aws_last_error() == AWS_ERROR_SHORT_BUFFER) {
                aws_byte_buf_init(
                    &testing_handler->pending_packet,
                    testing_handler->handler.alloc,
                    fixed_header_size + packet_header.remaining_length);
                aws_byte_buf_write_from_whole_cursor(&testing_handler->pending_packet, message_cursor);
                aws_reset_error();
                goto cleanup;
            }
        }
        struct aws_byte_cursor packet_data =
            aws_byte_cursor_advance(&message_cursor, fixed_header_size + packet_header.remaining_length);
        s_mqtt_mock_server_handler_process_packet(testing_handler, &packet_data);
    }
cleanup:
    aws_mem_release(message->allocator, message);
    aws_channel_slot_increment_read_window(slot, message->message_data.len);
    return AWS_OP_SUCCESS;
}

static void s_mqtt_send_publish_in_thread(
    struct aws_channel_task *channel_task,
    void *arg,
    enum aws_task_status status) {
    (void)channel_task;
    (void)status;
    struct mqtt_mock_server_publish_args *publish_args = arg;

    if (status == AWS_TASK_STATUS_RUN_READY) {
        struct aws_mqtt_packet_publish publish;
        aws_mqtt_packet_publish_init(
            &publish,
            false,
            publish_args->qos,
            false,
            publish_args->topic,
            ++publish_args->testing_handler->last_packet_id,
            publish_args->payload);
        struct aws_io_message *publish_msg = aws_channel_acquire_message_from_pool(
            publish_args->testing_handler->slot->channel,
            AWS_IO_MESSAGE_APPLICATION_DATA,
            publish_args->payload.len + 256);

        aws_mqtt_packet_publish_encode(&publish_msg->message_data, &publish);
        aws_channel_slot_send_message(publish_args->testing_handler->slot, publish_msg, AWS_CHANNEL_DIR_WRITE);
    }
    aws_mem_release(publish_args->testing_handler->handler.alloc, publish_args);
}

int mqtt_mock_server_send_publish(
    struct aws_channel_handler *handler,
    struct aws_byte_cursor *topic,
    struct aws_byte_cursor *payload,
    enum aws_mqtt_qos qos) {

    struct mqtt_mock_server_handler *testing_handler = handler->impl;

    struct mqtt_mock_server_publish_args *args =
        aws_mem_calloc(handler->alloc, 1, sizeof(struct mqtt_mock_server_publish_args));
    args->task.task_fn = s_mqtt_send_publish_in_thread;
    args->task.arg = args;
    args->topic = *topic;
    args->payload = *payload;
    args->qos = qos;
    args->testing_handler = testing_handler;

    aws_channel_schedule_task_now(testing_handler->slot->channel, &args->task);

    return AWS_OP_SUCCESS;
}
static int s_mqtt_mock_server_handler_process_write_message(
    struct aws_channel_handler *handler,
    struct aws_channel_slot *slot,
    struct aws_io_message *message) {
    (void)handler;
    (void)slot;
    (void)message;

    return AWS_OP_SUCCESS;
}

static int s_mqtt_mock_server_handler_increment_read_window(
    struct aws_channel_handler *handler,
    struct aws_channel_slot *slot,
    size_t size) {
    (void)handler;
    aws_channel_slot_increment_read_window(slot, size);
    return AWS_OP_SUCCESS;
}

void mqtt_mock_server_handler_update_slot(struct aws_channel_handler *handler, struct aws_channel_slot *slot) {
    struct mqtt_mock_server_handler *testing_handler = handler->impl;
    testing_handler->slot = slot;
}

static int s_mqtt_mock_server_handler_shutdown(
    struct aws_channel_handler *handler,
    struct aws_channel_slot *slot,
    enum aws_channel_direction dir,
    int error_code,
    bool free_scarce_resources_immediately) {
    (void)handler;
    return aws_channel_slot_on_handler_shutdown_complete(slot, dir, error_code, free_scarce_resources_immediately);
}

static size_t s_mqtt_mock_server_handler_initial_window_size(struct aws_channel_handler *handler) {
    (void)handler;
    return SIZE_MAX;
}

static size_t s_mqtt_mock_server_handler_message_overhead(struct aws_channel_handler *handler) {
    (void)handler;
    return 0;
}

static void s_mqtt_mock_server_handler_destroy(struct aws_channel_handler *handler) {
    (void)handler;
}

static struct aws_channel_handler_vtable s_mqtt_mock_server_handler_vtable = {
    .process_read_message = s_mqtt_mock_server_handler_process_read_message,
    .process_write_message = s_mqtt_mock_server_handler_process_write_message,
    .increment_read_window = s_mqtt_mock_server_handler_increment_read_window,
    .shutdown = s_mqtt_mock_server_handler_shutdown,
    .initial_window_size = s_mqtt_mock_server_handler_initial_window_size,
    .message_overhead = s_mqtt_mock_server_handler_message_overhead,
    .destroy = s_mqtt_mock_server_handler_destroy,
};

struct aws_channel_handler *new_mqtt_mock_server(struct aws_allocator *allocator) {

    struct mqtt_mock_server_handler *testing_handler =
        aws_mem_calloc(allocator, 1, sizeof(struct mqtt_mock_server_handler));
    aws_array_list_init_dynamic(&testing_handler->packets, allocator, 4, sizeof(struct mqtt_decoded_packet));
    aws_array_list_init_dynamic(
        &testing_handler->synced_data.received_messages, allocator, 4, sizeof(struct aws_byte_buf));
    aws_array_list_init_dynamic(&testing_handler->response_messages, allocator, 4, sizeof(struct aws_byte_buf));

    testing_handler->handler.impl = testing_handler;
    testing_handler->handler.vtable = &s_mqtt_mock_server_handler_vtable;
    testing_handler->handler.alloc = allocator;
    testing_handler->ping_resp_avail = SIZE_MAX;
    testing_handler->connacks_avail = SIZE_MAX;
    testing_handler->auto_ack = true;
    aws_mutex_init(&testing_handler->lock);
    aws_condition_variable_init(&testing_handler->cvar);

    return &testing_handler->handler;
}

void destroy_mqtt_mock_server(struct aws_channel_handler *handler) {
    struct mqtt_mock_server_handler *testing_handler = handler->impl;

    for (size_t i = 0; i < aws_array_list_length(&testing_handler->packets); ++i) {
        struct mqtt_decoded_packet *packet = NULL;
        aws_array_list_get_at_ptr(&testing_handler->packets, (void **)&packet, i);
        s_mqtt_decoded_packet_clean_up(packet);
    }
    aws_array_list_clean_up(&testing_handler->packets);

    for (size_t i = 0; i < aws_array_list_length(&testing_handler->synced_data.received_messages); ++i) {
        struct aws_byte_buf *byte_buf_ptr = NULL;
        aws_array_list_get_at_ptr(&testing_handler->synced_data.received_messages, (void **)&byte_buf_ptr, i);
        aws_byte_buf_clean_up(byte_buf_ptr);
    }
    aws_array_list_clean_up(&testing_handler->synced_data.received_messages);

    for (size_t i = 0; i < aws_array_list_length(&testing_handler->response_messages); ++i) {
        struct aws_byte_buf *byte_buf_ptr = NULL;
        aws_array_list_get_at_ptr(&testing_handler->response_messages, (void **)&byte_buf_ptr, i);
        aws_byte_buf_clean_up(byte_buf_ptr);
    }
    aws_array_list_clean_up(&testing_handler->response_messages);
    aws_mutex_clean_up(&testing_handler->lock);
    aws_condition_variable_clean_up(&testing_handler->cvar);
    aws_mem_release(handler->alloc, testing_handler);
}

void mqtt_mock_server_set_max_ping_resp(struct aws_channel_handler *handler, size_t max_ping) {
    struct mqtt_mock_server_handler *testing_handler = handler->impl;

    aws_mutex_lock(&testing_handler->lock);
    testing_handler->ping_resp_avail = max_ping;
    aws_mutex_unlock(&testing_handler->lock);
}

void mqtt_mock_server_set_max_connack(struct aws_channel_handler *handler, size_t connack_avail) {
    struct mqtt_mock_server_handler *testing_handler = handler->impl;

    aws_mutex_lock(&testing_handler->lock);
    testing_handler->connacks_avail = connack_avail;
    aws_mutex_unlock(&testing_handler->lock);
}

void mqtt_mock_server_disable_auto_ack(struct aws_channel_handler *handler) {
    struct mqtt_mock_server_handler *testing_handler = handler->impl;

    testing_handler->auto_ack = false;
}

void mqtt_mock_server_enable_auto_ack(struct aws_channel_handler *handler) {
    struct mqtt_mock_server_handler *testing_handler = handler->impl;

    testing_handler->auto_ack = true;
}

static int s_send_ack(struct aws_channel_handler *handler, uint16_t packetID, enum aws_mqtt_packet_type type) {
    struct mqtt_mock_server_handler *testing_handler = handler->impl;
    int err = 0;
    struct aws_mqtt_packet_ack ack;
    struct aws_io_message *msg =
        aws_channel_acquire_message_from_pool(testing_handler->slot->channel, AWS_IO_MESSAGE_APPLICATION_DATA, 256);
    switch (type) {
        case AWS_MQTT_PACKET_PUBACK:
            err |= aws_mqtt_packet_puback_init(&ack, packetID);
            break;

        case AWS_MQTT_PACKET_SUBACK:
            err |= aws_mqtt_packet_suback_init(&ack, packetID);
            break;
        case AWS_MQTT_PACKET_UNSUBACK:
            err |= aws_mqtt_packet_unsuback_init(&ack, packetID);
            break;
        default:
            break;
    }
    err |= aws_mqtt_packet_ack_encode(&msg->message_data, &ack);
    err |= aws_channel_slot_send_message(testing_handler->slot, msg, AWS_CHANNEL_DIR_WRITE);

    return err;
}

/* Send response back the client given the packet ID */
int mqtt_mock_server_send_suback(struct aws_channel_handler *handler, uint16_t packetID) {
    return s_send_ack(handler, packetID, AWS_MQTT_PACKET_SUBACK);
}
int mqtt_mock_server_send_unsuback(struct aws_channel_handler *handler, uint16_t packetID) {
    return s_send_ack(handler, packetID, AWS_MQTT_PACKET_UNSUBACK);
}
int mqtt_mock_server_send_puback(struct aws_channel_handler *handler, uint16_t packetID) {
    return s_send_ack(handler, packetID, AWS_MQTT_PACKET_PUBACK);
}

struct puback_waiter {
    struct mqtt_mock_server_handler *testing_handler;
    size_t wait_for_count;
};

static bool s_is_pubacks_complete(void *arg) {
    struct puback_waiter *waiter = arg;

    return waiter->testing_handler->pubacks_received >= waiter->wait_for_count;
}

void mqtt_mock_server_wait_for_pubacks(struct aws_channel_handler *handler, size_t puback_count) {
    struct mqtt_mock_server_handler *testing_handler = handler->impl;

    struct puback_waiter waiter;
    waiter.testing_handler = testing_handler;
    waiter.wait_for_count = puback_count;

    aws_mutex_lock(&testing_handler->lock);
    aws_condition_variable_wait_pred(&testing_handler->cvar, &testing_handler->lock, s_is_pubacks_complete, &waiter);
    aws_mutex_unlock(&testing_handler->lock);
}

size_t mqtt_mock_server_decoded_packets_count(struct aws_channel_handler *handler) {
    struct mqtt_mock_server_handler *testing_handler = handler->impl;
    return aws_array_list_length(&testing_handler->packets);
}

struct mqtt_decoded_packet *mqtt_mock_server_get_decoded_packet_by_index(
    struct aws_channel_handler *handler,
    size_t i) {
    struct mqtt_mock_server_handler *testing_handler = handler->impl;
    AWS_FATAL_ASSERT(mqtt_mock_server_decoded_packets_count(handler) > i);
    struct mqtt_decoded_packet *packet = NULL;
    aws_array_list_get_at_ptr(&testing_handler->packets, (void **)&packet, i);
    return packet;
}

struct mqtt_decoded_packet *mqtt_mock_server_get_latest_decoded_packet(struct aws_channel_handler *handler) {
    size_t packet_count = mqtt_mock_server_decoded_packets_count(handler);
    AWS_FATAL_ASSERT(packet_count > 0);
    return mqtt_mock_server_get_decoded_packet_by_index(handler, packet_count - 1);
}

struct mqtt_decoded_packet *mqtt_mock_server_find_decoded_packet_by_ID(
    struct aws_channel_handler *handler,
    size_t search_start_idx,
    uint16_t packetID,
    int *out_idx) {
    struct mqtt_mock_server_handler *testing_handler = handler->impl;
    size_t len = aws_array_list_length(&testing_handler->packets);
    AWS_FATAL_ASSERT(search_start_idx < len);
    for (size_t i = search_start_idx; i < len; i++) {
        struct mqtt_decoded_packet *packet = NULL;
        aws_array_list_get_at_ptr(&testing_handler->packets, (void **)&packet, i);
        if (packet->packet_identifier == packetID) {
            if (out_idx) {
                *out_idx = (int)i;
            }
            return packet;
        }
    }
    if (out_idx) {
        *out_idx = -1;
    }
    return NULL;
}

struct mqtt_decoded_packet *mqtt_mock_server_find_decoded_packet_by_type(
    struct aws_channel_handler *handler,
    size_t search_start_idx,
    enum aws_mqtt_packet_type type,
    int *out_idx) {
    struct mqtt_mock_server_handler *testing_handler = handler->impl;
    size_t len = aws_array_list_length(&testing_handler->packets);
    AWS_FATAL_ASSERT(search_start_idx < len);
    for (size_t i = search_start_idx; i < len; i++) {
        struct mqtt_decoded_packet *packet = NULL;
        aws_array_list_get_at_ptr(&testing_handler->packets, (void **)&packet, i);
        if (packet->type == type) {
            if (out_idx) {
                *out_idx = (int)i;
            }
            return packet;
        }
    }
    if (out_idx) {
        *out_idx = -1;
    }
    return NULL;
}

int mqtt_mock_server_decode_packets(struct aws_channel_handler *handler) {
    struct mqtt_mock_server_handler *testing_handler = handler->impl;

    aws_mutex_lock(&testing_handler->lock);
    struct aws_array_list received_messages = testing_handler->synced_data.received_messages;
    size_t length = aws_array_list_length(&received_messages);
    if (testing_handler->decoded_index >= length) {
        AWS_LOGF_ERROR(MOCK_LOG_SUBJECT, "server, no new packet received. Stop decoding.");

        aws_mutex_unlock(&testing_handler->lock);
        return AWS_OP_ERR;
    }
    for (size_t index = testing_handler->decoded_index; index < length; index++) {
        struct aws_byte_buf received_message = {0};
        ASSERT_SUCCESS(aws_array_list_get_at(&received_messages, &received_message, index));
        struct aws_byte_cursor message_cur = aws_byte_cursor_from_buf(&received_message);

        struct mqtt_decoded_packet packet;
        packet.index = index;
        ASSERT_SUCCESS(s_mqtt_decoded_packet_init(&packet, handler->alloc));
        packet.type = aws_mqtt_get_packet_type(message_cur.ptr);

        switch (packet.type) {
            case AWS_MQTT_PACKET_CONNECT: {
                struct aws_mqtt_packet_connect connect_packet;
                AWS_ZERO_STRUCT(connect_packet);
                ASSERT_SUCCESS(aws_mqtt_packet_connect_decode(&message_cur, &connect_packet));
                packet.clean_session = connect_packet.clean_session;
                packet.has_will = connect_packet.has_will;
                packet.will_retain = connect_packet.will_retain;
                packet.has_password = connect_packet.has_password;
                packet.has_username = connect_packet.has_username;
                packet.keep_alive_timeout = connect_packet.keep_alive_timeout;
                packet.will_qos = connect_packet.will_qos;
                packet.client_identifier = connect_packet.client_identifier;
                if (packet.has_will) {
                    packet.will_topic = connect_packet.will_topic;
                    packet.will_message = connect_packet.will_message;
                }
                if (packet.has_username) {
                    packet.username = connect_packet.username;
                }
                if (packet.has_password) {
                    packet.password = connect_packet.password;
                }
                break;
            }
            case AWS_MQTT_PACKET_SUBSCRIBE: {
                struct aws_mqtt_packet_subscribe subscribe_packet;
                AWS_ZERO_STRUCT(subscribe_packet);
                ASSERT_SUCCESS(aws_mqtt_packet_subscribe_init(&subscribe_packet, testing_handler->handler.alloc, 0));
                ASSERT_SUCCESS(aws_mqtt_packet_subscribe_decode(&message_cur, &subscribe_packet));
                packet.packet_identifier = subscribe_packet.packet_identifier;
                /* copy the array one by one for simplicity */
                for (size_t i = 0; i < aws_array_list_length(&subscribe_packet.topic_filters); i++) {
                    struct aws_mqtt_subscription val;
                    ASSERT_SUCCESS(aws_array_list_get_at(&subscribe_packet.topic_filters, &val, i));
                    ASSERT_SUCCESS(aws_array_list_push_back(&packet.sub_topic_filters, &val));
                }
                aws_mqtt_packet_subscribe_clean_up(&subscribe_packet);
                break;
            }
            case AWS_MQTT_PACKET_UNSUBSCRIBE: {
                struct aws_mqtt_packet_unsubscribe unsubscribe_packet;
                AWS_ZERO_STRUCT(unsubscribe_packet);
                ASSERT_SUCCESS(
                    aws_mqtt_packet_unsubscribe_init(&unsubscribe_packet, testing_handler->handler.alloc, 0));
                ASSERT_SUCCESS(aws_mqtt_packet_unsubscribe_decode(&message_cur, &unsubscribe_packet));
                packet.packet_identifier = unsubscribe_packet.packet_identifier;
                /* copy the array one by one for simplicity */
                for (size_t i = 0; i < aws_array_list_length(&unsubscribe_packet.topic_filters); i++) {
                    struct aws_byte_cursor val;
                    ASSERT_SUCCESS(aws_array_list_get_at(&unsubscribe_packet.topic_filters, &val, i));
                    ASSERT_SUCCESS(aws_array_list_push_back(&packet.unsub_topic_filters, &val));
                }
                aws_mqtt_packet_unsubscribe_clean_up(&unsubscribe_packet);
                break;
            }
            case AWS_MQTT_PACKET_PUBLISH: {
                struct aws_mqtt_packet_publish publish_packet;
                aws_mqtt_packet_publish_decode(&message_cur, &publish_packet);
                packet.packet_identifier = publish_packet.packet_identifier;
                packet.topic_name = publish_packet.topic_name;
                packet.publish_payload = publish_packet.payload;
                break;
            }
            case AWS_MQTT_PACKET_PUBACK: {
                struct aws_mqtt_packet_ack puback;
                ASSERT_SUCCESS(aws_mqtt_packet_ack_decode(&message_cur, &puback));
                packet.packet_identifier = puback.packet_identifier;
                break;
            }
            case AWS_MQTT_PACKET_DISCONNECT:
            case AWS_MQTT_PACKET_PINGREQ:
                /* Nothing to decode, just record that type of packet has received */
                break;
            default:
                AWS_LOGF_ERROR(
                    MOCK_LOG_SUBJECT, "server, unsupported packet decoded, the package type is %d", packet.type);
                aws_mutex_unlock(&testing_handler->lock);
                return AWS_OP_ERR;
        }

        ASSERT_SUCCESS(aws_array_list_push_back(&testing_handler->packets, &packet));
    }
    testing_handler->decoded_index = length;
    aws_mutex_unlock(&testing_handler->lock);
    return AWS_OP_SUCCESS;
}
