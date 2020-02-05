/*
 * Copyright 2010-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
#include <aws/io/channel.h>
#include <aws/mqtt/private/packets.h>

struct mqtt_mock_server_handler {
    struct aws_channel_handler handler;
    struct aws_channel_slot *slot;
    struct aws_array_list response_messages;
    struct aws_array_list received_messages;
    size_t ping_resp_avail;
    uint16_t last_packet_id;
};

static int s_mqtt_mock_server_handler_process_read_message(
        struct aws_channel_handler *handler,
        struct aws_channel_slot *slot,
        struct aws_io_message *message) {

    struct mqtt_mock_server_handler *testing_handler = handler->impl;

    struct aws_byte_buf received_message;
    aws_byte_buf_init_copy(&received_message, handler->alloc, &message->message_data);
    aws_array_list_push_back(&testing_handler->received_messages, &received_message);

    struct aws_mqtt_packet_connection packet;
    struct aws_byte_cursor message_cur = aws_byte_cursor_from_buf(&message->message_data);
    aws_mqtt_packet_connection_decode(&message_cur, &packet);

    if (packet.fixed_header.packet_type == AWS_MQTT_PACKET_PINGREQ) {
        aws_mem_release(message->allocator, message);
        if (testing_handler->ping_resp_avail--) {
           struct aws_io_message *ping_resp = aws_channel_acquire_message_from_pool(slot->channel, AWS_IO_MESSAGE_APPLICATION_DATA, 256);
           aws_mqtt_packet_pingresp_init(&packet);
           aws_mqtt_packet_connection_encode(&ping_resp->message_data, &packet);
           aws_channel_slot_send_message(slot, ping_resp, AWS_CHANNEL_DIR_WRITE);
        }
        return AWS_OP_SUCCESS;
    }

    if (packet.fixed_header.packet_type == AWS_MQTT_PACKET_SUBSCRIBE) {
        struct aws_mqtt_packet_subscribe subscribe_packet;
        aws_mqtt_packet_subscribe_init(&subscribe_packet, handler->alloc, 0);
        message_cur = aws_byte_cursor_from_buf(&message->message_data);
        aws_mqtt_packet_subscribe_decode(&message_cur, &subscribe_packet);
        aws_mem_release(message->allocator, message);

        struct aws_io_message *suback_msg = aws_channel_acquire_message_from_pool(slot->channel, AWS_IO_MESSAGE_APPLICATION_DATA, 256);
        struct aws_mqtt_packet_ack suback;
        aws_mqtt_packet_suback_init(&suback, subscribe_packet.packet_identifier);
        aws_mqtt_packet_subscribe_clean_up(&subscribe_packet);
        aws_mqtt_packet_ack_encode(&suback_msg->message_data, &suback);
        aws_channel_slot_send_message(slot, suback_msg, AWS_CHANNEL_DIR_WRITE);
        return AWS_OP_SUCCESS;
    }

    aws_mem_release(message->allocator, message);

    if (aws_array_list_length(&testing_handler->response_messages)) {
        struct aws_byte_buf response_message;
        aws_array_list_front(&testing_handler->response_messages, &response_message);
        aws_array_list_pop_front(&testing_handler->response_messages);

        struct aws_io_message *reply = aws_channel_acquire_message_from_pool(slot->channel, AWS_IO_MESSAGE_APPLICATION_DATA, response_message.len);
        aws_byte_buf_write_from_whole_buffer(&reply->message_data, response_message);
        aws_channel_slot_send_message(slot, reply, AWS_CHANNEL_DIR_WRITE);
    }

    return AWS_OP_SUCCESS;
}

struct mqtt_mock_server_publish_args {
    struct aws_channel_task task;
    struct aws_byte_cursor topic;
    struct aws_byte_cursor payload;
    enum aws_mqtt_qos qos;
    struct mqtt_mock_server_handler *testing_handler;
};

static void s_mqtt_send_publish_in_thread(struct aws_channel_task *channel_task, void *arg, enum aws_task_status status) {
    (void)channel_task;
    (void)status;
    struct mqtt_mock_server_publish_args *publish_args = arg;
    struct aws_mqtt_packet_publish publish;
    aws_mqtt_packet_publish_init(&publish, false, publish_args->qos, false, publish_args->topic, publish_args->testing_handler->last_packet_id++, publish_args->payload);
    struct aws_io_message *publish_msg = aws_channel_acquire_message_from_pool(publish_args->testing_handler->slot->channel, AWS_IO_MESSAGE_APPLICATION_DATA, publish_args->payload.len + 256);

    aws_mqtt_packet_publish_encode(&publish_msg->message_data, &publish);
    aws_channel_slot_send_message(publish_args->testing_handler->slot, publish_msg, AWS_CHANNEL_DIR_WRITE);
    aws_mem_release(publish_args->testing_handler->handler.alloc, publish_args);

}


static int s_mqtt_mock_server_send_publish(
        struct aws_channel_handler *handler,
        struct aws_byte_cursor *topic,
        struct aws_byte_cursor *payload,
        enum aws_mqtt_qos qos) {

    struct mqtt_mock_server_handler *testing_handler = handler->impl;

    struct mqtt_mock_server_publish_args *args = aws_mem_calloc(handler->alloc, 1, sizeof(struct mqtt_mock_server_publish_args));
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

static void s_mqtt_mock_server_handler_update_slot(
        struct aws_channel_handler *handler,
        struct aws_channel_slot *slot) {
    (void)handler;
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

static struct aws_channel_handler *s_new_mqtt_mock_server(
        struct aws_allocator *allocator) {

    struct mqtt_mock_server_handler *testing_handler = aws_mem_calloc(allocator, 1, sizeof(struct mqtt_mock_server_handler));
    aws_array_list_init_dynamic(&testing_handler->received_messages, allocator, 4, sizeof(struct aws_byte_buf));
    aws_array_list_init_dynamic(&testing_handler->response_messages, allocator, 4, sizeof(struct aws_byte_buf));

    testing_handler->handler.impl = testing_handler;
    testing_handler->handler.vtable = &s_mqtt_mock_server_handler_vtable;
    testing_handler->handler.alloc = allocator;

    return &testing_handler->handler;
}

static void s_destroy_mqtt_mock_server(struct aws_channel_handler *handler) {
    struct mqtt_mock_server_handler *testing_handler = handler->impl;

    for (size_t i = 0; i < aws_array_list_length(&testing_handler->received_messages); ++i) {
        struct aws_byte_buf *byte_buf_ptr = NULL;
        aws_array_list_get_at_ptr(&testing_handler->received_messages, (void **)&byte_buf_ptr, i);
        aws_byte_buf_clean_up(byte_buf_ptr);
    }
    aws_array_list_clean_up(&testing_handler->received_messages);

    for (size_t i = 0; i < aws_array_list_length(&testing_handler->response_messages); ++i) {
        struct aws_byte_buf *byte_buf_ptr = NULL;
        aws_array_list_get_at_ptr(&testing_handler->response_messages, (void **)&byte_buf_ptr, i);
        aws_byte_buf_clean_up(byte_buf_ptr);
    }
    aws_array_list_clean_up(&testing_handler->response_messages);

    aws_mem_release(handler->alloc, testing_handler);
}

static int s_mqtt_mock_server_push_reply_message(struct aws_channel_handler *handler, struct aws_byte_buf *buf) {
    struct mqtt_mock_server_handler *testing_handler = handler->impl;

    return aws_array_list_push_back(&testing_handler->response_messages, buf);
}

static struct aws_array_list *s_mqtt_mock_server_get_received_messages(struct aws_channel_handler *handler) {
    struct mqtt_mock_server_handler *testing_handler = handler->impl;

    return &testing_handler->received_messages;
}