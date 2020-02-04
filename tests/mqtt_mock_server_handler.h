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
    struct aws_linked_list response_messages;
    struct aws_linked_list received_messages;
};

static int s_mqtt_mock_server_handler_process_read_message(
        struct aws_channel_handler *handler,
        struct aws_channel_slot *slot,
        struct aws_io_message *message) {

    struct mqtt_mock_server_handler *testing_handler = handler->impl;
    aws_linked_list_push_back(&testing_handler->received_messages, &message->queueing_handle);


    if (!aws_linked_list_empty(&testing_handler->response_messages)) {
        struct aws_linked_list_node *reply = aws_linked_list_pop_front(&testing_handler->response_messages);
        aws_channel_slot_send_message(slot, AWS_CONTAINER_OF(reply, struct aws_io_message, queueing_handle), AWS_CHANNEL_DIR_WRITE);
    }

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

static void s_mqtt_mock_server_release_response_messages(struct mqtt_mock_server_handler *handler) {
    while (!aws_linked_list_empty(&handler->response_messages)) {
        struct aws_linked_list_node *node = aws_linked_list_pop_front(&handler->response_messages);
        struct aws_io_message *msg = AWS_CONTAINER_OF(node, struct aws_io_message, queueing_handle);
        aws_mem_release(msg->allocator, msg);
    }
}

/*
static void s_mqtt_mock_server_release_received_messages(struct mqtt_mock_server_handler *handler) {
    while (!aws_linked_list_empty(&handler->received_messages)) {
        struct aws_linked_list_node *node = aws_linked_list_pop_front(&handler->received_messages);
        struct aws_io_message *msg = AWS_CONTAINER_OF(node, struct aws_io_message, queueing_handle);
        aws_mem_release(msg->allocator, msg);
    }
}*/

static void s_mqtt_mock_server_handler_destroy(struct aws_channel_handler *handler) {
    struct mqtt_mock_server_handler *testing_handler = handler->impl;

    s_mqtt_mock_server_release_response_messages(testing_handler);
    /* don't clean the received messages since we're going to verify them */

    aws_mem_release(handler->alloc, testing_handler);
    aws_mem_release(handler->alloc, handler);
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
    aws_linked_list_init(&testing_handler->received_messages);
    aws_linked_list_init(&testing_handler->response_messages);

    testing_handler->handler.impl = testing_handler;
    testing_handler->handler.vtable = &s_mqtt_mock_server_handler_vtable;
    testing_handler->handler.alloc = allocator;

    return &testing_handler->handler;
}