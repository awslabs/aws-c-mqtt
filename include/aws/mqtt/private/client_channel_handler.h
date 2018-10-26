#ifndef AWS_MQTT_PRIVATE_CLIENT_CHANNEL_HANDLER_H
#define AWS_MQTT_PRIVATE_CLIENT_CHANNEL_HANDLER_H

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

#include <aws/mqtt/private/fixed_header.h>
#include <aws/mqtt/private/topic_tree.h>

#include <aws/common/hash_table.h>
#include <aws/common/task_scheduler.h>

#include <aws/io/channel.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/message_pool.h>
#include <aws/io/tls_channel_handler.h>

#define MQTT_CALL_CALLBACK(client_ptr, callback, ...)                                                                  \
    do {                                                                                                               \
        if (client_ptr->callbacks.callback) {                                                                          \
            client_ptr->callbacks.callback(client_ptr, __VA_ARGS__, client_ptr->callbacks.user_data);                  \
        }                                                                                                              \
    } while (false)

enum aws_mqtt_client_connection_state {
    AWS_MQTT_CLIENT_STATE_INIT,
    AWS_MQTT_CLIENT_STATE_CONNECTING,
    AWS_MQTT_CLIENT_STATE_CONNECTED,
    AWS_MQTT_CLIENT_STATE_DISCONNECTING,
};

extern const uint64_t request_timeout_ns;

/** Connection options specific to each host */
struct aws_mqtt_host {

    struct aws_allocator *allocator;
    struct aws_string *hostname;
    struct aws_client_bootstrap bootstrap;
    struct aws_tls_connection_options connection_options;
};

/* Called after the timeout if a matching ack packet hasn't arrived.
   Return true to consider request complete, false to schedule a timeout task. */
typedef bool(aws_mqtt_send_request_fn)(uint16_t message_id, bool is_first_attempt, void *userdata);

struct aws_mqtt_outstanding_request {
    struct aws_linked_list_node list_node;

    struct aws_allocator *allocator;
    struct aws_mqtt_client_connection *connection;

    struct aws_task timeout_task;

    uint16_t message_id;
    bool initiated;
    bool completed;
    bool cancelled;
    aws_mqtt_send_request_fn *send_request;
    void *send_request_ud;
    aws_mqtt_op_complete_fn *on_complete;
    void *on_complete_ud;
};

struct aws_mqtt_client_connection {

    struct aws_allocator *allocator;

    struct aws_mqtt_client *client;

    /* The host information */
    struct aws_mqtt_host *host;
    uint16_t port;
    struct aws_socket_options *socket_options;

    /* User callbacks */
    struct aws_mqtt_client_connection_callbacks callbacks;

    /* The state of the connection */
    enum aws_mqtt_client_connection_state state;

    /* Channel handler information */
    struct aws_channel_handler handler;
    struct aws_channel_slot *slot;

    /* Keeps track of all open subscriptions */
    struct aws_mqtt_topic_tree subscriptions;

    /* aws_mqtt_outstanding_request */
    struct aws_memory_pool requests_pool;
    /* uint16_t -> aws_mqtt_outstanding_request */
    struct aws_hash_table outstanding_requests;

    uint64_t last_pingresp_timestamp;

    /* Connect parameters */
    struct aws_byte_buf client_id;
    bool clean_session;
    uint16_t keep_alive_time;
    struct aws_string *username;
    struct aws_string *password;
    struct {
        struct aws_byte_buf topic;
        enum aws_mqtt_qos qos;
        bool retain;
        struct aws_byte_buf payload;
    } will;
};

struct aws_channel_handler_vtable *aws_mqtt_get_client_channel_vtable(void);

/* Helper for getting a message object for a packet */
struct aws_io_message *mqtt_get_message_for_packet(
    struct aws_mqtt_client_connection *connection,
    struct aws_mqtt_fixed_header *header);

/* This function registers a new outstanding request, calls send_request
 and returns the message identifier to use (or 0 on error). */
uint16_t mqtt_create_request(
    struct aws_mqtt_client_connection *connection,
    aws_mqtt_send_request_fn *send_request,
    void *send_request_ud,
    aws_mqtt_op_complete_fn *on_complete,
    void *on_complete_ud);

/* Call when an ack packet comes back from the server. */
void mqtt_request_complete(struct aws_mqtt_client_connection *connection, uint16_t message_id);

/* Call to close the connection with an error code */
void mqtt_disconnect_impl(struct aws_mqtt_client_connection *connection, int error_code);

#endif /* AWS_MQTT_PRIVATE_CLIENT_CHANNEL_HANDLER_H */
