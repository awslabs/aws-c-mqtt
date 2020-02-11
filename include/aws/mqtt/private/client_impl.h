#ifndef AWS_MQTT_PRIVATE_CLIENT_IMPL_H
#define AWS_MQTT_PRIVATE_CLIENT_IMPL_H

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

#include <aws/mqtt/client.h>

#include <aws/mqtt/private/fixed_header.h>
#include <aws/mqtt/private/topic_tree.h>

#include <aws/common/hash_table.h>
#include <aws/common/mutex.h>
#include <aws/common/task_scheduler.h>

#include <aws/io/channel.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/message_pool.h>
#include <aws/io/socket.h>
#include <aws/io/tls_channel_handler.h>

#define MQTT_CLIENT_CALL_CALLBACK(client_ptr, callback)                                                                \
    do {                                                                                                               \
        if ((client_ptr)->callback) {                                                                                  \
            (client_ptr)->callback((client_ptr), (client_ptr)->callback##_ud);                                         \
        }                                                                                                              \
    } while (false)
#define MQTT_CLIENT_CALL_CALLBACK_ARGS(client_ptr, callback, ...)                                                      \
    do {                                                                                                               \
        if ((client_ptr)->callback) {                                                                                  \
            (client_ptr)->callback((client_ptr), __VA_ARGS__, (client_ptr)->callback##_ud);                            \
        }                                                                                                              \
    } while (false)

enum aws_mqtt_client_connection_state {
    AWS_MQTT_CLIENT_STATE_CONNECTING,
    AWS_MQTT_CLIENT_STATE_CONNECTED,
    AWS_MQTT_CLIENT_STATE_RECONNECTING,
    AWS_MQTT_CLIENT_STATE_DISCONNECTING,
    AWS_MQTT_CLIENT_STATE_DISCONNECTED,
};

enum aws_mqtt_client_request_state {
    AWS_MQTT_CLIENT_REQUEST_ONGOING,
    AWS_MQTT_CLIENT_REQUEST_COMPLETE,
    AWS_MQTT_CLIENT_REQUEST_ERROR,
};

/* Called after the timeout if a matching ack packet hasn't arrived.
   Return AWS_MQTT_CLIENT_REQUEST_ONGOING to check on the task later.
   Return AWS_MQTT_CLIENT_REQUEST_COMPLETE to consider request complete.
   Return AWS_MQTT_CLIENT_REQUEST_ERROR cancel the task and report an error to the caller. */
typedef enum aws_mqtt_client_request_state(
    aws_mqtt_send_request_fn)(uint16_t message_id, bool is_first_attempt, void *userdata);

struct aws_mqtt_outstanding_request {
    struct aws_linked_list_node list_node;

    struct aws_allocator *allocator;
    struct aws_mqtt_client_connection *connection;

    struct aws_channel_task timeout_task;

    uint16_t message_id;
    bool initiated;
    bool completed;
    bool cancelled;
    aws_mqtt_send_request_fn *send_request;
    void *send_request_ud;
    aws_mqtt_op_complete_fn *on_complete;
    void *on_complete_ud;
};

struct aws_mqtt_reconnect_task {
    struct aws_task task;
    struct aws_atomic_var connection_ptr;
    struct aws_allocator *allocator;
};

struct aws_mqtt_client_connection {

    struct aws_allocator *allocator;

    struct aws_mqtt_client *client;

    /* The host information */
    struct aws_string *host_name;
    uint16_t port;
    struct aws_tls_connection_options tls_options;
    struct aws_socket_options socket_options;

    /* User connection callbacks */
    aws_mqtt_client_on_connection_complete_fn *on_connection_complete;
    void *on_connection_complete_ud;
    aws_mqtt_client_on_disconnect_fn *on_disconnect;
    void *on_disconnect_ud;
    aws_mqtt_client_on_connection_interrupted_fn *on_interrupted;
    void *on_interrupted_ud;
    aws_mqtt_client_on_connection_resumed_fn *on_resumed;
    void *on_resumed_ud;

    /* The state of the connection */
    enum aws_mqtt_client_connection_state state;

    /* Channel handler information */
    struct aws_channel_handler handler;
    struct aws_channel_slot *slot;

    /* Keeps track of all open subscriptions */
    struct aws_mqtt_topic_tree subscriptions;
    aws_mqtt_client_publish_received_fn *on_any_publish;
    void *on_any_publish_ud;

    /* aws_mqtt_outstanding_request */
    struct aws_memory_pool requests_pool;
    struct {
        /* uint16_t (packet id) -> aws_mqtt_outstanding_request */
        struct aws_hash_table table;
        struct aws_mutex mutex;
    } outstanding_requests;
    /* List of all requests that cannot be scheduled until the connection comes online */
    struct {
        struct aws_linked_list list;
        struct aws_mutex mutex;
    } pending_requests;
    struct aws_mqtt_reconnect_task *reconnect_task;
    struct aws_channel_task ping_task;

    struct {
        uint64_t current;      /* seconds */
        uint64_t min;          /* seconds */
        uint64_t max;          /* seconds */
        uint64_t next_attempt; /* milliseconds */
    } reconnect_timeouts;

    /* If an incomplete packet arrives, store the data here. */
    struct aws_byte_buf pending_packet;

    /* Connect parameters */
    struct aws_byte_buf client_id;
    bool clean_session;
    uint16_t keep_alive_time_secs;
    uint64_t request_timeout_ns;
    struct aws_string *username;
    struct aws_string *password;
    struct {
        struct aws_byte_buf topic;
        enum aws_mqtt_qos qos;
        bool retain;
        struct aws_byte_buf payload;
    } will;

    struct {
        aws_mqtt_transform_websocket_handshake_fn *handshake_transformer;
        void *handshake_transformer_ud;
        aws_mqtt_validate_websocket_handshake_fn *handshake_validator;
        void *handshake_validator_ud;
        bool enabled;

        struct {
            struct aws_byte_buf host;
            struct aws_byte_buf auth_username;
            struct aws_byte_buf auth_password;
            struct aws_tls_connection_options tls_options;
        } * proxy;
        struct aws_http_proxy_options *proxy_options;

        struct aws_http_message *handshake_request;
    } websocket;

    /* number of times this connection has successfully CONNACK-ed, used
     * to ensure on_connection_completed is sent on the first completed
     * CONNECT/CONNACK cycle */
    size_t connection_count;

    bool waiting_on_ping_response;
    bool use_tls;
};

struct aws_channel_handler_vtable *aws_mqtt_get_client_channel_vtable(void);

/* Helper for getting a message object for a packet */
struct aws_io_message *mqtt_get_message_for_packet(
    struct aws_mqtt_client_connection *connection,
    struct aws_mqtt_fixed_header *header);

/* This function registers a new outstanding request, calls send_request
 and returns the message identifier to use (or 0 on error). */
AWS_MQTT_API uint16_t mqtt_create_request(
    struct aws_mqtt_client_connection *connection,
    aws_mqtt_send_request_fn *send_request,
    void *send_request_ud,
    aws_mqtt_op_complete_fn *on_complete,
    void *on_complete_ud);

/* Call when an ack packet comes back from the server. */
AWS_MQTT_API void mqtt_request_complete(
    struct aws_mqtt_client_connection *connection,
    int error_code,
    uint16_t message_id);

/* Call to close the connection with an error code */
AWS_MQTT_API void mqtt_disconnect_impl(struct aws_mqtt_client_connection *connection, int error_code);

/* Creates the task used to reestablish a broken connection */
AWS_MQTT_API void aws_create_reconnect_task(struct aws_mqtt_client_connection *connection);

/*
 * Sends a PINGREQ packet to the server to keep the connection alive. This is not exported and should not ever
 * be called directly. This function is driven by the timeout values passed to aws_mqtt_client_connect().
 * If a PINGRESP is not received within a reasonable period of time, the connection will be closed.
 *
 * \params[in] connection   The connection to ping on
 *
 * \returns AWS_OP_SUCCESS if the connection is open and the PINGREQ is sent or queued to send,
 *              otherwise AWS_OP_ERR and aws_last_error() is set.
 */
int aws_mqtt_client_connection_ping(struct aws_mqtt_client_connection *connection);

#endif /* AWS_MQTT_PRIVATE_CLIENT_IMPL_H */
