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

#include <aws/mqtt/private/client_impl.h>
#include <aws/mqtt/private/packets.h>
#include <aws/mqtt/private/topic_tree.h>

#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/io/socket.h>
#include <aws/io/tls_channel_handler.h>

#include <aws/common/clock.h>
#include <aws/common/task_scheduler.h>

#include <assert.h>

#ifdef _MSC_VER
#    pragma warning(disable : 4204)
#endif

/*******************************************************************************
 * Client Init
 ******************************************************************************/

int aws_mqtt_client_init(
    struct aws_mqtt_client *client,
    struct aws_allocator *allocator,
    struct aws_client_bootstrap *bootstrap) {

    AWS_ZERO_STRUCT(*client);
    client->allocator = allocator;
    client->bootstrap = bootstrap;

    return AWS_OP_SUCCESS;
}

/*******************************************************************************
 * Client Clean Up
 ******************************************************************************/

void aws_mqtt_client_clean_up(struct aws_mqtt_client *client) {

    AWS_ZERO_STRUCT(*client);
}

/*******************************************************************************
 * Connection New
 ******************************************************************************/

/**
 * Channel has been initialized callback. Sets up channel handler and sends out CONNECT packet.
 * The on_connack callback is called with the CONNACK packet is received from the server.
 */
static void s_mqtt_client_init(
    struct aws_client_bootstrap *bootstrap,
    int error_code,
    struct aws_channel *channel,
    void *user_data) {

    (void)bootstrap;

    struct aws_mqtt_client_connection *connection = user_data;

    if (error_code != AWS_OP_SUCCESS) {
        MQTT_CLIENT_CALL_CALLBACK(connection, on_connection_failed, error_code);
        return;
    }

    /* Reset the current timeout timer */
    connection->reconnect_timeouts.current = connection->reconnect_timeouts.min;

    /* Create the slot and handler */
    connection->slot = aws_channel_slot_new(channel);

    if (!connection->slot) {
        MQTT_CLIENT_CALL_CALLBACK(connection, on_connection_failed, aws_last_error());
        return;
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

    if (connection->will.topic.buffer) {
        /* Add will if present */
        aws_mqtt_packet_connect_add_will(
            &connect,
            aws_byte_cursor_from_buf(&connection->will.topic),
            connection->will.qos,
            connection->will.retain,
            aws_byte_cursor_from_buf(&connection->will.payload));
    }

    if (connection->username) {
        struct aws_byte_cursor username_cur = aws_byte_cursor_from_string(connection->username);
        struct aws_byte_cursor password_cur = {
            .ptr = NULL,
            .len = 0,
        };

        if (connection->password) {
            password_cur = aws_byte_cursor_from_string(connection->password);
        }

        aws_mqtt_packet_connect_add_credentials(&connect, username_cur, password_cur);
    }

    struct aws_io_message *message = mqtt_get_message_for_packet(connection, &connect.fixed_header);
    if (!message) {
        goto handle_error;
    }
    struct aws_byte_cursor message_cursor = {
        .ptr = message->message_data.buffer,
        .len = message->message_data.capacity,
    };
    if (aws_mqtt_packet_connect_encode(&message_cursor, &connect)) {
        goto handle_error;
    }
    message->message_data.len = message->message_data.capacity - message_cursor.len;

    if (aws_channel_slot_send_message(connection->slot, message, AWS_CHANNEL_DIR_WRITE)) {
        goto handle_error;
    }

    return;

handle_error:
    MQTT_CLIENT_CALL_CALLBACK(connection, on_connection_failed, aws_last_error());

    if (message) {
        aws_channel_release_message_to_pool(connection->slot->channel, message);
    }
}

static void s_attempt_reconect(struct aws_task *task, void *userdata, enum aws_task_status status) {
    struct aws_mqtt_client_connection *connection = userdata;

    if (status == AWS_TASK_STATUS_RUN_READY && !connection->slot) {
        /* If the task is not cancelled and a connection has not succeeded, attempt reconnect */

        aws_mqtt_client_connection_connect(connection, NULL, connection->clean_session, connection->keep_alive_time);

        struct aws_event_loop *el = aws_event_loop_group_get_next_loop(connection->client->bootstrap->event_loop_group);

        uint64_t ttr = 0;
        aws_event_loop_current_clock_time(el, &ttr);
        ttr += aws_timestamp_convert(
            connection->reconnect_timeouts.current, AWS_TIMESTAMP_SECS, AWS_TIMESTAMP_NANOS, NULL);

        connection->reconnect_timeouts.current *= 2;
        if (connection->reconnect_timeouts.current > connection->reconnect_timeouts.max) {
            connection->reconnect_timeouts.current = connection->reconnect_timeouts.max;
        }

        /* Schedule checkup task */
        aws_event_loop_schedule_task_future(el, task, ttr);
    } else {

        aws_mem_release(connection->allocator, task);
    }
}

static void s_mqtt_client_shutdown(
    struct aws_client_bootstrap *bootstrap,
    int error_code,
    struct aws_channel *channel,
    void *user_data) {

    (void)bootstrap;
    (void)channel;

    struct aws_mqtt_client_connection *connection = user_data;

    /* Always clear slot, as that's what's been shutdown */
    aws_channel_slot_remove(connection->slot);
    connection->slot = NULL;

    if (connection->state != AWS_MQTT_CLIENT_STATE_DISCONNECTING) {
        /* Unintentionally disconnecting, reconnect */

        struct aws_task *reconnect_task = aws_mem_acquire(connection->allocator, sizeof(struct aws_task));
        aws_task_init(reconnect_task, s_attempt_reconect, connection);

        /* Attempt the reconnect immediately */
        reconnect_task->fn(reconnect_task, reconnect_task->arg, AWS_TASK_STATUS_RUN_READY);
    }

    /* Alert the connection we've shutdown */
    MQTT_CLIENT_CALL_CALLBACK(connection, on_disconnect, error_code);
}

static uint64_t s_hash_uint16_t(const void *item) {
    return *(uint16_t *)item;
}

static bool s_uint16_t_eq(const void *a, const void *b) {
    return *(uint16_t *)a == *(uint16_t *)b;
}

static void s_outstanding_request_destroy(void *item) {
    struct aws_mqtt_outstanding_request *request = item;

    if (request->cancelled) {
        /* Task ran as cancelled already, clean up the memory */
        aws_mem_release(request->allocator, request);
    } else {
        /* Signal task to clean up request */
        request->cancelled = true;
    }
}

struct aws_mqtt_client_connection *aws_mqtt_client_connection_new(
    struct aws_mqtt_client *client,
    struct aws_mqtt_client_connection_callbacks callbacks,
    const struct aws_byte_cursor *host_name,
    uint16_t port,
    struct aws_socket_options *socket_options,
    struct aws_tls_connection_options *tls_options) {

    assert(client);

    struct aws_mqtt_client_connection *connection =
        aws_mem_acquire(client->allocator, sizeof(struct aws_mqtt_client_connection));

    if (!connection) {
        return NULL;
    }

    /* Initialize the client */
    AWS_ZERO_STRUCT(*connection);
    connection->allocator = client->allocator;
    connection->client = client;
    connection->host_name = aws_string_new_from_array(connection->allocator, host_name->ptr, host_name->len);
    connection->port = port;
    connection->tls_options = tls_options;
    connection->socket_options = socket_options;
    connection->callbacks = callbacks;
    connection->state = AWS_MQTT_CLIENT_STATE_INIT;
    connection->reconnect_timeouts.min = 1;
    connection->reconnect_timeouts.max = 128;
    aws_linked_list_init(&connection->pending_requests.list);

    if (aws_mutex_init(&connection->pending_requests.mutex)) {

        goto handle_error;
    }

    if (aws_mqtt_topic_tree_init(&connection->subscriptions, connection->allocator)) {

        goto handle_error;
    }

    if (aws_hash_table_init(
            &connection->outstanding_requests,
            connection->allocator,
            sizeof(struct aws_mqtt_outstanding_request *),
            s_hash_uint16_t,
            s_uint16_t_eq,
            NULL,
            &s_outstanding_request_destroy)) {

        goto handle_error;
    }
    if (aws_memory_pool_init(
            &connection->requests_pool, connection->allocator, 32, sizeof(struct aws_mqtt_outstanding_request))) {

        goto handle_error;
    }

    /* Initialize the handler */
    connection->handler.alloc = connection->allocator;
    connection->handler.vtable = aws_mqtt_get_client_channel_vtable();
    connection->handler.impl = connection;

    return connection;

handle_error:

    aws_mqtt_topic_tree_clean_up(&connection->subscriptions);

    if (connection->outstanding_requests.p_impl) {
        aws_hash_table_clean_up(&connection->outstanding_requests);
    }

    if (connection->requests_pool.data_ptr) {
        aws_memory_pool_clean_up(&connection->requests_pool);
    }

    if (connection) {
        aws_mem_release(client->allocator, connection);
    }

    return NULL;
}

/*******************************************************************************
 * Connection Destroy
 ******************************************************************************/

void aws_mqtt_client_connection_destroy(struct aws_mqtt_client_connection *connection) {

    assert(connection);

    aws_string_destroy(connection->host_name);

    /* Clear the credentials */
    if (connection->username) {
        aws_string_destroy_secure(connection->username);
        connection->username = NULL;
    }
    if (connection->password) {
        aws_string_destroy_secure(connection->password);
        connection->password = NULL;
    }

    /* Clean up the will */
    aws_byte_buf_clean_up(&connection->will.topic);
    aws_byte_buf_clean_up(&connection->will.payload);

    /* Clear the client_id */
    aws_byte_buf_clean_up(&connection->client_id);

    /* Free all of the active subscriptions */
    aws_mqtt_topic_tree_clean_up(&connection->subscriptions);

    /* Cleanup outstanding requests */
    aws_hash_table_clean_up(&connection->outstanding_requests);
    aws_memory_pool_clean_up(&connection->requests_pool);

    if (connection->slot) {
        aws_channel_slot_remove(connection->slot);
    }

    /* Frees all allocated memory */
    aws_mem_release(connection->allocator, connection);
}

/*******************************************************************************
 * Connection Configuration
 ******************************************************************************/

int aws_mqtt_client_connection_set_will(
    struct aws_mqtt_client_connection *connection,
    const struct aws_byte_cursor *topic,
    enum aws_mqtt_qos qos,
    bool retain,
    const struct aws_byte_cursor *payload) {

    if (!aws_mqtt_is_valid_topic(topic)) {
        return aws_raise_error(AWS_ERROR_MQTT_INVALID_TOPIC);
    }

    struct aws_byte_buf topic_buf = aws_byte_buf_from_array(topic->ptr, topic->len);
    if (aws_byte_buf_init_copy(connection->allocator, &connection->will.topic, &topic_buf)) {
        goto cleanup;
    }

    connection->will.qos = qos;
    connection->will.retain = retain;

    struct aws_byte_buf payload_buf = aws_byte_buf_from_array(payload->ptr, payload->len);
    if (aws_byte_buf_init_copy(connection->allocator, &connection->will.payload, &payload_buf)) {
        goto cleanup;
    }

    return AWS_OP_SUCCESS;

cleanup:
    aws_byte_buf_clean_up(&connection->will.topic);
    aws_byte_buf_clean_up(&connection->will.payload);

    return AWS_OP_ERR;
}

int aws_mqtt_client_connection_set_login(
    struct aws_mqtt_client_connection *connection,
    const struct aws_byte_cursor *username,
    const struct aws_byte_cursor *password) {

    assert(connection);
    assert(username);

    connection->username = aws_string_new_from_array(connection->allocator, username->ptr, username->len);
    if (!connection->username) {
        return AWS_OP_ERR;
    }

    if (password) {
        connection->password = aws_string_new_from_array(connection->allocator, password->ptr, password->len);
        if (!connection->password) {
            return AWS_OP_ERR;
        }
    }

    return AWS_OP_SUCCESS;
}

int aws_mqtt_client_connection_set_reconnect_timeout(
    struct aws_mqtt_client_connection *connection,
    uint64_t min_timeout,
    uint64_t max_timeout) {

    assert(connection);

    connection->reconnect_timeouts.min = min_timeout;
    connection->reconnect_timeouts.max = max_timeout;

    return AWS_OP_SUCCESS;
}

/*******************************************************************************
 * Connect
 ******************************************************************************/

int aws_mqtt_client_connection_connect(
    struct aws_mqtt_client_connection *connection,
    const struct aws_byte_cursor *client_id,
    bool clean_session,
    uint16_t keep_alive_time) {

    connection->state = AWS_MQTT_CLIENT_STATE_CONNECTING;
    connection->clean_session = clean_session;
    connection->keep_alive_time = keep_alive_time;

    if (client_id) {
        /* Only set connection->client_id if a new one was provided */
        struct aws_byte_buf client_id_buf = aws_byte_buf_from_array(client_id->ptr, client_id->len);
        if (aws_byte_buf_init_copy(connection->allocator, &connection->client_id, &client_id_buf)) {
            return AWS_OP_ERR;
        }
    } else {
        /* If client_id not passed, one must already be set. */
        assert(connection->client_id.buffer);
    }

    if (clean_session) {
        /* If clean_session is set, all subscriptions will be reset by the server,
        so we can clean the local tree out too. */
        aws_mqtt_topic_tree_clean_up(&connection->subscriptions);
        aws_mqtt_topic_tree_init(&connection->subscriptions, connection->allocator);
    }

    int result = 0;
    if (connection->tls_options) {
        result = aws_client_bootstrap_new_tls_socket_channel(
            connection->client->bootstrap,
            (const char *)aws_string_bytes(connection->host_name),
            connection->port,
            connection->socket_options,
            connection->tls_options,
            &s_mqtt_client_init,
            &s_mqtt_client_shutdown,
            connection);
    } else {
        result = aws_client_bootstrap_new_socket_channel(
            connection->client->bootstrap,
            (const char *)aws_string_bytes(connection->host_name),
            connection->port,
            connection->socket_options,
            &s_mqtt_client_init,
            &s_mqtt_client_shutdown,
            connection);
    }
    if (result) {
        /* Connection attempt failed */
        MQTT_CLIENT_CALL_CALLBACK(connection, on_connection_failed, aws_last_error());
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

/*******************************************************************************
 * Disconnect
 ******************************************************************************/

int aws_mqtt_client_connection_disconnect(struct aws_mqtt_client_connection *connection) {

    if (connection && connection->slot) {
        mqtt_disconnect_impl(connection, AWS_OP_SUCCESS);
    }

    return AWS_OP_SUCCESS;
}

/*******************************************************************************
 * Subscribe
 ******************************************************************************/

struct subscribe_task_arg {
    struct aws_mqtt_client_connection *connection;
    const struct aws_string *filter;
    enum aws_mqtt_qos qos;

    aws_mqtt_client_publish_received_fn *on_publish;
    void *on_publish_ud;
};

static void s_on_publish_client_wrapper(
    const struct aws_byte_cursor *topic,
    const struct aws_byte_cursor *payload,
    void *userdata) {

    struct subscribe_task_arg *task_arg = userdata;

    /* Call out to the user callback */
    task_arg->on_publish(task_arg->connection, topic, payload, task_arg->on_publish_ud);
}

static void s_on_topic_clean_up(void *userdata) {

    struct subscribe_task_arg *task_arg = userdata;
    aws_mem_release(task_arg->connection->allocator, task_arg);
}

static bool s_subscribe_send(uint16_t message_id, bool is_first_attempt, void *userdata) {
    (void)is_first_attempt;
    struct subscribe_task_arg *task_arg = userdata;

    struct aws_io_message *message = NULL;

    struct aws_byte_cursor topic_cursor = aws_byte_cursor_from_string(task_arg->filter);

    /* Send the subscribe packet */
    struct aws_mqtt_packet_subscribe subscribe;
    if (aws_mqtt_packet_subscribe_init(&subscribe, task_arg->connection->allocator, message_id)) {
        goto handle_error;
    }
    if (aws_mqtt_packet_subscribe_add_topic(&subscribe, topic_cursor, task_arg->qos)) {
        goto handle_error;
    }

    message = mqtt_get_message_for_packet(task_arg->connection, &subscribe.fixed_header);
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

    if (aws_channel_slot_send_message(task_arg->connection->slot, message, AWS_CHANNEL_DIR_WRITE)) {
        goto handle_error;
    }

    if (aws_mqtt_topic_tree_insert(
            &task_arg->connection->subscriptions,
            task_arg->filter,
            task_arg->qos,
            s_on_publish_client_wrapper,
            s_on_topic_clean_up,
            task_arg)) {

        goto handle_error;
    }

    return false;

handle_error:

    aws_mqtt_packet_subscribe_clean_up(&subscribe);

    if (message) {
        aws_channel_release_message_to_pool(task_arg->connection->slot->channel, message);
    }

    aws_mqtt_topic_tree_remove(&task_arg->connection->subscriptions, &topic_cursor);

    aws_mem_release(task_arg->connection->allocator, task_arg);

    return true;
}

uint16_t aws_mqtt_client_connection_subscribe(
    struct aws_mqtt_client_connection *connection,
    const struct aws_byte_cursor *topic_filter,
    enum aws_mqtt_qos qos,
    aws_mqtt_client_publish_received_fn *on_publish,
    void *on_publish_ud,
    aws_mqtt_op_complete_fn *on_suback,
    void *on_suback_ud) {

    assert(connection);

    if (!aws_mqtt_is_valid_topic_filter(topic_filter)) {
        aws_raise_error(AWS_ERROR_MQTT_INVALID_TOPIC);
        return 0;
    }

    struct subscribe_task_arg *task_arg = aws_mem_acquire(connection->allocator, sizeof(struct subscribe_task_arg));
    if (!task_arg) {
        goto handle_error;
    }

    task_arg->connection = connection;
    task_arg->on_publish = on_publish;
    task_arg->on_publish_ud = on_publish_ud;

    task_arg->qos = qos;
    task_arg->filter = aws_string_new_from_array(connection->allocator, topic_filter->ptr, topic_filter->len);
    if (!task_arg->filter) {
        goto handle_error;
    }

    uint16_t packet_id =
        mqtt_create_request(task_arg->connection, &s_subscribe_send, task_arg, on_suback, on_suback_ud);

    if (packet_id) {
        return packet_id;
    }

handle_error:

    if (task_arg) {
        if (task_arg->filter) {
            aws_string_destroy((void *)task_arg->filter);
        }
        aws_mem_release(connection->allocator, task_arg);
    }
    return 0;
}

/*******************************************************************************
 * Unsubscribe
 ******************************************************************************/

struct unsubscribe_task_arg {
    struct aws_mqtt_client_connection *connection;
    struct aws_byte_cursor filter;
};

static bool s_unsubscribe_send(uint16_t message_id, bool is_first_attempt, void *userdata) {
    (void)is_first_attempt;

    struct unsubscribe_task_arg *task_arg = userdata;
    struct aws_io_message *message = NULL;

    aws_mqtt_topic_tree_remove(&task_arg->connection->subscriptions, &task_arg->filter);

    /* Send the unsubscribe packet */
    struct aws_mqtt_packet_unsubscribe unsubscribe;
    if (aws_mqtt_packet_unsubscribe_init(&unsubscribe, task_arg->connection->allocator, message_id)) {
        goto handle_error;
    }
    if (aws_mqtt_packet_unsubscribe_add_topic(&unsubscribe, task_arg->filter)) {
        goto handle_error;
    }

    message = mqtt_get_message_for_packet(task_arg->connection, &unsubscribe.fixed_header);
    if (!message) {
        goto handle_error;
    }
    struct aws_byte_cursor message_cursor = {
        .ptr = message->message_data.buffer,
        .len = message->message_data.capacity,
    };
    if (aws_mqtt_packet_unsubscribe_encode(&message_cursor, &unsubscribe)) {
        goto handle_error;
    }
    message->message_data.len = message->message_data.capacity - message_cursor.len;

    aws_mqtt_packet_unsubscribe_clean_up(&unsubscribe);

    if (aws_channel_slot_send_message(task_arg->connection->slot, message, AWS_CHANNEL_DIR_WRITE)) {
        goto handle_error;
    }

    aws_mem_release(task_arg->connection->allocator, task_arg);

    return false;

handle_error:

    aws_mqtt_packet_unsubscribe_clean_up(&unsubscribe);

    if (message) {
        aws_channel_release_message_to_pool(task_arg->connection->slot->channel, message);
    }

    aws_mem_release(task_arg->connection->allocator, task_arg);

    return true;
}

uint16_t aws_mqtt_client_connection_unsubscribe(
    struct aws_mqtt_client_connection *connection,
    const struct aws_byte_cursor *topic_filter,
    aws_mqtt_op_complete_fn *on_unsuback,
    void *on_unsuback_ud) {

    assert(connection);

    if (!aws_mqtt_is_valid_topic_filter(topic_filter)) {
        aws_raise_error(AWS_ERROR_MQTT_INVALID_TOPIC);
        return 0;
    }

    struct unsubscribe_task_arg *task_arg = aws_mem_acquire(connection->allocator, sizeof(struct unsubscribe_task_arg));
    if (!task_arg) {
        return 0;
    }
    task_arg->connection = connection;
    task_arg->filter = *topic_filter;

    return mqtt_create_request(connection, &s_unsubscribe_send, task_arg, on_unsuback, on_unsuback_ud);
}

/*******************************************************************************
 * Publish
 ******************************************************************************/

struct publish_task_arg {
    struct aws_mqtt_client_connection *connection;
    struct aws_byte_cursor topic;
    enum aws_mqtt_qos qos;
    bool retain;
    struct aws_byte_cursor payload;

    aws_mqtt_op_complete_fn *on_complete;
    void *userdata;
};

static bool s_publish_send(uint16_t message_id, bool is_first_attempt, void *userdata) {
    struct publish_task_arg *publish_arg = userdata;

    bool is_qos_0 = publish_arg->qos == AWS_MQTT_QOS_AT_MOST_ONCE;
    if (is_qos_0) {
        message_id = 0;
    }

    struct aws_mqtt_packet_publish publish;
    aws_mqtt_packet_publish_init(
        &publish,
        publish_arg->retain,
        publish_arg->qos,
        !is_first_attempt,
        publish_arg->topic,
        message_id,
        publish_arg->payload);

    struct aws_io_message *message = mqtt_get_message_for_packet(publish_arg->connection, &publish.fixed_header);
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

    if (aws_channel_slot_send_message(publish_arg->connection->slot, message, AWS_CHANNEL_DIR_WRITE)) {

        goto handle_error;
    }

    /* If QoS == 0, there will be no ack, so consider the request done now. */
    return is_qos_0;

handle_error:
    if (message) {
        aws_channel_release_message_to_pool(publish_arg->connection->slot->channel, message);
    }

    return false;
}

static void s_publish_complete(struct aws_mqtt_client_connection *connection, uint16_t packet_id, void *userdata) {
    struct publish_task_arg *publish_arg = userdata;

    if (publish_arg->on_complete) {
        publish_arg->on_complete(connection, packet_id, publish_arg->userdata);
    }

    aws_mem_release(connection->allocator, publish_arg);
}

uint16_t aws_mqtt_client_connection_publish(
    struct aws_mqtt_client_connection *connection,
    const struct aws_byte_cursor *topic,
    enum aws_mqtt_qos qos,
    bool retain,
    const struct aws_byte_cursor *payload,
    aws_mqtt_op_complete_fn *on_complete,
    void *userdata) {

    assert(connection);

    if (!aws_mqtt_is_valid_topic(topic)) {
        aws_raise_error(AWS_ERROR_MQTT_INVALID_TOPIC);
        return 0;
    }

    struct publish_task_arg *arg = aws_mem_acquire(connection->allocator, sizeof(struct publish_task_arg));
    if (!arg) {
        return 0;
    }

    arg->connection = connection;
    arg->topic = *topic;
    arg->qos = qos;
    arg->retain = retain;
    arg->payload = *payload;

    arg->on_complete = on_complete;
    arg->userdata = userdata;

    return mqtt_create_request(connection, &s_publish_send, arg, &s_publish_complete, arg);
}

/*******************************************************************************
 * Ping
 ******************************************************************************/

static bool s_pingreq_send(uint16_t message_id, bool is_first_attempt, void *userdata) {
    (void)message_id;
    (void)is_first_attempt;

    struct aws_mqtt_client_connection *connection = userdata;

    if (is_first_attempt) {
        /* First attempt, actually send the packet */

        struct aws_mqtt_packet_connection pingreq;
        aws_mqtt_packet_pingreq_init(&pingreq);

        struct aws_io_message *message = mqtt_get_message_for_packet(connection, &pingreq.fixed_header);
        if (!message) {
            goto handle_error;
        }
        struct aws_byte_cursor message_cursor = {
            .ptr = message->message_data.buffer,
            .len = message->message_data.capacity,
        };
        if (aws_mqtt_packet_connection_encode(&message_cursor, &pingreq)) {
            goto handle_error;
        }
        message->message_data.len = message->message_data.capacity - message_cursor.len;

        if (aws_channel_slot_send_message(connection->slot, message, AWS_CHANNEL_DIR_WRITE)) {

            goto handle_error;
        }

        return false;

    handle_error:
        if (message) {
            aws_channel_release_message_to_pool(connection->slot->channel, message);
        }

        return true;
    }

    /* Check that a pingresp has been recieved since pingreq was sent */

    uint64_t current_time = 0;
    aws_channel_current_clock_time(connection->slot->channel, &current_time);

    if (current_time - connection->last_pingresp_timestamp > request_timeout_ns) {
        /* It's been too long since the last ping, close the connection */

        mqtt_disconnect_impl(connection, AWS_ERROR_MQTT_TIMEOUT);
    }

    return true;
}

int aws_mqtt_client_connection_ping(struct aws_mqtt_client_connection *connection) {

    mqtt_create_request(connection, &s_pingreq_send, connection, NULL, NULL);

    return AWS_OP_SUCCESS;
}
