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

/* 3 seconds */
static const uint64_t s_default_request_timeout_ns = 3000000000;

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

static void s_mqtt_client_shutdown(
    struct aws_client_bootstrap *bootstrap,
    int error_code,
    struct aws_channel *channel,
    void *user_data) {

    (void)bootstrap;
    (void)channel;

    struct aws_mqtt_client_connection *connection = user_data;

    /* Always clear slot, as that's what's been shutdown */
    if (connection->slot) {
        if (connection->state == AWS_MQTT_CLIENT_STATE_CONNECTING) {
            /* if there was a slot and we were connecting, the socket at least
             * managed to connect (and was likely hung up gracefully during setup)
             * so this should behave like a dropped connection */
            connection->state = AWS_MQTT_CLIENT_STATE_CONNECTED;
        }
        aws_channel_slot_remove(connection->slot);
        connection->slot = NULL;
    }

    if (connection->state == AWS_MQTT_CLIENT_STATE_RECONNECTING) {
        /* If reconnect attempt failed, schedule the next attempt */
        struct aws_event_loop *el = aws_event_loop_group_get_next_loop(connection->client->bootstrap->event_loop_group);

        aws_event_loop_schedule_task_future(
            el, &connection->reconnect_task->task, connection->reconnect_timeouts.next_attempt);

    } else if (connection->state == AWS_MQTT_CLIENT_STATE_DISCONNECTING) {

        connection->state = AWS_MQTT_CLIENT_STATE_DISCONNECTED;

        /* Successfully shutdown, so clear the outstanding requests */
        aws_hash_table_clear(&connection->outstanding_requests.table);

        MQTT_CLIENT_CALL_CALLBACK(connection, on_disconnect);

    } else if (connection->state == AWS_MQTT_CLIENT_STATE_CONNECTING) {

        connection->state = AWS_MQTT_CLIENT_STATE_DISCONNECTED;
        MQTT_CLIENT_CALL_CALLBACK_ARGS(connection, on_connection_complete, error_code, 0, false);

    } else {

        assert(
            connection->state == AWS_MQTT_CLIENT_STATE_CONNECTED ||
            connection->state == AWS_MQTT_CLIENT_STATE_RECONNECTING ||
            connection->state == AWS_MQTT_CLIENT_STATE_DISCONNECTED);

        if (connection->state == AWS_MQTT_CLIENT_STATE_CONNECTED) {

            connection->state = AWS_MQTT_CLIENT_STATE_RECONNECTING;
            MQTT_CLIENT_CALL_CALLBACK_ARGS(connection, on_interrupted, error_code);
        }

        assert(
            connection->state == AWS_MQTT_CLIENT_STATE_RECONNECTING ||
            connection->state == AWS_MQTT_CLIENT_STATE_DISCONNECTING ||
            connection->state == AWS_MQTT_CLIENT_STATE_DISCONNECTED);

        /* This will only be true if the user called disconnect from the on_interrupted callback */
        if (connection->state == AWS_MQTT_CLIENT_STATE_DISCONNECTING) {
            connection->state = AWS_MQTT_CLIENT_STATE_DISCONNECTED;
            MQTT_CLIENT_CALL_CALLBACK(connection, on_disconnect);

        } else {
            /* Attempt the reconnect immediately, which will schedule a task to retry if it doesn't succeed */
            connection->reconnect_task->task.fn(
                &connection->reconnect_task->task, connection->reconnect_task->task.arg, AWS_TASK_STATUS_RUN_READY);
        }
    }
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
        /* client shutdown already handles this case, so just call that. */
        s_mqtt_client_shutdown(bootstrap, error_code, channel, user_data);
        return;
    }

    /* Reset the current timeout timer */
    connection->reconnect_timeouts.current = connection->reconnect_timeouts.min;

    /* Create the slot and handler */
    connection->slot = aws_channel_slot_new(channel);

    if (!connection->slot) {
        aws_channel_shutdown(channel, aws_last_error());
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
        connection->keep_alive_time_secs);

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

    if (aws_mqtt_packet_connect_encode(&message->message_data, &connect)) {
        goto handle_error;
    }

    if (aws_channel_slot_send_message(connection->slot, message, AWS_CHANNEL_DIR_WRITE)) {
        goto handle_error;
    }

    return;

handle_error:
    MQTT_CLIENT_CALL_CALLBACK_ARGS(connection, on_connection_complete, aws_last_error(), 0, false);

    if (message) {
        aws_mem_release(message->allocator, message);
    }
}

static void s_attempt_reconect(struct aws_task *task, void *userdata, enum aws_task_status status) {

    (void)task;

    struct aws_mqtt_reconnect_task *reconnect = userdata;
    struct aws_mqtt_client_connection *connection = aws_atomic_load_ptr(&reconnect->connection_ptr);

    if (status == AWS_TASK_STATUS_RUN_READY && connection) {
        /* If the task is not cancelled and a connection has not succeeded, attempt reconnect */

        aws_high_res_clock_get_ticks(&connection->reconnect_timeouts.next_attempt);
        connection->reconnect_timeouts.next_attempt += aws_timestamp_convert(
            connection->reconnect_timeouts.current, AWS_TIMESTAMP_SECS, AWS_TIMESTAMP_NANOS, NULL);

        /* Check before multipying to avoid potential overflow */
        if (connection->reconnect_timeouts.current > connection->reconnect_timeouts.max / 2) {
            connection->reconnect_timeouts.current = connection->reconnect_timeouts.max;
        } else {
            connection->reconnect_timeouts.current *= 2;
        }

        if (aws_mqtt_client_connection_reconnect(
                connection, connection->on_connection_complete, connection->on_connection_complete_ud)) {

            /* If reconnect attempt failed, schedule the next attempt */
            struct aws_event_loop *el =
                aws_event_loop_group_get_next_loop(connection->client->bootstrap->event_loop_group);
            aws_event_loop_schedule_task_future(
                el, &connection->reconnect_task->task, connection->reconnect_timeouts.next_attempt);
        } else {
            connection->reconnect_task->task.timestamp = 0;
        }
    } else {
        aws_mem_release(reconnect->allocator, reconnect);
    }
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
        aws_memory_pool_release(&request->connection->requests_pool, request);
    } else {
        /* Signal task to clean up request */
        request->cancelled = true;
    }
}

struct aws_mqtt_client_connection *aws_mqtt_client_connection_new(struct aws_mqtt_client *client) {

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
    connection->state = AWS_MQTT_CLIENT_STATE_DISCONNECTED;
    connection->reconnect_timeouts.min = 1;
    connection->reconnect_timeouts.max = 128;
    aws_mutex_init(&connection->outstanding_requests.mutex);
    aws_linked_list_init(&connection->pending_requests.list);

    if (aws_mutex_init(&connection->pending_requests.mutex)) {

        goto handle_error;
    }

    if (aws_mqtt_topic_tree_init(&connection->subscriptions, connection->allocator)) {

        goto handle_error;
    }

    if (aws_memory_pool_init(
            &connection->requests_pool, connection->allocator, 32, sizeof(struct aws_mqtt_outstanding_request))) {

        goto handle_error;
    }

    if (aws_hash_table_init(
            &connection->outstanding_requests.table,
            connection->allocator,
            sizeof(struct aws_mqtt_outstanding_request *),
            s_hash_uint16_t,
            s_uint16_t_eq,
            NULL,
            &s_outstanding_request_destroy)) {

        goto handle_error;
    }

    /* Initialize the handler */
    connection->handler.alloc = connection->allocator;
    connection->handler.vtable = aws_mqtt_get_client_channel_vtable();
    connection->handler.impl = connection;

    return connection;

handle_error:

    aws_mqtt_topic_tree_clean_up(&connection->subscriptions);

    aws_hash_table_clean_up(&connection->outstanding_requests.table);

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
    assert(connection->state == AWS_MQTT_CLIENT_STATE_DISCONNECTED);

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
    aws_hash_table_clean_up(&connection->outstanding_requests.table);
    aws_memory_pool_clean_up(&connection->requests_pool);

    if (connection->slot) {
        aws_channel_slot_remove(connection->slot);
    }
    aws_tls_connection_options_clean_up(&connection->tls_options);

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
    if (aws_byte_buf_init_copy(&connection->will.topic, connection->allocator, &topic_buf)) {
        goto cleanup;
    }

    connection->will.qos = qos;
    connection->will.retain = retain;

    struct aws_byte_buf payload_buf = aws_byte_buf_from_array(payload->ptr, payload->len);
    if (aws_byte_buf_init_copy(&connection->will.payload, connection->allocator, &payload_buf)) {
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

int aws_mqtt_client_connection_set_connection_interruption_handlers(
    struct aws_mqtt_client_connection *connection,
    aws_mqtt_client_on_connection_interrupted_fn *on_interrupted,
    void *on_interrupted_ud,
    aws_mqtt_client_on_connection_resumed_fn *on_resumed,
    void *on_resumed_ud) {

    connection->on_interrupted = on_interrupted;
    connection->on_interrupted_ud = on_interrupted_ud;
    connection->on_resumed = on_resumed;
    connection->on_resumed_ud = on_resumed_ud;

    return AWS_OP_SUCCESS;
}

/*******************************************************************************
 * Connect
 ******************************************************************************/

int aws_mqtt_client_connection_connect(
    struct aws_mqtt_client_connection *connection,
    const struct aws_mqtt_connection_options *connection_options) {

    if (connection->state != AWS_MQTT_CLIENT_STATE_DISCONNECTED) {
        return aws_raise_error(AWS_ERROR_MQTT_ALREADY_CONNECTED);
    }

    if (connection->host_name) {
        aws_string_destroy(connection->host_name);
    }

    connection->host_name = aws_string_new_from_array(
        connection->allocator, connection_options->host_name.ptr, connection_options->host_name.len);
    connection->port = connection_options->port;
    connection->socket_options = *connection_options->socket_options;
    connection->state = AWS_MQTT_CLIENT_STATE_CONNECTING;
    connection->clean_session = connection_options->clean_session;
    connection->keep_alive_time_secs = connection_options->keep_alive_time_secs;
    connection->connection_count = 0;

    if (!connection_options->ping_timeout_ms) {
        connection->request_timeout_ns = s_default_request_timeout_ns;
    } else {
        connection->request_timeout_ns = aws_timestamp_convert(
            (uint64_t)connection_options->ping_timeout_ms, AWS_TIMESTAMP_MILLIS, AWS_TIMESTAMP_NANOS, NULL);
    }

    /* Cheat and set the tls_options host_name to our copy if they're the same */
    if (connection_options->tls_options) {
        if (aws_tls_connection_options_copy(&connection->tls_options, connection_options->tls_options)) {
            return AWS_OP_ERR;
        }

        if (!connection_options->tls_options->server_name) {
            struct aws_byte_cursor host_name_cur = aws_byte_cursor_from_string(connection->host_name);
            if (aws_tls_connection_options_set_server_name(
                    &connection->tls_options, connection->allocator, &host_name_cur)) {
                goto error;
            }
        }

    } else {
        AWS_ZERO_STRUCT(connection->tls_options);
    }

    /* Clean up old client_id */
    if (connection->client_id.buffer) {
        aws_byte_buf_clean_up(&connection->client_id);
    }

    /* Create the reconnect task for use later (probably) */
    assert(!connection->reconnect_task);
    connection->reconnect_task = aws_mem_acquire(connection->allocator, sizeof(struct aws_mqtt_reconnect_task));
    if (!connection->reconnect_task) {
        goto error;
    }
    aws_atomic_init_ptr(&connection->reconnect_task->connection_ptr, connection);
    connection->reconnect_task->allocator = connection->allocator;
    aws_task_init(&connection->reconnect_task->task, s_attempt_reconect, connection->reconnect_task);

    /* Only set connection->client_id if a new one was provided */
    struct aws_byte_buf client_id_buf =
        aws_byte_buf_from_array(connection_options->client_id.ptr, connection_options->client_id.len);
    if (aws_byte_buf_init_copy(&connection->client_id, connection->allocator, &client_id_buf)) {
        goto client_id_alloc_failed;
    }

    if (aws_mqtt_client_connection_reconnect(
            connection, connection_options->on_connection_complete, connection_options->user_data)) {
        goto reconnect_failed;
    }

    return AWS_OP_SUCCESS;

reconnect_failed:
    aws_mem_release(connection->allocator, connection->reconnect_task);

client_id_alloc_failed:
    aws_mem_release(connection->allocator, connection->reconnect_task);

error:
    aws_tls_connection_options_clean_up(&connection->tls_options);
    AWS_ZERO_STRUCT(connection->tls_options);
    return AWS_OP_ERR;
}

/*******************************************************************************
 * Reconnect
 ******************************************************************************/

int aws_mqtt_client_connection_reconnect(
    struct aws_mqtt_client_connection *connection,
    aws_mqtt_client_on_connection_complete_fn *on_connection_complete,
    void *userdata) {

    connection->on_connection_complete = on_connection_complete;
    connection->on_connection_complete_ud = userdata;

    if (connection->clean_session) {
        /* If clean_session is set, all subscriptions will be reset by the server,
        so we can clean the local tree out too. */
        aws_mqtt_topic_tree_clean_up(&connection->subscriptions);
        aws_mqtt_topic_tree_init(&connection->subscriptions, connection->allocator);
    }

    int result = 0;
    if (connection->tls_options.ctx) {
        result = aws_client_bootstrap_new_tls_socket_channel(
            connection->client->bootstrap,
            (const char *)aws_string_bytes(connection->host_name),
            connection->port,
            &connection->socket_options,
            &connection->tls_options,
            &s_mqtt_client_init,
            &s_mqtt_client_shutdown,
            connection);
    } else {
        result = aws_client_bootstrap_new_socket_channel(
            connection->client->bootstrap,
            (const char *)aws_string_bytes(connection->host_name),
            connection->port,
            &connection->socket_options,
            &s_mqtt_client_init,
            &s_mqtt_client_shutdown,
            connection);
    }
    if (result) {
        /* Connection attempt failed */
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

/*******************************************************************************
 * Disconnect
 ******************************************************************************/

int aws_mqtt_client_connection_disconnect(
    struct aws_mqtt_client_connection *connection,
    aws_mqtt_client_on_disconnect_fn *on_disconnect,
    void *userdata) {

    if (connection->state == AWS_MQTT_CLIENT_STATE_CONNECTED ||
        connection->state == AWS_MQTT_CLIENT_STATE_RECONNECTING) {

        connection->on_disconnect = on_disconnect;
        connection->on_disconnect_ud = userdata;

        connection->state = AWS_MQTT_CLIENT_STATE_DISCONNECTING;
        mqtt_disconnect_impl(connection, AWS_OP_SUCCESS);

        return AWS_OP_SUCCESS;
    }

    return aws_raise_error(AWS_ERROR_MQTT_NOT_CONNECTED);
}

/*******************************************************************************
 * Subscribe
 ******************************************************************************/

/* The lifetime of this struct is the same as the lifetime of the subscription */
struct subscribe_task_topic {
    struct aws_mqtt_client_connection *connection;

    struct aws_mqtt_topic_subscription request;
    struct aws_string *filter;
};

/* The lifetime of this struct is from subscribe -> suback */
struct subscribe_task_arg {

    struct aws_mqtt_client_connection *connection;

    /* list of subscribe_task_topic *s */
    struct aws_array_list topics;

    /* Packet to populate */
    struct aws_mqtt_packet_subscribe subscribe;

    /* true if transaction was committed to the topic tree, false requires a retry */
    bool tree_updated;

    aws_mqtt_suback_multi_fn *on_suback;
    void *on_suback_ud;
};

static void s_on_publish_client_wrapper(
    const struct aws_byte_cursor *topic,
    const struct aws_byte_cursor *payload,
    void *userdata) {

    struct subscribe_task_topic *task_topic = userdata;

    /* Call out to the user callback */
    task_topic->request.on_publish(task_topic->connection, topic, payload, task_topic->request.on_publish_ud);
}

static void s_on_topic_clean_up(void *userdata) {

    struct subscribe_task_topic *task_topic = userdata;

    if (task_topic->request.on_cleanup) {
        task_topic->request.on_cleanup(task_topic->request.on_publish_ud);
    }

    aws_mem_release(task_topic->connection->allocator, task_topic);
}

static enum aws_mqtt_client_request_state s_subscribe_send(uint16_t message_id, bool is_first_attempt, void *userdata) {

    (void)is_first_attempt;

    struct subscribe_task_arg *task_arg = userdata;
    bool initing_packet = task_arg->subscribe.fixed_header.packet_type == 0;
    struct aws_io_message *message = NULL;

    if (initing_packet) {
        /* Init the subscribe packet */
        if (aws_mqtt_packet_subscribe_init(&task_arg->subscribe, task_arg->connection->allocator, message_id)) {
            return AWS_MQTT_CLIENT_REQUEST_ERROR;
        }
    }

    const size_t num_topics = aws_array_list_length(&task_arg->topics);
    assert(num_topics > 0);

    AWS_VARIABLE_LENGTH_ARRAY(uint8_t, transaction_buf, num_topics * aws_mqtt_topic_tree_action_size);
    struct aws_array_list transaction;
    aws_array_list_init_static(&transaction, transaction_buf, num_topics, aws_mqtt_topic_tree_action_size);

    for (size_t i = 0; i < num_topics; ++i) {

        struct subscribe_task_topic *topic = NULL;
        int result = aws_array_list_get_at(&task_arg->topics, &topic, i);
        assert(result == AWS_OP_SUCCESS); /* We know we're within bounds */
        (void)result;

        if (initing_packet) {
            if (aws_mqtt_packet_subscribe_add_topic(&task_arg->subscribe, topic->request.topic, topic->request.qos)) {
                goto handle_error;
            }
        }

        if (!task_arg->tree_updated) {
            if (aws_mqtt_topic_tree_transaction_insert(
                    &task_arg->connection->subscriptions,
                    &transaction,
                    topic->filter,
                    topic->request.qos,
                    s_on_publish_client_wrapper,
                    s_on_topic_clean_up,
                    topic)) {

                goto handle_error;
            }
        }
    }

    message = mqtt_get_message_for_packet(task_arg->connection, &task_arg->subscribe.fixed_header);
    if (!message) {

        goto handle_error;
    }

    if (aws_mqtt_packet_subscribe_encode(&message->message_data, &task_arg->subscribe)) {

        goto handle_error;
    }

    /* No need to handle this error, if the send fails, it'll just retry */
    aws_channel_slot_send_message(task_arg->connection->slot, message, AWS_CHANNEL_DIR_WRITE);

    if (!task_arg->tree_updated) {
        aws_mqtt_topic_tree_transaction_commit(&task_arg->connection->subscriptions, &transaction);
        task_arg->tree_updated = true;
    }

    aws_array_list_clean_up(&transaction);
    return AWS_MQTT_CLIENT_REQUEST_ONGOING;

handle_error:

    if (message) {
        aws_mem_release(message->allocator, message);
    }
    if (!task_arg->tree_updated) {
        aws_mqtt_topic_tree_transaction_roll_back(&task_arg->connection->subscriptions, &transaction);
    }

    aws_array_list_clean_up(&transaction);
    return AWS_MQTT_CLIENT_REQUEST_ERROR;
}

static void s_subscribe_complete(
    struct aws_mqtt_client_connection *connection,
    uint16_t packet_id,
    int error_code,
    void *userdata) {

    struct subscribe_task_arg *task_arg = userdata;

    if (task_arg->on_suback) {
        task_arg->on_suback(connection, packet_id, &task_arg->topics, error_code, task_arg->on_suback_ud);
    }

    aws_array_list_clean_up(&task_arg->topics);
    aws_mqtt_packet_subscribe_clean_up(&task_arg->subscribe);
    aws_mem_release(task_arg->connection->allocator, task_arg);
}

uint16_t aws_mqtt_client_connection_subscribe_multiple(
    struct aws_mqtt_client_connection *connection,
    const struct aws_array_list *topic_filters,
    aws_mqtt_suback_multi_fn *on_suback,
    void *on_suback_ud) {

    assert(connection);

    struct subscribe_task_arg *task_arg = aws_mem_acquire(connection->allocator, sizeof(struct subscribe_task_arg));
    if (!task_arg) {
        return 0;
    }
    AWS_ZERO_STRUCT(*task_arg);

    task_arg->connection = connection;
    task_arg->on_suback = on_suback;
    task_arg->on_suback_ud = on_suback_ud;

    const size_t num_topics = aws_array_list_length(topic_filters);

    if (aws_array_list_init_dynamic(&task_arg->topics, connection->allocator, num_topics, sizeof(void *))) {
        goto handle_error;
    }

    for (size_t i = 0; i < num_topics; ++i) {

        struct aws_mqtt_topic_subscription *request = NULL;
        aws_array_list_get_at_ptr(topic_filters, (void **)&request, i);

        if (!aws_mqtt_is_valid_topic_filter(&request->topic)) {
            aws_raise_error(AWS_ERROR_MQTT_INVALID_TOPIC);
            goto handle_error;
        }

        struct subscribe_task_topic *task_topic =
            aws_mem_acquire(connection->allocator, sizeof(struct subscribe_task_topic));
        if (!task_topic) {
            goto handle_error;
        }

        task_topic->connection = connection;
        task_topic->request = *request;

        task_topic->filter = aws_string_new_from_array(
            connection->allocator, task_topic->request.topic.ptr, task_topic->request.topic.len);
        if (!task_topic->filter) {
            aws_mem_release(connection->allocator, task_topic);
            goto handle_error;
        }

        /* Update request topic cursor to refer to owned string */
        task_topic->request.topic = aws_byte_cursor_from_string(task_topic->filter);

        /* Push into the list */
        aws_array_list_push_back(&task_arg->topics, &request);
    }

    uint16_t packet_id =
        mqtt_create_request(task_arg->connection, &s_subscribe_send, task_arg, &s_subscribe_complete, task_arg);

    if (packet_id) {
        return packet_id;
    }

handle_error:

    if (task_arg) {

        if (task_arg->topics.data) {

            const size_t num_added_topics = aws_array_list_length(&task_arg->topics);
            for (size_t i = 0; i < num_added_topics; ++i) {

                struct subscribe_task_topic *task_topic = NULL;
                aws_array_list_get_at(&task_arg->topics, (void **)&task_topic, i);

                aws_string_destroy(task_topic->filter);
                aws_mem_release(connection->allocator, task_topic);
            }

            aws_array_list_clean_up(&task_arg->topics);
        }

        aws_mem_release(connection->allocator, task_arg);
    }
    return 0;
}

/*******************************************************************************
 * Subscribe Single
 ******************************************************************************/

static void s_subscribe_single_complete(
    struct aws_mqtt_client_connection *connection,
    uint16_t packet_id,
    int error_code,
    void *userdata) {

    struct subscribe_task_arg *task_arg = userdata;

    assert(aws_array_list_length(&task_arg->topics) == 1);

    if (task_arg->on_suback) {
        struct subscribe_task_topic *topic = NULL;
        int result = aws_array_list_get_at(&task_arg->topics, &topic, 0);
        assert(result == AWS_OP_SUCCESS); /* There needs to be exactly 1 topic in this list */
        (void)result;

        aws_mqtt_suback_fn *suback = (aws_mqtt_suback_fn *)task_arg->on_suback;
        suback(connection, packet_id, &topic->request.topic, topic->request.qos, error_code, task_arg->on_suback_ud);
    }

    aws_array_list_clean_up(&task_arg->topics);
    aws_mqtt_packet_subscribe_clean_up(&task_arg->subscribe);
    aws_mem_release(task_arg->connection->allocator, task_arg);
}

uint16_t aws_mqtt_client_connection_subscribe(
    struct aws_mqtt_client_connection *connection,
    const struct aws_byte_cursor *topic_filter,
    enum aws_mqtt_qos qos,
    aws_mqtt_client_publish_received_fn *on_publish,
    void *on_publish_ud,
    aws_mqtt_userdata_cleanup_fn *on_ud_cleanup,
    aws_mqtt_suback_fn *on_suback,
    void *on_suback_ud) {

    assert(connection);

    if (!aws_mqtt_is_valid_topic_filter(topic_filter)) {
        aws_raise_error(AWS_ERROR_MQTT_INVALID_TOPIC);
        return 0;
    }

    /* Because we know we're only going to have 1 topic, we can cheat and allocate the array_list in the same block as
     * the task argument. */
    void *task_topic_storage = NULL;
    struct subscribe_task_topic *task_topic = NULL;
    struct subscribe_task_arg *task_arg = aws_mem_acquire_many(
        connection->allocator,
        2,
        &task_arg,
        sizeof(struct subscribe_task_arg),
        &task_topic_storage,
        sizeof(struct subscribe_task_topic));

    if (!task_arg) {
        goto handle_error;
    }
    AWS_ZERO_STRUCT(*task_arg);

    task_arg->connection = connection;
    task_arg->on_suback = (aws_mqtt_suback_multi_fn *)on_suback;
    task_arg->on_suback_ud = on_suback_ud;

    aws_array_list_init_static(&task_arg->topics, task_topic_storage, 1, sizeof(void *));

    /* Allocate the topic and push into the list */
    task_topic = aws_mem_acquire(connection->allocator, sizeof(struct subscribe_task_topic));
    if (!task_topic) {
        goto handle_error;
    }
    aws_array_list_push_back(&task_arg->topics, &task_topic);

    task_topic->filter = aws_string_new_from_array(connection->allocator, topic_filter->ptr, topic_filter->len);
    if (!task_topic->filter) {
        goto handle_error;
    }

    task_topic->connection = connection;
    task_topic->request.topic = aws_byte_cursor_from_string(task_topic->filter);
    task_topic->request.qos = qos;
    task_topic->request.on_publish = on_publish;
    task_topic->request.on_cleanup = on_ud_cleanup;
    task_topic->request.on_publish_ud = on_publish_ud;

    /* Update request topic cursor to refer to owned string */
    task_topic->request.topic = aws_byte_cursor_from_string(task_topic->filter);

    uint16_t packet_id =
        mqtt_create_request(task_arg->connection, &s_subscribe_send, task_arg, &s_subscribe_single_complete, task_arg);

    if (packet_id) {
        return packet_id;
    }

handle_error:

    if (task_topic) {
        if (task_topic->filter) {
            aws_string_destroy(task_topic->filter);
        }
        aws_mem_release(connection->allocator, task_topic);
    }

    if (task_arg) {

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
    /* Packet to populate */
    struct aws_mqtt_packet_unsubscribe unsubscribe;

    /* true if transaction was committed to the topic tree, false requires a retry */
    bool tree_updated;

    aws_mqtt_op_complete_fn *on_unsuback;
    void *on_unsuback_ud;
};

static enum aws_mqtt_client_request_state s_unsubscribe_send(
    uint16_t message_id,
    bool is_first_attempt,
    void *userdata) {

    (void)is_first_attempt;

    struct unsubscribe_task_arg *task_arg = userdata;
    struct aws_io_message *message = NULL;

    static const size_t num_topics = 1;

    AWS_VARIABLE_LENGTH_ARRAY(uint8_t, transaction_buf, num_topics * aws_mqtt_topic_tree_action_size);
    struct aws_array_list transaction;
    aws_array_list_init_static(&transaction, transaction_buf, num_topics, aws_mqtt_topic_tree_action_size);

    if (!task_arg->tree_updated) {

        if (aws_mqtt_topic_tree_transaction_remove(
                &task_arg->connection->subscriptions, &transaction, &task_arg->filter)) {
            goto handle_error;
        }
    }

    if (task_arg->unsubscribe.fixed_header.packet_type == 0) {
        /* If unsubscribe packet is uninitialized, init it */
        if (aws_mqtt_packet_unsubscribe_init(&task_arg->unsubscribe, task_arg->connection->allocator, message_id)) {
            goto handle_error;
        }
        if (aws_mqtt_packet_unsubscribe_add_topic(&task_arg->unsubscribe, task_arg->filter)) {
            goto handle_error;
        }
    }

    message = mqtt_get_message_for_packet(task_arg->connection, &task_arg->unsubscribe.fixed_header);
    if (!message) {
        goto handle_error;
    }

    if (aws_mqtt_packet_unsubscribe_encode(&message->message_data, &task_arg->unsubscribe)) {
        aws_mem_release(message->allocator, message);
        goto handle_error;
    }

    aws_channel_slot_send_message(task_arg->connection->slot, message, AWS_CHANNEL_DIR_WRITE);

    if (!task_arg->tree_updated) {
        aws_mqtt_topic_tree_transaction_commit(&task_arg->connection->subscriptions, &transaction);
        task_arg->tree_updated = true;
    }

    aws_array_list_clean_up(&transaction);
    return AWS_MQTT_CLIENT_REQUEST_ONGOING;

handle_error:

    if (message) {
        aws_mem_release(message->allocator, message);
    }
    if (!task_arg->tree_updated) {
        aws_mqtt_topic_tree_transaction_roll_back(&task_arg->connection->subscriptions, &transaction);
    }

    aws_array_list_clean_up(&transaction);
    return AWS_MQTT_CLIENT_REQUEST_ERROR;
}

static void s_unsubscribe_complete(
    struct aws_mqtt_client_connection *connection,
    uint16_t packet_id,
    int error_code,
    void *userdata) {

    struct unsubscribe_task_arg *task_arg = userdata;

    if (error_code != AWS_OP_ERR) {
    }

    if (task_arg->on_unsuback) {
        task_arg->on_unsuback(connection, packet_id, error_code, task_arg->on_unsuback_ud);
    }

    aws_mqtt_packet_unsubscribe_clean_up(&task_arg->unsubscribe);
    aws_mem_release(task_arg->connection->allocator, task_arg);
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
    AWS_ZERO_STRUCT(*task_arg);
    task_arg->connection = connection;
    task_arg->filter = *topic_filter;
    task_arg->on_unsuback = on_unsuback;
    task_arg->on_unsuback_ud = on_unsuback_ud;

    return mqtt_create_request(connection, &s_unsubscribe_send, task_arg, s_unsubscribe_complete, task_arg);
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

    /* Packet to populate */
    struct aws_mqtt_packet_publish publish;

    aws_mqtt_op_complete_fn *on_complete;
    void *userdata;
};

static enum aws_mqtt_client_request_state s_publish_send(uint16_t message_id, bool is_first_attempt, void *userdata) {
    struct publish_task_arg *task_arg = userdata;

    bool is_qos_0 = task_arg->qos == AWS_MQTT_QOS_AT_MOST_ONCE;
    if (is_qos_0) {
        message_id = 0;
    }

    if (is_first_attempt) {
        if (aws_mqtt_packet_publish_init(
                &task_arg->publish,
                task_arg->retain,
                task_arg->qos,
                !is_first_attempt,
                task_arg->topic,
                message_id,
                task_arg->payload)) {

            return AWS_MQTT_CLIENT_REQUEST_ERROR;
        }
    }

    struct aws_io_message *message = mqtt_get_message_for_packet(task_arg->connection, &task_arg->publish.fixed_header);
    if (!message) {
        return AWS_MQTT_CLIENT_REQUEST_ERROR;
    }

    /* Encode the headers, and everything but the payload */
    if (aws_mqtt_packet_publish_encode_headers(&message->message_data, &task_arg->publish)) {
        return AWS_MQTT_CLIENT_REQUEST_ERROR;
    }

    struct aws_byte_cursor payload_cur = task_arg->payload;
    {
    write_payload_chunk:
        (void)NULL;

        const size_t left_in_message = message->message_data.capacity - message->message_data.len;
        const size_t to_write = payload_cur.len < left_in_message ? payload_cur.len : left_in_message;

        /* Write this chunk */
        struct aws_byte_cursor to_write_cur = aws_byte_cursor_advance(&payload_cur, to_write);
        assert(to_write_cur.ptr); /* to_write is guaranteed to be inside the bounds of payload_cur */
        if (!aws_byte_buf_write_from_whole_cursor(&message->message_data, to_write_cur)) {

            aws_mem_release(message->allocator, message);
            return AWS_MQTT_CLIENT_REQUEST_ERROR;
        }

        if (aws_channel_slot_send_message(task_arg->connection->slot, message, AWS_CHANNEL_DIR_WRITE)) {

            aws_mem_release(message->allocator, message);
            return AWS_MQTT_CLIENT_REQUEST_ERROR;
        }

        /* If there's still payload left, get a new message and start again. */
        if (payload_cur.len) {
            message = mqtt_get_message_for_packet(task_arg->connection, &task_arg->publish.fixed_header);
            goto write_payload_chunk;
        }
    }

    /* If QoS == 0, there will be no ack, so consider the request done now. */
    return is_qos_0 ? AWS_MQTT_CLIENT_REQUEST_COMPLETE : AWS_MQTT_CLIENT_REQUEST_ONGOING;
}

static void s_publish_complete(
    struct aws_mqtt_client_connection *connection,
    uint16_t packet_id,
    int error_code,
    void *userdata) {
    struct publish_task_arg *task_arg = userdata;

    if (task_arg->on_complete) {
        task_arg->on_complete(connection, packet_id, error_code, task_arg->userdata);
    }

    aws_mem_release(connection->allocator, task_arg);
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

static enum aws_mqtt_client_request_state s_pingreq_send(uint16_t message_id, bool is_first_attempt, void *userdata) {
    (void)message_id;
    (void)is_first_attempt;

    struct aws_mqtt_client_connection *connection = userdata;

    if (is_first_attempt) {
        /* First attempt, actually send the packet */

        struct aws_mqtt_packet_connection pingreq;
        aws_mqtt_packet_pingreq_init(&pingreq);

        struct aws_io_message *message = mqtt_get_message_for_packet(connection, &pingreq.fixed_header);
        if (!message) {
            return AWS_MQTT_CLIENT_REQUEST_ERROR;
        }

        if (aws_mqtt_packet_connection_encode(&message->message_data, &pingreq)) {
            return AWS_MQTT_CLIENT_REQUEST_ERROR;
        }

        if (aws_channel_slot_send_message(connection->slot, message, AWS_CHANNEL_DIR_WRITE)) {

            aws_mem_release(message->allocator, message);
            return AWS_MQTT_CLIENT_REQUEST_ERROR;
        }

        return AWS_MQTT_CLIENT_REQUEST_ONGOING;
    }

    /* Check that a pingresp has been received since pingreq was sent */

    uint64_t current_time = 0;
    aws_channel_current_clock_time(connection->slot->channel, &current_time);

    if (current_time - connection->last_pingresp_timestamp > connection->request_timeout_ns) {
        /* It's been too long since the last ping, close the connection */

        mqtt_disconnect_impl(connection, AWS_ERROR_MQTT_TIMEOUT);
    }

    return AWS_MQTT_CLIENT_REQUEST_COMPLETE;
}

int aws_mqtt_client_connection_ping(struct aws_mqtt_client_connection *connection) {

    mqtt_create_request(connection, &s_pingreq_send, connection, NULL, NULL);

    return AWS_OP_SUCCESS;
}
