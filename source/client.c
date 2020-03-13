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
#include <aws/io/logging.h>
#include <aws/io/socket.h>
#include <aws/io/tls_channel_handler.h>
#include <aws/io/uri.h>

#include <aws/common/clock.h>
#include <aws/common/task_scheduler.h>

#include <inttypes.h>

#ifdef AWS_MQTT_WITH_WEBSOCKETS
#    include <aws/http/connection.h>
#    include <aws/http/request_response.h>
#    include <aws/http/websocket.h>
#endif

#ifdef _MSC_VER
#    pragma warning(disable : 4204)
#endif

/* 3 seconds */
static const uint64_t s_default_request_timeout_ns = 3000000000;

/* 20 minutes - This is the default (and max) for AWS IoT as of 2020.02.18 */
static const uint16_t s_default_keep_alive_sec = 1200;

/*******************************************************************************
 * Client Init
 ******************************************************************************/

int aws_mqtt_client_init(
    struct aws_mqtt_client *client,
    struct aws_allocator *allocator,
    struct aws_client_bootstrap *bootstrap) {

    aws_mqtt_fatal_assert_library_initialized();

    AWS_LOGF_DEBUG(AWS_LS_MQTT_CLIENT, "client=%p: Initalizing MQTT client", (void *)client);

    AWS_ZERO_STRUCT(*client);
    client->allocator = allocator;
    client->bootstrap = bootstrap;

    return AWS_OP_SUCCESS;
}

/*******************************************************************************
 * Client Clean Up
 ******************************************************************************/

void aws_mqtt_client_clean_up(struct aws_mqtt_client *client) {

    AWS_LOGF_DEBUG(AWS_LS_MQTT_CLIENT, "client=%p: Cleaning up MQTT client", (void *)client);

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

    AWS_LOGF_TRACE(
        AWS_LS_MQTT_CLIENT, "id=%p: Channel has been shutdown with error code %d", (void *)connection, error_code);

    /* Always clear slot, as that's what's been shutdown */
    if (connection->slot) {
        aws_channel_slot_remove(connection->slot);
        connection->slot = NULL;
    }

    /* If there's no error code and this wasn't user-requested, set the error code to something useful */
    if (error_code == AWS_ERROR_SUCCESS) {
        if (connection->state != AWS_MQTT_CLIENT_STATE_DISCONNECTING &&
            connection->state != AWS_MQTT_CLIENT_STATE_DISCONNECTED) {
            error_code = AWS_ERROR_MQTT_UNEXPECTED_HANGUP;
        }
    }

    if (connection->state == AWS_MQTT_CLIENT_STATE_RECONNECTING) {
        /* If reconnect attempt failed, schedule the next attempt */
        struct aws_event_loop *el = aws_event_loop_group_get_next_loop(connection->client->bootstrap->event_loop_group);

        AWS_LOGF_TRACE(AWS_LS_MQTT_CLIENT, "id=%p: Reconnect failed, retrying", (void *)connection);

        aws_event_loop_schedule_task_future(
            el, &connection->reconnect_task->task, connection->reconnect_timeouts.next_attempt);

    } else if (connection->state == AWS_MQTT_CLIENT_STATE_DISCONNECTING) {

        connection->state = AWS_MQTT_CLIENT_STATE_DISCONNECTED;

        AWS_LOGF_DEBUG(
            AWS_LS_MQTT_CLIENT,
            "id=%p: Disconnect completed, clearing request queue and calling callback",
            (void *)connection);

        /* Successfully shutdown, so clear the outstanding requests */
        aws_hash_table_clear(&connection->outstanding_requests.table);

        MQTT_CLIENT_CALL_CALLBACK(connection, on_disconnect);

    } else if (connection->state == AWS_MQTT_CLIENT_STATE_CONNECTING) {

        AWS_LOGF_TRACE(
            AWS_LS_MQTT_CLIENT, "id=%p: Initial connection attempt failed, calling callback", (void *)connection);

        connection->state = AWS_MQTT_CLIENT_STATE_DISCONNECTED;

        MQTT_CLIENT_CALL_CALLBACK_ARGS(connection, on_connection_complete, error_code, 0, false);

    } else {

        AWS_ASSERT(
            connection->state == AWS_MQTT_CLIENT_STATE_CONNECTED ||
            connection->state == AWS_MQTT_CLIENT_STATE_RECONNECTING ||
            connection->state == AWS_MQTT_CLIENT_STATE_DISCONNECTED);

        if (connection->state == AWS_MQTT_CLIENT_STATE_CONNECTED) {

            AWS_LOGF_DEBUG(
                AWS_LS_MQTT_CLIENT,
                "id=%p: Connection lost, calling callback and attempting reconnect",
                (void *)connection);

            connection->state = AWS_MQTT_CLIENT_STATE_RECONNECTING;
            MQTT_CLIENT_CALL_CALLBACK_ARGS(connection, on_interrupted, error_code);
        }

        AWS_ASSERT(
            connection->state == AWS_MQTT_CLIENT_STATE_RECONNECTING ||
            connection->state == AWS_MQTT_CLIENT_STATE_DISCONNECTING ||
            connection->state == AWS_MQTT_CLIENT_STATE_DISCONNECTED);

        /* This will only be true if the user called disconnect from the on_interrupted callback */
        if (connection->state == AWS_MQTT_CLIENT_STATE_DISCONNECTING) {

            AWS_LOGF_TRACE(
                AWS_LS_MQTT_CLIENT,
                "id=%p: Caller requested disconnect from on_interrupted callback, aborting reconnect",
                (void *)connection);

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
/* The assumption here is that a connection always outlives its channels, and the channel this task was scheduled on
 * always outlives this task, so all we need to do is check the connection state. If we are in a state that waits
 * for a CONNACK, kill it off. In the case that the connection died between scheduling this task and it being executed
 * the status will always be CANCELED because this task will be canceled when the owning channel goes away. */
static void s_connack_received_timeout(struct aws_channel_task *channel_task, void *arg, enum aws_task_status status) {
    struct aws_mqtt_client_connection *connection = arg;

    if (status == AWS_TASK_STATUS_RUN_READY) {
        if (connection->state == AWS_MQTT_CLIENT_STATE_CONNECTING ||
            connection->state == AWS_MQTT_CLIENT_STATE_RECONNECTING) {
            AWS_LOGF_ERROR(AWS_LS_MQTT_CLIENT, "id=%p: mqtt CONNACK response timeout detected", (void *)connection);
            aws_channel_shutdown(connection->slot->channel, AWS_ERROR_MQTT_TIMEOUT);
        }
    }

    aws_mem_release(connection->allocator, channel_task);
}

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
    struct aws_io_message *message = NULL;

    /* Setup callback contract is: if error_code is non-zero then channel is NULL. */
    AWS_FATAL_ASSERT((error_code != 0) == (channel == NULL));

    struct aws_mqtt_client_connection *connection = user_data;

    if (error_code != AWS_OP_SUCCESS) {
        /* client shutdown already handles this case, so just call that. */
        s_mqtt_client_shutdown(bootstrap, error_code, channel, user_data);
        return;
    }

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CLIENT, "id=%p: Connection successfully opened, sending CONNECT packet", (void *)connection);

    /* Create the slot and handler */
    connection->slot = aws_channel_slot_new(channel);

    if (!connection->slot) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT_CLIENT,
            "id=%p: Failed to create new slot, something has gone horribly wrong",
            (void *)connection);
        aws_channel_shutdown(channel, aws_last_error());
        return;
    }

    aws_channel_slot_insert_end(channel, connection->slot);
    aws_channel_slot_set_handler(connection->slot, &connection->handler);

    struct aws_channel_task *connack_task = aws_mem_calloc(connection->allocator, 1, sizeof(struct aws_channel_task));
    if (!connack_task) {
        AWS_LOGF_ERROR(AWS_LS_MQTT_CLIENT, "id=%p: Failed to allocate timeout task.", (void *)connection);
        goto handle_error;
    }

    aws_channel_task_init(connack_task, s_connack_received_timeout, connection, "mqtt_connack_timeout");

    uint64_t now = 0;
    aws_channel_current_clock_time(channel, &now);
    now += connection->request_timeout_ns;
    aws_channel_schedule_task_future(channel, connack_task, now);

    /* Send the connect packet */
    struct aws_mqtt_packet_connect connect;
    aws_mqtt_packet_connect_init(
        &connect,
        aws_byte_cursor_from_buf(&connection->client_id),
        connection->clean_session,
        connection->keep_alive_time_secs);

    if (connection->will.topic.buffer) {
        /* Add will if present */

        struct aws_byte_cursor topic_cur = aws_byte_cursor_from_buf(&connection->will.topic);
        struct aws_byte_cursor payload_cur = aws_byte_cursor_from_buf(&connection->will.payload);

        AWS_LOGF_DEBUG(
            AWS_LS_MQTT_CLIENT,
            "id=%p: Adding will to connection on " PRInSTR " with payload " PRInSTR,
            (void *)connection,
            AWS_BYTE_CURSOR_PRI(topic_cur),
            AWS_BYTE_CURSOR_PRI(payload_cur));
        aws_mqtt_packet_connect_add_will(
            &connect, topic_cur, connection->will.qos, connection->will.retain, payload_cur);
    }

    if (connection->username) {
        struct aws_byte_cursor username_cur = aws_byte_cursor_from_string(connection->username);

        AWS_LOGF_DEBUG(
            AWS_LS_MQTT_CLIENT,
            "id=%p: Adding username " PRInSTR " to connection",
            (void *)connection,
            AWS_BYTE_CURSOR_PRI(username_cur))

        struct aws_byte_cursor password_cur = {
            .ptr = NULL,
            .len = 0,
        };

        if (connection->password) {
            password_cur = aws_byte_cursor_from_string(connection->password);
        }

        aws_mqtt_packet_connect_add_credentials(&connect, username_cur, password_cur);
    }

    message = mqtt_get_message_for_packet(connection, &connect.fixed_header);
    if (!message) {

        AWS_LOGF_ERROR(AWS_LS_MQTT_CLIENT, "id=%p: Failed to get message from pool", (void *)connection);
        goto handle_error;
    }

    if (aws_mqtt_packet_connect_encode(&message->message_data, &connect)) {

        AWS_LOGF_ERROR(AWS_LS_MQTT_CLIENT, "id=%p: Failed to encode CONNECT packet", (void *)connection);
        goto handle_error;
    }

    if (aws_channel_slot_send_message(connection->slot, message, AWS_CHANNEL_DIR_WRITE)) {

        AWS_LOGF_ERROR(AWS_LS_MQTT_CLIENT, "id=%p: Failed to send encoded CONNECT packet upstream", (void *)connection);
        goto handle_error;
    }

    return;

handle_error:
    MQTT_CLIENT_CALL_CALLBACK_ARGS(connection, on_connection_complete, aws_last_error(), 0, false);
    aws_channel_shutdown(channel, aws_last_error());

    if (message) {
        aws_mem_release(message->allocator, message);
    }
}

static void s_attempt_reconnect(struct aws_task *task, void *userdata, enum aws_task_status status) {

    (void)task;

    struct aws_mqtt_reconnect_task *reconnect = userdata;
    struct aws_mqtt_client_connection *connection = aws_atomic_load_ptr(&reconnect->connection_ptr);

    if (status == AWS_TASK_STATUS_RUN_READY && connection) {
        /* If the task is not cancelled and a connection has not succeeded, attempt reconnect */

        aws_high_res_clock_get_ticks(&connection->reconnect_timeouts.next_attempt);
        connection->reconnect_timeouts.next_attempt += aws_timestamp_convert(
            connection->reconnect_timeouts.current, AWS_TIMESTAMP_SECS, AWS_TIMESTAMP_NANOS, NULL);

        AWS_LOGF_TRACE(
            AWS_LS_MQTT_CLIENT,
            "id=%p: Attempting reconnect, if it fails next attempt will be in %" PRIu64 " seconds",
            (void *)connection,
            connection->reconnect_timeouts.current);

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
            AWS_LOGF_TRACE(
                AWS_LS_MQTT_CLIENT,
                "id=%p: Scheduling reconnect, for %" PRIu64 " on event-loop %p",
                (void *)connection,
                connection->reconnect_timeouts.next_attempt,
                (void *)el);
        } else {
            connection->reconnect_task->task.timestamp = 0;
        }
    } else {
        aws_mem_release(reconnect->allocator, reconnect);
    }
}

void aws_create_reconnect_task(struct aws_mqtt_client_connection *connection) {
    if (connection->reconnect_task == NULL) {
        connection->reconnect_task = aws_mem_calloc(connection->allocator, 1, sizeof(struct aws_mqtt_reconnect_task));
        AWS_FATAL_ASSERT(connection->reconnect_task != NULL);

        aws_atomic_init_ptr(&connection->reconnect_task->connection_ptr, connection);
        connection->reconnect_task->allocator = connection->allocator;
        aws_task_init(
            &connection->reconnect_task->task, s_attempt_reconnect, connection->reconnect_task, "mqtt_reconnect");
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
        AWS_LOGF_TRACE(
            AWS_LS_MQTT_CLIENT,
            "id=%p: (table element remove) Releasing request %" PRIu16 " to connection memory pool",
            (void *)(request->connection),
            request->message_id);
        /* Task ran as cancelled already, clean up the memory */
        aws_memory_pool_release(&request->connection->requests_pool, request);
    } else {
        /* Signal task to clean up request */
        request->cancelled = true;
    }
}

struct aws_mqtt_client_connection *aws_mqtt_client_connection_new(struct aws_mqtt_client *client) {

    AWS_PRECONDITION(client);

    struct aws_mqtt_client_connection *connection =
        aws_mem_calloc(client->allocator, 1, sizeof(struct aws_mqtt_client_connection));
    if (!connection) {
        return NULL;
    }

    AWS_LOGF_DEBUG(AWS_LS_MQTT_CLIENT, "id=%p: Creating new connection", (void *)connection);

    /* Initialize the client */
    connection->allocator = client->allocator;
    connection->client = client;
    connection->state = AWS_MQTT_CLIENT_STATE_DISCONNECTED;
    connection->reconnect_timeouts.min = 1;
    connection->reconnect_timeouts.max = 128;
    aws_mutex_init(&connection->outstanding_requests.mutex);
    aws_linked_list_init(&connection->pending_requests.list);

    if (aws_mutex_init(&connection->pending_requests.mutex)) {

        AWS_LOGF_ERROR(AWS_LS_MQTT_CLIENT, "id=%p: Failed to initialize pending_requests mutex", (void *)connection);
        goto failed_init_pending_requests_mutex;
    }

    if (aws_mqtt_topic_tree_init(&connection->subscriptions, connection->allocator)) {

        AWS_LOGF_ERROR(AWS_LS_MQTT_CLIENT, "id=%p: Failed to initialize subscriptions topic_tree", (void *)connection);
        goto failed_init_subscriptions;
    }

    if (aws_memory_pool_init(
            &connection->requests_pool, connection->allocator, 32, sizeof(struct aws_mqtt_outstanding_request))) {

        AWS_LOGF_ERROR(AWS_LS_MQTT_CLIENT, "id=%p: Failed to initialize request pool", (void *)connection);
        goto failed_init_request_pool;
    }

    if (aws_hash_table_init(
            &connection->outstanding_requests.table,
            connection->allocator,
            sizeof(struct aws_mqtt_outstanding_request *),
            s_hash_uint16_t,
            s_uint16_t_eq,
            NULL,
            &s_outstanding_request_destroy)) {

        AWS_LOGF_ERROR(
            AWS_LS_MQTT_CLIENT, "id=%p: Failed to initialize outstanding requests table", (void *)connection);
        goto failed_init_outstanding_requests_table;
    }

    /* Initialize the handler */
    connection->handler.alloc = connection->allocator;
    connection->handler.vtable = aws_mqtt_get_client_channel_vtable();
    connection->handler.impl = connection;

    return connection;

failed_init_outstanding_requests_table:
    aws_memory_pool_clean_up(&connection->requests_pool);

failed_init_request_pool:
    aws_mqtt_topic_tree_clean_up(&connection->subscriptions);

failed_init_subscriptions:
    aws_mutex_clean_up(&connection->outstanding_requests.mutex);

failed_init_pending_requests_mutex:
    aws_mem_release(client->allocator, connection);

    return NULL;
}

/*******************************************************************************
 * Connection Destroy
 ******************************************************************************/

void aws_mqtt_client_connection_destroy(struct aws_mqtt_client_connection *connection) {

    AWS_PRECONDITION(connection);
    AWS_ASSERT(connection->state == AWS_MQTT_CLIENT_STATE_DISCONNECTED);

    AWS_LOGF_DEBUG(AWS_LS_MQTT_CLIENT, "id=%p: Destroying connection", (void *)connection);

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

    /* Clean up the websocket proxy options */
    if (connection->websocket.proxy) {
        aws_tls_connection_options_clean_up(&connection->websocket.proxy->tls_options);

        aws_mem_release(connection->allocator, connection->websocket.proxy);
        connection->websocket.proxy = NULL;
        connection->websocket.proxy_options = NULL;
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

    AWS_LOGF_TRACE(
        AWS_LS_MQTT_CLIENT,
        "id=%p: Setting last will with topic \"" PRInSTR "\"",
        (void *)connection,
        AWS_BYTE_CURSOR_PRI(*topic));

    if (!aws_mqtt_is_valid_topic(topic)) {
        AWS_LOGF_ERROR(AWS_LS_MQTT_CLIENT, "id=%p: Will topic is invalid", (void *)connection);
        return aws_raise_error(AWS_ERROR_MQTT_INVALID_TOPIC);
    }

    struct aws_byte_buf topic_buf = aws_byte_buf_from_array(topic->ptr, topic->len);
    if (aws_byte_buf_init_copy(&connection->will.topic, connection->allocator, &topic_buf)) {
        AWS_LOGF_ERROR(AWS_LS_MQTT_CLIENT, "id=%p: Failed to copy will topic", (void *)connection);
        goto cleanup;
    }

    connection->will.qos = qos;
    connection->will.retain = retain;

    struct aws_byte_buf payload_buf = aws_byte_buf_from_array(payload->ptr, payload->len);
    if (aws_byte_buf_init_copy(&connection->will.payload, connection->allocator, &payload_buf)) {
        AWS_LOGF_ERROR(AWS_LS_MQTT_CLIENT, "id=%p: Failed to copy will body", (void *)connection);
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

    AWS_PRECONDITION(connection);
    AWS_PRECONDITION(username);

    AWS_LOGF_TRACE(AWS_LS_MQTT_CLIENT, "id=%p: Setting username and password", (void *)connection);

    connection->username = aws_string_new_from_array(connection->allocator, username->ptr, username->len);
    if (!connection->username) {
        AWS_LOGF_ERROR(AWS_LS_MQTT_CLIENT, "id=%p: Failed to copy username", (void *)connection);
        return AWS_OP_ERR;
    }

    if (password) {
        connection->password = aws_string_new_from_array(connection->allocator, password->ptr, password->len);
        if (!connection->password) {
            AWS_LOGF_ERROR(AWS_LS_MQTT_CLIENT, "id=%p: Failed to copy password", (void *)connection);
            aws_string_destroy(connection->username);
            return AWS_OP_ERR;
        }
    }

    return AWS_OP_SUCCESS;
}

int aws_mqtt_client_connection_set_reconnect_timeout(
    struct aws_mqtt_client_connection *connection,
    uint64_t min_timeout,
    uint64_t max_timeout) {

    AWS_PRECONDITION(connection);

    AWS_LOGF_TRACE(
        AWS_LS_MQTT_CLIENT,
        "id=%p: Setting reconnect timeouts min: %" PRIu64 " max: %" PRIu64,
        (void *)connection,
        min_timeout,
        max_timeout);

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

    AWS_LOGF_TRACE(
        AWS_LS_MQTT_CLIENT, "id=%p: Setting connection interrupted and resumed handlers", (void *)connection);

    connection->on_interrupted = on_interrupted;
    connection->on_interrupted_ud = on_interrupted_ud;
    connection->on_resumed = on_resumed;
    connection->on_resumed_ud = on_resumed_ud;

    return AWS_OP_SUCCESS;
}

int aws_mqtt_client_connection_set_on_any_publish_handler(
    struct aws_mqtt_client_connection *connection,
    aws_mqtt_client_publish_received_fn *on_any_publish,
    void *on_any_publish_ud) {

    AWS_PRECONDITION(connection);

    AWS_LOGF_TRACE(AWS_LS_MQTT_CLIENT, "id=%p: Setting on_any_publish handler", (void *)connection);

    connection->on_any_publish = on_any_publish;
    connection->on_any_publish_ud = on_any_publish_ud;

    return AWS_OP_SUCCESS;
}

#ifdef AWS_MQTT_WITH_WEBSOCKETS

int aws_mqtt_client_connection_use_websockets(
    struct aws_mqtt_client_connection *connection,
    aws_mqtt_transform_websocket_handshake_fn *transformer,
    void *transformer_ud,
    aws_mqtt_validate_websocket_handshake_fn *validator,
    void *validator_ud) {

    connection->websocket.handshake_transformer = transformer;
    connection->websocket.handshake_transformer_ud = transformer_ud;
    connection->websocket.handshake_validator = validator;
    connection->websocket.handshake_validator_ud = validator_ud;
    connection->websocket.enabled = true;

    AWS_LOGF_TRACE(AWS_LS_MQTT_CLIENT, "id=%p: Using websockets", (void *)connection);

    return AWS_OP_SUCCESS;
}

int aws_mqtt_client_connection_set_websocket_proxy_options(
    struct aws_mqtt_client_connection *connection,
    struct aws_http_proxy_options *proxy_options) {

    /* If there is existing proxy options, nuke em */
    if (connection->websocket.proxy) {
        aws_tls_connection_options_clean_up(&connection->websocket.proxy->tls_options);

        aws_mem_release(connection->allocator, connection->websocket.proxy);
        connection->websocket.proxy = NULL;
        connection->websocket.proxy_options = NULL;
    }

    /* Allocate new proxy options object, and add space for buffered strings */
    void *host_buffer = NULL;
    void *username_buffer = NULL;
    void *password_buffer = NULL;

    /* clang-format off */
    void *alloc = aws_mem_acquire_many(connection->allocator, 5,
        &connection->websocket.proxy, sizeof(*connection->websocket.proxy),
        &connection->websocket.proxy_options, sizeof(struct aws_http_proxy_options),
        &host_buffer, proxy_options->host.len,
        &username_buffer, proxy_options->auth_username.len,
        &password_buffer, proxy_options->auth_password.len);
    /* clang-format on */

    if (!alloc) {
        return AWS_OP_ERR;
    }

    AWS_ZERO_STRUCT(*connection->websocket.proxy);
    AWS_ZERO_STRUCT(*connection->websocket.proxy_options);

    /* Copy the TLS options */
    if (proxy_options->tls_options) {
        if (aws_tls_connection_options_copy(&connection->websocket.proxy->tls_options, proxy_options->tls_options)) {
            aws_mem_release(connection->allocator, alloc);
            return AWS_OP_ERR;
        }
        connection->websocket.proxy_options->tls_options = &connection->websocket.proxy->tls_options;
    }

    /* Init the byte bufs */
    connection->websocket.proxy->host = aws_byte_buf_from_empty_array(host_buffer, proxy_options->host.len);
    connection->websocket.proxy->auth_username =
        aws_byte_buf_from_empty_array(username_buffer, proxy_options->auth_username.len);
    connection->websocket.proxy->auth_password =
        aws_byte_buf_from_empty_array(password_buffer, proxy_options->auth_password.len);

    /* Write out the various strings */
    bool succ = true;
    succ &= aws_byte_buf_write_from_whole_cursor(&connection->websocket.proxy->host, proxy_options->host);
    succ &=
        aws_byte_buf_write_from_whole_cursor(&connection->websocket.proxy->auth_username, proxy_options->auth_username);
    succ &=
        aws_byte_buf_write_from_whole_cursor(&connection->websocket.proxy->auth_password, proxy_options->auth_password);
    AWS_FATAL_ASSERT(succ);

    /* Update the proxy options cursors */
    connection->websocket.proxy_options->host = aws_byte_cursor_from_buf(&connection->websocket.proxy->host);
    connection->websocket.proxy_options->auth_username =
        aws_byte_cursor_from_buf(&connection->websocket.proxy->auth_username);
    connection->websocket.proxy_options->auth_password =
        aws_byte_cursor_from_buf(&connection->websocket.proxy->auth_password);

    /* Update proxy options values */
    connection->websocket.proxy_options->port = proxy_options->port;
    connection->websocket.proxy_options->auth_type = proxy_options->auth_type;

    return AWS_OP_SUCCESS;
}

static void s_on_websocket_shutdown(struct aws_websocket *websocket, int error_code, void *user_data) {
    struct aws_mqtt_client_connection *connection = user_data;

    struct aws_channel *channel = connection->slot ? connection->slot->channel : NULL;

    s_mqtt_client_shutdown(connection->client->bootstrap, error_code, channel, connection);

    if (websocket) {
        aws_websocket_release(websocket);
    }
}

static void s_on_websocket_setup(
    struct aws_websocket *websocket,
    int error_code,
    int handshake_response_status,
    const struct aws_http_header *handshake_response_header_array,
    size_t num_handshake_response_headers,
    void *user_data) {

    (void)handshake_response_status;

    /* Setup callback contract is: if error_code is non-zero then websocket is NULL. */
    AWS_FATAL_ASSERT((error_code != 0) == (websocket == NULL));

    struct aws_mqtt_client_connection *connection = user_data;
    struct aws_channel *channel = NULL;

    if (connection->websocket.handshake_request) {
        aws_http_message_release(connection->websocket.handshake_request);
        connection->websocket.handshake_request = NULL;
    }

    if (websocket) {
        channel = aws_websocket_get_channel(websocket);
        AWS_ASSERT(channel);

        /* Websocket must be "converted" before the MQTT handler can be installed next to it. */
        if (aws_websocket_convert_to_midchannel_handler(websocket)) {
            AWS_LOGF_ERROR(
                AWS_LS_MQTT_CLIENT,
                "id=%p: Failed converting websocket, error %d (%s)",
                (void *)connection,
                aws_last_error(),
                aws_error_name(aws_last_error()));

            aws_channel_shutdown(channel, aws_last_error());
            return;
        }

        /* If validation callback is set, let the user accept/reject the handshake */
        if (connection->websocket.handshake_validator) {
            AWS_LOGF_TRACE(AWS_LS_MQTT_CLIENT, "id=%p: Validating websocket handshake response.", (void *)connection);

            if (connection->websocket.handshake_validator(
                    connection,
                    handshake_response_header_array,
                    num_handshake_response_headers,
                    connection->websocket.handshake_validator_ud)) {

                AWS_LOGF_ERROR(
                    AWS_LS_MQTT_CLIENT,
                    "id=%p: Failure reported by websocket handshake validator callback, error %d (%s)",
                    (void *)connection,
                    aws_last_error(),
                    aws_error_name(aws_last_error()));

                aws_channel_shutdown(channel, aws_last_error());
                return;
            }

            AWS_LOGF_TRACE(
                AWS_LS_MQTT_CLIENT, "id=%p: Done validating websocket handshake response.", (void *)connection);
        }
    }

    /* Call into the channel-setup callback, the rest of the logic is the same. */
    s_mqtt_client_init(connection->client->bootstrap, error_code, channel, connection);
}

static aws_mqtt_transform_websocket_handshake_complete_fn s_websocket_handshake_transform_complete; /* fwd declare */

static int s_websocket_connect(struct aws_mqtt_client_connection *connection) {
    AWS_ASSERT(connection->websocket.enabled);

    /* These defaults were chosen because they're commmon in other MQTT libraries.
     * The user can modify the request in their transform callback if they need to. */
    const struct aws_byte_cursor default_path = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/mqtt");
    const struct aws_http_header default_protocol_header = {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Sec-WebSocket-Protocol"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("mqtt"),
    };

    /* Build websocket handshake request */
    connection->websocket.handshake_request = aws_http_message_new_websocket_handshake_request(
        connection->allocator, default_path, aws_byte_cursor_from_string(connection->host_name));

    if (!connection->websocket.handshake_request) {
        AWS_LOGF_ERROR(AWS_LS_MQTT_CLIENT, "id=%p: Failed to generate websocket handshake request", (void *)connection);
        goto error;
    }

    if (aws_http_message_add_header(connection->websocket.handshake_request, default_protocol_header)) {
        AWS_LOGF_ERROR(AWS_LS_MQTT_CLIENT, "id=%p: Failed to generate websocket handshake request", (void *)connection);
        goto error;
    }

    /* If user registered a transform callback, call it and wait for transform_complete() to be called.
     * If no callback registered, call the transform_complete() function ourselves. */
    if (connection->websocket.handshake_transformer) {
        AWS_LOGF_TRACE(AWS_LS_MQTT_CLIENT, "id=%p: Transforming websocket handshake request.", (void *)connection);

        connection->websocket.handshake_transformer(
            connection->websocket.handshake_request,
            connection->websocket.handshake_transformer_ud,
            s_websocket_handshake_transform_complete,
            connection);

    } else {
        s_websocket_handshake_transform_complete(
            connection->websocket.handshake_request, AWS_ERROR_SUCCESS, connection);
    }

    return AWS_OP_SUCCESS;

error:
    aws_http_message_release(connection->websocket.handshake_request);
    connection->websocket.handshake_request = NULL;
    return AWS_OP_ERR;
}

static void s_websocket_handshake_transform_complete(
    struct aws_http_message *handshake_request,
    int error_code,
    void *complete_ctx) {

    struct aws_mqtt_client_connection *connection = complete_ctx;

    if (error_code) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT_CLIENT,
            "id=%p: Failure reported by websocket handshake transform callback.",
            (void *)connection);

        goto error;
    }

    if (connection->websocket.handshake_transformer) {
        AWS_LOGF_TRACE(AWS_LS_MQTT_CLIENT, "id=%p: Done transforming websocket handshake request.", (void *)connection);
    }

    /* Call websocket connect() */
    struct aws_websocket_client_connection_options websocket_options = {
        .allocator = connection->allocator,
        .bootstrap = connection->client->bootstrap,
        .socket_options = &connection->socket_options,
        .tls_options = connection->tls_options.ctx ? &connection->tls_options : NULL,
        .proxy_options = connection->websocket.proxy_options,
        .host = aws_byte_cursor_from_string(connection->host_name),
        .port = connection->port,
        .handshake_request = handshake_request,
        .initial_window_size = 0, /* Prevent websocket data from arriving before the MQTT handler is installed */
        .user_data = connection,
        .on_connection_setup = s_on_websocket_setup,
        .on_connection_shutdown = s_on_websocket_shutdown,
    };

    if (aws_websocket_client_connect(&websocket_options)) {
        AWS_LOGF_ERROR(AWS_LS_MQTT_CLIENT, "id=%p: Failed to initiate websocket connection.", (void *)connection);
        error_code = aws_last_error();
        goto error;
    }

    /* Success */
    return;

error:
    /* Proceed to next step, telling it that we failed. */
    s_on_websocket_setup(NULL, error_code, -1, NULL, 0, connection);
}

#else  /* AWS_MQTT_WITH_WEBSOCKETS */
int aws_mqtt_client_connection_use_websockets(
    struct aws_mqtt_client_connection *connection,
    aws_mqtt_transform_websocket_handshake_fn *transformer,
    void *transformer_ud,
    aws_mqtt_validate_websocket_handshake_fn *validator,
    void *validator_ud) {

    (void)connection;
    (void)transformer;
    (void)transformer_ud;
    (void)validator;
    (void)validator_ud;

    AWS_LOGF_ERROR(
        AWS_LS_MQTT_CLIENT,
        "id=%p: Cannot use websockets unless library is built with MQTT_WITH_WEBSOCKETS option.",
        (void *)connection);

    return aws_raise_error(AWS_ERROR_MQTT_BUILT_WITHOUT_WEBSOCKETS);
}

int aws_mqtt_client_connection_set_websocket_proxy_options(
    struct aws_mqtt_client_connection *connection,
    struct aws_http_proxy_options *proxy_options) {

    (void)connection;
    (void)proxy_options;

    AWS_LOGF_ERROR(
        AWS_LS_MQTT_CLIENT,
        "id=%p: Cannot use websockets unless library is built with MQTT_WITH_WEBSOCKETS option.",
        (void *)connection);

    return aws_raise_error(AWS_ERROR_MQTT_BUILT_WITHOUT_WEBSOCKETS);
}
#endif /* AWS_MQTT_WITH_WEBSOCKETS */

/*******************************************************************************
 * Connect
 ******************************************************************************/

int aws_mqtt_client_connection_connect(
    struct aws_mqtt_client_connection *connection,
    const struct aws_mqtt_connection_options *connection_options) {

    AWS_LOGF_TRACE(AWS_LS_MQTT_CLIENT, "id=%p: Opening connection", (void *)connection);

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

    if (!connection->keep_alive_time_secs) {
        connection->keep_alive_time_secs = s_default_keep_alive_sec;
    }

    if (!connection_options->ping_timeout_ms) {
        connection->request_timeout_ns = s_default_request_timeout_ns;
    } else {
        connection->request_timeout_ns = aws_timestamp_convert(
            (uint64_t)connection_options->ping_timeout_ms, AWS_TIMESTAMP_MILLIS, AWS_TIMESTAMP_NANOS, NULL);
    }

    /* Keep alive time should always be greater than the timeouts. */
    if (AWS_UNLIKELY(
            connection->keep_alive_time_secs * (uint64_t)AWS_TIMESTAMP_NANOS <= connection->request_timeout_ns)) {
        AWS_LOGF_FATAL(
            AWS_LS_MQTT_CLIENT,
            "id=%p: Illegal configuration, Connection keep alive %" PRIu64
            "ns must be greater than the request timeouts %" PRIu64 "ns.",
            (void *)connection,
            (uint64_t)connection->keep_alive_time_secs * (uint64_t)AWS_TIMESTAMP_NANOS,
            connection->request_timeout_ns);
        AWS_FATAL_ASSERT(
            connection->keep_alive_time_secs * (uint64_t)AWS_TIMESTAMP_NANOS > connection->request_timeout_ns);
    }

    AWS_LOGF_INFO(
        AWS_LS_MQTT_CLIENT,
        "id=%p: using ping timeout of %" PRIu64 " ns",
        (void *)connection,
        connection->request_timeout_ns);

    /* Cheat and set the tls_options host_name to our copy if they're the same */
    if (connection_options->tls_options) {
        connection->use_tls = true;
        if (aws_tls_connection_options_copy(&connection->tls_options, connection_options->tls_options)) {

            AWS_LOGF_ERROR(
                AWS_LS_MQTT_CLIENT, "id=%p: Failed to copy TLS Connection Options into connection", (void *)connection);
            return AWS_OP_ERR;
        }

        if (!connection_options->tls_options->server_name) {
            struct aws_byte_cursor host_name_cur = aws_byte_cursor_from_string(connection->host_name);
            if (aws_tls_connection_options_set_server_name(
                    &connection->tls_options, connection->allocator, &host_name_cur)) {

                AWS_LOGF_ERROR(
                    AWS_LS_MQTT_CLIENT, "id=%p: Failed to set TLS Connection Options server name", (void *)connection);
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

    /* Only set connection->client_id if a new one was provided */
    struct aws_byte_buf client_id_buf =
        aws_byte_buf_from_array(connection_options->client_id.ptr, connection_options->client_id.len);
    if (aws_byte_buf_init_copy(&connection->client_id, connection->allocator, &client_id_buf)) {
        AWS_LOGF_ERROR(AWS_LS_MQTT_CLIENT, "id=%p: Failed to copy client_id into connection", (void *)connection);
        goto error;
    }

    if (aws_mqtt_client_connection_reconnect(
            connection, connection_options->on_connection_complete, connection_options->user_data)) {
        /* client_id has been updated with something but it will get cleaned up when the connection gets cleaned up
         * so we don't need to worry about it here*/
        goto error;
    }

    return AWS_OP_SUCCESS;

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

    int result = 0;
#ifdef AWS_MQTT_WITH_WEBSOCKETS
    if (connection->websocket.enabled) {
        result = s_websocket_connect(connection);
    } else
#endif /* AWS_MQTT_WITH_WEBSOCKETS */
    {
        struct aws_socket_channel_bootstrap_options channel_options;
        AWS_ZERO_STRUCT(channel_options);
        channel_options.bootstrap = connection->client->bootstrap;
        channel_options.host_name = aws_string_c_str(connection->host_name);
        channel_options.port = connection->port;
        channel_options.socket_options = &connection->socket_options;
        channel_options.tls_options = connection->use_tls ? &connection->tls_options : NULL;
        channel_options.setup_callback = &s_mqtt_client_init;
        channel_options.shutdown_callback = &s_mqtt_client_shutdown;
        channel_options.user_data = connection;

        result = aws_client_bootstrap_new_socket_channel(&channel_options);
    }

    if (result) {
        /* Connection attempt failed */
        AWS_LOGF_ERROR(
            AWS_LS_MQTT_CLIENT,
            "id=%p: Failed to begin connection routine, error %d (%s).",
            (void *)connection,
            aws_last_error(),
            aws_error_name(aws_last_error()));
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

    AWS_LOGF_DEBUG(AWS_LS_MQTT_CLIENT, "id=%p: user called disconnect.", (void *)connection);

    if (connection->state == AWS_MQTT_CLIENT_STATE_CONNECTED ||
        connection->state == AWS_MQTT_CLIENT_STATE_RECONNECTING) {

        AWS_LOGF_DEBUG(AWS_LS_MQTT_CLIENT, "id=%p: Closing connection", (void *)connection);

        connection->on_disconnect = on_disconnect;
        connection->on_disconnect_ud = userdata;

        connection->state = AWS_MQTT_CLIENT_STATE_DISCONNECTING;
        mqtt_disconnect_impl(connection, AWS_OP_SUCCESS);

        return AWS_OP_SUCCESS;
    }

    AWS_LOGF_ERROR(AWS_LS_MQTT_CLIENT, "id=%p: Connection is not open, and may not be closed", (void *)connection);
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
    bool is_local;
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

    struct {
        aws_mqtt_suback_multi_fn *multi;
        aws_mqtt_suback_fn *single;
    } on_suback;
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

    AWS_LOGF_TRACE(
        AWS_LS_MQTT_CLIENT,
        "id=%p: Attempting send of subscribe %" PRIu16 " (%s)",
        (void *)task_arg->connection,
        message_id,
        is_first_attempt ? "first attempt" : "resend");

    if (initing_packet) {
        /* Init the subscribe packet */
        if (aws_mqtt_packet_subscribe_init(&task_arg->subscribe, task_arg->connection->allocator, message_id)) {
            return AWS_MQTT_CLIENT_REQUEST_ERROR;
        }
    }

    const size_t num_topics = aws_array_list_length(&task_arg->topics);
    if (num_topics <= 0) {
        aws_raise_error(AWS_ERROR_MQTT_INVALID_TOPIC);
        return AWS_MQTT_CLIENT_REQUEST_ERROR;
    }

    AWS_VARIABLE_LENGTH_ARRAY(uint8_t, transaction_buf, num_topics * aws_mqtt_topic_tree_action_size);
    struct aws_array_list transaction;
    aws_array_list_init_static(&transaction, transaction_buf, num_topics, aws_mqtt_topic_tree_action_size);

    for (size_t i = 0; i < num_topics; ++i) {

        struct subscribe_task_topic *topic = NULL;
        aws_array_list_get_at(&task_arg->topics, &topic, i);
        AWS_ASSUME(topic); /* We know we're within bounds */

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

    /* This is not necessarily a fatal error; if the subscribe fails, it'll just retry.  Still need to clean up though.
     */
    if (aws_channel_slot_send_message(task_arg->connection->slot, message, AWS_CHANNEL_DIR_WRITE)) {
        aws_mem_release(message->allocator, message);
    }

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

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CLIENT,
        "id=%p: Subscribe %" PRIu16 " completed with error_code %d",
        (void *)connection,
        packet_id,
        error_code);

    if (task_arg->on_suback.multi) {
        task_arg->on_suback.multi(connection, packet_id, &task_arg->topics, error_code, task_arg->on_suback_ud);
    } else if (task_arg->on_suback.single) {
        struct subscribe_task_topic *topic = NULL;
        aws_array_list_get_at(&task_arg->topics, &topic, 0);
        AWS_ASSUME(topic);
        struct aws_byte_cursor topic_cur = aws_byte_cursor_from_string(topic->filter);
        task_arg->on_suback.single(
            connection, packet_id, &topic_cur, topic->request.qos, error_code, task_arg->on_suback_ud);
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

    AWS_PRECONDITION(connection);

    struct subscribe_task_arg *task_arg = aws_mem_calloc(connection->allocator, 1, sizeof(struct subscribe_task_arg));
    if (!task_arg) {
        return 0;
    }

    task_arg->connection = connection;
    task_arg->on_suback.multi = on_suback;
    task_arg->on_suback_ud = on_suback_ud;

    const size_t num_topics = aws_array_list_length(topic_filters);

    if (aws_array_list_init_dynamic(&task_arg->topics, connection->allocator, num_topics, sizeof(void *))) {
        goto handle_error;
    }

    AWS_LOGF_DEBUG(AWS_LS_MQTT_CLIENT, "id=%p: Starting multi-topic subscribe", (void *)connection);

    for (size_t i = 0; i < num_topics; ++i) {

        struct aws_mqtt_topic_subscription *request = NULL;
        aws_array_list_get_at_ptr(topic_filters, (void **)&request, i);

        if (!aws_mqtt_is_valid_topic_filter(&request->topic)) {
            aws_raise_error(AWS_ERROR_MQTT_INVALID_TOPIC);
            goto handle_error;
        }

        struct subscribe_task_topic *task_topic =
            aws_mem_calloc(connection->allocator, 1, sizeof(struct subscribe_task_topic));
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

        AWS_LOGF_DEBUG(
            AWS_LS_MQTT_CLIENT,
            "id=%p:     Adding topic \"" PRInSTR "\"",
            (void *)connection,
            AWS_BYTE_CURSOR_PRI(task_topic->request.topic));

        /* Push into the list */
        aws_array_list_push_back(&task_arg->topics, &request);
    }

    uint16_t packet_id =
        mqtt_create_request(task_arg->connection, &s_subscribe_send, task_arg, &s_subscribe_complete, task_arg);

    AWS_LOGF_DEBUG(AWS_LS_MQTT_CLIENT, "id=%p: Sending multi-topic subscribe %" PRIu16, (void *)connection, packet_id);

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
                AWS_ASSUME(task_topic);

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

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CLIENT,
        "id=%p: Subscribe %" PRIu16 " completed with error code %d",
        (void *)connection,
        packet_id,
        error_code);

    AWS_ASSERT(aws_array_list_length(&task_arg->topics) == 1);

    if (task_arg->on_suback.single) {
        struct subscribe_task_topic *topic = NULL;
        aws_array_list_get_at(&task_arg->topics, &topic, 0);
        AWS_ASSUME(topic); /* There needs to be exactly 1 topic in this list */

        aws_mqtt_suback_fn *suback = task_arg->on_suback.single;
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

    AWS_PRECONDITION(connection);

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
    task_arg->on_suback.single = on_suback;
    task_arg->on_suback_ud = on_suback_ud;

    aws_array_list_init_static(&task_arg->topics, task_topic_storage, 1, sizeof(void *));

    /* Allocate the topic and push into the list */
    task_topic = aws_mem_calloc(connection->allocator, 1, sizeof(struct subscribe_task_topic));
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

    uint16_t packet_id =
        mqtt_create_request(task_arg->connection, &s_subscribe_send, task_arg, &s_subscribe_single_complete, task_arg);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CLIENT,
        "id=%p: Starting subscribe %" PRIu16 " on topic " PRInSTR,
        (void *)connection,
        packet_id,
        AWS_BYTE_CURSOR_PRI(task_topic->request.topic));

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
 * Subscribe Local
 ******************************************************************************/

/* The lifetime of this struct is from subscribe -> suback */
struct subscribe_local_task_arg {

    struct aws_mqtt_client_connection *connection;

    struct subscribe_task_topic *task_topic;

    aws_mqtt_suback_fn *on_suback;
    void *on_suback_ud;
};

static enum aws_mqtt_client_request_state s_subscribe_local_send(
    uint16_t message_id,
    bool is_first_attempt,
    void *userdata) {

    (void)is_first_attempt;

    struct subscribe_local_task_arg *task_arg = userdata;

    AWS_LOGF_TRACE(
        AWS_LS_MQTT_CLIENT,
        "id=%p: Attempting save of local subscribe %" PRIu16 " (%s)",
        (void *)task_arg->connection,
        message_id,
        is_first_attempt ? "first attempt" : "redo");

    struct subscribe_task_topic *topic = task_arg->task_topic;
    if (aws_mqtt_topic_tree_insert(
            &task_arg->connection->subscriptions,
            topic->filter,
            topic->request.qos,
            s_on_publish_client_wrapper,
            s_on_topic_clean_up,
            topic)) {

        return AWS_MQTT_CLIENT_REQUEST_ERROR;
    }

    return AWS_MQTT_CLIENT_REQUEST_COMPLETE;
}

static void s_subscribe_local_complete(
    struct aws_mqtt_client_connection *connection,
    uint16_t packet_id,
    int error_code,
    void *userdata) {

    struct subscribe_local_task_arg *task_arg = userdata;

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CLIENT,
        "id=%p: Local subscribe %" PRIu16 " completed with error code %d",
        (void *)connection,
        packet_id,
        error_code);

    if (task_arg->on_suback) {
        struct subscribe_task_topic *topic = task_arg->task_topic;
        aws_mqtt_suback_fn *suback = task_arg->on_suback;

        suback(connection, packet_id, &topic->request.topic, topic->request.qos, error_code, task_arg->on_suback_ud);
    }

    aws_mem_release(task_arg->connection->allocator, task_arg);
}

uint16_t aws_mqtt_client_connection_subscribe_local(
    struct aws_mqtt_client_connection *connection,
    const struct aws_byte_cursor *topic_filter,
    aws_mqtt_client_publish_received_fn *on_publish,
    void *on_publish_ud,
    aws_mqtt_userdata_cleanup_fn *on_ud_cleanup,
    aws_mqtt_suback_fn *on_suback,
    void *on_suback_ud) {

    AWS_PRECONDITION(connection);

    if (!aws_mqtt_is_valid_topic_filter(topic_filter)) {
        aws_raise_error(AWS_ERROR_MQTT_INVALID_TOPIC);
        return 0;
    }

    struct subscribe_task_topic *task_topic = NULL;

    struct subscribe_local_task_arg *task_arg =
        aws_mem_calloc(connection->allocator, 1, sizeof(struct subscribe_local_task_arg));

    if (!task_arg) {
        goto handle_error;
    }
    AWS_ZERO_STRUCT(*task_arg);

    task_arg->connection = connection;
    task_arg->on_suback = on_suback;
    task_arg->on_suback_ud = on_suback_ud;
    task_topic = aws_mem_calloc(connection->allocator, 1, sizeof(struct subscribe_task_topic));
    if (!task_topic) {
        goto handle_error;
    }
    task_arg->task_topic = task_topic;

    task_topic->filter = aws_string_new_from_array(connection->allocator, topic_filter->ptr, topic_filter->len);
    if (!task_topic->filter) {
        goto handle_error;
    }

    task_topic->connection = connection;
    task_topic->is_local = true;
    task_topic->request.topic = aws_byte_cursor_from_string(task_topic->filter);
    task_topic->request.on_publish = on_publish;
    task_topic->request.on_cleanup = on_ud_cleanup;
    task_topic->request.on_publish_ud = on_publish_ud;

    uint16_t packet_id = mqtt_create_request(
        task_arg->connection, s_subscribe_local_send, task_arg, &s_subscribe_local_complete, task_arg);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CLIENT,
        "id=%p: Starting local subscribe %" PRIu16 " on topic " PRInSTR,
        (void *)connection,
        packet_id,
        AWS_BYTE_CURSOR_PRI(task_topic->request.topic));

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
 * Resubscribe
 ******************************************************************************/

static bool s_reconnect_resub_iterator(const struct aws_byte_cursor *topic, enum aws_mqtt_qos qos, void *user_data) {
    struct subscribe_task_arg *task_arg = user_data;

    struct aws_mqtt_topic_subscription sub;
    AWS_ZERO_STRUCT(sub);
    sub.topic = *topic;
    sub.qos = qos;

    aws_array_list_push_back(&task_arg->topics, &sub);

    return true;
}

static enum aws_mqtt_client_request_state s_resubscribe_send(
    uint16_t message_id,
    bool is_first_attempt,
    void *userdata) {

    (void)is_first_attempt;

    struct subscribe_task_arg *task_arg = userdata;
    bool initing_packet = task_arg->subscribe.fixed_header.packet_type == 0;
    struct aws_io_message *message = NULL;

    AWS_LOGF_TRACE(
        AWS_LS_MQTT_CLIENT,
        "id=%p: Attempting send of resubscribe %" PRIu16 " (%s)",
        (void *)task_arg->connection,
        message_id,
        is_first_attempt ? "first attempt" : "resend");

    if (initing_packet) {
        /* Init the subscribe packet */
        if (aws_mqtt_packet_subscribe_init(&task_arg->subscribe, task_arg->connection->allocator, message_id)) {
            return AWS_MQTT_CLIENT_REQUEST_ERROR;
        }

        const size_t num_topics = aws_array_list_length(&task_arg->topics);
        if (num_topics <= 0) {
            aws_raise_error(AWS_ERROR_MQTT_INVALID_TOPIC);
            return AWS_MQTT_CLIENT_REQUEST_ERROR;
        }

        for (size_t i = 0; i < num_topics; ++i) {

            struct aws_mqtt_topic_subscription *topic = NULL;
            aws_array_list_get_at_ptr(&task_arg->topics, (void **)&topic, i);
            AWS_ASSUME(topic); /* We know we're within bounds */

            if (aws_mqtt_packet_subscribe_add_topic(&task_arg->subscribe, topic->topic, topic->qos)) {
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

    /* This is not necessarily a fatal error; if the send fails, it'll just retry.  Still need to clean up though. */
    if (aws_channel_slot_send_message(task_arg->connection->slot, message, AWS_CHANNEL_DIR_WRITE)) {
        aws_mem_release(message->allocator, message);
    }

    return AWS_MQTT_CLIENT_REQUEST_ONGOING;

handle_error:

    if (message) {
        aws_mem_release(message->allocator, message);
    }

    return AWS_MQTT_CLIENT_REQUEST_ERROR;
}

uint16_t aws_mqtt_resubscribe_existing_topics(
    struct aws_mqtt_client_connection *connection,
    aws_mqtt_suback_multi_fn *on_suback,
    void *on_suback_ud) {

    size_t sub_count = aws_mqtt_topic_tree_get_sub_count(&connection->subscriptions);
    static const size_t sub_size = sizeof(struct aws_mqtt_topic_subscription);

    if (sub_count == 0) {
        aws_raise_error(AWS_ERROR_MQTT_NO_TOPICS_FOR_RESUBSCRIBE);
        AWS_LOGF_WARN(
            AWS_LS_MQTT_CLIENT,
            "id=%p: Not subscribed to any topics. Resubscribe is unnecessary, no packet will be sent. Error %s.",
            (void *)connection,
            aws_error_name(aws_last_error()));
        return 0;
    }

    struct subscribe_task_arg *task_arg = NULL;
    void *buffer = NULL;
    aws_mem_acquire_many(
        connection->allocator, 2, &task_arg, sizeof(struct subscribe_task_arg), &buffer, sub_count * sub_size);

    if (!task_arg) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT_CLIENT, "id=%p: failed to allocate storage for resubscribe arguments", (void *)connection);
        return 0;
    }

    AWS_ZERO_STRUCT(*task_arg);
    task_arg->connection = connection;
    task_arg->on_suback.multi = on_suback;
    task_arg->on_suback_ud = on_suback_ud;
    aws_array_list_init_static(&task_arg->topics, buffer, sub_count, sub_size);
    aws_mqtt_topic_tree_iterate(&connection->subscriptions, s_reconnect_resub_iterator, task_arg);

    uint16_t packet_id =
        mqtt_create_request(task_arg->connection, &s_resubscribe_send, task_arg, &s_subscribe_complete, task_arg);

    if (packet_id == 0) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT_CLIENT,
            "id=%p: Failed to send multi-topic resubscribe with error %s",
            (void *)connection,
            aws_error_name(aws_last_error()));
        return 0;
    }

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CLIENT, "id=%p: Sending multi-topic resubscribe %" PRIu16, (void *)connection, packet_id);

    return packet_id;
}

/*******************************************************************************
 * Unsubscribe
 ******************************************************************************/

struct unsubscribe_task_arg {
    struct aws_mqtt_client_connection *connection;
    struct aws_string *filter_string;
    struct aws_byte_cursor filter;
    bool is_local;
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

    AWS_LOGF_TRACE(
        AWS_LS_MQTT_CLIENT,
        "id=%p: Attempting send of unsubscribe %" PRIu16 " %s",
        (void *)task_arg->connection,
        message_id,
        is_first_attempt ? "first attempt" : "resend");

    static const size_t num_topics = 1;

    AWS_VARIABLE_LENGTH_ARRAY(uint8_t, transaction_buf, num_topics * aws_mqtt_topic_tree_action_size);
    struct aws_array_list transaction;
    aws_array_list_init_static(&transaction, transaction_buf, num_topics, aws_mqtt_topic_tree_action_size);

    if (!task_arg->tree_updated) {

        struct subscribe_task_topic *topic;
        if (aws_mqtt_topic_tree_transaction_remove(
                &task_arg->connection->subscriptions, &transaction, &task_arg->filter, (void **)&topic)) {
            goto handle_error;
        }

        task_arg->is_local = topic ? topic->is_local : false;
    }

    if (!task_arg->is_local) {
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
            goto handle_error;
        }

        if (aws_channel_slot_send_message(task_arg->connection->slot, message, AWS_CHANNEL_DIR_WRITE)) {
            goto handle_error;
        }
    }

    if (!task_arg->tree_updated) {
        aws_mqtt_topic_tree_transaction_commit(&task_arg->connection->subscriptions, &transaction);
        task_arg->tree_updated = true;
    }

    aws_array_list_clean_up(&transaction);
    /* If the subscribe is local-only, don't wait for a SUBACK to come back. */
    return task_arg->is_local ? AWS_MQTT_CLIENT_REQUEST_COMPLETE : AWS_MQTT_CLIENT_REQUEST_ONGOING;

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

    AWS_LOGF_DEBUG(AWS_LS_MQTT_CLIENT, "id=%p: Unsubscribe %" PRIu16 " complete", (void *)connection, packet_id);

    if (task_arg->on_unsuback) {
        task_arg->on_unsuback(connection, packet_id, error_code, task_arg->on_unsuback_ud);
    }

    aws_string_destroy(task_arg->filter_string);
    aws_mqtt_packet_unsubscribe_clean_up(&task_arg->unsubscribe);
    aws_mem_release(task_arg->connection->allocator, task_arg);
}

uint16_t aws_mqtt_client_connection_unsubscribe(
    struct aws_mqtt_client_connection *connection,
    const struct aws_byte_cursor *topic_filter,
    aws_mqtt_op_complete_fn *on_unsuback,
    void *on_unsuback_ud) {

    AWS_PRECONDITION(connection);

    if (!aws_mqtt_is_valid_topic_filter(topic_filter)) {
        aws_raise_error(AWS_ERROR_MQTT_INVALID_TOPIC);
        return 0;
    }

    struct unsubscribe_task_arg *task_arg =
        aws_mem_calloc(connection->allocator, 1, sizeof(struct unsubscribe_task_arg));
    if (!task_arg) {
        return 0;
    }

    task_arg->connection = connection;
    task_arg->filter_string = aws_string_new_from_array(connection->allocator, topic_filter->ptr, topic_filter->len);
    task_arg->filter = aws_byte_cursor_from_string(task_arg->filter_string);
    task_arg->on_unsuback = on_unsuback;
    task_arg->on_unsuback_ud = on_unsuback_ud;

    uint16_t packet_id =
        mqtt_create_request(connection, &s_unsubscribe_send, task_arg, s_unsubscribe_complete, task_arg);

    AWS_LOGF_DEBUG(AWS_LS_MQTT_CLIENT, "id=%p: Starting unsubscribe %" PRIu16, (void *)connection, packet_id);

    return packet_id;
}

/*******************************************************************************
 * Publish
 ******************************************************************************/

struct publish_task_arg {
    struct aws_mqtt_client_connection *connection;
    struct aws_string *topic_string;
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

    AWS_LOGF_TRACE(
        AWS_LS_MQTT_CLIENT,
        "id=%p: Attempting send of publish %" PRIu16 " %s",
        (void *)task_arg->connection,
        message_id,
        is_first_attempt ? "first attempt" : "resend");

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

        if (to_write) {
            /* Write this chunk */
            struct aws_byte_cursor to_write_cur = aws_byte_cursor_advance(&payload_cur, to_write);
            AWS_ASSERT(to_write_cur.ptr); /* to_write is guaranteed to be inside the bounds of payload_cur */
            if (!aws_byte_buf_write_from_whole_cursor(&message->message_data, to_write_cur)) {

                aws_mem_release(message->allocator, message);
                return AWS_MQTT_CLIENT_REQUEST_ERROR;
            }
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

    AWS_LOGF_DEBUG(AWS_LS_MQTT_CLIENT, "id=%p: Publish %" PRIu16 " complete", (void *)connection, packet_id);

    if (task_arg->on_complete) {
        task_arg->on_complete(connection, packet_id, error_code, task_arg->userdata);
    }

    aws_string_destroy(task_arg->topic_string);
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

    AWS_PRECONDITION(connection);

    if (!aws_mqtt_is_valid_topic(topic)) {
        aws_raise_error(AWS_ERROR_MQTT_INVALID_TOPIC);
        return 0;
    }

    struct publish_task_arg *arg = aws_mem_calloc(connection->allocator, 1, sizeof(struct publish_task_arg));
    if (!arg) {
        return 0;
    }

    arg->connection = connection;
    arg->topic_string = aws_string_new_from_array(connection->allocator, topic->ptr, topic->len);
    arg->topic = aws_byte_cursor_from_string(arg->topic_string);
    arg->qos = qos;
    arg->retain = retain;
    arg->payload = *payload;

    arg->on_complete = on_complete;
    arg->userdata = userdata;

    uint16_t packet_id = mqtt_create_request(connection, &s_publish_send, arg, &s_publish_complete, arg);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CLIENT,
        "id=%p: Starting publish %" PRIu16 " to topic " PRInSTR,
        (void *)connection,
        packet_id,
        AWS_BYTE_CURSOR_PRI(*topic));

    if (packet_id) {
        return packet_id;
    }

    /* bummer, we failed to make a new request */

    /* we know arg is valid, topic_string may or may not be valid */
    if (arg->topic_string) {
        aws_string_destroy(arg->topic_string);
    }

    aws_mem_release(connection->allocator, arg);

    return 0;
}

/*******************************************************************************
 * Ping
 ******************************************************************************/

static enum aws_mqtt_client_request_state s_pingreq_send(uint16_t message_id, bool is_first_attempt, void *userdata) {
    (void)message_id;

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
            aws_mem_release(message->allocator, message);
            return AWS_MQTT_CLIENT_REQUEST_ERROR;
        }

        if (aws_channel_slot_send_message(connection->slot, message, AWS_CHANNEL_DIR_WRITE)) {
            aws_mem_release(message->allocator, message);
            return AWS_MQTT_CLIENT_REQUEST_ERROR;
        }

        /* Mark down that now is when the last pingreq was sent */
        connection->waiting_on_ping_response = true;

        return AWS_MQTT_CLIENT_REQUEST_ONGOING;
    }

    /* Check that a pingresp has been received since pingreq was sent */
    if (connection->waiting_on_ping_response) {
        connection->waiting_on_ping_response = false;
        /* It's been too long since the last ping, close the connection */
        AWS_LOGF_ERROR(AWS_LS_MQTT_CLIENT, "id=%p: ping timeout detected", (void *)connection);
        aws_channel_shutdown(connection->slot->channel, AWS_ERROR_MQTT_TIMEOUT);
    }

    return AWS_MQTT_CLIENT_REQUEST_COMPLETE;
}

int aws_mqtt_client_connection_ping(struct aws_mqtt_client_connection *connection) {

    AWS_LOGF_DEBUG(AWS_LS_MQTT_CLIENT, "id=%p: Starting ping", (void *)connection);

    uint16_t packet_id = mqtt_create_request(connection, &s_pingreq_send, connection, NULL, NULL);

    AWS_LOGF_DEBUG(AWS_LS_MQTT_CLIENT, "id=%p: Starting ping with packet id %" PRIu16, (void *)connection, packet_id);

    return (packet_id > 0) ? AWS_OP_SUCCESS : AWS_OP_ERR;
}
