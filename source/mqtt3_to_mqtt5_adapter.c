/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>
#include <aws/mqtt/private/client_impl_shared.h>
#include <aws/mqtt/private/v5/mqtt5_client_impl.h>
#include <aws/mqtt/v5/mqtt5_listener.h>

enum aws_mqtt5_adapter_state {
    AWS_MQTT5_AS_ENABLED,
    AWS_MQTT5_AS_DISABLED,
};

struct aws_mqtt_client_connection_5_impl {
    struct aws_allocator *allocator;

    struct aws_mqtt_client_connection base;

    struct aws_mqtt5_client *client;
    struct aws_mqtt5_listener *listener;
    struct aws_event_loop *loop;

    struct aws_mutex lock;

    struct {
        enum aws_mqtt5_adapter_state state;
        uint64_t ref_count;
    } synced_data;

    /* 311 interface callbacks */
    aws_mqtt_client_on_connection_interrupted_fn *on_interrupted;
    void *on_interrupted_user_data;

    aws_mqtt_client_on_connection_resumed_fn *on_resumed;
    void *on_resumed_user_data;

    aws_mqtt_client_on_connection_closed_fn *on_closed;
    void *on_closed_user_data;

    aws_mqtt_client_publish_received_fn *on_any_publish;
    void *on_any_publish_user_data;

    aws_mqtt_transform_websocket_handshake_fn *websocket_handshake_transformer;
    void *websocket_handshake_transformer_user_data;

    aws_mqtt5_transform_websocket_handshake_complete_fn *mqtt5_websocket_handshake_completion_function;
    void *mqtt5_websocket_handshake_completion_user_data;
};

static void s_aws_mqtt5_client_connection_event_callback_adapter(const struct aws_mqtt5_client_lifecycle_event *event) {
    (void)event;
}

static bool s_aws_mqtt5_listener_publish_received_adapter(
    const struct aws_mqtt5_packet_publish_view *publish,
    void *user_data) {
    (void)publish;
    (void)user_data;

    return false;
}

static void s_mqtt_client_connection_5_impl_finish_destroy(void *context) {
    struct aws_mqtt_client_connection_5_impl *adapter = context;

    if (adapter->client->config->websocket_handshake_transform_user_data == adapter) {
        /*
         * If the mqtt5 client is pointing to us for websocket transform, then erase that.  The callback
         * is invoked from our pinned event loop so this is safe.
         */
        adapter->client->config->websocket_handshake_transform = NULL;
        adapter->client->config->websocket_handshake_transform_user_data = NULL;
    }

    adapter->client = aws_mqtt5_client_release(adapter->client);
    aws_mutex_clean_up(&adapter->lock);

    aws_mem_release(adapter->allocator, adapter);
}

struct aws_mqtt_set_interruption_handlers_task {
    struct aws_task task;
    struct aws_allocator *allocator;
    struct aws_mqtt_client_connection *connection;

    aws_mqtt_client_on_connection_interrupted_fn *on_interrupted;
    void *on_interrupted_user_data;

    aws_mqtt_client_on_connection_resumed_fn *on_resumed;
    void *on_resumed_user_data;
};

static void s_set_interruption_handlers_task_fn(struct aws_task *task, void *arg, enum aws_task_status status) {
    (void)task;

    struct aws_mqtt_set_interruption_handlers_task *set_task = arg;
    if (status != AWS_TASK_STATUS_RUN_READY) {
        goto done;
    }

    struct aws_mqtt_client_connection_5_impl *connection = set_task->connection->impl;

    connection->on_interrupted = set_task->on_interrupted;
    connection->on_interrupted_user_data = set_task->on_interrupted_user_data;
    connection->on_resumed = set_task->on_resumed;
    connection->on_resumed_user_data = set_task->on_resumed_user_data;

done:

    aws_mqtt_client_connection_release(set_task->connection);

    aws_mem_release(set_task->allocator, set_task);
}

static struct aws_mqtt_set_interruption_handlers_task *s_aws_mqtt_set_interruption_handlers_task_new(
    struct aws_allocator *allocator,
    struct aws_mqtt_client_connection_5_impl *connection,
    aws_mqtt_client_on_connection_interrupted_fn *on_interrupted,
    void *on_interrupted_user_data,
    aws_mqtt_client_on_connection_resumed_fn *on_resumed,
    void *on_resumed_user_data) {

    struct aws_mqtt_set_interruption_handlers_task *set_task =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_set_interruption_handlers_task));

    aws_task_init(
        &set_task->task, s_set_interruption_handlers_task_fn, (void *)set_task, "SetInterruptionHandlersTask");
    set_task->allocator = connection->allocator;
    set_task->connection = aws_mqtt_client_connection_acquire(&connection->base);
    set_task->on_interrupted = on_interrupted;
    set_task->on_interrupted_user_data = on_interrupted_user_data;
    set_task->on_resumed = on_resumed;
    set_task->on_resumed_user_data = on_resumed_user_data;

    return set_task;
}

static int s_aws_mqtt_client_connection_5_set_interruption_handlers(
    void *impl,
    aws_mqtt_client_on_connection_interrupted_fn *on_interrupted,
    void *on_interrupted_user_data,
    aws_mqtt_client_on_connection_resumed_fn *on_resumed,
    void *on_resumed_user_data) {
    struct aws_mqtt_client_connection_5_impl *connection = impl;

    struct aws_mqtt_set_interruption_handlers_task *task = s_aws_mqtt_set_interruption_handlers_task_new(
        connection->allocator, connection, on_interrupted, on_interrupted_user_data, on_resumed, on_resumed_user_data);
    if (task == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT_CLIENT, "id=%p: failed to create set interruption handlers task", (void *)connection);
        return AWS_OP_ERR;
    }

    aws_event_loop_schedule_task_now(connection->loop, &task->task);

    return AWS_OP_SUCCESS;
}

struct aws_mqtt_set_on_closed_handler_task {
    struct aws_task task;
    struct aws_allocator *allocator;
    struct aws_mqtt_client_connection *connection;

    aws_mqtt_client_on_connection_closed_fn *on_closed;
    void *on_closed_user_data;
};

static void s_set_on_closed_handler_task_fn(struct aws_task *task, void *arg, enum aws_task_status status) {
    (void)task;

    struct aws_mqtt_set_on_closed_handler_task *set_task = arg;
    if (status != AWS_TASK_STATUS_RUN_READY) {
        goto done;
    }

    struct aws_mqtt_client_connection_5_impl *connection = set_task->connection->impl;

    connection->on_closed = set_task->on_closed;
    connection->on_closed_user_data = set_task->on_closed_user_data;

done:

    aws_mqtt_client_connection_release(set_task->connection);

    aws_mem_release(set_task->allocator, set_task);
}

static struct aws_mqtt_set_on_closed_handler_task *s_aws_mqtt_set_on_closed_handler_task_new(
    struct aws_allocator *allocator,
    struct aws_mqtt_client_connection_5_impl *connection,
    aws_mqtt_client_on_connection_closed_fn *on_closed,
    void *on_closed_user_data) {

    struct aws_mqtt_set_on_closed_handler_task *set_task =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_set_on_closed_handler_task));

    aws_task_init(&set_task->task, s_set_on_closed_handler_task_fn, (void *)set_task, "SetOnClosedHandlerTask");
    set_task->allocator = connection->allocator;
    set_task->connection = aws_mqtt_client_connection_acquire(&connection->base);
    set_task->on_closed = on_closed;
    set_task->on_closed_user_data = on_closed_user_data;

    return set_task;
}

static int s_aws_mqtt_client_connection_5_set_on_closed_handler(
    void *impl,
    aws_mqtt_client_on_connection_closed_fn *on_closed,
    void *on_closed_user_data) {
    struct aws_mqtt_client_connection_5_impl *connection = impl;

    struct aws_mqtt_set_on_closed_handler_task *task =
        s_aws_mqtt_set_on_closed_handler_task_new(connection->allocator, connection, on_closed, on_closed_user_data);
    if (task == NULL) {
        AWS_LOGF_ERROR(AWS_LS_MQTT_CLIENT, "id=%p: failed to create set on closed handler task", (void *)connection);
        return AWS_OP_ERR;
    }

    aws_event_loop_schedule_task_now(connection->loop, &task->task);

    return AWS_OP_SUCCESS;
}

struct aws_mqtt_set_on_any_publish_handler_task {
    struct aws_task task;
    struct aws_allocator *allocator;
    struct aws_mqtt_client_connection *connection;

    aws_mqtt_client_publish_received_fn *on_any_publish;
    void *on_any_publish_user_data;
};

static void s_set_on_any_publish_handler_task_fn(struct aws_task *task, void *arg, enum aws_task_status status) {
    (void)task;

    struct aws_mqtt_set_on_any_publish_handler_task *set_task = arg;
    if (status != AWS_TASK_STATUS_RUN_READY) {
        goto done;
    }

    struct aws_mqtt_client_connection_5_impl *connection = set_task->connection->impl;

    connection->on_any_publish = set_task->on_any_publish;
    connection->on_any_publish_user_data = set_task->on_any_publish_user_data;

done:

    aws_mqtt_client_connection_release(set_task->connection);

    aws_mem_release(set_task->allocator, set_task);
}

static struct aws_mqtt_set_on_any_publish_handler_task *s_aws_mqtt_set_on_any_publish_handler_task_new(
    struct aws_allocator *allocator,
    struct aws_mqtt_client_connection_5_impl *connection,
    aws_mqtt_client_publish_received_fn *on_any_publish,
    void *on_any_publish_user_data) {

    struct aws_mqtt_set_on_any_publish_handler_task *set_task =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_set_on_any_publish_handler_task));

    aws_task_init(
        &set_task->task, s_set_on_any_publish_handler_task_fn, (void *)set_task, "SetOnAnyPublishHandlerTask");
    set_task->allocator = connection->allocator;
    set_task->connection = aws_mqtt_client_connection_acquire(&connection->base);
    set_task->on_any_publish = on_any_publish;
    set_task->on_any_publish_user_data = on_any_publish_user_data;

    return set_task;
}

static int s_aws_mqtt_client_connection_5_set_on_any_publish_handler(
    void *impl,
    aws_mqtt_client_publish_received_fn *on_any_publish,
    void *on_any_publish_user_data) {
    struct aws_mqtt_client_connection_5_impl *connection = impl;

    struct aws_mqtt_set_on_any_publish_handler_task *task = s_aws_mqtt_set_on_any_publish_handler_task_new(
        connection->allocator, connection, on_any_publish, on_any_publish_user_data);
    if (task == NULL) {
        AWS_LOGF_ERROR(AWS_LS_MQTT_CLIENT, "id=%p: failed to create set on any publish task", (void *)connection);
        return AWS_OP_ERR;
    }

    aws_event_loop_schedule_task_now(connection->loop, &task->task);

    return AWS_OP_SUCCESS;
}

struct aws_mqtt_set_reconnect_timeout_task {
    struct aws_task task;
    struct aws_allocator *allocator;
    struct aws_mqtt_client_connection *connection;

    uint64_t min_timeout;
    uint64_t max_timeout;
};

static void s_set_reconnect_timeout_task_fn(struct aws_task *task, void *arg, enum aws_task_status status) {
    (void)task;

    struct aws_mqtt_set_reconnect_timeout_task *set_task = arg;
    if (status != AWS_TASK_STATUS_RUN_READY) {
        goto done;
    }

    struct aws_mqtt_client_connection_5_impl *connection = set_task->connection->impl;

    /* we're in the mqtt5 client's event loop; it's safe to access internal state */
    connection->client->config->min_reconnect_delay_ms = set_task->min_timeout;
    connection->client->config->max_reconnect_delay_ms = set_task->max_timeout;
    connection->client->current_reconnect_delay_ms = set_task->min_timeout;

done:

    aws_mqtt_client_connection_release(set_task->connection);

    aws_mem_release(set_task->allocator, set_task);
}

static struct aws_mqtt_set_reconnect_timeout_task *s_aws_mqtt_set_reconnect_timeout_task_new(
    struct aws_allocator *allocator,
    struct aws_mqtt_client_connection_5_impl *connection,
    uint64_t min_timeout,
    uint64_t max_timeout) {

    struct aws_mqtt_set_reconnect_timeout_task *set_task =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_set_reconnect_timeout_task));

    aws_task_init(&set_task->task, s_set_reconnect_timeout_task_fn, (void *)set_task, "SetReconnectTimeoutTask");
    set_task->allocator = connection->allocator;
    set_task->connection = aws_mqtt_client_connection_acquire(&connection->base);
    set_task->min_timeout = aws_min_u64(min_timeout, max_timeout);
    set_task->max_timeout = aws_max_u64(min_timeout, max_timeout);

    return set_task;
}

static int s_aws_mqtt_client_connection_5_set_reconnect_timeout(
    void *impl,
    uint64_t min_timeout,
    uint64_t max_timeout) {
    struct aws_mqtt_client_connection_5_impl *connection = impl;

    struct aws_mqtt_set_reconnect_timeout_task *task =
        s_aws_mqtt_set_reconnect_timeout_task_new(connection->allocator, connection, min_timeout, max_timeout);
    if (task == NULL) {
        AWS_LOGF_ERROR(AWS_LS_MQTT_CLIENT, "id=%p: failed to create set reconnect timeout task", (void *)connection);
        return AWS_OP_ERR;
    }

    aws_event_loop_schedule_task_now(connection->loop, &task->task);

    return AWS_OP_SUCCESS;
}

struct aws_mqtt_set_http_proxy_options_task {
    struct aws_task task;
    struct aws_allocator *allocator;
    struct aws_mqtt_client_connection *connection;

    struct aws_http_proxy_config *proxy_config;
};

static void s_set_http_proxy_options_task_fn(struct aws_task *task, void *arg, enum aws_task_status status) {
    (void)task;

    struct aws_mqtt_set_http_proxy_options_task *set_task = arg;
    if (status != AWS_TASK_STATUS_RUN_READY) {
        goto done;
    }

    struct aws_mqtt_client_connection_5_impl *connection = set_task->connection->impl;

    /* we're in the mqtt5 client's event loop; it's safe to access internal state */
    aws_http_proxy_config_destroy(connection->client->config->http_proxy_config);

    /* move the proxy config from the set task to the client's config */
    connection->client->config->http_proxy_config = set_task->proxy_config;
    if (connection->client->config->http_proxy_config != NULL) {
        aws_http_proxy_options_init_from_config(
            &connection->client->config->http_proxy_options, connection->client->config->http_proxy_config);
    }

    /* don't clean up the proxy config if it was successfully assigned to the mqtt5 client */
    set_task->proxy_config = NULL;

done:

    aws_mqtt_client_connection_release(set_task->connection);

    /* If the task was canceled we need to clean this up because it didn't get assigned to the mqtt5 client */
    aws_http_proxy_config_destroy(set_task->proxy_config);

    aws_mem_release(set_task->allocator, set_task);
}

static struct aws_mqtt_set_http_proxy_options_task *s_aws_mqtt_set_http_proxy_options_task_new(
    struct aws_allocator *allocator,
    struct aws_mqtt_client_connection_5_impl *connection,
    struct aws_http_proxy_options *proxy_options) {

    struct aws_http_proxy_config *proxy_config =
        aws_http_proxy_config_new_tunneling_from_proxy_options(allocator, proxy_options);
    if (proxy_config == NULL) {
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        return NULL;
    }

    struct aws_mqtt_set_http_proxy_options_task *set_task =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_set_http_proxy_options_task));

    aws_task_init(&set_task->task, s_set_http_proxy_options_task_fn, (void *)set_task, "SetHttpProxyOptionsTask");
    set_task->allocator = connection->allocator;
    set_task->connection = aws_mqtt_client_connection_acquire(&connection->base);
    set_task->proxy_config = proxy_config;

    return set_task;
}

static int s_aws_mqtt_client_connection_5_set_http_proxy_options(
    void *impl,
    struct aws_http_proxy_options *proxy_options) {

    struct aws_mqtt_client_connection_5_impl *connection = impl;

    struct aws_mqtt_set_http_proxy_options_task *task =
        s_aws_mqtt_set_http_proxy_options_task_new(connection->allocator, connection, proxy_options);
    if (task == NULL) {
        AWS_LOGF_ERROR(AWS_LS_MQTT_CLIENT, "id=%p: failed to create set http proxy options task", (void *)connection);
        return AWS_OP_ERR;
    }

    aws_event_loop_schedule_task_now(connection->loop, &task->task);

    return AWS_OP_SUCCESS;
}

struct aws_mqtt_set_use_websockets_task {
    struct aws_task task;
    struct aws_allocator *allocator;
    struct aws_mqtt_client_connection *connection;

    aws_mqtt_transform_websocket_handshake_fn *transformer;
    void *transformer_user_data;
};

static void s_aws_mqtt5_adapter_websocket_handshake_completion_fn(
    struct aws_http_message *request,
    int error_code,
    void *complete_ctx) {

    struct aws_mqtt_client_connection_5_impl *adapter = complete_ctx;

    (*adapter->mqtt5_websocket_handshake_completion_function)(
        request, error_code, adapter->mqtt5_websocket_handshake_completion_user_data);

    aws_mqtt_client_connection_release(&adapter->base);
}

static void s_aws_mqtt5_adapter_transform_websocket_handshake_fn(
    struct aws_http_message *request,
    void *user_data,
    aws_mqtt5_transform_websocket_handshake_complete_fn *complete_fn,
    void *complete_ctx) {

    struct aws_mqtt_client_connection_5_impl *adapter = user_data;

    bool chain_callback = false;
    struct aws_http_message *completion_request = NULL;
    int completion_error_code = AWS_ERROR_SUCCESS;
    aws_mutex_lock(&adapter->lock);

    if (adapter->synced_data.state != AWS_MQTT5_AS_ENABLED) {
        completion_error_code = AWS_ERROR_MQTT5_USER_REQUESTED_STOP;
    } else if (adapter->websocket_handshake_transformer == NULL) {
        completion_request = request;
    } else {
        ++adapter->synced_data.ref_count;
        chain_callback = true;
    }

    aws_mutex_unlock(&adapter->lock);

    if (chain_callback) {
        adapter->mqtt5_websocket_handshake_completion_function = complete_fn;
        adapter->mqtt5_websocket_handshake_completion_user_data = complete_ctx;

        (*adapter->websocket_handshake_transformer)(
            request, user_data, s_aws_mqtt5_adapter_websocket_handshake_completion_fn, adapter);
    } else {
        (*complete_fn)(completion_request, completion_error_code, complete_ctx);
    }
}

static void s_set_use_websockets_task_fn(struct aws_task *task, void *arg, enum aws_task_status status) {
    (void)task;

    struct aws_mqtt_set_use_websockets_task *set_task = arg;
    if (status != AWS_TASK_STATUS_RUN_READY) {
        goto done;
    }

    struct aws_mqtt_client_connection_5_impl *connection = set_task->connection->impl;

    connection->websocket_handshake_transformer = set_task->transformer;
    connection->websocket_handshake_transformer_user_data = set_task->transformer_user_data;

    /* we're in the mqtt5 client's event loop; it's safe to access its internal state */
    connection->client->config->websocket_handshake_transform = s_aws_mqtt5_adapter_transform_websocket_handshake_fn;
    connection->client->config->websocket_handshake_transform_user_data = connection;

done:

    aws_mqtt_client_connection_release(set_task->connection);

    aws_mem_release(set_task->allocator, set_task);
}

static struct aws_mqtt_set_use_websockets_task *s_aws_mqtt_set_use_websockets_task_new(
    struct aws_allocator *allocator,
    struct aws_mqtt_client_connection_5_impl *connection,
    aws_mqtt_transform_websocket_handshake_fn *transformer,
    void *transformer_user_data) {

    struct aws_mqtt_set_use_websockets_task *set_task =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_set_use_websockets_task));

    aws_task_init(&set_task->task, s_set_use_websockets_task_fn, (void *)set_task, "SetUseWebsocketsTask");
    set_task->allocator = connection->allocator;
    set_task->connection = aws_mqtt_client_connection_acquire(&connection->base);
    set_task->transformer = transformer;
    set_task->transformer_user_data = transformer_user_data;

    return set_task;
}

static int s_aws_mqtt_client_connection_5_use_websockets(
    void *impl,
    aws_mqtt_transform_websocket_handshake_fn *transformer,
    void *transformer_user_data,
    aws_mqtt_validate_websocket_handshake_fn *validator,
    void *validator_user_data) {

    /* mqtt5 doesn't use these */
    (void)validator;
    (void)validator_user_data;

    struct aws_mqtt_client_connection_5_impl *connection = impl;

    struct aws_mqtt_set_use_websockets_task *task =
        s_aws_mqtt_set_use_websockets_task_new(connection->allocator, connection, transformer, transformer_user_data);
    if (task == NULL) {
        AWS_LOGF_ERROR(AWS_LS_MQTT_CLIENT, "id=%p: failed to create set use websockets task", (void *)connection);
        return AWS_OP_ERR;
    }

    aws_event_loop_schedule_task_now(connection->loop, &task->task);

    return AWS_OP_SUCCESS;
}

static int s_aws_mqtt_client_connection_5_set_host_resolution_options(
    void *impl,
    struct aws_host_resolution_config *host_resolution_config) {

    (void)impl;
    (void)host_resolution_config;

    /* No CRTs use this function */
    return aws_raise_error(AWS_ERROR_UNIMPLEMENTED);
}

struct aws_mqtt_set_will_task {
    struct aws_task task;
    struct aws_allocator *allocator;
    struct aws_mqtt_client_connection *connection;

    struct aws_byte_buf topic_buffer;
    enum aws_mqtt_qos qos;
    bool retain;
    struct aws_byte_buf payload_buffer;
};

static void s_aws_mqtt_set_will_task_destroy(struct aws_mqtt_set_will_task *task) {
    if (task == NULL) {
        return;
    }

    aws_byte_buf_clean_up(&task->topic_buffer);
    aws_byte_buf_clean_up(&task->payload_buffer);

    aws_mem_release(task->allocator, task);
}

static void s_set_will_task_fn(struct aws_task *task, void *arg, enum aws_task_status status) {
    (void)task;

    struct aws_mqtt_set_will_task *set_task = arg;
    if (status != AWS_TASK_STATUS_RUN_READY) {
        goto done;
    }

    struct aws_mqtt_client_connection_5_impl *connection = set_task->connection->impl;

    /* we're in the mqtt5 client's event loop; it's safe to access internal state */
    struct aws_mqtt5_packet_connect_storage *connect = connection->client->config->connect;

    /* clean up the old will if necessary */
    if (connect->will != NULL) {
        aws_mqtt5_packet_publish_storage_clean_up(connect->will);
        aws_mem_release(connect->allocator, connect->will);
        connect->will = NULL;
    }

    struct aws_mqtt5_packet_publish_view will = {
        .topic = aws_byte_cursor_from_buf(&set_task->topic_buffer),
        .qos = (enum aws_mqtt5_qos)set_task->qos,
        .retain = set_task->retain,
        .payload = aws_byte_cursor_from_buf(&set_task->payload_buffer),
    };

    /* make a new will */
    connect->will = aws_mem_calloc(connect->allocator, 1, sizeof(struct aws_mqtt5_packet_publish_storage));
    aws_mqtt5_packet_publish_storage_init(connect->will, connect->allocator, &will);

    /* manually update the storage view's will reference */
    connect->storage_view.will = &connect->will->storage_view;

done:

    aws_mqtt_client_connection_release(set_task->connection);

    s_aws_mqtt_set_will_task_destroy(set_task);
}

static struct aws_mqtt_set_will_task *s_aws_mqtt_set_will_task_new(
    struct aws_allocator *allocator,
    struct aws_mqtt_client_connection_5_impl *connection,
    const struct aws_byte_cursor *topic,
    enum aws_mqtt_qos qos,
    bool retain,
    const struct aws_byte_cursor *payload) {

    if (topic == NULL) {
        return NULL;
    }

    struct aws_mqtt_set_will_task *set_task = aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_set_will_task));

    aws_task_init(&set_task->task, s_set_will_task_fn, (void *)set_task, "SetWillTask");
    set_task->allocator = connection->allocator;
    set_task->connection = aws_mqtt_client_connection_acquire(&connection->base);

    set_task->qos = qos;
    set_task->retain = retain;
    aws_byte_buf_init_copy_from_cursor(&set_task->topic_buffer, allocator, *topic);
    if (payload != NULL) {
        aws_byte_buf_init_copy_from_cursor(&set_task->payload_buffer, allocator, *payload);
    }

    return set_task;
}

static int s_aws_mqtt_client_connection_5_set_will(
    void *impl,
    const struct aws_byte_cursor *topic,
    enum aws_mqtt_qos qos,
    bool retain,
    const struct aws_byte_cursor *payload) {

    struct aws_mqtt_client_connection_5_impl *connection = impl;

    struct aws_mqtt_set_will_task *task =
        s_aws_mqtt_set_will_task_new(connection->allocator, connection, topic, qos, retain, payload);
    if (task == NULL) {
        AWS_LOGF_ERROR(AWS_LS_MQTT_CLIENT, "id=%p: failed to create set will task", (void *)connection);
        return AWS_OP_ERR;
    }

    aws_event_loop_schedule_task_now(connection->loop, &task->task);

    return AWS_OP_SUCCESS;
}

struct aws_mqtt_set_login_task {
    struct aws_task task;
    struct aws_allocator *allocator;
    struct aws_mqtt_client_connection *connection;

    struct aws_byte_buf username_buffer;
    struct aws_byte_buf password_buffer;
};

static void s_aws_mqtt_set_login_task_destroy(struct aws_mqtt_set_login_task *task) {
    if (task == NULL) {
        return;
    }

    aws_byte_buf_clean_up_secure(&task->username_buffer);
    aws_byte_buf_clean_up_secure(&task->password_buffer);

    aws_mem_release(task->allocator, task);
}

static void s_set_login_task_fn(struct aws_task *task, void *arg, enum aws_task_status status) {
    (void)task;

    struct aws_mqtt_set_login_task *set_task = arg;
    if (status != AWS_TASK_STATUS_RUN_READY) {
        goto done;
    }

    struct aws_mqtt_client_connection_5_impl *connection = set_task->connection->impl;
    struct aws_byte_cursor username_cursor = aws_byte_cursor_from_buf(&set_task->username_buffer);
    struct aws_byte_cursor password_cursor = aws_byte_cursor_from_buf(&set_task->password_buffer);

    /* we're in the mqtt5 client's event loop; it's safe to access internal state */
    struct aws_mqtt5_packet_connect_storage *old_connect = connection->client->config->connect;

    /*
     * Packet storage stores binary data in a single buffer.  The safest way to replace some binary data is
     * to make a new storage from the old storage, deleting the old storage after construction is complete.
     */
    struct aws_mqtt5_packet_connect_view new_connect_view = old_connect->storage_view;

    if (set_task->username_buffer.len > 0) {
        new_connect_view.username = &username_cursor;
    } else {
        new_connect_view.username = NULL;
    }

    if (set_task->password_buffer.len > 0) {
        new_connect_view.password = &password_cursor;
    } else {
        new_connect_view.password = NULL;
    }

    if (aws_mqtt5_packet_connect_view_validate(&new_connect_view)) {
        goto done;
    }

    struct aws_mqtt5_packet_connect_storage *new_connect =
        aws_mem_calloc(connection->allocator, 1, sizeof(struct aws_mqtt5_packet_connect_storage));
    aws_mqtt5_packet_connect_storage_init(new_connect, connection->allocator, &new_connect_view);

    connection->client->config->connect = new_connect;
    aws_mqtt5_packet_connect_storage_clean_up(old_connect);
    aws_mem_release(old_connect->allocator, old_connect);

done:

    aws_mqtt_client_connection_release(set_task->connection);

    s_aws_mqtt_set_login_task_destroy(set_task);
}

static struct aws_mqtt_set_login_task *s_aws_mqtt_set_login_task_new(
    struct aws_allocator *allocator,
    struct aws_mqtt_client_connection_5_impl *connection,
    const struct aws_byte_cursor *username,
    const struct aws_byte_cursor *password) {

    struct aws_mqtt_set_login_task *set_task = aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_set_login_task));

    aws_task_init(&set_task->task, s_set_login_task_fn, (void *)set_task, "SetLoginTask");
    set_task->allocator = connection->allocator;
    set_task->connection = aws_mqtt_client_connection_acquire(&connection->base);

    if (username != NULL) {
        aws_byte_buf_init_copy_from_cursor(&set_task->username_buffer, allocator, *username);
    }

    if (password != NULL) {
        aws_byte_buf_init_copy_from_cursor(&set_task->password_buffer, allocator, *password);
    }

    return set_task;
}

static int s_aws_mqtt_client_connection_5_set_login(
    void *impl,
    const struct aws_byte_cursor *username,
    const struct aws_byte_cursor *password) {

    struct aws_mqtt_client_connection_5_impl *connection = impl;

    struct aws_mqtt_set_login_task *task =
        s_aws_mqtt_set_login_task_new(connection->allocator, connection, username, password);
    if (task == NULL) {
        AWS_LOGF_ERROR(AWS_LS_MQTT_CLIENT, "id=%p: failed to create set login task", (void *)connection);
        return AWS_OP_ERR;
    }

    aws_event_loop_schedule_task_now(connection->loop, &task->task);

    return AWS_OP_SUCCESS;
}

static struct aws_mqtt_client_connection *s_aws_mqtt_client_connection_5_acquire(void *impl) {
    struct aws_mqtt_client_connection_5_impl *adapter = impl;

    aws_mutex_lock(&adapter->lock);
    AWS_FATAL_ASSERT(adapter->synced_data.ref_count > 0);
    ++adapter->synced_data.ref_count;
    aws_mutex_unlock(&adapter->lock);

    return &adapter->base;
}

/*
 * The lock is held during callbacks to prevent invoking into something that is in the process of
 * destruction.  In general this isn't a performance worry since callbacks are invoked from a single
 * thread: the event loop that the client and adapter are seated on.
 *
 * But since we don't have recursive mutexes on all platforms, we need to be careful about the shutdown
 * process since if we naively always locked, then an adapter release inside a callback would deadlock.
 *
 * On the surface, it seems reasonable that if we're in the event loop thread we could just skip
 * locking entirely (because we've already locked it at the start of the callback).  Unfortunately, this isn't safe
 * because we don't actually truly know we're in our mqtt5 client's callback; we could be in some other
 * client/connection's callback that happens to be seated on the same event loop.  And while it's true that because
 * of the thread seating, nothing will be interfering with our shared state manipulation, there's one final
 * consideration which forces us to *try* to lock:
 *
 * Dependent on the memory model of the CPU architecture, changes to shared state, even if "safe" from data
 * races across threads, may not become visible to other cores on the same CPU unless some kind of synchronization
 * primitive (memory barrier) is invoked.  So in this extremely unlikely case, we use try-lock to guarantee that a
 * synchronization primitive is invoked when disable is coming through a callback from something else on the same
 * event loop.
 *
 * In the case that we're in our mqtt5 client's callback, the lock is already held, try fails, and the unlock at
 * the end of the callback will suffice for cache flush and synchronization.
 *
 * In the case that we're in something else's callback on the same thread, the try succeeds and its followup
 * unlock here will suffice for cache flush and synchronization.
 *
 * Some after-the-fact analysis hints that this extra step (in the case where we are in the event loop) may
 * be unnecessary because the only reader of the state change is the event loop thread itself.  I don't feel
 * confident enough in the memory semantics of thread<->CPU core bindings to relax this though.
 */
static void s_aws_mqtt_client_connection_5_release(void *impl) {
    struct aws_mqtt_client_connection_5_impl *adapter = impl;

    bool start_shutdown = false;
    bool lock_succeeded = aws_mutex_try_lock(&adapter->lock) == AWS_OP_SUCCESS;

    AWS_FATAL_ASSERT(adapter->synced_data.ref_count > 0);
    --adapter->synced_data.ref_count;
    if (adapter->synced_data.ref_count == 0) {
        adapter->synced_data.state = AWS_MQTT5_AS_DISABLED;
        start_shutdown = true;
    }

    if (lock_succeeded) {
        aws_mutex_unlock(&adapter->lock);
    }

    if (start_shutdown) {
        /*
         * When the adapter's ref count goes to zero, here's what we want to do:
         *
         *  (1) Put the adapter into the disabled mode, which tells it to stop processing callbacks from the mqtt5
         *      client
         *  (2) Release the client listener, starting its asynchronous shutdown process (since we're the only user
         *      of it)
         *  (3) Wait for the client listener to notify us that asynchronous shutdown is over.  At this point we
         *      are guaranteed that no more callbacks from the mqtt5 client will reach us.  We can safely release the
         *      mqtt5 client.
         *  (4) Synchronously clean up all further resources.
         *
         *  Step (1) was done within the lock above.
         *  Step (2) is done here.
         *  Steps (3) and (4) are accomplished via s_mqtt_client_connection_5_impl_finish_destroy.
         */
        aws_mqtt5_listener_release(adapter->listener);
    }
}

static struct aws_mqtt_client_connection_vtable s_aws_mqtt_client_connection_5_vtable = {
    .acquire_fn = s_aws_mqtt_client_connection_5_acquire,
    .release_fn = s_aws_mqtt_client_connection_5_release,
    .set_will_fn = s_aws_mqtt_client_connection_5_set_will,
    .set_login_fn = s_aws_mqtt_client_connection_5_set_login,
    .use_websockets_fn = s_aws_mqtt_client_connection_5_use_websockets,
    .set_http_proxy_options_fn = s_aws_mqtt_client_connection_5_set_http_proxy_options,
    .set_host_resolution_options_fn = s_aws_mqtt_client_connection_5_set_host_resolution_options,
    .set_reconnect_timeout_fn = s_aws_mqtt_client_connection_5_set_reconnect_timeout,
    .set_connection_interruption_handlers_fn = s_aws_mqtt_client_connection_5_set_interruption_handlers,
    .set_connection_closed_handler_fn = s_aws_mqtt_client_connection_5_set_on_closed_handler,
    .set_on_any_publish_handler_fn = s_aws_mqtt_client_connection_5_set_on_any_publish_handler,
    .connect_fn = NULL,
    .reconnect_fn = NULL,
    .disconnect_fn = NULL,
    .subscribe_multiple_fn = NULL,
    .subscribe_fn = NULL,
    .subscribe_local_fn = NULL,
    .resubscribe_existing_topics_fn = NULL,
    .unsubscribe_fn = NULL,
    .publish_fn = NULL,
    .get_stats_fn = NULL,
};

static struct aws_mqtt_client_connection_vtable *s_aws_mqtt_client_connection_5_vtable_ptr =
    &s_aws_mqtt_client_connection_5_vtable;

struct aws_mqtt_client_connection *aws_mqtt_client_connection_new_from_mqtt5_client(struct aws_mqtt5_client *client) {
    struct aws_allocator *allocator = client->allocator;
    struct aws_mqtt_client_connection_5_impl *adapter =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_client_connection_5_impl));

    adapter->allocator = allocator;

    adapter->base.vtable = s_aws_mqtt_client_connection_5_vtable_ptr;
    adapter->base.impl = adapter;

    adapter->client = aws_mqtt5_client_acquire(client);
    adapter->loop = client->loop;

    aws_mutex_init(&adapter->lock);

    /*
     * We start disabled to handle the case where someone passes in an mqtt5 client that is already "live."
     * We'll enable the adapter as soon as they try to connect via the 311 interface.  This
     * also ties in to how we simulate the 311 implementation's don't-reconnect-if-initial-connect-fails logic.
     * The 5 client will continue to try and reconnect, but the adapter will go disabled making it seem to the 311
     * user that is is offline.
     */
    adapter->synced_data.state = AWS_MQTT5_AS_DISABLED;
    adapter->synced_data.ref_count = 1;

    struct aws_mqtt5_listener_config listener_config = {
        .client = client,
        .listener_callbacks =
            {
                .listener_publish_received_handler = s_aws_mqtt5_listener_publish_received_adapter,
                .listener_publish_received_handler_user_data = adapter,
                .lifecycle_event_handler = s_aws_mqtt5_client_connection_event_callback_adapter,
                .lifecycle_event_handler_user_data = adapter,
            },
        .termination_callback = s_mqtt_client_connection_5_impl_finish_destroy,
        .termination_callback_user_data = adapter,
    };
    adapter->listener = aws_mqtt5_listener_new(allocator, &listener_config);

    return &adapter->base;
}
