/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

#include <aws/common/rw_lock.h>
#include <aws/mqtt/private/client_impl_shared.h>
#include <aws/mqtt/private/v5/mqtt5_client_impl.h>
#include <aws/mqtt/v5/mqtt5_listener.h>

/*
 * The adapter maintains a notion of state based on how its 311 API has been used.  This state guides how it handles
 * external lifecycle events.
 *
 * Operational events are always relayed unless the adapter has been terminated.
 */
enum aws_mqtt_adapter_state {

    /*
     * The 311 API has had connect() called but that connect has not yet resolved.
     *
     * If it resolves successfully we will move to the STAY_CONNECTED state which will relay lifecycle callbacks
     * transparently.
     *
     * If it resolves unsuccessfully, we will move to the STAY_DISCONNECTED state where we will ignore lifecycle
     * events because, from the 311 API's perspective, nothing should be getting emitted.
     */
    AWS_MQTT_AS_FIRST_CONNECT,

    /*
     * A call to the 311 connect API has resolved successfully.  Relay all lifecycle events until told otherwise.
     */
    AWS_MQTT_AS_STAY_CONNECTED,

    /*
     * We have not observed a successful initial connection attempt via the 311 API (or disconnect has been
     * invoked afterwards).  Ignore all lifecycle events.
     */
    AWS_MQTT_AS_STAY_DISCONNECTED,
};

struct aws_mqtt_client_connection_5_impl {

    struct aws_allocator *allocator;

    struct aws_mqtt_client_connection base;

    struct aws_mqtt5_client *client;
    struct aws_mqtt5_listener *listener;
    struct aws_event_loop *loop;

    /*
     * An event-loop-internal flag that we can read to check to see if we're in the scope of a callback
     * that has already locked the adapter's lock.  Can only be referenced from the event loop thread.
     *
     * We use the flag to avoid deadlock in a few cases where we can re-enter the adapter logic from within a callback.
     * It also provides a nice solution for the fact that we cannot safely upgrade a read lock to a write lock.
     */
    bool in_synchronous_callback;

    /*
     * What state the mqtt311 interface wants to be in.  Always either AWS_MCS_CONNECTED or AWS_MCS_STOPPED.
     * Tracked separately from the underlying mqtt5 client state in order to provide a coherent view that is
     * narrowed solely to the adapter interface.
     */
    enum aws_mqtt_adapter_state adapter_state;

    /*
     * Tracks all references from external sources (ie users).  Incremented and decremented by the public
     * acquire/release APIs of the 311 connection.
     *
     * When this value drops to zero, the terminated flag is set and no further callbacks will be invoked.  This
     * also starts the asynchronous destruction process for the adapter.
     */
    struct aws_ref_count external_refs;

    /*
     * Tracks all references to the adapter from internal sources (temporary async processes that need the
     * adapter to stay alive for an interval of time, like sending tasks across thread boundaries).
     *
     * Starts with a single reference that is held until the adapter's listener has fully detached from the mqtt5
     * client.
     *
     * Once the internal ref count drops to zero, the adapter may be destroyed synchronously.
     */
    struct aws_ref_count internal_refs;

    /*
     * We use the adapter lock to guarantee that we can synchronously sever all callbacks from the mqtt5 client even
     * though adapter shutdown is an asynchronous process.  This means the lock is held during callbacks which is a
     * departure from our normal usage patterns.  We prevent deadlock (due to logical re-entry) by using the
     * in_synchronous_callback flag.
     *
     * We hold a read lock when invoking callbacks and a write lock when setting terminated from false to true.
     */
    struct aws_rw_lock lock;

    /*
     * Synchronized data protected by the adapter lock.
     */
    struct {
        bool terminated;
    } synced_data;

    /* All fields after here are internal to the adapter event loop thread */

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

struct aws_mqtt_adapter_final_destroy_task {
    struct aws_task task;
    struct aws_allocator *allocator;
    struct aws_mqtt_client_connection *connection;
};

static void s_mqtt_adapter_final_destroy_task_fn(struct aws_task *task, void *arg, enum aws_task_status status) {
    (void)task;
    (void)status;

    struct aws_mqtt_adapter_final_destroy_task *destroy_task = arg;
    struct aws_mqtt_client_connection_5_impl *adapter = destroy_task->connection->impl;

    if (adapter->client->config->websocket_handshake_transform_user_data == adapter) {
        /*
         * If the mqtt5 client is pointing to us for websocket transform, then erase that.  The callback
         * is invoked from our pinned event loop so this is safe.
         *
         * TODO: It is possible that multiple adapters may have sequentially side-affected the websocket handshake.
         * For now, in that case, subsequent connection attempts will probably not succeed.
         */
        adapter->client->config->websocket_handshake_transform = NULL;
        adapter->client->config->websocket_handshake_transform_user_data = NULL;
    }

    adapter->client = aws_mqtt5_client_release(adapter->client);
    aws_rw_lock_clean_up(&adapter->lock);

    aws_mem_release(adapter->allocator, adapter);

    aws_mem_release(destroy_task->allocator, destroy_task);
}

static struct aws_mqtt_adapter_final_destroy_task *s_aws_mqtt_adapter_final_destroy_task_new(
    struct aws_allocator *allocator,
    struct aws_mqtt_client_connection_5_impl *adapter) {

    struct aws_mqtt_adapter_final_destroy_task *destroy_task =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_adapter_final_destroy_task));

    aws_task_init(
        &destroy_task->task, s_mqtt_adapter_final_destroy_task_fn, (void *)destroy_task, "MqttAdapterFinalDestroy");
    destroy_task->allocator = adapter->allocator;
    destroy_task->connection = &adapter->base; /* Do not acquire, we're at zero external and internal ref counts */

    return destroy_task;
}

static void s_aws_mqtt_adapter_final_destroy(struct aws_mqtt_client_connection_5_impl *adapter) {

    struct aws_mqtt_adapter_final_destroy_task *task =
        s_aws_mqtt_adapter_final_destroy_task_new(adapter->allocator, adapter);
    if (task == NULL) {
        AWS_LOGF_ERROR(AWS_LS_MQTT_CLIENT, "id=%p: failed to create adapter final destroy task", (void *)adapter);
        return;
    }

    aws_event_loop_schedule_task_now(adapter->loop, &task->task);
}

typedef int (*adapter_callback_fn)(struct aws_mqtt_client_connection_5_impl *adapter, void *context);

/*
 * The state/ref-count lock is held during synchronous callbacks to prevent invoking into something that is in the
 * process of destruction.  In general this isn't a performance worry since callbacks are invoked from a single thread:
 * the event loop that the client and adapter are seated on.
 *
 * But since we don't have recursive mutexes on all platforms, we need to be careful about the shutdown
 * process since if we naively always locked, then an adapter release from within a callback would deadlock.
 *
 * We need a way to tell if locking will result in a deadlock.  The specific case is invoking a synchronous
 * callback from the event loop that re-enters the adapter logic via releasing the connection.  We can recognize
 * this scenario by setting/clearing an internal flag (in_synchronous_callback) and checking it only if we're
 * in the event loop thread.  If it's true, we know we've already locked at the beginning of the synchronous callback
 * and we can safely skip locking, otherwise we must lock.
 *
 * This function gives us a helper for making these kinds of safe callbacks.  We use it in:
 *   (1) Releasing the connection
 *   (2) Websocket handshake transform
 *   (3) Making lifecycle and operation callbacks on the mqtt311 interface
 *
 * It works by
 *   (1) Correctly determining if locking would deadlock and skipping lock only in that case, otherwise locking
 *   (2) Invoke the callback
 *   (3) Unlock if we locked in step (1)
 *
 * It also properly sets/clears the in_synchronous_callback flag if we're in the event loop and are not in
 * a callback already.
 */
static int s_aws_mqtt5_adapter_perform_safe_callback(
    struct aws_mqtt_client_connection_5_impl *adapter,
    bool use_write_lock,
    adapter_callback_fn callback_fn,
    void *callback_user_data) {

    /* Step (1) - conditionally lock and manipulate the in_synchronous_callback flag */
    bool should_unlock = true;
    bool clear_synchronous_callback_flag = false;
    if (aws_event_loop_thread_is_callers_thread(adapter->loop)) {
        if (adapter->in_synchronous_callback) {
            should_unlock = false;
        } else {
            adapter->in_synchronous_callback = true;
            clear_synchronous_callback_flag = true;
        }
    }

    if (should_unlock) {
        if (use_write_lock) {
            aws_rw_lock_wlock(&adapter->lock);
        } else {
            aws_rw_lock_rlock(&adapter->lock);
        }
    }

    // Step (2) - perform the callback
    int result = (*callback_fn)(adapter, callback_user_data);

    // Step (3) - undo anything we did in step (1)
    if (should_unlock) {
        if (use_write_lock) {
            aws_rw_lock_wunlock(&adapter->lock);
        } else {
            aws_rw_lock_runlock(&adapter->lock);
        }
    }

    if (clear_synchronous_callback_flag) {
        adapter->in_synchronous_callback = false;
    }

    return result;
}

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

struct aws_mqtt_set_interruption_handlers_task {
    struct aws_task task;
    struct aws_allocator *allocator;
    struct aws_mqtt_client_connection_5_impl *adapter;

    aws_mqtt_client_on_connection_interrupted_fn *on_interrupted;
    void *on_interrupted_user_data;

    aws_mqtt_client_on_connection_resumed_fn *on_resumed;
    void *on_resumed_user_data;
};

static void s_set_interruption_handlers_task_fn(struct aws_task *task, void *arg, enum aws_task_status status) {
    (void)task;

    struct aws_mqtt_set_interruption_handlers_task *set_task = arg;
    struct aws_mqtt_client_connection_5_impl *adapter = set_task->adapter;

    if (status != AWS_TASK_STATUS_RUN_READY) {
        goto done;
    }

    adapter->on_interrupted = set_task->on_interrupted;
    adapter->on_interrupted_user_data = set_task->on_interrupted_user_data;
    adapter->on_resumed = set_task->on_resumed;
    adapter->on_resumed_user_data = set_task->on_resumed_user_data;

done:

    aws_ref_count_release(&adapter->internal_refs);

    aws_mem_release(set_task->allocator, set_task);
}

static struct aws_mqtt_set_interruption_handlers_task *s_aws_mqtt_set_interruption_handlers_task_new(
    struct aws_allocator *allocator,
    struct aws_mqtt_client_connection_5_impl *adapter,
    aws_mqtt_client_on_connection_interrupted_fn *on_interrupted,
    void *on_interrupted_user_data,
    aws_mqtt_client_on_connection_resumed_fn *on_resumed,
    void *on_resumed_user_data) {

    struct aws_mqtt_set_interruption_handlers_task *set_task =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_set_interruption_handlers_task));

    aws_task_init(
        &set_task->task, s_set_interruption_handlers_task_fn, (void *)set_task, "SetInterruptionHandlersTask");
    set_task->allocator = adapter->allocator;
    set_task->adapter = (struct aws_mqtt_client_connection_5_impl *)aws_ref_count_acquire(&adapter->internal_refs);
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
    struct aws_mqtt_client_connection_5_impl *adapter = impl;

    struct aws_mqtt_set_interruption_handlers_task *task = s_aws_mqtt_set_interruption_handlers_task_new(
        adapter->allocator, adapter, on_interrupted, on_interrupted_user_data, on_resumed, on_resumed_user_data);
    if (task == NULL) {
        AWS_LOGF_ERROR(AWS_LS_MQTT_CLIENT, "id=%p: failed to create set interruption handlers task", (void *)adapter);
        return AWS_OP_ERR;
    }

    aws_event_loop_schedule_task_now(adapter->loop, &task->task);

    return AWS_OP_SUCCESS;
}

struct aws_mqtt_set_on_closed_handler_task {
    struct aws_task task;
    struct aws_allocator *allocator;
    struct aws_mqtt_client_connection_5_impl *adapter;

    aws_mqtt_client_on_connection_closed_fn *on_closed;
    void *on_closed_user_data;
};

static void s_set_on_closed_handler_task_fn(struct aws_task *task, void *arg, enum aws_task_status status) {
    (void)task;

    struct aws_mqtt_set_on_closed_handler_task *set_task = arg;
    struct aws_mqtt_client_connection_5_impl *adapter = set_task->adapter;
    if (status != AWS_TASK_STATUS_RUN_READY) {
        goto done;
    }

    adapter->on_closed = set_task->on_closed;
    adapter->on_closed_user_data = set_task->on_closed_user_data;

done:

    aws_ref_count_release(&adapter->internal_refs);

    aws_mem_release(set_task->allocator, set_task);
}

static struct aws_mqtt_set_on_closed_handler_task *s_aws_mqtt_set_on_closed_handler_task_new(
    struct aws_allocator *allocator,
    struct aws_mqtt_client_connection_5_impl *adapter,
    aws_mqtt_client_on_connection_closed_fn *on_closed,
    void *on_closed_user_data) {

    struct aws_mqtt_set_on_closed_handler_task *set_task =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_set_on_closed_handler_task));

    aws_task_init(&set_task->task, s_set_on_closed_handler_task_fn, (void *)set_task, "SetOnClosedHandlerTask");
    set_task->allocator = adapter->allocator;
    set_task->adapter = (struct aws_mqtt_client_connection_5_impl *)aws_ref_count_acquire(&adapter->internal_refs);
    set_task->on_closed = on_closed;
    set_task->on_closed_user_data = on_closed_user_data;

    return set_task;
}

static int s_aws_mqtt_client_connection_5_set_on_closed_handler(
    void *impl,
    aws_mqtt_client_on_connection_closed_fn *on_closed,
    void *on_closed_user_data) {
    struct aws_mqtt_client_connection_5_impl *adapter = impl;

    struct aws_mqtt_set_on_closed_handler_task *task =
        s_aws_mqtt_set_on_closed_handler_task_new(adapter->allocator, adapter, on_closed, on_closed_user_data);
    if (task == NULL) {
        AWS_LOGF_ERROR(AWS_LS_MQTT_CLIENT, "id=%p: failed to create set on closed handler task", (void *)adapter);
        return AWS_OP_ERR;
    }

    aws_event_loop_schedule_task_now(adapter->loop, &task->task);

    return AWS_OP_SUCCESS;
}

struct aws_mqtt_set_on_any_publish_handler_task {
    struct aws_task task;
    struct aws_allocator *allocator;
    struct aws_mqtt_client_connection_5_impl *adapter;

    aws_mqtt_client_publish_received_fn *on_any_publish;
    void *on_any_publish_user_data;
};

static void s_set_on_any_publish_handler_task_fn(struct aws_task *task, void *arg, enum aws_task_status status) {
    (void)task;

    struct aws_mqtt_set_on_any_publish_handler_task *set_task = arg;
    struct aws_mqtt_client_connection_5_impl *adapter = set_task->adapter;
    if (status != AWS_TASK_STATUS_RUN_READY) {
        goto done;
    }

    adapter->on_any_publish = set_task->on_any_publish;
    adapter->on_any_publish_user_data = set_task->on_any_publish_user_data;

done:

    aws_ref_count_release(&adapter->internal_refs);

    aws_mem_release(set_task->allocator, set_task);
}

static struct aws_mqtt_set_on_any_publish_handler_task *s_aws_mqtt_set_on_any_publish_handler_task_new(
    struct aws_allocator *allocator,
    struct aws_mqtt_client_connection_5_impl *adapter,
    aws_mqtt_client_publish_received_fn *on_any_publish,
    void *on_any_publish_user_data) {

    struct aws_mqtt_set_on_any_publish_handler_task *set_task =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_set_on_any_publish_handler_task));

    aws_task_init(
        &set_task->task, s_set_on_any_publish_handler_task_fn, (void *)set_task, "SetOnAnyPublishHandlerTask");
    set_task->allocator = adapter->allocator;
    set_task->adapter = (struct aws_mqtt_client_connection_5_impl *)aws_ref_count_acquire(&adapter->internal_refs);
    set_task->on_any_publish = on_any_publish;
    set_task->on_any_publish_user_data = on_any_publish_user_data;

    return set_task;
}

static int s_aws_mqtt_client_connection_5_set_on_any_publish_handler(
    void *impl,
    aws_mqtt_client_publish_received_fn *on_any_publish,
    void *on_any_publish_user_data) {
    struct aws_mqtt_client_connection_5_impl *adapter = impl;

    struct aws_mqtt_set_on_any_publish_handler_task *task = s_aws_mqtt_set_on_any_publish_handler_task_new(
        adapter->allocator, adapter, on_any_publish, on_any_publish_user_data);
    if (task == NULL) {
        AWS_LOGF_ERROR(AWS_LS_MQTT_CLIENT, "id=%p: failed to create set on any publish task", (void *)adapter);
        return AWS_OP_ERR;
    }

    aws_event_loop_schedule_task_now(adapter->loop, &task->task);

    return AWS_OP_SUCCESS;
}

struct aws_mqtt_set_reconnect_timeout_task {
    struct aws_task task;
    struct aws_allocator *allocator;
    struct aws_mqtt_client_connection_5_impl *adapter;

    uint64_t min_timeout;
    uint64_t max_timeout;
};

static void s_set_reconnect_timeout_task_fn(struct aws_task *task, void *arg, enum aws_task_status status) {
    (void)task;

    struct aws_mqtt_set_reconnect_timeout_task *set_task = arg;
    struct aws_mqtt_client_connection_5_impl *adapter = set_task->adapter;
    if (status != AWS_TASK_STATUS_RUN_READY) {
        goto done;
    }

    /* we're in the mqtt5 client's event loop; it's safe to access internal state */
    adapter->client->config->min_reconnect_delay_ms = set_task->min_timeout;
    adapter->client->config->max_reconnect_delay_ms = set_task->max_timeout;
    adapter->client->current_reconnect_delay_ms = set_task->min_timeout;

done:

    aws_ref_count_release(&adapter->internal_refs);

    aws_mem_release(set_task->allocator, set_task);
}

static struct aws_mqtt_set_reconnect_timeout_task *s_aws_mqtt_set_reconnect_timeout_task_new(
    struct aws_allocator *allocator,
    struct aws_mqtt_client_connection_5_impl *adapter,
    uint64_t min_timeout,
    uint64_t max_timeout) {

    struct aws_mqtt_set_reconnect_timeout_task *set_task =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_set_reconnect_timeout_task));

    aws_task_init(&set_task->task, s_set_reconnect_timeout_task_fn, (void *)set_task, "SetReconnectTimeoutTask");
    set_task->allocator = adapter->allocator;
    set_task->adapter = (struct aws_mqtt_client_connection_5_impl *)aws_ref_count_acquire(&adapter->internal_refs);
    set_task->min_timeout = aws_min_u64(min_timeout, max_timeout);
    set_task->max_timeout = aws_max_u64(min_timeout, max_timeout);

    return set_task;
}

static int s_aws_mqtt_client_connection_5_set_reconnect_timeout(
    void *impl,
    uint64_t min_timeout,
    uint64_t max_timeout) {
    struct aws_mqtt_client_connection_5_impl *adapter = impl;

    struct aws_mqtt_set_reconnect_timeout_task *task =
        s_aws_mqtt_set_reconnect_timeout_task_new(adapter->allocator, adapter, min_timeout, max_timeout);
    if (task == NULL) {
        AWS_LOGF_ERROR(AWS_LS_MQTT_CLIENT, "id=%p: failed to create set reconnect timeout task", (void *)adapter);
        return AWS_OP_ERR;
    }

    aws_event_loop_schedule_task_now(adapter->loop, &task->task);

    return AWS_OP_SUCCESS;
}

struct aws_mqtt_set_http_proxy_options_task {
    struct aws_task task;
    struct aws_allocator *allocator;
    struct aws_mqtt_client_connection_5_impl *adapter;

    struct aws_http_proxy_config *proxy_config;
};

static void s_set_http_proxy_options_task_fn(struct aws_task *task, void *arg, enum aws_task_status status) {
    (void)task;

    struct aws_mqtt_set_http_proxy_options_task *set_task = arg;
    struct aws_mqtt_client_connection_5_impl *adapter = set_task->adapter;
    if (status != AWS_TASK_STATUS_RUN_READY) {
        goto done;
    }

    /* we're in the mqtt5 client's event loop; it's safe to access internal state */
    aws_http_proxy_config_destroy(adapter->client->config->http_proxy_config);

    /* move the proxy config from the set task to the client's config */
    adapter->client->config->http_proxy_config = set_task->proxy_config;
    if (adapter->client->config->http_proxy_config != NULL) {
        aws_http_proxy_options_init_from_config(
            &adapter->client->config->http_proxy_options, adapter->client->config->http_proxy_config);
    }

    /* don't clean up the proxy config if it was successfully assigned to the mqtt5 client */
    set_task->proxy_config = NULL;

done:

    aws_ref_count_release(&adapter->internal_refs);

    /* If the task was canceled we need to clean this up because it didn't get assigned to the mqtt5 client */
    aws_http_proxy_config_destroy(set_task->proxy_config);

    aws_mem_release(set_task->allocator, set_task);
}

static struct aws_mqtt_set_http_proxy_options_task *s_aws_mqtt_set_http_proxy_options_task_new(
    struct aws_allocator *allocator,
    struct aws_mqtt_client_connection_5_impl *adapter,
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
    set_task->allocator = adapter->allocator;
    set_task->adapter = (struct aws_mqtt_client_connection_5_impl *)aws_ref_count_acquire(&adapter->internal_refs);
    set_task->proxy_config = proxy_config;

    return set_task;
}

static int s_aws_mqtt_client_connection_5_set_http_proxy_options(
    void *impl,
    struct aws_http_proxy_options *proxy_options) {

    struct aws_mqtt_client_connection_5_impl *adapter = impl;

    struct aws_mqtt_set_http_proxy_options_task *task =
        s_aws_mqtt_set_http_proxy_options_task_new(adapter->allocator, adapter, proxy_options);
    if (task == NULL) {
        AWS_LOGF_ERROR(AWS_LS_MQTT_CLIENT, "id=%p: failed to create set http proxy options task", (void *)adapter);
        return AWS_OP_ERR;
    }

    aws_event_loop_schedule_task_now(adapter->loop, &task->task);

    return AWS_OP_SUCCESS;
}

struct aws_mqtt_set_use_websockets_task {
    struct aws_task task;
    struct aws_allocator *allocator;
    struct aws_mqtt_client_connection_5_impl *adapter;

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

    aws_ref_count_release(&adapter->internal_refs);
}

struct aws_mqtt5_adapter_websocket_handshake_args {
    bool chain_callback;
    struct aws_http_message *input_request;
    struct aws_http_message *output_request;
    int completion_error_code;
};

static int s_safe_websocket_handshake_fn(struct aws_mqtt_client_connection_5_impl *adapter, void *context) {
    struct aws_mqtt5_adapter_websocket_handshake_args *args = context;

    if (adapter->synced_data.terminated) {
        args->completion_error_code = AWS_ERROR_MQTT5_USER_REQUESTED_STOP;
    } else if (adapter->websocket_handshake_transformer == NULL) {
        args->output_request = args->input_request;
    } else {
        aws_ref_count_acquire(&adapter->internal_refs);
        args->chain_callback = true;
    }

    return AWS_OP_SUCCESS;
}

static void s_aws_mqtt5_adapter_transform_websocket_handshake_fn(
    struct aws_http_message *request,
    void *user_data,
    aws_mqtt5_transform_websocket_handshake_complete_fn *complete_fn,
    void *complete_ctx) {

    struct aws_mqtt_client_connection_5_impl *adapter = user_data;

    struct aws_mqtt5_adapter_websocket_handshake_args args = {
        .input_request = request,
    };

    s_aws_mqtt5_adapter_perform_safe_callback(adapter, false, s_safe_websocket_handshake_fn, &args);

    if (args.chain_callback) {
        adapter->mqtt5_websocket_handshake_completion_function = complete_fn;
        adapter->mqtt5_websocket_handshake_completion_user_data = complete_ctx;

        (*adapter->websocket_handshake_transformer)(
            request, user_data, s_aws_mqtt5_adapter_websocket_handshake_completion_fn, adapter);
    } else {
        (*complete_fn)(args.output_request, args.completion_error_code, complete_ctx);
    }
}

static void s_set_use_websockets_task_fn(struct aws_task *task, void *arg, enum aws_task_status status) {
    (void)task;

    struct aws_mqtt_set_use_websockets_task *set_task = arg;
    struct aws_mqtt_client_connection_5_impl *adapter = set_task->adapter;
    if (status != AWS_TASK_STATUS_RUN_READY) {
        goto done;
    }

    adapter->websocket_handshake_transformer = set_task->transformer;
    adapter->websocket_handshake_transformer_user_data = set_task->transformer_user_data;

    /* we're in the mqtt5 client's event loop; it's safe to access its internal state */
    adapter->client->config->websocket_handshake_transform = s_aws_mqtt5_adapter_transform_websocket_handshake_fn;
    adapter->client->config->websocket_handshake_transform_user_data = adapter;

done:

    aws_ref_count_release(&adapter->internal_refs);

    aws_mem_release(set_task->allocator, set_task);
}

static struct aws_mqtt_set_use_websockets_task *s_aws_mqtt_set_use_websockets_task_new(
    struct aws_allocator *allocator,
    struct aws_mqtt_client_connection_5_impl *adapter,
    aws_mqtt_transform_websocket_handshake_fn *transformer,
    void *transformer_user_data) {

    struct aws_mqtt_set_use_websockets_task *set_task =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_set_use_websockets_task));

    aws_task_init(&set_task->task, s_set_use_websockets_task_fn, (void *)set_task, "SetUseWebsocketsTask");
    set_task->allocator = adapter->allocator;
    set_task->adapter = (struct aws_mqtt_client_connection_5_impl *)aws_ref_count_acquire(&adapter->internal_refs);
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

    struct aws_mqtt_client_connection_5_impl *adapter = impl;

    struct aws_mqtt_set_use_websockets_task *task =
        s_aws_mqtt_set_use_websockets_task_new(adapter->allocator, adapter, transformer, transformer_user_data);
    if (task == NULL) {
        AWS_LOGF_ERROR(AWS_LS_MQTT_CLIENT, "id=%p: failed to create set use websockets task", (void *)adapter);
        return AWS_OP_ERR;
    }

    aws_event_loop_schedule_task_now(adapter->loop, &task->task);

    return AWS_OP_SUCCESS;
}

struct aws_mqtt_set_host_resolution_task {
    struct aws_task task;
    struct aws_allocator *allocator;
    struct aws_mqtt_client_connection_5_impl *adapter;

    struct aws_host_resolution_config host_resolution_config;
};

static void s_set_host_resolution_task_fn(struct aws_task *task, void *arg, enum aws_task_status status) {
    (void)task;

    struct aws_mqtt_set_host_resolution_task *set_task = arg;
    struct aws_mqtt_client_connection_5_impl *adapter = set_task->adapter;
    if (status != AWS_TASK_STATUS_RUN_READY) {
        goto done;
    }

    /* we're in the mqtt5 client's event loop; it's safe to access internal state */
    adapter->client->config->host_resolution_override = set_task->host_resolution_config;

done:

    aws_ref_count_release(&adapter->internal_refs);

    aws_mem_release(set_task->allocator, set_task);
}

static struct aws_mqtt_set_host_resolution_task *s_aws_mqtt_set_host_resolution_task_new(
    struct aws_allocator *allocator,
    struct aws_mqtt_client_connection_5_impl *adapter,
    const struct aws_host_resolution_config *host_resolution_config) {

    struct aws_mqtt_set_host_resolution_task *set_task =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_set_host_resolution_task));

    aws_task_init(&set_task->task, s_set_host_resolution_task_fn, (void *)set_task, "SetHostResolutionTask");
    set_task->allocator = adapter->allocator;
    set_task->adapter = (struct aws_mqtt_client_connection_5_impl *)aws_ref_count_acquire(&adapter->internal_refs);
    set_task->host_resolution_config = *host_resolution_config;

    return set_task;
}

static int s_aws_mqtt_client_connection_5_set_host_resolution_options(
    void *impl,
    const struct aws_host_resolution_config *host_resolution_config) {

    struct aws_mqtt_client_connection_5_impl *adapter = impl;

    struct aws_mqtt_set_host_resolution_task *task =
        s_aws_mqtt_set_host_resolution_task_new(adapter->allocator, adapter, host_resolution_config);
    if (task == NULL) {
        AWS_LOGF_ERROR(AWS_LS_MQTT_CLIENT, "id=%p: failed to create set reconnect timeout task", (void *)adapter);
        return AWS_OP_ERR;
    }

    aws_event_loop_schedule_task_now(adapter->loop, &task->task);

    return AWS_OP_SUCCESS;
}

struct aws_mqtt_set_will_task {
    struct aws_task task;
    struct aws_allocator *allocator;
    struct aws_mqtt_client_connection_5_impl *adapter;

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
    struct aws_mqtt_client_connection_5_impl *adapter = set_task->adapter;
    if (status != AWS_TASK_STATUS_RUN_READY) {
        goto done;
    }

    /* we're in the mqtt5 client's event loop; it's safe to access internal state */
    struct aws_mqtt5_packet_connect_storage *connect = adapter->client->config->connect;

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

    aws_ref_count_release(&adapter->internal_refs);

    s_aws_mqtt_set_will_task_destroy(set_task);
}

static struct aws_mqtt_set_will_task *s_aws_mqtt_set_will_task_new(
    struct aws_allocator *allocator,
    struct aws_mqtt_client_connection_5_impl *adapter,
    const struct aws_byte_cursor *topic,
    enum aws_mqtt_qos qos,
    bool retain,
    const struct aws_byte_cursor *payload) {

    if (topic == NULL) {
        return NULL;
    }

    struct aws_mqtt_set_will_task *set_task = aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_set_will_task));

    aws_task_init(&set_task->task, s_set_will_task_fn, (void *)set_task, "SetWillTask");
    set_task->allocator = adapter->allocator;
    set_task->adapter = (struct aws_mqtt_client_connection_5_impl *)aws_ref_count_acquire(&adapter->internal_refs);

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

    struct aws_mqtt_client_connection_5_impl *adapter = impl;

    struct aws_mqtt_set_will_task *task =
        s_aws_mqtt_set_will_task_new(adapter->allocator, adapter, topic, qos, retain, payload);
    if (task == NULL) {
        AWS_LOGF_ERROR(AWS_LS_MQTT_CLIENT, "id=%p: failed to create set will task", (void *)adapter);
        return AWS_OP_ERR;
    }

    aws_event_loop_schedule_task_now(adapter->loop, &task->task);

    return AWS_OP_SUCCESS;
}

struct aws_mqtt_set_login_task {
    struct aws_task task;
    struct aws_allocator *allocator;
    struct aws_mqtt_client_connection_5_impl *adapter;

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
    struct aws_mqtt_client_connection_5_impl *adapter = set_task->adapter;
    if (status != AWS_TASK_STATUS_RUN_READY) {
        goto done;
    }

    struct aws_byte_cursor username_cursor = aws_byte_cursor_from_buf(&set_task->username_buffer);
    struct aws_byte_cursor password_cursor = aws_byte_cursor_from_buf(&set_task->password_buffer);

    /* we're in the mqtt5 client's event loop; it's safe to access internal state */
    struct aws_mqtt5_packet_connect_storage *old_connect = adapter->client->config->connect;

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
        aws_mem_calloc(adapter->allocator, 1, sizeof(struct aws_mqtt5_packet_connect_storage));
    aws_mqtt5_packet_connect_storage_init(new_connect, adapter->allocator, &new_connect_view);

    adapter->client->config->connect = new_connect;
    aws_mqtt5_packet_connect_storage_clean_up(old_connect);
    aws_mem_release(old_connect->allocator, old_connect);

done:

    aws_ref_count_release(&adapter->internal_refs);

    s_aws_mqtt_set_login_task_destroy(set_task);
}

static struct aws_mqtt_set_login_task *s_aws_mqtt_set_login_task_new(
    struct aws_allocator *allocator,
    struct aws_mqtt_client_connection_5_impl *adapter,
    const struct aws_byte_cursor *username,
    const struct aws_byte_cursor *password) {

    struct aws_mqtt_set_login_task *set_task = aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_set_login_task));

    aws_task_init(&set_task->task, s_set_login_task_fn, (void *)set_task, "SetLoginTask");
    set_task->allocator = adapter->allocator;
    set_task->adapter = (struct aws_mqtt_client_connection_5_impl *)aws_ref_count_acquire(&adapter->internal_refs);

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

    struct aws_mqtt_client_connection_5_impl *adapter = impl;

    struct aws_mqtt_set_login_task *task =
        s_aws_mqtt_set_login_task_new(adapter->allocator, adapter, username, password);
    if (task == NULL) {
        AWS_LOGF_ERROR(AWS_LS_MQTT_CLIENT, "id=%p: failed to create set login task", (void *)adapter);
        return AWS_OP_ERR;
    }

    aws_event_loop_schedule_task_now(adapter->loop, &task->task);

    return AWS_OP_SUCCESS;
}

static void s_aws_mqtt3_to_mqtt5_adapter_on_zero_internal_refs(void *context) {
    struct aws_mqtt_client_connection_5_impl *adapter = context;

    s_aws_mqtt_adapter_final_destroy(adapter);
}

static void s_aws_mqtt3_to_mqtt5_adapter_on_listener_detached(void *context) {
    struct aws_mqtt_client_connection_5_impl *adapter = context;

    /*
     * Release the single internal reference that we started with.  Only ephemeral references for cross-thread
     * tasks might remain, and they will disappear quickly.
     */
    aws_ref_count_release(&adapter->internal_refs);
}

static struct aws_mqtt_client_connection *s_aws_mqtt_client_connection_5_acquire(void *impl) {
    struct aws_mqtt_client_connection_5_impl *adapter = impl;

    aws_ref_count_acquire(&adapter->external_refs);

    return &adapter->base;
}

static int s_decref_for_shutdown(struct aws_mqtt_client_connection_5_impl *adapter, void *context) {
    (void)context;

    adapter->synced_data.terminated = true;

    return AWS_OP_SUCCESS;
}

static void s_aws_mqtt3_to_mqtt5_adapter_on_zero_external_refs(void *impl) {
    struct aws_mqtt_client_connection_5_impl *adapter = impl;

    s_aws_mqtt5_adapter_perform_safe_callback(adapter, true, s_decref_for_shutdown, NULL);

    /*
     * When the adapter's exernal ref count goes to zero, here's what we want to do:
     *
     *  (1) Put the adapter into the terminated state, which tells it to stop processing callbacks from the mqtt5
     *      client
     *  (2) Release the client listener, starting its asynchronous shutdown process (since we're the only user
     *      of it)
     *  (3) Wait for the client listener to notify us that asynchronous shutdown is over.  At this point we
     *      are guaranteed that no more callbacks from the mqtt5 client will reach us.
     *  (4) Release the single internal ref we started with when the adapter was created.
     *  (5) On last internal ref, we can safely release the mqtt5 client and synchronously clean up all other
     *      resources
     *
     *  Step (1) was done within the lock-guarded safe callback above.
     *  Step (2) is done here.
     *  Steps (3) and (4) are accomplished by s_aws_mqtt3_to_mqtt5_adapter_on_listener_detached
     *  Step (5) is completed by s_aws_mqtt3_to_mqtt5_adapter_on_zero_internal_refs
     */
    aws_mqtt5_listener_release(adapter->listener);
}

static void s_aws_mqtt_client_connection_5_release(void *impl) {
    struct aws_mqtt_client_connection_5_impl *adapter = impl;

    aws_ref_count_release(&adapter->external_refs);
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
    adapter->adapter_state = AWS_MQTT_AS_STAY_DISCONNECTED;

    aws_ref_count_init(&adapter->external_refs, adapter, s_aws_mqtt3_to_mqtt5_adapter_on_zero_external_refs);
    aws_ref_count_init(&adapter->internal_refs, adapter, s_aws_mqtt3_to_mqtt5_adapter_on_zero_internal_refs);

    aws_rw_lock_init(&adapter->lock);

    /*
     * We start disabled to handle the case where someone passes in an mqtt5 client that is already "live."
     * We'll enable the adapter as soon as they try to connect via the 311 interface.  This
     * also ties in to how we simulate the 311 implementation's don't-reconnect-if-initial-connect-fails logic.
     * The 5 client will continue to try and reconnect, but the adapter will go disabled making it seem to the 311
     * user that it is offline.
     */
    adapter->synced_data.terminated = false;

    struct aws_mqtt5_listener_config listener_config = {
        .client = client,
        .listener_callbacks =
            {
                .listener_publish_received_handler = s_aws_mqtt5_listener_publish_received_adapter,
                .listener_publish_received_handler_user_data = adapter,
                .lifecycle_event_handler = s_aws_mqtt5_client_connection_event_callback_adapter,
                .lifecycle_event_handler_user_data = adapter,
            },
        .termination_callback = s_aws_mqtt3_to_mqtt5_adapter_on_listener_detached,
        .termination_callback_user_data = adapter,
    };
    adapter->listener = aws_mqtt5_listener_new(allocator, &listener_config);

    return &adapter->base;
}
