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

    struct aws_mutex state_lock;
    enum aws_mqtt5_adapter_state state;

    /* 311 interface callbacks */
    aws_mqtt_client_on_connection_interrupted_fn *on_interrupted;
    void *on_interrupted_ud;

    aws_mqtt_client_on_connection_resumed_fn *on_resumed;
    void *on_resumed_ud;

    aws_mqtt_client_on_connection_closed_fn *on_closed;
    void *on_closed_ud;

    aws_mqtt_client_publish_received_fn *on_any_publish;
    void *on_any_publish_ud;
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

static void s_disable_adapter(struct aws_mqtt_client_connection_5_impl *adapter) {
    /*
     * The lock is held during callbacks to prevent invoking into something that is in the process of
     * destruction.  In general this isn't a performance worry since callbacks are invoked from a single
     * thread: the event loop that the client and adapter are seated on.
     *
     * But since we don't have recursive mutexes on all platforms, we need to be careful about the disable
     * API since if we naively always locked, then an adapter release inside a callback would deadlock.
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
     */
    if (aws_event_loop_thread_is_callers_thread(adapter->loop)) {
        bool lock_succeeded = aws_mutex_try_lock(&adapter->state_lock) == AWS_OP_SUCCESS;
        adapter->state = AWS_MQTT5_AS_DISABLED;
        if (lock_succeeded) {
            aws_mutex_unlock(&adapter->state_lock);
        }
    } else {
        aws_mutex_lock(&adapter->state_lock);
        adapter->state = AWS_MQTT5_AS_DISABLED;
        aws_mutex_unlock(&adapter->state_lock);
    }
}

static void s_mqtt_client_connection_5_impl_finish_destroy(void *context) {
    struct aws_mqtt_client_connection_5_impl *adapter = context;

    adapter->client = aws_mqtt5_client_release(adapter->client);
    aws_mutex_clean_up(&adapter->state_lock);

    aws_mem_release(adapter->allocator, adapter);
}

/*
 * When the adapter's ref count goes to zero, here's what we want to do:
 *
 *  (1) Put the adapter into the disabled mode, which tells it to stop processing callbacks from the mqtt5 client
 *  (2) Release the client listener, starting its asynchronous shutdown process (since we're the only user of it)
 *  (3) Wait for the client listener to notify us that asynchronous shutdown is over.  At this point we are
 *      guaranteed that no more callbacks from the mqtt5 client will reach us.  We can safely release the mqtt5
 *      client.
 *  (4) Synchronously clean up all further resources.
 *
 *  This function does steps (1) and (2).  (3) and (4) are accomplished via
 *  s_mqtt_client_connection_5_impl_finish_destroy above.
 */
static void s_mqtt_client_connection_5_impl_start_destroy(void *context) {
    struct aws_mqtt_client_connection_5_impl *adapter = context;

    s_disable_adapter(adapter);

    aws_mqtt5_listener_release(adapter->listener);
}

struct aws_mqtt_set_interruption_handlers_task {
    struct aws_task task;
    struct aws_allocator *allocator;
    struct aws_mqtt_client_connection *connection;

    aws_mqtt_client_on_connection_interrupted_fn *on_interrupted;
    void *on_interrupted_ud;
    aws_mqtt_client_on_connection_resumed_fn *on_resumed;
    void *on_resumed_ud;
};

static void s_set_interruption_handlers_task_fn(struct aws_task *task, void *arg, enum aws_task_status status) {
    (void)task;

    struct aws_mqtt_set_interruption_handlers_task *set_task = arg;
    if (status != AWS_TASK_STATUS_RUN_READY) {
        goto done;
    }

    struct aws_mqtt_client_connection_5_impl *connection = set_task->connection->impl;

    connection->on_interrupted = set_task->on_interrupted;
    connection->on_interrupted_ud = set_task->on_interrupted_ud;
    connection->on_resumed = set_task->on_resumed;
    connection->on_resumed_ud = set_task->on_resumed_ud;

done:

    aws_mqtt_client_connection_release(set_task->connection);

    aws_mem_release(set_task->allocator, set_task);
}

static struct aws_mqtt_set_interruption_handlers_task *s_aws_mqtt_set_interruption_handlers_task_new(
    struct aws_allocator *allocator,
    struct aws_mqtt_client_connection_5_impl *connection,
    aws_mqtt_client_on_connection_interrupted_fn *on_interrupted,
    void *on_interrupted_ud,
    aws_mqtt_client_on_connection_resumed_fn *on_resumed,
    void *on_resumed_ud) {

    struct aws_mqtt_set_interruption_handlers_task *set_task =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_set_interruption_handlers_task));
    if (set_task == NULL) {
        return NULL;
    }

    aws_task_init(
        &set_task->task, s_set_interruption_handlers_task_fn, (void *)set_task, "SetInterruptionHandlersTask");
    set_task->allocator = connection->allocator;
    set_task->connection = aws_mqtt_client_connection_acquire(&connection->base);
    set_task->on_interrupted = on_interrupted;
    set_task->on_interrupted_ud = on_interrupted_ud;
    set_task->on_resumed = on_resumed;
    set_task->on_resumed_ud = on_resumed_ud;

    return set_task;
}

static int s_aws_mqtt_client_connection_5_set_interruption_handlers(
    void *impl,
    aws_mqtt_client_on_connection_interrupted_fn *on_interrupted,
    void *on_interrupted_ud,
    aws_mqtt_client_on_connection_resumed_fn *on_resumed,
    void *on_resumed_ud) {
    struct aws_mqtt_client_connection_5_impl *connection = impl;

    struct aws_mqtt_set_interruption_handlers_task *task = s_aws_mqtt_set_interruption_handlers_task_new(
        connection->allocator, connection, on_interrupted, on_interrupted_ud, on_resumed, on_resumed_ud);
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
    void *on_closed_ud;
};

static void s_set_on_closed_handler_task_fn(struct aws_task *task, void *arg, enum aws_task_status status) {
    (void)task;

    struct aws_mqtt_set_on_closed_handler_task *set_task = arg;
    if (status != AWS_TASK_STATUS_RUN_READY) {
        goto done;
    }

    struct aws_mqtt_client_connection_5_impl *connection = set_task->connection->impl;

    connection->on_closed = set_task->on_closed;
    connection->on_closed_ud = set_task->on_closed_ud;

done:

    aws_mqtt_client_connection_release(set_task->connection);

    aws_mem_release(set_task->allocator, set_task);
}

static struct aws_mqtt_set_on_closed_handler_task *s_aws_mqtt_set_on_closed_handler_task_new(
    struct aws_allocator *allocator,
    struct aws_mqtt_client_connection_5_impl *connection,
    aws_mqtt_client_on_connection_closed_fn *on_closed,
    void *on_closed_ud) {

    struct aws_mqtt_set_on_closed_handler_task *set_task =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_set_on_closed_handler_task));
    if (set_task == NULL) {
        return NULL;
    }

    aws_task_init(&set_task->task, s_set_on_closed_handler_task_fn, (void *)set_task, "SetOnClosedHandlerTask");
    set_task->allocator = connection->allocator;
    set_task->connection = aws_mqtt_client_connection_acquire(&connection->base);
    set_task->on_closed = on_closed;
    set_task->on_closed_ud = on_closed_ud;

    return set_task;
}

static int s_aws_mqtt_client_connection_5_set_on_closed_handler(
    void *impl,
    aws_mqtt_client_on_connection_closed_fn *on_closed,
    void *on_closed_ud) {
    struct aws_mqtt_client_connection_5_impl *connection = impl;

    struct aws_mqtt_set_on_closed_handler_task *task =
        s_aws_mqtt_set_on_closed_handler_task_new(connection->allocator, connection, on_closed, on_closed_ud);
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
    void *on_any_publish_ud;
};

static void s_set_on_any_publish_handler_task_fn(struct aws_task *task, void *arg, enum aws_task_status status) {
    (void)task;

    struct aws_mqtt_set_on_any_publish_handler_task *set_task = arg;
    if (status != AWS_TASK_STATUS_RUN_READY) {
        goto done;
    }

    struct aws_mqtt_client_connection_5_impl *connection = set_task->connection->impl;

    connection->on_any_publish = set_task->on_any_publish;
    connection->on_any_publish_ud = set_task->on_any_publish_ud;

done:

    aws_mqtt_client_connection_release(set_task->connection);

    aws_mem_release(set_task->allocator, set_task);
}

static struct aws_mqtt_set_on_any_publish_handler_task *s_aws_mqtt_set_on_any_publish_handler_task_new(
    struct aws_allocator *allocator,
    struct aws_mqtt_client_connection_5_impl *connection,
    aws_mqtt_client_publish_received_fn *on_any_publish,
    void *on_any_publish_ud) {

    struct aws_mqtt_set_on_any_publish_handler_task *set_task =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_set_on_any_publish_handler_task));
    if (set_task == NULL) {
        return NULL;
    }

    aws_task_init(
        &set_task->task, s_set_on_any_publish_handler_task_fn, (void *)set_task, "SetOnAnyPublishHandlerTask");
    set_task->allocator = connection->allocator;
    set_task->connection = aws_mqtt_client_connection_acquire(&connection->base);
    set_task->on_any_publish = on_any_publish;
    set_task->on_any_publish_ud = on_any_publish_ud;

    return set_task;
}

static int s_aws_mqtt_client_connection_5_set_on_any_publish_handler(
    void *impl,
    aws_mqtt_client_publish_received_fn *on_any_publish,
    void *on_any_publish_ud) {
    struct aws_mqtt_client_connection_5_impl *connection = impl;

    struct aws_mqtt_set_on_any_publish_handler_task *task = s_aws_mqtt_set_on_any_publish_handler_task_new(
        connection->allocator, connection, on_any_publish, on_any_publish_ud);
    if (task == NULL) {
        AWS_LOGF_ERROR(AWS_LS_MQTT_CLIENT, "id=%p: failed to create set on any publish task", (void *)connection);
        return AWS_OP_ERR;
    }

    aws_event_loop_schedule_task_now(connection->loop, &task->task);

    return AWS_OP_SUCCESS;
}

static struct aws_mqtt_client_connection_vtable s_aws_mqtt_client_connection_5_vtable = {
    .set_will_fn = NULL,
    .set_login_fn = NULL,
    .use_websockets_fn = NULL,
    .set_http_proxy_options_fn = NULL,
    .set_host_resolution_options_fn = NULL,
    .set_reconnect_timeout_fn = NULL,
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
    aws_ref_count_init(
        &adapter->base.ref_count,
        adapter,
        (aws_simple_completion_callback *)s_mqtt_client_connection_5_impl_start_destroy);

    adapter->client = aws_mqtt5_client_acquire(client);
    adapter->loop = client->loop;

    aws_mutex_init(&adapter->state_lock);

    /*
     * We start disabled to handle the case where someone passes in an mqtt5 client that is already "live."
     * In that case, we don't want callbacks coming back before construction is even over, so instead we "cork"
     * things by starting in the disabled state.  We'll enable the adapter as soon as they try to connect.
     */
    adapter->state = AWS_MQTT5_AS_DISABLED;

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
