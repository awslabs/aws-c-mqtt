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
         * client (2) Release the client listener, starting its asynchronous shutdown process (since we're the only user
         * of it) (3) Wait for the client listener to notify us that asynchronous shutdown is over.  At this point we
         * are guaranteed that no more callbacks from the mqtt5 client will reach us.  We can safely release the mqtt5
         *      client.
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
    .set_will_fn = NULL,
    .set_login_fn = NULL,
    .use_websockets_fn = NULL,
    .set_http_proxy_options_fn = NULL,
    .set_host_resolution_options_fn = NULL,
    .set_reconnect_timeout_fn = NULL,
    .set_connection_interruption_handlers_fn = NULL,
    .set_connection_closed_handler_fn = NULL,
    .set_on_any_publish_handler_fn = NULL,
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
     * In that case, we don't want callbacks coming back before construction is even over, so instead we "cork"
     * things by starting in the disabled state.  We'll enable the adapter as soon as they try to connect.
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
