/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>
#include <aws/mqtt/private/client_impl_shared.h>
#include <aws/mqtt/private/v5/mqtt5_client_impl.h>
#include <aws/mqtt/v5/mqtt5_listener.h>

enum aws_mqtt5_adapter_state { AWS_MQTT5_AS_ENABLED, AWS_MQTT5_AS_DISABLED };

struct aws_mqtt_client_connection_5_impl {
    struct aws_allocator *allocator;

    struct aws_mqtt_client_connection base;

    struct aws_mqtt5_client *client;
    struct aws_mqtt5_listener *listener;
    struct aws_event_loop *loop;

    struct aws_mutex state_lock;
    enum aws_mqtt5_adapter_state state;
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
    struct aws_mqtt_client_connection_5_impl *impl = context;

    impl->client = aws_mqtt5_client_release(impl->client);
    aws_mutex_clean_up(&impl->state_lock);

    aws_mem_release(impl->allocator, impl);
}

static void s_mqtt_client_connection_5_impl_start_destroy(void *context) {
    struct aws_mqtt_client_connection_5_impl *impl = context;

    s_disable_adapter(impl);

    aws_mqtt5_listener_release(impl->listener);
}

static struct aws_mqtt_client_connection_vtable s_aws_mqtt_client_connection_5_vtable = {
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
    struct aws_mqtt_client_connection_5_impl *mqtt5_impl =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_client_connection_5_impl));

    mqtt5_impl->allocator = allocator;

    mqtt5_impl->base.vtable = s_aws_mqtt_client_connection_5_vtable_ptr;
    mqtt5_impl->base.impl = mqtt5_impl;
    aws_ref_count_init(
        &mqtt5_impl->base.ref_count,
        mqtt5_impl,
        (aws_simple_completion_callback *)s_mqtt_client_connection_5_impl_start_destroy);

    mqtt5_impl->client = aws_mqtt5_client_acquire(client);
    mqtt5_impl->loop = client->loop;

    struct aws_mqtt5_listener_config listener_config = {
        .client = client,
        .listener_callbacks =
            {
                .listener_publish_received_handler = s_aws_mqtt5_listener_publish_received_adapter,
                .listener_publish_received_handler_user_data = mqtt5_impl,
                .lifecycle_event_handler = s_aws_mqtt5_client_connection_event_callback_adapter,
                .lifecycle_event_handler_user_data = mqtt5_impl,
            },
        .termination_callback = s_mqtt_client_connection_5_impl_finish_destroy,
        .termination_callback_user_data = mqtt5_impl,
    };
    mqtt5_impl->listener = aws_mqtt5_listener_new(allocator, &listener_config);

    aws_mutex_init(&mqtt5_impl->state_lock);
    mqtt5_impl->state = AWS_MQTT5_AS_ENABLED;

    return &mqtt5_impl->base;
}
