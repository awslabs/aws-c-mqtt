/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/private/request-response/protocol_adapter.h>

#include <aws/io/event_loop.h>
#include <aws/mqtt/private/client_impl_shared.h>
#include <aws/mqtt/private/request-response/weak_ref.h>
#include <aws/mqtt/private/v5/mqtt5_client_impl.h>
#include <aws/mqtt/v5/mqtt5_client.h>
#include <aws/mqtt/v5/mqtt5_listener.h>

enum aws_protocol_adapter_subscription_status {
    PASS_NONE,
    PASS_SUBSCRIBING,
    PASS_SUBSCRIBED,
    PASS_UNSUBSCRIBING,
};

struct aws_mqtt_protocol_adapter_subscription {
    struct aws_allocator *allocator;

    struct aws_byte_cursor topic_filter;
    struct aws_byte_buf topic_filter_buf;

    enum aws_protocol_adapter_subscription_status status;
};

static struct aws_mqtt_protocol_adapter_subscription *s_aws_mqtt_protocol_adapter_subscription_new(struct aws_allocator *allocator, struct aws_byte_cursor topic_filter) {
    struct aws_mqtt_protocol_adapter_subscription *subscription = aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_protocol_adapter_subscription));

    subscription->allocator = allocator;
    aws_byte_buf_init_copy_from_cursor(&subscription->topic_filter_buf, allocator, topic_filter);
    subscription->topic_filter = aws_byte_cursor_from_buf(&subscription->topic_filter_buf);

    return subscription;
}

static void s_aws_mqtt_protocol_adapter_subscription_destroy(struct aws_mqtt_protocol_adapter_subscription *subscription) {
    aws_byte_buf_clean_up(&subscription->topic_filter_buf);
    aws_mem_release(subscription->allocator, subscription);
}

struct aws_mqtt_protocol_adapter_subscription_set {
    struct aws_allocator *allocator;
    struct aws_mqtt_protocol_adapter *owner; // not an acquired reference due to the parent-child relationship
    struct aws_hash_table subscriptions; // aws_byte_cursor * -> aws_mqtt_protocol_adapter_subscription *

    aws_protocol_adapter_subscription_status_fn *subscription_status_update_callback;
    void *callback_user_data;
};

static int s_aws_mqtt_protocol_adapter_subscription_set_init(struct aws_mqtt_protocol_adapter_subscription_set *subscription_set, struct aws_allocator *allocator, struct aws_mqtt_protocol_adapter *owner, struct aws_mqtt_protocol_adapter_options *options) {
    subscription_set->allocator = allocator;
    subscription_set->owner = owner;
    subscription_set->subscription_status_update_callback = options->subscription_status_update_callback;
    subscription_set->callback_user_data = options->user_data;

    return aws_hash_table_init(&subscription_set->subscriptions, allocator, 0, aws_hash_byte_cursor_ptr, aws_mqtt_byte_cursor_hash_equality, NULL, NULL);
}

static int s_aws_mqtt_protocol_adapter_subscription_set_subscription_destroy(void *context, struct aws_hash_element *elem) {
    struct aws_mqtt_protocol_adapter_subscription_set *subscription_set = context;
    struct aws_mqtt_protocol_adapter *adapter = subscription_set->owner;

    struct aws_mqtt_protocol_adapter_subscription *subscription = elem->value;
    if (subscription->status != PASS_UNSUBSCRIBING) {
        struct aws_protocol_adapter_unsubscribe_options options = {
            .topic_filter = subscription->topic_filter,
        };

        aws_mqtt_protocol_adapter_unsubscribe(adapter, &options);
    }

    s_aws_mqtt_protocol_adapter_subscription_destroy(subscription);

    return AWS_COMMON_HASH_TABLE_ITER_CONTINUE | AWS_COMMON_HASH_TABLE_ITER_DELETE;
}

static void s_aws_mqtt_protocol_adapter_subscription_set_clean_up(struct aws_mqtt_protocol_adapter_subscription_set *subscription_set) {
    struct aws_hash_table subscriptions;
    AWS_ZERO_STRUCT(subscriptions);

    aws_hash_table_swap(&subscription_set->subscriptions, &subscriptions);

    aws_hash_table_foreach(&subscriptions, s_aws_mqtt_protocol_adapter_subscription_set_subscription_destroy, subscription_set);

    aws_hash_table_clean_up(&subscriptions);
}

/*
 * On subscribe success: if there's not an entry, is this possible? No because we're called only by a function that checks for adapter weak->strong first, so the adapter exists and we don't allow subscription removal without an unsubscribe complete and we don't allow the subscribe until the unsubscribe has completed.  But what
 * if
 * On subscribe success: if there's an entry, transition | subscribing -> subscribed, send an update
 * On subscribe failure: if there's not an entry, is this possible?
 * On subscribe failure: if there's an entry, transition -> unsubscribing, send an update
 *
 * Should we just blindly add if the adapter exists?  Yes: simplest.  No: represents undefined behavior if it shouldn't be happening
 *
 * In the design we said that the subscription set is just a dumb reflection of the ordered sequence of operations
 * from the rr client which implies we should just create_or_update.  The only time we don't want to create_or_update
 * is if we're in/post destruction but then there's no adapter and we early out
 */
static void s_aws_mqtt_protocol_adapter_subscription_set_on_subscribe_completion(struct aws_mqtt_protocol_adapter_subscription_set *subscription_set, struct aws_byte_cursor topic_filter, bool success) {
    if (!success) {
        struct aws_protocol_adapter_unsubscribe_options options = {
            .topic_filter = topic_filter,
        };

        aws_mqtt_protocol_adapter_unsubscribe(subscription_set->owner, &options);
    }

    struct aws_hash_element *hash_element = NULL;
    if (!aws_hash_table_find(&subscription_set->subscriptions, &topic_filter, &hash_element) || hash_element == NULL) {
        return;
    }

    struct aws_mqtt_protocol_adapter_subscription *subscription = hash_element->value;

}

static void s_aws_mqtt_protocol_adapter_subscription_set_update_subscription(struct aws_mqtt_protocol_adapter_subscription_set *subscription_set, struct aws_byte_cursor topic_filter, enum aws_protocol_adapter_subscription_status status) {
    (void)subscription_set;
    (void)topic_filter;
    (void)status;

    ??;
}

static void s_aws_mqtt_protocol_adapter_subscription_set_create_or_update_subscription(struct aws_mqtt_protocol_adapter_subscription_set *subscription_set, struct aws_byte_cursor topic_filter, enum aws_protocol_adapter_subscription_status status) {
    (void)subscription_set;
    (void)topic_filter;
    (void)status;

    ??;
}


/******************************************************************************************************************/

struct aws_mqtt_protocol_adapter *aws_mqtt_protocol_adapter_new_from_311(struct aws_allocator *allocator, struct aws_mqtt_protocol_adapter_options *options, struct aws_mqtt_client_connection *connection) {
    (void)allocator;
    (void)options;
    (void)connection;

    return NULL;
}

/******************************************************************************************************************/

struct aws_mqtt_protocol_adapter_5_impl {
    struct aws_allocator *allocator;
    struct aws_mqtt_protocol_adapter base;
    struct aws_protocol_adapter_weak_ref *callback_ref;
    struct aws_mqtt_protocol_adapter_options config;

    struct aws_event_loop *loop;
    struct aws_mqtt5_client *client;
    struct aws_mqtt5_listener *listener;

    struct aws_mqtt_protocol_adapter_subscription_set subscriptions;
};

static void s_aws_mqtt_protocol_adapter_5_release(void *impl) {
    struct aws_mqtt_protocol_adapter_5_impl *adapter = impl;

    aws_mqtt5_listener_release(adapter->listener);
}

struct aws_mqtt_protocol_adapter_5_subscribe_data {
    struct aws_allocator *allocator;

    struct aws_byte_buf *topic_filter;
    struct aws_protocol_adapter_weak_ref *callback_ref;
};

static struct aws_mqtt_protocol_adapter_5_subscribe_data *aws_mqtt_protocol_adapter_5_subscribe_data_new(struct aws_allocator *allocator, struct aws_byte_cursor topic_filter, struct aws_protocol_adapter_weak_ref *callback_ref) {
    struct aws_mqtt_protocol_adapter_5_subscribe_data *subscribe_data = aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_protocol_adapter_5_subscribe_data));

    subscribe_data->allocator = allocator;
    subscribe_data->callback_ref = aws_weak_ref_acquire(callback_ref);
    aws_byte_buf_init_copy_from_cursor(&subscribe_data->topic_filter, allocator, topic_filter);

    return subscribe_data;
}

static void aws_mqtt_protocol_adapter_5_subscribe_data_delete(struct aws_mqtt_protocol_adapter_5_subscribe_data *subscribe_data) {
    aws_weak_ref_release(subscribe_data->callback_ref);

    aws_mem_release(subscribe_data->allocator, subscribe_data);
}

static void s_protocol_adapter_5_subscribe_completion(const struct aws_mqtt5_packet_suback_view *suback,
                                                      int error_code,
                                                      void *complete_ctx) {
    struct aws_mqtt_protocol_adapter_5_subscribe_data *subscribe_data = complete_ctx;
    struct aws_mqtt_protocol_adapter_5_impl *adapter = aws_weak_ref_get_reference(subscribe_data->callback_ref);

    if (adapter == NULL) {
        goto done;
    }

    bool success = error_code == AWS_ERROR_SUCCESS && suback != NULL && suback->reason_code_count == 1 && suback->reason_codes[0] <= AWS_MQTT5_SARC_GRANTED_QOS_1;
    s_aws_mqtt_protocol_adapter_subscription_set_on_subscribe_completion(&adapter->subscriptions, aws_byte_cursor_from_buf(&subscribe_data->topic_filter), success);

done:

    aws_mqtt_protocol_adapter_5_subscribe_data_delete(subscribe_data);
}

int s_aws_mqtt_protocol_adapter_5_subscribe(void *impl, struct aws_protocol_adapter_subscribe_options *options) {
    struct aws_mqtt_protocol_adapter_5_impl *adapter = impl;

    struct aws_mqtt_protocol_adapter_5_subscribe_data *subscribe_data = aws_mqtt_protocol_adapter_5_subscribe_data_new(adapter->allocator, options->topic_filter, adapter->callback_ref);

    struct aws_mqtt5_subscription_view subscription_view = {
        .qos = AWS_MQTT5_QOS_AT_LEAST_ONCE,
        .topic_filter = options->topic_filter,
    };

    struct aws_mqtt5_packet_subscribe_view subscribe_view = {
        .subscriptions = &subscription_view,
        .subscription_count = 1,
    };

    struct aws_mqtt5_subscribe_completion_options completion_options = {
        .ack_timeout_seconds_override = options->ack_timeout_seconds,
        .completion_callback = s_protocol_adapter_5_subscribe_completion,
        .completion_user_data = subscribe_data,
    };

    if (aws_mqtt5_client_subscribe(adapter->client, &subscribe_view, &completion_options)) {
        goto error;
    }

    return AWS_OP_SUCCESS;

error:

    aws_mqtt_protocol_adapter_5_subscribe_data_delete(subscribe_data);

    return AWS_OP_ERR;
}

int s_aws_mqtt_protocol_adapter_5_unsubscribe(void *impl, struct aws_protocol_adapter_unsubscribe_options *options) {
    struct aws_mqtt_protocol_adapter_5_impl *adapter = impl;
    (void)adapter;
    (void)options;

    return aws_raise_error(AWS_ERROR_UNIMPLEMENTED);
}

int s_aws_mqtt_protocol_adapter_5_publish(void *impl, struct aws_protocol_adapter_publish_options *options) {
    struct aws_mqtt_protocol_adapter_5_impl *adapter = impl;
    (void)adapter;
    (void)options;

    return aws_raise_error(AWS_ERROR_UNIMPLEMENTED);
}

static bool s_protocol_adapter_mqtt5_listener_publish_received(const struct aws_mqtt5_packet_publish_view *publish, void *user_data) {
    (void)publish;
    (void)user_data;
}

static void s_protocol_adapter_mqtt5_lifecycle_event_callback(const struct aws_mqtt5_client_lifecycle_event *event) {
    (void)event;
}

static void s_protocol_adapter_mqtt5_listener_termination_callback(void *user_data) {
    struct aws_mqtt_protocol_adapter_5_impl *adapter = user_data;

    AWS_FATAL_ASSERT(aws_event_loop_thread_is_callers_thread(adapter->client->loop));

    aws_mqtt5_client_release(adapter->client);

    aws_weak_ref_zero_reference(adapter->callback_ref);
    aws_weak_ref_release(adapter->callback_ref);

    aws_mem_release(adapter->allocator, adapter);
}

static struct aws_mqtt_protocol_adapter_vtable s_protocol_adapter_mqtt5_vtable = {
    .aws_mqtt_protocol_adapter_release_fn = s_aws_mqtt_protocol_adapter_5_release,
    .aws_mqtt_protocol_adapter_subscribe_fn = s_aws_mqtt_protocol_adapter_5_subscribe,
    .aws_mqtt_protocol_adapter_unsubscribe_fn = s_aws_mqtt_protocol_adapter_5_unsubscribe,
    .aws_mqtt_protocol_adapter_publish_fn = s_aws_mqtt_protocol_adapter_5_publish,
};

struct aws_mqtt_protocol_adapter *aws_mqtt_protocol_adapter_new_from_5(struct aws_allocator *allocator, struct aws_mqtt_protocol_adapter_options *options, struct aws_mqtt5_client *client) {
    AWS_FATAL_ASSERT(aws_event_loop_thread_is_callers_thread(client->loop));

    struct aws_mqtt_protocol_adapter_5_impl *adapter = aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_protocol_adapter_5_impl));

    adapter->allocator = allocator;
    adapter->base.impl = adapter;
    adapter->base.vtable = &s_protocol_adapter_mqtt5_vtable;
    adapter->callback_ref = aws_weak_ref_new(allocator, adapter);
    adapter->config = *options;
    adapter->loop = client->loop;
    adapter->client = aws_mqtt5_client_acquire(client);

    struct aws_mqtt5_listener_config listener_options = {
        .client = client,
        .listener_callbacks = {
            .listener_publish_received_handler = s_protocol_adapter_mqtt5_listener_publish_received,
            .listener_publish_received_handler_user_data = adapter,
            .lifecycle_event_handler = s_protocol_adapter_mqtt5_lifecycle_event_callback,
            .lifecycle_event_handler_user_data = adapter
        },
        .termination_callback = s_protocol_adapter_mqtt5_listener_termination_callback,
        .termination_callback_user_data = adapter,
    };

    adapter->listener = aws_mqtt5_listener_new(allocator, &listener_options);

    return adapter;
}

void aws_mqtt_protocol_adapter_release(struct aws_mqtt_protocol_adapter *adapter) {
    (*adapter->vtable->aws_mqtt_protocol_adapter_release_fn)(adapter->impl);
}

int aws_mqtt_protocol_adapter_subscribe(struct aws_mqtt_protocol_adapter *adapter, struct aws_protocol_adapter_subscribe_options *options) {
    return (*adapter->vtable->aws_mqtt_protocol_adapter_subscribe_fn)(adapter->impl, options);
}

int aws_mqtt_protocol_adapter_unsubscribe(struct aws_mqtt_protocol_adapter *adapter, struct aws_protocol_adapter_unsubscribe_options *options) {
    return (*adapter->vtable->aws_mqtt_protocol_adapter_unsubscribe_fn)(adapter->impl, options);
}

int aws_mqtt_protocol_adapter_publish(struct aws_mqtt_protocol_adapter *adapter, struct aws_protocol_adapter_publish_options *options) {
    return (*adapter->vtable->aws_mqtt_protocol_adapter_publish_fn)(adapter->impl, options);
}