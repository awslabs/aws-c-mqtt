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

/*
 * New API contract
 *
 * Invariant 1: Subscribe is only called from the RR subscription manager when going from 0 to 1 pending operations
 * Invariant 2: Unsubscribe is only called from the RR subscription manager when there are 0 pending operations, not
 *   necessarily on the exact transition to zero though.
 *
 * Entries are not tracked with the exception of eventstream impl which needs the stream handles to close.
 * A subscribe failure should not trigger an unsubscribe, only notify the status callback.
 * Subscription event callback should be {subscribe_success, subscribe_failure, unsubscribe_success, unsubscribe_failure}.
 * The sub manager is responsible for calling Unsubscribe on all its entries when shutting down (before releasing
 *   hold of the adapter).
 *
 * How do we know not to retry unsubscribe failures because a subscribe came in?  Well, we don't retry failures; let
 * the manager make that decision.  No retry when the weak ref is zeroed either.  The potential for things to go wrong
 * is worse than the potential of a subscription "leaking."
 *
 * On subscribe failures with zeroed weak ref, trust that an Unsubscribe was sent that will resolve later and let it
 * decide what to do.
 */


struct aws_mqtt_protocol_adapter *aws_mqtt_protocol_adapter_new_from_311(struct aws_allocator *allocator, struct aws_mqtt_protocol_adapter_options *options, struct aws_mqtt_client_connection *connection) {
    (void)allocator;
    (void)options;
    (void)connection;

    // TODO
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
};

static void s_aws_mqtt_protocol_adapter_5_delete(void *impl) {
    struct aws_mqtt_protocol_adapter_5_impl *adapter = impl;

    // all the real cleanup is done in the listener termination callback
    aws_mqtt5_listener_release(adapter->listener);
}

// used by both subscribe and unsubscribe
struct aws_mqtt_protocol_adapter_5_subscription_op_data {
    struct aws_allocator *allocator;

    struct aws_byte_buf topic_filter;
    struct aws_protocol_adapter_weak_ref *callback_ref;
};

static struct aws_mqtt_protocol_adapter_5_subscription_op_data *s_aws_mqtt_protocol_adapter_5_subscription_op_data_new(struct aws_allocator *allocator, struct aws_byte_cursor topic_filter, struct aws_protocol_adapter_weak_ref *callback_ref) {
    struct aws_mqtt_protocol_adapter_5_subscription_op_data *subscribe_data = aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_protocol_adapter_5_subscription_op_data));

    subscribe_data->allocator = allocator;
    subscribe_data->callback_ref = aws_weak_ref_acquire(callback_ref);
    aws_byte_buf_init_copy_from_cursor(&subscribe_data->topic_filter, allocator, topic_filter);

    return subscribe_data;
}

static void s_aws_mqtt_protocol_adapter_5_subscription_op_data_delete(struct aws_mqtt_protocol_adapter_5_subscription_op_data *subscribe_data) {
    aws_weak_ref_release(subscribe_data->callback_ref);
    aws_byte_buf_clean_up(&subscribe_data->topic_filter);

    aws_mem_release(subscribe_data->allocator, subscribe_data);
}

/* Subscribe */

static void s_protocol_adapter_5_subscribe_completion(const struct aws_mqtt5_packet_suback_view *suback,
                                                      int error_code,
                                                      void *complete_ctx) {
    struct aws_mqtt_protocol_adapter_5_subscription_op_data *subscribe_data = complete_ctx;
    struct aws_mqtt_protocol_adapter_5_impl *adapter = aws_weak_ref_get_reference(subscribe_data->callback_ref);

    if (adapter == NULL) {
        goto done;
    }

    bool success = error_code == AWS_ERROR_SUCCESS && suback != NULL && suback->reason_code_count == 1 && suback->reason_codes[0] <= AWS_MQTT5_SARC_GRANTED_QOS_2;

    struct aws_protocol_adapter_subscription_event subscribe_event = {
        .topic_filter = aws_byte_cursor_from_buf(&subscribe_data->topic_filter),
        .event_type = success ? AWS_PASET_SUBSCRIBE_SUCCESS : AWS_PASET_SUBSCRIBE_FAILURE,
    };

    (*adapter->config.subscription_event_callback)(&subscribe_event, adapter->config.user_data);

done:

    s_aws_mqtt_protocol_adapter_5_subscription_op_data_delete(subscribe_data);
}

int s_aws_mqtt_protocol_adapter_5_subscribe(void *impl, struct aws_protocol_adapter_subscribe_options *options) {
    struct aws_mqtt_protocol_adapter_5_impl *adapter = impl;

    struct aws_mqtt_protocol_adapter_5_subscription_op_data *subscribe_data = s_aws_mqtt_protocol_adapter_5_subscription_op_data_new(adapter->allocator, options->topic_filter, adapter->callback_ref);

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

    s_aws_mqtt_protocol_adapter_5_subscription_op_data_delete(subscribe_data);

    return AWS_OP_ERR;
}

/* Unsubscribe */

static void s_protocol_adapter_5_unsubscribe_completion(const struct aws_mqtt5_packet_unsuback_view *unsuback,
                                                      int error_code,
                                                      void *complete_ctx) {
    struct aws_mqtt_protocol_adapter_5_subscription_op_data *unsubscribe_data = complete_ctx;
    struct aws_mqtt_protocol_adapter_5_impl *adapter = aws_weak_ref_get_reference(unsubscribe_data->callback_ref);

    if (adapter == NULL) {
        goto done;
    }

    bool success = error_code == AWS_ERROR_SUCCESS && unsuback != NULL && unsuback->reason_code_count == 1 && unsuback->reason_codes[0] < 128;

    struct aws_protocol_adapter_subscription_event unsubscribe_event = {
        .topic_filter = aws_byte_cursor_from_buf(&unsubscribe_data->topic_filter),
        .event_type = success ? AWS_PASET_UNSUBSCRIBE_SUCCESS : AWS_PASET_UNSUBSCRIBE_FAILURE,
    };

    (*adapter->config.subscription_event_callback)(&unsubscribe_event, adapter->config.user_data);

done:

    s_aws_mqtt_protocol_adapter_5_subscription_op_data_delete(unsubscribe_data);
}

int s_aws_mqtt_protocol_adapter_5_unsubscribe(void *impl, struct aws_protocol_adapter_unsubscribe_options *options) {
    struct aws_mqtt_protocol_adapter_5_impl *adapter = impl;

    struct aws_mqtt_protocol_adapter_5_subscription_op_data *unsubscribe_data = s_aws_mqtt_protocol_adapter_5_subscription_op_data_new(adapter->allocator, options->topic_filter, adapter->callback_ref);

    struct aws_mqtt5_packet_unsubscribe_view unsubscribe_view = {
        .topic_filters = &options->topic_filter,
        .topic_filter_count = 1,
    };

    struct aws_mqtt5_unsubscribe_completion_options completion_options = {
        .ack_timeout_seconds_override = options->ack_timeout_seconds,
        .completion_callback = s_protocol_adapter_5_unsubscribe_completion,
        .completion_user_data = unsubscribe_data,
    };

    if (aws_mqtt5_client_unsubscribe(adapter->client, &unsubscribe_view, &completion_options)) {
        goto error;
    }

    return AWS_OP_SUCCESS;

error:

    s_aws_mqtt_protocol_adapter_5_subscription_op_data_delete(unsubscribe_data);

    return AWS_OP_ERR;
}

/* Publish */

struct aws_mqtt_protocol_adapter_5_publish_op_data {
    struct aws_allocator *allocator;
    struct aws_protocol_adapter_weak_ref *callback_ref;

    void (*completion_callback_fn)(bool, void *);
    void *user_data;
};

static struct aws_mqtt_protocol_adapter_5_publish_op_data *s_aws_mqtt_protocol_adapter_5_publish_op_data_new(struct aws_allocator *allocator, const struct aws_protocol_adapter_publish_options *publish_options, struct aws_protocol_adapter_weak_ref *callback_ref) {
    struct aws_mqtt_protocol_adapter_5_publish_op_data *publish_data = aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_protocol_adapter_5_publish_op_data));

    publish_data->allocator = allocator;
    publish_data->callback_ref = aws_weak_ref_acquire(callback_ref);
    publish_data->completion_callback_fn = publish_options->completion_callback_fn;
    publish_data->user_data = publish_options->user_data;

    return publish_data;
}

static void s_aws_mqtt_protocol_adapter_5_publish_op_data_delete(struct aws_mqtt_protocol_adapter_5_publish_op_data *publish_data) {
    aws_weak_ref_release(publish_data->callback_ref);

    aws_mem_release(publish_data->allocator, publish_data);
}

static void s_protocol_adapter_5_publish_completion(
    enum aws_mqtt5_packet_type packet_type,
    const void *packet,
    int error_code,
    void *complete_ctx) {
    struct aws_mqtt_protocol_adapter_5_publish_op_data *publish_data = complete_ctx;
    struct aws_mqtt_protocol_adapter_5_impl *adapter = aws_weak_ref_get_reference(publish_data->callback_ref);

    if (adapter == NULL) {
        goto done;
    }

    bool success = false;
    if (error_code == AWS_ERROR_SUCCESS && packet_type == AWS_MQTT5_PT_PUBACK) {
        const struct aws_mqtt5_packet_puback_view *puback = packet;
        if (puback->reason_code < 128) {
            success = true;
        }
    }

    (*publish_data->completion_callback_fn)(success, publish_data->user_data);

done:

    s_aws_mqtt_protocol_adapter_5_publish_op_data_delete(publish_data);
}

int s_aws_mqtt_protocol_adapter_5_publish(void *impl, struct aws_protocol_adapter_publish_options *options) {
    struct aws_mqtt_protocol_adapter_5_impl *adapter = impl;
    struct aws_mqtt_protocol_adapter_5_publish_op_data *publish_data = s_aws_mqtt_protocol_adapter_5_publish_op_data_new(adapter->allocator, options, adapter->callback_ref);

    struct aws_mqtt5_packet_publish_view publish_view = {
        .topic = options->topic,
        .qos = AWS_MQTT5_QOS_AT_LEAST_ONCE,
        .payload = options->payload
    };

    struct aws_mqtt5_publish_completion_options completion_options = {
        .ack_timeout_seconds_override = options->ack_timeout_seconds,
        .completion_callback = s_protocol_adapter_5_publish_completion,
        .completion_user_data = publish_data,
    };

    if (aws_mqtt5_client_publish(adapter->client, &publish_view, &completion_options)) {
        goto error;
    }

    return AWS_OP_SUCCESS;

error:

    s_aws_mqtt_protocol_adapter_5_publish_op_data_delete(publish_data);

    return AWS_OP_ERR;
}

static bool s_protocol_adapter_mqtt5_listener_publish_received(const struct aws_mqtt5_packet_publish_view *publish, void *user_data) {
    struct aws_mqtt_protocol_adapter_5_impl *adapter = user_data;

    struct aws_protocol_adapter_incoming_publish_event publish_event = {
        .topic = publish->topic,
        .payload = publish->payload
    };

    (*adapter->config.incoming_publish_callback)(&publish_event, adapter->config.user_data);

    return false;
}

static void s_protocol_adapter_mqtt5_lifecycle_event_callback(const struct aws_mqtt5_client_lifecycle_event *event) {
    struct aws_mqtt_protocol_adapter_5_impl *adapter = event->user_data;

    if (event->event_type != AWS_MQTT5_CLET_CONNECTION_SUCCESS && event->event_type != AWS_MQTT5_CLET_DISCONNECTION) {
        return;
    }

    bool is_connection_success = event->event_type == AWS_MQTT5_CLET_CONNECTION_SUCCESS;

    struct aws_protocol_adapter_connection_event connection_event = {
        .event_type = is_connection_success ? AWS_PACET_ONLINE : AWS_PACET_OFFLINE,
    };

    if (is_connection_success) {
        connection_event.rejoined_session = event->settings->rejoined_session;
    }

    (*adapter->config.connection_event_callback)(&connection_event, adapter->config.user_data);
}

static void s_protocol_adapter_mqtt5_listener_termination_callback(void *user_data) {
    struct aws_mqtt_protocol_adapter_5_impl *adapter = user_data;

    AWS_FATAL_ASSERT(aws_event_loop_thread_is_callers_thread(adapter->client->loop));

    aws_weak_ref_zero_reference(adapter->callback_ref);
    aws_weak_ref_release(adapter->callback_ref);

    aws_mqtt5_client_release(adapter->client);

    aws_protocol_adapter_terminate_callback_fn *terminate_callback = adapter->config.terminate_callback;
    void *terminate_user_data = adapter->config.user_data;

    aws_mem_release(adapter->allocator, adapter);

    if (terminate_callback) {
        (*terminate_callback)(terminate_user_data);
    }
}

static struct aws_mqtt_protocol_adapter_vtable s_protocol_adapter_mqtt5_vtable = {
    .aws_mqtt_protocol_adapter_delete_fn = s_aws_mqtt_protocol_adapter_5_delete,
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

void aws_mqtt_protocol_adapter_delete(struct aws_mqtt_protocol_adapter *adapter) {
    (*adapter->vtable->aws_mqtt_protocol_adapter_delete_fn)(adapter->impl);
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