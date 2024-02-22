/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/private/request-response/subscription_manager.h>

#include <aws/common/logging.h>
#include <aws/mqtt/private/client_impl_shared.h>
#include <aws/mqtt/private/request-response/protocol_adapter.h>

enum aws_rr_subscription_status_type {
    ARRSST_SUBSCRIBED,
    ARRSST_NOT_SUBSCRIBED,
};

/*
 * Invariant: subscriptions can only transition from nothing -> {subscribing, unsubscribing}
 *
 * In particular, the logic blocks subscribing while unsubscribing and unsubscribing while subscribing (unless
 * shutting down).
 */
enum aws_rr_subscription_pending_action_type {
    ARRSPAT_NOTHING,
    ARRSPAT_SUBSCRIBING,
    ARRSPAT_UNSUBSCRIBING,
};

struct aws_rr_subscription_listener {
    struct aws_allocator *allocator;
    uint64_t operation_id;
};

static uint64_t s_aws_hash_subscription_listener(const void *item) {
    const struct aws_rr_subscription_listener *listener = item;

    return listener->operation_id;
}

static bool s_aws_subscription_listener_hash_equality(const void *a, const void *b) {
    const struct aws_rr_subscription_listener *a_listener = a;
    const struct aws_rr_subscription_listener *b_listener = b;

    return a_listener->operation_id == b_listener->operation_id;
}

static void s_aws_subscription_listener_destroy(void *element) {
    struct aws_rr_subscription_listener *listener = element;

    aws_mem_release(listener->allocator, listener);
}

struct aws_rr_subscription_record {
    struct aws_allocator *allocator;

    struct aws_byte_buf topic_filter;
    struct aws_byte_cursor topic_filter_cursor;

    struct aws_hash_table listeners;

    enum aws_rr_subscription_status_type status;
    enum aws_rr_subscription_pending_action_type pending_action;

    enum aws_rr_subscription_type type;
};

static void s_aws_rr_subscription_record_log_invariant_violations(const struct aws_rr_subscription_record *record) {
    if (record->status == ARRSST_SUBSCRIBED && record->pending_action == ARRSPAT_SUBSCRIBING) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT_REQUEST_RESPONSE,
            "MQTT request response subscription ('" PRInSTR "') invalid state",
            AWS_BYTE_CURSOR_PRI(record->topic_filter_cursor));
    }
}

static void s_aws_rr_subscription_record_destroy(void *element) {
    struct aws_rr_subscription_record *record = element;

    aws_byte_buf_clean_up(&record->topic_filter);
    aws_hash_table_clean_up(&record->listeners);

    aws_mem_release(record->allocator, record);
}

static struct aws_rr_subscription_record *s_aws_rr_subscription_new(
    struct aws_allocator *allocator,
    const struct aws_rr_acquire_subscription_options *options) {
    struct aws_rr_subscription_record *record = aws_mem_calloc(allocator, 1, sizeof(struct aws_rr_subscription_record));
    record->allocator = allocator;

    aws_byte_buf_init_copy_from_cursor(&record->topic_filter, allocator, options->topic_filter);
    record->topic_filter_cursor = aws_byte_cursor_from_buf(&record->topic_filter);

    aws_hash_table_init(
        &record->listeners,
        allocator,
        4,
        s_aws_hash_subscription_listener,
        s_aws_subscription_listener_hash_equality,
        NULL,
        s_aws_subscription_listener_destroy);

    record->status = ARRSST_NOT_SUBSCRIBED;
    record->pending_action = ARRSPAT_NOTHING;

    record->type = options->type;

    return record;
}

static void s_subscription_record_unsubscribe(
    struct aws_rr_subscription_manager *manager,
    struct aws_rr_subscription_record *record,
    bool shutdown) {

    bool currently_subscribed = record->status == ARRSST_SUBSCRIBED;
    bool currently_subscribing = record->pending_action == ARRSPAT_SUBSCRIBING;
    bool currently_unsubscribing = record->pending_action == ARRSPAT_UNSUBSCRIBING;

    /*
     * The difference between a shutdown unsubscribe and a normal unsubscribe is that on a shutdown we will "chase"
     * a pending subscribe with an unsubscribe (breaking the invariant of never having multiple MQTT operations
     * pending on a subscription).
     */
    bool should_unsubscribe = currently_subscribed && !currently_unsubscribing;
    if (shutdown) {
        should_unsubscribe = should_unsubscribe || currently_subscribing;
    }

    if (!should_unsubscribe) {
        return;
    }

    struct aws_protocol_adapter_unsubscribe_options unsubscribe_options = {
        .topic_filter = record->topic_filter_cursor,
        .ack_timeout_seconds = manager->config.operation_timeout_seconds,
    };

    if (aws_mqtt_protocol_adapter_unsubscribe(manager->protocol_adapter, &unsubscribe_options)) {
        return;
    }

    record->pending_action = ARRSPAT_UNSUBSCRIBING;

    // check_invariants may no longer be true now because we might have converted a pending subscribe to a pending
    // unsubscribe
}

/* Only called when shutting down the client */
static int s_rr_subscription_clean_up_foreach_wrap(void *context, struct aws_hash_element *elem) {
    struct aws_rr_subscription_manager *manager = context;
    struct aws_rr_subscription_record *subscription = elem->value;

    s_subscription_record_unsubscribe(manager, subscription, true);

    return AWS_COMMON_HASH_TABLE_ITER_CONTINUE | AWS_COMMON_HASH_TABLE_ITER_DELETE;
}

static struct aws_rr_subscription_record *s_get_subscription_record(
    struct aws_rr_subscription_manager *manager,
    struct aws_byte_cursor topic_filter) {
    struct aws_rr_subscription_record *subscription = NULL;
    struct aws_hash_element *element = NULL;
    if (aws_hash_table_find(&manager->subscriptions, &topic_filter, &element)) {
        return NULL;
    }

    if (element != NULL) {
        subscription = element->value;
    }

    return subscription;
}

struct aws_subscription_stats {
    size_t request_response_subscriptions;
    size_t event_stream_subscriptions;
};

static int s_rr_subscription_count_foreach_wrap(void *context, struct aws_hash_element *elem) {
    const struct aws_rr_subscription_record *subscription = elem->value;
    struct aws_subscription_stats *stats = context;

    if (subscription->type == ARRST_EVENT_STREAM) {
        ++stats->event_stream_subscriptions;
    } else {
        ++stats->request_response_subscriptions;
    }

    return AWS_COMMON_HASH_TABLE_ITER_CONTINUE;
}

static void s_get_subscription_stats(
    struct aws_rr_subscription_manager *manager,
    struct aws_subscription_stats *stats) {
    AWS_ZERO_STRUCT(*stats);

    aws_hash_table_foreach(&manager->subscriptions, s_rr_subscription_count_foreach_wrap, stats);
}

static void s_remove_listener_from_subscription_record(
    struct aws_rr_subscription_manager *manager,
    struct aws_byte_cursor topic_filter,
    uint64_t operation_id) {
    struct aws_rr_subscription_record *record = s_get_subscription_record(manager, topic_filter);
    if (record == NULL) {
        return;
    }

    struct aws_rr_subscription_listener listener = {
        .operation_id = operation_id,
    };

    aws_hash_table_remove(&record->listeners, &listener, NULL, NULL);
}

static void s_add_listener_to_subscription_record(struct aws_rr_subscription_record *record, uint64_t operation_id) {
    struct aws_rr_subscription_listener *listener =
        aws_mem_calloc(record->allocator, 1, sizeof(struct aws_rr_subscription_listener));
    listener->allocator = record->allocator;
    listener->operation_id = operation_id;

    aws_hash_table_put(&record->listeners, listener, listener, NULL);
}

static int s_rr_subscription_cull_unused_subscriptions_wrapper(void *context, struct aws_hash_element *elem) {
    struct aws_rr_subscription_record *record = elem->value;
    struct aws_rr_subscription_manager *manager = context;

    if (aws_hash_table_get_entry_count(&record->listeners) == 0) {
        if (manager->is_protocol_client_connected) {
            s_subscription_record_unsubscribe(manager, record, false);
        }

        if (record->status == ARRSST_NOT_SUBSCRIBED && record->pending_action == ARRSPAT_NOTHING) {
            return AWS_COMMON_HASH_TABLE_ITER_CONTINUE | AWS_COMMON_HASH_TABLE_ITER_DELETE;
        }
    }

    return AWS_COMMON_HASH_TABLE_ITER_CONTINUE;
}

static void s_cull_unused_subscriptions(struct aws_rr_subscription_manager *manager) {
    aws_hash_table_foreach(&manager->subscriptions, s_rr_subscription_cull_unused_subscriptions_wrapper, manager);
}

static int s_rr_activate_idle_subscription(
    struct aws_rr_subscription_manager *manager,
    struct aws_rr_subscription_record *record) {
    int result = AWS_OP_SUCCESS;

    if (manager->is_protocol_client_connected && aws_hash_table_get_entry_count(&record->listeners) > 0) {
        if (record->status == ARRSST_NOT_SUBSCRIBED && record->pending_action == ARRSPAT_NOTHING) {
            struct aws_protocol_adapter_subscribe_options subscribe_options = {
                .topic_filter = record->topic_filter_cursor,
                .ack_timeout_seconds = manager->config.operation_timeout_seconds,
            };

            result = aws_mqtt_protocol_adapter_subscribe(manager->protocol_adapter, &subscribe_options);
            if (result == AWS_OP_SUCCESS) {
                record->pending_action = ARRSPAT_SUBSCRIBING;
            }
        }
    }

    return result;
}

static void s_emit_subscription_event(
    const struct aws_rr_subscription_manager *manager,
    const struct aws_rr_subscription_record *record,
    enum aws_rr_subscription_event_type type) {

    for (struct aws_hash_iter iter = aws_hash_iter_begin(&record->listeners); !aws_hash_iter_done(&iter);
         aws_hash_iter_next(&iter)) {

        struct aws_rr_subscription_listener *listener = iter.element.value;
        struct aws_rr_subscription_status_event event = {
            .type = type,
            .topic_filter = record->topic_filter_cursor,
            .operation_id = listener->operation_id,
        };

        (*manager->config.subscription_status_callback)(&event, manager->config.userdata);
    }
}

enum aws_acquire_subscription_result_type aws_rr_subscription_manager_acquire_subscription(
    struct aws_rr_subscription_manager *manager,
    const struct aws_rr_acquire_subscription_options *options) {
    struct aws_rr_subscription_record *existing_record = s_get_subscription_record(manager, options->topic_filter);

    // is no subscription present?
    if (existing_record == NULL) {
        s_cull_unused_subscriptions(manager);

        // is the budget used up?
        struct aws_subscription_stats stats;
        s_get_subscription_stats(manager, &stats);

        bool space_for_subscription =
            stats.event_stream_subscriptions + stats.request_response_subscriptions < manager->config.max_subscriptions;
        if (options->type == ARRST_EVENT_STREAM) {
            // event stream subscriptions cannot hog the entire subscription budget
            space_for_subscription =
                space_for_subscription && (stats.event_stream_subscriptions + 1 < manager->config.max_subscriptions);
        }

        if (!space_for_subscription) {
            // could space eventually free up?
            if (options->type == ARRST_REQUEST_RESPONSE || stats.request_response_subscriptions > 1) {
                return AASRT_BLOCKED;
            } else {
                return AASRT_NO_CAPACITY;
            }
        }

        // create-and-add subscription
        existing_record = s_aws_rr_subscription_new(manager->allocator, options);
        aws_hash_table_put(&manager->subscriptions, &existing_record->topic_filter_cursor, existing_record, NULL);
    }

    AWS_FATAL_ASSERT(existing_record != NULL);
    AWS_FATAL_ASSERT(existing_record->type == options->type);
    s_aws_rr_subscription_record_log_invariant_violations(existing_record);

    // for simplicity, we require unsubscribes to complete before re-subscribing
    if (existing_record->pending_action == ARRSPAT_UNSUBSCRIBING) {
        return AASRT_BLOCKED;
    }

    // register the operation as a listener
    s_add_listener_to_subscription_record(existing_record, options->operation_id);
    if (existing_record->status == ARRSST_SUBSCRIBED) {
        return AASRT_SUBSCRIBED;
    }

    // do we need to send a subscribe?
    if (s_rr_activate_idle_subscription(manager, existing_record)) {
        s_emit_subscription_event(manager, existing_record, ARRSET_SUBSCRIPTION_SUBSCRIBE_FAILURE);
        return AASRT_FAILURE;
    }

    s_aws_rr_subscription_record_log_invariant_violations(existing_record);

    return AASRT_SUBSCRIBING;
}

void aws_rr_subscription_manager_release_subscription(
    struct aws_rr_subscription_manager *manager,
    const struct aws_rr_release_subscription_options *options) {
    s_remove_listener_from_subscription_record(manager, options->topic_filter, options->operation_id);
}

void aws_rr_subscription_manager_on_protocol_adapter_subscription_event(
    struct aws_rr_subscription_manager *manager,
    const struct aws_protocol_adapter_subscription_event *event) {
    struct aws_rr_subscription_record *record = s_get_subscription_record(manager, event->topic_filter);
    if (record == NULL) {
        return;
    }

    if (event->event_type == AWS_PASET_SUBSCRIBE) {
        AWS_FATAL_ASSERT(record->pending_action == ARRSPAT_SUBSCRIBING);

        if (event->error_code == AWS_ERROR_SUCCESS) {
            record->status = ARRSST_SUBSCRIBED;
            s_emit_subscription_event(manager, record, ARRSET_SUBSCRIPTION_SUBSCRIBE_SUCCESS);
        } else {
            s_emit_subscription_event(manager, record, ARRSET_SUBSCRIPTION_SUBSCRIBE_FAILURE);
        }
    } else if (event->event_type == AWS_PASET_UNSUBSCRIBE) {
        AWS_FATAL_ASSERT(record->pending_action == ARRSPAT_UNSUBSCRIBING);

        if (event->error_code == AWS_ERROR_SUCCESS) {
            record->status = ARRSST_NOT_SUBSCRIBED;
            s_emit_subscription_event(manager, record, ARRSET_SUBSCRIPTION_ENDED);
        }
    }

    record->pending_action = ARRSPAT_NOTHING;

    s_aws_rr_subscription_record_log_invariant_violations(record);
}

static int s_rr_activate_idle_subscriptions_wrapper(void *context, struct aws_hash_element *elem) {
    struct aws_rr_subscription_record *record = elem->value;
    struct aws_rr_subscription_manager *manager = context;

    s_rr_activate_idle_subscription(manager, record);

    s_aws_rr_subscription_record_log_invariant_violations(record);

    return AWS_COMMON_HASH_TABLE_ITER_CONTINUE;
}

static void s_activate_idle_subscriptions(struct aws_rr_subscription_manager *manager) {
    aws_hash_table_foreach(&manager->subscriptions, s_rr_activate_idle_subscriptions_wrapper, manager);
}

static int s_apply_session_lost_wrapper(void *context, struct aws_hash_element *elem) {
    struct aws_rr_subscription_record *record = elem->value;
    struct aws_rr_subscription_manager *manager = context;

    if (record->status == ARRSST_SUBSCRIBED) {
        record->status = ARRSST_NOT_SUBSCRIBED;
        s_emit_subscription_event(manager, record, ARRSET_SUBSCRIPTION_ENDED);

        if (record->pending_action != ARRSPAT_UNSUBSCRIBING) {
            return AWS_COMMON_HASH_TABLE_ITER_CONTINUE | AWS_COMMON_HASH_TABLE_ITER_DELETE;
        }
    }

    return AWS_COMMON_HASH_TABLE_ITER_CONTINUE;
}

static void s_apply_session_lost(struct aws_rr_subscription_manager *manager) {
    aws_hash_table_foreach(&manager->subscriptions, s_apply_session_lost_wrapper, manager);
}

void aws_rr_subscription_manager_on_protocol_adapter_connection_event(
    struct aws_rr_subscription_manager *manager,
    const struct aws_protocol_adapter_connection_event *event) {
    if (event->event_type == AWS_PACET_CONNECTED) {
        manager->is_protocol_client_connected = true;
        if (!event->joined_session) {
            s_apply_session_lost(manager);
        }

        s_cull_unused_subscriptions(manager);
        s_activate_idle_subscriptions(manager);
    } else {
        manager->is_protocol_client_connected = false;
    }
}

int aws_rr_subscription_manager_init(
    struct aws_rr_subscription_manager *manager,
    struct aws_allocator *allocator,
    struct aws_mqtt_protocol_adapter *protocol_adapter,
    const struct aws_rr_subscription_manager_options *options) {
    AWS_ZERO_STRUCT(*manager);

    if (options == NULL || options->max_subscriptions < 1 || options->operation_timeout_seconds == 0) {
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    manager->allocator = allocator;
    manager->config = *options;
    manager->protocol_adapter = protocol_adapter;

    aws_hash_table_init(
        &manager->subscriptions,
        allocator,
        options->max_subscriptions,
        aws_hash_byte_cursor_ptr,
        aws_mqtt_byte_cursor_hash_equality,
        NULL,
        s_aws_rr_subscription_record_destroy);

    manager->is_protocol_client_connected = aws_mqtt_protocol_adapter_is_connected(protocol_adapter);

    return AWS_OP_SUCCESS;
}

void aws_rr_subscription_manager_clean_up(struct aws_rr_subscription_manager *manager) {
    aws_hash_table_foreach(&manager->subscriptions, s_rr_subscription_clean_up_foreach_wrap, manager->protocol_adapter);
    aws_hash_table_clean_up(&manager->subscriptions);
}
