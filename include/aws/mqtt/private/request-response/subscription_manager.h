#ifndef AWS_MQTT_PRIVATE_REQUEST_RESPONSE_SUBSCRIPTION_MANAGER_H
#define AWS_MQTT_PRIVATE_REQUEST_RESPONSE_SUBSCRIPTION_MANAGER_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

#include <aws/common/hash_table.h>

struct aws_mqtt_protocol_adapter;
struct aws_protocol_adapter_connection_event;
struct aws_protocol_adapter_subscription_event;

enum aws_rr_subscription_event_type {
    ARRSET_SUBSCRIPTION_SUBSCRIBE_SUCCESS,
    ARRSET_SUBSCRIPTION_SUBSCRIBE_FAILURE,
    ARRSET_SUBSCRIPTION_ENDED
};

struct aws_rr_subscription_status_event {
    enum aws_rr_subscription_event_type type;
    struct aws_byte_cursor topic_filter;
    uint64_t operation_id;
};

/*
 * Invariant: despite being on the same thread, these callbacks must be queued as cross-thread tasks on the native
 * request-response client.  This allows us to iterate internal collections without worrying about external
 * callers disrupting things by invoking APIs back on us.
 */
typedef void(
    aws_rr_subscription_status_event_callbacK_fn)(const struct aws_rr_subscription_status_event *event, void *userdata);

struct aws_rr_subscription_manager_options {
    size_t max_subscriptions;
    uint32_t operation_timeout_seconds;

    aws_rr_subscription_status_event_callbacK_fn *subscription_status_callback;
    void *userdata;
};

/*
 * The subscription manager works from a purely lazy perspective.  Unsubscribes (from topic filters that are no longer
 * referenced) occur when looking for new subscription space.  Unsubscribe failures don't trigger anything special,
 * we'll just try again next time we look for subscription space.  Subscribes are attempted on idle subscriptions
 * that still need them, either in response to a new operation listener or a connection resumption event.
 *
 * We only allow one subscribe or unsubscribe to be outstanding at once for a given topic.  If an operation requires a
 * subscription while an unsubscribe is in progress the operation is blocked until the unsubscribe resolves.
 *
 * These invariants are dropped during shutdown.  In that case, we immediately send unsubscribes for everything
 * that is not already unsubscribing.
 */
struct aws_rr_subscription_manager {
    struct aws_allocator *allocator;

    struct aws_rr_subscription_manager_options config;

    /* non-owning reference; the client is responsible for destroying this asynchronously (listener detachment) */
    struct aws_mqtt_protocol_adapter *protocol_adapter;

    /* &aws_rr_subscription_record.topic_filter_cursor -> aws_rr_subscription_record * */
    struct aws_hash_table subscriptions;

    bool is_protocol_client_connected;
};

enum aws_rr_subscription_type {
    ARRST_EVENT_STREAM,
    ARRST_REQUEST_RESPONSE,
};

struct aws_rr_acquire_subscription_options {
    struct aws_byte_cursor topic_filter;
    uint64_t operation_id;
    enum aws_rr_subscription_type type;
};

struct aws_rr_release_subscription_options {
    struct aws_byte_cursor topic_filter;
    uint64_t operation_id;
};

enum aws_acquire_subscription_result_type {
    AASRT_SUBSCRIBED,
    AASRT_SUBSCRIBING,
    AASRT_BLOCKED,
    AASRT_NO_CAPACITY,
    AASRT_FAILURE
};

AWS_EXTERN_C_BEGIN

int aws_rr_subscription_manager_init(
    struct aws_rr_subscription_manager *manager,
    struct aws_allocator *allocator,
    struct aws_mqtt_protocol_adapter *protocol_adapter,
    const struct aws_rr_subscription_manager_options *options);

void aws_rr_subscription_manager_clean_up(struct aws_rr_subscription_manager *manager);

enum aws_acquire_subscription_result_type aws_rr_subscription_manager_acquire_subscription(
    struct aws_rr_subscription_manager *manager,
    const struct aws_rr_acquire_subscription_options *options);

void aws_rr_subscription_manager_release_subscription(
    struct aws_rr_subscription_manager *manager,
    const struct aws_rr_release_subscription_options *options);

void aws_rr_subscription_manager_on_protocol_adapter_subscription_event(
    struct aws_rr_subscription_manager *manager,
    const struct aws_protocol_adapter_subscription_event *event);

void aws_rr_subscription_manager_on_protocol_adapter_connection_event(
    struct aws_rr_subscription_manager *manager,
    const struct aws_protocol_adapter_connection_event *event);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_PRIVATE_REQUEST_RESPONSE_SUBSCRIPTION_MANAGER_H */
