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

/*
 * The kind of subscription event being emitted.
 */
enum aws_rr_subscription_event_type {

    /*
     * A subscribe succeeded
     */
    ARRSET_SUBSCRIPTION_SUBSCRIBE_SUCCESS,

    /*
     * A subscribe failed
     */
    ARRSET_SUBSCRIPTION_SUBSCRIBE_FAILURE,

    /*
     * A previously successful subscription has ended (generally due to a failure to resume a session)
     */
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
    aws_rr_subscription_status_event_callback_fn)(const struct aws_rr_subscription_status_event *event, void *userdata);

struct aws_rr_subscription_manager_options {

    /*
     * Maximum number of concurrent subscriptions allowed
     */
    size_t max_subscriptions;

    /*
     * Ack timeout to use for all subscribe and unsubscribe operations
     */
    uint32_t operation_timeout_seconds;

    aws_rr_subscription_status_event_callback_fn *subscription_status_callback;
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

    /*
     * The requested subscription already exists and is active.  The operation can proceed to the next stage.
     */
    AASRT_SUBSCRIBED,

    /*
     * The requested subscription now exists but is not yet active.  The operation must wait for the subscribe
     * to complete as success or failure.
     */
    AASRT_SUBSCRIBING,

    /*
     * The subscription does not exist and there is no room for it currently.  Room may open up in the future, so
     * the operation should wait.
     */
    AASRT_BLOCKED,

    /*
     * The subscription does not exist, there is no room for it, and unless an event stream subscription gets
     * closed, no room will be available in the future.  The operation should be failed.
     */
    AASRT_NO_CAPACITY,

    /*
     * An internal failure occurred while trying to establish the subscription.  The operation should be failed.
     */
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
