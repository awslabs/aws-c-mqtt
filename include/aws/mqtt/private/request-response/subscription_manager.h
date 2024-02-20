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

enum aws_rr_subscription_status {
    ARRSS_INVALID,
    ARRSS_SUBSCRIBING,
    ARRSS_SUBSCRIBED,
    ARRSS_UNSUBSCRIBING,
};

enum aws_rr_subscription_type {
    ARRST_EVENT_STREAM,
    ARRST_REQUEST_RESPONSE,
};

struct aws_rr_subscription {
    struct aws_allocator *allocator;

    struct aws_byte_buf topic_filter;
    struct aws_byte_cursor topic_filter_cursor;

    /* operation ids (uint64_t) */
    struct aws_array_list listening_operations;

    enum aws_rr_subscription_status status;
};

enum aws_acquire_subscription_result_type {
    AASRT_SUBSCRIBED,
    AASRT_SUBSCRIBING,
    AASRT_BLOCKED,
    AASRT_NO_CAPACITY,
    AASRT_FAILURE
};

struct aws_rr_subscription_status_event {
    struct aws_byte_cursor topic_filter;
    uint64_t operation_id;
    enum aws_rr_subscription_status status;
};

typedef void (aws_rr_subscription_status_event_callbacK_fn)(struct aws_rr_subscription_status_event, void *userdata);

struct aws_rr_subscription_manager_options {
    size_t max_subscriptions;
    uint32_t operation_timeout_seconds;

    aws_rr_subscription_status_event_callbacK_fn *subscription_status_callback;
    void *userdata;
};

struct aws_rr_subscription_manager {
    struct aws_allocator *allocator;

    struct aws_rr_subscription_manager_options config;

    /* non-owning reference; the client is responsible for destroying this asynchronously (listener detachment) */
    struct aws_mqtt_protocol_adapter *protocol_adapter;

    /* &aws_request_response_subscription.topic_filter_cursor -> aws_request_response_subscription * */
    struct aws_hash_table subscriptions;
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

AWS_EXTERN_C_BEGIN

int aws_rr_subscription_manager_init(struct aws_rr_subscription_manager *manager, struct aws_allocator *allocator, struct aws_mqtt_protocol_adapter *protocol_adapter, struct aws_rr_subscription_manager_options *options);

void aws_rr_subscription_manager_clean_up(struct aws_rr_subscription_manager *manager);

enum aws_acquire_subscription_result_type aws_rr_subscription_manager_acquire_subscription(struct aws_rr_subscription_manager *manager, struct aws_rr_acquire_subscription_options *options);

void aws_rr_subscription_manager_release_subscription(struct aws_rr_subscription_manager *manager, struct aws_rr_release_subscription_options *options);

void aws_rr_subscription_manager_on_protocol_adapter_subscription_event(struct aws_rr_subscription_manager *manager, struct aws_protocol_adapter_subscription_event *event);

void aws_rr_subscription_manager_on_protocol_adapter_connection_event(struct aws_rr_subscription_manager *manager, struct aws_protocol_adapter_connection_event *event);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_PRIVATE_REQUEST_RESPONSE_SUBSCRIPTION_MANAGER_H */
