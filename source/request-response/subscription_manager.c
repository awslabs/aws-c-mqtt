/**
* Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
* SPDX-License-Identifier: Apache-2.0.
*/

#include <aws/mqtt/private/request-response/subscription_manager.h>

#include <aws/mqtt/private/client_impl_shared.h>
#include <aws/mqtt/private/request-response/protocol_adapter.h>

static void s_aws_rr_subscription_destroy(void *element) {
    struct aws_rr_subscription *subscription = element;

    aws_byte_buf_clean_up(&subscription->topic_filter);
    aws_array_list_clean_up(&subscription->listening_operations);

    aws_mem_release(subscription->allocator, subscription);
}

int aws_rr_subscription_manager_init(struct aws_rr_subscription_manager *manager, struct aws_allocator *allocator, struct aws_mqtt_protocol_adapter *protocol_adapter, struct aws_rr_subscription_manager_options *options) {
    AWS_ZERO_STRUCT(*manager);

    if (options == NULL || options->max_subscriptions < 2 || options->operation_timeout_seconds == 0) {
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    manager->allocator = allocator;
    manager->config = *options;
    manager->protocol_adapter = protocol_adapter;

    if (aws_hash_table_init(&manager->subscriptions, allocator, options->max_subscriptions,         aws_hash_byte_cursor_ptr,
                            aws_mqtt_byte_cursor_hash_equality, NULL, s_aws_rr_subscription_destroy)) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

static int s_rr_subscription_clean_up_foreach_wrap(void *context, struct aws_hash_element *elem) {
    struct aws_rr_subscription_manager *manager = context;
    struct aws_rr_subscription *subscription = elem->value;

    struct aws_protocol_adapter_unsubscribe_options unsubscribe_options = {
        .topic_filter = subscription->topic_filter_cursor,
        .ack_timeout_seconds = manager->config.operation_timeout_seconds,
    };

    aws_mqtt_protocol_adapter_unsubscribe(manager->protocol_adapter, &unsubscribe_options);

    return AWS_COMMON_HASH_TABLE_ITER_CONTINUE | AWS_COMMON_HASH_TABLE_ITER_DELETE;
}

void aws_rr_subscription_manager_clean_up(struct aws_rr_subscription_manager *manager) {
    aws_hash_table_foreach(&manager->subscriptions, s_rr_subscription_clean_up_foreach_wrap, manager->protocol_adapter);
    aws_hash_table_clean_up(&manager->subscriptions);
}


enum aws_acquire_subscription_result_type aws_rr_subscription_manager_acquire_subscription(struct aws_rr_subscription_manager *manager, struct aws_rr_acquire_subscription_options *options) {
    (void)manager;
    (void)options;

    return AASRT_FAILURE;
}

void aws_rr_subscription_manager_release_subscription(struct aws_rr_subscription_manager *manager, struct aws_rr_release_subscription_options *options) {
    (void)manager;
    (void)options;
}

void aws_rr_subscription_manager_on_protocol_adapter_subscription_event(struct aws_rr_subscription_manager *manager, struct aws_protocol_adapter_subscription_event *event) {
    (void)manager;
    (void)event;
}

void aws_rr_subscription_manager_on_protocol_adapter_connection_event(struct aws_rr_subscription_manager *manager, struct aws_protocol_adapter_connection_event *event) {
    (void)manager;
    (void)event;
}