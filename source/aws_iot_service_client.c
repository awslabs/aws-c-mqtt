/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/aws_iot_service_client.h>

#include <aws/common/hash_table.h>
#include <aws/common/linked_list.h>

struct aws_iot_service_client {
    struct aws_allocator *allocator;

    struct aws_ref_count ref_count;

    struct aws_hash_table event_subscriptions;
};

enum aws_iot_service_client_event_subscription_state {
    AWS_ISC_ESC_INITIAL,
    AWS_ISC_ESC_SUBSCRIBE_PENDING,
    AWS_ISC_ESC_SUBSCRIBED,
    AWS_ISC_ESC_UNSUBSCRIBE_PENDING
};

struct aws_iot_service_client_event_subscription {
    enum aws_iot_service_client_event_subscription_state state;

    struct aws_linked_list node;

    struct aws_byte_cursor topic_name_cursor;
    struct aws_byte_buf topic_name_buffer;

};

static void s_aws_iot_service_client_destroy(void *client) {
    if (client == NULL) {
        return;
    }

    struct aws_iot_service_client *service_client = client;

    aws_mem_release(service_client->allocator, client);
}

struct aws_iot_service_client *aws_iot_service_client_acquire(struct aws_iot_service_client *client) {
    if (client != NULL) {
        aws_ref_count_acquire(&client->ref_count);
    }

    return client;
}

struct aws_iot_service_client *aws_iot_service_client_release(struct aws_iot_service_client *client) {
    if (client != NULL) {
        aws_ref_count_release(&client->ref_count);
    }

    return NULL;
}

int aws_iot_service_client_subscribe_to_event_stream(
    struct aws_iot_service_client *client,
    const struct aws_iot_service_client_subscribe_to_event_config *options) {
    (void)client;
    (void)options;

    return aws_raise_error(AWS_ERROR_UNIMPLEMENTED);
}

int aws_iot_service_client_unsubscribe_from_event_stream(
    struct aws_iot_service_client *client,
    const struct aws_iot_service_client_unsubscribe_from_event_config *options) {
    (void)client;
    (void)options;

    return aws_raise_error(AWS_ERROR_UNIMPLEMENTED);
}

int aws_iot_service_client_submit_request(
    struct aws_iot_service_client *client,
    const struct aws_iot_service_client_make_request_config *options) {
    (void)client;
    (void)options;

    return aws_raise_error(AWS_ERROR_UNIMPLEMENTED);
}

struct aws_iot_service_client *aws_iot_service_client_new(
    struct aws_allocator *allocator,
    const struct aws_iot_service_client_config *options) {
    (void) options;

    struct aws_iot_service_client *client = aws_mem_calloc(allocator, 1, sizeof(struct aws_iot_service_client));

    client->allocator = allocator;

    aws_ref_count_init(&client->ref_count, client, s_aws_iot_service_client_destroy);

    return client;
}
