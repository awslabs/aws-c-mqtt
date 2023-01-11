/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/aws_iot_service_client.h>

#include <aws/common/hash_table.h>
#include <aws/common/linked_list.h>
#include <aws/mqtt/private/v5/mqtt5_client_impl.h>
#include <aws/mqtt/v5/mqtt5_client.h>
#include <aws/mqtt/v5/mqtt5_listener.h>

struct aws_iot_service_client_config_storage {
    size_t max_event_subscriptions;
    size_t max_request_concurrency;
};

static void s_aws_iot_service_client_config_storage_init(
    struct aws_iot_service_client_config_storage *storage,
    const struct aws_iot_service_client_config *options) {
    storage->max_event_subscriptions = options->max_event_subscriptions;
    storage->max_request_concurrency = options->max_request_concurrency;
}

static void s_aws_iot_service_client_config_storage_clean_up(struct aws_iot_service_client_config_storage *storage) {
    (void)storage;
}

struct aws_iot_service_client {
    struct aws_allocator *allocator;

    struct aws_ref_count ref_count;

    struct aws_iot_service_client_config_storage config;
    struct aws_event_loop *loop;
    struct aws_mqtt5_listener *client_listener;

    struct aws_linked_list event_subscription_queue;
    struct aws_linked_list event_subscriptions;
    struct aws_hash_table event_subscriptions_by_topic;
};

enum aws_iot_service_client_event_subscription_state {
    AWS_ISC_ESC_INITIAL,
    AWS_ISC_ESC_SUBSCRIBE_PENDING,
    AWS_ISC_ESC_SUBSCRIBED,
    AWS_ISC_ESC_UNSUBSCRIBE_PENDING
};

struct aws_iot_service_client_event_subscription {
    struct aws_byte_cursor topic_name_cursor;
    struct aws_byte_buf topic_name_buffer;

    struct aws_linked_list node;

    enum aws_mqtt5_qos qos;

    enum aws_iot_service_client_event_subscription_state state;
};

int aws_iot_service_client_subscribe_to_event(
    struct aws_iot_service_client *client,
    const struct aws_iot_service_client_event_subscribe_config *options) {
    (void)client;
    (void)options;

    return aws_raise_error(AWS_ERROR_UNIMPLEMENTED);
}

int aws_iot_service_client_unsubscribe_from_event(
    struct aws_iot_service_client *client,
    const struct aws_iot_service_client_event_unsubscribe_config *options) {
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

static bool s_aws_iot_service_client_publish_received(
    const struct aws_mqtt5_packet_publish_view *publish,
    void *user_data) {
    (void)publish;
    (void)user_data;

    return false;
}

static void s_aws_iot_service_client_connection_event_callback(const struct aws_mqtt5_client_lifecycle_event *event) {
    (void)event;
}

static void s_on_mqtt5_listener_termination_completion_fn(void *complete_ctx) {
    struct aws_iot_service_client *service_client = complete_ctx;

    s_aws_iot_service_client_config_storage_clean_up(&service_client->config);

    aws_hash_table_clean_up(&service_client->event_subscriptions_by_topic);

    aws_mem_release(service_client->allocator, service_client);
}

static void s_aws_iot_service_client_start_shutdown(void *client) {
    if (client == NULL) {
        return;
    }

    struct aws_iot_service_client *service_client = client;

    if (service_client->client_listener != NULL) {
        aws_mqtt5_listener_release(service_client->client_listener);
    } else {
        s_on_mqtt5_listener_termination_completion_fn(service_client);
    }
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

static bool s_aws_iot_client_topic_hash_equality_fn(const void *a, const void *b) {
    const struct aws_byte_cursor *a_cursor = a;
    const struct aws_byte_cursor *b_cursor = b;

    return aws_byte_cursor_eq(a_cursor, b_cursor);
}

struct aws_iot_service_client *aws_iot_service_client_new(
    struct aws_allocator *allocator,
    const struct aws_iot_service_client_config *options) {

    struct aws_iot_service_client *client = aws_mem_calloc(allocator, 1, sizeof(struct aws_iot_service_client));

    client->allocator = allocator;

    aws_ref_count_init(&client->ref_count, client, s_aws_iot_service_client_start_shutdown);

    s_aws_iot_service_client_config_storage_init(&client->config, options);

    struct aws_mqtt5_listener_config listener_config = {
        .client = options->mqtt5_client,
        .listener_callbacks =
            {
                .listener_publish_received_handler = s_aws_iot_service_client_publish_received,
                .listener_publish_received_handler_user_data = client,
                .lifecycle_event_handler = s_aws_iot_service_client_connection_event_callback,
                .lifecycle_event_handler_user_data = client,
            },
        .termination_callback = s_on_mqtt5_listener_termination_completion_fn,
        .termination_callback_user_data = client,
    };

    aws_linked_list_init(&client->event_subscription_queue);
    aws_linked_list_init(&client->event_subscriptions);

    if (aws_hash_table_init(
            &client->event_subscriptions_by_topic,
            allocator,
            sizeof(struct aws_iot_service_client_event_subscription *),
            aws_hash_byte_cursor_ptr,
            s_aws_iot_client_topic_hash_equality_fn,
            NULL,
            NULL)) {
        goto done;
    }

    client->client_listener = aws_mqtt5_listener_new(allocator, &listener_config);
    AWS_FATAL_ASSERT(client->client_listener != NULL);

    client->loop = aws_mqtt5_client_get_event_loop(options->mqtt5_client);

    return client;

done:

    s_aws_iot_service_client_start_shutdown(client);

    return NULL;
}
