/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/v5/mqtt5_client.h>

#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/mqtt/private/v5/mqtt5_client_impl.h>
#include <aws/mqtt/v5/mqtt5_client_config.h>

static void s_mqtt5_client_final_destroy(struct aws_mqtt5_client *client) {
    if (client == NULL) {
        return;
    }

    aws_mqtt5_client_config_destroy((struct aws_mqtt5_client_config *)client->config);
    aws_mutex_clean_up(&client->lock);

    aws_mem_release(client->allocator, client);
}

static void s_on_mqtt5_client_zero_ref_count(void *user_data) {
    struct aws_mqtt5_client *client = user_data;

    /* Async and multi-step eventually */
    s_mqtt5_client_final_destroy(client);
}

struct aws_mqtt5_client *aws_mqtt5_client_new(struct aws_allocator *allocator, struct aws_mqtt5_client_config *config) {
    AWS_FATAL_ASSERT(allocator != NULL);
    AWS_FATAL_ASSERT(config != NULL);

    if (aws_mqtt5_client_config_validate(config)) {
        return NULL;
    }

    struct aws_mqtt5_client *client = aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt5_client));
    if (client == NULL) {
        return NULL;
    }

    client->allocator = allocator;

    aws_ref_count_init(&client->ref_count, client, s_on_mqtt5_client_zero_ref_count);

    client->config = aws_mqtt5_client_config_new_clone(allocator, config);
    if (client->config == NULL) {
        goto on_error;
    }

    /* all client activity will take place on this event loop, serializing things like reconnect, ping, etc... */
    client->loop = aws_event_loop_group_get_next_loop(config->bootstrap->event_loop_group);
    if (client->loop == NULL) {
        goto on_error;
    }

    if (aws_mutex_init(&client->lock)) {
        goto on_error;
    }

    return client;

on_error:

    aws_mqtt5_client_release(client);

    return NULL;
}

struct aws_mqtt5_client *aws_mqtt5_client_acquire(struct aws_mqtt5_client *client) {
    if (client != NULL) {
        aws_ref_count_acquire(&client->ref_count);
    }

    return client;
}

void aws_mqtt5_client_release(struct aws_mqtt5_client *client) {
    if (client != NULL) {
        aws_ref_count_release(&client->ref_count);
    }
}

void aws_mqtt5_client_start(struct aws_mqtt5_client *client) {
    (void)client;
}

void aws_mqtt5_client_stop(struct aws_mqtt5_client *client) {
    (void)client;
}
