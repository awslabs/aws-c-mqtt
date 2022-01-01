/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/v5/mqtt5_client.h>

#include <aws/mqtt/private/v5/mqtt5_client_impl.h>

static void s_mqtt5_client_final_destroy(struct aws_mqtt5_client *client) {
    if (client == NULL) {
        return;
    }

    aws_mqtt5_client_config_destroy(client->config);

    aws_mem_release(client->allocator, client);
}

static void s_on_mqtt5_client_zero_ref_count(void *user_data) {
    struct aws_mqtt5_client *client = user_data;

    s_mqtt5_client_final_destroy(client);
}

struct aws_mqtt5_client *aws_mqtt5_client_new(struct aws_allocator *allocator, struct aws_mqtt5_client_config *config) {
    AWS_FATAL_ASSERT(allocator != NULL);
    AWS_FATAL_ASSERT(config != NULL);

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
