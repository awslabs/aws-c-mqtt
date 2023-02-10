/**
* Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
* SPDX-License-Identifier: Apache-2.0.
*/

#include "aws/mqtt/private/v311/mqtt311_client_impl.h"



struct aws_mqtt311_client *aws_mqtt311_client_new(
    struct aws_allocator *allocator,
    const struct aws_mqtt311_client_options *options) {
    (void)allocator;
    (void)options;

    return NULL;
}

struct aws_mqtt311_client *aws_mqtt311_client_acquire(struct aws_mqtt311_client *client) {
    if (client != NULL) {
        aws_ref_count_acquire(&client->ref_count);
    }

    return client;
}

struct aws_mqtt311_client *aws_mqtt311_client_release(struct aws_mqtt311_client *client) {
    if (client != NULL) {
        aws_ref_count_release(&client->ref_count);
    }

    return NULL;
}
