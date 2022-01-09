/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/private/v5/mqtt5_operation.h>

struct aws_mqtt5_operation *aws_mqtt5_operation_acquire(struct aws_mqtt5_operation *operation) {
    if (operation == NULL) {
        return NULL;
    }

    aws_ref_count_acquire(&operation->ref_count);

    return operation;
}

struct aws_mqtt5_operation *aws_mqtt5_operation_release(struct aws_mqtt5_operation *operation) {
    if (operation != NULL) {
        aws_ref_count_release(&operation->ref_count);
    }

    return NULL;
}
