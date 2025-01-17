#ifndef AWS_MQTT_PRIVATE_REQUEST_RESPONSE_REQUEST_RESPONSE_SUBSCRIPTION_SET_H
#define AWS_MQTT_PRIVATE_REQUEST_RESPONSE_REQUEST_RESPONSE_SUBSCRIPTION_SET_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/byte_buf.h>
#include <aws/common/hash_table.h>
#include <aws/common/linked_list.h>
#include <aws/mqtt/exports.h>

/*
 * This is the (key and) value in hash table (4) above.
 */
struct aws_rr_operation_list_topic_filter_entry {
    struct aws_allocator *allocator;

    struct aws_byte_cursor topic_filter_cursor;
    struct aws_byte_buf topic_filter;

    struct aws_linked_list operations;
};

AWS_EXTERN_C_BEGIN

AWS_MQTT_API void search(const struct aws_hash_table *subscriptions, const struct aws_byte_cursor *topic);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_PRIVATE_REQUEST_RESPONSE_REQUEST_RESPONSE_SUBSCRIPTION_SET_H */
