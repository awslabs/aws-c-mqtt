/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#ifndef AWS_MQTT_MQTT3_TO_MQTT5_ADAPTER_SUBSCRIPTION_SET_H
#define AWS_MQTT_MQTT3_TO_MQTT5_ADAPTER_SUBSCRIPTION_SET_H

#include <aws/mqtt/mqtt.h>

#include <aws/common/hash_table.h>
#include <aws/mqtt/client.h>

struct aws_mqtt3_to_mqtt5_adapter_subscription_options {
    struct aws_byte_cursor topic_filter;

    aws_mqtt_client_publish_received_fn *on_publish_received;
    aws_mqtt_userdata_cleanup_fn *on_cleanup;

    void *callback_user_data;
};

struct aws_mqtt3_to_mqtt5_adapter_publish_received_options {
    struct aws_mqtt_client_connection *connection;

    struct aws_byte_cursor topic;
    enum aws_mqtt_qos qos;
    bool retain;
    bool dup;

    struct aws_byte_cursor payload;
};

struct aws_mqtt3_to_mqtt5_adapter_subscription_set_node {
    struct aws_allocator *allocator;

    struct aws_byte_cursor topic_segment_cursor; // segment can be empty
    struct aws_byte_buf topic_segment;

    struct aws_mqtt3_to_mqtt5_adapter_subscription_set_node *parent;
    struct aws_hash_table children; // (node's byte_cursor -> node)

    size_t ref_count;

    bool is_subscription; // or does callback_fn determine?  Technically you could subscribe with no callback and we
                          // either need to reject (no) or have  flag

    aws_mqtt_client_publish_received_fn *on_publish_received;
    aws_mqtt_userdata_cleanup_fn *on_cleanup;

    void *callback_user_data;
};

struct aws_mqtt3_to_mqtt5_adapter_subscription_set {
    struct aws_allocator *allocator;

    struct aws_mqtt3_to_mqtt5_adapter_subscription_set_node *root; // implicitly have a permanent ref
};

AWS_EXTERN_C_BEGIN

/**
 *
 * @param allocator
 * @param adapter
 * @return
 */
AWS_MQTT_API struct aws_mqtt3_to_mqtt5_adapter_subscription_set *aws_mqtt3_to_mqtt5_adapter_subscription_set_new(
    struct aws_allocator *allocator);

/**
 *
 * @param subscription_set
 */
AWS_MQTT_API void aws_mqtt3_to_mqtt5_adapter_subscription_set_destroy(
    struct aws_mqtt3_to_mqtt5_adapter_subscription_set *subscription_set);

/**
 *
 * @param subscription_set
 * @param topic_filter
 * @return
 */
AWS_MQTT_API bool aws_mqtt3_to_mqtt5_adapter_subscription_set_is_topic_filter_subscribed(
    struct aws_mqtt3_to_mqtt5_adapter_subscription_set *subscription_set,
    struct aws_byte_cursor topic_filter);

/**
 *
 * @param subscription_set
 * @param subscription_options
 */
AWS_MQTT_API void aws_mqtt3_to_mqtt5_adapter_subscription_set_add_subscription(
    struct aws_mqtt3_to_mqtt5_adapter_subscription_set *subscription_set,
    const struct aws_mqtt3_to_mqtt5_adapter_subscription_options *subscription_options);

/**
 *
 * @param subscription_set
 * @param topic_filter
 */
AWS_MQTT_API void aws_mqtt3_to_mqtt5_adapter_subscription_set_remove_subscription(
    struct aws_mqtt3_to_mqtt5_adapter_subscription_set *subscription_set,
    struct aws_byte_cursor topic_filter);

/**
 *
 * @param subscription_set
 * @param publish_options
 */
AWS_MQTT_API void aws_mqtt3_to_mqtt5_adapter_subscription_set_on_publish_received(
    struct aws_mqtt3_to_mqtt5_adapter_subscription_set *subscription_set,
    const struct aws_mqtt3_to_mqtt5_adapter_publish_received_options *publish_options);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_MQTT3_TO_MQTT5_ADAPTER_SUBSCRIPTION_SET_H */
