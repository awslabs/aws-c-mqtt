/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/private/v5/mqtt3_to_mqtt5_adapter_subscription_set.h>

#include <aws/mqtt/private/client_impl_shared.h>

/*

struct aws_mqtt3_to_mqtt5_adapter_publish_received_options {
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
either need to reject (no) or have  flag

    aws_mqtt_client_publish_received_fn *on_publish_received;
    aws_mqtt_userdata_cleanup_fn *on_cleanup;

    void *callback_user_data;
};

struct aws_mqtt3_to_mqtt5_adapter_subscription_set {

    struct aws_allocator *allocator;

    struct aws_mqtt_client_connection_5_impl *adapter;

    struct aws_mqtt3_to_mqtt5_adapter_subscription_set_node *root; // implicitly have a permanent ref
};

 */

#define SUBSCRIPTION_SET_DEFAULT_BRANCH_FACTOR 10

static struct aws_mqtt3_to_mqtt5_adapter_subscription_set_node *s_aws_mqtt3_to_mqtt5_adapter_subscription_set_node_new(
    struct aws_allocator *allocator,
    struct aws_mqtt3_to_mqtt5_adapter_subscription_set_node *parent) {

    struct aws_mqtt3_to_mqtt5_adapter_subscription_set_node *node =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt3_to_mqtt5_adapter_subscription_set_node));
    node->allocator = allocator;
    aws_hash_table_init(
        &node->children,
        allocator,
        SUBSCRIPTION_SET_DEFAULT_BRANCH_FACTOR,
        aws_hash_byte_cursor_ptr,
        aws_mqtt_byte_cursor_hash_equality,
        NULL,
        NULL);
    node->ref_count = 1;
    node->parent = parent;

    return node;
}

struct aws_mqtt3_to_mqtt5_adapter_subscription_set *aws_mqtt3_to_mqtt5_adapter_subscription_set_new(
    struct aws_allocator *allocator,
    struct aws_mqtt_client_connection_5_impl *adapter) {

    struct aws_mqtt3_to_mqtt5_adapter_subscription_set *subscription_set =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt3_to_mqtt5_adapter_subscription_set));

    subscription_set->allocator = allocator;
    subscription_set->adapter = adapter;
    subscription_set->root = s_aws_mqtt3_to_mqtt5_adapter_subscription_set_node_new(allocator, NULL);

    return subscription_set;
}

static int s_subscription_set_node_destroy_hash_foreach_wrap(void *context, struct aws_hash_element *elem);

static void s_aws_mqtt3_to_mqtt5_adapter_subscription_set_node_destroy_node(
    struct aws_mqtt3_to_mqtt5_adapter_subscription_set_node *node) {
    aws_hash_table_foreach(&node->children, s_subscription_set_node_destroy_hash_foreach_wrap, NULL);
    aws_hash_table_clean_up(&node->children);

    if (node->on_cleanup && node->callback_user_data) {
        node->on_cleanup(node->callback_user_data);
    }

    aws_byte_buf_clean_up(&node->topic_segment);

    aws_mem_release(node->allocator, node);
}

static void s_aws_mqtt3_to_mqtt5_adapter_subscription_set_node_destroy_tree(
    struct aws_mqtt3_to_mqtt5_adapter_subscription_set_node *tree) {
    if (tree == NULL) {
        return;
    }

    if (tree->parent != NULL) {
        aws_hash_table_remove(&tree->parent->children, &tree->topic_segment, NULL, NULL);
    }

    s_aws_mqtt3_to_mqtt5_adapter_subscription_set_node_destroy_node(tree);
}

static int s_subscription_set_node_destroy_hash_foreach_wrap(void *context, struct aws_hash_element *elem) {
    (void)context;

    s_aws_mqtt3_to_mqtt5_adapter_subscription_set_node_destroy_node(elem->value);

    return AWS_COMMON_HASH_TABLE_ITER_CONTINUE | AWS_COMMON_HASH_TABLE_ITER_DELETE;
}

void aws_mqtt3_to_mqtt5_adapter_subscription_set_destroy(
    struct aws_mqtt3_to_mqtt5_adapter_subscription_set *subscription_set) {
    if (subscription_set == NULL) {
        return;
    }

    s_aws_mqtt3_to_mqtt5_adapter_subscription_set_node_destroy_tree(subscription_set->root);

    aws_mem_release(subscription_set->allocator, subscription_set);
}

bool aws_mqtt3_to_mqtt5_adapter_subscription_set_is_topic_filter_subscribed(
    struct aws_mqtt3_to_mqtt5_adapter_subscription_set *subscription_set,
    struct aws_byte_cursor topic_filter) {
    struct aws_mqtt3_to_mqtt5_adapter_subscription_set_node *current_node = subscription_set->root;

    struct aws_byte_cursor topic_segment;
    AWS_ZERO_STRUCT(topic_segment);
    while (aws_byte_cursor_next_split(&topic_filter, '/', &topic_segment)) {
        struct aws_hash_element *hash_element = NULL;
        aws_hash_table_find(&current_node->children, &topic_segment, &hash_element);

        if (hash_element == NULL) {
            return false;
        } else {
            current_node = hash_element->value;
        }
    }

    return current_node->is_subscription == true;
}

/*
 * Walks the existing tree creating nodes as necessary to reach the subscription leaf implied by the topic filter.
 * Returns the node representing the final level of the topic filter. Each existing node has its ref count increased by
 * one.  Newly-created nodes start with a ref count of one.  Given that the topic filter has been validated, the only
 * possible error is a memory allocation error which is a crash anyways.
 *
 * If the leaf node already exists and has a cleanup callback, it will be invoked and both the callback and its user
 * data will be cleared .  The returned node will always have is_subscription set to true.
 */
static struct aws_mqtt3_to_mqtt5_adapter_subscription_set_node *
    s_aws_mqtt3_to_mqtt5_adapter_subscription_set_create_or_reference_topic_filter_path(
        struct aws_mqtt3_to_mqtt5_adapter_subscription_set_node *root,
        struct aws_byte_cursor topic_filter) {

    struct aws_mqtt3_to_mqtt5_adapter_subscription_set_node *current_node = root;

    struct aws_byte_cursor topic_segment;
    AWS_ZERO_STRUCT(topic_segment);
    while (aws_byte_cursor_next_split(&topic_filter, '/', &topic_segment)) {
        ++current_node->ref_count;

        struct aws_hash_element *hash_element = NULL;
        aws_hash_table_find(&current_node->children, &topic_segment, &hash_element);

        if (hash_element == NULL) {
            struct aws_mqtt3_to_mqtt5_adapter_subscription_set_node *new_node =
                s_aws_mqtt3_to_mqtt5_adapter_subscription_set_node_new(current_node->allocator, current_node);

            aws_byte_buf_init_copy_from_cursor(&new_node->topic_segment, new_node->allocator, topic_segment);
            new_node->topic_segment_cursor = aws_byte_cursor_from_buf(&new_node->topic_segment);

            aws_hash_table_put(&current_node->children, &new_node->topic_segment_cursor, new_node, NULL);

            current_node = new_node;
        } else {
            current_node = hash_element->value;
        }
    }

    if (current_node->on_cleanup) {
        (*current_node->on_cleanup)(current_node->callback_user_data);
        current_node->on_cleanup = NULL;
    }

    current_node->is_subscription = true;

    return current_node;
}

void aws_mqtt3_to_mqtt5_adapter_subscription_set_add_subscription(
    struct aws_mqtt3_to_mqtt5_adapter_subscription_set *subscription_set,
    const struct aws_mqtt3_to_mqtt5_adapter_subscription_options *subscription_options) {

    AWS_FATAL_ASSERT(aws_mqtt_is_valid_topic_filter(&subscription_options->topic_filter));

    struct aws_mqtt3_to_mqtt5_adapter_subscription_set_node *subscription_node =
        s_aws_mqtt3_to_mqtt5_adapter_subscription_set_create_or_reference_topic_filter_path(
            subscription_set->root, subscription_options->topic_filter);
    subscription_node->on_publish_received = subscription_options->on_publish_received;
    subscription_node->on_cleanup = subscription_options->on_cleanup;
    subscription_node->callback_user_data = subscription_options->callback_user_data;
}

void aws_mqtt3_to_mqtt5_adapter_subscription_set_remove_subscription(
    struct aws_mqtt3_to_mqtt5_adapter_subscription_set *subscription_set,
    struct aws_byte_cursor topic_filter) {
    if (!aws_mqtt3_to_mqtt5_adapter_subscription_set_is_topic_filter_subscribed(subscription_set, topic_filter)) {
        return;
    }

    struct aws_mqtt3_to_mqtt5_adapter_subscription_set_node *current_node = subscription_set->root;

    struct aws_byte_cursor topic_segment;
    AWS_ZERO_STRUCT(topic_segment);
    while (aws_byte_cursor_next_split(&topic_filter, '/', &topic_segment)) {
        --current_node->ref_count;

        if (current_node->ref_count == 0) {
            s_aws_mqtt3_to_mqtt5_adapter_subscription_set_node_destroy_tree(current_node);
            return;
        }

        struct aws_hash_element *hash_element = NULL;
        aws_hash_table_find(&current_node->children, &topic_segment, &hash_element);

        current_node = hash_element->value;
    }

    --current_node->ref_count;
    if (current_node->ref_count == 0) {
        s_aws_mqtt3_to_mqtt5_adapter_subscription_set_node_destroy_tree(current_node);
        return;
    }

    if (current_node->on_cleanup) {
        (*current_node->on_cleanup)(current_node->callback_user_data);
        current_node->on_cleanup = NULL;
    }
    current_node->on_publish_received = NULL;
    current_node->is_subscription = false;
}

void aws_mqtt3_to_mqtt5_adapter_subscription_set_on_publish_received(
    struct aws_mqtt3_to_mqtt5_adapter_subscription_set *subscription_set,
    const struct aws_mqtt3_to_mqtt5_adapter_publish_received_options *publish_options) {
    (void)subscription_set;
    (void)publish_options;
}
