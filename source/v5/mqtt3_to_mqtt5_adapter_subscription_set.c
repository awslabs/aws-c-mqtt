/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/private/v5/mqtt3_to_mqtt5_adapter_subscription_set.h>

#include <aws/mqtt/private/client_impl_shared.h>

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
    node->ref_count = 0;
    node->parent = parent;

    return node;
}

struct aws_mqtt3_to_mqtt5_adapter_subscription_set *aws_mqtt3_to_mqtt5_adapter_subscription_set_new(
    struct aws_allocator *allocator) {

    struct aws_mqtt3_to_mqtt5_adapter_subscription_set *subscription_set =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt3_to_mqtt5_adapter_subscription_set));

    subscription_set->allocator = allocator;
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

static struct aws_mqtt3_to_mqtt5_adapter_subscription_set_node *
    s_aws_mqtt3_to_mqtt5_adapter_subscription_set_get_existing_subscription_node(
        struct aws_mqtt3_to_mqtt5_adapter_subscription_set *subscription_set,
        struct aws_byte_cursor topic_filter) {

    struct aws_mqtt3_to_mqtt5_adapter_subscription_set_node *current_node = subscription_set->root;

    struct aws_byte_cursor topic_segment;
    AWS_ZERO_STRUCT(topic_segment);
    while (aws_byte_cursor_next_split(&topic_filter, '/', &topic_segment)) {
        struct aws_hash_element *hash_element = NULL;
        aws_hash_table_find(&current_node->children, &topic_segment, &hash_element);

        if (hash_element == NULL) {
            return NULL;
        } else {
            current_node = hash_element->value;
        }
    }

    if (!current_node->is_subscription) {
        return NULL;
    }

    return current_node;
}

bool aws_mqtt3_to_mqtt5_adapter_subscription_set_is_topic_filter_subscribed(
    struct aws_mqtt3_to_mqtt5_adapter_subscription_set *subscription_set,
    struct aws_byte_cursor topic_filter) {
    struct aws_mqtt3_to_mqtt5_adapter_subscription_set_node *existing_node =
        s_aws_mqtt3_to_mqtt5_adapter_subscription_set_get_existing_subscription_node(subscription_set, topic_filter);

    return existing_node != NULL;
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

    ++current_node->ref_count;

    return current_node;
}

void aws_mqtt3_to_mqtt5_adapter_subscription_set_add_subscription(
    struct aws_mqtt3_to_mqtt5_adapter_subscription_set *subscription_set,
    const struct aws_mqtt3_to_mqtt5_adapter_subscription_options *subscription_options) {

    AWS_FATAL_ASSERT(aws_mqtt_is_valid_topic_filter(&subscription_options->topic_filter));

    struct aws_mqtt3_to_mqtt5_adapter_subscription_set_node *subscription_node =
        s_aws_mqtt3_to_mqtt5_adapter_subscription_set_get_existing_subscription_node(
            subscription_set, subscription_options->topic_filter);
    if (subscription_node == NULL) {
        subscription_node = s_aws_mqtt3_to_mqtt5_adapter_subscription_set_create_or_reference_topic_filter_path(
            subscription_set->root, subscription_options->topic_filter);
    }

    if (subscription_node->on_cleanup) {
        (*subscription_node->on_cleanup)(subscription_node->callback_user_data);
        subscription_node->on_cleanup = NULL;
    }

    subscription_node->is_subscription = true;

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

        /* We previously validated the full path; this must exist */
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

struct aws_subscription_set_path_continuation {
    struct aws_byte_cursor current_fragment;
    struct aws_mqtt3_to_mqtt5_adapter_subscription_set_node *current_node;
};

static void s_add_subscription_set_path_continuation(
    struct aws_array_list *paths,
    struct aws_byte_cursor fragment,
    struct aws_mqtt3_to_mqtt5_adapter_subscription_set_node *node) {
    if (node == NULL) {
        return;
    }

    struct aws_subscription_set_path_continuation path = {
        .current_fragment = fragment,
        .current_node = node,
    };

    aws_array_list_push_back(paths, &path);
}

#define SUBSCRIPTION_SET_PATH_FRAGMENT_DEFAULT 10

AWS_STATIC_STRING_FROM_LITERAL(s_single_level_wildcard, "+");
AWS_STATIC_STRING_FROM_LITERAL(s_multi_level_wildcard, "#");

static struct aws_mqtt3_to_mqtt5_adapter_subscription_set_node *s_aws_subscription_set_node_find_child(
    struct aws_mqtt3_to_mqtt5_adapter_subscription_set_node *node,
    struct aws_byte_cursor fragment) {
    struct aws_hash_element *element = NULL;
    aws_hash_table_find(&node->children, &fragment, &element);

    if (element == NULL) {
        return NULL;
    }

    return element->value;
}

static void s_invoke_on_publish_received(
    struct aws_mqtt3_to_mqtt5_adapter_subscription_set_node *node,
    const struct aws_mqtt3_to_mqtt5_adapter_publish_received_options *publish_options) {
    if (node == NULL || !node->is_subscription || node->on_publish_received == NULL) {
        return;
    }

    (*node->on_publish_received)(
        publish_options->connection,
        &publish_options->topic,
        &publish_options->payload,
        publish_options->dup,
        publish_options->qos,
        publish_options->retain,
        node->callback_user_data);
}

void aws_mqtt3_to_mqtt5_adapter_subscription_set_on_publish_received(
    struct aws_mqtt3_to_mqtt5_adapter_subscription_set *subscription_set,
    const struct aws_mqtt3_to_mqtt5_adapter_publish_received_options *publish_options) {

    struct aws_byte_cursor slw_cursor = aws_byte_cursor_from_string(s_single_level_wildcard);
    struct aws_byte_cursor mlw_cursor = aws_byte_cursor_from_string(s_multi_level_wildcard);

    struct aws_array_list tree_paths;
    aws_array_list_init_dynamic(
        &tree_paths,
        subscription_set->allocator,
        SUBSCRIPTION_SET_PATH_FRAGMENT_DEFAULT,
        sizeof(struct aws_subscription_set_path_continuation));

    struct aws_byte_cursor empty_cursor;
    AWS_ZERO_STRUCT(empty_cursor);
    s_add_subscription_set_path_continuation(&tree_paths, empty_cursor, subscription_set->root);

    while (aws_array_list_length(&tree_paths) > 0) {
        struct aws_subscription_set_path_continuation path_continuation;
        AWS_ZERO_STRUCT(path_continuation);

        size_t path_count = aws_array_list_length(&tree_paths);
        aws_array_list_get_at(&tree_paths, &path_continuation, path_count - 1);
        aws_array_list_pop_back(&tree_paths);

        /*
         * Invoke multi-level wildcard check before checking split result; this allows a subscription like
         * 'a/b/#' to match an incoming 'a/b'
         */
        struct aws_mqtt3_to_mqtt5_adapter_subscription_set_node *mlw_node =
            s_aws_subscription_set_node_find_child(path_continuation.current_node, mlw_cursor);
        s_invoke_on_publish_received(mlw_node, publish_options);

        struct aws_byte_cursor next_fragment = path_continuation.current_fragment;
        if (!aws_byte_cursor_next_split(&publish_options->topic, '/', &next_fragment)) {
            s_invoke_on_publish_received(path_continuation.current_node, publish_options);
            continue;
        }

        struct aws_mqtt3_to_mqtt5_adapter_subscription_set_node *slw_node =
            s_aws_subscription_set_node_find_child(path_continuation.current_node, slw_cursor);
        s_add_subscription_set_path_continuation(&tree_paths, next_fragment, slw_node);

        struct aws_mqtt3_to_mqtt5_adapter_subscription_set_node *fragment_node =
            s_aws_subscription_set_node_find_child(path_continuation.current_node, next_fragment);
        s_add_subscription_set_path_continuation(&tree_paths, next_fragment, fragment_node);
    }

    aws_array_list_clean_up(&tree_paths);
}
