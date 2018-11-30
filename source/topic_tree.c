/*
 * Copyright 2010-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

#include <aws/mqtt/private/topic_tree.h>

#include <aws/common/byte_buf.h>
#include <aws/common/task_scheduler.h>

#ifdef _MSC_VER
/* disables warning non const declared initializers for Microsoft compilers */
#    pragma warning(disable : 4204)
#    include <malloc.h>
#endif /* _MSC_VER */

AWS_STATIC_STRING_FROM_LITERAL(s_single_level_wildcard, "+");
AWS_STATIC_STRING_FROM_LITERAL(s_multi_level_wildcard, "#");

static struct aws_mqtt_topic_node *s_topic_node_new(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *topic_filter,
    const struct aws_string *full_topic) {

    struct aws_mqtt_topic_node *node = aws_mem_acquire(allocator, sizeof(struct aws_mqtt_topic_node));
    if (!node) {
        return NULL;
    }
    AWS_ZERO_STRUCT(*node);
    assert(!topic_filter || full_topic);

    if (topic_filter) {
        node->topic = *topic_filter;
        node->topic_filter = full_topic;
    }

    /* Init the sub topics map */
    if (aws_hash_table_init(
            &node->subtopics, allocator, 0, aws_hash_byte_cursor_ptr, aws_byte_cursor_ptr_eq, NULL, NULL)) {

        aws_mem_release(allocator, node);
        return NULL;
    }

    return node;
}

static int s_topic_node_clean_up_hash_foreach_wrap(void *context, struct aws_hash_element *elem);

static void s_topic_node_clean_up(struct aws_mqtt_topic_node *node, struct aws_allocator *allocator) {

    /* Traverse all children and remove */
    aws_hash_table_foreach(&node->subtopics, s_topic_node_clean_up_hash_foreach_wrap, allocator);

    if (node->cleanup && node->userdata) {
        node->cleanup(node->userdata);
    }

    if (node->owns_topic_filter) {
        aws_string_destroy((void *)node->topic_filter);
    }

    aws_hash_table_clean_up(&node->subtopics);
    aws_mem_release(allocator, node);
}

static int s_topic_node_clean_up_hash_foreach_wrap(void *context, struct aws_hash_element *elem) {

    s_topic_node_clean_up(elem->value, context);

    return AWS_COMMON_HASH_TABLE_ITER_CONTINUE | AWS_COMMON_HASH_TABLE_ITER_DELETE;
}

int aws_mqtt_topic_tree_init(struct aws_mqtt_topic_tree *tree, struct aws_allocator *allocator) {

    assert(tree);
    assert(allocator);

    tree->root = s_topic_node_new(allocator, NULL, NULL);
    if (!tree->root) {
        return AWS_OP_ERR;
    }
    tree->allocator = allocator;

    return AWS_OP_SUCCESS;
}

void aws_mqtt_topic_tree_clean_up(struct aws_mqtt_topic_tree *tree) {

    assert(tree);

    if (tree->allocator && tree->root) {
        s_topic_node_clean_up(tree->root, tree->allocator);

        AWS_ZERO_STRUCT(*tree);
    }
}

bool s_topic_node_is_subscription(const struct aws_mqtt_topic_node *node) {
    return node->callback;
}

int aws_mqtt_topic_tree_insert(
    struct aws_mqtt_topic_tree *tree,
    const struct aws_string *topic_filter,
    enum aws_mqtt_qos qos,
    aws_mqtt_publish_received_fn *callback,
    aws_mqtt_userdata_cleanup_fn *cleanup,
    void *userdata) {

    assert(tree);
    assert(topic_filter);
    assert(callback);

    struct aws_mqtt_topic_node *current = tree->root;

    struct aws_byte_cursor topic_filter_cur = aws_byte_cursor_from_string(topic_filter);
    struct aws_byte_cursor sub_part;
    AWS_ZERO_STRUCT(sub_part);
    struct aws_byte_cursor last_part;
    AWS_ZERO_STRUCT(last_part);
    while (aws_byte_cursor_next_split(&topic_filter_cur, '/', &sub_part)) {

        last_part = sub_part;

        /* Add or find mid-node */
        struct aws_hash_element *elem = NULL;
        int was_created = 0;
        aws_hash_table_create(&current->subtopics, &sub_part, &elem, &was_created);

        if (was_created) {
            /* Node does not exist, add new one */
            current = s_topic_node_new(tree->allocator, &sub_part, topic_filter);

            /* Stash in the hash map */
            elem->key = &current->topic;
            elem->value = current;
        } else {
            /* If the node exists, just traverse it */
            current = elem->value;
        }
    }

    if (current->cleanup && current->userdata) {
        /* If there was userdata assigned to this node, pass it out. */
        current->cleanup(current->userdata);
    }

    /* Node found (or created), add the topic filter and callbacks */
    if (current->owns_topic_filter) {
        /* If the topic filter was already here, this is already a subscription.
           Free the new topic_filter so all existing byte_cursors remain valid. */
        aws_string_destroy((void *)topic_filter);
    } else {
        /* Node already existed but wasn't subscription. */
        current->topic = last_part;
        current->topic_filter = topic_filter;
    }
    current->qos = qos;
    current->owns_topic_filter = true;
    current->callback = callback;
    current->cleanup = cleanup;
    current->userdata = userdata;

    return AWS_OP_SUCCESS;
}

/* Searches subtree until a topic_filter with a different pointer value is found. */
static int s_topic_node_string_finder(void *userdata, struct aws_hash_element *elem) {

    const struct aws_string **topic_filter = userdata;
    struct aws_mqtt_topic_node *node = elem->value;

    /* We've found this node again, search it's children */
    if (*topic_filter == node->topic_filter) {
        if (0 == aws_hash_table_get_entry_count(&node->subtopics)) {
            /* If no children, then there must be siblings, so we can use those */
            return AWS_COMMON_HASH_TABLE_ITER_CONTINUE;
        }

        aws_hash_table_foreach(&node->subtopics, s_topic_node_string_finder, userdata);

        if (*topic_filter == node->topic_filter) {
            /* If the topic filter still hasn't changed, continue iterating */
            return AWS_COMMON_HASH_TABLE_ITER_CONTINUE;
        }

        return 0;
    }

    *topic_filter = node->topic_filter;
    return 0;
}

int aws_mqtt_topic_tree_remove(struct aws_mqtt_topic_tree *tree, const struct aws_byte_cursor *topic_filter) {

    assert(tree);
    assert(topic_filter);

    struct aws_array_list sub_topic_parts;
    AWS_ZERO_STRUCT(sub_topic_parts);

    if (aws_array_list_init_dynamic(&sub_topic_parts, tree->allocator, 1, sizeof(struct aws_byte_cursor))) {
        return AWS_OP_ERR;
    }

    if (aws_byte_cursor_split_on_char(topic_filter, '/', &sub_topic_parts)) {
        aws_array_list_clean_up(&sub_topic_parts);
        return AWS_OP_ERR;
    }
    const size_t sub_parts_len = aws_array_list_length(&sub_topic_parts);
    if (!sub_parts_len) {
        aws_array_list_clean_up(&sub_topic_parts);
        return AWS_OP_ERR;
    }

#ifdef _MSC_VER
    struct aws_mqtt_topic_node **visited = _alloca(sizeof(void *) * (sub_parts_len + 1));
#else
    struct aws_mqtt_topic_node *visited[sub_parts_len + 1];
#endif

    struct aws_mqtt_topic_node *current = tree->root;
    visited[0] = current;

    for (size_t i = 0; i < sub_parts_len; ++i) {

        /* Get the current topic part */
        struct aws_byte_cursor *sub_part = NULL;
        aws_array_list_get_at_ptr(&sub_topic_parts, (void **)&sub_part, i);

        /* Find mid-node */
        struct aws_hash_element *elem = NULL;
        aws_hash_table_find(&current->subtopics, sub_part, &elem);
        if (elem) {
            /* If the node exists, just traverse it */
            current = elem->value;
            visited[i + 1] = current;
        } else {
            /* If not, abandon ship */
            current = NULL;
            break;
        }
    }

    if (current) {
        /* If found the node, traverse up and remove each with no sub-topics.
         * Then update all nodes that were using current's topic_filter for topic. */

        /* "unsubscribe" current. */
        if (current->cleanup && current->userdata) {
            /* If there was userdata assigned to this node, pass it out. */
            current->cleanup(current->userdata);
        }
        current->callback = NULL;
        current->cleanup = NULL;
        current->userdata = NULL;

        /* Set to true if current needs to be cleaned up. */
        bool destroy_current = false;

        /* How many nodes are left after the great purge. */
        size_t nodes_left = sub_parts_len;

        /* Remove all subscription-less and child-less nodes. */
        for (size_t i = sub_parts_len; i > 0; --i) {
            struct aws_mqtt_topic_node *node = visited[i];

            if (!s_topic_node_is_subscription(node) && 0 == aws_hash_table_get_entry_count(&node->subtopics)) {

                /* No subscription and no children, this node needs to go. */
                struct aws_mqtt_topic_node *grandma = visited[i - 1];
                aws_hash_table_remove(&grandma->subtopics, &node->topic, NULL, NULL);

                /* Make sure the following loop doesn't hit this node. */
                --nodes_left;

                if (i != sub_parts_len) {

                    /* Clean up and delete */
                    s_topic_node_clean_up(node, tree->allocator);
                } else {
                    destroy_current = true;
                }
            } else {

                /* Once we've found one node with children, the rest are guaranteed to. */
                break;
            }
        }

        /* If current owns the full string, go fixup the pointer references. */
        if (nodes_left > 0) {

            /* If a new viable topic filter is found once, it can be used for all parents. */
            const struct aws_string *new_topic_filter = NULL;
            const struct aws_string *const old_topic_filter = current->topic_filter;
            /* How much of new_topic_filter should be lopped off the beginning. */

            struct aws_mqtt_topic_node *parent = visited[nodes_left];
            size_t topic_offset = parent->topic.ptr - aws_string_bytes(parent->topic_filter) + parent->topic.len + 1;

            /* -1 to avoid touching current */
            for (size_t i = nodes_left; i > 0; --i) {
                parent = visited[i];

                /* Remove this topic and following / from offset. */
                topic_offset -= (parent->topic.len + 1);

                if (parent->topic_filter == old_topic_filter) {
                    /* Uh oh, Mom's using my topic string again! Steal it and replace it with a new one, Indiana Jones
                     * style. */

                    if (!new_topic_filter) {
                        /* Set new_tf to old_tf so it's easier to check against the existing node.
                         * Basically, it's an INOUT param. */
                        new_topic_filter = old_topic_filter;

                        /* Search all subtopics until we find one that isn't current. */
                        aws_hash_table_foreach(
                            &parent->subtopics, s_topic_node_string_finder, (void *)&new_topic_filter);

                        /* This would only happen if there is only one topic in subtopics (current's) and
                         * it has no children (in which case it should have been removed above). */
                        assert(new_topic_filter != old_topic_filter);

                        /* Now that the new string has been found, the old one can be destroyed. */
                        aws_string_destroy((void *)current->topic_filter);
                        current->owns_topic_filter = false;
                    }

                    /* Update the pointers. */
                    parent->topic_filter = new_topic_filter;
                    parent->topic.ptr = (uint8_t *)aws_string_bytes(new_topic_filter) + topic_offset;
                }
            }
        }

        /* Now that the strings are update, remove current. */
        if (destroy_current) {
            s_topic_node_clean_up(current, tree->allocator);
        }
        current = NULL;
    }

    aws_array_list_clean_up(&sub_topic_parts);

    return AWS_OP_SUCCESS;
}

static void s_topic_tree_publish_do_recurse(
    struct aws_byte_cursor *sub_part,
    const struct aws_mqtt_topic_node *current,
    struct aws_mqtt_packet_publish *pub) {

    struct aws_byte_cursor hash_cur = aws_byte_cursor_from_string(s_multi_level_wildcard);
    struct aws_byte_cursor plus_cur = aws_byte_cursor_from_string(s_single_level_wildcard);

    struct aws_hash_element *elem = NULL;

    if (!aws_byte_cursor_next_split(&pub->topic_name, '/', sub_part)) {

        /* If this is the last node and is a sub, call it */
        current->callback(&pub->topic_name, &pub->payload, current->userdata);
        return;
    }

    /* Check multi-level wildcard */
    aws_hash_table_find(&current->subtopics, &hash_cur, &elem);
    if (elem) {
        /* Match! */
        struct aws_mqtt_topic_node *multi_wildcard = elem->value;
        /* Must be a subscription and have no children */
        assert(s_topic_node_is_subscription(multi_wildcard));
        assert(0 == aws_hash_table_get_entry_count(&multi_wildcard->subtopics));
        multi_wildcard->callback(&pub->topic_name, &pub->payload, multi_wildcard->userdata);
    }

    /* Check single level wildcard */
    aws_hash_table_find(&current->subtopics, &plus_cur, &elem);
    if (elem) {
        /* Recurse sub topics */
        s_topic_tree_publish_do_recurse(sub_part, elem->value, pub);
    }

    /* Check actual topic name */
    aws_hash_table_find(&current->subtopics, sub_part, &elem);
    if (elem) {
        /* Found the actual topic, recurse to it */
        s_topic_tree_publish_do_recurse(sub_part, elem->value, pub);
    }
}

int aws_mqtt_topic_tree_publish(const struct aws_mqtt_topic_tree *tree, struct aws_mqtt_packet_publish *pub) {

    assert(tree);
    assert(pub);

    struct aws_byte_cursor sub_part;
    AWS_ZERO_STRUCT(sub_part);
    s_topic_tree_publish_do_recurse(&sub_part, tree->root, pub);

    return AWS_OP_SUCCESS;
}
