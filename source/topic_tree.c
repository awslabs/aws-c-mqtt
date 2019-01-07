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
#endif /* _MSC_VER */

AWS_STATIC_STRING_FROM_LITERAL(s_single_level_wildcard, "+");
AWS_STATIC_STRING_FROM_LITERAL(s_multi_level_wildcard, "#");

/*******************************************************************************
 * Transactions
 ******************************************************************************/

struct topic_tree_action {
    enum {
        AWS_MQTT_TOPIC_TREE_ADD,
        AWS_MQTT_TOPIC_TREE_UPDATE,
        AWS_MQTT_TOPIC_TREE_REMOVE,
    } mode;

    /* All Modes */
    struct aws_mqtt_topic_node *node_to_update;

    /* ADD/UPDATE */
    struct aws_byte_cursor topic;
    const struct aws_string *topic_filter;
    enum aws_mqtt_qos qos;
    aws_mqtt_publish_received_fn *callback;
    aws_mqtt_userdata_cleanup_fn *cleanup;
    void *userdata;

    /* ADD */
    struct aws_mqtt_topic_node *last_found;
    struct aws_mqtt_topic_node *first_created;

    /* REMOVE */
    struct aws_array_list to_remove; /* topic_tree_node* */
};

size_t aws_mqtt_topic_tree_action_size = sizeof(struct topic_tree_action);

static struct topic_tree_action *s_topic_tree_action_create(struct aws_array_list *transaction) {

    struct topic_tree_action *action = NULL;

    bool was_pushed = false;

    /* Push an empty action into the transaction and get a pointer to it. */
    struct topic_tree_action empty_action;
    AWS_ZERO_STRUCT(empty_action);
    if (aws_array_list_push_back(transaction, &empty_action)) {

        goto handle_error;
    }
    was_pushed = true;

    if (aws_array_list_get_at_ptr(transaction, (void **)&action, aws_array_list_length(transaction) - 1)) {

        goto handle_error;
    }

    return action;

handle_error:

    if (was_pushed) {
        aws_array_list_pop_back(transaction);
    }

    return NULL;
}

static void s_topic_tree_action_destroy(struct topic_tree_action *action) {

    if (action->mode == AWS_MQTT_TOPIC_TREE_REMOVE) {
        aws_array_list_clean_up(&action->to_remove);
    }

    AWS_ZERO_STRUCT(*action);
}

static int s_topic_tree_action_to_remove(
    struct topic_tree_action *action,
    struct aws_allocator *allocator,
    size_t size_hint) {

    if (action->mode != AWS_MQTT_TOPIC_TREE_REMOVE) {
        if (aws_array_list_init_dynamic(&action->to_remove, allocator, size_hint, sizeof(void *))) {
            return AWS_OP_ERR;
        }
        action->mode = AWS_MQTT_TOPIC_TREE_REMOVE;
    }

    return AWS_OP_SUCCESS;
}

static bool byte_cursor_eq(const void *a, const void *b) {
    const struct aws_byte_cursor *cur_a = a;
    const struct aws_byte_cursor *cur_b = b;

    return aws_byte_cursor_eq(cur_a, cur_b);
}

/*******************************************************************************
 * Init
 ******************************************************************************/

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
    if (aws_hash_table_init(&node->subtopics, allocator, 0, aws_hash_byte_cursor_ptr, byte_cursor_eq, NULL, NULL)) {

        aws_mem_release(allocator, node);
        return NULL;
    }

    return node;
}

static int s_topic_node_destroy_hash_foreach_wrap(void *context, struct aws_hash_element *elem);

static void s_topic_node_destroy(struct aws_mqtt_topic_node *node, struct aws_allocator *allocator) {

    /* Traverse all children and remove */
    aws_hash_table_foreach(&node->subtopics, s_topic_node_destroy_hash_foreach_wrap, allocator);

    if (node->cleanup && node->userdata) {
        node->cleanup(node->userdata);
    }

    if (node->owns_topic_filter) {
        aws_string_destroy((void *)node->topic_filter);
    }

    aws_hash_table_clean_up(&node->subtopics);
    aws_mem_release(allocator, node);
}

static int s_topic_node_destroy_hash_foreach_wrap(void *context, struct aws_hash_element *elem) {

    s_topic_node_destroy(elem->value, context);

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

/*******************************************************************************
 * Clean Up
 ******************************************************************************/

void aws_mqtt_topic_tree_clean_up(struct aws_mqtt_topic_tree *tree) {

    assert(tree);

    if (tree->allocator && tree->root) {
        s_topic_node_destroy(tree->root, tree->allocator);

        AWS_ZERO_STRUCT(*tree);
    }
}

bool s_topic_node_is_subscription(const struct aws_mqtt_topic_node *node) {
    return node->callback;
}

/*******************************************************************************
 * Action Commit
 ******************************************************************************/

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

static void s_topic_tree_action_commit(struct topic_tree_action *action, struct aws_mqtt_topic_tree *tree) {
    (void)tree;

    assert(action->node_to_update);

    switch (action->mode) {
        case AWS_MQTT_TOPIC_TREE_ADD:
        case AWS_MQTT_TOPIC_TREE_UPDATE: {

            /* Destroy old userdata */
            if (action->node_to_update->cleanup && action->node_to_update->userdata) {
                /* If there was userdata assigned to this node, pass it out. */
                action->node_to_update->cleanup(action->node_to_update->userdata);
            }

            /* Update data */
            action->node_to_update->callback = action->callback;
            action->node_to_update->cleanup = action->cleanup;
            action->node_to_update->userdata = action->userdata;
            action->node_to_update->qos = action->qos;
            if (action->topic.ptr) {
                action->node_to_update->topic = action->topic;
            }
            if (action->topic_filter) {
                action->node_to_update->topic_filter = action->topic_filter;
                action->node_to_update->owns_topic_filter = true;
            }
            break;
        }

        case AWS_MQTT_TOPIC_TREE_REMOVE: {

            struct aws_mqtt_topic_node *current = action->node_to_update;
            const size_t sub_parts_len = aws_array_list_length(&action->to_remove) - 1;

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
                    struct aws_mqtt_topic_node *node = NULL;
                    aws_array_list_get_at(&action->to_remove, &node, i);
                    assert(node); /* Must be in bounds */

                    if (!s_topic_node_is_subscription(node) && 0 == aws_hash_table_get_entry_count(&node->subtopics)) {

                        /* No subscription and no children, this node needs to go. */
                        struct aws_mqtt_topic_node *grandma = NULL;
                        aws_array_list_get_at(&action->to_remove, &grandma, i - 1);
                        assert(grandma); /* Must be in bounds */

                        aws_hash_table_remove(&grandma->subtopics, &node->topic, NULL, NULL);

                        /* Make sure the following loop doesn't hit this node. */
                        --nodes_left;

                        if (i != sub_parts_len) {

                            /* Clean up and delete */
                            s_topic_node_destroy(node, tree->allocator);
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

                    struct aws_mqtt_topic_node *parent = NULL;
                    aws_array_list_get_at(&action->to_remove, &parent, nodes_left);
                    assert(parent); /* Must be in bounds */

                    size_t topic_offset =
                        parent->topic.ptr - aws_string_bytes(parent->topic_filter) + parent->topic.len + 1;

                    /* -1 to avoid touching current */
                    for (size_t i = nodes_left; i > 0; --i) {
                        aws_array_list_get_at(&action->to_remove, &parent, i);
                        assert(parent); /* Must be in bounds */

                        /* Remove this topic and following / from offset. */
                        topic_offset -= (parent->topic.len + 1);

                        if (parent->topic_filter == old_topic_filter) {
                            /* Uh oh, Mom's using my topic string again! Steal it and replace it with a new one, Indiana
                             * Jones style. */

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
                    s_topic_node_destroy(current, tree->allocator);
                }
                current = NULL;
            }

            break;
        }
    }

    s_topic_tree_action_destroy(action);
}

/*******************************************************************************
 * Action Roll Back
 ******************************************************************************/

static void s_topic_tree_action_roll_back(struct topic_tree_action *action, struct aws_mqtt_topic_tree *tree) {

    assert(action);

    switch (action->mode) {
        case AWS_MQTT_TOPIC_TREE_ADD: {
            /* Remove the first new node from it's parent's map */
            aws_hash_table_remove(&action->last_found->subtopics, &action->first_created->topic, NULL, NULL);
            /* Recursively destroy all other created nodes */
            s_topic_node_destroy(action->first_created, tree->allocator);

            if (action->topic_filter) {
                aws_string_destroy((void *)action->topic_filter);
            }

            break;
        }
        case AWS_MQTT_TOPIC_TREE_REMOVE:
        case AWS_MQTT_TOPIC_TREE_UPDATE: {
            /* Aborting a remove or update doesn't require any actions. */
            break;
        }
    }

    s_topic_tree_action_destroy(action);
}

/*******************************************************************************
 * Insert
 ******************************************************************************/

int aws_mqtt_topic_tree_transaction_insert(
    struct aws_mqtt_topic_tree *tree,
    struct aws_array_list *transaction,
    const struct aws_string *topic_filter,
    enum aws_mqtt_qos qos,
    aws_mqtt_publish_received_fn *callback,
    aws_mqtt_userdata_cleanup_fn *cleanup,
    void *userdata) {

    assert(tree);
    assert(transaction);
    assert(topic_filter);
    assert(callback);

    struct aws_mqtt_topic_node *current = tree->root;

    struct topic_tree_action *action = s_topic_tree_action_create(transaction);
    if (!action) {
        return AWS_OP_ERR;
    }

    /* Default to update unless a node was added */
    action->mode = AWS_MQTT_TOPIC_TREE_UPDATE;
    action->qos = qos;
    action->callback = callback;
    action->cleanup = cleanup;
    action->userdata = userdata;

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
            if (action->mode == AWS_MQTT_TOPIC_TREE_UPDATE) {
                /* Store the last found node */
                action->last_found = current;
            }

            /* Node does not exist, add new one */
            current = s_topic_node_new(tree->allocator, &sub_part, topic_filter);
            if (!current) {
                /* Don't do handle_error logic, the action needs to persist to be rolled back */
                return AWS_OP_ERR;
            }

            /* Stash in the hash map */
            elem->key = &current->topic;
            elem->value = current;

            if (action->mode == AWS_MQTT_TOPIC_TREE_UPDATE) {
                /* Store the node we just made, and make sure we don't store again */
                action->mode = AWS_MQTT_TOPIC_TREE_ADD;
                action->first_created = current;
            }
        } else {
            assert(action->mode == AWS_MQTT_TOPIC_TREE_UPDATE); /* Can't have found an existing node while adding */

            /* If the node exists, just traverse it */
            current = elem->value;
        }
    }

    action->node_to_update = current;

    /* Node found (or created), add the topic filter and callbacks */
    if (current->owns_topic_filter) {
        /* If the topic filter was already here, this is already a subscription.
           Free the new topic_filter so all existing byte_cursors remain valid. */
        aws_string_destroy((void *)topic_filter);
    } else {
        /* Node already existed (or was created) but wasn't subscription. */
        action->topic = last_part;
        action->topic_filter = topic_filter;
    }

    return AWS_OP_SUCCESS;
}

/*******************************************************************************
 * Remove
 ******************************************************************************/

int aws_mqtt_topic_tree_transaction_remove(
    struct aws_mqtt_topic_tree *tree,
    struct aws_array_list *transaction,
    const struct aws_byte_cursor *topic_filter) {

    assert(tree);
    assert(transaction);
    assert(topic_filter);

    struct topic_tree_action *action = s_topic_tree_action_create(transaction);
    if (!action) {
        return AWS_OP_ERR;
    }

    struct aws_array_list sub_topic_parts;
    AWS_ZERO_STRUCT(sub_topic_parts);

    if (aws_array_list_init_dynamic(&sub_topic_parts, tree->allocator, 1, sizeof(struct aws_byte_cursor))) {
        goto handle_error;
    }

    if (aws_byte_cursor_split_on_char(topic_filter, '/', &sub_topic_parts)) {
        goto handle_error;
    }
    const size_t sub_parts_len = aws_array_list_length(&sub_topic_parts);
    if (!sub_parts_len) {
        goto handle_error;
    }
    s_topic_tree_action_to_remove(action, tree->allocator, sub_parts_len);

    struct aws_mqtt_topic_node *current = tree->root;
    if (aws_array_list_push_back(&action->to_remove, &current)) {
        goto handle_error;
    }

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
            if (aws_array_list_push_back(&action->to_remove, &current)) {
                goto handle_error;
            }
        } else {
            /* If not, abandon ship */
            current = NULL;
            break;
        }
    }

    action->node_to_update = current;

    aws_array_list_clean_up(&sub_topic_parts);

    return AWS_OP_SUCCESS;

handle_error:
    aws_array_list_clean_up(&sub_topic_parts);

    s_topic_tree_action_destroy(action);
    aws_array_list_pop_back(transaction);

    return AWS_OP_ERR;
}

/*******************************************************************************
 * Commit
 ******************************************************************************/

void aws_mqtt_topic_tree_transaction_commit(struct aws_mqtt_topic_tree *tree, struct aws_array_list *transaction) {

    const size_t num_actions = aws_array_list_length(transaction);
    for (size_t i = 0; i < num_actions; ++i) {
        struct topic_tree_action *action = NULL;
        aws_array_list_get_at_ptr(transaction, (void **)&action, i);
        assert(action); /* Within bounds */

        s_topic_tree_action_commit(action, tree);
    }
    aws_array_list_clear(transaction);
}

/*******************************************************************************
 * Roll Back
 ******************************************************************************/

void aws_mqtt_topic_tree_transaction_roll_back(struct aws_mqtt_topic_tree *tree, struct aws_array_list *transaction) {

    const size_t num_actions = aws_array_list_length(transaction);
    for (size_t i = 1; i <= num_actions; ++i) {
        struct topic_tree_action *action = NULL;
        aws_array_list_get_at_ptr(transaction, (void **)&action, num_actions - i);
        assert(action); /* Within bounds */

        s_topic_tree_action_roll_back(action, tree);
    }
    aws_array_list_clear(transaction);
}

int aws_mqtt_topic_tree_insert(
    struct aws_mqtt_topic_tree *tree,
    const struct aws_string *topic_filter,
    enum aws_mqtt_qos qos,
    aws_mqtt_publish_received_fn *callback,
    aws_mqtt_userdata_cleanup_fn *cleanup,
    void *userdata) {

    AWS_VARIABLE_LENGTH_ARRAY(uint8_t, transaction_buf, aws_mqtt_topic_tree_action_size);
    struct aws_array_list transaction;
    aws_array_list_init_static(&transaction, transaction_buf, 1, aws_mqtt_topic_tree_action_size);

    if (aws_mqtt_topic_tree_transaction_insert(tree, &transaction, topic_filter, qos, callback, cleanup, userdata)) {

        aws_mqtt_topic_tree_transaction_roll_back(tree, &transaction);
        return AWS_OP_ERR;
    }

    aws_mqtt_topic_tree_transaction_commit(tree, &transaction);
    return AWS_OP_SUCCESS;
}

int aws_mqtt_topic_tree_remove(struct aws_mqtt_topic_tree *tree, const struct aws_byte_cursor *topic_filter) {

    AWS_VARIABLE_LENGTH_ARRAY(uint8_t, transaction_buf, aws_mqtt_topic_tree_action_size);
    struct aws_array_list transaction;
    aws_array_list_init_static(&transaction, transaction_buf, 1, aws_mqtt_topic_tree_action_size);

    if (aws_mqtt_topic_tree_transaction_remove(tree, &transaction, topic_filter)) {

        aws_mqtt_topic_tree_transaction_roll_back(tree, &transaction);
        return AWS_OP_ERR;
    }

    aws_mqtt_topic_tree_transaction_commit(tree, &transaction);
    return AWS_OP_SUCCESS;
}

/*******************************************************************************
 * Publish
 ******************************************************************************/

static void s_topic_tree_publish_do_recurse(
    const struct aws_byte_cursor *current_sub_part,
    const struct aws_mqtt_topic_node *current,
    const struct aws_mqtt_packet_publish *pub) {

    struct aws_byte_cursor hash_cur = aws_byte_cursor_from_string(s_multi_level_wildcard);
    struct aws_byte_cursor plus_cur = aws_byte_cursor_from_string(s_single_level_wildcard);

    struct aws_hash_element *elem = NULL;

    struct aws_byte_cursor sub_part = *current_sub_part;
    if (!aws_byte_cursor_next_split(&pub->topic_name, '/', &sub_part)) {

        /* If this is the last node and is a sub, call it */
        if (s_topic_node_is_subscription(current)) {
            current->callback(&pub->topic_name, &pub->payload, current->userdata);
        }
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
        s_topic_tree_publish_do_recurse(&sub_part, elem->value, pub);
    }

    /* Check actual topic name */
    aws_hash_table_find(&current->subtopics, &sub_part, &elem);
    if (elem) {
        /* Found the actual topic, recurse to it */
        s_topic_tree_publish_do_recurse(&sub_part, elem->value, pub);
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
