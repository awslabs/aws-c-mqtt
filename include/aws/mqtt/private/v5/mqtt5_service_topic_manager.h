#ifndef AWS_MQTT5_PRIVATE_SERVICE_TOPIC_MANAGER_H
#define AWS_MQTT5_PRIVATE_SERVICE_TOPIC_MANAGER_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/hash_table.h>

struct aws_mqtt5_service_topic_node {

    /* This node's part of the topic filter. If in another node's subtopics, this is the key. */
    struct aws_byte_cursor topic;

    /* user defined data type */
    void *userdata;
};

struct aws_mqtt5_service_topic_manager {
    struct aws_mqtt5_service_topic_node *root;
    struct aws_allocator *allocator;
};

/**
 * Initialize a topic manager with an allocator to later use.
 * Note that calling init allocates root.
 */
AWS_MQTT5_API int aws_mqtt5_service_topic_manager_init(struct aws_mqtt5_service_topic_manager *manager, struct aws_allocator *allocator);
/**
 * Cleanup and deallocate an entire topic manager.
 */
AWS_MQTT5_API void aws_mqtt5_service_topic_manager_destory(struct aws_mqtt5_service_topic_manager *manager);

/**
 * Insert a new topic into the topic manager
 *
 * \param[in]  manager      The manager to insert into.
 * \param[in]  topic_name   The topic filter to subscribe on. May contain wildcards.
 * \param[in]  userdata     The userdata to pass to callback.
 *
 * \returns AWS_OP_SUCCESS on successful insertion, AWS_OP_ERR with aws_last_error() populated on failure.
 *          If AWS_OP_ERR is returned, aws_mqtt5_topic_manager_transaction_rollback should be called to prevent leaks.
 */
AWS_MQTT5_API int aws_mqtt5_topic_manager_insert(
    struct aws_mqtt5_topic_manager *manager,
    const struct aws_byte_cursor *topic_name,
    void *userdata);

/**
 * Remove a topic name from the manager (unsubscribe).
 *
 * \param[in]  manager         The manager to remove from.
 * \param[in]  topic_name      The filter to remove (must be exactly the same as the topic_name passed to insert).
 *
 * \returns AWS_OP_SUCCESS on successful removal, AWS_OP_ERR with aws_last_error() populated on failure.
 *          If AWS_OP_ERR is returned, aws_mqtt5_topic_manager_transaction_rollback should be called to prevent leaks.
 */
AWS_MQTT5_API int aws_mqtt5_topic_manager_remove(
    struct aws_mqtt5_topic_manager *manager,
    const struct aws_byte_cursor *topic_name);

/**
 * Find the topic node with specified topic name
 *
 * \param[in]  manager         The manager to remove from.
 * \param[in]  topic_name      The filter to remove (must be exactly the same as the topic_name passed to insert).
 *
 * \returns aws_mqtt5_service_topic_node Return the node with sepcified topic name
 */
struct aws_mqtt5_service_topic_node* AWS_MQTT5_API
    aws_mqtt5_topic_manager_find(const struct aws_mqtt5_topic_manager *manager, const struct aws_byte_cursor *topic_name);

int aws_mqtt5_service_topic_node_init(aws_allocator * allocator,
    struct aws_byte_cursor* topic
    void* userdata);

int aws_mqtt5_service_topic_node_destory(aws_mqtt5_topic_node* node);

#endif /* AWS_MQTT5_PRIVATE_SERVICE_TOPIC_MANAGER_H */
