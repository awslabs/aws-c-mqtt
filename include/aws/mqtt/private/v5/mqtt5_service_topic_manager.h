#ifndef AWS_MQTT5_PRIVATE_SERVICE_TOPIC_MANAGER_H
#define AWS_MQTT5_PRIVATE_SERVICE_TOPIC_MANAGER_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include <aws/common/hash_table.h>
#include <aws/mqtt/exports.h>

struct aws_mqtt5_service_topic_node {

    /* The topic name, key of the node. */
    struct aws_string *topic;

    /* user defined data type, the topic node did not own the userdata */
    void *userdata;
};

struct aws_mqtt5_service_topic_manager {
    // The topic map will be a hash map for <struct aws_byte_cursor,struct aws_mqtt5_service_topic_node*>
    struct aws_hash_table *topic_map;
    struct aws_allocator *allocator;
};

/**
 * Initialize a topic manager with an allocator to later use.
 * Note that calling init allocates root.
 */
AWS_MQTT_API int aws_mqtt5_service_topic_manager_init(
    struct aws_mqtt5_service_topic_manager *manager,
    struct aws_allocator *allocator);
/**
 * Cleanup and deallocate an entire topic manager.
 */
AWS_MQTT_API int aws_mqtt5_service_topic_manager_clean_up(struct aws_mqtt5_service_topic_manager *manager);

/**
 * Insert a new topic into the topic manager
 *
 * \param[in]  manager      The manager to insert into.
 * \param[in]  topic_name   The topic filter to subscribe on. May contain wildcards.
 * \param[in]  userdata     The userdata to pass to callback.
 *
 * \returns AWS_OP_SUCCESS on successful insertion, AWS_OP_ERR with aws_last_error() populated on failure.
 */
AWS_MQTT_API int aws_mqtt5_service_topic_manager_insert(
    struct aws_mqtt5_service_topic_manager *manager,
    const struct aws_byte_cursor *topic_name,
    void *userdata);

/**
 * Remove a topic name from the manager (unsubscribe).
 *
 * \param[in]  manager         The manager to remove from.
 * \param[in]  topic_name      The filter to remove (must be exactly the same as the topic_name passed to insert).
 *
 * \returns AWS_OP_SUCCESS on successful removal, AWS_OP_ERR with aws_last_error() populated on failure.
 */
AWS_MQTT_API int aws_mqtt5_service_topic_manager_remove(
    struct aws_mqtt5_service_topic_manager *manager,
    const struct aws_byte_cursor *topic_name);

/**
 * Find the topic node with specified topic name
 *
 * \param[in]  manager         The manager to remove from.
 * \param[in]  topic_name      The filter to remove (must be exactly the same as the topic_name passed to insert).
 * \param[out] topic_node      The topic node returned from the hash table
 *
 * \returns AWS_OP_SUCCESS on successful found, AWS_OP_ERR with aws_last_error() populated on failure.
 */
AWS_MQTT_API int aws_mqtt5_topic_manager_find(
    const struct aws_mqtt5_service_topic_manager *manager,
    const struct aws_byte_cursor *topic_name,
    struct aws_mqtt5_service_topic_node **topic_node);

int aws_mqtt5_service_topic_node_init(
    struct aws_allocator *allocator,
    struct aws_mqtt5_service_topic_node *node,
    struct aws_byte_cursor *topic,
    void *userdata);

int aws_mqtt5_service_topic_node_clean_up(struct aws_mqtt5_service_topic_node *node);

#endif /* AWS_MQTT5_PRIVATE_SERVICE_TOPIC_MANAGER_H */
