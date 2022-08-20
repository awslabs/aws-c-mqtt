/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/private/v5/mqtt5_topic_alias.h>

#include <aws/common/string.h>

int aws_mqtt5_inbound_topic_alias_manager_init(
    struct aws_mqtt5_inbound_topic_alias_manager *manager,
    struct aws_allocator *allocator) {
    AWS_ZERO_STRUCT(*manager);
    manager->allocator = allocator;

    if (aws_array_list_init_dynamic(&manager->topic_aliases, allocator, 0, sizeof(struct aws_string *))) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

static void s_release_aliases(struct aws_mqtt5_inbound_topic_alias_manager *manager) {
    size_t cache_size = aws_array_list_length(&manager->topic_aliases);
    for (size_t i = 0; i < cache_size; ++i) {
        struct aws_string *topic = NULL;

        aws_array_list_get_at(&manager->topic_aliases, &topic, i);
        aws_string_destroy(topic);
    }
}

void aws_mqtt5_inbound_topic_alias_manager_clean_up(struct aws_mqtt5_inbound_topic_alias_manager *manager) {
    s_release_aliases(manager);
    aws_array_list_clean_up(&manager->topic_aliases);
}

int aws_mqtt5_inbound_topic_alias_manager_reset(
    struct aws_mqtt5_inbound_topic_alias_manager *manager,
    uint16_t cache_size) {
    s_release_aliases(manager);
    aws_array_list_clean_up(&manager->topic_aliases);

    if (aws_array_list_init_dynamic(
            &manager->topic_aliases, manager->allocator, cache_size, sizeof(struct aws_string *))) {
        return AWS_OP_ERR;
    }

    for (size_t i = 0; i < cache_size; ++i) {
        struct aws_string *topic = NULL;
        aws_array_list_push_back(&manager->topic_aliases, &topic);
    }

    return AWS_OP_SUCCESS;
}

int aws_mqtt5_inbound_topic_alias_manager_resolve_alias(
    struct aws_mqtt5_inbound_topic_alias_manager *manager,
    uint16_t alias,
    struct aws_byte_cursor *topic_out) {
    size_t cache_size = aws_array_list_length(&manager->topic_aliases);

    if (alias > cache_size || alias == 0) {
        return aws_raise_error(AWS_ERROR_MQTT5_INVALID_INBOUND_TOPIC_ALIAS);
    }

    size_t alias_index = alias - 1;
    struct aws_string *topic = NULL;
    aws_array_list_get_at(&manager->topic_aliases, &topic, alias_index);

    if (topic == NULL) {
        return aws_raise_error(AWS_ERROR_MQTT5_INVALID_INBOUND_TOPIC_ALIAS);
    }

    *topic_out = aws_byte_cursor_from_string(topic);
    return AWS_OP_SUCCESS;
}

int aws_mqtt5_inbound_topic_alias_manager_register_alias(
    struct aws_mqtt5_inbound_topic_alias_manager *manager,
    uint16_t alias,
    struct aws_byte_cursor topic) {
    size_t cache_size = aws_array_list_length(&manager->topic_aliases);

    if (alias > cache_size || alias == 0) {
        return aws_raise_error(AWS_ERROR_MQTT5_INVALID_INBOUND_TOPIC_ALIAS);
    }

    struct aws_string *new_entry = aws_string_new_from_cursor(manager->allocator, &topic);
    if (new_entry == NULL) {
        return AWS_OP_ERR;
    }

    size_t alias_index = alias - 1;
    struct aws_string *existing_entry = NULL;
    aws_array_list_get_at(&manager->topic_aliases, &existing_entry, alias_index);
    aws_string_destroy(existing_entry);

    aws_array_list_set_at(&manager->topic_aliases, &new_entry, alias_index);

    return AWS_OP_SUCCESS;
}
