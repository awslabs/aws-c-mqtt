/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#ifndef AWS_MQTT_MQTT5_TOPIC_ALIAS_H
#define AWS_MQTT_MQTT5_TOPIC_ALIAS_H

#include <aws/mqtt/mqtt.h>

#include <aws/common/array_list.h>

struct aws_mqtt5_inbound_topic_alias_manager {
    struct aws_allocator *allocator;

    struct aws_array_list topic_aliases;
};

struct aws_mqtt5_outbound_topic_alias_manager {
    struct aws_allocator *allocator;
};

AWS_EXTERN_C_BEGIN

int aws_mqtt5_inbound_topic_alias_manager_init(
    struct aws_mqtt5_inbound_topic_alias_manager *manager,
    struct aws_allocator *allocator);

void aws_mqtt5_inbound_topic_alias_manager_clean_up(struct aws_mqtt5_inbound_topic_alias_manager *manager);

int aws_mqtt5_inbound_topic_alias_manager_reset(
    struct aws_mqtt5_inbound_topic_alias_manager *manager,
    uint16_t cache_size);

int aws_mqtt5_inbound_topic_alias_manager_resolve_alias(
    struct aws_mqtt5_inbound_topic_alias_manager *manager,
    uint16_t alias,
    struct aws_byte_cursor *topic_out);

int aws_mqtt5_inbound_topic_alias_manager_register_alias(
    struct aws_mqtt5_inbound_topic_alias_manager *manager,
    uint16_t alias,
    struct aws_byte_cursor topic);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_MQTT5_TOPIC_ALIAS_H */
