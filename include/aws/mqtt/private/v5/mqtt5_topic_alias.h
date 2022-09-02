/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#ifndef AWS_MQTT_MQTT5_TOPIC_ALIAS_H
#define AWS_MQTT_MQTT5_TOPIC_ALIAS_H

#include <aws/mqtt/mqtt.h>

#include <aws/common/array_list.h>
#include <aws/mqtt/mqtt5_client.h>

struct aws_mqtt5_outbound_topic_alias_resolver;

struct aws_mqtt5_inbound_topic_alias_resolver {
    struct aws_allocator *allocator;

    struct aws_array_list topic_aliases;
};

AWS_EXTERN_C_BEGIN

int aws_mqtt5_inbound_topic_alias_resolver_init(
    struct aws_mqtt5_inbound_topic_alias_resolver *resolver,
    struct aws_allocator *allocator);

void aws_mqtt5_inbound_topic_alias_resolver_clean_up(struct aws_mqtt5_inbound_topic_alias_resolver *resolver);

int aws_mqtt5_inbound_topic_alias_resolver_reset(
    struct aws_mqtt5_inbound_topic_alias_resolver *resolver,
    uint16_t cache_size);

int aws_mqtt5_inbound_topic_alias_resolver_resolve_alias(
    struct aws_mqtt5_inbound_topic_alias_resolver *resolver,
    uint16_t alias,
    struct aws_byte_cursor *topic_out);

int aws_mqtt5_inbound_topic_alias_resolver_register_alias(
    struct aws_mqtt5_inbound_topic_alias_resolver *resolver,
    uint16_t alias,
    struct aws_byte_cursor topic);

struct aws_mqtt5_outbound_topic_alias_resolver *aws_mqtt5_outbound_topic_alias_resolver_new(
    struct aws_allocator *allocator,
    enum aws_mqtt5_client_outbound_topic_alias_behavior_type outbound_alias_behavior);

void aws_mqtt5_outbound_topic_alias_resolver_destroy(struct aws_mqtt5_outbound_topic_alias_resolver *resolver);

int aws_mqtt5_outbound_topic_alias_resolver_reset(
    struct aws_mqtt5_outbound_topic_alias_resolver *resolver,
    uint16_t topic_alias_maximum);

int aws_mqtt5_outbound_topic_alias_resolver_resolve_outbound_publish(
    struct aws_mqtt5_outbound_topic_alias_resolver *resolver,
    const struct aws_mqtt5_packet_publish_view *publish_view,
    uint16_t *topic_alias_out,
    struct aws_byte_cursor *topic_out);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_MQTT5_TOPIC_ALIAS_H */
