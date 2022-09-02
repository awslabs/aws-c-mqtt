/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/private/v5/mqtt5_topic_alias.h>

#include <aws/common/string.h>

int aws_mqtt5_inbound_topic_alias_resolver_init(
    struct aws_mqtt5_inbound_topic_alias_resolver *resolver,
    struct aws_allocator *allocator) {
    AWS_ZERO_STRUCT(*resolver);
    resolver->allocator = allocator;

    if (aws_array_list_init_dynamic(&resolver->topic_aliases, allocator, 0, sizeof(struct aws_string *))) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

static void s_release_aliases(struct aws_mqtt5_inbound_topic_alias_resolver *resolver) {
    size_t cache_size = aws_array_list_length(&resolver->topic_aliases);
    for (size_t i = 0; i < cache_size; ++i) {
        struct aws_string *topic = NULL;

        aws_array_list_get_at(&resolver->topic_aliases, &topic, i);
        aws_string_destroy(topic);
    }
}

void aws_mqtt5_inbound_topic_alias_resolver_clean_up(struct aws_mqtt5_inbound_topic_alias_resolver *resolver) {
    s_release_aliases(resolver);
    aws_array_list_clean_up(&resolver->topic_aliases);
}

int aws_mqtt5_inbound_topic_alias_resolver_reset(
    struct aws_mqtt5_inbound_topic_alias_resolver *resolver,
    uint16_t cache_size) {
    s_release_aliases(resolver);
    aws_array_list_clean_up(&resolver->topic_aliases);

    if (aws_array_list_init_dynamic(
            &resolver->topic_aliases, resolver->allocator, cache_size, sizeof(struct aws_string *))) {
        return AWS_OP_ERR;
    }

    for (size_t i = 0; i < cache_size; ++i) {
        struct aws_string *topic = NULL;
        aws_array_list_push_back(&resolver->topic_aliases, &topic);
    }

    return AWS_OP_SUCCESS;
}

int aws_mqtt5_inbound_topic_alias_resolver_resolve_alias(
    struct aws_mqtt5_inbound_topic_alias_resolver *resolver,
    uint16_t alias,
    struct aws_byte_cursor *topic_out) {
    size_t cache_size = aws_array_list_length(&resolver->topic_aliases);

    if (alias > cache_size || alias == 0) {
        return aws_raise_error(AWS_ERROR_MQTT5_INVALID_INBOUND_TOPIC_ALIAS);
    }

    size_t alias_index = alias - 1;
    struct aws_string *topic = NULL;
    aws_array_list_get_at(&resolver->topic_aliases, &topic, alias_index);

    if (topic == NULL) {
        return aws_raise_error(AWS_ERROR_MQTT5_INVALID_INBOUND_TOPIC_ALIAS);
    }

    *topic_out = aws_byte_cursor_from_string(topic);
    return AWS_OP_SUCCESS;
}

int aws_mqtt5_inbound_topic_alias_resolver_register_alias(
    struct aws_mqtt5_inbound_topic_alias_resolver *resolver,
    uint16_t alias,
    struct aws_byte_cursor topic) {
    size_t cache_size = aws_array_list_length(&resolver->topic_aliases);

    if (alias > cache_size || alias == 0) {
        return aws_raise_error(AWS_ERROR_MQTT5_INVALID_INBOUND_TOPIC_ALIAS);
    }

    struct aws_string *new_entry = aws_string_new_from_cursor(resolver->allocator, &topic);
    if (new_entry == NULL) {
        return AWS_OP_ERR;
    }

    size_t alias_index = alias - 1;
    struct aws_string *existing_entry = NULL;
    aws_array_list_get_at(&resolver->topic_aliases, &existing_entry, alias_index);
    aws_string_destroy(existing_entry);

    aws_array_list_set_at(&resolver->topic_aliases, &new_entry, alias_index);

    return AWS_OP_SUCCESS;
}

/****************************************************************************************************************/

struct aws_mqtt5_outbound_topic_alias_resolver_vtable {
    void (*destroy_fn)(struct aws_mqtt5_outbound_topic_alias_resolver *);
    int (*reset_fn)(struct aws_mqtt5_outbound_topic_alias_resolver *, uint16_t);
    int (*resolve_outbound_publish_fn)(
        struct aws_mqtt5_outbound_topic_alias_resolver *,
        const struct aws_mqtt5_packet_publish_view *,
        uint16_t *,
        struct aws_byte_cursor *);
};

struct aws_mqtt5_outbound_topic_alias_resolver {
    struct aws_allocator *allocator;

    struct aws_mqtt5_outbound_topic_alias_resolver_vtable *vtable;
    void *impl;
};

struct aws_mqtt5_outbound_topic_alias_resolver *aws_mqtt5_outbound_topic_alias_resolver_new(
    struct aws_allocator *allocator,
    enum aws_mqtt5_client_outbound_topic_alias_behavior_type outbound_alias_behavior) {
    (void)allocator;
    (void)outbound_alias_behavior;

    return NULL;
}

void aws_mqtt5_outbound_topic_alias_resolver_destroy(struct aws_mqtt5_outbound_topic_alias_resolver *resolver) {
    if (resolver == NULL) {
        return;
    }

    (*resolver->vtable->destroy_fn)(resolver);
}

int aws_mqtt5_outbound_topic_alias_manager_reset(
    struct aws_mqtt5_outbound_topic_alias_resolver *resolver,
    uint16_t topic_alias_maximum) {

    if (resolver == NULL) {
        return AWS_OP_ERR;
    }

    return (*resolver->vtable->reset_fn)(resolver, topic_alias_maximum);
}

int aws_mqtt5_outbound_topic_alias_manager_on_outbound_publish(
    struct aws_mqtt5_outbound_topic_alias_resolver *resolver,
    const struct aws_mqtt5_packet_publish_view *publish_view,
    uint16_t *topic_alias_out,
    struct aws_byte_cursor *topic_out) {
    if (resolver == NULL) {
        AWS_OP_ERR;
    }

    return (*resolver->vtable->resolve_outbound_publish_fn)(resolver, publish_view, topic_alias_out, topic_out);
}
