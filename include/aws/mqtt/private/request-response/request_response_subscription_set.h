#ifndef AWS_MQTT_PRIVATE_REQUEST_RESPONSE_REQUEST_RESPONSE_SUBSCRIPTION_SET_H
#define AWS_MQTT_PRIVATE_REQUEST_RESPONSE_REQUEST_RESPONSE_SUBSCRIPTION_SET_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/byte_buf.h>
#include <aws/common/hash_table.h>
#include <aws/common/linked_list.h>
#include <aws/mqtt/exports.h>
#include <aws/mqtt/private/request-response/protocol_adapter.h>

struct aws_mqtt_request_response_client;

/* Holds subscriptions for request-response client. */
struct aws_request_response_subscriptions {
    struct aws_allocator *allocator;

    /*
     * Map from cursor (topic filter) -> list of streaming operations using that filter
     */
    struct aws_hash_table streaming_operation_subscription_lists;

    /*
     * Map from cursor (topic filter with wildcards) -> list of streaming operations using that filter
     */
    struct aws_hash_table streaming_operation_wildcards_subscription_lists;

    /*
     * Map from cursor (topic) -> request response path (topic, correlation token json path)
     *
     * We don't garbage collect this table over the course of normal client operation.  We only clean it up
     * when the client is shutting down.
     */
    struct aws_hash_table request_response_paths;
};

/*
 * This is the (key and) value in hash table (4) above.
 */
struct aws_rr_operation_list_topic_filter_entry {
    struct aws_allocator *allocator;

    struct aws_byte_cursor topic_filter_cursor;
    struct aws_byte_buf topic_filter;

    struct aws_linked_list operations;
};

struct aws_rr_response_path_entry {
    struct aws_allocator *allocator;

    size_t ref_count;

    struct aws_byte_cursor topic_cursor;
    struct aws_byte_buf topic;

    struct aws_byte_buf correlation_token_json_path;
};

typedef void(aws_mqtt_stream_operation_subscription_match_fn)(
    struct aws_rr_operation_list_topic_filter_entry *entry,
    const struct aws_protocol_adapter_incoming_publish_event *publish_event);

typedef void(aws_mqtt_request_operation_subscription_match_fn)(
    struct aws_mqtt_request_response_client *rr_client,
    struct aws_rr_response_path_entry *entry,
    const struct aws_protocol_adapter_incoming_publish_event *publish_event);

AWS_EXTERN_C_BEGIN

AWS_MQTT_API void aws_mqtt_request_response_client_subscriptions_init(
    struct aws_request_response_subscriptions *subscriptions,
    struct aws_allocator *allocator);

AWS_MQTT_API void aws_mqtt_request_response_client_subscriptions_cleanup(
    struct aws_request_response_subscriptions *subscriptions);

AWS_MQTT_API struct aws_rr_operation_list_topic_filter_entry *
    aws_mqtt_request_response_client_subscriptions_add_stream_subscription(
        struct aws_mqtt_request_response_client *client,
        struct aws_request_response_subscriptions *subscriptions,
        const struct aws_byte_cursor *topic_filter);

AWS_MQTT_API int aws_mqtt_request_response_client_subscriptions_add_request_subscription(
    struct aws_request_response_subscriptions *subscriptions,
    const struct aws_array_list *paths);

AWS_MQTT_API void aws_mqtt_request_response_client_subscriptions_match(
    const struct aws_request_response_subscriptions *subscriptions,
    const struct aws_byte_cursor *topic,
    aws_mqtt_stream_operation_subscription_match_fn *on_stream_operation_subscription_match,
    aws_mqtt_request_operation_subscription_match_fn *on_request_operation_subscription_match,
    const struct aws_protocol_adapter_incoming_publish_event *publish_event,
    struct aws_mqtt_request_response_client *rr_client);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_PRIVATE_REQUEST_RESPONSE_REQUEST_RESPONSE_SUBSCRIPTION_SET_H */
