/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/private/request-response/request_response_subscription_set.h>

#include <aws/common/logging.h>
#include <aws/mqtt/mqtt.h>
#include <aws/mqtt/private/client_impl_shared.h>
#include <aws/mqtt/request-response/request_response_client.h>

#define MQTT_RR_CLIENT_RESPONSE_TABLE_DEFAULT_SIZE 50
#define MQTT_RR_CLIENT_OPERATION_TABLE_DEFAULT_SIZE 50

static void s_aws_rr_operation_list_topic_filter_entry_destroy(struct aws_rr_operation_list_topic_filter_entry *entry) {
    if (entry == NULL) {
        return;
    }

    aws_byte_buf_clean_up(&entry->topic_filter);

    aws_mem_release(entry->allocator, entry);
}

static void s_aws_rr_operation_list_topic_filter_entry_hash_element_destroy(void *value) {
    s_aws_rr_operation_list_topic_filter_entry_destroy(value);
}

static void s_aws_rr_response_path_entry_destroy(struct aws_rr_response_path_entry *entry) {
    if (entry == NULL) {
        return;
    }

    aws_byte_buf_clean_up(&entry->topic);
    aws_byte_buf_clean_up(&entry->correlation_token_json_path);

    aws_mem_release(entry->allocator, entry);
}

static void s_aws_rr_response_path_table_hash_element_destroy(void *value) {
    s_aws_rr_response_path_entry_destroy(value);
}

void aws_mqtt_request_response_client_subscriptions_init(
    struct aws_request_response_subscriptions *subscriptions,
    struct aws_allocator *allocator) {

    subscriptions->allocator = allocator;

    aws_hash_table_init(
        &subscriptions->streaming_operation_subscription_lists,
        allocator,
        MQTT_RR_CLIENT_OPERATION_TABLE_DEFAULT_SIZE,
        aws_hash_byte_cursor_ptr,
        aws_mqtt_byte_cursor_hash_equality,
        NULL,
        s_aws_rr_operation_list_topic_filter_entry_hash_element_destroy);

    aws_hash_table_init(
        &subscriptions->streaming_operation_wildcards_subscription_lists,
        allocator,
        MQTT_RR_CLIENT_OPERATION_TABLE_DEFAULT_SIZE,
        aws_hash_byte_cursor_ptr,
        aws_mqtt_byte_cursor_hash_equality,
        NULL,
        s_aws_rr_operation_list_topic_filter_entry_hash_element_destroy);

    aws_hash_table_init(
        &subscriptions->request_response_paths,
        allocator,
        MQTT_RR_CLIENT_RESPONSE_TABLE_DEFAULT_SIZE,
        aws_hash_byte_cursor_ptr,
        aws_mqtt_byte_cursor_hash_equality,
        NULL,
        s_aws_rr_response_path_table_hash_element_destroy);
}

void aws_mqtt_request_response_client_subscriptions_cleanup(struct aws_request_response_subscriptions *subscriptions) {
    aws_hash_table_clean_up(&subscriptions->streaming_operation_subscription_lists);
    aws_hash_table_clean_up(&subscriptions->streaming_operation_wildcards_subscription_lists);
    aws_hash_table_clean_up(&subscriptions->request_response_paths);
}

static struct aws_rr_operation_list_topic_filter_entry *s_aws_rr_operation_list_topic_filter_entry_new(
    struct aws_allocator *allocator,
    struct aws_byte_cursor topic_filter) {
    struct aws_rr_operation_list_topic_filter_entry *entry =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_rr_operation_list_topic_filter_entry));

    entry->allocator = allocator;
    aws_byte_buf_init_copy_from_cursor(&entry->topic_filter, allocator, topic_filter);
    entry->topic_filter_cursor = aws_byte_cursor_from_buf(&entry->topic_filter);

    aws_linked_list_init(&entry->operations);

    return entry;
}

struct aws_rr_operation_list_topic_filter_entry *aws_mqtt_request_response_client_subscriptions_add_stream_subscription(
    struct aws_mqtt_request_response_client *client,
    struct aws_request_response_subscriptions *subscriptions,
    const struct aws_byte_cursor *topic_filter) {

    bool is_topic_with_wildcard =
        (memchr(topic_filter->ptr, '+', topic_filter->len) || memchr(topic_filter->ptr, '#', topic_filter->len));

    struct aws_hash_table *subscription_lists = is_topic_with_wildcard
                                                    ? &subscriptions->streaming_operation_wildcards_subscription_lists
                                                    : &subscriptions->streaming_operation_subscription_lists;

    struct aws_hash_element *element = NULL;
    if (aws_hash_table_find(subscription_lists, topic_filter, &element)) {
        aws_raise_error(AWS_ERROR_MQTT_REQUEST_RESPONSE_INTERNAL_ERROR);
        return NULL;
    }

    struct aws_rr_operation_list_topic_filter_entry *entry = NULL;
    if (element == NULL) {
        entry = s_aws_rr_operation_list_topic_filter_entry_new(subscriptions->allocator, *topic_filter);
        aws_hash_table_put(subscription_lists, &entry->topic_filter_cursor, entry, NULL);
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT_REQUEST_RESPONSE,
            "id=%p: request-response client adding wildcard topic filter '" PRInSTR
            "' to streaming subscriptions table",
            (void *)client,
            AWS_BYTE_CURSOR_PRI(*topic_filter));
    } else {
        entry = element->value;
    }

    AWS_FATAL_ASSERT(entry != NULL);

    return entry;
}

static struct aws_rr_response_path_entry *s_aws_rr_response_path_entry_new(
    struct aws_allocator *allocator,
    struct aws_byte_cursor topic,
    struct aws_byte_cursor correlation_token_json_path) {
    struct aws_rr_response_path_entry *entry = aws_mem_calloc(allocator, 1, sizeof(struct aws_rr_response_path_entry));

    entry->allocator = allocator;
    entry->ref_count = 1;
    aws_byte_buf_init_copy_from_cursor(&entry->topic, allocator, topic);
    entry->topic_cursor = aws_byte_cursor_from_buf(&entry->topic);

    aws_byte_buf_init_copy_from_cursor(&entry->correlation_token_json_path, allocator, correlation_token_json_path);

    return entry;
}

int aws_mqtt_request_response_client_subscriptions_add_request_subscription(
    struct aws_request_response_subscriptions *subscriptions,
    const struct aws_array_list *paths) {

    size_t path_count = aws_array_list_length(paths);
    for (size_t i = 0; i < path_count; ++i) {
        struct aws_mqtt_request_operation_response_path path;
        aws_array_list_get_at(paths, &path, i);

        struct aws_hash_element *element = NULL;
        if (aws_hash_table_find(&subscriptions->request_response_paths, &path.topic, &element)) {
            return aws_raise_error(AWS_ERROR_MQTT_REQUEST_RESPONSE_INTERNAL_ERROR);
        }

        if (element != NULL) {
            struct aws_rr_response_path_entry *entry = element->value;
            ++entry->ref_count;
            continue;
        }

        struct aws_rr_response_path_entry *entry =
            s_aws_rr_response_path_entry_new(subscriptions->allocator, path.topic, path.correlation_token_json_path);
        if (aws_hash_table_put(&subscriptions->request_response_paths, &entry->topic_cursor, entry, NULL)) {
            s_aws_rr_response_path_entry_destroy(entry);
            return aws_raise_error(AWS_ERROR_MQTT_REQUEST_RESPONSE_INTERNAL_ERROR);
        }
    }

    return AWS_OP_SUCCESS;
}

static void s_match_wildcard_stream_subscriptions(
    const struct aws_hash_table *subscriptions,
    const struct aws_byte_cursor *topic) {

    AWS_LOGF_INFO(
        AWS_LS_MQTT_REQUEST_RESPONSE, "= Looking subscription for topic '" PRInSTR "'", AWS_BYTE_CURSOR_PRI(*topic));

    for (struct aws_hash_iter iter = aws_hash_iter_begin(subscriptions); !aws_hash_iter_done(&iter);
         aws_hash_iter_next(&iter)) {
        struct aws_rr_operation_list_topic_filter_entry *entry = iter.element.value;
        AWS_LOGF_INFO(
            AWS_LS_MQTT_REQUEST_RESPONSE,
            "= Checking subscription with topic filter " PRInSTR,
            AWS_BYTE_CURSOR_PRI(entry->topic_filter_cursor));

        struct aws_byte_cursor subscription_topic_filter_segment;
        AWS_ZERO_STRUCT(subscription_topic_filter_segment);

        struct aws_byte_cursor topic_segment;
        AWS_ZERO_STRUCT(topic_segment);

        bool match = true;

        while (aws_byte_cursor_next_split(&entry->topic_filter_cursor, '/', &subscription_topic_filter_segment)) {
            AWS_LOGF_INFO(
                AWS_LS_MQTT_REQUEST_RESPONSE,
                "=== subscription topic filter segment is '" PRInSTR "'",
                AWS_BYTE_CURSOR_PRI(subscription_topic_filter_segment));

            if (!aws_byte_cursor_next_split(topic, '/', &topic_segment)) {
                AWS_LOGF_INFO(AWS_LS_MQTT_REQUEST_RESPONSE, "=== topic segment is NULL");
                match = false;
                break;
            }

            AWS_LOGF_INFO(
                AWS_LS_MQTT_REQUEST_RESPONSE,
                "======= topic segment is '" PRInSTR "'",
                AWS_BYTE_CURSOR_PRI(topic_segment));

            if (!aws_byte_cursor_eq_c_str(&subscription_topic_filter_segment, "+") &&
                !aws_byte_cursor_eq_ignore_case(&topic_segment, &subscription_topic_filter_segment)) {
                AWS_LOGF_INFO(
                    AWS_LS_MQTT_REQUEST_RESPONSE, "======= topic segment differs", AWS_BYTE_CURSOR_PRI(topic_segment));
                match = false;
                break;
            }
        }

        if (aws_byte_cursor_next_split(topic, '/', &topic_segment)) {
            match = false;
        }

        if (match) {
            AWS_LOGF_INFO(AWS_LS_MQTT_REQUEST_RESPONSE, "=== found subscription match");
        } else {
            AWS_LOGF_INFO(AWS_LS_MQTT_REQUEST_RESPONSE, "=== this is not the right subscription");
        }
    }
}

void aws_mqtt_request_response_client_subscriptions_match(
    const struct aws_request_response_subscriptions *subscriptions,
    const struct aws_byte_cursor *topic,
    aws_mqtt_stream_operation_subscription_match_fn *on_stream_operation_subscription_match,
    aws_mqtt_request_operation_subscription_match_fn *on_request_operation_subscription_match,
    const struct aws_protocol_adapter_incoming_publish_event *publish_event,
    struct aws_mqtt_request_response_client *rr_client) {

    /* Streaming operation handling */
    struct aws_hash_element *subscription_filter_element = NULL;
    if (aws_hash_table_find(
            &subscriptions->streaming_operation_subscription_lists, topic, &subscription_filter_element) ==
            AWS_OP_SUCCESS &&
        subscription_filter_element != NULL) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT_REQUEST_RESPONSE,
            "id=%p: request-response client incoming publish on topic '" PRInSTR "' matches streaming topic",
            (void *)rr_client,
            AWS_BYTE_CURSOR_PRI(*topic));

        on_stream_operation_subscription_match(subscription_filter_element->value, publish_event);
    }

    s_match_wildcard_stream_subscriptions(&subscriptions->streaming_operation_wildcards_subscription_lists, topic);

    /* Request-Response handling */
    struct aws_hash_element *response_path_element = NULL;
    if (aws_hash_table_find(&subscriptions->request_response_paths, &publish_event->topic, &response_path_element) ==
            AWS_OP_SUCCESS &&
        response_path_element != NULL) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT_REQUEST_RESPONSE,
            "id=%p: request-response client incoming publish on topic '" PRInSTR "' matches response path",
            (void *)rr_client,
            AWS_BYTE_CURSOR_PRI(publish_event->topic));

        on_request_operation_subscription_match(rr_client, response_path_element->value, publish_event);
    }
}
