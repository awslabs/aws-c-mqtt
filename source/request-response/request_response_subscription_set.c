/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/private/request-response/request_response_subscription_set.h>

#include <aws/common/logging.h>
#include <aws/mqtt/mqtt.h>

void search(const struct aws_hash_table *subscriptions, const struct aws_byte_cursor *topic) {
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
