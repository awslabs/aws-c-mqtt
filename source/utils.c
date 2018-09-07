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

#include <aws/mqtt/private/utils.h>

#include <aws/common/byte_buf.h>
#include <aws/common/task_scheduler.h>

AWS_STATIC_STRING_FROM_LITERAL(s_single_level_wildcard, "+");
AWS_STATIC_STRING_FROM_LITERAL(s_multi_level_wildcard, "#");

bool aws_mqtt_subscription_matches_publish(
    struct aws_allocator *allocator,
    struct aws_mqtt_subscription_impl *sub,
    struct aws_mqtt_packet_publish *pub) {

    bool result = true;

    struct aws_byte_buf sub_topic = aws_byte_buf_from_array(aws_string_bytes(sub->filter), sub->filter->len);
    struct aws_byte_buf pub_topic = aws_byte_buf_from_array(pub->topic_name.ptr, pub->topic_name.len);

    struct aws_array_list sub_topic_parts;
    struct aws_array_list pub_topic_parts;

    aws_array_list_init_dynamic(&sub_topic_parts, allocator, 1, sizeof(struct aws_byte_cursor));
    aws_array_list_init_dynamic(&pub_topic_parts, allocator, 1, sizeof(struct aws_byte_cursor));

    aws_byte_buf_split_on_char(&sub_topic, '/', &sub_topic_parts);
    aws_byte_buf_split_on_char(&pub_topic, '/', &pub_topic_parts);

    size_t sub_parts_len = aws_array_list_length(&sub_topic_parts);
    size_t pub_parts_len = aws_array_list_length(&pub_topic_parts);

    /* This should never happen, but just in case */
    if (!sub_parts_len || !pub_parts_len) {
        result = false;
        goto clean_up;
    }

    /* The sub topic can have wildcards, but if the sub topic has more parts than the pub topic, it can't be a match */
    if (sub_parts_len > pub_parts_len) {
        result = false;
        goto clean_up;
    }

    struct aws_byte_cursor *sub_part = NULL;
    struct aws_byte_cursor *pub_part = NULL;

    aws_array_list_get_at_ptr(&sub_topic_parts, (void **)&sub_part, 0);
    aws_array_list_get_at_ptr(&pub_topic_parts, (void **)&pub_part, 0);

    /* [MQTT-4.7.2-1] If publish topic starts with $, disregard any subs with top level wildcards */
    if (pub_part->ptr[0] == '$') {
        if (aws_string_eq_byte_cursor(s_single_level_wildcard, sub_part) ||
            aws_string_eq_byte_cursor(s_multi_level_wildcard, sub_part)) {

            result = false;
            goto clean_up;
        }
    }

    size_t part_idx = 1;
    do {

        /* Check single-level wildcard */
        if (aws_string_eq_byte_cursor(s_single_level_wildcard, sub_part)) {
            continue;
        }

        /* Check multi level wildcard */
        if (aws_string_eq_byte_cursor(s_multi_level_wildcard, sub_part)) {
            if (part_idx != sub_parts_len - 1) {
                /* [MQTT-4.7.1-2] multi level wildcard must be the last part of a topic. */
                result = false;
            } else {
                result = true;
            }
            /* Skip parts length check if multi-level found */
            goto clean_up;
        }

        if (!aws_byte_cursor_eq(sub_part, pub_part)) {
            result = false;
            goto clean_up;
        }

        aws_array_list_get_at_ptr(&sub_topic_parts, (void **)&sub_part, part_idx);
        aws_array_list_get_at_ptr(&pub_topic_parts, (void **)&pub_part, part_idx);
    } while (part_idx++ < sub_parts_len);

    /* If we didn't match all parts of the published topic, then it's an incomplete match. */
    if (sub_parts_len != pub_parts_len) {
        result = false;
    }

clean_up:
    aws_array_list_clean_up(&sub_topic_parts);
    aws_array_list_clean_up(&pub_topic_parts);

    return result;
}

void aws_channel_schedule_or_run_task(
    struct aws_channel *channel,
    struct aws_task *task) {

    if (aws_channel_thread_is_callers_thread(channel)) {

        task->fn(task->arg, AWS_TASK_STATUS_RUN_READY);
    } else {

        aws_channel_schedule_task(channel, task, 0);
    }
}
