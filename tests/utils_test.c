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

#include <aws/testing/aws_test_harness.h>

#include <aws/common/string.h>

static struct aws_byte_cursor s_empty_cursor = {
    .ptr = NULL,
    .len = 0,
};

static bool s_check_topic_match(struct aws_allocator *allocator, const char *sub_filter, const char *pub_topic) {

    struct aws_byte_cursor filter_cursor = aws_byte_cursor_from_array(pub_topic, strlen(pub_topic));
    struct aws_mqtt_packet_publish publish;
    aws_mqtt_packet_publish_init(&publish, false, AWS_MQTT_QOS_EXACTLY_ONCE, false, filter_cursor, 1, s_empty_cursor);

    struct aws_mqtt_subscription_impl subscription;
    subscription.filter = aws_string_new_from_c_str(allocator, sub_filter);
    subscription.subscription.qos = AWS_MQTT_QOS_EXACTLY_ONCE;
    subscription.subscription.topic_filter = aws_byte_cursor_from_string(subscription.filter);

    bool result = aws_mqtt_subscription_matches_publish(allocator, &subscription, &publish);

    aws_string_destroy((void *)subscription.filter);

    return result;
}

AWS_TEST_CASE(mqtt_utils_topic_match, s_mqtt_utils_topic_match_fn)
static int s_mqtt_utils_topic_match_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    /* Check single-level filters */
    ASSERT_TRUE(s_check_topic_match(allocator, "a", "a"));
    ASSERT_FALSE(s_check_topic_match(allocator, "b", "B"));

    /* Check multi-level filters */
    ASSERT_TRUE(s_check_topic_match(allocator, "a/b", "a/b"));
    ASSERT_FALSE(s_check_topic_match(allocator, "a/b", "a/B"));
    ASSERT_FALSE(s_check_topic_match(allocator, "a/b", "a/b/c"));
    ASSERT_FALSE(s_check_topic_match(allocator, "a/b/c", "a/b"));
    ASSERT_TRUE(s_check_topic_match(allocator, "a/b/c", "a/b/c"));
    ASSERT_FALSE(s_check_topic_match(allocator, "a/b/c", "a/B/c"));

    /* Check single-level wildcard filters */
    ASSERT_TRUE(s_check_topic_match(allocator, "sport/tennis/+", "sport/tennis/player1"));
    ASSERT_TRUE(s_check_topic_match(allocator, "sport/tennis/+", "sport/tennis/player2"));
    ASSERT_FALSE(s_check_topic_match(allocator, "sport/tennis/+", "sport/tennis/player1/ranking"));

    ASSERT_TRUE(s_check_topic_match(allocator, "sport/+", "sport/"));
    ASSERT_FALSE(s_check_topic_match(allocator, "sport/+", "sport"));

    ASSERT_TRUE(s_check_topic_match(allocator, "+/+", "/finance"));
    ASSERT_TRUE(s_check_topic_match(allocator, "/+", "/finance"));
    ASSERT_FALSE(s_check_topic_match(allocator, "+", "/finance"));

    return AWS_OP_SUCCESS;
}
