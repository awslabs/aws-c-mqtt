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

#include <aws/mqtt/private/topic_tree.h>

#include <aws/testing/aws_test_harness.h>

#include <aws/common/string.h>

static struct aws_byte_cursor s_empty_cursor = {
    .ptr = NULL,
    .len = 0,
};

static bool was_called = false;
static void on_publish(const struct aws_byte_cursor *topic, const struct aws_byte_cursor *payload, void *user_data) {

    (void)topic;
    (void)payload;
    (void)user_data;

    was_called = true;
}

static bool s_check_topic_match(struct aws_allocator *allocator, const char *sub_filter, const char *pub_topic) {

    was_called = false;

    struct aws_mqtt_topic_tree tree;
    aws_mqtt_topic_tree_init(&tree, allocator);

    struct aws_string *topic_filter = aws_string_new_from_c_str(allocator, sub_filter);
    aws_mqtt_topic_tree_insert(&tree, topic_filter, AWS_MQTT_QOS_AT_MOST_ONCE, &on_publish, NULL, NULL);

    struct aws_byte_cursor filter_cursor = aws_byte_cursor_from_array(pub_topic, strlen(pub_topic));
    struct aws_mqtt_packet_publish publish;
    aws_mqtt_packet_publish_init(&publish, false, AWS_MQTT_QOS_AT_MOST_ONCE, false, filter_cursor, 1, s_empty_cursor);

    aws_mqtt_topic_tree_publish(&tree, &publish);

    aws_mqtt_topic_tree_clean_up(&tree);

    return was_called;
}

AWS_TEST_CASE(mqtt_topic_tree_match, s_mqtt_topic_tree_match_fn)
static int s_mqtt_topic_tree_match_fn(struct aws_allocator *allocator, void *ctx) {
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

    ASSERT_TRUE(s_check_topic_match(allocator, "///", "///"));
    ASSERT_FALSE(s_check_topic_match(allocator, "///", "//"));

    return AWS_OP_SUCCESS;
}

static struct aws_byte_cursor s_topic_a_a = {
    .ptr = (uint8_t *)"a/a",
    .len = 3,
};
static struct aws_byte_cursor s_topic_a_a_a = {
    .ptr = (uint8_t *)"a/a/a",
    .len = 5,
};
static struct aws_byte_cursor s_topic_a_a_b = {
    .ptr = (uint8_t *)"a/a/b",
    .len = 5,
};

AWS_TEST_CASE(mqtt_topic_tree_unsubscribe, s_mqtt_topic_tree_unsubscribe_fn)
static int s_mqtt_topic_tree_unsubscribe_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_mqtt_topic_tree tree;
    ASSERT_SUCCESS(aws_mqtt_topic_tree_init(&tree, allocator));

    struct aws_string *topic_a_a = aws_string_new_from_array(allocator, s_topic_a_a.ptr, s_topic_a_a.len);
    struct aws_string *topic_a_a_a = aws_string_new_from_array(allocator, s_topic_a_a_a.ptr, s_topic_a_a_a.len);
    struct aws_string *topic_a_a_b = aws_string_new_from_array(allocator, s_topic_a_a_b.ptr, s_topic_a_a_b.len);

    ASSERT_SUCCESS(aws_mqtt_topic_tree_insert(&tree, topic_a_a_a, AWS_MQTT_QOS_AT_MOST_ONCE, &on_publish, NULL, NULL));
    ASSERT_SUCCESS(aws_mqtt_topic_tree_remove(&tree, &s_topic_a_a_a, NULL));
    /* Re-create, it was nuked by remove. */
    topic_a_a_a = aws_string_new_from_array(allocator, s_topic_a_a_a.ptr, s_topic_a_a_a.len);

    /* Ensure that the intermediate 'a' node was removed as well. */
    ASSERT_UINT_EQUALS(0, aws_hash_table_get_entry_count(&tree.root->subtopics));

    /* Put it back so we can test removal of a partial tree. */
    ASSERT_SUCCESS(aws_mqtt_topic_tree_insert(&tree, topic_a_a_a, AWS_MQTT_QOS_AT_MOST_ONCE, &on_publish, NULL, NULL));
    ASSERT_SUCCESS(aws_mqtt_topic_tree_insert(&tree, topic_a_a, AWS_MQTT_QOS_AT_MOST_ONCE, &on_publish, NULL, NULL));
    ASSERT_SUCCESS(aws_mqtt_topic_tree_insert(&tree, topic_a_a_b, AWS_MQTT_QOS_AT_MOST_ONCE, &on_publish, NULL, NULL));

    /* Should remove the last /a, but not the first 2. */
    ASSERT_SUCCESS(aws_mqtt_topic_tree_remove(&tree, &s_topic_a_a_a, NULL));
    ASSERT_SUCCESS(aws_mqtt_topic_tree_remove(&tree, &s_topic_a_a, NULL));

    struct aws_mqtt_packet_publish publish;
    aws_mqtt_packet_publish_init(&publish, false, AWS_MQTT_QOS_AT_MOST_ONCE, false, s_topic_a_a_a, 1, s_topic_a_a_a);

    was_called = false;
    ASSERT_SUCCESS(aws_mqtt_topic_tree_publish(&tree, &publish));
    ASSERT_FALSE(was_called);

    publish.topic_name = s_topic_a_a_b;

    was_called = false;
    ASSERT_SUCCESS(aws_mqtt_topic_tree_publish(&tree, &publish));
    ASSERT_TRUE(was_called);

    aws_mqtt_topic_tree_clean_up(&tree);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt_topic_validation, s_mqtt_topic_validation_fn)
static int s_mqtt_topic_validation_fn(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    (void)ctx;

#define ASSERT_TOPIC_VALIDITY(expected, topic) do {\
        struct aws_byte_cursor topic_cursor; \
        topic_cursor.ptr = (uint8_t *)(topic); \
        topic_cursor.len = strlen(topic); \
        ASSERT_##expected(aws_mqtt_is_valid_topic_filter(&topic_cursor)); \
    } while (false)

    ASSERT_TOPIC_VALIDITY(TRUE, "#");
    ASSERT_TOPIC_VALIDITY(TRUE, "sport/tennis/#");
    ASSERT_TOPIC_VALIDITY(FALSE, "sport/tennis#");
    ASSERT_TOPIC_VALIDITY(FALSE, "sport/tennis/#/ranking");

    ASSERT_TOPIC_VALIDITY(TRUE, "+");
    ASSERT_TOPIC_VALIDITY(TRUE, "+/tennis/#");
    ASSERT_TOPIC_VALIDITY(TRUE, "sport/+/player1");
    ASSERT_TOPIC_VALIDITY(FALSE, "sport+");

    return AWS_OP_SUCCESS;
}
