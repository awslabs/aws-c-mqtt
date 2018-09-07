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

#include <aws/mqtt/private/client_channel_handler.h>

#include <aws/mqtt/mqtt.h>
#include <aws/mqtt/private/packets.h>

#include <aws/io/channel.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/io/socket.h>
#include <aws/io/socket_channel_handler.h>

#include <aws/common/condition_variable.h>
#include <aws/common/mutex.h>
#include <aws/common/string.h>
#include <aws/common/thread.h>

#include <aws/testing/aws_test_harness.h>

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

static struct aws_byte_cursor s_client_id_1 = {
    .ptr = (uint8_t *)"MyClientId1",
    .len = 11,
};
static struct aws_byte_cursor s_client_id_2 = {
    .ptr = (uint8_t *)"MyClientId2",
    .len = 11,
};
AWS_STATIC_STRING_FROM_LITERAL(s_subscribe_topic, "a/b");

static uint8_t s_payload[] = "This s_payload contains data. It is some good ol' fashioned data.";
enum { PAYLOAD_LEN = sizeof(s_payload) };

struct connection_args {
    struct aws_allocator *allocator;

    struct aws_condition_variable *condition_variable;

    struct aws_mqtt_client_connection *connection;

    bool retained_packet_recieved;
};

static void s_on_packet_recieved(
    struct aws_mqtt_client_connection *connection,
    const struct aws_mqtt_subscription *subscription,
    struct aws_byte_cursor payload,
    void *user_data) {

    (void)connection;
    (void)subscription;

    struct aws_byte_cursor expected_payload = {
        .ptr = s_payload,
        .len = PAYLOAD_LEN,
    };
    assert(aws_byte_cursor_eq(&payload, &expected_payload));

    struct connection_args *args = user_data;
    args->retained_packet_recieved = true;
}

static void s_mqtt_on_connack_1(
    struct aws_mqtt_client_connection *connection,
    enum aws_mqtt_connect_return_code return_code,
    bool session_present,
    void *user_data) {

    assert(return_code == AWS_MQTT_CONNECT_ACCEPTED);
    assert(session_present == false);

    struct connection_args *args = user_data;

    aws_mqtt_client_publish(
        connection,
        aws_byte_cursor_from_string(s_subscribe_topic),
        AWS_MQTT_QOS_EXACTLY_ONCE,
        true,
        aws_byte_cursor_from_array(s_payload, PAYLOAD_LEN));

    aws_condition_variable_notify_one(args->condition_variable);
}

static void s_mqtt_on_connack_2(
    struct aws_mqtt_client_connection *connection,
    enum aws_mqtt_connect_return_code return_code,
    bool session_present,
    void *user_data) {

    assert(return_code == AWS_MQTT_CONNECT_ACCEPTED);
    assert(session_present == false);

    struct connection_args *args = user_data;

    struct aws_mqtt_subscription sub;
    sub.topic_filter = aws_byte_cursor_from_array(aws_string_bytes(s_subscribe_topic), s_subscribe_topic->len);
    sub.qos = AWS_MQTT_QOS_EXACTLY_ONCE;
    aws_mqtt_client_subscribe(connection, &sub, &s_on_packet_recieved, user_data);

    aws_condition_variable_notify_one(args->condition_variable);
}

static void s_mqtt_on_disconnect(struct aws_mqtt_client_connection *connection, int error_code, void *user_data) {

    (void)connection;

    assert(error_code == AWS_OP_SUCCESS);

    struct connection_args *args = user_data;

    aws_condition_variable_notify_one(args->condition_variable);
}

int main(int argc, char **argv) {
    (void)argc;
    (void)argv;

    AWS_TEST_ALLOCATOR_INIT(paho_client);

    struct aws_mutex mutex = AWS_MUTEX_INIT;
    struct aws_condition_variable condition_variable = AWS_CONDITION_VARIABLE_INIT;

    struct connection_args args;
    AWS_ZERO_STRUCT(args);
    args.allocator = &paho_client_allocator;
    args.condition_variable = &condition_variable;

    struct aws_event_loop_group el_group;
    ASSERT_SUCCESS(aws_event_loop_group_default_init(&el_group, args.allocator));

    struct aws_socket_endpoint endpoint;
    AWS_ZERO_STRUCT(endpoint);
    sprintf(endpoint.address, "%s", "127.0.0.1");
    sprintf(endpoint.port, "%s", "1883");

    struct aws_socket_options options;
    AWS_ZERO_STRUCT(options);
    options.connect_timeout = 3000;
    options.type = AWS_SOCKET_STREAM;
    options.domain = AWS_SOCKET_IPV4;

    struct aws_client_bootstrap client_bootstrap;
    ASSERT_SUCCESS(aws_client_bootstrap_init(&client_bootstrap, args.allocator, &el_group));

    struct aws_mqtt_client_connection_callbacks callbacks;
    AWS_ZERO_STRUCT(callbacks);
    callbacks.on_connack = &s_mqtt_on_connack_1;
    callbacks.on_disconnect = &s_mqtt_on_disconnect;
    callbacks.user_data = &args;

    struct aws_mqtt_client client = {
        .client_bootstrap = &client_bootstrap,
        .socket_options = &options,
    };

    args.connection =
        aws_mqtt_client_connection_new(args.allocator, &client, callbacks, &endpoint, NULL, s_client_id_1, true, 0);

    aws_mutex_lock(&mutex);
    ASSERT_SUCCESS(aws_condition_variable_wait(&condition_variable, &mutex));
    aws_mutex_unlock(&mutex);

    sleep(3);
    aws_mqtt_client_connection_disconnect(args.connection);

    aws_mutex_lock(&mutex);
    ASSERT_SUCCESS(aws_condition_variable_wait(&condition_variable, &mutex));
    aws_mutex_unlock(&mutex);

    callbacks.on_connack = &s_mqtt_on_connack_2;

    args.connection =
        aws_mqtt_client_connection_new(args.allocator, &client, callbacks, &endpoint, NULL, s_client_id_2, true, 0);

    aws_mutex_lock(&mutex);
    ASSERT_SUCCESS(aws_condition_variable_wait(&condition_variable, &mutex));
    aws_mutex_unlock(&mutex);

    sleep(3);

    ASSERT_TRUE(args.retained_packet_recieved);

    struct aws_byte_cursor topic_filter =
        aws_byte_cursor_from_array(aws_string_bytes(s_subscribe_topic), s_subscribe_topic->len);
    aws_mqtt_client_unsubscribe(args.connection, &topic_filter);

    sleep(3);

    size_t outstanding_reqs = aws_hash_table_get_entry_count(&args.connection->outstanding_requests);
    ASSERT_UINT_EQUALS(0, outstanding_reqs);

    size_t outstanding_subs = aws_hash_table_get_entry_count(&args.connection->subscriptions);
    ASSERT_UINT_EQUALS(0, outstanding_subs);

    sleep(3);
    aws_mqtt_client_connection_disconnect(args.connection);

    aws_mutex_lock(&mutex);
    ASSERT_SUCCESS(aws_condition_variable_wait(&condition_variable, &mutex));
    aws_mutex_unlock(&mutex);

    args.connection = NULL;

    aws_event_loop_group_clean_up(&el_group);

    ASSERT_UINT_EQUALS(paho_client_alloc_impl.freed, paho_client_alloc_impl.allocated);

    return AWS_OP_SUCCESS;
}
