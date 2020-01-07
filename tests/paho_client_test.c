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

#include <aws/mqtt/private/client_impl.h>

#include <aws/mqtt/mqtt.h>
#include <aws/mqtt/private/packets.h>

#include <aws/io/channel.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/io/socket.h>
#include <aws/io/socket_channel_handler.h>

#include <aws/common/condition_variable.h>
#include <aws/common/device_random.h>
#include <aws/common/mutex.h>
#include <aws/common/string.h>
#include <aws/common/thread.h>

#include <aws/testing/aws_test_harness.h>

#include <stdio.h>
#include <stdlib.h>
#ifdef WIN32
#    include <Windows.h>
#    define sleep Sleep
#else
#    include <unistd.h>
#endif

static struct aws_byte_cursor s_client_id = {
    .ptr = (uint8_t *)"MyClientId1",
    .len = 11,
};
AWS_STATIC_STRING_FROM_LITERAL(s_subscribe_topic, "a/b");
AWS_STATIC_STRING_FROM_LITERAL(s_hostname, "localhost");

enum { PAYLOAD_LEN = 20000 };
static uint8_t s_payload[PAYLOAD_LEN];

struct connection_args {
    struct aws_allocator *allocator;

    struct aws_mutex *mutex;
    struct aws_condition_variable *condition_variable;

    struct aws_mqtt_client_connection *connection;

    bool retained_packet_received;
    bool on_any_publish_fired;
};

static void s_on_packet_received(
    struct aws_mqtt_client_connection *connection,
    const struct aws_byte_cursor *topic,
    const struct aws_byte_cursor *payload,
    void *user_data) {

    (void)connection;
    (void)topic;
    (void)payload;

    struct aws_byte_cursor expected_payload = {
        .ptr = s_payload,
        .len = PAYLOAD_LEN,
    };
    (void)expected_payload;
    AWS_FATAL_ASSERT(aws_byte_cursor_eq(payload, &expected_payload));

    struct connection_args *args = user_data;
    args->retained_packet_received = true;

    printf("2 started\n");

    aws_mutex_lock(args->mutex);
    aws_condition_variable_notify_one(args->condition_variable);
    aws_mutex_unlock(args->mutex);
}

static void s_on_any_packet_received(
    struct aws_mqtt_client_connection *connection,
    const struct aws_byte_cursor *topic,
    const struct aws_byte_cursor *payload,
    void *user_data) {

    (void)connection;
    (void)topic;
    (void)payload;

    struct aws_byte_cursor expected_payload = {
        .ptr = s_payload,
        .len = PAYLOAD_LEN,
    };
    (void)expected_payload;
    AWS_FATAL_ASSERT(aws_byte_cursor_eq(payload, &expected_payload));

    struct connection_args *args = user_data;
    args->on_any_publish_fired = true;
}

static void s_mqtt_on_puback(
    struct aws_mqtt_client_connection *connection,
    uint16_t packet_id,
    int error_code,
    void *userdata) {

    (void)connection;
    (void)packet_id;
    (void)error_code;

    AWS_FATAL_ASSERT(error_code == AWS_OP_SUCCESS);

    struct connection_args *args = userdata;

    printf("2 started\n");

    aws_mutex_lock(args->mutex);
    aws_condition_variable_notify_one(args->condition_variable);
    aws_mutex_unlock(args->mutex);
}

static void s_mqtt_on_connection_complete(
    struct aws_mqtt_client_connection *connection,
    int error_code,
    enum aws_mqtt_connect_return_code return_code,
    bool session_present,
    void *user_data) {

    (void)connection;
    (void)error_code;
    (void)return_code;
    (void)session_present;

    AWS_FATAL_ASSERT(error_code == AWS_OP_SUCCESS);
    AWS_FATAL_ASSERT(return_code == AWS_MQTT_CONNECT_ACCEPTED);
    AWS_FATAL_ASSERT(session_present == false);

    struct connection_args *args = user_data;

    printf("1 started\n");

    aws_mutex_lock(args->mutex);
    aws_condition_variable_notify_one(args->condition_variable);
    aws_mutex_unlock(args->mutex);
}

static void s_mqtt_on_interrupted(struct aws_mqtt_client_connection *connection, int error_code, void *userdata) {

    (void)connection;
    (void)error_code;
    (void)userdata;

    printf("Connection offline\n");
}

static void s_mqtt_on_resumed(
    struct aws_mqtt_client_connection *connection,
    enum aws_mqtt_connect_return_code return_code,
    bool session_present,
    void *userdata) {

    (void)connection;
    (void)return_code;
    (void)session_present;
    (void)userdata;

    printf("Connection resumed\n");
}

static void s_mqtt_on_unsuback(
    struct aws_mqtt_client_connection *connection,
    uint16_t packet_id,
    int error_code,
    void *userdata) {

    (void)connection;
    (void)packet_id;
    (void)error_code;

    AWS_FATAL_ASSERT(error_code == AWS_OP_SUCCESS);

    struct connection_args *args = userdata;

    printf("2 started\n");

    aws_mutex_lock(args->mutex);
    aws_condition_variable_notify_one(args->condition_variable);
    aws_mutex_unlock(args->mutex);
}

static void s_mqtt_on_disconnect(struct aws_mqtt_client_connection *connection, void *user_data) {

    (void)connection;

    struct connection_args *args = user_data;

    printf("3 started\n");

    aws_mutex_lock(args->mutex);
    aws_condition_variable_notify_one(args->condition_variable);
    aws_mutex_unlock(args->mutex);
}

int main(int argc, char **argv) {
    (void)argc;
    (void)argv;

    struct aws_allocator *allocator = aws_mem_tracer_new(aws_default_allocator(), NULL, AWS_MEMTRACE_BYTES, 0);

    aws_mqtt_library_init(allocator);

    struct aws_mutex mutex = AWS_MUTEX_INIT;
    struct aws_condition_variable condition_variable = AWS_CONDITION_VARIABLE_INIT;

    /* Populate the payload */
    struct aws_byte_buf payload_buf = aws_byte_buf_from_empty_array(s_payload, PAYLOAD_LEN);
    aws_device_random_buffer(&payload_buf);

    struct aws_byte_cursor subscribe_topic_cur = aws_byte_cursor_from_string(s_subscribe_topic);

    struct connection_args args;
    AWS_ZERO_STRUCT(args);
    args.allocator = allocator;
    args.mutex = &mutex;
    args.condition_variable = &condition_variable;

    struct aws_event_loop_group el_group;
    ASSERT_SUCCESS(aws_event_loop_group_default_init(&el_group, args.allocator, 1));

    struct aws_host_resolver resolver;
    ASSERT_SUCCESS(aws_host_resolver_init_default(&resolver, args.allocator, 8, &el_group));

    struct aws_client_bootstrap_options bootstrap_options = {
        .event_loop_group = &el_group,
        .host_resolver = &resolver,
    };
    struct aws_client_bootstrap *bootstrap = aws_client_bootstrap_new(args.allocator, &bootstrap_options);

    struct aws_socket_options options;
    AWS_ZERO_STRUCT(options);
    options.connect_timeout_ms = 3000;
    options.type = AWS_SOCKET_STREAM;
    options.domain = AWS_SOCKET_IPV4;

    struct aws_mqtt_client client;
    ASSERT_SUCCESS(aws_mqtt_client_init(&client, args.allocator, bootstrap));

    struct aws_byte_cursor host_name_cur = aws_byte_cursor_from_string(s_hostname);
    args.connection = aws_mqtt_client_connection_new(&client);
    ASSERT_NOT_NULL(args.connection);

    aws_mqtt_client_connection_set_connection_interruption_handlers(
        args.connection, s_mqtt_on_interrupted, NULL, s_mqtt_on_resumed, NULL);

    aws_mqtt_client_connection_set_on_any_publish_handler(args.connection, s_on_any_packet_received, &args);

    struct aws_mqtt_connection_options conn_options = {
        .host_name = host_name_cur,
        .port = 1883,
        .socket_options = &options,
        .tls_options = NULL,
        .client_id = s_client_id,
        .keep_alive_time_secs = 0,
        .ping_timeout_ms = 0,
        .on_connection_complete = s_mqtt_on_connection_complete,
        .user_data = &args,
        .clean_session = true,
    };

    ASSERT_SUCCESS(aws_mqtt_client_connection_connect(args.connection, &conn_options));

    /* Wait for connack */
    aws_mutex_lock(&mutex);
    ASSERT_SUCCESS(aws_condition_variable_wait(&condition_variable, &mutex));
    aws_mutex_unlock(&mutex);

    printf("1 done\n");

    struct aws_byte_cursor payload_cur = aws_byte_cursor_from_array(s_payload, PAYLOAD_LEN);
    aws_mqtt_client_connection_publish(
        args.connection, &subscribe_topic_cur, AWS_MQTT_QOS_EXACTLY_ONCE, true, &payload_cur, &s_mqtt_on_puback, &args);

    /* Wait for puback */
    aws_mutex_lock(&mutex);
    ASSERT_SUCCESS(aws_condition_variable_wait(&condition_variable, &mutex));
    aws_mutex_unlock(&mutex);

    printf("2 done\n");

    aws_mqtt_client_connection_disconnect(args.connection, s_mqtt_on_disconnect, &args);

    /* Wait for disconnack */
    aws_mutex_lock(&mutex);
    ASSERT_SUCCESS(aws_condition_variable_wait(&condition_variable, &mutex));
    aws_mutex_unlock(&mutex);

    printf("3 done\n");

    ASSERT_SUCCESS(aws_mqtt_client_connection_reconnect(args.connection, s_mqtt_on_connection_complete, &args));

    /* Wait for connack */
    aws_mutex_lock(&mutex);
    ASSERT_SUCCESS(aws_condition_variable_wait(&condition_variable, &mutex));
    aws_mutex_unlock(&mutex);

    printf("1 done\n");

    /* Subscribe (no on_suback, the on_message received will trigger the cv) */
    aws_mqtt_client_connection_subscribe(
        args.connection,
        &subscribe_topic_cur,
        AWS_MQTT_QOS_EXACTLY_ONCE,
        &s_on_packet_received,
        &args,
        NULL,
        NULL,
        NULL);

    /* Wait for PUBLISH */
    aws_mutex_lock(&mutex);
    ASSERT_SUCCESS(aws_condition_variable_wait(&condition_variable, &mutex));
    aws_mutex_unlock(&mutex);

    printf("2 done\n");

    ASSERT_TRUE(args.retained_packet_received);
    ASSERT_TRUE(args.on_any_publish_fired);

    struct aws_byte_cursor topic_filter =
        aws_byte_cursor_from_array(aws_string_bytes(s_subscribe_topic), s_subscribe_topic->len);
    aws_mqtt_client_connection_unsubscribe(args.connection, &topic_filter, &s_mqtt_on_unsuback, &args);

    /* Wait for UNSUBACK */
    aws_mutex_lock(&mutex);
    ASSERT_SUCCESS(aws_condition_variable_wait(&condition_variable, &mutex));
    aws_mutex_unlock(&mutex);

    printf("3 done\n");

    sleep(4);

    size_t outstanding_reqs = aws_hash_table_get_entry_count(&args.connection->outstanding_requests.table);
    ASSERT_UINT_EQUALS(0, outstanding_reqs);

    size_t outstanding_subs = aws_hash_table_get_entry_count(&args.connection->subscriptions.root->subtopics);
    ASSERT_UINT_EQUALS(0, outstanding_subs);

    aws_mqtt_client_connection_disconnect(args.connection, s_mqtt_on_disconnect, &args);

    aws_mutex_lock(&mutex);
    ASSERT_SUCCESS(aws_condition_variable_wait(&condition_variable, &mutex));
    aws_mutex_unlock(&mutex);

    aws_mqtt_client_connection_destroy(args.connection);
    args.connection = NULL;

    aws_client_bootstrap_release(bootstrap);
    aws_host_resolver_clean_up(&resolver);
    aws_event_loop_group_clean_up(&el_group);

    ASSERT_UINT_EQUALS(0, aws_mem_tracer_count(allocator));
    allocator = aws_mem_tracer_destroy(allocator);
    ASSERT_NOT_NULL(allocator);

    return AWS_OP_SUCCESS;
}
