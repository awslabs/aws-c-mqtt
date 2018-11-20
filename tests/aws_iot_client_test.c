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
#include <aws/io/tls_channel_handler.h>

#include <aws/common/condition_variable.h>
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

AWS_STATIC_STRING_FROM_LITERAL(s_client_id, "aws_iot_client_test");
AWS_STATIC_STRING_FROM_LITERAL(s_subscribe_topic, "a/b");
AWS_STATIC_STRING_FROM_LITERAL(s_hostname, "a1ba5f1mpna9k5-ats.iot.us-east-1.amazonaws.com");

static uint8_t s_payload[] = "This s_payload contains data. It is some good ol' fashioned data.";
enum { PAYLOAD_LEN = sizeof(s_payload) };

static uint8_t s_will_payload[] = "The client has gone offline!";
enum { WILL_PAYLOAD_LEN = sizeof(s_will_payload) };

struct connection_args {
    struct aws_allocator *allocator;

    struct aws_mutex *mutex;
    struct aws_condition_variable *condition_variable;

    struct aws_mqtt_client_connection *connection;

    bool retained_packet_recieved;
};

static void s_on_packet_recieved(
    struct aws_mqtt_client_connection *connection,
    const struct aws_byte_cursor *topic,
    const struct aws_byte_cursor *payload,
    void *user_data) {

    (void)connection;
    (void)topic;
    printf("Message recieved: ");
    fwrite(payload->ptr, 1, payload->len, stdout);
    printf("\n");

    struct connection_args *args = user_data;
    args->retained_packet_recieved = true;
}

static void s_mqtt_publish_complete(struct aws_mqtt_client_connection *connection, uint16_t packet_id, void *userdata) {

    (void)connection;
    (void)packet_id;

    aws_string_destroy(userdata);
}

static void s_mqtt_on_connack(
    struct aws_mqtt_client_connection *connection,
    enum aws_mqtt_connect_return_code return_code,
    bool session_present,
    void *user_data) {

    (void)connection;
    (void)return_code;
    (void)session_present;

    assert(return_code == AWS_MQTT_CONNECT_ACCEPTED);
    assert(session_present == false);

    struct connection_args *args = user_data;

    struct aws_byte_cursor subscribe_topic_cur = aws_byte_cursor_from_string(s_subscribe_topic);

    aws_mqtt_client_connection_subscribe(
        args->connection,
        &subscribe_topic_cur,
        AWS_MQTT_QOS_AT_LEAST_ONCE,
        &s_on_packet_recieved,
        args,
        NULL,
        NULL,
        NULL);

    aws_mutex_lock(args->mutex);
    aws_condition_variable_notify_one(args->condition_variable);
    aws_mutex_unlock(args->mutex);
}

static void s_mqtt_on_connection_failed(
    struct aws_mqtt_client_connection *connection,
    int error_code,
    void *user_data) {

    (void)connection;
    (void)user_data;

    fprintf(stderr, "Error recieved: %s\n", aws_error_debug_str(error_code));
}

static bool s_mqtt_on_disconnect(struct aws_mqtt_client_connection *connection, int error_code, void *user_data) {

    (void)connection;

    if (error_code != AWS_OP_SUCCESS) {
        fprintf(stderr, "Disconnected, error: %s\n", aws_error_debug_str(error_code));
        return false;
    }

    struct connection_args *args = user_data;

    aws_mqtt_client_connection_destroy(args->connection);
    args->connection = NULL;

    aws_mutex_lock(args->mutex);
    aws_condition_variable_notify_one(args->condition_variable);
    aws_mutex_unlock(args->mutex);

    return false;
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
    args.mutex = &mutex;
    args.condition_variable = &condition_variable;

    aws_tls_init_static_state(args.allocator);
    aws_load_error_strings();
    aws_io_load_error_strings();
    aws_mqtt_load_error_strings();

    struct aws_byte_cursor subscribe_topic_cur = aws_byte_cursor_from_string(s_subscribe_topic);

    struct aws_event_loop_group elg;
    aws_event_loop_group_default_init(&elg, args.allocator, 1);

    struct aws_client_bootstrap bootstrap;
    aws_client_bootstrap_init(&bootstrap, args.allocator, &elg, NULL, NULL);

    struct aws_tls_ctx_options tls_ctx_opt;
    aws_tls_ctx_options_init_client_mtls(&tls_ctx_opt, "9f0631f03a-certificate.pem.crt", "9f0631f03a-private.pem.key");
    aws_tls_ctx_options_set_alpn_list(&tls_ctx_opt, "x-amzn-mqtt-ca");
    aws_tls_ctx_options_override_default_trust_store(&tls_ctx_opt, NULL, "AmazonRootCA1.pem");

    struct aws_tls_ctx *tls_ctx = aws_tls_client_ctx_new(args.allocator, &tls_ctx_opt);
    ASSERT_NOT_NULL(tls_ctx);

    struct aws_tls_connection_options tls_con_opt;
    aws_tls_connection_options_init_from_ctx(&tls_con_opt, tls_ctx);

    struct aws_socket_options socket_options;
    AWS_ZERO_STRUCT(socket_options);
    socket_options.connect_timeout_ms = 500;
    socket_options.type = AWS_SOCKET_STREAM;
    socket_options.domain = AWS_SOCKET_IPV6;

    struct aws_mqtt_client_connection_callbacks callbacks;
    AWS_ZERO_STRUCT(callbacks);
    callbacks.on_connack = &s_mqtt_on_connack;
    callbacks.on_connection_failed = &s_mqtt_on_connection_failed;
    callbacks.on_disconnect = &s_mqtt_on_disconnect;
    callbacks.user_data = &args;

    struct aws_mqtt_client client;
    aws_mqtt_client_init(&client, args.allocator, &bootstrap);

    struct aws_byte_cursor host_name_cur = aws_byte_cursor_from_string(s_hostname);
    args.connection =
        aws_mqtt_client_connection_new(&client, callbacks, &host_name_cur, 8883, &socket_options, &tls_con_opt);

    struct aws_byte_cursor will_cur = aws_byte_cursor_from_array(s_will_payload, WILL_PAYLOAD_LEN);
    aws_mqtt_client_connection_set_will(args.connection, &subscribe_topic_cur, 1, false, &will_cur);

    struct aws_byte_cursor client_id_cur = aws_byte_cursor_from_string(s_client_id);
    aws_mqtt_client_connection_connect(args.connection, &client_id_cur, true, 0);

    aws_mutex_lock(&mutex);
    ASSERT_SUCCESS(aws_condition_variable_wait(&condition_variable, &mutex));
    aws_mutex_unlock(&mutex);

    struct aws_string *payload = aws_string_new_from_array(args.allocator, s_payload, PAYLOAD_LEN);
    struct aws_byte_cursor payload_cur = aws_byte_cursor_from_string(payload);

    for (int i = 0; i < 10; ++i) {
        aws_mqtt_client_connection_publish(
            args.connection,
            &subscribe_topic_cur,
            AWS_MQTT_QOS_AT_LEAST_ONCE,
            false,
            &payload_cur,
            NULL, // &s_mqtt_publish_complete,
            (void *)payload);

        sleep(1);
    }

    s_mqtt_publish_complete(args.connection, 0, payload);

    aws_mqtt_client_connection_disconnect(args.connection);

    aws_mutex_lock(&mutex);
    ASSERT_SUCCESS(aws_condition_variable_wait(&condition_variable, &mutex));
    aws_mutex_unlock(&mutex);

    aws_mqtt_client_clean_up(&client);

    aws_client_bootstrap_clean_up(&bootstrap);

    aws_event_loop_group_clean_up(&elg);

    aws_tls_ctx_destroy(tls_ctx);

    aws_tls_clean_up_static_state();

    ASSERT_UINT_EQUALS(paho_client_alloc_impl.freed, paho_client_alloc_impl.allocated);

    return AWS_OP_SUCCESS;
}
