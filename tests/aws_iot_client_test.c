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
#include <aws/io/tls_channel_handler.h>

#include <aws/common/condition_variable.h>
#include <aws/common/mutex.h>
#include <aws/common/string.h>
#include <aws/common/thread.h>

#include <aws/testing/aws_test_harness.h>

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

AWS_STATIC_STRING_FROM_LITERAL(s_client_id, "aws_iot_client_test");
AWS_STATIC_STRING_FROM_LITERAL(s_subscribe_topic, "a/b");

static uint8_t s_payload[] = "This s_payload contains data. It is some good ol' fashioned data.";
enum { PAYLOAD_LEN = sizeof(s_payload) };

struct connection_args {
    struct aws_allocator *allocator;

    struct aws_mutex *mutex;
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
    printf("Message recieved: ");
    fwrite(payload.ptr, 1, payload.len, stdout);
    printf("\n");

    struct connection_args *args = user_data;
    args->retained_packet_recieved = true;
}

static void s_mqtt_publish_complete(struct aws_mqtt_client_connection *connection, void *userdata) {

    (void)connection;

    aws_string_destroy(userdata);
}

static void s_mqtt_on_connack(
    struct aws_mqtt_client_connection *connection,
    enum aws_mqtt_connect_return_code return_code,
    bool session_present,
    void *user_data) {

    (void)connection;

    assert(return_code == AWS_MQTT_CONNECT_ACCEPTED);
    assert(session_present == false);

    struct connection_args *args = user_data;

    aws_mutex_lock(args->mutex);
    aws_condition_variable_notify_one(args->condition_variable);
    aws_mutex_unlock(args->mutex);
}

static void s_mqtt_on_disconnect(struct aws_mqtt_client_connection *connection, int error_code, void *user_data) {

    (void)connection;

    if (error_code != AWS_OP_SUCCESS) {
        printf("error: %s\n", aws_error_debug_str(error_code));
        abort();
    }

    struct connection_args *args = user_data;

    aws_mutex_lock(args->mutex);
    aws_condition_variable_notify_one(args->condition_variable);
    aws_mutex_unlock(args->mutex);
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

    struct aws_event_loop_group el_group;
    ASSERT_SUCCESS(aws_event_loop_group_default_init(&el_group, args.allocator));

    struct aws_tls_ctx_options tls_ctx_opt;
    aws_tls_ctx_options_init_client_mtls(&tls_ctx_opt, "iot.cert.pem", "iot.private.key");
    aws_tls_ctx_options_set_alpn_list(&tls_ctx_opt, "x-amzn-mqtt-ca");

    struct aws_tls_connection_options tls_conn_opt;
    aws_tls_connection_options_init_from_ctx_options(&tls_conn_opt, &tls_ctx_opt);

    aws_tls_connection_options_set_server_name(&tls_conn_opt, "a1ba5f1mpna9k5.iot.us-east-2.amazonaws.com");

    struct aws_tls_ctx *tls_ctx = aws_tls_client_ctx_new(args.allocator, &tls_ctx_opt);

    struct aws_socket_endpoint endpoint;
    AWS_ZERO_STRUCT(endpoint);
    sprintf(endpoint.address, "%s", "18.191.24.253");
    sprintf(endpoint.port, "%s", "443");

    struct aws_socket_options options;
    AWS_ZERO_STRUCT(options);
    options.connect_timeout = 3000;
    options.type = AWS_SOCKET_STREAM;
    options.domain = AWS_SOCKET_IPV4;

    struct aws_client_bootstrap client_bootstrap;
    ASSERT_SUCCESS(aws_client_bootstrap_init(&client_bootstrap, args.allocator, &el_group));
    aws_client_bootstrap_set_tls_ctx(&client_bootstrap, tls_ctx);

    struct aws_mqtt_client_connection_callbacks callbacks;
    AWS_ZERO_STRUCT(callbacks);
    callbacks.on_connack = &s_mqtt_on_connack;
    callbacks.on_disconnect = &s_mqtt_on_disconnect;
    callbacks.user_data = &args;

    struct aws_mqtt_client client = {
        .client_bootstrap = &client_bootstrap,
        .socket_options = &options,
    };

    args.connection = aws_mqtt_client_connection_new(
        args.allocator,
        &client,
        callbacks,
        &endpoint,
        &tls_conn_opt,
        aws_byte_cursor_from_string(s_client_id),
        true,
        0);

    aws_mutex_lock(&mutex);
    ASSERT_SUCCESS(aws_condition_variable_wait(&condition_variable, &mutex));
    aws_mutex_unlock(&mutex);

    struct aws_mqtt_subscription sub;
    sub.topic_filter = aws_byte_cursor_from_array(aws_string_bytes(s_subscribe_topic), s_subscribe_topic->len);
    sub.qos = AWS_MQTT_QOS_AT_LEAST_ONCE;
    aws_mqtt_client_subscribe(args.connection, &sub, &s_on_packet_recieved, &args);

    const struct aws_string *payload = aws_string_new_from_array(args.allocator, s_payload, PAYLOAD_LEN);

    aws_mqtt_client_publish(
        args.connection,
        aws_byte_cursor_from_string(s_subscribe_topic),
        AWS_MQTT_QOS_AT_MOST_ONCE,
        false,
        aws_byte_cursor_from_string(payload),
        &s_mqtt_publish_complete,
        (void *)payload);

    sleep(10);

    aws_mqtt_client_connection_disconnect(args.connection);

    aws_mutex_lock(&mutex);
    ASSERT_SUCCESS(aws_condition_variable_wait(&condition_variable, &mutex));
    aws_mutex_unlock(&mutex);

    args.connection = NULL;

    aws_event_loop_group_clean_up(&el_group);

    aws_tls_ctx_destroy(tls_ctx);

    aws_tls_clean_up_static_state(args.allocator);

    ASSERT_UINT_EQUALS(paho_client_alloc_impl.freed, paho_client_alloc_impl.allocated);

    return AWS_OP_SUCCESS;
}
