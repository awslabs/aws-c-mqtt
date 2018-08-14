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

static struct aws_byte_cursor s_client_id = {
    .ptr = (uint8_t *)"MyClientId",
    .len = 10,
};

struct connection_args {
    struct aws_allocator *allocator;

    struct aws_condition_variable *condition_variable;

    struct aws_mqtt_client_connection *client;
};

static void s_mqtt_on_connection(struct aws_mqtt_client_connection *connection, enum aws_mqtt_connect_return_code return_code, bool session_present, void *user_data) {

    assert(return_code == AWS_MQTT_CONNECT_ACCEPTED);
    assert(session_present == false);
    (void)user_data;

    sleep(3);

    aws_mqtt_client_connection_disconnect(connection);
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
    callbacks.on_connect = &s_mqtt_on_connection;
    callbacks.on_disconnect = &s_mqtt_on_disconnect;
    callbacks.user_data = &args;

    args.client =
        aws_mqtt_client_connection_new(args.allocator, callbacks, &client_bootstrap, &endpoint, &options, s_client_id, true, 0);

    aws_mutex_lock(&mutex);
    ASSERT_SUCCESS(aws_condition_variable_wait(&condition_variable, &mutex));
    aws_mutex_unlock(&mutex);

    args.client =
        aws_mqtt_client_connection_new(args.allocator, callbacks, &client_bootstrap, &endpoint, &options, s_client_id, true, 0);

    aws_mutex_lock(&mutex);
    ASSERT_SUCCESS(aws_condition_variable_wait(&condition_variable, &mutex));
    aws_mutex_unlock(&mutex);

    args.client = NULL;

    aws_event_loop_group_clean_up(&el_group);

    ASSERT_UINT_EQUALS(paho_client_alloc_impl.freed, paho_client_alloc_impl.allocated);

    return AWS_OP_SUCCESS;
}
