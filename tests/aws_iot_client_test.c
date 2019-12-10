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
#include <aws/io/logging.h>
#include <aws/io/socket.h>
#include <aws/io/socket_channel_handler.h>
#include <aws/io/tls_channel_handler.h>

#include <aws/common/condition_variable.h>
#include <aws/common/device_random.h>
#include <aws/common/mutex.h>
#include <aws/common/string.h>
#include <aws/common/thread.h>
#include <aws/common/uuid.h>

#include <aws/testing/aws_test_harness.h>

#include <stdio.h>
#include <stdlib.h>
#ifdef WIN32
#    include <Windows.h>
#    define sleep Sleep
#else
#    include <unistd.h>
#endif

const char s_client_id_prefix[] = "sdk-c-v2-";
AWS_STATIC_STRING_FROM_LITERAL(s_subscribe_topic, "sdk/test/c");

enum { PUBLISHES = 20 };

enum { PAYLOAD_LEN = 200 };
static uint8_t s_payload[PAYLOAD_LEN];

static uint8_t s_will_payload[] = "The client has gone offline!";
enum { WILL_PAYLOAD_LEN = sizeof(s_will_payload) };

struct connection_args {
    struct aws_allocator *allocator;

    struct aws_mutex *mutex;
    struct aws_condition_variable *condition_variable;

    struct aws_mqtt_client_connection *connection;

    size_t pubacks_gotten;
    size_t packets_gotten;
};

static void s_on_puback(
    struct aws_mqtt_client_connection *connection,
    uint16_t packet_id,
    int error_code,
    void *userdata) {

    (void)connection;
    (void)packet_id;
    (void)error_code;

    AWS_FATAL_ASSERT(error_code == AWS_ERROR_SUCCESS);

    struct connection_args *args = userdata;
    ++args->pubacks_gotten;
}

static void s_on_packet_received(
    struct aws_mqtt_client_connection *connection,
    const struct aws_byte_cursor *topic,
    const struct aws_byte_cursor *payload,
    void *userdata) {

    (void)connection;
    (void)topic;
    (void)payload;

    AWS_FATAL_ASSERT(payload->len == PAYLOAD_LEN);
    AWS_FATAL_ASSERT(0 == memcmp(payload->ptr, s_payload, PAYLOAD_LEN));

    struct connection_args *args = userdata;
    ++args->packets_gotten;

    if (args->packets_gotten == PUBLISHES) {
        aws_mutex_lock(args->mutex);
        aws_condition_variable_notify_one(args->condition_variable);
        aws_mutex_unlock(args->mutex);
    }
}

static bool s_all_packets_received_cond(void *userdata) {

    struct connection_args *args = userdata;
    return args->packets_gotten == PUBLISHES;
}

static void s_mqtt_on_connection_complete(
    struct aws_mqtt_client_connection *connection,
    int error_code,
    enum aws_mqtt_connect_return_code return_code,
    bool session_present,
    void *userdata) {

    (void)connection;
    (void)error_code;
    (void)return_code;
    (void)session_present;

    AWS_FATAL_ASSERT(error_code == AWS_ERROR_SUCCESS);
    AWS_FATAL_ASSERT(return_code == AWS_MQTT_CONNECT_ACCEPTED);
    AWS_FATAL_ASSERT(session_present == false);

    struct connection_args *args = userdata;

    aws_mutex_lock(args->mutex);
    aws_condition_variable_notify_one(args->condition_variable);
    aws_mutex_unlock(args->mutex);
}

static void s_mqtt_on_suback(
    struct aws_mqtt_client_connection *connection,
    uint16_t packet_id,
    const struct aws_byte_cursor *topic,
    enum aws_mqtt_qos qos,
    int error_code,
    void *userdata) {

    (void)connection;
    (void)packet_id;
    (void)topic;
    (void)qos;
    (void)error_code;

    AWS_FATAL_ASSERT(error_code == AWS_ERROR_SUCCESS);
    AWS_FATAL_ASSERT(qos != AWS_MQTT_QOS_FAILURE);

    struct connection_args *args = userdata;

    aws_mutex_lock(args->mutex);
    aws_condition_variable_notify_one(args->condition_variable);
    aws_mutex_unlock(args->mutex);
}

static void s_on_connection_interrupted(struct aws_mqtt_client_connection *connection, int error_code, void *userdata) {

    (void)connection;
    (void)userdata;
    printf("CONNECTION INTERRUPTED error_code=%d\n", error_code);
}

static void s_on_resubscribed(
    struct aws_mqtt_client_connection *connection,
    uint16_t packet_id,
    const struct aws_array_list *topic_subacks, /* contains aws_mqtt_topic_subscription pointers */
    int error_code,
    void *userdata) {

    (void)connection;
    (void)packet_id;
    (void)userdata;

    AWS_FATAL_ASSERT(error_code == AWS_ERROR_SUCCESS);

    size_t num_topics = aws_array_list_length(topic_subacks);
    printf("RESUBSCRIBE_COMPLETE. error_code=%d num_topics=%zu\n", error_code, num_topics);
    for (size_t i = 0; i < num_topics; ++i) {
        struct aws_mqtt_topic_subscription sub_i;
        aws_array_list_get_at(topic_subacks, &sub_i, i);
        printf("  topic=" PRInSTR " qos=%d\n", AWS_BYTE_CURSOR_PRI(sub_i.topic), sub_i.qos);
        AWS_FATAL_ASSERT(sub_i.qos != AWS_MQTT_QOS_FAILURE);
    }
}

static void s_on_connection_resumed(
    struct aws_mqtt_client_connection *connection,
    enum aws_mqtt_connect_return_code return_code,
    bool session_present,
    void *userdata) {

    (void)connection;
    (void)userdata;

    printf("CONNECTION RESUMED return_code=%d session_present=%d\n", return_code, session_present);
    if (!session_present) {
        printf("RESUBSCRIBING...");
        uint16_t packet_id = aws_mqtt_resubscribe_existing_topics(connection, s_on_resubscribed, NULL);
        if (!packet_id) {
            AWS_FATAL_ASSERT(aws_last_error() == AWS_ERROR_MQTT_NO_TOPICS_FOR_RESUBSCRIBE);
            printf("No topics to resubscribe to.");
        }
    }
}

static void s_mqtt_on_disconnect(struct aws_mqtt_client_connection *connection, void *userdata) {

    (void)connection;

    struct connection_args *args = userdata;

    aws_mqtt_client_connection_destroy(args->connection);
    args->connection = NULL;

    aws_mutex_lock(args->mutex);
    aws_condition_variable_notify_one(args->condition_variable);
    aws_mutex_unlock(args->mutex);
}

int main(int argc, char **argv) {

    if (argc < 5) {
        printf(
            "4 args required, only %d passed. Usage:\n"
            "aws-c-mqtt-iot-client [endpoint] [certificate] [private_key] [root_ca]\n",
            argc - 1);
        return 1;
    }

    const char *endpoint = argv[1];
    const char *cert = argv[2];
    const char *private_key = argv[3];
    const char *root_ca = argv[4];

    struct aws_allocator *allocator = aws_mem_tracer_new(aws_default_allocator(), NULL, AWS_MEMTRACE_BYTES, 0);

    struct aws_mutex mutex = AWS_MUTEX_INIT;
    struct aws_condition_variable condition_variable = AWS_CONDITION_VARIABLE_INIT;

    struct connection_args args;
    AWS_ZERO_STRUCT(args);
    args.allocator = allocator;
    args.mutex = &mutex;
    args.condition_variable = &condition_variable;

    aws_mqtt_library_init(args.allocator);

    struct aws_logger_standard_options logger_options = {
        .level = AWS_LL_TRACE,
        .file = stdout,
    };

    struct aws_logger logger;
    aws_logger_init_standard(&logger, args.allocator, &logger_options);
    aws_logger_set(&logger);

    /* Populate the payload */
    struct aws_byte_buf payload_buf = aws_byte_buf_from_empty_array(s_payload, PAYLOAD_LEN);
    aws_device_random_buffer(&payload_buf);

    struct aws_byte_cursor subscribe_topic_cur = aws_byte_cursor_from_string(s_subscribe_topic);

    struct aws_event_loop_group elg;
    aws_event_loop_group_default_init(&elg, args.allocator, 1);

    struct aws_host_resolver resolver;
    ASSERT_SUCCESS(aws_host_resolver_init_default(&resolver, args.allocator, 8, &elg));

    struct aws_client_bootstrap_options bootstrap_options = {
        .event_loop_group = &elg,
        .host_resolver = &resolver,
    };
    struct aws_client_bootstrap *bootstrap = aws_client_bootstrap_new(args.allocator, &bootstrap_options);

    struct aws_tls_ctx_options tls_ctx_opt;
    ASSERT_SUCCESS(aws_tls_ctx_options_init_client_mtls_from_path(&tls_ctx_opt, args.allocator, cert, private_key));
    ASSERT_SUCCESS(aws_tls_ctx_options_set_alpn_list(&tls_ctx_opt, "x-amzn-mqtt-ca"));
    ASSERT_SUCCESS(aws_tls_ctx_options_override_default_trust_store_from_path(&tls_ctx_opt, NULL, root_ca));

    struct aws_tls_ctx *tls_ctx = aws_tls_client_ctx_new(args.allocator, &tls_ctx_opt);
    ASSERT_NOT_NULL(tls_ctx);

    aws_tls_ctx_options_clean_up(&tls_ctx_opt);

    struct aws_tls_connection_options tls_con_opt;
    aws_tls_connection_options_init_from_ctx(&tls_con_opt, tls_ctx);

    struct aws_socket_options socket_options;
    AWS_ZERO_STRUCT(socket_options);
    socket_options.connect_timeout_ms = 500;
    socket_options.type = AWS_SOCKET_STREAM;
    socket_options.domain = AWS_SOCKET_IPV6;

    struct aws_mqtt_client client;
    aws_mqtt_client_init(&client, args.allocator, bootstrap);

    struct aws_byte_cursor host_name_cur = aws_byte_cursor_from_c_str(endpoint);
    args.connection = aws_mqtt_client_connection_new(&client);

    ASSERT_SUCCESS(aws_mqtt_client_connection_set_connection_interruption_handlers(
        args.connection, s_on_connection_interrupted, NULL, s_on_connection_resumed, NULL));

    struct aws_byte_cursor will_cur = aws_byte_cursor_from_array(s_will_payload, WILL_PAYLOAD_LEN);
    aws_mqtt_client_connection_set_will(args.connection, &subscribe_topic_cur, 1, false, &will_cur);

    /* Generate a random clientid */
    char client_id[128];
    struct aws_byte_buf client_id_buf = aws_byte_buf_from_empty_array(client_id, AWS_ARRAY_SIZE(client_id));

    aws_byte_buf_write(&client_id_buf, (const uint8_t *)s_client_id_prefix, AWS_ARRAY_SIZE(s_client_id_prefix));

    struct aws_uuid uuid;
    aws_uuid_init(&uuid);
    aws_uuid_to_str(&uuid, &client_id_buf);

    struct aws_byte_cursor client_id_cur = aws_byte_cursor_from_buf(&client_id_buf);

    struct aws_mqtt_connection_options conn_options = {
        .host_name = host_name_cur,
        .port = 8883,
        .socket_options = &socket_options,
        .tls_options = &tls_con_opt,
        .client_id = client_id_cur,
        .keep_alive_time_secs = 0,
        .ping_timeout_ms = 0,
        .on_connection_complete = s_mqtt_on_connection_complete,
        .user_data = &args,
        .clean_session = true,
    };
    aws_mqtt_client_connection_connect(args.connection, &conn_options);

    aws_tls_connection_options_clean_up(&tls_con_opt);

    aws_mutex_lock(&mutex);
    ASSERT_SUCCESS(aws_condition_variable_wait(&condition_variable, &mutex));
    aws_mutex_unlock(&mutex);

    aws_mqtt_client_connection_subscribe(
        args.connection,
        &subscribe_topic_cur,
        AWS_MQTT_QOS_AT_LEAST_ONCE,
        &s_on_packet_received,
        &args,
        NULL,
        s_mqtt_on_suback,
        &args);

    aws_mutex_lock(&mutex);
    ASSERT_SUCCESS(aws_condition_variable_wait(&condition_variable, &mutex));
    aws_mutex_unlock(&mutex);

    struct aws_byte_cursor payload_cur = aws_byte_cursor_from_buf(&payload_buf);

    for (int i = 0; i < PUBLISHES; ++i) {
        aws_mqtt_client_connection_publish(
            args.connection,
            &subscribe_topic_cur,
            AWS_MQTT_QOS_AT_LEAST_ONCE,
            false,
            &payload_cur,
            &s_on_puback,
            &args);

        /* Keep the service endpoint from throttling the connection */
        if (i != 0 && i % 100 == 0) {
            sleep(1);
        }
    }

    aws_mutex_lock(&mutex);
    ASSERT_SUCCESS(aws_condition_variable_wait_pred(&condition_variable, &mutex, &s_all_packets_received_cond, &args));
    aws_mutex_unlock(&mutex);

    ASSERT_UINT_EQUALS(PUBLISHES, args.packets_gotten);

    aws_mqtt_client_connection_disconnect(args.connection, s_mqtt_on_disconnect, &args);

    aws_mutex_lock(&mutex);
    ASSERT_SUCCESS(aws_condition_variable_wait(&condition_variable, &mutex));
    aws_mutex_unlock(&mutex);

    aws_mqtt_client_clean_up(&client);

    aws_client_bootstrap_release(bootstrap);

    aws_host_resolver_clean_up(&resolver);
    aws_event_loop_group_clean_up(&elg);

    aws_tls_ctx_destroy(tls_ctx);

    aws_logger_clean_up(&logger);

    aws_mqtt_library_clean_up();

    ASSERT_UINT_EQUALS(0, aws_mem_tracer_count(allocator));
    allocator = aws_mem_tracer_destroy(allocator);
    ASSERT_NOT_NULL(allocator);

    return AWS_OP_SUCCESS;
}
