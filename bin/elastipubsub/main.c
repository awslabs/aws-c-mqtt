/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/clock.h>
#include <aws/common/command_line_parser.h>
#include <aws/common/condition_variable.h>
#include <aws/common/hash_table.h>
#include <aws/common/log_channel.h>
#include <aws/common/log_formatter.h>
#include <aws/common/log_writer.h>
#include <aws/common/mutex.h>
#include <aws/common/string.h>

#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/io/logging.h>
#include <aws/io/socket.h>
#include <aws/io/stream.h>
#include <aws/io/tls_channel_handler.h>
#include <aws/io/uri.h>

#include <aws/mqtt/client.h>
#include <aws/mqtt/mqtt.h>

#include <inttypes.h>

#ifdef _MSC_VER
#    pragma warning(disable : 4996) /* Disable warnings about fopen() being insecure */
#    pragma warning(disable : 4204) /* Declared initializers */
#    pragma warning(disable : 4221) /* Local var in declared initializer */
#endif

struct app_ctx {
    struct aws_allocator *allocator;
    struct aws_mutex lock;
    struct aws_condition_variable signal;
    struct aws_uri uri;
    uint16_t port;
    const char *cacert;
    const char *cert;
    const char *key;
    int connect_timeout;
    int iterations;
    int connection_count;
    int message_count;
    int target_cops;
    int target_pops;
    int pending_publish_completions;
    int publish_successes;
    int publish_failures;
    int received_messages;

    struct aws_tls_connection_options tls_connection_options;

    struct aws_linked_list pending_connection_list;
    struct aws_linked_list established_connection_list;

    const char *log_filename;
    enum aws_log_level log_level;
};

static void s_usage(int exit_code) {

    fprintf(stderr, "usage: elastipubsub [options] endpoint\n");
    fprintf(stderr, " endpoint: url to connect to\n");
    fprintf(stderr, "\n Options:\n\n");
    fprintf(stderr, "      --cacert FILE: path to a CA certficate file.\n");
    fprintf(stderr, "      --cert FILE: path to a PEM encoded certificate to use with mTLS\n");
    fprintf(stderr, "      --key FILE: Path to a PEM encoded private key that matches cert.\n");
    fprintf(stderr, "      --cops INT: target control (connect, subscribe) operations per second\n");
    fprintf(stderr, "      --connect-timeout INT: time in milliseconds to wait for a connection.\n");
    fprintf(stderr, "  -i, --iterations INT: number of independent iterations to run the test for\n");
    fprintf(stderr, "  -k, --connections INT: number of independent connections to make.\n");
    fprintf(stderr, "  -l, --log FILE: dumps logs to FILE instead of stderr.\n");
    fprintf(stderr, "  -n, --messages INT: number of messages to publish per iteration\n");
    fprintf(stderr, "  -p, --pops INT: target publish operations per second\n");
    fprintf(stderr, "  -v, --verbose: ERROR|INFO|DEBUG|TRACE: log level to configure. Default is none.\n");
    fprintf(stderr, "  -h, --help\n");
    fprintf(stderr, "            Display this message and quit.\n");
    exit(exit_code);
}

static struct aws_cli_option s_long_options[] = {
    {"cacert", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'a'},
    {"cert", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'c'},
    {"cops", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'C'},
    {"key", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'e'},
    {"connect-timeout", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'f'},
    {"iterations", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'i'},
    {"connections", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'k'},
    {"log", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'l'},
    {"messages", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'n'},
    {"pops", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'p'},
    {"verbose", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'v'},
    {"help", AWS_CLI_OPTIONS_NO_ARGUMENT, NULL, 'h'},
    /* Per getopt(3) the last element of the array has to be filled with all zeros */
    {NULL, AWS_CLI_OPTIONS_NO_ARGUMENT, NULL, 0},
};

static void s_parse_options(int argc, char **argv, struct app_ctx *ctx) {
    while (true) {
        int option_index = 0;
        int c = aws_cli_getopt_long(argc, argv, "a:c:C:e:f:i:k:l:n:p:v:h", s_long_options, &option_index);
        if (c == -1) {
            break;
        }

        switch (c) {
            case 0:
                /* getopt_long() returns 0 if an option.flag is non-null */
                break;
            case 'a':
                ctx->cacert = aws_cli_optarg;
                break;
            case 'c':
                ctx->cert = aws_cli_optarg;
                break;
            case 'C':
                ctx->target_cops = atoi(aws_cli_optarg);
                break;
            case 'e':
                ctx->key = aws_cli_optarg;
                break;
            case 'f':
                ctx->connect_timeout = atoi(aws_cli_optarg);
                break;
            case 'i':
                ctx->iterations = atoi(aws_cli_optarg);
                break;
            case 'k':
                ctx->connection_count = atoi(aws_cli_optarg);
                break;
            case 'l':
                ctx->log_filename = aws_cli_optarg;
                break;
            case 'n':
                ctx->message_count = atoi(aws_cli_optarg);
                break;
            case 'p':
                ctx->target_pops = atoi(aws_cli_optarg);
                break;
            case 'v':
                if (!strcmp(aws_cli_optarg, "TRACE")) {
                    ctx->log_level = AWS_LL_TRACE;
                } else if (!strcmp(aws_cli_optarg, "INFO")) {
                    ctx->log_level = AWS_LL_INFO;
                } else if (!strcmp(aws_cli_optarg, "DEBUG")) {
                    ctx->log_level = AWS_LL_DEBUG;
                } else if (!strcmp(aws_cli_optarg, "ERROR")) {
                    ctx->log_level = AWS_LL_ERROR;
                } else {
                    fprintf(stderr, "unsupported log level %s.\n", aws_cli_optarg);
                    s_usage(1);
                }
                break;
            case 'h':
                s_usage(0);
                break;
            default:
                fprintf(stderr, "Unknown option\n");
                s_usage(1);
        }
    }

    if (aws_cli_optind < argc) {
        struct aws_byte_cursor uri_cursor = aws_byte_cursor_from_c_str(argv[aws_cli_optind++]);

        if (aws_uri_init_parse(&ctx->uri, ctx->allocator, &uri_cursor)) {
            fprintf(
                stderr,
                "Failed to parse uri %s with error %s\n",
                (char *)uri_cursor.ptr,
                aws_error_debug_str(aws_last_error()));
            s_usage(1);
        };
    } else {
        fprintf(stderr, "A URI for the request must be supplied.\n");
        s_usage(1);
    }
}

struct connection_user_data {
    struct aws_linked_list_node node;
    struct app_ctx *app_ctx;
    struct aws_mqtt_client *client;
    struct aws_mqtt_client_connection *connection;
    uint32_t id;
    uint32_t subscription_id;
};

static void s_final_destroy_connection_data(struct connection_user_data *user_data) {
    aws_mutex_lock(&user_data->app_ctx->lock);
    aws_linked_list_remove(&user_data->node);
    aws_mutex_unlock(&user_data->app_ctx->lock);
    aws_condition_variable_notify_one(&user_data->app_ctx->signal);

    if (user_data->connection != NULL) {
        aws_mqtt_client_connection_release(user_data->connection);
        user_data->connection = NULL;
    }

    if (user_data->client != NULL) {
        aws_mqtt_client_release(user_data->client);
    }
}

static void s_on_connection_complete(
    struct aws_mqtt_client_connection *connection,
    int error_code,
    enum aws_mqtt_connect_return_code return_code,
    bool session_present,
    void *userdata) {
    (void)connection;
    (void)session_present;

    struct connection_user_data *user_data = userdata;
    if (error_code != AWS_ERROR_SUCCESS || return_code != AWS_MQTT_CONNECT_ACCEPTED) {
        s_final_destroy_connection_data(user_data);
        return;
    }

    aws_mutex_lock(&user_data->app_ctx->lock);
    aws_linked_list_remove(&user_data->node);
    aws_linked_list_push_back(&user_data->app_ctx->established_connection_list, &user_data->node);
    aws_mutex_unlock(&user_data->app_ctx->lock);
    aws_condition_variable_notify_one(&user_data->app_ctx->signal);
}

static bool s_all_connections_complete(void *context) {
    struct app_ctx *app_ctx = context;

    return aws_linked_list_empty(&app_ctx->pending_connection_list);
}

#define MIN_SLEEP_TIME_MILLIS 10

static void s_throttle_operations(int target_ops, uint64_t start_time, int32_t operation_index) {
    if (target_ops == 0) {
        return;
    }

    uint64_t now = 0;
    aws_high_res_clock_get_ticks(&now);
    AWS_FATAL_ASSERT(now >= start_time);

    uint64_t elapsed_nanos = now - start_time;

    uint64_t nanos_per_second = aws_timestamp_convert(1, AWS_TIMESTAMP_SECS, AWS_TIMESTAMP_NANOS, NULL);
    uint64_t target_ops_u64 = (uint64_t)target_ops;
    uint64_t operation_count_u64 = (uint64_t)operation_index;
    uint64_t desired_elapsed_nanos = 0;

    uint64_t product = 0;
    if (aws_mul_u64_checked(operation_count_u64, nanos_per_second, &product)) {
        double product_d = (double)operation_count_u64 * (double)nanos_per_second;
        desired_elapsed_nanos = (uint64_t)(product_d / (double)target_ops_u64);
    } else {
        desired_elapsed_nanos = product / target_ops_u64;
    }

    if (desired_elapsed_nanos > elapsed_nanos) {
        uint64_t nano_difference = desired_elapsed_nanos - elapsed_nanos;
        uint64_t millis = aws_timestamp_convert(nano_difference, AWS_TIMESTAMP_NANOS, AWS_TIMESTAMP_MILLIS, NULL);
        if (millis > MIN_SLEEP_TIME_MILLIS) {
            aws_thread_current_sleep(nano_difference);
        }
    }
}

static void s_establish_connections(
    struct app_ctx *app_ctx,
    struct connection_user_data *connections,
    struct aws_client_bootstrap *bootstrap,
    struct aws_tls_connection_options *tls_connection_options) {
    struct aws_socket_options socket_options = {
        .type = AWS_SOCKET_STREAM,
        .connect_timeout_ms = (uint32_t)app_ctx->connect_timeout,
        .keep_alive_timeout_sec = 0,
        .keepalive = false,
        .keep_alive_interval_sec = 0,
    };

    aws_mutex_lock(&app_ctx->lock);
    for (int j = 0; j < app_ctx->connection_count; j++) {
        struct connection_user_data *connection_data = connections + j;
        aws_linked_list_push_back(&app_ctx->pending_connection_list, &connection_data->node);

        connection_data->app_ctx = app_ctx;
        connection_data->client = aws_mqtt_client_new(app_ctx->allocator, bootstrap);
        AWS_FATAL_ASSERT(connection_data->client != NULL);
        connection_data->connection = aws_mqtt_client_connection_new(connection_data->client);
        AWS_FATAL_ASSERT(connection_data->connection != NULL);
        connection_data->id = (uint32_t)j;
    }
    aws_mutex_unlock(&app_ctx->lock);

    uint64_t start_time = 0;
    aws_high_res_clock_get_ticks(&start_time);

    for (int j = 0; j < app_ctx->connection_count; j++) {
        struct connection_user_data *connection_data = connections + j;

        char client_id[32];
        snprintf(client_id, AWS_ARRAY_SIZE(client_id), "stress%d", j);

        struct aws_mqtt_connection_options connection_options = {
            .host_name = app_ctx->uri.host_name,
            .port = app_ctx->port,
            .socket_options = &socket_options,
            .tls_options = tls_connection_options,
            .client_id = aws_byte_cursor_from_c_str(client_id),
            .keep_alive_time_secs = 0,
            .ping_timeout_ms = 0,
            .protocol_operation_timeout_ms =
                (uint32_t)aws_timestamp_convert(10, AWS_TIMESTAMP_SECS, AWS_TIMESTAMP_MILLIS, NULL),
            .on_connection_complete = s_on_connection_complete,
            .user_data = connection_data,
            .clean_session = true,
        };

        if (aws_mqtt_client_connection_connect(connection_data->connection, &connection_options)) {
            s_final_destroy_connection_data(connection_data);
        }

        if (j > 0 && j % 100 == 0) {
            printf("Connection count: %d\n", j);
        }

        s_throttle_operations(app_ctx->target_cops, start_time, j + 1);
    }

    aws_mutex_lock(&app_ctx->lock);
    aws_condition_variable_wait_pred(&app_ctx->signal, &app_ctx->lock, s_all_connections_complete, app_ctx);
    AWS_FATAL_ASSERT(!aws_linked_list_empty(&app_ctx->established_connection_list));
    aws_mutex_unlock(&app_ctx->lock);
}

void s_on_publish(
    struct aws_mqtt_client_connection *connection,
    const struct aws_byte_cursor *topic,
    const struct aws_byte_cursor *payload,
    bool dup,
    enum aws_mqtt_qos qos,
    bool retain,
    void *userdata) {
    (void)connection;
    (void)topic;
    (void)payload;
    (void)dup;
    (void)qos;
    (void)retain;

    struct app_ctx *app_ctx = userdata;
    aws_mutex_lock(&app_ctx->lock);
    ++app_ctx->received_messages;
    aws_mutex_unlock(&app_ctx->lock);
}

void s_on_disconnect_complete(struct aws_mqtt_client_connection *connection, void *userdata) {
    (void)connection;
    s_final_destroy_connection_data(userdata);
}

void s_on_subscribe_complete(
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
    struct connection_user_data *connection_data = userdata;

    if (error_code != AWS_ERROR_SUCCESS) {
        if (aws_mqtt_client_connection_disconnect(
                connection_data->connection, s_on_disconnect_complete, connection_data)) {
            s_final_destroy_connection_data(connection_data);
        }
        return;
    }

    aws_mutex_lock(&connection_data->app_ctx->lock);
    aws_linked_list_remove(&connection_data->node);
    aws_linked_list_push_back(&connection_data->app_ctx->established_connection_list, &connection_data->node);
    aws_mutex_unlock(&connection_data->app_ctx->lock);
    aws_condition_variable_notify_one(&connection_data->app_ctx->signal);
}

void s_on_subscribe_removed(void *userdata) {
    (void)userdata;
}

static void s_establish_subscriptions(struct app_ctx *app_ctx) {

    struct aws_linked_list connections;
    aws_linked_list_init(&connections);

    aws_mutex_lock(&app_ctx->lock);
    aws_linked_list_swap_contents(&app_ctx->established_connection_list, &connections);
    aws_mutex_unlock(&app_ctx->lock);

    uint64_t start_time = 0;
    aws_high_res_clock_get_ticks(&start_time);
    uint32_t subscription_index = 1;
    while (!aws_linked_list_empty(&connections)) {
        struct aws_linked_list_node *node = aws_linked_list_pop_front(&connections);
        struct connection_user_data *connection_data = AWS_CONTAINER_OF(node, struct connection_user_data, node);

        aws_mutex_lock(&app_ctx->lock);
        aws_linked_list_push_back(&app_ctx->pending_connection_list, &connection_data->node);
        aws_mutex_unlock(&app_ctx->lock);

        connection_data->subscription_id = subscription_index;

        char buffer[32];
        snprintf(buffer, AWS_ARRAY_SIZE(buffer), "topic%d", subscription_index++);
        struct aws_byte_cursor topic_cursor = aws_byte_cursor_from_c_str(buffer);

        uint16_t id = aws_mqtt_client_connection_subscribe(
            connection_data->connection,
            &topic_cursor,
            AWS_MQTT_QOS_AT_LEAST_ONCE,
            s_on_publish,
            app_ctx,
            s_on_subscribe_removed,
            s_on_subscribe_complete,
            connection_data);

        if (id == 0) {
            if (aws_mqtt_client_connection_disconnect(
                    connection_data->connection, s_on_disconnect_complete, connection_data)) {
                s_final_destroy_connection_data(connection_data);
            }
        }

        if (subscription_index > 0 && subscription_index % 100 == 0) {
            printf("Subscribe count: %d\n", subscription_index);
        }

        s_throttle_operations(app_ctx->target_cops, start_time, subscription_index - 1);
    }

    aws_mutex_lock(&app_ctx->lock);
    aws_condition_variable_wait_pred(&app_ctx->signal, &app_ctx->lock, s_all_connections_complete, app_ctx);
    AWS_FATAL_ASSERT(!aws_linked_list_empty(&app_ctx->established_connection_list));
    aws_mutex_unlock(&app_ctx->lock);
}

static void s_teardown_connections(struct app_ctx *app_ctx) {

    struct aws_linked_list connections;
    aws_linked_list_init(&connections);

    aws_mutex_lock(&app_ctx->lock);
    aws_linked_list_swap_contents(&app_ctx->established_connection_list, &connections);
    aws_mutex_unlock(&app_ctx->lock);

    while (!aws_linked_list_empty(&connections)) {
        struct aws_linked_list_node *node = aws_linked_list_pop_front(&connections);
        struct connection_user_data *connection_data = AWS_CONTAINER_OF(node, struct connection_user_data, node);

        aws_mutex_lock(&app_ctx->lock);
        aws_linked_list_push_back(&app_ctx->pending_connection_list, &connection_data->node);
        aws_mutex_unlock(&app_ctx->lock);

        if (aws_mqtt_client_connection_disconnect(
                connection_data->connection, s_on_disconnect_complete, connection_data)) {
            s_final_destroy_connection_data(connection_data);
        }
    }

    aws_mutex_lock(&app_ctx->lock);
    aws_condition_variable_wait_pred(&app_ctx->signal, &app_ctx->lock, s_all_connections_complete, app_ctx);
    aws_mutex_unlock(&app_ctx->lock);
}

static void s_build_web(struct app_ctx *app_ctx, struct aws_array_list *connections, struct aws_array_list *topics) {
    aws_array_list_init_dynamic(
        connections, app_ctx->allocator, app_ctx->connection_count, sizeof(struct connection_user_data *));
    aws_array_list_init_dynamic(topics, app_ctx->allocator, app_ctx->connection_count, sizeof(uint32_t));

    aws_mutex_lock(&app_ctx->lock);

    struct aws_linked_list_node *node = aws_linked_list_begin(&app_ctx->established_connection_list);
    while (node != aws_linked_list_end(&app_ctx->established_connection_list)) {
        struct connection_user_data *connection_data = AWS_CONTAINER_OF(node, struct connection_user_data, node);
        uint32_t topic_id = connection_data->subscription_id;

        aws_array_list_push_back(connections, &connection_data);
        aws_array_list_push_back(topics, &topic_id);

        node = aws_linked_list_next(node);
    }
    aws_mutex_unlock(&app_ctx->lock);
}

static void s_on_publish_complete(
    struct aws_mqtt_client_connection *connection,
    uint16_t packet_id,
    int error_code,
    void *userdata) {
    (void)connection;
    (void)packet_id;

    struct app_ctx *app_ctx = userdata;
    aws_mutex_lock(&app_ctx->lock);
    --app_ctx->pending_publish_completions;
    if (error_code == AWS_ERROR_SUCCESS) {
        ++app_ctx->publish_successes;
    } else {
        ++app_ctx->publish_failures;
    }
    aws_mutex_unlock(&app_ctx->lock);
    aws_condition_variable_notify_one(&app_ctx->signal);
}

static bool s_all_publishes_complete(void *userdata) {
    struct app_ctx *app_ctx = userdata;

    return app_ctx->pending_publish_completions == 0;
}

static void s_publish(struct app_ctx *app_ctx) {
    struct aws_array_list valid_topics;
    AWS_ZERO_STRUCT(valid_topics);
    struct aws_array_list valid_connections;
    AWS_ZERO_STRUCT(valid_connections);

    s_build_web(app_ctx, &valid_connections, &valid_topics);
    printf("Using %d connections\n", (int)aws_array_list_length(&valid_connections));

    size_t connection_count = aws_array_list_length(&valid_connections);
    AWS_FATAL_ASSERT(connection_count > 0);
    size_t topic_count = aws_array_list_length(&valid_topics);
    AWS_FATAL_ASSERT(topic_count > 0);

    aws_mutex_lock(&app_ctx->lock);
    app_ctx->pending_publish_completions = app_ctx->message_count;
    aws_mutex_unlock(&app_ctx->lock);

    struct aws_byte_cursor payload_cursor = aws_byte_cursor_from_c_str("MESSAGE PAYLOAD");

    char topic_buffer[32];

    uint64_t now = 0;
    aws_high_res_clock_get_ticks(&now);
    int failed_publishes = 0;
    for (int i = 0; i < app_ctx->message_count; ++i) {
        size_t random_connection_index = rand() % connection_count;
        struct connection_user_data *connection_ud = NULL;
        aws_array_list_get_at(&valid_connections, &connection_ud, random_connection_index);

        size_t random_topic_index = rand() % topic_count;
        uint32_t topic_id = 0;
        aws_array_list_get_at(&valid_topics, &topic_id, random_topic_index);

        snprintf(topic_buffer, AWS_ARRAY_SIZE(topic_buffer), "topic%d", topic_id);
        struct aws_byte_cursor topic_cursor = aws_byte_cursor_from_c_str(topic_buffer);

        uint16_t id = aws_mqtt_client_connection_publish(
            connection_ud->connection,
            &topic_cursor,
            AWS_MQTT_QOS_AT_LEAST_ONCE,
            false,
            &payload_cursor,
            s_on_publish_complete,
            app_ctx);

        if (id == 0) {
            ++failed_publishes;
        }

        if (i > 0 && i % 1000 == 0) {
            printf("Publish count: %d\n", i);
        }

        s_throttle_operations(app_ctx->target_pops, now, i + 1);
    }

    aws_mutex_lock(&app_ctx->lock);
    app_ctx->pending_publish_completions -= failed_publishes;
    app_ctx->publish_failures += failed_publishes;
    aws_condition_variable_wait_pred(&app_ctx->signal, &app_ctx->lock, s_all_publishes_complete, app_ctx);
    aws_mutex_unlock(&app_ctx->lock);

    aws_array_list_clean_up(&valid_topics);
    aws_array_list_clean_up(&valid_connections);
}

static void s_reset_app_ctx(struct app_ctx *app_ctx) {
    AWS_FATAL_ASSERT(aws_linked_list_empty(&app_ctx->pending_connection_list));
    aws_linked_list_init(&app_ctx->pending_connection_list);
    AWS_FATAL_ASSERT(aws_linked_list_empty(&app_ctx->established_connection_list));
    aws_linked_list_init(&app_ctx->established_connection_list);

    app_ctx->pending_publish_completions = 0;
    app_ctx->publish_successes = 0;
    app_ctx->publish_failures = 0;
    app_ctx->received_messages = 0;
}

int main(int argc, char **argv) {
    struct aws_allocator *allocator = aws_mem_tracer_new(aws_default_allocator(), NULL, AWS_MEMTRACE_STACKS, 8);

    aws_mqtt_library_init(allocator);

    struct app_ctx app_ctx;
    AWS_ZERO_STRUCT(app_ctx);
    app_ctx.allocator = allocator;
    app_ctx.signal = (struct aws_condition_variable)AWS_CONDITION_VARIABLE_INIT;
    app_ctx.connect_timeout = 3000;
    app_ctx.iterations = 1;
    app_ctx.message_count = 10;
    app_ctx.connection_count = 1;
    aws_mutex_init(&app_ctx.lock);
    app_ctx.port = 8883;
    app_ctx.target_cops = 250;
    app_ctx.target_pops = 10000;

    aws_linked_list_init(&app_ctx.pending_connection_list);
    aws_linked_list_init(&app_ctx.established_connection_list);

    s_parse_options(argc, argv, &app_ctx);
    if (app_ctx.uri.port) {
        app_ctx.port = app_ctx.uri.port;
    }

    struct aws_logger logger;
    AWS_ZERO_STRUCT(logger);

    struct aws_logger_standard_options options = {
        .level = app_ctx.log_level,
    };

    if (app_ctx.log_level) {
        if (app_ctx.log_filename) {
            options.filename = app_ctx.log_filename;
        } else {
            options.file = stderr;
        }

        if (aws_logger_init_standard(&logger, allocator, &options)) {
            fprintf(stderr, "Failed to initialize logger with error %s\n", aws_error_debug_str(aws_last_error()));
            exit(1);
        }

        aws_logger_set(&logger);
    }

    for (int i = 0; i < app_ctx.iterations; ++i) {

        printf("Iteration %d starting\n", i + 1);
        s_reset_app_ctx(&app_ctx);

        struct aws_tls_ctx *tls_ctx = NULL;
        struct aws_tls_ctx_options tls_ctx_options;
        AWS_ZERO_STRUCT(tls_ctx_options);
        struct aws_tls_connection_options tls_connection_options;
        AWS_ZERO_STRUCT(tls_connection_options);

        if (app_ctx.cert && app_ctx.key) {
            if (aws_tls_ctx_options_init_client_mtls_from_path(
                    &tls_ctx_options, allocator, app_ctx.cert, app_ctx.key)) {
                fprintf(
                    stderr,
                    "Failed to load %s and %s with error %s.",
                    app_ctx.cert,
                    app_ctx.key,
                    aws_error_debug_str(aws_last_error()));
                exit(1);
            }
        } else {
            aws_tls_ctx_options_init_default_client(&tls_ctx_options, allocator);
        }

        if (app_ctx.cacert) {
            if (aws_tls_ctx_options_override_default_trust_store_from_path(&tls_ctx_options, NULL, app_ctx.cacert)) {
                fprintf(
                    stderr, "Failed to load %s with error %s", app_ctx.cacert, aws_error_debug_str(aws_last_error()));
                exit(1);
            }
        }

        if (aws_tls_ctx_options_set_alpn_list(&tls_ctx_options, "x-amzn-mqtt-ca")) {
            fprintf(stderr, "Failed to set alpn list with error %s.", aws_error_debug_str(aws_last_error()));
            exit(1);
        }

        tls_ctx = aws_tls_client_ctx_new(allocator, &tls_ctx_options);

        if (!tls_ctx) {
            fprintf(stderr, "Failed to initialize TLS context with error %s.", aws_error_debug_str(aws_last_error()));
            exit(1);
        }

        aws_tls_connection_options_init_from_ctx(&tls_connection_options, tls_ctx);
        if (aws_tls_connection_options_set_server_name(&tls_connection_options, allocator, &app_ctx.uri.host_name)) {
            fprintf(stderr, "Failed to set servername with error %s.", aws_error_debug_str(aws_last_error()));
            exit(1);
        }

        struct aws_event_loop_group *el_group = aws_event_loop_group_new_default(allocator, 2, NULL);

        struct aws_host_resolver_default_options resolver_options = {
            .el_group = el_group,
            .max_entries = 8,
        };

        struct aws_host_resolver *resolver = aws_host_resolver_new_default(allocator, &resolver_options);

        struct aws_client_bootstrap_options bootstrap_options = {
            .event_loop_group = el_group,
            .host_resolver = resolver,
        };
        struct aws_client_bootstrap *bootstrap = aws_client_bootstrap_new(allocator, &bootstrap_options);

        struct connection_user_data *connections =
            aws_mem_calloc(allocator, app_ctx.connection_count, sizeof(struct connection_user_data));
        AWS_FATAL_ASSERT(connections != NULL);

        s_establish_connections(&app_ctx, connections, bootstrap, &tls_connection_options);

        s_establish_subscriptions(&app_ctx);

        s_publish(&app_ctx);

        s_teardown_connections(&app_ctx);

        aws_mem_release(allocator, connections);

        aws_client_bootstrap_release(bootstrap);
        aws_host_resolver_release(resolver);
        aws_event_loop_group_release(el_group);

        if (tls_ctx) {
            aws_tls_connection_options_clean_up(&tls_connection_options);
            aws_tls_ctx_release(tls_ctx);
            aws_tls_ctx_options_clean_up(&tls_ctx_options);
        }

        aws_thread_join_all_managed();

        const size_t outstanding_bytes = aws_mem_tracer_bytes(allocator);
        printf("Iteration %d summary:\n", i + 1);
        printf("  Successful Publishes: %d\n", app_ctx.publish_successes);
        printf("  Failed Publishes: %d\n", app_ctx.publish_failures);
        printf("  Outstanding bytes: %zu\n\n", outstanding_bytes);
    }

    if (app_ctx.log_level) {
        aws_logger_set(NULL);
        aws_logger_clean_up(&logger);
    }

    aws_uri_clean_up(&app_ctx.uri);

    aws_mqtt_library_clean_up();

    const size_t leaked_bytes = aws_mem_tracer_bytes(allocator);
    if (leaked_bytes) {
        struct aws_logger memory_logger;
        AWS_ZERO_STRUCT(memory_logger);

        aws_logger_init_noalloc(&memory_logger, aws_default_allocator(), &options);
        aws_logger_set(&memory_logger);

        aws_mqtt_library_init(aws_default_allocator());

        printf("Writing memory leaks to log.\n");
        aws_mem_tracer_dump(allocator);

        aws_logger_set(NULL);
        aws_logger_clean_up(&memory_logger);

        aws_mqtt_library_clean_up();
    } else {
        printf("Finished, with no memory leaks\n");
    }

    aws_mem_tracer_destroy(allocator);

    return 0;
}
