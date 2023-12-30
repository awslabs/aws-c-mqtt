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

#include <aws/mqtt/private/v5/mqtt5_utils.h>
#include <aws/mqtt/v5/mqtt5_client.h>
#include <aws/mqtt/v5/mqtt5_types.h>

#include <inttypes.h>

#ifdef _MSC_VER
#    pragma warning(disable : 4996) /* Disable warnings about fopen() being insecure */
#    pragma warning(disable : 4204) /* Declared initializers */
#    pragma warning(disable : 4221) /* Local var in declared initializer */
#endif

#ifdef WIN32
// Windows does not need specific imports
#else
#    include <stdio.h>
#endif

struct app_ctx {
    struct aws_allocator *allocator;
    struct aws_mutex lock;
    struct aws_condition_variable signal;
    struct aws_uri uri;
    uint32_t port;
    const char *cacert;
    const char *cert;
    const char *key;
    int connect_timeout;
    bool use_websockets;

    struct aws_tls_connection_options tls_connection_options;

    const char *log_filename;
    enum aws_log_level log_level;
};

static void s_usage(int exit_code) {

    fprintf(stderr, "usage: elastipubsub5 [options] endpoint\n");
    fprintf(stderr, " endpoint: url to connect to\n");
    fprintf(stderr, "\n Options:\n\n");
    fprintf(stderr, "      --cacert FILE: path to a CA certficate file.\n");
    fprintf(stderr, "      --cert FILE: path to a PEM encoded certificate to use with mTLS\n");
    fprintf(stderr, "      --key FILE: Path to a PEM encoded private key that matches cert.\n");
    fprintf(stderr, "      --connect-timeout INT: time in milliseconds to wait for a connection.\n");
    fprintf(stderr, "  -l, --log FILE: dumps logs to FILE instead of stderr.\n");
    fprintf(stderr, "  -v, --verbose: ERROR|INFO|DEBUG|TRACE: log level to configure. Default is none.\n");
    fprintf(stderr, "  -w, --websockets: use mqtt-over-websockets rather than direct mqtt\n");
    fprintf(stderr, "  -h, --help\n");
    fprintf(stderr, "            Display this message and quit.\n");
    exit(exit_code);
}

static struct aws_cli_option s_long_options[] = {
    {"cacert", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'a'},
    {"cert", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'c'},
    {"key", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'e'},
    {"connect-timeout", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'f'},
    {"log", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'l'},
    {"verbose", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'v'},
    {"websockets", AWS_CLI_OPTIONS_NO_ARGUMENT, NULL, 'w'},
    {"help", AWS_CLI_OPTIONS_NO_ARGUMENT, NULL, 'h'},
    /* Per getopt(3) the last element of the array has to be filled with all zeros */
    {NULL, AWS_CLI_OPTIONS_NO_ARGUMENT, NULL, 0},
};

static void s_parse_options(int argc, char **argv, struct app_ctx *ctx) {
    bool uri_found = false;

    while (true) {
        int option_index = 0;
        int c = aws_cli_getopt_long(argc, argv, "a:c:e:f:l:v:wh", s_long_options, &option_index);
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
            case 'e':
                ctx->key = aws_cli_optarg;
                break;
            case 'f':
                ctx->connect_timeout = atoi(aws_cli_optarg);
                break;
            case 'l':
                ctx->log_filename = aws_cli_optarg;
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
            case 'w':
                ctx->use_websockets = true;
                break;
            case 0x02: {
                struct aws_byte_cursor uri_cursor = aws_byte_cursor_from_c_str(aws_cli_positional_arg);
                if (aws_uri_init_parse(&ctx->uri, ctx->allocator, &uri_cursor)) {
                    fprintf(
                        stderr,
                        "Failed to parse uri %s with error %s\n",
                        (char *)uri_cursor.ptr,
                        aws_error_debug_str(aws_last_error()));
                    s_usage(1);
                }
                uri_found = true;
                break;
            }

            default:
                fprintf(stderr, "Unknown option\n");
                s_usage(1);
        }
    }

    if (!uri_found) {
        fprintf(stderr, "A URI for the request must be supplied.\n");
        s_usage(1);
    }
}

static void s_on_subscribe_complete_fn(
    const struct aws_mqtt5_packet_suback_view *suback,
    int error_code,
    void *complete_ctx) {

    (void)error_code;
    (void)complete_ctx;

    printf("SUBACK received!\n");
    for (size_t i = 0; i < suback->reason_code_count; ++i) {
        printf("Subscription %d: %s\n", (int)i, aws_mqtt5_suback_reason_code_to_c_string(suback->reason_codes[i]));
    }
    fflush(stdout);
}

static void s_on_unsubscribe_complete_fn(
    const struct aws_mqtt5_packet_unsuback_view *unsuback,
    int error_code,
    void *complete_ctx) {

    (void)error_code;
    (void)complete_ctx;

    printf("UNSUBACK received!\n");
    for (size_t i = 0; i < unsuback->reason_code_count; ++i) {
        printf("Topic Filter %d: %s\n", (int)i, aws_mqtt5_unsuback_reason_code_to_c_string(unsuback->reason_codes[i]));
    }
    fflush(stdout);
}

static void s_on_publish_complete_fn(
    enum aws_mqtt5_packet_type packet_type,
    const void *packet,
    int error_code,
    void *complete_ctx) {
    (void)complete_ctx;

    switch (error_code) {
        case AWS_ERROR_MQTT5_OPERATION_FAILED_DUE_TO_OFFLINE_QUEUE_POLICY:
            printf("PUBLISH FAILED due to disconnect and offline queue policy");
            break;
        case AWS_ERROR_MQTT_TIMEOUT:
            printf("PUBLISH FAILED due to MQTT Timeout");
            break;
        case AWS_ERROR_SUCCESS:
            printf("PUBLISH SUCCESS\n");
            break;
        default:
            break;
    }

    if (packet_type == AWS_MQTT5_PT_PUBACK) {
        const struct aws_mqtt5_packet_puback_view *puback = packet;
        printf("PUBACK received!\n");
        printf("PUBACK id:%d %s\n", puback->packet_id, aws_mqtt5_puback_reason_code_to_c_string(puback->reason_code));
    } else {
        printf("PUBLISH Complete with no PUBACK\n");
    }

    fflush(stdout);
}

static void s_on_publish_received(const struct aws_mqtt5_packet_publish_view *publish, void *user_data) {
    (void)publish;
    (void)user_data;

    printf("PUBLISH received!\n");
    printf(
        "Publish received to topic:'" PRInSTR "' payload '" PRInSTR "'\n",
        AWS_BYTE_CURSOR_PRI(publish->topic),
        AWS_BYTE_CURSOR_PRI(publish->payload));
}

static void s_lifecycle_event_callback(const struct aws_mqtt5_client_lifecycle_event *event) {

    switch (event->event_type) {
        case AWS_MQTT5_CLET_STOPPED:
            printf("Lifecycle event: Stopped!\n");
            break;

        case AWS_MQTT5_CLET_ATTEMPTING_CONNECT:
            printf("Lifecycle event: Attempting Connect!\n");
            break;

        case AWS_MQTT5_CLET_CONNECTION_FAILURE:
            printf("Lifecycle event: Connection Failure!\n");
            printf("  Error Code: %d(%s)\n", event->error_code, aws_error_debug_str(event->error_code));
            break;

        case AWS_MQTT5_CLET_CONNECTION_SUCCESS:
            printf("Lifecycle event: Connection Success!\n");
            break;

        case AWS_MQTT5_CLET_DISCONNECTION:
            printf("Lifecycle event: Disconnect!\n");
            printf("  Error Code: %d(%s)\n", event->error_code, aws_error_debug_str(event->error_code));
            break;
    }

    fflush(stdout);
}

static bool s_skip_whitespace(uint8_t value) {
    return value == '\n' || value == '\r' || value == '\t' || value == ' ';
}

static void s_split_command_line(struct aws_byte_cursor cursor, struct aws_array_list *words) {
    struct aws_byte_cursor split_cursor;
    AWS_ZERO_STRUCT(split_cursor);

    while (aws_byte_cursor_next_split(&cursor, ' ', &split_cursor)) {
        struct aws_byte_cursor word_cursor = aws_byte_cursor_trim_pred(&split_cursor, &s_skip_whitespace);
        if (word_cursor.len > 0) {
            aws_array_list_push_back(words, &word_cursor);
        }
    }
}

static void s_handle_subscribe(
    struct aws_mqtt5_client *client,
    struct aws_allocator *allocator,
    struct aws_array_list *arguments) {
    struct aws_mqtt5_subscribe_completion_options subscribe_completion_options = {
        .completion_callback = &s_on_subscribe_complete_fn,
        .completion_user_data = NULL,
    };

    size_t argument_count = aws_array_list_length(arguments) - 1;
    if (argument_count < 2) {
        printf("invalid subscribe options:\n");
        printf("  subscribe <qos: [0, 1, 2]> topic1 topic2 ....\n");
        return;
    }

    struct aws_byte_cursor qos_cursor;
    AWS_ZERO_STRUCT(qos_cursor);
    aws_array_list_get_at(arguments, &qos_cursor, 1);

    struct aws_string *qos_string = aws_string_new_from_cursor(allocator, &qos_cursor);

    int qos_value = atoi((const char *)qos_string->bytes);
    enum aws_mqtt5_qos qos = qos_value;

    size_t topic_count = aws_array_list_length(arguments) - 2;

    struct aws_array_list subscriptions;
    aws_array_list_init_dynamic(&subscriptions, allocator, topic_count, sizeof(struct aws_mqtt5_subscription_view));

    printf("Subscribing to:\n");

    for (size_t i = 0; i < topic_count; ++i) {
        size_t topic_index = i + 2;
        struct aws_byte_cursor topic_filter_cursor;

        aws_array_list_get_at(arguments, &topic_filter_cursor, topic_index);

        struct aws_mqtt5_subscription_view subscription = {
            .topic_filter = topic_filter_cursor,
            .qos = qos,
            .no_local = false,
            .retain_as_published = false,
            .retain_handling_type = AWS_MQTT5_RHT_DONT_SEND,
        };

        printf(" %d:" PRInSTR "\n", (int)i, AWS_BYTE_CURSOR_PRI(topic_filter_cursor));
        aws_array_list_push_back(&subscriptions, &subscription);
    }

    struct aws_mqtt5_packet_subscribe_view packet_subscribe_view = {
        .subscription_count = aws_array_list_length(&subscriptions),
        .subscriptions = subscriptions.data,
    };

    aws_mqtt5_client_subscribe(client, &packet_subscribe_view, &subscribe_completion_options);

    aws_array_list_clean_up(&subscriptions);
    aws_string_destroy(qos_string);
}

static void s_handle_unsubscribe(struct aws_mqtt5_client *client, struct aws_array_list *arguments) {
    struct aws_mqtt5_unsubscribe_completion_options unsubscribe_completion_options = {
        .completion_callback = &s_on_unsubscribe_complete_fn,
        .completion_user_data = NULL,
    };

    size_t argument_count = aws_array_list_length(arguments) - 1;
    if (argument_count < 1) {
        printf("invalid unsubscribe options:\n");
        printf("  unsubscribe topic1 topic2 ....\n");
        return;
    }

    size_t topic_count = aws_array_list_length(arguments) - 1;

    printf("Unsubscribing to:\n");

    for (size_t i = 0; i < topic_count; ++i) {
        size_t topic_index = i + 1;
        struct aws_byte_cursor topic_filter_cursor;

        aws_array_list_get_at(arguments, &topic_filter_cursor, topic_index);

        printf(" %d:" PRInSTR "\n", (int)i, AWS_BYTE_CURSOR_PRI(topic_filter_cursor));
    }

    struct aws_mqtt5_packet_unsubscribe_view packet_unsubscribe_view = {
        .topic_filter_count = topic_count,
        .topic_filters = ((struct aws_byte_cursor *)arguments->data) + 1,
    };

    aws_mqtt5_client_unsubscribe(client, &packet_unsubscribe_view, &unsubscribe_completion_options);
}

static void s_handle_publish(
    struct aws_mqtt5_client *client,
    struct aws_allocator *allocator,
    struct aws_array_list *arguments,
    struct aws_byte_cursor *full_argument) {
    struct aws_mqtt5_publish_completion_options publish_completion_options = {
        .completion_callback = &s_on_publish_complete_fn,
        .completion_user_data = NULL,
    };

    size_t argument_count = aws_array_list_length(arguments) - 1;
    if (argument_count < 2) {
        printf("invalid publish call:\n");
        printf("  publish <qos: [0, 1, 2]> topic <text to publish>\n");
        return;
    }

    /* QoS  */
    struct aws_byte_cursor qos_cursor;
    AWS_ZERO_STRUCT(qos_cursor);
    aws_array_list_get_at(arguments, &qos_cursor, 1);
    struct aws_string *qos_string = aws_string_new_from_cursor(allocator, &qos_cursor);
    int qos_value = atoi((const char *)qos_string->bytes);
    enum aws_mqtt5_qos qos = qos_value;

    /* TOPIC */
    struct aws_byte_cursor topic_cursor;
    AWS_ZERO_STRUCT(topic_cursor);
    aws_array_list_get_at(arguments, &topic_cursor, 2);

    /* PAYLOAD */
    struct aws_byte_cursor payload_cursor;
    AWS_ZERO_STRUCT(payload_cursor);
    /* account for empty payload */

    if (argument_count > 2) {
        aws_array_list_get_at(arguments, &payload_cursor, 3);
        payload_cursor.len = (size_t)(full_argument->ptr + full_argument->len - payload_cursor.ptr);
    }

    printf(
        "Publishing to Topic:'" PRInSTR "' Payload:'" PRInSTR "'\n",
        AWS_BYTE_CURSOR_PRI(topic_cursor),
        AWS_BYTE_CURSOR_PRI(payload_cursor));

    struct aws_mqtt5_packet_publish_view packet_publish_view = {
        .qos = qos,
        .topic = topic_cursor,
        .retain = false,
        .duplicate = false,
        .payload = payload_cursor,
    };

    aws_mqtt5_client_publish(client, &packet_publish_view, &publish_completion_options);

    aws_string_destroy(qos_string);
}

static void s_on_disconnect_completion(int error_code, void *user_data) {
    (void)user_data;

    printf("DISCONNECT complete with error code %d(%s)!", error_code, aws_error_debug_str(error_code));
    fflush(stdout);
}

static void s_handle_stop(
    struct aws_mqtt5_client *client,
    struct aws_allocator *allocator,
    struct aws_array_list *arguments) {

    size_t argument_count = aws_array_list_length(arguments) - 1;
    switch (argument_count) {
        case 0:
            printf("Stopping client by shutting down channel!\n");
            aws_mqtt5_client_stop(client, NULL, NULL);
            break;

        case 1: {
            struct aws_byte_cursor reason_code_cursor;
            AWS_ZERO_STRUCT(reason_code_cursor);
            aws_array_list_get_at(arguments, &reason_code_cursor, 1);

            struct aws_string *reason_code_string = aws_string_new_from_cursor(allocator, &reason_code_cursor);

            int reason_code_value = atoi((const char *)reason_code_string->bytes);
            enum aws_mqtt5_disconnect_reason_code reason_code = reason_code_value;
            aws_string_destroy(reason_code_string);

            struct aws_mqtt5_packet_disconnect_view disconnect_options = {
                .reason_code = reason_code,
            };

            struct aws_mqtt5_disconnect_completion_options completion_options = {
                .completion_callback = s_on_disconnect_completion,
                .completion_user_data = client,
            };

            printf(
                "Stopping client cleanly by sending DISCONNECT packet with reason code %d(%s)!\n",
                reason_code_value,
                aws_mqtt5_disconnect_reason_code_to_c_string(reason_code, NULL));
            aws_mqtt5_client_stop(client, &disconnect_options, &completion_options);
            break;
        }

        default:
            printf("invalid stop options:\n");
            printf("  stop [optional int: reason_code]\n");
            break;
    }
}

static bool s_handle_input(struct aws_mqtt5_client *client, struct aws_allocator *allocator, const char *input_line) {

    struct aws_byte_cursor quit_cursor = aws_byte_cursor_from_c_str("quit");
    struct aws_byte_cursor start_cursor = aws_byte_cursor_from_c_str("start");
    struct aws_byte_cursor stop_cursor = aws_byte_cursor_from_c_str("stop");
    struct aws_byte_cursor subscribe_cursor = aws_byte_cursor_from_c_str("subscribe");
    struct aws_byte_cursor unsubscribe_cursor = aws_byte_cursor_from_c_str("unsubscribe");
    struct aws_byte_cursor publish_cursor = aws_byte_cursor_from_c_str("publish");

    struct aws_array_list words;
    aws_array_list_init_dynamic(&words, allocator, 10, sizeof(struct aws_byte_cursor));

    struct aws_byte_cursor line_cursor = aws_byte_cursor_from_c_str(input_line);
    line_cursor = aws_byte_cursor_trim_pred(&line_cursor, &s_skip_whitespace);

    bool done = false;

    s_split_command_line(line_cursor, &words);
    if (aws_array_list_length(&words) == 0) {
        printf("Empty command line\n");
        goto done;
    }

    struct aws_byte_cursor command_cursor;
    AWS_ZERO_STRUCT(command_cursor);
    aws_array_list_get_at(&words, &command_cursor, 0);

    if (aws_byte_cursor_eq_ignore_case(&command_cursor, &quit_cursor)) {
        printf("Quitting!\n");
        done = true;
    } else if (aws_byte_cursor_eq_ignore_case(&command_cursor, &start_cursor)) {
        printf("Starting client!\n");
        aws_mqtt5_client_start(client);
    } else if (aws_byte_cursor_eq_ignore_case(&command_cursor, &stop_cursor)) {
        s_handle_stop(client, allocator, &words);
    } else if (aws_byte_cursor_eq_ignore_case(&command_cursor, &subscribe_cursor)) {
        s_handle_subscribe(client, allocator, &words);
    } else if (aws_byte_cursor_eq_ignore_case(&command_cursor, &unsubscribe_cursor)) {
        s_handle_unsubscribe(client, &words);
    } else if (aws_byte_cursor_eq_ignore_case(&command_cursor, &publish_cursor)) {
        s_handle_publish(client, allocator, &words, &line_cursor);
    } else {
        printf("Unknown command: " PRInSTR "\n", AWS_BYTE_CURSOR_PRI(command_cursor));
    }

done:

    aws_array_list_clean_up(&words);
    return done;
}

static void s_aws_mqtt5_transform_websocket_handshake_fn(
    struct aws_http_message *request,
    void *user_data,
    aws_mqtt5_transform_websocket_handshake_complete_fn *complete_fn,
    void *complete_ctx) {

    (void)user_data;

    (*complete_fn)(request, AWS_ERROR_SUCCESS, complete_ctx);
}

AWS_STATIC_STRING_FROM_LITERAL(s_client_id, "HelloWorld");

int main(int argc, char **argv) {
    struct aws_allocator *allocator = aws_mem_tracer_new(aws_default_allocator(), NULL, AWS_MEMTRACE_STACKS, 15);

    aws_mqtt_library_init(allocator);

    struct app_ctx app_ctx;
    AWS_ZERO_STRUCT(app_ctx);
    app_ctx.allocator = allocator;
    app_ctx.signal = (struct aws_condition_variable)AWS_CONDITION_VARIABLE_INIT;
    app_ctx.connect_timeout = 3000;
    aws_mutex_init(&app_ctx.lock);
    app_ctx.port = 1883;

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

    bool use_tls = false;
    struct aws_tls_ctx *tls_ctx = NULL;
    struct aws_tls_ctx_options tls_ctx_options;
    AWS_ZERO_STRUCT(tls_ctx_options);
    struct aws_tls_connection_options tls_connection_options;
    AWS_ZERO_STRUCT(tls_connection_options);

    if (app_ctx.cert && app_ctx.key) {
        if (aws_tls_ctx_options_init_client_mtls_from_path(&tls_ctx_options, allocator, app_ctx.cert, app_ctx.key)) {
            fprintf(
                stderr,
                "Failed to load %s and %s with error %s.",
                app_ctx.cert,
                app_ctx.key,
                aws_error_debug_str(aws_last_error()));
            exit(1);
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

        use_tls = true;
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

    struct aws_socket_options socket_options = {
        .type = AWS_SOCKET_STREAM,
        .connect_timeout_ms = (uint32_t)app_ctx.connect_timeout,
        .keep_alive_timeout_sec = 0,
        .keepalive = false,
        .keep_alive_interval_sec = 0,
    };

    uint16_t receive_maximum = 9;
    uint32_t maximum_packet_size = 128 * 1024;

    struct aws_mqtt5_packet_connect_view connect_options = {
        .keep_alive_interval_seconds = 30,
        .client_id = aws_byte_cursor_from_string(s_client_id),
        .clean_start = true,
        .maximum_packet_size_bytes = &maximum_packet_size,
        .receive_maximum = &receive_maximum,
    };

    aws_mqtt5_transform_websocket_handshake_fn *websocket_handshake_transform = NULL;
    void *websocket_handshake_transform_user_data = NULL;
    if (app_ctx.use_websockets) {
        websocket_handshake_transform = &s_aws_mqtt5_transform_websocket_handshake_fn;
    }

    struct aws_mqtt5_client_options client_options = {
        .host_name = app_ctx.uri.host_name,
        .port = app_ctx.port,
        .bootstrap = bootstrap,
        .socket_options = &socket_options,
        .tls_options = (use_tls) ? &tls_connection_options : NULL,
        .connect_options = &connect_options,
        .session_behavior = AWS_MQTT5_CSBT_CLEAN,
        .lifecycle_event_handler = s_lifecycle_event_callback,
        .lifecycle_event_handler_user_data = NULL,
        .retry_jitter_mode = AWS_EXPONENTIAL_BACKOFF_JITTER_NONE,
        .min_reconnect_delay_ms = 1000,
        .max_reconnect_delay_ms = 120000,
        .min_connected_time_to_reset_reconnect_delay_ms = 30000,
        .ping_timeout_ms = 10000,
        .websocket_handshake_transform = websocket_handshake_transform,
        .websocket_handshake_transform_user_data = websocket_handshake_transform_user_data,
        .publish_received_handler = s_on_publish_received,
    };

    struct aws_mqtt5_client *client = aws_mqtt5_client_new(allocator, &client_options);
    aws_mqtt5_client_start(client);

    bool done = false;
    while (!done) {
        printf("Enter command:\n");

        char input_buffer[4096];
#ifdef WIN32
        char *line = gets_s(input_buffer, AWS_ARRAY_SIZE(input_buffer));
#else
        char *line = fgets(input_buffer, AWS_ARRAY_SIZE(input_buffer), stdin);
#endif
        done = s_handle_input(client, allocator, line);
    }

    aws_mqtt5_client_release(client);

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
    printf("Summary:\n");
    printf("  Outstanding bytes: %zu\n\n", outstanding_bytes);

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
