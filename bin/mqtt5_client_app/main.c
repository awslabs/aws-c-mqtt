/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/command_line_parser.h>
#include <aws/common/condition_variable.h>
#include <aws/common/mutex.h>
#include <aws/common/clock.h>
#include <aws/common/error.h>
#include <aws/common/allocator.h>
#include <aws/common/string.h>
#include <aws/common/uri.h>
#include <aws/io/tls_channel_handler.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/io/host_resolver.h>
#include <aws/io/socket.h>
#include <aws/io/socks5.h>
#include <aws/io/logging.h>
#include <aws/mqtt/client.h>
#include <aws/mqtt/mqtt.h>
#include <aws/mqtt/v5/mqtt5_client.h>
#include <aws/mqtt/v5/mqtt5_types.h> 
#include <aws/http/websocket.h>
#include <aws/http/proxy.h>
#include <aws/io/socket_channel_handler.h>
#include <aws/io/socks5_channel_handler.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <errno.h>
#include <stdbool.h>

struct socks5_proxy_settings {
    char *host;
    char *username;
    char *password;
    uint16_t port;
    bool resolve_host_with_proxy;
};

static void s_socks5_proxy_settings_clean_up(
    struct socks5_proxy_settings *settings,
    struct aws_allocator *allocator) {
    if (!settings) {
        return;
    }
    if (settings->host) {
        aws_mem_release(allocator, settings->host);
    }
    if (settings->username) {
        aws_mem_release(allocator, settings->username);
    }
    if (settings->password) {
        aws_mem_release(allocator, settings->password);
    }
    AWS_ZERO_STRUCT(*settings);
}

static int s_socks5_proxy_settings_init_from_uri(
    struct socks5_proxy_settings *settings,
    struct aws_allocator *allocator,
    const char *proxy_uri) {

    if (!settings || !allocator || !proxy_uri) {
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    s_socks5_proxy_settings_clean_up(settings, allocator);

    struct aws_byte_cursor uri_cursor = aws_byte_cursor_from_c_str(proxy_uri);
    struct aws_uri uri;
    AWS_ZERO_STRUCT(uri);

    if (aws_uri_init_parse(&uri, allocator, &uri_cursor)) {
        fprintf(stderr, "Failed to parse proxy URI \"%s\": %s\n", proxy_uri, aws_error_debug_str(aws_last_error()));
        goto on_error;
    }

    const struct aws_byte_cursor *scheme = aws_uri_scheme(&uri);
    if (!scheme || !scheme->len) {
        fprintf(stderr, "Proxy URI \"%s\" must include scheme socks5h://\n", proxy_uri);
        goto on_error;
    }

    if (aws_byte_cursor_eq_c_str_ignore_case(scheme, "socks5h")) {
        settings->resolve_host_with_proxy = true;
    } else if (aws_byte_cursor_eq_c_str_ignore_case(scheme, "socks5")) {
        settings->resolve_host_with_proxy = false;
    } else {
        fprintf(stderr, "Unsupported proxy scheme in \"%s\". Expected socks5h://\n", proxy_uri);
        goto on_error;
    }

    const struct aws_byte_cursor *host = aws_uri_host_name(&uri);
    if (!host || host->len == 0) {
        fprintf(stderr, "Proxy URI \"%s\" must include a host\n", proxy_uri);
        goto on_error;
    }

    settings->host = aws_mem_calloc(allocator, host->len + 1, sizeof(char));
    if (!settings->host) {
        fprintf(stderr, "Failed to allocate memory for proxy host\n");
        goto on_error;
    }
    memcpy(settings->host, host->ptr, host->len);
    settings->host[host->len] = '\0';

    uint32_t parsed_port = aws_uri_port(&uri);
    if (parsed_port == 0) {
        parsed_port = 1080;
    }
    if (parsed_port > UINT16_MAX) {
        fprintf(stderr, "Proxy port %" PRIu32 " exceeds uint16_t range\n", parsed_port);
        goto on_error;
    }
    settings->port = (uint16_t)parsed_port;

    if (uri.user.len > 0) {
        settings->username = aws_mem_calloc(allocator, uri.user.len + 1, sizeof(char));
        if (!settings->username) {
            fprintf(stderr, "Failed to allocate memory for proxy username\n");
            goto on_error;
        }
        memcpy(settings->username, uri.user.ptr, uri.user.len);
        settings->username[uri.user.len] = '\0';
    }

    if (uri.password.len > 0) {
        settings->password = aws_mem_calloc(allocator, uri.password.len + 1, sizeof(char));
        if (!settings->password) {
            fprintf(stderr, "Failed to allocate memory for proxy password\n");
            goto on_error;
        }
        memcpy(settings->password, uri.password.ptr, uri.password.len);
        settings->password[uri.password.len] = '\0';
    }

    aws_uri_clean_up(&uri);
    return AWS_OP_SUCCESS;

on_error:
    aws_uri_clean_up(&uri);
    s_socks5_proxy_settings_clean_up(settings, allocator);
    return AWS_OP_ERR;
}

/* ANSI Color codes for terminal output */
#define COLOR_RED     "\x1b[31m"
#define COLOR_GREEN   "\x1b[32m"
#define COLOR_YELLOW  "\x1b[33m"
#define COLOR_BLUE    "\x1b[34m"
#define COLOR_RESET   "\x1b[0m"

/*
 * This example demonstrates how to establish an MQTT connection through a SOCKS5 proxy
 * or directly to the MQTT broker. It supports various features including:
 * 
 * - MQTT connection via SOCKS5 proxy or direct connection
 * - TLS support for secure MQTT (MQTTS)
 * - WebSocket transport support (MQTT over WebSockets)
 * - Basic MQTT operations (subscribe, publish, receive)
 * - SOCKS5 proxy authentication
 *
 * The various transport options can be combined to support:
 * - Direct MQTT
 * - MQTT over SOCKS5
 * - MQTT over TLS
 * - MQTT over TLS over SOCKS5
 * - MQTT over WebSocket
 * - MQTT over WebSocket over SOCKS5
 * - MQTT over WebSocket over TLS
 * - MQTT over WebSocket over TLS over SOCKS5
 */

struct app_ctx {
    struct aws_allocator *allocator;
    struct aws_event_loop_group *event_loop_group;
    struct aws_host_resolver *host_resolver;
    struct aws_client_bootstrap *bootstrap;
    
    struct aws_mqtt5_client *mqtt_client;
    struct aws_mqtt5_client_connection *mqtt_connection;

    struct aws_mutex lock;
    struct aws_condition_variable signal;
    
    /* MQTT broker settings */
    const char *broker_host;
    uint16_t broker_port;
    
    /* MQTT client settings */
    const char *client_id;
    bool clean_session;
    uint16_t keep_alive_secs;
    
    /* SOCKS5 proxy settings */
    struct socks5_proxy_settings proxy;
    
    /* WebSocket settings */
    const char *ws_path;
    
    /* Certificate settings */
    const char *cert_file;
    const char *key_file;
    const char *ca_file;
    const char *ca_dir;
    
    /* Topic settings */
    const char *pub_topic;
    const char *sub_topic;
    const char *pub_message;
    
    /* Connection options */
    bool use_proxy;
    bool use_tls;
    bool use_websocket;
    bool verbose;
    
    /* State tracking */
    bool connection_complete;
    int connection_error_code;
    bool subscription_complete;
    int subscription_error_code;
    bool publish_complete;
    int publish_error_code;
    bool message_received;
    
    /* Packet IDs */
    uint16_t sub_packet_id;
    uint16_t pub_packet_id;
    
    /* Message buffer */
    struct aws_byte_buf received_message;
};


static void s_aws_mqtt5_transform_websocket_handshake_fn(
    struct aws_http_message *request,
    void *user_data,
    aws_mqtt5_transform_websocket_handshake_complete_fn *complete_fn,
    void *complete_ctx) {

    (void)user_data;

    (*complete_fn)(request, AWS_ERROR_SUCCESS, complete_ctx);
}

static bool s_subscription_completed_pred(void *user_data) {
    struct app_ctx *ctx = user_data;
    return ctx->subscription_complete;
}

static bool s_message_received_pred(void *user_data) {
    struct app_ctx *ctx = user_data;
    return ctx->message_received;
}

static void s_usage(int exit_code) {
    fprintf(stderr, "usage: mqtt_socks5_example [options]\n");
    fprintf(stderr, " --broker-host HOST: MQTT broker hostname (default: test.mosquitto.org)\n");
    fprintf(stderr, " --broker-port PORT: MQTT broker port (default: 1883 for MQTT, 8883 for MQTTS, 8080/8443 for WebSockets)\n");
    fprintf(stderr, " --client-id ID: MQTT client ID (default: aws-mqtt-socks5-example)\n");
    fprintf(stderr, " --clean-session: Use clean session (default: true)\n");
    fprintf(stderr, " --keep-alive SECS: MQTT keep-alive in seconds (default: 60)\n");
    
    fprintf(
        stderr,
        " --proxy URL: SOCKS5 proxy URI (socks5h://... for proxy DNS, socks5://... for local DNS)\n");
    
    fprintf(stderr, " --cert FILE: Client certificate file path (PEM format)\n");
    fprintf(stderr, " --key FILE: Private key file path (PEM format)\n");
    fprintf(stderr, " --ca-file FILE: CA certificate file path (PEM format)\n");
    fprintf(stderr, " --ca-dir DIR: Directory containing CA certificates\n");
    
    fprintf(stderr, " --ws-path PATH: WebSocket path (default: /mqtt)\n");
    
    fprintf(stderr, " --pub-topic TOPIC: Topic to publish to (default: test/mqtt-socks5-example)\n");
    fprintf(stderr, " --sub-topic TOPIC: Topic to subscribe to (default: same as pub-topic)\n");
    fprintf(stderr, " --message MSG: Message to publish (default: \"Hello from MQTT SOCKS5 example\")\n");
    fprintf(stderr, " --websocket: Use WebSockets for transport\n");
    fprintf(stderr, " --verbose: Print detailed logging\n");
    fprintf(stderr, " --help: Display this message and exit\n");
    
    exit(exit_code);
}

static struct aws_cli_option s_long_options[] = {
    {"broker-host", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'b'},
    {"broker-port", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'p'},
    {"client-id", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'i'},
    {"clean-session", AWS_CLI_OPTIONS_NO_ARGUMENT, NULL, 'c'},
    {"keep-alive", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'k'},
    
    {"proxy", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'x'},
    
    {"ws-path", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'P'},
    
    {"cert", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'C'},
    {"key", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'K'},
    {"ca-file", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'A'},
    {"ca-dir", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'D'},
    
    {"pub-topic", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 't'},
    {"sub-topic", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 's'},
    {"message", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'm'},
    {"websocket", AWS_CLI_OPTIONS_NO_ARGUMENT, NULL, 'W'},
    {"verbose", AWS_CLI_OPTIONS_NO_ARGUMENT, NULL, 'v'},
    {"help", AWS_CLI_OPTIONS_NO_ARGUMENT, NULL, 'h'},
    {NULL, 0, NULL, 0},
};

static void s_parse_options(int argc, char **argv, struct app_ctx *ctx) {
    /* Default values */
    ctx->broker_host = "test.mosquitto.org";
    ctx->broker_port = 0;  /* Will be set later based on options */
    ctx->client_id = "aws-mqtt-socks5-example";
    ctx->clean_session = true;
    ctx->keep_alive_secs = 60;
    
    ctx->cert_file = NULL;
    ctx->key_file = NULL;
    ctx->ca_file = NULL;
    ctx->ca_dir = NULL;
    
    ctx->ws_path = "/mqtt";
    
    ctx->pub_topic = "test/mqtt-socks5-example";
    ctx->sub_topic = NULL;  /* Default to same as pub_topic */
    ctx->pub_message = "Hello from MQTT SOCKS5 example";
    
    ctx->use_proxy = false;
    ctx->use_tls = false;
    ctx->use_websocket = false;
    ctx->verbose = false;

    while (true) {
        int option_index = 0;
        int c = aws_cli_getopt_long(argc, argv, "b:p:i:ck:x:P:C:K:A:D:t:s:m:Wvh", 
                                    s_long_options, &option_index);
        if (c == -1) {
            break;
        }

        switch (c) {
            case 'b':
                ctx->broker_host = aws_cli_optarg;
                break;
            case 'p':
                ctx->broker_port = (uint16_t)atoi(aws_cli_optarg);
                break;
            case 'i':
                ctx->client_id = aws_cli_optarg;
                break;
            case 'c':
                ctx->clean_session = true;
                break;
            case 'k':
                ctx->keep_alive_secs = (uint16_t)atoi(aws_cli_optarg);
                break;
                
            case 'x':
                if (s_socks5_proxy_settings_init_from_uri(&ctx->proxy, ctx->allocator, aws_cli_optarg)) {
                    s_usage(1);
                }
                ctx->use_proxy = true;
                break;
                
            case 'P':
                ctx->ws_path = aws_cli_optarg;
                break;
            
            case 'C':
                ctx->cert_file = aws_cli_optarg;
                break;
            case 'K':
                ctx->key_file = aws_cli_optarg;
                break;
            case 'A':
                ctx->ca_file = aws_cli_optarg;
                break;
            case 'D':
                ctx->ca_dir = aws_cli_optarg;
                break;
                
            case 't':
                ctx->pub_topic = aws_cli_optarg;
                break;
            case 's':
                ctx->sub_topic = aws_cli_optarg;
                break;
            case 'm':
                ctx->pub_message = aws_cli_optarg;
                break;
            case 'W':
                ctx->use_websocket = true;
                break;
            case 'v':
                ctx->verbose = true;
                break;
            case 'h':
                s_usage(0);
                break;
            default:
                fprintf(stderr, "Unknown option: %c\n", c);
                s_usage(1);
                break;
        }
    }

    if (!ctx->use_tls) {
        ctx->use_tls = ctx->ca_file || ctx->cert_file || ctx->key_file;
    }
    
    /* If sub_topic not specified, use the same as pub_topic */
    if (ctx->sub_topic == NULL) {
        ctx->sub_topic = ctx->pub_topic;
    }
    
    /* Set default port if not specified */
    if (ctx->broker_port == 0) {
        if (ctx->use_websocket) {
            ctx->broker_port = ctx->use_tls ? 443 : 80;
        } else {
            ctx->broker_port = ctx->use_tls ? 8883 : 1883;
        }
    }
}

/* Structure for user data passed to the socket channel creation callback */
static void s_app_ctx_init(struct app_ctx *ctx, struct aws_allocator *allocator) {
    AWS_ZERO_STRUCT(*ctx);
    ctx->allocator = allocator;
    aws_mutex_init(&ctx->lock);
    aws_condition_variable_init(&ctx->signal);
    aws_byte_buf_init(&ctx->received_message, allocator, 1024); /* Initial buffer capacity */
}

static void s_app_ctx_clean_up(struct app_ctx *ctx) {
    if (ctx->mqtt_connection) {
        //aws_mqtt5_client_connection_release(ctx->mqtt_connection);
    }

    if (ctx->mqtt_client) {
        //aws_mqtt5_client_release(ctx->mqtt_client);
    }
    
    if (ctx->bootstrap) {
        aws_client_bootstrap_release(ctx->bootstrap);
    }
    
    if (ctx->host_resolver) {
        aws_host_resolver_release(ctx->host_resolver);
    }
    
    if (ctx->event_loop_group) {
        aws_event_loop_group_release(ctx->event_loop_group);
    }
    
    aws_byte_buf_clean_up(&ctx->received_message);
    s_socks5_proxy_settings_clean_up(&ctx->proxy, ctx->allocator);
    aws_condition_variable_clean_up(&ctx->signal);
    aws_mutex_clean_up(&ctx->lock);
}

/* Define a lifecycle event handler that signals main thread to exit on connection failure */
static void s_lifecycle_event_handler(const struct aws_mqtt5_client_lifecycle_event *event) {
    struct app_ctx *ctx = (struct app_ctx *)event->user_data;
    if (event->event_type == AWS_MQTT5_CLET_CONNECTION_SUCCESS) {
        printf("MQTT5 connection established successfully.\n");
            aws_mutex_lock(&ctx->lock);
            ctx->connection_complete = true;
            aws_condition_variable_notify_one(&ctx->signal);
            aws_mutex_unlock(&ctx->lock);
    } else if (event->event_type == AWS_MQTT5_CLET_CONNECTION_FAILURE) {
        fprintf(stderr, "MQTT5 connection failed: %s\n", aws_error_debug_str(event->error_code));
        if (ctx) {
            aws_mutex_lock(&ctx->lock);
            ctx->connection_complete = true;
            ctx->connection_error_code = event->error_code;
            aws_condition_variable_notify_one(&ctx->signal);
            aws_mutex_unlock(&ctx->lock);
        }
    } else if (event->event_type == AWS_MQTT5_CLET_DISCONNECTION) {
        fprintf(stderr, "MQTT5 client disconnected: %s\n", aws_error_debug_str(event->error_code));
    }
}

/* Define a publish received handler */
static void s_publish_received_handler(const struct aws_mqtt5_packet_publish_view *publish, void *user_data) {
    printf("Message received on topic '%.*s': %.*s\n",
           (int)publish->topic.len, publish->topic.ptr,
           (int)publish->payload.len, publish->payload.ptr);

    struct app_ctx *ctx = user_data;
    if (ctx == NULL) {
        return;
    }

    aws_mutex_lock(&ctx->lock);
    ctx->message_received = true;
    aws_condition_variable_notify_one(&ctx->signal);
    aws_mutex_unlock(&ctx->lock);
}

static void s_on_subscribe_complete(
    const struct aws_mqtt5_packet_suback_view *suback,
    int error_code,
    void *user_data) {

    (void)suback;

    struct app_ctx *ctx = user_data;
    if (ctx == NULL) {
        return;
    }

    aws_mutex_lock(&ctx->lock);
    ctx->subscription_complete = true;
    ctx->subscription_error_code = error_code;
    aws_condition_variable_notify_one(&ctx->signal);
    aws_mutex_unlock(&ctx->lock);
}

// Subscribe to a topic
static int s_subscribe_to_topic(struct app_ctx *ctx) {
    if (!ctx->mqtt_client) {
        fprintf(stderr, "Cannot subscribe: No active MQTT client\n");
        return AWS_OP_ERR;
    }

    struct aws_mqtt5_subscription_view subscription = {
        .topic_filter = aws_byte_cursor_from_c_str(ctx->sub_topic),
        .qos = AWS_MQTT5_QOS_AT_LEAST_ONCE,
    };

    struct aws_mqtt5_packet_subscribe_view subscribe_view;
    AWS_ZERO_STRUCT(subscribe_view);
    subscribe_view.subscriptions = &subscription;
    subscribe_view.subscription_count = 1;

    struct aws_mqtt5_subscribe_completion_options completion_options;
    AWS_ZERO_STRUCT(completion_options);
    completion_options.completion_callback = s_on_subscribe_complete;
    completion_options.completion_user_data = ctx;

    aws_mutex_lock(&ctx->lock);
    ctx->subscription_complete = false;
    ctx->subscription_error_code = AWS_ERROR_SUCCESS;
    aws_mutex_unlock(&ctx->lock);

    if (aws_mqtt5_client_subscribe(ctx->mqtt_client, &subscribe_view, &completion_options)) {
        fprintf(stderr, COLOR_RED "Failed to subscribe: %s" COLOR_RESET "\n", aws_error_debug_str(aws_last_error()));
        return AWS_OP_ERR;
    }

    if (ctx->verbose) {
        printf("Subscription request sent for topic: %s\n", ctx->sub_topic);
    }

    aws_mutex_lock(&ctx->lock);
    int wait_result = aws_condition_variable_wait_pred(&ctx->signal, &ctx->lock, s_subscription_completed_pred, ctx);
    int sub_error_code = ctx->subscription_error_code;
    aws_mutex_unlock(&ctx->lock);

    if (wait_result != AWS_OP_SUCCESS) {
        fprintf(
            stderr,
            COLOR_RED "Subscription wait failed: %s" COLOR_RESET "\n",
            aws_error_debug_str(aws_last_error()));
        return AWS_OP_ERR;
    }

    if (sub_error_code != AWS_ERROR_SUCCESS) {
        fprintf(
            stderr,
            COLOR_RED "Subscription failed: %s" COLOR_RESET "\n",
            aws_error_debug_str(sub_error_code));
        return AWS_OP_ERR;
    }

    if (ctx->verbose) {
        printf("Subscription acknowledged by broker\n");
    }

    return AWS_OP_SUCCESS;
}

// Publish a message
static int s_publish_message(struct app_ctx *ctx) {
    if (!ctx->mqtt_client) {
        fprintf(stderr, "Cannot publish: No active MQTT client\n");
        return AWS_OP_ERR;
    }

    struct aws_mqtt5_packet_publish_view publish_view = {
        .topic = aws_byte_cursor_from_c_str(ctx->pub_topic),
        .payload = aws_byte_cursor_from_c_str(ctx->pub_message),
        .qos = AWS_MQTT5_QOS_AT_LEAST_ONCE,
        .retain = false,
    };

    if (aws_mqtt5_client_publish(ctx->mqtt_client, &publish_view, NULL)) {
        fprintf(stderr, COLOR_RED "Failed to publish: %s" COLOR_RESET "\n", aws_error_debug_str(aws_last_error()));
        return AWS_OP_ERR;
    }

    if (ctx->verbose) {
        printf("Publish request sent to topic: %s\n", ctx->pub_topic);
    }

    return AWS_OP_SUCCESS;
}

int main(int argc, char **argv) {

    int main_error_code = 0;
    bool socks5_options_valid = false;

    struct aws_allocator *allocator = aws_default_allocator();
    aws_common_library_init(allocator);
    aws_io_library_init(allocator);
    aws_mqtt_library_init(allocator);

    struct aws_tls_ctx *tls_ctx = NULL;
    struct app_ctx app_ctx;
    s_app_ctx_init(&app_ctx, allocator);

    /* Parse command line arguments */
    s_parse_options(argc, argv, &app_ctx);

    /* Initialize AWS CRT logger */
    struct aws_logger logger;
    struct aws_logger_standard_options logger_options = {
        /* Force debug level output for this run to diagnose the TLS issue */
        .level = app_ctx.verbose ? AWS_LL_TRACE : AWS_LL_ERROR,
        .file = stderr,
    };

    bool logger_initialized = false;
    if (aws_logger_init_standard(&logger, allocator, &logger_options) == AWS_OP_SUCCESS) {
        aws_logger_set(&logger);
        logger_initialized = true;

        if (app_ctx.verbose) {
            printf("Verbose mode enabled, using TRACE log level\n");
        }
    } else {
        fprintf(stderr, "[WARN] Failed to initialize AWS logger, logs will not be shown.\n");
    }

    /* Log the configuration */
    printf("MQTT%s connection to %s:%d\n", 
           app_ctx.use_tls ? "S" : "",
           app_ctx.broker_host, 
           app_ctx.broker_port);

    printf("Client ID: %s\n", app_ctx.client_id);
    printf("Clean session: %s\n", app_ctx.clean_session ? "yes" : "no");
    printf("Keep alive: %d seconds\n", app_ctx.keep_alive_secs);

    if (app_ctx.use_websocket) {
        printf("Using WebSocket transport\n");
    }

    if (app_ctx.use_proxy && app_ctx.proxy.host) {
        printf(
            "Using SOCKS5 proxy at %s:%" PRIu16 " (%s DNS)\n",
            app_ctx.proxy.host,
            app_ctx.proxy.port,
            app_ctx.proxy.resolve_host_with_proxy ? "proxy" : "client-side");
        if (app_ctx.proxy.username) {
            printf("With proxy authentication: username=%s\n", app_ctx.proxy.username);
        }
    } else {
        printf("Using direct connection (no proxy)\n");
    }

    /* Create event loop group */
    app_ctx.event_loop_group = aws_event_loop_group_new_default(allocator, 1, NULL);
    if (!app_ctx.event_loop_group) {
        fprintf(stderr, "Failed to create event loop group: %s\n", aws_error_debug_str(aws_last_error()));
        main_error_code = 1;
        goto cleanup;
    }

    /* Create host resolver */
    struct aws_host_resolver_default_options resolver_options = {
        .max_entries = 8,
        .el_group = app_ctx.event_loop_group
    };
    app_ctx.host_resolver = aws_host_resolver_new_default(allocator, &resolver_options);
    if (!app_ctx.host_resolver) {
        fprintf(stderr, "Failed to create host resolver: %s\n", aws_error_debug_str(aws_last_error()));
        main_error_code = 2;
        goto cleanup;
    }

    /* Create client bootstrap */
    struct aws_client_bootstrap_options bootstrap_options = {
        .event_loop_group = app_ctx.event_loop_group,
        .host_resolver = app_ctx.host_resolver
    };
    app_ctx.bootstrap = aws_client_bootstrap_new(allocator, &bootstrap_options);
    if (!app_ctx.bootstrap) {
        fprintf(stderr, "Failed to create client bootstrap: %s\n", aws_error_debug_str(aws_last_error()));
        main_error_code = 3;
        goto cleanup;
    }

    /* TLS configuration copied from mqtt5canary */
    struct aws_tls_connection_options tls_connection_options;
    AWS_ZERO_STRUCT(tls_connection_options);
    struct aws_socks5_proxy_options socks5_options;
    AWS_ZERO_STRUCT(socks5_options);

    if (app_ctx.use_tls) {
        struct aws_byte_cursor broker_host_cursor = aws_byte_cursor_from_c_str(app_ctx.broker_host);
        struct aws_tls_ctx_options tls_ctx_options;
        AWS_ZERO_STRUCT(tls_ctx_options);

        int tls_result = AWS_OP_SUCCESS;
        if (app_ctx.cert_file && app_ctx.key_file) {
            tls_result = aws_tls_ctx_options_init_client_mtls_from_path(
                &tls_ctx_options, allocator, app_ctx.cert_file, app_ctx.key_file);
        } else {
            aws_tls_ctx_options_init_default_client(&tls_ctx_options, allocator);
        }

        if (tls_result != AWS_OP_SUCCESS) {
            fprintf(
                stderr,
                "Failed to initialize TLS context options: %s\n",
                aws_error_debug_str(aws_last_error()));
            main_error_code = 9;
            goto cleanup;
        }

        if ((app_ctx.ca_dir || app_ctx.ca_file) &&
            aws_tls_ctx_options_override_default_trust_store_from_path(
                &tls_ctx_options, app_ctx.ca_dir, app_ctx.ca_file)) {
            fprintf(
                stderr,
                "Failed to configure TLS trust store (dir=%s, file=%s): %s\n",
                app_ctx.ca_dir ? app_ctx.ca_dir : "NULL",
                app_ctx.ca_file ? app_ctx.ca_file : "NULL",
                aws_error_debug_str(aws_last_error()));
            aws_tls_ctx_options_clean_up(&tls_ctx_options);
            main_error_code = 10;
            goto cleanup;
        }

        if (aws_tls_ctx_options_set_alpn_list(&tls_ctx_options, "x-amzn-mqtt-ca")) {
            fprintf(
                stderr,
                "Failed to set TLS ALPN list: %s\n",
                aws_error_debug_str(aws_last_error()));
            aws_tls_ctx_options_clean_up(&tls_ctx_options);
            main_error_code = 11;
            goto cleanup;
        }

        tls_ctx = aws_tls_client_ctx_new(allocator, &tls_ctx_options);
        aws_tls_ctx_options_clean_up(&tls_ctx_options);
        if (!tls_ctx) {
            fprintf(stderr, "Failed to create TLS context: %s\n", aws_error_debug_str(aws_last_error()));
            main_error_code = 12;
            goto cleanup;
        }

        aws_tls_connection_options_init_from_ctx(&tls_connection_options, tls_ctx);
        if (aws_tls_connection_options_set_server_name(&tls_connection_options, allocator, &broker_host_cursor)) {
            fprintf(
                stderr,
                "Failed to set TLS server name (%s): %s\n",
                app_ctx.broker_host,
                aws_error_debug_str(aws_last_error()));
            main_error_code = 13;
            goto cleanup;
        }
    }

    /* Correct socks5_options */
    if (app_ctx.use_proxy) {
        if (!app_ctx.proxy.host) {
            fprintf(stderr, "Proxy URI was requested but no host was parsed.\n");
            main_error_code = 14;
            goto cleanup;
        }

        struct aws_byte_cursor proxy_host = aws_byte_cursor_from_c_str(app_ctx.proxy.host);
        if (aws_socks5_proxy_options_init(&socks5_options, allocator, proxy_host, app_ctx.proxy.port) != AWS_OP_SUCCESS) {
            fprintf(
                stderr,
                "Failed to initialize SOCKS5 proxy options: %s\n",
                aws_error_debug_str(aws_last_error()));
            main_error_code = 14;
            goto cleanup;
        }
        aws_socks5_proxy_options_set_host_resolution_mode(
            &socks5_options,
            app_ctx.proxy.resolve_host_with_proxy ? AWS_SOCKS5_HOST_RESOLUTION_PROXY
                                                  : AWS_SOCKS5_HOST_RESOLUTION_CLIENT);

        if (app_ctx.proxy.username && app_ctx.proxy.password) {
            struct aws_byte_cursor username = aws_byte_cursor_from_c_str(app_ctx.proxy.username);
            struct aws_byte_cursor password = aws_byte_cursor_from_c_str(app_ctx.proxy.password);
            if (aws_socks5_proxy_options_set_auth(&socks5_options, allocator, username, password) != AWS_OP_SUCCESS) {
                fprintf(
                    stderr,
                    "Failed to set SOCKS5 auth: %s\n",
                    aws_error_debug_str(aws_last_error()));
                main_error_code = 14;
                goto cleanup;
            }
        }

        socks5_options_valid = true;
    }

    
    /* Set up socket options for the connection */
    struct aws_socket_options socket_options = {
        .type = AWS_SOCKET_STREAM,
        .domain = AWS_SOCKET_IPV4, /* Use IPv4 for better compatibility */
        .connect_timeout_ms = 10000, /* Allow enough time for connection */
    };
    
    /* Define mqtt5_options */
    struct aws_mqtt5_client_options mqtt5_options;
    AWS_ZERO_STRUCT(mqtt5_options);
    mqtt5_options.host_name = aws_byte_cursor_from_c_str(app_ctx.broker_host);
    mqtt5_options.port = app_ctx.broker_port;
    mqtt5_options.bootstrap = app_ctx.bootstrap;
    mqtt5_options.socket_options = &socket_options;
    mqtt5_options.tls_options = app_ctx.use_tls ? &tls_connection_options : NULL;
    mqtt5_options.lifecycle_event_handler = s_lifecycle_event_handler;
    mqtt5_options.lifecycle_event_handler_user_data = &app_ctx;
    mqtt5_options.publish_received_handler = s_publish_received_handler;
    mqtt5_options.publish_received_handler_user_data = &app_ctx;
    mqtt5_options.socks5_proxy_options = socks5_options_valid ? &socks5_options : NULL;

    aws_mqtt5_transform_websocket_handshake_fn *websocket_handshake_transform = NULL;
    void *websocket_handshake_transform_user_data = NULL;

    if (app_ctx.use_websocket) {
            websocket_handshake_transform = &s_aws_mqtt5_transform_websocket_handshake_fn;
    }
    mqtt5_options.websocket_handshake_transform = websocket_handshake_transform;
    mqtt5_options.websocket_handshake_transform_user_data = websocket_handshake_transform_user_data;

    /* Correct the initialization of connect options */
    struct aws_mqtt5_packet_connect_view connect_view = {
        .client_id = aws_byte_cursor_from_c_str(app_ctx.client_id),
        .keep_alive_interval_seconds = app_ctx.keep_alive_secs,
        .clean_start = app_ctx.clean_session,
        .will_delay_interval_seconds = 0, // Default value
        .session_expiry_interval_seconds = 0, // Default value
    };

    /* Update mqtt5_options to include the corrected connect options */
    mqtt5_options.connect_options = &connect_view;

    /* Create MQTT5 client */
    app_ctx.mqtt_client = aws_mqtt5_client_new(allocator, &mqtt5_options);
    if (!app_ctx.mqtt_client) {
        fprintf(stderr, "Failed to create MQTT 5 client: %s\n", aws_error_debug_str(aws_last_error()));
        main_error_code = 4;
        goto cleanup;
    }


    if (aws_mqtt5_client_start(app_ctx.mqtt_client) != AWS_OP_SUCCESS) {
        fprintf(stderr, "Failed to start MQTT 5 client: %s\n", aws_error_debug_str(aws_last_error()));
        main_error_code = 5;
        goto cleanup;
    }

    printf("MQTT 5 client started successfully\n");

    // Wait for connection to complete (success or failure)
    aws_mutex_lock(&app_ctx.lock);
    app_ctx.connection_complete = false;
    app_ctx.connection_error_code = AWS_ERROR_SUCCESS;
    while (!app_ctx.connection_complete) {
        aws_condition_variable_wait(&app_ctx.signal, &app_ctx.lock);
    }
    int connection_error = app_ctx.connection_error_code;
    aws_mutex_unlock(&app_ctx.lock);

    if (connection_error != AWS_ERROR_SUCCESS) {
        fprintf(stderr, COLOR_RED "Exiting due to connection failure.\n" COLOR_RESET);
        main_error_code = 6;
        goto cleanup;
    }

    // Subscribe to topic
    printf("Subscribing to topic: %s\n", app_ctx.sub_topic);
    if (s_subscribe_to_topic(&app_ctx) != AWS_OP_SUCCESS) {
        main_error_code = 21;
        goto cleanup;
    }

    aws_mutex_lock(&app_ctx.lock);
    app_ctx.message_received = false;
    aws_mutex_unlock(&app_ctx.lock);

    // Publish a message
    printf("Publishing message to topic: %s\n", app_ctx.pub_topic);
    if (s_publish_message(&app_ctx) != AWS_OP_SUCCESS) {
        main_error_code = 23;
        goto cleanup;
    }

    // Wait for message reception or timeout
    printf("Waiting to receive messages...\n");
    aws_mutex_lock(&app_ctx.lock);
    uint64_t wait_time_ns = aws_timestamp_convert(5, AWS_TIMESTAMP_SECS, AWS_TIMESTAMP_NANOS, NULL);
    int wait_for_result = aws_condition_variable_wait_for_pred(
        &app_ctx.signal, &app_ctx.lock, (int64_t)wait_time_ns, s_message_received_pred, &app_ctx);
    bool received = app_ctx.message_received;
    aws_mutex_unlock(&app_ctx.lock);
    printf("Done waiting for messages.\n");

    int wait_error = AWS_ERROR_SUCCESS;
    if (wait_for_result != AWS_OP_SUCCESS) {
        wait_error = aws_last_error();
        if (wait_error != AWS_ERROR_COND_VARIABLE_TIMED_OUT) {
            fprintf(
                stderr,
                COLOR_RED "Error while waiting for publish response: %s" COLOR_RESET "\n",
                aws_error_debug_str(wait_error));
            main_error_code = 24;
            goto cleanup;
        }
    }

    if (!received) {
        fprintf(stderr, COLOR_YELLOW "Timed out waiting for message.\n" COLOR_RESET);
    } else {
        printf("Message received and processed.\n");
    }

cleanup:
    aws_mqtt5_client_release(app_ctx.mqtt_client);

    if (app_ctx.use_tls) {
        aws_tls_connection_options_clean_up(&tls_connection_options);
        if (tls_ctx != NULL) {
            aws_tls_ctx_release(tls_ctx);
        }
    }
    if (socks5_options_valid) {
        aws_socks5_proxy_options_clean_up(&socks5_options);
    }

    aws_tls_connection_options_clean_up(&tls_connection_options);
    s_app_ctx_clean_up(&app_ctx);    
    aws_thread_join_all_managed();

    aws_mqtt_library_clean_up();
    aws_io_library_clean_up();
    aws_common_library_clean_up();
    if (logger_initialized) {
        aws_logger_clean_up(&logger);
    }
    return main_error_code;
}
