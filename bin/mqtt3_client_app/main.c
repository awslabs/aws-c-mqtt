/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/command_line_parser.h>
#include <aws/common/condition_variable.h>
#include <aws/common/mutex.h>
#include <aws/common/clock.h>
#include <aws/common/string.h>
#include <aws/common/error.h>
#include <aws/common/allocator.h>
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
    
    struct aws_mqtt_client *mqtt_client;
    struct aws_mqtt_client_connection *mqtt_connection;
    
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
/* Helper function to print SOCKS5 proxy configuration details */
static void print_socks5_config(struct aws_socks5_proxy_options *socks5_options) {
    printf("SOCKS5 proxy configuration:\n");
    struct aws_string * proxy_host = socks5_options->host;
    printf("  Proxy host: %.*s\n", (int)proxy_host->len, (const char *)proxy_host->bytes);
    printf("  Proxy port: %u\n", socks5_options->port);
    size_t username_len = socks5_options->username->len;
    size_t password_len = socks5_options->password->len;
    if (username_len > 0 && password_len > 0) {
        printf("  Authentication: Username/Password\n");
    } else {
        printf("  Authentication: None\n");
    }
}

/* Predicate functions for condition variables */
static bool s_connection_completed_predicate(void *arg) {
    struct app_ctx *ctx = arg;
    return ctx->connection_complete;
}

static bool s_subscription_completed_predicate(void *arg) {
    struct app_ctx *ctx = arg;
    return ctx->subscription_complete;
}

static bool s_publish_completed_predicate(void *arg) {
    struct app_ctx *ctx = arg;
    return ctx->publish_complete;
}

static void s_app_ctx_init(struct app_ctx *ctx, struct aws_allocator *allocator) {
    AWS_ZERO_STRUCT(*ctx);
    ctx->allocator = allocator;
    aws_mutex_init(&ctx->lock);
    aws_condition_variable_init(&ctx->signal);
    aws_byte_buf_init(&ctx->received_message, allocator, 1024); /* Initial buffer capacity */
}

static void s_app_ctx_clean_up(struct app_ctx *ctx) {
    if (ctx->mqtt_connection) {
        aws_mqtt_client_connection_release(ctx->mqtt_connection);
    }
    
    if (ctx->mqtt_client) {
        aws_mqtt_client_release(ctx->mqtt_client);
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

/* MQTT Connection callbacks */
static void s_on_connection_complete(
    struct aws_mqtt_client_connection *connection,
    int error_code,
    enum aws_mqtt_connect_return_code return_code,
    bool session_present,
    void *userdata) {
    
    struct app_ctx *ctx = userdata;
    (void)connection;
    (void)return_code;
    (void)session_present;
    
    aws_mutex_lock(&ctx->lock);
    
    ctx->connection_complete = true;
    ctx->connection_error_code = error_code;
    
    if (error_code == AWS_ERROR_SUCCESS) {
        if (ctx->verbose) {
            printf(COLOR_GREEN "MQTT connection established successfully" COLOR_RESET "\n");
            printf("Session present: %s\n", session_present ? "true" : "false");
        }
    } else {
        fprintf(stderr, COLOR_RED "MQTT connection failed: %s (error code: %d)" COLOR_RESET "\n", 
                aws_error_debug_str(error_code), error_code);
        fprintf(stderr, COLOR_YELLOW "Connection details: %s:%d (proxy: %s)" COLOR_RESET "\n", 
                ctx->broker_host, ctx->broker_port, 
                ctx->use_proxy ? "yes" : "no");
    }
    
    aws_condition_variable_notify_one(&ctx->signal);
    aws_mutex_unlock(&ctx->lock);
}

/* MQTT Subscription callbacks */
static void s_on_suback(
    struct aws_mqtt_client_connection *connection,
    uint16_t packet_id,
    const struct aws_byte_cursor *topic,
    enum aws_mqtt_qos qos,
    int error_code,
    void *userdata) {
    
    struct app_ctx *ctx = userdata;
    (void)connection;
    (void)topic;
    (void)qos;
    
    aws_mutex_lock(&ctx->lock);
    
    if (packet_id == ctx->sub_packet_id) {
        ctx->subscription_complete = true;
        ctx->subscription_error_code = error_code;
        
        if (ctx->verbose) {
            if (error_code == AWS_ERROR_SUCCESS) {
                printf(COLOR_GREEN "Subscription succeeded for topic: %.*s with QoS %d" COLOR_RESET "\n", 
                       (int)topic->len, topic->ptr, qos);
            } else {
                printf(COLOR_RED "Subscription failed: %s" COLOR_RESET "\n", aws_error_debug_str(error_code));
            }
        }
        
        aws_condition_variable_notify_one(&ctx->signal);
    }
    
    aws_mutex_unlock(&ctx->lock);
}

/* MQTT Message received callback */
static void s_on_publish_received(
    struct aws_mqtt_client_connection *connection,
    const struct aws_byte_cursor *topic,
    const struct aws_byte_cursor *payload,
    bool dup,
    enum aws_mqtt_qos qos,
    bool retain,
    void *userdata) {
    
    struct app_ctx *ctx = userdata;
    (void)connection;
    (void)dup;
    (void)qos;
    (void)retain;
    
    aws_mutex_lock(&ctx->lock);
    
    ctx->message_received = true;
    
    /* Clear previous message if any */
    aws_byte_buf_reset(&ctx->received_message, 0);
    
    /* Store the received message */
    aws_byte_buf_append_dynamic(&ctx->received_message, payload);
    
    if (ctx->verbose) {
        printf(COLOR_GREEN "Message received on topic '%.*s': %.*s" COLOR_RESET "\n", 
               (int)topic->len, topic->ptr,
               (int)payload->len, payload->ptr);
    }
    
    aws_mutex_unlock(&ctx->lock);
}

/* MQTT Publish completion callback */
static void s_on_publish_complete(
    struct aws_mqtt_client_connection *connection,
    uint16_t packet_id,
    int error_code,
    void *userdata) {
    
    struct app_ctx *ctx = userdata;
    (void)connection;
    
    aws_mutex_lock(&ctx->lock);
    
    if (packet_id == ctx->pub_packet_id) {
        ctx->publish_complete = true;
        ctx->publish_error_code = error_code;
        
        if (error_code == AWS_ERROR_SUCCESS) {
            if (ctx->verbose) {
                printf(COLOR_GREEN "Publish completed successfully" COLOR_RESET "\n");
            }
        } else {
            fprintf(stderr, COLOR_RED "Publish failed: %s" COLOR_RESET "\n", aws_error_debug_str(error_code));
        }
        
        aws_condition_variable_notify_one(&ctx->signal);
    }
    
    aws_mutex_unlock(&ctx->lock);
}

static int s_subscribe_to_topic(struct app_ctx *ctx) {
    if (!ctx->mqtt_connection) {
        fprintf(stderr, "Cannot subscribe: No active MQTT connection\n");
        return AWS_OP_ERR;
    }
    
    struct aws_byte_cursor topic_cur = aws_byte_cursor_from_c_str(ctx->sub_topic);
    
    /* Subscribe to the topic with QoS 1 */
    ctx->sub_packet_id = aws_mqtt_client_connection_subscribe(
        ctx->mqtt_connection,
        &topic_cur,
        AWS_MQTT_QOS_AT_LEAST_ONCE,
        s_on_publish_received,
        ctx,
        NULL,
        s_on_suback,
        ctx);
    
    if (ctx->sub_packet_id == 0) {
        fprintf(stderr, COLOR_RED "Failed to subscribe: %s" COLOR_RESET "\n", aws_error_debug_str(aws_last_error()));
        return AWS_OP_ERR;
    }
    
    if (ctx->verbose) {
        printf("Subscription request sent for topic: %s\n", ctx->sub_topic);
    }
    
    return AWS_OP_SUCCESS;
}

static int s_publish_message(struct app_ctx *ctx) {
    if (!ctx->mqtt_connection) {
        fprintf(stderr, "Cannot publish: No active MQTT connection\n");
        return AWS_OP_ERR;
    }
    
    struct aws_byte_cursor topic_cur = aws_byte_cursor_from_c_str(ctx->pub_topic);
    struct aws_byte_cursor message_cur = aws_byte_cursor_from_c_str(ctx->pub_message);
    
    /* Publish with QoS 1 */
    ctx->pub_packet_id = aws_mqtt_client_connection_publish(
        ctx->mqtt_connection,
        &topic_cur,
        AWS_MQTT_QOS_AT_LEAST_ONCE,
        false, /* retain */
        &message_cur,
        s_on_publish_complete,
        ctx);
    
    if (ctx->pub_packet_id == 0) {
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

    struct aws_allocator *allocator = aws_default_allocator();
    aws_common_library_init(allocator);
    aws_io_library_init(allocator);
    aws_mqtt_library_init(allocator);

    struct app_ctx app_ctx;
    s_app_ctx_init(&app_ctx, allocator);

    /* Parse command line arguments */
    s_parse_options(argc, argv, &app_ctx);

    struct aws_tls_connection_options *tls_connection_options = NULL;
    struct aws_tls_ctx *tls_ctx = NULL;
    struct aws_socks5_proxy_options *socks5_options = NULL;

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

    /* Create MQTT client */
    app_ctx.mqtt_client = aws_mqtt_client_new(allocator, app_ctx.bootstrap);
    if (!app_ctx.mqtt_client) {
        fprintf(stderr, "Failed to create MQTT client: %s\n", aws_error_debug_str(aws_last_error()));
        main_error_code = 4;
        goto cleanup;
    }

    /* Create TLS connection options if using TLS */
    if (app_ctx.use_tls) {
        /* Initialize TLS context options */
        struct aws_tls_ctx_options tls_ctx_options;
        aws_tls_ctx_options_init_default_client(&tls_ctx_options, allocator);
        
        /* If certificate files are provided, use them */
        if (app_ctx.cert_file && app_ctx.key_file) {
            printf("Using certificate (%s) and key (%s) files\n", app_ctx.cert_file, app_ctx.key_file);
            
            /* Clean up any previous options and reinitialize with client certificates */
            aws_tls_ctx_options_clean_up(&tls_ctx_options);
            
        if (aws_tls_ctx_options_init_client_mtls_from_path(
            &tls_ctx_options, allocator, app_ctx.cert_file, app_ctx.key_file) != AWS_OP_SUCCESS) {
        int err = aws_last_error();
        fprintf(stderr, COLOR_RED "Failed to load client certificates: %s (%d)" COLOR_RESET "\n", 
            aws_error_debug_str(err), err);
        main_error_code = 5;
        goto cleanup;
        }
        printf(COLOR_GREEN "Successfully loaded client certificates" COLOR_RESET "\n");
        }
        
        /* If CA file is provided, override the default trust store */
        if (app_ctx.ca_file) {
            printf("Using CA certificate file: %s\n", app_ctx.ca_file);
            
            /* Print some debug info about the CA certificate file */
            FILE *ca_file = fopen(app_ctx.ca_file, "r");
            if (ca_file) {
                char buf[128];
                size_t bytes_read = fread(buf, 1, sizeof(buf) - 1, ca_file);
                buf[bytes_read] = '\0';
                printf("CA file exists and is readable. First bytes: %.40s...\n", buf);
                fclose(ca_file);
            } else {
                printf("WARNING: CA file cannot be opened\n");
                fprintf(stderr, "Error opening CA file: %s\n", strerror(errno));
                goto cleanup;
            }
            
            /* The AWS CRT AWS_IO_TLS_CTX_ERROR often means the certificate is malformed */
            printf("Setting CA certificate (file: %s, dir: %s)...\n", 
                   app_ctx.ca_file, app_ctx.ca_dir ? app_ctx.ca_dir : "NULL");
            
            /* Clean up any previous TLS context options before reconfiguring */
            aws_tls_ctx_options_clean_up(&tls_ctx_options);
            aws_tls_ctx_options_init_default_client(&tls_ctx_options, allocator);
            
            /* Method 1: Load CA certificate directly from PEM file content */
            /* Read CA file into memory first */
            FILE *cert_file = fopen(app_ctx.ca_file, "rb");
            if (!cert_file) {
                fprintf(stderr, "Failed to open CA certificate file: %s\n", strerror(errno));
                goto cleanup;
            }
            
            /* Get file size */
            fseek(cert_file, 0, SEEK_END);
            size_t file_size = ftell(cert_file);
            fseek(cert_file, 0, SEEK_SET);
            
            /* Allocate memory for certificate content */
            char *cert_content = aws_mem_acquire(allocator, file_size + 1);
            if (!cert_content) {
                fprintf(stderr, "Failed to allocate memory for certificate\n");
                fclose(cert_file);
                main_error_code = 6;
                goto cleanup;
            }
            
            /* Read certificate content */
            size_t bytes_read = fread(cert_content, 1, file_size, cert_file);
            fclose(cert_file);
            
            if (bytes_read != file_size) {
                fprintf(stderr, "Failed to read certificate file completely\n");
                aws_mem_release(allocator, cert_content);
                main_error_code = 7;
                goto cleanup;
            }
            
            cert_content[bytes_read] = '\0';
            
            /* Load CA certificate from memory */
            printf("Loading CA certificate from memory (size: %zu bytes)\n", bytes_read);
            
            struct aws_byte_cursor ca_file_cursor = aws_byte_cursor_from_array(cert_content, bytes_read);
            if (aws_tls_ctx_options_override_default_trust_store(&tls_ctx_options, &ca_file_cursor) != AWS_OP_SUCCESS) {
                int err = aws_last_error();
                fprintf(stderr, "Failed to load CA certificate directly: %s (%d)\n", 
                        aws_error_debug_str(err), err);

                /* Try alternative method: Using system path-based trust store override */
                aws_tls_ctx_options_clean_up(&tls_ctx_options);
                aws_tls_ctx_options_init_default_client(&tls_ctx_options, allocator);

                if (aws_tls_ctx_options_override_default_trust_store_from_path(
                        &tls_ctx_options, app_ctx.ca_file, app_ctx.ca_dir) != AWS_OP_SUCCESS) {
                    err = aws_last_error();
                    fprintf(stderr, "Failed to load CA certificate from path: %s (%d)\n", 
                            aws_error_debug_str(err), err);
                    main_error_code = 8;
                    goto cleanup;
                } else {
                    printf("Successfully loaded CA certificate from path\n");
                }
            } else {
                printf(COLOR_GREEN "Successfully loaded CA certificate directly" COLOR_RESET "\n");
            }
            
            /* Free the certificate content */
            aws_mem_release(allocator, cert_content);
            
            /* Always explicitly enable peer verification */
            aws_tls_ctx_options_set_verify_peer(&tls_ctx_options, true);
        } else {
            /* When no CA file specified, still ensure peer verification is enabled */
            printf("No CA certificate file specified, using system defaults\n");
            aws_tls_ctx_options_set_verify_peer(&tls_ctx_options, true);
        }
        
        /* Create a new TLS context */
        printf("Creating TLS context...\n");
        
        /* Set minimum TLS version to TLS 1.2 to avoid compatibility issues */
        tls_ctx_options.minimum_tls_version = AWS_IO_TLSv1_2;
        
    tls_ctx = aws_tls_client_ctx_new(allocator, &tls_ctx_options);
    if (!tls_ctx) {
        int err = aws_last_error();
        fprintf(stderr, COLOR_RED "Failed to create TLS context: %s (%d)" COLOR_RESET "\n", 
            aws_error_debug_str(err), err);
        aws_tls_ctx_options_clean_up(&tls_ctx_options);
        main_error_code = 9;
        goto cleanup;
    }
        
        printf(COLOR_GREEN "TLS context created successfully" COLOR_RESET "\n");
        
        /* Initialize TLS connection options */
        tls_connection_options = aws_mem_calloc(allocator, 1, sizeof(struct aws_tls_connection_options));
        if (!tls_connection_options) {
            fprintf(stderr, "Failed to allocate memory for TLS connection options\n");
            aws_tls_ctx_options_clean_up(&tls_ctx_options);
            aws_tls_ctx_release(tls_ctx);
            main_error_code = 10;
            goto cleanup;
        }
        
        /* Initialize TLS connection options from context */
        aws_tls_connection_options_init_from_ctx(tls_connection_options, tls_ctx);
        
        /* Set server name for SNI */
        struct aws_byte_cursor server_name = aws_byte_cursor_from_c_str(app_ctx.broker_host);
        if (aws_tls_connection_options_set_server_name(tls_connection_options, allocator, 
            &server_name) != AWS_OP_SUCCESS) {
            fprintf(stderr, "Failed to set server name: %s\n", aws_error_debug_str(aws_last_error()));
            main_error_code = 11;
            goto cleanup;
        }
        
        /* Set options for AWS IoT Core endpoints */
        if (strstr(app_ctx.broker_host, "iot.") && strstr(app_ctx.broker_host, ".amazonaws.com")) {
            /* In AWS CRT, we need to use the exact certificate that's registered in AWS IoT Core */
            printf("Connecting to AWS IoT Core endpoint: %s\n", app_ctx.broker_host);
            printf("Make sure the certificate is registered in AWS IoT Core for this endpoint\n");
            printf("The certificate should have an IoT policy attached that allows connect, publish, and subscribe\n");
            
            /* Override the default certificate path */
            if (app_ctx.verbose) {
                printf("Setting ALPN protocol to 'mqtt' for AWS IoT Core connection\n");
                printf("Note: ALPN protocol 'mqtt' would be applied for AWS IoT Core\n");
            }
        }
        
        printf("TLS enabled for connection to %s\n", app_ctx.broker_host);
    }
    
    /* Configure SOCKS5 proxy options if using proxy */
    
    if (app_ctx.use_proxy) {
        if (!app_ctx.proxy.host) {
            fprintf(stderr, "Proxy URI was requested but no host was parsed.\n");
            main_error_code = 12;
            goto cleanup;
        }

        printf("Configuring SOCKS5 proxy %s:%" PRIu16 "\n", app_ctx.proxy.host, app_ctx.proxy.port);

        /* Allocate and initialize the SOCKS5 options structure */
        socks5_options = aws_mem_calloc(allocator, 1, sizeof(struct aws_socks5_proxy_options));
        if (!socks5_options) {
            fprintf(stderr, "Failed to allocate memory for SOCKS5 proxy options\n");
            main_error_code = 12;
            goto cleanup;
        }

        /* Set up SOCKS5-specific options */
        struct aws_byte_cursor proxy_host = aws_byte_cursor_from_c_str(app_ctx.proxy.host);

        /* Use the standard AWS SOCKS5 initialization function */
        int result = aws_socks5_proxy_options_init(socks5_options, allocator, proxy_host, app_ctx.proxy.port);
        if (result != AWS_OP_SUCCESS) {
            int error_code = aws_last_error();
            fprintf(
                stderr,
                "Failed to initialize SOCKS5 proxy options: %s (code: %d)\n",
                aws_error_debug_str(error_code),
                error_code);
            main_error_code = 13;
            goto cleanup;
        }
        aws_socks5_proxy_options_set_host_resolution_mode(
            socks5_options,
            app_ctx.proxy.resolve_host_with_proxy ? AWS_SOCKS5_HOST_RESOLUTION_PROXY
                                                  : AWS_SOCKS5_HOST_RESOLUTION_CLIENT);

        /* Setup auth if provided */
        if (app_ctx.proxy.username && app_ctx.proxy.password) {
            struct aws_byte_cursor username = aws_byte_cursor_from_c_str(app_ctx.proxy.username);
            struct aws_byte_cursor password = aws_byte_cursor_from_c_str(app_ctx.proxy.password);
            if (aws_socks5_proxy_options_set_auth(socks5_options, allocator, username, password) != AWS_OP_SUCCESS) {
                int error_code = aws_last_error();
                fprintf(
                    stderr,
                    "Failed to set SOCKS5 auth: %s (code: %d)\n",
                    aws_error_debug_str(error_code),
                    error_code);
                main_error_code = 14;
                goto cleanup;
            }
        }

        /* Set connection timeout */
        socks5_options->connection_timeout_ms = 5000;

        if (app_ctx.verbose) {
            printf(
                "SOCKS5 proxy configured with target %s:%d%s (destination resolved by %s)\n",
                app_ctx.broker_host,
                app_ctx.broker_port,
                app_ctx.use_tls ? " (with TLS)" : "",
                app_ctx.proxy.resolve_host_with_proxy ? "proxy" : "client");
        }
    } else {
        /* Ensure we're using a direct connection with no proxy options */
        if (app_ctx.verbose) {
            printf("Using direct connection without SOCKS5 proxy\n");
        }
    }
    
    /* Create MQTT connection */
    app_ctx.mqtt_connection = aws_mqtt_client_connection_new(app_ctx.mqtt_client);
    if (!app_ctx.mqtt_connection) {
        fprintf(stderr, "Failed to create MQTT connection: %s\n", aws_error_debug_str(aws_last_error()));
        main_error_code = 16;
        goto cleanup;
    }

    /* Configure MQTT connection options */
    struct aws_mqtt_connection_options mqtt_conn_options = {
        .host_name = aws_byte_cursor_from_c_str(app_ctx.broker_host),
        .port = app_ctx.broker_port,
        .client_id = aws_byte_cursor_from_c_str(app_ctx.client_id),
        .keep_alive_time_secs = app_ctx.keep_alive_secs,
        .ping_timeout_ms = 5000,
        .protocol_operation_timeout_ms = 5000,
        .clean_session = app_ctx.clean_session,
        .on_connection_complete = s_on_connection_complete,
        .user_data = &app_ctx,
        .tls_options = app_ctx.use_tls ? tls_connection_options : NULL
    };
    
    /* Set up socket options for the connection */
    struct aws_socket_options socket_options = {
        .type = AWS_SOCKET_STREAM,
        .domain = AWS_SOCKET_IPV4, /* Use IPv4 for better compatibility */
        .connect_timeout_ms = 10000, /* Allow enough time for connection */
    };
    
    mqtt_conn_options.socket_options = &socket_options;
    
    /* Configure WebSocket transport if needed */
    if (app_ctx.use_websocket) {
        if (aws_mqtt_client_connection_use_websockets(
                app_ctx.mqtt_connection, NULL, NULL, NULL, NULL) != AWS_OP_SUCCESS) {
            fprintf(stderr, "Failed to configure WebSocket transport: %s\n", aws_error_debug_str(aws_last_error()));
            main_error_code = 17;
            goto cleanup;
        }

        printf("Using WebSocket transport with path '%s'\n", app_ctx.ws_path);
    }
    
    /* Configure proxy options */
    if (app_ctx.use_proxy && app_ctx.proxy.host) {
        printf(
            "Using SOCKS5 proxy at %s:%" PRIu16 " (%s DNS)\n",
            app_ctx.proxy.host,
            app_ctx.proxy.port,
            app_ctx.proxy.resolve_host_with_proxy ? "proxy" : "client-side");

        /* SOCKS5 options were already configured above */
        if (app_ctx.verbose) {
            printf("SOCKS5 proxy options have already been configured.\n");
        }
    } else {
        printf("Not using any proxy, connecting directly.\n");
        /* Ensure socks5_options is NULL for direct connections */
        if (socks5_options) {
            aws_socks5_proxy_options_clean_up(socks5_options);
            aws_mem_release(allocator, socks5_options);
            socks5_options = NULL;
        }
    }

    /* Connect to MQTT broker */
    printf("Connecting to MQTT broker %s:%d...\n", app_ctx.broker_host, app_ctx.broker_port);
    
    if (app_ctx.verbose) {
        printf("Connection details:\n");
        printf("  Transport: %s%s\n", 
               app_ctx.use_tls ? "TLS" : "TCP",
               app_ctx.use_websocket ? " over WebSocket" : "");
        printf("  Client ID: %s\n", app_ctx.client_id);
        printf("  Clean session: %s\n", app_ctx.clean_session ? "yes" : "no");
        if (app_ctx.use_proxy && app_ctx.proxy.host) {
            printf("  Via SOCKS5 proxy: %s:%" PRIu16 "\n", app_ctx.proxy.host, app_ctx.proxy.port);
        } else {
            printf("  Direct connection (no proxy)\n");
        }
    }
    
    int connect_result;
    if (app_ctx.use_proxy && app_ctx.proxy.host && socks5_options) {
        /* Using SOCKS5 proxy for MQTT connection */
        printf(
            "Connecting via SOCKS5 proxy %s:%" PRIu16 " to %s:%d\n",
            app_ctx.proxy.host,
            app_ctx.proxy.port,
            app_ctx.broker_host,
            app_ctx.broker_port);
               
        /* Print the SOCKS5 configuration */
        if (app_ctx.verbose) {
            print_socks5_config(socks5_options);
        }
        
        /* IMPORTANT: For SOCKS5 with TLS, make sure TLS options are properly set BEFORE setting SOCKS5 options */
        if (app_ctx.use_tls) {
            if (app_ctx.verbose) {
                printf("TLS with SOCKS5: Ensuring TLS options are properly configured\n");
                printf("  CA file: %s\n", app_ctx.ca_file ? app_ctx.ca_file : "(system default)");
                if (app_ctx.cert_file && app_ctx.key_file) {
                    printf("  Client certificate: %s\n", app_ctx.cert_file);
                } else {
                    printf("  Using one-way TLS (server authentication only)\n");
                }
            }
            
            /* Critical: Ensure TLS options are set in the MQTT connection options */
            if (tls_connection_options) {
                mqtt_conn_options.tls_options = tls_connection_options;
            } else if (app_ctx.verbose) {
                fprintf(stderr, "WARNING: TLS is enabled but TLS options are NULL!\n");
            }
        }
        
        /* Set the SOCKS5 proxy options in the MQTT client connection AFTER TLS is configured */
        if (aws_mqtt_client_connection_set_socks5_proxy_options(app_ctx.mqtt_connection, socks5_options) != AWS_OP_SUCCESS) {
            fprintf(stderr, "Failed to set SOCKS5 proxy options: %s\n", aws_error_debug_str(aws_last_error()));
            main_error_code = 18;
            goto cleanup;
        }
        
        /* Proceed with connection using the MQTT connection API */
        connect_result = aws_mqtt_client_connection_connect(app_ctx.mqtt_connection, &mqtt_conn_options);
    } else {
        /* Use the standard connection function for direct connections */
        connect_result = aws_mqtt_client_connection_connect(app_ctx.mqtt_connection, &mqtt_conn_options);
    }
    
    if (connect_result != AWS_OP_SUCCESS) {
        int error_code = aws_last_error();
        fprintf(stderr, "Failed to start MQTT connection: %s (error code: %d)\n", 
                aws_error_debug_str(error_code), error_code);
        main_error_code = 19;
        goto cleanup;
    }
    
    /* Wait for connection to complete */
    aws_mutex_lock(&app_ctx.lock);
    aws_condition_variable_wait_pred(
        &app_ctx.signal, &app_ctx.lock, s_connection_completed_predicate, &app_ctx);
    aws_mutex_unlock(&app_ctx.lock);
    
    if (app_ctx.connection_error_code != AWS_ERROR_SUCCESS) {
        fprintf(stderr, COLOR_RED "Failed to connect to MQTT broker: %s" COLOR_RESET "\n", 
                aws_error_debug_str(app_ctx.connection_error_code));
        main_error_code = 20;
        goto cleanup;
    }
    
    printf(COLOR_GREEN "Connected to MQTT broker successfully" COLOR_RESET "\n");
    
    /* Subscribe to topic */
    printf("Subscribing to topic: %s\n", app_ctx.sub_topic);
    if (s_subscribe_to_topic(&app_ctx) != AWS_OP_SUCCESS) {
        main_error_code = 21;
        goto cleanup;
    }
    
    /* Wait for subscription to complete */
    aws_mutex_lock(&app_ctx.lock);
    aws_condition_variable_wait_pred(
        &app_ctx.signal, &app_ctx.lock, s_subscription_completed_predicate, &app_ctx);
    aws_mutex_unlock(&app_ctx.lock);
    
    if (app_ctx.subscription_error_code != AWS_ERROR_SUCCESS) {
        fprintf(stderr, COLOR_RED "Failed to subscribe: %s" COLOR_RESET "\n", 
                aws_error_debug_str(app_ctx.subscription_error_code));
        main_error_code = 22;
        goto cleanup;
    }
    
    printf(COLOR_GREEN "Subscribed to topic: %s" COLOR_RESET "\n", app_ctx.sub_topic);
    
    /* Publish a message */
    printf("Publishing message to topic: %s\n", app_ctx.pub_topic);
    if (s_publish_message(&app_ctx) != AWS_OP_SUCCESS) {
        main_error_code = 23;
        goto cleanup;
    }
    
    /* Wait for publish to complete */
    aws_mutex_lock(&app_ctx.lock);
    aws_condition_variable_wait_pred(
        &app_ctx.signal, &app_ctx.lock, s_publish_completed_predicate, &app_ctx);
    aws_mutex_unlock(&app_ctx.lock);
    
    if (app_ctx.publish_error_code != AWS_ERROR_SUCCESS) {
        fprintf(stderr, COLOR_RED "Failed to publish: %s" COLOR_RESET "\n", aws_error_debug_str(app_ctx.publish_error_code));
        main_error_code = 24;
        goto cleanup;
    }
    
    printf(COLOR_GREEN "Published message successfully" COLOR_RESET "\n");
    
    /* Wait a short time to potentially receive the message we just published */
    printf("Waiting to receive message...\n");
    aws_thread_current_sleep(3000000000ULL); /* 3 seconds */
    
    aws_mutex_lock(&app_ctx.lock);
    if (app_ctx.message_received) {
        printf(COLOR_GREEN "Received message: %.*s" COLOR_RESET "\n", 
               (int)app_ctx.received_message.len, 
               (char *)app_ctx.received_message.buffer);
    } else {
        printf(COLOR_YELLOW "No message received within timeout" COLOR_RESET "\n");
    }
    aws_mutex_unlock(&app_ctx.lock);
    
    /* Disconnect from MQTT broker */
    printf("Disconnecting from MQTT broker...\n");
    aws_mqtt_client_connection_disconnect(app_ctx.mqtt_connection, NULL, NULL);
    
    /* Wait a bit for the disconnect to complete */
    aws_thread_current_sleep(1 * 1000000000); /* 1 second */
    
cleanup:
    /* Clean up TLS resources if used */
    if (tls_connection_options != NULL) {
        aws_tls_connection_options_clean_up(tls_connection_options);
        aws_mem_release(allocator, tls_connection_options);
    }
    
    if (tls_ctx != NULL) {
        aws_tls_ctx_release(tls_ctx);
    }
    
    /* Clean up proxy options if used */
    if (socks5_options) {
        aws_socks5_proxy_options_clean_up(socks5_options);
        aws_mem_release(allocator, socks5_options);
    }
    
    /* Clean up the app context */
    s_app_ctx_clean_up(&app_ctx);
    
    /* Clean up libraries */
    aws_mqtt_library_clean_up();
    aws_io_library_clean_up();
    aws_common_library_clean_up();
    
    /* Clean up logger before exit */
    if (logger_initialized) {
        aws_logger_clean_up(&logger);
    }
    
    return main_error_code;
}
