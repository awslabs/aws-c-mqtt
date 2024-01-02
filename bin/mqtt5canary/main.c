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

#include <aws/mqtt/private/v5/mqtt5_client_impl.h>
#include <aws/mqtt/private/v5/mqtt5_utils.h>
#include <aws/mqtt/v5/mqtt5_client.h>
#include <aws/mqtt/v5/mqtt5_types.h>

#include <inttypes.h>

#ifdef _MSC_VER
#    pragma warning(disable : 4996) /* Disable warnings about fopen() being insecure */
#    pragma warning(disable : 4204) /* Declared initializers */
#    pragma warning(disable : 4221) /* Local var in declared initializer */
#endif

#define AWS_MQTT5_CANARY_CLIENT_CREATION_SLEEP_TIME 10000000
#define AWS_MQTT5_CANARY_OPERATION_ARRAY_SIZE 10000
#define AWS_MQTT5_CANARY_TOPIC_ARRAY_SIZE 256
#define AWS_MQTT5_CANARY_CLIENT_MAX 50
#define AWS_MQTT5_CANARY_PAYLOAD_SIZE_MAX UINT16_MAX

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

enum aws_mqtt5_canary_operations {
    AWS_MQTT5_CANARY_OPERATION_NULL = 0,
    AWS_MQTT5_CANARY_OPERATION_START = 1,
    AWS_MQTT5_CANARY_OPERATION_STOP = 2,
    AWS_MQTT5_CANARY_OPERATION_DESTROY = 3,
    AWS_MQTT5_CANARY_OPERATION_SUBSCRIBE = 4,
    AWS_MQTT5_CANARY_OPERATION_UNSUBSCRIBE = 5,
    AWS_MQTT5_CANARY_OPERATION_UNSUBSCRIBE_BAD = 6,
    AWS_MQTT5_CANARY_OPERATION_PUBLISH_QOS0 = 7,
    AWS_MQTT5_CANARY_OPERATION_PUBLISH_QOS1 = 8,
    AWS_MQTT5_CANARY_OPERATION_PUBLISH_TO_SUBSCRIBED_TOPIC_QOS0 = 9,
    AWS_MQTT5_CANARY_OPERATION_PUBLISH_TO_SUBSCRIBED_TOPIC_QOS1 = 10,
    AWS_MQTT5_CANARY_OPERATION_PUBLISH_TO_SHARED_TOPIC_QOS0 = 11,
    AWS_MQTT5_CANARY_OPERATION_PUBLISH_TO_SHARED_TOPIC_QOS1 = 12,
    AWS_MQTT5_CANARY_OPERATION_COUNT = 13,
};

struct aws_mqtt5_canary_tester_options {
    uint16_t elg_max_threads;
    uint16_t client_count;
    size_t tps;
    uint64_t tps_sleep_time;
    size_t distributions_total;
    enum aws_mqtt5_canary_operations *operations;
    size_t test_run_seconds;
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
    fprintf(stderr, "  -p, --port: Port to use when making MQTT connections\n");

    fprintf(stderr, "  -t, --threads: number of eventloop group threads to use\n");
    fprintf(stderr, "  -C, --clients: number of mqtt5 clients to use\n");
    fprintf(stderr, "  -T, --tps: operations to run per second\n");
    fprintf(stderr, "  -s, --seconds: seconds to run canary test (set as 0 to run forever)\n");
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
    {"port", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'p'},
    {"help", AWS_CLI_OPTIONS_NO_ARGUMENT, NULL, 'h'},

    {"threads", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 't'},
    {"clients", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'C'},
    {"tps", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'T'},
    {"seconds", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 's'},
    /* Per getopt(3) the last element of the array has to be filled with all zeros */
    {NULL, AWS_CLI_OPTIONS_NO_ARGUMENT, NULL, 0},
};

static void s_parse_options(
    int argc,
    char **argv,
    struct app_ctx *ctx,
    struct aws_mqtt5_canary_tester_options *tester_options) {
    bool uri_found = false;

    while (true) {
        int option_index = 0;
        int c = aws_cli_getopt_long(argc, argv, "a:c:e:f:l:v:wht:p:C:T:s:", s_long_options, &option_index);
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
            case 'p':
                ctx->port = (uint32_t)atoi(aws_cli_optarg);
                break;
            case 't':
                tester_options->elg_max_threads = (uint16_t)atoi(aws_cli_optarg);
                break;
            case 'C':
                tester_options->client_count = (uint16_t)atoi(aws_cli_optarg);
                if (tester_options->client_count > AWS_MQTT5_CANARY_CLIENT_MAX) {
                    tester_options->client_count = AWS_MQTT5_CANARY_CLIENT_MAX;
                }
                break;
            case 'T':
                tester_options->tps = atoi(aws_cli_optarg);
                break;
            case 's':
                tester_options->test_run_seconds = atoi(aws_cli_optarg);
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

/**********************************************************
 * MQTT5 CANARY OPTIONS
 **********************************************************/

static void s_aws_mqtt5_canary_update_tps_sleep_time(struct aws_mqtt5_canary_tester_options *tester_options) {
    tester_options->tps_sleep_time =
        (aws_timestamp_convert(1, AWS_TIMESTAMP_SECS, AWS_TIMESTAMP_NANOS, NULL) / tester_options->tps);
}

static void s_aws_mqtt5_canary_init_tester_options(struct aws_mqtt5_canary_tester_options *tester_options) {
    /* number of eventloop group threads to use */
    tester_options->elg_max_threads = 3;
    /* number of mqtt5 clients to use */
    tester_options->client_count = 10;
    /* operations per second to run */
    tester_options->tps = 50;
    /* How long to run the test before exiting */
    tester_options->test_run_seconds = 25200;
}

struct aws_mqtt5_canary_test_client {
    struct aws_mqtt5_client *client;
    const struct aws_mqtt5_negotiated_settings *settings;
    struct aws_byte_cursor shared_topic;
    struct aws_byte_cursor client_id;
    size_t subscription_count;
    bool is_connected;
};

/**********************************************************
 * OPERATION DISTRIBUTION
 **********************************************************/

typedef int(aws_mqtt5_canary_operation_fn)(struct aws_mqtt5_canary_test_client *test_client);

struct aws_mqtt5_canary_operations_function_table {
    aws_mqtt5_canary_operation_fn *operation_by_operation_type[AWS_MQTT5_CANARY_OPERATION_COUNT];
};

static void s_aws_mqtt5_canary_add_operation_to_array(
    struct aws_mqtt5_canary_tester_options *tester_options,
    enum aws_mqtt5_canary_operations operation_type,
    size_t probability) {
    for (size_t i = 0; i < probability; ++i) {

        tester_options->operations[tester_options->distributions_total] = operation_type;
        tester_options->distributions_total += 1;
    }
}

/* Add operations and their weighted probability to the list of possible operations */
static void s_aws_mqtt5_canary_init_weighted_operations(struct aws_mqtt5_canary_tester_options *tester_options) {

    s_aws_mqtt5_canary_add_operation_to_array(tester_options, AWS_MQTT5_CANARY_OPERATION_STOP, 1);
    s_aws_mqtt5_canary_add_operation_to_array(tester_options, AWS_MQTT5_CANARY_OPERATION_SUBSCRIBE, 200);
    s_aws_mqtt5_canary_add_operation_to_array(tester_options, AWS_MQTT5_CANARY_OPERATION_UNSUBSCRIBE, 200);
    s_aws_mqtt5_canary_add_operation_to_array(tester_options, AWS_MQTT5_CANARY_OPERATION_UNSUBSCRIBE_BAD, 100);
    s_aws_mqtt5_canary_add_operation_to_array(tester_options, AWS_MQTT5_CANARY_OPERATION_PUBLISH_QOS0, 300);
    s_aws_mqtt5_canary_add_operation_to_array(tester_options, AWS_MQTT5_CANARY_OPERATION_PUBLISH_QOS1, 150);
    s_aws_mqtt5_canary_add_operation_to_array(
        tester_options, AWS_MQTT5_CANARY_OPERATION_PUBLISH_TO_SUBSCRIBED_TOPIC_QOS0, 100);
    s_aws_mqtt5_canary_add_operation_to_array(
        tester_options, AWS_MQTT5_CANARY_OPERATION_PUBLISH_TO_SUBSCRIBED_TOPIC_QOS1, 50);
    s_aws_mqtt5_canary_add_operation_to_array(
        tester_options, AWS_MQTT5_CANARY_OPERATION_PUBLISH_TO_SHARED_TOPIC_QOS0, 50);
    s_aws_mqtt5_canary_add_operation_to_array(
        tester_options, AWS_MQTT5_CANARY_OPERATION_PUBLISH_TO_SHARED_TOPIC_QOS1, 50);
}

static enum aws_mqtt5_canary_operations s_aws_mqtt5_canary_get_random_operation(
    struct aws_mqtt5_canary_tester_options *tester_options) {
    size_t random_index = rand() % tester_options->distributions_total;

    return tester_options->operations[random_index];
}

/**********************************************************
 * PACKET CALLBACKS
 **********************************************************/

static void s_on_publish_received(const struct aws_mqtt5_packet_publish_view *publish, void *user_data) {
    (void)publish;

    struct aws_mqtt5_canary_test_client *test_client = user_data;

    AWS_LOGF_INFO(
        AWS_LS_MQTT5_CANARY,
        "ID:" PRInSTR " Publish Received on topic " PRInSTR,
        AWS_BYTE_CURSOR_PRI(test_client->client_id),
        AWS_BYTE_CURSOR_PRI(publish->topic));
}

/**********************************************************
 * LIFECYCLE EVENTS
 **********************************************************/

static void s_handle_lifecycle_event_connection_success(
    struct aws_mqtt5_canary_test_client *test_client,
    const struct aws_mqtt5_negotiated_settings *settings) {
    AWS_ASSERT(test_client != NULL);
    test_client->is_connected = true;
    test_client->settings = settings;
    test_client->client_id = aws_byte_cursor_from_buf(&settings->client_id_storage);

    AWS_LOGF_INFO(
        AWS_LS_MQTT5_CANARY,
        "ID:" PRInSTR " Lifecycle Event: Connection Success",
        AWS_BYTE_CURSOR_PRI(test_client->client_id));
}

static void s_handle_lifecycle_event_disconnection(struct aws_mqtt5_canary_test_client *test_client) {
    AWS_ASSERT(test_client != NULL);
    test_client->is_connected = false;
    AWS_LOGF_INFO(
        AWS_LS_MQTT5_CANARY, "ID:" PRInSTR " Lifecycle Event: Disconnect", AWS_BYTE_CURSOR_PRI(test_client->client_id));
}

static void s_handle_lifecycle_event_stopped(struct aws_mqtt5_canary_test_client *test_client) {
    AWS_ASSERT(test_client != NULL);
    AWS_LOGF_INFO(
        AWS_LS_MQTT5_CANARY, "ID:" PRInSTR " Lifecycle Event: Stopped", AWS_BYTE_CURSOR_PRI(test_client->client_id));
}

static void s_lifecycle_event_callback(const struct aws_mqtt5_client_lifecycle_event *event) {
    switch (event->event_type) {
        case AWS_MQTT5_CLET_STOPPED:
            s_handle_lifecycle_event_stopped(event->user_data);
            break;

        case AWS_MQTT5_CLET_ATTEMPTING_CONNECT:
            AWS_LOGF_INFO(AWS_LS_MQTT5_CANARY, "Lifecycle event: Attempting Connect!");
            break;

        case AWS_MQTT5_CLET_CONNECTION_FAILURE:
            AWS_LOGF_INFO(AWS_LS_MQTT5_CANARY, "Lifecycle event: Connection Failure!");
            AWS_LOGF_INFO(
                AWS_LS_MQTT5_CANARY, "  Error Code: %d(%s)", event->error_code, aws_error_debug_str(event->error_code));
            break;

        case AWS_MQTT5_CLET_CONNECTION_SUCCESS:
            s_handle_lifecycle_event_connection_success(event->user_data, event->settings);
            break;

        case AWS_MQTT5_CLET_DISCONNECTION:
            s_handle_lifecycle_event_disconnection(event->user_data);
            AWS_LOGF_INFO(
                AWS_LS_MQTT5_CANARY, "  Error Code: %d(%s)", event->error_code, aws_error_debug_str(event->error_code));
            break;
    }
}

static void s_aws_mqtt5_transform_websocket_handshake_fn(
    struct aws_http_message *request,
    void *user_data,
    aws_mqtt5_transform_websocket_handshake_complete_fn *complete_fn,
    void *complete_ctx) {

    (void)user_data;

    (*complete_fn)(request, AWS_ERROR_SUCCESS, complete_ctx);
}

/**********************************************************
 * OPERATION CALLBACKS
 **********************************************************/

static void s_aws_mqtt5_canary_operation_subscribe_completion(
    const struct aws_mqtt5_packet_suback_view *suback,
    int error_code,
    void *complete_ctx) {
    (void)suback;

    if (error_code != AWS_MQTT5_UARC_SUCCESS) {
        struct aws_mqtt5_canary_test_client *test_client = (struct aws_mqtt5_canary_test_client *)(complete_ctx);
        if (test_client != NULL) {
            AWS_LOGF_ERROR(
                AWS_LS_MQTT5_CANARY,
                "ID:" PRInSTR " Subscribe completed with error code: %i",
                AWS_BYTE_CURSOR_PRI(test_client->client_id),
                error_code);
        }
    }
}

static void s_aws_mqtt5_canary_operation_unsubscribe_completion(
    const struct aws_mqtt5_packet_unsuback_view *unsuback,
    int error_code,
    void *complete_ctx) {
    (void)unsuback;

    if (error_code != AWS_MQTT5_UARC_SUCCESS) {
        struct aws_mqtt5_canary_test_client *test_client = (struct aws_mqtt5_canary_test_client *)(complete_ctx);
        if (test_client != NULL) {
            AWS_LOGF_ERROR(
                AWS_LS_MQTT5_CANARY,
                "ID:" PRInSTR " Unsubscribe completed with error code: %i",
                AWS_BYTE_CURSOR_PRI(test_client->client_id),
                error_code);
        }
    }
}

static void s_aws_mqtt5_canary_operation_publish_completion(
    enum aws_mqtt5_packet_type packet_type,
    const void *packet,
    int error_code,
    void *complete_ctx) {
    (void)packet;
    (void)packet_type;

    if (error_code != AWS_MQTT5_PARC_SUCCESS) {
        struct aws_mqtt5_canary_test_client *test_client = (struct aws_mqtt5_canary_test_client *)(complete_ctx);
        if (test_client != NULL) {
            AWS_LOGF_ERROR(
                AWS_LS_MQTT5_CANARY,
                "ID:" PRInSTR " Publish completed with error code: %i",
                AWS_BYTE_CURSOR_PRI(test_client->client_id),
                error_code);
        }
    }
}

/**********************************************************
 * OPERATION FUNCTIONS
 **********************************************************/

static int s_aws_mqtt5_canary_operation_start(struct aws_mqtt5_canary_test_client *test_client) {
    if (test_client->is_connected) {
        return AWS_OP_SUCCESS;
    }
    aws_mqtt5_client_start(test_client->client);
    struct aws_byte_cursor client_id;
    if (test_client->client_id.len) {
        client_id.ptr = test_client->client_id.ptr;
        client_id.len = test_client->client_id.len;
    } else {
        client_id = aws_byte_cursor_from_c_str("Client ID not set");
    }
    AWS_LOGF_INFO(AWS_LS_MQTT5_CANARY, "ID:" PRInSTR " Start", AWS_BYTE_CURSOR_PRI(client_id));
    return AWS_OP_SUCCESS;
}

static int s_aws_mqtt5_canary_operation_stop(struct aws_mqtt5_canary_test_client *test_client) {
    if (!test_client->is_connected) {
        return AWS_OP_SUCCESS;
    }
    aws_mqtt5_client_stop(test_client->client, NULL, NULL);
    test_client->subscription_count = 0;
    AWS_LOGF_INFO(AWS_LS_MQTT5_CANARY, "ID:" PRInSTR " Stop", AWS_BYTE_CURSOR_PRI(test_client->client_id));
    return AWS_OP_SUCCESS;
}

static int s_aws_mqtt5_canary_operation_subscribe(struct aws_mqtt5_canary_test_client *test_client) {
    if (!test_client->is_connected) {
        return s_aws_mqtt5_canary_operation_start(test_client);
    }
    char topic_array[AWS_MQTT5_CANARY_TOPIC_ARRAY_SIZE] = "";
    snprintf(
        topic_array,
        sizeof topic_array,
        PRInSTR "_%zu",
        AWS_BYTE_CURSOR_PRI(test_client->client_id),
        test_client->subscription_count);

    struct aws_mqtt5_subscription_view subscriptions[] = {
        {
            .topic_filter = aws_byte_cursor_from_c_str(topic_array),
            .qos = AWS_MQTT5_QOS_AT_LEAST_ONCE,
            .no_local = false,
            .retain_as_published = false,
            .retain_handling_type = AWS_MQTT5_RHT_SEND_ON_SUBSCRIBE,
        },
        {
            .topic_filter =
                {
                    .ptr = test_client->shared_topic.ptr,
                    .len = test_client->shared_topic.len,
                },
            .qos = AWS_MQTT5_QOS_AT_LEAST_ONCE,
            .no_local = false,
            .retain_as_published = false,
            .retain_handling_type = AWS_MQTT5_RHT_SEND_ON_SUBSCRIBE,
        },
    };

    struct aws_mqtt5_packet_subscribe_view subscribe_view = {
        .subscriptions = subscriptions,
        .subscription_count = AWS_ARRAY_SIZE(subscriptions),
    };

    struct aws_mqtt5_subscribe_completion_options subscribe_view_options = {
        .completion_callback = &s_aws_mqtt5_canary_operation_subscribe_completion,
        .completion_user_data = test_client,
    };

    test_client->subscription_count++;

    AWS_LOGF_INFO(
        AWS_LS_MQTT5_CANARY,
        "ID:" PRInSTR " Subscribe to topic: " PRInSTR,
        AWS_BYTE_CURSOR_PRI(test_client->client_id),
        AWS_BYTE_CURSOR_PRI(subscriptions->topic_filter));
    return aws_mqtt5_client_subscribe(test_client->client, &subscribe_view, &subscribe_view_options);
}

static int s_aws_mqtt5_canary_operation_unsubscribe_bad(struct aws_mqtt5_canary_test_client *test_client) {
    if (!test_client->is_connected) {
        return s_aws_mqtt5_canary_operation_start(test_client);
    }
    char topic_array[AWS_MQTT5_CANARY_TOPIC_ARRAY_SIZE] = "";
    snprintf(
        topic_array, sizeof topic_array, PRInSTR "_non_existing_topic", AWS_BYTE_CURSOR_PRI(test_client->client_id));
    struct aws_byte_cursor topic = aws_byte_cursor_from_c_str(topic_array);
    struct aws_byte_cursor unsubscribes[] = {
        {
            .ptr = topic.ptr,
            .len = topic.len,
        },
    };

    struct aws_mqtt5_packet_unsubscribe_view unsubscribe_view = {
        .topic_filters = unsubscribes,
        .topic_filter_count = AWS_ARRAY_SIZE(unsubscribes),
    };

    AWS_LOGF_INFO(AWS_LS_MQTT5_CANARY, "ID:" PRInSTR " Unsubscribe Bad", AWS_BYTE_CURSOR_PRI(test_client->client_id));
    return aws_mqtt5_client_unsubscribe(test_client->client, &unsubscribe_view, NULL);
}

static int s_aws_mqtt5_canary_operation_unsubscribe(struct aws_mqtt5_canary_test_client *test_client) {
    if (!test_client->is_connected) {
        return s_aws_mqtt5_canary_operation_start(test_client);
    }

    if (test_client->subscription_count <= 0) {
        return s_aws_mqtt5_canary_operation_unsubscribe_bad(test_client);
    }

    test_client->subscription_count--;
    char topic_array[AWS_MQTT5_CANARY_TOPIC_ARRAY_SIZE] = "";
    snprintf(
        topic_array,
        sizeof topic_array,
        PRInSTR "_%zu",
        AWS_BYTE_CURSOR_PRI(test_client->client_id),
        test_client->subscription_count);
    struct aws_byte_cursor topic = aws_byte_cursor_from_c_str(topic_array);
    struct aws_byte_cursor unsubscribes[] = {
        {
            .ptr = topic.ptr,
            .len = topic.len,
        },
    };

    struct aws_mqtt5_packet_unsubscribe_view unsubscribe_view = {
        .topic_filters = unsubscribes,
        .topic_filter_count = AWS_ARRAY_SIZE(unsubscribes),
    };

    struct aws_mqtt5_unsubscribe_completion_options unsubscribe_view_options = {
        .completion_callback = &s_aws_mqtt5_canary_operation_unsubscribe_completion,
        .completion_user_data = test_client};

    AWS_LOGF_INFO(
        AWS_LS_MQTT5_CANARY,
        "ID:" PRInSTR " Unsubscribe from topic: " PRInSTR,
        AWS_BYTE_CURSOR_PRI(test_client->client_id),
        AWS_BYTE_CURSOR_PRI(topic));
    return aws_mqtt5_client_unsubscribe(test_client->client, &unsubscribe_view, &unsubscribe_view_options);
}

static int s_aws_mqtt5_canary_operation_publish(
    struct aws_mqtt5_canary_test_client *test_client,
    struct aws_byte_cursor topic_filter,
    enum aws_mqtt5_qos qos) {

    struct aws_byte_cursor property_cursor = aws_byte_cursor_from_c_str("property");
    struct aws_mqtt5_user_property user_properties[] = {
        {
            .name =
                {
                    .ptr = property_cursor.ptr,
                    .len = property_cursor.len,
                },
            .value =
                {
                    .ptr = property_cursor.ptr,
                    .len = property_cursor.len,
                },
        },
        {
            .name =
                {
                    .ptr = property_cursor.ptr,
                    .len = property_cursor.len,
                },
            .value =
                {
                    .ptr = property_cursor.ptr,
                    .len = property_cursor.len,
                },
        },
    };

    uint16_t payload_size = (rand() % UINT16_MAX) + 1;
    uint8_t payload_data[AWS_MQTT5_CANARY_PAYLOAD_SIZE_MAX];

    struct aws_mqtt5_packet_publish_view packet_publish_view = {
        .qos = qos,
        .topic = topic_filter,
        .retain = false,
        .duplicate = false,
        .payload =
            {
                .ptr = payload_data,
                .len = payload_size,
            },
        .user_properties = user_properties,
        .user_property_count = AWS_ARRAY_SIZE(user_properties),
    };

    struct aws_mqtt5_publish_completion_options packet_publish_view_options = {
        .completion_callback = &s_aws_mqtt5_canary_operation_publish_completion, .completion_user_data = test_client};

    return aws_mqtt5_client_publish(test_client->client, &packet_publish_view, &packet_publish_view_options);
}

static int s_aws_mqtt5_canary_operation_publish_qos0(struct aws_mqtt5_canary_test_client *test_client) {
    if (!test_client->is_connected) {
        return s_aws_mqtt5_canary_operation_start(test_client);
    }

    struct aws_byte_cursor topic_cursor;
    AWS_ZERO_STRUCT(topic_cursor);
    topic_cursor = aws_byte_cursor_from_c_str("topic1");
    AWS_LOGF_INFO(AWS_LS_MQTT5_CANARY, "ID:" PRInSTR " Publish qos0", AWS_BYTE_CURSOR_PRI(test_client->client_id));
    return s_aws_mqtt5_canary_operation_publish(test_client, topic_cursor, AWS_MQTT5_QOS_AT_MOST_ONCE);
}

static int s_aws_mqtt5_canary_operation_publish_qos1(struct aws_mqtt5_canary_test_client *test_client) {
    if (!test_client->is_connected) {
        return s_aws_mqtt5_canary_operation_start(test_client);
    }
    struct aws_byte_cursor topic_cursor;
    AWS_ZERO_STRUCT(topic_cursor);
    topic_cursor = aws_byte_cursor_from_c_str("topic1");
    AWS_LOGF_INFO(AWS_LS_MQTT5_CANARY, "ID:" PRInSTR " Publish qos1", AWS_BYTE_CURSOR_PRI(test_client->client_id));
    return s_aws_mqtt5_canary_operation_publish(test_client, topic_cursor, AWS_MQTT5_QOS_AT_LEAST_ONCE);
}

static int s_aws_mqtt5_canary_operation_publish_to_subscribed_topic_qos0(
    struct aws_mqtt5_canary_test_client *test_client) {
    if (!test_client->is_connected) {
        return s_aws_mqtt5_canary_operation_start(test_client);
    }

    if (test_client->subscription_count < 1) {
        return s_aws_mqtt5_canary_operation_publish_qos0(test_client);
    }
    char topic_array[AWS_MQTT5_CANARY_TOPIC_ARRAY_SIZE] = "";
    snprintf(
        topic_array,
        sizeof topic_array,
        PRInSTR "_%zu",
        AWS_BYTE_CURSOR_PRI(test_client->client_id),
        test_client->subscription_count - 1);
    struct aws_byte_cursor topic_cursor = aws_byte_cursor_from_c_str(topic_array);
    AWS_LOGF_INFO(
        AWS_LS_MQTT5_CANARY,
        "ID:" PRInSTR " Publish qos 0 to subscribed topic: " PRInSTR,
        AWS_BYTE_CURSOR_PRI(test_client->client_id),
        AWS_BYTE_CURSOR_PRI(topic_cursor));
    return s_aws_mqtt5_canary_operation_publish(test_client, topic_cursor, AWS_MQTT5_QOS_AT_MOST_ONCE);
}

static int s_aws_mqtt5_canary_operation_publish_to_subscribed_topic_qos1(
    struct aws_mqtt5_canary_test_client *test_client) {
    if (!test_client->is_connected) {
        return s_aws_mqtt5_canary_operation_start(test_client);
    }

    if (test_client->subscription_count < 1) {
        return s_aws_mqtt5_canary_operation_publish_qos1(test_client);
    }

    char topic_array[AWS_MQTT5_CANARY_TOPIC_ARRAY_SIZE] = "";
    snprintf(
        topic_array,
        sizeof topic_array,
        PRInSTR "_%zu",
        AWS_BYTE_CURSOR_PRI(test_client->client_id),
        test_client->subscription_count - 1);
    struct aws_byte_cursor topic_cursor = aws_byte_cursor_from_c_str(topic_array);
    AWS_LOGF_INFO(
        AWS_LS_MQTT5_CANARY,
        "ID:" PRInSTR " Publish qos 1 to subscribed topic: " PRInSTR,
        AWS_BYTE_CURSOR_PRI(test_client->client_id),
        AWS_BYTE_CURSOR_PRI(topic_cursor));
    return s_aws_mqtt5_canary_operation_publish(test_client, topic_cursor, AWS_MQTT5_QOS_AT_LEAST_ONCE);
}

static int s_aws_mqtt5_canary_operation_publish_to_shared_topic_qos0(struct aws_mqtt5_canary_test_client *test_client) {
    if (!test_client->is_connected) {
        return s_aws_mqtt5_canary_operation_start(test_client);
    }
    AWS_LOGF_INFO(
        AWS_LS_MQTT5_CANARY,
        "ID:" PRInSTR " Publish qos 0 to shared topic: " PRInSTR,
        AWS_BYTE_CURSOR_PRI(test_client->client_id),
        AWS_BYTE_CURSOR_PRI(test_client->shared_topic));
    return s_aws_mqtt5_canary_operation_publish(test_client, test_client->shared_topic, AWS_MQTT5_QOS_AT_MOST_ONCE);
}

static int s_aws_mqtt5_canary_operation_publish_to_shared_topic_qos1(struct aws_mqtt5_canary_test_client *test_client) {
    if (!test_client->is_connected) {
        return s_aws_mqtt5_canary_operation_start(test_client);
    }
    AWS_LOGF_INFO(
        AWS_LS_MQTT5_CANARY,
        "ID:" PRInSTR " Publish qos 1 to shared topic: " PRInSTR,
        AWS_BYTE_CURSOR_PRI(test_client->client_id),
        AWS_BYTE_CURSOR_PRI(test_client->shared_topic));
    return s_aws_mqtt5_canary_operation_publish(test_client, test_client->shared_topic, AWS_MQTT5_QOS_AT_LEAST_ONCE);
}

static struct aws_mqtt5_canary_operations_function_table s_aws_mqtt5_canary_operation_table = {
    .operation_by_operation_type =
        {
            NULL,                                                           /* null */
            &s_aws_mqtt5_canary_operation_start,                            /* start */
            &s_aws_mqtt5_canary_operation_stop,                             /* stop */
            NULL,                                                           /* destroy */
            &s_aws_mqtt5_canary_operation_subscribe,                        /* subscribe */
            &s_aws_mqtt5_canary_operation_unsubscribe,                      /* unsubscribe */
            &s_aws_mqtt5_canary_operation_unsubscribe_bad,                  /* unsubscribe_bad */
            &s_aws_mqtt5_canary_operation_publish_qos0,                     /* publish_qos0 */
            &s_aws_mqtt5_canary_operation_publish_qos1,                     /* publish_qos1 */
            &s_aws_mqtt5_canary_operation_publish_to_subscribed_topic_qos0, /* publish_to_subscribed_topic_qos0 */
            &s_aws_mqtt5_canary_operation_publish_to_subscribed_topic_qos1, /* publish_to_subscribed_topic_qos1 */
            &s_aws_mqtt5_canary_operation_publish_to_shared_topic_qos0,     /* publish_to_shared_topic_qos0 */
            &s_aws_mqtt5_canary_operation_publish_to_shared_topic_qos1,     /* publish_to_shared_topic_qos1 */
        },
};

/**********************************************************
 * MAIN
 **********************************************************/

int main(int argc, char **argv) {

    struct aws_allocator *allocator = aws_mem_tracer_new(aws_default_allocator(), NULL, AWS_MEMTRACE_STACKS, 15);
    aws_mqtt_library_init(allocator);

    struct app_ctx app_ctx;
    AWS_ZERO_STRUCT(app_ctx);
    app_ctx.allocator = allocator;
    app_ctx.signal = (struct aws_condition_variable)AWS_CONDITION_VARIABLE_INIT;
    app_ctx.connect_timeout = 3000;
    aws_mutex_init(&app_ctx.lock);
    if (app_ctx.port == 0) {
        app_ctx.port = 1883;
    }

    struct aws_mqtt5_canary_tester_options tester_options;
    AWS_ZERO_STRUCT(tester_options);
    s_aws_mqtt5_canary_init_tester_options(&tester_options);
    enum aws_mqtt5_canary_operations operations[AWS_MQTT5_CANARY_OPERATION_ARRAY_SIZE];
    AWS_ZERO_STRUCT(operations);
    tester_options.operations = operations;

    s_parse_options(argc, argv, &app_ctx, &tester_options);
    if (app_ctx.uri.port) {
        app_ctx.port = app_ctx.uri.port;
    }

    s_aws_mqtt5_canary_update_tps_sleep_time(&tester_options);
    s_aws_mqtt5_canary_init_weighted_operations(&tester_options);

    /**********************************************************
     * LOGGING
     **********************************************************/
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
            fprintf(stderr, "Faled to initialize logger with error %s", aws_error_debug_str(aws_last_error()));
            exit(1);
        }
        aws_logger_set(&logger);
    } else {
        options.file = stderr;
    }

    /**********************************************************
     * TLS
     **********************************************************/
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

    /**********************************************************
     * EVENT LOOP GROUP
     **********************************************************/
    struct aws_event_loop_group *el_group =
        aws_event_loop_group_new_default(allocator, tester_options.elg_max_threads, NULL);

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

    aws_mqtt5_transform_websocket_handshake_fn *websocket_handshake_transform = NULL;
    void *websocket_handshake_transform_user_data = NULL;
    if (app_ctx.use_websockets) {
        websocket_handshake_transform = &s_aws_mqtt5_transform_websocket_handshake_fn;
    }

    /**********************************************************
     * MQTT5 CLIENT CREATION
     **********************************************************/

    struct aws_mqtt5_packet_connect_view connect_options = {
        .keep_alive_interval_seconds = 30,
        .clean_start = true,
        .maximum_packet_size_bytes = &maximum_packet_size,
        .receive_maximum = &receive_maximum,
    };

    struct aws_mqtt5_client_options client_options = {
        .host_name = app_ctx.uri.host_name,
        .port = app_ctx.port,
        .bootstrap = bootstrap,
        .socket_options = &socket_options,
        .tls_options = (use_tls) ? &tls_connection_options : NULL,
        .connect_options = &connect_options,
        .session_behavior = AWS_MQTT5_CSBT_CLEAN,
        .lifecycle_event_handler = s_lifecycle_event_callback,
        .retry_jitter_mode = AWS_EXPONENTIAL_BACKOFF_JITTER_NONE,
        .min_reconnect_delay_ms = 1000,
        .max_reconnect_delay_ms = 120000,
        .min_connected_time_to_reset_reconnect_delay_ms = 30000,
        .ping_timeout_ms = 10000,
        .websocket_handshake_transform = websocket_handshake_transform,
        .websocket_handshake_transform_user_data = websocket_handshake_transform_user_data,
        .publish_received_handler = s_on_publish_received,
        .ack_timeout_seconds = 300, /* 5 minute timeout */
    };

    struct aws_mqtt5_canary_test_client clients[AWS_MQTT5_CANARY_CLIENT_MAX];
    AWS_ZERO_STRUCT(clients);

    uint64_t start_time = 0;
    aws_high_res_clock_get_ticks(&start_time);
    char shared_topic_array[AWS_MQTT5_CANARY_TOPIC_ARRAY_SIZE] = "";
    snprintf(shared_topic_array, sizeof shared_topic_array, "%" PRIu64 "_shared_topic", start_time);
    struct aws_byte_cursor shared_topic = aws_byte_cursor_from_c_str(shared_topic_array);

    for (size_t i = 0; i < tester_options.client_count; ++i) {

        client_options.lifecycle_event_handler_user_data = &clients[i];
        client_options.publish_received_handler_user_data = &clients[i];

        clients[i].shared_topic = shared_topic;
        clients[i].client = aws_mqtt5_client_new(allocator, &client_options);

        aws_mqtt5_canary_operation_fn *operation_fn =
            s_aws_mqtt5_canary_operation_table.operation_by_operation_type[AWS_MQTT5_CANARY_OPERATION_START];
        (*operation_fn)(&clients[i]);

        aws_thread_current_sleep(AWS_MQTT5_CANARY_CLIENT_CREATION_SLEEP_TIME);
    }

    fprintf(stderr, "Clients created\n");

    /**********************************************************
     * TESTING
     **********************************************************/
    bool done = false;
    size_t operations_executed = 0;
    uint64_t time_test_finish = 0;
    aws_high_res_clock_get_ticks(&time_test_finish);
    time_test_finish +=
        aws_timestamp_convert(tester_options.test_run_seconds, AWS_TIMESTAMP_SECS, AWS_TIMESTAMP_NANOS, NULL);

    if (tester_options.test_run_seconds > 0) {
        printf("Running test for %zu seconds\n", tester_options.test_run_seconds);
    } else {
        printf("Running test forever\n");
    }

    while (!done) {
        uint64_t now = 0;
        aws_high_res_clock_get_ticks(&now);
        operations_executed++;

        enum aws_mqtt5_canary_operations next_operation = s_aws_mqtt5_canary_get_random_operation(&tester_options);
        aws_mqtt5_canary_operation_fn *operation_fn =
            s_aws_mqtt5_canary_operation_table.operation_by_operation_type[next_operation];

        (*operation_fn)(&clients[rand() % tester_options.client_count]);

        if (now > time_test_finish && tester_options.test_run_seconds > 0) {
            done = true;
        }

        aws_thread_current_sleep(tester_options.tps_sleep_time);
    }

    /**********************************************************
     * CLEAN UP
     **********************************************************/
    for (size_t i = 0; i < tester_options.client_count; ++i) {
        struct aws_mqtt5_client *client = clients[i].client;
        aws_mqtt5_client_release(client);
    }

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
    printf("   Outstanding bytes: %zu\n", outstanding_bytes);

    if (app_ctx.log_level) {
        aws_logger_set(NULL);
        aws_logger_clean_up(&logger);
    }

    aws_uri_clean_up(&app_ctx.uri);

    aws_mqtt_library_clean_up();

    printf("   Operations executed: %zu\n", operations_executed);
    printf("   Operating TPS average over test: %zu\n\n", operations_executed / tester_options.test_run_seconds);

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
