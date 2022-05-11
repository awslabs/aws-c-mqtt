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
    bool use_websockets;

    struct aws_tls_connection_options tls_connection_options;

    const char *log_filename;
    enum aws_log_level log_level;
};

struct aws_mqtt5_canary_tester_options {
    uint16_t elg_max_threads;
    uint16_t client_count;
    size_t tps;
    uint64_t tps_sleep_time;
    size_t distributions_total;
    bool apply_operations_to_all_clients;
    size_t test_run_hours;
    size_t test_run_minutes;
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

    fprintf(stderr, "  -t, --threads: number of eventloop group threads to use\n");
    fprintf(stderr, "  -C, --clients: number of mqtt5 clients to use\n");
    fprintf(stderr, "  -T, --tps: operations to run per second\n");
    fprintf(stderr, "  -s, --seconds: seconds to run canary test\n");
    fprintf(stderr, "  -m, --minutes: minutes to run canary test\n");
    fprintf(stderr, "  -H, --hours: hours to run canary test\n");
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

    {"threads", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 't'},
    {"clients", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'C'},
    {"tps", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'T'},
    {"seconds", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 's'},
    {"minutes", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'm'},
    {"hours", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'H'},
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
        int c = aws_cli_getopt_long(argc, argv, "a:c:e:f:l:v:wht:C:T:s:m:H:", s_long_options, &option_index);
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
            case 't':
                tester_options->elg_max_threads = atoi(aws_cli_optarg);
                break;
            case 'C':
                tester_options->client_count = atoi(aws_cli_optarg);
                break;
            case 'T':
                tester_options->tps = atoi(aws_cli_optarg);
                break;
            case 's':
                tester_options->test_run_seconds = atoi(aws_cli_optarg);
                break;
            case 'm':
                tester_options->test_run_minutes = atoi(aws_cli_optarg);
                break;
            case 'H':
                tester_options->test_run_hours = atoi(aws_cli_optarg);
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
    /* should every operation run on every client */
    tester_options->apply_operations_to_all_clients = false;

    /* How long to run the test before exiting */
    tester_options->test_run_seconds = 1;
    tester_options->test_run_minutes = 0;
    tester_options->test_run_hours = 0;
}

struct aws_mqtt5_canary_test_client {
    struct aws_mqtt5_client *client;
    const struct aws_mqtt5_negotiated_settings *settings;
    struct aws_byte_cursor client_id;
    size_t subscription_count;
    bool is_connected;

    size_t index;
};

/**********************************************************
 * OPERATION DISTRIBUTION
 **********************************************************/

typedef int(aws_mqtt5_canary_operation_fn)(struct aws_mqtt5_canary_test_client *test_client);

#define AWS_MQTT5_CANARY_DISTRIBUTIONS_COUNT 13
struct aws_mqtt5_canary_operations_function_table {
    aws_mqtt5_canary_operation_fn *operation_by_operation_type[AWS_MQTT5_CANARY_DISTRIBUTIONS_COUNT];
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
};

struct operation_distribution {
    struct aws_linked_list_node node;
    enum aws_mqtt5_canary_operations operation;
    size_t probability;
};

static void s_aws_mqtt5_canary_add_operation_distribution(
    struct aws_mqtt5_canary_tester_options *tester_options,
    struct operation_distribution *distribution_storage,
    struct aws_linked_list *distributions,
    enum aws_mqtt5_canary_operations operation_type,
    size_t probability) {
    distribution_storage->operation = operation_type;
    distribution_storage->probability = probability;

    aws_linked_list_push_back(distributions, &distribution_storage->node);
    tester_options->distributions_total += probability;
}

/* Add operations and their weighted probability to the list of possible operations */
static void s_aws_mqtt5_canary_init_operation_distributions(
    struct aws_mqtt5_canary_tester_options *tester_options,
    struct operation_distribution distribution_storage[],
    struct aws_linked_list *distributions) {

    /* Start is automatically called on stopped clients when it receives an operation. */
    /*
    s_aws_mqtt5_canary_add_operation_distribution(
        tester_options,
        &distribution_storage[AWS_MQTT5_CANARY_OPERATION_START],
        distributions,
        AWS_MQTT5_CANARY_OPERATION_START,
    1);
    */
    s_aws_mqtt5_canary_add_operation_distribution(
        tester_options,
        &distribution_storage[AWS_MQTT5_CANARY_OPERATION_STOP],
        distributions,
        AWS_MQTT5_CANARY_OPERATION_STOP,
        1);
    s_aws_mqtt5_canary_add_operation_distribution(
        tester_options,
        &distribution_storage[AWS_MQTT5_CANARY_OPERATION_SUBSCRIBE],
        distributions,
        AWS_MQTT5_CANARY_OPERATION_SUBSCRIBE,
        20);
    s_aws_mqtt5_canary_add_operation_distribution(
        tester_options,
        &distribution_storage[AWS_MQTT5_CANARY_OPERATION_UNSUBSCRIBE],
        distributions,
        AWS_MQTT5_CANARY_OPERATION_UNSUBSCRIBE,
        10);
    s_aws_mqtt5_canary_add_operation_distribution(
        tester_options,
        &distribution_storage[AWS_MQTT5_CANARY_OPERATION_UNSUBSCRIBE_BAD],
        distributions,
        AWS_MQTT5_CANARY_OPERATION_UNSUBSCRIBE_BAD,
        5);
    s_aws_mqtt5_canary_add_operation_distribution(
        tester_options,
        &distribution_storage[AWS_MQTT5_CANARY_OPERATION_PUBLISH_QOS0],
        distributions,
        AWS_MQTT5_CANARY_OPERATION_PUBLISH_QOS0,
        40);
    s_aws_mqtt5_canary_add_operation_distribution(
        tester_options,
        &distribution_storage[AWS_MQTT5_CANARY_OPERATION_PUBLISH_QOS1],
        distributions,
        AWS_MQTT5_CANARY_OPERATION_PUBLISH_QOS1,
        30);
    s_aws_mqtt5_canary_add_operation_distribution(
        tester_options,
        &distribution_storage[AWS_MQTT5_CANARY_OPERATION_PUBLISH_TO_SUBSCRIBED_TOPIC_QOS0],
        distributions,
        AWS_MQTT5_CANARY_OPERATION_PUBLISH_TO_SUBSCRIBED_TOPIC_QOS0,
        20);
    s_aws_mqtt5_canary_add_operation_distribution(
        tester_options,
        &distribution_storage[AWS_MQTT5_CANARY_OPERATION_PUBLISH_TO_SUBSCRIBED_TOPIC_QOS1],
        distributions,
        AWS_MQTT5_CANARY_OPERATION_PUBLISH_TO_SUBSCRIBED_TOPIC_QOS1,
        20);
    s_aws_mqtt5_canary_add_operation_distribution(
        tester_options,
        &distribution_storage[AWS_MQTT5_CANARY_OPERATION_PUBLISH_TO_SHARED_TOPIC_QOS0],
        distributions,
        AWS_MQTT5_CANARY_OPERATION_PUBLISH_TO_SHARED_TOPIC_QOS0,
        10);
    s_aws_mqtt5_canary_add_operation_distribution(
        tester_options,
        &distribution_storage[AWS_MQTT5_CANARY_OPERATION_PUBLISH_TO_SHARED_TOPIC_QOS1],
        distributions,
        AWS_MQTT5_CANARY_OPERATION_PUBLISH_TO_SHARED_TOPIC_QOS1,
        10);
}

static enum aws_mqtt5_canary_operations s_aws_mqtt5_canary_get_next_random_operation(
    struct aws_mqtt5_canary_tester_options *tester_options,
    struct aws_linked_list *distributions) {
    size_t next_weighted = rand() % tester_options->distributions_total;

    struct aws_linked_list_node *node = aws_linked_list_begin(distributions);
    while (node != aws_linked_list_end(distributions)) {
        struct operation_distribution *operation = AWS_CONTAINER_OF(node, struct operation_distribution, node);
        if (next_weighted < operation->probability) {
            return operation->operation;
        }
        next_weighted -= operation->probability;
        node = aws_linked_list_next(node);
    }

    return AWS_MQTT5_CANARY_OPERATION_NULL;
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

    AWS_LOGF_INFO(AWS_LS_MQTT5_CANARY, "ID:" PRInSTR " Stop", AWS_BYTE_CURSOR_PRI(test_client->client_id));
    return AWS_OP_SUCCESS;
}

static int s_aws_mqtt5_canary_operation_subscribe(struct aws_mqtt5_canary_test_client *test_client) {
    if (!test_client->is_connected) {
        return s_aws_mqtt5_canary_operation_start(test_client);
    }
    char topic_array[256] = "";
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
            .topic_filter = aws_byte_cursor_from_c_str("shared_topic"),
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

    test_client->subscription_count++;

    AWS_LOGF_INFO(
        AWS_LS_MQTT5_CANARY,
        "ID:" PRInSTR " Subscribe to topic: " PRInSTR,
        AWS_BYTE_CURSOR_PRI(test_client->client_id),
        AWS_BYTE_CURSOR_PRI(subscriptions->topic_filter));
    return aws_mqtt5_client_subscribe(test_client->client, &subscribe_view, NULL);
}

static int s_aws_mqtt5_canary_operation_unsubscribe_bad(struct aws_mqtt5_canary_test_client *test_client) {
    if (!test_client->is_connected) {
        return s_aws_mqtt5_canary_operation_start(test_client);
    }
    char topic_array[256] = "";
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
    char topic_array[256] = "";
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

    AWS_LOGF_INFO(
        AWS_LS_MQTT5_CANARY,
        "ID:" PRInSTR " Unsubscribe from topic: " PRInSTR,
        AWS_BYTE_CURSOR_PRI(test_client->client_id),
        AWS_BYTE_CURSOR_PRI(topic));
    return aws_mqtt5_client_unsubscribe(test_client->client, &unsubscribe_view, NULL);
}

static int s_aws_mqtt5_canary_operation_publish(
    struct aws_mqtt5_canary_test_client *test_client,
    struct aws_byte_cursor topic_filter,
    enum aws_mqtt5_qos qos) {

    uint16_t payload_size = rand() % UINT16_MAX;
    uint8_t payload_data[payload_size];

    struct aws_mqtt5_packet_publish_view packet_publish_view = {
        .qos = qos,
        .topic = topic_filter,
        .retain = false,
        .duplicate = false,
        .payload =
            {
                .ptr = payload_data,
                .len = AWS_ARRAY_SIZE(payload_data) - 1,
            },
    };

    return aws_mqtt5_client_publish(test_client->client, &packet_publish_view, NULL);
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

    char topic_array[256] = "";
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

    char topic_array[256] = "";
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
    struct aws_byte_cursor topic_cursor;
    AWS_ZERO_STRUCT(topic_cursor);
    topic_cursor = aws_byte_cursor_from_c_str("shared_topic");
    AWS_LOGF_INFO(
        AWS_LS_MQTT5_CANARY,
        "ID:" PRInSTR " Publish qos 0 to shared topic: " PRInSTR,
        AWS_BYTE_CURSOR_PRI(test_client->client_id),
        AWS_BYTE_CURSOR_PRI(topic_cursor));
    return s_aws_mqtt5_canary_operation_publish(test_client, topic_cursor, AWS_MQTT5_QOS_AT_MOST_ONCE);
}

static int s_aws_mqtt5_canary_operation_publish_to_shared_topic_qos1(struct aws_mqtt5_canary_test_client *test_client) {
    if (!test_client->is_connected) {
        return s_aws_mqtt5_canary_operation_start(test_client);
    }
    struct aws_byte_cursor topic_cursor;
    AWS_ZERO_STRUCT(topic_cursor);
    topic_cursor = aws_byte_cursor_from_c_str("shared_topic");
    AWS_LOGF_INFO(
        AWS_LS_MQTT5_CANARY,
        "ID:" PRInSTR " Publish qos 1 to shared topic: " PRInSTR,
        AWS_BYTE_CURSOR_PRI(test_client->client_id),
        AWS_BYTE_CURSOR_PRI(topic_cursor));
    return s_aws_mqtt5_canary_operation_publish(test_client, topic_cursor, AWS_MQTT5_QOS_AT_LEAST_ONCE);
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
    app_ctx.port = 1883;

    struct aws_mqtt5_canary_tester_options tester_options;
    AWS_ZERO_STRUCT(tester_options);
    s_aws_mqtt5_canary_init_tester_options(&tester_options);

    s_parse_options(argc, argv, &app_ctx, &tester_options);
    if (app_ctx.uri.port) {
        app_ctx.port = app_ctx.uri.port;
    }

    s_aws_mqtt5_canary_update_tps_sleep_time(&tester_options);

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
     * OPERATION DISTRIBUTION
     **********************************************************/
    struct operation_distribution distributions_storage[AWS_MQTT5_CANARY_DISTRIBUTIONS_COUNT];
    AWS_ZERO_STRUCT(distributions_storage);

    struct aws_linked_list distributions;
    aws_linked_list_init(&distributions);
    s_aws_mqtt5_canary_init_operation_distributions(&tester_options, distributions_storage, &distributions);

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
        .outbound_topic_aliasing_behavior = AWS_MQTT5_COTABT_LRU,
        .lifecycle_event_handler = s_lifecycle_event_callback,
        .retry_jitter_mode = AWS_EXPONENTIAL_BACKOFF_JITTER_NONE,
        .min_reconnect_delay_ms = 1000,
        .max_reconnect_delay_ms = 120000,
        .min_connected_time_to_reset_reconnect_delay_ms = 30000,
        .ping_timeout_ms = 10000,
        .websocket_handshake_transform = websocket_handshake_transform,
        .websocket_handshake_transform_user_data = websocket_handshake_transform_user_data,
        .publish_received_handler = s_on_publish_received,
    };

    struct aws_mqtt5_canary_test_client clients[tester_options.client_count];
    AWS_ZERO_STRUCT(clients);

    for (size_t i = 0; i < tester_options.client_count; ++i) {

        client_options.lifecycle_event_handler_user_data = &clients[i];
        client_options.publish_received_handler_user_data = &clients[i];

        clients[i].index = i;

        clients[i].client = aws_mqtt5_client_new(allocator, &client_options);

        aws_mqtt5_canary_operation_fn *operation_fn =
            s_aws_mqtt5_canary_operation_table.operation_by_operation_type[AWS_MQTT5_CANARY_OPERATION_START];
        (*operation_fn)(&clients[i]);

        aws_thread_current_sleep(10000000);
    }

    fprintf(stderr, "Clients created\n");

    /**********************************************************
     * TESTING
     **********************************************************/
    bool done = false;
    size_t operations = 0;
    uint64_t time_test_finish = 0;
    aws_high_res_clock_get_ticks(&time_test_finish);
    uint64_t test_time = 0;
    test_time = tester_options.test_run_hours * 60;
    test_time = (test_time + tester_options.test_run_minutes) * 60;
    test_time += tester_options.test_run_seconds;
    time_test_finish += aws_timestamp_convert(test_time, AWS_TIMESTAMP_SECS, AWS_TIMESTAMP_NANOS, NULL);
    uint64_t time_next_op = 0;

    printf(
        "Running test for %zu hours %zu minutes %zu seconds\n",
        tester_options.test_run_hours,
        tester_options.test_run_minutes,
        tester_options.test_run_seconds);
    aws_high_res_clock_get_ticks(&time_next_op);

    while (!done) {
        uint64_t now = 0;
        aws_high_res_clock_get_ticks(&now);
        if (now >= time_next_op) {
            time_next_op += tester_options.tps_sleep_time;
            operations++;

            enum aws_mqtt5_canary_operations next_operation =
                s_aws_mqtt5_canary_get_next_random_operation(&tester_options, &distributions);
            aws_mqtt5_canary_operation_fn *operation_fn =
                s_aws_mqtt5_canary_operation_table.operation_by_operation_type[next_operation];

            if (tester_options.apply_operations_to_all_clients) {
                for (size_t i = 0; i < tester_options.client_count; ++i) {
                    (*operation_fn)(&clients[i]);
                }
            } else {
                (*operation_fn)(&clients[rand() % tester_options.client_count]);
            }
        }
        if (now > time_test_finish) {
            done = true;
        }
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

    aws_linked_list_empty(&distributions);

    const size_t outstanding_bytes = aws_mem_tracer_bytes(allocator);
    printf("Summary:\n");
    printf("   Outstanding bytes: %zu\n\n", outstanding_bytes);

    if (app_ctx.log_level) {
        aws_logger_set(NULL);
        aws_logger_clean_up(&logger);
    }

    aws_uri_clean_up(&app_ctx.uri);

    aws_mqtt_library_clean_up();

    printf("   Operations executed: %zu\n", operations);

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
