/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/command_line_parser.h>
#include <aws/common/condition_variable.h>
#include <aws/common/hash_table.h>
#include <aws/common/log_channel.h>
#include <aws/common/mutex.h>
#include <aws/common/string.h>
#include <aws/common/uuid.h>

#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/io/socket.h>
#include <aws/io/tls_channel_handler.h>
#include <aws/io/uri.h>

#include <aws/mqtt/private/shared.h>
#include <aws/mqtt/request-response/request_response_client.h>
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
    const char *cert;
    const char *key;

    struct aws_tls_connection_options tls_connection_options;

    const char *log_filename;
    enum aws_log_level log_level;

    struct aws_mqtt5_client *client;
    struct aws_mqtt_request_response_client *rr_client;

    /*
     * &aws_shadow_streaming_operation.id -> aws_shadow_streaming_operation *
     */
    struct aws_hash_table streaming_operations;
    uint64_t next_id;
};

struct aws_shadow_streaming_operation {
    struct aws_allocator *allocator;

    struct aws_byte_buf thing;
    struct aws_byte_buf shadow;
    struct aws_byte_buf topic_filter;

    uint64_t id;
    struct aws_mqtt_rr_client_operation *streaming_operation;
};

struct aws_shadow_streaming_operation *s_aws_shadow_streaming_operation_new(
    struct aws_allocator *allocator,
    struct app_ctx *app_ctx,
    struct aws_byte_cursor thing,
    struct aws_byte_cursor shadow,
    struct aws_byte_cursor topic_filter) {
    struct aws_shadow_streaming_operation *operation =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_shadow_streaming_operation));

    operation->allocator = allocator;
    operation->id = ++app_ctx->next_id;
    operation->streaming_operation = NULL;

    aws_byte_buf_init_copy_from_cursor(&operation->thing, allocator, thing);
    aws_byte_buf_init_copy_from_cursor(&operation->shadow, allocator, shadow);
    aws_byte_buf_init_copy_from_cursor(&operation->topic_filter, allocator, topic_filter);

    return operation;
}

void s_aws_shadow_streaming_operation_destroy(struct aws_shadow_streaming_operation *operation) {
    if (operation == NULL) {
        return;
    }

    aws_byte_buf_clean_up(&operation->thing);
    aws_byte_buf_clean_up(&operation->shadow);
    aws_byte_buf_clean_up(&operation->topic_filter);

    aws_mem_release(operation->allocator, operation);
}

static void s_release_streaming_operation(void *value) {
    struct aws_shadow_streaming_operation *operation = value;

    if (operation->streaming_operation != NULL) {
        aws_mqtt_rr_client_operation_release(operation->streaming_operation);
    } else {
        s_aws_shadow_streaming_operation_destroy(operation);
    }
}

static void s_usage(int exit_code) {

    fprintf(stderr, "usage: elastishadow [options] endpoint\n");
    fprintf(stderr, " endpoint: url to connect to\n");
    fprintf(stderr, "\n Options:\n\n");
    fprintf(stderr, "      --cert FILE: path to a PEM encoded certificate to use with mTLS\n");
    fprintf(stderr, "      --key FILE: Path to a PEM encoded private key that matches cert.\n");
    fprintf(stderr, "  -l, --log FILE: dumps logs to FILE instead of stderr.\n");
    fprintf(stderr, "  -v, --verbose: ERROR|INFO|DEBUG|TRACE: log level to configure. Default is none.\n");
    fprintf(stderr, "  -h, --help\n");
    fprintf(stderr, "            Display this message and quit.\n");
    exit(exit_code);
}

static struct aws_cli_option s_long_options[] = {
    {"cert", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'c'},
    {"key", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'e'},
    {"log", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'l'},
    {"verbose", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'v'},
    {"help", AWS_CLI_OPTIONS_NO_ARGUMENT, NULL, 'h'},
    /* Per getopt(3) the last element of the array has to be filled with all zeros */
    {NULL, AWS_CLI_OPTIONS_NO_ARGUMENT, NULL, 0},
};

static void s_parse_options(int argc, char **argv, struct app_ctx *ctx) {
    bool uri_found = false;

    while (true) {
        int option_index = 0;
        int c = aws_cli_getopt_long(argc, argv, "c:e:l:v:h", s_long_options, &option_index);
        if (c == -1) {
            break;
        }

        switch (c) {
            case 0:
                /* getopt_long() returns 0 if an option.flag is non-null */
                break;
            case 'c':
                ctx->cert = aws_cli_optarg;
                break;
            case 'e':
                ctx->key = aws_cli_optarg;
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

static void s_write_correlation_token_string(struct aws_byte_cursor scratch_space) {
    struct aws_byte_buf correlation_token_buf = aws_byte_buf_from_empty_array(scratch_space.ptr, scratch_space.len);

    struct aws_uuid uuid;
    aws_uuid_init(&uuid);
    aws_uuid_to_str(&uuid, &correlation_token_buf);
}

static void s_on_get_shadow_complete(
    const struct aws_byte_cursor *response_topic,
    const struct aws_byte_cursor *payload,
    int error_code,
    void *user_data) {

    struct aws_string *correlation_token = user_data;

    if (payload != NULL) {
        printf(
            "GetNamedShadow request '%s' response received on topic '" PRInSTR "' with body:\n  " PRInSTR "\n\n",
            correlation_token->bytes,
            AWS_BYTE_CURSOR_PRI(*response_topic),
            AWS_BYTE_CURSOR_PRI(*payload));
    } else {
        printf(
            "GetNamedShadow request '%s' failed with error code %d(%s)\n\n",
            correlation_token->bytes,
            error_code,
            aws_error_debug_str(error_code));
    }

    aws_string_destroy(correlation_token);
}

static void s_handle_get(struct app_ctx *context, struct aws_allocator *allocator, struct aws_array_list *arguments) {

    size_t argument_count = aws_array_list_length(arguments) - 1;
    if (argument_count != 2) {
        printf("invalid get-named-shadow options:\n");
        printf("  get-named-shadow <thing-name> <shadow-name>\n\n");
        return;
    }

    struct aws_byte_cursor thing_name_cursor;
    AWS_ZERO_STRUCT(thing_name_cursor);
    aws_array_list_get_at(arguments, &thing_name_cursor, 1);

    struct aws_byte_cursor shadow_name_cursor;
    AWS_ZERO_STRUCT(shadow_name_cursor);
    aws_array_list_get_at(arguments, &shadow_name_cursor, 2);

    char subscription_topic_filter[128];
    snprintf(
        subscription_topic_filter,
        AWS_ARRAY_SIZE(subscription_topic_filter),
        "$aws/things/" PRInSTR "/shadow/name/" PRInSTR "/get/+",
        AWS_BYTE_CURSOR_PRI(thing_name_cursor),
        AWS_BYTE_CURSOR_PRI(shadow_name_cursor));
    struct aws_byte_cursor subscription_topic_filter_cursor = aws_byte_cursor_from_c_str(subscription_topic_filter);

    char accepted_path[128];
    snprintf(
        accepted_path,
        AWS_ARRAY_SIZE(accepted_path),
        "$aws/things/" PRInSTR "/shadow/name/" PRInSTR "/get/accepted",
        AWS_BYTE_CURSOR_PRI(thing_name_cursor),
        AWS_BYTE_CURSOR_PRI(shadow_name_cursor));

    char rejected_path[128];
    snprintf(
        rejected_path,
        AWS_ARRAY_SIZE(rejected_path),
        "$aws/things/" PRInSTR "/shadow/name/" PRInSTR "/get/rejected",
        AWS_BYTE_CURSOR_PRI(thing_name_cursor),
        AWS_BYTE_CURSOR_PRI(shadow_name_cursor));

    struct aws_byte_cursor correlation_token_path = aws_byte_cursor_from_c_str("clientToken");
    struct aws_mqtt_request_operation_response_path response_paths[] = {
        {
            .topic = aws_byte_cursor_from_c_str(accepted_path),
            .correlation_token_json_path = correlation_token_path,
        },
        {
            .topic = aws_byte_cursor_from_c_str(rejected_path),
            .correlation_token_json_path = correlation_token_path,
        },
    };

    char publish_topic[128];
    snprintf(
        publish_topic,
        AWS_ARRAY_SIZE(publish_topic),
        "$aws/things/" PRInSTR "/shadow/name/" PRInSTR "/get",
        AWS_BYTE_CURSOR_PRI(thing_name_cursor),
        AWS_BYTE_CURSOR_PRI(shadow_name_cursor));

    char correlation_token[128];
    s_write_correlation_token_string(aws_byte_cursor_from_array(correlation_token, AWS_ARRAY_SIZE(correlation_token)));
    struct aws_string *correlation_token_string = aws_string_new_from_c_str(allocator, correlation_token);

    char request[256];
    snprintf(request, AWS_ARRAY_SIZE(request), "{\"clientToken\":\"%s\"}", correlation_token);

    struct aws_mqtt_request_operation_options get_options = {
        .subscription_topic_filters = &subscription_topic_filter_cursor,
        .subscription_topic_filter_count = 1,
        .response_paths = response_paths,
        .response_path_count = 2,
        .publish_topic = aws_byte_cursor_from_c_str(publish_topic),
        .serialized_request = aws_byte_cursor_from_c_str(request),
        .correlation_token = aws_byte_cursor_from_c_str(correlation_token),
        .completion_callback = s_on_get_shadow_complete,
        .user_data = correlation_token_string,
    };

    printf(
        "Submitting GetNamedShadow request for shadow '" PRInSTR "' of thing '" PRInSTR
        "' using correlation token %s...\n",
        AWS_BYTE_CURSOR_PRI(shadow_name_cursor),
        AWS_BYTE_CURSOR_PRI(thing_name_cursor),
        correlation_token);

    if (aws_mqtt_request_response_client_submit_request(context->rr_client, &get_options) == AWS_OP_ERR) {
        int error_code = aws_last_error();
        printf("GetNamedShadow synchronous failure: %d(%s)\n", error_code, aws_error_debug_str(error_code));
        aws_string_destroy(correlation_token_string);
    }

    printf("\n");
}

static void s_on_update_shadow_complete(
    const struct aws_byte_cursor *response_topic,
    const struct aws_byte_cursor *payload,
    int error_code,
    void *user_data) {

    struct aws_string *correlation_token = user_data;

    if (payload != NULL) {
        printf(
            "UpdateNamedShadow request '%s' response received on topic '" PRInSTR "' with body:\n  " PRInSTR "\n\n",
            correlation_token->bytes,
            AWS_BYTE_CURSOR_PRI(*response_topic),
            AWS_BYTE_CURSOR_PRI(*payload));
    } else {
        printf(
            "UpdateNamedShadow request '%s' failed with error code %d(%s)\n\n",
            correlation_token->bytes,
            error_code,
            aws_error_debug_str(error_code));
    }

    aws_string_destroy(correlation_token);
}

static void s_handle_update(
    struct app_ctx *context,
    struct aws_allocator *allocator,
    struct aws_array_list *arguments,
    struct aws_byte_cursor desired_state,
    struct aws_byte_cursor correlation_token) {

    struct aws_byte_cursor thing_name_cursor;
    AWS_ZERO_STRUCT(thing_name_cursor);
    aws_array_list_get_at(arguments, &thing_name_cursor, 1);

    struct aws_byte_cursor shadow_name_cursor;
    AWS_ZERO_STRUCT(shadow_name_cursor);
    aws_array_list_get_at(arguments, &shadow_name_cursor, 2);

    char subscription_topic_filter_accepted[128];
    snprintf(
        subscription_topic_filter_accepted,
        AWS_ARRAY_SIZE(subscription_topic_filter_accepted),
        "$aws/things/" PRInSTR "/shadow/name/" PRInSTR "/update/accepted",
        AWS_BYTE_CURSOR_PRI(thing_name_cursor),
        AWS_BYTE_CURSOR_PRI(shadow_name_cursor));
    struct aws_byte_cursor subscription_topic_filter_accepted_cursor =
        aws_byte_cursor_from_c_str(subscription_topic_filter_accepted);

    char subscription_topic_filter_rejected[128];
    snprintf(
        subscription_topic_filter_rejected,
        AWS_ARRAY_SIZE(subscription_topic_filter_rejected),
        "$aws/things/" PRInSTR "/shadow/name/" PRInSTR "/update/rejected",
        AWS_BYTE_CURSOR_PRI(thing_name_cursor),
        AWS_BYTE_CURSOR_PRI(shadow_name_cursor));
    struct aws_byte_cursor subscription_topic_filter_rejected_cursor =
        aws_byte_cursor_from_c_str(subscription_topic_filter_rejected);

    struct aws_byte_cursor subscription_topic_filters[] = {
        subscription_topic_filter_accepted_cursor,
        subscription_topic_filter_rejected_cursor,
    };

    char accepted_path[128];
    snprintf(
        accepted_path,
        AWS_ARRAY_SIZE(accepted_path),
        "$aws/things/" PRInSTR "/shadow/name/" PRInSTR "/update/accepted",
        AWS_BYTE_CURSOR_PRI(thing_name_cursor),
        AWS_BYTE_CURSOR_PRI(shadow_name_cursor));

    char rejected_path[128];
    snprintf(
        rejected_path,
        AWS_ARRAY_SIZE(rejected_path),
        "$aws/things/" PRInSTR "/shadow/name/" PRInSTR "/update/rejected",
        AWS_BYTE_CURSOR_PRI(thing_name_cursor),
        AWS_BYTE_CURSOR_PRI(shadow_name_cursor));

    struct aws_byte_cursor correlation_token_path = aws_byte_cursor_from_c_str("clientToken");
    struct aws_mqtt_request_operation_response_path response_paths[] = {
        {
            .topic = aws_byte_cursor_from_c_str(accepted_path),
            .correlation_token_json_path = correlation_token_path,
        },
        {
            .topic = aws_byte_cursor_from_c_str(rejected_path),
            .correlation_token_json_path = correlation_token_path,
        },
    };

    char publish_topic[128];
    snprintf(
        publish_topic,
        AWS_ARRAY_SIZE(publish_topic),
        "$aws/things/" PRInSTR "/shadow/name/" PRInSTR "/update",
        AWS_BYTE_CURSOR_PRI(thing_name_cursor),
        AWS_BYTE_CURSOR_PRI(shadow_name_cursor));

    struct aws_string *correlation_token_string = aws_string_new_from_cursor(allocator, &correlation_token);

    struct aws_mqtt_request_operation_options get_options = {
        .subscription_topic_filters = subscription_topic_filters,
        .subscription_topic_filter_count = 2,
        .response_paths = response_paths,
        .response_path_count = 2,
        .publish_topic = aws_byte_cursor_from_c_str(publish_topic),
        .serialized_request = desired_state,
        .correlation_token = correlation_token,
        .completion_callback = s_on_update_shadow_complete,
        .user_data = correlation_token_string,
    };

    printf(
        "Submitting UpdateNamedShadow request for shadow '" PRInSTR "' of thing '" PRInSTR
        "' using correlation token '" PRInSTR "'...\n",
        AWS_BYTE_CURSOR_PRI(shadow_name_cursor),
        AWS_BYTE_CURSOR_PRI(thing_name_cursor),
        AWS_BYTE_CURSOR_PRI(correlation_token));

    if (aws_mqtt_request_response_client_submit_request(context->rr_client, &get_options) == AWS_OP_ERR) {
        int error_code = aws_last_error();
        printf("UpdateNamedShadow synchronous failure: %d(%s)\n", error_code, aws_error_debug_str(error_code));
        aws_string_destroy(correlation_token_string);
    }

    printf("\n");
}

static void s_handle_update_desired(
    struct app_ctx *context,
    struct aws_allocator *allocator,
    struct aws_array_list *arguments,
    struct aws_byte_cursor line_cursor) {

    size_t argument_count = aws_array_list_length(arguments) - 1;
    if (argument_count < 3) {
        printf("invalid update-named-shadow-desired options:\n");
        printf("  update-named-shadow-desired <thing-name> <shadow-name> <desired state JSON blob>\n\n");
        return;
    }

    struct aws_byte_cursor desired_state_cursor;
    aws_array_list_get_at(arguments, &desired_state_cursor, 3);
    desired_state_cursor.len = (size_t)(line_cursor.ptr + line_cursor.len - desired_state_cursor.ptr);

    char correlation_token[128];
    s_write_correlation_token_string(aws_byte_cursor_from_array(correlation_token, AWS_ARRAY_SIZE(correlation_token)));

    char request[256];
    snprintf(
        request,
        AWS_ARRAY_SIZE(request),
        "{\"clientToken\":\"%s\",\"state\":{\"desired\":" PRInSTR "}}",
        correlation_token,
        AWS_BYTE_CURSOR_PRI(desired_state_cursor));

    s_handle_update(
        context,
        allocator,
        arguments,
        aws_byte_cursor_from_c_str(request),
        aws_byte_cursor_from_c_str(correlation_token));
}

static void s_handle_update_reported(
    struct app_ctx *context,
    struct aws_allocator *allocator,
    struct aws_array_list *arguments,
    struct aws_byte_cursor line_cursor) {

    size_t argument_count = aws_array_list_length(arguments) - 1;
    if (argument_count < 3) {
        printf("invalid update-named-shadow-reported options:\n");
        printf("  update-named-shadow-reported <thing-name> <shadow-name> <reported state JSON blob>\n\n");
        return;
    }

    struct aws_byte_cursor reported_state_cursor;
    aws_array_list_get_at(arguments, &reported_state_cursor, 3);
    reported_state_cursor.len = (size_t)(line_cursor.ptr + line_cursor.len - reported_state_cursor.ptr);

    char correlation_token[128];
    s_write_correlation_token_string(aws_byte_cursor_from_array(correlation_token, AWS_ARRAY_SIZE(correlation_token)));

    char request[256];
    snprintf(
        request,
        AWS_ARRAY_SIZE(request),
        "{\"clientToken\":\"%s\",\"state\":{\"reported\":" PRInSTR "}}",
        correlation_token,
        AWS_BYTE_CURSOR_PRI(reported_state_cursor));

    s_handle_update(
        context,
        allocator,
        arguments,
        aws_byte_cursor_from_c_str(request),
        aws_byte_cursor_from_c_str(correlation_token));
}

static void s_on_delete_shadow_complete(
    const struct aws_byte_cursor *response_topic,
    const struct aws_byte_cursor *payload,
    int error_code,
    void *user_data) {

    struct aws_string *correlation_token = user_data;

    if (payload != NULL) {
        printf(
            "DeleteNamedShadow request '%s' response received on topic '" PRInSTR "' with body:\n  " PRInSTR "\n\n",
            correlation_token->bytes,
            AWS_BYTE_CURSOR_PRI(*response_topic),
            AWS_BYTE_CURSOR_PRI(*payload));
    } else {
        printf(
            "DeleteNamedShadow request '%s' failed with error code %d(%s)\n\n",
            correlation_token->bytes,
            error_code,
            aws_error_debug_str(error_code));
    }

    aws_string_destroy(correlation_token);
}

static void s_handle_delete(
    struct app_ctx *context,
    struct aws_allocator *allocator,
    struct aws_array_list *arguments) {

    size_t argument_count = aws_array_list_length(arguments) - 1;
    if (argument_count != 2) {
        printf("invalid delete-named-shadow options:\n");
        printf("  delete-named-shadow <thing-name> <shadow-name>\n\n");
        return;
    }

    struct aws_byte_cursor thing_name_cursor;
    AWS_ZERO_STRUCT(thing_name_cursor);
    aws_array_list_get_at(arguments, &thing_name_cursor, 1);

    struct aws_byte_cursor shadow_name_cursor;
    AWS_ZERO_STRUCT(shadow_name_cursor);
    aws_array_list_get_at(arguments, &shadow_name_cursor, 2);

    char subscription_topic_filter[128];
    snprintf(
        subscription_topic_filter,
        AWS_ARRAY_SIZE(subscription_topic_filter),
        "$aws/things/" PRInSTR "/shadow/name/" PRInSTR "/delete/+",
        AWS_BYTE_CURSOR_PRI(thing_name_cursor),
        AWS_BYTE_CURSOR_PRI(shadow_name_cursor));
    struct aws_byte_cursor subscription_topic_filter_cursor = aws_byte_cursor_from_c_str(subscription_topic_filter);

    char accepted_path[128];
    snprintf(
        accepted_path,
        AWS_ARRAY_SIZE(accepted_path),
        "$aws/things/" PRInSTR "/shadow/name/" PRInSTR "/delete/accepted",
        AWS_BYTE_CURSOR_PRI(thing_name_cursor),
        AWS_BYTE_CURSOR_PRI(shadow_name_cursor));

    char rejected_path[128];
    snprintf(
        rejected_path,
        AWS_ARRAY_SIZE(rejected_path),
        "$aws/things/" PRInSTR "/shadow/name/" PRInSTR "/delete/rejected",
        AWS_BYTE_CURSOR_PRI(thing_name_cursor),
        AWS_BYTE_CURSOR_PRI(shadow_name_cursor));

    struct aws_byte_cursor correlation_token_path = aws_byte_cursor_from_c_str("clientToken");
    struct aws_mqtt_request_operation_response_path response_paths[] = {
        {
            .topic = aws_byte_cursor_from_c_str(accepted_path),
            .correlation_token_json_path = correlation_token_path,
        },
        {
            .topic = aws_byte_cursor_from_c_str(rejected_path),
            .correlation_token_json_path = correlation_token_path,
        },
    };

    char publish_topic[128];
    snprintf(
        publish_topic,
        AWS_ARRAY_SIZE(publish_topic),
        "$aws/things/" PRInSTR "/shadow/name/" PRInSTR "/delete",
        AWS_BYTE_CURSOR_PRI(thing_name_cursor),
        AWS_BYTE_CURSOR_PRI(shadow_name_cursor));

    char correlation_token[128];
    struct aws_byte_buf correlation_token_buf =
        aws_byte_buf_from_empty_array(correlation_token, AWS_ARRAY_SIZE(correlation_token));

    struct aws_uuid uuid;
    aws_uuid_init(&uuid);
    aws_uuid_to_str(&uuid, &correlation_token_buf);

    char request[256];
    snprintf(request, AWS_ARRAY_SIZE(request), "{\"clientToken\":\"%s\"}", correlation_token);
    struct aws_string *correlation_token_string = aws_string_new_from_c_str(allocator, correlation_token);

    struct aws_mqtt_request_operation_options get_options = {
        .subscription_topic_filters = &subscription_topic_filter_cursor,
        .subscription_topic_filter_count = 1,
        .response_paths = response_paths,
        .response_path_count = 2,
        .publish_topic = aws_byte_cursor_from_c_str(publish_topic),
        .serialized_request = aws_byte_cursor_from_c_str(request),
        .correlation_token = aws_byte_cursor_from_c_str(correlation_token),
        .completion_callback = s_on_delete_shadow_complete,
        .user_data = correlation_token_string,
    };

    printf(
        "Submitting DeleteNamedShadow request for shadow '" PRInSTR "' of thing '" PRInSTR
        "' using correlation token %s...\n",
        AWS_BYTE_CURSOR_PRI(shadow_name_cursor),
        AWS_BYTE_CURSOR_PRI(thing_name_cursor),
        correlation_token);

    if (aws_mqtt_request_response_client_submit_request(context->rr_client, &get_options) == AWS_OP_ERR) {
        int error_code = aws_last_error();
        printf("DeleteNamedShadow synchronous failure: %d(%s)\n", error_code, aws_error_debug_str(error_code));
        aws_string_destroy(correlation_token_string);
    }

    printf("\n");
}

static int s_print_streaming_operation(void *context, struct aws_hash_element *elem) {
    (void)context;
    struct aws_shadow_streaming_operation *operation = elem->value;

    struct aws_byte_cursor thing_cursor = aws_byte_cursor_from_buf(&operation->thing);
    struct aws_byte_cursor shadow_cursor = aws_byte_cursor_from_buf(&operation->shadow);
    struct aws_byte_cursor topic_filter_cursor = aws_byte_cursor_from_buf(&operation->topic_filter);

    printf(
        "  %" PRIu64 "   Thing:'" PRInSTR "', Shadow: '" PRInSTR "', TopicFilter:'" PRInSTR "'\n",
        operation->id,
        AWS_BYTE_CURSOR_PRI(thing_cursor),
        AWS_BYTE_CURSOR_PRI(shadow_cursor),
        AWS_BYTE_CURSOR_PRI(topic_filter_cursor));

    return AWS_COMMON_HASH_TABLE_ITER_CONTINUE;
}

static void s_handle_list_streams(
    struct app_ctx *context,
    struct aws_allocator *allocator,
    struct aws_array_list *arguments) {
    (void)allocator;
    (void)arguments;

    printf("Open Streams:\n");
    aws_hash_table_foreach(&context->streaming_operations, s_print_streaming_operation, NULL);
    printf("\n");
}

static const char *s_rr_streaming_subscription_event_type_to_c_str(
    enum aws_rr_streaming_subscription_event_type status) {
    switch (status) {
        case ARRSSET_SUBSCRIPTION_ESTABLISHED:
            return "SubscriptionEstablished";

        case ARRSSET_SUBSCRIPTION_LOST:
            return "SubscriptionLost";

        case ARRSSET_SUBSCRIPTION_HALTED:
            return "SubscriptionHalted";

        default:
            return "Unknown";
    }
}

static void s_stream_subscription_status_fn(
    enum aws_rr_streaming_subscription_event_type status,
    int error_code,
    void *user_data) {

    struct aws_shadow_streaming_operation *operation = user_data;

    struct aws_byte_cursor topic_filter_cursor = aws_byte_cursor_from_buf(&operation->topic_filter);

    printf(
        "Streaming operation %" PRIu64 " received subscription status event on topic filter '" PRInSTR "':\n",
        operation->id,
        AWS_BYTE_CURSOR_PRI(topic_filter_cursor));
    printf(
        "  Status: %d(%s), ErrorCode: %d(%s)\n\n",
        status,
        s_rr_streaming_subscription_event_type_to_c_str(status),
        error_code,
        aws_error_debug_str(error_code));
}

static void s_stream_incoming_publish_fn(
    struct aws_byte_cursor payload,
    struct aws_byte_cursor topic,
    void *user_data) {
    (void)topic;
    struct aws_shadow_streaming_operation *operation = user_data;

    struct aws_byte_cursor thing_cursor = aws_byte_cursor_from_buf(&operation->thing);
    struct aws_byte_cursor shadow_cursor = aws_byte_cursor_from_buf(&operation->shadow);
    struct aws_byte_cursor topic_filter_cursor = aws_byte_cursor_from_buf(&operation->topic_filter);

    printf(
        "Streaming operation %" PRIu64 " ('" PRInSTR "', ' " PRInSTR "') received publish on topic filter '" PRInSTR
        "':\n",
        operation->id,
        AWS_BYTE_CURSOR_PRI(thing_cursor),
        AWS_BYTE_CURSOR_PRI(shadow_cursor),
        AWS_BYTE_CURSOR_PRI(topic_filter_cursor));
    printf("  " PRInSTR "\n\n", AWS_BYTE_CURSOR_PRI(payload));
}

static void s_stream_terminated_fn(void *user_data) {
    struct aws_shadow_streaming_operation *operation = user_data;

    printf("Stream %" PRIu64 " terminated\n\n", operation->id);

    s_aws_shadow_streaming_operation_destroy(operation);
}

static void s_handle_open_delta_stream(
    struct app_ctx *context,
    struct aws_allocator *allocator,
    struct aws_array_list *arguments) {

    size_t argument_count = aws_array_list_length(arguments) - 1;
    if (argument_count != 2) {
        printf("invalid open-named-shadow-delta-stream options:\n");
        printf("  open-named-shadow-delta-stream <thing-name> <shadow-name>\n\n");
        return;
    }

    struct aws_byte_cursor thing_name_cursor;
    AWS_ZERO_STRUCT(thing_name_cursor);
    aws_array_list_get_at(arguments, &thing_name_cursor, 1);

    struct aws_byte_cursor shadow_name_cursor;
    AWS_ZERO_STRUCT(shadow_name_cursor);
    aws_array_list_get_at(arguments, &shadow_name_cursor, 2);

    char subscription_topic_filter[128];
    snprintf(
        subscription_topic_filter,
        AWS_ARRAY_SIZE(subscription_topic_filter),
        "$aws/things/" PRInSTR "/shadow/name/" PRInSTR "/update/delta",
        AWS_BYTE_CURSOR_PRI(thing_name_cursor),
        AWS_BYTE_CURSOR_PRI(shadow_name_cursor));

    struct aws_shadow_streaming_operation *operation = s_aws_shadow_streaming_operation_new(
        allocator,
        context,
        thing_name_cursor,
        shadow_name_cursor,
        aws_byte_cursor_from_c_str(subscription_topic_filter));
    aws_hash_table_put(&context->streaming_operations, &operation->id, operation, NULL);

    struct aws_mqtt_streaming_operation_options open_stream_options = {
        .topic_filter = aws_byte_cursor_from_c_str(subscription_topic_filter),
        .subscription_status_callback = s_stream_subscription_status_fn,
        .incoming_publish_callback = s_stream_incoming_publish_fn,
        .terminated_callback = s_stream_terminated_fn,
        .user_data = operation,
    };

    printf(
        "Opening NamedShadow delta stream with id %" PRIu64 " for shadow '" PRInSTR "' of thing '" PRInSTR "'...\n",
        operation->id,
        AWS_BYTE_CURSOR_PRI(shadow_name_cursor),
        AWS_BYTE_CURSOR_PRI(thing_name_cursor));

    operation->streaming_operation =
        aws_mqtt_request_response_client_create_streaming_operation(context->rr_client, &open_stream_options);
    if (operation->streaming_operation == NULL ||
        aws_mqtt_rr_client_operation_activate(operation->streaming_operation)) {
        int error_code = aws_last_error();
        printf(
            "NamedShadow delta stream synchronous open failure: %d(%s)\n", error_code, aws_error_debug_str(error_code));

        aws_hash_table_remove(&context->streaming_operations, &operation->id, NULL, NULL);
    }

    printf("\n");
}

static void s_handle_open_document_stream(
    struct app_ctx *context,
    struct aws_allocator *allocator,
    struct aws_array_list *arguments) {

    size_t argument_count = aws_array_list_length(arguments) - 1;
    if (argument_count != 2) {
        printf("invalid open-named-shadow-document-stream options:\n");
        printf("  open-named-shadow-document-stream <thing-name> <shadow-name>\n\n");
        return;
    }

    struct aws_byte_cursor thing_name_cursor;
    AWS_ZERO_STRUCT(thing_name_cursor);
    aws_array_list_get_at(arguments, &thing_name_cursor, 1);

    struct aws_byte_cursor shadow_name_cursor;
    AWS_ZERO_STRUCT(shadow_name_cursor);
    aws_array_list_get_at(arguments, &shadow_name_cursor, 2);

    char subscription_topic_filter[128];
    snprintf(
        subscription_topic_filter,
        AWS_ARRAY_SIZE(subscription_topic_filter),
        "$aws/things/" PRInSTR "/shadow/name/" PRInSTR "/update/document",
        AWS_BYTE_CURSOR_PRI(thing_name_cursor),
        AWS_BYTE_CURSOR_PRI(shadow_name_cursor));

    struct aws_shadow_streaming_operation *operation = s_aws_shadow_streaming_operation_new(
        allocator,
        context,
        thing_name_cursor,
        shadow_name_cursor,
        aws_byte_cursor_from_c_str(subscription_topic_filter));
    aws_hash_table_put(&context->streaming_operations, &operation->id, operation, NULL);

    struct aws_mqtt_streaming_operation_options open_stream_options = {
        .topic_filter = aws_byte_cursor_from_c_str(subscription_topic_filter),
        .subscription_status_callback = s_stream_subscription_status_fn,
        .incoming_publish_callback = s_stream_incoming_publish_fn,
        .terminated_callback = s_stream_terminated_fn,
        .user_data = operation,
    };

    printf(
        "Opening NamedShadow document stream with id %" PRIu64 " for shadow '" PRInSTR "' of thing '" PRInSTR "'...\n",
        operation->id,
        AWS_BYTE_CURSOR_PRI(shadow_name_cursor),
        AWS_BYTE_CURSOR_PRI(thing_name_cursor));

    operation->streaming_operation =
        aws_mqtt_request_response_client_create_streaming_operation(context->rr_client, &open_stream_options);
    if (operation->streaming_operation == NULL ||
        aws_mqtt_rr_client_operation_activate(operation->streaming_operation)) {
        int error_code = aws_last_error();
        printf(
            "NamedShadow document stream synchronous open failure: %d(%s)\n",
            error_code,
            aws_error_debug_str(error_code));

        aws_hash_table_remove(&context->streaming_operations, &operation->id, NULL, NULL);
    }

    printf("\n");
}

static void s_handle_close_stream(
    struct app_ctx *context,
    struct aws_allocator *allocator,
    struct aws_array_list *arguments) {
    (void)allocator;

    size_t argument_count = aws_array_list_length(arguments) - 1;
    if (argument_count != 1) {
        printf("invalid close-stream options:\n");
        printf("  close-stream <id>\n\n");
        return;
    }

    struct aws_byte_cursor id_cursor;
    AWS_ZERO_STRUCT(id_cursor);
    aws_array_list_get_at(arguments, &id_cursor, 1);

    char id_buffer[32];
    snprintf(id_buffer, AWS_ARRAY_SIZE(id_buffer), PRInSTR, AWS_BYTE_CURSOR_PRI(id_cursor));
    uint64_t id = atoi(id_buffer);

    int was_present = 0;
    aws_hash_table_remove(&context->streaming_operations, &id, NULL, &was_present);

    if (was_present == 0) {
        printf("Stream %" PRIu64 " does not exist\n\n", id);
    } else {
        printf("Closing stream %" PRIu64 "\n\n", id);
    }
}

static bool s_handle_input(struct app_ctx *context, struct aws_allocator *allocator, const char *input_line) {

    struct aws_mqtt5_client *client = context->client;

    struct aws_byte_cursor quit_cursor = aws_byte_cursor_from_c_str("quit");
    struct aws_byte_cursor start_cursor = aws_byte_cursor_from_c_str("start");
    struct aws_byte_cursor stop_cursor = aws_byte_cursor_from_c_str("stop");
    struct aws_byte_cursor get_cursor = aws_byte_cursor_from_c_str("get-named-shadow");
    struct aws_byte_cursor update_reported_cursor = aws_byte_cursor_from_c_str("update-named-shadow-reported");
    struct aws_byte_cursor update_desired_cursor = aws_byte_cursor_from_c_str("update-named-shadow-desired");
    struct aws_byte_cursor delete_cursor = aws_byte_cursor_from_c_str("delete-named-shadow");
    struct aws_byte_cursor list_streams_cursor = aws_byte_cursor_from_c_str("list-streams");
    struct aws_byte_cursor open_delta_stream_cursor = aws_byte_cursor_from_c_str("open-named-shadow-delta-stream");
    struct aws_byte_cursor open_document_stream_cursor =
        aws_byte_cursor_from_c_str("open-named-shadow-document-stream");
    struct aws_byte_cursor close_stream_cursor = aws_byte_cursor_from_c_str("close-stream");

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
        printf("Stopping client!\n");
        aws_mqtt5_client_stop(client, NULL, NULL);
    } else if (aws_byte_cursor_eq_ignore_case(&command_cursor, &get_cursor)) {
        s_handle_get(context, allocator, &words);
    } else if (aws_byte_cursor_eq_ignore_case(&command_cursor, &update_reported_cursor)) {
        s_handle_update_reported(context, allocator, &words, line_cursor);
    } else if (aws_byte_cursor_eq_ignore_case(&command_cursor, &update_desired_cursor)) {
        s_handle_update_desired(context, allocator, &words, line_cursor);
    } else if (aws_byte_cursor_eq_ignore_case(&command_cursor, &delete_cursor)) {
        s_handle_delete(context, allocator, &words);
    } else if (aws_byte_cursor_eq_ignore_case(&command_cursor, &list_streams_cursor)) {
        s_handle_list_streams(context, allocator, &words);
    } else if (aws_byte_cursor_eq_ignore_case(&command_cursor, &open_delta_stream_cursor)) {
        s_handle_open_delta_stream(context, allocator, &words);
    } else if (aws_byte_cursor_eq_ignore_case(&command_cursor, &open_document_stream_cursor)) {
        s_handle_open_document_stream(context, allocator, &words);
    } else if (aws_byte_cursor_eq_ignore_case(&command_cursor, &close_stream_cursor)) {
        s_handle_close_stream(context, allocator, &words);
    } else {
        printf("Unknown command: " PRInSTR "\n", AWS_BYTE_CURSOR_PRI(command_cursor));
        printf("\nValid commands:\n");
        printf("  get-named-shadow - gets the full state of a named shadow\n");
        printf("  delete-named-shadow - deletes a named shadow\n");
        printf("  update-named-shadow-reported - updates the reported state of a named shadow\n");
        printf("  update-named-shadow-desired - updates the desired state of a named shadow\n");
        printf("\n");
        printf("  list-streams - lists all open shadow streams\n");
        printf("  open-named-shadow-delta-stream - creates and opens a new shadow delta stream\n");
        printf("  open-named-shadow-document-stream - creates and opens a new shadow document stream\n");
        printf("  close-stream - closes a specific shadow stream\n");
        printf("\n");
        printf("  start - starts the mqtt5 client underlying the request-response client\n");
        printf("  stop - starts the mqtt5 client underlying the request-response client\n");
        printf("  quit - quits the program\n");
        printf("\n");
    }

done:

    aws_array_list_clean_up(&words);
    return done;
}

static void s_on_publish_received(const struct aws_mqtt5_packet_publish_view *publish, void *user_data) {
    (void)publish;
    (void)user_data;
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

AWS_STATIC_STRING_FROM_LITERAL(s_client_id, "HelloWorld");

int main(int argc, char **argv) {
    struct aws_allocator *allocator = aws_mem_tracer_new(aws_default_allocator(), NULL, AWS_MEMTRACE_STACKS, 15);

    aws_mqtt_library_init(allocator);

    struct app_ctx app_ctx;
    AWS_ZERO_STRUCT(app_ctx);
    app_ctx.allocator = allocator;
    app_ctx.signal = (struct aws_condition_variable)AWS_CONDITION_VARIABLE_INIT;
    aws_mutex_init(&app_ctx.lock);
    app_ctx.port = 1883;

    aws_hash_table_init(
        &app_ctx.streaming_operations,
        allocator,
        10,
        aws_hash_uint64_t_by_identity,
        aws_hash_compare_uint64_t_eq,
        NULL,
        s_release_streaming_operation);

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

    if (!app_ctx.cert || !app_ctx.key) {
        fprintf(stderr, "Elastishadow requires mtls connections.  You must specify a cert and key to use.\n");
        exit(1);
    }

    struct aws_tls_ctx *tls_ctx = NULL;
    struct aws_tls_ctx_options tls_ctx_options;
    AWS_ZERO_STRUCT(tls_ctx_options);
    struct aws_tls_connection_options tls_connection_options;
    AWS_ZERO_STRUCT(tls_connection_options);

    if (aws_tls_ctx_options_init_client_mtls_from_path(&tls_ctx_options, allocator, app_ctx.cert, app_ctx.key)) {
        fprintf(
            stderr,
            "Failed to load %s and %s with error %s.",
            app_ctx.cert,
            app_ctx.key,
            aws_error_debug_str(aws_last_error()));
        exit(1);
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

    struct aws_socket_options socket_options = {
        .type = AWS_SOCKET_STREAM,
        .connect_timeout_ms = (uint32_t)10000,
        .keep_alive_timeout_sec = 0,
        .keepalive = false,
        .keep_alive_interval_sec = 0,
    };

    struct aws_mqtt5_packet_connect_view connect_options = {
        .keep_alive_interval_seconds = 30,
        .client_id = aws_byte_cursor_from_string(s_client_id),
    };

    struct aws_mqtt5_client_options client_options = {
        .host_name = app_ctx.uri.host_name,
        .port = app_ctx.port,
        .bootstrap = bootstrap,
        .socket_options = &socket_options,
        .tls_options = &tls_connection_options,
        .connect_options = &connect_options,
        .session_behavior = AWS_MQTT5_CSBT_CLEAN,
        .lifecycle_event_handler = s_lifecycle_event_callback,
        .lifecycle_event_handler_user_data = NULL,
        .retry_jitter_mode = AWS_EXPONENTIAL_BACKOFF_JITTER_NONE,
        .min_reconnect_delay_ms = 1000,
        .max_reconnect_delay_ms = 120000,
        .min_connected_time_to_reset_reconnect_delay_ms = 30000,
        .ping_timeout_ms = 10000,
        .publish_received_handler = s_on_publish_received,
    };

    app_ctx.client = aws_mqtt5_client_new(allocator, &client_options);

    struct aws_mqtt_request_response_client_options rr_client_options = {
        .max_request_response_subscriptions = 10,
        .max_streaming_subscriptions = 5,
        .operation_timeout_seconds = 60,
    };

    app_ctx.rr_client =
        aws_mqtt_request_response_client_new_from_mqtt5_client(allocator, app_ctx.client, &rr_client_options);

    bool done = false;
    while (!done) {
        printf("Enter command:\n");

        char input_buffer[4096];
#ifdef WIN32
        char *line = gets_s(input_buffer, AWS_ARRAY_SIZE(input_buffer));
#else
        char *line = fgets(input_buffer, AWS_ARRAY_SIZE(input_buffer), stdin);
#endif
        done = s_handle_input(&app_ctx, allocator, line);
    }

    aws_hash_table_clean_up(&app_ctx.streaming_operations);

    aws_mqtt_request_response_client_release(app_ctx.rr_client);
    aws_mqtt5_client_release(app_ctx.client);

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
