/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/string.h>
#include <aws/http/proxy.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/mqtt/private/v5/mqtt5_client_impl.h>
#include <aws/mqtt/private/v5/mqtt5_utils.h>
#include <aws/mqtt/v5/mqtt5_client_config.h>

#include <aws/testing/aws_test_harness.h>

static int s_mqtt5_client_config_new_destroy_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct aws_mqtt5_client_config *config = aws_mqtt5_client_config_new(allocator);

    ASSERT_FAILS(aws_mqtt5_client_config_validate(config));

    /* check defaults */
    ASSERT_NULL(config->host_name);
    ASSERT_UINT_EQUALS(0, config->port);
    ASSERT_NULL(config->bootstrap);
    ASSERT_NULL(config->tls_options_ptr);
    ASSERT_NULL(config->http_proxy_host_name);
    ASSERT_UINT_EQUALS(0, config->http_proxy_port);
    ASSERT_NULL(config->http_proxy_tls_options_ptr);
    ASSERT_NULL(config->http_proxy_strategy);
    ASSERT_NULL(config->websocket_handshake_transform);
    ASSERT_NULL(config->websocket_handshake_transform_user_data);
    ASSERT_INT_EQUALS(AWS_MQTT5_CRBT_RECONNECT_IF_INITIAL_SUCCESS, config->reconnect_behavior);
    ASSERT_UINT_EQUALS(AWS_MQTT5_DEFAULT_MIN_RECONNECT_DELAY_MS, config->min_reconnect_delay_ms);
    ASSERT_UINT_EQUALS(AWS_MQTT5_DEFAULT_MAX_RECONNECT_DELAY_MS, config->max_reconnect_delay_ms);
    ASSERT_UINT_EQUALS(
        AWS_MQTT5_DEFAULT_MIN_CONNECTED_TIME_TO_RESET_RECONNECT_DELAY_MS,
        config->min_connected_time_to_reset_reconnect_delay_ms);
    ASSERT_UINT_EQUALS(AWS_MQTT5_DEFAULT_KEEP_ALIVE_INTERVAL_MS, config->keep_alive_interval_ms);
    ASSERT_UINT_EQUALS(AWS_MQTT5_DEFAULT_PING_TIMEOUT_MS, config->ping_timeout_ms);
    ASSERT_NULL(config->username_ptr);
    ASSERT_NULL(config->password_ptr);
    ASSERT_UINT_EQUALS(AWS_MQTT5_DEFAULT_SESSION_EXPIRY_INTERVAL_SECONDS, config->session_expiry_interval_seconds);
    ASSERT_INT_EQUALS(AWS_MQTT5_CSBT_CLEAN, config->session_behavior);
    ASSERT_NULL(config->authentication_method_ptr);
    ASSERT_NULL(config->authentication_data_ptr);
    ASSERT_FALSE(config->request_response_information);
    ASSERT_TRUE(config->request_problem_information);
    ASSERT_UINT_EQUALS(0, config->receive_maximum);
    ASSERT_UINT_EQUALS(0, config->topic_alias_maximum);
    ASSERT_UINT_EQUALS(0, config->maximum_packet_size_bytes);
    ASSERT_UINT_EQUALS(0, aws_array_list_length(&config->connect_user_properties));
    ASSERT_INT_EQUALS(AWS_MQTT5_PFI_NOT_SET, config->will_payload_format);
    ASSERT_NULL(config->will_message_expiry_seconds_ptr);
    ASSERT_NULL(config->will_content_type_ptr);
    ASSERT_NULL(config->will_response_topic_ptr);
    ASSERT_NULL(config->will_correlation_data_ptr);
    ASSERT_UINT_EQUALS(0, config->will_delay_seconds);
    ASSERT_INT_EQUALS(AWS_MQTT5_QOS_AT_MOST_ONCE, config->will_qos);
    ASSERT_UINT_EQUALS(0, config->will_topic.len);
    ASSERT_UINT_EQUALS(0, config->will_payload.len);
    ASSERT_FALSE(config->will_retained);
    ASSERT_UINT_EQUALS(0, aws_array_list_length(&config->will_user_properties));
    ASSERT_NULL(config->lifecycle_event_handler);
    ASSERT_NULL(config->lifecycle_event_handler_user_data);

    aws_mqtt5_client_config_destroy(config);

    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_client_config_new_destroy, s_mqtt5_client_config_new_destroy_fn)

struct property_pair {
    const char *name;
    const char *value;
};

typedef int(add_property_fn)(struct aws_mqtt5_client_config *config, struct aws_mqtt5_user_property *property);

static int s_add_user_properties(
    struct aws_mqtt5_client_config *config,
    add_property_fn add_property_function,
    size_t property_count,
    const struct property_pair *properties) {
    for (size_t i = 0; i < property_count; ++i) {
        const struct property_pair *property = &properties[i];

        struct aws_mqtt5_user_property user_property = {
            .name = aws_byte_cursor_from_c_str(property->name),
            .value = aws_byte_cursor_from_c_str(property->value),
        };

        ASSERT_SUCCESS(add_property_function(config, &user_property));
    }

    return AWS_OP_SUCCESS;
}

static int s_verify_user_properties(
    struct aws_array_list *property_list,
    size_t expected_count,
    const struct property_pair *expected_properties) {
    ASSERT_UINT_EQUALS(expected_count, aws_array_list_length(property_list));

    for (size_t i = 0; i < expected_count; ++i) {
        const struct property_pair *expected_property = &expected_properties[i];
        struct aws_byte_cursor expected_name = aws_byte_cursor_from_c_str(expected_property->name);
        struct aws_byte_cursor expected_value = aws_byte_cursor_from_c_str(expected_property->value);

        bool found = false;
        for (size_t j = 0; j < expected_count; ++j) {
            struct aws_mqtt5_name_value_pair *nv_pair = NULL;
            if (aws_array_list_get_at_ptr(property_list, (void **)&nv_pair, j)) {
                return AWS_OP_ERR;
            }

            if (aws_byte_cursor_compare_lexical(&expected_name, &nv_pair->name) == 0 &&
                aws_byte_cursor_compare_lexical(&expected_value, &nv_pair->value) == 0) {
                found = true;
                break;
            }
        }

        if (!found) {
            return AWS_OP_ERR;
        }
    }

    return AWS_OP_SUCCESS;
}

static struct aws_client_bootstrap *s_new_bootstrap(struct aws_allocator *allocator) {
    struct aws_event_loop_group *elg = aws_event_loop_group_new_default(allocator, 1, NULL);

    struct aws_host_resolver_default_options hr_options = {
        .max_entries = 16,
        .el_group = elg,
    };

    struct aws_host_resolver *hr = aws_host_resolver_new_default(allocator, &hr_options);

    struct aws_client_bootstrap_options options = {
        .event_loop_group = elg,
        .host_resolver = hr,
    };

    struct aws_client_bootstrap *bootstrap = aws_client_bootstrap_new(allocator, &options);
    aws_event_loop_group_release(elg);
    aws_host_resolver_release(hr);

    return bootstrap;
}

static int s_init_tls_options(
    struct aws_allocator *allocator,
    const char *server_name,
    const char *alpn_list,
    struct aws_tls_connection_options *options_out) {
    struct aws_tls_ctx_options tls_ctx_options;
    AWS_ZERO_STRUCT(tls_ctx_options);
    aws_tls_ctx_options_init_default_client(&tls_ctx_options, allocator);

    struct aws_tls_ctx *tls_context = aws_tls_client_ctx_new(allocator, &tls_ctx_options);

    aws_tls_connection_options_init_from_ctx(options_out, tls_context);
    struct aws_byte_cursor server_name_cursor = aws_byte_cursor_from_c_str(server_name);
    ASSERT_SUCCESS(aws_tls_connection_options_set_server_name(options_out, allocator, &server_name_cursor));
    ASSERT_SUCCESS(aws_tls_connection_options_set_alpn_list(options_out, allocator, alpn_list));

    aws_tls_ctx_release(tls_context);

    return AWS_OP_SUCCESS;
}

static void s_websocket_transform(
    struct aws_http_message *request,
    void *user_data,
    aws_mqtt5_transform_websocket_handshake_complete_fn *complete_fn,
    void *complete_ctx) {
    (void)request;
    (void)user_data;
    (void)complete_fn;
    (void)complete_ctx;
}

static void s_lifecycle_event_handler(struct aws_mqtt5_client_lifecycle_event *event) {
    (void)event;
}

AWS_STATIC_STRING_FROM_LITERAL(HOST_NAME, "hello-world.com");
static const uint16_t PORT = 8883;
static struct aws_socket_options SOCKET_OPTIONS = {
    .type = AWS_SOCKET_STREAM,
    .domain = AWS_SOCKET_IPV4,
    .connect_timeout_ms = 5000,
    .keep_alive_interval_sec = 1200,
    .keep_alive_timeout_sec = 10,
    .keep_alive_max_failed_probes = 3,
    .keepalive = true,
};
static const char *ALPN_LIST = "mqtt";
AWS_STATIC_STRING_FROM_LITERAL(PROXY_HOST_NAME, "imaproxy.org");
static const uint16_t PROXY_PORT = 8080;
static const char *PROXY_ALPN_LIST = "http";
static void *WEBSOCKET_TRANSFORM_USER_DATA = (void *)1;
static const uint64_t RECONNECT_DELAY_MIN = 500;
static const uint64_t RECONNECT_DELAY_MAX = 1200000;
static const uint64_t RECONNECT_DELAY_RESET_INTERVAL = 20000;
static const uint32_t KEEP_ALIVE_INTERVAL = 60000;
static const uint32_t PING_TIMEOUT_INTERVAL = 10000;
static const char *CLIENT_ID = "MyClientId";
static const char *USERNAME = "Username";
static const char *PASSWORD = "Password";
static const uint32_t SESSION_EXPIRY_SECONDS = 1200;
static const char *AUTHENTICATION_METHOD = "GSSAPI";
static const char *AUTHENTICATION_DATA = "Nothing";
static const uint16_t RECEIVE_MAXIMUM = 4096;
static const uint16_t TOPIC_ALIAS_MAXIMUM = 32;
static const uint32_t MAXIMUM_PACKET_SIZE = 65536;
static const struct property_pair CONNECT_PROPERTIES[] = {
    {
        .name = "Somename",
        .value = "Somevalue",
    },
    {
        .name = "Anothername",
        .value = "Anothervalue",
    },
};
static const struct property_pair WILL_PROPERTIES[] = {
    {
        .name = "Willname",
        .value = "Willvalue",
    },
};
static const uint32_t WILL_MESSAGE_EXPIRY = 1000;
static const char *WILL_CONTENT_TYPE = "SomeContentType";
static const char *WILL_RESPONSE_TOPIC = "SomeResponseTopic";
static const char *WILL_CORRELATION_DATA = "SomeCorrelationData";
static const uint32_t WILL_DELAY = 60;
static void *LIFECYCLE_EVENT_HANDLER_USER_DATA = (void *)2;
static const char *WILL_TOPIC = "SomeTopic";
static const char *WILL_PAYLOAD = "SomePayload";

static int s_set_config(struct aws_mqtt5_client_config *config) {

    ASSERT_SUCCESS(aws_mqtt5_client_config_set_host_name(config, aws_byte_cursor_from_string(HOST_NAME)));
    aws_mqtt5_client_config_set_port(config, PORT);

    struct aws_client_bootstrap *bootstrap = s_new_bootstrap(config->allocator);
    aws_mqtt5_client_config_set_bootstrap(config, bootstrap);
    aws_client_bootstrap_release(bootstrap);

    aws_mqtt5_client_config_set_socket_options(config, &SOCKET_OPTIONS);

    struct aws_tls_connection_options tls_conn_options;
    AWS_ZERO_STRUCT(tls_conn_options);
    ASSERT_SUCCESS(s_init_tls_options(config->allocator, aws_string_c_str(HOST_NAME), ALPN_LIST, &tls_conn_options));

    ASSERT_SUCCESS(aws_mqtt5_client_config_set_tls_connection_options(config, &tls_conn_options));
    aws_tls_connection_options_clean_up(&tls_conn_options);

    ASSERT_SUCCESS(
        aws_mqtt5_client_config_set_http_proxy_host_name(config, aws_byte_cursor_from_string(PROXY_HOST_NAME)));
    aws_mqtt5_client_config_set_http_proxy_port(config, PROXY_PORT);

    struct aws_tls_connection_options proxy_tls_conn_options;
    AWS_ZERO_STRUCT(proxy_tls_conn_options);
    ASSERT_SUCCESS(s_init_tls_options(
        config->allocator, aws_string_c_str(PROXY_HOST_NAME), PROXY_ALPN_LIST, &proxy_tls_conn_options));

    ASSERT_SUCCESS(aws_mqtt5_client_config_set_http_proxy_tls_connection_options(config, &proxy_tls_conn_options));
    aws_tls_connection_options_clean_up(&proxy_tls_conn_options);

    struct aws_http_proxy_strategy_basic_auth_options strategy_options = {
        .proxy_connection_type = AWS_HPCT_HTTP_TUNNEL,
        .password = aws_byte_cursor_from_c_str("password"),
        .user_name = aws_byte_cursor_from_c_str("user"),
    };

    struct aws_http_proxy_strategy *proxy_strategy =
        aws_http_proxy_strategy_new_basic_auth(config->allocator, &strategy_options);
    aws_mqtt5_client_config_set_http_proxy_strategy(config, proxy_strategy);
    aws_http_proxy_strategy_release(proxy_strategy);

    aws_mqtt5_client_config_set_websocket_transform(config, s_websocket_transform);
    aws_mqtt5_client_config_set_websocket_transform_user_data(config, WEBSOCKET_TRANSFORM_USER_DATA);

    aws_mqtt5_client_config_set_reconnect_behavior(config, AWS_MQTT5_CRBT_RECONNECT_ALWAYS);
    aws_mqtt5_client_config_set_reconnect_delay_ms(config, RECONNECT_DELAY_MIN, RECONNECT_DELAY_MAX);
    aws_mqtt5_client_config_set_reconnect_delay_reset_interval_ms(config, RECONNECT_DELAY_RESET_INTERVAL);

    aws_mqtt5_client_config_set_keep_alive_interval_ms(config, KEEP_ALIVE_INTERVAL);
    aws_mqtt5_client_config_set_ping_timeout_ms(config, PING_TIMEOUT_INTERVAL);

    ASSERT_SUCCESS(aws_mqtt5_client_config_set_client_id(config, aws_byte_cursor_from_c_str(CLIENT_ID)));
    ASSERT_SUCCESS(aws_mqtt5_client_config_set_connect_username(config, aws_byte_cursor_from_c_str(USERNAME)));
    ASSERT_SUCCESS(aws_mqtt5_client_config_set_connect_password(config, aws_byte_cursor_from_c_str(PASSWORD)));

    aws_mqtt5_client_config_set_connect_session_expiry_interval_seconds(config, SESSION_EXPIRY_SECONDS);
    aws_mqtt5_client_config_set_connect_session_behavior(config, AWS_MQTT5_CSBT_REJOIN_AND_RESUB_ON_CLEAN);

    ASSERT_SUCCESS(aws_mqtt5_client_config_set_connect_authentication_method(
        config, aws_byte_cursor_from_c_str(AUTHENTICATION_METHOD)));
    ASSERT_SUCCESS(aws_mqtt5_client_config_set_connect_authentication_data(
        config, aws_byte_cursor_from_c_str(AUTHENTICATION_DATA)));

    aws_mqtt5_client_config_set_connect_request_response_information(config, true);
    aws_mqtt5_client_config_set_connect_request_problem_information(config, false);
    aws_mqtt5_client_config_set_connect_receive_maximum(config, RECEIVE_MAXIMUM);
    aws_mqtt5_client_config_set_connect_topic_alias_maximum(config, TOPIC_ALIAS_MAXIMUM);
    aws_mqtt5_client_config_set_connect_maximum_packet_size(config, MAXIMUM_PACKET_SIZE);

    aws_mqtt5_client_config_clear_connect_user_properties(config);
    ASSERT_SUCCESS(s_add_user_properties(
        config,
        aws_mqtt5_client_config_add_connect_user_property,
        AWS_ARRAY_SIZE(CONNECT_PROPERTIES),
        &CONNECT_PROPERTIES[0]));

    aws_mqtt5_client_config_set_will_payload_format(config, AWS_MQTT5_PFI_BYTES);
    aws_mqtt5_client_config_set_will_message_expiry(config, WILL_MESSAGE_EXPIRY);
    ASSERT_SUCCESS(
        aws_mqtt5_client_config_set_will_content_type(config, aws_byte_cursor_from_c_str(WILL_CONTENT_TYPE)));
    ASSERT_SUCCESS(
        aws_mqtt5_client_config_set_will_response_topic(config, aws_byte_cursor_from_c_str(WILL_RESPONSE_TOPIC)));
    ASSERT_SUCCESS(
        aws_mqtt5_client_config_set_will_correlation_data(config, aws_byte_cursor_from_c_str(WILL_CORRELATION_DATA)));
    aws_mqtt5_client_config_set_will_delay(config, WILL_DELAY);
    ASSERT_SUCCESS(aws_mqtt5_client_config_set_will(
        config,
        aws_byte_cursor_from_c_str(WILL_TOPIC),
        aws_byte_cursor_from_c_str(WILL_PAYLOAD),
        AWS_MQTT5_QOS_AT_LEAST_ONCE));
    aws_mqtt5_client_config_set_will_retained(config, true);

    aws_mqtt5_client_config_clear_will_user_properties(config);
    ASSERT_SUCCESS(s_add_user_properties(
        config, aws_mqtt5_client_config_add_will_user_property, AWS_ARRAY_SIZE(WILL_PROPERTIES), &WILL_PROPERTIES[0]));

    aws_mqtt5_client_config_set_lifecycle_event_handler(config, s_lifecycle_event_handler);
    aws_mqtt5_client_config_set_lifecycle_event_handler_user_data(config, LIFECYCLE_EVENT_HANDLER_USER_DATA);

    return AWS_OP_SUCCESS;
}

static int s_verify_socket_options(
    struct aws_socket_options *expected_options,
    struct aws_socket_options *actual_options) {
    ASSERT_INT_EQUALS(expected_options->type, actual_options->type);
    ASSERT_INT_EQUALS(expected_options->domain, actual_options->domain);
    ASSERT_UINT_EQUALS(expected_options->connect_timeout_ms, actual_options->connect_timeout_ms);
    ASSERT_UINT_EQUALS(expected_options->keep_alive_interval_sec, actual_options->keep_alive_interval_sec);
    ASSERT_UINT_EQUALS(expected_options->keep_alive_timeout_sec, actual_options->keep_alive_timeout_sec);
    ASSERT_UINT_EQUALS(expected_options->keep_alive_max_failed_probes, actual_options->keep_alive_max_failed_probes);
    ASSERT_INT_EQUALS(expected_options->keepalive, actual_options->keepalive);

    return AWS_OP_SUCCESS;
}

static int s_verify_buffer_contains_exactly_c_str(struct aws_byte_buf *buffer, const char *string) {
    ASSERT_UINT_EQUALS(strlen(string), buffer->len);
    ASSERT_BIN_ARRAYS_EQUALS(string, strlen(string), buffer->buffer, buffer->len);

    return AWS_OP_SUCCESS;
}

static int s_verify_string_contains_exactly_c_str(struct aws_string *string, const char *c_str) {
    ASSERT_UINT_EQUALS(strlen(c_str), string->len);
    ASSERT_BIN_ARRAYS_EQUALS(c_str, strlen(c_str), string->bytes, string->len);

    return AWS_OP_SUCCESS;
}

static int s_verify_set_config(struct aws_mqtt5_client_config *config) {
    ASSERT_TRUE(aws_string_eq(config->host_name, HOST_NAME));
    ASSERT_UINT_EQUALS(PORT, config->port);
    ASSERT_SUCCESS(s_verify_socket_options(&SOCKET_OPTIONS, &config->socket_options));

    ASSERT_SUCCESS(
        s_verify_string_contains_exactly_c_str(config->tls_options_ptr->server_name, aws_string_c_str(HOST_NAME)));
    ASSERT_SUCCESS(s_verify_string_contains_exactly_c_str(config->tls_options_ptr->alpn_list, ALPN_LIST));

    ASSERT_TRUE(aws_string_eq(config->http_proxy_host_name, PROXY_HOST_NAME));
    ASSERT_UINT_EQUALS(PROXY_PORT, config->http_proxy_port);
    ASSERT_NOT_NULL(config->http_proxy_tls_options_ptr);
    ASSERT_SUCCESS(s_verify_string_contains_exactly_c_str(
        config->http_proxy_tls_options_ptr->server_name, aws_string_c_str(PROXY_HOST_NAME)));
    ASSERT_SUCCESS(
        s_verify_string_contains_exactly_c_str(config->http_proxy_tls_options_ptr->alpn_list, PROXY_ALPN_LIST));
    ASSERT_NOT_NULL(config->http_proxy_strategy);

    ASSERT_PTR_EQUALS(s_websocket_transform, config->websocket_handshake_transform);
    ASSERT_PTR_EQUALS(WEBSOCKET_TRANSFORM_USER_DATA, config->websocket_handshake_transform_user_data);

    ASSERT_INT_EQUALS(AWS_MQTT5_CRBT_RECONNECT_ALWAYS, config->reconnect_behavior);
    ASSERT_UINT_EQUALS(RECONNECT_DELAY_MIN, config->min_reconnect_delay_ms);
    ASSERT_UINT_EQUALS(RECONNECT_DELAY_MAX, config->max_reconnect_delay_ms);
    ASSERT_UINT_EQUALS(RECONNECT_DELAY_RESET_INTERVAL, config->min_connected_time_to_reset_reconnect_delay_ms);

    ASSERT_UINT_EQUALS(KEEP_ALIVE_INTERVAL, config->keep_alive_interval_ms);
    ASSERT_UINT_EQUALS(PING_TIMEOUT_INTERVAL, config->ping_timeout_ms);

    ASSERT_SUCCESS(s_verify_buffer_contains_exactly_c_str(&config->client_id, CLIENT_ID));
    ASSERT_SUCCESS(s_verify_buffer_contains_exactly_c_str(config->username_ptr, USERNAME));
    ASSERT_SUCCESS(s_verify_buffer_contains_exactly_c_str(config->password_ptr, PASSWORD));
    ASSERT_UINT_EQUALS(SESSION_EXPIRY_SECONDS, config->session_expiry_interval_seconds);
    ASSERT_INT_EQUALS(AWS_MQTT5_CSBT_REJOIN_AND_RESUB_ON_CLEAN, config->session_behavior);

    ASSERT_SUCCESS(s_verify_buffer_contains_exactly_c_str(&config->authentication_method, AUTHENTICATION_METHOD));
    ASSERT_SUCCESS(s_verify_buffer_contains_exactly_c_str(&config->authentication_data, AUTHENTICATION_DATA));

    ASSERT_TRUE(config->request_response_information);
    ASSERT_FALSE(config->request_problem_information);
    ASSERT_UINT_EQUALS(RECEIVE_MAXIMUM, config->receive_maximum);
    ASSERT_UINT_EQUALS(TOPIC_ALIAS_MAXIMUM, config->topic_alias_maximum);
    ASSERT_UINT_EQUALS(MAXIMUM_PACKET_SIZE, config->maximum_packet_size_bytes);

    ASSERT_SUCCESS(s_verify_user_properties(
        &config->connect_user_properties, AWS_ARRAY_SIZE(CONNECT_PROPERTIES), &CONNECT_PROPERTIES[0]));

    ASSERT_INT_EQUALS(AWS_MQTT5_PFI_BYTES, config->will_payload_format);
    ASSERT_UINT_EQUALS(WILL_MESSAGE_EXPIRY, *config->will_message_expiry_seconds_ptr);
    ASSERT_SUCCESS(s_verify_buffer_contains_exactly_c_str(&config->will_content_type, WILL_CONTENT_TYPE));
    ASSERT_SUCCESS(s_verify_buffer_contains_exactly_c_str(&config->will_response_topic, WILL_RESPONSE_TOPIC));
    ASSERT_SUCCESS(s_verify_buffer_contains_exactly_c_str(&config->will_correlation_data, WILL_CORRELATION_DATA));
    ASSERT_UINT_EQUALS(WILL_DELAY, config->will_delay_seconds);

    ASSERT_SUCCESS(s_verify_buffer_contains_exactly_c_str(&config->will_topic, WILL_TOPIC));
    ASSERT_SUCCESS(s_verify_buffer_contains_exactly_c_str(&config->will_payload, WILL_PAYLOAD));
    ASSERT_INT_EQUALS(AWS_MQTT5_QOS_AT_LEAST_ONCE, config->will_qos);

    ASSERT_TRUE(config->will_retained);

    ASSERT_SUCCESS(
        s_verify_user_properties(&config->will_user_properties, AWS_ARRAY_SIZE(WILL_PROPERTIES), &WILL_PROPERTIES[0]));

    ASSERT_PTR_EQUALS(s_lifecycle_event_handler, config->lifecycle_event_handler);
    ASSERT_PTR_EQUALS(LIFECYCLE_EVENT_HANDLER_USER_DATA, config->lifecycle_event_handler_user_data);

    return AWS_OP_SUCCESS;
}

static void s_alt_lifecycle_event_handler(struct aws_mqtt5_client_lifecycle_event *event) {
    (void)event;
}

AWS_STATIC_STRING_FROM_LITERAL(ALT_HOST_NAME, "alt-hello.org");
static const uint16_t ALT_PORT = 1883;
static struct aws_socket_options ALT_SOCKET_OPTIONS = {
    .type = AWS_SOCKET_STREAM,
    .domain = AWS_SOCKET_IPV6,
    .connect_timeout_ms = 6000,
    .keep_alive_interval_sec = 1800,
    .keep_alive_timeout_sec = 15,
    .keep_alive_max_failed_probes = 4,
    .keepalive = true,
};
AWS_STATIC_STRING_FROM_LITERAL(ALT_PROXY_HOST_NAME, "anotherproxy.org");
static const uint16_t ALT_PROXY_PORT = 8081;
static const uint64_t ALT_RECONNECT_DELAY_MIN = 600;
static const uint64_t ALT_RECONNECT_DELAY_MAX = 1100000;
static const uint64_t ALT_RECONNECT_DELAY_RESET_INTERVAL = 30000;
static const uint32_t ALT_KEEP_ALIVE_INTERVAL = 360000;
static const uint32_t ALT_PING_TIMEOUT_INTERVAL = 5000;
static const char *EMPTY_STRING = "";
static const uint32_t ALT_SESSION_EXPIRY_SECONDS = 60;
static const char *ALT_AUTHENTICATION_METHOD = "";
static const char *ALT_AUTHENTICATION_DATA = "Hmm";
static const uint16_t ALT_RECEIVE_MAXIMUM = 128;
static const uint16_t ALT_TOPIC_ALIAS_MAXIMUM = 64;
static const uint32_t ALT_MAXIMUM_PACKET_SIZE = 2048;
static const struct property_pair ALT_CONNECT_PROPERTIES[] = {
    {
        .name = "Hello",
        .value = "There",
    },
};
static const uint32_t ALT_WILL_MESSAGE_EXPIRY = 120;
static const char *ALT_WILL_CONTENT_TYPE = "DifferentContentType";
static const char *ALT_WILL_RESPONSE_TOPIC = "DifferentResponseTopic";
static const char *ALT_WILL_CORRELATION_DATA = "DifferentCorrelationData";
static const uint32_t ALT_WILL_DELAY = 120;
static const char *ALT_WILL_TOPIC = "DifferentTopic";
static const char *ALT_WILL_PAYLOAD = "DifferentPayload";

static int s_set_config_alt(struct aws_mqtt5_client_config *config) {

    ASSERT_SUCCESS(aws_mqtt5_client_config_set_host_name(config, aws_byte_cursor_from_string(ALT_HOST_NAME)));
    aws_mqtt5_client_config_set_port(config, ALT_PORT);

    struct aws_client_bootstrap *bootstrap = s_new_bootstrap(config->allocator);
    aws_mqtt5_client_config_set_bootstrap(config, bootstrap);
    aws_client_bootstrap_release(bootstrap);

    aws_mqtt5_client_config_set_socket_options(config, &ALT_SOCKET_OPTIONS);

    ASSERT_SUCCESS(aws_mqtt5_client_config_set_tls_connection_options(config, NULL));

    ASSERT_SUCCESS(
        aws_mqtt5_client_config_set_http_proxy_host_name(config, aws_byte_cursor_from_string(ALT_PROXY_HOST_NAME)));
    aws_mqtt5_client_config_set_http_proxy_port(config, ALT_PROXY_PORT);
    aws_mqtt5_client_config_set_http_proxy_strategy(config, NULL);

    ASSERT_SUCCESS(aws_mqtt5_client_config_set_http_proxy_tls_connection_options(config, NULL));

    aws_mqtt5_client_config_set_websocket_transform(config, NULL);
    aws_mqtt5_client_config_set_websocket_transform_user_data(config, NULL);

    aws_mqtt5_client_config_set_reconnect_behavior(config, AWS_MQTT5_CRBT_RECONNECT_NEVER);
    aws_mqtt5_client_config_set_reconnect_delay_ms(config, ALT_RECONNECT_DELAY_MIN, ALT_RECONNECT_DELAY_MAX);
    aws_mqtt5_client_config_set_reconnect_delay_reset_interval_ms(config, ALT_RECONNECT_DELAY_RESET_INTERVAL);

    aws_mqtt5_client_config_set_keep_alive_interval_ms(config, ALT_KEEP_ALIVE_INTERVAL);
    aws_mqtt5_client_config_set_ping_timeout_ms(config, ALT_PING_TIMEOUT_INTERVAL);

    ASSERT_SUCCESS(aws_mqtt5_client_config_set_client_id(config, aws_byte_cursor_from_c_str(EMPTY_STRING)));
    ASSERT_SUCCESS(aws_mqtt5_client_config_set_connect_username(config, aws_byte_cursor_from_c_str(EMPTY_STRING)));
    ASSERT_SUCCESS(aws_mqtt5_client_config_set_connect_password(config, aws_byte_cursor_from_c_str(EMPTY_STRING)));

    aws_mqtt5_client_config_set_connect_session_expiry_interval_seconds(config, ALT_SESSION_EXPIRY_SECONDS);
    aws_mqtt5_client_config_set_connect_session_behavior(config, AWS_MQTT5_CSBT_REJOIN);

    aws_mqtt5_client_config_set_connect_authentication_method(
        config, aws_byte_cursor_from_c_str(ALT_AUTHENTICATION_METHOD));
    aws_mqtt5_client_config_set_connect_authentication_data(
        config, aws_byte_cursor_from_c_str(ALT_AUTHENTICATION_DATA));

    aws_mqtt5_client_config_set_connect_request_response_information(config, false);
    aws_mqtt5_client_config_set_connect_request_problem_information(config, true);
    aws_mqtt5_client_config_set_connect_receive_maximum(config, ALT_RECEIVE_MAXIMUM);
    aws_mqtt5_client_config_set_connect_topic_alias_maximum(config, ALT_TOPIC_ALIAS_MAXIMUM);
    aws_mqtt5_client_config_set_connect_maximum_packet_size(config, ALT_MAXIMUM_PACKET_SIZE);

    aws_mqtt5_client_config_clear_connect_user_properties(config);
    ASSERT_SUCCESS(s_add_user_properties(
        config,
        aws_mqtt5_client_config_add_connect_user_property,
        AWS_ARRAY_SIZE(ALT_CONNECT_PROPERTIES),
        &ALT_CONNECT_PROPERTIES[0]));

    aws_mqtt5_client_config_set_will_payload_format(config, AWS_MQTT5_PFI_UTF8);
    aws_mqtt5_client_config_set_will_message_expiry(config, ALT_WILL_MESSAGE_EXPIRY);
    aws_mqtt5_client_config_set_will_content_type(config, aws_byte_cursor_from_c_str(ALT_WILL_CONTENT_TYPE));
    aws_mqtt5_client_config_set_will_response_topic(config, aws_byte_cursor_from_c_str(ALT_WILL_RESPONSE_TOPIC));
    aws_mqtt5_client_config_set_will_correlation_data(config, aws_byte_cursor_from_c_str(ALT_WILL_CORRELATION_DATA));
    aws_mqtt5_client_config_set_will_delay(config, ALT_WILL_DELAY);
    ASSERT_SUCCESS(aws_mqtt5_client_config_set_will(
        config,
        aws_byte_cursor_from_c_str(ALT_WILL_TOPIC),
        aws_byte_cursor_from_c_str(ALT_WILL_PAYLOAD),
        AWS_MQTT5_QOS_EXACTLY_ONCE));
    aws_mqtt5_client_config_set_will_retained(config, false);

    aws_mqtt5_client_config_clear_will_user_properties(config);
    ASSERT_SUCCESS(s_add_user_properties(config, aws_mqtt5_client_config_add_will_user_property, 0, NULL));

    aws_mqtt5_client_config_set_lifecycle_event_handler(config, s_alt_lifecycle_event_handler);
    aws_mqtt5_client_config_set_lifecycle_event_handler_user_data(config, NULL);

    return AWS_OP_SUCCESS;
}

static int s_verify_set_config_alt(struct aws_mqtt5_client_config *config) {

    ASSERT_TRUE(aws_string_eq(config->host_name, ALT_HOST_NAME));
    ASSERT_UINT_EQUALS(ALT_PORT, config->port);
    ASSERT_SUCCESS(s_verify_socket_options(&ALT_SOCKET_OPTIONS, &config->socket_options));

    ASSERT_NULL(config->tls_options_ptr);

    ASSERT_TRUE(aws_string_eq(config->http_proxy_host_name, ALT_PROXY_HOST_NAME));
    ASSERT_UINT_EQUALS(ALT_PROXY_PORT, config->http_proxy_port);

    ASSERT_NULL(config->http_proxy_tls_options_ptr);
    ASSERT_NULL(config->http_proxy_strategy);
    ASSERT_NULL(config->websocket_handshake_transform);
    ASSERT_NULL(config->websocket_handshake_transform_user_data);

    ASSERT_INT_EQUALS(AWS_MQTT5_CRBT_RECONNECT_NEVER, config->reconnect_behavior);
    ASSERT_UINT_EQUALS(ALT_RECONNECT_DELAY_MIN, config->min_reconnect_delay_ms);
    ASSERT_UINT_EQUALS(ALT_RECONNECT_DELAY_MAX, config->max_reconnect_delay_ms);
    ASSERT_UINT_EQUALS(ALT_RECONNECT_DELAY_RESET_INTERVAL, config->min_connected_time_to_reset_reconnect_delay_ms);

    ASSERT_UINT_EQUALS(ALT_KEEP_ALIVE_INTERVAL, config->keep_alive_interval_ms);
    ASSERT_UINT_EQUALS(ALT_PING_TIMEOUT_INTERVAL, config->ping_timeout_ms);

    ASSERT_SUCCESS(s_verify_buffer_contains_exactly_c_str(&config->client_id, EMPTY_STRING));
    ASSERT_SUCCESS(s_verify_buffer_contains_exactly_c_str(&config->username, EMPTY_STRING));
    ASSERT_SUCCESS(s_verify_buffer_contains_exactly_c_str(&config->password, EMPTY_STRING));
    ASSERT_UINT_EQUALS(ALT_SESSION_EXPIRY_SECONDS, config->session_expiry_interval_seconds);
    ASSERT_INT_EQUALS(AWS_MQTT5_CSBT_REJOIN, config->session_behavior);

    ASSERT_SUCCESS(s_verify_buffer_contains_exactly_c_str(&config->authentication_method, ALT_AUTHENTICATION_METHOD));
    ASSERT_SUCCESS(s_verify_buffer_contains_exactly_c_str(&config->authentication_data, ALT_AUTHENTICATION_DATA));

    ASSERT_FALSE(config->request_response_information);
    ASSERT_TRUE(config->request_problem_information);
    ASSERT_UINT_EQUALS(ALT_RECEIVE_MAXIMUM, config->receive_maximum);
    ASSERT_UINT_EQUALS(ALT_TOPIC_ALIAS_MAXIMUM, config->topic_alias_maximum);
    ASSERT_UINT_EQUALS(ALT_MAXIMUM_PACKET_SIZE, config->maximum_packet_size_bytes);

    ASSERT_SUCCESS(s_verify_user_properties(
        &config->connect_user_properties, AWS_ARRAY_SIZE(ALT_CONNECT_PROPERTIES), &ALT_CONNECT_PROPERTIES[0]));

    ASSERT_INT_EQUALS(AWS_MQTT5_PFI_UTF8, config->will_payload_format);
    ASSERT_UINT_EQUALS(ALT_WILL_MESSAGE_EXPIRY, *config->will_message_expiry_seconds_ptr);
    ASSERT_SUCCESS(s_verify_buffer_contains_exactly_c_str(&config->will_content_type, ALT_WILL_CONTENT_TYPE));
    ASSERT_SUCCESS(s_verify_buffer_contains_exactly_c_str(&config->will_response_topic, ALT_WILL_RESPONSE_TOPIC));
    ASSERT_SUCCESS(s_verify_buffer_contains_exactly_c_str(&config->will_correlation_data, ALT_WILL_CORRELATION_DATA));
    ASSERT_UINT_EQUALS(ALT_WILL_DELAY, config->will_delay_seconds);

    ASSERT_SUCCESS(s_verify_buffer_contains_exactly_c_str(&config->will_topic, ALT_WILL_TOPIC));
    ASSERT_SUCCESS(s_verify_buffer_contains_exactly_c_str(&config->will_payload, ALT_WILL_PAYLOAD));
    ASSERT_INT_EQUALS(AWS_MQTT5_QOS_EXACTLY_ONCE, config->will_qos);

    ASSERT_FALSE(config->will_retained);

    ASSERT_UINT_EQUALS(0, aws_array_list_length(&config->will_user_properties));

    ASSERT_PTR_EQUALS(s_alt_lifecycle_event_handler, config->lifecycle_event_handler);
    ASSERT_NULL(config->lifecycle_event_handler_user_data);

    return AWS_OP_SUCCESS;
}

static int s_mqtt5_client_config_set_all_once_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct aws_mqtt5_client_config *config = aws_mqtt5_client_config_new(allocator);

    ASSERT_SUCCESS(s_set_config(config));
    ASSERT_SUCCESS(s_verify_set_config(config));
    ASSERT_SUCCESS(aws_mqtt5_client_config_validate(config));

    aws_mqtt5_client_config_destroy(config);

    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_client_config_set_all_once, s_mqtt5_client_config_set_all_once_fn)

static int s_mqtt5_client_config_set_all_twice_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct aws_mqtt5_client_config *config = aws_mqtt5_client_config_new(allocator);

    ASSERT_SUCCESS(s_set_config(config));
    ASSERT_SUCCESS(s_verify_set_config(config));
    ASSERT_SUCCESS(s_set_config(config));
    ASSERT_SUCCESS(s_verify_set_config(config));

    aws_mqtt5_client_config_destroy(config);

    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_client_config_set_all_twice, s_mqtt5_client_config_set_all_twice_fn)

static int s_mqtt5_client_config_overwrite_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct aws_mqtt5_client_config *config = aws_mqtt5_client_config_new(allocator);

    ASSERT_SUCCESS(s_set_config_alt(config));
    ASSERT_SUCCESS(s_verify_set_config_alt(config));
    ASSERT_SUCCESS(s_set_config(config));
    ASSERT_SUCCESS(s_verify_set_config(config));
    ASSERT_SUCCESS(s_set_config_alt(config));
    ASSERT_SUCCESS(s_verify_set_config_alt(config));
    ASSERT_SUCCESS(aws_mqtt5_client_config_validate(config));

    aws_mqtt5_client_config_destroy(config);

    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_client_config_overwrite, s_mqtt5_client_config_overwrite_fn)

static int s_mqtt5_client_config_clone_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct aws_mqtt5_client_config *config = aws_mqtt5_client_config_new(allocator);

    ASSERT_SUCCESS(s_set_config(config));

    struct aws_mqtt5_client_config *config_clone = aws_mqtt5_client_config_new_clone(allocator, config);
    ASSERT_SUCCESS(s_verify_set_config(config_clone));
    ASSERT_SUCCESS(aws_mqtt5_client_config_validate(config_clone));

    aws_mqtt5_client_config_destroy(config);
    aws_mqtt5_client_config_destroy(config_clone);

    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_client_config_clone, s_mqtt5_client_config_clone_fn)
