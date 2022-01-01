/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/v5/mqtt5_client_config.h>

#include <aws/common/string.h>
#include <aws/http/proxy.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/tls_channel_handler.h>
#include <aws/mqtt/v5/private/mqtt5_client_impl.h>

#include <inttypes.h>

static void s_clear_user_properties_array_list(struct aws_array_list *property_list) {
    if (property_list == NULL) {
        return;
    }

    size_t property_count = aws_array_list_length(property_list);
    for (size_t i = 0; i < property_count; ++i) {
        struct aws_mqtt5_name_value_pair *nv_pair = NULL;
        if (aws_array_list_get_at_ptr(property_list, (void **)&nv_pair, i)) {
            continue;
        }

        aws_byte_buf_clean_up(&nv_pair->name_value_pair);
    }

    aws_array_list_clear(property_list);
}

static int s_add_user_property_to_array_list(
    struct aws_mqtt5_client_config *config,
    struct aws_mqtt5_user_property *property,
    struct aws_array_list *property_list,
    const char *logging_qualifier) {
    struct aws_mqtt5_name_value_pair nv_pair;
    AWS_ZERO_STRUCT(nv_pair);

    if (aws_byte_buf_init(&nv_pair.name_value_pair, config->allocator, property->name.len + property->value.len)) {
        goto on_error;
    }

    nv_pair.name = property->name;
    nv_pair.value = property->value;

    if (aws_byte_buf_append_and_update(&nv_pair.name_value_pair, &nv_pair.name)) {
        goto on_error;
    }

    if (aws_byte_buf_append_and_update(&nv_pair.name_value_pair, &nv_pair.value)) {
        goto on_error;
    }

    if (aws_array_list_push_back(property_list, &nv_pair)) {
        goto on_error;
    }

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CONFIG,
        "(%p) mqtt5_client_config - adding %s user property - name: \"" PRInSTR "\", value: \"" PRInSTR "\"",
        (void *)config,
        logging_qualifier,
        AWS_BYTE_CURSOR_PRI(property->name),
        AWS_BYTE_CURSOR_PRI(property->value));

    return AWS_OP_SUCCESS;

on_error:

    aws_byte_buf_clean_up(&nv_pair.name_value_pair);

    return AWS_OP_ERR;
}

static int s_copy_user_properties_array_list(
    struct aws_mqtt5_client_config *config,
    const struct aws_array_list *source_list,
    struct aws_array_list *dest_list,
    const char *logging_qualifier) {
    if (source_list == NULL || dest_list == NULL) {
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    size_t property_count = aws_array_list_length(source_list);
    for (size_t i = 0; i < property_count; ++i) {
        struct aws_mqtt5_name_value_pair *nv_pair = NULL;
        if (aws_array_list_get_at_ptr(source_list, (void **)&nv_pair, i)) {
            return AWS_OP_ERR;
        }

        struct aws_mqtt5_user_property property;
        AWS_ZERO_STRUCT(property);
        property.name = nv_pair->name;
        property.value = nv_pair->value;

        if (s_add_user_property_to_array_list(config, &property, dest_list, logging_qualifier)) {
            return AWS_OP_ERR;
        }
    }

    return AWS_OP_SUCCESS;
}

struct aws_mqtt5_client_config *aws_mqtt5_client_config_new(struct aws_allocator *allocator) {
    AWS_FATAL_ASSERT(allocator != NULL);

    struct aws_mqtt5_client_config *config = aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt5_client_config));

    config->allocator = allocator;

    if (aws_array_list_init_dynamic(
            &config->connect_user_properties, allocator, 0, sizeof(struct aws_mqtt5_name_value_pair))) {
        goto on_error;
    }

    if (aws_array_list_init_dynamic(
            &config->will_user_properties, allocator, 0, sizeof(struct aws_mqtt5_name_value_pair))) {
        goto on_error;
    }

    /* "Zeroed" is a sensible default for many, but not all, values in the config */
    config->min_reconnect_delay_ms = AWS_MQTT5_DEFAULT_MIN_RECONNECT_DELAY_MS;
    config->max_reconnect_delay_ms = AWS_MQTT5_DEFAULT_MAX_RECONNECT_DELAY_MS;
    config->min_connected_time_to_reset_reconnect_delay_ms =
        AWS_MQTT5_DEFAULT_MIN_CONNECTED_TIME_TO_RESET_RECONNECT_DELAY_MS;
    config->keep_alive_interval_ms = AWS_MQTT5_DEFAULT_KEEP_ALIVE_INTERVAL_MS;
    config->ping_timeout_ms = AWS_MQTT5_DEFAULT_PING_TIMEOUT_MS;
    config->session_expiry_interval_seconds = AWS_MQTT5_DEFAULT_SESSION_EXPIRY_INTERVAL_SECONDS;
    config->request_problem_information = true;
    config->will_payload_format = AWS_MQTT5_PFI_NOT_SET;

    return config;

on_error:

    aws_mqtt5_client_config_destroy(config);

    return NULL;
}

struct aws_mqtt5_client_config *aws_mqtt5_client_config_new_clone(
    struct aws_allocator *allocator,
    struct aws_mqtt5_client_config *from) {

    AWS_FATAL_ASSERT(allocator != NULL);
    struct aws_mqtt5_client_config *config = aws_mqtt5_client_config_new(allocator);
    if (config == NULL) {
        goto on_error;
    }

    if (aws_mqtt5_client_config_set_host_name(config, aws_byte_cursor_from_buf(&from->host_name))) {
        goto on_error;
    }

    aws_mqtt5_client_config_set_port(config, from->port);
    aws_mqtt5_client_config_set_bootstrap(config, from->bootstrap);
    aws_mqtt5_client_config_set_socket_options(config, &from->socket_options);

    if (aws_mqtt5_client_config_set_tls_connection_options(config, from->tls_options_ptr)) {
        goto on_error;
    }

    if (aws_mqtt5_client_config_set_http_proxy_host_name(
            config, aws_byte_cursor_from_buf(&from->http_proxy_host_name))) {
        goto on_error;
    }

    aws_mqtt5_client_config_set_http_proxy_port(config, from->http_proxy_port);
    if (aws_mqtt5_client_config_set_http_proxy_tls_connection_options(config, from->http_proxy_tls_options_ptr)) {
        goto on_error;
    }

    aws_mqtt5_client_config_set_http_proxy_strategy(config, from->http_proxy_strategy);

    aws_mqtt5_client_config_set_websocket_transform(config, from->websocket_handshake_transform);
    aws_mqtt5_client_config_set_websocket_transform_user_data(config, from->websocket_handshake_transform_user_data);

    aws_mqtt5_client_config_set_reconnect_behavior(config, from->reconnect_behavior);
    aws_mqtt5_client_config_set_reconnect_delay_ms(config, from->min_reconnect_delay_ms, from->max_reconnect_delay_ms);
    aws_mqtt5_client_config_set_reconnect_delay_reset_interval_ms(
        config, from->min_connected_time_to_reset_reconnect_delay_ms);
    aws_mqtt5_client_config_set_keep_alive_interval_ms(config, from->keep_alive_interval_ms);
    aws_mqtt5_client_config_set_ping_timeout_ms(config, from->ping_timeout_ms);

    if (aws_mqtt5_client_config_set_client_id(config, aws_byte_cursor_from_buf(&from->client_id))) {
        goto on_error;
    }
    if (from->username_ptr != NULL) {
        if (aws_mqtt5_client_config_set_connect_username(config, aws_byte_cursor_from_buf(&from->username))) {
            goto on_error;
        }
    }
    if (aws_mqtt5_client_config_set_connect_password(config, aws_byte_cursor_from_buf(&from->password))) {
        goto on_error;
    }
    aws_mqtt5_client_config_set_connect_session_expiry_interval_seconds(config, from->session_expiry_interval_seconds);
    aws_mqtt5_client_config_set_connect_session_behavior(config, from->session_behavior);
    if (from->authentication_method_ptr != NULL) {
        if (aws_mqtt5_client_config_set_connect_authentication_method(
                config, aws_byte_cursor_from_buf(&from->authentication_method))) {
            goto on_error;
        }
    }
    if (from->authentication_data_ptr != NULL) {
        if (aws_mqtt5_client_config_set_connect_authentication_data(
                config, aws_byte_cursor_from_buf(&from->authentication_data))) {
            goto on_error;
        }
    }
    aws_mqtt5_client_config_set_connect_request_response_information(config, from->request_response_information);
    aws_mqtt5_client_config_set_connect_request_problem_information(config, from->request_problem_information);
    aws_mqtt5_client_config_set_connect_receive_maximum(config, from->receive_maximum);
    aws_mqtt5_client_config_set_connect_topic_alias_maximum(config, from->topic_alias_maximum);
    aws_mqtt5_client_config_set_connect_maximum_packet_size(config, from->maximum_packet_size_bytes);
    if (s_copy_user_properties_array_list(
            config, &from->connect_user_properties, &config->connect_user_properties, "CONNECT")) {
        goto on_error;
    }
    aws_mqtt5_client_config_set_will_payload_format(config, from->will_payload_format);

    if (from->will_message_expiry_seconds_ptr != NULL) {
        aws_mqtt5_client_config_set_will_message_expiry(config, *from->will_message_expiry_seconds_ptr);
    }

    if (from->will_content_type_ptr != NULL) {
        if (aws_mqtt5_client_config_set_will_content_type(config, aws_byte_cursor_from_buf(&from->will_content_type))) {
            goto on_error;
        }
    }

    if (from->will_response_topic_ptr != NULL) {
        if (aws_mqtt5_client_config_set_will_response_topic(
                config, aws_byte_cursor_from_buf(&from->will_response_topic))) {
            goto on_error;
        }
    }

    if (from->will_correlation_data_ptr != NULL) {
        if (aws_mqtt5_client_config_set_will_correlation_data(
                config, aws_byte_cursor_from_buf(&from->will_correlation_data))) {
            goto on_error;
        }
    }

    aws_mqtt5_client_config_set_will_delay(config, from->will_delay_seconds);

    if (from->will_topic.len > 0) {
        if (aws_mqtt5_client_config_set_will(
                config,
                aws_byte_cursor_from_buf(&from->will_topic),
                aws_byte_cursor_from_buf(&from->will_payload),
                from->will_qos)) {
            goto on_error;
        }
    }

    aws_mqtt5_client_config_set_will_retained(config, from->will_retained);

    if (s_copy_user_properties_array_list(config, &from->will_user_properties, &config->will_user_properties, "Will")) {
        goto on_error;
    }

    aws_mqtt5_client_config_set_lifecycle_event_handler(config, from->lifecycle_event_handler);
    aws_mqtt5_client_config_set_lifecycle_event_handler_user_data(config, from->lifecycle_event_handler_user_data);

    return config;

on_error:

    aws_mqtt5_client_config_destroy(config);

    return NULL;
}

void aws_mqtt5_client_config_destroy(struct aws_mqtt5_client_config *config) {
    if (config == NULL) {
        return;
    }

    aws_byte_buf_clean_up(&config->host_name);
    aws_client_bootstrap_release(config->bootstrap);
    aws_tls_connection_options_clean_up(&config->tls_options);
    aws_byte_buf_clean_up(&config->http_proxy_host_name);
    aws_tls_connection_options_clean_up(&config->http_proxy_tls_options);
    aws_http_proxy_strategy_release(config->http_proxy_strategy);
    aws_byte_buf_clean_up(&config->client_id);
    aws_byte_buf_clean_up(&config->username);
    aws_byte_buf_clean_up_secure(&config->password);
    aws_byte_buf_clean_up(&config->authentication_method);
    aws_byte_buf_clean_up_secure(&config->authentication_data);
    s_clear_user_properties_array_list(&config->connect_user_properties);
    aws_array_list_clean_up(&config->connect_user_properties);
    aws_byte_buf_clean_up(&config->will_content_type);
    aws_byte_buf_clean_up(&config->will_response_topic);
    aws_byte_buf_clean_up(&config->will_correlation_data);
    aws_byte_buf_clean_up(&config->will_topic);
    aws_byte_buf_clean_up(&config->will_payload);
    s_clear_user_properties_array_list(&config->will_user_properties);
    aws_array_list_clean_up(&config->will_user_properties);

    aws_mem_release(config->allocator, config);
}

int aws_mqtt5_client_config_validate(struct aws_mqtt5_client_config *config) {
    if (config->host_name.len == 0) {
        return aws_raise_error(AWS_ERROR_MQTT_CONFIG_VALIDATION_HOST_NOT_SET);
    }

    if (config->port == 0) {
        return aws_raise_error(AWS_ERROR_MQTT_CONFIG_VALIDATION_PORT_NOT_SET);
    }

    if (config->bootstrap == NULL) {
        return aws_raise_error(AWS_ERROR_MQTT_CONFIG_VALIDATION_CLIENT_BOOTSTRAP_NOT_SET);
    }

    /* forbid no-timeout until someone convinces me otherwise */
    if (config->socket_options.type != AWS_SOCKET_STREAM || config->socket_options.connect_timeout_ms == 0) {
        return aws_raise_error(AWS_ERROR_MQTT_CONFIG_VALIDATION_INVALID_SOCKET_OPTIONS);
    }

    if (config->http_proxy_host_name.len > 0) {
        if (config->http_proxy_port == 0) {
            return aws_raise_error(AWS_ERROR_MQTT_CONFIG_VALIDATION_PROXY_PORT_NOT_SET);
        }
    }

    return AWS_OP_SUCCESS;
}

/*
 * Helper that resets a byte buffer to a cursor, but does nothing if both buffer and cursor are empty.
 */
static int s_conditional_init_byte_buf_to_cursor(
    struct aws_byte_buf *dest,
    struct aws_allocator *allocator,
    struct aws_byte_cursor cursor) {
    if (cursor.len > 0 || dest->len > 0) {
        aws_byte_buf_clean_up(dest);
        return aws_byte_buf_init_copy_from_cursor(dest, allocator, cursor);
    }

    return AWS_OP_SUCCESS;
}

int aws_mqtt5_client_config_set_host_name(struct aws_mqtt5_client_config *config, struct aws_byte_cursor host_name) {

    AWS_FATAL_ASSERT(config != NULL);

    if (s_conditional_init_byte_buf_to_cursor(&config->host_name, config->allocator, host_name)) {
        return AWS_OP_ERR;
    }

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CONFIG,
        "(%p) mqtt5_client_config - setting host to " PRInSTR,
        (void *)config,
        AWS_BYTE_CURSOR_PRI(host_name));

    return AWS_OP_SUCCESS;
}

void aws_mqtt5_client_config_set_port(struct aws_mqtt5_client_config *config, uint16_t port) {
    AWS_FATAL_ASSERT(config != NULL);
    AWS_LOGF_DEBUG(AWS_LS_MQTT_CONFIG, "(%p) mqtt5_client_config - setting port to %d", (void *)config, (int)port);

    config->port = port;
}

void aws_mqtt5_client_config_set_bootstrap(
    struct aws_mqtt5_client_config *config,
    struct aws_client_bootstrap *bootstrap) {

    AWS_FATAL_ASSERT(config != NULL);
    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CONFIG,
        "(%p) mqtt5_client_config - setting client bootstrap to %p",
        (void *)config,
        (void *)bootstrap);

    aws_client_bootstrap_release(config->bootstrap);
    config->bootstrap = aws_client_bootstrap_acquire(bootstrap);
}

void aws_mqtt5_client_config_set_socket_options(
    struct aws_mqtt5_client_config *config,
    struct aws_socket_options *socket_options) {

    AWS_FATAL_ASSERT(config != NULL);
    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CONFIG,
        "(%p) mqtt5_client_config - setting setting socket options, type = %d, domain = %d, connect_timeout_ms = "
        "%" PRIu32,
        (void *)config,
        (int)socket_options->type,
        (int)socket_options->domain,
        socket_options->connect_timeout_ms);
    if (socket_options->keepalive) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT_CONFIG,
            "(%p) mqtt5_client_config - setting setting socket options, keep_alive_interval_sec = %d, "
            "keep_alive_timeout_sec = %d, keep_alive_max_failed_probes = %d",
            (void *)config,
            (int)socket_options->keep_alive_interval_sec,
            (int)socket_options->keep_alive_timeout_sec,
            (int)socket_options->keep_alive_max_failed_probes);
    }

    config->socket_options = *socket_options;
}

static void s_log_tls_connection_options(
    struct aws_mqtt5_client_config *config,
    struct aws_tls_connection_options *tls_options,
    const char *log_text) {
    AWS_LOGF_DEBUG(AWS_LS_MQTT_CONFIG, "(%p) mqtt5_client_config - setting %s", (void *)config, log_text);

    if (tls_options->advertise_alpn_message && tls_options->alpn_list) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT_CONFIG,
            "(%p) mqtt5_client_config - %s alpn protocol list: %s",
            (void *)config,
            log_text,
            aws_string_c_str(tls_options->alpn_list));
    } else {
        AWS_LOGF_DEBUG(AWS_LS_MQTT_CONFIG, "(%p) mqtt5_client_config - %s alpn not used", (void *)config, log_text);
    }

    if (tls_options->server_name) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT_CONFIG,
            "(%p) mqtt5_client_config - %s SNI value: %s",
            (void *)config,
            log_text,
            aws_string_c_str(tls_options->server_name));
    } else {
        AWS_LOGF_DEBUG(AWS_LS_MQTT_CONFIG, "(%p) mqtt5_client_config - %s SNI not used", (void *)config, log_text);
    }

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CONFIG,
        "(%p) mqtt5_client_config - %s tls context: %p",
        (void *)config,
        log_text,
        (void *)(tls_options->ctx));
    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CONFIG,
        "(%p) mqtt5_client_config - %s tls handshake timeout: %" PRIu32,
        (void *)config,
        log_text,
        tls_options->timeout_ms);
}

int aws_mqtt5_client_config_set_tls_connection_options(
    struct aws_mqtt5_client_config *config,
    struct aws_tls_connection_options *tls_options) {
    AWS_FATAL_ASSERT(config != NULL);

    /*
     * ToDo: Make failure idempotent?  Pain in the ass, questionable value, but there's something gross about
     * a copy failure wiping out any existing tls configuration.
     */
    aws_tls_connection_options_clean_up(&config->tls_options);
    AWS_ZERO_STRUCT(config->tls_options);
    config->tls_options_ptr = NULL;

    if (tls_options != NULL) {
        if (aws_tls_connection_options_copy(&config->tls_options, tls_options)) {
            return AWS_OP_ERR;
        }
        config->tls_options_ptr = &config->tls_options;

        s_log_tls_connection_options(config, config->tls_options_ptr, "tls options");
    } else {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT_CONFIG, "(%p) mqtt5_client_config - not using tls to remote endpoint", (void *)config);
    }

    return AWS_OP_SUCCESS;
}

int aws_mqtt5_client_config_set_http_proxy_host_name(
    struct aws_mqtt5_client_config *config,
    struct aws_byte_cursor proxy_host_name) {

    AWS_FATAL_ASSERT(config != NULL);
    if (s_conditional_init_byte_buf_to_cursor(&config->http_proxy_host_name, config->allocator, proxy_host_name)) {
        return AWS_OP_ERR;
    }

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CONFIG,
        "(%p) mqtt5_client_config - setting proxy host to " PRInSTR,
        (void *)config,
        AWS_BYTE_CURSOR_PRI(proxy_host_name));

    return AWS_OP_SUCCESS;
}

void aws_mqtt5_client_config_set_http_proxy_port(struct aws_mqtt5_client_config *config, uint16_t port) {

    AWS_FATAL_ASSERT(config != NULL);
    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CONFIG, "(%p) mqtt5_client_config - setting proxy port to %d", (void *)config, (int)port);

    config->http_proxy_port = port;
}

int aws_mqtt5_client_config_set_http_proxy_tls_connection_options(
    struct aws_mqtt5_client_config *config,
    struct aws_tls_connection_options *tls_options) {

    AWS_FATAL_ASSERT(config != NULL);

    aws_tls_connection_options_clean_up(&config->http_proxy_tls_options);
    AWS_ZERO_STRUCT(config->http_proxy_tls_options);
    config->http_proxy_tls_options_ptr = NULL;

    if (tls_options != NULL) {
        if (aws_tls_connection_options_copy(&config->http_proxy_tls_options, tls_options)) {
            return AWS_OP_ERR;
        }
        config->http_proxy_tls_options_ptr = &config->http_proxy_tls_options;

        s_log_tls_connection_options(config, config->http_proxy_tls_options_ptr, "http proxy tls options");
    } else {
        AWS_LOGF_DEBUG(AWS_LS_MQTT_CONFIG, "(%p) mqtt5_client_config - not using tls to proxy", (void *)config);
    }

    return AWS_OP_SUCCESS;
}

void aws_mqtt5_client_config_set_http_proxy_strategy(
    struct aws_mqtt5_client_config *config,
    struct aws_http_proxy_strategy *strategy) {

    AWS_FATAL_ASSERT(config != NULL);

    /* ToDo: add (and use) an API to proxy strategy that returns a debug string (Basic, Adaptive, etc...) */
    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CONFIG,
        "(%p) mqtt5_client_config - setting http proxy strategy to %p",
        (void *)config,
        (void *)strategy);

    aws_http_proxy_strategy_release(config->http_proxy_strategy);
    config->http_proxy_strategy = aws_http_proxy_strategy_acquire(strategy);
}

void aws_mqtt5_client_config_set_websocket_transform(
    struct aws_mqtt5_client_config *config,
    aws_mqtt5_transform_websocket_handshake_fn *transform) {

    AWS_FATAL_ASSERT(config != NULL);
    if (transform != NULL) {
        AWS_LOGF_DEBUG(AWS_LS_MQTT_CONFIG, "(%p) mqtt5_client_config - enabling websockets", (void *)config);
    } else {
        AWS_LOGF_DEBUG(AWS_LS_MQTT_CONFIG, "(%p) mqtt5_client_config - disabling websockets", (void *)config);
    }

    config->websocket_handshake_transform = transform;
}

void aws_mqtt5_client_config_set_websocket_transform_user_data(
    struct aws_mqtt5_client_config *config,
    void *user_data) {

    AWS_FATAL_ASSERT(config != NULL);
    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CONFIG,
        "(%p) mqtt5_client_config - setting websocket handshake transform user data to: %p",
        (void *)config,
        user_data);

    config->websocket_handshake_transform_user_data = user_data;
}

void aws_mqtt5_client_config_set_reconnect_behavior(
    struct aws_mqtt5_client_config *config,
    enum aws_mqtt5_client_reconnect_behavior_type reconnect_behavior) {

    AWS_FATAL_ASSERT(config != NULL);
    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CONFIG,
        "(%p) mqtt5_client_config - setting reconnect behavior to: %d(%s)",
        (void *)config,
        (int)reconnect_behavior,
        aws_mqtt5_client_reconnect_behavior_type_to_c_string(reconnect_behavior));

    config->reconnect_behavior = reconnect_behavior;
}

void aws_mqtt5_client_config_set_reconnect_delay_ms(
    struct aws_mqtt5_client_config *config,
    uint64_t min_reconnect_delay_ms,
    uint64_t max_reconnect_delay_ms) {

    AWS_FATAL_ASSERT(config != NULL);
    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CONFIG,
        "(%p) mqtt5_client_config - setting reconnect delay boundaries, min: %" PRIu64 " ms, max: %" PRIu64 " ms",
        (void *)config,
        min_reconnect_delay_ms,
        max_reconnect_delay_ms);

    config->min_reconnect_delay_ms = min_reconnect_delay_ms;
    config->max_reconnect_delay_ms = max_reconnect_delay_ms;
}

void aws_mqtt5_client_config_set_reconnect_delay_reset_interval_ms(
    struct aws_mqtt5_client_config *config,
    uint64_t min_connected_time_to_reset_reconnect_delay_ms) {

    AWS_FATAL_ASSERT(config != NULL);
    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CONFIG,
        "(%p) mqtt5_client_config - setting minimum necessary connection time in order to reset the reconnect "
        "delay: %" PRIu64 " ms",
        (void *)config,
        min_connected_time_to_reset_reconnect_delay_ms);

    config->min_connected_time_to_reset_reconnect_delay_ms = min_connected_time_to_reset_reconnect_delay_ms;
}

void aws_mqtt5_client_config_set_keep_alive_interval_ms(
    struct aws_mqtt5_client_config *config,
    uint32_t keep_alive_interval_ms) {

    AWS_FATAL_ASSERT(config != NULL);
    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CONFIG,
        "(%p) mqtt5_client_config - setting keep alive interval to %" PRIu32 " ms",
        (void *)config,
        keep_alive_interval_ms);

    config->keep_alive_interval_ms = keep_alive_interval_ms;
}

void aws_mqtt5_client_config_set_ping_timeout_ms(struct aws_mqtt5_client_config *config, uint32_t ping_timeout_ms) {
    AWS_FATAL_ASSERT(config != NULL);
    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CONFIG,
        "(%p) mqtt5_client_config - setting ping timeout interval to %" PRIu32 " ms",
        (void *)config,
        ping_timeout_ms);

    config->ping_timeout_ms = ping_timeout_ms;
}

int aws_mqtt5_client_config_set_client_id(struct aws_mqtt5_client_config *config, struct aws_byte_cursor client_id) {
    AWS_FATAL_ASSERT(config != NULL);

    if (s_conditional_init_byte_buf_to_cursor(&config->client_id, config->allocator, client_id)) {
        return AWS_OP_ERR;
    }

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CONFIG,
        "(%p) mqtt5_client_config - setting client id to " PRInSTR,
        (void *)config,
        AWS_BYTE_CURSOR_PRI(client_id));

    return AWS_OP_SUCCESS;
}

int aws_mqtt5_client_config_set_connect_username(
    struct aws_mqtt5_client_config *config,
    struct aws_byte_cursor username) {

    AWS_FATAL_ASSERT(config != NULL);

    if (s_conditional_init_byte_buf_to_cursor(&config->username, config->allocator, username)) {
        return AWS_OP_ERR;
    }

    config->username_ptr = &config->username;

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CONFIG,
        "(%p) mqtt5_client_config - setting username to \"" PRInSTR "\"",
        (void *)config,
        AWS_BYTE_CURSOR_PRI(username));

    return AWS_OP_SUCCESS;
}

int aws_mqtt5_client_config_set_connect_password(
    struct aws_mqtt5_client_config *config,
    struct aws_byte_cursor password) {

    AWS_FATAL_ASSERT(config != NULL);

    if (s_conditional_init_byte_buf_to_cursor(&config->password, config->allocator, password)) {
        return AWS_OP_ERR;
    }

    config->password_ptr = &config->password;

    AWS_LOGF_DEBUG(AWS_LS_MQTT_CONFIG, "(%p) mqtt5_client_config - setting password", (void *)config);

    return AWS_OP_SUCCESS;
}

void aws_mqtt5_client_config_set_connect_session_expiry_interval_seconds(
    struct aws_mqtt5_client_config *config,
    uint32_t session_expiry_interval_seconds) {

    AWS_FATAL_ASSERT(config != NULL);
    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CONFIG,
        "(%p) mqtt5_client_config - setting session expiry interval to %" PRIu32 " seconds",
        (void *)config,
        session_expiry_interval_seconds);

    config->session_expiry_interval_seconds = session_expiry_interval_seconds;
}

void aws_mqtt5_client_config_set_connect_session_behavior(
    struct aws_mqtt5_client_config *config,
    enum aws_mqtt5_client_session_behavior_type session_behavior) {

    AWS_FATAL_ASSERT(config != NULL);
    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CONFIG,
        "(%p) mqtt5_client_config - setting connect session behavior to: %d(%s)",
        (void *)config,
        (int)session_behavior,
        aws_mqtt5_client_session_behavior_type_to_c_string(session_behavior));

    config->session_behavior = session_behavior;
}

int aws_mqtt5_client_config_set_connect_authentication_method(
    struct aws_mqtt5_client_config *config,
    struct aws_byte_cursor authentication_method) {

    AWS_FATAL_ASSERT(config != NULL);

    if (s_conditional_init_byte_buf_to_cursor(
            &config->authentication_method, config->allocator, authentication_method)) {
        return AWS_OP_ERR;
    }

    config->authentication_method_ptr = &config->authentication_method;

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CONFIG,
        "(%p) mqtt5_client_config - setting authentication method to \"" PRInSTR "\"",
        (void *)config,
        AWS_BYTE_CURSOR_PRI(authentication_method));

    return AWS_OP_SUCCESS;
}

int aws_mqtt5_client_config_set_connect_authentication_data(
    struct aws_mqtt5_client_config *config,
    struct aws_byte_cursor authentication_data) {

    AWS_FATAL_ASSERT(config != NULL);

    if (s_conditional_init_byte_buf_to_cursor(&config->authentication_data, config->allocator, authentication_data)) {
        return AWS_OP_ERR;
    }

    config->authentication_data_ptr = &config->authentication_data;

    AWS_LOGF_DEBUG(AWS_LS_MQTT_CONFIG, "(%p) mqtt5_client_config - setting authentication data", (void *)config);

    return AWS_OP_SUCCESS;
}

void aws_mqtt5_client_config_set_connect_request_response_information(
    struct aws_mqtt5_client_config *config,
    bool request_response_information) {

    AWS_FATAL_ASSERT(config != NULL);
    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CONFIG,
        "(%p) mqtt5_client_config - setting request response information to %d",
        (void *)config,
        (int)request_response_information);

    config->request_response_information = request_response_information;
}

void aws_mqtt5_client_config_set_connect_request_problem_information(
    struct aws_mqtt5_client_config *config,
    bool request_problem_information) {

    AWS_FATAL_ASSERT(config != NULL);
    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CONFIG,
        "(%p) mqtt5_client_config - setting request problem information to %d",
        (void *)config,
        (int)request_problem_information);

    config->request_problem_information = request_problem_information;
}

void aws_mqtt5_client_config_set_connect_receive_maximum(
    struct aws_mqtt5_client_config *config,
    uint16_t receive_maximum) {

    AWS_FATAL_ASSERT(config != NULL);
    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CONFIG,
        "(%p) mqtt5_client_config - setting receive maximum to %d",
        (void *)config,
        (int)receive_maximum);

    config->receive_maximum = receive_maximum;
}

void aws_mqtt5_client_config_set_connect_topic_alias_maximum(
    struct aws_mqtt5_client_config *config,
    uint16_t topic_alias_maximum) {

    AWS_FATAL_ASSERT(config != NULL);
    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CONFIG,
        "(%p) mqtt5_client_config - setting topic alias maximum to %d",
        (void *)config,
        (int)topic_alias_maximum);

    config->topic_alias_maximum = topic_alias_maximum;
}

void aws_mqtt5_client_config_set_connect_maximum_packet_size(
    struct aws_mqtt5_client_config *config,
    uint32_t maximum_packet_size_bytes) {

    AWS_FATAL_ASSERT(config != NULL);
    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CONFIG,
        "(%p) mqtt5_client_config - setting maximum packet size to %" PRIu32 " bytes",
        (void *)config,
        maximum_packet_size_bytes);

    config->maximum_packet_size_bytes = maximum_packet_size_bytes;
}

int aws_mqtt5_client_config_add_connect_user_property(
    struct aws_mqtt5_client_config *config,
    struct aws_mqtt5_user_property *property) {

    return s_add_user_property_to_array_list(config, property, &config->connect_user_properties, "CONNECT");
}

void aws_mqtt5_client_config_clear_connect_user_properties(struct aws_mqtt5_client_config *config) {
    s_clear_user_properties_array_list(&config->connect_user_properties);
}

void aws_mqtt5_client_config_set_will_payload_format(
    struct aws_mqtt5_client_config *config,
    enum aws_mqtt5_payload_format_indicator payload_format) {

    AWS_FATAL_ASSERT(config != NULL);
    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CONFIG,
        "(%p) mqtt5_client_config - setting Will payload format indicator to %d(%s)",
        (void *)config,
        (int)payload_format,
        aws_mqtt5_payload_format_indicator_to_c_string(payload_format));

    config->will_payload_format = payload_format;
}

void aws_mqtt5_client_config_set_will_message_expiry(
    struct aws_mqtt5_client_config *config,
    uint32_t message_expiry_seconds) {

    AWS_FATAL_ASSERT(config != NULL);
    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CONFIG,
        "(%p) mqtt5_client_config - setting Will message expiry to %" PRIu32 " seconds",
        (void *)config,
        message_expiry_seconds);

    config->will_message_expiry_seconds = message_expiry_seconds;
    config->will_message_expiry_seconds_ptr = &config->will_message_expiry_seconds;
}

int aws_mqtt5_client_config_set_will_content_type(
    struct aws_mqtt5_client_config *config,
    struct aws_byte_cursor content_type) {

    AWS_FATAL_ASSERT(config != NULL);

    if (s_conditional_init_byte_buf_to_cursor(&config->will_content_type, config->allocator, content_type)) {
        return AWS_OP_ERR;
    }

    config->will_content_type_ptr = &config->will_content_type;

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CONFIG,
        "(%p) mqtt5_client_config - setting Will content type to \"" PRInSTR "\"",
        (void *)config,
        AWS_BYTE_CURSOR_PRI(content_type));

    return AWS_OP_SUCCESS;
}

int aws_mqtt5_client_config_set_will_response_topic(
    struct aws_mqtt5_client_config *config,
    struct aws_byte_cursor response_topic) {

    AWS_FATAL_ASSERT(config != NULL);

    if (s_conditional_init_byte_buf_to_cursor(&config->will_response_topic, config->allocator, response_topic)) {
        return AWS_OP_ERR;
    }

    config->will_response_topic_ptr = &config->will_response_topic;

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CONFIG,
        "(%p) mqtt5_client_config - setting Will response topic to \"" PRInSTR "\"",
        (void *)config,
        AWS_BYTE_CURSOR_PRI(response_topic));

    return AWS_OP_SUCCESS;
}

int aws_mqtt5_client_config_set_will_correlation_data(
    struct aws_mqtt5_client_config *config,
    struct aws_byte_cursor correlation_data) {

    AWS_FATAL_ASSERT(config != NULL);

    if (s_conditional_init_byte_buf_to_cursor(&config->will_correlation_data, config->allocator, correlation_data)) {
        return AWS_OP_ERR;
    }

    config->will_correlation_data_ptr = &config->will_correlation_data;

    AWS_LOGF_DEBUG(AWS_LS_MQTT_CONFIG, "(%p) mqtt5_client_config - setting Will correlation data", (void *)config);

    return AWS_OP_SUCCESS;
}

void aws_mqtt5_client_config_set_will_delay(struct aws_mqtt5_client_config *config, uint32_t will_delay_seconds) {
    AWS_FATAL_ASSERT(config != NULL);
    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CONFIG,
        "(%p) mqtt5_client_config - setting Will delay to %" PRIu32 " seconds",
        (void *)config,
        will_delay_seconds);

    config->will_delay_seconds = will_delay_seconds;
}

int aws_mqtt5_client_config_set_will(
    struct aws_mqtt5_client_config *config,
    struct aws_byte_cursor topic,
    struct aws_byte_cursor payload,
    enum aws_mqtt5_qos qos) {
    AWS_FATAL_ASSERT(config != NULL);

    if (!aws_mqtt_is_valid_topic(&topic)) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT_CONFIG,
            "(%p) mqtt5_client_config - failed to set Will, invalid topic: \"" PRInSTR "\"",
            (void *)config,
            AWS_BYTE_CURSOR_PRI(topic));
        return aws_raise_error(AWS_ERROR_MQTT_INVALID_TOPIC);
    }

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CONFIG,
        "(%p) mqtt5_client_config - initializing Will.  Topic = \"" PRInSTR "\", qos = %d",
        (void *)config,
        AWS_BYTE_CURSOR_PRI(topic),
        (int)qos);

    if (s_conditional_init_byte_buf_to_cursor(&config->will_topic, config->allocator, topic) ||
        s_conditional_init_byte_buf_to_cursor(&config->will_payload, config->allocator, payload)) {

        struct aws_byte_cursor empty_topic = {.ptr = NULL, .len = 0};
        s_conditional_init_byte_buf_to_cursor(&config->will_topic, config->allocator, empty_topic);
        return AWS_OP_ERR;
    }

    config->will_qos = qos;

    return AWS_OP_SUCCESS;
}

void aws_mqtt5_client_config_set_will_retained(struct aws_mqtt5_client_config *config, bool retained) {
    AWS_FATAL_ASSERT(config != NULL);
    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CONFIG, "(%p) mqtt5_client_config - setting Will retained to %d", (void *)config, (int)retained);

    config->will_retained = retained;
}

int aws_mqtt5_client_config_add_will_user_property(
    struct aws_mqtt5_client_config *config,
    struct aws_mqtt5_user_property *property) {

    return s_add_user_property_to_array_list(config, property, &config->will_user_properties, "Will");
}

void aws_mqtt5_client_config_clear_will_user_properties(struct aws_mqtt5_client_config *config) {
    s_clear_user_properties_array_list(&config->will_user_properties);
}

void aws_mqtt5_client_config_set_lifecycle_event_handler(
    struct aws_mqtt5_client_config *config,
    aws_mqtt5_client_connection_event_callback_fn *lifecycle_event_handler) {

    AWS_FATAL_ASSERT(config != NULL);
    AWS_LOGF_DEBUG(AWS_LS_MQTT_CONFIG, "(%p) mqtt5_client_config - setting lifecycle event handler", (void *)config);

    config->lifecycle_event_handler = lifecycle_event_handler;
}

void aws_mqtt5_client_config_set_lifecycle_event_handler_user_data(
    struct aws_mqtt5_client_config *config,
    void *user_data) {

    AWS_FATAL_ASSERT(config != NULL);
    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CONFIG,
        "(%p) mqtt5_client_config - setting lifecycle event handler user data to %p",
        (void *)config,
        user_data);

    config->lifecycle_event_handler_user_data = user_data;
}
