/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/v5/mqtt5_client_config.h>

#include <aws/io/channel_bootstrap.h>
#include <aws/mqtt/v5/private/mqtt5_client_impl.h>

struct aws_mqtt5_client_config *aws_mqtt5_client_config_new(struct aws_allocator *allocator) {
    AWS_FATAL_ASSERT(allocator != NULL);

    struct aws_mqtt5_client_config *config = aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt5_client_config));

    config->allocator = allocator;

    return config;
}

struct aws_mqtt5_client_config *aws_mqtt5_client_config_new_clone(
    struct aws_allocator *allocator,
    struct aws_mqtt5_client_config *from) {

    AWS_FATAL_ASSERT(allocator != NULL);
    struct aws_mqtt5_client_config *config = aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt5_client_config));

    config->allocator = allocator;

    aws_mqtt5_client_config_set_host_name(config, aws_byte_cursor_from_buf(&from->host_name));
    aws_mqtt5_client_config_set_port(config, from->port);
    aws_mqtt5_client_config_set_bootstrap(config, from->bootstrap);
    aws_mqtt5_client_config_set_socket_options(config, &from->socket_options);

    return config;
}

void aws_mqtt5_client_config_destroy(struct aws_mqtt5_client_config *config) {
    if (config == NULL) {
        return;
    }

    aws_byte_buf_clean_up(&config->host_name);
    aws_client_bootstrap_release(config->bootstrap);

    aws_mem_release(config->allocator, config);
}

int aws_mqtt5_client_config_validate(struct aws_mqtt5_client_config *config) {
    if (config->host_name.len == 0) {
        return AWS_ERROR_MQTT_CONFIG_VALIDATION_HOST_NOT_SET;
    }

    if (config->port == 0) {
        return AWS_ERROR_MQTT_CONFIG_VALIDATION_PORT_NOT_SET;
    }

    if (config->bootstrap == NULL) {
        return AWS_ERROR_MQTT_CONFIG_VALIDATION_CLIENT_BOOTSTRAP_NOT_SET;
    }

    /* forbid no-timeout until someone convinces me otherwise */
    if (config->socket_options.type != AWS_SOCKET_STREAM || config->socket_options.connect_timeout_ms == 0) {
        return AWS_ERROR_MQTT_CONFIG_VALIDATION_INVALID_SOCKET_OPTIONS;
    }

    return AWS_ERROR_SUCCESS;
}

void aws_mqtt5_client_config_set_host_name(struct aws_mqtt5_client_config *config, struct aws_byte_cursor host_name) {

    AWS_FATAL_ASSERT(config != NULL);
    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_CONFIG,
        "(%p) mqtt5_client_config - setting host to " PRInSTR,
        (void *)config,
        AWS_BYTE_CURSOR_PRI(host_name));

    aws_byte_buf_clean_up(&config->host_name);
    aws_byte_buf_init_copy_from_cursor(&config->host_name, config->allocator, host_name);
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
        "(%p) mqtt5_client_config - setting setting socket options, type = %d, domain = %d, connect_timeout_ms = %u",
        (void *)config,
        (int)socket_options->type,
        (int)socket_options->domain,
        (uint32_t)socket_options->connect_timeout_ms);
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

void aws_mqtt5_client_config_set_tls_connection_options(
    struct aws_mqtt5_client_config *config,
    struct aws_tls_connection_options *tls_options) {
    AWS_FATAL_ASSERT(config != NULL);
}

void aws_mqtt5_client_config_set_http_proxy_options(
    struct aws_mqtt5_client_config *config,
    struct aws_http_proxy_options *http_proxy_options) {
    AWS_FATAL_ASSERT(config != NULL);
}

void aws_mqtt5_client_config_set_websocket_options(
    struct aws_mqtt5_client_config *config,
    struct aws_mqtt5_websocket_options *websocket_options) {
    AWS_FATAL_ASSERT(config != NULL);
}

void aws_mqtt5_client_config_set_reconnect_behavior(
    struct aws_mqtt5_client_config *config,
    enum aws_mqtt5_client_reconnect_behavior_type reconnect_behavior);

void aws_mqtt5_client_config_set_reconnect_delay(
    struct aws_mqtt5_client_config *config,
    uint64_t min_reconnect_delay_ms,
    uint64_t max_reconnect_delay_ms);

void aws_mqtt5_client_config_set_reconnect_delay_reset_interval(
    struct aws_mqtt5_client_config *config,
    uint64_t min_connected_time_to_reset_reconnect_delay_ms);

void aws_mqtt5_client_config_set_keep_alive_interval_ms(
    struct aws_mqtt5_client_config *config,
    uint32_t keep_alive_interval_ms);

void aws_mqtt5_client_config_set_ping_timeout_ms(struct aws_mqtt5_client_config *config, uint32_t ping_timeout_ms);

void aws_mqtt5_client_config_set_client_id(struct aws_mqtt5_client_config *config, struct aws_byte_cursor client_id);

void aws_mqtt5_client_config_set_connect_username(
    struct aws_mqtt5_client_config *config,
    struct aws_byte_cursor username);

void aws_mqtt5_client_config_set_connect_password(
    struct aws_mqtt5_client_config *config,
    struct aws_byte_cursor password);

void aws_mqtt5_client_config_set_connect_session_expiry_interval_seconds(
    struct aws_mqtt5_client_config *config,
    uint32_t session_expiry_interval_seconds);

void aws_mqtt5_client_config_set_connect_session_behavior(
    struct aws_mqtt5_client_config *config,
    enum aws_mqtt5_client_session_behavior_type);

void aws_mqtt5_client_config_set_connect_authentication_method(
    struct aws_mqtt5_client_config *config,
    struct aws_byte_cursor authentication_method);

void aws_mqtt5_client_config_set_connect_authentication_data(
    struct aws_mqtt5_client_config *config,
    struct aws_byte_cursor authentication_data);

void aws_mqtt5_client_config_set_connect_request_response_information(
    struct aws_mqtt5_client_config *config,
    bool request_response_information);

void aws_mqtt5_client_config_set_connect_request_problem_information(
    struct aws_mqtt5_client_config *config,
    bool request_problem_information);

void aws_mqtt5_client_config_set_connect_receive_maximum(
    struct aws_mqtt5_client_config *config,
    uint16_t receive_maximum);

void aws_mqtt5_client_config_set_connect_topic_alias_maximum(
    struct aws_mqtt5_client_config *config,
    uint16_t topic_alias_maximum);

void aws_mqtt5_client_config_set_connect_maximum_packet_size(
    struct aws_mqtt5_client_config *config,
    uint32_t maximum_packet_size_bytes);

void aws_mqtt5_client_config_set_will_payload_format(
    struct aws_mqtt5_client_config *config,
    enum aws_mqtt5_payload_format_indicator payload_format);

void aws_mqtt5_client_config_set_will_message_expiry(
    struct aws_mqtt5_client_config *config,
    uint32_t message_expiry_seconds);

void aws_mqtt5_client_config_set_will_content_type(
    struct aws_mqtt5_client_config *config,
    struct aws_byte_cursor content_type);

void aws_mqtt5_client_config_set_will_response_topic(
    struct aws_mqtt5_client_config *config,
    struct aws_byte_cursor response_topic);

void aws_mqtt5_client_config_set_will_correlation_data(
    struct aws_mqtt5_client_config *config,
    struct aws_byte_cursor correlation_data);

void aws_mqtt5_client_config_set_will_delay(struct aws_mqtt5_client_config *config, uint32_t will_delay_seconds);

void aws_mqtt5_client_config_set_will_qos(struct aws_mqtt5_client_config *config, enum aws_mqtt5_qos qos);

void aws_mqtt5_client_config_set_will_topic(struct aws_mqtt5_client_config *config, struct aws_byte_cursor topic);

void aws_mqtt5_client_config_set_will_payload(struct aws_mqtt5_client_config *config, struct aws_byte_cursor payload);

void aws_mqtt5_client_config_set_will_retained(struct aws_mqtt5_client_config *config, bool retained);

void aws_mqtt5_client_config_add_will_user_property(
    struct aws_mqtt5_client_config *config,
    struct aws_mqtt5_user_property *property);

void aws_mqtt5_client_config_set_lifecycle_event_handler(
    struct aws_mqtt5_client_config *config,
    struct aws_mqtt5_client_lifecycle_event_handler_options *lifecycle_event_handling_options);
