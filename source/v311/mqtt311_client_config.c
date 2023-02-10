/**
* Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
* SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/mqtt/private/v311/mqtt311_client_config.h"

#include <aws/io/channel_bootstrap.h>

struct aws_mqtt311_client_config *aws_mqtt311_client_config_new_from_options(
    struct aws_allocator *allocator,
    const struct aws_mqtt311_client_options *options) {

    struct aws_mqtt311_client_config *config = aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt311_client_config));
    config->allocator = allocator;

    config->bootstrap = aws_client_bootstrap_acquire(options->bootstrap);

    return config;
}

void aws_mqtt311_client_config_destroy(struct aws_mqtt311_client_config *config) {
    if (config == NULL) {
        return;
    }

    if (config->tls_options_ptr != NULL) {
        aws_tls_connection_options_clean_up(config->tls_options_ptr);
    }

    aws_byte_buf_clean_up(&config->host);
    aws_byte_buf_clean_up(&config->client_id);
    
    aws_client_bootstrap_release(config->bootstrap);

    aws_mem_release(config->allocator, config);
}

struct aws_mqtt311_client_connect_config {
    struct aws_allocator *allocator;

    struct aws_byte_buf host;
    uint16_t port;
    struct aws_socket_options socket_options;
    struct aws_tls_connection_options *tls_options_ptr;
    struct aws_tls_connection_options tls_options;
    struct aws_byte_buf client_id;
    uint16_t keep_alive_time_secs;
    uint32_t ping_timeout_ms;
    uint64_t protocol_operation_timeout_ms;
    aws_mqtt_client_on_connection_complete_fn *on_connection_complete;
    void *user_data;
    bool clean_session;
};

#define AWS_MQTT311_DEFAULT_KEEP_ALIVE_SECONDS 1200
#define AWS_MQTT311_DEFAULT_SOCKET_CONNECT_TIMEOUT_MS 10000

struct aws_mqtt311_client_connect_config *aws_mqtt311_client_connect_config_new_from_options(
    struct aws_allocator *allocator,
    const struct aws_mqtt311_client_connect_options *options) {

    struct aws_mqtt311_client_connect_config *config = aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt311_client_connect_config));
    config->allocator = allocator;
    aws_byte_buf_init_copy_from_cursor(&config->host, allocator, options->host);
    config->port = options->port;
    if (options->socket_options != NULL) {
        config->socket_options = *options->socket_options;
    } else {
        config->socket_options.type = AWS_SOCKET_STREAM;
        config->socket_options.connect_timeout_ms = AWS_MQTT311_DEFAULT_SOCKET_CONNECT_TIMEOUT_MS;
    }

    if (options->tls_options != NULL) {
        if (aws_tls_connection_options_copy(&config->tls_options, options->tls_options)) {
            goto error;
        }
        config->tls_options_ptr = &config->tls_options;
    }

    aws_byte_buf_init_copy_from_cursor(&config->client_id, allocator, options->client_id);

    config->keep_alive_time_secs = options->keep_alive_time_secs > 0 ? options->keep_alive_time_secs : AWS_MQTT311_DEFAULT_KEEP_ALIVE_SECONDS;
    config->ping_timeout_ms = options->ping_timeout_ms;
    config->protocol_operation_timeout_ms = options->protocol_operation_timeout_ms > 0 ? options->protocol_operation_timeout_ms : UINT64_MAX;
    config->on_connection_complete = options->on_connection_complete;
    config->user_data = options->user_data;
    config->clean_session = options->clean_session;

    return config;

error:

    aws_mqtt311_client_connect_config_destroy(config);

    return NULL;
}

void aws_mqtt311_client_connect_config_destroy(struct aws_mqtt311_client_connect_config *config) {
    if (config == NULL) {
        return;
    }

    if (config->tls_options_ptr != NULL) {
        aws_tls_connection_options_clean_up(config->tls_options_ptr);
    }

    aws_byte_buf_clean_up(&config->host);
    aws_byte_buf_clean_up(&config->client_id);

    aws_mem_release(config->allocator, config);
}

void aws_mqtt311_client_config_apply_connect_config(struct aws_mqtt311_client_config *config,
                                                                 const struct aws_mqtt311_client_connect_config *connect_config) {

    aws_byte_buf_clean_up(&config->host);
    aws_byte_buf_init_copy_from_cursor(&config->host, config->allocator, aws_byte_cursor_from_buf(&connect_config->host));

    config->port = connect_config->port;
    config->socket_options = connect_config->socket_options;

    if (config->tls_options_ptr) {
        aws_tls_connection_options_clean_up(config->tls_options_ptr);
    }

    if (connect_config->tls_options_ptr != NULL) {
        aws_tls_connection_options_copy(&config->tls_options, connect_config->tls_options_ptr);
        config->tls_options_ptr = &config->tls_options;
    } else {
        config->tls_options_ptr = NULL;
    }

    aws_byte_buf_clean_up(&config->client_id);
    aws_byte_buf_init_copy_from_cursor(&config->client_id, config->allocator, aws_byte_cursor_from_buf(&connect_config->client_id));

    config->keep_alive_time_secs = connect_config->keep_alive_time_secs;
    config->ping_timeout_ms = connect_config->ping_timeout_ms;
    config->protocol_operation_timeout_ms = connect_config->protocol_operation_timeout_ms;
    config->on_connection_complete = connect_config->on_connection_complete;
    config->user_data = connect_config->user_data;
    config->clean_session = connect_config->clean_session;
}
