#ifndef AWS_MQTT_V311_CLIENT_CONFIG_H
#define AWS_MQTT_V311_CLIENT_CONFIG_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

#include <aws/common/byte_buf.h>
#include <aws/io/socket.h>
#include <aws/io/tls_channel_handler.h>
#include <aws/mqtt/v311/mqtt311_client.h>

struct aws_client_bootstrap;


struct aws_mqtt311_client_connect_options {
    struct aws_byte_cursor host;
    uint16_t port;
    struct aws_socket_options *socket_options;
    struct aws_tls_connection_options *tls_options;
    struct aws_byte_cursor client_id;
    uint16_t keep_alive_time_secs;
    uint32_t ping_timeout_ms;
    uint64_t protocol_operation_timeout_ms;
    aws_mqtt_client_on_connection_complete_fn *on_connection_complete;
    void *user_data;
    bool clean_session;
};

struct aws_mqtt311_client_options {
    struct aws_client_bootstrap *bootstrap;
};

struct aws_mqtt311_client_config {
    struct aws_allocator *allocator;

    struct aws_byte_buf host;
    uint16_t port;
    struct aws_socket_options socket_options;
    struct aws_tls_connection_options *tls_options_ptr;
    struct aws_tls_connection_options tls_options;
    struct aws_client_bootstrap *bootstrap;
    struct aws_byte_buf client_id;
    uint16_t keep_alive_time_secs;
    uint32_t ping_timeout_ms;
    uint64_t protocol_operation_timeout_ms;
    aws_mqtt_client_on_connection_complete_fn *on_connection_complete;
    void *user_data;
    bool clean_session;
};

AWS_EXTERN_C_BEGIN

AWS_MQTT_API struct aws_mqtt311_client_config *aws_mqtt311_client_config_new_from_options(
    struct aws_allocator *allocator,
    const struct aws_mqtt311_client_options *options);

AWS_MQTT_API void aws_mqtt311_client_config_destroy(struct aws_mqtt311_client_config *config);

AWS_MQTT_API struct aws_mqtt311_client_connect_config *aws_mqtt311_client_connect_config_new_from_options(
    struct aws_allocator *allocator,
    const struct aws_mqtt311_client_connect_options *options);

AWS_MQTT_API void aws_mqtt311_client_connect_config_destroy(struct aws_mqtt311_client_connect_config *config);

AWS_MQTT_API void aws_mqtt311_client_config_apply_connect_config(struct aws_mqtt311_client_config *config,
                                                                 const struct aws_mqtt311_client_connect_config *connect_config);

AWS_EXTERN_C_END

#endif // AWS_MQTT_V311_CLIENT_CONFIG_H
