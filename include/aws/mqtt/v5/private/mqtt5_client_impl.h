#ifndef AWS_MQTT_MQTT5_CLIENT_IMPL_H
#define AWS_MQTT_MQTT5_CLIENT_IMPL_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

#include <aws/io/socket.h>
#include <aws/io/tls_channel_handler.h>
#include <aws/mqtt/v5/mqtt5_types.h>

struct aws_client_bootstrap;

struct aws_mqtt5_client_config {
    struct aws_allocator *allocator;

    struct aws_byte_buf host_name;
    uint16_t port;
    struct aws_client_bootstrap *bootstrap;
    struct aws_socket_options socket_options;

    struct aws_tls_connection_options tls_options;
    struct aws_tls_connection_options *tls_options_ptr;

    struct aws_byte_buf http_proxy_host_name;
    uint16_t http_proxy_port;
    struct aws_tls_connection_options http_proxy_tls_options;
    struct aws_tls_connection_options *http_proxy_tls_options_ptr;
    struct aws_http_proxy_strategy *http_proxy_strategy;

    aws_mqtt5_transform_websocket_handshake_fn *websocket_handshake_transform;
    void *websocket_handshake_transform_user_data;

    enum aws_mqtt5_client_reconnect_behavior_type reconnect_behavior;
    uint64_t min_reconnect_delay_ms;
    uint64_t max_reconnect_delay_ms;
    uint64_t min_connected_time_to_reset_reconnect_delay_ms;

    uint32_t keep_alive_interval_ms;
    uint32_t ping_timeout_ms;

    struct aws_byte_buf client_id;

    struct aws_byte_buf username;
    struct aws_byte_buf *username_ptr;

    struct aws_byte_buf password;

    uint32_t session_expiry_interval_seconds;
    enum aws_mqtt5_client_session_behavior_type session_behavior;

    struct aws_byte_buf authentication_method;
    struct aws_byte_buf *authentication_method_ptr;

    struct aws_byte_buf authentication_data;
    struct aws_byte_buf *authentication_data_ptr;

    bool request_response_information;
    bool request_problem_information;
    uint16_t receive_maximum;
    uint16_t topic_alias_maximum;
    uint32_t maximum_packet_size_bytes;
};

struct aws_mqtt5_client {
    struct aws_mqtt5_client_config *config;
};

#endif /* AWS_MQTT_MQTT5_CLIENT_IMPL_H */
