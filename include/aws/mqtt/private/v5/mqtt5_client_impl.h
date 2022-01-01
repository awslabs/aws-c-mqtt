#ifndef AWS_MQTT_MQTT5_CLIENT_IMPL_H
#define AWS_MQTT_MQTT5_CLIENT_IMPL_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

#include <aws/common/ref_count.h>
#include <aws/io/socket.h>
#include <aws/io/tls_channel_handler.h>
#include <aws/mqtt/v5/mqtt5_types.h>

struct aws_client_bootstrap;
struct aws_event_loop;

#define AWS_MQTT5_DEFAULT_MIN_RECONNECT_DELAY_MS 1000
#define AWS_MQTT5_DEFAULT_MAX_RECONNECT_DELAY_MS 120000
#define AWS_MQTT5_DEFAULT_MIN_CONNECTED_TIME_TO_RESET_RECONNECT_DELAY_MS 30000
#define AWS_MQTT5_DEFAULT_KEEP_ALIVE_INTERVAL_MS 1200000
#define AWS_MQTT5_DEFAULT_PING_TIMEOUT_MS 3000

/* ToDo: this is almost certainly the wrong value to use as a default */
#define AWS_MQTT5_DEFAULT_SESSION_EXPIRY_INTERVAL_SECONDS 0

struct aws_mqtt5_name_value_pair {
    struct aws_byte_buf name_value_pair;
    struct aws_byte_cursor name;
    struct aws_byte_cursor value;
};

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
    struct aws_byte_buf *password_ptr;

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

    struct aws_array_list connect_user_properties;

    enum aws_mqtt5_payload_format_indicator will_payload_format;

    uint32_t will_message_expiry_seconds;
    uint32_t *will_message_expiry_seconds_ptr;

    struct aws_byte_buf will_content_type;
    struct aws_byte_buf *will_content_type_ptr;

    struct aws_byte_buf will_response_topic;
    struct aws_byte_buf *will_response_topic_ptr;

    struct aws_byte_buf will_correlation_data;
    struct aws_byte_buf *will_correlation_data_ptr;

    uint32_t will_delay_seconds;
    enum aws_mqtt5_qos will_qos;

    struct aws_byte_buf will_topic;
    struct aws_byte_buf will_payload;

    bool will_retained;

    struct aws_array_list will_user_properties;

    aws_mqtt5_client_connection_event_callback_fn *lifecycle_event_handler;
    void *lifecycle_event_handler_user_data;
};

struct aws_mqtt5_client {
    struct aws_allocator *allocator;
    struct aws_ref_count ref_count;
    const struct aws_mqtt5_client_config *config;

    struct aws_event_loop *loop;
};

/*
 * Testing only APIs
 */

AWS_EXTERN_C_BEGIN

AWS_MQTT_API void aws_mqtt5_client_config_clear_connect_user_properties(struct aws_mqtt5_client_config *config);

AWS_MQTT_API void aws_mqtt5_client_config_clear_will_user_properties(struct aws_mqtt5_client_config *config);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_MQTT5_CLIENT_IMPL_H */
