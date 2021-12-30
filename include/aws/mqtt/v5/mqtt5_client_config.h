#ifndef AWS_MQTT_MQTT5_CLIENT_CONFIG_H
#define AWS_MQTT_MQTT5_CLIENT_CONFIG_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

#include <aws/mqtt/v5/mqtt5_types.h>

struct aws_allocator;
struct aws_mqtt5_client_config;
struct aws_http_proxy_strategy;
struct aws_tls_connection_options;
struct aws_socket_options;
struct aws_client_bootstrap;
struct aws_byte_cursor;

AWS_EXTERN_C_BEGIN

AWS_MQTT_API
struct aws_mqtt5_client_config *aws_mqtt5_client_config_new(struct aws_allocator *allocator);

AWS_MQTT_API
struct aws_mqtt5_client_config *aws_mqtt5_client_config_new_clone(
    struct aws_allocator *allocator,
    struct aws_mqtt5_client_config *from);

AWS_MQTT_API
void aws_mqtt5_client_config_destroy(struct aws_mqtt5_client_config *config);

AWS_MQTT_API
int aws_mqtt5_client_config_validate(struct aws_mqtt5_client_config *config);

/*
 * Underlying transport (pre-mqtt - tcp/websocket/tls) configuration
 *
 * host_name
 * port
 * connection bootstrap
 * socket options
 * tls options
 * http proxy options
 * websocket options
 */

AWS_MQTT_API int aws_mqtt5_client_config_set_host_name(
    struct aws_mqtt5_client_config *config,
    struct aws_byte_cursor host_name);

AWS_MQTT_API void aws_mqtt5_client_config_set_port(struct aws_mqtt5_client_config *config, uint16_t port);

AWS_MQTT_API void aws_mqtt5_client_config_set_bootstrap(
    struct aws_mqtt5_client_config *config,
    struct aws_client_bootstrap *bootstrap);

AWS_MQTT_API void aws_mqtt5_client_config_set_socket_options(
    struct aws_mqtt5_client_config *config,
    struct aws_socket_options *socket_options);

AWS_MQTT_API int aws_mqtt5_client_config_set_tls_connection_options(
    struct aws_mqtt5_client_config *config,
    struct aws_tls_connection_options *tls_options);

AWS_MQTT_API int aws_mqtt5_client_config_set_http_proxy_host_name(
    struct aws_mqtt5_client_config *config,
    struct aws_byte_cursor proxy_host_name);

AWS_MQTT_API void aws_mqtt5_client_config_set_http_proxy_port(struct aws_mqtt5_client_config *config, uint16_t port);

AWS_MQTT_API int aws_mqtt5_client_config_set_http_proxy_tls_connection_options(
    struct aws_mqtt5_client_config *config,
    struct aws_tls_connection_options *tls_options);

AWS_MQTT_API void aws_mqtt5_client_config_set_http_proxy_strategy(
    struct aws_mqtt5_client_config *config,
    struct aws_http_proxy_strategy *strategy);

AWS_MQTT_API void aws_mqtt5_client_config_set_websocket_transform(
    struct aws_mqtt5_client_config *config,
    aws_mqtt5_transform_websocket_handshake_fn *transform);

AWS_MQTT_API void aws_mqtt5_client_config_set_websocket_transform_user_data(
    struct aws_mqtt5_client_config *config,
    void *user_data);

/*
 * Reconnect configuration
 *
 * reconnect behavior
 * reconnect min/max delay
 * reconnect delay reset interval
 */

AWS_MQTT_API void aws_mqtt5_client_config_set_reconnect_behavior(
    struct aws_mqtt5_client_config *config,
    enum aws_mqtt5_client_reconnect_behavior_type reconnect_behavior);

AWS_MQTT_API void aws_mqtt5_client_config_set_reconnect_delay_ms(
    struct aws_mqtt5_client_config *config,
    uint64_t min_reconnect_delay_ms,
    uint64_t max_reconnect_delay_ms);

AWS_MQTT_API void aws_mqtt5_client_config_set_reconnect_delay_reset_interval_ms(
    struct aws_mqtt5_client_config *config,
    uint64_t min_connected_time_to_reset_reconnect_delay_ms);

/*
 * Mqtt timeout options
 *
 * keep alive interval
 * ping timeout
 */

AWS_MQTT_API void aws_mqtt5_client_config_set_keep_alive_interval_ms(
    struct aws_mqtt5_client_config *config,
    uint32_t keep_alive_interval_ms);

AWS_MQTT_API void aws_mqtt5_client_config_set_ping_timeout_ms(
    struct aws_mqtt5_client_config *config,
    uint32_t ping_timeout_ms);

/*
 * CONNECT configuration - basic connect properties
 *
 * client id
 * username
 * password
 * session expiry interval
 * session behavior
 * authentication method
 * authentication data
 * request response information
 * request problem information
 * receive maximum
 * topic alias maximum
 * maximum packet size
 */

AWS_MQTT_API int aws_mqtt5_client_config_set_client_id(
    struct aws_mqtt5_client_config *config,
    struct aws_byte_cursor client_id);

AWS_MQTT_API int aws_mqtt5_client_config_set_connect_username(
    struct aws_mqtt5_client_config *config,
    struct aws_byte_cursor username);

AWS_MQTT_API int aws_mqtt5_client_config_set_connect_password(
    struct aws_mqtt5_client_config *config,
    struct aws_byte_cursor password);

AWS_MQTT_API void aws_mqtt5_client_config_set_connect_session_expiry_interval_seconds(
    struct aws_mqtt5_client_config *config,
    uint32_t session_expiry_interval_seconds);

AWS_MQTT_API void aws_mqtt5_client_config_set_connect_session_behavior(
    struct aws_mqtt5_client_config *config,
    enum aws_mqtt5_client_session_behavior_type session_behavior);

AWS_MQTT_API int aws_mqtt5_client_config_set_connect_authentication_method(
    struct aws_mqtt5_client_config *config,
    struct aws_byte_cursor authentication_method);

AWS_MQTT_API int aws_mqtt5_client_config_set_connect_authentication_data(
    struct aws_mqtt5_client_config *config,
    struct aws_byte_cursor authentication_data);

AWS_MQTT_API void aws_mqtt5_client_config_set_connect_request_response_information(
    struct aws_mqtt5_client_config *config,
    bool request_response_information);

AWS_MQTT_API void aws_mqtt5_client_config_set_connect_request_problem_information(
    struct aws_mqtt5_client_config *config,
    bool request_problem_information);

AWS_MQTT_API void aws_mqtt5_client_config_set_connect_receive_maximum(
    struct aws_mqtt5_client_config *config,
    uint16_t receive_maximum);

AWS_MQTT_API void aws_mqtt5_client_config_set_connect_topic_alias_maximum(
    struct aws_mqtt5_client_config *config,
    uint16_t topic_alias_maximum);

AWS_MQTT_API void aws_mqtt5_client_config_set_connect_maximum_packet_size(
    struct aws_mqtt5_client_config *config,
    uint32_t maximum_packet_size_bytes);

AWS_MQTT_API int aws_mqtt5_client_config_add_connect_user_property(
    struct aws_mqtt5_client_config *config,
    struct aws_mqtt5_user_property *property);

/*
 * CONNECT configuration - will properties
 *
 * ToDo: Consider moving this to whatever the Publish operation representation ends up being, likely a single function
 * taking a compound data type Caveat: there are some will-only properties here (delay)
 */

AWS_MQTT_API void aws_mqtt5_client_config_set_will_payload_format(
    struct aws_mqtt5_client_config *config,
    enum aws_mqtt5_payload_format_indicator payload_format);

AWS_MQTT_API void aws_mqtt5_client_config_set_will_message_expiry(
    struct aws_mqtt5_client_config *config,
    uint32_t message_expiry_seconds);

AWS_MQTT_API int aws_mqtt5_client_config_set_will_content_type(
    struct aws_mqtt5_client_config *config,
    struct aws_byte_cursor content_type);

AWS_MQTT_API int aws_mqtt5_client_config_set_will_response_topic(
    struct aws_mqtt5_client_config *config,
    struct aws_byte_cursor response_topic);

AWS_MQTT_API int aws_mqtt5_client_config_set_will_correlation_data(
    struct aws_mqtt5_client_config *config,
    struct aws_byte_cursor correlation_data);

AWS_MQTT_API void aws_mqtt5_client_config_set_will_delay(
    struct aws_mqtt5_client_config *config,
    uint32_t will_delay_seconds);

AWS_MQTT_API int aws_mqtt5_client_config_set_will(
    struct aws_mqtt5_client_config *config,
    struct aws_byte_cursor topic,
    struct aws_byte_cursor payload,
    enum aws_mqtt5_qos qos);

AWS_MQTT_API void aws_mqtt5_client_config_set_will_retained(struct aws_mqtt5_client_config *config, bool retained);

AWS_MQTT_API int aws_mqtt5_client_config_add_will_user_property(
    struct aws_mqtt5_client_config *config,
    struct aws_mqtt5_user_property *property);

/*
 * Lifecycle event handling configuration
 */
AWS_MQTT_API void aws_mqtt5_client_config_set_lifecycle_event_handler(
    struct aws_mqtt5_client_config *config,
    aws_mqtt5_client_connection_event_callback_fn *lifecycle_event_handler);

AWS_MQTT_API void aws_mqtt5_client_config_set_lifecycle_event_handler_user_data(
    struct aws_mqtt5_client_config *config,
    void *user_data);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_MQTT5_CLIENT_CONFIG_H */
