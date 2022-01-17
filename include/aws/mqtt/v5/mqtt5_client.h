#ifndef AWS_MQTT_MQTT5_CLIENT_H
#define AWS_MQTT_MQTT5_CLIENT_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

#include <aws/mqtt/v5/mqtt5_types.h>

struct aws_allocator;
struct aws_byte_cursor;
struct aws_client_bootstrap;
struct aws_input_stream;
struct aws_mqtt5_client;
struct aws_tls_connection_options;
struct aws_socket_options;

struct aws_mqtt5_client_options {
    struct aws_byte_cursor host_name;
    uint16_t port;
    struct aws_client_bootstrap *bootstrap;
    struct aws_socket_options *socket_options;
    struct aws_tls_connection_options *tls_options;
    struct aws_http_proxy_options *http_proxy_options;

    aws_mqtt5_transform_websocket_handshake_fn *websocket_handshake_transform;
    void *websocket_handshake_transform_user_data;

    struct aws_mqtt5_packet_connect_view *connect_options;

    enum aws_mqtt5_client_outbound_topic_alias_behavior_type outbound_topic_aliasing_behavior;

    enum aws_mqtt5_client_reconnect_behavior_type reconnect_behavior;
    uint64_t min_reconnect_delay_ms;
    uint64_t max_reconnect_delay_ms;
    uint64_t min_connected_time_to_reset_reconnect_delay_ms;

    uint32_t ping_timeout_ms;

    aws_mqtt5_client_connection_event_callback_fn *lifecycle_event_handler;
    void *lifecycle_event_handler_user_data;
};

AWS_EXTERN_C_BEGIN

AWS_MQTT_API
struct aws_mqtt5_client *aws_mqtt5_client_new(
    struct aws_allocator *allocator,
    struct aws_mqtt5_client_options *options);

AWS_MQTT_API
struct aws_mqtt5_client *aws_mqtt5_client_acquire(struct aws_mqtt5_client *client);

AWS_MQTT_API
struct aws_mqtt5_client *aws_mqtt5_client_release(struct aws_mqtt5_client *client);

AWS_MQTT_API
int aws_mqtt5_client_start(struct aws_mqtt5_client *client);

AWS_MQTT_API
int aws_mqtt5_client_stop(
    struct aws_mqtt5_client *client,
    const struct aws_mqtt5_packet_disconnect_view *disconnect_options);

AWS_MQTT_API
int aws_mqtt5_client_publish(
    struct aws_mqtt5_client *client,
    const struct aws_mqtt5_packet_publish_view *publish_options,
    struct aws_input_stream *payload,
    const struct aws_mqtt5_publish_completion_options *completion_options);

AWS_MQTT_API
int aws_mqtt5_client_subscribe(
    struct aws_mqtt5_client *client,
    const struct aws_mqtt5_packet_subscribe_view *subscribe_options,
    const struct aws_mqtt5_subscribe_completion_options *completion_options);

AWS_MQTT_API
int aws_mqtt5_client_unsubscribe(
    struct aws_mqtt5_client *client,
    const struct aws_mqtt5_packet_unsubscribe_view *unsubscribe_options,
    const struct aws_mqtt5_unsubscribe_completion_options *completion_options);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_MQTT5_CLIENT_H */
