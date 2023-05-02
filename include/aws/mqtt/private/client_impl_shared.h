#ifndef AWS_MQTT_PRIVATE_CLIENT_IMPL_SHARED_H
#define AWS_MQTT_PRIVATE_CLIENT_IMPL_SHARED_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/client.h>

struct aws_mqtt_client_connection;

struct aws_mqtt_client_connection_vtable {

    int (*set_will_fn)(
        void *impl,
        const struct aws_byte_cursor *topic,
        enum aws_mqtt_qos qos,
        bool retain,
        const struct aws_byte_cursor *payload);

    int (*set_login_fn)(void *impl, const struct aws_byte_cursor *username, const struct aws_byte_cursor *password);

    int (*use_websockets_fn)(
        void *impl,
        aws_mqtt_transform_websocket_handshake_fn *transformer,
        void *transformer_ud,
        aws_mqtt_validate_websocket_handshake_fn *validator,
        void *validator_ud);

    int (*set_http_proxy_options_fn)(void *impl, struct aws_http_proxy_options *proxy_options);

    int (*set_host_resolution_options_fn)(void *impl, struct aws_host_resolution_config *host_resolution_config);

    int (*set_reconnect_timeout_fn)(void *impl, uint64_t min_timeout, uint64_t max_timeout);

    int (*set_connection_interruption_handlers_fn)(
        void *impl,
        aws_mqtt_client_on_connection_interrupted_fn *on_interrupted,
        void *on_interrupted_ud,
        aws_mqtt_client_on_connection_resumed_fn *on_resumed,
        void *on_resumed_ud);

    int (*set_connection_closed_handler_fn)(
        void *impl,
        aws_mqtt_client_on_connection_closed_fn *on_closed,
        void *on_closed_ud);

    int (*set_on_any_publish_handler_fn)(
        void *impl,
        aws_mqtt_client_publish_received_fn *on_any_publish,
        void *on_any_publish_ud);

    int (*connect_fn)(void *impl, const struct aws_mqtt_connection_options *connection_options);

    int (*reconnect_fn)(void *impl, aws_mqtt_client_on_connection_complete_fn *on_connection_complete, void *userdata);

    int (*disconnect_fn)(void *impl, aws_mqtt_client_on_disconnect_fn *on_disconnect, void *userdata);

    uint16_t (*subscribe_multiple_fn)(
        void *impl,
        const struct aws_array_list *topic_filters,
        aws_mqtt_suback_multi_fn *on_suback,
        void *on_suback_ud);

    uint16_t (*subscribe_fn)(
        void *impl,
        const struct aws_byte_cursor *topic_filter,
        enum aws_mqtt_qos qos,
        aws_mqtt_client_publish_received_fn *on_publish,
        void *on_publish_ud,
        aws_mqtt_userdata_cleanup_fn *on_ud_cleanup,
        aws_mqtt_suback_fn *on_suback,
        void *on_suback_ud);

    uint16_t (*subscribe_local_fn)(
        void *impl,
        const struct aws_byte_cursor *topic_filter,
        aws_mqtt_client_publish_received_fn *on_publish,
        void *on_publish_ud,
        aws_mqtt_userdata_cleanup_fn *on_ud_cleanup,
        aws_mqtt_suback_fn *on_suback,
        void *on_suback_ud);

    uint16_t (*resubscribe_existing_topics_fn)(void *impl, aws_mqtt_suback_multi_fn *on_suback, void *on_suback_ud);

    uint16_t (*unsubscribe_fn)(
        void *impl,
        const struct aws_byte_cursor *topic_filter,
        aws_mqtt_op_complete_fn *on_unsuback,
        void *on_unsuback_ud);

    uint16_t (*publish_fn)(
        void *impl,
        const struct aws_byte_cursor *topic,
        enum aws_mqtt_qos qos,
        bool retain,
        const struct aws_byte_cursor *payload,
        aws_mqtt_op_complete_fn *on_complete,
        void *userdata);

    int (*get_stats_fn)(void *impl, struct aws_mqtt_connection_operation_statistics *stats);
};

struct aws_mqtt_client_connection {
    struct aws_mqtt_client_connection_vtable *vtable;
    void *impl;
    struct aws_ref_count ref_count;
};

#endif /* AWS_MQTT_PRIVATE_CLIENT_IMPL_SHARED_H */
