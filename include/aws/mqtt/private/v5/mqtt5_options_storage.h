#ifndef AWS_MQTT_MQTT5_OPERATION_H
#define AWS_MQTT_MQTT5_OPERATION_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

#include <aws/common/linked_list.h>
#include <aws/common/ref_count.h>
#include <aws/http/proxy.h>
#include <aws/io/socket.h>
#include <aws/io/tls_channel_handler.h>
#include <aws/mqtt/v5/mqtt5_types.h>

struct aws_client_bootstrap;
struct aws_input_stream;
struct aws_mqtt5_client_options;
struct aws_mqtt5_operation;
struct aws_string;

struct aws_mqtt5_user_property_set {
    struct aws_array_list properties;
    struct aws_byte_buf property_storage;
};

/*
 * Some data binding rules of thumb:
 *
 * name-value pair - pair of cursors + aws_byte_buf
 * Required binary data - aws_byte_buf
 * Optional binary data - aws_byte_buf and aws_byte_buf *
 * UTF-8 String - aws_string * (NULL implies not present)
 * Required variable length integer - uint32_t
 * Optional variable length integer - uint32_t, uint32_t *
 * Require fixed-size integer = uint.._t
 * Optional fixed_size integer = uint.._t, uint.._t *
 *
 * Optional enum values may use the (value, value *) pattern but some enums have a value that indicates no-value.
 */

/*
 * At its core, an operation is anything you want to tell the mqtt5 client to do.
 *
 * This is intentionally different and *not* in sync with aws_mqtt5_packet_type
 *
 * Current thinking is that user-facing packets are operations, but we reserve the right to make additional
 * operations that may represent compound (sub-pub-unsub for example) or external concepts
 *
 * We must be careful to make the functional interface for operations not assume a 1-1 with MQTT packets
 */
enum aws_mqtt5_operation_type {
    AWS_MOT_CONNECT,
    AWS_MOT_DISCONNECT,
    AWS_MOT_SUBSCRIBE,
    AWS_MOT_UNSUBSCRIBE,
    AWS_MOT_PUBLISH,
};

/**
 * This is the base structure for all mqtt operations.  It includes the type, a ref count, vtable and list
 * management.
 */
struct aws_mqtt5_operation {
    enum aws_mqtt5_operation_type operation_type;
    struct aws_ref_count ref_count;
    struct aws_linked_list_node node;
    void *impl;
};

struct aws_mqtt5_packet_connect_storage {
    uint32_t keep_alive_interval_seconds;

    struct aws_string *client_id;

    struct aws_string *username;

    struct aws_byte_buf password;
    struct aws_byte_buf *password_ptr;

    uint32_t session_expiry_interval_seconds;
    enum aws_mqtt5_client_session_behavior_type session_behavior;

    bool request_response_information;
    bool request_problem_information;
    uint16_t receive_maximum;
    uint16_t topic_alias_maximum;
    uint32_t maximum_packet_size_bytes;

    struct aws_mqtt5_operation_publish *will;
    uint32_t will_delay_interval_seconds;
    struct aws_input_stream *will_payload;

    struct aws_mqtt5_user_property_set user_properties;
};

struct aws_mqtt5_operation_connect {
    struct aws_mqtt5_operation base;
    struct aws_allocator *allocator;

    struct aws_mqtt5_packet_connect_storage options_storage;
};

struct aws_mqtt5_packet_publish_storage {
    bool dup;
    enum aws_mqtt5_qos qos;
    bool retain;
    struct aws_string *topic;
    enum aws_mqtt5_payload_format_indicator payload_format;
    uint32_t message_expiry_interval_seconds;
    uint32_t *message_expiry_interval_seconds_ptr;
    uint16_t topic_alias;
    uint16_t *topic_alias_ptr;
    struct aws_string *response_topic;
    struct aws_byte_buf correlation_data;
    struct aws_byte_buf *correlation_data_ptr;
    struct aws_string *content_type;

    struct aws_mqtt5_user_property_set user_properties;
};

struct aws_mqtt5_operation_publish {
    struct aws_mqtt5_operation base;
    struct aws_allocator *allocator;

    struct aws_mqtt5_packet_publish_storage options_storage;

    struct aws_input_stream *payload;

    struct aws_mqtt5_publish_completion_options completion_options;
};

struct aws_mqtt5_packet_disconnect_storage {
    enum aws_mqtt5_disconnect_reason_code reason_code;
    uint32_t session_expiry_interval_seconds;
    uint32_t *session_expiry_interval_seconds_ptr;
    struct aws_string *reason_string;

    struct aws_mqtt5_user_property_set user_properties;

    struct aws_string *server_reference;
};

struct aws_mqtt5_operation_disconnect {
    struct aws_mqtt5_operation base;
    struct aws_allocator *allocator;

    struct aws_mqtt5_packet_disconnect_storage options_storage;
};

struct aws_mqtt5_packet_subscribe_storage {
    uint32_t subscription_identifier;
    uint32_t *subscription_identifier_ptr;

    struct aws_array_list subscriptions;
    struct aws_byte_buf topic_filter_storage;

    struct aws_mqtt5_user_property_set user_properties;
};

struct aws_mqtt5_operation_subscribe {
    struct aws_mqtt5_operation base;
    struct aws_allocator *allocator;

    struct aws_mqtt5_packet_subscribe_storage options_storage;

    struct aws_mqtt5_subscribe_completion_options completion_options;
};

struct aws_mqtt5_packet_unsubscribe_storage {
    struct aws_array_list topics;
    struct aws_byte_buf topic_storage;

    struct aws_mqtt5_user_property_set user_properties;
};

struct aws_mqtt5_operation_unsubscribe {
    struct aws_mqtt5_operation base;
    struct aws_allocator *allocator;

    struct aws_mqtt5_packet_unsubscribe_storage options_storage;

    struct aws_mqtt5_unsubscribe_completion_options completion_options;
};

#define AWS_MQTT5_DEFAULT_MIN_RECONNECT_DELAY_MS 1000
#define AWS_MQTT5_DEFAULT_MAX_RECONNECT_DELAY_MS 120000
#define AWS_MQTT5_DEFAULT_MIN_CONNECTED_TIME_TO_RESET_RECONNECT_DELAY_MS 30000
#define AWS_MQTT5_DEFAULT_PING_TIMEOUT_MS 3000

struct aws_mqtt5_client_options_storage {
    struct aws_allocator *allocator;

    struct aws_string *host_name;
    uint16_t port;
    struct aws_client_bootstrap *bootstrap;
    struct aws_socket_options socket_options;

    struct aws_tls_connection_options tls_options;
    struct aws_tls_connection_options *tls_options_ptr;

    struct aws_http_proxy_options http_proxy_options;
    struct aws_http_proxy_config *http_proxy_config;

    aws_mqtt5_transform_websocket_handshake_fn *websocket_handshake_transform;
    void *websocket_handshake_transform_user_data;

    enum aws_mqtt5_client_outbound_topic_alias_behavior_type outbound_topic_aliasing_behavior;

    enum aws_mqtt5_client_reconnect_behavior_type reconnect_behavior;
    uint64_t min_reconnect_delay_ms;
    uint64_t max_reconnect_delay_ms;
    uint64_t min_connected_time_to_reset_reconnect_delay_ms;

    uint32_t ping_timeout_ms;

    struct aws_mqtt5_operation_connect *connect;

    aws_mqtt5_client_connection_event_callback_fn *lifecycle_event_handler;
    void *lifecycle_event_handler_user_data;
};

AWS_EXTERN_C_BEGIN

/* User properties */

AWS_MQTT_API int aws_mqtt5_user_property_set_init(
    struct aws_mqtt5_user_property_set *property_set,
    struct aws_allocator *allocator,
    size_t property_count,
    const struct aws_mqtt5_user_property *properties);

AWS_MQTT_API void aws_mqtt5_user_property_set_clean_up(struct aws_mqtt5_user_property_set *property_set);

AWS_MQTT_API size_t aws_mqtt5_user_property_set_size(struct aws_mqtt5_user_property_set *property_set);

AWS_MQTT_API int aws_mqtt5_user_property_set_get_property(
    struct aws_mqtt5_user_property_set *property_set,
    size_t index,
    struct aws_mqtt5_user_property *property_out);

/* Operation base */

AWS_MQTT_API struct aws_mqtt5_operation *aws_mqtt5_operation_acquire(struct aws_mqtt5_operation *operation);

AWS_MQTT_API struct aws_mqtt5_operation *aws_mqtt5_operation_release(struct aws_mqtt5_operation *operation);

/* Connect */

AWS_MQTT_API struct aws_mqtt5_operation_connect *aws_mqtt5_operation_connect_new(
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_connect_view *connect_options);

AWS_MQTT_API int aws_mqtt5_packet_connect_storage_init(
    struct aws_mqtt5_packet_connect_storage *connect_storage,
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_connect_view *connect_options);

AWS_MQTT_API void aws_mqtt5_packet_connect_storage_clean_up(struct aws_mqtt5_packet_connect_storage *connect_storage);

/* Disconnect */

AWS_MQTT_API struct aws_mqtt5_operation_disconnect *aws_mqtt5_operation_disconnect_new(
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_disconnect_view *disconnect_options);

AWS_MQTT_API struct aws_mqtt5_operation_disconnect *aws_mqtt5_operation_disconnect_acquire(
    struct aws_mqtt5_operation_disconnect *disconnect_op);

AWS_MQTT_API struct aws_mqtt5_operation_disconnect *aws_mqtt5_operation_disconnect_release(
    struct aws_mqtt5_operation_disconnect *disconnect_op);

AWS_MQTT_API int aws_mqtt5_packet_disconnect_storage_init(
    struct aws_mqtt5_packet_disconnect_storage *disconnect_storage,
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_disconnect_view *disconnect_options);

AWS_MQTT_API void aws_mqtt5_packet_disconnect_storage_clean_up(
    struct aws_mqtt5_packet_disconnect_storage *disconnect_storage);

/* Publish */

AWS_MQTT_API struct aws_mqtt5_operation_publish *aws_mqtt5_operation_publish_new(
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_publish_view *publish_options,
    struct aws_input_stream *payload,
    const struct aws_mqtt5_publish_completion_options *completion_options);

AWS_MQTT_API int aws_mqtt5_packet_publish_storage_init(
    struct aws_mqtt5_packet_publish_storage *publish_storage,
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_publish_view *publish_options);

AWS_MQTT_API void aws_mqtt5_packet_publish_storage_clean_up(struct aws_mqtt5_packet_publish_storage *publish_storage);

/* Subscribe */

AWS_MQTT_API struct aws_mqtt5_operation_subscribe *aws_mqtt5_operation_subscribe_new(
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_subscribe_view *subscribe_options,
    const struct aws_mqtt5_subscribe_completion_options *completion_options);

AWS_MQTT_API int aws_mqtt5_packet_subscribe_storage_init(
    struct aws_mqtt5_packet_subscribe_storage *subscribe_storage,
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_subscribe_view *subscribe_options);

AWS_MQTT_API void aws_mqtt5_packet_subscribe_storage_clean_up(
    struct aws_mqtt5_packet_subscribe_storage *subscribe_storage);

/* Unsubscribe */

AWS_MQTT_API struct aws_mqtt5_operation_unsubscribe *aws_mqtt5_operation_unsubscribe_new(
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_unsubscribe_view *unsubscribe_options,
    const struct aws_mqtt5_unsubscribe_completion_options *completion_options);

AWS_MQTT_API int aws_mqtt5_packet_unsubscribe_storage_init(
    struct aws_mqtt5_packet_unsubscribe_storage *unsubscribe_storage,
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_unsubscribe_view *unsubscribe_options);

AWS_MQTT_API void aws_mqtt5_packet_unsubscribe_storage_clean_up(
    struct aws_mqtt5_packet_unsubscribe_storage *unsubscribe_storage);

/* client */

AWS_MQTT_API
struct aws_mqtt5_client_options_storage *aws_mqtt5_client_options_storage_new(
    struct aws_allocator *allocator,
    const struct aws_mqtt5_client_options *options);

AWS_MQTT_API
void aws_mqtt5_client_options_storage_destroy(struct aws_mqtt5_client_options_storage *options_storage);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_MQTT5_OPERATION_H */
