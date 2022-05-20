#ifndef AWS_MQTT_MQTT5_OPERATION_H
#define AWS_MQTT_MQTT5_OPERATION_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

#include <aws/common/linked_list.h>
#include <aws/common/logging.h>
#include <aws/common/ref_count.h>
#include <aws/http/proxy.h>
#include <aws/io/retry_strategy.h>
#include <aws/io/socket.h>
#include <aws/io/tls_channel_handler.h>
#include <aws/mqtt/v5/mqtt5_client.h>
#include <aws/mqtt/v5/mqtt5_types.h>

struct aws_client_bootstrap;
struct aws_input_stream;
struct aws_mqtt5_client;
struct aws_mqtt5_client_options;
struct aws_mqtt5_operation;
struct aws_string;

struct aws_mqtt5_user_property_set {
    struct aws_array_list properties;
};

struct aws_mqtt5_operation_vtable {
    void (*aws_mqtt5_operation_completion_fn)(
        struct aws_mqtt5_operation *operation,
        int error_code,
        const void *completion_view);

    void (
        *aws_mqtt5_operation_set_packet_id_fn)(struct aws_mqtt5_operation *operation, aws_mqtt5_packet_id_t packet_id);

    aws_mqtt5_packet_id_t *(*aws_mqtt5_operation_get_packet_id_address_fn)(const struct aws_mqtt5_operation *operation);

    int (*aws_mqtt5_operation_validate_vs_connection_settings_fn)(
        const void *operation_packet_view,
        const struct aws_mqtt5_client *client);
};

enum aws_mqtt5_operation_statistic_state_flags {
    AWS_MQTT5_OSS_NONE = 0,
    AWS_MQTT5_OSS_INCOMPLETE = 1 << 0,
    AWS_MQTT5_OSS_UNACKED = 1 << 1,
};

/**
 * This is the base structure for all mqtt5 operations.  It includes the type, a ref count, timeout timepoint,
 * and list management.
 */
struct aws_mqtt5_operation {
    const struct aws_mqtt5_operation_vtable *vtable;
    struct aws_ref_count ref_count;
    uint64_t timeout_timepoint_ns;
    struct aws_linked_list_node node;

    enum aws_mqtt5_packet_type packet_type;
    const void *packet_view;

    enum aws_mqtt5_operation_statistic_state_flags statistic_state_flags;
    size_t packet_size;

    void *impl;
};

struct aws_mqtt5_packet_connect_storage {
    struct aws_allocator *allocator;

    struct aws_mqtt5_packet_connect_view storage_view;

    struct aws_byte_cursor username;

    struct aws_byte_cursor password;

    uint32_t session_expiry_interval_seconds;

    uint8_t request_response_information;

    uint8_t request_problem_information;

    uint16_t receive_maximum;

    uint16_t topic_alias_maximum;

    uint32_t maximum_packet_size_bytes;

    struct aws_mqtt5_packet_publish_storage *will;

    uint32_t will_delay_interval_seconds;

    struct aws_mqtt5_user_property_set user_properties;

    struct aws_byte_cursor authentication_method;

    struct aws_byte_cursor authentication_data;

    struct aws_byte_buf storage;
};

struct aws_mqtt5_operation_connect {
    struct aws_mqtt5_operation base;
    struct aws_allocator *allocator;

    struct aws_mqtt5_packet_connect_storage options_storage;
};

struct aws_mqtt5_packet_connack_storage {
    struct aws_allocator *allocator;

    struct aws_mqtt5_packet_connack_view storage_view;

    uint32_t session_expiry_interval;

    uint16_t receive_maximum;

    enum aws_mqtt5_qos maximum_qos;

    bool retain_available;

    uint32_t maximum_packet_size;

    struct aws_byte_cursor assigned_client_identifier;

    uint16_t topic_alias_maximum;

    struct aws_byte_cursor reason_string;

    bool wildcard_subscriptions_available;

    bool subscription_identifiers_available;

    bool shared_subscriptions_available;

    uint16_t server_keep_alive;

    struct aws_byte_cursor response_information;

    struct aws_byte_cursor server_reference;

    struct aws_byte_cursor authentication_method;

    struct aws_byte_cursor authentication_data;

    struct aws_mqtt5_user_property_set user_properties;

    struct aws_byte_buf storage;
};

struct aws_mqtt5_packet_suback_storage {

    struct aws_allocator *allocator;

    struct aws_mqtt5_packet_suback_view storage_view;

    struct aws_byte_cursor reason_string;

    struct aws_mqtt5_user_property_set user_properties;

    struct aws_array_list reason_codes;

    struct aws_byte_buf storage;
};

struct aws_mqtt5_packet_unsuback_storage {

    struct aws_allocator *allocator;

    struct aws_mqtt5_packet_unsuback_view storage_view;

    struct aws_byte_cursor reason_string;

    struct aws_mqtt5_user_property_set user_properties;

    struct aws_array_list reason_codes;

    struct aws_byte_buf storage;
};

struct aws_mqtt5_packet_publish_storage {
    struct aws_mqtt5_packet_publish_view storage_view;

    enum aws_mqtt5_payload_format_indicator payload_format;

    uint32_t message_expiry_interval_seconds;

    uint16_t topic_alias;

    struct aws_byte_cursor response_topic;

    struct aws_byte_cursor correlation_data;

    struct aws_byte_cursor content_type;

    struct aws_mqtt5_user_property_set user_properties;
    struct aws_array_list subscription_identifiers;

    struct aws_byte_buf storage;
};

struct aws_mqtt5_operation_publish {
    struct aws_mqtt5_operation base;
    struct aws_allocator *allocator;

    struct aws_mqtt5_packet_publish_storage options_storage;

    struct aws_mqtt5_publish_completion_options completion_options;
};

struct aws_mqtt5_packet_puback_storage {
    struct aws_mqtt5_packet_puback_view storage_view;

    struct aws_byte_cursor reason_string;

    struct aws_mqtt5_user_property_set user_properties;

    struct aws_byte_buf storage;
};

struct aws_mqtt5_operation_puback {
    struct aws_mqtt5_operation base;
    struct aws_allocator *allocator;

    struct aws_mqtt5_packet_puback_storage options_storage;
};

struct aws_mqtt5_packet_disconnect_storage {
    struct aws_mqtt5_packet_disconnect_view storage_view;

    uint32_t session_expiry_interval_seconds;

    struct aws_byte_cursor reason_string;

    struct aws_mqtt5_user_property_set user_properties;

    struct aws_byte_cursor server_reference;

    struct aws_byte_buf storage;
};

struct aws_mqtt5_operation_disconnect {
    struct aws_mqtt5_operation base;
    struct aws_allocator *allocator;

    struct aws_mqtt5_packet_disconnect_storage options_storage;

    struct aws_mqtt5_disconnect_completion_options external_completion_options;
    struct aws_mqtt5_disconnect_completion_options internal_completion_options;
};

struct aws_mqtt5_packet_subscribe_storage {
    struct aws_mqtt5_packet_subscribe_view storage_view;

    uint32_t subscription_identifier;

    struct aws_array_list subscriptions;

    struct aws_mqtt5_user_property_set user_properties;

    struct aws_byte_buf storage;
};

struct aws_mqtt5_operation_subscribe {
    struct aws_mqtt5_operation base;
    struct aws_allocator *allocator;

    struct aws_mqtt5_packet_subscribe_storage options_storage;

    struct aws_mqtt5_subscribe_completion_options completion_options;
};

struct aws_mqtt5_packet_unsubscribe_storage {
    struct aws_mqtt5_packet_unsubscribe_view storage_view;

    struct aws_array_list topic_filters;

    struct aws_mqtt5_user_property_set user_properties;

    struct aws_byte_buf storage;
};

struct aws_mqtt5_operation_unsubscribe {
    struct aws_mqtt5_operation base;
    struct aws_allocator *allocator;

    struct aws_mqtt5_packet_unsubscribe_storage options_storage;

    struct aws_mqtt5_unsubscribe_completion_options completion_options;
};

struct aws_mqtt5_operation_pingreq {
    struct aws_mqtt5_operation base;
    struct aws_allocator *allocator;
};

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

    aws_mqtt5_publish_received_fn *publish_received_handler;
    void *publish_received_handler_user_data;

    enum aws_mqtt5_client_session_behavior_type session_behavior;
    enum aws_mqtt5_client_outbound_topic_alias_behavior_type outbound_topic_aliasing_behavior;
    enum aws_mqtt5_extended_validation_and_flow_control_options extended_validation_and_flow_control_options;
    enum aws_mqtt5_client_operation_queue_behavior_type offline_queue_behavior;

    enum aws_exponential_backoff_jitter_mode retry_jitter_mode;
    uint64_t min_reconnect_delay_ms;
    uint64_t max_reconnect_delay_ms;
    uint64_t min_connected_time_to_reset_reconnect_delay_ms;

    uint64_t operation_timeout_seconds;

    uint32_t ping_timeout_ms;
    uint32_t connack_timeout_ms;

    struct aws_mqtt5_packet_connect_storage connect;

    aws_mqtt5_client_connection_event_callback_fn *lifecycle_event_handler;
    void *lifecycle_event_handler_user_data;
};

AWS_EXTERN_C_BEGIN

/* User properties */

AWS_MQTT_API int aws_mqtt5_user_property_set_init_with_storage(
    struct aws_mqtt5_user_property_set *property_set,
    struct aws_allocator *allocator,
    struct aws_byte_buf *storage_buffer,
    size_t property_count,
    const struct aws_mqtt5_user_property *properties);

AWS_MQTT_API void aws_mqtt5_user_property_set_clean_up(struct aws_mqtt5_user_property_set *property_set);

AWS_MQTT_API size_t aws_mqtt5_user_property_set_size(const struct aws_mqtt5_user_property_set *property_set);

/* Operation base */

AWS_MQTT_API struct aws_mqtt5_operation *aws_mqtt5_operation_acquire(struct aws_mqtt5_operation *operation);

AWS_MQTT_API struct aws_mqtt5_operation *aws_mqtt5_operation_release(struct aws_mqtt5_operation *operation);

AWS_MQTT_API void aws_mqtt5_operation_complete(
    struct aws_mqtt5_operation *operation,
    int error_code,
    const void *associated_view);

AWS_MQTT_API void aws_mqtt5_operation_set_packet_id(
    struct aws_mqtt5_operation *operation,
    aws_mqtt5_packet_id_t packet_id);

AWS_MQTT_API aws_mqtt5_packet_id_t aws_mqtt5_operation_get_packet_id(const struct aws_mqtt5_operation *operation);

AWS_MQTT_API aws_mqtt5_packet_id_t *aws_mqtt5_operation_get_packet_id_address(
    const struct aws_mqtt5_operation *operation);

AWS_MQTT_API int aws_mqtt5_operation_validate_vs_connection_settings(
    const struct aws_mqtt5_operation *operation,
    const struct aws_mqtt5_client *client);

/* Connect */

AWS_MQTT_API struct aws_mqtt5_operation_connect *aws_mqtt5_operation_connect_new(
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_connect_view *connect_options);

AWS_MQTT_API int aws_mqtt5_packet_connect_storage_init(
    struct aws_mqtt5_packet_connect_storage *connect_storage,
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_connect_view *connect_options);

AWS_MQTT_API int aws_mqtt5_packet_connect_storage_init_from_external_storage(
    struct aws_mqtt5_packet_connect_storage *connect_storage,
    struct aws_allocator *allocator);

AWS_MQTT_API void aws_mqtt5_packet_connect_storage_clean_up(struct aws_mqtt5_packet_connect_storage *connect_storage);

AWS_MQTT_API int aws_mqtt5_packet_connect_view_validate(const struct aws_mqtt5_packet_connect_view *connect_view);

AWS_MQTT_API void aws_mqtt5_packet_connect_view_log(
    const struct aws_mqtt5_packet_connect_view *connect_view,
    enum aws_log_level level);

/* Connack */

AWS_MQTT_API int aws_mqtt5_packet_connack_storage_init(
    struct aws_mqtt5_packet_connack_storage *connack_storage,
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_connack_view *connack_options);

AWS_MQTT_API int aws_mqtt5_packet_connack_storage_init_from_external_storage(
    struct aws_mqtt5_packet_connack_storage *connack_storage,
    struct aws_allocator *allocator);

AWS_MQTT_API void aws_mqtt5_packet_connack_storage_clean_up(struct aws_mqtt5_packet_connack_storage *connack_storage);

AWS_MQTT_API void aws_mqtt5_packet_connack_view_log(
    const struct aws_mqtt5_packet_connack_view *connack_view,
    enum aws_log_level level);

/* Disconnect */

AWS_MQTT_API struct aws_mqtt5_operation_disconnect *aws_mqtt5_operation_disconnect_new(
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_disconnect_view *disconnect_options,
    const struct aws_mqtt5_disconnect_completion_options *external_completion_options,
    const struct aws_mqtt5_disconnect_completion_options *internal_completion_options);

AWS_MQTT_API struct aws_mqtt5_operation_disconnect *aws_mqtt5_operation_disconnect_acquire(
    struct aws_mqtt5_operation_disconnect *disconnect_op);

AWS_MQTT_API struct aws_mqtt5_operation_disconnect *aws_mqtt5_operation_disconnect_release(
    struct aws_mqtt5_operation_disconnect *disconnect_op);

AWS_MQTT_API int aws_mqtt5_packet_disconnect_storage_init(
    struct aws_mqtt5_packet_disconnect_storage *disconnect_storage,
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_disconnect_view *disconnect_options);

AWS_MQTT_API int aws_mqtt5_packet_disconnect_storage_init_from_external_storage(
    struct aws_mqtt5_packet_disconnect_storage *disconnect_storage,
    struct aws_allocator *allocator);

AWS_MQTT_API void aws_mqtt5_packet_disconnect_storage_clean_up(
    struct aws_mqtt5_packet_disconnect_storage *disconnect_storage);

AWS_MQTT_API int aws_mqtt5_packet_disconnect_view_validate(
    const struct aws_mqtt5_packet_disconnect_view *disconnect_view);

AWS_MQTT_API void aws_mqtt5_packet_disconnect_view_log(
    const struct aws_mqtt5_packet_disconnect_view *disconnect_view,
    enum aws_log_level level);

/* Publish */

AWS_MQTT_API struct aws_mqtt5_operation_publish *aws_mqtt5_operation_publish_new(
    struct aws_allocator *allocator,
    const struct aws_mqtt5_client *client,
    const struct aws_mqtt5_packet_publish_view *publish_options,
    const struct aws_mqtt5_publish_completion_options *completion_options);

AWS_MQTT_API int aws_mqtt5_packet_publish_storage_init(
    struct aws_mqtt5_packet_publish_storage *publish_storage,
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_publish_view *publish_options);

AWS_MQTT_API int aws_mqtt5_packet_publish_storage_init_from_external_storage(
    struct aws_mqtt5_packet_publish_storage *publish_storage,
    struct aws_allocator *allocator);

AWS_MQTT_API void aws_mqtt5_packet_publish_storage_clean_up(struct aws_mqtt5_packet_publish_storage *publish_storage);

AWS_MQTT_API int aws_mqtt5_packet_publish_view_validate(const struct aws_mqtt5_packet_publish_view *publish_view);

AWS_MQTT_API int aws_mqtt5_packet_publish_view_validate_vs_iot_core(
    const struct aws_mqtt5_packet_publish_view *publish_view);

AWS_MQTT_API void aws_mqtt5_packet_publish_view_log(
    const struct aws_mqtt5_packet_publish_view *publish_view,
    enum aws_log_level level);

/* Puback */

AWS_MQTT_API struct aws_mqtt5_operation_puback *aws_mqtt5_operation_puback_new(
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_puback_view *puback_options);

AWS_MQTT_API int aws_mqtt5_packet_puback_storage_init(
    struct aws_mqtt5_packet_puback_storage *puback_storage,
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_puback_view *puback_view);

AWS_MQTT_API int aws_mqtt5_packet_puback_storage_init_from_external_storage(
    struct aws_mqtt5_packet_puback_storage *puback_storage,
    struct aws_allocator *allocator);

AWS_MQTT_API void aws_mqtt5_packet_puback_storage_clean_up(struct aws_mqtt5_packet_puback_storage *puback_storage);

AWS_MQTT_API void aws_mqtt5_packet_puback_view_log(
    const struct aws_mqtt5_packet_puback_view *puback_view,
    enum aws_log_level level);

/* Subscribe */

AWS_MQTT_API struct aws_mqtt5_operation_subscribe *aws_mqtt5_operation_subscribe_new(
    struct aws_allocator *allocator,
    const struct aws_mqtt5_client *client,
    const struct aws_mqtt5_packet_subscribe_view *subscribe_options,
    const struct aws_mqtt5_subscribe_completion_options *completion_options);

AWS_MQTT_API int aws_mqtt5_packet_subscribe_storage_init(
    struct aws_mqtt5_packet_subscribe_storage *subscribe_storage,
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_subscribe_view *subscribe_options);

AWS_MQTT_API int aws_mqtt5_packet_subscribe_storage_init_from_external_storage(
    struct aws_mqtt5_packet_subscribe_storage *subscribe_storage,
    struct aws_allocator *allocator);

AWS_MQTT_API void aws_mqtt5_packet_subscribe_storage_clean_up(
    struct aws_mqtt5_packet_subscribe_storage *subscribe_storage);

AWS_MQTT_API int aws_mqtt5_packet_subscribe_view_validate(const struct aws_mqtt5_packet_subscribe_view *subscribe_view);

AWS_MQTT_API int aws_mqtt5_packet_subscribe_view_validate_vs_iot_core(
    const struct aws_mqtt5_packet_subscribe_view *subscribe_view);

AWS_MQTT_API void aws_mqtt5_packet_subscribe_view_log(
    const struct aws_mqtt5_packet_subscribe_view *subscribe_view,
    enum aws_log_level level);

/* Suback */

AWS_MQTT_API int aws_mqtt5_packet_suback_storage_init(
    struct aws_mqtt5_packet_suback_storage *suback_storage,
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_suback_view *suback_view);

AWS_MQTT_API int aws_mqtt5_packet_suback_storage_init_from_external_storage(
    struct aws_mqtt5_packet_suback_storage *suback_storage,
    struct aws_allocator *allocator);

AWS_MQTT_API void aws_mqtt5_packet_suback_storage_clean_up(struct aws_mqtt5_packet_suback_storage *suback_storage);

AWS_MQTT_API void aws_mqtt5_packet_suback_view_log(
    const struct aws_mqtt5_packet_suback_view *suback_view,
    enum aws_log_level level);

/* Unsubscribe */

AWS_MQTT_API struct aws_mqtt5_operation_unsubscribe *aws_mqtt5_operation_unsubscribe_new(
    struct aws_allocator *allocator,
    const struct aws_mqtt5_client *client,
    const struct aws_mqtt5_packet_unsubscribe_view *unsubscribe_options,
    const struct aws_mqtt5_unsubscribe_completion_options *completion_options);

AWS_MQTT_API int aws_mqtt5_packet_unsubscribe_storage_init(
    struct aws_mqtt5_packet_unsubscribe_storage *unsubscribe_storage,
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_unsubscribe_view *unsubscribe_options);

AWS_MQTT_API int aws_mqtt5_packet_unsubscribe_storage_init_from_external_storage(
    struct aws_mqtt5_packet_unsubscribe_storage *unsubscribe_storage,
    struct aws_allocator *allocator);

AWS_MQTT_API void aws_mqtt5_packet_unsubscribe_storage_clean_up(
    struct aws_mqtt5_packet_unsubscribe_storage *unsubscribe_storage);

AWS_MQTT_API int aws_mqtt5_packet_unsubscribe_view_validate(
    const struct aws_mqtt5_packet_unsubscribe_view *unsubscribe_view);

AWS_MQTT_API int aws_mqtt5_packet_unsubscribe_view_validate_vs_iot_core(
    const struct aws_mqtt5_packet_unsubscribe_view *unsubscribe_view);

AWS_MQTT_API void aws_mqtt5_packet_unsubscribe_view_log(
    const struct aws_mqtt5_packet_unsubscribe_view *unsubscribe_view,
    enum aws_log_level level);

/* Unsuback */

AWS_MQTT_API int aws_mqtt5_packet_unsuback_storage_init(
    struct aws_mqtt5_packet_unsuback_storage *unsuback_storage,
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_unsuback_view *unsuback_view);

AWS_MQTT_API int aws_mqtt5_packet_unsuback_storage_init_from_external_storage(
    struct aws_mqtt5_packet_unsuback_storage *unsuback_storage,
    struct aws_allocator *allocator);

AWS_MQTT_API void aws_mqtt5_packet_unsuback_storage_clean_up(
    struct aws_mqtt5_packet_unsuback_storage *unsuback_storage);

AWS_MQTT_API void aws_mqtt5_packet_unsuback_view_log(
    const struct aws_mqtt5_packet_unsuback_view *unsuback_view,
    enum aws_log_level level);

/* PINGREQ */

AWS_MQTT_API struct aws_mqtt5_operation_pingreq *aws_mqtt5_operation_pingreq_new(struct aws_allocator *allocator);

/* client */

AWS_MQTT_API
struct aws_mqtt5_client_options_storage *aws_mqtt5_client_options_storage_new(
    struct aws_allocator *allocator,
    const struct aws_mqtt5_client_options *options);

AWS_MQTT_API
void aws_mqtt5_client_options_storage_destroy(struct aws_mqtt5_client_options_storage *options_storage);

AWS_MQTT_API int aws_mqtt5_client_options_validate(const struct aws_mqtt5_client_options *client_options);

AWS_MQTT_API void aws_mqtt5_client_options_storage_log(
    const struct aws_mqtt5_client_options_storage *options_storage,
    enum aws_log_level level);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_MQTT5_OPERATION_H */
