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

/**
 * This is the base structure for all mqtt5 operations.  It includes the type, a ref count, timeout counter,
 * and list management.
 */
struct aws_mqtt5_operation {
    const struct aws_mqtt5_operation_vtable *vtable;
    struct aws_ref_count ref_count;
    uint64_t timeout_counter;
    struct aws_linked_list_node timeout_node;
    struct aws_linked_list_node node;
    enum aws_mqtt5_packet_type packet_type;
    const void *packet_view;

    void *impl;
};

struct aws_mqtt5_packet_connect_storage {
    struct aws_allocator *allocator;

    struct aws_mqtt5_packet_connect_view storage_view;

    uint16_t keep_alive_interval_seconds;

    struct aws_byte_cursor client_id;

    struct aws_byte_cursor username;
    struct aws_byte_cursor *username_ptr;

    struct aws_byte_cursor password;
    struct aws_byte_cursor *password_ptr;

    bool clean_start;

    uint32_t session_expiry_interval_seconds;
    uint32_t *session_expiry_interval_seconds_ptr;

    uint8_t request_response_information;
    uint8_t *request_response_information_ptr;

    uint8_t request_problem_information;
    uint8_t *request_problem_information_ptr;

    uint16_t receive_maximum;
    uint16_t *receive_maximum_ptr;

    uint16_t topic_alias_maximum;
    uint16_t *topic_alias_maximum_ptr;

    uint32_t maximum_packet_size_bytes;
    uint32_t *maximum_packet_size_bytes_ptr;

    struct aws_mqtt5_packet_publish_storage *will;

    uint32_t will_delay_interval_seconds;
    uint32_t *will_delay_interval_seconds_ptr;

    struct aws_mqtt5_user_property_set user_properties;

    struct aws_byte_cursor authentication_method;
    struct aws_byte_cursor *authentication_method_ptr;

    struct aws_byte_cursor authentication_data;
    struct aws_byte_cursor *authentication_data_ptr;

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

    bool session_present;
    enum aws_mqtt5_connect_reason_code reason_code;

    uint32_t session_expiry_interval;
    uint32_t *session_expiry_interval_ptr;

    uint16_t receive_maximum;
    uint16_t *receive_maximum_ptr;

    enum aws_mqtt5_qos maximum_qos;
    enum aws_mqtt5_qos *maximum_qos_ptr;

    bool retain_available;
    bool *retain_available_ptr;

    uint32_t maximum_packet_size;
    uint32_t *maximum_packet_size_ptr;

    struct aws_byte_cursor assigned_client_identifier;
    struct aws_byte_cursor *assigned_client_identifier_ptr;

    uint16_t topic_alias_maximum;
    uint16_t *topic_alias_maximum_ptr;

    struct aws_byte_cursor reason_string;
    struct aws_byte_cursor *reason_string_ptr;

    bool wildcard_subscriptions_available;
    bool *wildcard_subscriptions_available_ptr;

    bool subscription_identifiers_available;
    bool *subscription_identifiers_available_ptr;

    bool shared_subscriptions_available;
    bool *shared_subscriptions_available_ptr;

    uint16_t server_keep_alive;
    uint16_t *server_keep_alive_ptr;

    struct aws_byte_cursor response_information;
    struct aws_byte_cursor *response_information_ptr;

    struct aws_byte_cursor server_reference;
    struct aws_byte_cursor *server_reference_ptr;

    struct aws_byte_cursor authentication_method;
    struct aws_byte_cursor *authentication_method_ptr;

    struct aws_byte_cursor authentication_data;
    struct aws_byte_cursor *authentication_data_ptr;

    struct aws_mqtt5_user_property_set user_properties;

    struct aws_byte_buf storage;
};

struct aws_mqtt5_packet_suback_storage {

    struct aws_allocator *allocator;

    struct aws_mqtt5_packet_suback_view storage_view;

    aws_mqtt5_packet_id_t packet_id;

    struct aws_byte_cursor reason_string;
    struct aws_byte_cursor *reason_string_ptr;

    struct aws_mqtt5_user_property_set user_properties;

    struct aws_array_list reason_codes;

    struct aws_byte_buf storage;
};

struct aws_mqtt5_packet_unsuback_storage {

    struct aws_allocator *allocator;

    struct aws_mqtt5_packet_unsuback_view storage_view;

    aws_mqtt5_packet_id_t packet_id;

    struct aws_byte_cursor reason_string;
    struct aws_byte_cursor *reason_string_ptr;

    struct aws_mqtt5_user_property_set user_properties;

    struct aws_array_list reason_codes;

    struct aws_byte_buf storage;
};

struct aws_mqtt5_packet_publish_storage {
    struct aws_mqtt5_packet_publish_view storage_view;

    /* This field is always empty on received messages */
    struct aws_byte_cursor payload;

    /* packet_id is only set for QoS 1 and QoS 2 */
    aws_mqtt5_packet_id_t packet_id;

    bool dup;
    enum aws_mqtt5_qos qos;
    bool retain;
    bool duplicate;
    struct aws_byte_cursor topic;

    enum aws_mqtt5_payload_format_indicator payload_format;
    enum aws_mqtt5_payload_format_indicator *payload_format_ptr;

    uint32_t message_expiry_interval_seconds;
    uint32_t *message_expiry_interval_seconds_ptr;

    uint16_t topic_alias;
    uint16_t *topic_alias_ptr;

    struct aws_byte_cursor response_topic;
    struct aws_byte_cursor *response_topic_ptr;

    struct aws_byte_cursor correlation_data;
    struct aws_byte_cursor *correlation_data_ptr;

    struct aws_byte_cursor content_type;
    struct aws_byte_cursor *content_type_ptr;

    struct aws_mqtt5_user_property_set user_properties;
    struct aws_array_list subscription_identifiers;

    struct aws_byte_buf storage;
};

struct aws_mqtt5_packet_puback_storage {
    struct aws_mqtt5_packet_puback_view storage_view;

    aws_mqtt5_packet_id_t packet_id;

    enum aws_mqtt5_puback_reason_code reason_code;

    struct aws_byte_cursor reason_string;
    struct aws_byte_cursor *reason_string_ptr;

    struct aws_mqtt5_user_property_set user_properties;

    struct aws_byte_buf storage;
};

struct aws_mqtt5_operation_publish {
    struct aws_mqtt5_operation base;
    struct aws_allocator *allocator;

    struct aws_mqtt5_packet_publish_storage options_storage;

    struct aws_mqtt5_publish_completion_options completion_options;
};

struct aws_mqtt5_operation_puback {
    struct aws_mqtt5_operation base;
    struct aws_allocator *allocator;
    struct aws_mqtt5_packet_puback_storage options_storage;
};

struct aws_mqtt5_packet_disconnect_storage {
    struct aws_mqtt5_packet_disconnect_view storage_view;

    enum aws_mqtt5_disconnect_reason_code reason_code;

    uint32_t session_expiry_interval_seconds;
    uint32_t *session_expiry_interval_seconds_ptr;

    struct aws_byte_cursor reason_string;
    struct aws_byte_cursor *reason_string_ptr;

    struct aws_mqtt5_user_property_set user_properties;

    struct aws_byte_cursor server_reference;
    struct aws_byte_cursor *server_reference_ptr;

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
    uint32_t *subscription_identifier_ptr;

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

    struct aws_array_list topics;

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

    aws_mqtt5_publish_received_fn *publish_received;

    enum aws_mqtt5_client_session_behavior_type session_behavior;
    enum aws_mqtt5_client_outbound_topic_alias_behavior_type outbound_topic_aliasing_behavior;
    enum aws_mqtt5_extended_validation_and_flow_control_options extended_validation_and_flow_control_options;

    enum aws_exponential_backoff_jitter_mode retry_jitter_mode;
    uint64_t min_reconnect_delay_ms;
    uint64_t max_reconnect_delay_ms;
    uint64_t min_connected_time_to_reset_reconnect_delay_ms;

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

AWS_MQTT_API int aws_mqtt5_user_property_set_get_property(
    const struct aws_mqtt5_user_property_set *property_set,
    size_t index,
    struct aws_mqtt5_user_property *property_out);

AWS_MQTT_API int aws_mqtt5_user_property_set_add_stored_property(
    struct aws_mqtt5_user_property_set *property_set,
    struct aws_mqtt5_user_property *property);

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

AWS_MQTT_API void aws_mqtt5_packet_connect_view_init_from_storage(
    struct aws_mqtt5_packet_connect_view *connect_view,
    const struct aws_mqtt5_packet_connect_storage *connect_storage);

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

AWS_MQTT_API void aws_mqtt5_packet_connack_view_init_from_storage(
    struct aws_mqtt5_packet_connack_view *connack_view,
    const struct aws_mqtt5_packet_connack_storage *connack_storage);

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

AWS_MQTT_API void aws_mqtt5_packet_disconnect_view_init_from_storage(
    struct aws_mqtt5_packet_disconnect_view *disconnect_view,
    const struct aws_mqtt5_packet_disconnect_storage *disconnect_storage);

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

AWS_MQTT_API void aws_mqtt5_packet_publish_view_init_from_storage(
    struct aws_mqtt5_packet_publish_view *publish_view,
    const struct aws_mqtt5_packet_publish_storage *publish_storage);

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

AWS_MQTT_API void aws_mqtt5_packet_puback_view_init_from_storage(
    struct aws_mqtt5_packet_puback_view *puback_view,
    const struct aws_mqtt5_packet_puback_storage *puback_storage);

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

AWS_MQTT_API void aws_mqtt5_packet_subscribe_view_init_from_storage(
    struct aws_mqtt5_packet_subscribe_view *subscribe_view,
    const struct aws_mqtt5_packet_subscribe_storage *subscribe_storage);

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

AWS_MQTT_API void aws_mqtt5_packet_suback_view_init_from_storage(
    struct aws_mqtt5_packet_suback_view *suback_view,
    const struct aws_mqtt5_packet_suback_storage *suback_storage);

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

AWS_MQTT_API void aws_mqtt5_packet_unsubscribe_view_init_from_storage(
    struct aws_mqtt5_packet_unsubscribe_view *unsubscribe_view,
    const struct aws_mqtt5_packet_unsubscribe_storage *unsubscribe_storage);

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

AWS_MQTT_API void aws_mqtt5_packet_unsuback_view_init_from_storage(
    struct aws_mqtt5_packet_unsuback_view *unsuback_view,
    const struct aws_mqtt5_packet_unsuback_storage *unsuback_storage);

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
