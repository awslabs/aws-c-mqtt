#ifndef AWS_MQTT_MQTT5_OPERATION_CONNECT_H
#define AWS_MQTT_MQTT5_OPERATION_CONNECT_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

#include <aws/mqtt/v5/mqtt5_types.h>

struct aws_allocator;
struct aws_byte_cursor;
struct aws_mqtt5_operation_connect;
struct aws_mqtt5_operation_publish;
struct aws_mqtt5_user_property;

AWS_EXTERN_C_BEGIN

AWS_MQTT_API struct aws_mqtt5_operation_connect *aws_mqtt5_operation_connect_new(struct aws_allocator *allocator);

AWS_MQTT_API struct aws_mqtt5_operation_connect *aws_mqtt5_operation_connect_acquire(
    struct aws_mqtt5_operation_connect *connect_operation);

AWS_MQTT_API struct aws_mqtt5_operation_connect *aws_mqtt5_operation_connect_release(
    struct aws_mqtt5_operation_connect *connect_operation);

AWS_MQTT_API void aws_mqtt5_operation_connect_set_keep_alive_interval(
    struct aws_mqtt5_operation_connect *connect_operation,
    uint32_t keep_alive_interval_seconds);

AWS_MQTT_API int aws_mqtt5_operation_connect_set_client_id(
    struct aws_mqtt5_operation_connect *connect_operation,
    struct aws_byte_cursor client_id);

AWS_MQTT_API int aws_mqtt5_operation_connect_set_username(
    struct aws_mqtt5_operation_connect *connect_operation,
    struct aws_byte_cursor username);

AWS_MQTT_API int aws_mqtt5_operation_connect_set_password(
    struct aws_mqtt5_operation_connect *connect_operation,
    struct aws_byte_cursor password);

AWS_MQTT_API void aws_mqtt5_operation_connect_set_session_expiry_interval(
    struct aws_mqtt5_operation_connect *connect_operation,
    uint32_t session_expiry_interval_seconds);

AWS_MQTT_API void aws_mqtt5_operation_connect_set_session_behavior(
    struct aws_mqtt5_operation_connect *connect_operation,
    enum aws_mqtt5_client_session_behavior_type session_behavior);

AWS_MQTT_API void aws_mqtt5_operation_connect_set_request_response_information(
    struct aws_mqtt5_operation_connect *connect_operation,
    bool request_response_information);

AWS_MQTT_API void aws_mqtt5_operation_connect_set_request_problem_information(
    struct aws_mqtt5_operation_connect *connect_operation,
    bool request_problem_information);

AWS_MQTT_API void aws_mqtt5_operation_connect_set_receive_maximum(
    struct aws_mqtt5_operation_connect *connect_operation,
    uint16_t receive_maximum);

AWS_MQTT_API void aws_mqtt5_operation_connect_set_topic_alias_maximum(
    struct aws_mqtt5_operation_connect *connect_operation,
    uint16_t topic_alias_maximum);

AWS_MQTT_API void aws_mqtt5_operation_connect_set_maximum_packet_size(
    struct aws_mqtt5_operation_connect *connect_operation,
    uint32_t maximum_packet_size_bytes);

AWS_MQTT_API void aws_mqtt5_operation_connect_set_will(
    struct aws_mqtt5_operation_connect *connect_operation,
    struct aws_mqtt5_operation_publish *will);

AWS_MQTT_API void aws_mqtt5_operation_connect_set_will_delay_interval(
    struct aws_mqtt5_operation_connect *connect_operation,
    uint32_t will_delay_interval_seconds);

AWS_MQTT_API int aws_mqtt5_operation_connect_add_user_property(
    struct aws_mqtt5_operation_connect *connect_operation,
    struct aws_mqtt5_user_property *property);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_MQTT5_OPERATION_CONNECT_H */
