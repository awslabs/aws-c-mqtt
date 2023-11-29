#ifndef AWS_MQTT_PRIVATE_REQUEST_RESPONSE_PROTOCOL_ADAPTER_H
#define AWS_MQTT_PRIVATE_REQUEST_RESPONSE_PROTOCOL_ADAPTER_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/exports.h>

#include <aws/common/byte_buf.h>

struct aws_allocator;
struct aws_mqtt_client_connection;
struct aws_mqtt5_client;

struct aws_protocol_adapter_subscribe_options {
    struct aws_byte_cursor topic_filter;
    uint32_t ack_timeout_seconds;
};

struct aws_protocol_adapter_unsubscribe_options {
    struct aws_byte_cursor topic_filter;
    uint32_t ack_timeout_seconds;
};

struct aws_protocol_adapter_publish_options {
    struct aws_byte_cursor topic;
    struct aws_byte_cursor payload;

    void (*completion_callback_fn)(int, void *);
    void *user_data;
    uint32_t ack_timeout_seconds;
};

enum aws_protocol_adapter_subscription_status_update {
    AWS_PASS_ESTABLISHMENT_SUCCESS,
    AWS_PASS_ESTABLISHMENT_FAILURE,
    AWS_PASS_REMOVED
};

struct aws_protocol_adapter_subscription_status_update_event {
    struct aws_byte_cursor topic_filter;
    enum aws_protocol_adapter_subscription_status_update status_update;
};

struct aws_protocol_adapter_incoming_publish_event {
    struct aws_byte_cursor topic;
    struct aws_byte_cursor payload;
};

typedef void(aws_protocol_adapter_subscription_status_fn)(struct aws_protocol_adapter_subscription_status_update_event *update, void *user_data);
typedef void(aws_protocol_adapter_incoming_publish_fn)(struct aws_protocol_adapter_incoming_publish_event *publish, void *user_data);
typedef void(aws_protocol_adapter_terminate_callback_fn)(void *user_data);

struct aws_mqtt_protocol_adapter_options {
    aws_protocol_adapter_subscription_status_fn *subscription_status_update_callback;
    aws_protocol_adapter_incoming_publish_fn *incoming_publish_callback;
    aws_protocol_adapter_terminate_callback_fn *terminate_callback;

    void *user_data;
};

struct aws_mqtt_protocol_adapter_vtable {

    void (*aws_mqtt_protocol_adapter_release_fn)(void *);

    int (*aws_mqtt_protocol_adapter_subscribe_fn)(void *, struct aws_protocol_adapter_subscribe_options *);

    int (*aws_mqtt_protocol_adapter_unsubscribe_fn)(void *, struct aws_protocol_adapter_unsubscribe_options *);

    int (*aws_mqtt_protocol_adapter_publish_fn)(void *, struct aws_protocol_adapter_publish_options *);
};

struct aws_mqtt_protocol_adapter {
    const struct aws_mqtt_protocol_adapter_vtable *vtable;
    void *impl;
};

AWS_EXTERN_C_BEGIN

AWS_MQTT_API struct aws_mqtt_protocol_adapter *aws_mqtt_protocol_adapter_new_from_311(struct aws_allocator *allocator, struct aws_mqtt_protocol_adapter_options *options, struct aws_mqtt_client_connection *connection);

AWS_MQTT_API struct aws_mqtt_protocol_adapter *aws_mqtt_protocol_adapter_new_from_5(struct aws_allocator *allocator, struct aws_mqtt_protocol_adapter_options *options, struct aws_mqtt5_client *client);

AWS_MQTT_API void aws_mqtt_protocol_adapter_release(struct aws_mqtt_protocol_adapter *adapter);

AWS_MQTT_API int aws_mqtt_protocol_adapter_subscribe(struct aws_mqtt_protocol_adapter *adapter, struct aws_protocol_adapter_subscribe_options *options);

AWS_MQTT_API int aws_mqtt_protocol_adapter_unsubscribe(struct aws_mqtt_protocol_adapter *adapter, struct aws_protocol_adapter_unsubscribe_options *options);

AWS_MQTT_API int aws_mqtt_protocol_adapter_publish(struct aws_mqtt_protocol_adapter *adapter, struct aws_protocol_adapter_publish_options *options);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_PRIVATE_REQUEST_RESPONSE_PROTOCOL_ADAPTER_H */
