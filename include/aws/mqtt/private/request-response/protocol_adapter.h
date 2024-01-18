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

    void (*completion_callback_fn)(bool, void *);
    void *user_data;
    uint32_t ack_timeout_seconds;
};

enum aws_protocol_adapter_subscription_event_type {
    AWS_PASET_SUBSCRIBE_SUCCESS,
    AWS_PASET_SUBSCRIBE_FAILURE,
    AWS_PASET_UNSUBSCRIBE_SUCCESS,
    AWS_PASET_UNSUBSCRIBE_FAILURE,
};

struct aws_protocol_adapter_subscription_event {
    struct aws_byte_cursor topic_filter;
    enum aws_protocol_adapter_subscription_event_type event_type;
};

struct aws_protocol_adapter_incoming_publish_event {
    struct aws_byte_cursor topic;
    struct aws_byte_cursor payload;
};

enum aws_protocol_adapter_connection_event_type {
    AWS_PACET_OFFLINE,
    AWS_PACET_ONLINE,
};

struct aws_protocol_adapter_connection_event {
    enum aws_protocol_adapter_connection_event_type event_type;
    bool rejoined_session;
};

typedef void(aws_protocol_adapter_subscription_event_fn)(struct aws_protocol_adapter_subscription_event *event, void *user_data);
typedef void(aws_protocol_adapter_incoming_publish_fn)(struct aws_protocol_adapter_incoming_publish_event *publish, void *user_data);
typedef void(aws_protocol_adapter_terminate_callback_fn)(void *user_data);
typedef void(aws_protocol_adapter_connection_event_fn)(struct aws_protocol_adapter_connection_event *event, void *user_data);

struct aws_mqtt_protocol_adapter_options {
    aws_protocol_adapter_subscription_event_fn *subscription_event_callback;
    aws_protocol_adapter_incoming_publish_fn *incoming_publish_callback;
    aws_protocol_adapter_terminate_callback_fn *terminate_callback;
    aws_protocol_adapter_connection_event_fn *connection_event_callback;

    void *user_data;
};

struct aws_mqtt_protocol_adapter_vtable {

    void (*aws_mqtt_protocol_adapter_delete_fn)(void *);

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

AWS_MQTT_API void aws_mqtt_protocol_adapter_delete(struct aws_mqtt_protocol_adapter *adapter);

AWS_MQTT_API int aws_mqtt_protocol_adapter_subscribe(struct aws_mqtt_protocol_adapter *adapter, struct aws_protocol_adapter_subscribe_options *options);

AWS_MQTT_API int aws_mqtt_protocol_adapter_unsubscribe(struct aws_mqtt_protocol_adapter *adapter, struct aws_protocol_adapter_unsubscribe_options *options);

AWS_MQTT_API int aws_mqtt_protocol_adapter_publish(struct aws_mqtt_protocol_adapter *adapter, struct aws_protocol_adapter_publish_options *options);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_PRIVATE_REQUEST_RESPONSE_PROTOCOL_ADAPTER_H */
