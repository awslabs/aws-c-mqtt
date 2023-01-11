/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#ifndef AWS_MQTT_AWS_IOT_SERVICE_CLIENT_H
#define AWS_MQTT_AWS_IOT_SERVICE_CLIENT_H

#include <aws/mqtt/mqtt.h>

#include <aws/common/ref_count.h>
#include <aws/mqtt/v5/mqtt5_types.h>

struct aws_iot_service_client;
struct aws_mqtt5_client;

struct aws_iot_service_client_config {
    struct aws_mqtt5_client *mqtt5_client;

    size_t max_event_subscriptions;
    size_t max_request_concurrency;

    /* Missing:
     *
     * Retry controls
     * Timeout controls
     */
};

struct aws_iot_service_client_event_subscription_complete_data {
    struct aws_iot_service_client *client;

    int error_code;

    enum aws_mqtt5_qos granted_qos;

    void *user_data;
};

struct aws_iot_service_client_event_subscription_lost_data {
    struct aws_iot_service_client *client;

    int error_code;

    void *user_data;
};

struct aws_iot_service_client_event_received_data {
    struct aws_iot_service_client *client;

    struct aws_byte_cursor event_topic;
    struct aws_byte_cursor event_payload;

    void *user_data;
};

typedef void(aws_iot_service_client_event_subscription_complete_fn)(
    const struct aws_iot_service_client_event_subscription_complete_data *subscription_complete_data);

typedef void(aws_iot_service_client_event_subscription_lost_fn)(
    const struct aws_iot_service_client_event_subscription_lost_data *subscription_lost_data);

typedef void(aws_iot_service_client_event_received_fn)(
    const struct aws_iot_service_client_event_received_data *event_received_data);

struct aws_iot_service_client_event_subscribe_config {

    struct aws_byte_cursor event_topic_name;

    enum aws_mqtt5_qos qos;

    aws_iot_service_client_event_subscription_complete_fn *subscription_complete_callback;
    aws_iot_service_client_event_subscription_lost_fn *subscription_lost_callback;
    aws_iot_service_client_event_received_fn *event_received_callback;

    void *user_data;
};

struct aws_iot_service_client_event_unsubscribe_complete_data {
    struct aws_iot_service_client *client;

    int error_code;

    void *user_data;
};

typedef void(aws_iot_service_client_event_unsubscribe_complete_fn)(
    const struct aws_iot_service_client_event_unsubscribe_complete_data *unsubscribe_complete_data);

struct aws_iot_service_client_event_unsubscribe_config {
    struct aws_byte_cursor event_topic_name;

    aws_iot_service_client_event_unsubscribe_complete_fn *unsubscribe_complete_callback;

    void *user_data;
};

struct aws_iot_service_client_make_request_config {
    struct aws_byte_cursor request_topic_prefix;

    struct aws_byte_cursor request_payload;

    enum aws_mqtt5_qos qos;

    /* Missing: client token (request id) extractor, callback function and user data */
};

AWS_EXTERN_C_BEGIN

AWS_MQTT_API struct aws_iot_service_client *aws_iot_service_client_new(
    struct aws_allocator *allocator,
    const struct aws_iot_service_client_config *options);

AWS_MQTT_API struct aws_iot_service_client *aws_iot_service_client_acquire(struct aws_iot_service_client *client);

AWS_MQTT_API struct aws_iot_service_client *aws_iot_service_client_release(struct aws_iot_service_client *client);

AWS_MQTT_API int aws_iot_service_client_subscribe_to_event(
    struct aws_iot_service_client *client,
    const struct aws_iot_service_client_event_subscribe_config *options);

AWS_MQTT_API int aws_iot_service_client_unsubscribe_from_event(
    struct aws_iot_service_client *client,
    const struct aws_iot_service_client_event_unsubscribe_config *options);

AWS_MQTT_API int aws_iot_service_client_submit_request(
    struct aws_iot_service_client *client,
    const struct aws_iot_service_client_make_request_config *options);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_AWS_IOT_SERVICE_CLIENT_H */
