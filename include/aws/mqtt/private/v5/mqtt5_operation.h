#ifndef AWS_MQTT_MQTT5_OPERATION_H
#define AWS_MQTT_MQTT5_OPERATION_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

#include <aws/common/linked_list.h>
#include <aws/common/ref_count.h>
#include <aws/mqtt/v5/mqtt5_types.h>

struct aws_mqtt5_operation;

enum aws_mqtt5_operation_type {
    AWS_MOT_CONNECT,
    AWS_MOT_SUBSCRIBE,
    AWS_MOT_UNSUBSCRIBE,
    AWS_MOT_PUBLISH,
    AWS_MOT_PUBLISH_ACK,
};

typedef void (*aws_mqtt5_operation_destroy_fn)(struct aws_mqtt5_operation *operation);

/*
 * TODO: need to think about encode/decode because i'd like to
 * keep open the option for operations that are not associated with a single mqtt packet
 */
struct aws_mqtt5_operation_vtable {
    aws_mqtt5_operation_destroy_fn destroy;
};

struct aws_mqtt5_operation {
    enum aws_mqtt5_operation_type operation_type;
    aws_mqtt5_event_id_t id;
    struct aws_ref_count ref_count;
    struct aws_linked_list_node node;
    struct aws_mqtt5_operation_vtable *vtable;
    void *impl;
};

struct aws_mqtt5_operation_connect {
    struct aws_mqtt5_operation base;
};

struct aws_mqtt5_operation_subscribe {
    struct aws_mqtt5_operation base;
};

struct aws_mqtt5_operation_unsubscribe {
    struct aws_mqtt5_operation base;
};

struct aws_mqtt5_operation_publish {
    struct aws_mqtt5_operation base;
};

AWS_EXTERN_C_BEGIN

AWS_MQTT_API struct aws_mqtt5_operation *aws_mqtt5_operation_acquire(struct aws_mqtt5_operation *operation);

AWS_MQTT_API struct aws_mqtt5_operation *aws_mqtt5_operation_release(struct aws_mqtt5_operation *operation);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_MQTT5_OPERATION_H */
