/**
* Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
* SPDX-License-Identifier: Apache-2.0.
*/

#ifndef AWS_MQTT_MQTT3_TO_MQTT5_ADAPTER_IMPL_H
#define AWS_MQTT_MQTT3_TO_MQTT5_ADAPTER_IMPL_H

#include <aws/mqtt/mqtt.h>

struct aws_mqtt3_to_mqtt5_adapter_operation_vtable {
    void (*fail_fn)(struct aws_mqtt3_to_mqtt5_adapter_operation_base *operation, int error_code);
};

struct aws_mqtt3_to_mqtt5_adapter_operation_base {
    struct aws_allocator *allocator;
    const aws_mqtt3_to_mqtt5_adapter_operation_vtable *vtable;
    void *impl;
    enum aws_mqtt3_to_mqtt5_adapter_operation_type type;
    uint16_t id;
};

struct aws_mqtt3_to_mqtt5_adapter_operation_publish {
    struct aws_mqtt3_to_mqtt5_adapter_operation_base base;

    struct aws_byte_buf topic;
    struct aws_byte_buf payload;
    enum aws_mqtt_qos qos;
    bool retain;

    aws_mqtt_op_complete_fn on_publish_complete;
    void *on_publish_complete_user_data;
};

struct aws_mqtt3_to_mqtt5_adapter_operation_table {
    struct aws_hash_table operations;
    uint16_t next_id;
};

AWS_MQTT_API int aws_mqtt3_to_mqtt5_adapter_operation_table_init(struct aws_mqtt3_to_mqtt5_adapter_operation_table *table, struct aws_allocator *allocator);

AWS_MQTT_API void aws_mqtt3_to_mqtt5_adapter_operation_table_clean_up(struct aws_mqtt3_to_mqtt5_adapter_operation_table *table);

AWS_MQTT_API int aws_mqtt3_to_mqtt5_adapter_operation_table_add_operation(struct aws_mqtt3_to_mqtt5_adapter_operation_table *table, struct aws_mqtt3_to_mqtt5_adapter_operation_base *operation);

AWS_MQTT_API int aws_mqtt3_to_mqtt5_adapter_operation_table_remove_operation(struct aws_mqtt3_to_mqtt5_adapter_operation_table *table, uint16_t operation_id);

#endif /* AWS_MQTT_MQTT3_TO_MQTT5_ADAPTER_IMPL_H */
