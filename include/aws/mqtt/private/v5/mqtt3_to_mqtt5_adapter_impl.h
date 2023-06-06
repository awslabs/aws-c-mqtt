/**
* Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
* SPDX-License-Identifier: Apache-2.0.
*/

#ifndef AWS_MQTT_MQTT3_TO_MQTT5_ADAPTER_IMPL_H
#define AWS_MQTT_MQTT3_TO_MQTT5_ADAPTER_IMPL_H

#include <aws/mqtt/mqtt.h>

#include <aws/common/mutex.h>
#include <aws/mqtt/client.h>

struct aws_mqtt3_to_mqtt5_adapter_operation_base;

struct aws_mqtt3_to_mqtt5_adapter_operation_vtable {
    void (*destroy_fn)(struct aws_mqtt3_to_mqtt5_adapter_operation_base *operation);
    void (*fail_fn)(struct aws_mqtt3_to_mqtt5_adapter_operation_base *operation, int error_code);
    void (*complete_fn)(struct aws_mqtt3_to_mqtt5_adapter_operation_base *operation, void *completion_data);
};

struct aws_mqtt3_to_mqtt5_adapter_publish_options {
    const struct aws_byte_cursor *topic;
    enum aws_mqtt_qos qos;
    bool retain;
    const struct aws_byte_cursor *payload;

    aws_mqtt_op_complete_fn *on_complete;
    void *userdata;
};

enum aws_mqtt3_to_mqtt5_adapter_operation_type {
    AWS_MQTT3TO5_AOT_PUBLISH,
    AWS_MQTT3TO5_AOT_SUBSCRIBE,
    AWS_MQTT3TO5_AOT_UNSUBSCRIBE,
};

struct aws_mqtt3_to_mqtt5_adapter_operation_base {
    struct aws_allocator *allocator;
    const struct aws_mqtt3_to_mqtt5_adapter_operation_vtable *vtable;
    void *impl;

    enum aws_mqtt3_to_mqtt5_adapter_operation_type type;
    uint16_t id;
};

struct aws_mqtt3_to_mqtt5_adapter_operation_publish {
    struct aws_mqtt3_to_mqtt5_adapter_operation_base base;

    /*
     * Holds a reference while traveling to the event loop only.
     *
     * We avoid calling back into a deleted adapter by zeroing out the
     * mqtt5 operation callbacks for everything we've submitted when the listener is fully detached (in the
     * event loop thread).
     */
    struct aws_mqtt3_to_mqtt5_adapter *adapter;

    /* holds a reference */
    struct aws_mqtt5_operation_publish *publish_op;

    aws_mqtt_op_complete_fn *on_publish_complete;
    void *on_publish_complete_user_data;
};

/*

  Issue 1: An adapter op could be in flight to the event loop when the listener detach resolves.  It's also in the
  table, how to handle?
  A: Don't use listener detach as the cleanup event, use internal ref count -> zero

  Sequencing (PUBLISH example):

  Mqtt311 public API call
     Create cross thread task
     Create adapter op -> Create mqtt5 op
     allocate id and add operation to adapter table
     submit cross thread task to event loop
     return id or 0

  Adapter Op reaches event loop task function: (from this point, all callbacks must be safe-guarded)
     terminated = true
     Safe handler:
        If adapter not terminated:
            terminated = false
            Synchronously enqueue operation to mqtt5 client
     if terminated:
         remove adapter op from table
         destroy adapter op
     Release internal ref to adapter

  On publish completion:
     Safe handler:
        If not terminated:
            invoke mqtt311 callback
     Remove adapter op from table
     Destroy adapter op

  On final destroy (zero internal refs):
     Iterate all incomplete adapter operations and cancel them: zero callbacks and remove from queue if in queue and
        unbound
     Destroy all adapter ops
     Clear table
*/

struct aws_mqtt3_to_mqtt5_adapter_operation_table {
    struct aws_mutex lock;

    struct aws_hash_table operations;
    uint16_t next_id;
};

AWS_MQTT_API void aws_mqtt3_to_mqtt5_adapter_operation_table_init(struct aws_mqtt3_to_mqtt5_adapter_operation_table *table, struct aws_allocator *allocator);

/*
 * No call backs because by the time we call clean up we're terminated.  Cancel all ops though?  A: We haven't released our mqtt5 client reference yet so we are safe to internally manipulate.
 */
AWS_MQTT_API void aws_mqtt3_to_mqtt5_adapter_operation_table_clean_up(struct aws_mqtt3_to_mqtt5_adapter_operation_table *table);

AWS_MQTT_API int aws_mqtt3_to_mqtt5_adapter_operation_table_add_operation(struct aws_mqtt3_to_mqtt5_adapter_operation_table *table, struct aws_mqtt3_to_mqtt5_adapter_operation_base *operation);

AWS_MQTT_API void aws_mqtt3_to_mqtt5_adapter_operation_table_remove_operation(struct aws_mqtt3_to_mqtt5_adapter_operation_table *table, uint16_t operation_id);

AWS_MQTT_API struct aws_mqtt3_to_mqtt5_adapter_operation_publish *aws_mqtt3_to_mqtt5_adapter_operation_new_publish(struct aws_allocator *allocator, struct aws_mqtt3_to_mqtt5_adapter_publish_options *options);

AWS_MQTT_API void aws_mqtt3_to_mqtt5_adapter_operation_destroy(struct aws_mqtt3_to_mqtt5_adapter_operation_base *operation);

#endif /* AWS_MQTT_MQTT3_TO_MQTT5_ADAPTER_IMPL_H */
