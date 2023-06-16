/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#ifndef AWS_MQTT_MQTT3_TO_MQTT5_ADAPTER_IMPL_H
#define AWS_MQTT_MQTT3_TO_MQTT5_ADAPTER_IMPL_H

#include <aws/mqtt/mqtt.h>

#include <aws/common/mutex.h>
#include <aws/common/rw_lock.h>
#include <aws/common/task_scheduler.h>
#include <aws/mqtt/client.h>
#include <aws/mqtt/private/client_impl_shared.h>
#include <aws/mqtt/v5/mqtt5_client.h>

struct aws_mqtt3_to_mqtt5_adapter_operation_base;

struct aws_mqtt3_to_mqtt5_adapter_operation_vtable {
    void (*destroy_fn)(struct aws_mqtt3_to_mqtt5_adapter_operation_base *operation);
    void (*fail_fn)(struct aws_mqtt3_to_mqtt5_adapter_operation_base *operation, int error_code);
    void (*complete_fn)(struct aws_mqtt3_to_mqtt5_adapter_operation_base *operation, void *completion_data);
};

struct aws_mqtt3_to_mqtt5_adapter_publish_options {
    struct aws_mqtt_client_connection_5_impl *adapter;

    const struct aws_byte_cursor topic;
    enum aws_mqtt_qos qos;
    bool retain;
    const struct aws_byte_cursor payload;

    aws_mqtt_op_complete_fn *on_complete;
    void *on_complete_userdata;
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

    /*
     * Holds an external reference while traveling to the event loop only.
     *
     * We avoid calling back into a deleted adapter by zeroing out the
     * mqtt5 operation callbacks for everything we've submitted before final mqtt5 client release.
     */
    struct aws_mqtt_client_connection_5_impl *adapter;
    bool holding_adapter_ref;

    struct aws_task submission_task;

    enum aws_mqtt3_to_mqtt5_adapter_operation_type type;
    uint16_t id;
};

struct aws_mqtt3_to_mqtt5_adapter_operation_publish {
    struct aws_mqtt3_to_mqtt5_adapter_operation_base base;

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

/*
 * The adapter maintains a notion of state based on how its 311 API has been used.  This state guides how it handles
 * external lifecycle events.
 *
 * Operational events are always relayed unless the adapter has been terminated.
 */
enum aws_mqtt_adapter_state {

    /*
     * The 311 API has had connect() called but that connect has not yet resolved.
     *
     * If it resolves successfully we will move to the STAY_CONNECTED state which will relay lifecycle callbacks
     * transparently.
     *
     * If it resolves unsuccessfully, we will move to the STAY_DISCONNECTED state where we will ignore lifecycle
     * events because, from the 311 API's perspective, nothing should be getting emitted.
     */
    AWS_MQTT_AS_FIRST_CONNECT,

    /*
     * A call to the 311 connect API has resolved successfully.  Relay all lifecycle events until told otherwise.
     */
    AWS_MQTT_AS_STAY_CONNECTED,

    /*
     * We have not observed a successful initial connection attempt via the 311 API (or disconnect has been
     * invoked afterwards).  Ignore all lifecycle events.
     */
    AWS_MQTT_AS_STAY_DISCONNECTED,
};

struct aws_mqtt_client_connection_5_impl {

    struct aws_allocator *allocator;

    struct aws_mqtt_client_connection base;

    struct aws_mqtt5_client *client;
    struct aws_mqtt5_listener *listener;
    struct aws_event_loop *loop;

    /*
     * An event-loop-internal flag that we can read to check to see if we're in the scope of a callback
     * that has already locked the adapter's lock.  Can only be referenced from the event loop thread.
     *
     * We use the flag to avoid deadlock in a few cases where we can re-enter the adapter logic from within a callback.
     * It also provides a nice solution for the fact that we cannot safely upgrade a read lock to a write lock.
     */
    bool in_synchronous_callback;

    /*
     * The current adapter state based on the sequence of connect(), disconnect(), and connection completion events.
     * This affects how the adapter reacts to incoming mqtt5 events.  Under certain conditions, we may change
     * this state value based on unexpected events (stopping the mqtt5 client underneath the adapter, for example)
     */
    enum aws_mqtt_adapter_state adapter_state;

    /*
     * Tracks all references from external sources (ie users).  Incremented and decremented by the public
     * acquire/release APIs of the 311 connection.
     *
     * When this value drops to zero, the terminated flag is set and no further callbacks will be invoked.  This
     * also starts the asynchronous destruction process for the adapter.
     */
    struct aws_ref_count external_refs;

    /*
     * Tracks all references to the adapter from internal sources (temporary async processes that need the
     * adapter to stay alive for an interval of time, like sending tasks across thread boundaries).
     *
     * Starts with a single reference that is held until the adapter's listener has fully detached from the mqtt5
     * client.
     *
     * Once the internal ref count drops to zero, the adapter may be destroyed synchronously.
     */
    struct aws_ref_count internal_refs;

    /*
     * We use the adapter lock to guarantee that we can synchronously sever all callbacks from the mqtt5 client even
     * though adapter shutdown is an asynchronous process.  This means the lock is held during callbacks which is a
     * departure from our normal usage patterns.  We prevent deadlock (due to logical re-entry) by using the
     * in_synchronous_callback flag.
     *
     * We hold a read lock when invoking callbacks and a write lock when setting terminated from false to true.
     */
    struct aws_rw_lock lock;

    /*
     * Synchronized data protected by the adapter lock.
     */
    struct {
        bool terminated;
    } synced_data;

    struct aws_mqtt3_to_mqtt5_adapter_operation_table operational_state;

    /* All fields after here are internal to the adapter event loop thread */

    /* 311 interface callbacks */
    aws_mqtt_client_on_connection_interrupted_fn *on_interrupted;
    void *on_interrupted_user_data;

    aws_mqtt_client_on_connection_resumed_fn *on_resumed;
    void *on_resumed_user_data;

    aws_mqtt_client_on_connection_closed_fn *on_closed;
    void *on_closed_user_data;

    aws_mqtt_client_publish_received_fn *on_any_publish;
    void *on_any_publish_user_data;

    aws_mqtt_transform_websocket_handshake_fn *websocket_handshake_transformer;
    void *websocket_handshake_transformer_user_data;

    aws_mqtt5_transform_websocket_handshake_complete_fn *mqtt5_websocket_handshake_completion_function;
    void *mqtt5_websocket_handshake_completion_user_data;

    /* (mutually exclusive) 311 interface one-time transient callbacks */
    aws_mqtt_client_on_disconnect_fn *on_disconnect;
    void *on_disconnect_user_data;

    aws_mqtt_client_on_connection_complete_fn *on_connection_complete;
    void *on_connection_complete_user_data;
};

AWS_EXTERN_C_BEGIN

AWS_MQTT_API void aws_mqtt3_to_mqtt5_adapter_operation_table_init(
    struct aws_mqtt3_to_mqtt5_adapter_operation_table *table,
    struct aws_allocator *allocator);

/*
 * Q: No call backs because by the time we call clean up we're terminated.  Cancel all ops though?
 * A: We haven't released our mqtt5 client reference yet so we are safe to internally manipulate (release our ref and
 * zero completion callbacks)
 */
AWS_MQTT_API void aws_mqtt3_to_mqtt5_adapter_operation_table_clean_up(
    struct aws_mqtt3_to_mqtt5_adapter_operation_table *table);

AWS_MQTT_API int aws_mqtt3_to_mqtt5_adapter_operation_table_add_operation(
    struct aws_mqtt3_to_mqtt5_adapter_operation_table *table,
    struct aws_mqtt3_to_mqtt5_adapter_operation_base *operation);

AWS_MQTT_API void aws_mqtt3_to_mqtt5_adapter_operation_table_remove_operation(
    struct aws_mqtt3_to_mqtt5_adapter_operation_table *table,
    uint16_t operation_id);

AWS_MQTT_API struct aws_mqtt3_to_mqtt5_adapter_operation_publish *aws_mqtt3_to_mqtt5_adapter_operation_new_publish(
    struct aws_allocator *allocator,
    struct aws_mqtt3_to_mqtt5_adapter_publish_options *options);

AWS_MQTT_API void aws_mqtt3_to_mqtt5_adapter_operation_destroy(
    struct aws_mqtt3_to_mqtt5_adapter_operation_base *operation);

AWS_MQTT_API void aws_mqtt3_to_mqtt5_adapter_operation_reference_adapter(
    struct aws_mqtt3_to_mqtt5_adapter_operation_base *operation);
AWS_MQTT_API void aws_mqtt3_to_mqtt5_adapter_operation_dereference_adapter(
    struct aws_mqtt3_to_mqtt5_adapter_operation_base *operation);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_MQTT3_TO_MQTT5_ADAPTER_IMPL_H */
