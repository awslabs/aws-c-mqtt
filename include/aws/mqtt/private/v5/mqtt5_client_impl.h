#ifndef AWS_MQTT_MQTT5_CLIENT_IMPL_H
#define AWS_MQTT_MQTT5_CLIENT_IMPL_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

#include <aws/common/ref_count.h>
#include <aws/io/channel.h>
#include <aws/mqtt/private/topic_tree.h>
#include <aws/mqtt/v5/mqtt5_types.h>

struct aws_event_loop;
struct aws_http_message;
struct aws_mqtt5_client_options_storage;
struct aws_mqtt5_operation;

enum aws_mqtt5_client_state {
    AWS_MCS_STOPPED,
    AWS_MCS_CONNECTING,
    AWS_MCS_MQTT_CONNECT,
    AWS_MCS_CONNECTED,
    AWS_MCS_CLEAN_DISCONNECT,
    AWS_MCS_CHANNEL_SHUTDOWN,
    AWS_MCS_PENDING_RECONNECT,
    AWS_MCS_TERMINATED,
};

struct aws_mqtt5_client {
    struct aws_task service_task;
    uint64_t next_service_task_run_time;

    struct aws_allocator *allocator;
    struct aws_ref_count ref_count;
    const struct aws_mqtt5_client_options_storage *config;

    struct aws_event_loop *loop;

    /* Channel handler information */
    struct aws_channel_handler handler;
    struct aws_channel_slot *slot;

    enum aws_mqtt5_client_state desired_state;
    enum aws_mqtt5_client_state current_state;

    /*
     * Temporary state-related data.
     *
     * disconnect_operation exists from Stop invocation until clean disconnect completes or is skipped/failed.
     *
     * handshake exists on websocket-configured clients between the transform completion timepoint and the
     * websocket setup callback.
     */
    struct aws_mqtt5_operation_disconnect *disconnect_operation;
    struct aws_http_message *handshake;

    aws_mqtt5_packet_id_t next_mqtt_packet_id;

    /*
     * Operation-related state
     *
     * operation flow:
     *   (qos 0)
     *      user (via cross thread task) ->
     *      queued_operations -> (on front of queue)
     *      current_operation -> (on completely encoded and passed to next handler)
     *      write_completion_operations -> (on socket write complete)
     *      release
     *
     *   (qos 1+)
     *      user (via cross thread task) ->
     *      queued_operations -> (on front of queue)
     *      current_operation -> (on completely encoded and passed to next handler)
     *      unacked_operations && unacked_operations_table -> (on ack received)
     *      release
     *
     *      QoS 1+ requires both a table and a list holding the same operations in order to support fast lookups by
     *      mqtt packet id and in-order re-queueing in the case of a disconnection (required by spec)
     *
     *   On disconnect:
     *      Fail and release all QoS0 operations in queued_operations, current_operation, write_completion_operations
     *      If current_operation is QoS1+, move to tail of unacked_operations
     *      Append unacked_operations to the head of queued_operations
     *      Clear unacked_operations_table
     */
    struct aws_linked_list queued_operations;
    struct aws_mqtt5_operation *current_operation;
    struct aws_hash_table unacked_operations_table;
    struct aws_linked_list unacked_operations;
    struct aws_linked_list write_completion_operations;

    struct aws_mqtt_topic_tree subscriptions;

    /* 
     *TODO: topic alias mappings, from-server and to-server have independent mappings
     * 
     * From-server requires a single table
     * To-server requires both a table and a list (for LRU)
     */

    /* TODO: statistics that use atomics because we don't care about consistency/isolation */

    /* next event times and related data */
    uint64_t next_ping_time;
    uint64_t next_ping_timeout_time;

    uint64_t next_reconnect_time;
    uint64_t current_reconnect_delay_interval_ms;
    uint64_t next_reconnect_delay_interval_reset_time;

    uint64_t next_mqtt_connect_packet_timeout_time;
};

AWS_EXTERN_C_BEGIN

AWS_EXTERN_C_END

#endif /* AWS_MQTT_MQTT5_CLIENT_IMPL_H */
