#ifndef AWS_MQTT_MQTT5_CLIENT_IMPL_H
#define AWS_MQTT_MQTT5_CLIENT_IMPL_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

#include <aws/common/hash_table.h>
#include <aws/common/ref_count.h>
#include <aws/io/channel.h>
#include <aws/mqtt/private/v5/mqtt5_decoder.h>
#include <aws/mqtt/private/v5/mqtt5_encoder.h>
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

    struct aws_mqtt5_negotiated_settings negotiated_settings;

    struct aws_event_loop *loop;

    /* Channel handler information */
    struct aws_channel_handler handler;
    struct aws_channel_slot *slot;

    enum aws_mqtt5_client_state desired_state;
    enum aws_mqtt5_client_state current_state;

    struct aws_mqtt5_encoder encoder;
    struct aws_mqtt5_decoder decoder;

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
     *   (qos 0 publish, disconnect, connect)
     *      user (via cross thread task) ->
     *      queued_operations -> (on front of queue)
     *      current_operation -> (on completely encoded and passed to next handler)
     *      write_completion_operations -> (on socket write complete)
     *      release
     *
     *   (qos 1+ publish, sub/unsub)
     *      user (via cross thread task) ->
     *      queued_operations -> (on front of queue)
     *      current_operation -> (on completely encoded and passed to next handler)
     *      unacked_operations && unacked_operations_table -> (on ack received)
     *      release
     *
     *      QoS 1+ requires both a table and a list holding the same operations in order to support fast lookups by
     *      mqtt packet id and in-order re-queueing in the case of a disconnection (required by spec)
     *
     *   On disconnect (on transition to PENDING_RECONNECT or STOPPED):
     *      If current_operation, move current_operation to head of queued_operations
     *      If disconnect_queue_policy is fail(x):
     *          Fail, release, and remove everything in queued_operations with property (x)
     *
     *   On reconnect (post CONNACK):
     *      Fail, remove, and release unacked_operations if:
     *          rejoined_session = false
     *          or operation-is-not(qos-1+-publish)
     *
     *      Move-Append unacked_operations to the head of queued_operations
     *
     *      Clear unacked_operations_table
     */
    struct aws_linked_list queued_operations;
    struct aws_mqtt5_operation *current_operation;
    struct aws_hash_table unacked_operations_table;
    struct aws_linked_list unacked_operations;
    struct aws_linked_list write_completion_operations;
    bool pending_write_completion;

    /*
     * TODO: topic alias mappings, from-server and to-server have independent mappings
     *
     * From-server requires a single table
     * To-server requires both a table and a list (for LRU)
     */

    /* TODO: statistics that use atomics because we don't care about consistency/isolation */

    /*
     * TODO: flow control system.  Initial requires only a count of # of unacked QoS1+ publishes.
     * Followup will support per-op-type token-bucket rate controls against fixed IoT Core limits as an opt-in option,
     * as well as throughput throttles and any other modellable IoT Core limit.
     */

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
