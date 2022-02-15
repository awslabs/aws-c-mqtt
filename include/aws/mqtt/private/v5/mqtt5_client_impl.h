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
struct aws_http_proxy_options;
struct aws_mqtt5_client_options_storage;
struct aws_mqtt5_operation;
struct aws_websocket_client_connection_options;

struct aws_mqtt5_client_vtable {
    uint64_t (*get_current_time_fn)(void);                                   /* aws_high_res_clock_get_ticks */
    int (*channel_shutdown_fn)(struct aws_channel *channel, int error_code); /* aws_channel_shutdown */
    int (*websocket_connect_fn)(
        const struct aws_websocket_client_connection_options *options); /* aws_websocket_client_connect */
    int (*client_bootstrap_new_socket_channel_fn)(
        struct aws_socket_channel_bootstrap_options *options); /* aws_client_bootstrap_new_socket_channel */
    int (*http_proxy_new_socket_channel_fn)(
        struct aws_socket_channel_bootstrap_options *channel_options,
        const struct aws_http_proxy_options *proxy_options); /* aws_http_proxy_new_socket_channel */

    /*
     * Potential additional candidates:
     *
     * aws_channel_slot_remove
     * aws_websocket_release
     * aws_websocket_get_channel
     * aws_websocket_convert_to_midchannel_handler
     */
};

/**
 * The various states that the client can be in.
 */
enum aws_mqtt5_client_state {

    /*
     * The client is not connected and not waiting for anything to happen.
     *
     * Next States:
     *    CONNECTING - if the user invokes Start() on the client
     *    TERMINATED - if the user releases the last ref count on the client
     */
    AWS_MCS_STOPPED,

    /*
     * The client is attempting to connect to a remote endpoint, and is waiting for channel setup to complete.
     *
     * Next States:
     *    MQTT_CONNECT - if the channel completes setup with no error and desired state is still CONNECTED
     *    CHANNEL_SHUTDOWN - if the channel completes setup with no error, but desired state is not CONNECTED
     *    PENDING_RECONNECT - if the channel fails to complete setup and desired state is still CONNECTED
     *    STOPPED - if the channel fails to complete setup and desired state is not CONNECTED
     */
    AWS_MCS_CONNECTING,

    /*
     * The client is sending a CONNECT packet and waiting on a CONNACK packet.
     *
     * Next States:
     *    CONNECTED - if a successful CONNACK is received and desired state is still CONNECTED
     *    CHANNEL_SHUTDOWN - On send/encode errors, read/decode errors, unsuccessful CONNACK, timeout to receive
     *       CONNACK, successful CONNACK but desired state is no longer CONNECTED
     */
    AWS_MCS_MQTT_CONNECT,

    /*
     * The client is ready to perform user-requested mqtt operations.
     *
     * Next States:
     *    CHANNEL_SHUTDOWN - On send/encode errors, read/decode errors, DISCONNECT packet received, desired state
     *       no longer CONNECTED
     *    PENDING_RECONNECT - unexpected channel shutdown completion
     */
    AWS_MCS_CONNECTED,

    /*
     * NYI/TODO: a state tentatively earmarked for processing the current mqtt outbound operation as well as a
     * user-or-client-created outbound DISCONNECT.
     */
    AWS_MCS_CLEAN_DISCONNECT,

    /*
     * The client is waiting for the io channel to completely shut down.
     *
     * Next States:
     *    PENDING_RECONNECT - the io channel has shut down and desired state is still CONNECTED
     *    STOPPED - the io channel has shut down and desired state is not CONNECTED
     */
    AWS_MCS_CHANNEL_SHUTDOWN,

    /*
     * The client is waiting for the reconnect timer to expire before attempting to connect again.
     *
     * Next States:
     *    CONNECTING - the reconnect timer has expired and desired state is still CONNECTED
     *    STOPPED - desired state is no longer CONNECTED
     */
    AWS_MCS_PENDING_RECONNECT,

    /*
     * The client is performing final shutdown and release of all resources.  This state is only realized for
     * a single service.
     */
    AWS_MCS_TERMINATED,
};

/*
 * In order to make it easier to guarantee the lifecycle events are properly paired and emitted, we track
 * a separate state (from aws_mqtt5_client_state) and emit lifecycle events based on it.
 *
 * For example, if our lifecycle event is state CONNECTING, than anything going wrong becomes a CONNECTION_FAILED event
 * whereas if we were in  CONNECTED, it must be a DISCONNECTED event.  By setting the state to NONE after emitting
 * a CONNECTION_FAILED or DISCONNECTED event, then emission spots further down the execution pipeline will not
 * accidentally emit an additional event.  This also allows us to emit immediately when an event happens, if
 * appropriate, without having to persist additional event data (like packet views) until some singular point.
 *
 * For example:
 *
 * If I'm in CONNECTING and the channel shuts down, I want to emit a CONNECTION_FAILED event with the error code.
 * If I'm in CONNECTING and I receive a failed CONNACK, I want to emit a CONNECTION_FAILED event immediately with
 *   the CONNACK view in it and then invoke channel shutdown (and channel shutdown completing later should not emit an
 *   event).
 * If I'm in CONNECTED and the channel shuts down, I want to emit a DISCONNECTED event with the error code.
 * If I'm in CONNECTED and get a DISCONNECT packet from the server, I want to emit a DISCONNECTED event with
 *  the DISCONNECT packet in it, invoke channel shutdown,  and then I *don't* want to emit a DISCONNECTED event
 *  when the channel finishes shutting down.
 */
enum aws_mqtt5_lifecycle_state {
    AWS_MQTT5_LS_NONE,
    AWS_MQTT5_LS_CONNECTING,
    AWS_MQTT5_LS_CONNECTED,
};

struct aws_mqtt5_client {

    struct aws_allocator *allocator;
    struct aws_ref_count ref_count;

    const struct aws_mqtt5_client_vtable *vtable;

    const struct aws_mqtt5_client_options_storage *config;

    struct aws_task service_task;
    uint64_t next_service_task_run_time;
    bool in_service;

    struct aws_mqtt5_negotiated_settings negotiated_settings;

    struct aws_event_loop *loop;

    /* Channel handler information */
    struct aws_channel_handler handler;
    struct aws_channel_slot *slot;

    enum aws_mqtt5_client_state desired_state;
    enum aws_mqtt5_client_state current_state;

    enum aws_mqtt5_lifecycle_state lifecycle_state;

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
     * Operation-related state notes
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
     *   On PUBLISH completely received (and final callback invoked):
     *      Add PUBACK at head of queued_operations
     *
     *   On disconnect (on transition to PENDING_RECONNECT or STOPPED):
     *      If current_operation, move current_operation to head of queued_operations
     *      If disconnect_queue_policy is fail(x):
     *          Fail, release, and remove everything in queued_operations with property (x)
     *          Release and remove: PUBACK, DISCONNECT
     *      Fail, remove, and release unacked_operations if:
     *          Operation is not Qos 1+ publish
     *
     *   On reconnect (post CONNACK):
     *      if rejoined_session == false:
     *          Fail, remove, and release unacked_operations
     *
     *      Move-Append unacked_operations to the head of queued_operations
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

AWS_MQTT_API void aws_mqtt5_client_set_vtable(
    struct aws_mqtt5_client *client,
    const struct aws_mqtt5_client_vtable *vtable);

AWS_MQTT_API const struct aws_mqtt5_client_vtable *aws_mqtt5_client_get_default_vtable(void);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_MQTT5_CLIENT_IMPL_H */
