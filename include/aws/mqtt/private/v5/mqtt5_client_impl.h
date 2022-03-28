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

/**
 * The various states that the client can be in.  A client has both a current state and a desired state.
 * Desired state is only allowed to be one of {STOPPED, CONNECTED, TERMINATED}.  The client transitions states
 * based on either
 *  (1) changes in desired state, or
 *  (2) external events.
 *
 * Most states are interruptible (in the sense of a change in desired state causing an immediate change in state) but
 * CONNECTING and CHANNEL_SHUTDOWN cannot be interrupted due to waiting for an asynchronous callback (that has no
 * cancel) to complete.
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
     * The client is attempting to connect to a remote endpoint, and is waiting for channel setup to complete. This
     * state is not interruptible by any means other than channel setup completion.
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
     *       CONNACK, desired state is no longer CONNECTED
     *    PENDING_RECONNECT - unexpected channel shutdown completion and desired state still CONNECTED
     *    STOPPED - unexpected channel shutdown completion and desired state no longer CONNECTED
     */
    AWS_MCS_MQTT_CONNECT,

    /*
     * The client is ready to perform user-requested mqtt operations.
     *
     * Next States:
     *    CHANNEL_SHUTDOWN - On send/encode errors, read/decode errors, DISCONNECT packet received, desired state
     *       no longer CONNECTED, PINGRESP timeout
     *    PENDING_RECONNECT - unexpected channel shutdown completion and desired state still CONNECTED
     *    STOPPED - unexpected channel shutdown completion and desired state no longer CONNECTED
     */
    AWS_MCS_CONNECTED,

    /*
     * NYI/TODO: a state tentatively earmarked for processing the current mqtt outbound operation as well as a
     * user-or-client-created outbound DISCONNECT.
     */
    AWS_MCS_CLEAN_DISCONNECT,

    /*
     * The client is waiting for the io channel to completely shut down.  This state is not interruptible.
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
     * a non-observable instant of time (transition out of STOPPED).
     */
    AWS_MCS_TERMINATED,
};

/**
 * Table of overridable external functions to allow mocking and monitoring of the client.
 */
struct aws_mqtt5_client_vtable {
    /* aws_high_res_clock_get_ticks */
    uint64_t (*get_current_time_fn)(void);

    /* aws_channel_shutdown */
    int (*channel_shutdown_fn)(struct aws_channel *channel, int error_code);

    /* aws_websocket_client_connect */
    int (*websocket_connect_fn)(const struct aws_websocket_client_connection_options *options);

    /* aws_client_bootstrap_new_socket_channel */
    int (*client_bootstrap_new_socket_channel_fn)(struct aws_socket_channel_bootstrap_options *options);

    /* aws_http_proxy_new_socket_channel */
    int (*http_proxy_new_socket_channel_fn)(
        struct aws_socket_channel_bootstrap_options *channel_options,
        const struct aws_http_proxy_options *proxy_options);

    /* This doesn't replace anything, it's just for test verification of state changes */
    void (*on_client_state_change_callback_fn)(
        struct aws_mqtt5_client *client,
        enum aws_mqtt5_client_state old_state,
        enum aws_mqtt5_client_state new_state,
        void *vtable_user_data);

    /* aws_channel_acquire_message_from_pool */
    struct aws_io_message *(*aws_channel_acquire_message_from_pool_fn)(
        struct aws_channel *channel,
        enum aws_io_message_type message_type,
        size_t size_hint,
        void *user_data);

    /* aws_channel_slot_send_message */
    int (*aws_channel_slot_send_message_fn)(
        struct aws_channel_slot *slot,
        struct aws_io_message *message,
        enum aws_channel_direction dir,
        void *user_data);

    void *vtable_user_data;
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
 *      current_operation (allocate packet id if necessary) -> (on completely encoded and passed to next handler)
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
struct aws_mqtt5_client_operational_state {

    /* back pointer to the client */
    struct aws_mqtt5_client *client;

    /*
     * One more than the most recently used packet id.  This is the best starting point for a forward search through
     * the id space for a free id.
     */
    aws_mqtt5_packet_id_t next_mqtt_packet_id;

    struct aws_linked_list queued_operations;
    struct aws_mqtt5_operation *current_operation;
    struct aws_hash_table unacked_operations_table;
    struct aws_linked_list unacked_operations;
    struct aws_linked_list write_completion_operations;

    /*
     * Is there an io message in transit (to the socket) that has not invoked its write completion callback yet?
     * The client implementation only allows one in-transit message at a time, and so if this is true, we don't
     * send additional ones/
     */
    bool pending_write_completion;
};

struct aws_mqtt5_client {

    struct aws_allocator *allocator;
    struct aws_ref_count ref_count;

    const struct aws_mqtt5_client_vtable *vtable;

    /*
     * Client configuration
     */
    const struct aws_mqtt5_client_options_storage *config;

    /*
     * The recurrent task that runs all client logic outside of external event callbacks.  Bound to the client's
     * event loop.
     */
    struct aws_task service_task;

    /*
     * Tracks when the client's service task is next schedule to run.  Is zero if the task is not scheduled to run or
     * we are in the middle of a service (so technically not scheduled too).
     */
    uint64_t next_service_task_run_time;

    /*
     * True if the client's service task is running.  Used to skip service task reevaluation due to state changes
     * while running the service task.  Reevaluation will occur at the very end of the service.
     */
    bool in_service;

    /*
     * The final mqtt5 settings negotiated between defaults, CONNECT, and CONNACK.  Only valid while in
     * CONNECTED or CLEAN_DISCONNECT states.
     */
    struct aws_mqtt5_negotiated_settings negotiated_settings;

    /*
     * Event loop all the client's connections and any related tasks will be pinned to, ensuring serialization and
     * concurrency safety.
     */
    struct aws_event_loop *loop;

    /* Channel handler information */
    struct aws_channel_handler handler;
    struct aws_channel_slot *slot;

    /*
     * What state is the client working towards?
     */
    enum aws_mqtt5_client_state desired_state;

    /*
     * What is the client's current state?
     */
    enum aws_mqtt5_client_state current_state;

    /*
     * The client's lifecycle state.  Used to correctly emit lifecycle events in spite of the complicated
     * async execution pathways that are possible.
     */
    enum aws_mqtt5_lifecycle_state lifecycle_state;

    /*
     * The client's MQTT packet encoder
     */
    struct aws_mqtt5_encoder encoder;

    /*
     * The client's MQTT packet decoder
     */
    struct aws_mqtt5_decoder decoder;

    /*
     * Temporary state-related data.
     *
     * clean_disconnect_error_code - the CLEAN_DISCONNECT state takes time to complete and we want to be able
     * to pass an error code from a prior event to the channel shutdown.  This holds the "override" error code
     * that we'd like to shutdown the channel with while CLEAN_DISCONNECT is processed.
     *
     * handshake exists on websocket-configured clients between the transform completion timepoint and the
     * websocket setup callback.
     */
    int clean_disconnect_error_code;
    struct aws_http_message *handshake;

    /*
     * Wraps all state related to pending and in-progress MQTT operations within the client.
     */
    struct aws_mqtt5_client_operational_state operational_state;

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

    /*
     * When should the next PINGREQ be sent?  Automatically pushed out with ever socket write completion.
     */
    uint64_t next_ping_time;

    /*
     * When should we shut down the channel due to failure to receive a PINGRESP?  Only non-zero when an outstanding
     * PINGREQ has not been answered.
     */
    uint64_t next_ping_timeout_time;

    /*
     * When should the client next attempt to reconnect?  Only used by PENDING_RECONNECT state.
     */
    uint64_t next_reconnect_time_ns;

    /*
     * How many consecutive reconnect failures have we experienced?
     */
    uint64_t reconnect_count;

    /*
     * How much should we wait before our next reconnect attempt?
     */
    uint64_t current_reconnect_delay_ms;

    /*
     * When should the client reset current_reconnect_delay_interval_ms to the minimum value?  Only relevant to the
     * CONNECTED state.
     */
    uint64_t next_reconnect_delay_reset_time_ns;

    /*
     * When should we shut down the channel due to failure to receive a CONNACK?  Only relevant during the MQTT_CONNECT
     * state.
     */
    uint64_t next_mqtt_connect_packet_timeout_time;
};

AWS_EXTERN_C_BEGIN

/*
 * A number of private APIs which are either set up for mocking parts of the client or testing subsystems within it by
 * exposing what would normally be static functions internal to the implementation.
 */

AWS_MQTT_API void aws_mqtt5_client_set_vtable(
    struct aws_mqtt5_client *client,
    const struct aws_mqtt5_client_vtable *vtable);

AWS_MQTT_API const struct aws_mqtt5_client_vtable *aws_mqtt5_client_get_default_vtable(void);

/*
 * Sets the packet id, if necessary, on an operation based on the current pending acks table.  The caller is
 * responsible for adding the operation to the unacked table when the packet has been encoding in an io message.
 *
 * There is an argument that the operation should go into the table only on socket write completion, but that breaks
 * allocation unless an additional, independent table is added, which I'd prefer not to do presently.  Also, socket
 * write completion callbacks can be a bit delayed which could lead to a situation where the response from a local
 * server could arrive before the write completion runs which would be a disaster.
 */
AWS_MQTT_API int aws_mqtt5_operation_bind_packet_id(
    struct aws_mqtt5_operation *operation,
    struct aws_mqtt5_client_operational_state *client_operational_state);

/*
 * Initialize and clean up of the client operational state.  Exposed (privately) to enabled tests to reuse the
 * init/cleanup used by the client itself.
 */
AWS_MQTT_API int aws_mqtt5_client_operational_state_init(
    struct aws_mqtt5_client_operational_state *client_operational_state,
    struct aws_allocator *allocator,
    struct aws_mqtt5_client *client);

AWS_MQTT_API void aws_mqtt5_client_operational_state_clean_up(
    struct aws_mqtt5_client_operational_state *client_operational_state);

/*
 * Resets the client's operational state based on a disconnection (from above comment):
 *
 *   Fail all operations in the pending write completion list
 *   Fail all non-qos-1+ publishes in the pending ack list/table
 *   Fail the current operation
 *   Fail all operations in the queued_operations list
 */
AWS_MQTT_API void aws_mqtt5_client_on_disconnection_update_operational_state(struct aws_mqtt5_client *client);

/*
 * Updates the client's operational state based on a successfully established connection event.
 *
 * If a session was resumed:
 *    Moves unacked QoS 1+ publishes in the unacked_operations list to the head of the pending operation queue
 * Else
 *    Fails and releases all operations in the unacked_operations list (which must be QoS 1+ publishes)
 */
AWS_MQTT_API void aws_mqtt5_client_on_connection_update_operational_state(struct aws_mqtt5_client *client);

/*
 * Processes the pending operation queue based on the current state of the associated client
 */
AWS_MQTT_API int aws_mqtt5_client_service_operational_state(
    struct aws_mqtt5_client_operational_state *client_operational_state);

AWS_MQTT_API void aws_mqtt5_client_operational_state_handle_ack(
    struct aws_mqtt5_client_operational_state *client_operational_state,
    aws_mqtt5_packet_id_t packet_id,
    const void *packet_view);

AWS_MQTT_API bool aws_mqtt5_client_are_negotiated_settings_valid(const struct aws_mqtt5_client *client);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_MQTT5_CLIENT_IMPL_H */
