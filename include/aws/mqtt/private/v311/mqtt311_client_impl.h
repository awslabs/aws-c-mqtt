#ifndef AWS_MQTT_V311_CLIENT_IMPL_H
#define AWS_MQTT_V311_CLIENT_IMPL_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

#include <aws/common/hash_table.h>
#include <aws/common/linked_list.h>
#include <aws/common/ref_count.h>
#include <aws/common/task_scheduler.h>
#include <aws/io/channel.h>
#include <aws/mqtt/v311/mqtt311_client.h>

struct aws_event_loop;
struct aws_http_message;
struct aws_mqtt_request;
struct aws_mqtt311_client;
struct aws_mqtt311_client_config;
struct aws_mqtt311_client_options;

struct aws_websocket_client_connection_options;
struct aws_socket_channel_bootstrap_options;
struct aws_http_proxy_options;


enum aws_mqtt311_client_state {

    /*
     * The client is not connected and not waiting for anything to happen.
     *
     * Next States:
     *    CONNECTING - if the user invokes Connect() on the client
     *    TERMINATED - if the user releases the last ref count on the client
     */
    AWS_MQTT311_CS_STOPPED,

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
    AWS_MQTT311_CS_CONNECTING,

    /*
     * The client is sending a CONNECT packet and waiting on a CONNACK packet.
     *
     * Next States:
     *    CONNECTED - if a successful CONNACK is received and desired state is still CONNECTED
     *    CHANNEL_SHUTDOWN - On send/encode errors, read/decode errors, unsuccessful CONNACK, timeout to receive
     *       CONNACK, desired state is no longer CONNECTED
     *    PENDING_RECONNECT - unexpected channel shutdown completion and desired state is still CONNECTED
     *    STOPPED - unexpected channel shutdown completion and desired state no longer CONNECTED
     */
    AWS_MQTT311_CS_MQTT_CONNECT,

    /*
     * The client is ready to perform user-requested mqtt operations.
     *
     * Next States:
     *    CHANNEL_SHUTDOWN - On send/encode errors, read/decode errors, desired state
     *       no longer CONNECTED, PINGRESP timeout
     *    PENDING_RECONNECT - unexpected channel shutdown completion and desired state still CONNECTED
     *    STOPPED - unexpected channel shutdown completion and desired state no longer CONNECTED
     */
    AWS_MQTT311_CS_CONNECTED,

    /*
     * The client is attempt to shut down a connection cleanly by finishing the current operation and then
     * transmitting an outbound DISCONNECT.
     *
     * Next States:
     *    CHANNEL_SHUTDOWN - on successful (or unsuccessful) send of the DISCONNECT
     *    PENDING_RECONNECT - unexpected channel shutdown completion and desired state still CONNECTED
     *    STOPPED - unexpected channel shutdown completion and desired state no longer CONNECTED
     */
    AWS_MQTT311_CS_CLEAN_DISCONNECT,

    /*
     * The client is waiting for the io channel to completely shut down.  This state is not interruptible.
     *
     * Next States:
     *    PENDING_RECONNECT - the io channel has shut down and desired state is still CONNECTED
     *    STOPPED - the io channel has shut down and desired state is not CONNECTED
     */
    AWS_MQTT311_CS_CHANNEL_SHUTDOWN,

    /*
     * The client is waiting for the reconnect timer to expire before attempting to connect again.
     *
     * Next States:
     *    CONNECTING - the reconnect timer has expired and desired state is still CONNECTED
     *    STOPPED - desired state is no longer CONNECTED
     */
    AWS_MQTT311_CS_PENDING_RECONNECT,

    /*
     * The client is performing final shutdown and release of all resources.  This state is only realized for
     * a non-observable instant of time (transition out of STOPPED).
     */
    AWS_MQTT311_CS_TERMINATED,
};

struct aws_mqtt311_client_vtable {
    /* downstream APIs for test mocking */
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
        struct aws_mqtt311_client *client,
        enum aws_mqtt311_client_state old_state,
        enum aws_mqtt311_client_state new_state,
        void *vtable_user_data);

    /* This doesn't replace anything, it's just for test verification of statistic changes */
    void (*on_client_statistics_changed_callback_fn)(
        struct aws_mqtt311_client *client,
        struct aws_mqtt_request *request,
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



struct aws_mqtt311_client_operational_state {

    /*
     * One more than the most recently used packet id.  This is the best starting point for a forward search through
     * the id space for a free id.
     */
    uint16_t next_mqtt_packet_id;

    struct aws_linked_list queued_requests;
    struct aws_mqtt_request *current_request;
    struct aws_hash_table unacked_requests_table;
    struct aws_linked_list unacked_requests;
    struct aws_linked_list write_completion_requests;

    /*
     * Is there an io message in transit (to the socket) that has not invoked its write completion callback yet?
     * The client implementation only allows one in-transit message at a time, and so if this is true, we don't
     * send additional ones
     */
    bool pending_write_completion;
};

struct aws_mqtt311_client {
    struct aws_allocator *allocator;
    struct aws_ref_count ref_count;

    const struct aws_mqtt311_client_vtable *vtable;

    struct aws_mqtt311_client_config *config;

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
    enum aws_mqtt311_client_state desired_state;

    /*
     * What is the client's current state?
     */
    enum aws_mqtt311_client_state current_state;

    struct aws_mqtt311_client_operational_state operational_state;

    /*
     * Temporary state-related data.
     *
     * clean_disconnect_error_code - the CLEAN_DISCONNECT state takes time to complete and we want to be able
     * to pass an error code from a prior event to the channel shutdown.  This holds the "override" error code
     * that we'd like to shut down the channel with while CLEAN_DISCONNECT is processed.
     *
     * handshake exists on websocket-configured clients between the transform completion timepoint and the
     * websocket setup callback.
     */
    int clean_disconnect_error_code;
    struct aws_http_message *handshake;

    /*
     * When should the next PINGREQ be sent?
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

    /*
     * Starts false and set to true as soon as a successful connection is established.  If the session resumption
     * behavior is AWS_MQTT5_CSBT_REJOIN_POST_SUCCESS then this must be true before the client sends CONNECT packets
     * with clean start set to false.
     */
    bool has_connected_successfully;
};

AWS_EXTERN_C_BEGIN

AWS_MQTT_API
struct aws_mqtt311_client *aws_mqtt311_client_new(
    struct aws_allocator *allocator,
    const struct aws_mqtt311_client_options *options);

AWS_MQTT_API
struct aws_mqtt311_client *aws_mqtt311_client_acquire(struct aws_mqtt311_client *client);

AWS_MQTT_API
struct aws_mqtt311_client *aws_mqtt311_client_release(struct aws_mqtt311_client *client);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_V311_CLIENT_IMPL_H */
