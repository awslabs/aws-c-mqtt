#ifndef AWS_MQTT_MQTT5_CLIENT_H
#define AWS_MQTT_MQTT5_CLIENT_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

#include <aws/mqtt/v5/mqtt5_types.h>

struct aws_allocator;
struct aws_client_bootstrap;
struct aws_input_stream;
struct aws_mqtt5_client;
struct aws_tls_connection_options;
struct aws_socket_options;

/**
 * Basic mqtt5 client configuration struct.
 *
 * Contains desired connection properties
 * Configuration that represents properties of the mqtt5 CONNECT packet go in the connect view (connect_options)
 */
struct aws_mqtt5_client_options {

    /**
     * Host to establish mqtt connections to
     */
    struct aws_byte_cursor host_name;

    /**
     * Port to establish mqtt connections to
     */
    uint16_t port;

    /**
     * Client bootstrap to use whenever this client establishes a connection
     */
    struct aws_client_bootstrap *bootstrap;

    /**
     * Socket options to use whenever this client establishes a connection
     */
    const struct aws_socket_options *socket_options;

    /**
     * (Optional) Tls options to use whenever this client establishes a connection
     */
    const struct aws_tls_connection_options *tls_options;

    /**
     * (Optional) Http proxy options to use whenever this client establishes a connection
     */
    const struct aws_http_proxy_options *http_proxy_options;

    /**
     * (Optional) Websocket handshake transformation function and user data.  Websockets are used if the
     * transformation function is non-null.
     */
    aws_mqtt5_transform_websocket_handshake_fn *websocket_handshake_transform;
    void *websocket_handshake_transform_user_data;

    /**
     * All CONNECT-related options, includes the will configuration, if desired
     */
    const struct aws_mqtt5_packet_connect_view *connect_options;

    /**
     * Will payload stream, if the will is set in the connect options
     */
    struct aws_input_stream *will_payload;

    /**
     * Controls session rejoin behavior
     */
    enum aws_mqtt5_client_session_behavior_type session_behavior;

    /**
     * Controls how the client uses mqtt5 topic aliasing when processing outbound PUBLISH packets
     */
    enum aws_mqtt5_client_outbound_topic_alias_behavior_type outbound_topic_aliasing_behavior;

    /**
     * Controls the offline queue behavior of the mqtt5 client.
     */
    enum aws_mqtt5_client_offline_queue_behavior_type offline_queue_behavior;

    /**
     * Minimum and maximum amount of time to wait to reconnect after a disconnect.  Basic exponential backoff is
     * used (double the current delay).
     */
    uint64_t min_reconnect_delay_ms;
    uint64_t max_reconnect_delay_ms;

    /**
     * Amount of time that must elapse with a good connection before the reconnect delay is reset to the minimum
     */
    uint64_t min_connected_time_to_reset_reconnect_delay_ms;

    /**
     * Time interval to wait after sending a PINGREQ for a PINGRESP to arrive.  If one does not arrive, the connection
     * will be shut down.
     */
    uint32_t ping_timeout_ms;

    /**
     * Callback and user data for all client lifecycle events.
     * Life cycle events include:
     *    ConnectionSuccess
     *    ConnectionFailure,
     *    Disconnect
     *    (client) Stopped
     *
     *  Disconnect lifecycle events are 1-1 with -- strictly after -- ConnectionSuccess events.
     */
    aws_mqtt5_client_connection_event_callback_fn *lifecycle_event_handler;
    void *lifecycle_event_handler_user_data;
};

AWS_EXTERN_C_BEGIN

/**
 * Creates a new mqtt5 client using the supplied configuration
 *
 * @param allocator allocator to use with all memory operations related to this client's creation and operation
 * @param options mqtt5 client configuration
 * @return a new mqtt5 client or NULL
 */
AWS_MQTT_API
struct aws_mqtt5_client *aws_mqtt5_client_new(
    struct aws_allocator *allocator,
    const struct aws_mqtt5_client_options *options);

/**
 * Acquires a reference to an mqtt5 client
 *
 * @param client client to acquire a reference to.  May be NULL.
 * @return what was passed in as the client (a client or NULL)
 */
AWS_MQTT_API
struct aws_mqtt5_client *aws_mqtt5_client_acquire(struct aws_mqtt5_client *client);

/**
 * Release a reference to an mqtt5 client.  When the client ref count drops to zero, the client will automatically
 * trigger a stop and once the stop completes, the client will delete itself.
 *
 * @param client client to release a reference to.  May be NULL.
 * @return NULL
 */
AWS_MQTT_API
struct aws_mqtt5_client *aws_mqtt5_client_release(struct aws_mqtt5_client *client);

/**
 * Asynchronous notify to the mqtt5 client that you want it to attempt to connect to the configured endpoint. After
 * connecting the client will attempt to stay connected using the properties of the reconnect-related parameters
 * in the mqtt5 client configuration.
 *
 * @param client mqtt5 client to start
 * @return success/failure in the synchronous logic that kicks off the start process
 */
AWS_MQTT_API
int aws_mqtt5_client_start(struct aws_mqtt5_client *client);

/**
 * Asynchronous notify to the mqtt5 client that you want it to transition to the stopped state.
 *
 * @param client mqtt5 client to stop
 * @param disconnect_options (optional) properties of a DISCONNECT packet to send as part of the shutdown process
 * @return success/failure in the synchronous logic that kicks off the stop process
 */
AWS_MQTT_API
int aws_mqtt5_client_stop(
    struct aws_mqtt5_client *client,
    const struct aws_mqtt5_packet_disconnect_view *disconnect_options);

/**
 * Queues a Publish operation in an mqtt5 client
 *
 * @param client mqtt5 client to queue a Publish for
 * @param publish_options configuration options for the Publish operation
 * @param completion_options completion callback configuration.  QoS 0 publishes invoke the callback when the
 * data has been written to the socket.  QoS1+ publishes invoke the callback when the corresponding ack is received.
 * @return success/failure in the synchronous logic that kicks off the publish operation
 */
AWS_MQTT_API
int aws_mqtt5_client_publish(
    struct aws_mqtt5_client *client,
    const struct aws_mqtt5_packet_publish_view *publish_options,
    const struct aws_mqtt5_publish_completion_options *completion_options);

/**
 * Queues a Subscribe operation in an mqtt5 client
 *
 * @param client mqtt5 client to queue a Subscribe for
 * @param subscribe_options configuration options for the Subscribe operation
 * @param completion_options Completion callback configuration.  Invoked when the corresponding SUBACK is received.
 * @return success/failure in the synchronous logic that kicks off the Subscribe operation
 */
AWS_MQTT_API
int aws_mqtt5_client_subscribe(
    struct aws_mqtt5_client *client,
    const struct aws_mqtt5_packet_subscribe_view *subscribe_options,
    const struct aws_mqtt5_subscribe_completion_options *completion_options);

/**
 * Queues an Unsubscribe operation in an mqtt5 client
 *
 * @param client mqtt5 client to queue an Unsubscribe for
 * @param unsubscribe_options configuration options for the Unsubscribe operation
 * @param completion_options Completion callback configuration.  Invoked when the corresponding UNSUBACK is received.
 * @return success/failure in the synchronous logic that kicks off the Unsubscribe operation
 */
AWS_MQTT_API
int aws_mqtt5_client_unsubscribe(
    struct aws_mqtt5_client *client,
    const struct aws_mqtt5_packet_unsubscribe_view *unsubscribe_options,
    const struct aws_mqtt5_unsubscribe_completion_options *completion_options);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_MQTT5_CLIENT_H */
