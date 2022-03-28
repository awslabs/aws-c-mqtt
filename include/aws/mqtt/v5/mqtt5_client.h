#ifndef AWS_MQTT_MQTT5_CLIENT_H
#define AWS_MQTT_MQTT5_CLIENT_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

#include <aws/io/retry_strategy.h>
#include <aws/mqtt/v5/mqtt5_types.h>

struct aws_allocator;
struct aws_client_bootstrap;
struct aws_input_stream;
struct aws_mqtt5_client;
struct aws_mqtt5_client_lifecycle_event;
struct aws_mqtt5_publish_payload_delivery_options;
struct aws_tls_connection_options;
struct aws_socket_options;

/* public client-related enums */

/**
 * Controls how the mqtt client should behave with respect to mqtt sessions.
 */
enum aws_mqtt5_client_session_behavior_type {
    /**
     * Always join a new, clean session
     */
    AWS_MQTT5_CSBT_CLEAN,

    /**
     * Attempt to rejoin an existing session.
     */
    AWS_MQTT5_CSBT_REJOIN,
};

/*
 * Outbound aliasing behavior is controlled by this type.
 *
 * Topic alias behavior is described in https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901113
 *
 * Incoming topic aliases are handled opaquely by the client.  Alias id will appear without modification
 * in the received publish view, but the client will always inject the correct topic.
 *
 * If topic aliasing is not supported by the server, this setting has no affect and any attempts to directly
 * manipulate the topic alias id in outbound publishes will be ignored.
 *
 * IMPORTANT: topic aliases are not currently supported
 */
enum aws_mqtt5_client_outbound_topic_alias_behavior_type {
    /*
     * Outbound aliasing is the user's responsibility.  Client will cache and auto-use
     * previously-established aliases if they fall within the negotiated limits of the connection.
     */
    AWS_MQTT5_COTABT_DUMB,

    /*
     * Client ignores any user-specified topic aliasing and acts on the outbound alias set as an LRU cache.
     */
    AWS_MQTT5_COTABT_LRU,
};

/**
 * Extended validation and flow control options
 *
 * Potentially a point of expansion in the future.  We could add custom controls letting people override
 * the Aws IOT Core limits based on their account properties.  We could, with IoT Core support, add dynamic
 * limit recognition via user properties as well.
 */
enum aws_mqtt5_extended_validation_and_flow_control_options {
    /**
     * Do not do any additional validation or flow control outside of the MQTT5 spec
     */
    AWS_MQTT5_EVAFCO_NONE,

    /**
     * Apply additional client-side validation and operational flow control that respects the
     * default AWS IoT Core limits.
     */
    AWS_MQTT5_EVAFCO_AWS_IOT_CORE_DEFAULTS,
};

/**
 * Type of a client lifecycle event
 */
enum aws_mqtt5_client_lifecycle_event_type {
    /**
     * Emitted when the client begins an attempt to connect to the remote endpoint.
     *
     * Mandatory event fields: client, user_data
     */
    AWS_MQTT5_CLET_ATTEMPTING_CONNECT,

    /**
     * Emitted after the client connects to the remote endpoint and receives a successful CONNACK.
     * Every ATTEMPTING_CONNECT will be followed by exactly one CONNECTION_SUCCESS or one CONNECTION_FAILURE.
     *
     * Mandatory event fields: client, user_data, connack_data, settings
     */
    AWS_MQTT5_CLET_CONNECTION_SUCCESS,

    /**
     * Emitted at any point during the connection process when it has conclusively failed.
     * Every ATTEMPTING_CONNECT will be followed by exactly one CONNECTION_SUCCESS or one CONNECTION_FAILURE.
     *
     * Mandatory event fields: client, user_data, error_code
     * Conditional event fields: connack_data
     */
    AWS_MQTT5_CLET_CONNECTION_FAILURE,

    /**
     * Lifecycle event containing information about a disconnect.  Every CONNECTION_SUCCESS will eventually be
     * followed by one and only one DISCONNECTION.
     *
     * Mandatory event fields: client, user_data, error_code
     * Conditional event fields: disconnect_data
     */
    AWS_MQTT5_CLET_DISCONNECTION,

    /**
     * Lifecycle event notifying the user that the client has entered the STOPPED state.
     *
     * Mandatory event fields: client, user_data
     */
    AWS_MQTT5_CLET_STOPPED,
};

/* client-related callback function signatures */

/**
 * Signature of the continuation function to be called after user-code transforms a websocket handshake request
 */
typedef void(aws_mqtt5_transform_websocket_handshake_complete_fn)(
    struct aws_http_message *request,
    int error_code,
    void *complete_ctx);

/**
 * Signature of the websocket handshake request transformation function.  After transformation, the completion
 * function must be invoked to send the request.
 */
typedef void(aws_mqtt5_transform_websocket_handshake_fn)(
    struct aws_http_message *request,
    void *user_data,
    aws_mqtt5_transform_websocket_handshake_complete_fn *complete_fn,
    void *complete_ctx);

/**
 * Callback signature for mqtt5 client lifecycle events.
 */
typedef void(aws_mqtt5_client_connection_event_callback_fn)(const struct aws_mqtt5_client_lifecycle_event *event);

/**
 * Signature of callback to invoke on Publish success/failure.
 */
typedef void(aws_mqtt5_publish_completion_fn)(
    const struct aws_mqtt5_packet_puback_view *puback,
    int error_code,
    void *complete_ctx);

/**
 * Signature of callback to invoke on Subscribe success/failure.
 */
typedef void(aws_mqtt5_subscribe_completion_fn)(
    const struct aws_mqtt5_packet_suback_view *suback,
    int error_code,
    void *complete_ctx);

/**
 * Signature of callback to invoke on Unsubscribe success/failure.
 */
typedef void(aws_mqtt5_unsubscribe_completion_fn)(
    const struct aws_mqtt5_packet_unsuback_view *unsuback,
    int error_code,
    void *complete_ctx);

/**
 * Signature of callback to invoke when a DISCONNECT is fully written to the socket (or fails to be)
 */
typedef void(aws_mqtt5_disconnect_completion_fn)(int error_code, void *complete_ctx);

/**
 * Invoked by the client when there is additional payload data ready to be read by the receiver.
 */
typedef int(aws_mqtt5_payload_delivery_on_stream_data_callback_fn)(
    struct aws_mqtt5_packet_publish_view *message_view,
    struct aws_byte_cursor payload_data,
    void *user_data);

/**
 * Invoked by the client when the message payload has been completely streamed.  Message delivery is considered
 * successful and finished at this point.
 */
typedef void(aws_mqtt5_payload_delivery_on_stream_complete_callback_fn)(
    struct aws_mqtt5_packet_publish_view *message_view,
    void *user_data);

/**
 * Invoked by the client when there's an error during payload stream processing.  User should consider delivery to
 * have failed and a failing PUBACK will be sent to the server if the connection is still good.
 */
typedef void(aws_mqtt5_payload_delivery_on_stream_error_callback_fn)(
    struct aws_mqtt5_packet_publish_view *message_view,
    int error_code,
    void *user_data);

/**
 * Fundamental message delivery user callback.
 *
 * Receiver must set all delivery_options_out callback members in order to receive the payload.
 */
typedef int(aws_mqtt5_on_message_received_callback_fn)(
    struct aws_mqtt5_packet_publish_view *message_view,
    struct aws_mqtt5_publish_payload_delivery_options *delivery_options_out,
    void *user_data);

/* operation completion options structures */

/**
 * Completion callback options for the Publish operation
 */
struct aws_mqtt5_publish_completion_options {
    aws_mqtt5_publish_completion_fn *completion_callback;
    void *completion_user_data;
};

/**
 * Completion callback options for the Subscribe operation
 */
struct aws_mqtt5_subscribe_completion_options {
    aws_mqtt5_subscribe_completion_fn *completion_callback;
    void *completion_user_data;
};

/**
 * Completion callback options for the Unsubscribe operation
 */
struct aws_mqtt5_unsubscribe_completion_options {
    aws_mqtt5_unsubscribe_completion_fn *completion_callback;
    void *completion_user_data;
};

/**
 * Public completion callback options for the a DISCONNECT operation
 */
struct aws_mqtt5_disconnect_completion_options {
    aws_mqtt5_disconnect_completion_fn *completion_callback;
    void *completion_user_data;
};

/**
 * Mqtt behavior settings that are dynamically negotiated as part of the CONNECT/CONNACK exchange.
 */
struct aws_mqtt5_negotiated_settings {
    enum aws_mqtt5_qos maximum_qos;

    uint32_t session_expiry_interval;
    uint16_t receive_maximum_from_server;
    uint32_t maximum_packet_size_to_server;
    uint16_t topic_alias_maximum_to_server;
    uint16_t topic_alias_maximum_to_client;
    uint16_t server_keep_alive;

    bool retain_available;
    bool wildcard_subscriptions_available;
    bool subscription_identifiers_available;
    bool shared_subscriptions_available;
    bool rejoined_session;
};

/**
 * Details about a client lifecycle event.
 */
struct aws_mqtt5_client_lifecycle_event {

    /**
     * Type of event this is.
     */
    enum aws_mqtt5_client_lifecycle_event_type event_type;

    /**
     * Client this event corresponds to.  Necessary (can't be replaced with user data) because the client
     * doesn't exist at the time the event callback user data is configured.
     */
    struct aws_mqtt5_client *client;

    /**
     * Aws-c-* error code associated with the event
     */
    int error_code;

    /**
     * User data associated with the client's lifecycle event handler.  Set with client configuration.
     */
    void *user_data;

    /**
     * If this event was caused by receiving a CONNACK, this will be a view of that packet.
     */
    const struct aws_mqtt5_packet_connack_view *connack_data;

    /**
     * If this is a successful connection establishment, this will contain the negotiated mqtt5 behavioral settings
     */
    const struct aws_mqtt5_negotiated_settings *settings;

    /**
     * If this event was caused by receiving a DISCONNECT, this will be a view of that packet.
     */
    const struct aws_mqtt5_packet_disconnect_view *disconnect_data;
};

/*
 * Types related to incoming message delivery.
 *
 * We include the message view on the payload callbacks to let the bindings avoid having to persist the message view
 * when they want to deliver the payload in a single buffer.  This implies we need to persist the view and its backing
 * contents in the client/decoder until the message has been fully processed.
 */

/**
 * Configuration options for streaming delivery of an mqtt message payload
 */
struct aws_mqtt5_publish_payload_delivery_options {
    aws_mqtt5_payload_delivery_on_stream_data_callback_fn *on_data;
    aws_mqtt5_payload_delivery_on_stream_complete_callback_fn *on_complete;
    aws_mqtt5_payload_delivery_on_stream_error_callback_fn *on_error;
    void *user_data;
};

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
     * Controls session rejoin behavior
     */
    enum aws_mqtt5_client_session_behavior_type session_behavior;

    /**
     * Controls how the client uses mqtt5 topic aliasing when processing outbound PUBLISH packets
     */
    enum aws_mqtt5_client_outbound_topic_alias_behavior_type outbound_topic_aliasing_behavior;

    /**
     * Controls if any additional AWS-specific validation or flow control should be performed by the client.
     */
    enum aws_mqtt5_extended_validation_and_flow_control_options extended_validation_and_flow_control_options;

    /**
     * Minimum and maximum amount of time to wait to reconnect after a disconnect.
     */
    enum aws_exponential_backoff_jitter_mode retry_jitter_mode;
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
     * Time interval to wait after sending a CONNECT request for a CONNACK to arrive.  If one does not arrive, the
     * connection will be shut down.
     */
    uint32_t connack_timeout_ms;

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
    const struct aws_mqtt5_packet_disconnect_view *disconnect_options,
    const struct aws_mqtt5_disconnect_completion_options *completion_options);

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
