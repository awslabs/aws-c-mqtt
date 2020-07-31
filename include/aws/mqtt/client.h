#ifndef AWS_MQTT_CLIENT_H
#define AWS_MQTT_CLIENT_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/hash_table.h>

#include <aws/common/byte_buf.h>
#include <aws/common/string.h>

#include <aws/io/event_loop.h>
#include <aws/io/host_resolver.h>

#include <aws/mqtt/mqtt.h>

/* forward declares */
struct aws_client_bootstrap;
struct aws_http_header;
struct aws_http_message;
struct aws_http_proxy_options;
struct aws_socket_options;
struct aws_tls_connection_options;

struct aws_mqtt_client {
    struct aws_allocator *allocator;
    struct aws_client_bootstrap *bootstrap;
};

struct aws_mqtt_client_connection;

/**
 * Callback called when a request roundtrip is complete (QoS0 immediately, QoS1 on PUBACK, QoS2 on PUBCOMP). Either
 * succeed or not
 */
typedef void(aws_mqtt_op_complete_fn)(
    struct aws_mqtt_client_connection *connection,
    uint16_t packet_id,
    int error_code,
    void *userdata);

/**
 * Called when a connection attempt is completed, either in success or error.
 *
 * If error code is AWS_ERROR_SUCCESS, then a CONNACK has been received from the server and return_code and
 * session_present contain the values received. If error_code is not AWS_ERROR_SUCCESS, it refers to the internal error
 * that occurred during connection, and return_code and session_present are invalid.
 */
typedef void(aws_mqtt_client_on_connection_complete_fn)(
    struct aws_mqtt_client_connection *connection,
    int error_code,
    enum aws_mqtt_connect_return_code return_code,
    bool session_present,
    void *userdata);

/* Called if the connection to the server is lost. */
typedef void(aws_mqtt_client_on_connection_interrupted_fn)(
    struct aws_mqtt_client_connection *connection,
    int error_code,
    void *userdata);

/**
 * Called when a connection to the server is resumed
 * (if clean_session is true, calling aws_mqtt_resubscribe_existing_topics is suggested)
 */
typedef void(aws_mqtt_client_on_connection_resumed_fn)(
    struct aws_mqtt_client_connection *connection,
    enum aws_mqtt_connect_return_code return_code,
    bool session_present,
    void *userdata);

/** Called when a multi-topic subscription request is complete */
typedef void(aws_mqtt_suback_multi_fn)(
    struct aws_mqtt_client_connection *connection,
    uint16_t packet_id,
    const struct aws_array_list *topic_subacks, /* contains aws_mqtt_topic_subscription pointers */
    int error_code,
    void *userdata);

/** Called when a single-topic subscription request is complete */
typedef void(aws_mqtt_suback_fn)(
    struct aws_mqtt_client_connection *connection,
    uint16_t packet_id,
    const struct aws_byte_cursor *topic,
    enum aws_mqtt_qos qos,
    int error_code,
    void *userdata);

/** Type of function called when a publish received matches a subscription (client specific) */
typedef void(aws_mqtt_client_publish_received_fn)(
    struct aws_mqtt_client_connection *connection,
    const struct aws_byte_cursor *topic,
    const struct aws_byte_cursor *payload,
    void *userdata);

/** Called when a connection is closed, right before any resources are deleted */
typedef void(aws_mqtt_client_on_disconnect_fn)(struct aws_mqtt_client_connection *connection, void *userdata);

/**
 * Function to invoke when the websocket handshake request transformation completes.
 * This function MUST be invoked or the application will soft-lock.
 *
 * `request` and `complete_ctx` must be the same pointers provided to the `aws_mqtt_transform_websocket_handshake_fn`.
 * `error_code` should should be AWS_ERROR_SUCCESS if transformation was successful,
 * otherwise pass a different AWS_ERROR_X value.
 */
typedef void(aws_mqtt_transform_websocket_handshake_complete_fn)(
    struct aws_http_message *request,
    int error_code,
    void *complete_ctx);

/**
 * Function that may transform the websocket handshake request.
 * Called each time a websocket connection is attempted.
 *
 * The default request uses path "/mqtt". All required headers are present,
 * plus the optional header "Sec-WebSocket-Protocol: mqtt".
 *
 * The user MUST invoke the `complete_fn` when transformation is complete or the application will soft-lock.
 * When invoking the `complete_fn`, pass along the `request` and `complete_ctx` provided here and an error code.
 * The error code should be AWS_ERROR_SUCCESS if transformation was successful,
 * otherwise pass a different AWS_ERROR_X value.
 */
typedef void(aws_mqtt_transform_websocket_handshake_fn)(
    struct aws_http_message *request,
    void *user_data,
    aws_mqtt_transform_websocket_handshake_complete_fn *complete_fn,
    void *complete_ctx);

/**
 * Function that may accept or reject a websocket handshake response.
 * Called each time a valid websocket connection is established.
 *
 * All required headers have been checked already (ex: "Sec-Websocket-Accept"),
 * but optional headers have not (Ex: "Sec-Websocket-Protocol").
 *
 * Return AWS_OP_SUCCESS to accept the connection or AWS_OP_ERR to stop the connection attempt.
 */
typedef int aws_mqtt_validate_websocket_handshake_fn(
    struct aws_mqtt_client_connection *connection,
    const struct aws_http_header *header_array,
    size_t num_headers,
    void *userdata);

/** Passed to subscribe() and suback callbacks */
struct aws_mqtt_topic_subscription {
    struct aws_byte_cursor topic;
    enum aws_mqtt_qos qos;

    aws_mqtt_client_publish_received_fn *on_publish;
    aws_mqtt_userdata_cleanup_fn *on_cleanup;
    void *on_publish_ud;
};

/**
 * host_name                 The server name to connect to. This resource may be freed immediately on return.
 * port                      The port on the server to connect to
 * client_id                 The clientid to place in the CONNECT packet.
 * socket_options            The socket options to pass to the aws_client_bootstrap functions.
 *                           This is copied into the connection
 * tls_options               TLS settings to use when opening a connection.
 *                           This is copied into the connection
 *                           Pass NULL to connect without TLS (NOT RECOMMENDED)
 * clean_session             True to discard all server session data and start fresh
 * keep_alive_time_secs      The keep alive value to place in the CONNECT PACKET, a PING will automatically
 *                           be sent at this interval as well. If you specify 0, defaults will be used
 *                           and a ping will be sent once per 20 minutes.
 *                           This duration must be longer than ping_timeout_ms.
 * ping_timeout_ms           Network connection is re-established if a ping response is not received
 *                           within this amount of time (milliseconds). If you specify 0, a default value of 3 seconds
 *                           is used. Alternatively, tcp keep-alive may be away to accomplish this in a more efficient
 *                           (low-power) scenario, but keep-alive options may not work the same way on every platform
 *                           and OS version. This duration must be shorter than keep_alive_time_secs. It's also for
 *                           reattempt other requests.
 * TODO: The documentation is not clear. it's probably more clear to be named as request_timeout_ms, since we are using
 * this for all the requests that need a response.
 * Note: For CONNECT, if the response is not received within this amount of time, the connection will shutdown, and
 * reconnect will not happen automatically. For PINGREQ, the connection will shutdown and try to reconnect
 * automactically. For other requests, they will be resent in this case.
 *
 * on_connection_complete    The callback to fire when the connection attempt completes user_data
 *                           Passed to the userdata param of on_connection_complete
 */
struct aws_mqtt_connection_options {
    struct aws_byte_cursor host_name;
    uint16_t port;
    struct aws_socket_options *socket_options;
    struct aws_tls_connection_options *tls_options;
    struct aws_byte_cursor client_id;
    uint16_t keep_alive_time_secs;
    uint32_t ping_timeout_ms;
    aws_mqtt_client_on_connection_complete_fn *on_connection_complete;
    void *user_data;
    bool clean_session;
};

AWS_EXTERN_C_BEGIN

/**
 * Initializes an instance of aws_mqtt_client.
 * It is expected that the user will manage the client's object lifetime, this function will not allocate one.
 *
 * \param[in] client    The client to initialize
 * \param[in] allocator The allocator the client will use for all future allocations
 * \param[in] bootstrap The client bootstrap to use to initiate new socket connections
 *
 * \returns AWS_OP_SUCCESS if successfully initialized, otherwise AWS_OP_ERR and aws_last_error() will be set.
 */
AWS_MQTT_API
int aws_mqtt_client_init(
    struct aws_mqtt_client *client,
    struct aws_allocator *allocator,
    struct aws_client_bootstrap *bootstrap);

/**
 * Cleans up and frees all memory allocated by the client.
 *
 * Note that calling this function before all connections are closed is undefined behavior.
 *
 * \param[in] client    The client to shut down
 */
AWS_MQTT_API
void aws_mqtt_client_clean_up(struct aws_mqtt_client *client);

/**
 * Spawns a new connection object.
 *
 * \param[in] client    The client to spawn the connection from
 *
 * \returns AWS_OP_SUCCESS on success, otherwise AWS_OP_ERR and aws_last_error() is set.
 */
AWS_MQTT_API
struct aws_mqtt_client_connection *aws_mqtt_client_connection_new(struct aws_mqtt_client *client);

/**
 * Cleans up and destroys a connection object. Only safe to call when the connection get disconnected, or never
 * connected.
 *
 * \param[in] connection    The connection object
 */
AWS_MQTT_API
void aws_mqtt_client_connection_destroy(struct aws_mqtt_client_connection *connection);

/**
 * Sets the will message to send with the CONNECT packet.
 *
 * \param[in] connection    The connection object
 * \param[in] topic         The topic to publish the will on
 * \param[in] qos           The QoS to publish the will with
 * \param[in] retain        The retain flag to publish the will with
 * \param[in] payload       The data if the will message
 */
AWS_MQTT_API
int aws_mqtt_client_connection_set_will(
    struct aws_mqtt_client_connection *connection,
    const struct aws_byte_cursor *topic,
    enum aws_mqtt_qos qos,
    bool retain,
    const struct aws_byte_cursor *payload);

/**
 * Sets the username and/or password to send with the CONNECT packet.
 *
 * \param[in] connection    The connection object
 * \param[in] username      The username to connect with
 * \param[in] password      [optional] The password to connect with
 */
AWS_MQTT_API
int aws_mqtt_client_connection_set_login(
    struct aws_mqtt_client_connection *connection,
    const struct aws_byte_cursor *username,
    const struct aws_byte_cursor *password);

/**
 * Use MQTT over websockets when connecting.
 * Requires the MQTT_WITH_WEBSOCKETS build option.
 *
 * In this scenario, an HTTP connection is established, which is then upgraded to a websocket connection,
 * which is then used to send MQTT data.
 *
 * \param[in] connection        The connection object.
 * \param[in] transformer       [optional] Function that may transform the websocket handshake request.
 *                              See `aws_mqtt_transform_websocket_handshake_fn` for more info.
 * \param[in] transformer_ud    [optional] Userdata for request_transformer.
 * \param[in] validator         [optional] Function that may reject the websocket handshake response.
 * \param[in] validator_ud      [optional] Userdata for response_validator.
 */
AWS_MQTT_API
int aws_mqtt_client_connection_use_websockets(
    struct aws_mqtt_client_connection *connection,
    aws_mqtt_transform_websocket_handshake_fn *transformer,
    void *transformer_ud,
    aws_mqtt_validate_websocket_handshake_fn *validator,
    void *validator_ud);

/**
 * Set proxy options for the websocket connection.
 */
AWS_MQTT_API
int aws_mqtt_client_connection_set_websocket_proxy_options(
    struct aws_mqtt_client_connection *connection,
    struct aws_http_proxy_options *proxy_options);

/**
 * Sets the minimum and maximum reconnect timeouts.
 *
 * The time between reconnect attempts will start at min and multiply by 2 until max is reached.
 *
 * \param[in] connection    The connection object
 * \param[in] min_timeout   The timeout to start with
 * \param[in] max_timeout   The highest allowable wait time between reconnect attempts
 */
AWS_MQTT_API
int aws_mqtt_client_connection_set_reconnect_timeout(
    struct aws_mqtt_client_connection *connection,
    uint64_t min_timeout,
    uint64_t max_timeout);

/**
 * Sets the callbacks to call when a connection is interrupted and resumed.
 *
 * \param[in] connection        The connection object
 * \param[in] on_interrupted    The function to call when a connection is lost
 * \param[in] on_interrupted_ud Userdata for on_interrupted
 * \param[in] on_resumed        The function to call when a connection is resumed
                                (if clean_session is true, calling aws_mqtt_resubscribe_existing_topics is suggested)
 * \param[in] on_resumed_ud     Userdata for on_resumed
 */
AWS_MQTT_API
int aws_mqtt_client_connection_set_connection_interruption_handlers(
    struct aws_mqtt_client_connection *connection,
    aws_mqtt_client_on_connection_interrupted_fn *on_interrupted,
    void *on_interrupted_ud,
    aws_mqtt_client_on_connection_resumed_fn *on_resumed,
    void *on_resumed_ud);

/**
 * Sets the callback to call whenever ANY publish packet is received.
 *
 * \param[in] connection        The connection object
 * \param[in] on_any_publish    The function to call when a publish is received (pass NULL to unset)
 * \param[in] on_any_publish_ud Userdata for on_any_publish
 */
AWS_MQTT_API
int aws_mqtt_client_connection_set_on_any_publish_handler(
    struct aws_mqtt_client_connection *connection,
    aws_mqtt_client_publish_received_fn *on_any_publish,
    void *on_any_publish_ud);

/**
 * Opens the actual connection defined by aws_mqtt_client_connection_new.
 * Once the connection is opened, on_connack will be called. Only called when connection is disconnected.
 *
 * \param[in] connection                The connection object
 * \returns AWS_OP_SUCCESS if the connection has been successfully initiated,
 *              otherwise AWS_OP_ERR and aws_last_error() will be set.
 */
AWS_MQTT_API
int aws_mqtt_client_connection_connect(
    struct aws_mqtt_client_connection *connection,
    const struct aws_mqtt_connection_options *connection_options);

/**
 * DEPRECATED
 * Opens the actual connection defined by aws_mqtt_client_connection_new.
 * Once the connection is opened, on_connack will be called.
 *
 * Must be called on a connection that has previously been open,
 * as the parameters passed during the last connection will be reused.
 *
 * \param[in] connection                The connection object
 * \param[in] on_connection_complete    The callback to fire when the connection attempt completes
 * \param[in] userdata                  Passed to the userdata param of on_connection_complete
 *
 * \returns AWS_OP_SUCCESS if the connection has been successfully initiated,
 *              otherwise AWS_OP_ERR and aws_last_error() will be set.
 */
int aws_mqtt_client_connection_reconnect(
    struct aws_mqtt_client_connection *connection,
    aws_mqtt_client_on_connection_complete_fn *on_connection_complete,
    void *userdata);

/**
 * Closes the connection asynchronously, calls the on_disconnect callback.
 * All uncompleted requests (publish/subscribe/unsubscribe) will be cancelled, regardless to the status of
 * clean_session. DISCONNECT packet will be sent, which will delete the will message from server.
 *
 * \param[in] connection    The connection to close
 *
 * \returns AWS_OP_SUCCESS if the connection is open and is being shutdown,
 *              otherwise AWS_OP_ERR and aws_last_error() is set.
 */
AWS_MQTT_API
int aws_mqtt_client_connection_disconnect(
    struct aws_mqtt_client_connection *connection,
    aws_mqtt_client_on_disconnect_fn *on_disconnect,
    void *userdata);

/**
 * Subscribe to topic filters. on_publish will be called when a PUBLISH matching each topic_filter is received.
 *
 * \param[in] connection        The connection to subscribe on
 * \param[in] topic_filters     An array_list of aws_mqtt_topic_subscription (NOT pointers) describing the requests.
 * \param[in] on_suback         Called when a SUBACK has been received from the server and the subscription is complete
 * \param[in] on_suback_ud      Passed to on_suback
 *
 * \returns The packet id of the subscribe packet if successfully sent, otherwise 0.
 */
AWS_MQTT_API
uint16_t aws_mqtt_client_connection_subscribe_multiple(
    struct aws_mqtt_client_connection *connection,
    const struct aws_array_list *topic_filters,
    aws_mqtt_suback_multi_fn *on_suback,
    void *on_suback_ud);

/**
 * Subscribe to a single topic filter. on_publish will be called when a PUBLISH matching topic_filter is received.
 *
 * \param[in] connection    The connection to subscribe on
 * \param[in] topic_filter  The topic filter to subscribe on.  This resource must persist until on_suback.
 * \param[in] qos           The maximum QoS of messages to receive
 * \param[in] on_publish    Called when a PUBLISH packet matching topic_filter is received
 * \param[in] on_publish_ud Passed to on_publish
 * \param[in] on_ud_cleanup Called when a subscription is removed, on_publish_ud is passed.
 * \param[in] on_suback     Called when a SUBACK has been received from the server and the subscription is complete
 * \param[in] on_suback_ud  Passed to on_suback
 *
 * \returns The packet id of the subscribe packet if successfully sent, otherwise 0.
 */
AWS_MQTT_API
uint16_t aws_mqtt_client_connection_subscribe(
    struct aws_mqtt_client_connection *connection,
    const struct aws_byte_cursor *topic_filter,
    enum aws_mqtt_qos qos,
    aws_mqtt_client_publish_received_fn *on_publish,
    void *on_publish_ud,
    aws_mqtt_userdata_cleanup_fn *on_ud_cleanup,
    aws_mqtt_suback_fn *on_suback,
    void *on_suback_ud);

/**
 * Subscribe to a single topic filter WITHOUT sending a SUBSCRIBE packet.
 * This is useful if you expect the broker to send PUBLISHES without first subscribing.
 * on_publish will be called when a PUBLISH matching topic_filter is received.
 *
 * \param[in] connection    The connection to subscribe on
 * \param[in] topic_filter  The topic filter to subscribe on.  This resource must persist until on_suback.
 * \param[in] on_publish    Called when a PUBLISH packet matching topic_filter is received
 * \param[in] on_publish_ud Passed to on_publish
 * \param[in] on_ud_cleanup Called when a subscription is removed, on_publish_ud is passed.
 * \param[in] on_suback     Called when a SUBACK has been received from the server and the subscription is complete
 * \param[in] on_suback_ud  Passed to on_suback
 *
 * \returns The "packet id" of the operation if successfully initiated, otherwise 0.
 */
AWS_MQTT_API
uint16_t aws_mqtt_client_connection_subscribe_local(
    struct aws_mqtt_client_connection *connection,
    const struct aws_byte_cursor *topic_filter,
    aws_mqtt_client_publish_received_fn *on_publish,
    void *on_publish_ud,
    aws_mqtt_userdata_cleanup_fn *on_ud_cleanup,
    aws_mqtt_suback_fn *on_suback,
    void *on_suback_ud);

/**
 * Resubscribe to all topics currently subscribed to. This is to help when resuming a connection with a clean session.
 *
 * \param[in] connection    The connection to subscribe on
 * \param[in] on_suback     Called when a SUBACK has been received from the server and the subscription is complete
 * \param[in] on_suback_ud  Passed to on_suback
 *
 * \returns The packet id of the subscribe packet if successfully sent, otherwise 0 (and aws_last_error() will be set).
 */
AWS_MQTT_API
uint16_t aws_mqtt_resubscribe_existing_topics(
    struct aws_mqtt_client_connection *connection,
    aws_mqtt_suback_multi_fn *on_suback,
    void *on_suback_ud);

/**
 * Unsubscribe to a topic filter.
 *
 * \param[in] connection        The connection to unsubscribe on
 * \param[in] topic_filter      The topic filter to unsubscribe on. This resource must persist until on_unsuback.
 * \param[in] on_unsuback       Called when a UNSUBACK has been received from the server and the subscription is removed
 * \param[in] on_unsuback_ud    Passed to on_unsuback
 *
 * \returns The packet id of the unsubscribe packet if successfully sent, otherwise 0.
 */
AWS_MQTT_API
uint16_t aws_mqtt_client_connection_unsubscribe(
    struct aws_mqtt_client_connection *connection,
    const struct aws_byte_cursor *topic_filter,
    aws_mqtt_op_complete_fn *on_unsuback,
    void *on_unsuback_ud);

/**
 * Send a PUBLISH packet over connection.
 *
 * \param[in] connection    The connection to publish on
 * \param[in] topic         The topic to publish on
 * \param[in] qos           The requested QoS of the packet
 * \param[in] retain        True to have the server save the packet, and send to all new subscriptions matching topic
 * \param[in] payload       The data to send as the payload of the publish
 * \param[in] on_complete   For QoS 0, called as soon as the packet is sent
 *                          For QoS 1, called when PUBACK is received
 *                          For QoS 2, called when PUBCOMP is received
 *
 * \returns The packet id of the publish packet if successfully sent, otherwise 0.
 */
AWS_MQTT_API
uint16_t aws_mqtt_client_connection_publish(
    struct aws_mqtt_client_connection *connection,
    const struct aws_byte_cursor *topic,
    enum aws_mqtt_qos qos,
    bool retain,
    const struct aws_byte_cursor *payload,
    aws_mqtt_op_complete_fn *on_complete,
    void *userdata);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_CLIENT_H */
