#ifndef AWS_MQTT_MQTT5_CLIENT_CONFIG_H
#define AWS_MQTT_MQTT5_CLIENT_CONFIG_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

#include <aws/mqtt/v5/mqtt5_types.h>

struct aws_allocator;
struct aws_mqtt5_client_config;
struct aws_http_proxy_strategy;
struct aws_tls_connection_options;
struct aws_socket_options;
struct aws_client_bootstrap;
struct aws_byte_cursor;

/*
 * Configuration API for the mqtt5 client.
 *
 * Once a client is created, its configuration is fixed and immutable.  The same configuration can be used to
 * create multiple clients with changes between calls to _new().
 *
 * Nothing in the configuration API is thread-safe.
 */

AWS_EXTERN_C_BEGIN

/**
 * Create a new mqtt5 client configuration structure with default values.
 * Key "non-zero" defaults include:
 *    MinReconnectDelay - 1000 (milliseconds)
 *    MaxReconnectDelay - 120000 (milliseconds)
 *    ConnectedTimeToResetReconnectDelay - 30000 (milliseconds)
 *    KeepAliveInterval - 1200000 (milliseconds)
 *    PingTimeout - 3000 (milliseconds)
 *    RequestProblemInformation - true
 *
 * @param allocator allocator to use
 * @return a new mqtt5 client configuration object, or NULL
 */
AWS_MQTT_API
struct aws_mqtt5_client_config *aws_mqtt5_client_config_new(struct aws_allocator *allocator);

/**
 * Creates a new mqtt5 configuration object that is a clone of an existing one.
 *
 * @param allocator allocator to use
 * @param from existing configuration object to clone
 * @return a new configuration object mirroring the supplied configuration object, or NULL
 */
AWS_MQTT_API
struct aws_mqtt5_client_config *aws_mqtt5_client_config_new_clone(
    struct aws_allocator *allocator,
    struct aws_mqtt5_client_config *from);

/**
 * Destroys an mqtt5 client configuration object.  Currently do not see a compelling reason to ref count these.
 * @param config mqtt5 client configuration to destroy
 */
AWS_MQTT_API
void aws_mqtt5_client_config_destroy(struct aws_mqtt5_client_config *config);

/**
 * Checks if a mqtt5 client configuration is valid.  Validity does not guarantee future success, but will catch
 * some basic mistakes.
 *
 * @param config mqtt5 client configuration to check for validity
 * @return true if the configuration is valid, false otherwise
 */
AWS_MQTT_API
int aws_mqtt5_client_config_validate(struct aws_mqtt5_client_config *config);

/***************************************************************************
 * Underlying transport (pre-mqtt - tcp/websocket/tls) configuration
 *
 * host_name
 * port
 * connection bootstrap
 * socket options
 * tls options
 * http proxy options
 * websocket options
 **************************************************************************/

/**
 * Sets the endpoint address to connect the mqtt client to
 * @param config mqtt5 client configuration to update
 * @param host_name address of the endpoint to connect to
 * @return AWS_OP_SUCCESS or AWS_OP_ERR
 */
AWS_MQTT_API int aws_mqtt5_client_config_set_host_name(
    struct aws_mqtt5_client_config *config,
    struct aws_byte_cursor host_name);

/**
 * Sets the endpoint port to connect the mqtt client to
 * @param config mqtt5 client configuration to update
 * @param port port of the endpoint to connect to
 */
AWS_MQTT_API void aws_mqtt5_client_config_set_port(struct aws_mqtt5_client_config *config, uint16_t port);

/**
 * Sets the client bootstrap to use to build the client's network connection channels from
 * @param config mqtt5 client configuration to update
 * @param bootstrap client bootstrap to use
 */
AWS_MQTT_API void aws_mqtt5_client_config_set_bootstrap(
    struct aws_mqtt5_client_config *config,
    struct aws_client_bootstrap *bootstrap);

/**
 * Sets the socket options to use when this client establishes network connections
 * @param config mqtt5 client configuration to update
 * @param socket_options socket options to use when establishing network connections
 */
AWS_MQTT_API void aws_mqtt5_client_config_set_socket_options(
    struct aws_mqtt5_client_config *config,
    struct aws_socket_options *socket_options);

/**
 * Sets the tls options (relative to the endpoint) to use when this client establishes network connections
 * @param config mqtt5 client configuration to update
 * @param tls_options tls options that should be used when connecting to the remote endpoint
 * @return AWS_OP_SUCCESS or AWS_OP_ERR
 */
AWS_MQTT_API int aws_mqtt5_client_config_set_tls_connection_options(
    struct aws_mqtt5_client_config *config,
    struct aws_tls_connection_options *tls_options);

/**
 * Sets the http proxy endpoint to establish connections through.  A non-empty proxy host implies proxy usage.
 * @param config mqtt5 client configuration to update
 * @param proxy_host_name address of the http proxy to establish network connections through
 * @return AWS_OP_SUCCESS or AWS_OP_ERR
 */
AWS_MQTT_API int aws_mqtt5_client_config_set_http_proxy_host_name(
    struct aws_mqtt5_client_config *config,
    struct aws_byte_cursor proxy_host_name);

/**
 * Sets the http proxy port to establish connections through.  Only relevant if the proxy host name is non-empty.
 * @param config mqtt5 client configuration to update
 * @param port port of the http proxy to establish network connections through
 */
AWS_MQTT_API void aws_mqtt5_client_config_set_http_proxy_port(struct aws_mqtt5_client_config *config, uint16_t port);

/**
 * Sets the tls options to use when establishing a direct connection to the http proxy.
 * @param config mqtt5 client configuration to update
 * @param tls_options tls options that should be used when connecting to the proxy
 * @return AWS_OP_SUCCESS or AWS_OP_ERR
 */
AWS_MQTT_API int aws_mqtt5_client_config_set_http_proxy_tls_connection_options(
    struct aws_mqtt5_client_config *config,
    struct aws_tls_connection_options *tls_options);

/**
 * Sets the http proxy authentication strategy to use when connecting to an http proxy
 * @param config mqtt5 client configuration to update
 * @param strategy
 */
AWS_MQTT_API void aws_mqtt5_client_config_set_http_proxy_strategy(
    struct aws_mqtt5_client_config *config,
    struct aws_http_proxy_strategy *strategy);

/**
 * Sets the websocket handshake transformation function to use.  Setting this to a non-null value implies the usage
 * of mqtt-over-websockets.  Setting it to null implies a direct mqtt connection.
 * @param config mqtt5 client configuration to update
 * @param transform websocket handshake transformation function to use
 */
AWS_MQTT_API void aws_mqtt5_client_config_set_websocket_transform(
    struct aws_mqtt5_client_config *config,
    aws_mqtt5_transform_websocket_handshake_fn *transform);

/**
 * Sets the websocket handshake transformation function user data.  Only used if the transform is non-null.  This data
 * is passed to the transform function every time a websocket handshake is performed.
 * @param config mqtt5 client configuration to update
 * @param user_data websocket handshake transformation user data to use
 */
AWS_MQTT_API void aws_mqtt5_client_config_set_websocket_transform_user_data(
    struct aws_mqtt5_client_config *config,
    void *user_data);

/*
 * Reconnect configuration
 *
 * reconnect behavior
 * reconnect min/max delay
 * reconnect delay reset interval
 */

/**
 * Sets the client's reconnect behavior
 * @param config mqtt5 client configuration to update
 * @param reconnect_behavior reconnect behavior to use
 */
AWS_MQTT_API void aws_mqtt5_client_config_set_reconnect_behavior(
    struct aws_mqtt5_client_config *config,
    enum aws_mqtt5_client_reconnect_behavior_type reconnect_behavior);

/**
 * Sets the min and max possible delays for reconnect behavior.  Reconnect will do a simple exponential (doubling)
 * backoff from the min up to the max as failures accumulate.
 * @param config mqtt5 client configuration to update
 * @param min_reconnect_delay_ms minimum time, in milliseconds, between reconnect attempts
 * @param max_reconnect_delay_ms maximum time, in milliseconds, between reconnect attempts
 */
AWS_MQTT_API void aws_mqtt5_client_config_set_reconnect_delay_ms(
    struct aws_mqtt5_client_config *config,
    uint64_t min_reconnect_delay_ms,
    uint64_t max_reconnect_delay_ms);

/**
 * Sets the the length of time a new connection must stay valid before we reset the reconnect delay.  This is useful
 * to avoid fast reconnect storms when there are post-connect issues forcing rapid disconnects, like a permissions
 * problem.
 * @param config mqtt5 client configuration to update
 * @param min_connected_time_to_reset_reconnect_delay_ms minimum time, in milliseconds, a network connection must
 * be good for before the client resets its reconnection delay back to the minimum.
 */
AWS_MQTT_API void aws_mqtt5_client_config_set_reconnect_delay_reset_interval_ms(
    struct aws_mqtt5_client_config *config,
    uint64_t min_connected_time_to_reset_reconnect_delay_ms);

/*
 * Mqtt timeout options
 *
 * keep alive interval
 * ping timeout
 */

/**
 * Sets the time between mqtt keep alive PINGs
 * @param config mqtt5 client configuration to update
 * @param keep_alive_interval_ms time, in milliseconds, between PINGs sent out by the client
 */
AWS_MQTT_API void aws_mqtt5_client_config_set_keep_alive_interval_ms(
    struct aws_mqtt5_client_config *config,
    uint32_t keep_alive_interval_ms);

/**
 * Sets the time, in milliseconds, the client will wait for a PINGRESP before a ping is considered lost
 * @param config mqtt5 client configuration to update
 * @param ping_timeout_ms time interval, in milliseconds, for the client to wait for a PINGRESP to a PING
 */
AWS_MQTT_API void aws_mqtt5_client_config_set_ping_timeout_ms(
    struct aws_mqtt5_client_config *config,
    uint32_t ping_timeout_ms);

/*
 * CONNECT configuration - basic connect properties
 *
 * client id
 * username
 * password
 * session expiry interval
 * session behavior
 * authentication method
 * authentication data
 * request response information
 * request problem information
 * receive maximum
 * topic alias maximum
 * maximum packet size
 */

/**
 * Sets the mqtt client id to use when connecting.  Can be left empty to ask the server to assign a client id.  If left
 * empty, followup reconnect attempts will use the server-assigned client id rather than request a new one via an empty
 * client id.
 *
 * @param config mqtt5 client configuration to update
 * @param client_id client identifier to use when connecting
 * @return AWS_OP_SUCCESS or AWS_OP_ERR
 */
AWS_MQTT_API int aws_mqtt5_client_config_set_client_id(
    struct aws_mqtt5_client_config *config,
    struct aws_byte_cursor client_id);

/**
 * Sets the user name to use when connecting.  By default, no username is used unless this function is
 * called.  It is valid to call this function with an empty cursor.
 *
 * @param config mqtt5 client configuration to update
 * @param username user name to use when connecting
 * @return AWS_OP_SUCCESS or AWS_OP_ERR
 */
AWS_MQTT_API int aws_mqtt5_client_config_set_connect_username(
    struct aws_mqtt5_client_config *config,
    struct aws_byte_cursor username);

/**
 * Sets the password to use when connecting.  By default, no password is used unless this function is
 * called.  It is valid to call this function with an empty cursor.
 *
 * ToDo: like authentication method/data, consider if this needs to be dynamic over time (i.e. a callback)
 *
 * @param config mqtt5 client configuration to update
 * @param password password to use when connecting
 * @return AWS_OP_SUCCESS or AWS_OP_ERR
 */
AWS_MQTT_API int aws_mqtt5_client_config_set_connect_password(
    struct aws_mqtt5_client_config *config,
    struct aws_byte_cursor password);

/**
 * Sets the requested session expiry interval (in seconds).  The server is free to return a different value in the
 * CONNACK.
 *
 * @param config mqtt5 client configuration to update
 * @param session_expiry_interval_seconds session expiry interval in seconds
 */
AWS_MQTT_API void aws_mqtt5_client_config_set_connect_session_expiry_interval_seconds(
    struct aws_mqtt5_client_config *config,
    uint32_t session_expiry_interval_seconds);

/**
 * Sets the desired session behavior for the mqtt client.
 *
 * @param config mqtt5 client configuration to update
 * @param session_behavior session behavior that the client should use
 */
AWS_MQTT_API void aws_mqtt5_client_config_set_connect_session_behavior(
    struct aws_mqtt5_client_config *config,
    enum aws_mqtt5_client_session_behavior_type session_behavior);

/**
 * Sets the authentication method property for the CONNECT packets sent by the client.
 *
 * @param config mqtt5 client configuration to update
 * @param authentication_method desired authentication method
 * @return AWS_OP_SUCCESS or AWS_OP_ERR
 */
AWS_MQTT_API int aws_mqtt5_client_config_set_connect_authentication_method(
    struct aws_mqtt5_client_config *config,
    struct aws_byte_cursor authentication_method);

/**
 * Sets the authentication data property for the CONNECT packets sent by the client.
 *
 * @param config mqtt5 client configuration to update
 * @param authentication_data desired authentication data
 * @return AWS_OP_SUCCESS or AWS_OP_ERR
 *
 * ToDo: Consider making this a callback function instead, allowing dynamic auth data generation.  Perhaps even
 * make both authentication properties dynamic.  Perhaps remove these APIs completely until a decision is made.
 */
AWS_MQTT_API int aws_mqtt5_client_config_set_connect_authentication_data(
    struct aws_mqtt5_client_config *config,
    struct aws_byte_cursor authentication_data);

/**
 * Sets whether or not the client wishes to receive request-response information in the CONNACK packet.
 *
 * @param config mqtt5 client configuration to update
 * @param request_response_information whether or not the client wishes to receive CONNACK request-response information
 */
AWS_MQTT_API void aws_mqtt5_client_config_set_connect_request_response_information(
    struct aws_mqtt5_client_config *config,
    bool request_response_information);

/**
 * Sets whether or not the client wishes to receive diagnostic reason strings or user properties in appropriate packets.
 *
 * @param config mqtt5 client configuration to update
 * @param request_problem_information whether or not the client wishes to receive reason strings or user properties in
 * appropriate packets
 */
AWS_MQTT_API void aws_mqtt5_client_config_set_connect_request_problem_information(
    struct aws_mqtt5_client_config *config,
    bool request_problem_information);

/**
 * Sets the client's desired maximum number of incomplete non-qos-0 operations.  The client will not exceed the
 * negotiated value under any circumstances.
 *
 * @param config mqtt5 client configuration to update
 * @param receive_maximum maximum number of incomplete non-qos-0 operations
 */
AWS_MQTT_API void aws_mqtt5_client_config_set_connect_receive_maximum(
    struct aws_mqtt5_client_config *config,
    uint16_t receive_maximum);

/**
 * Sets the client's desired maximum number of topic aliases.
 *
 * @param config mqtt5 client configuration to update
 * @param topic_alias_maximum the client's desired maximum number of topic aliases
 */
AWS_MQTT_API void aws_mqtt5_client_config_set_connect_topic_alias_maximum(
    struct aws_mqtt5_client_config *config,
    uint16_t topic_alias_maximum);

/**
 * Sets the client's desired maximum packet size, in bytes.
 *
 * @param config mqtt5 client configuration to update
 * @param maximum_packet_size_bytes the client's desired maximum packet size, in bytes
 */
AWS_MQTT_API void aws_mqtt5_client_config_set_connect_maximum_packet_size(
    struct aws_mqtt5_client_config *config,
    uint32_t maximum_packet_size_bytes);

/**
 * Adds a user property to the set of user properties to be sent with each CONNECT packet
 *
 * @param config mqtt5 client configuration to update
 * @param property user property to include in each CONNECT packet
 * @return AWS_OP_SUCCESS or AWS_OP_ERR
 */
AWS_MQTT_API int aws_mqtt5_client_config_add_connect_user_property(
    struct aws_mqtt5_client_config *config,
    struct aws_mqtt5_user_property *property);

/*
 * CONNECT configuration - will properties
 *
 * ToDo: Consider moving this to whatever the Publish operation representation ends up being.
 * Caveat: there are some will-only properties here (delay)
 */

/**
 * Sets the basic properties of the will message to include in the CONNECT packet.  Calling this with an empty
 * topic will disable will configuration.
 *
 * @param config mqtt5 client configuration to update
 * @param topic topic that the will message should be sent to
 * @param payload bytewise payload of the will message
 * @param qos quality of service to send the will message with
 * @return AWS_OP_SUCCESS or AWS_OP_ERR
 */
AWS_MQTT_API int aws_mqtt5_client_config_set_will(
    struct aws_mqtt5_client_config *config,
    struct aws_byte_cursor topic,
    struct aws_byte_cursor payload,
    enum aws_mqtt5_qos qos);

/**
 * Sets the payload format indicator of the will message
 *
 * @param config mqtt5 client configuration to update
 * @param payload_format payload format indicator for the will message
 */
AWS_MQTT_API void aws_mqtt5_client_config_set_will_payload_format(
    struct aws_mqtt5_client_config *config,
    enum aws_mqtt5_payload_format_indicator payload_format);

/**
 * Sets the will message expiry, in seconds
 *
 * @param config mqtt5 client configuration to update
 * @param message_expiry_seconds the will message expiry, in seconds
 */
AWS_MQTT_API void aws_mqtt5_client_config_set_will_message_expiry(
    struct aws_mqtt5_client_config *config,
    uint32_t message_expiry_seconds);

/**
 * Sets the content type property of the will message
 *
 * @param config mqtt5 client configuration to update
 * @param content_type the will message content type property value
 * @return AWS_OP_SUCCESS or AWS_OP_ERR
 */
AWS_MQTT_API int aws_mqtt5_client_config_set_will_content_type(
    struct aws_mqtt5_client_config *config,
    struct aws_byte_cursor content_type);

/**
 * Sets the response topic property value for the will message.
 *
 * @param config mqtt5 client configuration to update
 * @param response_topic response topic property value
 * @return AWS_OP_SUCCESS or AWS_OP_ERR
 */
AWS_MQTT_API int aws_mqtt5_client_config_set_will_response_topic(
    struct aws_mqtt5_client_config *config,
    struct aws_byte_cursor response_topic);

/**
 * Sets the (request-response) correlation data property value for the will message.
 * @param config mqtt5 client configuration to update
 * @param correlation_data correlation data property value
 * @return AWS_OP_SUCCESS or AWS_OP_ERR
 */
AWS_MQTT_API int aws_mqtt5_client_config_set_will_correlation_data(
    struct aws_mqtt5_client_config *config,
    struct aws_byte_cursor correlation_data);

/**
 * Sets the desired delay, in seconds, before the will message send should be triggered on the server.  If the client
 * successfully reconnects before this time, the will should not be sent.
 *
 * @param config mqtt5 client configuration to update
 * @param will_delay_seconds delay, in seconds, between a client disconnect and triggering a will message send
 */
AWS_MQTT_API void aws_mqtt5_client_config_set_will_delay(
    struct aws_mqtt5_client_config *config,
    uint32_t will_delay_seconds);

/**
 * Sets whether or not the will message should be a retained message.
 *
 * @param config mqtt5 client configuration to update
 * @param retained whether or not the will message should be a retained message
 */
AWS_MQTT_API void aws_mqtt5_client_config_set_will_retained(struct aws_mqtt5_client_config *config, bool retained);

/**
 * Adds a user property to the set of user properties in the will message
 *
 * @param config mqtt5 client configuration to update
 * @param property user property to include in the will message
 * @return AWS_OP_SUCCESS or AWS_OP_ERR
 */
AWS_MQTT_API int aws_mqtt5_client_config_add_will_user_property(
    struct aws_mqtt5_client_config *config,
    struct aws_mqtt5_user_property *property);

/*
 * Lifecycle event handling configuration
 */

/**
 * Sets the lifecycle event callback that the client should invoke every time a lifecycle event occurs
 * @param config mqtt5 client configuration to update
 * @param lifecycle_event_handler lifecycle event callback that the client should use
 */
AWS_MQTT_API void aws_mqtt5_client_config_set_lifecycle_event_handler(
    struct aws_mqtt5_client_config *config,
    aws_mqtt5_client_connection_event_callback_fn *lifecycle_event_handler);

/**
 * Sets the user data to use with the client's lifecycle event callback
 * @param config mqtt5 client configuration to update
 * @param user_data user data value to include with all lifecycle event callback invocations
 */
AWS_MQTT_API void aws_mqtt5_client_config_set_lifecycle_event_handler_user_data(
    struct aws_mqtt5_client_config *config,
    void *user_data);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_MQTT5_CLIENT_CONFIG_H */
