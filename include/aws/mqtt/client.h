#ifndef AWS_MQTT_CLIENT_H
#define AWS_MQTT_CLIENT_H

/*
 * Copyright 2010-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

#include <aws/common/hash_table.h>

#include <aws/common/byte_buf.h>
#include <aws/common/string.h>

#include <aws/io/event_loop.h>
#include <aws/io/host_resolver.h>

#include <aws/mqtt/mqtt.h>

/* forward declares */
struct aws_client_bootstrap;
struct aws_socket_options;
struct aws_tls_connection_options;

struct aws_mqtt_client {
    struct aws_allocator *allocator;
    struct aws_client_bootstrap *bootstrap;
};

struct aws_mqtt_client_connection;

/** Callback called when a request roundtrip is complete (QoS0 immediately, QoS1 on PUBACK, QoS2 on PUBCOMP). */
typedef void(aws_mqtt_op_complete_fn)(
    struct aws_mqtt_client_connection *connection,
    uint16_t packet_id,
    int error_code,
    void *userdata);

/**
 * Called when a connection attempt is completed, either in success or error.
 *
 * If error code is AWS_OP_SUCCESS, then a CONNACK has been received from the server and return_code and session_present
 *      contain the values received.
 * If error_code is not AWS_OP_SUCCESS, it refers to the internal error that occured during connection, and return_code
 *      and session_present are invalid.
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

/* Called when a connection to the server is resumed. */
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

/* Called when a connection is closed, right before any resources are deleted */
typedef void(aws_mqtt_client_on_disconnect_fn)(struct aws_mqtt_client_connection *connection, void *userdata);

/** Passed to subscribe() and suback callbacks */
struct aws_mqtt_topic_subscription {
    struct aws_byte_cursor topic;
    enum aws_mqtt_qos qos;

    aws_mqtt_client_publish_received_fn *on_publish;
    aws_mqtt_userdata_cleanup_fn *on_cleanup;
    void *on_publish_ud;
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
 * \param[in] client            The client to spawn the connection from
 *
 * \returns AWS_OP_SUCCESS on success, otherwise AWS_OP_ERR and aws_last_error() is set.
 */
AWS_MQTT_API
struct aws_mqtt_client_connection *aws_mqtt_client_connection_new(struct aws_mqtt_client *client);

/**
 * Cleans up and destroys a connection object.
 */
AWS_MQTT_API
void aws_mqtt_client_connection_destroy(struct aws_mqtt_client_connection *connection);

/**
 * Sets the will message to send with the CONNECT packet.
 *
 * \param[in] connection    The connection object
 * \param[in] topic         The topic to publish the will on
 * \param[in] qos           The QoS to publish the will with
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
 * Sets the minimum and maximum reconnect timeouts.
 *
 * The time between reconnect attempts will start at min and multipy by 2 until max is reached.
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
 * \param[in] on_resumed    The function to call when a connection is resumed
 * \param[in] on_resumed_ud Userdata for on_resumed
 */
AWS_MQTT_API
int aws_mqtt_client_connection_set_connection_interruption_handlers(
    struct aws_mqtt_client_connection *connection,
    aws_mqtt_client_on_connection_interrupted_fn *on_interrupted,
    void *on_interrupted_ud,
    aws_mqtt_client_on_connection_resumed_fn *on_resumed,
    void *on_resumed_ud);

/**
 * Opens the actual connection defined by aws_mqtt_client_connection_new.
 * Once the connection is opened, on_connack will be called.
 *
 * \param[in] connection                The connection object
 * \param[in] host_name                 The server name to connect to. This resource may be freed immediately on return.
 * \param[in] port                      The port on the server to connect to
 * \param[in] client_id                 The clientid to place in the CONNECT packet.
 * \param[in] socket_options            The socket options to pass to the aws_client_bootstrap functions
 *                                          This is copied into the connection
 * \param[in] tls_options               TLS settings to use when opening a connection.
 *                                          This is copied into the connection
 *                                          Pass NULL to connect without TLS (NOT RECOMMENDED)
 * \param[in] clean_session             True to discard all server session data and start fresh
 * \param[in] keep_alive_time           The keep alive value to place in the CONNECT PACKET
 * \param[in] on_connection_complete    The callback to fire when the connection attempt completes
 * \param[in] userdata                  Passed to the userdata param of on_connection_complete
 *
 * \returns AWS_OP_SUCCESS if the connection has been successfully initiated,
 *              otherwise AWS_OP_ERR and aws_last_error() will be set.
 */
AWS_MQTT_API
int aws_mqtt_client_connection_connect(
    struct aws_mqtt_client_connection *connection,
    const struct aws_byte_cursor *host_name,
    uint16_t port,
    const struct aws_socket_options *socket_options,
    const struct aws_tls_connection_options *tls_options,
    const struct aws_byte_cursor *client_id,
    bool clean_session,
    uint16_t keep_alive_time,
    aws_mqtt_client_on_connection_complete_fn *on_connection_complete,
    void *userdata);

/**
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
AWS_MQTT_API
int aws_mqtt_client_connection_reconnect(
    struct aws_mqtt_client_connection *connection,
    aws_mqtt_client_on_connection_complete_fn *on_connection_complete,
    void *userdata);

/**
 * Closes the connection asyncronously, calls the on_disconnect callback, and destroys the connection object.
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
 * \param[in] connection    The connection to subscribe on
 * \param[in] topics        An array_list of aws_mqtt_topic_subscriptions (NOT pointers) describing the requests.
 * \param[in] on_suback     Called when a SUBACK has been received from the server and the subscription is complete
 * \param[in] on_suback_ud  Passed to on_suback
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
 * \param[in] qos           The maximum QoS of messages to recieve
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
 * Send a PUBLSIH packet over connection.
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

/**
 * Sends a PINGREQ packet to the server to keep the connection alive.
 * If a PINGRESP is not received within a reasonable period of time, the connection will be closed.
 *
 * \params[in] connection   The connection to ping on
 *
 * \returns AWS_OP_SUCCESS if the connection is open and the PINGREQ is sent or queued to send,
 *              otherwise AWS_OP_ERR and aws_last_error() is set.
 */
AWS_MQTT_API
int aws_mqtt_client_connection_ping(struct aws_mqtt_client_connection *connection);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_CLIENT_H */
