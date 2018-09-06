#ifndef AWS_MQTT_MQTT_H
#define AWS_MQTT_MQTT_H

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

#include <aws/mqtt/exports.h>

#include <aws/common/byte_buf.h>
#include <aws/common/string.h>

/* forward declares */
struct aws_client_bootstrap;
struct aws_socket_endpoint;
struct aws_socket_options;
struct aws_tls_connection_options;

/* Represents the types of the MQTT control packets [MQTT-2.2.1]. */
enum aws_mqtt_packet_type {
    /* reserved = 0, */
    AWS_MQTT_PACKET_CONNECT = 1,
    AWS_MQTT_PACKET_CONNACK,
    AWS_MQTT_PACKET_PUBLISH,
    AWS_MQTT_PACKET_PUBACK,
    AWS_MQTT_PACKET_PUBREC,
    AWS_MQTT_PACKET_PUBREL,
    AWS_MQTT_PACKET_PUBCOMP,
    AWS_MQTT_PACKET_SUBSCRIBE,
    AWS_MQTT_PACKET_SUBACK,
    AWS_MQTT_PACKET_UNSUBSCRIBE,
    AWS_MQTT_PACKET_UNSUBACK,
    AWS_MQTT_PACKET_PINGREQ,
    AWS_MQTT_PACKET_PINGRESP,
    AWS_MQTT_PACKET_DISCONNECT,
    /* reserved = 15, */
};

/* Quality of Service associated with a publish action or subscription [MQTT-4.3]. */
enum aws_mqtt_qos {
    AWS_MQTT_QOS_AT_MOST_ONCE = 0,
    AWS_MQTT_QOS_AT_LEAST_ONCE = 1,
    AWS_MQTT_QOS_EXACTLY_ONCE = 2,
    /* reserved = 3 */
};

/* Result of a connect request [MQTT-3.2.2.3]. */
enum aws_mqtt_connect_return_code {
    AWS_MQTT_CONNECT_ACCEPTED,
    AWS_MQTT_CONNECT_UNACCEPTABLE_PROTOCOL_VERSION,
    AWS_MQTT_CONNECT_IDENTIFIER_REJECTED,
    AWS_MQTT_CONNECT_SERVER_UNAVAILABLE,
    AWS_MQTT_CONNECT_BAD_USERNAME_OR_PASSWORD,
    AWS_MQTT_CONNECT_NOT_AUTHORIZED,
    /* reserved = 6 - 255 */
};

struct aws_mqtt_subscription {
    /* Topic filte to subscribe to [MQTT-4.7]. */
    struct aws_byte_cursor topic_filter;
    /* Maximum QoS of messages to receive [MQTT-4.3]. */
    enum aws_mqtt_qos qos;
};

struct aws_mqtt_client_connection;

/** Type of function called when a publish recieved matches a subscription */
typedef void(aws_mqtt_publish_recieved_fn)(
    struct aws_mqtt_client_connection *connection,
    const struct aws_mqtt_subscription *subscription,
    struct aws_byte_cursor payload,
    void *user_data);

struct aws_mqtt_client_connection_callbacks {
    /* Called if the connection to the server is not completed.
     * Note that if a CONNACK is recieved, this function will not be called no matter what the return code is */
    void (*on_connection_failed)(struct aws_mqtt_client_connection *connection, int error_code, void *user_data);
    /* Called when a connection acknowlegement is received.
     * If return_code is not ACCEPT, the connetion is automatically closed. */
    void (*on_connack)(
        struct aws_mqtt_client_connection *connection,
        enum aws_mqtt_connect_return_code return_code,
        bool session_present,
        void *user_data);
    /* Called when a connection is closed, right before any resources are deleted. */
    void (*on_disconnect)(struct aws_mqtt_client_connection *connection, int error_code, void *user_data);

    void *user_data;
};

enum aws_mqtt_error {
    AWS_ERROR_MQTT_INVALID_RESERVED_BITS = 0x1400,
    AWS_ERROR_MQTT_BUFFER_TOO_BIG,
    AWS_ERROR_MQTT_INVALID_REMAINING_LENGTH,
    AWS_ERROR_MQTT_UNSUPPORTED_PROTOCOL_NAME,
    AWS_ERROR_MQTT_UNSUPPORTED_PROTOCOL_LEVEL,
    AWS_ERROR_MQTT_INVALID_CREDENTIALS,
    AWS_ERROR_MQTT_INVALID_QOS,
    AWS_ERROR_MQTT_INVALID_PACKET_TYPE,
    AWS_ERROR_MQTT_PROTOCOL_ERROR,

    AWS_ERROR_END_MQTT_RANGE = 0x1800,
};

struct aws_mqtt_client {
    struct aws_client_bootstrap *client_bootstrap;
    struct aws_socket_options *socket_options;
};

#ifdef __cplusplus
extern "C" {
#endif

AWS_MQTT_API
struct aws_mqtt_client_connection *aws_mqtt_client_connection_new(
    struct aws_allocator *allocator,
    struct aws_mqtt_client *client,
    struct aws_mqtt_client_connection_callbacks callbacks,
    struct aws_socket_endpoint *endpoint,
    struct aws_tls_connection_options *tls_options,
    struct aws_byte_cursor client_id,
    bool clean_session,
    uint16_t keep_alive_time);

AWS_MQTT_API
int aws_mqtt_client_connection_disconnect(struct aws_mqtt_client_connection *connection);

AWS_MQTT_API
int aws_mqtt_client_subscribe(
    struct aws_mqtt_client_connection *connection,
    const struct aws_mqtt_subscription *subscription,
    aws_mqtt_publish_recieved_fn *callback,
    void *user_data);

AWS_MQTT_API
int aws_mqtt_client_unsubscribe(
    struct aws_mqtt_client_connection *connection,
    const struct aws_byte_cursor *filter);

AWS_MQTT_API
int aws_mqtt_client_publish(
    struct aws_mqtt_client_connection *connection,
    struct aws_byte_cursor topic,
    enum aws_mqtt_qos qos,
    bool retain,
    struct aws_byte_cursor payload);

/*
 * Loads error strings for debugging and logging purposes.
 */
AWS_MQTT_API
void aws_mqtt_load_error_strings();

#ifdef __cplusplus
}
#endif

#endif /* AWS_MQTT_MQTT_H */
