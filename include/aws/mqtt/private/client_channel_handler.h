#ifndef AWS_MQTT_PRIVATE_CLIENT_CHANNEL_HANDLER_H
#define AWS_MQTT_PRIVATE_CLIENT_CHANNEL_HANDLER_H

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

#include <aws/mqtt/mqtt.h>

#include <aws/mqtt/private/fixed_header.h>

#include <aws/common/hash_table.h>

#include <aws/io/channel.h>

#define MQTT_CALL_CALLBACK(client_ptr, callback, ...)                                                                  \
    do {                                                                                                               \
        if (client_ptr->callbacks.callback) {                                                                          \
            client_ptr->callbacks.callback(client_ptr, __VA_ARGS__, client_ptr->callbacks.user_data);                  \
        }                                                                                                              \
    } while (false)

enum aws_mqtt_client_connection_state {
    AWS_MQTT_CLIENT_STATE_CONNECTING,
    AWS_MQTT_CLIENT_STATE_CONNECTED,
    AWS_MQTT_CLIENT_STATE_DISCONNECTING,
};

/** This serves as the value of the subscriptions table */
struct aws_mqtt_subscription_impl {
    struct aws_mqtt_client_connection *connection;
    /* Public facing subscription */
    struct aws_mqtt_subscription subscription;

    const struct aws_string *filter;
    aws_mqtt_publish_recieved_fn *callback;
    void *user_data;
};

struct aws_mqtt_client_connection {

    struct aws_allocator *allocator;

    /* User callbacks */
    struct aws_mqtt_client_connection_callbacks callbacks;

    /* The state of the connection */
    enum aws_mqtt_client_connection_state state;

    /* Channel handler information */
    struct aws_channel_handler handler;
    struct aws_channel_slot *slot;

    /* Keeps track of all open subscriptions */
    struct aws_hash_table subscriptions;

    /* Connect parameters */
    struct aws_byte_buf client_id;
    bool clean_session;
    uint16_t keep_alive_time;
};

struct aws_channel_handler_vtable aws_mqtt_get_client_channel_vtable();

/* Helper for getting a message object for a packet */
struct aws_io_message *mqtt_get_message_for_packet(
    struct aws_mqtt_client_connection *connection,
    struct aws_mqtt_fixed_header *header);

#endif /* AWS_MQTT_PRIVATE_CLIENT_CHANNEL_HANDLER_H */
