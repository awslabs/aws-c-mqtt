#ifndef MQTT_MOCK_SERVER_HANDLER_H
#define MQTT_MOCK_SERVER_HANDLER_H
/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include <aws/common/condition_variable.h>
#include <aws/common/mutex.h>
#include <aws/io/channel.h>
#include <aws/mqtt/private/packets.h>

static const int MOCK_LOG_SUBJECT = 60000;

struct mqtt_decoded_packet {
    enum aws_mqtt_packet_type type;

    /* CONNECT */
    bool clean_session;
    bool has_will;
    bool will_retain;
    bool has_password;
    bool has_username;
    uint16_t keep_alive_timeout;
    enum aws_mqtt_qos will_qos;
    struct aws_byte_cursor client_identifier; /* These cursors live with the received_message */
    struct aws_byte_cursor will_topic;
    struct aws_byte_cursor will_message;
    struct aws_byte_cursor username;
    struct aws_byte_cursor password;

    /* PUBLISH SUBSCRIBE UNSUBSCRIBE */
    uint16_t packet_identifier;
    struct aws_byte_cursor topic_name;         /* PUBLISH topic */
    struct aws_byte_cursor publish_payload;    /* PUBLISH payload */
    struct aws_array_list sub_topic_filters;   /* list of aws_mqtt_subscription for SUBSCRIBE */
    struct aws_array_list unsub_topic_filters; /* list of aws_byte_cursor for UNSUBSCRIBE */
};

struct mqtt_mock_server_handler {
    struct aws_channel_handler handler;
    struct aws_channel_slot *slot;
    struct aws_array_list response_messages;
    struct aws_array_list received_messages;
    size_t ping_resp_avail;
    uint16_t last_packet_id;
    size_t pubacks_received;
    size_t connacks_avail;
    struct aws_mutex lock;
    struct aws_condition_variable cvar;
    struct aws_byte_buf pending_packet;

    struct aws_array_list packets; /* contains mqtt_decoded_packet */
    size_t decoded_index;
};

struct mqtt_mock_server_publish_args {
    struct aws_channel_task task;
    struct aws_byte_cursor topic;
    struct aws_byte_cursor payload;
    enum aws_mqtt_qos qos;
    struct mqtt_mock_server_handler *testing_handler;
};

struct aws_channel_handler *new_mqtt_mock_server(struct aws_allocator *allocator);
void destroy_mqtt_mock_server(struct aws_channel_handler *handler);
void mqtt_mock_server_handler_update_slot(struct aws_channel_handler *handler, struct aws_channel_slot *slot);

/* Mock server sends a publish packet back to client */
int mqtt_mock_server_send_publish(
    struct aws_channel_handler *handler,
    struct aws_byte_cursor *topic,
    struct aws_byte_cursor *payload,
    enum aws_mqtt_qos qos);
/* Set max number of PINGRESP that mock server will send back to client */
void mqtt_mock_server_set_max_ping_resp(struct aws_channel_handler *handler, size_t max_ping);
/* Set max number of CONACK that mock server will send back to client */
void mqtt_mock_server_set_max_connack(struct aws_channel_handler *handler, size_t connack_avail);

/* Wait for puback_count PUBACK packages from client */
void mqtt_mock_server_wait_for_pubacks(struct aws_channel_handler *handler, size_t puback_count);

/* Getters for decoded packets, call mqtt_mock_server_decode_packets first. */
size_t mqtt_mock_server_decoded_packets_count(struct aws_channel_handler *handler);
struct mqtt_decoded_packet *mqtt_mock_server_get_decoded_packet(struct aws_channel_handler *handler, size_t i);
struct mqtt_decoded_packet *mqtt_mock_server_get_latest_decoded_packet(struct aws_channel_handler *handler);

/* Run all received messages through, and decode the messages. */
int mqtt_mock_server_decode_packets(struct aws_channel_handler *handler);

/* this is only safe to call when not attached to a channel. */
struct aws_array_list *mqtt_mock_server_get_received_messages(struct aws_channel_handler *handler);

#endif /* MQTT_MOCK_SERVER_HANDLER_H */
