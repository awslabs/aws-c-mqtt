#ifndef AWS_MQTT_MQTT5_OPERATION_H
#define AWS_MQTT_MQTT5_OPERATION_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

#include <aws/common/linked_list.h>
#include <aws/common/ref_count.h>
#include <aws/mqtt/v5/mqtt5_types.h>

struct aws_mqtt5_operation;

/* ToDo: this is almost certainly the wrong value to use as a default */
#define AWS_MQTT5_DEFAULT_SESSION_EXPIRY_INTERVAL_SECONDS 0
#define AWS_MQTT5_DEFAULT_KEEP_ALIVE_INTERVAL_SECONDS 1200

/*
 * Some data binding rules of thumb:
 *
 * name-value pair - pair of cursors + aws_byte_buf
 * Required binary data - aws_byte_buf
 * Optional binary data - aws_byte_buf and aws_byte_buf *
 * UTF-8 String - aws_string * (NULL implies not present)
 * Required variable length integer - uint32_t
 * Optional variable length integer - uint32_t, uint32_t *
 * Require fixed-size integer = uint.._t
 * Optional fixed_size integer = uint.._t, uint.._t *
 *
 * Optional enum values may use the (value, value *) pattern but some enums have a value that indicates no-value.
 */

/*
 * This is intentionally different and *not* in sync with aws_mqtt5_packet_type
 *
 * Current thinking is that user-facing packets are operations, but we reserve the right to make additional
 * operations that may represent compound (sub-pub-unsub for example) or external concepts
 *
 * We must be careful to make the functional interface for operations not assume a 1-1 with MQTT packets
 */
enum aws_mqtt5_operation_type {
    AWS_MOT_CONNECT,
    AWS_MOT_DISCONNECT,
    AWS_MOT_SUBSCRIBE,
    AWS_MOT_UNSUBSCRIBE,
    AWS_MOT_PUBLISH,
};

typedef size_t (*aws_mqtt5_operation_get_encoded_size_fn)(struct aws_mqtt5_operation *operation);

/*
 * TODO: See above note about not making this assume 1-1 with MQTT packets
 */
struct aws_mqtt5_operation_vtable {
    aws_mqtt5_operation_get_encoded_size_fn get_encoded_size;
};

/**
 * This is the base structure for all mqtt operations.  It includes the type, a ref count, vtable and list
 * management.
 */
struct aws_mqtt5_operation {
    enum aws_mqtt5_operation_type operation_type;
    aws_mqtt5_event_id_t id;
    struct aws_ref_count ref_count;
    struct aws_linked_list_node node;
    struct aws_mqtt5_operation_vtable *vtable;
    void *impl;
};

struct aws_mqtt5_operation_connect {
    struct aws_mqtt5_operation base;
    struct aws_allocator *allocator;

    uint32_t keep_alive_interval_seconds;

    struct aws_string *client_id;

    struct aws_string *username;

    struct aws_byte_buf password;
    struct aws_byte_buf *password_ptr;

    uint32_t session_expiry_interval_seconds;
    enum aws_mqtt5_client_session_behavior_type session_behavior;

    bool request_response_information;
    bool request_problem_information;
    uint16_t receive_maximum;
    uint16_t topic_alias_maximum;
    uint32_t maximum_packet_size_bytes;

    struct aws_array_list user_properties;

    struct aws_mqtt5_operation_publish *will;
    uint32_t will_delay_interval_seconds;
    uint32_t *will_delay_interval_seconds_ptr;
};

struct aws_mqtt5_operation_publish {
    struct aws_mqtt5_operation base;
    struct aws_allocator *allocator;

    struct aws_byte_buf payload; /* possibly an input stream in the future */

    enum aws_mqtt5_qos qos;
    bool retain;
    struct aws_string *topic;
    enum aws_mqtt5_payload_format_indicator payload_format;
    uint32_t message_expiry_interval_seconds;
    uint32_t *message_expiry_interval_seconds_ptr;
    uint16_t topic_alias;
    uint16_t *topic_alias_ptr;
    struct aws_string *response_topic;
    struct aws_byte_buf correlation_data;
    struct aws_byte_buf *correlation_data_ptr;
    uint32_t subscription_identifier;
    uint32_t *subscription_identifier_ptr;
    struct aws_string *content_type;
    struct aws_array_list user_properties;
};

struct aws_mqtt5_operation_disconnect {
    struct aws_mqtt5_operation base;
    struct aws_allocator *allocator;

    enum aws_mqtt5_disconnect_reason_code reason_code;
    uint32_t session_expiry_interval_seconds;
    uint32_t *session_expiry_interval_seconds_ptr;
    struct aws_string *reason_string;
    struct aws_array_list user_properties;
};

struct aws_mqtt5_operation_subscribe {
    struct aws_mqtt5_operation base;
    struct aws_allocator *allocator;
    uint32_t subscription_identifier;
    uint32_t *subscription_identifier_ptr;
    struct aws_array_list user_properties;
    struct aws_array_list subscriptions;
};

struct aws_mqtt5_operation_unsubscribe {
    struct aws_mqtt5_operation base;
    struct aws_allocator *allocator;
    struct aws_array_list user_properties;
    struct aws_array_list unsubscribe_topics;
};

AWS_EXTERN_C_BEGIN

AWS_MQTT_API struct aws_mqtt5_operation *aws_mqtt5_operation_acquire(struct aws_mqtt5_operation *operation);

AWS_MQTT_API struct aws_mqtt5_operation *aws_mqtt5_operation_release(struct aws_mqtt5_operation *operation);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_MQTT5_OPERATION_H */
