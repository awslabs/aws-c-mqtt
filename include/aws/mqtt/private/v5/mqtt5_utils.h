#ifndef AWS_MQTT_MQTT5_UTILS_H
#define AWS_MQTT_MQTT5_UTILS_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

#include <aws/mqtt/v5/mqtt5_types.h>

struct aws_allocator;
struct aws_array_list;
struct aws_byte_buf;
struct aws_byte_cursor;
struct aws_mqtt5_user_property;
struct aws_string;

#define AWS_MQTT5_MAXIMUM_VARIABLE_LENGTH_INTEGER ((uint32_t)268435455)

struct aws_mqtt5_name_value_pair {
    struct aws_byte_buf name_value_pair;
    struct aws_byte_cursor name;
    struct aws_byte_cursor value;
};

struct aws_mqtt5_subscription {
    struct aws_string *topic;
    enum aws_mqtt5_qos qos;
    bool no_local;
    bool retain_as_published;
    enum aws_mqtt5_retain_handling_type retain_handling_type;
};

AWS_EXTERN_C_BEGIN

/*
 * Helper that resets a byte buffer to a cursor, but does nothing if both buffer and cursor are empty.
 */
int aws_byte_buf_init_conditional_from_cursor(
    struct aws_byte_buf *dest,
    struct aws_allocator *allocator,
    struct aws_byte_cursor cursor);

void aws_mqtt5_clear_user_properties_array_list(struct aws_array_list *property_list);

int aws_mqtt5_add_user_property_to_array_list(
    struct aws_allocator *allocator,
    struct aws_mqtt5_user_property *property,
    struct aws_array_list *property_list,
    enum aws_mqtt_log_subject log_subject,
    void *log_context,
    const char *log_prefix);

int aws_mqtt5_copy_user_properties_array_list(
    struct aws_allocator *allocator,
    const struct aws_array_list *source_list,
    struct aws_array_list *dest_list,
    enum aws_mqtt_log_subject log_subject,
    void *log_context,
    const char *log_prefix);

int aws_mqtt5_encode_variable_length_integer(struct aws_byte_buf *buf, uint32_t value);

int aws_mqtt5_operation_set_string_property(
    struct aws_string **property,
    struct aws_allocator *allocator,
    struct aws_byte_cursor value,
    enum aws_mqtt_log_subject log_subject,
    void *log_context,
    const char *log_prefix);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_MQTT5_UTILS_H */
