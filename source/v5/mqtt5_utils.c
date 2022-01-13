#include <aws/mqtt/private/v5/mqtt5_utils.h>

#include <aws/common/byte_buf.h>

#include <aws/common/string.h>
#include <aws/mqtt/v5/mqtt5_types.h>

int aws_byte_buf_init_conditional_from_cursor(
    struct aws_byte_buf *dest,
    struct aws_allocator *allocator,
    struct aws_byte_cursor cursor) {
    if (cursor.len > 0 || dest->len > 0) {
        aws_byte_buf_clean_up(dest);
        return aws_byte_buf_init_copy_from_cursor(dest, allocator, cursor);
    }

    return AWS_OP_SUCCESS;
}

void aws_mqtt5_clear_user_properties_array_list(struct aws_array_list *property_list) {
    if (property_list == NULL) {
        return;
    }

    size_t property_count = aws_array_list_length(property_list);
    for (size_t i = 0; i < property_count; ++i) {
        struct aws_mqtt5_name_value_pair *nv_pair = NULL;
        if (aws_array_list_get_at_ptr(property_list, (void **)&nv_pair, i)) {
            continue;
        }

        aws_byte_buf_clean_up(&nv_pair->name_value_pair);
    }

    aws_array_list_clear(property_list);
}

int aws_mqtt5_add_user_property_to_array_list(
    struct aws_allocator *allocator,
    struct aws_mqtt5_user_property *property,
    struct aws_array_list *property_list,
    enum aws_mqtt_log_subject log_subject,
    void *log_context,
    const char *log_prefix) {
    struct aws_mqtt5_name_value_pair nv_pair;
    AWS_ZERO_STRUCT(nv_pair);

    if (aws_byte_buf_init(&nv_pair.name_value_pair, allocator, property->name.len + property->value.len)) {
        goto on_error;
    }

    nv_pair.name = property->name;
    nv_pair.value = property->value;

    if (aws_byte_buf_append_and_update(&nv_pair.name_value_pair, &nv_pair.name)) {
        goto on_error;
    }

    if (aws_byte_buf_append_and_update(&nv_pair.name_value_pair, &nv_pair.value)) {
        goto on_error;
    }

    if (aws_array_list_push_back(property_list, &nv_pair)) {
        goto on_error;
    }

    AWS_LOGF_DEBUG(
        log_subject,
        "(%p) %s added user property with name: \"" PRInSTR "\", value: \"" PRInSTR "\"",
        log_context,
        log_prefix,
        AWS_BYTE_CURSOR_PRI(property->name),
        AWS_BYTE_CURSOR_PRI(property->value));

    return AWS_OP_SUCCESS;

on_error:

    aws_byte_buf_clean_up(&nv_pair.name_value_pair);

    return AWS_OP_ERR;
}

int aws_mqtt5_copy_user_properties_array_list(
    struct aws_allocator *allocator,
    const struct aws_array_list *source_list,
    struct aws_array_list *dest_list,
    enum aws_mqtt_log_subject log_subject,
    void *log_context,
    const char *log_prefix) {
    if (source_list == NULL || dest_list == NULL) {
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    size_t property_count = aws_array_list_length(source_list);
    for (size_t i = 0; i < property_count; ++i) {
        struct aws_mqtt5_name_value_pair *nv_pair = NULL;
        if (aws_array_list_get_at_ptr(source_list, (void **)&nv_pair, i)) {
            return AWS_OP_ERR;
        }

        struct aws_mqtt5_user_property property;
        AWS_ZERO_STRUCT(property);
        property.name = nv_pair->name;
        property.value = nv_pair->value;

        if (aws_mqtt5_add_user_property_to_array_list(
                allocator, &property, dest_list, log_subject, log_context, log_prefix)) {
            return AWS_OP_ERR;
        }
    }

    return AWS_OP_SUCCESS;
}

int aws_mqtt5_operation_set_string_property(
    struct aws_string **property,
    struct aws_allocator *allocator,
    struct aws_byte_cursor value,
    enum aws_mqtt_log_subject log_subject,
    void *log_context,
    const char *log_prefix) {
    struct aws_string *value_string = aws_string_new_from_cursor(allocator, &value);
    if (value_string == NULL) {
        return AWS_OP_ERR;
    }

    aws_string_destroy(*property);
    *property = value_string;

    AWS_LOGF_DEBUG(log_subject, "(%p) %s set to %s", log_context, log_prefix, aws_string_c_str(value_string));

    return AWS_OP_SUCCESS;
}

int aws_mqtt5_encode_variable_length_integer(struct aws_byte_buf *buf, uint32_t value) {
    AWS_PRECONDITION(buf);

    if (value > AWS_MQTT5_MAXIMUM_VARIABLE_LENGTH_INTEGER) {
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    do {
        uint8_t encoded_byte = value % 128;
        value /= 128;
        if (value) {
            encoded_byte |= 128;
        }
        if (!aws_byte_buf_write_u8(buf, encoded_byte)) {
            return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
        }
    } while (value);

    return AWS_OP_SUCCESS;
}
