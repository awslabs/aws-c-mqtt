/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/private/v5/mqtt5_options_storage.h>

#include <aws/common/string.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/mqtt/private/v5/mqtt5_client_impl.h>
#include <aws/mqtt/private/v5/mqtt5_utils.h>
#include <aws/mqtt/v5/mqtt5_client.h>

#include <inttypes.h>

/*********************************************************************************************************************
 * Property set
 ********************************************************************************************************************/

int aws_mqtt5_user_property_set_init(
    struct aws_mqtt5_user_property_set *property_set,
    struct aws_allocator *allocator,
    size_t property_count,
    const struct aws_mqtt5_user_property *properties) {
    AWS_ZERO_STRUCT(*property_set);

    /* TODO: use checked arithmetic? */
    size_t property_size_sum = 0;
    for (size_t i = 0; i < property_count; ++i) {
        const struct aws_mqtt5_user_property *property = &properties[i];
        property_size_sum += property->name.len;
        property_size_sum += property->value.len;
    }

    if (aws_byte_buf_init(&property_set->property_storage, allocator, property_size_sum)) {
        return AWS_OP_ERR;
    }

    if (aws_array_list_init_dynamic(
            &property_set->properties, allocator, property_count, sizeof(struct aws_mqtt5_user_property))) {
        goto error;
    }

    /* nothing in here should ever fail because we pre-allocated enough room */
    for (size_t i = 0; i < property_count; ++i) {
        const struct aws_mqtt5_user_property *property = &properties[i];
        struct aws_mqtt5_user_property property_clone = *property;

        if (aws_byte_buf_append_and_update(&property_set->property_storage, &property_clone.name)) {
            goto error;
        }

        if (aws_byte_buf_append_and_update(&property_set->property_storage, &property_clone.value)) {
            goto error;
        }

        if (aws_array_list_push_back(&property_set->properties, &property_clone)) {
            goto error;
        }
    }

    return AWS_OP_SUCCESS;

error:

    aws_mqtt5_user_property_set_clean_up(property_set);

    return AWS_OP_ERR;
}

void aws_mqtt5_user_property_set_clean_up(struct aws_mqtt5_user_property_set *property_set) {
    aws_array_list_clean_up(&property_set->properties);
    aws_byte_buf_clean_up_secure(&property_set->property_storage);
}

size_t aws_mqtt5_user_property_set_size(const struct aws_mqtt5_user_property_set *property_set) {
    return aws_array_list_length(&property_set->properties);
}

int aws_mqtt5_user_property_set_get_property(
    const struct aws_mqtt5_user_property_set *property_set,
    size_t index,
    struct aws_mqtt5_user_property *property_out) {
    return aws_array_list_get_at(&property_set->properties, property_out, index);
}

static void s_aws_mqtt5_user_property_set_log(
    const struct aws_mqtt5_user_property *properties,
    size_t property_count,
    void *log_context,
    enum aws_log_level level,
    const char *log_prefix) {

    AWS_LOGF(level, AWS_LS_MQTT5_GENERAL, "(%p) %s with %zu user properties:", log_context, log_prefix, property_count);

    for (size_t i = 0; i < property_count; ++i) {
        const struct aws_mqtt5_user_property *property = &properties[i];

        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "(%p) %s user property %zu with name: \"" PRInSTR "\", value: \"" PRInSTR "\"",
            log_context,
            log_prefix,
            i,
            AWS_BYTE_CURSOR_PRI(property->name),
            AWS_BYTE_CURSOR_PRI(property->value));
    }
}

/*********************************************************************************************************************
 * Operation base
 ********************************************************************************************************************/

struct aws_mqtt5_operation *aws_mqtt5_operation_acquire(struct aws_mqtt5_operation *operation) {
    if (operation == NULL) {
        return NULL;
    }

    aws_ref_count_acquire(&operation->ref_count);

    return operation;
}

struct aws_mqtt5_operation *aws_mqtt5_operation_release(struct aws_mqtt5_operation *operation) {
    if (operation != NULL) {
        aws_ref_count_release(&operation->ref_count);
    }

    return NULL;
}

/*********************************************************************************************************************
 * Connect
 ********************************************************************************************************************/

int aws_mqtt5_packet_connect_view_validate(
    const struct aws_mqtt5_packet_connect_view *connect_options,
    struct aws_mqtt5_client *client) {
    if (connect_options == NULL) {
        AWS_LOGF_ERROR(AWS_LS_MQTT5_GENERAL, "Null CONNECT options");
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    if (connect_options->receive_maximum != NULL) {
        if (*connect_options->receive_maximum == 0) {
            AWS_LOGF_ERROR(AWS_LS_MQTT5_GENERAL, "ReceiveMaximum property of CONNECT packet may not be zero.");
            return aws_raise_error(AWS_ERROR_MQTT5_CONNECT_OPTIONS_VALIDATION);
        }
    }

    if (connect_options->maximum_packet_size_bytes != NULL) {
        if (*connect_options->maximum_packet_size_bytes == 0) {
            AWS_LOGF_ERROR(AWS_LS_MQTT5_GENERAL, "MaximumPacketSize property of CONNECT packet may not be zero.");
            return aws_raise_error(AWS_ERROR_MQTT5_CONNECT_OPTIONS_VALIDATION);
        }
    }

    if (connect_options->will != NULL) {
        if (aws_mqtt5_packet_publish_view_validate(connect_options->will, client)) {
            AWS_LOGF_ERROR(AWS_LS_MQTT5_GENERAL, "CONNECT packet Will message failed validation");
            return AWS_OP_ERR;
        }
    }

    return AWS_OP_SUCCESS;
}

void aws_mqtt5_packet_connect_view_log(
    const struct aws_mqtt5_packet_connect_view *connect_view,
    enum aws_log_level level) {
    struct aws_logger *logger = aws_logger_get();
    if (logger == NULL || logger->vtable->get_log_level(logger, AWS_LS_MQTT5_GENERAL) < level) {
        return;
    }

    /* TODO: constantly checking the log level at this point is kind of dumb but there's no better API atm */

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "(%p) aws_mqtt5_packet_connect_view keep alive interval set to %" PRIu16,
        (void *)connect_view,
        connect_view->keep_alive_interval_seconds);

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "(%p) aws_mqtt5_packet_connect_view client id set to \"" PRInSTR "\"",
        (void *)connect_view,
        AWS_BYTE_CURSOR_PRI(connect_view->client_id));

    if (connect_view->username != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "(%p) aws_mqtt5_packet_connect_view username set to \"" PRInSTR "\"",
            (void *)connect_view,
            AWS_BYTE_CURSOR_PRI(*connect_view->username));
    }

    if (connect_view->password != NULL) {
        AWS_LOGF(level, AWS_LS_MQTT5_GENERAL, "(%p) aws_mqtt5_packet_connect_view password set", (void *)connect_view);
    }

    if (connect_view->session_expiry_interval_seconds != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "(%p) aws_mqtt5_packet_connect_view session expiry interval set to %" PRIu32,
            (void *)connect_view,
            *connect_view->session_expiry_interval_seconds);
    }

    if (connect_view->request_response_information != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "(%p) aws_mqtt5_packet_connect_view request response information set to %d",
            (void *)connect_view,
            (int)*connect_view->request_response_information);
    }

    if (connect_view->request_problem_information) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "(%p) aws_mqtt5_packet_connect_view request problem information set to %d",
            (void *)connect_view,
            (int)*connect_view->request_problem_information);
    }

    if (connect_view->receive_maximum != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "(%p) aws_mqtt5_packet_connect_view receive maximum set to %" PRIu16,
            (void *)connect_view,
            *connect_view->receive_maximum);
    }

    if (connect_view->topic_alias_maximum != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "(%p) aws_mqtt5_packet_connect_view topic alias maximum set to %" PRIu16,
            (void *)connect_view,
            *connect_view->topic_alias_maximum);
    }

    if (connect_view->maximum_packet_size_bytes != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "(%p) aws_mqtt5_packet_connect_view maximum packet size set to %" PRIu32,
            (void *)connect_view,
            *connect_view->maximum_packet_size_bytes);
    }

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "(%p) aws_mqtt5_packet_connect_view set will to (%p)",
        (void *)connect_view,
        (void *)connect_view->will);

    aws_mqtt5_packet_publish_view_log(connect_view->will, level);

    if (connect_view->will_delay_interval_seconds != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "(%p) aws_mqtt5_packet_connect_view will delay interval set to %" PRIu32,
            (void *)connect_view,
            *connect_view->will_delay_interval_seconds);
    }

    s_aws_mqtt5_user_property_set_log(
        connect_view->user_properties,
        connect_view->user_property_count,
        (void *)connect_view,
        level,
        "aws_mqtt5_packet_connect_view");
}

void aws_mqtt5_packet_connect_storage_clean_up(struct aws_mqtt5_packet_connect_storage *storage) {
    if (storage == NULL) {
        return;
    }

    aws_string_destroy(storage->client_id);
    aws_string_destroy(storage->username);
    aws_byte_buf_clean_up_secure(&storage->password);

    if (storage->will != NULL) {
        aws_mqtt5_packet_publish_storage_clean_up(storage->will);
        aws_mem_release(storage->allocator, storage->will);
    }

    aws_mqtt5_user_property_set_clean_up(&storage->user_properties);
}

void aws_mqtt5_packet_connect_view_init_from_storage(
    struct aws_mqtt5_packet_connect_view *view,
    const struct aws_mqtt5_packet_connect_storage *storage) {
    view->keep_alive_interval_seconds = storage->keep_alive_interval_seconds;
    view->client_id = aws_byte_cursor_from_string(storage->client_id);

    if (storage->username != NULL) {
        view->username = &storage->username_cursor;
    }

    if (storage->password_cursor.ptr != NULL) {
        view->password = &storage->password_cursor;
    }

    view->session_expiry_interval_seconds = storage->session_expiry_interval_seconds_ptr;
    view->request_response_information = storage->request_response_information_ptr;
    view->request_problem_information = storage->request_problem_information_ptr;
    view->receive_maximum = storage->receive_maximum_ptr;
    view->topic_alias_maximum = storage->topic_alias_maximum_ptr;
    view->maximum_packet_size_bytes = storage->maximum_packet_size_bytes_ptr;
    if (storage->will != NULL) {
        view->will = &storage->will->storage_view;
    }
    view->will_payload = storage->will_payload;
    view->will_delay_interval_seconds = storage->will_delay_interval_seconds_ptr;
    view->user_property_count = aws_mqtt5_user_property_set_size(&storage->user_properties);
    view->user_properties = storage->user_properties.properties.data;
}

int aws_mqtt5_packet_connect_storage_init(
    struct aws_mqtt5_packet_connect_storage *storage,
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_connect_view *view) {

    storage->allocator = allocator;
    storage->keep_alive_interval_seconds = view->keep_alive_interval_seconds;

    storage->client_id = aws_string_new_from_cursor(allocator, &view->client_id);
    if (storage->client_id == NULL) {
        return AWS_OP_ERR;
    }

    if (view->username != NULL) {
        storage->username = aws_string_new_from_cursor(allocator, view->username);
        if (storage->username == NULL) {
            return AWS_OP_ERR;
        }

        storage->username_cursor = aws_byte_cursor_from_string(storage->username);
    }

    if (view->password != NULL) {
        if (aws_byte_buf_init_copy_from_cursor(&storage->password, allocator, *view->password)) {
            return AWS_OP_ERR;
        }

        storage->password_cursor = aws_byte_cursor_from_buf(&storage->password);
    }

    if (view->session_expiry_interval_seconds != NULL) {
        storage->session_expiry_interval_seconds = *view->session_expiry_interval_seconds;
        storage->session_expiry_interval_seconds_ptr = &storage->session_expiry_interval_seconds;
    }

    if (view->request_response_information != NULL) {
        storage->request_response_information = *view->request_response_information;
        storage->request_response_information_ptr = &storage->request_response_information;
    }

    if (view->request_problem_information != NULL) {
        storage->request_problem_information = *view->request_problem_information;
        storage->request_problem_information_ptr = &storage->request_problem_information;
    }

    if (view->receive_maximum != NULL) {
        storage->receive_maximum = *view->receive_maximum;
        storage->receive_maximum_ptr = &storage->receive_maximum;
    }

    if (view->topic_alias_maximum != NULL) {
        storage->topic_alias_maximum = *view->topic_alias_maximum;
        storage->topic_alias_maximum_ptr = &storage->topic_alias_maximum;
    }

    if (view->maximum_packet_size_bytes != NULL) {
        storage->maximum_packet_size_bytes = *view->maximum_packet_size_bytes;
        storage->maximum_packet_size_bytes_ptr = &storage->maximum_packet_size_bytes;
    }

    if (view->will != NULL) {
        storage->will_payload = view->will_payload; /* TODO ref count */
        storage->will = aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt5_packet_publish_storage));
        if (storage->will == NULL) {
            return AWS_OP_ERR;
        }

        if (aws_mqtt5_packet_publish_storage_init(storage->will, allocator, view->will)) {
            return AWS_OP_ERR;
        }
    }

    if (view->will_delay_interval_seconds != 0) {
        storage->will_delay_interval_seconds = *view->will_delay_interval_seconds;
        storage->will_delay_interval_seconds_ptr = &storage->will_delay_interval_seconds;
    }

    if (aws_mqtt5_user_property_set_init(
            &storage->user_properties, allocator, view->user_property_count, view->user_properties)) {
        return AWS_OP_ERR;
    }

    aws_mqtt5_packet_connect_view_init_from_storage(&storage->storage_view, storage);

    return AWS_OP_SUCCESS;
}

static void s_destroy_operation_connect(void *object) {
    if (object == NULL) {
        return;
    }

    struct aws_mqtt5_operation_connect *connect_op = object;

    aws_mqtt5_packet_connect_storage_clean_up(&connect_op->options_storage);

    aws_mem_release(connect_op->allocator, connect_op);
}

struct aws_mqtt5_operation_connect *aws_mqtt5_operation_connect_new(
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_connect_view *connect_options) {
    AWS_PRECONDITION(allocator != NULL);
    AWS_PRECONDITION(connect_options != NULL);

    if (aws_mqtt5_packet_connect_view_validate(connect_options, NULL)) {
        return NULL;
    }

    struct aws_mqtt5_operation_connect *connect_op =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt5_operation_connect));
    if (connect_op == NULL) {
        return NULL;
    }

    connect_op->allocator = allocator;
    connect_op->base.operation_type = AWS_MOT_CONNECT;
    aws_ref_count_init(&connect_op->base.ref_count, connect_op, s_destroy_operation_connect);
    connect_op->base.impl = connect_op;

    if (aws_mqtt5_packet_connect_storage_init(&connect_op->options_storage, allocator, connect_options)) {
        goto error;
    }

    return connect_op;

error:

    aws_mqtt5_operation_release(&connect_op->base);

    return NULL;
}

/*********************************************************************************************************************
 * Disconnect
 ********************************************************************************************************************/

int aws_mqtt5_packet_disconnect_view_validate(
    const struct aws_mqtt5_packet_disconnect_view *disconnect_view,
    struct aws_mqtt5_client *client) {
    (void)client;

    if (disconnect_view == NULL) {
        AWS_LOGF_ERROR(AWS_LS_MQTT5_GENERAL, "null DISCONNECT packet options");
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    bool is_valid_reason_code = true;
    aws_mqtt5_disconnect_reason_code_to_c_string(disconnect_view->reason_code, &is_valid_reason_code);
    if (!is_valid_reason_code) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_GENERAL, "Invalid DISCONNECT packet reason code:%d", (int)disconnect_view->reason_code);
        return aws_raise_error(AWS_ERROR_MQTT5_DISCONNECT_OPTIONS_VALIDATION);
    }

    return AWS_OP_SUCCESS;
}

void aws_mqtt5_packet_disconnect_view_log(
    const struct aws_mqtt5_packet_disconnect_view *disconnect_view,
    enum aws_log_level level) {
    struct aws_logger *logger = aws_logger_get();
    if (logger == NULL || logger->vtable->get_log_level(logger, AWS_LS_MQTT5_GENERAL) < level) {
        return;
    }

    /* TODO: constantly checking the log level at this point is kind of dumb but there's no better API atm */

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "(%p) aws_mqtt5_packet_disconnect_view reason code set to %d(%s)",
        (void *)disconnect_view,
        (int)disconnect_view->reason_code,
        aws_mqtt5_disconnect_reason_code_to_c_string(disconnect_view->reason_code, NULL));

    if (disconnect_view->session_expiry_interval_seconds != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "(%p) aws_mqtt5_packet_disconnect_view session expiry interval set to %" PRIu32,
            (void *)disconnect_view,
            *disconnect_view->session_expiry_interval_seconds);
    }

    if (disconnect_view->reason_string != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "(%p) aws_mqtt5_packet_disconnect_view reason string set to \"" PRInSTR "\"",
            (void *)disconnect_view,
            AWS_BYTE_CURSOR_PRI(*disconnect_view->reason_string));
    }

    if (disconnect_view->server_reference != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "(%p) aws_mqtt5_packet_disconnect_view server reference set to \"" PRInSTR "\"",
            (void *)disconnect_view,
            AWS_BYTE_CURSOR_PRI(*disconnect_view->server_reference));
    }

    s_aws_mqtt5_user_property_set_log(
        disconnect_view->user_properties,
        disconnect_view->user_property_count,
        (void *)disconnect_view,
        level,
        "aws_mqtt5_packet_disconnect_view");
}

void aws_aws_mqtt5_packet_disconnect_storage_clean_up(struct aws_mqtt5_packet_disconnect_storage *disconnect_storage) {
    if (disconnect_storage == NULL) {
        return;
    }

    aws_string_destroy(disconnect_storage->reason_string);
    aws_mqtt5_user_property_set_clean_up(&disconnect_storage->user_properties);
    aws_string_destroy(disconnect_storage->server_reference);
}

void aws_mqtt5_packet_disconnect_view_init_from_storage(
    struct aws_mqtt5_packet_disconnect_view *disconnect_view,
    const struct aws_mqtt5_packet_disconnect_storage *disconnect_storage) {
    disconnect_view->reason_code = disconnect_storage->reason_code;
    disconnect_view->session_expiry_interval_seconds = disconnect_storage->session_expiry_interval_seconds_ptr;

    if (disconnect_storage->reason_string != NULL) {
        disconnect_view->reason_string = &disconnect_storage->reason_string_cursor;
    }

    if (disconnect_storage->server_reference != NULL) {
        disconnect_view->server_reference = &disconnect_storage->server_reference_cursor;
    }

    disconnect_view->user_property_count = aws_mqtt5_user_property_set_size(&disconnect_storage->user_properties);
    disconnect_view->user_properties = disconnect_storage->user_properties.properties.data;
}

int aws_mqtt5_packet_disconnect_storage_init(
    struct aws_mqtt5_packet_disconnect_storage *disconnect_storage,
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_disconnect_view *disconnect_options) {

    disconnect_storage->reason_code = disconnect_options->reason_code;

    if (disconnect_options->session_expiry_interval_seconds != NULL) {
        disconnect_storage->session_expiry_interval_seconds = *disconnect_options->session_expiry_interval_seconds;
    }

    if (disconnect_options->reason_string != NULL) {
        disconnect_storage->reason_string = aws_string_new_from_cursor(allocator, disconnect_options->reason_string);
        if (disconnect_storage->reason_string == NULL) {
            return AWS_OP_ERR;
        }

        disconnect_storage->reason_string_cursor = aws_byte_cursor_from_string(disconnect_storage->reason_string);
    }

    if (aws_mqtt5_user_property_set_init(
            &disconnect_storage->user_properties,
            allocator,
            disconnect_options->user_property_count,
            disconnect_options->user_properties)) {
        return AWS_OP_ERR;
    }

    if (disconnect_options->server_reference != NULL) {
        disconnect_storage->server_reference =
            aws_string_new_from_cursor(allocator, disconnect_options->server_reference);
        if (disconnect_storage->server_reference == NULL) {
            return AWS_OP_ERR;
        }

        disconnect_storage->server_reference_cursor = aws_byte_cursor_from_string(disconnect_storage->server_reference);
    }

    aws_mqtt5_packet_disconnect_view_init_from_storage(&disconnect_storage->storage_view, disconnect_storage);

    return AWS_OP_SUCCESS;
}

static void s_destroy_operation_disconnect(void *object) {
    if (object == NULL) {
        return;
    }

    struct aws_mqtt5_operation_disconnect *disconnect_op = object;

    aws_aws_mqtt5_packet_disconnect_storage_clean_up(&disconnect_op->options_storage);

    aws_mem_release(disconnect_op->allocator, disconnect_op);
}

struct aws_mqtt5_operation_disconnect *aws_mqtt5_operation_disconnect_new(
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_disconnect_view *disconnect_options) {
    AWS_PRECONDITION(allocator != NULL);

    if (aws_mqtt5_packet_disconnect_view_validate(disconnect_options, NULL)) {
        return NULL;
    }

    struct aws_mqtt5_operation_disconnect *disconnect_op =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt5_operation_disconnect));
    if (disconnect_op == NULL) {
        return NULL;
    }

    disconnect_op->allocator = allocator;
    disconnect_op->base.operation_type = AWS_MOT_DISCONNECT;
    aws_ref_count_init(&disconnect_op->base.ref_count, disconnect_op, s_destroy_operation_disconnect);
    disconnect_op->base.impl = disconnect_op;

    if (aws_mqtt5_packet_disconnect_storage_init(&disconnect_op->options_storage, allocator, disconnect_options)) {
        goto error;
    }

    return disconnect_op;

error:

    aws_mqtt5_operation_release(&disconnect_op->base);

    return NULL;
}

/*********************************************************************************************************************
 * Publish
 ********************************************************************************************************************/

int aws_mqtt5_packet_publish_view_validate(
    const struct aws_mqtt5_packet_publish_view *publish_view,
    struct aws_mqtt5_client *client) {
    (void)client;

    if (publish_view == NULL) {
        AWS_LOGF_ERROR(AWS_LS_MQTT5_GENERAL, "null PUBLISH packet options");
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    if (publish_view->qos < AWS_MQTT5_QOS_AT_MOST_ONCE || publish_view->qos > AWS_MQTT5_QOS_EXACTLY_ONCE) {
        AWS_LOGF_ERROR(AWS_LS_MQTT5_GENERAL, "invalid QoS value in PUBLISH packet options: %d", (int)publish_view->qos);
        return aws_raise_error(AWS_ERROR_MQTT5_PUBLISH_OPTIONS_VALIDATION);
    }

    /* 0-length topic is valid if there's an alias, otherwise we need a valid topic */
    if (publish_view->topic.len == 0) {
        if (publish_view->topic_alias == NULL) {
            AWS_LOGF_ERROR(AWS_LS_MQTT5_GENERAL, "Missing topic in PUBLISH packet options");
            return aws_raise_error(AWS_ERROR_MQTT5_PUBLISH_OPTIONS_VALIDATION);
        }
    } else if (!aws_mqtt_is_valid_topic(&publish_view->topic)) {
        AWS_LOGF_ERROR(AWS_LS_MQTT5_GENERAL, "Invalid topic in PUBLISH packet options");
        return aws_raise_error(AWS_ERROR_MQTT5_PUBLISH_OPTIONS_VALIDATION);
    }

    if (publish_view->payload_format < AWS_MQTT5_PFI_NOT_SET || publish_view->payload_format > AWS_MQTT5_PFI_UTF8) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_GENERAL,
            "invalid payload format value in PUBLISH packet options: %d",
            (int)publish_view->payload_format);
        return aws_raise_error(AWS_ERROR_MQTT5_PUBLISH_OPTIONS_VALIDATION);
    }

    /*
     * validate is done from a client perspective and client's should never generate subscription identifier in a
     * publish message
     */
    if (publish_view->subscription_identifier_count != 0) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_GENERAL, "Client-initiated PUBLISH packets may not contain subscription identifiers");
        return aws_raise_error(AWS_ERROR_MQTT5_PUBLISH_OPTIONS_VALIDATION);
    }

    /*
     * TODO: if client is defined and negotiated settings are final (CONNECTED) then check:
     *   qos vs. maximum_qos
     *   topic_alias vs. maximum_topic_alias and topic_alias_support
     *
     * This implies that a will's qos will be of unknown validity
     */

    return AWS_OP_SUCCESS;
}

void aws_mqtt5_packet_publish_view_log(
    const struct aws_mqtt5_packet_publish_view *publish_view,
    enum aws_log_level level) {
    struct aws_logger *logger = aws_logger_get();
    if (logger == NULL || logger->vtable->get_log_level(logger, AWS_LS_MQTT5_GENERAL) < level) {
        return;
    }

    /* TODO: constantly checking the log level at this point is kind of dumb but there's no better API atm */

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "(%p) aws_mqtt5_packet_publish_view qos set to %d",
        (void *)publish_view,
        (int)publish_view->qos);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_GENERAL,
        "(%p) aws_mqtt5_packet_publish_view retain set to %d",
        (void *)publish_view,
        (int)publish_view->retain);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_GENERAL,
        "(%p) aws_mqtt5_packet_publish_view topic set to \"" PRInSTR "\"",
        (void *)publish_view,
        AWS_BYTE_CURSOR_PRI(publish_view->topic));

    if (publish_view->payload_format != AWS_MQTT5_PFI_NOT_SET) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_GENERAL,
            "(%p) aws_mqtt5_packet_publish_view payload format indicator set to %d(%s)",
            (void *)publish_view,
            (int)publish_view->payload_format,
            aws_mqtt5_payload_format_indicator_to_c_string(publish_view->payload_format));
    }

    if (publish_view->message_expiry_interval_seconds != NULL) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_GENERAL,
            "(%p) aws_mqtt5_packet_publish_view message expiry interval set to %" PRIu32,
            (void *)publish_view,
            *publish_view->message_expiry_interval_seconds);
    }

    if (publish_view->topic_alias != NULL) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_GENERAL,
            "(%p) aws_mqtt5_packet_publish_view topic alias set to %" PRIu16,
            (void *)publish_view,
            *publish_view->topic_alias);
    }

    if (publish_view->response_topic != NULL) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_GENERAL,
            "(%p) aws_mqtt5_packet_publish_view response topic set to \"" PRInSTR "\"",
            (void *)publish_view,
            AWS_BYTE_CURSOR_PRI(*publish_view->response_topic));
    }

    if (publish_view->correlation_data != NULL) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_GENERAL, "(%p) aws_mqtt5_packet_publish_view - set correlation data", (void *)publish_view);
    }

    if (publish_view->content_type != NULL) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_GENERAL,
            "(%p) aws_mqtt5_packet_publish_view content type set to \"" PRInSTR "\"",
            (void *)publish_view,
            AWS_BYTE_CURSOR_PRI(*publish_view->content_type));
    }

    s_aws_mqtt5_user_property_set_log(
        publish_view->user_properties,
        publish_view->user_property_count,
        (void *)publish_view,
        level,
        "aws_mqtt5_packet_publish_view");
}

void aws_mqtt5_packet_publish_view_init_from_storage(
    struct aws_mqtt5_packet_publish_view *publish_view,
    const struct aws_mqtt5_packet_publish_storage *publish_storage) {
    publish_view->qos = publish_storage->qos;
    publish_view->retain = publish_storage->retain;
    publish_view->topic = aws_byte_cursor_from_string(publish_storage->topic);
    publish_view->payload_format = publish_storage->payload_format;
    publish_view->message_expiry_interval_seconds = publish_storage->message_expiry_interval_seconds_ptr;
    publish_view->topic_alias = publish_storage->topic_alias_ptr;

    if (publish_storage->response_topic != NULL) {
        publish_view->response_topic = &publish_storage->response_topic_cursor;
    }

    if (publish_storage->correlation_data_cursor.ptr != NULL) {
        publish_view->correlation_data = &publish_storage->correlation_data_cursor;
    }

    if (publish_storage->content_type != NULL) {
        publish_view->content_type = &publish_storage->content_type_cursor;
    }

    publish_view->user_property_count = aws_mqtt5_user_property_set_size(&publish_storage->user_properties);
    publish_view->user_properties = publish_storage->user_properties.properties.data;
}

int aws_mqtt5_packet_publish_storage_init(
    struct aws_mqtt5_packet_publish_storage *publish_storage,
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_publish_view *publish_options) {
    publish_storage->qos = publish_options->qos;
    publish_storage->retain = publish_options->retain;

    publish_storage->topic = aws_string_new_from_cursor(allocator, &publish_options->topic);
    if (publish_storage->topic == NULL) {
        return AWS_OP_ERR;
    }

    publish_storage->payload_format = publish_options->payload_format;

    if (publish_options->message_expiry_interval_seconds != NULL) {
        publish_storage->message_expiry_interval_seconds = *publish_options->message_expiry_interval_seconds;
        publish_storage->message_expiry_interval_seconds_ptr = &publish_storage->message_expiry_interval_seconds;
    }

    if (publish_options->topic_alias != NULL) {
        publish_storage->topic_alias = *publish_options->topic_alias;
        publish_storage->topic_alias_ptr = &publish_storage->topic_alias;
    }

    if (publish_options->response_topic != NULL) {
        publish_storage->response_topic = aws_string_new_from_cursor(allocator, publish_options->response_topic);
        if (publish_storage->response_topic == NULL) {
            return AWS_OP_ERR;
        }

        publish_storage->response_topic_cursor = aws_byte_cursor_from_string(publish_storage->response_topic);
    }

    if (publish_options->correlation_data != NULL) {
        if (aws_byte_buf_init_copy_from_cursor(
                &publish_storage->correlation_data, allocator, *publish_options->correlation_data)) {
            return AWS_OP_ERR;
        }

        publish_storage->correlation_data_cursor = aws_byte_cursor_from_buf(&publish_storage->correlation_data);
    }

    if (publish_options->content_type != NULL) {
        publish_storage->content_type = aws_string_new_from_cursor(allocator, publish_options->content_type);
        if (publish_storage->content_type == NULL) {
            return AWS_OP_ERR;
        }

        publish_storage->content_type_cursor = aws_byte_cursor_from_string(publish_storage->content_type);
    }

    if (aws_mqtt5_user_property_set_init(
            &publish_storage->user_properties,
            allocator,
            publish_options->user_property_count,
            publish_options->user_properties)) {
        return AWS_OP_ERR;
    }

    aws_mqtt5_packet_publish_view_init_from_storage(&publish_storage->storage_view, publish_storage);

    return AWS_OP_SUCCESS;
}

void aws_mqtt5_packet_publish_storage_clean_up(struct aws_mqtt5_packet_publish_storage *publish_storage) {

    aws_string_destroy(publish_storage->topic);
    aws_string_destroy(publish_storage->response_topic);
    aws_byte_buf_clean_up(&publish_storage->correlation_data);
    aws_string_destroy(publish_storage->content_type);

    aws_mqtt5_user_property_set_clean_up(&publish_storage->user_properties);
}

static void s_destroy_operation_publish(void *object) {
    if (object == NULL) {
        return;
    }

    struct aws_mqtt5_operation_publish *publish_op = object;

    /* TODO: payload release */

    aws_mqtt5_packet_publish_storage_clean_up(&publish_op->options_storage);

    aws_mem_release(publish_op->allocator, publish_op);
}

struct aws_mqtt5_operation_publish *aws_mqtt5_operation_publish_new(
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_publish_view *publish_options,
    struct aws_input_stream *payload,
    const struct aws_mqtt5_publish_completion_options *completion_options) {
    AWS_PRECONDITION(allocator != NULL);
    AWS_PRECONDITION(publish_options != NULL);

    if (aws_mqtt5_packet_publish_view_validate(publish_options, NULL)) {
        return NULL;
    }

    struct aws_mqtt5_operation_publish *publish_op =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt5_operation_publish));
    if (publish_op == NULL) {
        return NULL;
    }

    publish_op->allocator = allocator;
    publish_op->base.operation_type = AWS_MOT_PUBLISH;
    aws_ref_count_init(&publish_op->base.ref_count, publish_op, s_destroy_operation_publish);
    publish_op->base.impl = publish_op;

    publish_op->payload = payload; /* TODO: ref count */

    if (aws_mqtt5_packet_publish_storage_init(&publish_op->options_storage, allocator, publish_options)) {
        goto error;
    }

    if (completion_options != NULL) {
        publish_op->completion_options = *completion_options;
    }

    return publish_op;

error:

    aws_mqtt5_operation_release(&publish_op->base);

    return NULL;
}

/*********************************************************************************************************************
 * Unsubscribe
 ********************************************************************************************************************/

int aws_mqtt5_packet_unsubscribe_view_validate(
    const struct aws_mqtt5_packet_unsubscribe_view *unsubscribe_view,
    struct aws_mqtt5_client *client) {
    (void)client;

    if (unsubscribe_view == NULL) {
        AWS_LOGF_ERROR(AWS_LS_MQTT5_GENERAL, "null UNSUBSCRIBE packet options");
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    if (unsubscribe_view->topic_count == 0) {
        AWS_LOGF_ERROR(AWS_LS_MQTT5_GENERAL, "UNSUBSCRIBE packet must contain at least one topic");
        return aws_raise_error(AWS_ERROR_MQTT5_UNSUBSCRIBE_OPTIONS_VALIDATION);
    }

    return AWS_OP_SUCCESS;
}

void aws_mqtt5_packet_unsubscribe_view_log(
    const struct aws_mqtt5_packet_unsubscribe_view *unsubscribe_view,
    enum aws_log_level level) {
    struct aws_logger *logger = aws_logger_get();
    if (logger == NULL || logger->vtable->get_log_level(logger, AWS_LS_MQTT5_GENERAL) < level) {
        return;
    }

    /* TODO: constantly checking the log level at this point is kind of dumb but there's no better API atm */

    size_t topic_count = unsubscribe_view->topic_count;
    for (size_t i = 0; i < topic_count; ++i) {
        const struct aws_byte_cursor *topic_cursor = &unsubscribe_view->topics[i];

        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_GENERAL,
            "(%p) aws_mqtt5_packet_unsubscribe_view topic %zu: \"" PRInSTR "\"",
            (void *)unsubscribe_view,
            i,
            AWS_BYTE_CURSOR_PRI(*topic_cursor));
    }

    s_aws_mqtt5_user_property_set_log(
        unsubscribe_view->user_properties,
        unsubscribe_view->user_property_count,
        (void *)unsubscribe_view,
        level,
        "aws_mqtt5_packet_unsubscribe_view");
}

void aws_mqtt5_packet_unsubscribe_storage_clean_up(struct aws_mqtt5_packet_unsubscribe_storage *unsubscribe_storage) {
    if (unsubscribe_storage == NULL) {
        return;
    }

    aws_byte_buf_clean_up(&unsubscribe_storage->topic_storage);
    aws_array_list_clean_up(&unsubscribe_storage->topics);

    aws_mqtt5_user_property_set_clean_up(&unsubscribe_storage->user_properties);
}

void aws_mqtt5_packet_unsubscribe_view_init_from_storage(
    struct aws_mqtt5_packet_unsubscribe_view *unsubscribe_view,
    const struct aws_mqtt5_packet_unsubscribe_storage *unsubscribe_storage) {
    unsubscribe_view->topic_count = aws_array_list_length(&unsubscribe_storage->topics);
    unsubscribe_view->topics = unsubscribe_storage->topics.data;

    unsubscribe_view->user_property_count = aws_mqtt5_user_property_set_size(&unsubscribe_storage->user_properties);
    unsubscribe_view->user_properties = unsubscribe_storage->user_properties.properties.data;
}

static int s_aws_mqtt5_packet_unsubscribe_build_topic_list(
    struct aws_mqtt5_packet_unsubscribe_storage *unsubscribe_storage,
    struct aws_allocator *allocator,
    size_t topic_count,
    const struct aws_byte_cursor *topics) {
    size_t topic_total_length = 0;
    for (size_t i = 0; i < topic_count; ++i) {
        const struct aws_byte_cursor *topic_cursor = &topics[i];
        topic_total_length += topic_cursor->len;
    }

    if (aws_byte_buf_init(&unsubscribe_storage->topic_storage, allocator, topic_total_length)) {
        return AWS_OP_ERR;
    }

    if (aws_array_list_init_dynamic(
            &unsubscribe_storage->topics, allocator, topic_count, sizeof(struct aws_byte_cursor))) {
        return AWS_OP_ERR;
    }

    for (size_t i = 0; i < topic_count; ++i) {
        const struct aws_byte_cursor *topic_cursor_ptr = &topics[i];
        struct aws_byte_cursor topic_cursor = *topic_cursor_ptr;

        if (aws_byte_buf_append_and_update(&unsubscribe_storage->topic_storage, &topic_cursor)) {
            return AWS_OP_ERR;
        }

        if (aws_array_list_push_back(&unsubscribe_storage->topics, &topic_cursor)) {
            return AWS_OP_ERR;
        }
    }

    return AWS_OP_SUCCESS;
}

int aws_mqtt5_packet_unsubscribe_storage_init(
    struct aws_mqtt5_packet_unsubscribe_storage *unsubscribe_storage,
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_unsubscribe_view *unsubscribe_options) {
    if (s_aws_mqtt5_packet_unsubscribe_build_topic_list(
            unsubscribe_storage, allocator, unsubscribe_options->topic_count, unsubscribe_options->topics)) {
        return AWS_OP_ERR;
    }

    if (aws_mqtt5_user_property_set_init(
            &unsubscribe_storage->user_properties,
            allocator,
            unsubscribe_options->user_property_count,
            unsubscribe_options->user_properties)) {
        return AWS_OP_ERR;
    }

    aws_mqtt5_packet_unsubscribe_view_init_from_storage(&unsubscribe_storage->storage_view, unsubscribe_storage);

    return AWS_OP_SUCCESS;
}

static void s_destroy_operation_unsubscribe(void *object) {
    if (object == NULL) {
        return;
    }

    struct aws_mqtt5_operation_unsubscribe *unsubscribe_op = object;

    aws_mqtt5_packet_unsubscribe_storage_clean_up(&unsubscribe_op->options_storage);

    aws_mem_release(unsubscribe_op->allocator, unsubscribe_op);
}

struct aws_mqtt5_operation_unsubscribe *aws_mqtt5_operation_unsubscribe_new(
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_unsubscribe_view *unsubscribe_options,
    const struct aws_mqtt5_unsubscribe_completion_options *completion_options) {
    AWS_PRECONDITION(allocator != NULL);
    AWS_PRECONDITION(unsubscribe_options != NULL);

    if (aws_mqtt5_packet_unsubscribe_view_validate(unsubscribe_options, NULL)) {
        return NULL;
    }

    struct aws_mqtt5_operation_unsubscribe *unsubscribe_op =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt5_operation_unsubscribe));
    if (unsubscribe_op == NULL) {
        return NULL;
    }

    unsubscribe_op->allocator = allocator;
    unsubscribe_op->base.operation_type = AWS_MOT_UNSUBSCRIBE;
    aws_ref_count_init(&unsubscribe_op->base.ref_count, unsubscribe_op, s_destroy_operation_unsubscribe);
    unsubscribe_op->base.impl = unsubscribe_op;

    if (aws_mqtt5_packet_unsubscribe_storage_init(&unsubscribe_op->options_storage, allocator, unsubscribe_options)) {
        goto error;
    }

    if (completion_options != NULL) {
        unsubscribe_op->completion_options = *completion_options;
    }

    return unsubscribe_op;

error:

    aws_mqtt5_operation_release(&unsubscribe_op->base);

    return NULL;
}

/*********************************************************************************************************************
 * Subscribe
 ********************************************************************************************************************/

int aws_mqtt5_packet_subscribe_view_validate(
    const struct aws_mqtt5_packet_subscribe_view *subscribe_view,
    struct aws_mqtt5_client *client) {
    (void)client;

    if (subscribe_view == NULL) {
        AWS_LOGF_ERROR(AWS_LS_MQTT5_GENERAL, "null SUBSCRIBE packet options");
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    if (subscribe_view->subscription_count == 0) {
        AWS_LOGF_ERROR(AWS_LS_MQTT5_GENERAL, "SUBSCRIBE packet must contain at least one subscription");
        return aws_raise_error(AWS_ERROR_MQTT5_SUBSCRIBE_OPTIONS_VALIDATION);
    }

    if (subscribe_view->subscription_identifier != NULL) {
        if (*subscribe_view->subscription_identifier > AWS_MQTT5_MAXIMUM_VARIABLE_LENGTH_INTEGER) {
            AWS_LOGF_ERROR(
                AWS_LS_MQTT5_GENERAL,
                "SUBSCRIBE packet subscription identifier (%" PRIu32 ") too large",
                *subscribe_view->subscription_identifier);
            return aws_raise_error(AWS_ERROR_MQTT5_SUBSCRIBE_OPTIONS_VALIDATION);
        }
    }

    return AWS_OP_SUCCESS;
}

void aws_mqtt5_packet_subscribe_view_log(
    const struct aws_mqtt5_packet_subscribe_view *subscribe_view,
    enum aws_log_level level) {
    struct aws_logger *logger = aws_logger_get();
    if (logger == NULL || logger->vtable->get_log_level(logger, AWS_LS_MQTT5_GENERAL) < level) {
        return;
    }

    /* TODO: constantly checking the log level at this point is kind of dumb but there's no better API atm */

    size_t subscription_count = subscribe_view->subscription_count;
    for (size_t i = 0; i < subscription_count; ++i) {
        const struct aws_mqtt5_subscription_view *view = &subscribe_view->subscriptions[i];

        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_GENERAL,
            "(%p) aws_mqtt5_packet_subscribe_view subscription %zu: topic filter \"" PRInSTR
            "\", qos %d, no local %d, retain as "
            "published %d, retain handling %d(%s)",
            (void *)subscribe_view,
            i,
            AWS_BYTE_CURSOR_PRI(view->topic_filter),
            (int)view->qos,
            (int)view->no_local,
            (int)view->retain_as_published,
            (int)view->retain_handling_type,
            aws_mqtt5_retain_handling_type_to_c_string(view->retain_handling_type));
    }

    if (subscribe_view->subscription_identifier != NULL) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_GENERAL,
            "(%p) aws_mqtt5_packet_subscribe_view subscription identifier set to %" PRIu32,
            (void *)subscribe_view,
            *subscribe_view->subscription_identifier);
    }

    s_aws_mqtt5_user_property_set_log(
        subscribe_view->user_properties,
        subscribe_view->subscription_count,
        (void *)subscribe_view,
        level,
        "aws_mqtt5_packet_subscribe_view");
}

void aws_mqtt5_packet_subscribe_storage_clean_up(struct aws_mqtt5_packet_subscribe_storage *subscribe_storage) {
    if (subscribe_storage == NULL) {
        return;
    }

    aws_byte_buf_clean_up(&subscribe_storage->topic_filter_storage);
    aws_array_list_clean_up(&subscribe_storage->subscriptions);

    aws_mqtt5_user_property_set_clean_up(&subscribe_storage->user_properties);
}

void aws_mqtt5_packet_subscribe_view_init_from_storage(
    struct aws_mqtt5_packet_subscribe_view *subscribe_view,
    const struct aws_mqtt5_packet_subscribe_storage *subscribe_storage) {

    subscribe_view->subscription_identifier = subscribe_storage->subscription_identifier_ptr;

    subscribe_view->subscription_count = aws_array_list_length(&subscribe_storage->subscriptions);
    subscribe_view->subscriptions = subscribe_storage->subscriptions.data;

    subscribe_view->user_property_count = aws_mqtt5_user_property_set_size(&subscribe_storage->user_properties);
    subscribe_view->user_properties = subscribe_storage->user_properties.properties.data;
}

static int s_aws_mqtt5_packet_subscribe_storage_init_subscriptions(
    struct aws_mqtt5_packet_subscribe_storage *subscribe_storage,
    struct aws_allocator *allocator,
    size_t subscription_count,
    const struct aws_mqtt5_subscription_view *subscriptions) {

    size_t total_topic_filter_length = 0;
    for (size_t i = 0; i < subscription_count; ++i) {
        const struct aws_mqtt5_subscription_view *view = &subscriptions[i];
        total_topic_filter_length += view->topic_filter.len;
    }

    if (aws_byte_buf_init(&subscribe_storage->topic_filter_storage, allocator, total_topic_filter_length)) {
        return AWS_OP_ERR;
    }

    if (aws_array_list_init_dynamic(
            &subscribe_storage->subscriptions,
            allocator,
            subscription_count,
            sizeof(struct aws_mqtt5_subscription_view))) {
        return AWS_OP_ERR;
    }

    for (size_t i = 0; i < subscription_count; ++i) {
        const struct aws_mqtt5_subscription_view *source = &subscriptions[i];
        struct aws_mqtt5_subscription_view copy = *source;

        if (aws_byte_buf_append_and_update(&subscribe_storage->topic_filter_storage, &copy.topic_filter)) {
            return AWS_OP_ERR;
        }

        if (aws_array_list_push_back(&subscribe_storage->subscriptions, &copy)) {
            return AWS_OP_ERR;
        }
    }

    return AWS_OP_SUCCESS;
}

int aws_mqtt5_packet_subscribe_storage_init(
    struct aws_mqtt5_packet_subscribe_storage *subscribe_storage,
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_subscribe_view *subscribe_options) {
    if (subscribe_options->subscription_identifier != NULL) {
        subscribe_storage->subscription_identifier = *subscribe_options->subscription_identifier;
        subscribe_storage->subscription_identifier_ptr = &subscribe_storage->subscription_identifier;
    }

    if (s_aws_mqtt5_packet_subscribe_storage_init_subscriptions(
            subscribe_storage, allocator, subscribe_options->subscription_count, subscribe_options->subscriptions)) {
        return AWS_OP_ERR;
    }

    if (aws_mqtt5_user_property_set_init(
            &subscribe_storage->user_properties,
            allocator,
            subscribe_options->user_property_count,
            subscribe_options->user_properties)) {
        return AWS_OP_ERR;
    }

    aws_mqtt5_packet_subscribe_view_init_from_storage(&subscribe_storage->storage_view, subscribe_storage);

    return AWS_OP_SUCCESS;
}

static void s_destroy_operation_subscribe(void *object) {
    if (object == NULL) {
        return;
    }

    struct aws_mqtt5_operation_subscribe *subscribe_op = object;

    aws_mqtt5_packet_subscribe_storage_clean_up(&subscribe_op->options_storage);

    aws_mem_release(subscribe_op->allocator, subscribe_op);
}

struct aws_mqtt5_operation_subscribe *aws_mqtt5_operation_subscribe_new(
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_subscribe_view *subscribe_options,
    const struct aws_mqtt5_subscribe_completion_options *completion_options) {
    AWS_PRECONDITION(allocator != NULL);
    AWS_PRECONDITION(subscribe_options != NULL);

    if (aws_mqtt5_packet_subscribe_view_validate(subscribe_options, NULL)) {
        return NULL;
    }

    struct aws_mqtt5_operation_subscribe *subscribe_op =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt5_operation_subscribe));
    if (subscribe_op == NULL) {
        return NULL;
    }

    subscribe_op->allocator = allocator;
    subscribe_op->base.operation_type = AWS_MOT_SUBSCRIBE;
    aws_ref_count_init(&subscribe_op->base.ref_count, subscribe_op, s_destroy_operation_subscribe);
    subscribe_op->base.impl = subscribe_op;

    if (aws_mqtt5_packet_subscribe_storage_init(&subscribe_op->options_storage, allocator, subscribe_options)) {
        goto error;
    }

    if (completion_options != NULL) {
        subscribe_op->completion_options = *completion_options;
    }

    return subscribe_op;

error:

    aws_mqtt5_operation_release(&subscribe_op->base);

    return NULL;
}

/*********************************************************************************************************************
 * Client storage options
 ********************************************************************************************************************/

int aws_mqtt5_client_options_validate(const struct aws_mqtt5_client_options *options) {
    if (options == NULL) {
        AWS_LOGF_ERROR(AWS_LS_MQTT5_GENERAL, "null mqtt5 client configuration options");
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    if (options->host_name.len == 0) {
        AWS_LOGF_ERROR(AWS_LS_MQTT5_GENERAL, "host name not set in mqtt5 client configuration");
        return aws_raise_error(AWS_ERROR_MQTT5_CLIENT_OPTIONS_VALIDATION);
    }

    if (options->port == 0) {
        AWS_LOGF_ERROR(AWS_LS_MQTT5_GENERAL, "port not set in mqtt5 client configuration");
        return aws_raise_error(AWS_ERROR_MQTT5_CLIENT_OPTIONS_VALIDATION);
    }

    if (options->bootstrap == NULL) {
        AWS_LOGF_ERROR(AWS_LS_MQTT5_GENERAL, "client bootstrap not set in mqtt5 client configuration");
        return aws_raise_error(AWS_ERROR_MQTT5_CLIENT_OPTIONS_VALIDATION);
    }

    /* forbid no-timeout until someone convinces me otherwise */
    if (options->socket_options == NULL || options->socket_options->type != AWS_SOCKET_STREAM ||
        options->socket_options->connect_timeout_ms == 0) {
        AWS_LOGF_ERROR(AWS_LS_MQTT5_GENERAL, "invalid socket options in mqtt5 client configuration");
        return aws_raise_error(AWS_ERROR_MQTT5_CLIENT_OPTIONS_VALIDATION);
    }

    if (options->http_proxy_options != NULL) {
        if (options->http_proxy_options->host.len == 0) {
            AWS_LOGF_ERROR(AWS_LS_MQTT5_GENERAL, "proxy host name not set in mqtt5 client configuration");
            return aws_raise_error(AWS_ERROR_MQTT5_CLIENT_OPTIONS_VALIDATION);
        }

        if (options->http_proxy_options->port == 0) {
            AWS_LOGF_ERROR(AWS_LS_MQTT5_GENERAL, "proxy port not set in mqtt5 client configuration");
            return aws_raise_error(AWS_ERROR_MQTT5_CLIENT_OPTIONS_VALIDATION);
        }
    }

    /* can't think of why you'd ever want an MQTT client without lifecycle event notifications */
    if (options->lifecycle_event_handler == NULL) {
        AWS_LOGF_ERROR(AWS_LS_MQTT5_GENERAL, "lifecycle event handler not set in mqtt5 client configuration");
        return aws_raise_error(AWS_ERROR_MQTT5_CLIENT_OPTIONS_VALIDATION);
    }

    if (aws_mqtt5_packet_connect_view_validate(options->connect_options, NULL)) {
        AWS_LOGF_ERROR(AWS_LS_MQTT5_GENERAL, "invalid CONNECT options in mqtt5 client configuration");
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

static void s_log_tls_connection_options(
    const struct aws_mqtt5_client_options_storage *options_storage,
    const struct aws_tls_connection_options *tls_options,
    enum aws_log_level level,
    const char *log_text) {
    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "(%p) aws_mqtt5_client_options_storage %s tls options set:",
        (void *)options_storage,
        log_text);
    if (tls_options->advertise_alpn_message && tls_options->alpn_list) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "(%p) aws_mqtt5_client_options_storage %s tls options alpn protocol list set to \"%s\"",
            (void *)options_storage,
            log_text,
            aws_string_c_str(tls_options->alpn_list));
    } else {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "(%p) aws_mqtt5_client_options_storage %s tls options alpn not used",
            (void *)options_storage,
            log_text);
    }

    if (tls_options->server_name) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "(%p) aws_mqtt5_client_options_storage %s tls options SNI value set to \"%s\"",
            (void *)options_storage,
            log_text,
            aws_string_c_str(tls_options->server_name));
    } else {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "(%p) aws_mqtt5_client_options_storage %s tls options SNI not used",
            (void *)options_storage,
            log_text);
    }

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "(%p) aws_mqtt5_client_options_storage %s tls options tls context set to (%p)",
        (void *)options_storage,
        log_text,
        (void *)(tls_options->ctx));
    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "(%p) aws_mqtt5_client_options_storage %s tls options handshake timeout set to %" PRIu32,
        (void *)options_storage,
        log_text,
        tls_options->timeout_ms);
}

void aws_mqtt5_client_options_storage_log(
    const struct aws_mqtt5_client_options_storage *options_storage,
    enum aws_log_level level) {
    struct aws_logger *logger = aws_logger_get();
    if (logger == NULL || logger->vtable->get_log_level(logger, AWS_LS_MQTT5_GENERAL) < level) {
        return;
    }

    /* TODO: constantly checking the log level at this point is kind of dumb but there's no better API atm */

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "(%p) aws_mqtt5_client_options_storage host name set to %s",
        (void *)options_storage,
        aws_string_c_str(options_storage->host_name));

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "(%p) aws_mqtt5_client_options_storage port set to %" PRIu16,
        (void *)options_storage,
        options_storage->port);
    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "(%p) aws_mqtt5_client_options_storage client bootstrap set to (%p)",
        (void *)options_storage,
        (void *)options_storage->bootstrap);

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "(%p) aws_mqtt5_client_options_storage socket options set to: type = %d, domain = %d, connect_timeout_ms = "
        "%" PRIu32,
        (void *)options_storage,
        (int)options_storage->socket_options.type,
        (int)options_storage->socket_options.domain,
        options_storage->socket_options.connect_timeout_ms);
    if (options_storage->socket_options.keepalive) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "(%p) aws_mqtt5_client_options_storage socket keepalive options set to: keep_alive_interval_sec = %" PRIu16
            ", "
            "keep_alive_timeout_sec = %" PRIu16 ", keep_alive_max_failed_probes = %" PRIu16,
            (void *)options_storage,
            options_storage->socket_options.keep_alive_interval_sec,
            options_storage->socket_options.keep_alive_timeout_sec,
            options_storage->socket_options.keep_alive_max_failed_probes);
    }

    if (options_storage->tls_options_ptr != NULL) {
        s_log_tls_connection_options(options_storage, options_storage->tls_options_ptr, level, "");
    }

    if (options_storage->http_proxy_config != NULL) {

        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "(%p) aws_mqtt5_client_options_storage using http proxy:",
            (void *)options_storage);

        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "(%p) aws_mqtt5_client_options_storage http proxy host name set to " PRInSTR,
            (void *)options_storage,
            AWS_BYTE_CURSOR_PRI(options_storage->http_proxy_options.host));

        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "(%p) aws_mqtt5_client_options_storage http proxy port set to %" PRIu16,
            (void *)options_storage,
            options_storage->http_proxy_options.port);

        if (options_storage->http_proxy_options.tls_options != NULL) {
            s_log_tls_connection_options(options_storage, options_storage->tls_options_ptr, level, "http proxy");
        }

        /* ToDo: add (and use) an API to proxy strategy that returns a debug string (Basic, Adaptive, etc...) */
        if (options_storage->http_proxy_options.proxy_strategy != NULL) {
            AWS_LOGF(
                level,
                AWS_LS_MQTT5_GENERAL,
                "(%p) aws_mqtt5_client_options_storage http proxy strategy set to (%p)",
                (void *)options_storage,
                (void *)options_storage->http_proxy_options.proxy_strategy);
        }
    }

    if (options_storage->websocket_handshake_transform != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "(%p) aws_mqtt5_client_options_storage enabling websockets",
            (void *)options_storage);

        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "(%p) aws_mqtt5_client_options_storage websocket handshake transform user data set to (%p)",
            (void *)options_storage,
            options_storage->websocket_handshake_transform_user_data);
    } else {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "(%p) mqtt5_client_options_storage disabling websockets",
            (void *)options_storage);
    }

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "(%p) aws_mqtt5_client_options_storage outbound topic aliasing behavior set to %d(%s)",
        (void *)options_storage,
        (int)options_storage->outbound_topic_aliasing_behavior,
        aws_mqtt5_outbound_topic_alias_behavior_type_to_c_string(options_storage->outbound_topic_aliasing_behavior));

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "(%p) aws_mqtt5_client_options_storage reconnect behavior set to %d(%s)",
        (void *)options_storage,
        (int)options_storage->reconnect_behavior,
        aws_mqtt5_client_reconnect_behavior_type_to_c_string(options_storage->reconnect_behavior));

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "(%p) mqtt5_client_options_storage reconnect delay min set to %" PRIu64 " ms, max set to %" PRIu64 " ms",
        (void *)options_storage,
        options_storage->min_reconnect_delay_ms,
        options_storage->max_reconnect_delay_ms);

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "(%p) aws_mqtt5_client_options_storage minimum necessary connection time in order to reset the reconnect delay "
        "set "
        "to %" PRIu64 " ms",
        (void *)options_storage,
        options_storage->min_connected_time_to_reset_reconnect_delay_ms);

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "(%p) aws_mqtt5_client_options_storage ping timeout interval set to %" PRIu32 " ms",
        (void *)options_storage,
        options_storage->ping_timeout_ms);

    AWS_LOGF(
        level, AWS_LS_MQTT5_GENERAL, "(%p) aws_mqtt5_client_options_storage connect options:", (void *)options_storage);

    aws_mqtt5_packet_connect_view_log(&options_storage->connect.storage_view, level);

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "(%p) aws_mqtt5_client_options_storage lifecycle event handler user data set to (%p)",
        (void *)options_storage,
        options_storage->lifecycle_event_handler_user_data);
}

void aws_mqtt5_client_options_storage_destroy(struct aws_mqtt5_client_options_storage *options_storage) {
    if (options_storage == NULL) {
        return;
    }

    aws_string_destroy(options_storage->host_name);
    aws_client_bootstrap_release(options_storage->bootstrap);

    aws_tls_connection_options_clean_up(&options_storage->tls_options);
    aws_http_proxy_config_destroy(options_storage->http_proxy_config);

    aws_mqtt5_packet_connect_storage_clean_up(&options_storage->connect);

    aws_mem_release(options_storage->allocator, options_storage);
}

struct aws_mqtt5_client_options_storage *aws_mqtt5_client_options_storage_new(
    struct aws_allocator *allocator,
    const struct aws_mqtt5_client_options *options) {
    AWS_PRECONDITION(allocator != NULL);
    AWS_PRECONDITION(options != NULL);

    if (aws_mqtt5_client_options_validate(options)) {
        return NULL;
    }

    struct aws_mqtt5_client_options_storage *options_storage =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt5_client_options_storage));
    if (options_storage == NULL) {
        return NULL;
    }

    options_storage->allocator = allocator;
    options_storage->host_name = aws_string_new_from_cursor(allocator, &options->host_name);
    if (options_storage->host_name == NULL) {
        goto error;
    }

    options_storage->port = options->port;
    options_storage->bootstrap = aws_client_bootstrap_acquire(options->bootstrap);
    options_storage->socket_options = *options->socket_options;

    if (options->tls_options != NULL) {
        if (aws_tls_connection_options_copy(&options_storage->tls_options, options->tls_options)) {
            goto error;
        }
        options_storage->tls_options_ptr = &options_storage->tls_options;
    }

    if (options->http_proxy_options != NULL) {
        options_storage->http_proxy_config =
            aws_http_proxy_config_new_from_proxy_options(allocator, options->http_proxy_options);
        if (options_storage->http_proxy_config == NULL) {
            goto error;
        }

        aws_http_proxy_options_init_from_config(
            &options_storage->http_proxy_options, options_storage->http_proxy_config);
    }

    options_storage->websocket_handshake_transform = options->websocket_handshake_transform;
    options_storage->websocket_handshake_transform_user_data = options->websocket_handshake_transform_user_data;

    options_storage->session_behavior = options->session_behavior;
    options_storage->outbound_topic_aliasing_behavior = options->outbound_topic_aliasing_behavior;

    options_storage->reconnect_behavior = options->reconnect_behavior;
    options_storage->min_reconnect_delay_ms = options->min_reconnect_delay_ms;
    options_storage->max_reconnect_delay_ms = options->max_reconnect_delay_ms;
    options_storage->min_connected_time_to_reset_reconnect_delay_ms =
        options->min_connected_time_to_reset_reconnect_delay_ms;

    options_storage->ping_timeout_ms = options->ping_timeout_ms;

    if (aws_mqtt5_packet_connect_storage_init(&options_storage->connect, allocator, options->connect_options)) {
        goto error;
    }

    options_storage->lifecycle_event_handler = options->lifecycle_event_handler;
    options_storage->lifecycle_event_handler_user_data = options->lifecycle_event_handler_user_data;

    return options_storage;

error:

    aws_mqtt5_client_options_storage_destroy(options_storage);

    return NULL;
}

struct aws_mqtt5_operation_disconnect *aws_mqtt5_operation_disconnect_acquire(
    struct aws_mqtt5_operation_disconnect *disconnect_op) {
    if (disconnect_op != NULL) {
        aws_mqtt5_operation_acquire(&disconnect_op->base);
    }

    return disconnect_op;
}

struct aws_mqtt5_operation_disconnect *aws_mqtt5_operation_disconnect_release(
    struct aws_mqtt5_operation_disconnect *disconnect_op) {
    if (disconnect_op != NULL) {
        aws_mqtt5_operation_release(&disconnect_op->base);
    }

    return NULL;
}
