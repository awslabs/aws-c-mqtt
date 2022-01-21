/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/private/v5/mqtt5_options_storage.h>

#include <aws/common/string.h>
#include <aws/io/channel_bootstrap.h>
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

size_t aws_mqtt5_user_property_set_size(struct aws_mqtt5_user_property_set *property_set) {
    return aws_array_list_length(&property_set->properties);
}

int aws_mqtt5_user_property_set_get_property(
    struct aws_mqtt5_user_property_set *property_set,
    size_t index,
    struct aws_mqtt5_user_property *property_out) {
    return aws_array_list_get_at(&property_set->properties, property_out, index);
}

static void s_aws_mqtt5_user_property_set_log(
    struct aws_mqtt5_user_property_set *property_set,
    void *log_context,
    const char *log_prefix) {
    size_t property_count = aws_mqtt5_user_property_set_size(property_set);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_OPERATION, "(%p) %s with %zu user properties:", log_context, log_prefix, property_count);

    for (size_t i = 0; i < property_count; ++i) {
        struct aws_mqtt5_user_property property;
        if (aws_mqtt5_user_property_set_get_property(property_set, i, &property)) {
            continue;
        }

        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_OPERATION,
            "(%p) %s user property %zu with name: \"" PRInSTR "\", value: \"" PRInSTR "\"",
            log_context,
            log_prefix,
            i,
            AWS_BYTE_CURSOR_PRI(property.name),
            AWS_BYTE_CURSOR_PRI(property.value));
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

int aws_mqtt5_packet_connect_view_validate(const struct aws_mqtt5_packet_connect_view *connect_options, struct aws_mqtt5_client *client) {
    if (connect_options == NULL) {
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    if (connect_options->receive_maximum != NULL) {
        if (*connect_options->receive_maximum == 0) {
            return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        }
    }

    if (connect_options->maximum_packet_size_bytes != NULL) {
        if (*connect_options->maximum_packet_size_bytes == 0) {
            return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        }
    }

    if (connect_options->will != NULL) {
        if (aws_mqtt5_packet_publish_view_validate(connect_options->will, client)) {
            return AWS_OP_ERR;
        }
    }

    return AWS_OP_SUCCESS;
}

static void s_aws_mqtt5_packet_connect_storage_log(struct aws_mqtt5_packet_connect_storage *connect_storage) {
    struct aws_logger *logger = aws_logger_get();
    if (logger == NULL || logger->vtable->get_log_level(logger, AWS_LS_MQTT5_OPERATION) < AWS_LL_DEBUG) {
        return;
    }

    /* TODO: constantly checking the log level at this point is kind of dumb but there's no better API atm */

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_OPERATION,
        "(%p) aws_mqtt5_packet_connect_storage keep alive interval set to %" PRIu32,
        (void *)connect_storage,
        connect_storage->keep_alive_interval_seconds);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_OPERATION,
        "(%p) aws_mqtt5_packet_connect_storage client id set to \"%s\"",
        (void *)connect_storage,
        aws_string_c_str(connect_storage->client_id));

    if (connect_storage->username != NULL) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_OPERATION,
            "(%p) aws_mqtt5_packet_connect_storage username set to \"%s\"",
            (void *)connect_storage,
            aws_string_c_str(connect_storage->username));
    }

    if (connect_storage->password_ptr != NULL) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_OPERATION, "(%p) aws_mqtt5_packet_connect_storage password set", (void *)connect_storage);
    }

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_OPERATION,
        "(%p) aws_mqtt5_packet_connect_storage session expiry interval set to %" PRIu32,
        (void *)connect_storage,
        connect_storage->session_expiry_interval_seconds);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_OPERATION,
        "(%p) aws_mqtt5_packet_connect_storage session behavior set to %d(%s)",
        (void *)connect_storage,
        (int)connect_storage->session_behavior,
        aws_mqtt5_client_session_behavior_type_to_c_string(connect_storage->session_behavior));

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_OPERATION,
        "(%p) aws_mqtt5_packet_connect_storage request response information set to %d",
        (void *)connect_storage,
        (int)connect_storage->request_response_information);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_OPERATION,
        "(%p) aws_mqtt5_packet_connect_storage request problem information set to %d",
        (void *)connect_storage,
        (int)connect_storage->request_problem_information);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_OPERATION,
        "(%p) aws_mqtt5_packet_connect_storage receive maximum set to %" PRIu16,
        (void *)connect_storage,
        connect_storage->receive_maximum);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_OPERATION,
        "(%p) aws_mqtt5_packet_connect_storage topic alias maximum set to %" PRIu16,
        (void *)connect_storage,
        connect_storage->topic_alias_maximum);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_OPERATION,
        "(%p) aws_mqtt5_packet_connect_storage maximum packet size set to %" PRIu32,
        (void *)connect_storage,
        connect_storage->maximum_packet_size_bytes);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_OPERATION,
        "(%p) aws_mqtt5_packet_connect_storage set will to (%p)",
        (void *)connect_storage,
        (void *)connect_storage->will);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_OPERATION,
        "(%p) aws_mqtt5_packet_connect_storage will delay interval set to %" PRIu32,
        (void *)connect_storage,
        connect_storage->will_delay_interval_seconds);

    s_aws_mqtt5_user_property_set_log(
        &connect_storage->user_properties, connect_storage, "aws_mqtt5_packet_connect_storage");
}

void aws_mqtt5_packet_storage_connect_clean_up(struct aws_mqtt5_packet_connect_storage *storage) {
    if (storage == NULL) {
        return;
    }

    aws_string_destroy(storage->client_id);
    aws_string_destroy(storage->username);
    aws_byte_buf_clean_up_secure(&storage->password);

    if (storage->will != NULL) {
        aws_mqtt5_operation_release(&storage->will->base);
    }

    aws_mqtt5_user_property_set_clean_up(&storage->user_properties);
}

int aws_mqtt5_packet_connect_storage_init(
    struct aws_mqtt5_packet_connect_storage *storage,
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_connect_view *view) {
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
    }

    if (view->password != NULL) {
        if (aws_byte_buf_init_copy_from_cursor(&storage->password, allocator, *view->password)) {
            return AWS_OP_ERR;
        }
        storage->password_ptr = &storage->password;
    }

    storage->session_expiry_interval_seconds = view->session_expiry_interval_seconds;
    storage->session_behavior = view->session_behavior;
    storage->request_response_information = view->request_response_information;
    storage->request_problem_information = view->request_problem_information;
    storage->receive_maximum = view->receive_maximum;
    storage->topic_alias_maximum = view->topic_alias_maximum;
    storage->maximum_packet_size_bytes = view->maximum_packet_size_bytes;

    if (view->will != NULL) {
        storage->will_payload = view->will_payload; /* TODO ref count */
        storage->will = aws_mqtt5_operation_publish_new(allocator, view->will, storage->will_payload, NULL);
        if (view->will != NULL && storage->will == NULL) {
            return AWS_OP_ERR;
        }
    }

    storage->will_delay_interval_seconds = view->will_delay_interval_seconds;

    if (aws_mqtt5_user_property_set_init(
            &storage->user_properties, allocator, view->user_property_count, view->user_properties)) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

static void s_destroy_operation_connect(void *object) {
    if (object == NULL) {
        return;
    }

    struct aws_mqtt5_operation_connect *connect_op = object;

    aws_mqtt5_packet_storage_connect_clean_up(&connect_op->options_storage);

    aws_mem_release(connect_op->allocator, connect_op);
}

struct aws_mqtt5_operation_connect *aws_mqtt5_operation_connect_new(
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_connect_view *connect_options) {
    AWS_PRECONDITION(allocator != NULL);
    AWS_PRECONDITION(connect_options != NULL);

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

    s_aws_mqtt5_packet_connect_storage_log(&connect_op->options_storage);

    return connect_op;

error:

    aws_mqtt5_operation_release(&connect_op->base);

    return NULL;
}

/*********************************************************************************************************************
 * Disconnect
 ********************************************************************************************************************/

static void s_aws_mqtt5_packet_disconnect_storage_log(struct aws_mqtt5_packet_disconnect_storage *disconnect_storage) {
    struct aws_logger *logger = aws_logger_get();
    if (logger == NULL || logger->vtable->get_log_level(logger, AWS_LS_MQTT5_OPERATION) < AWS_LL_DEBUG) {
        return;
    }

    /* TODO: constantly checking the log level at this point is kind of dumb but there's no better API atm */

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_OPERATION,
        "(%p) aws_mqtt5_packet_disconnect_storage reason code set to %d(%s)",
        (void *)disconnect_storage,
        (int)disconnect_storage->reason_code,
        aws_mqtt5_disconnect_reason_code_to_c_string(disconnect_storage->reason_code));

    if (disconnect_storage->session_expiry_interval_seconds_ptr != NULL) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_OPERATION,
            "(%p) aws_mqtt5_packet_disconnect_storage session expiry interval set to %" PRIu32,
            (void *)disconnect_storage,
            disconnect_storage->session_expiry_interval_seconds);
    }

    if (disconnect_storage->reason_string != NULL) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_OPERATION,
            "(%p) aws_mqtt5_packet_disconnect_storage reason string set to \"%s\"",
            (void *)disconnect_storage,
            aws_string_c_str(disconnect_storage->reason_string));
    }

    if (disconnect_storage->server_reference != NULL) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_OPERATION,
            "(%p) aws_mqtt5_packet_disconnect_storage server reference set to \"%s\"",
            (void *)disconnect_storage,
            aws_string_c_str(disconnect_storage->server_reference));
    }

    s_aws_mqtt5_user_property_set_log(
        &disconnect_storage->user_properties, disconnect_storage, "aws_mqtt5_packet_disconnect_storage");
}

void aws_aws_mqtt5_packet_disconnect_storage_clean_up(struct aws_mqtt5_packet_disconnect_storage *disconnect_storage) {
    if (disconnect_storage == NULL) {
        return;
    }

    aws_string_destroy(disconnect_storage->reason_string);
    aws_mqtt5_user_property_set_clean_up(&disconnect_storage->user_properties);
    aws_string_destroy(disconnect_storage->server_reference);
}

int aws_mqtt5_packet_disconnect_storage_init(
    struct aws_mqtt5_packet_disconnect_storage *disconnect_storage,
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_disconnect_view *disconnect_options) {
    disconnect_storage->reason_code = disconnect_options->reason_code;
    if (disconnect_options->session_expiry_interval_seconds != NULL) {
        disconnect_storage->session_expiry_interval_seconds = *disconnect_options->session_expiry_interval_seconds;
        disconnect_storage->session_expiry_interval_seconds_ptr = &disconnect_storage->session_expiry_interval_seconds;
    }

    if (disconnect_options->reason_string != NULL) {
        disconnect_storage->reason_string = aws_string_new_from_cursor(allocator, disconnect_options->reason_string);
        if (disconnect_storage->reason_string == NULL) {
            return AWS_OP_ERR;
        }
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
    }

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

    s_aws_mqtt5_packet_disconnect_storage_log(&disconnect_op->options_storage);

    return disconnect_op;

error:

    aws_mqtt5_operation_release(&disconnect_op->base);

    return NULL;
}

/*********************************************************************************************************************
 * Publish
 ********************************************************************************************************************/

static void s_aws_mqtt5_packet_publish_storage_log(struct aws_mqtt5_packet_publish_storage *publish_storage) {
    struct aws_logger *logger = aws_logger_get();
    if (logger == NULL || logger->vtable->get_log_level(logger, AWS_LS_MQTT5_OPERATION) < AWS_LL_DEBUG) {
        return;
    }

    /* TODO: constantly checking the log level at this point is kind of dumb but there's no better API atm */

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_CONFIG,
        "(%p) aws_mqtt5_packet_publish_storage qos set to %d",
        (void *)publish_storage,
        (int)publish_storage->qos);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_CONFIG,
        "(%p) aws_mqtt5_packet_publish_storage retain set to %d",
        (void *)publish_storage,
        (int)publish_storage->retain);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_OPERATION,
        "(%p) aws_mqtt5_packet_publish_storage topic set to \"%s\"",
        (void *)publish_storage,
        aws_string_c_str(publish_storage->topic));

    if (publish_storage->payload_format != AWS_MQTT5_PFI_NOT_SET) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_OPERATION,
            "(%p) aws_mqtt5_packet_publish_storage payload format indicator set to %d(%s)",
            (void *)publish_storage,
            (int)publish_storage->payload_format,
            aws_mqtt5_payload_format_indicator_to_c_string(publish_storage->payload_format));
    }

    if (publish_storage->message_expiry_interval_seconds_ptr != NULL) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_OPERATION,
            "(%p) aws_mqtt5_packet_publish_storage message expiry interval set to %" PRIu32,
            (void *)publish_storage,
            publish_storage->message_expiry_interval_seconds);
    }

    if (publish_storage->topic_alias_ptr != NULL) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_OPERATION,
            "(%p) aws_mqtt5_packet_publish_storage topic alias set to %" PRIu16,
            (void *)publish_storage,
            publish_storage->topic_alias);
    }

    if (publish_storage->response_topic != NULL) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_OPERATION,
            "(%p) aws_mqtt5_packet_publish_storage response topic set to \"%s\"",
            (void *)publish_storage,
            aws_string_c_str(publish_storage->response_topic));
    }

    if (publish_storage->correlation_data_ptr != NULL) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_CONFIG,
            "(%p) aws_mqtt5_packet_publish_storage - set correlation data",
            (void *)publish_storage);
    }

    if (publish_storage->content_type != NULL) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_OPERATION,
            "(%p) aws_mqtt5_packet_publish_storage content type set to \"%s\"",
            (void *)publish_storage,
            aws_string_c_str(publish_storage->content_type));
    }

    s_aws_mqtt5_user_property_set_log(
        &publish_storage->user_properties, publish_storage, "aws_mqtt5_packet_publish_storage");
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
    }

    if (publish_options->correlation_data != NULL) {
        if (aws_byte_buf_init_copy_from_cursor(
                &publish_storage->correlation_data, allocator, *publish_options->correlation_data)) {
            return AWS_OP_ERR;
        }

        publish_storage->correlation_data_ptr = &publish_storage->correlation_data;
    }

    if (publish_options->content_type != NULL) {
        publish_storage->content_type = aws_string_new_from_cursor(allocator, publish_options->content_type);
        if (publish_storage->content_type == NULL) {
            return AWS_OP_ERR;
        }
    }

    if (aws_mqtt5_user_property_set_init(
            &publish_storage->user_properties,
            allocator,
            publish_options->user_property_count,
            publish_options->user_properties)) {
        return AWS_OP_ERR;
    }

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

    s_aws_mqtt5_packet_publish_storage_log(&publish_op->options_storage);

    return publish_op;

error:

    aws_mqtt5_operation_release(&publish_op->base);

    return NULL;
}

/*********************************************************************************************************************
 * Unsubscribe
 ********************************************************************************************************************/

static void s_aws_mqtt5_packet_unsubscribe_storage_log(
    struct aws_mqtt5_packet_unsubscribe_storage *unsubscribe_storage) {
    struct aws_logger *logger = aws_logger_get();
    if (logger == NULL || logger->vtable->get_log_level(logger, AWS_LS_MQTT5_OPERATION) < AWS_LL_DEBUG) {
        return;
    }

    /* TODO: constantly checking the log level at this point is kind of dumb but there's no better API atm */

    size_t topic_count = aws_array_list_length(&unsubscribe_storage->topics);
    for (size_t i = 0; i < topic_count; ++i) {
        struct aws_byte_cursor topic_cursor;
        if (aws_array_list_get_at(&unsubscribe_storage->topics, &topic_cursor, i)) {
            continue;
        }

        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_OPERATION,
            "(%p) aws_mqtt5_packet_unsubscribe_storage topic %zu: \"" PRInSTR "\"",
            (void *)unsubscribe_storage,
            i,
            AWS_BYTE_CURSOR_PRI(topic_cursor));
    }

    s_aws_mqtt5_user_property_set_log(
        &unsubscribe_storage->user_properties, unsubscribe_storage, "aws_mqtt5_packet_unsubscribe_storage");
}

void aws_mqtt5_packet_unsubscribe_storage_clean_up(struct aws_mqtt5_packet_unsubscribe_storage *unsubscribe_storage) {
    if (unsubscribe_storage == NULL) {
        return;
    }

    aws_byte_buf_clean_up(&unsubscribe_storage->topic_storage);
    aws_array_list_clean_up(&unsubscribe_storage->topics);

    aws_mqtt5_user_property_set_clean_up(&unsubscribe_storage->user_properties);
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

static int s_validate_unsubscribe_view(const struct aws_mqtt5_packet_unsubscribe_view *unsubscribe_view) {
    if (unsubscribe_view->topic_count == 0) {
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    return AWS_OP_SUCCESS;
}

struct aws_mqtt5_operation_unsubscribe *aws_mqtt5_operation_unsubscribe_new(
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_unsubscribe_view *unsubscribe_options,
    const struct aws_mqtt5_unsubscribe_completion_options *completion_options) {
    AWS_PRECONDITION(allocator != NULL);
    AWS_PRECONDITION(unsubscribe_options != NULL);

    if (s_validate_unsubscribe_view(unsubscribe_options)) {
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

    s_aws_mqtt5_packet_unsubscribe_storage_log(&unsubscribe_op->options_storage);

    return unsubscribe_op;

error:

    aws_mqtt5_operation_release(&unsubscribe_op->base);

    return NULL;
}

/*********************************************************************************************************************
 * Subscribe
 ********************************************************************************************************************/

static void s_aws_mqtt5_packet_subscribe_storage_log(struct aws_mqtt5_packet_subscribe_storage *subscribe_storage) {
    struct aws_logger *logger = aws_logger_get();
    if (logger == NULL || logger->vtable->get_log_level(logger, AWS_LS_MQTT5_OPERATION) < AWS_LL_DEBUG) {
        return;
    }

    /* TODO: constantly checking the log level at this point is kind of dumb but there's no better API atm */

    size_t subscription_count = aws_array_list_length(&subscribe_storage->subscriptions);
    for (size_t i = 0; i < subscription_count; ++i) {
        struct aws_mqtt5_subscription_view *view = NULL;
        if (aws_array_list_get_at_ptr(&subscribe_storage->subscriptions, (void **)&view, i)) {
            continue;
        }

        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_OPERATION,
            "(%p) aws_mqtt5_packet_subscribe_storage subscription %zu: topic filter \"" PRInSTR
            "\", qos %d, no local %d, retain as "
            "published %d, retain handling %d(%s)",
            (void *)subscribe_storage,
            i,
            AWS_BYTE_CURSOR_PRI(view->topic_filter),
            (int)view->qos,
            (int)view->no_local,
            (int)view->retain_as_published,
            (int)view->retain_handling_type,
            aws_mqtt5_retain_handling_type_to_c_string(view->retain_handling_type));
    }

    if (subscribe_storage->subscription_identifier_ptr != NULL) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_OPERATION,
            "(%p) aws_mqtt5_packet_subscribe_storage subscription identifier set to %" PRIu32,
            (void *)subscribe_storage,
            subscribe_storage->subscription_identifier);
    }

    s_aws_mqtt5_user_property_set_log(
        &subscribe_storage->user_properties, subscribe_storage, "aws_mqtt5_packet_subscribe_storage");
}

void aws_mqtt5_packet_subscribe_storage_clean_up(struct aws_mqtt5_packet_subscribe_storage *subscribe_storage) {
    if (subscribe_storage == NULL) {
        return;
    }

    aws_byte_buf_clean_up(&subscribe_storage->topic_filter_storage);
    aws_array_list_clean_up(&subscribe_storage->subscriptions);

    aws_mqtt5_user_property_set_clean_up(&subscribe_storage->user_properties);
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

static int s_validate_subscribe_view(const struct aws_mqtt5_packet_subscribe_view *subscribe_view) {
    if (subscribe_view->subscription_count == 0) {
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    if (subscribe_view->subscription_identifier != NULL) {
        if (*subscribe_view->subscription_identifier > AWS_MQTT5_MAXIMUM_VARIABLE_LENGTH_INTEGER) {
            return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        }
    }

    return AWS_OP_SUCCESS;
}

struct aws_mqtt5_operation_subscribe *aws_mqtt5_operation_subscribe_new(
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_subscribe_view *subscribe_options,
    const struct aws_mqtt5_subscribe_completion_options *completion_options) {
    AWS_PRECONDITION(allocator != NULL);
    AWS_PRECONDITION(subscribe_options != NULL);

    if (s_validate_subscribe_view(subscribe_options)) {
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

    s_aws_mqtt5_packet_subscribe_storage_log(&subscribe_op->options_storage);

    return subscribe_op;

error:

    aws_mqtt5_operation_release(&subscribe_op->base);

    return NULL;
}

/*********************************************************************************************************************
 * Client storage options
 ********************************************************************************************************************/

static void s_log_tls_connection_options(
    struct aws_mqtt5_client_options_storage *options_storage,
    struct aws_tls_connection_options *tls_options,
    const char *log_text) {
    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_CONFIG,
        "(%p) aws_mqtt5_client_options_storage %s tls options set:",
        (void *)options_storage,
        log_text);
    if (tls_options->advertise_alpn_message && tls_options->alpn_list) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_CONFIG,
            "(%p) aws_mqtt5_client_options_storage %s tls options alpn protocol list set to \"%s\"",
            (void *)options_storage,
            log_text,
            aws_string_c_str(tls_options->alpn_list));
    } else {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_CONFIG,
            "(%p) aws_mqtt5_client_options_storage %s tls options alpn not used",
            (void *)options_storage,
            log_text);
    }

    if (tls_options->server_name) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_CONFIG,
            "(%p) aws_mqtt5_client_options_storage %s tls options SNI value set to \"%s\"",
            (void *)options_storage,
            log_text,
            aws_string_c_str(tls_options->server_name));
    } else {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_CONFIG,
            "(%p) aws_mqtt5_client_options_storage %s tls options SNI not used",
            (void *)options_storage,
            log_text);
    }

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_CONFIG,
        "(%p) aws_mqtt5_client_options_storage %s tls options tls context set to (%p)",
        (void *)options_storage,
        log_text,
        (void *)(tls_options->ctx));
    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_CONFIG,
        "(%p) aws_mqtt5_client_options_storage %s tls options handshake timeout set to %" PRIu32,
        (void *)options_storage,
        log_text,
        tls_options->timeout_ms);
}

static void s_aws_mqtt5_client_options_storage_log(struct aws_mqtt5_client_options_storage *options_storage) {
    struct aws_logger *logger = aws_logger_get();
    if (logger == NULL || logger->vtable->get_log_level(logger, AWS_LS_MQTT5_OPERATION) < AWS_LL_DEBUG) {
        return;
    }

    /* TODO: constantly checking the log level at this point is kind of dumb but there's no better API atm */

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_CONFIG,
        "(%p) aws_mqtt5_client_options_storage host name set to %s",
        (void *)options_storage,
        aws_string_c_str(options_storage->host_name));

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_CONFIG,
        "(%p) aws_mqtt5_client_options_storage port set to %" PRIu16,
        (void *)options_storage,
        options_storage->port);
    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_CONFIG,
        "(%p) aws_mqtt5_client_options_storage client bootstrap set to (%p)",
        (void *)options_storage,
        (void *)options_storage->bootstrap);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_CONFIG,
        "(%p) aws_mqtt5_client_options_storage socket options set to: type = %d, domain = %d, connect_timeout_ms = "
        "%" PRIu32,
        (void *)options_storage,
        (int)options_storage->socket_options.type,
        (int)options_storage->socket_options.domain,
        options_storage->socket_options.connect_timeout_ms);
    if (options_storage->socket_options.keepalive) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_CONFIG,
            "(%p) aws_mqtt5_client_options_storage socket keepalive options set to: keep_alive_interval_sec = %" PRIu16
            ", "
            "keep_alive_timeout_sec = %" PRIu16 ", keep_alive_max_failed_probes = %" PRIu16,
            (void *)options_storage,
            options_storage->socket_options.keep_alive_interval_sec,
            options_storage->socket_options.keep_alive_timeout_sec,
            options_storage->socket_options.keep_alive_max_failed_probes);
    }

    if (options_storage->tls_options_ptr != NULL) {
        s_log_tls_connection_options(options_storage, options_storage->tls_options_ptr, "");
    }

    if (options_storage->http_proxy_config != NULL) {

        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_CONFIG, "(%p) aws_mqtt5_client_options_storage using http proxy:", (void *)options_storage);

        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_CONFIG,
            "(%p) aws_mqtt5_client_options_storage http proxy host name set to " PRInSTR,
            (void *)options_storage,
            AWS_BYTE_CURSOR_PRI(options_storage->http_proxy_options.host));

        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_CONFIG,
            "(%p) aws_mqtt5_client_options_storage http proxy port set to %" PRIu16,
            (void *)options_storage,
            options_storage->http_proxy_options.port);

        if (options_storage->http_proxy_options.tls_options != NULL) {
            s_log_tls_connection_options(options_storage, options_storage->tls_options_ptr, "http proxy");
        }

        /* ToDo: add (and use) an API to proxy strategy that returns a debug string (Basic, Adaptive, etc...) */
        if (options_storage->http_proxy_options.proxy_strategy != NULL) {
            AWS_LOGF_DEBUG(
                AWS_LS_MQTT5_CONFIG,
                "(%p) aws_mqtt5_client_options_storage http proxy strategy set to (%p)",
                (void *)options_storage,
                (void *)options_storage->http_proxy_options.proxy_strategy);
        }
    }

    if (options_storage->websocket_handshake_transform != NULL) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_CONFIG, "(%p) aws_mqtt5_client_options_storage enabling websockets", (void *)options_storage);

        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_CONFIG,
            "(%p) aws_mqtt5_client_options_storage websocket handshake transform user data set to (%p)",
            (void *)options_storage,
            options_storage->websocket_handshake_transform_user_data);
    } else {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_CONFIG, "(%p) mqtt5_client_options_storage disabling websockets", (void *)options_storage);
    }

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_CONFIG,
        "(%p) aws_mqtt5_client_options_storage outbound topic aliasing behavior set to %d(%s)",
        (void *)options_storage,
        (int)options_storage->outbound_topic_aliasing_behavior,
        aws_mqtt5_outbound_topic_alias_behavior_type_to_c_string(options_storage->outbound_topic_aliasing_behavior));

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_CONFIG,
        "(%p) aws_mqtt5_client_options_storage reconnect behavior set to %d(%s)",
        (void *)options_storage,
        (int)options_storage->reconnect_behavior,
        aws_mqtt5_client_reconnect_behavior_type_to_c_string(options_storage->reconnect_behavior));

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_CONFIG,
        "(%p) mqtt5_client_options_storage reconnect delay min set to %" PRIu64 " ms, max set to %" PRIu64 " ms",
        (void *)options_storage,
        options_storage->min_reconnect_delay_ms,
        options_storage->max_reconnect_delay_ms);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_CONFIG,
        "(%p) aws_mqtt5_client_options_storage minimum necessary connection time in order to reset the reconnect delay "
        "set "
        "to %" PRIu64 " ms",
        (void *)options_storage,
        options_storage->min_connected_time_to_reset_reconnect_delay_ms);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_CONFIG,
        "(%p) aws_mqtt5_client_options_storage ping timeout interval set to %" PRIu32 " ms",
        (void *)options_storage,
        options_storage->ping_timeout_ms);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_CONFIG,
        "(%p) aws_mqtt5_client_options_storage connect data set to (%p)",
        (void *)options_storage,
        (void *)options_storage->connect);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_CONFIG,
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

    if (options_storage->connect) {
        aws_mqtt5_operation_release(&options_storage->connect->base);
    }

    aws_mem_release(options_storage->allocator, options_storage);
}

static int s_aws_mqtt5_client_options_validate(const struct aws_mqtt5_client_options *options) {
    if (options->host_name.len == 0) {
        return aws_raise_error(AWS_ERROR_MQTT_CONFIG_VALIDATION_HOST_NOT_SET);
    }

    if (options->port == 0) {
        return aws_raise_error(AWS_ERROR_MQTT_CONFIG_VALIDATION_PORT_NOT_SET);
    }

    if (options->bootstrap == NULL) {
        return aws_raise_error(AWS_ERROR_MQTT_CONFIG_VALIDATION_CLIENT_BOOTSTRAP_NOT_SET);
    }

    /* forbid no-timeout until someone convinces me otherwise */
    if (options->socket_options == NULL || options->socket_options->type != AWS_SOCKET_STREAM ||
        options->socket_options->connect_timeout_ms == 0) {
        return aws_raise_error(AWS_ERROR_MQTT_CONFIG_VALIDATION_INVALID_SOCKET_OPTIONS);
    }

    if (options->http_proxy_options != NULL) {
        if (options->http_proxy_options->host.len == 0) {
            return aws_raise_error(AWS_ERROR_MQTT_CONFIG_VALIDATION_PROXY_HOST_NOT_SET);
        }

        if (options->http_proxy_options->port == 0) {
            return aws_raise_error(AWS_ERROR_MQTT_CONFIG_VALIDATION_PROXY_PORT_NOT_SET);
        }
    }

    /* can't think of why you'd ever want an MQTT client without lifecycle event notifications */
    if (options->lifecycle_event_handler == NULL) {
        return aws_raise_error(AWS_ERROR_MQTT_CONFIG_VALIDATION_NO_LIFECYCLE_HANDLER_SET);
    }

    if (options->connect_options == NULL) {
        return aws_raise_error(AWS_ERROR_MQTT_CONFIG_VALIDATION_CONNECT_OPTIONS_NOT_SET);
    }

    if (options->connect_options->will != NULL) {
        if (!aws_mqtt_is_valid_topic(&options->connect_options->will->topic)) {
            return aws_raise_error(AWS_ERROR_MQTT_CONFIG_VALIDATION_INVALID_WILL_TOPIC);
        }
    }

    return AWS_OP_SUCCESS;
}

struct aws_mqtt5_client_options_storage *aws_mqtt5_client_options_storage_new(
    struct aws_allocator *allocator,
    const struct aws_mqtt5_client_options *options) {
    AWS_PRECONDITION(allocator != NULL);
    AWS_PRECONDITION(options != NULL);

    if (s_aws_mqtt5_client_options_validate(options)) {
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

    options_storage->connect = aws_mqtt5_operation_connect_new(allocator, options->connect_options);
    if (options_storage->connect == NULL) {
        goto error;
    }

    options_storage->lifecycle_event_handler = options->lifecycle_event_handler;
    options_storage->lifecycle_event_handler_user_data = options->lifecycle_event_handler_user_data;

    s_aws_mqtt5_client_options_storage_log(options_storage);

    return options_storage;

error:

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
