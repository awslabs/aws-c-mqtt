/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/private/v5/mqtt5_options_storage.h>

#include <aws/common/clock.h>
#include <aws/common/string.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/stream.h>
#include <aws/mqtt/private/v5/mqtt5_client_impl.h>
#include <aws/mqtt/private/v5/mqtt5_utils.h>
#include <aws/mqtt/v5/mqtt5_client.h>

#include <inttypes.h>

/*********************************************************************************************************************
 * Property set
 ********************************************************************************************************************/

int aws_mqtt5_user_property_set_init(
    struct aws_mqtt5_user_property_set *property_set,
    struct aws_allocator *allocator) {
    AWS_ZERO_STRUCT(*property_set);

    if (aws_array_list_init_dynamic(&property_set->properties, allocator, 0, sizeof(struct aws_mqtt5_user_property))) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

int aws_mqtt5_user_property_set_init_with_storage(
    struct aws_mqtt5_user_property_set *property_set,
    struct aws_allocator *allocator,
    struct aws_byte_buf *storage,
    size_t property_count,
    const struct aws_mqtt5_user_property *properties) {
    AWS_ZERO_STRUCT(*property_set);

    if (aws_array_list_init_dynamic(
            &property_set->properties, allocator, property_count, sizeof(struct aws_mqtt5_user_property))) {
        goto error;
    }

    for (size_t i = 0; i < property_count; ++i) {
        const struct aws_mqtt5_user_property *property = &properties[i];
        struct aws_mqtt5_user_property property_clone = *property;

        if (aws_byte_buf_append_and_update(storage, &property_clone.name)) {
            goto error;
        }

        if (aws_byte_buf_append_and_update(storage, &property_clone.value)) {
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

int aws_mqtt5_user_property_set_add_stored_property(
    struct aws_mqtt5_user_property_set *property_set,
    struct aws_mqtt5_user_property *property) {
    return aws_array_list_push_back(&property_set->properties, property);
}

static void s_aws_mqtt5_user_property_set_log(
    const struct aws_mqtt5_user_property *properties,
    size_t property_count,
    void *log_context,
    enum aws_log_level level,
    const char *log_prefix) {

    AWS_LOGF(
        level, AWS_LS_MQTT5_GENERAL, "id=%p: %s with %zu user properties:", log_context, log_prefix, property_count);

    for (size_t i = 0; i < property_count; ++i) {
        const struct aws_mqtt5_user_property *property = &properties[i];

        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: %s user property %zu with name \"" PRInSTR "\", value \"" PRInSTR "\"",
            log_context,
            log_prefix,
            i,
            AWS_BYTE_CURSOR_PRI(property->name),
            AWS_BYTE_CURSOR_PRI(property->value));
    }
}

static size_t s_aws_mqtt5_user_property_set_compute_storage_size(
    const struct aws_mqtt5_user_property *properties,
    size_t property_count) {
    size_t storage_size = 0;
    for (size_t i = 0; i < property_count; ++i) {
        const struct aws_mqtt5_user_property *property = &properties[i];
        storage_size += property->name.len;
        storage_size += property->value.len;
    }

    return storage_size;
}

static int s_aws_mqtt5_user_property_set_validate(
    const struct aws_mqtt5_user_property *properties,
    size_t property_count,
    const char *log_prefix,
    void *log_context) {
    if (properties == NULL) {
        if (property_count == 0) {
            return AWS_OP_SUCCESS;
        } else {
            AWS_LOGF_ERROR(
                AWS_LS_MQTT5_GENERAL,
                "id=%p: %s - Invalid user property configuration, null properties, non-zero property count",
                log_context,
                log_prefix);
            return aws_raise_error(AWS_ERROR_MQTT5_USER_PROPERTY_VALIDATION);
        }
    }

    if (property_count > AWS_MQTT5_CLIENT_MAXIMUM_USER_PROPERTIES) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_GENERAL,
            "id=%p: %s - user property limit (%d) exceeded (%zu)",
            log_context,
            log_prefix,
            (int)AWS_MQTT5_CLIENT_MAXIMUM_USER_PROPERTIES,
            property_count);
        return aws_raise_error(AWS_ERROR_MQTT5_USER_PROPERTY_VALIDATION);
    }

    for (size_t i = 0; i < property_count; ++i) {
        const struct aws_mqtt5_user_property *property = &properties[i];
        if (property->name.len > UINT16_MAX) {
            AWS_LOGF_ERROR(
                AWS_LS_MQTT5_GENERAL,
                "id=%p: %s - user property #%zu name too long (%zu)",
                log_context,
                log_prefix,
                i,
                property->name.len);
            return aws_raise_error(AWS_ERROR_MQTT5_USER_PROPERTY_VALIDATION);
        }
        if (property->value.len > UINT16_MAX) {
            AWS_LOGF_ERROR(
                AWS_LS_MQTT5_GENERAL,
                "id=%p: %s - user property #%zu value too long (%zu)",
                log_context,
                log_prefix,
                i,
                property->value.len);
            return aws_raise_error(AWS_ERROR_MQTT5_USER_PROPERTY_VALIDATION);
        }
    }

    return AWS_OP_SUCCESS;
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

void aws_mqtt5_operation_complete(struct aws_mqtt5_operation *operation, int error_code, const void *associated_view) {
    AWS_FATAL_ASSERT(operation->vtable != NULL);
    if (operation->vtable->aws_mqtt5_operation_completion_fn != NULL) {
        (*operation->vtable->aws_mqtt5_operation_completion_fn)(operation, error_code, associated_view);
    }
}

void aws_mqtt5_operation_set_packet_id(struct aws_mqtt5_operation *operation, aws_mqtt5_packet_id_t packet_id) {
    AWS_FATAL_ASSERT(operation->vtable != NULL);
    if (operation->vtable->aws_mqtt5_operation_set_packet_id_fn != NULL) {
        (*operation->vtable->aws_mqtt5_operation_set_packet_id_fn)(operation, packet_id);
    }
}

aws_mqtt5_packet_id_t aws_mqtt5_operation_get_packet_id(const struct aws_mqtt5_operation *operation) {
    AWS_FATAL_ASSERT(operation->vtable != NULL);
    if (operation->vtable->aws_mqtt5_operation_get_packet_id_address_fn != NULL) {
        aws_mqtt5_packet_id_t *packet_id_ptr =
            (*operation->vtable->aws_mqtt5_operation_get_packet_id_address_fn)(operation);
        if (packet_id_ptr != NULL) {
            return *packet_id_ptr;
        }
    }

    return 0;
}

aws_mqtt5_packet_id_t *aws_mqtt5_operation_get_packet_id_address(const struct aws_mqtt5_operation *operation) {
    AWS_FATAL_ASSERT(operation->vtable != NULL);
    if (operation->vtable->aws_mqtt5_operation_get_packet_id_address_fn != NULL) {
        return (*operation->vtable->aws_mqtt5_operation_get_packet_id_address_fn)(operation);
    }

    return NULL;
}

static struct aws_mqtt5_operation_vtable s_empty_operation_vtable = {
    .aws_mqtt5_operation_completion_fn = NULL,
    .aws_mqtt5_operation_set_packet_id_fn = NULL,
    .aws_mqtt5_operation_get_packet_id_address_fn = NULL,
};

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

    if (connect_options->client_id.len > UINT16_MAX) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_GENERAL, "id=%p: aws_mqtt5_packet_connect_view - client id too long", (void *)connect_options);
        return aws_raise_error(AWS_ERROR_MQTT5_CONNECT_OPTIONS_VALIDATION);
    }

    if (connect_options->username != NULL) {
        if (connect_options->username->len > UINT16_MAX) {
            AWS_LOGF_ERROR(
                AWS_LS_MQTT5_GENERAL,
                "id=%p: aws_mqtt5_packet_connect_view - username too long",
                (void *)connect_options);
            return aws_raise_error(AWS_ERROR_MQTT5_CONNECT_OPTIONS_VALIDATION);
        }
    }

    if (connect_options->password != NULL) {
        if (connect_options->password->len > UINT16_MAX) {
            AWS_LOGF_ERROR(
                AWS_LS_MQTT5_GENERAL,
                "id=%p: aws_mqtt5_packet_connect_view - password too long",
                (void *)connect_options);
            return aws_raise_error(AWS_ERROR_MQTT5_CONNECT_OPTIONS_VALIDATION);
        }
    }

    if (connect_options->receive_maximum != NULL) {
        if (*connect_options->receive_maximum == 0) {
            AWS_LOGF_ERROR(
                AWS_LS_MQTT5_GENERAL,
                "id=%p: aws_mqtt5_packet_connect_view - receive maximum property of CONNECT packet may not be zero.",
                (void *)connect_options);
            return aws_raise_error(AWS_ERROR_MQTT5_CONNECT_OPTIONS_VALIDATION);
        }
    }

    if (connect_options->maximum_packet_size_bytes != NULL) {
        if (*connect_options->maximum_packet_size_bytes == 0) {
            AWS_LOGF_ERROR(
                AWS_LS_MQTT5_GENERAL,
                "id=%p: aws_mqtt5_packet_connect_view - maximum packet size property of CONNECT packet may not be "
                "zero.",
                (void *)connect_options);
            return aws_raise_error(AWS_ERROR_MQTT5_CONNECT_OPTIONS_VALIDATION);
        }
    }

    if (connect_options->will != NULL) {
        const struct aws_mqtt5_packet_publish_view *will_options = connect_options->will;
        if (aws_mqtt5_packet_publish_view_validate(will_options, client)) {
            AWS_LOGF_ERROR(
                AWS_LS_MQTT5_GENERAL,
                "id=%p: aws_mqtt5_packet_connect_view - CONNECT packet Will message failed validation",
                (void *)connect_options);
            return AWS_OP_ERR;
        }

        if (will_options->payload.len > UINT16_MAX) {
            AWS_LOGF_ERROR(
                AWS_LS_MQTT5_GENERAL,
                "id=%p: aws_mqtt5_packet_connect_view - will payload larger than %d",
                (void *)connect_options,
                (int)UINT16_MAX);
            return aws_raise_error(AWS_ERROR_MQTT5_CONNECT_OPTIONS_VALIDATION);
        }
    }

    if (connect_options->request_problem_information != NULL) {
        if (*connect_options->request_problem_information > 1) {
            AWS_LOGF_ERROR(
                AWS_LS_MQTT5_GENERAL,
                "id=%p: aws_mqtt5_packet_connect_view - CONNECT packet request problem information has invalid value",
                (void *)connect_options);
            return aws_raise_error(AWS_ERROR_MQTT5_CONNECT_OPTIONS_VALIDATION);
        }
    }

    if (connect_options->request_response_information != NULL) {
        if (*connect_options->request_response_information > 1) {
            AWS_LOGF_ERROR(
                AWS_LS_MQTT5_GENERAL,
                "id=%p: aws_mqtt5_packet_connect_view - CONNECT packet request response information has invalid value",
                (void *)connect_options);
            return aws_raise_error(AWS_ERROR_MQTT5_CONNECT_OPTIONS_VALIDATION);
        }
    }

    if (s_aws_mqtt5_user_property_set_validate(
            connect_options->user_properties,
            connect_options->user_property_count,
            "aws_mqtt5_packet_connect_view",
            (void *)connect_options)) {
        return AWS_OP_ERR;
    }

    if (connect_options->authentication_method != NULL || connect_options->authentication_data != NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_connect_view - CONNECT packet has unsupported authentication fields set.",
            (void *)connect_options);
        return aws_raise_error(AWS_ERROR_MQTT5_CONNECT_OPTIONS_VALIDATION);
    }

    return AWS_OP_SUCCESS;
}

void aws_mqtt5_packet_connect_view_log(
    const struct aws_mqtt5_packet_connect_view *connect_view,
    enum aws_log_level level) {
    struct aws_logger *temp_logger = aws_logger_get();
    if (temp_logger == NULL || temp_logger->vtable->get_log_level(temp_logger, AWS_LS_MQTT5_GENERAL) < level) {
        return;
    }

    /* TODO: constantly checking the log level at this point is kind of dumb but there's no better API atm */

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "id=%p: aws_mqtt5_packet_connect_view keep alive interval set to %" PRIu16,
        (void *)connect_view,
        connect_view->keep_alive_interval_seconds);

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "id=%p: aws_mqtt5_packet_connect_view client id set to \"" PRInSTR "\"",
        (void *)connect_view,
        AWS_BYTE_CURSOR_PRI(connect_view->client_id));

    if (connect_view->username != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_connect_view username set to \"" PRInSTR "\"",
            (void *)connect_view,
            AWS_BYTE_CURSOR_PRI(*connect_view->username));
    }

    if (connect_view->password != NULL) {
        AWS_LOGF(
            level, AWS_LS_MQTT5_GENERAL, "id=%p: aws_mqtt5_packet_connect_view password set", (void *)connect_view);
    }

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "id=%p: aws_mqtt5_packet_connect_view clean start set to %d",
        (void *)connect_view,
        (int)(connect_view->clean_start ? 1 : 0));

    if (connect_view->session_expiry_interval_seconds != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_connect_view session expiry interval set to %" PRIu32,
            (void *)connect_view,
            *connect_view->session_expiry_interval_seconds);
    }

    if (connect_view->request_response_information != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_connect_view request response information set to %d",
            (void *)connect_view,
            (int)*connect_view->request_response_information);
    }

    if (connect_view->request_problem_information) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_connect_view request problem information set to %d",
            (void *)connect_view,
            (int)*connect_view->request_problem_information);
    }

    if (connect_view->receive_maximum != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_connect_view receive maximum set to %" PRIu16,
            (void *)connect_view,
            *connect_view->receive_maximum);
    }

    if (connect_view->topic_alias_maximum != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_connect_view topic alias maximum set to %" PRIu16,
            (void *)connect_view,
            *connect_view->topic_alias_maximum);
    }

    if (connect_view->maximum_packet_size_bytes != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_connect_view maximum packet size set to %" PRIu32,
            (void *)connect_view,
            *connect_view->maximum_packet_size_bytes);
    }

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "id=%p: aws_mqtt5_packet_connect_view set will to (%p)",
        (void *)connect_view,
        (void *)connect_view->will);

    if (connect_view->will != NULL) {
        aws_mqtt5_packet_publish_view_log(connect_view->will, level);
    }

    if (connect_view->will_delay_interval_seconds != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_connect_view will delay interval set to %" PRIu32,
            (void *)connect_view,
            *connect_view->will_delay_interval_seconds);
    }

    s_aws_mqtt5_user_property_set_log(
        connect_view->user_properties,
        connect_view->user_property_count,
        (void *)connect_view,
        level,
        "aws_mqtt5_packet_connect_view");

    if (connect_view->authentication_method != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_connect_view authentication method set",
            (void *)connect_view);
    }

    if (connect_view->password != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_connect_view authentication data set",
            (void *)connect_view);
    }
}

void aws_mqtt5_packet_connect_storage_clean_up(struct aws_mqtt5_packet_connect_storage *storage) {
    if (storage == NULL) {
        return;
    }

    if (storage->will != NULL) {
        aws_mqtt5_packet_publish_storage_clean_up(storage->will);
        aws_mem_release(storage->allocator, storage->will);
    }

    aws_mqtt5_user_property_set_clean_up(&storage->user_properties);

    aws_byte_buf_clean_up_secure(&storage->storage);
}

static size_t s_aws_mqtt5_packet_connect_compute_storage_size(const struct aws_mqtt5_packet_connect_view *view) {
    if (view == NULL) {
        return 0;
    }

    size_t storage_size = 0;

    storage_size += view->client_id.len;
    if (view->username != NULL) {
        storage_size += view->username->len;
    }
    if (view->password != NULL) {
        storage_size += view->password->len;
    }

    storage_size +=
        s_aws_mqtt5_user_property_set_compute_storage_size(view->user_properties, view->user_property_count);

    if (view->authentication_method != NULL) {
        storage_size += view->authentication_method->len;
    }

    if (view->authentication_data != NULL) {
        storage_size += view->authentication_data->len;
    }

    return storage_size;
}

void aws_mqtt5_packet_connect_view_init_from_storage(
    struct aws_mqtt5_packet_connect_view *view,
    const struct aws_mqtt5_packet_connect_storage *storage) {
    view->keep_alive_interval_seconds = storage->keep_alive_interval_seconds;
    view->client_id = storage->client_id;
    view->username = storage->username_ptr;
    view->password = storage->password_ptr;
    view->clean_start = storage->clean_start;
    view->session_expiry_interval_seconds = storage->session_expiry_interval_seconds_ptr;
    view->request_response_information = storage->request_response_information_ptr;
    view->request_problem_information = storage->request_problem_information_ptr;
    view->receive_maximum = storage->receive_maximum_ptr;
    view->topic_alias_maximum = storage->topic_alias_maximum_ptr;
    view->maximum_packet_size_bytes = storage->maximum_packet_size_bytes_ptr;
    if (storage->will != NULL) {
        view->will = &storage->will->storage_view;
    }
    view->will_delay_interval_seconds = storage->will_delay_interval_seconds_ptr;
    view->user_property_count = aws_mqtt5_user_property_set_size(&storage->user_properties);
    view->user_properties = storage->user_properties.properties.data;
    view->authentication_method = storage->authentication_method_ptr;
    view->authentication_data = storage->authentication_data_ptr;
}

int aws_mqtt5_packet_connect_storage_init(
    struct aws_mqtt5_packet_connect_storage *storage,
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_connect_view *view) {
    AWS_ZERO_STRUCT(*storage);

    size_t storage_capacity = s_aws_mqtt5_packet_connect_compute_storage_size(view);
    if (aws_byte_buf_init(&storage->storage, allocator, storage_capacity)) {
        return AWS_OP_ERR;
    }

    storage->allocator = allocator;
    storage->keep_alive_interval_seconds = view->keep_alive_interval_seconds;

    storage->client_id = view->client_id;
    if (aws_byte_buf_append_and_update(&storage->storage, &storage->client_id)) {
        return AWS_OP_ERR;
    }

    if (view->username != NULL) {
        storage->username = *view->username;
        if (aws_byte_buf_append_and_update(&storage->storage, &storage->username)) {
            return AWS_OP_ERR;
        }

        storage->username_ptr = &storage->username;
    }

    if (view->password != NULL) {
        storage->password = *view->password;
        if (aws_byte_buf_append_and_update(&storage->storage, &storage->password)) {
            return AWS_OP_ERR;
        }

        storage->password_ptr = &storage->password;
    }

    storage->clean_start = view->clean_start;

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

    if (aws_mqtt5_user_property_set_init_with_storage(
            &storage->user_properties,
            allocator,
            &storage->storage,
            view->user_property_count,
            view->user_properties)) {
        return AWS_OP_ERR;
    }

    if (view->authentication_method != NULL) {
        storage->authentication_method = *view->authentication_method;
        if (aws_byte_buf_append_and_update(&storage->storage, &storage->authentication_method)) {
            return AWS_OP_ERR;
        }

        storage->authentication_method_ptr = &storage->authentication_method;
    }

    if (view->authentication_data != NULL) {
        storage->authentication_data = *view->authentication_data;
        if (aws_byte_buf_append_and_update(&storage->storage, &storage->authentication_data)) {
            return AWS_OP_ERR;
        }

        storage->authentication_data_ptr = &storage->authentication_data;
    }

    aws_mqtt5_packet_connect_view_init_from_storage(&storage->storage_view, storage);

    return AWS_OP_SUCCESS;
}

int aws_mqtt5_packet_connect_storage_init_from_external_storage(
    struct aws_mqtt5_packet_connect_storage *connect_storage,
    struct aws_allocator *allocator) {
    AWS_ZERO_STRUCT(*connect_storage);

    if (aws_mqtt5_user_property_set_init(&connect_storage->user_properties, allocator)) {
        return AWS_OP_ERR;
    }

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
    connect_op->base.vtable = &s_empty_operation_vtable;
    connect_op->base.packet_type = AWS_MQTT5_PT_CONNECT;
    aws_ref_count_init(&connect_op->base.ref_count, connect_op, s_destroy_operation_connect);
    connect_op->base.impl = connect_op;

    if (aws_mqtt5_packet_connect_storage_init(&connect_op->options_storage, allocator, connect_options)) {
        goto error;
    }

    connect_op->base.packet_view = &connect_op->options_storage.storage_view;

    return connect_op;

error:

    aws_mqtt5_operation_release(&connect_op->base);

    return NULL;
}

/*********************************************************************************************************************
 * Connack
 ********************************************************************************************************************/

static size_t s_aws_mqtt5_packet_connack_compute_storage_size(const struct aws_mqtt5_packet_connack_view *view) {
    if (view == NULL) {
        return 0;
    }

    size_t storage_size = 0;

    if (view->assigned_client_identifier != NULL) {
        storage_size += view->assigned_client_identifier->len;
    }

    if (view->reason_string != NULL) {
        storage_size += view->reason_string->len;
    }

    if (view->response_information != NULL) {
        storage_size += view->response_information->len;
    }

    if (view->server_reference != NULL) {
        storage_size += view->server_reference->len;
    }

    if (view->authentication_method != NULL) {
        storage_size += view->authentication_method->len;
    }

    if (view->authentication_data != NULL) {
        storage_size += view->authentication_data->len;
    }

    storage_size +=
        s_aws_mqtt5_user_property_set_compute_storage_size(view->user_properties, view->user_property_count);

    return storage_size;
}

int aws_mqtt5_packet_connack_storage_init(
    struct aws_mqtt5_packet_connack_storage *connack_storage,
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_connack_view *connack_view) {

    AWS_ZERO_STRUCT(*connack_storage);
    size_t storage_capacity = s_aws_mqtt5_packet_connack_compute_storage_size(connack_view);
    if (aws_byte_buf_init(&connack_storage->storage, allocator, storage_capacity)) {
        return AWS_OP_ERR;
    }

    connack_storage->allocator = allocator;

    connack_storage->session_present = connack_view->session_present;
    connack_storage->reason_code = connack_view->reason_code;

    if (connack_view->session_expiry_interval != NULL) {
        connack_storage->session_expiry_interval = *connack_view->session_expiry_interval;
        connack_storage->session_expiry_interval_ptr = &connack_storage->session_expiry_interval;
    }

    if (connack_view->receive_maximum != NULL) {
        connack_storage->receive_maximum = *connack_view->receive_maximum;
        connack_storage->receive_maximum_ptr = &connack_storage->receive_maximum;
    }

    if (connack_view->maximum_qos != NULL) {
        connack_storage->maximum_qos = *connack_view->maximum_qos;
        connack_storage->maximum_qos_ptr = &connack_storage->maximum_qos;
    }

    if (connack_view->retain_available != NULL) {
        connack_storage->retain_available = *connack_view->retain_available;
        connack_storage->retain_available_ptr = &connack_storage->retain_available;
    }

    if (connack_view->maximum_packet_size != NULL) {
        connack_storage->maximum_packet_size = *connack_view->maximum_packet_size;
        connack_storage->maximum_packet_size_ptr = &connack_storage->maximum_packet_size;
    }

    if (connack_view->assigned_client_identifier != NULL) {
        connack_storage->assigned_client_identifier = *connack_view->assigned_client_identifier;
        if (aws_byte_buf_append_and_update(&connack_storage->storage, &connack_storage->assigned_client_identifier)) {
            return AWS_OP_ERR;
        }

        connack_storage->assigned_client_identifier_ptr = &connack_storage->assigned_client_identifier;
    }

    if (connack_view->topic_alias_maximum != NULL) {
        connack_storage->topic_alias_maximum = *connack_view->topic_alias_maximum;
        connack_storage->topic_alias_maximum_ptr = &connack_storage->topic_alias_maximum;
    }

    if (connack_view->reason_string != NULL) {
        connack_storage->reason_string = *connack_view->reason_string;
        if (aws_byte_buf_append_and_update(&connack_storage->storage, &connack_storage->reason_string)) {
            return AWS_OP_ERR;
        }

        connack_storage->reason_string_ptr = &connack_storage->reason_string;
    }

    if (connack_view->wildcard_subscriptions_available != NULL) {
        connack_storage->wildcard_subscriptions_available = *connack_view->wildcard_subscriptions_available;
        connack_storage->wildcard_subscriptions_available_ptr = &connack_storage->wildcard_subscriptions_available;
    }

    if (connack_view->subscription_identifiers_available != NULL) {
        connack_storage->subscription_identifiers_available = *connack_view->subscription_identifiers_available;
        connack_storage->subscription_identifiers_available_ptr = &connack_storage->subscription_identifiers_available;
    }

    if (connack_view->shared_subscriptions_available != NULL) {
        connack_storage->shared_subscriptions_available = *connack_view->shared_subscriptions_available;
        connack_storage->shared_subscriptions_available_ptr = &connack_storage->shared_subscriptions_available;
    }

    if (connack_view->server_keep_alive != NULL) {
        connack_storage->server_keep_alive = *connack_view->server_keep_alive;
        connack_storage->server_keep_alive_ptr = &connack_storage->server_keep_alive;
    }

    if (connack_view->response_information != NULL) {
        connack_storage->response_information = *connack_view->response_information;
        if (aws_byte_buf_append_and_update(&connack_storage->storage, &connack_storage->response_information)) {
            return AWS_OP_ERR;
        }

        connack_storage->response_information_ptr = &connack_storage->response_information;
    }

    if (connack_view->server_reference != NULL) {
        connack_storage->server_reference = *connack_view->server_reference;
        if (aws_byte_buf_append_and_update(&connack_storage->storage, &connack_storage->server_reference)) {
            return AWS_OP_ERR;
        }

        connack_storage->server_reference_ptr = &connack_storage->server_reference;
    }

    if (connack_view->authentication_method != NULL) {
        connack_storage->authentication_method = *connack_view->authentication_method;
        if (aws_byte_buf_append_and_update(&connack_storage->storage, &connack_storage->authentication_method)) {
            return AWS_OP_ERR;
        }

        connack_storage->authentication_method_ptr = &connack_storage->authentication_method;
    }

    if (connack_view->authentication_data != NULL) {
        connack_storage->authentication_data = *connack_view->authentication_data;
        if (aws_byte_buf_append_and_update(&connack_storage->storage, &connack_storage->authentication_data)) {
            return AWS_OP_ERR;
        }

        connack_storage->authentication_data_ptr = &connack_storage->authentication_data;
    }

    if (aws_mqtt5_user_property_set_init_with_storage(
            &connack_storage->user_properties,
            allocator,
            &connack_storage->storage,
            connack_view->user_property_count,
            connack_view->user_properties)) {
        return AWS_OP_ERR;
    }

    aws_mqtt5_packet_connack_view_init_from_storage(&connack_storage->storage_view, connack_storage);

    return AWS_OP_SUCCESS;
}

int aws_mqtt5_packet_connack_storage_init_from_external_storage(
    struct aws_mqtt5_packet_connack_storage *connack_storage,
    struct aws_allocator *allocator) {
    AWS_ZERO_STRUCT(*connack_storage);

    if (aws_mqtt5_user_property_set_init(&connack_storage->user_properties, allocator)) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

void aws_mqtt5_packet_connack_storage_clean_up(struct aws_mqtt5_packet_connack_storage *connack_storage) {
    if (connack_storage == NULL) {
        return;
    }

    aws_mqtt5_user_property_set_clean_up(&connack_storage->user_properties);
    aws_byte_buf_clean_up(&connack_storage->storage);
}

void aws_mqtt5_packet_connack_view_log(
    const struct aws_mqtt5_packet_connack_view *connack_view,
    enum aws_log_level level) {
    struct aws_logger *temp_logger = aws_logger_get();
    if (temp_logger == NULL || temp_logger->vtable->get_log_level(temp_logger, AWS_LS_MQTT5_GENERAL) < level) {
        return;
    }

    /* TODO: constantly checking the log level at this point is kind of dumb but there's no better API atm */

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "id=%p: aws_mqtt5_packet_connack_view reason code set to %d(%s)",
        (void *)connack_view,
        (int)connack_view->reason_code,
        aws_mqtt5_connect_reason_code_to_c_string(connack_view->reason_code));

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "id=%p: aws_mqtt5_packet_connack_view session present set to %d",
        (void *)connack_view,
        (int)connack_view->session_present);

    if (connack_view->session_expiry_interval != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_connack_view session expiry interval set to %" PRIu32,
            (void *)connack_view,
            *connack_view->session_expiry_interval);
    }

    if (connack_view->receive_maximum != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_connack_view receive maximum set to %" PRIu16,
            (void *)connack_view,
            *connack_view->receive_maximum);
    }

    if (connack_view->maximum_qos != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_connack_view maximum qos set to %d",
            (void *)connack_view,
            (int)(*connack_view->maximum_qos));
    }

    if (connack_view->retain_available != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_connack_view retain available set to %d",
            (void *)connack_view,
            (int)(*connack_view->retain_available));
    }

    if (connack_view->maximum_packet_size != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_connack_view maximum packet size set to %" PRIu32,
            (void *)connack_view,
            *connack_view->maximum_packet_size);
    }

    if (connack_view->assigned_client_identifier != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_connack_view assigned client identifier set to \"" PRInSTR "\"",
            (void *)connack_view,
            AWS_BYTE_CURSOR_PRI(*connack_view->assigned_client_identifier));
    }

    if (connack_view->topic_alias_maximum != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_connack_view topic alias maximum set to %" PRIu16,
            (void *)connack_view,
            *connack_view->topic_alias_maximum);
    }

    if (connack_view->reason_string != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_connack_view reason string set to \"" PRInSTR "\"",
            (void *)connack_view,
            AWS_BYTE_CURSOR_PRI(*connack_view->reason_string));
    }

    if (connack_view->wildcard_subscriptions_available != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_connack_view wildcard subscriptions available set to %d",
            (void *)connack_view,
            (int)(*connack_view->wildcard_subscriptions_available));
    }

    if (connack_view->subscription_identifiers_available != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_connack_view subscription identifiers available set to %d",
            (void *)connack_view,
            (int)(*connack_view->subscription_identifiers_available));
    }

    if (connack_view->shared_subscriptions_available != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_connack_view shared subscriptions available set to %d",
            (void *)connack_view,
            (int)(*connack_view->shared_subscriptions_available));
    }

    if (connack_view->server_keep_alive != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_connack_view server keep alive set to %" PRIu16,
            (void *)connack_view,
            *connack_view->server_keep_alive);
    }

    if (connack_view->response_information != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_connack_view response information set to \"" PRInSTR "\"",
            (void *)connack_view,
            AWS_BYTE_CURSOR_PRI(*connack_view->response_information));
    }

    if (connack_view->server_reference != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_connack_view server reference set to \"" PRInSTR "\"",
            (void *)connack_view,
            AWS_BYTE_CURSOR_PRI(*connack_view->server_reference));
    }

    if (connack_view->authentication_method != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_connack_view authentication method set",
            (void *)connack_view);
    }

    if (connack_view->authentication_data != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_connack_view authentication data set",
            (void *)connack_view);
    }

    s_aws_mqtt5_user_property_set_log(
        connack_view->user_properties,
        connack_view->user_property_count,
        (void *)connack_view,
        level,
        "aws_mqtt5_packet_connack_view");
}

void aws_mqtt5_packet_connack_view_init_from_storage(
    struct aws_mqtt5_packet_connack_view *connack_view,
    const struct aws_mqtt5_packet_connack_storage *connack_storage) {

    connack_view->session_present = connack_storage->session_present;
    connack_view->reason_code = connack_storage->reason_code;
    connack_view->session_expiry_interval = connack_storage->session_expiry_interval_ptr;
    connack_view->receive_maximum = connack_storage->receive_maximum_ptr;
    connack_view->maximum_qos = connack_storage->maximum_qos_ptr;
    connack_view->retain_available = connack_storage->retain_available_ptr;
    connack_view->maximum_packet_size = connack_storage->maximum_packet_size_ptr;
    connack_view->assigned_client_identifier = connack_storage->assigned_client_identifier_ptr;
    connack_view->topic_alias_maximum = connack_storage->topic_alias_maximum_ptr;
    connack_view->reason_string = connack_storage->reason_string_ptr;

    connack_view->user_property_count = aws_mqtt5_user_property_set_size(&connack_storage->user_properties);
    connack_view->user_properties = connack_storage->user_properties.properties.data;

    connack_view->wildcard_subscriptions_available = connack_storage->wildcard_subscriptions_available_ptr;
    connack_view->subscription_identifiers_available = connack_storage->subscription_identifiers_available_ptr;
    connack_view->shared_subscriptions_available = connack_storage->shared_subscriptions_available_ptr;

    connack_view->server_keep_alive = connack_storage->server_keep_alive_ptr;
    connack_view->response_information = connack_storage->response_information_ptr;
    connack_view->server_reference = connack_storage->server_reference_ptr;
    connack_view->authentication_method = connack_storage->authentication_method_ptr;
    connack_view->authentication_data = connack_storage->authentication_data_ptr;
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
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_disconnect_view - invalid DISCONNECT reason code:%d",
            (void *)disconnect_view,
            (int)disconnect_view->reason_code);
        return aws_raise_error(AWS_ERROR_MQTT5_DISCONNECT_OPTIONS_VALIDATION);
    }

    if (disconnect_view->session_expiry_interval_seconds != NULL) {
        if (client != NULL) {
            /*
             * By spec (https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901211), you
             * cannot set a non-zero value here if you sent a 0-value or no value in the CONNECT (presumably allows
             * the server to skip tracking session state, and we can't undo that now)
             */
            const uint32_t *session_expiry_ptr = client->config->connect.storage_view.session_expiry_interval_seconds;
            if (session_expiry_ptr == NULL || *session_expiry_ptr == 0) {
                AWS_LOGF_ERROR(
                    AWS_LS_MQTT5_GENERAL,
                    "id=%p: aws_mqtt5_packet_disconnect_view - cannot specify a positive session expiry after "
                    "committing "
                    "to 0-valued session expiry in CONNECT",
                    (void *)disconnect_view);
                return aws_raise_error(AWS_ERROR_MQTT5_DISCONNECT_OPTIONS_VALIDATION);
            }
        }
    }

    if (disconnect_view->reason_string != NULL) {
        if (disconnect_view->reason_string->len > UINT16_MAX) {
            AWS_LOGF_ERROR(
                AWS_LS_MQTT5_GENERAL,
                "id=%p: aws_mqtt5_packet_disconnect_view - reason string too long",
                (void *)disconnect_view);
            return aws_raise_error(AWS_ERROR_MQTT5_DISCONNECT_OPTIONS_VALIDATION);
        }
    }

    if (disconnect_view->server_reference != NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_disconnect_view - sending a server reference with a client-sourced DISCONNECT is "
            "not allowed",
            (void *)disconnect_view);
        return aws_raise_error(AWS_ERROR_MQTT5_DISCONNECT_OPTIONS_VALIDATION);
    }

    if (s_aws_mqtt5_user_property_set_validate(
            disconnect_view->user_properties,
            disconnect_view->user_property_count,
            "aws_mqtt5_packet_disconnect_view",
            (void *)disconnect_view)) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

void aws_mqtt5_packet_disconnect_view_log(
    const struct aws_mqtt5_packet_disconnect_view *disconnect_view,
    enum aws_log_level level) {
    struct aws_logger *temp_logger = aws_logger_get();
    if (temp_logger == NULL || temp_logger->vtable->get_log_level(temp_logger, AWS_LS_MQTT5_GENERAL) < level) {
        return;
    }

    /* TODO: constantly checking the log level at this point is kind of dumb but there's no better API atm */

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "id=%p: aws_mqtt5_packet_disconnect_view reason code set to %d(%s)",
        (void *)disconnect_view,
        (int)disconnect_view->reason_code,
        aws_mqtt5_disconnect_reason_code_to_c_string(disconnect_view->reason_code, NULL));

    if (disconnect_view->session_expiry_interval_seconds != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_disconnect_view session expiry interval set to %" PRIu32,
            (void *)disconnect_view,
            *disconnect_view->session_expiry_interval_seconds);
    }

    if (disconnect_view->reason_string != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_disconnect_view reason string set to \"" PRInSTR "\"",
            (void *)disconnect_view,
            AWS_BYTE_CURSOR_PRI(*disconnect_view->reason_string));
    }

    if (disconnect_view->server_reference != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_disconnect_view server reference set to \"" PRInSTR "\"",
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

void aws_mqtt5_packet_disconnect_storage_clean_up(struct aws_mqtt5_packet_disconnect_storage *disconnect_storage) {
    if (disconnect_storage == NULL) {
        return;
    }

    aws_mqtt5_user_property_set_clean_up(&disconnect_storage->user_properties);
    aws_byte_buf_clean_up(&disconnect_storage->storage);
}

void aws_mqtt5_packet_disconnect_view_init_from_storage(
    struct aws_mqtt5_packet_disconnect_view *disconnect_view,
    const struct aws_mqtt5_packet_disconnect_storage *disconnect_storage) {

    disconnect_view->reason_code = disconnect_storage->reason_code;
    disconnect_view->session_expiry_interval_seconds = disconnect_storage->session_expiry_interval_seconds_ptr;
    disconnect_view->reason_string = disconnect_storage->reason_string_ptr;
    disconnect_view->server_reference = disconnect_storage->server_reference_ptr;
    disconnect_view->user_property_count = aws_mqtt5_user_property_set_size(&disconnect_storage->user_properties);
    disconnect_view->user_properties = disconnect_storage->user_properties.properties.data;
}

static size_t s_aws_mqtt5_packet_disconnect_compute_storage_size(
    const struct aws_mqtt5_packet_disconnect_view *disconnect_view) {
    size_t storage_size = s_aws_mqtt5_user_property_set_compute_storage_size(
        disconnect_view->user_properties, disconnect_view->user_property_count);

    if (disconnect_view->reason_string != NULL) {
        storage_size += disconnect_view->reason_string->len;
    }

    if (disconnect_view->server_reference != NULL) {
        storage_size += disconnect_view->server_reference->len;
    }

    return storage_size;
}

int aws_mqtt5_packet_disconnect_storage_init(
    struct aws_mqtt5_packet_disconnect_storage *disconnect_storage,
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_disconnect_view *disconnect_options) {

    AWS_ZERO_STRUCT(*disconnect_storage);
    size_t storage_capacity = s_aws_mqtt5_packet_disconnect_compute_storage_size(disconnect_options);
    if (aws_byte_buf_init(&disconnect_storage->storage, allocator, storage_capacity)) {
        return AWS_OP_ERR;
    }

    disconnect_storage->reason_code = disconnect_options->reason_code;

    if (disconnect_options->session_expiry_interval_seconds != NULL) {
        disconnect_storage->session_expiry_interval_seconds = *disconnect_options->session_expiry_interval_seconds;
        disconnect_storage->session_expiry_interval_seconds_ptr = &disconnect_storage->session_expiry_interval_seconds;
    }

    if (disconnect_options->reason_string != NULL) {
        disconnect_storage->reason_string = *disconnect_options->reason_string;
        if (aws_byte_buf_append_and_update(&disconnect_storage->storage, &disconnect_storage->reason_string)) {
            return AWS_OP_ERR;
        }

        disconnect_storage->reason_string_ptr = &disconnect_storage->reason_string;
    }

    if (disconnect_options->server_reference != NULL) {
        disconnect_storage->server_reference = *disconnect_options->server_reference;
        if (aws_byte_buf_append_and_update(&disconnect_storage->storage, &disconnect_storage->server_reference)) {
            return AWS_OP_ERR;
        }

        disconnect_storage->server_reference_ptr = &disconnect_storage->server_reference;
    }

    if (aws_mqtt5_user_property_set_init_with_storage(
            &disconnect_storage->user_properties,
            allocator,
            &disconnect_storage->storage,
            disconnect_options->user_property_count,
            disconnect_options->user_properties)) {
        return AWS_OP_ERR;
    }

    aws_mqtt5_packet_disconnect_view_init_from_storage(&disconnect_storage->storage_view, disconnect_storage);

    return AWS_OP_SUCCESS;
}

int aws_mqtt5_packet_disconnect_storage_init_from_external_storage(
    struct aws_mqtt5_packet_disconnect_storage *disconnect_storage,
    struct aws_allocator *allocator) {
    AWS_ZERO_STRUCT(*disconnect_storage);

    if (aws_mqtt5_user_property_set_init(&disconnect_storage->user_properties, allocator)) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

static void s_destroy_operation_disconnect(void *object) {
    if (object == NULL) {
        return;
    }

    struct aws_mqtt5_operation_disconnect *disconnect_op = object;

    aws_mqtt5_packet_disconnect_storage_clean_up(&disconnect_op->options_storage);

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
    disconnect_op->base.vtable = &s_empty_operation_vtable;
    disconnect_op->base.packet_type = AWS_MQTT5_PT_DISCONNECT;
    aws_ref_count_init(&disconnect_op->base.ref_count, disconnect_op, s_destroy_operation_disconnect);
    disconnect_op->base.impl = disconnect_op;

    if (aws_mqtt5_packet_disconnect_storage_init(&disconnect_op->options_storage, allocator, disconnect_options)) {
        goto error;
    }

    disconnect_op->base.packet_view = &disconnect_op->options_storage.storage_view;

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

    /* TODO: eventually allow QoS 2 */
    if (publish_view->qos < AWS_MQTT5_QOS_AT_MOST_ONCE || publish_view->qos > AWS_MQTT5_QOS_AT_LEAST_ONCE) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_publish_view - unsupported QoS value in PUBLISH packet options: %d",
            (void *)publish_view,
            (int)publish_view->qos);
        return aws_raise_error(AWS_ERROR_MQTT5_PUBLISH_OPTIONS_VALIDATION);
    }

    /* 0-length topic is valid if there's an alias, otherwise we need a valid topic */
    if (publish_view->topic.len == 0) {
        if (publish_view->topic_alias == NULL) {
            AWS_LOGF_ERROR(
                AWS_LS_MQTT5_GENERAL, "id=%p: aws_mqtt5_packet_publish_view - missing topic", (void *)publish_view);
            return aws_raise_error(AWS_ERROR_MQTT5_PUBLISH_OPTIONS_VALIDATION);
        }
    } else if (!aws_mqtt_is_valid_topic(&publish_view->topic)) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_GENERAL, "id=%p: aws_mqtt5_packet_publish_view - invalid topic", (void *)publish_view);
        return aws_raise_error(AWS_ERROR_MQTT5_PUBLISH_OPTIONS_VALIDATION);
    } else if (publish_view->topic.len > UINT16_MAX) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_GENERAL, "id=%p: aws_mqtt5_packet_publish_view - topic too long", (void *)publish_view);
        return aws_raise_error(AWS_ERROR_MQTT5_PUBLISH_OPTIONS_VALIDATION);
    }

    if (publish_view->payload_format != NULL) {
        if (*publish_view->payload_format < AWS_MQTT5_PFI_BYTES || *publish_view->payload_format > AWS_MQTT5_PFI_UTF8) {
            AWS_LOGF_ERROR(
                AWS_LS_MQTT5_GENERAL,
                "id=%p: aws_mqtt5_packet_publish_view - invalid payload format value: %d",
                (void *)publish_view,
                (int)*publish_view->payload_format);
            return aws_raise_error(AWS_ERROR_MQTT5_PUBLISH_OPTIONS_VALIDATION);
        }
    }

    if (publish_view->response_topic != NULL) {
        if (publish_view->response_topic->len >= UINT16_MAX) {
            AWS_LOGF_ERROR(
                AWS_LS_MQTT5_GENERAL,
                "id=%p: aws_mqtt5_packet_publish_view - response topic too long",
                (void *)publish_view);
            return aws_raise_error(AWS_ERROR_MQTT5_PUBLISH_OPTIONS_VALIDATION);
        }
    }

    if (publish_view->correlation_data != NULL) {
        if (publish_view->correlation_data->len >= UINT16_MAX) {
            AWS_LOGF_ERROR(
                AWS_LS_MQTT5_GENERAL,
                "id=%p: aws_mqtt5_packet_publish_view - correlation data too long",
                (void *)publish_view);
            return aws_raise_error(AWS_ERROR_MQTT5_PUBLISH_OPTIONS_VALIDATION);
        }
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

    if (publish_view->content_type != NULL) {
        if (publish_view->content_type->len >= UINT16_MAX) {
            AWS_LOGF_ERROR(
                AWS_LS_MQTT5_GENERAL,
                "id=%p: aws_mqtt5_packet_publish_view - content type too long",
                (void *)publish_view);
            return aws_raise_error(AWS_ERROR_MQTT5_PUBLISH_OPTIONS_VALIDATION);
        }
    }

    if (s_aws_mqtt5_user_property_set_validate(
            publish_view->user_properties,
            publish_view->user_property_count,
            "aws_mqtt5_packet_publish_view",
            (void *)publish_view)) {
        return AWS_OP_ERR;
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
    struct aws_logger *temp_logger = aws_logger_get();
    if (temp_logger == NULL || temp_logger->vtable->get_log_level(temp_logger, AWS_LS_MQTT5_GENERAL) < level) {
        return;
    }

    /* TODO: constantly checking the log level at this point is kind of dumb but there's no better API atm */

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "id=%p: aws_mqtt5_packet_publish_view payload set containing %zu bytes",
        (void *)publish_view,
        publish_view->payload.len);

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "id=%p: aws_mqtt5_packet_publish_view qos set to %d",
        (void *)publish_view,
        (int)publish_view->qos);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_GENERAL,
        "id=%p: aws_mqtt5_packet_publish_view retain set to %d",
        (void *)publish_view,
        (int)publish_view->retain);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_GENERAL,
        "id=%p: aws_mqtt5_packet_publish_view topic set to \"" PRInSTR "\"",
        (void *)publish_view,
        AWS_BYTE_CURSOR_PRI(publish_view->topic));

    if (publish_view->payload_format != NULL) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_publish_view payload format indicator set to %d(%s)",
            (void *)publish_view,
            (int)*publish_view->payload_format,
            aws_mqtt5_payload_format_indicator_to_c_string(*publish_view->payload_format));
    }

    if (publish_view->message_expiry_interval_seconds != NULL) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_publish_view message expiry interval set to %" PRIu32,
            (void *)publish_view,
            *publish_view->message_expiry_interval_seconds);
    }

    if (publish_view->topic_alias != NULL) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_publish_view topic alias set to %" PRIu16,
            (void *)publish_view,
            *publish_view->topic_alias);
    }

    if (publish_view->response_topic != NULL) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_publish_view response topic set to \"" PRInSTR "\"",
            (void *)publish_view,
            AWS_BYTE_CURSOR_PRI(*publish_view->response_topic));
    }

    if (publish_view->correlation_data != NULL) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_GENERAL, "id=%p: aws_mqtt5_packet_publish_view - set correlation data", (void *)publish_view);
    }

    if (publish_view->content_type != NULL) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_publish_view content type set to \"" PRInSTR "\"",
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

    publish_view->payload = publish_storage->payload;
    publish_view->qos = publish_storage->qos;
    publish_view->retain = publish_storage->retain;
    publish_view->topic = publish_storage->topic;
    publish_view->payload_format = publish_storage->payload_format_ptr;
    publish_view->message_expiry_interval_seconds = publish_storage->message_expiry_interval_seconds_ptr;
    publish_view->topic_alias = publish_storage->topic_alias_ptr;
    publish_view->response_topic = publish_storage->response_topic_ptr;
    publish_view->correlation_data = publish_storage->correlation_data_ptr;
    publish_view->content_type = publish_storage->content_type_ptr;
    publish_view->user_property_count = aws_mqtt5_user_property_set_size(&publish_storage->user_properties);
    publish_view->user_properties = publish_storage->user_properties.properties.data;
}

static size_t s_aws_mqtt5_packet_publish_compute_storage_size(
    const struct aws_mqtt5_packet_publish_view *publish_view) {
    size_t storage_size = s_aws_mqtt5_user_property_set_compute_storage_size(
        publish_view->user_properties, publish_view->user_property_count);

    storage_size += publish_view->topic.len;
    storage_size += publish_view->payload.len;

    if (publish_view->response_topic != NULL) {
        storage_size += publish_view->response_topic->len;
    }

    if (publish_view->correlation_data != NULL) {
        storage_size += publish_view->correlation_data->len;
    }

    if (publish_view->content_type != NULL) {
        storage_size += publish_view->content_type->len;
    }

    return storage_size;
}

int aws_mqtt5_packet_publish_storage_init(
    struct aws_mqtt5_packet_publish_storage *publish_storage,
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_publish_view *publish_options) {

    AWS_ZERO_STRUCT(*publish_storage);
    size_t storage_capacity = s_aws_mqtt5_packet_publish_compute_storage_size(publish_options);
    if (aws_byte_buf_init(&publish_storage->storage, allocator, storage_capacity)) {
        return AWS_OP_ERR;
    }

    publish_storage->payload = publish_options->payload;
    if (aws_byte_buf_append_and_update(&publish_storage->storage, &publish_storage->payload)) {
        return AWS_OP_ERR;
    }

    publish_storage->qos = publish_options->qos;
    publish_storage->retain = publish_options->retain;

    publish_storage->topic = publish_options->topic;
    if (aws_byte_buf_append_and_update(&publish_storage->storage, &publish_storage->topic)) {
        return AWS_OP_ERR;
    }

    if (publish_options->payload_format != NULL) {
        publish_storage->payload_format = *publish_options->payload_format;
        publish_storage->payload_format_ptr = &publish_storage->payload_format;
    }

    if (publish_options->message_expiry_interval_seconds != NULL) {
        publish_storage->message_expiry_interval_seconds = *publish_options->message_expiry_interval_seconds;
        publish_storage->message_expiry_interval_seconds_ptr = &publish_storage->message_expiry_interval_seconds;
    }

    if (publish_options->topic_alias != NULL) {
        publish_storage->topic_alias = *publish_options->topic_alias;
        publish_storage->topic_alias_ptr = &publish_storage->topic_alias;
    }

    if (publish_options->response_topic != NULL) {
        publish_storage->response_topic = *publish_options->response_topic;
        if (aws_byte_buf_append_and_update(&publish_storage->storage, &publish_storage->response_topic)) {
            return AWS_OP_ERR;
        }

        publish_storage->response_topic_ptr = &publish_storage->response_topic;
    }

    if (publish_options->correlation_data != NULL) {
        publish_storage->correlation_data = *publish_options->correlation_data;
        if (aws_byte_buf_append_and_update(&publish_storage->storage, &publish_storage->correlation_data)) {
            return AWS_OP_ERR;
        }

        publish_storage->correlation_data_ptr = &publish_storage->correlation_data;
    }

    if (publish_options->content_type != NULL) {
        publish_storage->content_type = *publish_options->content_type;
        if (aws_byte_buf_append_and_update(&publish_storage->storage, &publish_storage->content_type)) {
            return AWS_OP_ERR;
        }

        publish_storage->content_type_ptr = &publish_storage->content_type;
    }

    if (aws_mqtt5_user_property_set_init_with_storage(
            &publish_storage->user_properties,
            allocator,
            &publish_storage->storage,
            publish_options->user_property_count,
            publish_options->user_properties)) {
        return AWS_OP_ERR;
    }

    aws_mqtt5_packet_publish_view_init_from_storage(&publish_storage->storage_view, publish_storage);

    return AWS_OP_SUCCESS;
}

int aws_mqtt5_packet_publish_storage_init_from_external_storage(
    struct aws_mqtt5_packet_publish_storage *publish_storage,
    struct aws_allocator *allocator) {
    AWS_ZERO_STRUCT(*publish_storage);

    if (aws_mqtt5_user_property_set_init(&publish_storage->user_properties, allocator)) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

void aws_mqtt5_packet_publish_storage_clean_up(struct aws_mqtt5_packet_publish_storage *publish_storage) {
    aws_mqtt5_user_property_set_clean_up(&publish_storage->user_properties);
    aws_byte_buf_clean_up(&publish_storage->storage);
}

static void s_aws_mqtt5_operation_publish_complete(
    struct aws_mqtt5_operation *operation,
    int error_code,
    const void *completion_view) {
    struct aws_mqtt5_operation_publish *publish_op = operation->impl;

    if (publish_op->completion_options.completion_callback != NULL) {
        (*publish_op->completion_options.completion_callback)(
            completion_view, error_code, publish_op->completion_options.completion_user_data);
    }
}

static void s_aws_mqtt5_operation_publish_set_packet_id(
    struct aws_mqtt5_operation *operation,
    aws_mqtt5_packet_id_t packet_id) {
    struct aws_mqtt5_operation_publish *publish_op = operation->impl;
    publish_op->options_storage.storage_view.packet_id = packet_id;
}

static aws_mqtt5_packet_id_t *s_aws_mqtt5_operation_publish_get_packet_id_address(
    const struct aws_mqtt5_operation *operation) {
    struct aws_mqtt5_operation_publish *publish_op = operation->impl;
    return &publish_op->options_storage.storage_view.packet_id;
}

static struct aws_mqtt5_operation_vtable s_publish_operation_vtable = {
    .aws_mqtt5_operation_completion_fn = s_aws_mqtt5_operation_publish_complete,
    .aws_mqtt5_operation_set_packet_id_fn = s_aws_mqtt5_operation_publish_set_packet_id,
    .aws_mqtt5_operation_get_packet_id_address_fn = s_aws_mqtt5_operation_publish_get_packet_id_address,
};

static void s_destroy_operation_publish(void *object) {
    if (object == NULL) {
        return;
    }

    struct aws_mqtt5_operation_publish *publish_op = object;

    aws_mqtt5_packet_publish_storage_clean_up(&publish_op->options_storage);

    aws_mem_release(publish_op->allocator, publish_op);
}

struct aws_mqtt5_operation_publish *aws_mqtt5_operation_publish_new(
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_publish_view *publish_options,
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
    publish_op->base.vtable = &s_publish_operation_vtable;
    publish_op->base.packet_type = AWS_MQTT5_PT_PUBLISH;
    aws_ref_count_init(&publish_op->base.ref_count, publish_op, s_destroy_operation_publish);
    publish_op->base.impl = publish_op;

    if (aws_mqtt5_packet_publish_storage_init(&publish_op->options_storage, allocator, publish_options)) {
        goto error;
    }

    publish_op->base.packet_view = &publish_op->options_storage.storage_view;

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
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_unsubscribe_view - must contain at least one topic",
            (void *)unsubscribe_view);
        return aws_raise_error(AWS_ERROR_MQTT5_UNSUBSCRIBE_OPTIONS_VALIDATION);
    }

    if (unsubscribe_view->topic_count > AWS_MQTT5_CLIENT_MAXIMUM_TOPICS_PER_UNSUBSCRIBE) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_unsubscribe_view - contains too many topics (%zu)",
            (void *)unsubscribe_view,
            unsubscribe_view->topic_count);
        return aws_raise_error(AWS_ERROR_MQTT5_UNSUBSCRIBE_OPTIONS_VALIDATION);
    }

    for (size_t i = 0; i < unsubscribe_view->topic_count; ++i) {
        const struct aws_byte_cursor *topic = &unsubscribe_view->topics[i];
        if (topic->len > UINT16_MAX) {
            AWS_LOGF_ERROR(
                AWS_LS_MQTT5_GENERAL,
                "id=%p: aws_mqtt5_packet_unsubscribe_view - topic too long",
                (void *)unsubscribe_view);
            return aws_raise_error(AWS_ERROR_MQTT5_UNSUBSCRIBE_OPTIONS_VALIDATION);
        }
    }

    if (s_aws_mqtt5_user_property_set_validate(
            unsubscribe_view->user_properties,
            unsubscribe_view->user_property_count,
            "aws_mqtt5_packet_unsubscribe_view",
            (void *)unsubscribe_view)) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

void aws_mqtt5_packet_unsubscribe_view_log(
    const struct aws_mqtt5_packet_unsubscribe_view *unsubscribe_view,
    enum aws_log_level level) {
    struct aws_logger *temp_logger = aws_logger_get();
    if (temp_logger == NULL || temp_logger->vtable->get_log_level(temp_logger, AWS_LS_MQTT5_GENERAL) < level) {
        return;
    }

    /* TODO: constantly checking the log level at this point is kind of dumb but there's no better API atm */

    size_t topic_count = unsubscribe_view->topic_count;
    for (size_t i = 0; i < topic_count; ++i) {
        const struct aws_byte_cursor *topic_cursor = &unsubscribe_view->topics[i];

        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_unsubscribe_view topic %zu: \"" PRInSTR "\"",
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

    aws_array_list_clean_up(&unsubscribe_storage->topics);
    aws_mqtt5_user_property_set_clean_up(&unsubscribe_storage->user_properties);
    aws_byte_buf_clean_up(&unsubscribe_storage->storage);
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

    if (aws_array_list_init_dynamic(
            &unsubscribe_storage->topics, allocator, topic_count, sizeof(struct aws_byte_cursor))) {
        return AWS_OP_ERR;
    }

    for (size_t i = 0; i < topic_count; ++i) {
        const struct aws_byte_cursor *topic_cursor_ptr = &topics[i];
        struct aws_byte_cursor topic_cursor = *topic_cursor_ptr;

        if (aws_byte_buf_append_and_update(&unsubscribe_storage->storage, &topic_cursor)) {
            return AWS_OP_ERR;
        }

        if (aws_array_list_push_back(&unsubscribe_storage->topics, &topic_cursor)) {
            return AWS_OP_ERR;
        }
    }

    return AWS_OP_SUCCESS;
}

static size_t s_aws_mqtt5_packet_unsubscribe_compute_storage_size(
    const struct aws_mqtt5_packet_unsubscribe_view *unsubscribe_view) {
    size_t storage_size = s_aws_mqtt5_user_property_set_compute_storage_size(
        unsubscribe_view->user_properties, unsubscribe_view->user_property_count);

    for (size_t i = 0; i < unsubscribe_view->topic_count; ++i) {
        const struct aws_byte_cursor *topic = &unsubscribe_view->topics[i];
        storage_size += topic->len;
    }

    return storage_size;
}

int aws_mqtt5_packet_unsubscribe_storage_init(
    struct aws_mqtt5_packet_unsubscribe_storage *unsubscribe_storage,
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_unsubscribe_view *unsubscribe_options) {

    AWS_ZERO_STRUCT(*unsubscribe_storage);
    size_t storage_capacity = s_aws_mqtt5_packet_unsubscribe_compute_storage_size(unsubscribe_options);
    if (aws_byte_buf_init(&unsubscribe_storage->storage, allocator, storage_capacity)) {
        return AWS_OP_ERR;
    }

    if (s_aws_mqtt5_packet_unsubscribe_build_topic_list(
            unsubscribe_storage, allocator, unsubscribe_options->topic_count, unsubscribe_options->topics)) {
        return AWS_OP_ERR;
    }

    if (aws_mqtt5_user_property_set_init_with_storage(
            &unsubscribe_storage->user_properties,
            allocator,
            &unsubscribe_storage->storage,
            unsubscribe_options->user_property_count,
            unsubscribe_options->user_properties)) {
        return AWS_OP_ERR;
    }

    aws_mqtt5_packet_unsubscribe_view_init_from_storage(&unsubscribe_storage->storage_view, unsubscribe_storage);

    return AWS_OP_SUCCESS;
}

static void s_aws_mqtt5_operation_unsubscribe_complete(
    struct aws_mqtt5_operation *operation,
    int error_code,
    const void *completion_view) {
    struct aws_mqtt5_operation_unsubscribe *unsubscribe_op = operation->impl;

    if (unsubscribe_op->completion_options.completion_callback != NULL) {
        (*unsubscribe_op->completion_options.completion_callback)(
            completion_view, error_code, unsubscribe_op->completion_options.completion_user_data);
    }
}

static void s_aws_mqtt5_operation_unsubscribe_set_packet_id(
    struct aws_mqtt5_operation *operation,
    aws_mqtt5_packet_id_t packet_id) {
    struct aws_mqtt5_operation_unsubscribe *unsubscribe_op = operation->impl;
    unsubscribe_op->options_storage.storage_view.packet_id = packet_id;
}

static aws_mqtt5_packet_id_t *s_aws_mqtt5_operation_unsubscribe_get_packet_id_address(
    const struct aws_mqtt5_operation *operation) {
    struct aws_mqtt5_operation_unsubscribe *unsubscribe_op = operation->impl;
    return &unsubscribe_op->options_storage.storage_view.packet_id;
}

static struct aws_mqtt5_operation_vtable s_unsubscribe_operation_vtable = {
    .aws_mqtt5_operation_completion_fn = s_aws_mqtt5_operation_unsubscribe_complete,
    .aws_mqtt5_operation_set_packet_id_fn = s_aws_mqtt5_operation_unsubscribe_set_packet_id,
    .aws_mqtt5_operation_get_packet_id_address_fn = s_aws_mqtt5_operation_unsubscribe_get_packet_id_address,
};

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
    unsubscribe_op->base.vtable = &s_unsubscribe_operation_vtable;
    unsubscribe_op->base.packet_type = AWS_MQTT5_PT_UNSUBSCRIBE;
    aws_ref_count_init(&unsubscribe_op->base.ref_count, unsubscribe_op, s_destroy_operation_unsubscribe);
    unsubscribe_op->base.impl = unsubscribe_op;

    if (aws_mqtt5_packet_unsubscribe_storage_init(&unsubscribe_op->options_storage, allocator, unsubscribe_options)) {
        goto error;
    }

    unsubscribe_op->base.packet_view = &unsubscribe_op->options_storage.storage_view;

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

static int s_aws_mqtt5_validate_subscription(
    const struct aws_mqtt5_subscription_view *subscription,
    struct aws_mqtt5_client *client,
    void *log_context) {
    (void)client;

    if (!aws_mqtt_is_valid_topic_filter(&subscription->topic_filter)) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_subscribe_view - invalid topic filter \"" PRInSTR "\" in subscription",
            log_context,
            AWS_BYTE_CURSOR_PRI(subscription->topic_filter));
        return aws_raise_error(AWS_ERROR_MQTT5_SUBSCRIBE_OPTIONS_VALIDATION);
    }

    if (subscription->topic_filter.len > UINT16_MAX) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_subscribe_view - subscription contains too-long topic filter",
            log_context);
        return aws_raise_error(AWS_ERROR_MQTT5_SUBSCRIBE_OPTIONS_VALIDATION);
    }

    if (subscription->qos < AWS_MQTT5_QOS_AT_MOST_ONCE || subscription->qos > AWS_MQTT5_QOS_AT_LEAST_ONCE) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_subscribe_view - unsupported QoS value: %d",
            log_context,
            (int)subscription->qos);
        return aws_raise_error(AWS_ERROR_MQTT5_SUBSCRIBE_OPTIONS_VALIDATION);
    }

    if (subscription->retain_handling_type < AWS_MQTT5_RHT_SEND_ON_SUBSCRIBE ||
        subscription->retain_handling_type > AWS_MQTT5_RHT_DONT_SEND) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_subscribe_view - unsupported retain handling value: %d",
            log_context,
            (int)subscription->retain_handling_type);
        return aws_raise_error(AWS_ERROR_MQTT5_SUBSCRIBE_OPTIONS_VALIDATION);
    }

    return AWS_OP_SUCCESS;
}

int aws_mqtt5_packet_subscribe_view_validate(
    const struct aws_mqtt5_packet_subscribe_view *subscribe_view,
    struct aws_mqtt5_client *client) {
    (void)client;

    if (subscribe_view == NULL) {
        AWS_LOGF_ERROR(AWS_LS_MQTT5_GENERAL, "null SUBSCRIBE packet options");
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    if (subscribe_view->subscription_count == 0) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_subscribe_view - must contain at least one subscription",
            (void *)subscribe_view);
        return aws_raise_error(AWS_ERROR_MQTT5_SUBSCRIBE_OPTIONS_VALIDATION);
    }

    if (subscribe_view->subscription_count > AWS_MQTT5_CLIENT_MAXIMUM_SUBSCRIPTIONS_PER_SUBSCRIBE) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_subscribe_view - too many subscriptions",
            (void *)subscribe_view);
        return aws_raise_error(AWS_ERROR_MQTT5_SUBSCRIBE_OPTIONS_VALIDATION);
    }

    for (size_t i = 0; i < subscribe_view->subscription_count; ++i) {
        const struct aws_mqtt5_subscription_view *subscription = &subscribe_view->subscriptions[i];
        if (s_aws_mqtt5_validate_subscription(subscription, client, (void *)subscribe_view)) {
            AWS_LOGF_ERROR(
                AWS_LS_MQTT5_GENERAL,
                "id=%p: aws_mqtt5_packet_subscribe_view - invalid subscription",
                (void *)subscribe_view);
            return aws_raise_error(AWS_ERROR_MQTT5_SUBSCRIBE_OPTIONS_VALIDATION);
        }
    }

    if (subscribe_view->subscription_identifier != NULL) {
        if (*subscribe_view->subscription_identifier > AWS_MQTT5_MAXIMUM_VARIABLE_LENGTH_INTEGER) {
            AWS_LOGF_ERROR(
                AWS_LS_MQTT5_GENERAL,
                "id=%p: aws_mqtt5_packet_subscribe_view - subscription identifier (%" PRIu32 ") too large",
                (void *)subscribe_view,
                *subscribe_view->subscription_identifier);
            return aws_raise_error(AWS_ERROR_MQTT5_SUBSCRIBE_OPTIONS_VALIDATION);
        }
    }

    if (s_aws_mqtt5_user_property_set_validate(
            subscribe_view->user_properties,
            subscribe_view->user_property_count,
            "aws_mqtt5_packet_subscribe_view",
            (void *)subscribe_view)) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

void aws_mqtt5_packet_subscribe_view_log(
    const struct aws_mqtt5_packet_subscribe_view *subscribe_view,
    enum aws_log_level level) {
    struct aws_logger *temp_logger = aws_logger_get();
    if (temp_logger == NULL || temp_logger->vtable->get_log_level(temp_logger, AWS_LS_MQTT5_GENERAL) < level) {
        return;
    }

    /* TODO: constantly checking the log level at this point is kind of dumb but there's no better API atm */

    size_t subscription_count = subscribe_view->subscription_count;
    for (size_t i = 0; i < subscription_count; ++i) {
        const struct aws_mqtt5_subscription_view *view = &subscribe_view->subscriptions[i];

        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_packet_subscribe_view subscription %zu: topic filter \"" PRInSTR
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
            "id=%p: aws_mqtt5_packet_subscribe_view subscription identifier set to %" PRIu32,
            (void *)subscribe_view,
            *subscribe_view->subscription_identifier);
    }

    s_aws_mqtt5_user_property_set_log(
        subscribe_view->user_properties,
        subscribe_view->user_property_count,
        (void *)subscribe_view,
        level,
        "aws_mqtt5_packet_subscribe_view");
}

void aws_mqtt5_packet_subscribe_storage_clean_up(struct aws_mqtt5_packet_subscribe_storage *subscribe_storage) {
    if (subscribe_storage == NULL) {
        return;
    }

    aws_array_list_clean_up(&subscribe_storage->subscriptions);

    aws_mqtt5_user_property_set_clean_up(&subscribe_storage->user_properties);
    aws_byte_buf_clean_up(&subscribe_storage->storage);
}

void aws_mqtt5_packet_subscribe_view_init_from_storage(
    struct aws_mqtt5_packet_subscribe_view *subscribe_view,
    const struct aws_mqtt5_packet_subscribe_storage *subscribe_storage) {
    subscribe_view->packet_id = subscribe_storage->storage_view.packet_id;
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

        if (aws_byte_buf_append_and_update(&subscribe_storage->storage, &copy.topic_filter)) {
            return AWS_OP_ERR;
        }

        if (aws_array_list_push_back(&subscribe_storage->subscriptions, &copy)) {
            return AWS_OP_ERR;
        }
    }

    return AWS_OP_SUCCESS;
}

static size_t s_aws_mqtt5_packet_subscribe_compute_storage_size(
    const struct aws_mqtt5_packet_subscribe_view *subscribe_view) {
    size_t storage_size = s_aws_mqtt5_user_property_set_compute_storage_size(
        subscribe_view->user_properties, subscribe_view->user_property_count);

    for (size_t i = 0; i < subscribe_view->subscription_count; ++i) {
        const struct aws_mqtt5_subscription_view *subscription = &subscribe_view->subscriptions[i];
        storage_size += subscription->topic_filter.len;
    }

    return storage_size;
}

int aws_mqtt5_packet_subscribe_storage_init(
    struct aws_mqtt5_packet_subscribe_storage *subscribe_storage,
    struct aws_allocator *allocator,
    const struct aws_mqtt5_packet_subscribe_view *subscribe_options) {

    AWS_ZERO_STRUCT(*subscribe_storage);
    size_t storage_capacity = s_aws_mqtt5_packet_subscribe_compute_storage_size(subscribe_options);
    if (aws_byte_buf_init(&subscribe_storage->storage, allocator, storage_capacity)) {
        return AWS_OP_ERR;
    }

    subscribe_storage->storage_view.packet_id = subscribe_options->packet_id;

    if (subscribe_options->subscription_identifier != NULL) {
        subscribe_storage->subscription_identifier = *subscribe_options->subscription_identifier;
        subscribe_storage->subscription_identifier_ptr = &subscribe_storage->subscription_identifier;
    }

    if (s_aws_mqtt5_packet_subscribe_storage_init_subscriptions(
            subscribe_storage, allocator, subscribe_options->subscription_count, subscribe_options->subscriptions)) {
        return AWS_OP_ERR;
    }

    if (aws_mqtt5_user_property_set_init_with_storage(
            &subscribe_storage->user_properties,
            allocator,
            &subscribe_storage->storage,
            subscribe_options->user_property_count,
            subscribe_options->user_properties)) {
        return AWS_OP_ERR;
    }
    aws_mqtt5_packet_subscribe_view_init_from_storage(&subscribe_storage->storage_view, subscribe_storage);

    return AWS_OP_SUCCESS;
}

int aws_mqtt5_packet_subscribe_storage_init_from_external_storage(
    struct aws_mqtt5_packet_subscribe_storage *subscribe_storage,
    struct aws_allocator *allocator) {
    AWS_ZERO_STRUCT(*subscribe_storage);

    if (aws_mqtt5_user_property_set_init(&subscribe_storage->user_properties, allocator)) {
        return AWS_OP_ERR;
    }

    if (aws_array_list_init_dynamic(
            &subscribe_storage->subscriptions, allocator, 0, sizeof(struct aws_mqtt5_subscription_view))) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

static void s_aws_mqtt5_operation_subscribe_complete(
    struct aws_mqtt5_operation *operation,
    int error_code,
    const void *completion_view) {
    struct aws_mqtt5_operation_subscribe *subscribe_op = operation->impl;

    if (subscribe_op->completion_options.completion_callback != NULL) {
        (*subscribe_op->completion_options.completion_callback)(
            completion_view, error_code, subscribe_op->completion_options.completion_user_data);
    }
}

static void s_aws_mqtt5_operation_subscribe_set_packet_id(
    struct aws_mqtt5_operation *operation,
    aws_mqtt5_packet_id_t packet_id) {
    struct aws_mqtt5_operation_subscribe *subscribe_op = operation->impl;
    subscribe_op->options_storage.storage_view.packet_id = packet_id;
}

static aws_mqtt5_packet_id_t *s_aws_mqtt5_operation_subscribe_get_packet_id_address(
    const struct aws_mqtt5_operation *operation) {
    struct aws_mqtt5_operation_subscribe *subscribe_op = operation->impl;
    return &subscribe_op->options_storage.storage_view.packet_id;
}

static struct aws_mqtt5_operation_vtable s_subscribe_operation_vtable = {
    .aws_mqtt5_operation_completion_fn = s_aws_mqtt5_operation_subscribe_complete,
    .aws_mqtt5_operation_set_packet_id_fn = s_aws_mqtt5_operation_subscribe_set_packet_id,
    .aws_mqtt5_operation_get_packet_id_address_fn = s_aws_mqtt5_operation_subscribe_get_packet_id_address,
};

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
    subscribe_op->base.vtable = &s_subscribe_operation_vtable;
    subscribe_op->base.packet_type = AWS_MQTT5_PT_SUBSCRIBE;
    aws_ref_count_init(&subscribe_op->base.ref_count, subscribe_op, s_destroy_operation_subscribe);
    subscribe_op->base.impl = subscribe_op;

    if (aws_mqtt5_packet_subscribe_storage_init(&subscribe_op->options_storage, allocator, subscribe_options)) {
        goto error;
    }

    subscribe_op->base.packet_view = &subscribe_op->options_storage.storage_view;

    if (completion_options != NULL) {
        subscribe_op->completion_options = *completion_options;
    }

    return subscribe_op;

error:

    aws_mqtt5_operation_release(&subscribe_op->base);

    return NULL;
}

/*********************************************************************************************************************
 * Suback
 ********************************************************************************************************************/

int aws_mqtt5_packet_suback_storage_init_from_external_storage(
    struct aws_mqtt5_packet_suback_storage *suback_storage,
    struct aws_allocator *allocator) {
    AWS_ZERO_STRUCT(*suback_storage);

    if (aws_mqtt5_user_property_set_init(&suback_storage->user_properties, allocator)) {
        return AWS_OP_ERR;
    }

    if (aws_array_list_init_dynamic(
            &suback_storage->reason_codes, allocator, 0, sizeof(enum aws_mqtt5_suback_reason_code))) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

void aws_mqtt5_packet_suback_storage_clean_up(struct aws_mqtt5_packet_suback_storage *suback_storage) {
    if (suback_storage == NULL) {
        return;
    }
    aws_mqtt5_user_property_set_clean_up(&suback_storage->user_properties);

    aws_array_list_clean_up(&suback_storage->reason_codes);

    aws_byte_buf_clean_up(&suback_storage->storage);
}

void aws_mqtt5_packet_suback_view_log(
    const struct aws_mqtt5_packet_suback_view *suback_view,
    enum aws_log_level level) {
    struct aws_logger *logger = aws_logger_get();
    if (logger == NULL || logger->vtable->get_log_level(logger, AWS_LS_MQTT5_GENERAL) < level) {
        return;
    }

    /* TODO: constantly checking the log level at this point is kind of dumb but there's no better API atm */

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "(%p) aws_mqtt5_packet_suback_view packet id set to %d",
        (void *)suback_view,
        (int)suback_view->packet_id);

    for (size_t i = 0; i < suback_view->reason_code_count; ++i) {
        enum aws_mqtt5_unsuback_reason_code reason_code = suback_view->reason_codes[i];
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: topic %zu reason code:%d %s",
            (void *)suback_view,
            i,
            (int)reason_code,
            aws_mqtt5_suback_reason_code_to_c_string(reason_code));
    }

    s_aws_mqtt5_user_property_set_log(
        suback_view->user_properties,
        suback_view->user_property_count,
        (void *)suback_view,
        level,
        "aws_mqtt5_packet_suback_view");
}

void aws_mqtt5_packet_suback_view_init_from_storage(
    struct aws_mqtt5_packet_suback_view *suback_view,
    const struct aws_mqtt5_packet_suback_storage *suback_storage) {

    suback_view->packet_id = suback_storage->packet_id;
    suback_view->reason_string = suback_storage->reason_string_ptr;

    suback_view->user_property_count = aws_mqtt5_user_property_set_size(&suback_storage->user_properties);
    suback_view->user_properties = suback_storage->user_properties.properties.data;

    suback_view->reason_code_count = aws_array_list_length(&suback_storage->reason_codes);
    suback_view->reason_codes = suback_storage->reason_codes.data;
}

/*********************************************************************************************************************
 * PINGREQ
 ********************************************************************************************************************/

static void s_destroy_operation_pingreq(void *object) {
    if (object == NULL) {
        return;
    }

    struct aws_mqtt5_operation_pingreq *pingreq_op = object;
    aws_mem_release(pingreq_op->allocator, pingreq_op);
}

struct aws_mqtt5_operation_pingreq *aws_mqtt5_operation_pingreq_new(struct aws_allocator *allocator) {
    struct aws_mqtt5_operation_pingreq *pingreq_op =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt5_operation_pingreq));
    if (pingreq_op == NULL) {
        return NULL;
    }

    pingreq_op->allocator = allocator;
    pingreq_op->base.vtable = &s_empty_operation_vtable;
    pingreq_op->base.packet_type = AWS_MQTT5_PT_PINGREQ;
    aws_ref_count_init(&pingreq_op->base.ref_count, pingreq_op, s_destroy_operation_pingreq);
    pingreq_op->base.impl = pingreq_op;

    return pingreq_op;
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

    if (options->bootstrap == NULL) {
        AWS_LOGF_ERROR(AWS_LS_MQTT5_GENERAL, "client bootstrap not set in mqtt5 client configuration");
        return aws_raise_error(AWS_ERROR_MQTT5_CLIENT_OPTIONS_VALIDATION);
    }

    /* forbid no-timeout until someone convinces me otherwise */
    if (options->socket_options == NULL || options->socket_options->type == AWS_SOCKET_DGRAM ||
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

    /* The client will not behave properly if ping timeout is not significantly shorter than the keep alive interval */
    uint64_t keep_alive_ms = aws_timestamp_convert(
        options->connect_options->keep_alive_interval_seconds, AWS_TIMESTAMP_SECS, AWS_TIMESTAMP_MILLIS, NULL);
    uint64_t one_second_ms = aws_timestamp_convert(1, AWS_TIMESTAMP_SECS, AWS_TIMESTAMP_MILLIS, NULL);
    if (options->ping_timeout_ms + one_second_ms > keep_alive_ms) {
        AWS_LOGF_ERROR(AWS_LS_MQTT5_GENERAL, "keep alive interval is too small relative to ping timeout interval");
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
        "id=%p: aws_mqtt5_client_options_storage %s tls options set:",
        (void *)options_storage,
        log_text);
    if (tls_options->advertise_alpn_message && tls_options->alpn_list) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_client_options_storage %s tls options alpn protocol list set to \"%s\"",
            (void *)options_storage,
            log_text,
            aws_string_c_str(tls_options->alpn_list));
    } else {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_client_options_storage %s tls options alpn not used",
            (void *)options_storage,
            log_text);
    }

    if (tls_options->server_name) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_client_options_storage %s tls options SNI value set to \"%s\"",
            (void *)options_storage,
            log_text,
            aws_string_c_str(tls_options->server_name));
    } else {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_client_options_storage %s tls options SNI not used",
            (void *)options_storage,
            log_text);
    }

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "id=%p: aws_mqtt5_client_options_storage %s tls options tls context set to (%p)",
        (void *)options_storage,
        log_text,
        (void *)(tls_options->ctx));
    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "id=%p: aws_mqtt5_client_options_storage %s tls options handshake timeout set to %" PRIu32,
        (void *)options_storage,
        log_text,
        tls_options->timeout_ms);
}

void aws_mqtt5_client_options_storage_log(
    const struct aws_mqtt5_client_options_storage *options_storage,
    enum aws_log_level level) {
    struct aws_logger *temp_logger = aws_logger_get();
    if (temp_logger == NULL || temp_logger->vtable->get_log_level(temp_logger, AWS_LS_MQTT5_GENERAL) < level) {
        return;
    }

    /* TODO: constantly checking the log level at this point is kind of dumb but there's no better API atm */

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "id=%p: aws_mqtt5_client_options_storage host name set to %s",
        (void *)options_storage,
        aws_string_c_str(options_storage->host_name));

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "id=%p: aws_mqtt5_client_options_storage port set to %" PRIu16,
        (void *)options_storage,
        options_storage->port);
    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "id=%p: aws_mqtt5_client_options_storage client bootstrap set to (%p)",
        (void *)options_storage,
        (void *)options_storage->bootstrap);

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "id=%p: aws_mqtt5_client_options_storage socket options set to: type = %d, domain = %d, connect_timeout_ms = "
        "%" PRIu32,
        (void *)options_storage,
        (int)options_storage->socket_options.type,
        (int)options_storage->socket_options.domain,
        options_storage->socket_options.connect_timeout_ms);
    if (options_storage->socket_options.keepalive) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_client_options_storage socket keepalive options set to: keep_alive_interval_sec = "
            "%" PRIu16 ", "
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
            "id=%p: aws_mqtt5_client_options_storage using http proxy:",
            (void *)options_storage);

        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_client_options_storage http proxy host name set to " PRInSTR,
            (void *)options_storage,
            AWS_BYTE_CURSOR_PRI(options_storage->http_proxy_options.host));

        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_client_options_storage http proxy port set to %" PRIu16,
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
                "id=%p: aws_mqtt5_client_options_storage http proxy strategy set to (%p)",
                (void *)options_storage,
                (void *)options_storage->http_proxy_options.proxy_strategy);
        }
    }

    if (options_storage->websocket_handshake_transform != NULL) {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_client_options_storage enabling websockets",
            (void *)options_storage);

        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: aws_mqtt5_client_options_storage websocket handshake transform user data set to (%p)",
            (void *)options_storage,
            options_storage->websocket_handshake_transform_user_data);
    } else {
        AWS_LOGF(
            level,
            AWS_LS_MQTT5_GENERAL,
            "id=%p: mqtt5_client_options_storage disabling websockets",
            (void *)options_storage);
    }

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "id=%p: aws_mqtt5_client_options_storage outbound topic aliasing behavior set to %d(%s)",
        (void *)options_storage,
        (int)options_storage->outbound_topic_aliasing_behavior,
        aws_mqtt5_outbound_topic_alias_behavior_type_to_c_string(options_storage->outbound_topic_aliasing_behavior));

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "id=%p: aws_mqtt5_client_options_storage offline queue behavior set to %d(%s)",
        (void *)options_storage,
        (int)options_storage->offline_queue_behavior,
        aws_mqtt5_client_offline_queue_behavior_type_to_c_string(options_storage->offline_queue_behavior));

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "id=%p: aws_mqtt5_client_options_storage reconnect jitter mode set to %d",
        (void *)options_storage,
        (int)options_storage->retry_jitter_mode);

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "id=%p: mqtt5_client_options_storage reconnect delay min set to %" PRIu64 " ms, max set to %" PRIu64 " ms",
        (void *)options_storage,
        options_storage->min_reconnect_delay_ms,
        options_storage->max_reconnect_delay_ms);

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "id=%p: aws_mqtt5_client_options_storage minimum necessary connection time in order to reset the reconnect "
        "delay "
        "set "
        "to %" PRIu64 " ms",
        (void *)options_storage,
        options_storage->min_connected_time_to_reset_reconnect_delay_ms);

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "id=%p: aws_mqtt5_client_options_storage ping timeout interval set to %" PRIu32 " ms",
        (void *)options_storage,
        options_storage->ping_timeout_ms);

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "id=%p: aws_mqtt5_client_options_storage connack timeout interval set to %" PRIu32 " ms",
        (void *)options_storage,
        options_storage->connack_timeout_ms);

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "id=%p: aws_mqtt5_client_options_storage connect options:",
        (void *)options_storage);

    aws_mqtt5_packet_connect_view_log(&options_storage->connect.storage_view, level);

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "id=%p: aws_mqtt5_client_options_storage lifecycle event handler user data set to (%p)",
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

    options_storage->offline_queue_behavior = options->offline_queue_behavior;
    options_storage->retry_jitter_mode = options->retry_jitter_mode;
    options_storage->min_reconnect_delay_ms = options->min_reconnect_delay_ms;
    options_storage->max_reconnect_delay_ms = options->max_reconnect_delay_ms;
    options_storage->min_connected_time_to_reset_reconnect_delay_ms =
        options->min_connected_time_to_reset_reconnect_delay_ms;

    options_storage->ping_timeout_ms = options->ping_timeout_ms;
    options_storage->connack_timeout_ms = options->connack_timeout_ms;

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
