/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/private/v5/mqtt5_operation.h>

#include <aws/common/string.h>
#include <aws/mqtt/private/v5/mqtt5_utils.h>
#include <aws/mqtt/v5/operations/mqtt5_operation_connect.h>
#include <aws/mqtt/v5/operations/mqtt5_operation_disconnect.h>
#include <aws/mqtt/v5/operations/mqtt5_operation_publish.h>
#include <aws/mqtt/v5/operations/mqtt5_operation_subscribe.h>
#include <aws/mqtt/v5/operations/mqtt5_operation_unsubscribe.h>

#include <inttypes.h>

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

static void s_destroy_operation_connect(void *object) {
    if (object == NULL) {
        return;
    }

    AWS_LOGF_TRACE(AWS_LS_MQTT5_OPERATION, "(%p) aws_mqtt5_operation_connect destroy", object);

    struct aws_mqtt5_operation_connect *connect_op = object;

    aws_string_destroy(connect_op->client_id);
    aws_string_destroy(connect_op->username);
    aws_byte_buf_clean_up_secure(&connect_op->password);
    aws_mqtt5_operation_publish_release(connect_op->will);

    aws_mqtt5_clear_user_properties_array_list(&connect_op->user_properties);
    aws_array_list_clean_up(&connect_op->user_properties);

    aws_mem_release(connect_op->allocator, connect_op);
}

struct aws_mqtt5_operation_connect *aws_mqtt5_operation_connect_new(struct aws_allocator *allocator) {
    AWS_PRECONDITION(allocator != NULL);

    struct aws_mqtt5_operation_connect *connect_op =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt5_operation_connect));
    if (connect_op == NULL) {
        return NULL;
    }

    connect_op->allocator = allocator;
    connect_op->base.operation_type = AWS_MOT_CONNECT;
    aws_ref_count_init(&connect_op->base.ref_count, connect_op, s_destroy_operation_connect);
    connect_op->base.impl = connect_op;

    if (aws_array_list_init_dynamic(
            &connect_op->user_properties, allocator, 0, sizeof(struct aws_mqtt5_name_value_pair))) {
        goto error;
    }

    /* non-zero defaults */
    connect_op->keep_alive_interval_seconds = AWS_MQTT5_DEFAULT_KEEP_ALIVE_INTERVAL_SECONDS;
    connect_op->request_problem_information = true;
    connect_op->session_expiry_interval_seconds = AWS_MQTT5_DEFAULT_SESSION_EXPIRY_INTERVAL_SECONDS;

    return connect_op;

error:

    aws_mqtt5_operation_connect_release(connect_op);

    return NULL;
}

struct aws_mqtt5_operation_connect *aws_mqtt5_operation_connect_acquire(
    struct aws_mqtt5_operation_connect *connect_operation) {
    if (connect_operation != NULL) {
        AWS_LOGF_TRACE(AWS_LS_MQTT5_OPERATION, "(%p) aws_mqtt5_operation_connect_acquire", (void *)connect_operation);

        aws_mqtt5_operation_acquire(&connect_operation->base);
    }

    return connect_operation;
}

struct aws_mqtt5_operation_connect *aws_mqtt5_operation_connect_release(
    struct aws_mqtt5_operation_connect *connect_operation) {
    if (connect_operation != NULL) {
        AWS_LOGF_TRACE(AWS_LS_MQTT5_OPERATION, "(%p) aws_mqtt5_operation_connect_release", (void *)connect_operation);

        aws_mqtt5_operation_release(&connect_operation->base);
    }

    return NULL;
}

void aws_mqtt5_operation_connect_set_keep_alive_interval(
    struct aws_mqtt5_operation_connect *connect_operation,
    uint32_t keep_alive_interval_seconds) {
    AWS_PRECONDITION(connect_operation != NULL);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_CONFIG,
        "(%p) aws_mqtt5_operation_connect keep alive interval set to %" PRIu32,
        (void *)connect_operation,
        keep_alive_interval_seconds);

    connect_operation->keep_alive_interval_seconds = keep_alive_interval_seconds;
}

int aws_mqtt5_operation_connect_set_client_id(
    struct aws_mqtt5_operation_connect *connect_operation,
    struct aws_byte_cursor client_id) {
    AWS_PRECONDITION(connect_operation != NULL);

    return aws_mqtt5_operation_set_string_property(
        &connect_operation->client_id,
        connect_operation->allocator,
        client_id,
        AWS_LS_MQTT5_OPERATION,
        connect_operation,
        "aws_mqtt5_operation_connect client id");
}

int aws_mqtt5_operation_connect_set_username(
    struct aws_mqtt5_operation_connect *connect_operation,
    struct aws_byte_cursor username) {
    AWS_PRECONDITION(connect_operation != NULL);

    return aws_mqtt5_operation_set_string_property(
        &connect_operation->username,
        connect_operation->allocator,
        username,
        AWS_LS_MQTT5_OPERATION,
        connect_operation,
        "aws_mqtt5_operation_connect username ");
}

int aws_mqtt5_operation_connect_set_password(
    struct aws_mqtt5_operation_connect *connect_operation,
    struct aws_byte_cursor password) {
    AWS_PRECONDITION(connect_operation != NULL);

    connect_operation->password_ptr = NULL;
    if (aws_byte_buf_init_conditional_from_cursor(
            &connect_operation->password, connect_operation->allocator, password)) {
        return AWS_OP_ERR;
    }

    AWS_LOGF_DEBUG(AWS_LS_MQTT5_CONFIG, "(%p) aws_mqtt5_operation_publish - set password", (void *)connect_operation);

    connect_operation->password_ptr = &connect_operation->password;

    return AWS_OP_SUCCESS;
}

void aws_mqtt5_operation_connect_set_session_expiry_interval(
    struct aws_mqtt5_operation_connect *connect_operation,
    uint32_t session_expiry_interval_seconds) {
    AWS_PRECONDITION(connect_operation != NULL);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_CONFIG,
        "(%p) aws_mqtt5_operation_connect session expiry interval set to %" PRIu32,
        (void *)connect_operation,
        session_expiry_interval_seconds);

    connect_operation->session_expiry_interval_seconds = session_expiry_interval_seconds;
}

void aws_mqtt5_operation_connect_set_session_behavior(
    struct aws_mqtt5_operation_connect *connect_operation,
    enum aws_mqtt5_client_session_behavior_type session_behavior) {
    AWS_PRECONDITION(connect_operation != NULL);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_CONFIG,
        "(%p) aws_mqtt5_operation_connect session behavior set to %d(%s)",
        (void *)connect_operation,
        (int)session_behavior,
        aws_mqtt5_client_session_behavior_type_to_c_string(session_behavior));

    connect_operation->session_behavior = session_behavior;
}

void aws_mqtt5_operation_connect_set_request_response_information(
    struct aws_mqtt5_operation_connect *connect_operation,
    bool request_response_information) {
    AWS_PRECONDITION(connect_operation != NULL);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_CONFIG,
        "(%p) aws_mqtt5_operation_connect request response information set to %d",
        (void *)connect_operation,
        (int)request_response_information);

    connect_operation->request_response_information = request_response_information;
}

void aws_mqtt5_operation_connect_set_request_problem_information(
    struct aws_mqtt5_operation_connect *connect_operation,
    bool request_problem_information) {
    AWS_PRECONDITION(connect_operation != NULL);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_CONFIG,
        "(%p) aws_mqtt5_operation_connect request problem information set to %d",
        (void *)connect_operation,
        (int)request_problem_information);

    connect_operation->request_problem_information = request_problem_information;
}

void aws_mqtt5_operation_connect_set_receive_maximum(
    struct aws_mqtt5_operation_connect *connect_operation,
    uint16_t receive_maximum) {
    AWS_PRECONDITION(connect_operation != NULL);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_CONFIG,
        "(%p) aws_mqtt5_operation_connect receive maximum set to %" PRIu16,
        (void *)connect_operation,
        receive_maximum);

    connect_operation->receive_maximum = receive_maximum;
}

void aws_mqtt5_operation_connect_set_topic_alias_maximum(
    struct aws_mqtt5_operation_connect *connect_operation,
    uint16_t topic_alias_maximum) {
    AWS_PRECONDITION(connect_operation != NULL);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_CONFIG,
        "(%p) aws_mqtt5_operation_connect topic alias maximum set to %" PRIu16,
        (void *)connect_operation,
        topic_alias_maximum);

    connect_operation->topic_alias_maximum = topic_alias_maximum;
}

void aws_mqtt5_operation_connect_set_maximum_packet_size(
    struct aws_mqtt5_operation_connect *connect_operation,
    uint32_t maximum_packet_size_bytes) {
    AWS_PRECONDITION(connect_operation != NULL);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_CONFIG,
        "(%p) aws_mqtt5_operation_connect maximum packet size set to %" PRIu32,
        (void *)connect_operation,
        maximum_packet_size_bytes);

    connect_operation->maximum_packet_size_bytes = maximum_packet_size_bytes;
}

void aws_mqtt5_operation_connect_set_will(
    struct aws_mqtt5_operation_connect *connect_operation,
    struct aws_mqtt5_operation_publish *will) {

    AWS_PRECONDITION(connect_operation != NULL);

    aws_mqtt5_operation_publish_release(connect_operation->will);
    connect_operation->will = aws_mqtt5_operation_publish_acquire(will);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_CONFIG,
        "(%p) aws_mqtt5_operation_connect set will to (%p)",
        (void *)connect_operation,
        (void *)will);
}

void aws_mqtt5_operation_connect_set_will_delay_interval(
    struct aws_mqtt5_operation_connect *connect_operation,
    uint32_t will_delay_interval_seconds) {
    AWS_PRECONDITION(connect_operation != NULL);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_CONFIG,
        "(%p) aws_mqtt5_operation_connect will delay interval set to %" PRIu32,
        (void *)connect_operation,
        will_delay_interval_seconds);

    connect_operation->will_delay_interval_seconds = will_delay_interval_seconds;
    connect_operation->will_delay_interval_seconds_ptr = &connect_operation->will_delay_interval_seconds;
}

int aws_mqtt5_operation_connect_add_user_property(
    struct aws_mqtt5_operation_connect *connect_operation,
    struct aws_mqtt5_user_property *property) {
    AWS_PRECONDITION(connect_operation != NULL);

    return aws_mqtt5_add_user_property_to_array_list(
        connect_operation->allocator,
        property,
        &connect_operation->user_properties,
        AWS_LS_MQTT5_OPERATION,
        connect_operation,
        "aws_mqtt5_operation_connect - adding user property");
}

/*********************************************************************************************************************
 * Disconnect
 ********************************************************************************************************************/

static void s_destroy_operation_disconnect(void *object) {
    if (object == NULL) {
        return;
    }

    AWS_LOGF_TRACE(AWS_LS_MQTT5_OPERATION, "(%p) aws_mqtt5_operation_disconnect destroy", object);

    struct aws_mqtt5_operation_disconnect *disconnect_op = object;

    aws_string_destroy(disconnect_op->reason_string);

    aws_mqtt5_clear_user_properties_array_list(&disconnect_op->user_properties);
    aws_array_list_clean_up(&disconnect_op->user_properties);

    aws_mem_release(disconnect_op->allocator, disconnect_op);
}

struct aws_mqtt5_operation_disconnect *aws_mqtt5_operation_disconnect_new(struct aws_allocator *allocator) {
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

    if (aws_array_list_init_dynamic(
            &disconnect_op->user_properties, allocator, 0, sizeof(struct aws_mqtt5_name_value_pair))) {
        goto error;
    }

    return disconnect_op;

error:

    aws_mqtt5_operation_disconnect_release(disconnect_op);

    return NULL;
}

struct aws_mqtt5_operation_disconnect *aws_mqtt5_operation_disconnect_acquire(
    struct aws_mqtt5_operation_disconnect *disconnect_operation) {
    if (disconnect_operation != NULL) {
        AWS_LOGF_TRACE(
            AWS_LS_MQTT5_OPERATION, "(%p) aws_mqtt5_operation_disconnect_acquire", (void *)disconnect_operation);

        aws_mqtt5_operation_acquire(&disconnect_operation->base);
    }

    return disconnect_operation;
}

struct aws_mqtt5_operation_disconnect *aws_mqtt5_operation_disconnect_release(
    struct aws_mqtt5_operation_disconnect *disconnect_operation) {
    if (disconnect_operation != NULL) {
        AWS_LOGF_TRACE(
            AWS_LS_MQTT5_OPERATION, "(%p) aws_mqtt5_operation_disconnect_release", (void *)disconnect_operation);

        aws_mqtt5_operation_release(&disconnect_operation->base);
    }

    return NULL;
}

void aws_mqtt5_operation_disconnect_set_reason_code(
    struct aws_mqtt5_operation_disconnect *disconnect_operation,
    enum aws_mqtt5_disconnect_reason_code reason_code) {
    AWS_PRECONDITION(disconnect_operation != NULL);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_OPERATION,
        "(%p) aws_mqtt5_operation_disconnect reason code set to %d(%s)",
        (void *)disconnect_operation,
        (int)reason_code,
        aws_mqtt5_disconnect_reason_code_to_c_string(reason_code));

    disconnect_operation->reason_code = reason_code;
}

void aws_mqtt5_operation_disconnect_set_session_expiry_interval(
    struct aws_mqtt5_operation_disconnect *disconnect_operation,
    uint32_t session_expiry_interval_seconds) {
    AWS_PRECONDITION(disconnect_operation != NULL);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_OPERATION,
        "(%p) aws_mqtt5_operation_disconnect session expiry interval set to %" PRIu32,
        (void *)disconnect_operation,
        session_expiry_interval_seconds);

    disconnect_operation->session_expiry_interval_seconds = session_expiry_interval_seconds;
    disconnect_operation->session_expiry_interval_seconds_ptr = &disconnect_operation->session_expiry_interval_seconds;
}

int aws_mqtt5_operation_disconnect_set_reason_string(
    struct aws_mqtt5_operation_disconnect *disconnect_operation,
    struct aws_byte_cursor reason) {

    return aws_mqtt5_operation_set_string_property(
        &disconnect_operation->reason_string,
        disconnect_operation->allocator,
        reason,
        AWS_LS_MQTT5_OPERATION,
        disconnect_operation,
        "aws_mqtt5_operation_disconnect reason string");
}

int aws_mqtt5_operation_disconnect_add_user_property(
    struct aws_mqtt5_operation_disconnect *disconnect_operation,
    struct aws_mqtt5_user_property *property) {
    AWS_PRECONDITION(disconnect_operation != NULL);

    return aws_mqtt5_add_user_property_to_array_list(
        disconnect_operation->allocator,
        property,
        &disconnect_operation->user_properties,
        AWS_LS_MQTT5_OPERATION,
        disconnect_operation,
        "aws_mqtt5_operation_disconnect - adding user property");
}

/*********************************************************************************************************************
 * Publish
 ********************************************************************************************************************/

static void s_destroy_operation_publish(void *object) {
    if (object == NULL) {
        return;
    }

    AWS_LOGF_TRACE(AWS_LS_MQTT5_OPERATION, "(%p) aws_mqtt5_operation_publish destroy", object);

    struct aws_mqtt5_operation_publish *publish_op = object;

    aws_byte_buf_clean_up(&publish_op->payload);
    aws_string_destroy(publish_op->topic);
    aws_string_destroy(publish_op->response_topic);
    aws_byte_buf_clean_up(&publish_op->correlation_data);
    aws_string_destroy(publish_op->content_type);

    aws_mqtt5_clear_user_properties_array_list(&publish_op->user_properties);
    aws_array_list_clean_up(&publish_op->user_properties);

    aws_mem_release(publish_op->allocator, publish_op);
}

struct aws_mqtt5_operation_publish *aws_mqtt5_operation_publish_new(struct aws_allocator *allocator) {
    AWS_PRECONDITION(allocator != NULL);

    struct aws_mqtt5_operation_publish *publish_op =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt5_operation_publish));
    if (publish_op == NULL) {
        return NULL;
    }

    publish_op->allocator = allocator;
    publish_op->base.operation_type = AWS_MOT_PUBLISH;
    aws_ref_count_init(&publish_op->base.ref_count, publish_op, s_destroy_operation_publish);
    publish_op->base.impl = publish_op;

    if (aws_array_list_init_dynamic(
            &publish_op->user_properties, allocator, 0, sizeof(struct aws_mqtt5_name_value_pair))) {
        goto error;
    }

    publish_op->payload_format = AWS_MQTT5_PFI_NOT_SET;

    return publish_op;

error:

    aws_mqtt5_operation_publish_release(publish_op);

    return NULL;
}

struct aws_mqtt5_operation_publish *aws_mqtt5_operation_publish_acquire(
    struct aws_mqtt5_operation_publish *publish_operation) {
    if (publish_operation != NULL) {
        AWS_LOGF_TRACE(AWS_LS_MQTT5_OPERATION, "(%p) aws_mqtt5_operation_publish_acquire", (void *)publish_operation);

        aws_mqtt5_operation_acquire(&publish_operation->base);
    }

    return publish_operation;
}

struct aws_mqtt5_operation_publish *aws_mqtt5_operation_publish_release(
    struct aws_mqtt5_operation_publish *publish_operation) {
    if (publish_operation != NULL) {
        AWS_LOGF_TRACE(AWS_LS_MQTT5_OPERATION, "(%p) aws_mqtt5_operation_publish_release", (void *)publish_operation);

        aws_mqtt5_operation_release(&publish_operation->base);
    }

    return NULL;
}

int aws_mqtt5_operation_publish_set_payload_by_cursor(
    struct aws_mqtt5_operation_publish *publish_operation,
    struct aws_byte_cursor payload) {
    AWS_PRECONDITION(publish_operation != NULL);

    if (aws_byte_buf_init_conditional_from_cursor(&publish_operation->payload, publish_operation->allocator, payload)) {
        return AWS_OP_ERR;
    }

    AWS_LOGF_DEBUG(AWS_LS_MQTT5_CONFIG, "(%p) aws_mqtt5_operation_publish - set payload", (void *)publish_operation);

    return AWS_OP_SUCCESS;
}

void aws_mqtt5_operation_publish_set_qos(
    struct aws_mqtt5_operation_publish *publish_operation,
    enum aws_mqtt5_qos qos) {
    AWS_PRECONDITION(publish_operation != NULL);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_CONFIG, "(%p) aws_mqtt5_operation_publish qos set to %d", (void *)publish_operation, (int)qos);

    publish_operation->qos = qos;
}

void aws_mqtt5_operation_publish_set_retain(struct aws_mqtt5_operation_publish *publish_operation, bool retain) {
    AWS_PRECONDITION(publish_operation != NULL);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_CONFIG,
        "(%p) aws_mqtt5_operation_publish retain set to %d",
        (void *)publish_operation,
        (int)retain);

    publish_operation->retain = retain;
}

int aws_mqtt5_operation_publish_set_topic(
    struct aws_mqtt5_operation_publish *publish_operation,
    struct aws_byte_cursor topic) {
    AWS_PRECONDITION(publish_operation != NULL);

    return aws_mqtt5_operation_set_string_property(
        &publish_operation->topic,
        publish_operation->allocator,
        topic,
        AWS_LS_MQTT5_OPERATION,
        publish_operation,
        "aws_mqtt5_operation_publish topic");
}

void aws_mqtt5_operation_publish_set_payload_format_indicator(
    struct aws_mqtt5_operation_publish *publish_operation,
    enum aws_mqtt5_payload_format_indicator payload_format) {
    AWS_PRECONDITION(publish_operation != NULL);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_OPERATION,
        "(%p) aws_mqtt5_operation_publish payload format indicator set to %d(%s)",
        (void *)publish_operation,
        (int)payload_format,
        aws_mqtt5_payload_format_indicator_to_c_string(payload_format));

    publish_operation->payload_format = payload_format;
}

void aws_mqtt5_operation_publish_set_message_expiry_interval(
    struct aws_mqtt5_operation_publish *publish_operation,
    uint32_t message_expiry_interval_seconds) {
    AWS_PRECONDITION(publish_operation != NULL);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_OPERATION,
        "(%p) aws_mqtt5_operation_publish message expiry interval set to %" PRIu32,
        (void *)publish_operation,
        message_expiry_interval_seconds);

    publish_operation->message_expiry_interval_seconds = message_expiry_interval_seconds;
    publish_operation->message_expiry_interval_seconds_ptr = &publish_operation->message_expiry_interval_seconds;
}

void aws_mqtt5_operation_publish_set_topic_alias(
    struct aws_mqtt5_operation_publish *publish_operation,
    uint16_t topic_alias) {
    AWS_PRECONDITION(publish_operation != NULL);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_OPERATION,
        "(%p) aws_mqtt5_operation_publish topic alias set to %" PRIu16,
        (void *)publish_operation,
        topic_alias);

    publish_operation->topic_alias = topic_alias;
    publish_operation->topic_alias_ptr = &publish_operation->topic_alias;
}

int aws_mqtt5_operation_publish_set_response_topic(
    struct aws_mqtt5_operation_publish *publish_operation,
    struct aws_byte_cursor response_topic) {
    AWS_PRECONDITION(publish_operation != NULL);

    return aws_mqtt5_operation_set_string_property(
        &publish_operation->response_topic,
        publish_operation->allocator,
        response_topic,
        AWS_LS_MQTT5_OPERATION,
        publish_operation,
        "aws_mqtt5_operation_publish response topic");
}

int aws_mqtt5_operation_publish_set_correlation_data(
    struct aws_mqtt5_operation_publish *publish_operation,
    struct aws_byte_cursor correlation_data) {
    AWS_PRECONDITION(publish_operation != NULL);

    publish_operation->correlation_data_ptr = NULL;
    if (aws_byte_buf_init_conditional_from_cursor(
            &publish_operation->correlation_data, publish_operation->allocator, correlation_data)) {
        return AWS_OP_ERR;
    }

    publish_operation->correlation_data_ptr = &publish_operation->correlation_data;

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_CONFIG, "(%p) aws_mqtt5_operation_publish - set correlation data", (void *)publish_operation);

    return AWS_OP_SUCCESS;
}

int aws_mqtt5_operation_publish_set_subscription_identifier(
    struct aws_mqtt5_operation_publish *publish_operation,
    uint32_t subscription_identifier) {
    AWS_PRECONDITION(publish_operation != NULL);

    if (subscription_identifier > AWS_MQTT5_MAXIMUM_VARIABLE_LENGTH_INTEGER) {
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_OPERATION,
        "(%p) aws_mqtt5_operation_publish subscription identifier set to %" PRIu32,
        (void *)publish_operation,
        subscription_identifier);

    publish_operation->subscription_identifier = subscription_identifier;
    publish_operation->subscription_identifier_ptr = &publish_operation->subscription_identifier;

    return AWS_OP_SUCCESS;
}

int aws_mqtt5_operation_publish_set_content_type(
    struct aws_mqtt5_operation_publish *publish_operation,
    struct aws_byte_cursor content_type) {
    AWS_PRECONDITION(publish_operation != NULL);

    return aws_mqtt5_operation_set_string_property(
        &publish_operation->content_type,
        publish_operation->allocator,
        content_type,
        AWS_LS_MQTT5_OPERATION,
        publish_operation,
        "aws_mqtt5_operation_publish content type");
}

int aws_mqtt5_operation_publish_add_user_property(
    struct aws_mqtt5_operation_publish *publish_operation,
    struct aws_mqtt5_user_property *property) {
    AWS_PRECONDITION(publish_operation != NULL);

    return aws_mqtt5_add_user_property_to_array_list(
        publish_operation->allocator,
        property,
        &publish_operation->user_properties,
        AWS_LS_MQTT5_OPERATION,
        publish_operation,
        "aws_mqtt5_operation_publish - adding user property");
}

/*********************************************************************************************************************
 * Unsubscribe
 ********************************************************************************************************************/

static void s_destroy_operation_unsubscribe(void *object) {
    if (object == NULL) {
        return;
    }

    AWS_LOGF_TRACE(AWS_LS_MQTT5_OPERATION, "(%p) aws_mqtt5_operation_unsubscribe destroy", object);

    struct aws_mqtt5_operation_unsubscribe *unsubscribe_op = object;

    size_t topic_count = aws_array_list_length(&unsubscribe_op->unsubscribe_topics);
    for (size_t i = 0; i < topic_count; ++i) {
        struct aws_string *topic_string = NULL;
        if (aws_array_list_get_at(&unsubscribe_op->unsubscribe_topics, &topic_string, i)) {
            continue;
        }

        aws_string_destroy(topic_string);
    }
    aws_array_list_clean_up(&unsubscribe_op->unsubscribe_topics);

    aws_mqtt5_clear_user_properties_array_list(&unsubscribe_op->user_properties);
    aws_array_list_clean_up(&unsubscribe_op->user_properties);

    aws_mem_release(unsubscribe_op->allocator, unsubscribe_op);
}

struct aws_mqtt5_operation_unsubscribe *aws_mqtt5_operation_unsubscribe_new(struct aws_allocator *allocator) {
    AWS_FATAL_ASSERT(allocator != NULL);

    struct aws_mqtt5_operation_unsubscribe *unsubscribe_op =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt5_operation_unsubscribe));
    if (unsubscribe_op == NULL) {
        return NULL;
    }

    unsubscribe_op->allocator = allocator;
    unsubscribe_op->base.operation_type = AWS_MOT_UNSUBSCRIBE;
    aws_ref_count_init(&unsubscribe_op->base.ref_count, unsubscribe_op, s_destroy_operation_unsubscribe);
    unsubscribe_op->base.impl = unsubscribe_op;

    if (aws_array_list_init_dynamic(
            &unsubscribe_op->user_properties, allocator, 0, sizeof(struct aws_mqtt5_name_value_pair))) {
        goto error;
    }

    if (aws_array_list_init_dynamic(&unsubscribe_op->unsubscribe_topics, allocator, 1, sizeof(struct aws_string *))) {
        goto error;
    }

    return unsubscribe_op;

error:

    aws_mqtt5_operation_unsubscribe_release(unsubscribe_op);

    return NULL;
}

struct aws_mqtt5_operation_unsubscribe *aws_mqtt5_operation_unsubscribe_acquire(
    struct aws_mqtt5_operation_unsubscribe *unsubscribe_operation) {

    if (unsubscribe_operation != NULL) {
        AWS_LOGF_TRACE(
            AWS_LS_MQTT5_OPERATION, "(%p) aws_mqtt5_operation_unsubscribe_acquire", (void *)unsubscribe_operation);

        aws_mqtt5_operation_acquire(&unsubscribe_operation->base);
    }

    return unsubscribe_operation;
}

struct aws_mqtt5_operation_unsubscribe *aws_mqtt5_operation_unsubscribe_release(
    struct aws_mqtt5_operation_unsubscribe *unsubscribe_operation) {

    if (unsubscribe_operation != NULL) {
        AWS_LOGF_TRACE(
            AWS_LS_MQTT5_OPERATION, "(%p) aws_mqtt5_operation_unsubscribe_release", (void *)unsubscribe_operation);

        aws_mqtt5_operation_release(&unsubscribe_operation->base);
    }

    return NULL;
}

int aws_mqtt5_operation_unsubscribe_add_topic(
    struct aws_mqtt5_operation_unsubscribe *unsubscribe_operation,
    struct aws_byte_cursor topic) {

    struct aws_string *topic_string = aws_string_new_from_cursor(unsubscribe_operation->allocator, &topic);
    if (topic_string == NULL) {
        return AWS_OP_ERR;
    }

    if (aws_array_list_push_back(&unsubscribe_operation->unsubscribe_topics, &topic_string)) {
        goto error;
    }

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_OPERATION,
        "(%p) aws_mqtt5_operation_unsubscribe added topic %s",
        (void *)unsubscribe_operation,
        aws_string_c_str(topic_string));

    return AWS_OP_SUCCESS;

error:

    aws_string_destroy(topic_string);

    return AWS_OP_ERR;
}

int aws_mqtt5_operation_unsubscribe_add_user_property(
    struct aws_mqtt5_operation_unsubscribe *unsubscribe_operation,
    struct aws_mqtt5_user_property *property) {

    return aws_mqtt5_add_user_property_to_array_list(
        unsubscribe_operation->allocator,
        property,
        &unsubscribe_operation->user_properties,
        AWS_LS_MQTT5_OPERATION,
        unsubscribe_operation,
        "aws_mqtt5_operation_unsubscribe");
}

/*********************************************************************************************************************
 * Subscribe
 ********************************************************************************************************************/

static void s_destroy_operation_subscribe(void *object) {
    if (object == NULL) {
        return;
    }

    AWS_LOGF_TRACE(AWS_LS_MQTT5_OPERATION, "(%p) aws_mqtt5_operation_subscribe destroy", object);

    struct aws_mqtt5_operation_subscribe *subscribe_op = object;

    size_t subscription_count = aws_array_list_length(&subscribe_op->subscriptions);
    for (size_t i = 0; i < subscription_count; ++i) {
        struct aws_mqtt5_subscription *subscription = NULL;
        if (aws_array_list_get_at_ptr(&subscribe_op->subscriptions, (void **)&subscription, i)) {
            continue;
        }

        aws_string_destroy(subscription->topic);
    }
    aws_array_list_clean_up(&subscribe_op->subscriptions);

    aws_mqtt5_clear_user_properties_array_list(&subscribe_op->user_properties);
    aws_array_list_clean_up(&subscribe_op->user_properties);

    aws_mem_release(subscribe_op->allocator, subscribe_op);
}

struct aws_mqtt5_operation_subscribe *aws_mqtt5_operation_subscribe_new(struct aws_allocator *allocator) {
    AWS_FATAL_ASSERT(allocator != NULL);

    struct aws_mqtt5_operation_subscribe *subscribe_op =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt5_operation_subscribe));
    if (subscribe_op == NULL) {
        return NULL;
    }

    subscribe_op->allocator = allocator;
    subscribe_op->base.operation_type = AWS_MOT_SUBSCRIBE;
    aws_ref_count_init(&subscribe_op->base.ref_count, subscribe_op, s_destroy_operation_subscribe);
    subscribe_op->base.impl = subscribe_op;

    if (aws_array_list_init_dynamic(
            &subscribe_op->user_properties, allocator, 0, sizeof(struct aws_mqtt5_name_value_pair))) {
        goto error;
    }

    if (aws_array_list_init_dynamic(
            &subscribe_op->subscriptions, allocator, 1, sizeof(struct aws_mqtt5_subscription))) {
        goto error;
    }

    return subscribe_op;

error:

    aws_mqtt5_operation_subscribe_release(subscribe_op);

    return NULL;
}

struct aws_mqtt5_operation_subscribe *aws_mqtt5_operation_subscribe_acquire(
    struct aws_mqtt5_operation_subscribe *subscribe_operation) {

    if (subscribe_operation != NULL) {
        AWS_LOGF_TRACE(
            AWS_LS_MQTT5_OPERATION, "(%p) aws_mqtt5_operation_subscribe_acquire", (void *)subscribe_operation);

        aws_mqtt5_operation_acquire(&subscribe_operation->base);
    }

    return subscribe_operation;
}

struct aws_mqtt5_operation_subscribe *aws_mqtt5_operation_subscribe_release(
    struct aws_mqtt5_operation_subscribe *subscribe_operation) {

    if (subscribe_operation != NULL) {
        AWS_LOGF_TRACE(
            AWS_LS_MQTT5_OPERATION, "(%p) aws_mqtt5_operation_subscribe_release", (void *)subscribe_operation);

        aws_mqtt5_operation_release(&subscribe_operation->base);
    }

    return NULL;
}

int aws_mqtt5_operation_subscribe_add_user_property(
    struct aws_mqtt5_operation_subscribe *subscribe_operation,
    struct aws_mqtt5_user_property *property) {

    return aws_mqtt5_add_user_property_to_array_list(
        subscribe_operation->allocator,
        property,
        &subscribe_operation->user_properties,
        AWS_LS_MQTT5_OPERATION,
        subscribe_operation,
        "aws_mqtt5_operation_subscribe");
}

int aws_mqtt5_operation_subscribe_add_subscription(
    struct aws_mqtt5_operation_subscribe *subscribe_operation,
    struct aws_mqtt5_subscripion_view *subscription_view) {
    AWS_PRECONDITION(subscribe_operation != NULL);
    AWS_PRECONDITION(subscription_view != NULL);

    struct aws_mqtt5_subscription subscription;
    AWS_ZERO_STRUCT(subscription);

    subscription.topic = aws_string_new_from_cursor(subscribe_operation->allocator, &subscription_view->topic);
    if (subscription.topic == NULL) {
        return AWS_OP_ERR;
    }

    subscription.no_local = subscription_view->no_local;
    subscription.qos = subscription_view->qos;
    subscription.retain_as_published = subscription_view->retain_as_published;
    subscription.retain_handling_type = subscription_view->retain_handling_type;

    if (aws_array_list_push_back(&subscribe_operation->subscriptions, &subscription_view)) {
        goto error;
    }

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_OPERATION,
        "(%p) aws_mqtt5_operation_subscribe adding subscription to topic %s with qos %d, no local %d, retain as "
        "published %d, retain handling %d(%s)",
        (void *)subscribe_operation,
        aws_string_c_str(subscription.topic),
        (int)subscription.qos,
        (int)subscription.no_local,
        (int)subscription.retain_as_published,
        (int)subscription.retain_handling_type,
        aws_mqtt5_retain_handling_type_to_c_string(subscription.retain_handling_type));

    return AWS_OP_SUCCESS;

error:

    aws_string_destroy(subscription.topic);

    return AWS_OP_ERR;
}

int aws_mqtt5_operation_subscribe_set_subscription_identifier(
    struct aws_mqtt5_operation_subscribe *subscribe_operation,
    uint32_t subscription_identifier) {
    AWS_PRECONDITION(subscribe_operation != NULL);

    if (subscription_identifier > AWS_MQTT5_MAXIMUM_VARIABLE_LENGTH_INTEGER) {
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    subscribe_operation->subscription_identifier = subscription_identifier;
    subscribe_operation->subscription_identifier_ptr = &subscribe_operation->subscription_identifier;

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_OPERATION,
        "(%p) aws_mqtt5_operation_subscribe subscription identifier set to %" PRIu32,
        (void *)subscribe_operation,
        subscription_identifier);

    return AWS_OP_SUCCESS;
}
