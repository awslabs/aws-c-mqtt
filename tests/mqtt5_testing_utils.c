/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "mqtt5_testing_utils.h"

#include <aws/mqtt/private/v5/mqtt5_encoder.h>
#include <aws/mqtt/private/v5/mqtt5_utils.h>

#include <aws/testing/aws_test_harness.h>

#include <inttypes.h>

int aws_mqtt5_test_verify_user_properties_raw(
    size_t property_count,
    const struct aws_mqtt5_user_property *properties,
    size_t expected_count,
    const struct aws_mqtt5_user_property *expected_properties) {

    ASSERT_UINT_EQUALS(expected_count, property_count);

    for (size_t i = 0; i < expected_count; ++i) {
        const struct aws_mqtt5_user_property *expected_property = &expected_properties[i];
        struct aws_byte_cursor expected_name = expected_property->name;
        struct aws_byte_cursor expected_value = expected_property->value;

        bool found = false;
        for (size_t j = 0; j < property_count; ++j) {
            const struct aws_mqtt5_user_property *nv_pair = &properties[j];

            if (aws_byte_cursor_compare_lexical(&expected_name, &nv_pair->name) == 0 &&
                aws_byte_cursor_compare_lexical(&expected_value, &nv_pair->value) == 0) {
                found = true;
                break;
            }
        }

        if (!found) {
            return AWS_OP_ERR;
        }
    }

    return AWS_OP_SUCCESS;
}

static int s_compute_connack_variable_length_fields(
    struct aws_mqtt5_packet_connack_view *connack_view,
    uint32_t *total_remaining_length,
    uint32_t *property_length) {

    size_t local_property_length =
        aws_mqtt5_compute_user_property_encode_length(connack_view->user_properties, connack_view->user_property_count);

    ADD_OPTIONAL_U32_PROPERTY_LENGTH(connack_view->session_expiry_interval, local_property_length);
    ADD_OPTIONAL_U16_PROPERTY_LENGTH(connack_view->receive_maximum, local_property_length);
    ADD_OPTIONAL_U8_PROPERTY_LENGTH(connack_view->maximum_qos, local_property_length);
    ADD_OPTIONAL_U8_PROPERTY_LENGTH(connack_view->retain_available, local_property_length);
    ADD_OPTIONAL_U32_PROPERTY_LENGTH(connack_view->maximum_packet_size, local_property_length);
    ADD_OPTIONAL_CURSOR_PROPERTY_LENGTH(connack_view->assigned_client_identifier, local_property_length);
    ADD_OPTIONAL_U16_PROPERTY_LENGTH(connack_view->topic_alias_maximum, local_property_length);
    ADD_OPTIONAL_CURSOR_PROPERTY_LENGTH(connack_view->reason_string, local_property_length);
    ADD_OPTIONAL_U8_PROPERTY_LENGTH(connack_view->wildcard_subscriptions_available, local_property_length);
    ADD_OPTIONAL_U8_PROPERTY_LENGTH(connack_view->subscription_identifiers_available, local_property_length);
    ADD_OPTIONAL_U8_PROPERTY_LENGTH(connack_view->shared_subscriptions_available, local_property_length);
    ADD_OPTIONAL_U16_PROPERTY_LENGTH(connack_view->server_keep_alive, local_property_length);
    ADD_OPTIONAL_CURSOR_PROPERTY_LENGTH(connack_view->response_information, local_property_length);
    ADD_OPTIONAL_CURSOR_PROPERTY_LENGTH(connack_view->server_reference, local_property_length);
    ADD_OPTIONAL_CURSOR_PROPERTY_LENGTH(connack_view->authentication_method, local_property_length);
    ADD_OPTIONAL_CURSOR_PROPERTY_LENGTH(connack_view->authentication_data, local_property_length);

    *property_length = (uint32_t)local_property_length;

    size_t property_length_encoding_length = 0;
    if (aws_mqtt5_get_variable_length_encode_size(local_property_length, &property_length_encoding_length)) {
        return AWS_OP_ERR;
    }

    /* reason code (1 byte) + flags (1 byte) */
    *total_remaining_length = *property_length + property_length_encoding_length + 2;

    return AWS_OP_SUCCESS;
}

int aws_mqtt5_encoder_begin_connack(
    struct aws_mqtt5_encoder *encoder,
    struct aws_mqtt5_packet_connack_view *connack_view) {
    uint32_t total_remaining_length = 0;
    uint32_t property_length = 0;
    if (s_compute_connack_variable_length_fields(connack_view, &total_remaining_length, &property_length)) {
        int error_code = aws_last_error();
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_GENERAL,
            "(%p) mqtt5 client encoder - failed to compute variable length values for CONNACK packet with error "
            "%d(%s)",
            (void *)encoder->client,
            error_code,
            aws_error_debug_str(error_code));
        return AWS_OP_ERR;
    }

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_GENERAL,
        "(%p) mqtt5 client encoder - setting up encode for a CONNACK packet with remaining length %" PRIu32,
        (void *)encoder->client,
        total_remaining_length);

    uint8_t flags = connack_view->session_present ? 1 : 0;

    ADD_ENCODE_STEP_U8(encoder, aws_mqtt5_compute_fixed_header_byte1(AWS_MQTT5_PT_CONNACK, 0));
    ADD_ENCODE_STEP_VLI(encoder, total_remaining_length);
    ADD_ENCODE_STEP_U8(encoder, flags);
    ADD_ENCODE_STEP_U8(encoder, (uint8_t)connack_view->reason_code);
    ADD_ENCODE_STEP_VLI(encoder, property_length);

    if (property_length > 0) {
        ADD_ENCODE_STEP_OPTIONAL_U32_PROPERTY(
            encoder, AWS_MQTT5_PROPERTY_TYPE_SESSION_EXPIRY_INTERVAL, connack_view->session_expiry_interval);
        ADD_ENCODE_STEP_OPTIONAL_U16_PROPERTY(
            encoder, AWS_MQTT5_PROPERTY_TYPE_RECEIVE_MAXIMUM, connack_view->receive_maximum);
        ADD_ENCODE_STEP_OPTIONAL_U8_PROPERTY(encoder, AWS_MQTT5_PROPERTY_TYPE_MAXIMUM_QOS, connack_view->maximum_qos);
        ADD_ENCODE_STEP_OPTIONAL_U8_PROPERTY(
            encoder, AWS_MQTT5_PROPERTY_TYPE_RETAIN_AVAILABLE, connack_view->retain_available);
        ADD_ENCODE_STEP_OPTIONAL_U32_PROPERTY(
            encoder, AWS_MQTT5_PROPERTY_TYPE_MAXIMUM_PACKET_SIZE, connack_view->maximum_packet_size);
        ADD_ENCODE_STEP_OPTIONAL_CURSOR_PROPERTY(
            encoder, AWS_MQTT5_PROPERTY_TYPE_ASSIGNED_CLIENT_IDENTIFIER, connack_view->assigned_client_identifier);
        ADD_ENCODE_STEP_OPTIONAL_U16_PROPERTY(
            encoder, AWS_MQTT5_PROPERTY_TYPE_TOPIC_ALIAS_MAXIMUM, connack_view->topic_alias_maximum);
        ADD_ENCODE_STEP_OPTIONAL_CURSOR_PROPERTY(
            encoder, AWS_MQTT5_PROPERTY_TYPE_REASON_STRING, connack_view->reason_string);
        ADD_ENCODE_STEP_OPTIONAL_U8_PROPERTY(
            encoder,
            AWS_MQTT5_PROPERTY_TYPE_WILDCARD_SUBSCRIPTIONS_AVAILABLE,
            connack_view->wildcard_subscriptions_available);
        ADD_ENCODE_STEP_OPTIONAL_U8_PROPERTY(
            encoder,
            AWS_MQTT5_PROPERTY_TYPE_SUBSCRIPTION_IDENTIFIERS_AVAILABLE,
            connack_view->subscription_identifiers_available);
        ADD_ENCODE_STEP_OPTIONAL_U8_PROPERTY(
            encoder,
            AWS_MQTT5_PROPERTY_TYPE_SHARED_SUBSCRIPTIONS_AVAILABLE,
            connack_view->shared_subscriptions_available);
        ADD_ENCODE_STEP_OPTIONAL_U16_PROPERTY(
            encoder, AWS_MQTT5_PROPERTY_TYPE_SERVER_KEEP_ALIVE, connack_view->server_keep_alive);
        ADD_ENCODE_STEP_OPTIONAL_CURSOR_PROPERTY(
            encoder, AWS_MQTT5_PROPERTY_TYPE_RESPONSE_INFORMATION, connack_view->response_information);
        ADD_ENCODE_STEP_OPTIONAL_CURSOR_PROPERTY(
            encoder, AWS_MQTT5_PROPERTY_TYPE_SERVER_REFERENCE, connack_view->server_reference);
        ADD_ENCODE_STEP_OPTIONAL_CURSOR_PROPERTY(
            encoder, AWS_MQTT5_PROPERTY_TYPE_AUTHENTICATION_METHOD, connack_view->authentication_method);
        ADD_ENCODE_STEP_OPTIONAL_CURSOR_PROPERTY(
            encoder, AWS_MQTT5_PROPERTY_TYPE_AUTHENTICATION_DATA, connack_view->authentication_data);

        aws_mqtt5_add_user_property_encoding_steps(
            encoder, connack_view->user_properties, connack_view->user_property_count);
    }

    return AWS_OP_SUCCESS;
}

int aws_mqtt5_encoder_begin_pingresp(struct aws_mqtt5_encoder *encoder) {
    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_GENERAL,
        "(%p) mqtt5 client encoder - setting up encode for a PINGRESP packet",
        (void *)encoder->client);

    /* A ping response is just a fixed header with a 0-valued remaining length which we encode as a 0 u8 */
    ADD_ENCODE_STEP_U8(encoder, aws_mqtt5_compute_fixed_header_byte1(AWS_MQTT5_PT_PINGRESP, 0));
    ADD_ENCODE_STEP_U8(encoder, 0);

    return AWS_OP_SUCCESS;
}
