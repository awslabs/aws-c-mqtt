#include <aws/mqtt/private/v5/mqtt5_utils.h>

#include <aws/common/byte_buf.h>
#include <aws/io/stream.h>
#include <inttypes.h>

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

int aws_mqtt5_get_variable_length_encode_size(size_t value, size_t *encode_size) {
    if (value > AWS_MQTT5_MAXIMUM_VARIABLE_LENGTH_INTEGER) {
        return AWS_OP_ERR;
    }

    if (value < 128) {
        *encode_size = 1;
    } else if (value < 16384) {
        *encode_size = 2;
    } else if (value < 2097152) {
        *encode_size = 3;
    } else {
        *encode_size = 4;
    }

    return AWS_OP_SUCCESS;
}

void aws_mqtt5_negotiated_settings_log(
    struct aws_mqtt5_negotiated_settings *negotiated_settings,
    enum aws_log_level level) {

    struct aws_logger *logger = aws_logger_get();
    if (logger == NULL || logger->vtable->get_log_level(logger, AWS_LS_MQTT5_GENERAL) < level) {
        return;
    }

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "(%p) aws_mqtt5_negotiated_settings maxiumum qos set to %d",
        (void *)negotiated_settings,
        negotiated_settings->maximum_qos);

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "(%p) aws_mqtt5_negotiated_settings session expiry interval set to %" PRIu32,
        (void *)negotiated_settings,
        negotiated_settings->session_expiry_interval);

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "(%p) aws_mqtt5_negotiated_settings receive maximum set to %" PRIu16,
        (void *)negotiated_settings,
        negotiated_settings->receive_maximum);

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "(%p) aws_mqtt5_negotiated_settings maximum packet size set to %" PRIu32,
        (void *)negotiated_settings,
        negotiated_settings->maximum_packet_size);

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "(%p) aws_mqtt5_negotiated_settings to server topic alias maximum set to %" PRIu16,
        (void *)negotiated_settings,
        negotiated_settings->to_server_topic_alias_maximum);

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "(%p) aws_mqtt5_negotiated_settings to client topic alias maximum set to %" PRIu16,
        (void *)negotiated_settings,
        negotiated_settings->to_client_topic_alias_maximum);

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "(%p) aws_mqtt5_negotiated_settings server keep alive set to %" PRIu16,
        (void *)negotiated_settings,
        negotiated_settings->server_keep_alive);

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "(%p) aws_mqtt5_negotiated_settings retain available set to %s",
        (void *)negotiated_settings,
        negotiated_settings->retain_available ? "true" : "false");

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "(%p) aws_mqtt5_negotiated_settings wildcard subscriptions available set to %s",
        (void *)negotiated_settings,
        negotiated_settings->wildcard_subscriptions_available ? "true" : "false");

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "(%p) aws_mqtt5_negotiated_settings subscription identifiers available set to %s",
        (void *)negotiated_settings,
        negotiated_settings->subscription_identifiers_available ? "true" : "false");

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "(%p) aws_mqtt5_negotiated_settings shared subscriptions available set to %s",
        (void *)negotiated_settings,
        negotiated_settings->shared_subscriptions_available ? "true" : "false");
}

/**
 * Resets negotiated_settings to defaults reconciled with client set properties.
 * Called on init of mqtt5 Client and just prior to a CONNECT.
 *
 * @param negotiated_settings struct containing settings to be set
 * @param packet_connect_view
 * @return int
 */
void aws_mqtt5_negotiated_settings_reset(
    struct aws_mqtt5_negotiated_settings *negotiated_settings,
    struct aws_mqtt5_packet_connect_view *packet_connect_view) {
    AWS_PRECONDITION(negotiated_settings != NULL);
    AWS_PRECONDITION(packet_connect_view != NULL);

    /** Assign defaults values to negotiated_settings */

    /* Properties that may be sent in CONNECT to Server. These should only be sent if Client
       changes them from their default values.
    */
    negotiated_settings->server_keep_alive = packet_connect_view->keep_alive_interval_seconds;
    negotiated_settings->session_expiry_interval = 0;
    negotiated_settings->receive_maximum = 65535;
    negotiated_settings->maximum_packet_size = 0; // 0 means no limit. 0 should not be sent to server.
    negotiated_settings->to_client_topic_alias_maximum = 0;

    // Default for Client is QoS 1. Server default is 2.
    // This should only be changed if server returns a 0 in the CONNACK
    negotiated_settings->maximum_qos = AWS_MQTT5_QOS_AT_LEAST_ONCE;
    negotiated_settings->to_server_topic_alias_maximum = 0;

    // Default is true for following settings but can be changed by Server on CONNACK
    negotiated_settings->retain_available = true;
    negotiated_settings->wildcard_subscriptions_available = true;
    negotiated_settings->subscription_identifiers_available = true;
    negotiated_settings->shared_subscriptions_available = true;

    /**
     * Apply user set properties to negotiated_settings
     * NULL pointers indicate user has not set a property and it should remain the default value.
     */

    if (packet_connect_view->session_expiry_interval_seconds != NULL) {
        negotiated_settings->session_expiry_interval = *packet_connect_view->session_expiry_interval_seconds;
    }

    if (packet_connect_view->receive_maximum != NULL) {
        negotiated_settings->receive_maximum = *packet_connect_view->receive_maximum;
    }

    if (packet_connect_view->maximum_packet_size_bytes != NULL) {
        negotiated_settings->maximum_packet_size = *packet_connect_view->maximum_packet_size_bytes;
    }

    if (packet_connect_view->topic_alias_maximum != NULL) {
        negotiated_settings->to_client_topic_alias_maximum = *packet_connect_view->topic_alias_maximum;
    }
}

/**
 * Checks properties received from Server CONNACK and reconcile with negotiated_settings
 *
 * @param negotiated_settings struct containing settings to be set
 * @param connack_data
 * @return int
 */
int aws_mqtt5_negotiated_settings_apply_connack(
    struct aws_mqtt5_negotiated_settings *negotiated_settings,
    struct aws_mqtt5_packet_connack_view *connack_data) {
    AWS_PRECONDITION(negotiated_settings != NULL);
    AWS_PRECONDITION(connack_data != NULL);

    /**
     * Reconcile CONNACK set properties with current negotiated_settings values
     * NULL pointers indicate Server has not set a property
     */

    if (connack_data->session_expiry_interval != NULL) {
        negotiated_settings->session_expiry_interval = *connack_data->session_expiry_interval;
    }

    if (connack_data->receive_maximum != NULL) {
        negotiated_settings->receive_maximum = *connack_data->receive_maximum;
    } else {
        negotiated_settings->receive_maximum = 65535;
    }

    // NULL = Maximum QoS of 2.
    if (connack_data->maximum_qos != NULL) {
        if (*connack_data->maximum_qos < negotiated_settings->maximum_qos) {
            negotiated_settings->maximum_qos = *connack_data->maximum_qos;
        }
    }

    if (connack_data->retain_available != NULL) {
        negotiated_settings->retain_available = *connack_data->retain_available;
    }

    if (connack_data->maximum_packet_size != NULL) {
        negotiated_settings->maximum_packet_size = *connack_data->maximum_packet_size;
    } else {
        // Property not being present means the Server is set to unlimited.
        negotiated_settings->maximum_packet_size = 0;
    }

    // If a value is not sent by Server, the Client must not send any Topic Aliases to the Server.
    if (connack_data->topic_alias_maximum != NULL) {
        negotiated_settings->to_server_topic_alias_maximum = *connack_data->topic_alias_maximum;
    }

    if (connack_data->wildcard_subscriptions_available != NULL) {
        negotiated_settings->wildcard_subscriptions_available = *connack_data->wildcard_subscriptions_available;
    }

    if (connack_data->subscription_identifiers_available != NULL) {
        negotiated_settings->subscription_identifiers_available = *connack_data->subscription_identifiers_available;
    }

    if (connack_data->shared_subscriptions_available != NULL) {
        negotiated_settings->shared_subscriptions_available = *connack_data->shared_subscriptions_available;
    }

    if (connack_data->server_keep_alive != NULL) {
        negotiated_settings->server_keep_alive = *connack_data->server_keep_alive;
    }

    return AWS_OP_SUCCESS;
}
