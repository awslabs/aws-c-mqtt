#include <aws/mqtt/private/v5/mqtt5_utils.h>

#include <aws/common/byte_buf.h>
#include <aws/io/stream.h>
#include <inttypes.h>

uint8_t aws_mqtt5_compute_fixed_header_byte1(enum aws_mqtt5_packet_type packet_type, uint8_t flags) {
    return flags | ((uint8_t)packet_type << 4);
}

/* encodes a utf8-string (2 byte length + "MQTT") + the version value (5) */
static uint8_t s_connect_variable_length_header_prefix[7] = {0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x05};

struct aws_byte_cursor g_aws_mqtt5_connect_protocol_cursor = {
    .ptr = &s_connect_variable_length_header_prefix[0],
    .len = AWS_ARRAY_SIZE(s_connect_variable_length_header_prefix),
};

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
        "(%p) aws_mqtt5_negotiated_settings receive maximum from server set to %" PRIu16,
        (void *)negotiated_settings,
        negotiated_settings->receive_maximum_from_server);

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "(%p) aws_mqtt5_negotiated_settings maximum packet size set to %" PRIu32,
        (void *)negotiated_settings,
        negotiated_settings->maximum_packet_size);

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "(%p) aws_mqtt5_negotiated_settings topic alias maximum to server set to %" PRIu16,
        (void *)negotiated_settings,
        negotiated_settings->topic_alias_maximum_to_server);

    AWS_LOGF(
        level,
        AWS_LS_MQTT5_GENERAL,
        "(%p) aws_mqtt5_negotiated_settings topic alias maximum to client set to %" PRIu16,
        (void *)negotiated_settings,
        negotiated_settings->topic_alias_maximum_to_client);

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
    negotiated_settings->receive_maximum_from_server = 65535;
    /* Default is no limit (256MB is MQTT5 max packet size).
     * This value should not be sent unless set by client to something else. */
    negotiated_settings->maximum_packet_size = 268435456;
    negotiated_settings->topic_alias_maximum_to_client = 0;

    // Default for Client is QoS 1. Server default is 2.
    // This should only be changed if server returns a 0 in the CONNACK
    negotiated_settings->maximum_qos = AWS_MQTT5_QOS_AT_LEAST_ONCE;
    negotiated_settings->topic_alias_maximum_to_server = 0;

    // Default is true for following settings but can be changed by Server on CONNACK
    negotiated_settings->retain_available = true;
    negotiated_settings->wildcard_subscriptions_available = true;
    negotiated_settings->subscription_identifiers_available = true;
    negotiated_settings->shared_subscriptions_available = true;

    negotiated_settings->rejoined_session = false;

    /**
     * Apply user set properties to negotiated_settings
     * NULL pointers indicate user has not set a property and it should remain the default value.
     */

    if (packet_connect_view->session_expiry_interval_seconds != NULL) {
        negotiated_settings->session_expiry_interval = *packet_connect_view->session_expiry_interval_seconds;
    }

    if (packet_connect_view->maximum_packet_size_bytes != NULL) {
        negotiated_settings->maximum_packet_size = *packet_connect_view->maximum_packet_size_bytes;
    }

    if (packet_connect_view->topic_alias_maximum != NULL) {
        negotiated_settings->topic_alias_maximum_to_client = *packet_connect_view->topic_alias_maximum;
    }
}

void aws_mqtt5_negotiated_settings_apply_connack(
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
        negotiated_settings->receive_maximum_from_server = *connack_data->receive_maximum;
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
        /* Property not being present means the Server is set to unlimited.
         * 256MB is the MQTT max packet size */
        negotiated_settings->maximum_packet_size = 268435456;
    }

    // If a value is not sent by Server, the Client must not send any Topic Aliases to the Server.
    if (connack_data->topic_alias_maximum != NULL) {
        negotiated_settings->topic_alias_maximum_to_server = *connack_data->topic_alias_maximum;
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

    negotiated_settings->rejoined_session = connack_data->session_present;
}
