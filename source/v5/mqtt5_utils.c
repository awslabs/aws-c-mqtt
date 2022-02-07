#include <aws/mqtt/private/v5/mqtt5_utils.h>

#include <aws/common/byte_buf.h>

uint8_t aws_mqtt5_compute_fixed_header_byte1(enum aws_mqtt5_packet_type packet_type, uint8_t flags) {
    return flags | ((uint8_t)packet_type << 4);
}

/* encodes a utf8-string (2 byte length + "MQTT") + the version value (5) */
static uint8_t s_connect_variable_length_header_prefix[7] = {0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x05};

struct aws_byte_cursor g_aws_mqtt5_connect_protocol_cursor = {
    .ptr = &s_connect_variable_length_header_prefix[0],
    .len = AWS_ARRAY_SIZE(s_connect_variable_length_header_prefix),
};
