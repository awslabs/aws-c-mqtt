#include <aws/mqtt/private/v5/mqtt5_utils.h>

#include <aws/common/byte_buf.h>

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
