#ifndef AWS_MQTT_MQTT5_UTILS_H
#define AWS_MQTT_MQTT5_UTILS_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

struct aws_byte_buf;

#define AWS_MQTT5_MAXIMUM_VARIABLE_LENGTH_INTEGER ((uint32_t)268435455)

AWS_EXTERN_C_BEGIN

int aws_mqtt5_encode_variable_length_integer(struct aws_byte_buf *buf, uint32_t value);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_MQTT5_UTILS_H */
