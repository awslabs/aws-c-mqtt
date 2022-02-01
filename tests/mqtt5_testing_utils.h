#ifndef MQTT_MQTT5_TESTING_UTILS_H
#define MQTT_MQTT5_TESTING_UTILS_H
/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

#include <aws/mqtt/v5/mqtt5_types.h>

struct aws_mqtt5_encoder;

AWS_EXTERN_C_BEGIN

AWS_MQTT_API int aws_mqtt5_test_verify_user_properties_raw(
    size_t property_count,
    const struct aws_mqtt5_user_property *properties,
    size_t expected_count,
    const struct aws_mqtt5_user_property *expected_properties);

/* Testing-only encode implementations */

AWS_MQTT_API int aws_mqtt5_encoder_begin_connack(
    struct aws_mqtt5_encoder *encoder,
    struct aws_mqtt5_packet_connack_view *connack_view);

AWS_MQTT_API int aws_mqtt5_encoder_begin_pingresp(struct aws_mqtt5_encoder *encoder);

AWS_EXTERN_C_END

#endif /* MQTT_MQTT5_TESTING_UTILS_H */
