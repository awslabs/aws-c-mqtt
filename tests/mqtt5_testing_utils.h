#ifndef MQTT_MQTT5_TESTING_UTILS_H
#define MQTT_MQTT5_TESTING_UTILS_H
/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

#include <aws/mqtt/v5/mqtt5_types.h>

struct aws_mqtt5_encoder;
struct aws_mqtt5_decoder_function_table;
struct aws_mqtt5_encoder_function_table;

AWS_EXTERN_C_BEGIN

AWS_MQTT_API int aws_mqtt5_test_verify_user_properties_raw(
    size_t property_count,
    const struct aws_mqtt5_user_property *properties,
    size_t expected_count,
    const struct aws_mqtt5_user_property *expected_properties);

AWS_MQTT_API void aws_mqtt5_encode_init_testing_function_table(struct aws_mqtt5_encoder_function_table *function_table);

AWS_MQTT_API void aws_mqtt5_decode_init_testing_function_table(struct aws_mqtt5_decoder_function_table *function_table);

AWS_EXTERN_C_END

#endif /* MQTT_MQTT5_TESTING_UTILS_H */
