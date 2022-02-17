#ifndef MQTT_MQTT5_TESTING_UTILS_H
#define MQTT_MQTT5_TESTING_UTILS_H
/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

#include <aws/common/array_list.h>
#include <aws/io/channel.h>
#include <aws/mqtt/private/v5/mqtt5_client_impl.h>
#include <aws/mqtt/private/v5/mqtt5_decoder.h>
#include <aws/mqtt/private/v5/mqtt5_encoder.h>
#include <aws/mqtt/v5/mqtt5_types.h>

struct aws_mqtt5_mock_server_packet_record {
    struct aws_allocator *storage_allocator;

    uint64_t timestamp;

    void *packet_storage;
    enum aws_mqtt5_packet_type packet_type;
};

struct aws_mqtt5_mock_server_vtable {};

struct aws_mqtt5_client_mqtt5_mock_test_fixture_options {
    struct aws_allocator *allocator;
    const struct aws_mqtt5_client_options *client_options;
    const struct aws_mqtt5_mock_server_vtable *server_table;
};

struct aws_mqtt5_client_mock_test_fixture {
    struct aws_allocator *allocator;

    struct aws_mqtt5_client_vtable client_vtable;
    struct aws_mqtt5_client *mocked_client;

    struct aws_channel *unified_channel;
    struct aws_channel_handler server_handler;
    struct aws_channel_slot *server_slot;

    struct aws_mqtt5_encoder_function_table encoding_table;
    struct aws_mqtt5_encoder server_encoder;

    struct aws_mqtt5_decoder_function_table decoding_table;
    struct aws_mqtt5_decoder server_decoder;

    struct aws_array_list server_received_packets;
};

AWS_EXTERN_C_BEGIN

AWS_MQTT_API int aws_mqtt5_test_verify_user_properties_raw(
    size_t property_count,
    const struct aws_mqtt5_user_property *properties,
    size_t expected_count,
    const struct aws_mqtt5_user_property *expected_properties);

AWS_MQTT_API void aws_mqtt5_encode_init_testing_function_table(struct aws_mqtt5_encoder_function_table *function_table);

AWS_MQTT_API void aws_mqtt5_decode_init_testing_function_table(struct aws_mqtt5_decoder_function_table *function_table);

AWS_MQTT_API int aws_mqtt5_client_mock_test_fixture_init(
    struct aws_mqtt5_client_mock_test_fixture *test_fixture,
    struct aws_mqtt5_client_mqtt5_mock_test_fixture_options *options);

AWS_MQTT_API void aws_mqtt5_client_mock_test_fixture_clean_up(struct aws_mqtt5_client_mock_test_fixture *test_fixture);

AWS_EXTERN_C_END

#endif /* MQTT_MQTT5_TESTING_UTILS_H */
