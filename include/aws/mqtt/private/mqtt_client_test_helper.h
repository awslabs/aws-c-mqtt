#ifndef AWS_MQTT_CLIENT_TEST_HELPER_H
#define AWS_MQTT_CLIENT_TEST_HELPER_H
/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/allocator.h>
#include <aws/common/byte_buf.h>
#include <aws/common/stdint.h>
#include <aws/common/string.h>
#include <aws/mqtt/exports.h>

#ifndef AWS_UNSTABLE_TESTING_API
#    error The functions in this header file are for testing purposes only!
#endif

struct aws_mqtt_client_connection;

AWS_EXTERN_C_BEGIN

/** This is for testing applications sending MQTT payloads. Don't ever include this file outside of a unit test. */
AWS_MQTT_API
void aws_mqtt_client_get_payload_for_outstanding_publish_packet(
    struct aws_mqtt_client_connection *connection,
    uint16_t packet_id,
    struct aws_byte_cursor *result);

AWS_MQTT_API
void aws_mqtt_client_get_topic_for_outstanding_publish_packet(
    struct aws_mqtt_client_connection *connection,
    uint16_t packet_id,
    struct aws_allocator *allocator,
    struct aws_string **result);

AWS_EXTERN_C_END

#endif // AWS_C_IOT_MQTT_CLIENT_TEST_HELPER_H
