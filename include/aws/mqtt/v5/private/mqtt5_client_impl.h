#ifndef AWS_MQTT_MQTT5_CLIENT_IMPL_H
#define AWS_MQTT_MQTT5_CLIENT_IMPL_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

#include <aws/io/socket.h>
#include <aws/mqtt/v5/mqtt5_types.h>

struct aws_client_bootstrap;

struct aws_mqtt5_client_config {
    struct aws_allocator *allocator;

    struct aws_byte_buf host_name;
    uint16_t port;
    struct aws_client_bootstrap *bootstrap;
    struct aws_socket_options socket_options;
};

struct aws_mqtt5_client {
    struct aws_mqtt5_client_config *config;
};

#endif /* AWS_MQTT_MQTT5_CLIENT_IMPL_H */
