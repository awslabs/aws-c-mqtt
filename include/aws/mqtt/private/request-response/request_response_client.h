#ifndef AWS_MQTT_PRIVATE_REQUEST_RESPONSE_REQUEST_RESPONSE_CLIENT_H
#define AWS_MQTT_PRIVATE_REQUEST_RESPONSE_REQUEST_RESPONSE_CLIENT_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

struct aws_mqtt_protocol_adapter;
struct aws_mqtt_protocol_adapter_options;
struct aws_mqtt_request_response_client;
struct aws_mqtt_request_response_client_options;

struct aws_protocol_adapter_factory_options {
    struct aws_event_loop *loop;
    void *creation_context;
    struct aws_mqtt_protocol_adapter * (*mqtt_protocol_adaptor_factory_fn)(struct aws_mqtt_request_response_client *, struct aws_mqtt_protocol_adapter_options *, void *);
};

AWS_EXTERN_C_BEGIN

struct aws_mqtt_request_response_client *aws_mqtt_request_response_client_new_from_adaptor_factory(struct aws_allocator *allocator, const struct aws_protocol_adapter_factory_options *factory_options, const struct aws_mqtt_request_response_client_options *client_options);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_PRIVATE_REQUEST_RESPONSE_REQUEST_RESPONSE_CLIENT_H */
