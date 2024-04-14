/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#ifndef AWS_MQTT_SHARED_CONSTANTS_H
#define AWS_MQTT_SHARED_CONSTANTS_H

#include <aws/mqtt/mqtt.h>

#include <aws/mqtt/private/request-response/subscription_manager.h>

AWS_EXTERN_C_BEGIN

AWS_MQTT_API extern const struct aws_byte_cursor *g_websocket_handshake_default_path;
AWS_MQTT_API extern const struct aws_http_header *g_websocket_handshake_default_protocol_header;

AWS_MQTT_API uint64_t aws_mqtt_hash_uint64_t(const void *item);
AWS_MQTT_API bool aws_mqtt_compare_uint64_t_eq(const void *a, const void *b);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_SHARED_CONSTANTS_H */
