#ifndef AWS_MQTT_PRIVATE_UTILS_H
#define AWS_MQTT_PRIVATE_UTILS_H

/*
 * Copyright 2010-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

#include <aws/mqtt/private/client_channel_handler.h>
#include <aws/mqtt/private/packets.h>

bool aws_mqtt_subscription_matches_publish(
    struct aws_allocator *allocator,
    struct aws_mqtt_subscription_impl *sub,
    struct aws_mqtt_packet_publish *pub);

void aws_channel_schedule_or_run_task(
    struct aws_channel *channel,
    struct aws_task *task);

#endif /* AWS_MQTT_PRIVATE_UTILS_H */
