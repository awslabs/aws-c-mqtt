#ifndef AWS_MQTT_MANUAL_PUBACK_H
#define AWS_MQTT_MANUAL_PUBACK_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/v5/mqtt5_client.h>

/**
 * Result type for manual PUBACK operations.  Lets the caller signal if the manual ack
 * operation succeeded or if it failed (and why).
 *
 */
enum aws_mqtt5_manual_puback_result {
    AWS_MQTT5_MPR_SUCCESS = 0,
    AWS_MQTT5_MPR_PUBACK_CANCELLED = 1,
    AWS_MQTT5_MPR_PUBACK_INVALID = 2,
    AWS_MQTT5_MPR_CRT_FAILURE = 3,
};

/**
 * Signature of callback invoked when a manual PUBACK operation completes.
 *
 * @param puback_result result of the PUBACK operation
 * @param complete_ctx user data passed in with the completion options
 */
typedef void(
    aws_mqtt5_manual_puback_completion_fn)(enum aws_mqtt5_manual_puback_result puback_result, void *complete_ctx);

/**
 * Completion options for the manual puback operation
 */
struct aws_mqtt5_manual_puback_completion_options {
    aws_mqtt5_manual_puback_completion_fn *completion_callback;
    void *completion_user_data;
};

AWS_EXTERN_C_BEGIN

/**
 * Takes manual control of PUBACK for the given PUBLISH packet.
 *
 * @param client mqtt5 client that received the PUBLISH packet to take manual PUBACK control from.
 * @param publish_view the view of the PUBLISH packet that PUBACK control is taken from.
 * @return puback_control_id of the PUBLISH packet. This can be used to schedule a PUBACK for the PUBLISH packet
 * when used with the aws_mqtt5_manual_puback function call.
 */
AWS_MQTT_API uint64_t aws_mqtt5_take_manual_puback_control(
    struct aws_mqtt5_client *client,
    const struct aws_mqtt5_packet_publish_view *publish_view);

/**
 * Send PUBACK for provided control id. Callback in completion_options will be invoked with an
 * aws_mqtt5_manual_puback_result once a PUBACK operation has been completed.
 *
 * @param client mqtt5 client to queue a puback for
 * @param puback_control_id Control ID of aws_mqtt5_manual_puback_entry to send to broker/server
 * @return success/failure of the manual PUBACK operation.
 */
AWS_MQTT_API
int aws_mqtt5_manual_puback(
    struct aws_mqtt5_client *client,
    uint64_t puback_control_id,
    const struct aws_mqtt5_manual_puback_completion_options *completion_options);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_MANUAL_PUBACK_H */
