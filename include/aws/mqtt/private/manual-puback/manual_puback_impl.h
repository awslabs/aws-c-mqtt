#ifndef AWS_MQTT_MANUAL_PUBACK_IMPL_H
#define AWS_MQTT_MANUAL_PUBACK_IMPL_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/**
 * This is used to track which PUBLISH packets a user has taken manual PUBACK control from.
 */
struct aws_mqtt5_manual_puback_entry {
    struct aws_allocator *allocator;

    /* control id for internal tracking */
    uint64_t puback_control_id;
    /* packet_id of controlled publish */
    uint16_t packet_id;
};

/**
 * After a PUBLISH packet has been provided to the user, this check will determine if the PUBACK has had its PUBACK
 * control taken. If the PUBACK control has been taken, the user is responsible for sending a PUBACK for the PUBLISH
 * packet.
 *
 * @param client mqtt5 client that received the PUBLISH packet to take manual PUBACK control from.
 * @param publish_view the view of the PUBLISH packet that's PUBACK is being handled.
 */
void aws_mqtt5_handle_puback(struct aws_mqtt5_client *client, const struct aws_mqtt5_packet_publish_view *publish_view);

/**
 * Clean up function for the aws_mqtt5_manual_puback_entry used by hash sets.
 */
void aws_mqtt5_manual_puback_entry_destroy(void *value);

#endif /* AWS_MQTT_MANUAL_PUBACK_IMPL_H */
