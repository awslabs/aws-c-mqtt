#ifndef AWS_MQTT_PRIVATE_REQUEST_RESPONSE_REQUEST_RESPONSE_H
#define AWS_MQTT_PRIVATE_REQUEST_RESPONSE_REQUEST_RESPONSE_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

/*
 * Describes a change to the state of a request-response client subscription
 */
enum aws_rr_subscription_event_type {

    /*
     * A subscribe succeeded
     */
    ARRSET_SUBSCRIPTION_SUBSCRIBE_SUCCESS,

    /*
     * A subscribe failed
     */
    ARRSET_SUBSCRIPTION_SUBSCRIBE_FAILURE,

    /*
     * A previously successful subscription has ended (generally due to a failure to resume a session)
     */
    ARRSET_SUBSCRIPTION_ENDED
};

#endif /* AWS_MQTT_PRIVATE_REQUEST_RESPONSE_REQUEST_RESPONSE_H */

