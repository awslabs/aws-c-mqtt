#ifndef AWS_MQTT_PRIVATE_REQUEST_RESPONSE_REQUEST_RESPONSE_H
#define AWS_MQTT_PRIVATE_REQUEST_RESPONSE_REQUEST_RESPONSE_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

/*
 * Describes a change to the state of a request operation subscription
 */
enum aws_rr_subscription_event_type {

    /*
     * A request subscription subscribe succeeded
     */
    ARRSET_REQUEST_SUBSCRIPTION_SUBSCRIBE_SUCCESS,

    /*
     * A request subscription subscribe failed
     */
    ARRSET_REQUEST_SUBSCRIPTION_SUBSCRIBE_FAILURE,

    /*
     * A previously successful request subscription has ended.
     *
     * Under normal circumstances this can happen when
     *
     * (1) failure to rejoin a session
     * (2) a successful unsubscribe when the subscription is no longer needed
     */
    ARRSET_REQUEST_SUBSCRIPTION_SUBSCRIPTION_ENDED,

    /*
     * A streaming subscription subscribe succeeded
     */
    ARRSET_STREAMING_SUBSCRIPTION_ESTABLISHED,

    /*
     * The protocol client failed to rejoin a session containing a previously-established streaming subscription
     */
    ARRSET_STREAMING_SUBSCRIPTION_LOST,

    /*
     * A streaming subscription subscribe attempt resulted in an error or reason code that the client has determined
     * will result in indefinite failures to subscribe.  In this case, we stop attempting to resubscribe.
     *
     * Situations that can lead to this:
     * (1) Permission failures
     * (2) Invalid topic filter
     */
    ARRSET_STREAMING_SUBSCRIPTION_HALTED
};

#endif /* AWS_MQTT_PRIVATE_REQUEST_RESPONSE_REQUEST_RESPONSE_H */
