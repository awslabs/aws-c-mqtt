/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/testing/aws_test_harness.h>

#include <aws/mqtt/aws_iot_service_client.h>
#include <aws/mqtt/v5/mqtt5_client.h>

static int s_aws_iot_service_client_new_destroy_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    aws_thread_join_all_managed();

    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(aws_iot_service_client_new_destroy, s_aws_iot_service_client_new_destroy_fn)