/**
* Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
* SPDX-License-Identifier: Apache-2.0.
*/

#include <aws/mqtt/private/request-response/protocol_adapter.h>
#include <aws/mqtt/private/request-response/request_response_client.h>
#include <aws/mqtt/private/request-response/subscription_manager.h>
#include <aws/mqtt/request-response/request_response_client.h>

#include <aws/testing/aws_test_harness.h>

struct aws_mqtt_protocol_adapter_pinned_mock {
    struct aws_allocator *allocator;
    struct aws_mqtt_protocol_adapter base;

    struct aws_event_loop *loop;
    void *test_context;
    bool is_connected;
};

static void s_aws_mqtt_protocol_adapter_pinned_mock_destroy(void *impl) {
    struct aws_mqtt_protocol_adapter_pinned_mock *adapter = impl;

    aws_mem_release(adapter->allocator, adapter);
}

static int s_aws_mqtt_protocol_adapter_pinned_mock_subscribe(void *impl, struct aws_protocol_adapter_subscribe_options *options) {
    (void)options;

    struct aws_mqtt_protocol_adapter_pinned_mock *adapter = impl;

    return AWS_OP_SUCCESS;
}

static int s_aws_mqtt_protocol_adapter_pinned_mock_unsubscribe(void *impl, struct aws_protocol_adapter_unsubscribe_options *options) {
    (void)options;

    struct aws_mqtt_protocol_adapter_pinned_mock *adapter = impl;

    return AWS_OP_SUCCESS;
}

static int s_aws_mqtt_protocol_adapter_pinned_mock_publish(void *impl, struct aws_protocol_adapter_publish_options *options) {
    (void)options;

    struct aws_mqtt_protocol_adapter_pinned_mock *adapter = impl;

    return AWS_OP_SUCCESS;
}

static bool s_aws_mqtt_protocol_adapter_pinned_mock_is_connected(void *impl) {
    struct aws_mqtt_protocol_adapter_pinned_mock *adapter = impl;

    return adapter->is_connected;
}


static struct aws_mqtt_protocol_adapter_vtable s_default_protocol_adapter_pinned_mock_vtable = {
    .aws_mqtt_protocol_adapter_destroy_fn = s_aws_mqtt_protocol_adapter_pinned_mock_destroy,
    .aws_mqtt_protocol_adapter_subscribe_fn = s_aws_mqtt_protocol_adapter_pinned_mock_subscribe,
    .aws_mqtt_protocol_adapter_unsubscribe_fn = s_aws_mqtt_protocol_adapter_pinned_mock_unsubscribe,
    .aws_mqtt_protocol_adapter_publish_fn = s_aws_mqtt_protocol_adapter_pinned_mock_publish,
    .aws_mqtt_protocol_adapter_is_connected_fn = s_aws_mqtt_protocol_adapter_pinned_mock_is_connected,
};

static struct aws_mqtt_protocol_adapter_pinned_mock *s_aws_mqtt_protocol_adapter_new_pinned_mock(
    struct aws_allocator *allocator,
    const struct aws_mqtt_protocol_adapter_vtable *vtable,
    struct aws_event_loop *loop,
    void *test_context) {
    struct aws_mqtt_protocol_adapter_pinned_mock *adapter =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_protocol_adapter_pinned_mock));

    adapter->allocator = allocator;
    adapter->test_context = test_context;
    adapter->loop = loop;
    adapter->base.impl = adapter;
    if (vtable != NULL) {
        adapter->base.vtable = vtable;
    } else {
        adapter->base.vtable = &s_default_protocol_adapter_pinned_mock_vtable;
    }

    return adapter;
}

struct aws_rr_client_test_fixture {
    struct aws_allocator *allocator;

    struct aws_event_loop_group *elg;
    struct aws_event_loop *loop;

    struct aws_request_response_client *client;

    struct aws_mqtt_protocol_adapter_pinned_mock *mock_adapter;

    void *test_context;
};