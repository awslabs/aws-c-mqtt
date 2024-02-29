/**
* Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
* SPDX-License-Identifier: Apache-2.0.
*/

#include <aws/mqtt/request-response/request_response_client.h>

#include <aws/common/ref_count.h>
#include <aws/common/task_scheduler.h>
#include <aws/io/event_loop.h>
#include <aws/mqtt/private/client_impl_shared.h>
#include <aws/mqtt/private/request-response/protocol_adapter.h>
#include <aws/mqtt/private/request-response/request_response_client.h>
#include <aws/mqtt/private/request-response/subscription_manager.h>
#include <aws/mqtt/private/v5/mqtt5_client_impl.h>

enum aws_request_response_client_state {
    AWS_RRCS_ACTIVE,

    // asynchronously shutting down, no more servicing will be done and all protocol adapter callbacks are ignored
    AWS_RRCS_SHUTTING_DOWN,
};

struct aws_mqtt_request_response_client {
    struct aws_allocator *allocator;

    struct aws_ref_count external_ref_count;
    struct aws_ref_count internal_ref_count;

    struct aws_mqtt_request_response_client_options config;

    struct aws_mqtt_protocol_adapter *client_adapter;

    struct aws_rr_subscription_manager subscription_manager;

    struct aws_event_loop *loop;

    struct aws_task external_shutdown_task;
    struct aws_task internal_shutdown_task;

    enum aws_request_response_client_state state;
};

static void s_aws_rr_client_on_zero_internal_ref_count(void *context) {
    struct aws_mqtt_request_response_client *client = context;

    aws_event_loop_schedule_task_now(client->loop, &client->internal_shutdown_task);
}

static void s_aws_rr_client_on_zero_external_ref_count(void *context) {
    struct aws_mqtt_request_response_client *client = context;

    aws_event_loop_schedule_task_now(client->loop, &client->external_shutdown_task);
}

static void s_mqtt_request_response_client_final_destroy(struct aws_mqtt_request_response_client *client) {
    aws_mem_release(client->allocator, client);
}

static void s_mqtt_request_response_client_internal_shutdown_task_fn(struct aws_task *task, void *arg, enum aws_task_status task_status) {
    (void)task;
    (void)task_status;

    struct aws_mqtt_request_response_client *client = arg;

    /*
     * All internal and external refs are gone; it is safe to clean up synchronously.
     *
     * The subscription manager is cleaned up and the protocol adapter has been shut down.  All that's left is to
     * free memory.
     */
    s_mqtt_request_response_client_final_destroy(client);
}

static void s_mqtt_request_response_client_external_shutdown_task_fn(struct aws_task *task, void *arg, enum aws_task_status task_status) {
    (void)task;

    AWS_FATAL_ASSERT(task_status != AWS_TASK_STATUS_CANCELED);

    struct aws_mqtt_request_response_client *client = arg;

    /* stop handling adapter event callbacks */
    client->state = AWS_RRCS_SHUTTING_DOWN;

    aws_rr_subscription_manager_clean_up(&client->subscription_manager);

    if (client->client_adapter != NULL) {
        aws_mqtt_protocol_adapter_destroy(client->client_adapter);
    }

    aws_ref_count_release(&client->internal_ref_count);
}

static void s_aws_rr_client_subscription_status_event_callback(const struct aws_rr_subscription_status_event *event, void *userdata) {
    (void)event;
    (void)userdata;

    /*
     * We must be on the event loop, but it's safer overall to process this event as a top-level event loop task.  The subscription
     * manager assumes that we won't call APIs on it while iterating subscription records and listeners.
     *
     * These tasks hold an internal reference while they exist.
     */

    // NYI
}

static void s_aws_rr_client_protocol_adapter_subscription_event_callback(const struct aws_protocol_adapter_subscription_event *event, void *user_data) {
    struct aws_mqtt_request_response_client *rr_client = user_data;

    AWS_FATAL_ASSERT(aws_event_loop_thread_is_callers_thread(rr_client->loop));

    if (rr_client->state != AWS_RRCS_ACTIVE) {
        return;
    }

    aws_rr_subscription_manager_on_protocol_adapter_subscription_event(&rr_client->subscription_manager, event);
}

static void s_aws_rr_client_protocol_adapter_incoming_publish_callback(
    const struct aws_protocol_adapter_incoming_publish_event *publish,
    void *user_data) {
    (void)publish;

    struct aws_mqtt_request_response_client *rr_client = user_data;

    AWS_FATAL_ASSERT(aws_event_loop_thread_is_callers_thread(rr_client->loop));

    if (rr_client->state != AWS_RRCS_ACTIVE) {
        return;
    }

    // NYI
}

static void s_aws_rr_client_protocol_adapter_terminate_callback(void *user_data) {
    struct aws_mqtt_request_response_client *rr_client = user_data;

    // release the internal ref count "held" by the protocol adapter's existence
    aws_ref_count_release(&rr_client->internal_ref_count);
}

static void s_aws_rr_client_protocol_adapter_connection_event_callback(const struct aws_protocol_adapter_connection_event *event, void *user_data) {
    struct aws_mqtt_request_response_client *rr_client = user_data;

    AWS_FATAL_ASSERT(aws_event_loop_thread_is_callers_thread(rr_client->loop));

    if (rr_client->state != AWS_RRCS_ACTIVE) {
        return;
    }

    aws_rr_subscription_manager_on_protocol_adapter_connection_event(&rr_client->subscription_manager, event);
}

static struct aws_mqtt_request_response_client *s_aws_mqtt_request_response_client_new(struct aws_allocator *allocator, const struct aws_mqtt_request_response_client_options *options, struct aws_event_loop *loop) {
    struct aws_mqtt_request_response_client *rr_client = aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_request_response_client));

    rr_client->allocator = allocator;
    rr_client->config = *options;
    rr_client->loop = loop;
    rr_client->state = AWS_RRCS_ACTIVE;

    aws_task_init(&rr_client->external_shutdown_task, s_mqtt_request_response_client_external_shutdown_task_fn, rr_client, "mqtt_rr_client_external_shutdown");
    aws_task_init(&rr_client->internal_shutdown_task, s_mqtt_request_response_client_internal_shutdown_task_fn, rr_client, "mqtt_rr_client_internal_shutdown");

    // 1 external ref to the caller
    aws_ref_count_init(&rr_client->external_ref_count, rr_client, s_aws_rr_client_on_zero_external_ref_count);

    // 1 internal ref count belongs to ourselves (the external ref count shutdown task)
    aws_ref_count_init(&rr_client->internal_ref_count, rr_client, s_aws_rr_client_on_zero_internal_ref_count);

    return rr_client;
}

static int s_aws_rr_client_init_subscription_manager(struct aws_mqtt_request_response_client *rr_client, struct aws_allocator *allocator) {
    struct aws_rr_subscription_manager_options subscription_manager_options = {
        .operation_timeout_seconds = rr_client->config.operation_timeout_seconds,
        .max_subscriptions = rr_client->config.max_subscriptions,
        .subscription_status_callback = s_aws_rr_client_subscription_status_event_callback,
        .userdata = rr_client,
    };

    return aws_rr_subscription_manager_init(&rr_client->subscription_manager, allocator, rr_client->client_adapter, &subscription_manager_options);
}

static struct aws_mqtt_protocol_adapter *s_mqtt_protocol_adaptor_factory_from_mqtt311_client(struct aws_mqtt_request_response_client *rr_client, struct aws_mqtt_protocol_adapter_options *adapter_options, void *context) {

    struct aws_mqtt_client_connection *client = context;

    return aws_mqtt_protocol_adapter_new_from_311(rr_client->allocator, adapter_options, client);
}

struct aws_mqtt_request_response_client *aws_mqtt_request_response_client_new_from_mqtt311_client(struct aws_allocator *allocator, struct aws_mqtt_client_connection *client, const struct aws_mqtt_request_response_client_options *options) {

    struct aws_protocol_adapter_factory_options mqtt311_factory_options = {
        .loop = aws_mqtt_client_connection_get_event_loop(client),
        .creation_context = client,
        .mqtt_protocol_adaptor_factory_fn = s_mqtt_protocol_adaptor_factory_from_mqtt311_client,
    };

    return aws_mqtt_request_response_client_new_from_adaptor_factory(allocator, &mqtt311_factory_options, options);
}

static struct aws_mqtt_protocol_adapter *s_mqtt_protocol_adaptor_factory_from_mqtt5_client(struct aws_mqtt_request_response_client *rr_client, struct aws_mqtt_protocol_adapter_options *adapter_options, void *context) {

    struct aws_mqtt5_client *client = context;

    return aws_mqtt_protocol_adapter_new_from_5(rr_client->allocator, adapter_options, client);
}

struct aws_mqtt_request_response_client *aws_mqtt_request_response_client_new_from_mqtt5_client(struct aws_allocator *allocator, struct aws_mqtt5_client *client, const struct aws_mqtt_request_response_client_options *options) {

    struct aws_protocol_adapter_factory_options mqtt5_factory_options = {
        .loop = client->loop,
        .creation_context = client,
        .mqtt_protocol_adaptor_factory_fn = s_mqtt_protocol_adaptor_factory_from_mqtt5_client,
    };

    return aws_mqtt_request_response_client_new_from_adaptor_factory(allocator, &mqtt5_factory_options, options);
}

struct aws_mqtt_request_response_client *aws_mqtt_request_response_client_new_from_adaptor_factory(struct aws_allocator *allocator, const struct aws_protocol_adapter_factory_options *factory_options, const struct aws_mqtt_request_response_client_options *client_options) {
    struct aws_mqtt_request_response_client *rr_client = s_aws_mqtt_request_response_client_new(allocator, client_options, factory_options->loop);

    struct aws_mqtt_protocol_adapter_options adapter_options = {
        .subscription_event_callback = s_aws_rr_client_protocol_adapter_subscription_event_callback,
        .incoming_publish_callback = s_aws_rr_client_protocol_adapter_incoming_publish_callback,
        .terminate_callback = s_aws_rr_client_protocol_adapter_terminate_callback,
        .connection_event_callback = s_aws_rr_client_protocol_adapter_connection_event_callback,
        .user_data = rr_client,
    };

    rr_client->client_adapter = (*factory_options->mqtt_protocol_adaptor_factory_fn)(rr_client, &adapter_options, factory_options->creation_context);
    if (rr_client->client_adapter == NULL) {
        goto error;
    }

    // now that it exists, 1 internal ref belongs to protocol adapter termination
    aws_ref_count_acquire(&rr_client->internal_ref_count);

    if (s_aws_rr_client_init_subscription_manager(rr_client, allocator)) {
        goto error;
    }

    return rr_client;

error:

    // even on construction failures we still need to walk through the async shutdown process
    aws_mqtt_request_response_client_release(rr_client);

    return NULL;
}


struct aws_mqtt_request_response_client *aws_mqtt_request_response_client_acquire(struct aws_mqtt_request_response_client *client) {
    if (client != NULL) {
        aws_ref_count_acquire(&client->external_ref_count);
    }

    return client;
}

struct aws_mqtt_request_response_client *aws_mqtt_request_response_client_release(struct aws_mqtt_request_response_client *client) {
    if (client != NULL) {
        aws_ref_count_release(&client->external_ref_count);
    }

    return NULL;
}
