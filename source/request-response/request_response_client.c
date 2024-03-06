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
#include <aws/mqtt/private/request-response/subscription_manager.h>
#include <aws/mqtt/private/v5/mqtt5_client_impl.h>

enum aws_mqtt_request_response_operation_type {
    AWS_MRROT_REQUEST,
    AWS_MRROT_STREAMING,
};

enum aws_mqtt_request_response_operation_state {
    AWS_MRROS_NONE, // creation -> in event loop enqueue
    AWS_MRROS_QUEUED, // in event loop queue -> non blocked response from subscription manager
    AWS_MRROS_PENDING_SUBSCRIPTION, // subscribing response from sub manager -> subscription success/failure event
    AWS_MRROS_AWAITING_RESPONSE, // (request only) subscription success -> (publish failure OR correlated response received)
    AWS_MRROS_SUBSCRIBED, // (streaming only) subscription success -> (operation finished OR subscription ended event)
    AWS_MRROS_TERMINAL, // (streaming only) (subscription failure OR subscription ended) -> operation close/terminate
};

/*

Tables/Lookups

    (Authoritative operation container)
    1. &operation.id -> &operation // added on in-thread enqueue, removed on operation completion/destruction

    (Response topic -> Correlation token extraction info)
    2. &topic -> &{topic, topic_buffer, correlation token json path buffer} // per-message-path add/replace on in-thread enqueue, removed on client destruction

    (Correlation token -> request operation)
    3. &operation.correlation token -> (request) &operation // added on in-thread request op move to awaiting response state, removed on operation completion/destruction

    (Subscription filter -> all operations using that filter)
    4. &topic_filter -> &{topic_filter, linked_list} // added on in-thread pop from queue, removed from list on operation completion/destruction also checked for empty and removed from table

*/

/* All operations have an internal ref to the client they are a part of */

/*
 On submit request operation API [Anywhere]:

 Allocate id
 Create operation
 Submit cross-thread task

 */

/*
 On submit streaming operation API [Anywhere]:

 Allocate id
 Create operation
 Submit cross-thread task
 Return (ref-counted) operation

 */

/*
 On receive operation [Event Loop, top-level task]:

 Add to operations table
 (Request) Add message paths to path table if no exist or different value
 Add to timeout priority queue
 Add operation to end of queue list
 state <- QUEUED
 Wake service task

 */

/*
 Complete (request) operation [Event Loop]:

 Completion Callback (Success/Failure)
 State <- TERMINAL
 Decref operation
 */

/*
 On operation ref to zero [Anywhere]:

 Submit cross-thread task to destroy operation (operation terminate callback chains directly to the binding)
 */

/*
 On operation destroy [Event Loop, top-level task]:

 Remove from operations table
 Remove from intrusive list
 (Request only) Remove from correlation token table
 (Streaming only) Check streaming topic table for empty list, remove entry if so
 Remove from timeout priority queue
 Remove from subscription manager
 Wake service task // What if this is the last internal ref?  Should service task have an internal reference while scheduled?
 (Streaming) Invoke termination callback
 Release client internal ref

 */

/*
 On incoming publish [Event Loop]:

 If topic in streaming routes table
    for all streaming operations in list
       if operation.state == SUBSCRIBED
          invoke publish received callback

 If topic in paths table:
    If correlation token extraction success
        If entry exists in correlation token table
            Complete operation with publish payload
 */

/*
 On Publish completion [Event Loop]:

 If Error
    Complete and Fail Operation(id)

 */


/*
 On protocol adapter connection event [Event Loop]:

 Notify subscription manager
 Wake service task
 */

/*
 On subscription status event [Event Loop, top-level task]:

 For all streaming operations in topic_filter table list:
    If Success and state == SUBSCRIBING
        state <- SUBSCRIBED
        Invoke Success/Failure callback with success
    Else If Failure and state == SUBSCRIBING
        state <- TERMINAL
        Invoke Success/Failure callback with failure
    Else if Subscription Ended and state != TERMINAL
        state <- TERMINAL
        Invoke Ended callback
    If Failure or Ended:
        sub manager release_subscription(operation id)

 For all request operations in request topic filter list:
    If Success and state == SUBSCRIBING
       MakeRequest(op)

    If Failure or Ended
       Complete operation with failure

 */

/*
 MakeRequest(op) [Event Loop]:

    state <- AWAITING_RESPONSE
    if publish fails synchronously
        Complete operation with failure
        Decref(op)
 */

/*
 Handle acquire sub result(op, result) [Event Loop, Service Task Loop]:

 If result == {No Capacity, Failure}
    If op is streaming
       Invoke failure callback
       state <- TERMINAL
    else
       Complete operation with failure
       Decref
    return

 If streaming
    Add operation to topic filter table
    State <- {SUBSCRIBING, SUBSCRIBED}

 If request
    Add operation to topic filter table
    if result == SUBSCRIBING
       state <- SUBSCRIBING
    else // (SUBSCRIBED)
       MakeRequest(op)


 */
/*
 Service task [Event Loop]:

 For all timed out operations:
    Invoke On Operation Timeout

 While OperationQueue is not empty:
    op = peek queue
    result = subscription manager acquire sub(op)
    if result == Blocked
       break
    pop op
    handle acquire sub result (op, result)

 Reschedule Service for next timeout if it exists
 */

/*
 On operation timeout [Event Loop, Service Task Loop]:

 If request
    Complete with failure
    Decref-op
 If streaming and state != {SUBSCRIBED, TERMINAL}
    state <- TERMINAL
    Invoke failure callback

 */

struct aws_mqtt_rr_client_operation {
    struct aws_allocator *allocator;

    struct aws_ref_count ref_count;

    struct aws_mqtt_request_response_client *internal_client_ref;

    uint64_t id;

    enum aws_mqtt_request_response_operation_type type;

    union {
        struct aws_mqtt_streaming_operation_storage streaming_storage;
        struct aws_mqtt_request_operation_storage request_storage;
    } storage;

    uint64_t ack_timeout_timepoint_ns;
    struct aws_priority_queue_node priority_queue_node;
    struct aws_linked_list_node node;

    enum aws_mqtt_request_response_operation_state state;
};

/*******************************************************************************************/

/* Tracks the current state of the request-response client */
enum aws_request_response_client_state {

    /* cross-thread initialization has not completed and all protocol adapter callbacks are ignored */
    AWS_RRCS_UNINITIALIZED,

    /* Normal operating state for the client. */
    AWS_RRCS_ACTIVE,

    /* asynchronously shutting down, no more servicing will be done and all protocol adapter callbacks are ignored */
    AWS_RRCS_SHUTTING_DOWN,
};

/*
 * Request-Response Client Notes
 *
 * Ref-counting/Shutdown
 *
 * The request-response client uses a double ref-count pattern.
 *
 * External references represent user references.  When the external reference reaches zero, the client's asynchronous
 * shutdown process is started.
 *
 * Internal references block final destruction.  Asynchronous shutdown will not complete until all internal references
 * are dropped.  In addition to one long-lived internal reference (the protocol client adapter's back reference to
 * the request-response client), all event loop tasks that target the request-response client hold an internal
 * reference between task submission and task completion.  This ensures that the task always has a valid reference
 * to the client, even if we're trying to shut down at the same time.
 *
 *
 * Initialization
 *
 * Initialization is complicated by the fact that the subscription manager needs to be initialized from the
 * event loop thread that the client/protocol adapter/protocol client are all seated on.  To do this safely,
 * we add an uninitialized state that ignores all callbacks and we schedule a task on initial construction to do
 * the event-loop-only initialization.  Once that initialization completes on the event loop thread, we move
 * the client into an active state where it will process operations and protocol adapter callbacks.
 */
struct aws_mqtt_request_response_client {
    struct aws_allocator *allocator;

    struct aws_ref_count external_ref_count;
    struct aws_ref_count internal_ref_count;

    struct aws_mqtt_request_response_client_options config;

    struct aws_mqtt_protocol_adapter *client_adapter;

    struct aws_rr_subscription_manager subscription_manager;

    struct aws_event_loop *loop;

    struct aws_task initialize_task;
    struct aws_task external_shutdown_task;
    struct aws_task internal_shutdown_task;

    enum aws_request_response_client_state state;
};

static void s_aws_rr_client_on_zero_internal_ref_count(void *context) {
    struct aws_mqtt_request_response_client *client = context;

    /* Both ref counts are zero, but it's still safest to schedule final destruction, not invoke it directly */
    aws_event_loop_schedule_task_now(client->loop, &client->internal_shutdown_task);
}

static void s_aws_rr_client_on_zero_external_ref_count(void *context) {
    struct aws_mqtt_request_response_client *client = context;

    /* Start the asynchronous shutdown process */
    aws_event_loop_schedule_task_now(client->loop, &client->external_shutdown_task);
}

static void s_mqtt_request_response_client_final_destroy(struct aws_mqtt_request_response_client *client) {
    aws_mqtt_request_response_client_terminated_callback_fn *terminate_callback = client->config.terminated_callback;
    void *user_data = client->config.user_data;

    aws_mem_release(client->allocator, client);

    if (terminate_callback != NULL) {
        (*terminate_callback)(user_data);
    }
}

static void s_mqtt_request_response_client_internal_shutdown_task_fn(
    struct aws_task *task,
    void *arg,
    enum aws_task_status task_status) {
    (void)task;
    (void)task_status;

    struct aws_mqtt_request_response_client *client = arg;

    /*
     * All internal and external refs are gone; it is safe to clean up synchronously.
     *
     * The subscription manager is cleaned up and the protocol adapter has been shut down.  No tasks targeting the
     * client are active (other than this one).  All that's left is to free memory.
     */
    s_mqtt_request_response_client_final_destroy(client);
}

static void s_mqtt_request_response_client_external_shutdown_task_fn(
    struct aws_task *task,
    void *arg,
    enum aws_task_status task_status) {
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

static void s_aws_rr_client_subscription_status_event_callback(
    const struct aws_rr_subscription_status_event *event,
    void *userdata) {
    (void)event;
    (void)userdata;

    /*
     * We must be on the event loop, but it's safer overall to process this event as a top-level event loop task.  The
     * subscription manager assumes that we won't call APIs on it while iterating subscription records and listeners.
     *
     * These tasks hold an internal reference while they exist.
     */

    /* NYI */
}

static void s_aws_rr_client_protocol_adapter_subscription_event_callback(
    const struct aws_protocol_adapter_subscription_event *event,
    void *user_data) {
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

    /* NYI */
}

static void s_aws_rr_client_protocol_adapter_terminate_callback(void *user_data) {
    struct aws_mqtt_request_response_client *rr_client = user_data;

    /* release the internal ref count "held" by the protocol adapter's existence */
    aws_ref_count_release(&rr_client->internal_ref_count);
}

static void s_aws_rr_client_protocol_adapter_connection_event_callback(
    const struct aws_protocol_adapter_connection_event *event,
    void *user_data) {
    struct aws_mqtt_request_response_client *rr_client = user_data;

    AWS_FATAL_ASSERT(aws_event_loop_thread_is_callers_thread(rr_client->loop));

    if (rr_client->state != AWS_RRCS_ACTIVE) {
        return;
    }

    aws_rr_subscription_manager_on_protocol_adapter_connection_event(&rr_client->subscription_manager, event);
}

static struct aws_mqtt_request_response_client *s_aws_mqtt_request_response_client_new(
    struct aws_allocator *allocator,
    const struct aws_mqtt_request_response_client_options *options,
    struct aws_event_loop *loop) {
    struct aws_rr_subscription_manager_options sm_options = {
        .max_subscriptions = options->max_subscriptions,
        .operation_timeout_seconds = options->operation_timeout_seconds,
    };

    /*
     * We can't initialize the subscription manager until we're running on the event loop, so make sure that
     * initialize can't fail by checking its options for validity now.
     */
    if (!aws_rr_subscription_manager_are_options_valid(&sm_options)) {
        return NULL;
    }

    struct aws_mqtt_request_response_client *rr_client =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_request_response_client));

    rr_client->allocator = allocator;
    rr_client->config = *options;
    rr_client->loop = loop;
    rr_client->state = AWS_RRCS_UNINITIALIZED;

    aws_task_init(
        &rr_client->external_shutdown_task,
        s_mqtt_request_response_client_external_shutdown_task_fn,
        rr_client,
        "mqtt_rr_client_external_shutdown");
    aws_task_init(
        &rr_client->internal_shutdown_task,
        s_mqtt_request_response_client_internal_shutdown_task_fn,
        rr_client,
        "mqtt_rr_client_internal_shutdown");

    /* The initial external ref belongs to the caller */
    aws_ref_count_init(&rr_client->external_ref_count, rr_client, s_aws_rr_client_on_zero_external_ref_count);

    /* The initial internal ref belongs to ourselves (the external ref count shutdown task) */
    aws_ref_count_init(&rr_client->internal_ref_count, rr_client, s_aws_rr_client_on_zero_internal_ref_count);

    return rr_client;
}

static void s_aws_rr_client_init_subscription_manager(
    struct aws_mqtt_request_response_client *rr_client,
    struct aws_allocator *allocator) {
    AWS_FATAL_ASSERT(aws_event_loop_thread_is_callers_thread(rr_client->loop));

    struct aws_rr_subscription_manager_options subscription_manager_options = {
        .operation_timeout_seconds = rr_client->config.operation_timeout_seconds,
        .max_subscriptions = rr_client->config.max_subscriptions,
        .subscription_status_callback = s_aws_rr_client_subscription_status_event_callback,
        .userdata = rr_client,
    };

    aws_rr_subscription_manager_init(
        &rr_client->subscription_manager, allocator, rr_client->client_adapter, &subscription_manager_options);
}

static void s_mqtt_request_response_client_initialize_task_fn(
    struct aws_task *task,
    void *arg,
    enum aws_task_status task_status) {
    (void)task;

    AWS_FATAL_ASSERT(task_status != AWS_TASK_STATUS_CANCELED);

    struct aws_mqtt_request_response_client *client = arg;

    if (client->state == AWS_RRCS_UNINITIALIZED) {
        s_aws_rr_client_init_subscription_manager(client, client->allocator);

        client->state = AWS_RRCS_ACTIVE;
    }

    if (client->config.initialized_callback != NULL) {
        (*client->config.initialized_callback)(client->config.user_data);
    }

    /* give up the internal ref we held while the task was pending */
    aws_ref_count_release(&client->internal_ref_count);
}

static void s_setup_cross_thread_initialization(struct aws_mqtt_request_response_client *rr_client) {
    /* now that it exists, 1 internal ref belongs to protocol adapter termination */
    aws_ref_count_acquire(&rr_client->internal_ref_count);

    /* 1 internal ref belongs to the initialize task until it runs */
    aws_ref_count_acquire(&rr_client->internal_ref_count);
    aws_task_init(
        &rr_client->initialize_task,
        s_mqtt_request_response_client_initialize_task_fn,
        rr_client,
        "mqtt_rr_client_initialize");
    aws_event_loop_schedule_task_now(rr_client->loop, &rr_client->initialize_task);
}

struct aws_mqtt_request_response_client *aws_mqtt_request_response_client_new_from_mqtt311_client(
    struct aws_allocator *allocator,
    struct aws_mqtt_client_connection *client,
    const struct aws_mqtt_request_response_client_options *options) {

    struct aws_mqtt_request_response_client *rr_client =
        s_aws_mqtt_request_response_client_new(allocator, options, aws_mqtt_client_connection_get_event_loop(client));

    struct aws_mqtt_protocol_adapter_options adapter_options = {
        .subscription_event_callback = s_aws_rr_client_protocol_adapter_subscription_event_callback,
        .incoming_publish_callback = s_aws_rr_client_protocol_adapter_incoming_publish_callback,
        .terminate_callback = s_aws_rr_client_protocol_adapter_terminate_callback,
        .connection_event_callback = s_aws_rr_client_protocol_adapter_connection_event_callback,
        .user_data = rr_client,
    };

    rr_client->client_adapter = aws_mqtt_protocol_adapter_new_from_311(rr_client->allocator, &adapter_options, client);
    if (rr_client->client_adapter == NULL) {
        goto error;
    }

    s_setup_cross_thread_initialization(rr_client);

    return rr_client;

error:

    /* even on construction failures we still need to walk through the async shutdown process */
    aws_mqtt_request_response_client_release(rr_client);

    return NULL;
}

struct aws_mqtt_request_response_client *aws_mqtt_request_response_client_new_from_mqtt5_client(
    struct aws_allocator *allocator,
    struct aws_mqtt5_client *client,
    const struct aws_mqtt_request_response_client_options *options) {

    struct aws_mqtt_request_response_client *rr_client =
        s_aws_mqtt_request_response_client_new(allocator, options, client->loop);

    struct aws_mqtt_protocol_adapter_options adapter_options = {
        .subscription_event_callback = s_aws_rr_client_protocol_adapter_subscription_event_callback,
        .incoming_publish_callback = s_aws_rr_client_protocol_adapter_incoming_publish_callback,
        .terminate_callback = s_aws_rr_client_protocol_adapter_terminate_callback,
        .connection_event_callback = s_aws_rr_client_protocol_adapter_connection_event_callback,
        .user_data = rr_client,
    };

    rr_client->client_adapter = aws_mqtt_protocol_adapter_new_from_5(rr_client->allocator, &adapter_options, client);
    if (rr_client->client_adapter == NULL) {
        goto error;
    }

    s_setup_cross_thread_initialization(rr_client);

    return rr_client;

error:

    /* even on construction failures we still need to walk through the async shutdown process */
    aws_mqtt_request_response_client_release(rr_client);

    return NULL;
}

struct aws_mqtt_request_response_client *aws_mqtt_request_response_client_acquire(
    struct aws_mqtt_request_response_client *client) {
    if (client != NULL) {
        aws_ref_count_acquire(&client->external_ref_count);
    }

    return client;
}

struct aws_mqtt_request_response_client *aws_mqtt_request_response_client_release(
    struct aws_mqtt_request_response_client *client) {
    if (client != NULL) {
        aws_ref_count_release(&client->external_ref_count);
    }

    return NULL;
}

int aws_mqtt_request_response_client_submit_request(
    struct aws_mqtt_request_response_client *client,
    struct aws_mqtt_request_operation_options *request_options) {
    (void)client;
    (void)request_options;

    return aws_raise_error(AWS_ERROR_UNIMPLEMENTED);
}

AWS_MQTT_API struct aws_mqtt_streaming_operation *aws_mqtt_request_response_client_create_streaming_operation(
    struct aws_mqtt_request_response_client *client,
    struct aws_mqtt_streaming_operation_options *streaming_options) {

    (void)client;
    (void)streaming_options;

    return NULL;
}

AWS_MQTT_API struct aws_mqtt_streaming_operation *aws_mqtt_streaming_operation_acquire(struct aws_mqtt_streaming_operation *operation) {
    (void)operation;

    return NULL;
}

AWS_MQTT_API struct aws_mqtt_streaming_operation *aws_mqtt_streaming_operation_release(struct aws_mqtt_streaming_operation *operation) {
    (void)operation;

    return NULL;
}
