/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/request-response/request_response_client.h>

#include <aws/common/clock.h>
#include <aws/common/ref_count.h>
#include <aws/common/task_scheduler.h>
#include <aws/io/event_loop.h>
#include <aws/mqtt/private/client_impl_shared.h>
#include <aws/mqtt/private/request-response/protocol_adapter.h>
#include <aws/mqtt/private/request-response/subscription_manager.h>
#include <aws/mqtt/private/v5/mqtt5_client_impl.h>

#include <inttypes.h>

#define MQTT_RR_CLIENT_OPERATION_TABLE_DEFAULT_SIZE 50

enum aws_mqtt_request_response_operation_type {
    AWS_MRROT_REQUEST,
    AWS_MRROT_STREAMING,
};

enum aws_mqtt_request_response_operation_state {
    AWS_MRROS_NONE,                 // creation -> in event loop enqueue
    AWS_MRROS_QUEUED,               // in event loop queue -> non blocked response from subscription manager
    AWS_MRROS_PENDING_SUBSCRIPTION, // subscribing response from sub manager -> subscription success/failure event
    AWS_MRROS_PENDING_RESPONSE,     // (request only) subscription success -> (publish failure OR correlated response
                                    // received)
    AWS_MRROS_SUBSCRIBED, // (streaming only) subscription success -> (operation finished OR subscription ended event)
    AWS_MRROS_TERMINAL,   // (streaming only) (subscription failure OR subscription ended) -> operation close/terminate
    AWS_MRROS_PENDING_DESTROY, // (request only) the operation's destroy task has been scheduled but not yet
                               // executed
};

const char *s_aws_mqtt_request_response_operation_state_to_c_str(enum aws_mqtt_request_response_operation_state state) {
    switch (state) {
        case AWS_MRROS_NONE:
            return "NONE";

        case AWS_MRROS_QUEUED:
            return "QUEUED";

        case AWS_MRROS_PENDING_SUBSCRIPTION:
            return "PENDING_SUBSCRIPTION";

        case AWS_MRROS_PENDING_RESPONSE:
            return "PENDING_RESPONSE";

        case AWS_MRROS_SUBSCRIBED:
            return "SUBSCRIBED";

        case AWS_MRROS_TERMINAL:
            return "TERMINAL";

        case AWS_MRROS_PENDING_DESTROY:
            return "PENDING_DESTROY";

        default:
            return "Unknown";
    }
}

const char *s_aws_acquire_subscription_result_type(enum aws_acquire_subscription_result_type result) {
    switch (result) {
        case AASRT_SUBSCRIBED:
            return "SUBSCRIBED";

        case AASRT_SUBSCRIBING:
            return "SUBSCRIBING";

        case AASRT_BLOCKED:
            return "BLOCKED";

        case AASRT_NO_CAPACITY:
            return "NO_CAPACITY";

        case AASRT_FAILURE:
            return "FAILURE";

        default:
            return "Unknown";
    }
}

/*

Client Tables/Lookups

    (Authoritative operation container)
    1. &operation.id -> &operation // added on in-thread enqueue, removed on operation completion/destruction

    (Response topic -> Correlation token extraction info)
    2. &topic -> &{topic, topic_buffer, correlation token json path buffer} // per-message-path add/replace on in-thread
enqueue, removed on client destruction

    (Correlation token -> request operation)
    3. &operation.correlation token -> (request) &operation // added on in-thread request op move to awaiting response
state, removed on operation completion/destruction

    (Subscription filter -> all operations using that filter)
    4. &topic_filter -> &{topic_filter, linked_list} // added on in-thread pop from queue, removed from list on
operation completion/destruction also checked for empty and removed from table

    Note: 4 tracks both streaming and request-response operations but each uses the table in different ways.  Both use
    the table to react to subscription status events to move the operation forward state-wise.  Additionally,
    streaming operations use the table to map incoming publishes to listening streaming operations.  OTOH, request
    operations use table 2 and then 3 to map incoming publishes to operations.

*/

struct aws_rr_operation_list_topic_filter_entry {
    struct aws_allocator *allocator;

    struct aws_byte_cursor topic_filter_cursor;
    struct aws_byte_buf topic_filter;

    struct aws_linked_list operations;
};

struct aws_rr_operation_list_topic_filter_entry *s_aws_rr_operation_list_topic_filter_entry_new(
    struct aws_allocator *allocator,
    struct aws_byte_cursor topic_filter) {
    struct aws_rr_operation_list_topic_filter_entry *entry =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_rr_operation_list_topic_filter_entry));

    entry->allocator = allocator;
    aws_byte_buf_init_copy_from_cursor(&entry->topic_filter, allocator, topic_filter);
    entry->topic_filter_cursor = aws_byte_cursor_from_buf(&entry->topic_filter);

    aws_linked_list_init(&entry->operations);

    return entry;
}

void s_aws_rr_operation_list_topic_filter_entry_destroy(struct aws_rr_operation_list_topic_filter_entry *entry) {
    if (entry == NULL) {
        return;
    }

    aws_byte_buf_clean_up(&entry->topic_filter);

    aws_mem_release(entry->allocator, entry);
}

void s_aws_rr_operation_list_topic_filter_entry_hash_element_destroy(void *value) {
    s_aws_rr_operation_list_topic_filter_entry_destroy(value);
}

/* All operations have an internal ref to the client they are a part of */

/*
 SubmitRequestOperation(options) [Anywhere]:

 Validate options
 Allocate id
 Create operation with ref count == 2
 Submit cross-thread task

 */

/*
 CreateStreamingOperation(options) [Anywhere]:

 Validate options
 Allocate id
 Create operation with ref count == 2
 Submit cross-thread task
 Return (ref-counted) operation

 */

/*
 OperationSubmissionThreadTask(operation) [Event Loop, top-level task]:

 Add to client.operations table
 (Request) Add message paths to client.paths table if no exist or different value
 Add to client's timeout priority queue
 Add operation to end of client.operation_queue list
 operation.state <- QUEUED
 WakeServiceTask
 operation.decref (2 -> 1)

 */

/*
 CompleteRequestOperation(operation, payload, error_code) [Event Loop]:

 if operation.state != PENDING_DESTROY
     CompletionCallback(payload, error_code)
     operation.state <- PENDING_DESTROY
     operation.decref // schedules destroy task
 */

/*
 OnOperationZeroRefCount(operation) [Anywhere]:

 Schedule operation's destroy task on client event loop
 */

/*
 WakeServiceTask(client) [Event Loop]:

 If client.state == ACTIVE && client.connected
    RescheduleServiceTask(now)
 */

/*
 OperationDestroyTask(operation) [Event Loop, top-level task]:

 Remove from client.operations
 Remove from (client) intrusive list
 Remove from client's timeout priority queue
 if operation.type == REQUEST
    Remove from client's correlation token table
    Zero publish completion weak ref wrapper around operation
    dec-ref weak-ref-operation-wrapper
 Check client's topic filter table entry for empty list, remove entry if so. (intrusive list removal already unlinked it
 from table) If client is not shutting down remove from subscription manager (otherwise it's already been cleaned up)

 client.subscription_manager.release_subscription(operation.topic_filter)
 WakeServiceTask // queue may now be unblocked, does nothing if shutting down
 (Streaming) Invoke termination callback
 Release client internal ref

 */

/*
 OnIncomingPublish(publish) [Event Loop]:

 if client.state != ACTIVE
    // If shutting down, request operations are all in PENDING_DESTROY
    // If initializing, publish cannot be relevant
    return

 If publish.topic in client's topic filter table
    for all streaming operations in list
       if operation.state == SUBSCRIBED
          invoke publish received callback

 If publish.topic in paths table:
    If correlation token extraction success
        If operation entry exists in correlation token table
            CompleteRequestOperation(operation, payload) // Complete does nothing if the operation is being killed
 */

/*
 OnProtocolAdapterConnectionEvent(event) [Event Loop]:

 client.connected <- event.connected
 client.subscription_manager.notify(event)
 WakeServiceTask
 */

/*
 OnPublishCompletion(result, userdata) [Event Loop, Direct From Protocol Adapter, Operation as UserData]:

 weak-ref-operation-wrapper = userdata
 if weak-ref-operation-wrapper can be resolved to an operation:
    If result is error
        CompleteRequestOperation(operation, error)

 dec-ref weak-ref-operation-wrapper
 */

/*
 MakeRequest(operation) [Event Loop]:

 operation.state <- SUBSCRIBED
 if !client.connected
    return

 // Critical Requirement - the user data for the publish completion callback must be a weak ref that wraps
 // the operation.  On operation destruction, we zero the weak ref (and dec ref it).
 operation.state <- AWAITING_RESPONSE
 if publish fails synchronously
    CompleteRequestOperation(operation, error)
 */

/*
 RequestOperationOnSubscriptionStatusEvent(operation, event) [Event loop, top-level task loop]:

 If event.type == SUBSCRIBE_SUCCESS and operation.state == SUBSCRIBING
    MakeRequest(operation)

 If event.type == {SUBSCRIBE_FAILURE, ENDED}
    CompleteRequestOperation(failure)
 */

/*
 StreamingOperationOnSubscriptionStatusEvent(operation, event) [Event loop, top-level task loop]:

 If event.type == Success
    Emit SubscriptionEstablished
 Else If event.type == Lost
    Emit SubscriptionLost
 Else if event.type == Halted
    operation.state <- TERMINAL
    Emit SubscriptionHalted

 */

/*
 OnSubscriptionStatusEvent(event) [Event Loop, top-level task from sub manager]:

 For all operations in topic_filter table list:
    if operation.type == Streaming
        StreamingOperationOnSubscriptionStatusEvent(operation, event)
    else
        RequestOperationOnSubscriptionStatusEvent(operation, event)
 */

/*
 HandleAcquireSubscriptionResult(operation, result) [Event Loop, Service Task Loop]:

 // invariant, BLOCKED is not possible, it was already handled
 If result == {No Capacity, Failure}
    If operation is streaming
       Invoke failure callback
       operation.state <- TERMINAL
    else
       CompleteRequestOperation(operation, error)
    return

 // invariant, must be SUBSCRIBING or SUBSCRIBED at this point
 Add operation to client's topic filter table

 If operation is streaming
    Add operation to topic filter table
    operation.state <- {SUBSCRIBING, SUBSCRIBED}

 If operation is request
    if result == SUBSCRIBING
       operation.state <- SUBSCRIBING
    else // (SUBSCRIBED)
       MakeRequest(op)
 */

/*
 Service task [Event Loop]:

 For all timed out operations:
    OnOperationTimeout(operation)

 if client connected

     For all request operations where state == SUBSCRIBED
        MakeRequest(operation)

     While OperationQueue is not empty:
        operation = peek queue
        result = subscription manager acquire sub(operation)
        if result == Blocked
           break
        pop operation
        HandleAcquireSubscriptionResult(operation, result)

 Reschedule Service for next timeout if it exists
 */

/*
 OnOperationTimeout(operation) [Event Loop, Service Task Loop, operation is request]:

 CompleteRequestOperation(operation, error)

 */

struct aws_mqtt_rr_client_operation {
    struct aws_allocator *allocator;

    /*
     * Operation ref-counting is a bit tricky and un-intuitive because it differs based on the type of operation.
     *
     * Streaming operations are managed by the user, and so the ref count is their responsibility to drop to zero.
     * Dropping a streaming operation's ref count to zero schedules a task on the client event loop to destroy the
     * operation.  It is expected that the binding client will track (with proper synchronization) all unclosed
     * streaming operations and safely close them for the user when close is called on the binding client.
     *
     * Request operations are managed by the client, and so the ref count is dropped to zero when either the
     * operation completes normally (success or failure) or when the client is shutdown due to its external ref
     * count dropping to zero.  In all cases, this event happens naturally on the client event loop.
     *
     * So the summary is:
     *
     * (1) Streaming operation clean up is initiated by the user calling dec ref on the streaming operation
     * (2) Request operation clean up is initiated by normal completion or client shutdown invoking dec ref.
     *
     * The upshot is that client shutdown dec-refs request operations but not streaming operations.
     */
    struct aws_ref_count ref_count;

    struct aws_mqtt_request_response_client *client_internal_ref;

    uint64_t id;

    enum aws_mqtt_request_response_operation_type type;

    union {
        struct aws_mqtt_streaming_operation_storage streaming_storage;
        struct aws_mqtt_request_operation_storage request_storage;
    } storage;

    uint64_t timeout_timepoint_ns;
    struct aws_priority_queue_node priority_queue_node;

    /* Sometimes this is client->operation_queue, other times it is an entry in the client's topic_filter table */
    struct aws_linked_list_node node;

    enum aws_mqtt_request_response_operation_state state;

    struct aws_task submit_task;
    struct aws_task destroy_task;
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

    uint64_t scheduled_service_timepoint_ns;
    struct aws_task service_task;

    enum aws_request_response_client_state state;

    struct aws_atomic_var next_id;

    struct aws_linked_list operation_queue;

    /* &operation->id -> &operation */
    struct aws_hash_table operations;

    /*
     * heap of operation pointers where the timeout is the sort value.  Elements are added to this on operation
     * submission and removed on operation timeout/completion/termination.  Request-response operations have actual
     * timeouts, while streaming operations have UINT64_MAX timeouts.
     */
    struct aws_priority_queue operations_by_timeout;

    struct aws_hash_table operation_lists_by_subscription_filter;
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

    AWS_FATAL_ASSERT(aws_hash_table_get_entry_count(&client->operations) == 0);
    aws_hash_table_clean_up(&client->operations);

    aws_priority_queue_clean_up(&client->operations_by_timeout);
    aws_hash_table_clean_up(&client->operation_lists_by_subscription_filter);

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

    /* All internal and external refs are gone; it is safe to clean up synchronously. */
    s_mqtt_request_response_client_final_destroy(client);
}

static void s_remove_operation_from_timeout_queue(struct aws_mqtt_rr_client_operation *operation) {
    struct aws_mqtt_request_response_client *client = operation->client_internal_ref;

    if (aws_priority_queue_node_is_in_queue(&operation->priority_queue_node)) {
        struct aws_mqtt_rr_client_operation *queued_operation = NULL;
        aws_priority_queue_remove(&client->operations_by_timeout, &queued_operation, &operation->priority_queue_node);
    }
}

static void s_change_operation_state(
    struct aws_mqtt_rr_client_operation *operation,
    enum aws_mqtt_request_response_operation_state new_state) {
    enum aws_mqtt_request_response_operation_state old_state = operation->state;
    if (old_state == new_state) {
        return;
    }

    operation->state = new_state;

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_REQUEST_RESPONSE,
        "id=%p: request-response operation %" PRIu64 " changing state from %s to %s",
        (void *)operation->client_internal_ref,
        operation->id,
        s_aws_mqtt_request_response_operation_state_to_c_str(old_state),
        s_aws_mqtt_request_response_operation_state_to_c_str(new_state));
}

static void s_complete_request_operation_with_failure(struct aws_mqtt_rr_client_operation *operation, int error_code) {
    AWS_FATAL_ASSERT(operation->type == AWS_MRROT_REQUEST);
    AWS_FATAL_ASSERT(error_code != AWS_ERROR_SUCCESS);

    if (operation->state == AWS_MRROS_PENDING_DESTROY) {
        return;
    }

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_REQUEST_RESPONSE,
        "id=%p: request-response operation %" PRIu64 " failed with error code %d(%s)",
        (void *)operation->client_internal_ref,
        operation->id,
        error_code,
        aws_error_debug_str(error_code));

    aws_mqtt_request_operation_completion_fn *completion_callback =
        operation->storage.request_storage.options.completion_callback;
    void *user_data = operation->storage.request_storage.options.user_data;

    if (completion_callback != NULL) {
        (*completion_callback)(NULL, error_code, user_data);
    }

    s_change_operation_state(operation, AWS_MRROS_PENDING_DESTROY);

    aws_mqtt_rr_client_operation_release(operation);
}

static void s_halt_streaming_operation_with_failure(struct aws_mqtt_rr_client_operation *operation, int error_code) {
    AWS_FATAL_ASSERT(operation->type == AWS_MRROT_STREAMING);
    AWS_FATAL_ASSERT(error_code != AWS_ERROR_SUCCESS);

    if (operation->state == AWS_MRROS_PENDING_DESTROY || operation->state == AWS_MRROS_TERMINAL) {
        return;
    }

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_REQUEST_RESPONSE,
        "id=%p: streaming operation %" PRIu64 " halted with error code %d(%s)",
        (void *)operation->client_internal_ref,
        operation->id,
        error_code,
        aws_error_debug_str(error_code));

    aws_mqtt_streaming_operation_subscription_status_fn *subscription_status_callback =
        operation->storage.streaming_storage.options.subscription_status_callback;

    if (subscription_status_callback != NULL) {
        void *user_data = operation->storage.streaming_storage.options.user_data;
        (*subscription_status_callback)(ARRSET_STREAMING_SUBSCRIPTION_HALTED, error_code, user_data);
    }

    s_change_operation_state(operation, AWS_MRROS_TERMINAL);
}

static void s_request_response_fail_operation(struct aws_mqtt_rr_client_operation *operation, int error_code) {
    if (operation->type == AWS_MRROT_STREAMING) {
        s_halt_streaming_operation_with_failure(operation, error_code);
    } else {
        s_complete_request_operation_with_failure(operation, error_code);
    }
}

static int s_rr_client_clean_up_operation(void *context, struct aws_hash_element *elem) {
    (void)context;
    struct aws_mqtt_rr_client_operation *operation = elem->value;

    s_request_response_fail_operation(operation, AWS_ERROR_MQTT_REQUEST_RESPONSE_CLIENT_SHUT_DOWN);

    return AWS_COMMON_HASH_TABLE_ITER_CONTINUE;
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

    if (client->scheduled_service_timepoint_ns != 0) {
        aws_event_loop_cancel_task(client->loop, &client->service_task);
        client->scheduled_service_timepoint_ns = 0;
    }

    aws_rr_subscription_manager_clean_up(&client->subscription_manager);

    if (client->client_adapter != NULL) {
        aws_mqtt_protocol_adapter_destroy(client->client_adapter);
    }

    /*
     * It is a client invariant that when external shutdown starts, it must be the case that there are no in-flight
     * operations with un-executed submit tasks.  This means it safe to assume that all tracked request operations are
     * either in the process of cleaning up already (state == AWS_MRROS_PENDING_DESTROY) or can be
     * completed now (state != AWS_MRROS_PENDING_DESTROY).  Non-terminal streaming operations are moved into
     * a terminal state and emit an appropriate failure/ended event.
     *
     * Actual operation destruction and client ref-count release is done by a scheduled task
     * on the operation that is triggered by dec-refing it (assuming streaming operations get closed by the binding
     * client).
     */
    aws_hash_table_foreach(&client->operations, s_rr_client_clean_up_operation, NULL);

    aws_ref_count_release(&client->internal_ref_count);
}

static void s_mqtt_request_response_client_wake_service(struct aws_mqtt_request_response_client *client) {
    uint64_t now = 0;
    aws_high_res_clock_get_ticks(&now);

    AWS_FATAL_ASSERT(aws_event_loop_thread_is_callers_thread(client->loop));

    if (client->state != AWS_RRCS_ACTIVE) {
        return;
    }

    if (client->scheduled_service_timepoint_ns == 0 || now < client->scheduled_service_timepoint_ns) {
        if (now < client->scheduled_service_timepoint_ns) {
            aws_event_loop_cancel_task(client->loop, &client->service_task);
        }

        client->scheduled_service_timepoint_ns = now;
        aws_event_loop_schedule_task_now(client->loop, &client->service_task);

        AWS_LOGF_DEBUG(
            AWS_LS_MQTT_REQUEST_RESPONSE, "id=%p: request-response client service task woke", (void *)client);
    }
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

    struct aws_mqtt_request_response_client *rr_client = userdata;

    AWS_FATAL_ASSERT(aws_event_loop_thread_is_callers_thread(rr_client->loop));
    AWS_FATAL_ASSERT(rr_client->state != AWS_RRCS_SHUTTING_DOWN);

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

static void s_apply_publish_to_streaming_operation_list(
    struct aws_rr_operation_list_topic_filter_entry *entry,
    const struct aws_protocol_adapter_incoming_publish_event *publish_event) {
    AWS_FATAL_ASSERT(entry != NULL);

    struct aws_linked_list_node *node = aws_linked_list_begin(&entry->operations);
    while (node != aws_linked_list_end(&entry->operations)) {
        struct aws_mqtt_rr_client_operation *operation =
            AWS_CONTAINER_OF(node, struct aws_mqtt_rr_client_operation, node);
        node = aws_linked_list_next(node);

        if (operation->type != AWS_MRROT_STREAMING) {
            continue;
        }

        if (operation->state == AWS_MRROS_PENDING_DESTROY || operation->state == AWS_MRROS_TERMINAL) {
            continue;
        }

        aws_mqtt_streaming_operation_incoming_publish_fn *incoming_publish_callback =
            operation->storage.streaming_storage.options.incoming_publish_callback;
        if (!incoming_publish_callback) {
            continue;
        }

        void *user_data = operation->storage.streaming_storage.options.user_data;
        (*incoming_publish_callback)(publish_event->payload, user_data);

        AWS_LOGF_DEBUG(
            AWS_LS_MQTT_REQUEST_RESPONSE,
            "id=%p: request-response client incoming publish on topic '" PRInSTR
            "' routed to streaming operation %" PRIu64,
            (void *)operation->client_internal_ref,
            AWS_BYTE_CURSOR_PRI(publish_event->topic),
            operation->id);
    }
}

static void s_aws_rr_client_protocol_adapter_incoming_publish_callback(
    const struct aws_protocol_adapter_incoming_publish_event *publish_event,
    void *user_data) {

    struct aws_mqtt_request_response_client *rr_client = user_data;

    AWS_FATAL_ASSERT(aws_event_loop_thread_is_callers_thread(rr_client->loop));

    if (rr_client->state != AWS_RRCS_ACTIVE) {
        return;
    }

    /* Streaming operation handling */
    struct aws_hash_element *subscription_filter_element = NULL;
    if (aws_hash_table_find(
            &rr_client->operation_lists_by_subscription_filter, &publish_event->topic, &subscription_filter_element) ==
        AWS_OP_SUCCESS) {
        if (subscription_filter_element != NULL) {
            AWS_LOGF_DEBUG(
                AWS_LS_MQTT_REQUEST_RESPONSE,
                "id=%p: request-response client incoming publish on topic '" PRInSTR "'",
                (void *)rr_client,
                AWS_BYTE_CURSOR_PRI(publish_event->topic));

            s_apply_publish_to_streaming_operation_list(subscription_filter_element->value, publish_event);
        }
    }

    /* Request-Response handling NYI */
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

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_REQUEST_RESPONSE,
        "id=%p: request-response client applying connection event to subscription manager",
        (void *)rr_client);

    aws_rr_subscription_manager_on_protocol_adapter_connection_event(&rr_client->subscription_manager, event);
}

uint64_t aws_mqtt_hash_uint64_t(const void *item) {
    return *(uint64_t *)item;
}

bool aws_mqtt_compare_uint64_t_eq(const void *a, const void *b) {
    return *(uint64_t *)a == *(uint64_t *)b;
}

static int s_compare_rr_operation_timeouts(const void *a, const void *b) {
    const struct aws_mqtt_rr_client_operation **operation_a_ptr = (void *)a;
    const struct aws_mqtt_rr_client_operation *operation_a = *operation_a_ptr;

    const struct aws_mqtt_rr_client_operation **operation_b_ptr = (void *)b;
    const struct aws_mqtt_rr_client_operation *operation_b = *operation_b_ptr;

    if (operation_a->timeout_timepoint_ns < operation_b->timeout_timepoint_ns) {
        return -1;
    } else if (operation_a->timeout_timepoint_ns > operation_b->timeout_timepoint_ns) {
        return 1;
    } else {
        return 0;
    }
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
        AWS_LOGF_ERROR(
            AWS_LS_MQTT_REQUEST_RESPONSE, "(static) request response client creation failed - invalid client options");
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        return NULL;
    }

    struct aws_mqtt_request_response_client *rr_client =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_request_response_client));

    rr_client->allocator = allocator;
    rr_client->config = *options;
    rr_client->loop = loop;
    rr_client->state = AWS_RRCS_UNINITIALIZED;

    aws_hash_table_init(
        &rr_client->operations,
        allocator,
        MQTT_RR_CLIENT_OPERATION_TABLE_DEFAULT_SIZE,
        aws_mqtt_hash_uint64_t,
        aws_mqtt_compare_uint64_t_eq,
        NULL,
        NULL);

    aws_priority_queue_init_dynamic(
        &rr_client->operations_by_timeout,
        allocator,
        100,
        sizeof(struct aws_mqtt_rr_client_operation *),
        s_compare_rr_operation_timeouts);

    aws_hash_table_init(
        &rr_client->operation_lists_by_subscription_filter,
        allocator,
        MQTT_RR_CLIENT_OPERATION_TABLE_DEFAULT_SIZE,
        aws_hash_byte_cursor_ptr,
        aws_mqtt_byte_cursor_hash_equality,
        NULL,
        s_aws_rr_operation_list_topic_filter_entry_hash_element_destroy);

    aws_linked_list_init(&rr_client->operation_queue);

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

    aws_atomic_store_int(&rr_client->next_id, 1);

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

static void s_check_for_operation_timeouts(struct aws_mqtt_request_response_client *client) {
    uint64_t now = 0;
    aws_high_res_clock_get_ticks(&now);

    struct aws_priority_queue *timeout_queue = &client->operations_by_timeout;

    bool done = aws_priority_queue_size(timeout_queue) == 0;
    while (!done) {
        struct aws_mqtt_rr_client_operation **next_operation_by_timeout_ptr = NULL;
        aws_priority_queue_top(timeout_queue, (void **)&next_operation_by_timeout_ptr);
        AWS_FATAL_ASSERT(next_operation_by_timeout_ptr != NULL);
        struct aws_mqtt_rr_client_operation *next_operation_by_timeout = *next_operation_by_timeout_ptr;
        AWS_FATAL_ASSERT(next_operation_by_timeout != NULL);

        // If the current top of the heap hasn't timed out than nothing has
        if (next_operation_by_timeout->timeout_timepoint_ns > now) {
            break;
        }

        /* Ack timeout for this operation has been reached */
        aws_priority_queue_pop(timeout_queue, &next_operation_by_timeout);

        s_request_response_fail_operation(next_operation_by_timeout, AWS_ERROR_MQTT_REQUEST_RESPONSE_TIMEOUT);

        done = aws_priority_queue_size(timeout_queue) == 0;
    }
}

static uint64_t s_mqtt_request_response_client_get_next_service_time(struct aws_mqtt_request_response_client *client) {
    if (aws_priority_queue_size(&client->operations_by_timeout) > 0) {
        struct aws_mqtt_rr_client_operation **next_operation_by_timeout_ptr = NULL;
        aws_priority_queue_top(&client->operations_by_timeout, (void **)&next_operation_by_timeout_ptr);
        AWS_FATAL_ASSERT(next_operation_by_timeout_ptr != NULL);
        struct aws_mqtt_rr_client_operation *next_operation_by_timeout = *next_operation_by_timeout_ptr;
        AWS_FATAL_ASSERT(next_operation_by_timeout != NULL);

        return next_operation_by_timeout->timeout_timepoint_ns;
    }

    return UINT64_MAX;
}

static struct aws_byte_cursor s_aws_mqtt_rr_operation_get_subscription_topic_filter(
    struct aws_mqtt_rr_client_operation *operation) {
    if (operation->type == AWS_MRROT_REQUEST) {
        return operation->storage.request_storage.options.subscription_topic_filter;
    } else {
        return operation->storage.streaming_storage.options.topic_filter;
    }
}

/* TODO: add aws-c-common API? */
static bool s_is_operation_in_list(const struct aws_mqtt_rr_client_operation *operation) {
    return aws_linked_list_node_prev_is_valid(&operation->node) && aws_linked_list_node_next_is_valid(&operation->node);
}

static int s_add_operation_to_subscription_topic_filter_table(
    struct aws_mqtt_request_response_client *client,
    struct aws_mqtt_rr_client_operation *operation) {

    struct aws_byte_cursor topic_filter_cursor = s_aws_mqtt_rr_operation_get_subscription_topic_filter(operation);

    struct aws_hash_element *element = NULL;
    if (aws_hash_table_find(&client->operation_lists_by_subscription_filter, &topic_filter_cursor, &element)) {
        return aws_raise_error(AWS_ERROR_MQTT_REQUEST_RESPONSE_INTERNAL_ERROR);
    }

    struct aws_rr_operation_list_topic_filter_entry *entry = NULL;
    if (element == NULL) {
        entry = s_aws_rr_operation_list_topic_filter_entry_new(client->allocator, topic_filter_cursor);
        aws_hash_table_put(&client->operation_lists_by_subscription_filter, &entry->topic_filter_cursor, entry, NULL);
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT_REQUEST_RESPONSE,
            "id=%p: request-response client adding topic filter '" PRInSTR "' to subscriptions table",
            (void *)client,
            AWS_BYTE_CURSOR_PRI(topic_filter_cursor));
    } else {
        entry = element->value;
    }

    AWS_FATAL_ASSERT(entry != NULL);

    if (s_is_operation_in_list(operation)) {
        aws_linked_list_remove(&operation->node);
    }

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_REQUEST_RESPONSE,
        "id=%p: request-response client adding operation %" PRIu64 " to subscription table with topic_filter '" PRInSTR
        "'",
        (void *)client,
        operation->id,
        AWS_BYTE_CURSOR_PRI(topic_filter_cursor));

    aws_linked_list_push_back(&entry->operations, &operation->node);

    return AWS_OP_SUCCESS;
}

static void s_make_mqtt_request(
    struct aws_mqtt_request_response_client *client,
    struct aws_mqtt_rr_client_operation *operation) {
    (void)client;

    AWS_FATAL_ASSERT(operation->type == AWS_MRROT_REQUEST);

    // TODO: NYI
}

static void s_handle_operation_subscribe_result(
    struct aws_mqtt_request_response_client *client,
    struct aws_mqtt_rr_client_operation *operation,
    enum aws_acquire_subscription_result_type subscribe_result) {
    if (subscribe_result == AASRT_FAILURE || subscribe_result == AASRT_NO_CAPACITY) {
        int error_code = (subscribe_result == AASRT_NO_CAPACITY)
                             ? AWS_ERROR_MQTT_REQUEST_RESPONSE_NO_SUBSCRIPTION_CAPACITY
                             : AWS_ERROR_MQTT_REQUEST_RESPONSE_SUBSCRIBE_FAILURE;
        s_request_response_fail_operation(operation, error_code);
        return;
    }

    if (s_add_operation_to_subscription_topic_filter_table(client, operation)) {
        s_request_response_fail_operation(operation, AWS_ERROR_MQTT_REQUEST_RESPONSE_INTERNAL_ERROR);
        return;
    }

    if (subscribe_result == AASRT_SUBSCRIBING) {
        s_change_operation_state(operation, AWS_MRROS_PENDING_SUBSCRIPTION);
        return;
    }

    if (operation->type == AWS_MRROT_STREAMING) {
        s_change_operation_state(operation, AWS_MRROS_SUBSCRIBED);
    } else {
        s_make_mqtt_request(client, operation);
    }
}

static enum aws_rr_subscription_type s_rr_operation_type_to_subscription_type(
    enum aws_mqtt_request_response_operation_type type) {
    if (type == AWS_MRROT_REQUEST) {
        return ARRST_REQUEST_RESPONSE;
    }

    return ARRST_EVENT_STREAM;
}

static void s_process_queued_operations(struct aws_mqtt_request_response_client *client) {
    while (!aws_linked_list_empty(&client->operation_queue)) {
        struct aws_linked_list_node *head = aws_linked_list_front(&client->operation_queue);
        struct aws_mqtt_rr_client_operation *head_operation =
            AWS_CONTAINER_OF(head, struct aws_mqtt_rr_client_operation, node);

        struct aws_rr_acquire_subscription_options subscribe_options = {
            .topic_filter = s_aws_mqtt_rr_operation_get_subscription_topic_filter(head_operation),
            .operation_id = head_operation->id,
            .type = s_rr_operation_type_to_subscription_type(head_operation->type),
        };

        enum aws_acquire_subscription_result_type subscribe_result =
            aws_rr_subscription_manager_acquire_subscription(&client->subscription_manager, &subscribe_options);

        AWS_LOGF_DEBUG(
            AWS_LS_MQTT_REQUEST_RESPONSE,
            "id=%p: request-response client intake, queued operation %" PRIu64
            " yielded acquire subscription result: %s",
            (void *)client,
            head_operation->id,
            s_aws_acquire_subscription_result_type(subscribe_result));

        if (subscribe_result == AASRT_BLOCKED) {
            break;
        }

        aws_linked_list_pop_front(&client->operation_queue);
        s_handle_operation_subscribe_result(client, head_operation, subscribe_result);
    }
}

static void s_mqtt_request_response_service_task_fn(
    struct aws_task *task,
    void *arg,
    enum aws_task_status task_status) {
    (void)task;

    if (task_status == AWS_TASK_STATUS_CANCELED) {
        return;
    }

    struct aws_mqtt_request_response_client *client = arg;
    client->scheduled_service_timepoint_ns = 0;

    if (client->state == AWS_RRCS_ACTIVE) {

        // timeouts
        s_check_for_operation_timeouts(client);

        // operation queue
        s_process_queued_operations(client);

        // schedule next service
        client->scheduled_service_timepoint_ns = s_mqtt_request_response_client_get_next_service_time(client);
        aws_event_loop_schedule_task_future(
            client->loop, &client->service_task, client->scheduled_service_timepoint_ns);

        AWS_LOGF_DEBUG(
            AWS_LS_MQTT_REQUEST_RESPONSE,
            "id=%p: request-response client service, next timepoint: %" PRIu64,
            (void *)client,
            client->scheduled_service_timepoint_ns);
    }
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

        aws_task_init(&client->service_task, s_mqtt_request_response_service_task_fn, client, "mqtt_rr_client_service");

        aws_event_loop_schedule_task_future(client->loop, &client->service_task, UINT64_MAX);
        client->scheduled_service_timepoint_ns = UINT64_MAX;
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

    if (rr_client == NULL) {
        return NULL;
    }

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

    if (rr_client == NULL) {
        return NULL;
    }

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

struct aws_mqtt_request_response_client *s_aws_mqtt_request_response_client_acquire_internal(
    struct aws_mqtt_request_response_client *client) {
    if (client != NULL) {
        aws_ref_count_acquire(&client->internal_ref_count);
    }

    return client;
}

struct aws_mqtt_request_response_client *s_aws_mqtt_request_response_client_release_internal(
    struct aws_mqtt_request_response_client *client) {
    if (client != NULL) {
        aws_ref_count_release(&client->internal_ref_count);
    }

    return NULL;
}

/////////////////////////////////////////////////

static bool s_are_request_operation_options_valid(
    const struct aws_mqtt_request_response_client *client,
    const struct aws_mqtt_request_operation_options *request_options) {
    if (request_options == NULL) {
        AWS_LOGF_ERROR(AWS_LS_MQTT_REQUEST_RESPONSE, "(%p) rr client - NULL request options", (void *)client);
        return false;
    }

    if (request_options->response_path_count == 0) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT_REQUEST_RESPONSE,
            "(%p) rr client request options - no response paths supplied",
            (void *)client);
        return false;
    }

    for (size_t i = 0; i < request_options->response_path_count; ++i) {
        const struct aws_mqtt_request_operation_response_path *path = &request_options->response_paths[i];
        if (!aws_mqtt_is_valid_topic(&path->topic)) {
            AWS_LOGF_ERROR(
                AWS_LS_MQTT_REQUEST_RESPONSE,
                "(%p) rr client request options - " PRInSTR " is not a valid topic",
                (void *)client,
                AWS_BYTE_CURSOR_PRI(path->topic));
            return false;
        }

        if (path->correlation_token_json_path.len == 0) {
            AWS_LOGF_ERROR(
                AWS_LS_MQTT_REQUEST_RESPONSE,
                "(%p) rr client request options - empty correlation token json path",
                (void *)client);
            return false;
        }
    }

    if (request_options->correlation_token.len == 0) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT_REQUEST_RESPONSE, "(%p) rr client request options - empty correlation token", (void *)client);
        return false;
    }

    if (!aws_mqtt_is_valid_topic(&request_options->publish_topic)) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT_REQUEST_RESPONSE,
            "(%p) rr client request options - " PRInSTR " is not a valid topic",
            (void *)client,
            AWS_BYTE_CURSOR_PRI(request_options->publish_topic));
        return false;
    }

    if (request_options->serialized_request.len == 0) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT_REQUEST_RESPONSE, "(%p) rr client request options - empty request payload", (void *)client);
        return false;
    }

    return true;
}

static bool s_are_streaming_operation_options_valid(
    struct aws_mqtt_request_response_client *client,
    const struct aws_mqtt_streaming_operation_options *streaming_options) {
    if (streaming_options == NULL) {
        AWS_LOGF_ERROR(AWS_LS_MQTT_REQUEST_RESPONSE, "(%p) rr client - NULL streaming options", (void *)client);
        return false;
    }

    if (!aws_mqtt_is_valid_topic_filter(&streaming_options->topic_filter)) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT_REQUEST_RESPONSE,
            "(%p) rr client streaming options - " PRInSTR " is not a valid topic filter",
            (void *)client,
            AWS_BYTE_CURSOR_PRI(streaming_options->topic_filter));
        return false;
    }

    return true;
}

static uint64_t s_aws_mqtt_request_response_client_allocate_operation_id(
    struct aws_mqtt_request_response_client *client) {
    return aws_atomic_fetch_add(&client->next_id, 1);
}

static void s_mqtt_rr_client_submit_operation(struct aws_task *task, void *arg, enum aws_task_status status) {
    (void)task;

    struct aws_mqtt_rr_client_operation *operation = arg;
    struct aws_mqtt_request_response_client *client = operation->client_internal_ref;

    if (status == AWS_TASK_STATUS_CANCELED) {
        goto done;
    }

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT_REQUEST_RESPONSE,
        "id=%p: request-response client, queuing operation %" PRIu64,
        (void *)client,
        operation->id);

    // add appropriate client table entries
    aws_hash_table_put(&client->operations, &operation->id, operation, NULL);

    // NYI other tables

    // add to timeout priority queue
    aws_priority_queue_push_ref(&client->operations_by_timeout, (void *)&operation, &operation->priority_queue_node);

    // enqueue
    aws_linked_list_push_back(&operation->client_internal_ref->operation_queue, &operation->node);

    s_change_operation_state(operation, AWS_MRROS_QUEUED);

    s_mqtt_request_response_client_wake_service(operation->client_internal_ref);

done:

    /*
     * We hold a second reference to the operation during submission.  This ensures that even if a streaming operation
     * is immediately dec-refed by the creator (before submission completes), the operation will not get destroyed.
     *
     * It is now safe and correct to release that reference.
     *
     * After this, streaming operation lifetime is completely user-driven, while request operation lifetime is
     * completely client-internal.
     */
    aws_mqtt_rr_client_operation_release(operation);
}

static void s_aws_mqtt_streaming_operation_storage_clean_up(struct aws_mqtt_streaming_operation_storage *storage) {
    aws_byte_buf_clean_up(&storage->operation_data);
}

static void s_aws_mqtt_request_operation_storage_clean_up(struct aws_mqtt_request_operation_storage *storage) {
    aws_array_list_clean_up(&storage->operation_response_paths);
    aws_byte_buf_clean_up(&storage->operation_data);
}

static void s_mqtt_rr_client_destroy_operation(struct aws_task *task, void *arg, enum aws_task_status status) {
    (void)task;
    (void)status;

    struct aws_mqtt_rr_client_operation *operation = arg;
    struct aws_mqtt_request_response_client *client = operation->client_internal_ref;

    aws_hash_table_remove(&client->operations, &operation->id, NULL, NULL);
    s_remove_operation_from_timeout_queue(operation);

    if (s_is_operation_in_list(operation)) {
        aws_linked_list_remove(&operation->node);
    }

    if (client->state != AWS_RRCS_SHUTTING_DOWN) {
        struct aws_rr_release_subscription_options release_options = {
            .topic_filter = s_aws_mqtt_rr_operation_get_subscription_topic_filter(operation),
            .operation_id = operation->id,
        };
        aws_rr_subscription_manager_release_subscription(&client->subscription_manager, &release_options);
    }

    /*
     NYI:

     Remove from correlation token table

     */

    s_aws_mqtt_request_response_client_release_internal(operation->client_internal_ref);

    if (operation->type == AWS_MRROT_STREAMING) {
        s_aws_mqtt_streaming_operation_storage_clean_up(&operation->storage.streaming_storage);
    } else {
        s_aws_mqtt_request_operation_storage_clean_up(&operation->storage.request_storage);
    }

    aws_mqtt_streaming_operation_terminated_fn *terminated_callback = NULL;
    void *terminated_user_data = NULL;
    if (operation->type == AWS_MRROT_STREAMING) {
        terminated_callback = operation->storage.streaming_storage.options.terminated_callback;
        terminated_user_data = operation->storage.streaming_storage.options.user_data;
    }

    aws_mem_release(operation->allocator, operation);

    if (terminated_callback != NULL) {
        (*terminated_callback)(terminated_user_data);
    }
}

static void s_on_mqtt_rr_client_operation_zero_ref_count(void *context) {
    struct aws_mqtt_rr_client_operation *operation = context;

    aws_event_loop_schedule_task_now(operation->client_internal_ref->loop, &operation->destroy_task);
}

static void s_aws_mqtt_rr_client_operation_init_shared(
    struct aws_mqtt_rr_client_operation *operation,
    struct aws_mqtt_request_response_client *client) {
    operation->allocator = client->allocator;
    aws_ref_count_init(&operation->ref_count, operation, s_on_mqtt_rr_client_operation_zero_ref_count);

    /*
     * We hold a second reference to the operation during submission.  This ensures that even if a streaming operation
     * is immediately dec-refed by the creator (before submission runs), the operation will not get destroyed.
     */
    aws_mqtt_rr_client_operation_acquire(operation);

    operation->client_internal_ref = s_aws_mqtt_request_response_client_acquire_internal(client);
    operation->id = s_aws_mqtt_request_response_client_allocate_operation_id(client);
    s_change_operation_state(operation, AWS_MRROS_NONE);

    aws_task_init(
        &operation->submit_task,
        s_mqtt_rr_client_submit_operation,
        operation,
        "MQTTRequestResponseClientOperationSubmit");
    aws_task_init(
        &operation->destroy_task,
        s_mqtt_rr_client_destroy_operation,
        operation,
        "MQTTRequestResponseClientOperationDestroy");
}

void s_aws_mqtt_request_operation_storage_init_from_options(
    struct aws_mqtt_request_operation_storage *storage,
    struct aws_allocator *allocator,
    const struct aws_mqtt_request_operation_options *request_options) {

    size_t bytes_needed = 0;
    bytes_needed += request_options->publish_topic.len;
    bytes_needed += request_options->serialized_request.len;
    bytes_needed += request_options->correlation_token.len;
    bytes_needed += request_options->subscription_topic_filter.len;

    for (size_t i = 0; i < request_options->response_path_count; ++i) {
        const struct aws_mqtt_request_operation_response_path *response_path = &request_options->response_paths[i];

        bytes_needed += response_path->topic.len;
        bytes_needed += response_path->correlation_token_json_path.len;
    }

    storage->options = *request_options;

    aws_byte_buf_init(&storage->operation_data, allocator, bytes_needed);
    aws_array_list_init_dynamic(
        &storage->operation_response_paths,
        allocator,
        request_options->response_path_count,
        sizeof(struct aws_mqtt_request_operation_response_path));

    AWS_FATAL_ASSERT(
        aws_byte_buf_append_and_update(&storage->operation_data, &storage->options.publish_topic) == AWS_OP_SUCCESS);
    AWS_FATAL_ASSERT(
        aws_byte_buf_append_and_update(&storage->operation_data, &storage->options.serialized_request) ==
        AWS_OP_SUCCESS);
    AWS_FATAL_ASSERT(
        aws_byte_buf_append_and_update(&storage->operation_data, &storage->options.correlation_token) ==
        AWS_OP_SUCCESS);
    AWS_FATAL_ASSERT(
        aws_byte_buf_append_and_update(&storage->operation_data, &storage->options.subscription_topic_filter) ==
        AWS_OP_SUCCESS);

    for (size_t i = 0; i < request_options->response_path_count; ++i) {
        struct aws_mqtt_request_operation_response_path response_path = request_options->response_paths[i];

        AWS_FATAL_ASSERT(
            aws_byte_buf_append_and_update(&storage->operation_data, &response_path.topic) == AWS_OP_SUCCESS);
        AWS_FATAL_ASSERT(
            aws_byte_buf_append_and_update(&storage->operation_data, &response_path.correlation_token_json_path) ==
            AWS_OP_SUCCESS);

        aws_array_list_push_back(&storage->operation_response_paths, &response_path);
    }

    storage->options.response_paths = storage->operation_response_paths.data;
}

static void s_log_request_response_operation(
    struct aws_mqtt_rr_client_operation *operation,
    struct aws_mqtt_request_response_client *client) {
    struct aws_logger *log_handle = aws_logger_get_conditional(AWS_LS_MQTT_REQUEST_RESPONSE, AWS_LL_DEBUG);
    if (log_handle == NULL) {
        return;
    }

    struct aws_mqtt_request_operation_options *options = &operation->storage.request_storage.options;

    AWS_LOGUF(
        log_handle,
        AWS_LL_DEBUG,
        AWS_LS_MQTT_REQUEST_RESPONSE,
        "id=%p: request-response client operation %" PRIu64 " - subscription topic filter: '" PRInSTR "'",
        (void *)client,
        operation->id,
        AWS_BYTE_CURSOR_PRI(options->subscription_topic_filter));

    AWS_LOGUF(
        log_handle,
        AWS_LL_DEBUG,
        AWS_LS_MQTT_REQUEST_RESPONSE,
        "id=%p: request-response client operation %" PRIu64 " - correlation token: '" PRInSTR "'",
        (void *)client,
        operation->id,
        AWS_BYTE_CURSOR_PRI(options->correlation_token));

    AWS_LOGUF(
        log_handle,
        AWS_LL_DEBUG,
        AWS_LS_MQTT_REQUEST_RESPONSE,
        "id=%p: request-response client operation %" PRIu64 " - publish topic: '" PRInSTR "'",
        (void *)client,
        operation->id,
        AWS_BYTE_CURSOR_PRI(options->publish_topic));

    AWS_LOGUF(
        log_handle,
        AWS_LL_DEBUG,
        AWS_LS_MQTT_REQUEST_RESPONSE,
        "id=%p: request-response client operation %" PRIu64 " - %zu response paths:",
        (void *)client,
        operation->id,
        options->response_path_count);

    for (size_t i = 0; i < options->response_path_count; ++i) {
        struct aws_mqtt_request_operation_response_path *response_path = &options->response_paths[i];

        AWS_LOGUF(
            log_handle,
            AWS_LL_DEBUG,
            AWS_LS_MQTT_REQUEST_RESPONSE,
            "id=%p: request-response client operation %" PRIu64 " - response path %zu topic '" PRInSTR "'",
            (void *)client,
            operation->id,
            i,
            AWS_BYTE_CURSOR_PRI(response_path->topic));

        AWS_LOGUF(
            log_handle,
            AWS_LL_DEBUG,
            AWS_LS_MQTT_REQUEST_RESPONSE,
            "id=%p: request-response client operation %" PRIu64 " - response path %zu correlation token path '" PRInSTR
            "'",
            (void *)client,
            operation->id,
            i,
            AWS_BYTE_CURSOR_PRI(response_path->correlation_token_json_path));
    }
}

int aws_mqtt_request_response_client_submit_request(
    struct aws_mqtt_request_response_client *client,
    const struct aws_mqtt_request_operation_options *request_options) {

    if (client == NULL) {
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    if (!s_are_request_operation_options_valid(client, request_options)) {
        /* all failure cases have logged the problem already */
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    uint64_t now = 0;
    if (aws_high_res_clock_get_ticks(&now)) {
        return aws_raise_error(AWS_ERROR_CLOCK_FAILURE);
    }

    struct aws_allocator *allocator = client->allocator;
    struct aws_mqtt_rr_client_operation *operation =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_rr_client_operation));
    operation->allocator = allocator;
    operation->type = AWS_MRROT_REQUEST;
    operation->timeout_timepoint_ns =
        now +
        aws_timestamp_convert(client->config.operation_timeout_seconds, AWS_TIMESTAMP_SECS, AWS_TIMESTAMP_NANOS, NULL);

    s_aws_mqtt_request_operation_storage_init_from_options(
        &operation->storage.request_storage, allocator, request_options);
    s_aws_mqtt_rr_client_operation_init_shared(operation, client);

    AWS_LOGF_INFO(
        AWS_LS_MQTT_REQUEST_RESPONSE,
        "id=%p: request-response client - submitting request-response operation with id %" PRIu64,
        (void *)client,
        operation->id);

    s_log_request_response_operation(operation, client);

    aws_event_loop_schedule_task_now(client->loop, &operation->submit_task);

    return AWS_OP_SUCCESS;
}

void s_aws_mqtt_streaming_operation_storage_init_from_options(
    struct aws_mqtt_streaming_operation_storage *storage,
    struct aws_allocator *allocator,
    const struct aws_mqtt_streaming_operation_options *streaming_options) {
    size_t bytes_needed = streaming_options->topic_filter.len;

    storage->options = *streaming_options;
    aws_byte_buf_init(&storage->operation_data, allocator, bytes_needed);

    AWS_FATAL_ASSERT(
        aws_byte_buf_append_and_update(&storage->operation_data, &storage->options.topic_filter) == AWS_OP_SUCCESS);
}

static void s_log_streaming_operation(
    struct aws_mqtt_rr_client_operation *operation,
    struct aws_mqtt_request_response_client *client) {
    struct aws_logger *log_handle = aws_logger_get_conditional(AWS_LS_MQTT_REQUEST_RESPONSE, AWS_LL_DEBUG);
    if (log_handle == NULL) {
        return;
    }

    struct aws_mqtt_streaming_operation_options *options = &operation->storage.streaming_storage.options;

    AWS_LOGUF(
        log_handle,
        AWS_LL_DEBUG,
        AWS_LS_MQTT_REQUEST_RESPONSE,
        "id=%p: request-response client streaming operation %" PRIu64 ": topic filter: '" PRInSTR "'",
        (void *)client,
        operation->id,
        AWS_BYTE_CURSOR_PRI(options->topic_filter));
}

struct aws_mqtt_rr_client_operation *aws_mqtt_request_response_client_create_streaming_operation(
    struct aws_mqtt_request_response_client *client,
    const struct aws_mqtt_streaming_operation_options *streaming_options) {

    if (client == NULL) {
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        return NULL;
    }

    if (!s_are_streaming_operation_options_valid(client, streaming_options)) {
        /* all failure cases have logged the problem already */
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        return NULL;
    }

    struct aws_allocator *allocator = client->allocator;
    struct aws_mqtt_rr_client_operation *operation =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_rr_client_operation));
    operation->allocator = allocator;
    operation->type = AWS_MRROT_STREAMING;
    operation->timeout_timepoint_ns = UINT64_MAX;

    s_aws_mqtt_streaming_operation_storage_init_from_options(
        &operation->storage.streaming_storage, allocator, streaming_options);
    s_aws_mqtt_rr_client_operation_init_shared(operation, client);

    AWS_LOGF_INFO(
        AWS_LS_MQTT_REQUEST_RESPONSE,
        "id=%p: request-response client - submitting streaming operation with id %" PRIu64,
        (void *)client,
        operation->id);

    s_log_streaming_operation(operation, client);

    aws_event_loop_schedule_task_now(client->loop, &operation->submit_task);

    return operation;
}

struct aws_mqtt_rr_client_operation *aws_mqtt_rr_client_operation_acquire(
    struct aws_mqtt_rr_client_operation *operation) {
    if (operation != NULL) {
        aws_ref_count_acquire(&operation->ref_count);
    }

    return operation;
}

struct aws_mqtt_rr_client_operation *aws_mqtt_rr_client_operation_release(
    struct aws_mqtt_rr_client_operation *operation) {
    if (operation != NULL) {
        aws_ref_count_release(&operation->ref_count);
    }

    return NULL;
}
