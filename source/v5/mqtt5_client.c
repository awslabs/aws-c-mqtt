/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/v5/mqtt5_client.h>

#include <aws/common/clock.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/mqtt/private/v5/mqtt5_client_impl.h>
#include <aws/mqtt/v5/mqtt5_client_config.h>

static int s_aws_mqtt5_client_change_desired_state(
    struct aws_mqtt5_client *client,
    enum aws_mqtt5_client_state desired_state);

static uint64_t s_hash_uint16_t(const void *item) {
    return *(uint16_t *)item;
}

static bool s_uint16_t_eq(const void *a, const void *b) {
    return *(uint16_t *)a == *(uint16_t *)b;
}

static void s_mqtt5_client_final_destroy(struct aws_mqtt5_client *client) {
    if (client == NULL) {
        return;
    }

    aws_mqtt_topic_tree_clean_up(&client->subscriptions);

    // TODO: outstanding requests table entries (should be empty)
    aws_hash_table_clean_up(&client->unacked_operations_table);

    // TODO: queued operations list elements (can be non-empty)

    // TODO: current operation (should never actually be NULL)

    // TODO: write completion list elements (should be empty)

    aws_mqtt5_client_config_destroy((struct aws_mqtt5_client_config *)client->config);

    aws_mem_release(client->allocator, client);
}

static void s_on_mqtt5_client_zero_ref_count(void *user_data) {
    struct aws_mqtt5_client *client = user_data;

    s_aws_mqtt5_client_change_desired_state(client, AWS_MCS_TERMINATED);
}

/*
 * next_service_time == 0 means to not service the client, i.e. a state that only cares about async events
 *
 * This includes connecting, disconnecting.  Terminated is also included, but it's a state that only exists
 * instantaneously before final destruction.
 */
static uint64_t s_compute_next_service_time_client_stopped(struct aws_mqtt5_client *client, uint64_t now) {
    /* have we been told to connect or terminate? */
    if (client->desired_state != AWS_MCS_STOPPED) {
        return now;
    }

    return 0;
}

static uint64_t s_compute_next_service_time_client_connecting(struct aws_mqtt5_client *client, uint64_t now) {
    (void)client;
    (void)now;

    return 0;
}

static uint64_t s_compute_next_service_time_client_mqtt_connect(struct aws_mqtt5_client *client, uint64_t now) {
    /* This state is interruptible by a stop/terminate */
    if (client->desired_state != AWS_MCS_CONNECTED) {
        return now;
    }

    return client->next_mqtt_connect_packet_timeout_time;
}

static uint64_t s_compute_next_service_time_client_connected(struct aws_mqtt5_client *client, uint64_t now) {

    /* ping and ping timeout */
    uint64_t next_service_time = client->next_ping_time;
    if (client->next_ping_timeout_time != 0) {
        next_service_time = aws_min_u64(next_service_time, client->next_ping_timeout_time);
    }

    if (client->desired_state != AWS_MCS_CONNECTED) {
        next_service_time = now;
    }

    /* TODO: apply flow control */
    if (!aws_linked_list_empty(&client->queued_operations) || client->current_operation != NULL) {
        next_service_time = now;
    }

    /* reset reconnect delay interval */
    next_service_time = aws_min_u64(next_service_time, client->next_reconnect_delay_interval_reset_time);

    return next_service_time;
}

static uint64_t s_compute_next_service_time_client_clean_disconnect(struct aws_mqtt5_client *client, uint64_t now) {
    uint64_t next_service_time = 0;

    /* TODO: apply flow control */
    if (!aws_linked_list_empty(&client->queued_operations) || client->current_operation != NULL) {
        next_service_time = now;
    }

    return next_service_time;
}

static uint64_t s_compute_next_service_time_client_channel_shutdown(struct aws_mqtt5_client *client, uint64_t now) {
    (void)client;
    (void)now;

    return 0;
}

static uint64_t s_compute_next_service_time_client_pending_reconnect(struct aws_mqtt5_client *client, uint64_t now) {
    (void)now;

    return client->next_reconnect_time;
}

static uint64_t s_compute_next_service_time_client_terminated(struct aws_mqtt5_client *client, uint64_t now) {
    (void)client;
    (void)now;

    return 0;
}

static uint64_t s_compute_next_service_time_by_current_state(struct aws_mqtt5_client *client, uint64_t now) {
    switch (client->current_state) {
        case AWS_MCS_STOPPED:
            return s_compute_next_service_time_client_stopped(client, now);
        case AWS_MCS_CONNECTING:
            return s_compute_next_service_time_client_connecting(client, now);
        case AWS_MCS_MQTT_CONNECT:
            return s_compute_next_service_time_client_mqtt_connect(client, now);
        case AWS_MCS_CONNECTED:
            return s_compute_next_service_time_client_connected(client, now);
        case AWS_MCS_CLEAN_DISCONNECT:
            return s_compute_next_service_time_client_clean_disconnect(client, now);
        case AWS_MCS_CHANNEL_SHUTDOWN:
            return s_compute_next_service_time_client_channel_shutdown(client, now);
        case AWS_MCS_PENDING_RECONNECT:
            return s_compute_next_service_time_client_pending_reconnect(client, now);
        case AWS_MCS_TERMINATED:
            return s_compute_next_service_time_client_terminated(client, now);
    }

    return 0;
}

static void s_reevaluate_service_task(struct aws_mqtt5_client *client) {
    (void)client;

    uint64_t now = 0;
    if (aws_high_res_clock_get_ticks(&now)) {
        return;
    }

    uint64_t next_service_time = s_compute_next_service_time_by_current_state(client, now);

    /*
     * This catches both the case when there's an existing service schedule and we either want to not
     * perform it (next_service_time == 0) or need to run service earlier than the current scheduled time.
     */
    if (next_service_time < client->next_service_task_run_time) {
        aws_event_loop_cancel_task(client->loop, &client->service_task);
    }

    if (next_service_time > 0 && next_service_time < client->next_service_task_run_time) {
        aws_event_loop_schedule_task_future(client->loop, &client->service_task, next_service_time);
    }

    client->next_service_task_run_time = next_service_time;
}

static void s_change_current_state_to_stopped(struct aws_mqtt5_client *client) {
    (void)client;
    AWS_ASSERT(
        client->current_state == AWS_MCS_DISCONNECTING || client->current_state == AWS_MCS_PENDING_RECONNECT ||
        client->current_state == AWS_MCS_CONNECTING);

    /* TODO: Emit stopped lifecycle event */
}

static void s_change_current_state_to_connecting(struct aws_mqtt5_client *client) {
    (void)client;
    AWS_ASSERT(client->current_state == AWS_MCS_STOPPED || client->current_state == AWS_MCS_PENDING_RECONNECT);

    /* TODO: Kick off channel setup, sync failure => LifecycleEvent(ConnFailure), EnterState(PENDING_RECONNECT) */
}

static void s_change_current_state_to_mqtt_connect(struct aws_mqtt5_client *client) {
    (void)client;
    AWS_ASSERT(client->current_state == AWS_MCS_CONNECTING);

    /* TODO: Send CONNECT packet, sync failure => LifecycleEvent(ConnFailure), EnterState(CHANNEL_SHUTDOWN) */

    /* TODO: set mqtt CONNACK timeout */
}

static void s_reset_ping(struct aws_mqtt5_client *client) {
    uint64_t now = 0;
    aws_high_res_clock_get_ticks(&now);

    uint64_t keep_alive_interval_nanos =
        aws_timestamp_convert(client->config->keep_alive_interval_ms, AWS_TIMESTAMP_MILLIS, AWS_TIMESTAMP_NANOS, NULL);
    client->next_ping_time = aws_add_u64_saturating(now, keep_alive_interval_nanos);

    uint64_t pint_timeout_nanos =
        aws_timestamp_convert(client->config->ping_timeout_ms, AWS_TIMESTAMP_MILLIS, AWS_TIMESTAMP_NANOS, NULL);
    client->next_ping_timeout_time = aws_add_u64_saturating(client->next_ping_timeout_time, pint_timeout_nanos);
}

static void s_reset_reconnection_delay_time(struct aws_mqtt5_client *client) {
    uint64_t now = 0;
    aws_high_res_clock_get_ticks(&now);

    uint64_t reset_reconnection_delay_time_nanos = aws_timestamp_convert(
        client->config->min_connected_time_to_reset_reconnect_delay_ms,
        AWS_TIMESTAMP_MILLIS,
        AWS_TIMESTAMP_NANOS,
        NULL);
    client->next_reconnect_delay_interval_reset_time = aws_add_u64_saturating(now, reset_reconnection_delay_time_nanos);
}

static void s_change_current_state_to_connected(struct aws_mqtt5_client *client) {
    AWS_ASSERT(client->current_state == AWS_MCS_MQTT_CONNECT);

    s_reset_ping(client);
    s_reset_reconnection_delay_time(client);
}

static void s_change_current_state_to_clean_disconnect(struct aws_mqtt5_client *client) {
    (void)client;
    AWS_ASSERT(client->current_state == AWS_MCS_MQTT_CONNECT || client->current_state == AWS_MCS_CONNECTED);

    /* TODO: Queue DISCONNECT packet as QoS 0, failure => EnterState(CHANNEL_SHUTDOWN) */
}

static void s_change_current_state_to_channel_shutdown(struct aws_mqtt5_client *client) {
    enum aws_mqtt5_client_state current_state = client->current_state;
    AWS_ASSERT(
        current_state == AWS_MCS_MQTT_CONNECT || current_state == AWS_MCS_CONNECTING ||
        current_state == AWS_MCS_CONNECTED || current_state == AWS_MCS_CLEAN_DISCONNECT);

    if (current_state == AWS_MCS_CONNECTED) {
        /* TODO: fail and release all QoS0 operations in queued_operations, current_operation, and
         * write_completion_operations */

        /* TODO: move all unacked (QoS1+) operations in {current_operation, unacked_operations} back into
         * queued_operations, preserving order */

        /* TODO: clear unacked_operations_table */
    }

    /* TODO: shutdown channel if not in shutdown */
}

static void s_change_current_state_to_pending_reconnect(struct aws_mqtt5_client *client) {
    AWS_ASSERT(client->current_state == AWS_MCS_MQTT_CONNECTING || client->current_state == AWS_MCS_CHANNEL_SHUTDOWN);

    uint64_t now = 0;
    aws_high_res_clock_get_ticks(&now);

    uint64_t reconnect_delay_nanos = aws_timestamp_convert(
        client->current_reconnect_delay_interval_ms, AWS_TIMESTAMP_MILLIS, AWS_TIMESTAMP_NANOS, NULL);
    client->next_reconnect_time = aws_add_u64_saturating(now, reconnect_delay_nanos);

    uint64_t double_reconnect_delay = aws_add_u64_saturating(
        client->current_reconnect_delay_interval_ms, client->current_reconnect_delay_interval_ms);
    client->current_reconnect_delay_interval_ms =
        aws_min_u64(double_reconnect_delay, client->config->max_reconnect_delay_ms);
}

static void s_change_current_state_to_terminated(struct aws_mqtt5_client *client) {
    s_mqtt5_client_final_destroy(client);
}

static void s_change_current_state(struct aws_mqtt5_client *client, enum aws_mqtt5_client_state next_state) {
    AWS_ASSERT(next_state != client->current_state);
    if (next_state == client->current_state) {
        return;
    }

    switch (next_state) {
        case AWS_MCS_STOPPED:
            s_change_current_state_to_stopped(client);
            break;
        case AWS_MCS_CONNECTING:
            s_change_current_state_to_connecting(client);
            break;
        case AWS_MCS_MQTT_CONNECT:
            s_change_current_state_to_mqtt_connect(client);
            break;
        case AWS_MCS_CONNECTED:
            s_change_current_state_to_connected(client);
            break;
        case AWS_MCS_CLEAN_DISCONNECT:
            s_change_current_state_to_clean_disconnect(client);
            break;
        case AWS_MCS_CHANNEL_SHUTDOWN:
            s_change_current_state_to_channel_shutdown(client);
            break;
        case AWS_MCS_PENDING_RECONNECT:
            s_change_current_state_to_pending_reconnect(client);
            break;
        case AWS_MCS_TERMINATED:
            s_change_current_state_to_terminated(client);
            break;
    }
}

static bool s_service_state_stopped(struct aws_mqtt5_client *client) {
    enum aws_mqtt5_client_state desired_state = client->desired_state;
    if (desired_state == AWS_MCS_CONNECTED) {
        s_change_current_state(client, AWS_MCS_CONNECTING);
    } else if (desired_state == AWS_MCS_TERMINATED) {
        s_change_current_state(client, AWS_MCS_TERMINATED);
        return true;
    }

    return false;
}

static void s_service_state_connecting(struct aws_mqtt5_client *client) {
    (void)client;
}

static void s_service_state_mqtt_connect(struct aws_mqtt5_client *client, uint64_t now) {
    enum aws_mqtt5_client_state desired_state = client->desired_state;
    if (desired_state != AWS_MCS_CONNECTED) {
        /* TODO: emit lifecycle event ConnFailure(user requested, no packet data) */

        /* TODO: init DISCONNECT packet */

        s_change_current_state(client, AWS_MCS_CLEAN_DISCONNECT);
        return;
    }

    if (now >= client->next_mqtt_connect_packet_timeout_time) {
        /* TODO: emit lifecycle event ConnFailure(timeout, no packet data) */

        /* TODO: init DISCONNECT packet */

        s_change_current_state(client, AWS_MCS_CLEAN_DISCONNECT);
        return;
    }
}

static void s_service_state_connected(struct aws_mqtt5_client *client, uint64_t now) {
    enum aws_mqtt5_client_state desired_state = client->desired_state;
    if (desired_state != AWS_MCS_CONNECTED) {
        /* TODO: emit lifecycle event ConnFailure(user requested, no packet data) */

        /* TODO: init DISCONNECT packet */

        s_change_current_state(client, AWS_MCS_CLEAN_DISCONNECT);
        return;
    }

    if (now >= client->next_ping_timeout_time) {
        /* TODO: emit lifecycle event ConnFailure(keep alive timeout, no packet data) */

        /* TODO: init DISCONNECT packet */

        s_change_current_state(client, AWS_MCS_CLEAN_DISCONNECT);
        return;
    }

    if (now >= client->next_ping_time) {
        /* TODO: Add ping operation at head of queued_operations */
        ;
    }

    if (now >= client->next_reconnect_delay_interval_reset_time) {
        client->current_reconnect_delay_interval_ms = client->config->min_reconnect_delay_ms;
    }

    /* TODO: flow control */
    if (client->current_operation != NULL || !aws_linked_list_empty(&client->queued_operations)) {
        /* TODO: process operations */
        ;
    }
}

static void s_service_state_clean_disconnect(struct aws_mqtt5_client *client) {
    /* TODO: flow control */
    if (client->current_operation != NULL || !aws_linked_list_empty(&client->queued_operations)) {
        /* TODO: process operations up to queued DISCONNECT */
        ;
    }
}

static void s_service_state_channel_shutdown(struct aws_mqtt5_client *client) {
    (void)client;
}

static void s_service_state_pending_reconnect(struct aws_mqtt5_client *client, uint64_t now) {
    if (client->desired_state != AWS_MCS_CONNECTED) {
        s_change_current_state(client, AWS_MCS_STOPPED);
        return;
    }

    if (now >= client->next_reconnect_time) {
        s_change_current_state(client, AWS_MCS_CONNECTING);
        return;
    }
}

static void s_mqtt5_service_task_fn(struct aws_task *task, void *arg, enum aws_task_status status) {
    (void)task;
    if (status != AWS_TASK_STATUS_RUN_READY) {
        return;
    }

    struct aws_mqtt5_client *client = arg;

    uint64_t now = 0;
    /* there's no reasonable recovery for a failure here */
    aws_high_res_clock_get_ticks(&now);

    bool terminated = false;
    switch (client->current_state) {
        case AWS_MCS_STOPPED:
            terminated = s_service_state_stopped(client);
            break;
        case AWS_MCS_CONNECTING:
            s_service_state_connecting(client);
            break;
        case AWS_MCS_MQTT_CONNECT:
            s_service_state_mqtt_connect(client, now);
            break;
        case AWS_MCS_CONNECTED:
            s_service_state_connected(client, now);
            break;
        case AWS_MCS_CLEAN_DISCONNECT:
            s_service_state_clean_disconnect(client);
            break;
        case AWS_MCS_CHANNEL_SHUTDOWN:
            s_service_state_channel_shutdown(client);
            break;
        case AWS_MCS_PENDING_RECONNECT:
            s_service_state_pending_reconnect(client, now);
            break;
        default:
            break;
    }

    /*
     * We can only enter the terminated state from stopped.  If we do so, the client memory is now freed and we
     * need to not access anything anymore.
     */
    if (terminated) {
        return;
    }

    /* we're not scheduled anymore, reschedule as needed */
    client->next_service_task_run_time = 0;
    s_reevaluate_service_task(client);
}

struct aws_mqtt5_client *aws_mqtt5_client_new(struct aws_allocator *allocator, struct aws_mqtt5_client_config *config) {
    AWS_FATAL_ASSERT(allocator != NULL);
    AWS_FATAL_ASSERT(config != NULL);

    if (aws_mqtt5_client_config_validate(config)) {
        return NULL;
    }

    struct aws_mqtt5_client *client = aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt5_client));
    if (client == NULL) {
        return NULL;
    }

    aws_task_init(&client->service_task, s_mqtt5_service_task_fn, client, "Mqtt5Service");

    client->allocator = allocator;

    aws_ref_count_init(&client->ref_count, client, s_on_mqtt5_client_zero_ref_count);

    client->config = aws_mqtt5_client_config_new_clone(allocator, config);
    if (client->config == NULL) {
        goto on_error;
    }

    /* all client activity will take place on this event loop, serializing things like reconnect, ping, etc... */
    client->loop = aws_event_loop_group_get_next_loop(config->bootstrap->event_loop_group);
    if (client->loop == NULL) {
        goto on_error;
    }

    client->desired_state = AWS_MCS_STOPPED;
    client->current_state = AWS_MCS_STOPPED;

    aws_atomic_init_int(&client->next_event_id, 0);
    client->next_mqtt_packet_id = 1;

    aws_linked_list_init(&client->queued_operations);
    aws_linked_list_init(&client->write_completion_operations);
    aws_linked_list_init(&client->unacked_operations);
    if (aws_hash_table_init(
            &client->unacked_operations_table,
            allocator,
            sizeof(struct aws_mqtt5_operation *),
            s_hash_uint16_t,
            s_uint16_t_eq,
            NULL,
            NULL)) {
        goto on_error;
    }

    if (aws_mqtt_topic_tree_init(&client->subscriptions, allocator)) {
        goto on_error;
    }

    aws_atomic_init_int(&client->statistics.total_pending_operations, 0);
    aws_atomic_init_int(&client->statistics.total_pending_payload_bytes, 0);
    aws_atomic_init_int(&client->statistics.incomplete_operations, 0);
    aws_atomic_init_int(&client->statistics.incomplete_payload_bytes, 0);

    client->current_reconnect_delay_interval_ms = config->min_reconnect_delay_ms;

    return client;

on_error:

    /* release isn't usable here since we may not even have an event loop */
    s_mqtt5_client_final_destroy(client);

    return NULL;
}

struct aws_mqtt5_client *aws_mqtt5_client_acquire(struct aws_mqtt5_client *client) {
    if (client != NULL) {
        aws_ref_count_acquire(&client->ref_count);
    }

    return client;
}

void aws_mqtt5_client_release(struct aws_mqtt5_client *client) {
    if (client != NULL) {
        aws_ref_count_release(&client->ref_count);
    }
}

struct aws_mqtt_change_desired_state_task {
    struct aws_task task;
    struct aws_allocator *allocator;
    struct aws_mqtt5_client *client;
    enum aws_mqtt5_client_state desired_state;
};

void s_change_state_task_fn(struct aws_task *task, void *arg, enum aws_task_status status) {
    (void)task;

    struct aws_mqtt_change_desired_state_task *change_state_task = arg;
    if (status != AWS_TASK_STATUS_RUN_READY) {
        goto done;
    }

    enum aws_mqtt5_client_state desired_state = change_state_task->desired_state;
    struct aws_mqtt5_client *client = change_state_task->client;

    if (client->desired_state != desired_state) {
        client->desired_state = desired_state;
        s_reevaluate_service_task(client);
    }

done:

    aws_mqtt5_client_release(change_state_task->client);

    aws_mem_release(change_state_task->allocator, change_state_task);
}

static struct aws_mqtt_change_desired_state_task *s_aws_mqtt_change_desired_state_task_new(
    struct aws_allocator *allocator,
    struct aws_mqtt5_client *client,
    enum aws_mqtt5_client_state desired_state) {

    struct aws_mqtt_change_desired_state_task *change_state_task =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_change_desired_state_task));
    if (change_state_task == NULL) {
        return NULL;
    }

    aws_task_init(&change_state_task->task, s_change_state_task_fn, (void *)change_state_task, "ChangeStateTask");

    change_state_task->allocator = client->allocator;
    change_state_task->client = aws_mqtt5_client_acquire(client);
    change_state_task->desired_state = desired_state;

    return change_state_task;
}

static bool s_is_valid_desired_state(enum aws_mqtt5_client_state desired_state) {
    switch (desired_state) {
        case AWS_MCS_STOPPED:
        case AWS_MCS_CONNECTED:
        case AWS_MCS_TERMINATED:
            return true;

        default:
            return false;
    }
}

static int s_aws_mqtt5_client_change_desired_state(
    struct aws_mqtt5_client *client,
    enum aws_mqtt5_client_state desired_state) {
    AWS_FATAL_ASSERT(client != NULL);
    AWS_FATAL_ASSERT(client->loop != NULL);

    if (!s_is_valid_desired_state(desired_state)) {
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    struct aws_mqtt_change_desired_state_task *task =
        s_aws_mqtt_change_desired_state_task_new(client->allocator, client, desired_state);
    if (task == NULL) {
        return AWS_OP_ERR;
    }

    aws_event_loop_schedule_task_now(client->loop, &task->task);

    return AWS_OP_SUCCESS;
}

int aws_mqtt5_client_start(struct aws_mqtt5_client *client) {
    return s_aws_mqtt5_client_change_desired_state(client, AWS_MCS_CONNECTED);
}

int aws_mqtt5_client_stop(struct aws_mqtt5_client *client) {
    return s_aws_mqtt5_client_change_desired_state(client, AWS_MCS_STOPPED);
}
