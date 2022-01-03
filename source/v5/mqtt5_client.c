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

    // ToDo: outstanding requests table entries
    aws_hash_table_clean_up(&client->outstanding_requests_table);

    // ToDo: queued operations list elements

    aws_mqtt5_client_config_destroy((struct aws_mqtt5_client_config *)client->config);

    aws_mem_release(client->allocator, client);
}

static void s_on_mqtt5_client_zero_ref_count(void *user_data) {
    struct aws_mqtt5_client *client = user_data;

    /* Async and multi-step eventually */
    s_mqtt5_client_final_destroy(client);
}

static const uint64_t MAXIMUM_SERVICE_INTERVAL_SECONDS = 10;

static void s_reevaluate_service_task(struct aws_mqtt5_client *client) {
    (void)client;

    uint64_t now = 0;
    if (aws_high_res_clock_get_ticks(&now)) {
        return;
    }

    uint64_t next_service_time =
        now + aws_timestamp_convert(MAXIMUM_SERVICE_INTERVAL_SECONDS, AWS_TIMESTAMP_SECS, AWS_TIMESTAMP_NANOS, NULL);

    // transition from stopped to connected
    if (client->current_state == AWS_MCS_STOPPED && client->desired_state == AWS_MCS_CONNECTED) {
        next_service_time = now;
    }

    // transition from connecting/ed to stopped
    if (client->current_state != AWS_MCS_STOPPED && client->current_state != AWS_MCS_DISCONNECTING &&
        client->desired_state == AWS_MCS_STOPPED) {
        next_service_time = now;
    }

    // no flow control for now
    if (!aws_linked_list_empty(&client->queued_operations)) {
        next_service_time = now;
    }

    // factor in ping and ping timeout
    if (client->current_state == AWS_MCS_CONNECTED) {
        next_service_time = aws_min_u64(next_service_time, client->next_ping_time);
        if (client->next_ping_timeout_time != 0) {
            next_service_time = aws_min_u64(next_service_time, client->next_ping_timeout_time);
        }
    }

    // factor in reconnect
    if (client->current_state == AWS_MCS_PENDING_RECONNECT) {
        next_service_time = aws_min_u64(next_service_time, client->next_reconnect_time);
    }

    // if we need to reschedule to a sooner time, or if the task hasn't been started yet
    // ToDo: consider a reschedule task api for event loop and task scheduler (bubble up/down in heap)
    if (next_service_time < client->next_service_task_run_time || client->next_service_task_run_time == 0) {
        if (next_service_time < client->next_service_task_run_time) {
            aws_event_loop_cancel_task(client->loop, &client->service_task);
        }

        aws_event_loop_schedule_task_future(client->loop, &client->service_task, next_service_time);
        client->next_service_task_run_time = next_service_time;
    }
}

static void s_mqtt5_service_task_fn(struct aws_task *task, void *arg, enum aws_task_status status) {
    (void)task;
    if (status != AWS_TASK_STATUS_RUN_READY) {
        return;
    }

    struct aws_mqtt5_client *client = arg;

    // ToDo - all service logic

    // reschedule
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

    aws_task_init(&client->service_task, s_mqtt5_service_task_fn, client);

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
    aws_linked_list_init(&client->queued_operations);
    client->next_mqtt_packet_id = 1;

    if (aws_hash_table_init(
            &client->outstanding_requests_table,
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

    aws_mqtt5_client_release(client);

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
    struct aws_mqtt_change_desired_state_task *change_state_task = arg;
    if (status != AWS_TASK_STATUS_RUN_READY) {
        goto done;
    }

    if (change_state_task->client->desired_state != change_state_task->desired_state) {
        change_state_task->client->desired_state = change_state_task->desired_state;
        s_reevaluate_service_task(change_state_task->client);
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

static int s_aws_mqtt5_client_change_desired_state(
    struct aws_mqtt5_client *client,
    enum aws_mqtt5_client_state desired_state) {
    AWS_FATAL_ASSERT(client != NULL);
    AWS_FATAL_ASSERT(client->loop != NULL);

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
