/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/io/event_loop.h>
#include <aws/mqtt/manual-puback/manual_puback.h>
#include <aws/mqtt/private/manual-puback/manual_puback_impl.h>
#include <aws/mqtt/private/v5/mqtt5_client_impl.h>
#include <aws/mqtt/private/v5/mqtt5_utils.h>

static struct aws_mqtt5_manual_puback_entry *s_aws_mqtt_manual_puback_entry_new(
    struct aws_allocator *allocator,
    uint16_t packet_id,
    uint64_t puback_control_id) {

    struct aws_mqtt5_manual_puback_entry *manual_puback_entry =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt5_manual_puback_entry));

    manual_puback_entry->allocator = allocator;
    manual_puback_entry->packet_id = packet_id;
    manual_puback_entry->puback_control_id = puback_control_id;

    return manual_puback_entry;
}

void aws_mqtt5_manual_puback_entry_destroy(void *value) {
    struct aws_mqtt5_manual_puback_entry *manual_puback_entry = value;
    if (manual_puback_entry != NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_CLIENT, "======== destroying puback entry: %llu", manual_puback_entry->puback_control_id);
        aws_mem_release(manual_puback_entry->allocator, manual_puback_entry);
    }
    return;
}

uint64_t aws_mqtt5_take_manual_puback_control(
    struct aws_mqtt5_client *client,
    const struct aws_mqtt5_packet_publish_view *publish_view) {
    AWS_PRECONDITION(client != NULL);
    AWS_PRECONDITION(publish_view != NULL);
    /* This should only ever be called by the user within the publish received callback */
    AWS_FATAL_ASSERT(aws_event_loop_thread_is_callers_thread(client->loop));

    if (publish_view->qos == AWS_MQTT5_QOS_AT_MOST_ONCE) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_CLIENT, "id=%p: PUBACK control cannot be taken for a QoS 0 PUBLISH packet.", (void *)client);
        return 0;
    }

    /* First check if the packet_id for the PUBLISH is already being controlled */
    struct aws_hash_element *elem = NULL;
    aws_hash_table_find(&client->operational_state.manual_puback_packet_id_table, &publish_view->packet_id, &elem);
    if (elem != NULL) {
        struct aws_mqtt5_manual_puback_entry *entry = elem->value;
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_CLIENT,
            "id=%p: Attempting to take PUBACK control for packet id: %d but this packet id is already being "
            "controlled with control id: %llu.\n",
            (void *)client,
            publish_view->packet_id,
            entry->puback_control_id);
        return entry->puback_control_id;
    }

    // The current_control_packet_id is incremented each time a new manual puback is scheduled.
    uint64_t current_control_packet_id = client->operational_state.next_mqtt5_puback_control_id;
    struct aws_mqtt5_manual_puback_entry *manual_puback =
        s_aws_mqtt_manual_puback_entry_new(client->allocator, publish_view->packet_id, current_control_packet_id);

    /* Allows lookup of manual puback entries by packet id */
    if (aws_hash_table_put(
            &client->operational_state.manual_puback_packet_id_table, &manual_puback->packet_id, manual_puback, NULL)) {
        int error_code = aws_last_error();
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_CLIENT,
            "id=%p: Failed to insert manual PUBACK entry into packet ID table: %d(%s)",
            (void *)client,
            error_code,
            aws_error_debug_str(error_code));
        aws_mem_release(manual_puback->allocator, manual_puback);
        return 0;
    }

    /* Allows lookup of manual puback entries by control id */
    if (aws_hash_table_put(
            &client->operational_state.manual_puback_control_id_table,
            &manual_puback->puback_control_id,
            manual_puback,
            NULL)) {
        int error_code = aws_last_error();
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_CLIENT,
            "id=%p: Failed to insert manual PUBACK entry into control ID table: %d(%s)",
            (void *)client,
            error_code,
            aws_error_debug_str(error_code));
        // clean up the manual puback entry from the packet id table and deallocate.
        aws_hash_table_remove(
            &client->operational_state.manual_puback_packet_id_table, &manual_puback->packet_id, NULL, NULL);
        aws_mem_release(manual_puback->allocator, manual_puback);
        return 0;
    }

    /* Increment next_mqtt5_puback_control_id for next use */
    client->operational_state.next_mqtt5_puback_control_id = current_control_packet_id + 1;
    return manual_puback->puback_control_id;
}

// Calls into mqtt5_client.c private aws_mqtt5_client_queue_puback function via wrapper.
static int aws_mqtt5_send_puback(
    struct aws_mqtt5_client *client,
    uint16_t packet_id,
    const struct aws_mqtt5_manual_puback_completion_options *completion_options) {
    int result = aws_mqtt5_client_queue_puback_internal(client, packet_id, completion_options);
    if (result != AWS_OP_SUCCESS) {
        int error_code = aws_last_error();
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_CLIENT,
            "id=%p: decode failure with error %d(%s)",
            (void *)client,
            error_code,
            aws_error_debug_str(error_code));

        aws_mqtt5_client_shutdown_channel_internal(client, error_code);
    }
    return result;
}

void aws_mqtt5_handle_puback(
    struct aws_mqtt5_client *client,
    const struct aws_mqtt5_packet_publish_view *publish_view) {

    // Check if this PUBLSIH packet is manually controlled
    struct aws_hash_element *elem = NULL;
    aws_hash_table_find(&client->operational_state.manual_puback_packet_id_table, &publish_view->packet_id, &elem);

    /* This PUBLISH isn't a manually controlled PUBACK. We schedule the PUBACK to be sent immediately. */
    if (elem == NULL) {
        aws_mqtt5_send_puback(client, publish_view->packet_id, NULL);
        return;
    }
}

struct aws_mqtt5_manual_puback_task {
    struct aws_task task;
    struct aws_allocator *allocator;
    struct aws_mqtt5_client *client;
    uint64_t puback_control_id;
    struct aws_mqtt5_manual_puback_completion_options completion_options;
};

static void s_mqtt5_manual_puback_task_fn(struct aws_task *task, void *arg, enum aws_task_status status) {
    (void)task;

    struct aws_mqtt5_manual_puback_task *manual_puback_task = arg;
    struct aws_mqtt5_client *client = manual_puback_task->client;
    uint64_t puback_control_id = manual_puback_task->puback_control_id;

    if (status != AWS_TASK_STATUS_RUN_READY) {
        goto cleanup;
    }

    AWS_FATAL_ASSERT(aws_event_loop_thread_is_callers_thread(client->loop));

    enum aws_mqtt5_manual_puback_result puback_result = AWS_MQTT5_MPR_SUCCESS;

    struct aws_hash_element *elem = NULL;
    aws_hash_table_find(&client->operational_state.manual_puback_control_id_table, &puback_control_id, &elem);

    // We can schedule the PUBACK as an mqtt operation if it exists in the control id table.
    if (elem != NULL) {
        struct aws_mqtt5_manual_puback_entry *manual_puback_entry =
            (struct aws_mqtt5_manual_puback_entry *)(elem->value);

        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_CLIENT,
            "id=%p: Scheuduling puback for control id: %llu for packet id: %d \n",
            (void *)client,
            manual_puback_entry->puback_control_id,
            manual_puback_entry->packet_id);

        uint16_t packet_id = manual_puback_entry->packet_id;
        uint64_t puback_control_id = manual_puback_entry->puback_control_id;

        aws_hash_table_remove(&client->operational_state.manual_puback_packet_id_table, &packet_id, NULL, NULL);
        // This removal from the control id table will also deallocate the manual puback entry.
        aws_hash_table_remove(
            &client->operational_state.manual_puback_control_id_table, &puback_control_id, NULL, NULL);

        if (aws_mqtt5_send_puback(client, packet_id, &manual_puback_task->completion_options) != AWS_OP_SUCCESS) {
            // this failure doesn't trigger the completion so we do it here before cleanup.
            puback_result = AWS_MQTT5_MPR_CRT_FAILURE;
            goto completion;
        }

        // Completion will be handled by the puback mqtt operation so we go to cleanup.
        goto cleanup;
    }

    // If the control id isn't in the active table, check if it's been cancelled.
    aws_hash_table_find(&client->operational_state.manual_puback_cancelled_control_id_table, &puback_control_id, &elem);
    if (elem != NULL) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_CLIENT,
            "id=%p: puback_control_id: %llu has been cancelled due to a disconnection.",
            (void *)client,
            puback_control_id);
        struct aws_mqtt5_manual_puback_entry *manual_puback_entry =
            (struct aws_mqtt5_manual_puback_entry *)(elem->value);
        uint64_t puback_control_id = manual_puback_entry->puback_control_id;

        // A cancelled control id has been used so it can be cleared from the table and deallocated. We only report
        // a cancellation once. Past that we will simply treat it as invalid.
        aws_hash_table_remove(
            &client->operational_state.manual_puback_cancelled_control_id_table, &puback_control_id, NULL, NULL);
        puback_result = AWS_MQTT5_MPR_PUBACK_CANCELLED;
        goto completion;
    }

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_CLIENT,
        "id=%p: puback_control_id: %llu is not tracked in any way.",
        (void *)client,
        puback_control_id);
    puback_result = AWS_MQTT5_MPR_PUBACK_INVALID;

completion:
    // We call the completion callback here in cases where there is no PUBACK operation scheduled on the client.
    if (manual_puback_task->completion_options.completion_callback != NULL) {
        manual_puback_task->completion_options.completion_callback(
            puback_result, manual_puback_task->completion_options.completion_user_data);
    }

cleanup:
    aws_mqtt5_client_release(manual_puback_task->client);
    aws_mem_release(manual_puback_task->allocator, manual_puback_task);
}

// Schedules task to process a manual PUBACK for provided puback_control_id
int aws_mqtt5_manual_puback(
    struct aws_mqtt5_client *client,
    uint64_t puback_control_id,
    const struct aws_mqtt5_manual_puback_completion_options *completion_options) {
    AWS_PRECONDITION(client != NULL);

    struct aws_mqtt5_manual_puback_task *manual_puback_task =
        aws_mem_calloc(client->allocator, 1, sizeof(struct aws_mqtt5_manual_puback_task));

    aws_task_init(
        &manual_puback_task->task, s_mqtt5_manual_puback_task_fn, manual_puback_task, "Mqtt5ManualPubackTask");
    manual_puback_task->allocator = client->allocator;
    manual_puback_task->client = aws_mqtt5_client_acquire(client);
    manual_puback_task->puback_control_id = puback_control_id;
    if (completion_options != NULL) {
        manual_puback_task->completion_options = *completion_options;
    }

    aws_event_loop_schedule_task_now(client->loop, &manual_puback_task->task);

    return AWS_OP_SUCCESS;
}
