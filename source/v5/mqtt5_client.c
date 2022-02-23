/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/v5/mqtt5_client.h>

#include <aws/common/clock.h>
#include <aws/common/device_random.h>
#include <aws/common/string.h>
#include <aws/http/proxy.h>
#include <aws/http/request_response.h>
#include <aws/http/websocket.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/mqtt/private/v5/mqtt5_client_impl.h>
#include <aws/mqtt/private/v5/mqtt5_options_storage.h>
#include <aws/mqtt/private/v5/mqtt5_utils.h>

#include <inttypes.h>

static const char *s_aws_mqtt5_client_state_to_c_str(enum aws_mqtt5_client_state state) {
    switch (state) {
        case AWS_MCS_STOPPED:
            return "STOPPED";

        case AWS_MCS_CONNECTING:
            return "CONNECTING";

        case AWS_MCS_MQTT_CONNECT:
            return "MQTT_CONNECT";

        case AWS_MCS_CONNECTED:
            return "CONNECTED";

        case AWS_MCS_CLEAN_DISCONNECT:
            return "CLEAN_DISCONNECT";

        case AWS_MCS_CHANNEL_SHUTDOWN:
            return "CHANNEL_SHUTDOWN";

        case AWS_MCS_PENDING_RECONNECT:
            return "PENDING_RECONNECT";

        case AWS_MCS_TERMINATED:
            return "TERMINATED";

        default:
            return "UNKNOWN";
    }
}

static int s_aws_mqtt5_client_change_desired_state(
    struct aws_mqtt5_client *client,
    enum aws_mqtt5_client_state desired_state,
    struct aws_mqtt5_operation_disconnect *disconnect_operation);

static uint64_t s_hash_uint16_t(const void *item) {
    return *(uint16_t *)item;
}

static bool s_uint16_t_eq(const void *a, const void *b) {
    return *(uint16_t *)a == *(uint16_t *)b;
}

static void s_complete_operation_list(
    struct aws_mqtt5_client *client,
    int error_code,
    struct aws_linked_list *operation_list) {
    (void)client;

    struct aws_linked_list_node *node = aws_linked_list_begin(operation_list);
    while (node != aws_linked_list_end(operation_list)) {
        struct aws_mqtt5_operation *operation = AWS_CONTAINER_OF(node, struct aws_mqtt5_operation, node);

        aws_mqtt5_operation_complete(operation, error_code, NULL);
        aws_mqtt5_operation_release(operation);

        node = aws_linked_list_next(node);
    }

    /* we've released everything, so reset the list to empty */
    aws_linked_list_init(operation_list);
}

static void s_mqtt5_client_final_destroy(struct aws_mqtt5_client *client) {
    if (client == NULL) {
        return;
    }

    AWS_ASSERT(aws_hash_table_get_entry_count(&client->unacked_operations_table) == 0);
    AWS_ASSERT(client->current_operation == NULL);

    aws_hash_table_clean_up(&client->unacked_operations_table);

    s_complete_operation_list(client, AWS_ERROR_MQTT5_CLIENT_TERMINATED, &client->queued_operations);
    s_complete_operation_list(client, AWS_ERROR_MQTT5_CLIENT_TERMINATED, &client->write_completion_operations);
    s_complete_operation_list(client, AWS_ERROR_MQTT5_CLIENT_TERMINATED, &client->unacked_operations);

    aws_mqtt5_client_options_storage_destroy((struct aws_mqtt5_client_options_storage *)client->config);

    aws_mqtt5_operation_disconnect_release(client->disconnect_operation);
    aws_http_message_release(client->handshake);

    aws_mqtt5_encoder_clean_up(&client->encoder);
    aws_mqtt5_decoder_clean_up(&client->decoder);

    aws_mem_release(client->allocator, client);
}

static void s_on_mqtt5_client_zero_ref_count(void *user_data) {
    struct aws_mqtt5_client *client = user_data;

    s_aws_mqtt5_client_change_desired_state(client, AWS_MCS_TERMINATED, NULL);
}

static void s_aws_mqtt5_client_emit_stopped_lifecycle_event(struct aws_mqtt5_client *client) {
    AWS_LOGF_INFO(AWS_LS_MQTT5_CLIENT, "id=%p: emitting stopped lifecycle event", (void *)client);

    if (client->config->lifecycle_event_handler != NULL) {
        struct aws_mqtt5_client_lifecycle_event event;
        AWS_ZERO_STRUCT(event);

        event.event_type = AWS_MQTT5_CLET_STOPPED;
        event.client = client;
        event.user_data = client->config->lifecycle_event_handler_user_data;

        (*client->config->lifecycle_event_handler)(&event);
    }
}

static void s_aws_mqtt5_client_emit_connecting_lifecycle_event(struct aws_mqtt5_client *client) {
    AWS_LOGF_INFO(AWS_LS_MQTT5_CLIENT, "id=%p: emitting connecting lifecycle event", (void *)client);

    client->lifecycle_state = AWS_MQTT5_LS_CONNECTING;

    if (client->config->lifecycle_event_handler != NULL) {
        struct aws_mqtt5_client_lifecycle_event event;
        AWS_ZERO_STRUCT(event);

        event.event_type = AWS_MQTT5_CLET_ATTEMPTING_CONNECT;
        event.client = client;
        event.user_data = client->config->lifecycle_event_handler_user_data;

        (*client->config->lifecycle_event_handler)(&event);
    }
}

static void s_aws_mqtt5_client_emit_connection_success_lifecycle_event(
    struct aws_mqtt5_client *client,
    const struct aws_mqtt5_packet_connack_view *connack_view) {

    AWS_LOGF_INFO(AWS_LS_MQTT5_CLIENT, "id=%p: emitting connection success lifecycle event", (void *)client);

    client->lifecycle_state = AWS_MQTT5_LS_CONNECTED;

    if (client->config->lifecycle_event_handler != NULL) {
        struct aws_mqtt5_client_lifecycle_event event;
        AWS_ZERO_STRUCT(event);

        event.event_type = AWS_MQTT5_CLET_CONNECTION_SUCCESS;
        event.client = client;
        event.user_data = client->config->lifecycle_event_handler_user_data;
        event.settings = &client->negotiated_settings;
        event.connack_data = connack_view;

        (*client->config->lifecycle_event_handler)(&event);
    }
}

/*
 * Emits either a CONNECTION_FAILED or DISCONNECT event based on the current life cycle state.  Once a "final"
 * event is emitted by the client, it must attempt to reconnect before another one will be emitted, since the
 * lifecycle state check will early out until then.  It is expected that this function may get called unnecessarily
 * often during various channel shutdown or disconnection/failure flows.  This will not affect overall correctness.
 */
static void s_aws_mqtt5_client_emit_final_lifecycle_event(
    struct aws_mqtt5_client *client,
    int error_code,
    const struct aws_mqtt5_packet_connack_view *connack_view,
    const struct aws_mqtt5_packet_disconnect_view *disconnect_view) {

    if (client->lifecycle_state == AWS_MQTT5_LS_NONE) {
        /* we already emitted a final event earlier */
        return;
    }

    struct aws_mqtt5_client_lifecycle_event event;
    AWS_ZERO_STRUCT(event);

    if (client->lifecycle_state == AWS_MQTT5_LS_CONNECTING) {
        AWS_FATAL_ASSERT(disconnect_view == NULL);
        event.event_type = AWS_MQTT5_CLET_CONNECTION_FAILURE;

        AWS_LOGF_INFO(
            AWS_LS_MQTT5_CLIENT,
            "id=%p: emitting connection failure lifecycle event with error code %d(%s)",
            (void *)client,
            error_code,
            aws_error_debug_str(error_code));
    } else {
        AWS_FATAL_ASSERT(client->lifecycle_state == AWS_MQTT5_LS_CONNECTED);
        AWS_FATAL_ASSERT(connack_view == NULL);
        event.event_type = AWS_MQTT5_CLET_DISCONNECTION;

        AWS_LOGF_INFO(
            AWS_LS_MQTT5_CLIENT,
            "id=%p: emitting disconnection lifecycle event with error code %d(%s)",
            (void *)client,
            error_code,
            aws_error_debug_str(error_code));
    }

    event.error_code = error_code;
    event.user_data = client->config->lifecycle_event_handler_user_data;
    event.connack_data = connack_view;
    event.disconnect_data = disconnect_view;

    client->lifecycle_state = AWS_MQTT5_LS_NONE;

    if (client->config->lifecycle_event_handler != NULL) {
        (*client->config->lifecycle_event_handler)(&event);
    }
}

static void s_enqueue_operation(struct aws_mqtt5_client *client, struct aws_mqtt5_operation *operation) {
    /* TODO: when statistics are added, we'll need to update them here */

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_CLIENT,
        "id=%p: enqueuing %s operation",
        (void *)client,
        aws_mqtt5_packet_type_to_c_string(operation->packet_type));

    aws_linked_list_push_back(&client->queued_operations, &operation->node);
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
    /* This state is interruptable by a stop/terminate */
    if (client->desired_state != AWS_MCS_CONNECTED) {
        return now;
    }

    /*
     * The transition to MQTT_CONNECT just makes the CONNECT operation and assigns it to current_operation.
     * It's up to the service task to actually encode and push it down the handler chain.
     *
     * Note: no flow control on this.
     */
    if (client->current_operation != NULL && !client->pending_write_completion) {
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
    if (client->next_reconnect_delay_reset_time_ns > 0) {
        next_service_time = aws_min_u64(next_service_time, client->next_reconnect_delay_reset_time_ns);
    }

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
    if (client->desired_state != AWS_MCS_CONNECTED) {
        return now;
    }

    return client->next_reconnect_time_ns;
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
    /*
     * This causes the client to only reevaluate service schedule time at the end of the service call or in
     * a callback from an external event.
     */
    if (client->in_service) {
        return;
    }

    uint64_t now = (*client->vtable->get_current_time_fn)();
    uint64_t next_service_time = s_compute_next_service_time_by_current_state(client, now);

    /*
     * This catches both the case when there's an existing service schedule and we either want to not
     * perform it (next_service_time == 0) or need to run service at a different time than the current scheduled time.
     */
    if (next_service_time != client->next_service_task_run_time && client->next_service_task_run_time > 0) {
        aws_event_loop_cancel_task(client->loop, &client->service_task);
        client->next_service_task_run_time = 0;

        AWS_LOGF_TRACE(AWS_LS_MQTT5_CLIENT, "id=%p: cancelling previously scheduled service task", (void *)client);
    }

    if (next_service_time > 0 &&
        (next_service_time < client->next_service_task_run_time || client->next_service_task_run_time == 0)) {
        aws_event_loop_schedule_task_future(client->loop, &client->service_task, next_service_time);

        AWS_LOGF_TRACE(
            AWS_LS_MQTT5_CLIENT, "id=%p: scheduled service task for time %" PRIu64, (void *)client, next_service_time);
    }

    client->next_service_task_run_time = next_service_time;
}

static void s_change_current_state(struct aws_mqtt5_client *client, enum aws_mqtt5_client_state next_state);

static void s_change_current_state_to_stopped(struct aws_mqtt5_client *client) {
    client->current_state = AWS_MCS_STOPPED;

    aws_hash_table_clear(&client->unacked_operations_table);

    s_complete_operation_list(client, AWS_ERROR_MQTT5_USER_REQUESTED_STOP, &client->queued_operations);
    s_complete_operation_list(client, AWS_ERROR_MQTT5_USER_REQUESTED_STOP, &client->write_completion_operations);
    s_complete_operation_list(client, AWS_ERROR_MQTT5_USER_REQUESTED_STOP, &client->unacked_operations);

    s_aws_mqtt5_client_emit_stopped_lifecycle_event(client);
}

static void s_aws_mqtt5_client_shutdown_channel(struct aws_mqtt5_client *client, int error_code) {
    if (error_code == AWS_ERROR_SUCCESS) {
        error_code = AWS_ERROR_UNKNOWN;
    }

    s_aws_mqtt5_client_emit_final_lifecycle_event(client, error_code, NULL, NULL);

    if (client->current_state != AWS_MCS_MQTT_CONNECT && client->current_state != AWS_MCS_CONNECTED &&
        client->current_state != AWS_MCS_CLEAN_DISCONNECT) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_CLIENT,
            "id=%p: client channel shutdown invoked from unexpected state %d(%s)",
            (void *)client,
            (int)client->current_state,
            s_aws_mqtt5_client_state_to_c_str(client->current_state));
        return;
    }

    if (client->slot == NULL || client->slot->channel == NULL) {
        AWS_LOGF_ERROR(AWS_LS_MQTT5_CLIENT, "id=%p: client channel shutdown invoked without a channel", (void *)client);
        return;
    }

    s_change_current_state(client, AWS_MCS_CHANNEL_SHUTDOWN);
    (*client->vtable->channel_shutdown_fn)(client->slot->channel, error_code);
}

static void s_mqtt5_client_shutdown(
    struct aws_client_bootstrap *bootstrap,
    int error_code,
    struct aws_channel *channel,
    void *user_data) {

    (void)bootstrap;
    (void)channel;

    struct aws_mqtt5_client *client = user_data;

    if (error_code == AWS_ERROR_SUCCESS) {
        error_code = AWS_ERROR_MQTT_UNEXPECTED_HANGUP;
    }

    s_aws_mqtt5_client_emit_final_lifecycle_event(client, error_code, NULL, NULL);

    AWS_LOGF_INFO(
        AWS_LS_MQTT5_CLIENT,
        "id=%p: channel tore down with error code %d(%s)",
        (void *)client,
        error_code,
        aws_error_debug_str(error_code));

    if (client->slot) {
        aws_channel_slot_remove(client->slot);
        AWS_LOGF_TRACE(AWS_LS_MQTT5_CLIENT, "id=%p: slot removed successfully", (void *)client);
        client->slot = NULL;
    }

    if (client->desired_state == AWS_MCS_CONNECTED) {
        s_change_current_state(client, AWS_MCS_PENDING_RECONNECT);
    } else {
        s_change_current_state(client, AWS_MCS_STOPPED);
    }
}

static void s_mqtt5_client_setup(
    struct aws_client_bootstrap *bootstrap,
    int error_code,
    struct aws_channel *channel,
    void *user_data) {

    (void)bootstrap;

    /* Setup callback contract is: if error_code is non-zero then channel is NULL. */
    AWS_FATAL_ASSERT((error_code != 0) == (channel == NULL));
    struct aws_mqtt5_client *client = user_data;

    AWS_FATAL_ASSERT(client->current_state == AWS_MCS_CONNECTING);

    if (error_code != AWS_OP_SUCCESS) {
        /* client shutdown already handles this case, so just call that. */
        s_mqtt5_client_shutdown(bootstrap, error_code, channel, user_data);
        return;
    }

    if (client->desired_state != AWS_MCS_CONNECTED) {
        aws_raise_error(AWS_ERROR_MQTT5_USER_REQUESTED_STOP);
        goto error;
    }

    client->slot = aws_channel_slot_new(channel); /* allocs or crashes */

    if (aws_channel_slot_insert_end(channel, client->slot)) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_CLIENT,
            "id=%p: Failed to insert slot into channel %p, error %d (%s).",
            (void *)client,
            (void *)channel,
            aws_last_error(),
            aws_error_name(aws_last_error()));
        goto error;
    }

    if (aws_channel_slot_set_handler(client->slot, &client->handler)) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_CLIENT,
            "id=%p: Failed to set MQTT handler into slot on channel %p, error %d (%s).",
            (void *)client,
            (void *)channel,
            aws_last_error(),
            aws_error_name(aws_last_error()));

        goto error;
    }

    s_change_current_state(client, AWS_MCS_MQTT_CONNECT);

    return;

error:

    s_change_current_state(client, AWS_MCS_CHANNEL_SHUTDOWN);
    (*client->vtable->channel_shutdown_fn)(channel, aws_last_error());
}

static void s_on_websocket_shutdown(struct aws_websocket *websocket, int error_code, void *user_data) {
    struct aws_mqtt5_client *client = user_data;

    struct aws_channel *channel = client->slot ? client->slot->channel : NULL;

    s_mqtt5_client_shutdown(client->config->bootstrap, error_code, channel, client);

    if (websocket) {
        aws_websocket_release(websocket);
    }
}

static void s_on_websocket_setup(
    struct aws_websocket *websocket,
    int error_code,
    int handshake_response_status,
    const struct aws_http_header *handshake_response_header_array,
    size_t num_handshake_response_headers,
    void *user_data) {

    (void)handshake_response_status;
    (void)handshake_response_header_array;
    (void)num_handshake_response_headers;

    struct aws_mqtt5_client *client = user_data;
    client->handshake = aws_http_message_release(client->handshake);

    /* Setup callback contract is: if error_code is non-zero then websocket is NULL. */
    AWS_FATAL_ASSERT((error_code != 0) == (websocket == NULL));

    struct aws_channel *channel = NULL;

    if (websocket) {
        channel = aws_websocket_get_channel(websocket);
        AWS_ASSERT(channel);

        /* Websocket must be "converted" before the MQTT handler can be installed next to it. */
        if (aws_websocket_convert_to_midchannel_handler(websocket)) {
            AWS_LOGF_ERROR(
                AWS_LS_MQTT5_CLIENT,
                "id=%p: Failed converting websocket, error %d (%s)",
                (void *)client,
                aws_last_error(),
                aws_error_name(aws_last_error()));

            (*client->vtable->channel_shutdown_fn)(channel, aws_last_error());
            return;
        }
    }

    /* Call into the channel-setup callback, the rest of the logic is the same. */
    s_mqtt5_client_setup(client->config->bootstrap, error_code, channel, client);
}

struct aws_mqtt5_websocket_transform_complete_task {
    struct aws_task task;
    struct aws_allocator *allocator;
    struct aws_mqtt5_client *client;
    int error_code;
    struct aws_http_message *handshake;
};

void s_websocket_transform_complete_task_fn(struct aws_task *task, void *arg, enum aws_task_status status) {
    (void)task;

    struct aws_mqtt5_websocket_transform_complete_task *websocket_transform_complete_task = arg;
    if (status != AWS_TASK_STATUS_RUN_READY) {
        goto done;
    }

    struct aws_mqtt5_client *client = websocket_transform_complete_task->client;

    aws_http_message_release(client->handshake);
    client->handshake = aws_http_message_acquire(websocket_transform_complete_task->handshake);

    int error_code = websocket_transform_complete_task->error_code;

    /*
     * TODO: for now there is no timeout that will change state out of CONNECTING, so assume we're still in it.
     * Since we haven't kicked off channel creation yet, we could (and probably should) add one.
     */
    AWS_ASSERT(client->current_state == AWS_MCS_CONNECTING);
    if (error_code == 0 && client->desired_state == AWS_MCS_CONNECTED) {

        struct aws_websocket_client_connection_options websocket_options = {
            .allocator = client->allocator,
            .bootstrap = client->config->bootstrap,
            .socket_options = &client->config->socket_options,
            .tls_options = client->config->tls_options_ptr,
            .host = aws_byte_cursor_from_string(client->config->host_name),
            .port = client->config->port,
            .handshake_request = websocket_transform_complete_task->handshake,
            .initial_window_size = 0, /* Prevent websocket data from arriving before the MQTT handler is installed */
            .user_data = client,
            .on_connection_setup = s_on_websocket_setup,
            .on_connection_shutdown = s_on_websocket_shutdown,
        };

        if (client->config->http_proxy_config != NULL) {
            websocket_options.proxy_options = &client->config->http_proxy_options;
        }

        if (client->vtable->websocket_connect_fn(&websocket_options)) {
            AWS_LOGF_ERROR(AWS_LS_MQTT5_CLIENT, "id=%p: Failed to initiate websocket connection.", (void *)client);
            error_code = aws_last_error();
            goto error;
        }

        goto done;

    } else {
        if (error_code == AWS_ERROR_SUCCESS) {
            AWS_ASSERT(client->desired_state != AWS_MCS_CONNECTED);
            error_code = AWS_ERROR_MQTT5_USER_REQUESTED_STOP;
        }
    }

error:

    s_on_websocket_setup(NULL, error_code, -1, NULL, 0, client);

done:

    aws_http_message_release(websocket_transform_complete_task->handshake);
    aws_mqtt5_client_release(websocket_transform_complete_task->client);

    aws_mem_release(websocket_transform_complete_task->allocator, websocket_transform_complete_task);
}

static void s_websocket_handshake_transform_complete(
    struct aws_http_message *handshake_request,
    int error_code,
    void *complete_ctx) {

    struct aws_mqtt5_client *client = complete_ctx;

    struct aws_mqtt5_websocket_transform_complete_task *task =
        aws_mem_calloc(client->allocator, 1, sizeof(struct aws_mqtt5_websocket_transform_complete_task));

    aws_task_init(
        &task->task, s_websocket_transform_complete_task_fn, (void *)task, "WebsocketHandshakeTransformComplete");

    task->allocator = client->allocator;
    task->client = aws_mqtt5_client_acquire(client);
    task->error_code = error_code;
    task->handshake = handshake_request;

    aws_event_loop_schedule_task_now(client->loop, &task->task);

    aws_mqtt5_client_release(client);
}

static int s_websocket_connect(struct aws_mqtt5_client *client) {
    AWS_ASSERT(client);
    AWS_ASSERT(client->config->websocket_handshake_transform);

    /* These defaults were chosen because they're commmon in other MQTT libraries.
     * The user can modify the request in their transform callback if they need to. */
    /* TODO: share these with the mqtt3.1 impl in client.c */
    const struct aws_byte_cursor default_path = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/mqtt");
    const struct aws_http_header default_protocol_header = {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Sec-WebSocket-Protocol"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("mqtt"),
    };

    /* Build websocket handshake request */
    struct aws_http_message *handshake = aws_http_message_new_websocket_handshake_request(
        client->allocator, default_path, aws_byte_cursor_from_string(client->config->host_name));

    if (handshake == NULL) {
        AWS_LOGF_ERROR(AWS_LS_MQTT5_CLIENT, "id=%p: Failed to generate websocket handshake request", (void *)client);
        return AWS_OP_ERR;
    }

    if (aws_http_message_add_header(handshake, default_protocol_header)) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_CLIENT, "id=%p: Failed to add default header to websocket handshake request", (void *)client);
        goto on_error;
    }

    AWS_LOGF_TRACE(AWS_LS_MQTT5_CLIENT, "id=%p: Transforming websocket handshake request.", (void *)client);

    aws_mqtt5_client_acquire(client);
    client->config->websocket_handshake_transform(
        handshake,
        client->config->websocket_handshake_transform_user_data,
        s_websocket_handshake_transform_complete,
        client);

    return AWS_OP_SUCCESS;

on_error:

    aws_http_message_release(handshake);

    return AWS_OP_ERR;
}

static void s_change_current_state_to_connecting(struct aws_mqtt5_client *client) {
    (void)client;
    AWS_ASSERT(client->current_state == AWS_MCS_STOPPED || client->current_state == AWS_MCS_PENDING_RECONNECT);

    client->current_state = AWS_MCS_CONNECTING;
    client->disconnect_operation = aws_mqtt5_operation_disconnect_release(client->disconnect_operation);

    s_aws_mqtt5_client_emit_connecting_lifecycle_event(client);

    int result = 0;
    if (client->config->websocket_handshake_transform != NULL) {
        result = s_websocket_connect(client);
    } else {
        struct aws_socket_channel_bootstrap_options channel_options;
        AWS_ZERO_STRUCT(channel_options);
        channel_options.bootstrap = client->config->bootstrap;
        channel_options.host_name = aws_string_c_str(client->config->host_name);
        channel_options.port = client->config->port;
        channel_options.socket_options = &client->config->socket_options;
        channel_options.tls_options = client->config->tls_options_ptr;
        channel_options.setup_callback = &s_mqtt5_client_setup;
        channel_options.shutdown_callback = &s_mqtt5_client_shutdown;
        channel_options.user_data = client;
        channel_options.requested_event_loop = client->loop;

        if (client->config->http_proxy_config == NULL) {
            result = (*client->vtable->client_bootstrap_new_socket_channel_fn)(&channel_options);
        } else {
            result = (*client->vtable->http_proxy_new_socket_channel_fn)(
                &channel_options, &client->config->http_proxy_options);
        }
    }

    if (result) {
        int error_code = aws_last_error();
        AWS_LOGF_INFO(
            AWS_LS_MQTT5_CLIENT,
            "id=%p: failed to kick off connection with error %d(%s)",
            (void *)client,
            error_code,
            aws_error_debug_str(error_code));

        s_aws_mqtt5_client_emit_final_lifecycle_event(client, aws_last_error(), NULL, NULL);

        s_change_current_state(client, AWS_MCS_PENDING_RECONNECT);
    }
}

static int s_aws_mqtt5_client_set_current_operation(
    struct aws_mqtt5_client *client,
    struct aws_mqtt5_operation *operation) {
    if (aws_mqtt5_encoder_append_packet_encoding(&client->encoder, operation->packet_type, operation->packet_view)) {
        int error_code = aws_last_error();
        AWS_LOGF_INFO(
            AWS_LS_MQTT5_CLIENT,
            "id=%p: failed to append packet encoding sequence for current operation with error %d(%s)",
            (void *)client,
            error_code,
            aws_error_debug_str(error_code));

        s_aws_mqtt5_client_shutdown_channel(client, error_code);
        return AWS_OP_ERR;
    }

    client->current_operation = operation;

    return AWS_OP_SUCCESS;
}

static void s_reset_ping(struct aws_mqtt5_client *client) {
    uint64_t now = (*client->vtable->get_current_time_fn)();
    uint16_t keep_alive_seconds = client->negotiated_settings.server_keep_alive;

    uint64_t keep_alive_interval_nanos =
        aws_timestamp_convert(keep_alive_seconds, AWS_TIMESTAMP_SECS, AWS_TIMESTAMP_NANOS, NULL);
    client->next_ping_time = aws_add_u64_saturating(now, keep_alive_interval_nanos);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_CLIENT, "id=%p: next PINGREQ scheduled for time %" PRIu64, (void *)client, client->next_ping_time);
}

static void s_aws_mqtt5_on_socket_write_completion_mqtt_connect(struct aws_mqtt5_client *client, int error_code) {
    if (error_code != AWS_ERROR_SUCCESS) {
        s_aws_mqtt5_client_shutdown_channel(client, error_code);
        return;
    }

    s_reevaluate_service_task(client);
}

static void s_aws_mqtt5_on_socket_write_completion_connected(struct aws_mqtt5_client *client, int error_code) {
    if (error_code != AWS_ERROR_SUCCESS) {
        s_aws_mqtt5_client_shutdown_channel(client, error_code);
        return;
    }

    /* Push the ping timer out every time something (including a pingreq) goes out on the wire */
    s_reset_ping(client);
    s_reevaluate_service_task(client);
}

static void s_aws_mqtt5_on_socket_write_completion_clean_disconnect(struct aws_mqtt5_client *client, int error_code) {
    /* TODO */
    (void)client;
    (void)error_code;
}

static void s_aws_mqtt5_on_socket_write_completion(
    struct aws_channel *channel,
    struct aws_io_message *message,
    int error_code,
    void *user_data) {

    (void)channel;
    (void)message;

    struct aws_mqtt5_client *client = user_data;
    client->pending_write_completion = false;

    if (error_code != AWS_ERROR_SUCCESS) {
        AWS_LOGF_INFO(
            AWS_LS_MQTT5_CLIENT,
            "id=%p: socket write completion invoked with error %d(%s)",
            (void *)client,
            error_code,
            aws_error_debug_str(error_code));
    }

    switch (client->current_state) {
        case AWS_MCS_MQTT_CONNECT:
            s_aws_mqtt5_on_socket_write_completion_mqtt_connect(client, error_code);
            break;

        case AWS_MCS_CONNECTED:
            s_aws_mqtt5_on_socket_write_completion_connected(client, error_code);
            break;

        case AWS_MCS_CLEAN_DISCONNECT:
            s_aws_mqtt5_on_socket_write_completion_clean_disconnect(client, error_code);
            break;

        default:
            break;
    }

    s_complete_operation_list(client, error_code, &client->write_completion_operations);
}

#define AWS_MQTT5_IO_MESSAGE_DEFAULT_LENGTH 4096

static int s_aws_mqtt5_client_write_current_operation_only(struct aws_mqtt5_client *client) {
    if (client->current_operation == NULL || client->pending_write_completion) {
        return AWS_OP_SUCCESS;
    }

    struct aws_io_message *message = aws_channel_acquire_message_from_pool(
        client->slot->channel, AWS_IO_MESSAGE_APPLICATION_DATA, AWS_MQTT5_IO_MESSAGE_DEFAULT_LENGTH);
    if (message == NULL) {
        return AWS_OP_ERR;
    }

    enum aws_mqtt5_encoding_result result =
        aws_mqtt5_encoder_encode_to_buffer(&client->encoder, &message->message_data);
    if (result == AWS_MQTT5_ER_FINISHED) {
        aws_linked_list_push_back(&client->write_completion_operations, &client->current_operation->node);
        client->current_operation = NULL;
    }

    if (result != AWS_MQTT5_ER_ERROR) {
        message->on_completion = s_aws_mqtt5_on_socket_write_completion;
        message->user_data = client;
        client->pending_write_completion = true;

        if (aws_channel_slot_send_message(client->slot, message, AWS_CHANNEL_DIR_WRITE)) {
            int error_code = aws_last_error();
            AWS_LOGF_ERROR(
                AWS_LS_MQTT5_CLIENT,
                "id=%p: socket write failed with error %d(%s)",
                (void *)client,
                error_code,
                aws_error_debug_str(error_code));

            client->pending_write_completion = false;
            aws_mem_release(message->allocator, message);
            return AWS_OP_ERR;
        }
    } else {
        aws_mem_release(message->allocator, message);
        return aws_raise_error(AWS_ERROR_MQTT5_ENCODE_FAILURE);
    }

    return AWS_OP_SUCCESS;
}

#define AWS_MQTT5_DEFAULT_CONNACK_PACKET_TIMEOUT_MS 10000

static void s_change_current_state_to_mqtt_connect(struct aws_mqtt5_client *client) {
    AWS_FATAL_ASSERT(client->current_state == AWS_MCS_CONNECTING);
    AWS_FATAL_ASSERT(client->current_operation == NULL);

    client->current_state = AWS_MCS_MQTT_CONNECT;
    client->pending_write_completion = false;

    aws_mqtt5_encoder_reset(&client->encoder);
    aws_mqtt5_decoder_reset(&client->decoder);
    aws_mqtt5_negotiated_settings_reset(&client->negotiated_settings, &client->config->connect.storage_view);

    struct aws_mqtt5_operation_connect *connect_op =
        aws_mqtt5_operation_connect_new(client->allocator, &client->config->connect.storage_view);
    if (connect_op == NULL) {
        int error_code = aws_last_error();
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_CLIENT,
            "id=%p: failed to create CONNECT operation with error %d(%s)",
            (void *)client,
            error_code,
            aws_error_debug_str(error_code));

        s_aws_mqtt5_client_shutdown_channel(client, error_code);
        return;
    }

    if (s_aws_mqtt5_client_set_current_operation(client, &connect_op->base)) {
        int error_code = aws_last_error();
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_CLIENT,
            "id=%p: failed to set current client operation to CONNECT with error %d(%s)",
            (void *)client,
            error_code,
            aws_error_debug_str(error_code));

        aws_mqtt5_operation_release(&connect_op->base);
        s_aws_mqtt5_client_shutdown_channel(client, aws_last_error());
        return;
    }

    if (s_aws_mqtt5_client_write_current_operation_only(client)) {
        int error_code = aws_last_error();
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_CLIENT,
            "id=%p: failed to write CONNECT packet to channel with error %d(%s)",
            (void *)client,
            error_code,
            aws_error_debug_str(error_code));

        s_aws_mqtt5_client_shutdown_channel(client, error_code);
        return;
    }

    uint32_t timeout_ms = client->config->connack_timeout_ms;
    if (timeout_ms == 0) {
        timeout_ms = AWS_MQTT5_DEFAULT_CONNACK_PACKET_TIMEOUT_MS;
    }

    uint64_t now = (*client->vtable->get_current_time_fn)();
    client->next_mqtt_connect_packet_timeout_time =
        now + aws_timestamp_convert(timeout_ms, AWS_TIMESTAMP_MILLIS, AWS_TIMESTAMP_NANOS, NULL);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_CLIENT,
        "id=%p: setting CONNECT timeout to %" PRIu64,
        (void *)client,
        client->next_mqtt_connect_packet_timeout_time);
}

static void s_reset_reconnection_delay_time(struct aws_mqtt5_client *client) {
    uint64_t now = (*client->vtable->get_current_time_fn)();
    uint64_t reset_reconnection_delay_time_nanos = aws_timestamp_convert(
        client->config->min_connected_time_to_reset_reconnect_delay_ms,
        AWS_TIMESTAMP_MILLIS,
        AWS_TIMESTAMP_NANOS,
        NULL);
    client->next_reconnect_delay_reset_time_ns = aws_add_u64_saturating(now, reset_reconnection_delay_time_nanos);

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_CLIENT,
        "id=%p: reconnection delay reset time set to %" PRIu64,
        (void *)client,
        client->next_reconnect_delay_reset_time_ns);
}

static void s_aws_mqtt5_client_reset_operations_for_new_connection(struct aws_mqtt5_client *client) {
    (void)client;

    /*
     * TODO:
     *
     *   On reconnect (post CONNACK):
     *      if rejoined_session == false:
     *          Fail, remove, and release unacked_operations
     *
     *      Move-Append unacked_operations to the head of queued_operations
     *      Clear unacked_operations_table
     */
}

static void s_change_current_state_to_connected(struct aws_mqtt5_client *client) {
    AWS_FATAL_ASSERT(client->current_state == AWS_MCS_MQTT_CONNECT);

    client->current_state = AWS_MCS_CONNECTED;

    s_aws_mqtt5_client_reset_operations_for_new_connection(client);

    client->next_ping_timeout_time = 0;
    s_reset_ping(client);
    s_reset_reconnection_delay_time(client);
}

static void s_change_current_state_to_clean_disconnect(struct aws_mqtt5_client *client) {
    (void)client;
    AWS_FATAL_ASSERT(client->current_state == AWS_MCS_MQTT_CONNECT || client->current_state == AWS_MCS_CONNECTED);

    client->current_state = AWS_MCS_CLEAN_DISCONNECT;

    /* TODO: Queue DISCONNECT packet, failure => EnterState(CHANNEL_SHUTDOWN) */
}

static void s_aws_mqtt5_client_reset_offline_queue(struct aws_mqtt5_client *client) {
    if (client->current_operation != NULL) {
        aws_linked_list_push_front(&client->queued_operations, &client->current_operation->node);
        client->current_operation = NULL;
    }

    /*
     * TODO:
     *      If disconnect_queue_policy is fail(x):
     *          Fail, release, and remove everything in queued_operations with property (x)
     *          Release and remove: PUBACK, DISCONNECT
     *      Fail, remove, and release unacked_operations if:
     *          Operation is not Qos 1+ publish
     */
}

static void s_change_current_state_to_channel_shutdown(struct aws_mqtt5_client *client) {
    enum aws_mqtt5_client_state current_state = client->current_state;
    AWS_FATAL_ASSERT(
        current_state == AWS_MCS_MQTT_CONNECT || current_state == AWS_MCS_CONNECTING ||
        current_state == AWS_MCS_CONNECTED || current_state == AWS_MCS_CLEAN_DISCONNECT);

    client->current_state = AWS_MCS_CHANNEL_SHUTDOWN;

    s_aws_mqtt5_client_reset_offline_queue(client);

    /*
     * Critical requirement: The caller must invoke the channel shutdown function themselves (with the desired error
     * code) *after* changing state.
     *
     * The caller is the only one with the error context and we want to be safe and avoid the possibility of a
     * synchronous channel shutdown (mocks) leading to a situation where we get the shutdown callback before we've
     * transitioned into the CHANNEL_SHUTDOWN state.
     *
     * We could relax this if a synchronous channel shutdown is literally impossible even with mocked channels.
     */
}

/* TODO: refactor and reunify with internals of retry strategy to expose these as usable functions in aws-c-io */

static uint64_t s_aws_mqtt5_client_random_in_range(uint64_t from, uint64_t to) {
    uint64_t max = aws_max_u64(from, to);
    uint64_t min = aws_min_u64(from, to);

    uint64_t diff = max - min;
    if (!diff) {
        return min;
    }

    uint64_t random_value = 0;
    if (aws_device_random_u64(&random_value)) {
        return min;
    }

    if (diff == UINT64_MAX) {
        return random_value;
    }

    return min + random_value % (diff + 1); /* + 1 is safe due to previous check */
}

static uint64_t s_aws_mqtt5_compute_reconnect_backoff_no_jitter(struct aws_mqtt5_client *client) {
    uint64_t retry_count = aws_min_u64(client->reconnect_count, 63);
    return aws_mul_u64_saturating((uint64_t)1 << retry_count, client->config->min_reconnect_delay_ms);
}

static uint64_t s_aws_mqtt5_compute_reconnect_backoff_full_jitter(struct aws_mqtt5_client *client) {
    uint64_t non_jittered = s_aws_mqtt5_compute_reconnect_backoff_no_jitter(client);
    return s_aws_mqtt5_client_random_in_range(0, non_jittered);
}

static uint64_t s_compute_deccorelated_jitter(struct aws_mqtt5_client *client) {
    uint64_t last_backoff_val = client->current_reconnect_delay_ms;

    if (!last_backoff_val) {
        return s_aws_mqtt5_compute_reconnect_backoff_full_jitter(client);
    }

    return s_aws_mqtt5_client_random_in_range(
        client->config->min_reconnect_delay_ms, aws_mul_u64_saturating(last_backoff_val, 3));
}

static void s_update_reconnect_delay_for_pending_reconnect(struct aws_mqtt5_client *client) {
    uint64_t delay_ms = 0;

    switch (client->config->retry_jitter_mode) {
        case AWS_EXPONENTIAL_BACKOFF_JITTER_DECORRELATED:
            delay_ms = s_compute_deccorelated_jitter(client);
            break;

        case AWS_EXPONENTIAL_BACKOFF_JITTER_NONE:
            delay_ms = s_aws_mqtt5_compute_reconnect_backoff_no_jitter(client);
            break;

        case AWS_EXPONENTIAL_BACKOFF_JITTER_FULL:
        case AWS_EXPONENTIAL_BACKOFF_JITTER_DEFAULT:
        default:
            delay_ms = s_aws_mqtt5_compute_reconnect_backoff_full_jitter(client);
            break;
    }

    delay_ms = aws_min_u64(delay_ms, client->config->max_reconnect_delay_ms);
    uint64_t now = (*client->vtable->get_current_time_fn)();

    client->next_reconnect_time_ns =
        aws_add_u64_saturating(now, aws_timestamp_convert(delay_ms, AWS_TIMESTAMP_MILLIS, AWS_TIMESTAMP_NANOS, NULL));

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_CLIENT, "id=%p: next connection attempt in %" PRIu64 " milliseconds", (void *)client, delay_ms);

    client->reconnect_count++;
}

static void s_change_current_state_to_pending_reconnect(struct aws_mqtt5_client *client) {
    AWS_ASSERT(client->current_state == AWS_MCS_CONNECTING || client->current_state == AWS_MCS_CHANNEL_SHUTDOWN);

    client->current_state = AWS_MCS_PENDING_RECONNECT;

    s_update_reconnect_delay_for_pending_reconnect(client);

    s_aws_mqtt5_client_reset_offline_queue(client);
}

static void s_change_current_state_to_terminated(struct aws_mqtt5_client *client) {
    client->current_state = AWS_MCS_TERMINATED;

    s_mqtt5_client_final_destroy(client);
}

static void s_change_current_state(struct aws_mqtt5_client *client, enum aws_mqtt5_client_state next_state) {
    AWS_ASSERT(next_state != client->current_state);
    if (next_state == client->current_state) {
        return;
    }

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_CLIENT,
        "id=%p: switching current state from %s to %s",
        (void *)client,
        s_aws_mqtt5_client_state_to_c_str(client->current_state),
        s_aws_mqtt5_client_state_to_c_str(next_state));

    if (client->vtable->on_client_state_change_callback_fn != NULL) {
        (*client->vtable->on_client_state_change_callback_fn)(
            client, client->current_state, next_state, client->vtable->vtable_user_data);
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
            return;
    }

    s_reevaluate_service_task(client);
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
        s_aws_mqtt5_client_emit_final_lifecycle_event(client, AWS_ERROR_MQTT5_USER_REQUESTED_STOP, NULL, NULL);

        /* TODO: use CLEAN_DISCONNECT instead */

        s_aws_mqtt5_client_shutdown_channel(client, AWS_ERROR_MQTT5_USER_REQUESTED_STOP);
        return;
    }

    if (now >= client->next_mqtt_connect_packet_timeout_time) {
        s_aws_mqtt5_client_emit_final_lifecycle_event(client, AWS_ERROR_MQTT5_CONNACK_TIMEOUT, NULL, NULL);

        AWS_LOGF_INFO(AWS_LS_MQTT5_CLIENT, "id=%p: shutting down channel due to CONNACK timeout", (void *)client);

        /* TODO: use clean disconnect rather than channel termination */

        s_aws_mqtt5_client_shutdown_channel(client, AWS_ERROR_MQTT5_CONNACK_TIMEOUT);
        return;
    }

    if (client->current_operation != NULL && !client->pending_write_completion) {
        if (s_aws_mqtt5_client_write_current_operation_only(client)) {
            int error_code = aws_last_error();
            AWS_LOGF_ERROR(
                AWS_LS_MQTT5_CLIENT,
                "id=%p: failed to write CONNECT packet continuation to channel with error %d(%s)",
                (void *)client,
                error_code,
                aws_error_debug_str(error_code));

            s_aws_mqtt5_client_shutdown_channel(client, error_code);
            return;
        }
    }
}

static int s_aws_mqtt5_client_send_ping(struct aws_mqtt5_client *client, uint64_t now) {
    s_reset_ping(client);

    uint64_t ping_timeout_nanos =
        aws_timestamp_convert(client->config->ping_timeout_ms, AWS_TIMESTAMP_MILLIS, AWS_TIMESTAMP_NANOS, NULL);
    client->next_ping_timeout_time = aws_add_u64_saturating(now, ping_timeout_nanos);

    if (client->current_operation != NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_CLIENT, "id=%p: ping timer hit while there's a current outbound operation", (void *)client);
        return AWS_OP_SUCCESS;
    }

    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_CLIENT,
        "id=%p: sending ping with timeout in %" PRIu32 " ms",
        (void *)client,
        client->config->ping_timeout_ms);

    struct aws_mqtt5_operation_pingreq *pingreq_op = aws_mqtt5_operation_pingreq_new(client->allocator);
    if (s_aws_mqtt5_client_set_current_operation(client, &pingreq_op->base)) {
        aws_mqtt5_operation_release(&pingreq_op->base);
        return AWS_OP_ERR;
    }

    if (s_aws_mqtt5_client_write_current_operation_only(client)) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

static void s_service_state_connected(struct aws_mqtt5_client *client, uint64_t now) {
    enum aws_mqtt5_client_state desired_state = client->desired_state;
    if (desired_state != AWS_MCS_CONNECTED) {
        s_aws_mqtt5_client_emit_final_lifecycle_event(client, AWS_ERROR_MQTT5_USER_REQUESTED_STOP, NULL, NULL);

        AWS_LOGF_INFO(AWS_LS_MQTT5_CLIENT, "id=%p: channel shutdown due to user Stop request", (void *)client);

        /* TODO: use clean disconnect rather than channel termination */

        s_aws_mqtt5_client_shutdown_channel(client, AWS_ERROR_MQTT5_USER_REQUESTED_STOP);
        return;
    }

    if (now >= client->next_ping_timeout_time && client->next_ping_timeout_time != 0) {
        s_aws_mqtt5_client_emit_final_lifecycle_event(client, AWS_ERROR_MQTT5_PING_RESPONSE_TIMEOUT, NULL, NULL);

        AWS_LOGF_INFO(AWS_LS_MQTT5_CLIENT, "id=%p: channel shutdown due to PINGRESP timeout", (void *)client);

        /* TODO: use clean disconnect rather than channel termination */

        s_aws_mqtt5_client_shutdown_channel(client, AWS_ERROR_MQTT5_PING_RESPONSE_TIMEOUT);
        return;
    }

    if (now >= client->next_ping_time) {
        if (s_aws_mqtt5_client_send_ping(client, now)) {
            int error_code = aws_last_error();
            AWS_LOGF_ERROR(
                AWS_LS_MQTT5_CLIENT,
                "id=%p: failed to send PINGREQ with error %d(%s)",
                (void *)client,
                error_code,
                aws_error_debug_str(error_code));

            s_aws_mqtt5_client_shutdown_channel(client, error_code);
            return;
        }
    }

    if (now >= client->next_reconnect_delay_reset_time_ns && client->next_reconnect_delay_reset_time_ns != 0) {
        AWS_LOGF_DEBUG(
            AWS_LS_MQTT5_CLIENT,
            "id=%p: connected sufficiently long that reconnect backoff delay has been reset back to "
            "minimum value",
            (void *)client);

        client->reconnect_count = 0;
        client->current_reconnect_delay_ms = 0;
        client->next_reconnect_delay_reset_time_ns = 0;
    }

    /* TODO: flow control, operation queue processing, etc... */
    if (client->current_operation != NULL && !client->pending_write_completion) {
        if (s_aws_mqtt5_client_write_current_operation_only(client)) {
            int error_code = aws_last_error();
            AWS_LOGF_ERROR(
                AWS_LS_MQTT5_CLIENT,
                "id=%p: failed to write current operation with error %d(%s)",
                (void *)client,
                error_code,
                aws_error_debug_str(error_code));

            s_aws_mqtt5_client_shutdown_channel(client, error_code);
            return;
        }
    }
}

static void s_service_state_clean_disconnect(struct aws_mqtt5_client *client) {
    (void)client;

    /* TODO: everything */
}

static void s_service_state_channel_shutdown(struct aws_mqtt5_client *client) {
    (void)client;
}

static void s_service_state_pending_reconnect(struct aws_mqtt5_client *client, uint64_t now) {
    if (client->desired_state != AWS_MCS_CONNECTED) {
        s_change_current_state(client, AWS_MCS_STOPPED);
        return;
    }

    if (now >= client->next_reconnect_time_ns) {
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
    client->next_service_task_run_time = 0;
    client->in_service = true;

    uint64_t now = (*client->vtable->get_current_time_fn)();
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
     * will crash if we access anything anymore.
     */
    if (terminated) {
        return;
    }

    /* we're not scheduled anymore, reschedule as needed */
    client->in_service = false;
    s_reevaluate_service_task(client);
}

static int s_process_read_message(
    struct aws_channel_handler *handler,
    struct aws_channel_slot *slot,
    struct aws_io_message *message) {

    struct aws_mqtt5_client *client = handler->impl;

    if (message->message_type != AWS_IO_MESSAGE_APPLICATION_DATA) {
        AWS_LOGF_ERROR(AWS_LS_MQTT5_CLIENT, "id=%p: unexpected io message data", (void *)client);
        return AWS_OP_ERR;
    }

    AWS_LOGF_TRACE(
        AWS_LS_MQTT5_CLIENT, "id=%p: processing read message of size %zu", (void *)client, message->message_data.len);

    struct aws_byte_cursor message_cursor = aws_byte_cursor_from_buf(&message->message_data);

    int result = aws_mqtt5_decoder_on_data_received(&client->decoder, message_cursor);
    if (result != AWS_OP_SUCCESS) {
        int error_code = aws_last_error();
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_CLIENT,
            "id=%p: decode failure with error %d(%s)",
            (void *)client,
            error_code,
            aws_error_debug_str(error_code));

        /* TODO: conditional clean disconnect */
        s_aws_mqtt5_client_shutdown_channel(client, error_code);
        return AWS_OP_ERR;
    }

    aws_channel_slot_increment_read_window(slot, message->message_data.len);
    aws_mem_release(message->allocator, message);

    return AWS_OP_SUCCESS;
}

static int s_shutdown(
    struct aws_channel_handler *handler,
    struct aws_channel_slot *slot,
    enum aws_channel_direction dir,
    int error_code,
    bool free_scarce_resources_immediately) {

    (void)handler;

    return aws_channel_slot_on_handler_shutdown_complete(slot, dir, error_code, free_scarce_resources_immediately);
}

static size_t s_initial_window_size(struct aws_channel_handler *handler) {
    (void)handler;

    return SIZE_MAX;
}

static void s_destroy(struct aws_channel_handler *handler) {
    (void)handler;
}

static size_t s_message_overhead(struct aws_channel_handler *handler) {
    (void)handler;

    return 0;
}

static struct aws_channel_handler_vtable s_mqtt5_channel_handler_vtable = {
    .process_read_message = &s_process_read_message,
    .process_write_message = NULL,
    .increment_read_window = NULL,
    .shutdown = &s_shutdown,
    .initial_window_size = &s_initial_window_size,
    .message_overhead = &s_message_overhead,
    .destroy = &s_destroy,
};

static bool s_aws_is_successful_reason_code(int value) {
    return value < 128;
}

static void s_aws_mqtt5_client_on_connack(
    struct aws_mqtt5_client *client,
    struct aws_mqtt5_packet_connack_view *connack_view) {
    AWS_FATAL_ASSERT(client->current_state == AWS_MCS_MQTT_CONNECT);

    bool is_successful = s_aws_is_successful_reason_code((int)connack_view->reason_code);
    if (!is_successful) {
        s_aws_mqtt5_client_emit_final_lifecycle_event(
            client, AWS_ERROR_MQTT5_CONNACK_CONNECTION_REFUSED, connack_view, NULL);

        enum aws_mqtt5_connect_reason_code reason_code = connack_view->reason_code;

        AWS_LOGF_INFO(
            AWS_LS_MQTT5_CLIENT,
            "id=%p: connection refused (via failed CONNACK) by remote host with reason code %d(%s)",
            (void *)client,
            (int)reason_code,
            aws_mqtt5_connect_reason_code_to_c_string(reason_code));

        s_aws_mqtt5_client_shutdown_channel(client, AWS_ERROR_MQTT5_CONNACK_CONNECTION_REFUSED);
        return;
    }

    aws_mqtt5_negotiated_settings_apply_connack(&client->negotiated_settings, connack_view);

    s_change_current_state(client, AWS_MCS_CONNECTED);
    s_aws_mqtt5_client_emit_connection_success_lifecycle_event(client, connack_view);
}

static void s_aws_mqtt5_client_log_received_packet(
    struct aws_mqtt5_client *client,
    enum aws_mqtt5_packet_type type,
    void *packet_view) {
    AWS_LOGF_DEBUG(
        AWS_LS_MQTT5_CLIENT, "id=%p: Received %s packet", (void *)client, aws_mqtt5_packet_type_to_c_string(type));

    switch (type) {
        case AWS_MQTT5_PT_CONNACK:
            aws_mqtt5_packet_connack_view_log(packet_view, AWS_LL_DEBUG);
            break;

        case AWS_MQTT5_PT_PUBLISH:
            aws_mqtt5_packet_publish_view_log(packet_view, AWS_LL_TRACE);
            break;

        case AWS_MQTT5_PT_PUBACK:
            /* TODO: puback view not impl yet */
            break;

        case AWS_MQTT5_PT_SUBACK:
            /* TODO: suback view not impl yet */
            break;

        case AWS_MQTT5_PT_UNSUBACK:
            /* TODO: unsuback view not impl yet */
            break;

        case AWS_MQTT5_PT_PINGRESP:
            break; /* nothing to log */

        case AWS_MQTT5_PT_DISCONNECT:
            aws_mqtt5_packet_disconnect_view_log(packet_view, AWS_LL_DEBUG);
            break;

        default:
            break;
    }
}

static void s_aws_mqtt5_client_mqtt_connect_on_packet_received(
    struct aws_mqtt5_client *client,
    enum aws_mqtt5_packet_type type,
    void *packet_view) {
    if (type == AWS_MQTT5_PT_CONNACK) {
        s_aws_mqtt5_client_on_connack(client, (struct aws_mqtt5_packet_connack_view *)packet_view);
    } else {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_CLIENT, "id=%p: Invalid packet type received while in MQTT_CONNECT state", (void *)client);

        s_aws_mqtt5_client_shutdown_channel(client, AWS_ERROR_MQTT5_DECODE_PROTOCOL_ERROR);
    }
}

static void s_aws_mqtt5_client_connected_on_packet_received(
    struct aws_mqtt5_client *client,
    enum aws_mqtt5_packet_type type,
    void *packet_view) {

    switch (type) {
        case AWS_MQTT5_PT_PINGRESP:
            AWS_LOGF_DEBUG(AWS_LS_MQTT5_CLIENT, "id=%p: resetting PINGREQ timer", (void *)client);

            client->next_ping_timeout_time = 0;
            break;

        case AWS_MQTT5_PT_DISCONNECT:
            s_aws_mqtt5_client_emit_final_lifecycle_event(
                client, AWS_ERROR_MQTT5_DISCONNECT_RECEIVED, NULL, packet_view);

            AWS_LOGF_INFO(AWS_LS_MQTT5_CLIENT, "id=%p: shutting down channel due to DISCONNECT", (void *)client);

            s_aws_mqtt5_client_shutdown_channel(client, AWS_ERROR_MQTT5_DISCONNECT_RECEIVED);
            break;

        default:
            break;
    }
}

static int s_aws_mqtt5_client_on_packet_received(
    enum aws_mqtt5_packet_type type,
    void *packet_view,
    void *decoder_callback_user_data) {

    struct aws_mqtt5_client *client = decoder_callback_user_data;

    s_aws_mqtt5_client_log_received_packet(client, type, packet_view);

    switch (client->current_state) {
        case AWS_MCS_MQTT_CONNECT:
            s_aws_mqtt5_client_mqtt_connect_on_packet_received(client, type, packet_view);
            break;

        case AWS_MCS_CONNECTED:
            s_aws_mqtt5_client_connected_on_packet_received(client, type, packet_view);
            break;

        /* TODO: all other cases */
        default:
            break;
    }

    s_reevaluate_service_task(client);

    return AWS_OP_SUCCESS;
}

static int s_aws_mqtt5_client_on_publish_payload_received(
    struct aws_mqtt5_packet_publish_view *publish_view,
    struct aws_byte_cursor payload,
    void *decoder_callback_user_data) {
    (void)publish_view;
    (void)payload;
    (void)decoder_callback_user_data;

    /* TODO: implement */

    return aws_raise_error(AWS_ERROR_UNIMPLEMENTED);
}

static uint64_t s_aws_high_res_clock_get_ticks_proxy(void) {
    uint64_t current_time = 0;
    AWS_FATAL_ASSERT(aws_high_res_clock_get_ticks(&current_time) == AWS_OP_SUCCESS);

    return current_time;
}

static struct aws_mqtt5_client_vtable s_default_client_vtable = {
    .get_current_time_fn = s_aws_high_res_clock_get_ticks_proxy,
    .channel_shutdown_fn = aws_channel_shutdown,
    .websocket_connect_fn = aws_websocket_client_connect,
    .client_bootstrap_new_socket_channel_fn = aws_client_bootstrap_new_socket_channel,
    .http_proxy_new_socket_channel_fn = aws_http_proxy_new_socket_channel,

    .vtable_user_data = NULL,
};

void aws_mqtt5_client_set_vtable(struct aws_mqtt5_client *client, const struct aws_mqtt5_client_vtable *vtable) {
    client->vtable = vtable;
}

const struct aws_mqtt5_client_vtable *aws_mqtt5_client_get_default_vtable(void) {
    return &s_default_client_vtable;
}

struct aws_mqtt5_client *aws_mqtt5_client_new(
    struct aws_allocator *allocator,
    const struct aws_mqtt5_client_options *options) {
    AWS_FATAL_ASSERT(allocator != NULL);
    AWS_FATAL_ASSERT(options != NULL);

    struct aws_mqtt5_client *client = aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt5_client));
    if (client == NULL) {
        return NULL;
    }

    aws_task_init(&client->service_task, s_mqtt5_service_task_fn, client, "Mqtt5Service");

    client->allocator = allocator;
    client->vtable = &s_default_client_vtable;

    aws_ref_count_init(&client->ref_count, client, s_on_mqtt5_client_zero_ref_count);

    aws_linked_list_init(&client->queued_operations);
    aws_linked_list_init(&client->write_completion_operations);
    aws_linked_list_init(&client->unacked_operations);

    client->config = aws_mqtt5_client_options_storage_new(allocator, options);
    if (client->config == NULL) {
        goto on_error;
    }

    /* all client activity will take place on this event loop, serializing things like reconnect, ping, etc... */
    client->loop = aws_event_loop_group_get_next_loop(client->config->bootstrap->event_loop_group);
    if (client->loop == NULL) {
        goto on_error;
    }

    client->desired_state = AWS_MCS_STOPPED;
    client->current_state = AWS_MCS_STOPPED;
    client->lifecycle_state = AWS_MQTT5_LS_NONE;

    client->next_mqtt_packet_id = 1;

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

    struct aws_mqtt5_decoder_options decoder_options = {
        .callback_user_data = client,
        .on_packet_received = s_aws_mqtt5_client_on_packet_received,
        .on_publish_payload_data = s_aws_mqtt5_client_on_publish_payload_received,
    };

    if (aws_mqtt5_decoder_init(&client->decoder, allocator, &decoder_options)) {
        goto on_error;
    }

    struct aws_mqtt5_encoder_options encoder_options = {
        .client = client,
    };

    if (aws_mqtt5_encoder_init(&client->encoder, allocator, &encoder_options)) {
        goto on_error;
    }

    client->current_reconnect_delay_ms = 0;

    client->handler.alloc = client->allocator;
    client->handler.vtable = &s_mqtt5_channel_handler_vtable;
    client->handler.impl = client;

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

struct aws_mqtt5_client *aws_mqtt5_client_release(struct aws_mqtt5_client *client) {
    if (client != NULL) {
        aws_ref_count_release(&client->ref_count);
    }

    return NULL;
}

struct aws_mqtt_change_desired_state_task {
    struct aws_task task;
    struct aws_allocator *allocator;
    struct aws_mqtt5_client *client;
    enum aws_mqtt5_client_state desired_state;
    struct aws_mqtt5_operation_disconnect *disconnect_operation;
};

static void s_change_state_task_fn(struct aws_task *task, void *arg, enum aws_task_status status) {
    (void)task;

    struct aws_mqtt_change_desired_state_task *change_state_task = arg;
    struct aws_mqtt5_client *client = change_state_task->client;
    enum aws_mqtt5_client_state desired_state = change_state_task->desired_state;
    if (status != AWS_TASK_STATUS_RUN_READY) {
        goto done;
    }

    if (client->desired_state != desired_state) {
        AWS_LOGF_INFO(
            AWS_LS_MQTT5_CLIENT,
            "id=%p: changing desired client state from %s to %s",
            (void *)client,
            s_aws_mqtt5_client_state_to_c_str(client->desired_state),
            s_aws_mqtt5_client_state_to_c_str(desired_state));

        client->desired_state = desired_state;
        aws_mqtt5_operation_disconnect_release(client->disconnect_operation);
        client->disconnect_operation = aws_mqtt5_operation_disconnect_acquire(change_state_task->disconnect_operation);

        s_reevaluate_service_task(client);
    }

done:

    aws_mqtt5_operation_disconnect_release(change_state_task->disconnect_operation);
    if (desired_state != AWS_MCS_TERMINATED) {
        aws_mqtt5_client_release(client);
    }

    aws_mem_release(change_state_task->allocator, change_state_task);
}

static struct aws_mqtt_change_desired_state_task *s_aws_mqtt_change_desired_state_task_new(
    struct aws_allocator *allocator,
    struct aws_mqtt5_client *client,
    enum aws_mqtt5_client_state desired_state,
    struct aws_mqtt5_operation_disconnect *disconnect_operation) {

    struct aws_mqtt_change_desired_state_task *change_state_task =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_change_desired_state_task));
    if (change_state_task == NULL) {
        return NULL;
    }

    aws_task_init(&change_state_task->task, s_change_state_task_fn, (void *)change_state_task, "ChangeStateTask");

    change_state_task->allocator = client->allocator;
    change_state_task->client = (desired_state == AWS_MCS_TERMINATED) ? client : aws_mqtt5_client_acquire(client);
    change_state_task->desired_state = desired_state;
    change_state_task->disconnect_operation = aws_mqtt5_operation_disconnect_acquire(disconnect_operation);

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
    enum aws_mqtt5_client_state desired_state,
    struct aws_mqtt5_operation_disconnect *disconnect_operation) {
    AWS_FATAL_ASSERT(client != NULL);
    AWS_FATAL_ASSERT(client->loop != NULL);
    AWS_FATAL_ASSERT(disconnect_operation == NULL || desired_state == AWS_MCS_STOPPED);

    if (!s_is_valid_desired_state(desired_state)) {
        AWS_LOGF_ERROR(
            AWS_LS_MQTT5_CLIENT,
            "id=%p: invalid desired state argument %d(%s)",
            (void *)client,
            (int)desired_state,
            s_aws_mqtt5_client_state_to_c_str(desired_state));

        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    struct aws_mqtt_change_desired_state_task *task =
        s_aws_mqtt_change_desired_state_task_new(client->allocator, client, desired_state, disconnect_operation);
    if (task == NULL) {
        AWS_LOGF_ERROR(AWS_LS_MQTT5_CLIENT, "id=%p: failed to create change desired state task", (void *)client);
        return AWS_OP_ERR;
    }

    aws_event_loop_schedule_task_now(client->loop, &task->task);

    return AWS_OP_SUCCESS;
}

int aws_mqtt5_client_start(struct aws_mqtt5_client *client) {
    return s_aws_mqtt5_client_change_desired_state(client, AWS_MCS_CONNECTED, NULL);
}

int aws_mqtt5_client_stop(struct aws_mqtt5_client *client, const struct aws_mqtt5_packet_disconnect_view *options) {
    struct aws_mqtt5_operation_disconnect *disconnect_op = NULL;
    if (options != NULL) {
        disconnect_op = aws_mqtt5_operation_disconnect_new(client->allocator, options);
        if (disconnect_op == NULL) {
            AWS_LOGF_ERROR(
                AWS_LS_MQTT5_CLIENT, "id=%p: failed to create requested DISCONNECT operation", (void *)client);
            return AWS_OP_ERR;
        }
    }

    int result = s_aws_mqtt5_client_change_desired_state(client, AWS_MCS_STOPPED, disconnect_op);

    aws_mqtt5_operation_disconnect_release(disconnect_op);

    return result;
}

struct aws_mqtt5_submit_operation_task {
    struct aws_task task;
    struct aws_allocator *allocator;
    struct aws_mqtt5_client *client;
    struct aws_mqtt5_operation *operation;
};

static void s_mqtt5_submit_operation_task_fn(struct aws_task *task, void *arg, enum aws_task_status status) {
    (void)task;

    struct aws_mqtt5_submit_operation_task *submit_operation_task = arg;
    if (status != AWS_TASK_STATUS_RUN_READY) {
        goto error;
    }

    aws_mqtt5_operation_acquire(submit_operation_task->operation);
    s_enqueue_operation(submit_operation_task->client, submit_operation_task->operation);

    goto done;

error:

    /* TODO: any failure or cancel should also result in errored completion callback on the operation */
    ;

done:

    aws_mqtt5_operation_release(submit_operation_task->operation);
    aws_mqtt5_client_release(submit_operation_task->client);

    aws_mem_release(submit_operation_task->allocator, submit_operation_task);
}

static int s_submit_operation(struct aws_mqtt5_client *client, struct aws_mqtt5_operation *operation) {
    struct aws_mqtt5_submit_operation_task *submit_task =
        aws_mem_calloc(client->allocator, 1, sizeof(struct aws_mqtt5_submit_operation_task));
    if (submit_task == NULL) {
        return AWS_OP_ERR;
    }

    aws_task_init(&submit_task->task, s_mqtt5_submit_operation_task_fn, submit_task, "Mqtt5SubmitOperation");
    submit_task->allocator = client->allocator;
    submit_task->client = aws_mqtt5_client_acquire(client);
    submit_task->operation = operation;

    aws_event_loop_schedule_task_now(client->loop, &submit_task->task);

    return AWS_OP_SUCCESS;
}

int aws_mqtt5_client_publish(
    struct aws_mqtt5_client *client,
    const struct aws_mqtt5_packet_publish_view *publish_options,
    const struct aws_mqtt5_publish_completion_options *completion_options) {

    AWS_PRECONDITION(client != NULL);
    AWS_PRECONDITION(publish_options != NULL);

    struct aws_mqtt5_operation_publish *publish_op =
        aws_mqtt5_operation_publish_new(client->allocator, publish_options, completion_options);
    if (publish_op == NULL) {
        return AWS_OP_ERR;
    }

    if (s_submit_operation(client, &publish_op->base)) {
        goto error;
    }

    return AWS_OP_SUCCESS;

error:

    aws_mqtt5_operation_release(&publish_op->base);

    return AWS_OP_ERR;
}

int aws_mqtt5_client_subscribe(
    struct aws_mqtt5_client *client,
    const struct aws_mqtt5_packet_subscribe_view *subscribe_options,
    const struct aws_mqtt5_subscribe_completion_options *completion_options) {

    AWS_PRECONDITION(client != NULL);
    AWS_PRECONDITION(subscribe_options != NULL);

    struct aws_mqtt5_operation_subscribe *subscribe_op =
        aws_mqtt5_operation_subscribe_new(client->allocator, subscribe_options, completion_options);
    if (subscribe_op == NULL) {
        return AWS_OP_ERR;
    }

    if (s_submit_operation(client, &subscribe_op->base)) {
        goto error;
    }

    return AWS_OP_SUCCESS;

error:

    aws_mqtt5_operation_release(&subscribe_op->base);

    return AWS_OP_ERR;
}

int aws_mqtt5_client_unsubscribe(
    struct aws_mqtt5_client *client,
    const struct aws_mqtt5_packet_unsubscribe_view *unsubscribe_options,
    const struct aws_mqtt5_unsubscribe_completion_options *completion_options) {

    AWS_PRECONDITION(client != NULL);
    AWS_PRECONDITION(unsubscribe_options != NULL);

    struct aws_mqtt5_operation_unsubscribe *unsubscribe_op =
        aws_mqtt5_operation_unsubscribe_new(client->allocator, unsubscribe_options, completion_options);
    if (unsubscribe_op == NULL) {
        return AWS_OP_ERR;
    }

    if (s_submit_operation(client, &unsubscribe_op->base)) {
        goto error;
    }

    return AWS_OP_SUCCESS;

error:

    aws_mqtt5_operation_release(&unsubscribe_op->base);

    return AWS_OP_ERR;
}
