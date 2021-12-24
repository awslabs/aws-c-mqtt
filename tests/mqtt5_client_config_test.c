/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/v5/mqtt5_client_config.h>
#include <aws/mqtt/v5/private/mqtt5_client_impl.h>

#include <aws/testing/aws_test_harness.h>

static int s_mqtt5_client_config_new_destroy_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct aws_mqtt5_client_config *config = aws_mqtt5_client_config_new(allocator);
    aws_mqtt5_client_config_destroy(config);

    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_client_config_new_destroy, s_mqtt5_client_config_new_destroy_fn)

/*
aws_mqtt5_client_config_set_host_name
aws_mqtt5_client_config_set_port
aws_mqtt5_client_config_set_bootstrap
aws_mqtt5_client_config_set_socket_options
aws_mqtt5_client_config_set_tls_connection_options
aws_mqtt5_client_config_set_http_proxy_host_name
aws_mqtt5_client_config_set_http_proxy_port
aws_mqtt5_client_config_set_http_proxy_tls_connection_options
aws_mqtt5_client_config_set_http_proxy_strategy
aws_mqtt5_client_config_set_websocket_transform
aws_mqtt5_client_config_set_websocket_transform_user_data
aws_mqtt5_client_config_set_reconnect_behavior
aws_mqtt5_client_config_set_reconnect_delay
aws_mqtt5_client_config_set_reconnect_delay_reset_interval
aws_mqtt5_client_config_set_keep_alive_interval_ms
aws_mqtt5_client_config_set_ping_timeout_ms
aws_mqtt5_client_config_set_client_id
aws_mqtt5_client_config_set_connect_username
aws_mqtt5_client_config_set_connect_password
aws_mqtt5_client_config_set_connect_session_expiry_interval_seconds
aws_mqtt5_client_config_set_connect_session_behavior
aws_mqtt5_client_config_set_connect_authentication_method
aws_mqtt5_client_config_set_connect_authentication_data
aws_mqtt5_client_config_set_connect_request_response_information
aws_mqtt5_client_config_set_connect_request_problem_information
aws_mqtt5_client_config_set_connect_receive_maximum
aws_mqtt5_client_config_set_connect_topic_alias_maximum
aws_mqtt5_client_config_set_connect_maximum_packet_size
aws_mqtt5_client_config_add_connect_user_property
aws_mqtt5_client_config_set_will_payload_format
aws_mqtt5_client_config_set_will_message_expiry
aws_mqtt5_client_config_set_will_content_type
aws_mqtt5_client_config_set_will_response_topic
aws_mqtt5_client_config_set_will_correlation_data
aws_mqtt5_client_config_set_will_delay
aws_mqtt5_client_config_set_willvoid
aws_mqtt5_client_config_set_will_retained
aws_mqtt5_client_config_add_will_user_property
aws_mqtt5_client_config_set_lifecycle_event_handler
aws_mqtt5_client_config_set_lifecycle_event_handler_user_data
*/

static int s_set_config(struct aws_mqtt5_client_config *config) {

    return AWS_OP_SUCCESS;
}

static int s_verify_set_config(struct aws_mqtt5_client_config *config) {

    return AWS_OP_SUCCESS;
}

static int s_mqtt5_client_config_set_all_once_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct aws_mqtt5_client_config *config = aws_mqtt5_client_config_new(allocator);

    ASSERT_SUCCESS(s_set_config(config));
    ASSERT_SUCCESS(s_verify_set_config(config));

    aws_mqtt5_client_config_destroy(config);

    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_client_config_set_all_once, s_mqtt5_client_config_set_all_once_fn)

static int s_set_config_alt(struct aws_mqtt5_client_config *config) {

    return AWS_OP_SUCCESS;
}

static int s_mqtt5_client_config_set_all_twice_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct aws_mqtt5_client_config *config = aws_mqtt5_client_config_new(allocator);

    ASSERT_SUCCESS(s_set_config_alt(config));
    ASSERT_SUCCESS(s_set_config(config));
    ASSERT_SUCCESS(s_verify_set_config(config));

    aws_mqtt5_client_config_destroy(config);

    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_client_config_set_all_twice, s_mqtt5_client_config_set_all_twice_fn)

static int s_mqtt5_client_config_clone_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_mqtt_library_init(allocator);

    struct aws_mqtt5_client_config *config = aws_mqtt5_client_config_new(allocator);

    ASSERT_SUCCESS(s_set_config(config));

    struct aws_mqtt5_client_config *config_clone = aws_mqtt5_client_config_new_clone(allocator, config);
    ASSERT_SUCCESS(s_verify_set_config(config_clone));

    aws_mqtt5_client_config_destroy(config);
    aws_mqtt5_client_config_destroy(config_clone);

    aws_mqtt_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(mqtt5_client_config_clone, s_mqtt5_client_config_clone_fn)
