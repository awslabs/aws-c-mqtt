/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/client.h>
#include <aws/mqtt/private/client_impl_shared.h>

#include <aws/io/socks5.h>
#include <aws/testing/aws_test_harness.h>

/*
 * Fake implementation to track SOCKS5 proxy option usage and connection calls.
 */
struct fake_socks5_impl {
    struct aws_socks5_proxy_options *last_options;
    struct aws_socks5_proxy_options *expected_options;
    bool set_called;
    bool connect_called;
};

/* Called when SOCKS5 proxy options are set on the connection. */
static int s_fake_set_socks5(void *impl, struct aws_socks5_proxy_options *options) {
    struct fake_socks5_impl *fake = impl;
    fake->set_called = true;
    fake->last_options = options;
    return AWS_OP_SUCCESS;
}

/* Called when connect is invoked; checks that SOCKS5 options were set. */
static int s_fake_connect(void *impl, const struct aws_mqtt_connection_options *connection_options) {
    (void)connection_options;
    struct fake_socks5_impl *fake = impl;
    fake->connect_called = true;
    ASSERT_PTR_EQUALS(fake->expected_options, fake->last_options);
    return AWS_OP_SUCCESS;
}

/*
 * Vtable with only SOCKS5 proxy and connect functions implemented for testing.
 */
static struct aws_mqtt_client_connection_vtable s_fake_vtable = {
    .acquire_fn = NULL,
    .release_fn = NULL,
    .set_will_fn = NULL,
    .set_login_fn = NULL,
    .use_websockets_fn = NULL,
    .set_http_proxy_options_fn = NULL,
    .set_socks5_proxy_options_fn = s_fake_set_socks5,
    .set_host_resolution_options_fn = NULL,
    .set_reconnect_timeout_fn = NULL,
    .set_connection_interruption_handlers_fn = NULL,
    .set_connection_result_handlers = NULL,
    .set_connection_closed_handler_fn = NULL,
    .set_on_any_publish_handler_fn = NULL,
    .set_connection_termination_handler_fn = NULL,
    .connect_fn = s_fake_connect,
    .reconnect_fn = NULL,
    .disconnect_fn = NULL,
    .subscribe_multiple_fn = NULL,
    .subscribe_fn = NULL,
    .resubscribe_existing_topics_fn = NULL,
    .unsubscribe_fn = NULL,
    .publish_fn = NULL,
    .get_stats_fn = NULL,
};

/*
 * Test: Setting SOCKS5 proxy options on the connection invokes the vtable callback.
 */
static int s_mqtt_set_socks5_proxy_options_invokes_vtable(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct aws_socks5_proxy_options socks5_options;
    ASSERT_SUCCESS(aws_socks5_proxy_options_init(
        &socks5_options, allocator, aws_byte_cursor_from_c_str("proxy.example.com"), 1080));

    struct fake_socks5_impl fake_impl;
    AWS_ZERO_STRUCT(fake_impl);
    struct aws_mqtt_client_connection connection = {
        .vtable = &s_fake_vtable,
        .impl = &fake_impl,
    };

    ASSERT_SUCCESS(aws_mqtt_client_connection_set_socks5_proxy_options(&connection, &socks5_options));
    ASSERT_TRUE(fake_impl.set_called);
    ASSERT_PTR_EQUALS(&socks5_options, fake_impl.last_options);

    aws_socks5_proxy_options_clean_up(&socks5_options);
    return AWS_OP_SUCCESS;
}
AWS_TEST_CASE(mqtt_set_socks5_proxy_options_invokes_vtable, s_mqtt_set_socks5_proxy_options_invokes_vtable);

/*
 * Test: After setting SOCKS5 proxy options, connect uses the correct options.
 */
static int s_mqtt_connect_uses_socks5_after_set(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct aws_socks5_proxy_options socks5_options;
    ASSERT_SUCCESS(aws_socks5_proxy_options_init(
        &socks5_options, allocator, aws_byte_cursor_from_c_str("proxy.example.com"), 1080));

    struct fake_socks5_impl fake_impl;
    AWS_ZERO_STRUCT(fake_impl);
    fake_impl.expected_options = &socks5_options;

    struct aws_mqtt_client_connection connection = {
        .vtable = &s_fake_vtable,
        .impl = &fake_impl,
    };

    ASSERT_SUCCESS(aws_mqtt_client_connection_set_socks5_proxy_options(&connection, &socks5_options));
    ASSERT_TRUE(fake_impl.set_called);

    struct aws_mqtt_connection_options connect_options;
    AWS_ZERO_STRUCT(connect_options);
    ASSERT_SUCCESS(aws_mqtt_client_connection_connect(&connection, &connect_options));
    ASSERT_TRUE(fake_impl.connect_called);

    aws_socks5_proxy_options_clean_up(&socks5_options);
    return AWS_OP_SUCCESS;
}
AWS_TEST_CASE(mqtt_connect_uses_socks5_after_set, s_mqtt_connect_uses_socks5_after_set);
