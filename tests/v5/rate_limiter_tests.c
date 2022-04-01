/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>
#include <aws/mqtt/private/v5/rate_limiters.h>

#include <aws/common/clock.h>
#include <aws/testing/aws_test_harness.h>

static uint64_t s_test_time = 0;
static int s_get_test_time(uint64_t *time) {
    *time = s_test_time;

    return AWS_OP_SUCCESS;
}

static int s_rate_limiter_token_bucket_init_invalid_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    (void)allocator;

    struct aws_rate_limiter_token_bucket token_bucket;

    struct aws_rate_limiter_token_bucket_options invalid_options1 = {
        .clock_fn = s_get_test_time,
        .tokens_per_second = 0,
        .maximum_token_count = 1,
    };

    ASSERT_FAILS(aws_rate_limiter_token_bucket_init(&token_bucket, &invalid_options1));

    struct aws_rate_limiter_token_bucket_options invalid_options2 = {
        .clock_fn = s_get_test_time,
        .tokens_per_second = 1000,
        .maximum_token_count = 0,
    };

    ASSERT_FAILS(aws_rate_limiter_token_bucket_init(&token_bucket, &invalid_options2));

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(rate_limiter_token_bucket_init_invalid, s_rate_limiter_token_bucket_init_invalid_fn)

static int s_rate_limiter_token_bucket_regeneration_integral_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    (void)allocator;

    struct aws_rate_limiter_token_bucket_options options = {
        .clock_fn = s_get_test_time,
        .tokens_per_second = 5,
        .maximum_token_count = 10,
    };

    struct aws_rate_limiter_token_bucket token_bucket;
    ASSERT_SUCCESS(aws_rate_limiter_token_bucket_init(&token_bucket, &options));

    ASSERT_TRUE(aws_rate_limiter_token_bucket_can_take_tokens(&token_bucket, 0));
    ASSERT_FALSE(aws_rate_limiter_token_bucket_can_take_tokens(&token_bucket, 1));
    ASSERT_SUCCESS(aws_rate_limiter_token_bucket_take_tokens(&token_bucket, 0));
    ASSERT_FAILS(aws_rate_limiter_token_bucket_take_tokens(&token_bucket, 1));
    ASSERT_INT_EQUALS(0, aws_rate_limiter_token_bucket_compute_wait_for_tokens(&token_bucket, 0));
    ASSERT_INT_EQUALS(2 * AWS_TIMESTAMP_NANOS, aws_rate_limiter_token_bucket_compute_wait_for_tokens(&token_bucket, 10));

    /* one second elapsed, should be able to take 5 tokens now */
    s_test_time = AWS_TIMESTAMP_NANOS;
    ASSERT_TRUE(aws_rate_limiter_token_bucket_can_take_tokens(&token_bucket, 5));
    ASSERT_FALSE(aws_rate_limiter_token_bucket_can_take_tokens(&token_bucket, 6));
    ASSERT_INT_EQUALS(AWS_TIMESTAMP_NANOS, aws_rate_limiter_token_bucket_compute_wait_for_tokens(&token_bucket, 10));
    ASSERT_INT_EQUALS(0, aws_rate_limiter_token_bucket_compute_wait_for_tokens(&token_bucket, 1));
    ASSERT_INT_EQUALS(0, aws_rate_limiter_token_bucket_compute_wait_for_tokens(&token_bucket, 5));
    ASSERT_SUCCESS(aws_rate_limiter_token_bucket_take_tokens(&token_bucket, 5));
    ASSERT_FALSE(aws_rate_limiter_token_bucket_can_take_tokens(&token_bucket, 1));
    ASSERT_FAILS(aws_rate_limiter_token_bucket_take_tokens(&token_bucket, 1));
    ASSERT_INT_EQUALS(2 * AWS_TIMESTAMP_NANOS, aws_rate_limiter_token_bucket_compute_wait_for_tokens(&token_bucket, 10));

    /* three more elapsed seconds, regen should be maxed but clamped */
    s_test_time += 3 * (uint64_t)AWS_TIMESTAMP_NANOS;
    ASSERT_TRUE(aws_rate_limiter_token_bucket_can_take_tokens(&token_bucket, 10));
    ASSERT_FALSE(aws_rate_limiter_token_bucket_can_take_tokens(&token_bucket, 11));
    ASSERT_INT_EQUALS(0, aws_rate_limiter_token_bucket_compute_wait_for_tokens(&token_bucket, 10));
    ASSERT_SUCCESS(aws_rate_limiter_token_bucket_take_tokens(&token_bucket, 10));
    ASSERT_FALSE(aws_rate_limiter_token_bucket_can_take_tokens(&token_bucket, 1));
    ASSERT_FAILS(aws_rate_limiter_token_bucket_take_tokens(&token_bucket, 1));
    ASSERT_INT_EQUALS(2 * (uint64_t)AWS_TIMESTAMP_NANOS, aws_rate_limiter_token_bucket_compute_wait_for_tokens(&token_bucket, 10));

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(rate_limiter_token_bucket_regeneration_integral, s_rate_limiter_token_bucket_regeneration_integral_fn)

static int s_rate_limiter_token_bucket_regeneration_fractional_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    (void)allocator;

    struct aws_rate_limiter_token_bucket_options options = {
        .clock_fn = s_get_test_time,
        .initial_token_count = 2,
        .tokens_per_second = 3,
        .maximum_token_count = 20,
    };

    struct aws_rate_limiter_token_bucket token_bucket;
    ASSERT_SUCCESS(aws_rate_limiter_token_bucket_init(&token_bucket, &options));

    ASSERT_TRUE(aws_rate_limiter_token_bucket_can_take_tokens(&token_bucket, 2));
    ASSERT_FALSE(aws_rate_limiter_token_bucket_can_take_tokens(&token_bucket, 3));
    ASSERT_INT_EQUALS(AWS_TIMESTAMP_NANOS / 3 + 1, aws_rate_limiter_token_bucket_compute_wait_for_tokens(&token_bucket, 3));
    ASSERT_INT_EQUALS(AWS_TIMESTAMP_NANOS, aws_rate_limiter_token_bucket_compute_wait_for_tokens(&token_bucket, 5));

    /* one nanosecond elapsed, wait should shrink by 1 */
    s_test_time = 1;
    ASSERT_INT_EQUALS(AWS_TIMESTAMP_NANOS / 3, aws_rate_limiter_token_bucket_compute_wait_for_tokens(&token_bucket, 3));
    /* idempotent check */
    ASSERT_INT_EQUALS(AWS_TIMESTAMP_NANOS / 3, aws_rate_limiter_token_bucket_compute_wait_for_tokens(&token_bucket, 3));
    ASSERT_INT_EQUALS(AWS_TIMESTAMP_NANOS - 1, aws_rate_limiter_token_bucket_compute_wait_for_tokens(&token_bucket, 5));

    s_test_time = 2;
    ASSERT_INT_EQUALS(AWS_TIMESTAMP_NANOS / 3 - 1, aws_rate_limiter_token_bucket_compute_wait_for_tokens(&token_bucket, 3));
    ASSERT_INT_EQUALS(AWS_TIMESTAMP_NANOS - 2, aws_rate_limiter_token_bucket_compute_wait_for_tokens(&token_bucket, 5));

    /* one nanosecond short of a token's worth of time, nothing should change */
    s_test_time = AWS_TIMESTAMP_NANOS / 3;
    ASSERT_TRUE(aws_rate_limiter_token_bucket_can_take_tokens(&token_bucket, 2));
    ASSERT_FALSE(aws_rate_limiter_token_bucket_can_take_tokens(&token_bucket, 3));
    ASSERT_INT_EQUALS(1, aws_rate_limiter_token_bucket_compute_wait_for_tokens(&token_bucket, 3));
    ASSERT_INT_EQUALS(AWS_TIMESTAMP_NANOS - AWS_TIMESTAMP_NANOS / 3, aws_rate_limiter_token_bucket_compute_wait_for_tokens(&token_bucket, 5));

    /* one more nanosecond, should give us a token */
    s_test_time += 1;
    ASSERT_TRUE(aws_rate_limiter_token_bucket_can_take_tokens(&token_bucket, 3));
    ASSERT_FALSE(aws_rate_limiter_token_bucket_can_take_tokens(&token_bucket, 4));
    ASSERT_INT_EQUALS(0, aws_rate_limiter_token_bucket_compute_wait_for_tokens(&token_bucket, 3));
    ASSERT_INT_EQUALS(AWS_TIMESTAMP_NANOS / 3, aws_rate_limiter_token_bucket_compute_wait_for_tokens(&token_bucket, 4));
    ASSERT_INT_EQUALS(AWS_TIMESTAMP_NANOS * 2 / 3, aws_rate_limiter_token_bucket_compute_wait_for_tokens(&token_bucket, 5));
    ASSERT_INT_EQUALS(AWS_TIMESTAMP_NANOS, aws_rate_limiter_token_bucket_compute_wait_for_tokens(&token_bucket, 6));
    ASSERT_INT_EQUALS((uint64_t)AWS_TIMESTAMP_NANOS * 4 / 3 + 1, aws_rate_limiter_token_bucket_compute_wait_for_tokens(&token_bucket, 7));

    /* now let's do multi-second plus fractional */
    s_test_time += (uint64_t)AWS_TIMESTAMP_NANOS * 2 + AWS_TIMESTAMP_NANOS / 2;
    ASSERT_TRUE(aws_rate_limiter_token_bucket_can_take_tokens(&token_bucket, 10));
    ASSERT_FALSE(aws_rate_limiter_token_bucket_can_take_tokens(&token_bucket, 11));
    ASSERT_INT_EQUALS(0, aws_rate_limiter_token_bucket_compute_wait_for_tokens(&token_bucket, 10));
    ASSERT_INT_EQUALS((uint64_t)AWS_TIMESTAMP_NANOS * 2 / 3 - AWS_TIMESTAMP_NANOS / 2, aws_rate_limiter_token_bucket_compute_wait_for_tokens(&token_bucket, 11));

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(rate_limiter_token_bucket_regeneration_fractional, s_rate_limiter_token_bucket_regeneration_fractional_fn)

static int s_rate_limiter_token_bucket_reset_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    (void)allocator;

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(rate_limiter_token_bucket_reset, s_rate_limiter_token_bucket_reset_fn)
