#ifndef AWS_RATE_LIMITERS_H
#define AWS_RATE_LIMITERS_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

#include <aws/io/io.h>

struct aws_rate_limiter_token_bucket_options {
    aws_io_clock_fn *clock_fn;

    uint64_t tokens_per_second;
    uint64_t initial_token_count;
    uint64_t maximum_token_count;
};

struct aws_rate_limiter_token_bucket {
    uint64_t last_service_time;
    uint64_t current_token_count;

    uint64_t fractional_nanos;
    uint64_t fractional_nano_tokens;

    struct aws_rate_limiter_token_bucket_options config;
};

AWS_EXTERN_C_BEGIN

AWS_MQTT_API int aws_rate_limiter_token_bucket_init(
    struct aws_rate_limiter_token_bucket *limiter,
    const struct aws_rate_limiter_token_bucket_options *options);

AWS_MQTT_API void aws_rate_limiter_token_bucket_reset(struct aws_rate_limiter_token_bucket *limiter);

AWS_MQTT_API bool aws_rate_limiter_token_bucket_can_take_tokens(
    struct aws_rate_limiter_token_bucket *limiter,
    uint64_t token_count);

AWS_MQTT_API int aws_rate_limiter_token_bucket_take_tokens(
    struct aws_rate_limiter_token_bucket *limiter,
    uint64_t token_count);

AWS_MQTT_API uint64_t aws_rate_limiter_token_bucket_compute_wait_for_tokens(
    struct aws_rate_limiter_token_bucket *limiter,
    uint64_t token_count);

AWS_EXTERN_C_END

#endif /* AWS_RATE_LIMITERS_H */
