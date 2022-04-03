/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/private/v5/rate_limiters.h>

#include <aws/common/clock.h>

static int s_rate_limit_time_fn(const struct aws_rate_limiter_token_bucket_options *options, uint64_t *current_time) {
    if (options->clock_fn != NULL) {
        return (*options->clock_fn)(current_time);
    }

    return aws_high_res_clock_get_ticks(current_time);
}

int aws_rate_limiter_token_bucket_init(
    struct aws_rate_limiter_token_bucket *limiter,
    const struct aws_rate_limiter_token_bucket_options *options) {
    AWS_ZERO_STRUCT(*limiter);

    if (options->tokens_per_second == 0 || options->maximum_token_count == 0) {
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    limiter->config = *options;

    aws_rate_limiter_token_bucket_reset(limiter);

    return AWS_OP_SUCCESS;
}

void aws_rate_limiter_token_bucket_reset(struct aws_rate_limiter_token_bucket *limiter) {

    limiter->current_token_count =
        aws_min_u64(limiter->config.initial_token_count, limiter->config.maximum_token_count);
    limiter->fractional_nanos = 0;
    limiter->fractional_nano_tokens = 0;

    uint64_t now = 0;
    AWS_FATAL_ASSERT(s_rate_limit_time_fn(&limiter->config, &now) == AWS_OP_SUCCESS);

    limiter->last_service_time = now;
}

static void s_regenerate_tokens(struct aws_rate_limiter_token_bucket *limiter) {
    uint64_t now = 0;
    AWS_FATAL_ASSERT(s_rate_limit_time_fn(&limiter->config, &now) == AWS_OP_SUCCESS);

    if (now <= limiter->last_service_time) {
        return;
    }

    uint64_t nanos_elapsed = now - limiter->last_service_time;

    /*
     * We break the regeneration calculation into two distinct steps:
     *   (1) Perform regeneration based on whole seconds elapsed (nice and easy just multiply times the regen rate)
     *   (2) Perform regeneration based on the remaining fraction of a second elapsed
     *
     * We do this to minimize the chances of multiplication saturation before the divide necessary to normalize to
     * nanos.
     *
     * In particular, by doing this, we won't see saturation unless a regeneration rate in the multi-billions is used
     * or elapsed_seconds is in the billions.  This is similar reasoning to what we do in aws_timestamp_convert_u64.
     *
     * Additionally, we use a (sub-second) fractional counter/accumulator (fractional_nanos, fractional_nano_tokens)
     * in order to prevent error accumulation due to integer division rounding.
     */
    uint64_t remainder_nanos = 0;
    uint64_t elapsed_seconds =
        aws_timestamp_convert(nanos_elapsed, AWS_TIMESTAMP_NANOS, AWS_TIMESTAMP_SECS, &remainder_nanos);

    uint64_t tokens_regenerated = aws_mul_u64_saturating(elapsed_seconds, limiter->config.tokens_per_second);
    limiter->fractional_nanos += remainder_nanos;
    if (limiter->fractional_nanos < AWS_TIMESTAMP_NANOS) {
        uint64_t new_fractional_tokens =
            aws_mul_u64_saturating(limiter->fractional_nanos, limiter->config.tokens_per_second) / AWS_TIMESTAMP_NANOS;
        tokens_regenerated += new_fractional_tokens - limiter->fractional_nano_tokens;
        limiter->fractional_nano_tokens = new_fractional_tokens;
    } else {
        tokens_regenerated += limiter->config.tokens_per_second - limiter->fractional_nano_tokens;
        limiter->fractional_nanos -= AWS_TIMESTAMP_NANOS;
        limiter->fractional_nano_tokens =
            aws_mul_u64_saturating(limiter->fractional_nanos, limiter->config.tokens_per_second) / AWS_TIMESTAMP_NANOS;
        tokens_regenerated += limiter->fractional_nano_tokens;
    }

    limiter->current_token_count = aws_add_u64_saturating(tokens_regenerated, limiter->current_token_count);
    if (limiter->current_token_count > limiter->config.maximum_token_count) {
        limiter->current_token_count = limiter->config.maximum_token_count;
    }

    limiter->last_service_time = now;
}

bool aws_rate_limiter_token_bucket_can_take_tokens(
    struct aws_rate_limiter_token_bucket *limiter,
    uint64_t token_count) {
    s_regenerate_tokens(limiter);

    return limiter->current_token_count >= token_count;
}

int aws_rate_limiter_token_bucket_take_tokens(struct aws_rate_limiter_token_bucket *limiter, uint64_t token_count) {
    s_regenerate_tokens(limiter);

    if (limiter->current_token_count < token_count) {
        /* TODO: correct error once seated in aws-c-common */
        return aws_raise_error(AWS_ERROR_INVALID_STATE);
    }

    limiter->current_token_count -= token_count;
    return AWS_OP_SUCCESS;
}

uint64_t aws_rate_limiter_token_bucket_compute_wait_for_tokens(
    struct aws_rate_limiter_token_bucket *limiter,
    uint64_t token_count) {
    s_regenerate_tokens(limiter);

    if (limiter->current_token_count >= token_count) {
        return 0;
    }

    uint64_t token_rate = limiter->config.tokens_per_second;
    AWS_FATAL_ASSERT(limiter->fractional_nanos < AWS_TIMESTAMP_NANOS);
    AWS_FATAL_ASSERT(limiter->fractional_nano_tokens <= token_rate);

    uint64_t expected_wait = 0;

    uint64_t deficit = token_count - limiter->current_token_count;
    uint64_t remaining_fractional_tokens = token_rate - limiter->fractional_nano_tokens;

    if (deficit < remaining_fractional_tokens) {
        uint64_t target_fractional_tokens = aws_add_u64_saturating(deficit, limiter->fractional_nano_tokens);
        uint64_t remainder_wait_unnormalized = aws_mul_u64_saturating(target_fractional_tokens, AWS_TIMESTAMP_NANOS);

        expected_wait = remainder_wait_unnormalized / token_rate - limiter->fractional_nanos;
        if (remainder_wait_unnormalized % token_rate) {
            ++expected_wait;
        }
    } else {
        expected_wait = AWS_TIMESTAMP_NANOS - limiter->fractional_nanos;
        deficit -= remaining_fractional_tokens;

        uint64_t expected_wait_seconds = deficit / token_rate;
        uint64_t deficit_remainder = deficit % token_rate;

        expected_wait += aws_mul_u64_saturating(expected_wait_seconds, AWS_TIMESTAMP_NANOS);

        uint64_t remainder_wait_unnormalized = aws_mul_u64_saturating(deficit_remainder, AWS_TIMESTAMP_NANOS);
        expected_wait += remainder_wait_unnormalized / token_rate;
        if (remainder_wait_unnormalized % token_rate) {
            ++expected_wait;
        }
    }

    return expected_wait;
}
