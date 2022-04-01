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
     */
    uint64_t remainder_nanos = 0;
    uint64_t elapsed_seconds = aws_timestamp_convert(nanos_elapsed, AWS_TIMESTAMP_NANOS, AWS_TIMESTAMP_SECS, &remainder_nanos);
    uint64_t tokens_regenerated_from_elapsed_seconds = aws_mul_u64_saturating(elapsed_seconds, limiter->config.tokens_per_second);
    uint64_t nano_token_product = aws_mul_u64_saturating(remainder_nanos, limiter->config.tokens_per_second);
    uint64_t tokens_regenerated_from_remainder_nanos = nano_token_product / AWS_TIMESTAMP_NANOS;
    uint64_t fractional_token_nanos = nano_token_product % AWS_TIMESTAMP_NANOS;
    uint64_t tokens_regenerated = aws_add_u64_saturating(tokens_regenerated_from_elapsed_seconds, tokens_regenerated_from_remainder_nanos);
    if (tokens_regenerated == 0) {
        return;
    }

    limiter->current_token_count = aws_add_u64_saturating(tokens_regenerated, limiter->current_token_count);
    if (limiter->current_token_count > limiter->config.maximum_token_count) {
        limiter->current_token_count = limiter->config.maximum_token_count;
    }

    /*
     * Finally, we don't update last service time to current time because that may skip a fraction's worth
     * of token regeneration that we cannot account for with integers.
     *
     * Instead, set last_service_time to now minus the number of nanos we're past a non-fractional token amount.
     *
     * In this way, the last_service_time is *always* a timepoint representing a non-fractional token amount.
     */
    limiter->last_service_time = now - fractional_token_nanos;
}

bool aws_rate_limiter_token_bucket_can_take_tokens(struct aws_rate_limiter_token_bucket *limiter, uint64_t token_count) {
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

    uint64_t deficit = token_count - limiter->current_token_count;

    uint64_t unnormalized_token_nanos = aws_mul_u64_saturating(deficit, AWS_TIMESTAMP_NANOS);
    uint64_t normalized_nanos = unnormalized_token_nanos / limiter->config.tokens_per_second;

    /* If the above division was fractional, we need to round up to the next nanosecond */
    if (unnormalized_token_nanos % limiter->config.tokens_per_second != 0) {
        ++normalized_nanos;
    }

    /* do our regen calculations based off of last_service_time, but our wait is based off of current time */
    uint64_t now = 0;
    AWS_FATAL_ASSERT(s_rate_limit_time_fn(&limiter->config, &now) == AWS_OP_SUCCESS);

    uint64_t final_time = aws_add_u64_saturating(limiter->last_service_time, normalized_nanos);
    if (final_time < now) {
        return 0;
    }

    return final_time - now;
}
