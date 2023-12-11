#ifndef AWS_MQTT_PRIVATE_REQUEST_RESPONSE_WEAK_REF_H
#define AWS_MQTT_PRIVATE_REQUEST_RESPONSE_WEAK_REF_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/exports.h>

#include <aws/common/common.h>

struct aws_weak_ref;

AWS_EXTERN_C_BEGIN

AWS_MQTT_API struct aws_weak_ref *aws_weak_ref_new(struct aws_allocator *allocator, void *referenced);

AWS_MQTT_API struct aws_weak_ref *aws_weak_ref_acquire(struct aws_weak_ref *weak_ref);

AWS_MQTT_API struct aws_weak_ref *aws_weak_ref_release(struct aws_weak_ref *weak_ref);

AWS_MQTT_API void *aws_weak_ref_get_reference(struct aws_weak_ref *weak_ref);

AWS_MQTT_API void aws_weak_ref_zero_reference(struct aws_weak_ref *weak_ref);

AWS_EXTERN_C_END


#endif /* AWS_MQTT_PRIVATE_REQUEST_RESPONSE_WEAK_REF_H */
