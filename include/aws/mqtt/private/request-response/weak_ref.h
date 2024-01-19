#ifndef AWS_MQTT_PRIVATE_REQUEST_RESPONSE_WEAK_REF_H
#define AWS_MQTT_PRIVATE_REQUEST_RESPONSE_WEAK_REF_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/exports.h>

#include <aws/common/common.h>

/*
 * This is a simplification of the notion of a weak reference particular to the needs of the request-response
 * MQTT service clients.  This is not suitable for general use but could be extended
 * for general use in the future.  Until then, it stays private, here.
 *
 * This weak reference is a ref-counted object with an opaque value.  The opaque value may be cleared or
 * queried.  These two operations *do not* provide any thread safety.
 *
 * The primary use is to allow one object to safely use asynchronous callback-driven APIs on a second object, despite
 * the fact that the first object may get destroyed unpredictably.  The two objects must be exclusive to a single
 * event loop (because there's no thread safety or mutual exclusion on the opaque value held by the weak ref).
 *
 * The initial use is the request-response protocol adapter submitting operations to an MQTT client or an
 * eventstream RPC connection.  We use a single weak ref to the protocol adapter and zero its opaque value when
 * the protocol adapter is destroyed.  Operation callbacks that subsequently resolve can then short circuit and do
 * nothing rather than call into garbage and crash.
 */
struct aws_weak_ref;

AWS_EXTERN_C_BEGIN

/*
 * Creates a new weak reference to an opaque value.
 */
AWS_MQTT_API struct aws_weak_ref *aws_weak_ref_new(struct aws_allocator *allocator, void *referenced);

/*
 * Acquires a reference to the weak ref object.
 */
AWS_MQTT_API struct aws_weak_ref *aws_weak_ref_acquire(struct aws_weak_ref *weak_ref);

/*
 * Removes a reference to the weak ref object.  When the last reference is removed, the weak ref object will be
 * destroyed.  This has no effect on the opaque value held by the weak ref.
 */
AWS_MQTT_API struct aws_weak_ref *aws_weak_ref_release(struct aws_weak_ref *weak_ref);

/*
 * Gets the current value of the opaque data held by the weak ref.
 */
AWS_MQTT_API void *aws_weak_ref_get_reference(struct aws_weak_ref *weak_ref);

/*
 * Clears the opaque data held by the weak ref.
 */
AWS_MQTT_API void aws_weak_ref_zero_reference(struct aws_weak_ref *weak_ref);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_PRIVATE_REQUEST_RESPONSE_WEAK_REF_H */
