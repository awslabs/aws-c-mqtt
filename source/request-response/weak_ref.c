/**
* Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
* SPDX-License-Identifier: Apache-2.0.
*/

#include <aws/mqtt/private/request-response/weak_ref.h>

#include <aws/common/ref_count.h>

struct aws_weak_ref {
    struct aws_allocator *allocator;
    struct aws_ref_count refcount;
    void *referenced;
};

static void s_destroy_weak_ref(void *value) {
    struct aws_weak_ref *weak_ref = value;

    aws_mem_release(weak_ref->allocator, weak_ref);
}

struct aws_weak_ref *aws_weak_ref_new(struct aws_allocator *allocator, void *referenced) {
    struct aws_weak_ref *weak_ref = aws_mem_calloc(allocator, 1, sizeof(struct aws_weak_ref));

    aws_ref_count_init(&weak_ref->refcount, weak_ref, s_destroy_weak_ref);
    weak_ref->allocator = allocator;
    weak_ref->referenced = referenced;

    return weak_ref;
}

struct aws_weak_ref *aws_weak_ref_acquire(struct aws_weak_ref *weak_ref) {
    if (NULL != weak_ref) {
        aws_ref_count_acquire(&weak_ref->refcount);
    }

    return weak_ref;
}

struct aws_weak_ref *aws_weak_ref_release(struct aws_weak_ref *weak_ref) {
    if (NULL != weak_ref) {
        aws_ref_count_release(&weak_ref->refcount);
    }

    return NULL;
}

void *aws_weak_ref_get_reference(struct aws_weak_ref *weak_ref) {
    return weak_ref->referenced;
}

void aws_weak_ref_zero_reference(struct aws_weak_ref *weak_ref) {
    weak_ref->referenced = NULL;
}
