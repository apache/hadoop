/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "common/htable.h"
#include "expect.h"
#include "hdfs_test.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Disable type cast and loss of precision warnings, because the test
// manipulates void* values manually on purpose.
#ifdef WIN32
#pragma warning(disable: 4244 4306)
#endif

static uint32_t simple_hash(const void *key, uint32_t size)
{
    uintptr_t k = (uintptr_t)key;
    return ((13 + k) * 6367) % size;
}

static int simple_compare(const void *a, const void *b)
{
    return a == b;
}

static void expect_102(void *f, void *k, void *v)
{
    int *found_102 = f;
    uintptr_t key = (uintptr_t)k;
    uintptr_t val = (uintptr_t)v;

    if ((key == 2) && (val == 102)) {
        *found_102 = 1;
    } else {
        abort();
    }
}

static void *htable_pop_val(struct htable *ht, void *key)
{
    void *old_key, *old_val;

    htable_pop(ht, key, &old_key, &old_val);
    return old_val;
}

int main(void)
{
    struct htable *ht;
    int found_102 = 0;

    ht = htable_alloc(4, simple_hash, simple_compare);
    EXPECT_INT_EQ(0, htable_used(ht));
    EXPECT_INT_EQ(4, htable_capacity(ht));
    EXPECT_NULL(htable_get(ht, (void*)123));
    EXPECT_NULL(htable_pop_val(ht, (void*)123));
    EXPECT_ZERO(htable_put(ht, (void*)123, (void*)456));
    EXPECT_INT_EQ(456, (uintptr_t)htable_get(ht, (void*)123));
    EXPECT_INT_EQ(456, (uintptr_t)htable_pop_val(ht, (void*)123));
    EXPECT_NULL(htable_pop_val(ht, (void*)123));

    // Enlarge the hash table
    EXPECT_ZERO(htable_put(ht, (void*)1, (void*)101));
    EXPECT_ZERO(htable_put(ht, (void*)2, (void*)102));
    EXPECT_ZERO(htable_put(ht, (void*)3, (void*)103));
    EXPECT_INT_EQ(3, htable_used(ht));
    EXPECT_INT_EQ(8, htable_capacity(ht));
    EXPECT_INT_EQ(102, (uintptr_t)htable_get(ht, (void*)2));
    EXPECT_INT_EQ(101, (uintptr_t)htable_pop_val(ht, (void*)1));
    EXPECT_INT_EQ(103, (uintptr_t)htable_pop_val(ht, (void*)3));
    EXPECT_INT_EQ(1, htable_used(ht));
    htable_visit(ht, expect_102, &found_102);
    EXPECT_INT_EQ(1, found_102);
    htable_free(ht);

    fprintf(stderr, "SUCCESS.\n");
    return EXIT_SUCCESS;
}

// vim: ts=4:sw=4:tw=79:et
