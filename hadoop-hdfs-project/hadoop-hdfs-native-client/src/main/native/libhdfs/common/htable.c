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

#include <errno.h>
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

struct htable_pair {
    void *key;
    void *val;
};

/**
 * A hash table which uses linear probing.
 */
struct htable {
    uint32_t capacity;
    uint32_t used;
    htable_hash_fn_t hash_fun;
    htable_eq_fn_t eq_fun;
    struct htable_pair *elem;
};

/**
 * An internal function for inserting a value into the hash table.
 *
 * Note: this function assumes that you have made enough space in the table.
 *
 * @param nelem         The new element to insert.
 * @param capacity      The capacity of the hash table.
 * @param hash_fun      The hash function to use.
 * @param key           The key to insert.
 * @param val           The value to insert.
 */
static void htable_insert_internal(struct htable_pair *nelem, 
        uint32_t capacity, htable_hash_fn_t hash_fun, void *key,
        void *val)
{
    uint32_t i;

    i = hash_fun(key, capacity);
    while (1) {
        if (!nelem[i].key) {
            nelem[i].key = key;
            nelem[i].val = val;
            return;
        }
        i++;
        if (i == capacity) {
            i = 0;
        }
    }
}

static int htable_realloc(struct htable *htable, uint32_t new_capacity)
{
    struct htable_pair *nelem;
    uint32_t i, old_capacity = htable->capacity;
    htable_hash_fn_t hash_fun = htable->hash_fun;

    nelem = calloc(new_capacity, sizeof(struct htable_pair));
    if (!nelem) {
        return ENOMEM;
    }
    for (i = 0; i < old_capacity; i++) {
        struct htable_pair *pair = htable->elem + i;
        if (pair->key) {
            htable_insert_internal(nelem, new_capacity, hash_fun,
                                   pair->key, pair->val);
        }
    }
    free(htable->elem);
    htable->elem = nelem;
    htable->capacity = new_capacity;
    return 0;
}

static uint32_t round_up_to_power_of_2(uint32_t i)
{
    if (i == 0) {
        return 1;
    }
    i--;
    i |= i >> 1;
    i |= i >> 2;
    i |= i >> 4;
    i |= i >> 8;
    i |= i >> 16;
    i++;
    return i;
}

struct htable *htable_alloc(uint32_t size,
                htable_hash_fn_t hash_fun, htable_eq_fn_t eq_fun)
{
    struct htable *htable;

    htable = calloc(1, sizeof(*htable));
    if (!htable) {
        return NULL;
    }
    size = round_up_to_power_of_2(size);
    if (size < HTABLE_MIN_SIZE) {
        size = HTABLE_MIN_SIZE;
    }
    htable->hash_fun = hash_fun;
    htable->eq_fun = eq_fun;
    htable->used = 0;
    if (htable_realloc(htable, size)) {
        free(htable);
        return NULL;
    }
    return htable;
}

void htable_visit(struct htable *htable, visitor_fn_t fun, void *ctx)
{
    uint32_t i;

    for (i = 0; i != htable->capacity; ++i) {
        struct htable_pair *elem = htable->elem + i;
        if (elem->key) {
            fun(ctx, elem->key, elem->val);
        }
    }
}

void htable_free(struct htable *htable)
{
    if (htable) {
        free(htable->elem);
        free(htable);
    }
}

int htable_put(struct htable *htable, void *key, void *val)
{
    int ret;
    uint32_t nused;

    // NULL is not a valid key value.
    // This helps us implement htable_get_internal efficiently, since we know
    // that we can stop when we encounter the first NULL key.
    if (!key) {
        return EINVAL;
    }
    // NULL is not a valid value.  Otherwise the results of htable_get would
    // be confusing (does a NULL return mean entry not found, or that the
    // entry was found and was NULL?) 
    if (!val) {
        return EINVAL;
    }
    // Re-hash if we have used more than half of the hash table
    nused = htable->used + 1;
    if (nused >= (htable->capacity / 2)) {
        ret = htable_realloc(htable, htable->capacity * 2);
        if (ret)
            return ret;
    }
    htable_insert_internal(htable->elem, htable->capacity,
                                htable->hash_fun, key, val);
    htable->used++;
    return 0;
}

static int htable_get_internal(const struct htable *htable,
                               const void *key, uint32_t *out)
{
    uint32_t start_idx, idx;

    start_idx = htable->hash_fun(key, htable->capacity);
    idx = start_idx;
    while (1) {
        struct htable_pair *pair = htable->elem + idx;
        if (!pair->key) {
            // We always maintain the invariant that the entries corresponding
            // to a given key are stored in a contiguous block, not separated
            // by any NULLs.  So if we encounter a NULL, our search is over.
            return ENOENT;
        } else if (htable->eq_fun(pair->key, key)) {
            *out = idx;
            return 0;
        }
        idx++;
        if (idx == htable->capacity) {
            idx = 0;
        }
        if (idx == start_idx) {
            return ENOENT;
        }
    }
}

void *htable_get(const struct htable *htable, const void *key)
{
    uint32_t idx;

    if (htable_get_internal(htable, key, &idx)) {
        return NULL;
    }
    return htable->elem[idx].val;
}

void htable_pop(struct htable *htable, const void *key,
                void **found_key, void **found_val)
{
    uint32_t hole, i;
    const void *nkey;

    if (htable_get_internal(htable, key, &hole)) {
        *found_key = NULL;
        *found_val = NULL;
        return;
    }
    i = hole;
    htable->used--;
    // We need to maintain the compactness invariant used in
    // htable_get_internal.  This invariant specifies that the entries for any
    // given key are never separated by NULLs (although they may be separated
    // by entries for other keys.)
    while (1) {
        i++;
        if (i == htable->capacity) {
            i = 0;
        }
        nkey = htable->elem[i].key;
        if (!nkey) {
            *found_key = htable->elem[hole].key;
            *found_val = htable->elem[hole].val;
            htable->elem[hole].key = NULL;
            htable->elem[hole].val = NULL;
            return;
        } else if (htable->eq_fun(key, nkey)) {
            htable->elem[hole].key = htable->elem[i].key;
            htable->elem[hole].val = htable->elem[i].val;
            hole = i;
        }
    }
}

uint32_t htable_used(const struct htable *htable)
{
    return htable->used;
}

uint32_t htable_capacity(const struct htable *htable)
{
    return htable->capacity;
}

uint32_t ht_hash_string(const void *str, uint32_t max)
{
    const char *s = str;
    uint32_t hash = 0;

    while (*s) {
        hash = (hash * 31) + *s;
        s++;
    }
    return hash % max;
}

int ht_compare_string(const void *a, const void *b)
{
    return strcmp(a, b) == 0;
}

// vim: ts=4:sw=4:tw=79:et
