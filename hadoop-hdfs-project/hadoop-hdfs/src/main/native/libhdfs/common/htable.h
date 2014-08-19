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

#ifndef HADOOP_CORE_COMMON_HASH_TABLE
#define HADOOP_CORE_COMMON_HASH_TABLE

#include <inttypes.h>
#include <stdio.h>
#include <stdint.h>

#define HTABLE_MIN_SIZE 4

struct htable;

/**
 * An HTable hash function.
 *
 * @param key       The key.
 * @param capacity  The total capacity.
 *
 * @return          The hash slot.  Must be less than the capacity.
 */
typedef uint32_t (*htable_hash_fn_t)(const void *key, uint32_t capacity);

/**
 * An HTable equality function.  Compares two keys.
 *
 * @param a         First key.
 * @param b         Second key.
 *
 * @return          nonzero if the keys are equal.
 */
typedef int (*htable_eq_fn_t)(const void *a, const void *b);

/**
 * Allocate a new hash table.
 *
 * @param capacity  The minimum suggested starting capacity.
 * @param hash_fun  The hash function to use in this hash table.
 * @param eq_fun    The equals function to use in this hash table.
 *
 * @return          The new hash table on success; NULL on OOM.
 */
struct htable *htable_alloc(uint32_t capacity, htable_hash_fn_t hash_fun,
                            htable_eq_fn_t eq_fun);

typedef void (*visitor_fn_t)(void *ctx, void *key, void *val);

/**
 * Visit all of the entries in the hash table.
 *
 * @param htable    The hash table.
 * @param fun       The callback function to invoke on each key and value.
 * @param ctx       Context pointer to pass to the callback.
 */
void htable_visit(struct htable *htable, visitor_fn_t fun, void *ctx);

/**
 * Free the hash table.
 *
 * It is up the calling code to ensure that the keys and values inside the
 * table are de-allocated, if that is necessary.
 *
 * @param htable    The hash table.
 */
void htable_free(struct htable *htable);

/**
 * Add an entry to the hash table.
 *
 * @param htable    The hash table.
 * @param key       The key to add.  This cannot be NULL.
 * @param fun       The value to add.  This cannot be NULL.
 *
 * @return          0 on success;
 *                  EEXIST if the value already exists in the table;
 *                  ENOMEM if there is not enough memory to add the element.
 *                  EFBIG if the hash table has too many entries to fit in 32
 *                      bits.
 */
int htable_put(struct htable *htable, void *key, void *val);

/**
 * Get an entry from the hash table.
 *
 * @param htable    The hash table.
 * @param key       The key to find.
 *
 * @return          NULL if there is no such entry; the entry otherwise.
 */
void *htable_get(const struct htable *htable, const void *key);

/**
 * Get an entry from the hash table and remove it.
 *
 * @param htable    The hash table.
 * @param key       The key for the entry find and remove.
 * @param found_key (out param) NULL if the entry was not found; the found key
 *                      otherwise.
 * @param found_val (out param) NULL if the entry was not found; the found
 *                      value otherwise.
 */
void htable_pop(struct htable *htable, const void *key,
                void **found_key, void **found_val);

/**
 * Get the number of entries used in the hash table.
 *
 * @param htable    The hash table.
 *
 * @return          The number of entries used in the hash table.
 */
uint32_t htable_used(const struct htable *htable);

/**
 * Get the capacity of the hash table.
 *
 * @param htable    The hash table.
 *
 * @return          The capacity of the hash table.
 */
uint32_t htable_capacity(const struct htable *htable);

/**
 * Hash a string.
 *
 * @param str       The string.
 * @param max       Maximum hash value
 *
 * @return          A number less than max.
 */
uint32_t ht_hash_string(const void *str, uint32_t max);

/**
 * Compare two strings.
 *
 * @param a         The first string.
 * @param b         The second string.
 *
 * @return          1 if the strings are identical; 0 otherwise.
 */
int ht_compare_string(const void *a, const void *b);

#endif

// vim: ts=4:sw=4:tw=79:et
