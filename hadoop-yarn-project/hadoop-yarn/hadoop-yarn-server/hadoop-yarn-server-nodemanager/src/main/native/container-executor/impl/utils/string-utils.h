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

#ifdef __FreeBSD__
#define _WITH_GETLINE
#endif

#ifndef _UTILS_STRING_UTILS_H_
#define _UTILS_STRING_UTILS_H_

#include <stdbool.h>
#include <stddef.h>

typedef struct strbuf_struct {
  char* buffer;               // points to beginning of the string
  size_t length;              // strlen of buffer (sans trailing NUL)
  size_t capacity;            // capacity of the buffer
} strbuf;


/*
 * Get numbers split by comma from a input string
 * return false/true
 */
int validate_container_id(const char* input);

/*
 * return 0 if succeeded
 */
int get_numbers_split_by_comma(const char* input, int** numbers, size_t* n_numbers);

/*
 * String format utility
 */
char *make_string(const char *fmt, ...);

/*
 * Compare string end with a suffix.
 * return 1 if succeeded
 */
int str_ends_with(const char *s, const char *suffix);

/**
 * Converts a sequence of bytes into a hexadecimal string.
 *
 * Returns a pointer to the allocated string on success or NULL on error.
 */
char* to_hexstring(unsigned char* bytes, unsigned int len);

/**
 * Allocate and initialize a strbuf with the specified initial capacity.
 *
 * Returns a pointer to the allocated and initialized strbuf or NULL on error.
 */
strbuf* strbuf_alloc(size_t initial_capacity);

/**
 * Initialize an uninitialized strbuf with the specified initial capacity.
 *
 * Returns true on success or false if memory could not be allocated.
 */
bool strbuf_init(strbuf* sb, size_t initial_capacity);

/**
 * Resize a strbuf to the specified new capacity.
 *
 * Returns true on success or false if there was an error.
 */
bool strbuf_realloc(strbuf* sb, size_t new_capacity);

/**
 * Detach the underlying character buffer from a string buffer.
 *
 * Returns the heap-allocated, NULL-terminated character buffer.
 * NOTE: The caller is responsible for freeing the result.
 */
char* strbuf_detach_buffer(strbuf* sb);

/**
 * Releases the memory underneath a string buffer but does NOT free the
 * strbuf structure itself. This is particularly useful for stack-allocated
 * strbuf objects or structures that embed a strbuf structure.
 * strbuf_free should be used for heap-allocated string buffers.
 */
void strbuf_destroy(strbuf* sb);

/**
 * Free a strbuf and all memory associated with it.
 */
void strbuf_free(strbuf* sb);

/**
 * Append a formatted string to the current contents of a strbuf.
 *
 * Returns true on success or false if there was an error.
 */
bool strbuf_append_fmt(strbuf* sb, size_t realloc_extra, const char* fmt, ...);

#endif
