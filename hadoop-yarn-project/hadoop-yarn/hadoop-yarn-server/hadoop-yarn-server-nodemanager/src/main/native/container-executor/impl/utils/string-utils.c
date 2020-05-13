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
#include "util.h"

#include <unistd.h>
#include <sys/types.h>
#include <dirent.h>
#include <limits.h>
#include <errno.h>
#include <stdarg.h>
#include <strings.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>

#include "string-utils.h"

/*
 * if all chars in the input str are numbers
 * return true/false
 */
static int all_numbers(char* input) {
  for (; input[0] != 0; input++) {
    if (input[0] < '0' || input[0] > '9') {
      return 0;
    }
  }
  return 1;
}

int get_numbers_split_by_comma(const char* input, int** numbers,
                               size_t* ret_n_numbers) {
  size_t allocation_size = 1;
  int i = 0;
  while (input[i] != 0) {
    if (input[i] == ',') {
      allocation_size++;
    }
    i++;
  }

  (*numbers) = malloc(sizeof(int) * allocation_size);
  if (!(*numbers)) {
    fprintf(ERRORFILE, "Failed to allocating memory for *numbers: %s\n",
            __func__);
    exit(OUT_OF_MEMORY);
  }
  memset(*numbers, 0, sizeof(int) * allocation_size);

  char* input_cpy = strdup(input);
  if (!input_cpy) {
    fprintf(ERRORFILE, "Failed to allocating memory for input_cpy: %s\n",
            __func__);
    exit(OUT_OF_MEMORY);
  }

  char* p = strtok(input_cpy, ",");
  int idx = 0;
  size_t n_numbers = 0;
  while (p != NULL) {
    char *temp;
    long n = strtol(p, &temp, 0);
    // According to answer:
    // https://stackoverflow.com/questions/14176123/correct-usage-of-strtol
    // We need to properly check errno and overflows
    if (temp == p || *temp != '\0' ||
        ((n == LONG_MIN || n == LONG_MAX) && errno == ERANGE)) {
      fprintf(stderr,
              "Could not convert '%s' to long and leftover string is: '%s'\n",
              p, temp);
      free(input_cpy);
      return -1;
    }

    n_numbers++;
    (*numbers)[idx] = n;
    p = strtok(NULL, ",");
    idx++;
  }

  free(input_cpy);
  *ret_n_numbers = n_numbers;

  return 0;
}

int validate_container_id(const char* input) {
  int is_container_id = 1;

  /*
   * Two different forms of container_id
   * container_e17_1410901177871_0001_01_000005
   * container_1410901177871_0001_01_000005
   */
  if (!input) {
    return 0;
  }

  char* input_cpy = strdup(input);
  if (!input_cpy) {
    return 0;
  }

  char* p = strtok(input_cpy, "_");
  int idx = 0;
  while (p != NULL) {
    if (0 == idx) {
      if (0 != strcmp("container", p)) {
        is_container_id = 0;
        goto cleanup;
      }
    } else if (1 == idx) {
      // this could be e[n][n], or [n][n]...
      if (!all_numbers(p)) {
        if (p[0] == 0) {
          is_container_id = 0;
          goto cleanup;
        }
        if (p[0] != 'e') {
          is_container_id = 0;
          goto cleanup;
        }
        if (!all_numbers(p + 1)) {
          is_container_id = 0;
          goto cleanup;
        }
      }
    } else {
      // otherwise, should be all numbers
      if (!all_numbers(p)) {
        is_container_id = 0;
        goto cleanup;
      }
    }

    p = strtok(NULL, "_");
    idx++;
  }

cleanup:
  if (input_cpy) {
    free(input_cpy);
  }

  // We should have [5,6] elements split by '_'
  if (idx > 6 || idx < 5) {
    is_container_id = 0;
  }
  return is_container_id;
}

/*
 * Format string utility.
 */
char *make_string(const char *fmt, ...) {
  va_list vargs;
  va_start(vargs, fmt);
  size_t buflen = vsnprintf(NULL, 0, fmt, vargs) + 1;
  va_end(vargs);
  if (buflen <= 0) {
    return NULL;
  }
  char* buf = malloc(buflen);
  if (buf != NULL) {
    va_start(vargs, fmt);
    int ret = vsnprintf(buf, buflen, fmt, vargs);
    va_end(vargs);
    if (ret < 0) {
      buf = NULL;
    }
  }
  return buf;
}

int str_ends_with(const char *s, const char *suffix) {
    size_t slen = strlen(s);
    size_t suffix_len = strlen(suffix);
    return suffix_len <= slen && !strcmp(s + slen - suffix_len, suffix);
}

/* Returns the corresponding hexadecimal character for a nibble. */
static char nibble_to_hex(unsigned char nib) {
  return nib < 10 ? '0' + nib : 'a' + nib - 10;
}

/**
 * Converts a sequence of bytes into a hexadecimal string.
 *
 * Returns a pointer to the allocated string on success or NULL on error.
 */
char* to_hexstring(unsigned char* bytes, unsigned int len) {
  char* hexstr = malloc(len * 2 + 1);
  if (hexstr == NULL) {
    return NULL;
  }
  unsigned char* src = bytes;
  char* dest = hexstr;
  for (unsigned int i = 0; i < len; ++i) {
    unsigned char val = *src++;
    *dest++ = nibble_to_hex((val >> 4) & 0xF);
    *dest++ = nibble_to_hex(val & 0xF);
  }
  *dest = '\0';
  return hexstr;
}

/**
 * Initialize an uninitialized strbuf with the specified initial capacity.
 *
 * Returns true on success or false if memory could not be allocated.
 */
bool strbuf_init(strbuf* sb, size_t initial_capacity) {
  memset(sb, 0, sizeof(*sb));
  char* new_buffer = malloc(initial_capacity);
  if (new_buffer == NULL) {
    return false;
  }
  sb->buffer = new_buffer;
  sb->capacity = initial_capacity;
  sb->length = 0;
  return true;
}

/**
 * Allocate and initialize a strbuf with the specified initial capacity.
 *
 * Returns a pointer to the allocated and initialized strbuf or NULL on error.
 */
strbuf* strbuf_alloc(size_t initial_capacity) {
  strbuf* sb = malloc(sizeof(*sb));
  if (sb != NULL) {
    if (!strbuf_init(sb, initial_capacity)) {
      free(sb);
      sb = NULL;
    }
  }
  return sb;
}

/**
 * Detach the underlying character buffer from a string buffer.
 *
 * Returns the heap-allocated, NULL-terminated character buffer.
 * NOTE: The caller is responsible for freeing the result.
 */
char* strbuf_detach_buffer(strbuf* sb) {
  char* result = NULL;
  if (sb != NULL) {
    result = sb->buffer;
    sb->buffer = NULL;
    sb->length = 0;
    sb->capacity = 0;
  }
  return result;
}

/**
 * Release memory associated with a strbuf but not the strbuf structure itself.
 * Useful for stack-allocated strbuf objects or structures that embed a strbuf.
 * Use strbuf_free for heap-allocated string buffers.
 */
void strbuf_destroy(strbuf* sb) {
  if (sb != NULL) {
    free(sb->buffer);
    sb->buffer = NULL;
    sb->capacity = 0;
    sb->length = 0;
  }
}

/**
 * Free a strbuf and all memory associated with it.
 */
void strbuf_free(strbuf* sb) {
  if (sb != NULL) {
    strbuf_destroy(sb);
    free(sb);
  }
}

/**
 * Resize a strbuf to the specified new capacity.
 *
 * Returns true on success or false if there was an error.
 */
bool strbuf_realloc(strbuf* sb, size_t new_capacity) {
  if (new_capacity < sb->length + 1) {
    // New capacity would result in a truncation of the existing string.
    return false;
  }

  char* new_buffer = realloc(sb->buffer, new_capacity);
  if (!new_buffer) {
    return false;
  }

  sb->buffer = new_buffer;
  sb->capacity = new_capacity;
  return true;
}

/**
 * Append a formatted string to the current contents of a strbuf.
 *
 * Returns true on success or false if there was an error.
 */
bool strbuf_append_fmt(strbuf* sb, size_t realloc_extra,
    const char* fmt, ...) {
  if (sb->length > sb->capacity) {
    return false;
  }

  if (sb->length == sb->capacity) {
    size_t incr = (realloc_extra == 0) ? 1024 : realloc_extra;
    if (!strbuf_realloc(sb, sb->capacity + incr)) {
      return false;
    }
  }

  size_t remain = sb->capacity - sb->length;
  va_list vargs;
  va_start(vargs, fmt);
  int needed = vsnprintf(sb->buffer + sb->length, remain, fmt, vargs);
  va_end(vargs);
  if (needed == -1) {
    return false;
  }

  needed += 1;  // vsnprintf result does NOT include terminating NUL
  if (needed > remain) {
    // result was truncated so need to realloc and reprint
    size_t new_size = sb->length + needed + realloc_extra;
    if (!strbuf_realloc(sb, new_size)) {
      return false;
    }
    remain = sb->capacity - sb->length;
    va_start(vargs, fmt);
    needed = vsnprintf(sb->buffer + sb->length, remain, fmt, vargs);
    va_end(vargs);
    if (needed == -1) {
      return false;
    }
    needed += 1;  // vsnprintf result does NOT include terminating NUL
  }

  sb->length += needed - 1;  // length does not include terminating NUL
  return true;
}
