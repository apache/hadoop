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

#include <limits.h>
#include <errno.h>
#include <strings.h>
#include <string.h>
#include <stdlib.h>

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
