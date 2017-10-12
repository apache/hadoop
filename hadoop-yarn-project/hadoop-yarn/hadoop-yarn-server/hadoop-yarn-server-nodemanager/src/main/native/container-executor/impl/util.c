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
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

char** split_delimiter(char *value, const char *delim) {
  char **return_values = NULL;
  char *temp_tok = NULL;
  char *tempstr = NULL;
  int size = 0;
  int per_alloc_size = 10;
  int return_values_size = per_alloc_size;
  int failed = 0;

  //first allocate any array of 10
  if(value != NULL) {
    return_values = (char **) malloc(sizeof(char *) * return_values_size);
    if (!return_values) {
      fprintf(ERRORFILE, "Allocation error for return_values in %s.\n",
              __func__);
      failed = 1;
      goto cleanup;
    }
    memset(return_values, 0, sizeof(char *) * return_values_size);

    temp_tok = strtok_r(value, delim, &tempstr);
    while (temp_tok != NULL) {
      temp_tok = strdup(temp_tok);
      if (NULL == temp_tok) {
        fprintf(ERRORFILE, "Allocation error in %s.\n", __func__);
        failed = 1;
        goto cleanup;
      }

      return_values[size++] = temp_tok;

      // Make sure returned values has enough space for the trailing NULL.
      if (size >= return_values_size - 1) {
        return_values_size += per_alloc_size;
        return_values = (char **) realloc(return_values,(sizeof(char *) *
          return_values_size));

        // Make sure new added memory are filled with NULL
        for (int i = size; i < return_values_size; i++) {
          return_values[i] = NULL;
        }
      }
      temp_tok = strtok_r(NULL, delim, &tempstr);
    }
  }

  // Put trailing NULL to indicate values terminates.
  if (return_values != NULL) {
    return_values[size] = NULL;
  }

cleanup:
  if (failed) {
    free_values(return_values);
    return NULL;
  }

  return return_values;
}

/**
 * Extracts array of values from the '%' separated list of values.
 */
char** split(char *value) {
  return split_delimiter(value, "%");
}

// free an entry set of values
void free_values(char** values) {
  if (values != NULL) {
    int idx = 0;
    while (values[idx]) {
      free(values[idx]);
      idx++;
    }
    free(values);
  }
}

/**
 * Trim whitespace from beginning and end.
*/
char* trim(const char* input) {
    const char *val_begin;
    const char *val_end;
    char *ret;

    if (input == NULL) {
      return NULL;
    }

    val_begin = input;
    val_end = input + strlen(input);

    while (val_begin < val_end && isspace(*val_begin))
      val_begin++;
    while (val_end > val_begin && isspace(*(val_end - 1)))
      val_end--;

    ret = (char *) malloc(
            sizeof(char) * (val_end - val_begin + 1));
    if (ret == NULL) {
      fprintf(ERRORFILE, "Allocation error\n");
      exit(OUT_OF_MEMORY);
    }

    strncpy(ret, val_begin, val_end - val_begin);
    ret[val_end - val_begin] = '\0';
    return ret;
}
