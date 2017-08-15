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

#include <strings.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

/*
 * if all chars in the input str are numbers
 * return true/false
 */
static int all_numbers(char* input) {
  if (0 == strlen(input)) {
    return 0;
  }

  for (int i = 0; i < strlen(input); i++) {
    if (input[i] < '0' || input[i] > '9') {
      return 0;
    }
  }
  return 1;
}

int validate_container_id(const char* input) {
  /*
   * Two different forms of container_id
   * container_e17_1410901177871_0001_01_000005
   * container_1410901177871_0001_01_000005
   */
  char* input_cpy = strdup(input);
  char* p = strtok(input_cpy, "_");
  int idx = 0;
  while (p != NULL) {
    if (0 == idx) {
      if (0 != strcmp("container", p)) {
        return 0;
      }
    } else if (1 == idx) {
      // this could be e[n][n], or [n][n]...
      if (!all_numbers(p)) {
        if (strlen(p) == 0) {
          return 0;
        }
        if (p[0] != 'e') {
          return 0;
        }
        if (!all_numbers(p + 1)) {
          return 0;
        }
      }
    } else {
      // otherwise, should be all numbers
      if (!all_numbers(p)) {
        return 0;
      }
    }

    p = strtok(NULL, "_");
    idx++;
  }
  free(input_cpy);

  // We should have [5,6] elements split by '_'
  if (idx > 6 || idx < 5) {
    return 0;
  }
  return 1;
}