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

#include <strings.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

int verify_path_safety(const char* path) {
  if (!path || path[0] == 0) {
    return 1;
  }

  char* dup = strdup(path);
  if (!dup) {
    fprintf(ERRORFILE, "%s: Failed to allocate memory for path.\n", __func__);
    return 0;
  }

  char* p = strtok(dup, "/");
  int succeeded = 1;

  while (p != NULL) {
    if (0 == strcmp(p, "..")) {
      fprintf(ERRORFILE, "%s: Path included \"..\", path=%s.\n", __func__, path);
      succeeded = 0;
      break;
    }

    p = strtok(NULL, "/");
  }
  free(dup);

  return succeeded;
}