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

#include "configuration.h"
#include "container-executor.h"
#include "utils/string-utils.h"
#include "utils/path-utils.h"
#include "modules/common/module-configs.h"
#include "modules/common/constants.h"
#include "modules/cgroups/cgroups-operations.h"
#include "util.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>

#define MAX_PATH_LEN 4096

static const struct section* cgroup_cfg_section = NULL;

void reload_cgroups_configuration() {
  cgroup_cfg_section = get_configuration_section(CGROUPS_SECTION_NAME, get_cfg());
}

char* get_cgroups_path_to_write(
    const char* hierarchy_name,
    const char* param_name,
    const char* group_id) {
  int failed = 0;
  char* buffer = NULL;
  const char* cgroups_root = get_section_value(CGROUPS_ROOT_KEY,
     cgroup_cfg_section);
  const char* yarn_hierarchy_name = get_section_value(
     CGROUPS_YARN_HIERARCHY_KEY, cgroup_cfg_section);

  // Make sure it is defined.
  if (!cgroups_root || cgroups_root[0] == 0) {
    fprintf(ERRORFILE, "%s is not defined in container-executor.cfg\n",
      CGROUPS_ROOT_KEY);
    failed = 1;
    goto cleanup;
  }

  // Make sure it is defined.
  if (!yarn_hierarchy_name || yarn_hierarchy_name[0] == 0) {
    fprintf(ERRORFILE, "%s is not defined in container-executor.cfg\n",
      CGROUPS_YARN_HIERARCHY_KEY);
    failed = 1;
    goto cleanup;
  }

  buffer = malloc(MAX_PATH_LEN + 1);
  if (!buffer) {
    fprintf(ERRORFILE, "Failed to allocate memory for output path.\n");
    failed = 1;
    goto cleanup;
  }

  // Make a path.
  // CGroups path should not be too long.
  if (snprintf(buffer, MAX_PATH_LEN, "%s/%s/%s/%s/%s.%s",
    cgroups_root, hierarchy_name, yarn_hierarchy_name,
    group_id, hierarchy_name, param_name) < 0) {
    fprintf(ERRORFILE, "Failed to print output path.\n");
    failed = 1;
    goto cleanup;
  }

cleanup:
  free((void *) cgroups_root);
  free((void *) yarn_hierarchy_name);
  if (failed) {
    if (buffer) {
      free(buffer);
    }
    return NULL;
  }
  return buffer;
}

int update_cgroups_parameters(
   const char* hierarchy_name,
   const char* param_name,
   const char* group_id,
   const char* value) {
#ifndef __linux
  fprintf(ERRORFILE, "Failed to update cgroups parameters, not supported\n");
  return -1;
#endif
  int failure = 0;

  if (!cgroup_cfg_section) {
    reload_cgroups_configuration();
  }

  char* full_path = get_cgroups_path_to_write(hierarchy_name, param_name,
    group_id);

  if (!full_path) {
    fprintf(ERRORFILE,
      "Failed to get cgroups path to write, it should be a configuration issue");
    failure = 1;
    goto cleanup;
  }

  if (!verify_path_safety(full_path)) {
    failure = 1;
    goto cleanup;
  }

  // Make sure file exists
  struct stat sb;
  if (stat(full_path, &sb) != 0) {
    fprintf(ERRORFILE, "CGroups: Could not find file to write, %s", full_path);
    failure = 1;
    goto cleanup;
  }

  fprintf(ERRORFILE, "CGroups: Updating cgroups, path=%s, value=%s",
    full_path, value);

  // Write values to file
  FILE *f;
  f = fopen(full_path, "a");
  if (!f) {
    fprintf(ERRORFILE, "CGroups: Failed to open cgroups file, %s", full_path);
    failure = 1;
    goto cleanup;
  }
  if (fprintf(f, "%s", value) < 0) {
    fprintf(ERRORFILE, "CGroups: Failed to write cgroups file, %s", full_path);
    fclose(f);
    failure = 1;
    goto cleanup;
  }
  if (fclose(f) != 0) {
    fprintf(ERRORFILE, "CGroups: Failed to close cgroups file, %s", full_path);
    failure = 1;
    goto cleanup;
  }

cleanup:
  if (full_path) {
    free(full_path);
  }
  return -failure;
}
