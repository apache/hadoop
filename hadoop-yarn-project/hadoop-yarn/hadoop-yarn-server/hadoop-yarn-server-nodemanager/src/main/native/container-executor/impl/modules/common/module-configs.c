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

#include "module-configs.h"
#include "util.h"
#include "modules/common/constants.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#define ENABLED_CONFIG_KEY "module.enabled"

int module_enabled(const struct section* section_cfg, const char* module_name) {
  char* enabled_str = get_section_value(ENABLED_CONFIG_KEY, section_cfg);
  int enabled = 0;
  if (enabled_str && 0 == strcmp(enabled_str, "true")) {
    enabled = 1;
  } else {
    fprintf(LOGFILE, "Module %s is disabled\n", module_name);
  }

  free(enabled_str);
  return enabled;
}
