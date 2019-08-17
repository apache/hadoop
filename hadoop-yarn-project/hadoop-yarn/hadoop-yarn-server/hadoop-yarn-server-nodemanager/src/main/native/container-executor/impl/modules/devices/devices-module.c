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
#include "modules/devices/devices-module.h"
#include "modules/cgroups/cgroups-operations.h"
#include "modules/common/module-configs.h"
#include "modules/common/constants.h"
#include "util.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <getopt.h>
#include <unistd.h>
#include <sys/stat.h>

#define EXCLUDED_DEVICES_OPTION "excluded_devices"
#define ALLOWED_DEVICES_OPTION "allowed_devices"
#define CONTAINER_ID_OPTION "container_id"
#define MAX_CONTAINER_ID_LEN 128

static const struct section* cfg_section;

// Search a string in a string list, return 1 when found
static int search_in_list(char** list, char* token) {
  int i = 0;
  char** iterator = list;
  // search token in  list
  while (iterator[i] != NULL) {
    if (strstr(token, iterator[i]) != NULL ||
        strstr(iterator[i], token) != NULL) {
      // Found deny device in allowed list
      return 1;
    }
    i++;
  }
  return 0;
}

static int is_block_device(const char* value) {
  int is_block = 0;
  int max_path_size = 512;
  char* block_path = malloc(max_path_size);
  if (block_path == NULL) {
    fprintf(ERRORFILE, "Failed to allocate memory for sys device path string.\n");
    goto cleanup;
  }
  if (snprintf(block_path, max_path_size, "/sys/dev/block/%s",
    value) < 0) {
    fprintf(ERRORFILE, "Failed to construct system block device path.\n");
    goto cleanup;
  }
  struct stat sb;
  // file exists, is block device
  if (stat(block_path, &sb) == 0) {
    is_block = 1;
  }
cleanup:
  if (block_path) {
    free(block_path);
  }
  return is_block;
}

static int internal_handle_devices_request(
    update_cgroups_param_function update_cgroups_parameters_func_p,
    char** deny_devices_number_tokens,
    char** allow_devices_number_tokens,
    const char* container_id) {
  int return_code = 0;

  char** ce_denied_numbers = NULL;
  char* ce_denied_str = get_section_value(DEVICES_DENIED_NUMBERS,
     cfg_section);
  // Get denied "major:minor" device numbers from cfg, if not set, means all
  // devices can be used by YARN.
  if (ce_denied_str != NULL) {
    ce_denied_numbers = split_delimiter(ce_denied_str, ",");
    if (NULL == ce_denied_numbers) {
      fprintf(ERRORFILE,
          "Invalid value set for %s, value=%s\n",
          DEVICES_DENIED_NUMBERS,
          ce_denied_str);
      return_code = -1;
      goto cleanup;
    }
    // Check allowed devices passed in
    char** allow_iterator = allow_devices_number_tokens;
    int allow_count = 0;
    while (allow_iterator[allow_count] != NULL) {
      if (search_in_list(ce_denied_numbers, allow_iterator[allow_count])) {
        fprintf(ERRORFILE,
          "Trying to allow device with device number=%s which is not permitted in container-executor.cfg. %s\n",
          allow_iterator[allow_count],
          "It could be caused by a mismatch of devices reported by device plugin");
        return_code = -1;
        goto cleanup;
      }
      allow_count++;
    }

    // Deny devices configured in c-e.cfg
    char** ce_iterator = ce_denied_numbers;
    int ce_count = 0;
    while (ce_iterator[ce_count] != NULL) {
      // skip if duplicate with denied numbers passed in
      if (search_in_list(deny_devices_number_tokens, ce_iterator[ce_count])) {
        ce_count++;
        continue;
      }
      char param_value[128];
      char type = 'c';
      memset(param_value, 0, sizeof(param_value));
      if (is_block_device(ce_iterator[ce_count])) {
        type = 'b';
      }
      snprintf(param_value, sizeof(param_value), "%c %s rwm",
               type,
               ce_iterator[ce_count]);
      // Update device cgroups value
      int rc = update_cgroups_parameters_func_p("devices", "deny",
        container_id, param_value);

      if (0 != rc) {
        fprintf(ERRORFILE, "CGroups: Failed to update cgroups. %s\n", param_value);
        return_code = -1;
        goto cleanup;
      }
      ce_count++;
    }
  }

  // Deny devices passed from java side
  char** iterator = deny_devices_number_tokens;
  int count = 0;
  char* value = NULL;
  int index = 0;
  while (iterator[count] != NULL) {
    // Replace like "c-242:0-rwm" to "c 242:0 rwm"
    value = iterator[count];
    index = 0;
    while (value[index] != '\0') {
      if (value[index] == '-') {
        value[index] = ' ';
      }
      index++;
    }
    // Update device cgroups value
    int rc = update_cgroups_parameters_func_p("devices", "deny",
      container_id, iterator[count]);

    if (0 != rc) {
      fprintf(ERRORFILE, "CGroups: Failed to update cgroups\n");
      return_code = -1;
      goto cleanup;
    }
    count++;
  }

cleanup:
  if (ce_denied_numbers != NULL) {
    free_values(ce_denied_numbers);
  }
  return return_code;
}

void reload_devices_configuration() {
  cfg_section = get_configuration_section(DEVICES_MODULE_SECTION_NAME, get_cfg());
}

/*
 * Format of devices request commandline:
 * The excluded_devices is comma separated device cgroups values with device type.
 * The "-" will be replaced with " " to match the cgroups parameter
 * c-e --module-devices \
 * --excluded_devices b-8:16-rwm,c-244:0-rwm,c-244:1-rwm \
 * --allowed_devices 8:32,8:48,243:2 \
 * --container_id container_x_y
 */
int handle_devices_request(update_cgroups_param_function func,
    const char* module_name, int module_argc, char** module_argv) {
  if (!cfg_section) {
    reload_devices_configuration();
  }

  if (!module_enabled(cfg_section, DEVICES_MODULE_SECTION_NAME)) {
    fprintf(ERRORFILE,
      "Please make sure devices module is enabled before using it.\n");
    return -1;
  }

  static struct option long_options[] = {
    {EXCLUDED_DEVICES_OPTION, required_argument, 0, 'e' },
    {ALLOWED_DEVICES_OPTION, required_argument, 0, 'a' },
    {CONTAINER_ID_OPTION, required_argument, 0, 'c' },
    {0, 0, 0, 0}
  };

  int c = 0;
  int option_index = 0;

  char** deny_device_value_tokens = NULL;
  char** allow_device_value_tokens = NULL;
  char container_id[MAX_CONTAINER_ID_LEN];
  memset(container_id, 0, sizeof(container_id));
  int failed = 0;

  optind = 1;
  while((c = getopt_long(module_argc, module_argv, "e:a:c:",
                         long_options, &option_index)) != -1) {
    switch(c) {
      case 'e':
        deny_device_value_tokens = split_delimiter(optarg, ",");
        break;
      case 'a':
        allow_device_value_tokens = split_delimiter(optarg, ",");
        break;
      case 'c':
        if (!validate_container_id(optarg)) {
          fprintf(ERRORFILE,
            "Specified container_id=%s is invalid\n", optarg);
          failed = 1;
          goto cleanup;
        }
        strncpy(container_id, optarg, MAX_CONTAINER_ID_LEN);
        break;
      default:
        fprintf(ERRORFILE,
          "Unknown option in devices command character %d %c, optionindex = %d\n",
          c, c, optind);
        failed = 1;
        goto cleanup;
    }
  }

  if (0 == container_id[0]) {
    fprintf(ERRORFILE,
      "[%s] --container_id must be specified.\n", __func__);
    failed = 1;
    goto cleanup;
  }

  if (NULL == deny_device_value_tokens) {
     // Devices number is null, skip following call.
     fprintf(ERRORFILE, "--excluded_devices is not specified, skip cgroups call.\n");
     goto cleanup;
  }

  failed = internal_handle_devices_request(func,
         deny_device_value_tokens,
         allow_device_value_tokens,
         container_id);

cleanup:
  if (deny_device_value_tokens) {
    free_values(deny_device_value_tokens);
  }
  if (allow_device_value_tokens) {
    free_values(allow_device_value_tokens);
  }
  return failed;
}
