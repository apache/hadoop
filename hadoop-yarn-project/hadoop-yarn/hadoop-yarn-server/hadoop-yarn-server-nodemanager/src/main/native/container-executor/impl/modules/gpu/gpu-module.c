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
#include "modules/gpu/gpu-module.h"
#include "modules/cgroups/cgroups-operations.h"
#include "modules/common/module-configs.h"
#include "modules/common/constants.h"
#include "util.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <getopt.h>
#include <unistd.h>

#define EXCLUDED_GPUS_OPTION "excluded_gpus"
#define CONTAINER_ID_OPTION "container_id"
#define DEFAULT_NVIDIA_MAJOR_NUMBER 195
#define MAX_CONTAINER_ID_LEN 128

static const struct section* cfg_section;

static int internal_handle_gpu_request(
    update_cgroups_parameters_func update_cgroups_parameters_func_p,
    size_t n_minor_devices_to_block, int minor_devices[],
    const char* container_id) {
  char* allowed_minor_numbers_str = NULL;
  int* allowed_minor_numbers = NULL;
  size_t n_allowed_minor_numbers = 0;
  int return_code = 0;

  if (n_minor_devices_to_block == 0) {
    // no device to block, just return;
    return 0;
  }

  // Get major device number from cfg, if not set, major number of (Nvidia)
  // will be the default value.
  int major_device_number;
  char* major_number_str = get_section_value(GPU_MAJOR_NUMBER_CONFIG_KEY,
     cfg_section);
  if (!major_number_str || 0 == major_number_str[0]) {
    // Default major number of Nvidia devices
    major_device_number = DEFAULT_NVIDIA_MAJOR_NUMBER;
  } else {
    major_device_number = strtol(major_number_str, NULL, 0);
  }

  // Get allowed minor device numbers from cfg, if not set, means all minor
  // devices can be used by YARN
  allowed_minor_numbers_str = get_section_value(
      GPU_ALLOWED_DEVICES_MINOR_NUMBERS,
      cfg_section);
  if (!allowed_minor_numbers_str || 0 == allowed_minor_numbers_str[0]) {
    allowed_minor_numbers = NULL;
  } else {
    int rc = get_numbers_split_by_comma(allowed_minor_numbers_str,
                                        &allowed_minor_numbers,
                                        &n_allowed_minor_numbers);
    if (0 != rc) {
      fprintf(ERRORFILE,
          "Failed to get allowed minor device numbers from cfg, value=%s\n",
          allowed_minor_numbers_str);
      return_code = -1;
      goto cleanup;
    }

    // Make sure we're trying to black devices allowed in config
    for (int i = 0; i < n_minor_devices_to_block; i++) {
      int found = 0;
      for (int j = 0; j < n_allowed_minor_numbers; j++) {
        if (minor_devices[i] == allowed_minor_numbers[j]) {
          found = 1;
          break;
        }
      }

      if (!found) {
        fprintf(ERRORFILE,
          "Trying to blacklist device with minor-number=%d which is not on allowed list\n",
          minor_devices[i]);
        return_code = -1;
        goto cleanup;
      }
    }
  }

  // Use cgroup helpers to blacklist devices
  for (int i = 0; i < n_minor_devices_to_block; i++) {
    char param_value[128];
    memset(param_value, 0, sizeof(param_value));
    snprintf(param_value, sizeof(param_value), "c %d:%d rwm",
             major_device_number, minor_devices[i]);

    int rc = update_cgroups_parameters_func_p("devices", "deny",
      container_id, param_value);

    if (0 != rc) {
      fprintf(ERRORFILE, "CGroups: Failed to update cgroups\n");
      return_code = -1;
      goto cleanup;
    }
  }

cleanup:
  if (major_number_str) {
    free(major_number_str);
  }
  if (allowed_minor_numbers) {
    free(allowed_minor_numbers);
  }
  if (allowed_minor_numbers_str) {
    free(allowed_minor_numbers_str);
  }

  return return_code;
}

void reload_gpu_configuration() {
  cfg_section = get_configuration_section(GPU_MODULE_SECTION_NAME, get_cfg());
}

/*
 * Format of GPU request commandline:
 *
 * c-e gpu --excluded_gpus 0,1,3 --container_id container_x_y
 */
int handle_gpu_request(update_cgroups_parameters_func func,
    const char* module_name, int module_argc, char** module_argv) {
  if (!cfg_section) {
    reload_gpu_configuration();
  }

  if (!module_enabled(cfg_section, GPU_MODULE_SECTION_NAME)) {
    fprintf(ERRORFILE,
      "Please make sure gpu module is enabled before using it.\n");
    return -1;
  }

  static struct option long_options[] = {
    {EXCLUDED_GPUS_OPTION, required_argument, 0, 'e' },
    {CONTAINER_ID_OPTION, required_argument, 0, 'c' },
    {0, 0, 0, 0}
  };

  int rc = 0;
  int c = 0;
  int option_index = 0;

  int* minor_devices = NULL;
  char container_id[MAX_CONTAINER_ID_LEN];
  memset(container_id, 0, sizeof(container_id));
  size_t n_minor_devices_to_block = 0;
  int failed = 0;

  optind = 1;
  while((c = getopt_long(module_argc, module_argv, "e:c:",
                         long_options, &option_index)) != -1) {
    switch(c) {
      case 'e':
        rc = get_numbers_split_by_comma(optarg, &minor_devices,
          &n_minor_devices_to_block);
        if (0 != rc) {
          fprintf(ERRORFILE,
            "Failed to get minor devices number from command line, value=%s\n",
            optarg);
          failed = 1;
          goto cleanup;
        }
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
          "Unknown option in gpu command character %d %c, optionindex = %d\n",
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

  if (!minor_devices) {
     // Minor devices is null, skip following call.
     fprintf(ERRORFILE, "is not specified, skip cgroups call.\n");
     goto cleanup;
  }

  failed = internal_handle_gpu_request(func, n_minor_devices_to_block,
         minor_devices,
         container_id);

cleanup:
  if (minor_devices) {
    free(minor_devices);
  }
  return failed;
}
