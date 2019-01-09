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

#ifndef _MODULES_GPU_GPU_MUDULE_H_
#define _MODULES_GPU_GPU_MUDULE_H_

#define GPU_MAJOR_NUMBER_CONFIG_KEY "gpu.major-device-number"
#define GPU_ALLOWED_DEVICES_MINOR_NUMBERS "gpu.allowed-device-minor-numbers"
#define GPU_MODULE_SECTION_NAME "gpu"

// For unit test stubbing
typedef int (*update_cgroups_parameters_func)(const char*, const char*,
   const char*, const char*);

/**
 * Handle gpu requests
 */
int handle_gpu_request(update_cgroups_parameters_func func,
   const char* module_name, int module_argc, char** module_argv);

/**
 * Reload config from filesystem, visible for testing.
 */
void reload_gpu_configuration();

#endif