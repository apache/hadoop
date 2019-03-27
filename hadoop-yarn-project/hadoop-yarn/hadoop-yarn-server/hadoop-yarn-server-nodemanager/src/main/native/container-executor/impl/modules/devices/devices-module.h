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

#ifndef _MODULES_DEVICES_MUDULE_H_
#define _MODULES_DEVICES_MUDULE_H_

// Denied device list. value format is "major1:minor1,major2:minor2"
#define DEVICES_DENIED_NUMBERS "devices.denied-numbers"
#define DEVICES_MODULE_SECTION_NAME "devices"

// For unit test stubbing
typedef int (*update_cgroups_param_function)(const char*, const char*,
   const char*, const char*);

/**
 * Handle devices requests
 */
int handle_devices_request(update_cgroups_param_function func,
   const char* module_name, int module_argc, char** module_argv);

/**
 * Reload config from filesystem, visible for testing.
 */
void reload_devices_configuration();

#endif