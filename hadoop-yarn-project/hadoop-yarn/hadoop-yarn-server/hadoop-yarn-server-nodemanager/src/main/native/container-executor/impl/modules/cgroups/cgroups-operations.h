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

#ifndef _CGROUPS_OPERATIONS_H_
#define _CGROUPS_OPERATIONS_H_

#define CGROUPS_SECTION_NAME "cgroups"
#define CGROUPS_ROOT_KEY "root"
#define CGROUPS_YARN_HIERARCHY_KEY "yarn-hierarchy"

/**
 * Handle update CGroups parameter update requests:
 * - hierarchy_name: e.g. devices / cpu,cpuacct
 * - param_name: e.g. deny
 * - group_id: e.g. container_x_y
 * - value: e.g. "a *:* rwm"
 *
 * return 0 if succeeded
 */
int update_cgroups_parameters(
   const char* hierarchy_name,
   const char* param_name,
   const char* group_id,
   const char* value);

 /**
  * Get CGroups path to update. Visible for testing.
  * Return 0 if succeeded
  */
 char* get_cgroups_path_to_write(
    const char* hierarchy_name,
    const char* param_name,
    const char* group_id);

 /**
  * Reload config from filesystem, visible for testing.
  */
 void reload_cgroups_configuration();

#endif