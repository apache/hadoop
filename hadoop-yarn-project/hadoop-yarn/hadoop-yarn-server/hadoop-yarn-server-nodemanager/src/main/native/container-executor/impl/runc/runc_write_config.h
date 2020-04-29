/*
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
#ifndef RUNC_RUNC_WRITE_CONFIG_H
#define RUNC_RUNC_WRITE_CONFIG_H

/**
 *  * Creates a runC runtime configuration JSON
 *   *
 *    * Returns the config JSON or NULL on error
 *     */
cJSON* build_runc_config_json(const runc_launch_cmd* rlc,
                               const char* rootfs_path);

/**
 * Creates the runC runtime configuration file for a container.
 *
 * Returns the path to the written configuration file or NULL on error.
 */
char* write_runc_runc_config(const runc_launch_cmd* rlc, const char* rootfs_path);

#endif /* RUNC_RUNC_WRITE_CONFIG_H */
