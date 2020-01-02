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
#ifndef RUNC_RUNC_LAUNCH_CMD_H
#define RUNC_RUNC_LAUNCH_CMD_H

#include "utils/cJSON/cJSON.h"

// NOTE: Update free_runc_launch_cmd when this is changed.
typedef struct runc_launch_cmd_layer_spec {
  char* media_type; // MIME type for layer data
  char* path;       // local filesystem location of layer data
} rlc_layer_spec;

// NOTE: Update free_runc_launch_cmd when this is changed.
typedef struct runc_config_process_struct {
  cJSON* args;       // execve-style command and arguments
  cJSON* cwd;        // working dir for container
  cJSON* env;        // execve-style environment
} runc_config_process;

// NOTE: Update free_runc_launch_cmd when this is changed.
typedef struct runc_config_struct {
  cJSON* hostname;             // hostname for the container
  cJSON* linux_config;         // Linux section of the runC config
  cJSON* mounts;               // bind-mounts for the container
  runc_config_process process; // process config for the container
} runc_config;

// NOTE: Update free_runc_launch_cmd when this is changed.
typedef struct runc_launch_cmd_struct {
  char* run_as_user;          // user name of the runAs user
  char* username;             // user name of the container user
  char* app_id;               // YARN application ID
  char* container_id;         // YARN container ID
  char* pid_file;             // pid file path to create
  char* script_path;          // path to container launch script
  char* cred_path;            // path to container credentials file
  int https;                  // whether or not https is enabled
  char* keystore_path;        // path to keystore file
  char* truststore_path;      // path to truststore file
  char** local_dirs;          // NULL-terminated array of local dirs
  char** log_dirs;            // NULL-terminated array of log dirs
  rlc_layer_spec* layers;     // array of layers
  unsigned int num_layers;    // number of entries in the layers array
  int num_reap_layers_keep;   // number of total layer mounts to preserve
  runc_config config;         // runC config for the container
} runc_launch_cmd;


/**
 * Free a runC launch command structure and all memory assruncated with it.
 */
void free_runc_launch_cmd(runc_launch_cmd* rlc);


/**
 *
 * Valildate runC container launch command.
 * Returns true on valid and false on invalid.
 */
bool is_valid_runc_launch_cmd(const runc_launch_cmd* rlc);

/**
 * Read, parse, and validate a runC container launch command.
 *
 * Returns a pointer to the launch command or NULL on error.
 */
runc_launch_cmd* parse_runc_launch_cmd(const char* command_filename);

#endif /* RUNC_RUNC_LAUNCH_CMD_H */
