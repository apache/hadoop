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

#ifndef __YARN_POSIX_CONTAINER_EXECUTOR_DOCKER_UTIL_H__
#define __YARN_POSIX_CONTAINER_EXECUTOR_DOCKER_UTIL_H__

#include "configuration.h"

#define CONTAINER_EXECUTOR_CFG_DOCKER_SECTION "docker"
#define DOCKER_BINARY_KEY "docker.binary"
#define DOCKER_INSPECT_MAX_RETRIES_KEY "docker.inspect.max.retries"
#define DOCKER_COMMAND_FILE_SECTION "docker-command-execution"
#define DOCKER_INSPECT_COMMAND "inspect"
#define DOCKER_LOAD_COMMAND "load"
#define DOCKER_PULL_COMMAND "pull"
#define DOCKER_RM_COMMAND "rm"
#define DOCKER_RUN_COMMAND "run"
#define DOCKER_STOP_COMMAND "stop"
#define DOCKER_KILL_COMMAND "kill"
#define DOCKER_VOLUME_COMMAND "volume"
#define DOCKER_START_COMMAND "start"
#define DOCKER_EXEC_COMMAND "exec"
#define DOCKER_IMAGES_COMMAND "images"
#define DOCKER_SERVICE_MODE_ENABLED_KEY "docker.service-mode.enabled"
#define DOCKER_ARG_MAX 1024
#define ARGS_INITIAL_VALUE { 0 };

typedef struct args {
    int length;
    char *data[DOCKER_ARG_MAX];
} args;

/**
 * Get the full path for the docker binary.
 * @param conf Configuration for the container-executor
 * @return String containing the docker binary to be freed by the user.
 */
char *get_docker_binary(const struct configuration *conf);

/**
 * Get the Docker command line string. The function will inspect the params file to determine the command to be run.
 * @param command_file File containing the params for the Docker command
 * @param conf Configuration struct containing the container-executor.cfg details
 * @param args Buffer to construct argv
 * @return Return code with 0 indicating success and non-zero codes indicating error
 */
int get_docker_command(const char* command_file, const struct configuration* conf, args *args);

/**
 * Check if use-entry-point flag is set.
 * @return 0 when use-entry-point flag is set.
 */
int get_use_entry_point_flag();

/**
 * Get the Docker inspect command line string. The function will verify that the params file is meant for the
 * inspect command.
 * @param command_file File containing the params for the Docker inspect command
 * @param conf Configuration struct containing the container-executor.cfg details
 * @param args Buffer to construct argv
 * @return Return code with 0 indicating success and non-zero codes indicating error
 */
int get_docker_inspect_command(const char* command_file, const struct configuration* conf, args *args);

/**
 * Get the Docker load command line string. The function will verify that the params file is meant for the load command.
 * @param command_file File containing the params for the Docker load command
 * @param conf Configuration struct containing the container-executor.cfg details
 * @param args Buffer to construct argv
 * @return Return code with 0 indicating success and non-zero codes indicating error
 */
int get_docker_load_command(const char* command_file, const struct configuration* conf, args *args);

/**
 * Get the Docker pull command line string. The function will verify that the params file is meant for the pull command.
 * @param command_file File containing the params for the Docker pull command
 * @param conf Configuration struct containing the container-executor.cfg details
 * @param args Buffer to construct argv
 * @return Return code with 0 indicating success and non-zero codes indicating error
 */
int get_docker_pull_command(const char* command_file, const struct configuration* conf, args *args);

/**
 * Get the Docker rm command line string. The function will verify that the params file is meant for the rm command.
 * @param command_file File containing the params for the Docker rm command
 * @param conf Configuration struct containing the container-executor.cfg details
 * @param args Buffer to construct argv
 * @return Return code with 0 indicating success and non-zero codes indicating error
 */
int get_docker_rm_command(const char* command_file, const struct configuration* conf, args *args);

/**
 * Get the Docker run command line string. The function will verify that the params file is meant for the run command.
 * @param command_file File containing the params for the Docker run command
 * @param conf Configuration struct containing the container-executor.cfg details
 * @param args Buffer to construct argv
 * @return Return code with 0 indicating success and non-zero codes indicating error
 */
int get_docker_run_command(const char* command_file, const struct configuration* conf, args *args);

/**
 * Get the Docker stop command line string. The function will verify that the params file is meant for the stop command.
 * @param command_file File containing the params for the Docker stop command
 * @param conf Configuration struct containing the container-executor.cfg details
 * @param args Buffer to construct argv
 * @return Return code with 0 indicating success and non-zero codes indicating error
 */
int get_docker_stop_command(const char* command_file, const struct configuration* conf, args *args);

/**
 * Get the Docker kill command line string. The function will verify that the params file is meant for the kill command.
 * @param command_file File containing the params for the Docker kill command
 * @param conf Configuration struct containing the container-executor.cfg details
 * @param args Buffer to construct argv
 * @return Return code with 0 indicating success and non-zero codes indicating error
 */
int get_docker_kill_command(const char* command_file, const struct configuration* conf, args *args);

/**
 * Get the Docker volume command line string. The function will verify that the
 * params file is meant for the volume command.
 * @param command_file File containing the params for the Docker volume command
 * @param conf Configuration struct containing the container-executor.cfg details
 * @param args Buffer to construct argv
 * @return Return code with 0 indicating success and non-zero codes indicating error
 */
int get_docker_volume_command(const char *command_file, const struct configuration *conf, args *args);

/**
 * Get the Docker start command line string. The function will verify that the params file is meant for the start command.
 * @param command_file File containing the params for the Docker start command
 * @param conf Configuration struct containing the container-executor.cfg details
 * @param args Buffer to construct argv
 * @return Return code with 0 indicating success and non-zero codes indicating error
 */
int get_docker_start_command(const char* command_file, const struct configuration* conf, args *args);

/**
 * Get the Docker exec command line string. The function will verify that the params file is meant for the exec command.
 * @param command_file File containing the params for the Docker start command
 * @param conf Configuration struct containing the container-executor.cfg details
 * @param args Buffer to construct argv
 * @return Return code with 0 indicating success and non-zero codes indicating error
 */
int get_docker_exec_command(const char* command_file, const struct configuration* conf, args *args);

/**
 * Give an error message for the supplied error code
 * @param error_code the error code
 * @return const string containing the error message
 */
const char *get_docker_error_message(const int error_code);

/**
 * Determine if the docker module is enabled in the config.
 * @param conf Configuration structure
 * @return 1 if enabled, 0 otherwise
 */
int docker_module_enabled(const struct configuration *conf);

/**
 * Helper function to reset args data structure.
 * @param args Pointer reference to args data structure
 */
void reset_args(args *args);

/**
 * Extract execv args from args data structure.
 * @param args Pointer reference to args data structure
 */
char** extract_execv_args(args *args);

/**
 * Get max retries for docker inspect.
 * @param conf Configuration structure
 * @return value of max retries
 */
int get_max_retries(const struct configuration *conf);

/**
 * Get the Docker images command line string. The function will verify that the params file is meant for the images
 * command.
 * @param command_file File containing the params for the Docker images command
 * @param conf Configuration struct containing the container-executor.cfg details
 * @param args Buffer to construct argv
 * @return Return code with 0 indicating success and non-zero codes indicating error
 */
int get_docker_images_command(const char* command_file, const struct configuration* conf, args *args);

#endif
