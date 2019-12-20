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

#include "util.h"
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <regex.h>
#include <stdio.h>

char** split_delimiter(char *value, const char *delim) {
  char **return_values = NULL;
  char *temp_tok = NULL;
  char *tempstr = NULL;
  int size = 0;
  int per_alloc_size = 10;
  int return_values_size = per_alloc_size;
  int failed = 0;

  //first allocate any array of 10
  if(value != NULL) {
    return_values = (char **) malloc(sizeof(char *) * return_values_size);
    if (!return_values) {
      fprintf(ERRORFILE, "Allocation error for return_values in %s.\n",
              __func__);
      failed = 1;
      goto cleanup;
    }
    memset(return_values, 0, sizeof(char *) * return_values_size);

    temp_tok = strtok_r(value, delim, &tempstr);
    if (NULL == temp_tok) {
      return_values[size++] = strdup(value);
    }
    while (temp_tok != NULL) {
      temp_tok = strdup(temp_tok);
      if (NULL == temp_tok) {
        fprintf(ERRORFILE, "Allocation error in %s.\n", __func__);
        failed = 1;
        goto cleanup;
      }

      return_values[size++] = temp_tok;

      // Make sure returned values has enough space for the trailing NULL.
      if (size >= return_values_size - 1) {
        return_values_size += per_alloc_size;
        return_values = (char **) realloc(return_values,(sizeof(char *) *
          return_values_size));

        // Make sure new added memory are filled with NULL
        for (int i = size; i < return_values_size; i++) {
          return_values[i] = NULL;
        }
      }
      temp_tok = strtok_r(NULL, delim, &tempstr);
    }
  }

  // Put trailing NULL to indicate values terminates.
  if (return_values != NULL) {
    return_values[size] = NULL;
  }

cleanup:
  if (failed) {
    free_values(return_values);
    return NULL;
  }

  return return_values;
}

/**
 * Extracts array of values from the '%' separated list of values.
 */
char** split(char *value) {
  return split_delimiter(value, "%");
}

// free an entry set of values
void free_values(char** values) {
  if (values != NULL) {
    int idx = 0;
    while (values[idx]) {
      free(values[idx]);
      idx++;
    }
    free(values);
  }
}

/**
 * Trim whitespace from beginning and end.
*/
char* trim(const char* input) {
    const char *val_begin;
    const char *val_end;
    char *ret;

    if (input == NULL) {
      return NULL;
    }

    val_begin = input;
    val_end = input + strlen(input);

    while (val_begin < val_end && isspace(*val_begin))
      val_begin++;
    while (val_end > val_begin && isspace(*(val_end - 1)))
      val_end--;

    ret = (char *) malloc(
            sizeof(char) * (val_end - val_begin + 1));
    if (ret == NULL) {
      fprintf(ERRORFILE, "Allocation error\n");
      exit(OUT_OF_MEMORY);
    }

    strncpy(ret, val_begin, val_end - val_begin);
    ret[val_end - val_begin] = '\0';
    return ret;
}

int execute_regex_match(const char *regex_str, const char *input) {
  regex_t regex;
  int regex_match;
  if (0 != regcomp(&regex, regex_str, REG_EXTENDED|REG_NOSUB)) {
    fprintf(LOGFILE, "Unable to compile regex.\n");
    exit(ERROR_COMPILING_REGEX);
  }
  regex_match = regexec(&regex, input, (size_t) 0, NULL, 0);
  regfree(&regex);
  if(0 == regex_match) {
    return 0;
  }
  return 1;
}

char* escape_single_quote(const char *str) {
  int p = 0;
  int i = 0;
  char replacement[] = "'\"'\"'";
  size_t replacement_length = strlen(replacement);
  size_t ret_size = strlen(str) * replacement_length + 1;
  char *ret = (char *) alloc_and_clear_memory(ret_size, sizeof(char));
  if(ret == NULL) {
    exit(OUT_OF_MEMORY);
  }
  while(str[p] != '\0') {
    if(str[p] == '\'') {
      strncat(ret, replacement, ret_size - strlen(ret));
      i += replacement_length;
    }
    else {
      ret[i] = str[p];
      i++;
    }
    p++;
  }
  ret[i] = '\0';
  return ret;
}

void quote_and_append_arg(char **str, size_t *size, const char* param, const char *arg) {
  char *tmp = escape_single_quote(arg);
  const char *append_format = "%s'%s' ";
  size_t append_size = snprintf(NULL, 0, append_format, param, tmp);
  append_size += 1;   // for the terminating NUL
  size_t len_str = strlen(*str);
  size_t new_size = len_str + append_size;
  if (new_size > *size) {
      *size = new_size + QUOTE_AND_APPEND_ARG_GROWTH;
      *str = (char *) realloc(*str, *size);
      if (*str == NULL) {
          exit(OUT_OF_MEMORY);
      }
  }
  char *cur_ptr = *str + len_str;
  sprintf(cur_ptr, append_format, param, tmp);
  free(tmp);
}


const char *get_error_message(const int error_code) {
    switch (error_code) {
      case INVALID_ARGUMENT_NUMBER:
        return "Invalid argument number";
      case INVALID_COMMAND_PROVIDED:
        return "Invalid command provided";
      case INVALID_NM_ROOT_DIRS:
        return "Invalid NM root dirs";
      case SETUID_OPER_FAILED:
        return "setuid operation failed";
      case UNABLE_TO_EXECUTE_CONTAINER_SCRIPT:
        return "Unable to execute container script";
      case UNABLE_TO_SIGNAL_CONTAINER:
        return "Unable to signal container";
      case INVALID_CONTAINER_PID:
        return "Invalid container PID";
      case OUT_OF_MEMORY:
        return "Out of memory";
      case INITIALIZE_USER_FAILED:
        return "Initialize user failed";
      case PATH_TO_DELETE_IS_NULL:
        return "Path to delete is null";
      case INVALID_CONTAINER_EXEC_PERMISSIONS:
        return "Invalid container-executor permissions";
      case INVALID_CONFIG_FILE:
        return "Invalid config file";
      case SETSID_OPER_FAILED:
        return "setsid operation failed";
      case WRITE_PIDFILE_FAILED:
        return "Write to pidfile failed";
      case WRITE_CGROUP_FAILED:
        return "Write to cgroup failed";
      case TRAFFIC_CONTROL_EXECUTION_FAILED:
        return "Traffic control execution failed";
      case DOCKER_RUN_FAILED:
        return "Docker run failed";
      case ERROR_OPENING_DOCKER_FILE:
        return "Error opening Docker file";
      case ERROR_READING_DOCKER_FILE:
        return "Error reading Docker file";
      case FEATURE_DISABLED:
        return "Feature disabled";
      case COULD_NOT_CREATE_SCRIPT_COPY:
        return "Could not create script copy";
      case COULD_NOT_CREATE_CREDENTIALS_COPY:
        return "Could not create credentials copy";
      case COULD_NOT_CREATE_WORK_DIRECTORIES:
        return "Could not create work dirs";
      case COULD_NOT_CREATE_APP_LOG_DIRECTORIES:
        return "Could not create app log dirs";
      case COULD_NOT_CREATE_TMP_DIRECTORIES:
        return "Could not create tmp dirs";
      case ERROR_CREATE_CONTAINER_DIRECTORIES_ARGUMENTS:
        return "Error in create container directories arguments";
      case ERROR_SANITIZING_DOCKER_COMMAND:
        return "Error sanitizing Docker command";
      case DOCKER_IMAGE_INVALID:
        return "Docker image invalid";
      case ERROR_COMPILING_REGEX:
        return "Error compiling regex";
      case INVALID_CONTAINER_ID:
        return "Invalid container id";
      case DOCKER_EXEC_FAILED:
        return "Docker exec failed";
      case COULD_NOT_CREATE_KEYSTORE_COPY:
        return "Could not create keystore copy";
      case COULD_NOT_CREATE_TRUSTSTORE_COPY:
        return "Could not create truststore copy";
      case ERROR_CALLING_SETVBUF:
        return "Error calling setvbuf";
      case BUFFER_TOO_SMALL:
        return "Buffer too small";
      case INVALID_MOUNT:
        return "Invalid mount";
      case INVALID_RO_MOUNT:
        return "Invalid read-only mount";
      case INVALID_RW_MOUNT:
        return "Invalid read-write mount";
      case MOUNT_ACCESS_ERROR:
        return "Mount access error";
      case INVALID_DOCKER_COMMAND_FILE:
        return "Invalid docker command file passed";
      case INCORRECT_DOCKER_COMMAND:
        return "Incorrect command";
      case INVALID_DOCKER_CONTAINER_NAME:
        return "Invalid docker container name";
      case INVALID_DOCKER_IMAGE_NAME:
        return "Invalid docker image name";
      case INVALID_DOCKER_USER_NAME:
        return "Invalid docker user name";
      case INVALID_DOCKER_INSPECT_FORMAT:
        return "Invalid docker inspect format";
      case UNKNOWN_DOCKER_COMMAND:
        return "Unknown docker command";
      case INVALID_DOCKER_NETWORK:
        return "Invalid docker network";
      case INVALID_DOCKER_PORTS_MAPPING:
        return "Invalid docker ports mapping";
      case INVALID_DOCKER_CAPABILITY:
        return "Invalid docker capability";
      case PRIVILEGED_DOCKER_CONTAINERS_DISABLED:
        return "Privileged docker containers are disabled";
      case INVALID_DOCKER_DEVICE:
        return "Invalid docker device";
      case INVALID_DOCKER_STOP_COMMAND:
        return "Invalid docker stop command";
      case INVALID_DOCKER_KILL_COMMAND:
        return "Invalid docker kill command";
      case INVALID_DOCKER_VOLUME_DRIVER:
        return "Invalid docker volume-driver";
      case INVALID_DOCKER_VOLUME_NAME:
        return "Invalid docker volume name";
      case INVALID_DOCKER_VOLUME_COMMAND:
        return "Invalid docker volume command";
      case DOCKER_PID_HOST_DISABLED:
        return "Docker host pid namespace is disabled";
      case INVALID_DOCKER_PID_NAMESPACE:
        return "Invalid docker pid namespace";
      case INVALID_DOCKER_IMAGE_TRUST:
        return "Docker image is not trusted";
      case INVALID_DOCKER_TMPFS_MOUNT:
        return "Invalid docker tmpfs mount";
      case INVALID_DOCKER_RUNTIME:
        return "Invalid docker runtime";
      case DOCKER_SERVICE_MODE_DISABLED:
        return "Docker service mode disabled";
      case ERROR_RUNC_SETUP_FAILED:
        return "runC setup failed";
      case ERROR_RUNC_RUN_FAILED:
        return "runC run failed";
      case ERROR_RUNC_REAP_LAYER_MOUNTS_FAILED:
        return "runC reap layer mounts failed";
      default:
        return "Unknown error code";
    }
}

int is_regex(const char *str) {
    // regex should begin with prefix "regex:"
    return (strncmp(str, "regex:", 6) == 0);
}
