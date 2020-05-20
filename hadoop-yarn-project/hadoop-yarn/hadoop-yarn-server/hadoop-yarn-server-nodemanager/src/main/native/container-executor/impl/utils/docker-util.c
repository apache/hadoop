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

#include <stdio.h>
#include <unistd.h>
#include <sys/wait.h>
#include <string.h>
#include <strings.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <ctype.h>
#include "../modules/common/module-configs.h"
#include "docker-util.h"
#include "string-utils.h"
#include "util.h"
#include "container-executor.h"
#include <grp.h>
#include <pwd.h>
#include <errno.h>
#include "mount-utils.h"

int entry_point = 0;

static int read_and_verify_command_file(const char *command_file, const char *docker_command,
                                        struct configuration *command_config) {
  int ret = 0;
  ret = read_config(command_file, command_config);
  if (ret != 0) {
    return INVALID_DOCKER_COMMAND_FILE;
  }
  char *command = get_configuration_value("docker-command", DOCKER_COMMAND_FILE_SECTION, command_config);
  if (command == NULL || (strcmp(command, docker_command) != 0)) {
    ret = INCORRECT_DOCKER_COMMAND;
  }
  free(command);
  return ret;
}

static int add_to_args(args *args, const char *string) {
  if (string == NULL) {
    return -1;
  }
  if (args->data == NULL || args->length >= DOCKER_ARG_MAX) {
    return -1;
  }
  char *clone = strdup(string);
  if (clone == NULL) {
    return -1;
  }
  if (args->data != NULL) {
    args->data[args->length] = clone;
    args->length++;
  }
  return 0;
}

void reset_args(args *args) {
  int i = 0;
  if (args == NULL) {
    return;
  }
  for (i = 0; i < args->length; i++) {
    free(args->data[i]);
  }
  args->length = 0;
}

char** extract_execv_args(args* args) {
  char** copy = (char**)malloc((args->length + 1) * sizeof(char*));
  for (int i = 0; i < args->length; i++) {
    copy[i] = args->data[i];
  }
  copy[args->length] = NULL;
  args->length = 0;
  return copy;
}

static int add_param_to_command(const struct configuration *command_config, const char *key, const char *param,
                                const int with_argument, args *args) {
  int ret = 0;
  char *tmp_buffer = NULL;
  char *value = get_configuration_value(key, DOCKER_COMMAND_FILE_SECTION, command_config);
  if (value != NULL) {
    if (with_argument) {
      tmp_buffer = make_string("%s%s", param, value);
      ret = add_to_args(args, tmp_buffer);
      free(tmp_buffer);
    } else if (strcmp(value, "true") == 0) {
      ret = add_to_args(args, param);
    }
    free(value);
    if (ret != 0) {
      ret = BUFFER_TOO_SMALL;
    }
  }
  return ret;
}

int check_trusted_image(const struct configuration *command_config, const struct configuration *conf) {
  int found = 0;
  int i = 0;
  int ret = 0;
  int no_registry_prefix_in_image_name = 0;
  char *image_name = get_configuration_value("image", DOCKER_COMMAND_FILE_SECTION, command_config);
  char *privileged = NULL;
  char **privileged_registry = NULL;
  privileged = get_configuration_value("privileged", DOCKER_COMMAND_FILE_SECTION, command_config);
  if (privileged != NULL && strcasecmp(privileged, "true") == 0 ) {
    privileged_registry = get_configuration_values_delimiter("docker.privileged-containers.registries", CONTAINER_EXECUTOR_CFG_DOCKER_SECTION, conf, ",");
  }
  if (privileged_registry == NULL) {
    privileged_registry = get_configuration_values_delimiter("docker.trusted.registries", CONTAINER_EXECUTOR_CFG_DOCKER_SECTION, conf, ",");
  }
  char *registry_ptr = NULL;
  if (image_name == NULL) {
    ret = INVALID_DOCKER_IMAGE_NAME;
    goto free_and_exit;
  }
  if (strchr(image_name, '/') == NULL) {
    no_registry_prefix_in_image_name = 1;
  }
  if (privileged_registry != NULL) {
    for (i = 0; privileged_registry[i] != NULL; i++) {
      // "library" means we trust public top
      if (strncmp(privileged_registry[i], "library", strlen("library")) == 0) {
        if (no_registry_prefix_in_image_name) {
          // if image doesn't exists, docker pull will automatically happen
          found = 1;
          fprintf(LOGFILE, "image: %s is a trusted top-level image.\n", image_name);
          break;
        }
      }
      int len = strlen(privileged_registry[i]);
      if (privileged_registry[i][len - 1] != '/') {
        registry_ptr = (char *) alloc_and_clear_memory(len + 2, sizeof(char));
        strncpy(registry_ptr, privileged_registry[i], len);
        registry_ptr[len] = '/';
        registry_ptr[len + 1] = '\0';
      } else {
        registry_ptr = strdup(privileged_registry[i]);
      }
      if (strncmp(image_name, registry_ptr, strlen(registry_ptr))==0) {
        found=1;
        free(registry_ptr);
        break;
      }
      free(registry_ptr);
    }
  }
  if (found==0) {
    fprintf(ERRORFILE, "image: %s is not trusted.\n", image_name);
    ret = INVALID_DOCKER_IMAGE_TRUST;
  }

free_and_exit:
  free(privileged);
  free(image_name);
  free_values(privileged_registry);
  return ret;
}

static int is_valid_tmpfs_mount(const char *mount) {
  const char *regex_str = "^/[^:]+$";
  // execute_regex_match return 0 is matched success
  return execute_regex_match(regex_str, mount) == 0;
}

static int is_valid_ports_mapping(const char *ports_mapping) {
  const char *regex_str = "^:[0-9]+|^[0-9]+:[0-9]+|^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.)"
                          "{3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]):[0-9]+:[0-9]+$";
  // execute_regex_match return 0 is matched success
  return execute_regex_match(regex_str, ports_mapping) == 0;
}

static int add_param_to_command_if_allowed(const struct configuration *command_config,
                                           const struct configuration *executor_cfg,
                                           const char *key, const char *allowed_key, const char *param,
                                           const int multiple_values, const char prefix,
                                           args *args) {
  char *tmp_buffer = NULL;
  char *tmp_ptr = NULL;
  char **values = NULL;
  char **permitted_values = get_configuration_values_delimiter(allowed_key,
                                                               CONTAINER_EXECUTOR_CFG_DOCKER_SECTION, executor_cfg,
                                                               ",");
  int i = 0, j = 0, permitted = 0, ret = 0;
  if (multiple_values) {
    values = get_configuration_values_delimiter(key, DOCKER_COMMAND_FILE_SECTION, command_config, ",");
  } else {
    values = (char **) alloc_and_clear_memory(2, sizeof(char *));
    values[0] = get_configuration_value(key, DOCKER_COMMAND_FILE_SECTION, command_config);
    values[1] = NULL;
    if (values[0] == NULL) {
      ret = 0;
      goto free_and_exit;
    }
  }

  if (values != NULL) {
    // Disable capabilities, devices if image is not trusted.
    if (strcmp(key, "net") != 0) {
      if (check_trusted_image(command_config, executor_cfg) != 0) {
        fprintf(ERRORFILE, "Disable %s for untrusted image\n", key);
        ret = INVALID_DOCKER_IMAGE_TRUST;
        goto free_and_exit;
      }
    }

    if (permitted_values != NULL) {
      // Values are user requested.
      for (i = 0; values[i] != NULL; ++i) {
        permitted = 0;
        if(prefix != 0) {
          tmp_ptr = strchr(values[i], prefix);
          if (tmp_ptr == NULL) {
            fprintf(ERRORFILE, "Prefix char '%c' not found in '%s'\n",
                    prefix, values[i]);
            ret = -1;
            goto free_and_exit;
          }
        }
        // Iterate through each permitted value
        char* dst = NULL;
        char* pattern = NULL;

        for (j = 0; permitted_values[j] != NULL; ++j) {
          if (prefix == 0) {
            ret = strcmp(values[i], permitted_values[j]);
          } else {
            // If permitted-Values[j] is a REGEX, use REGEX to compare
            if (is_regex(permitted_values[j])) {
              dst = strndup(values[i], tmp_ptr - values[i]);
              pattern = strdup(permitted_values[j] + 6);
              ret = execute_regex_match(pattern, dst);
              free(dst);
              free(pattern);
            } else {
              ret = strncmp(values[i], permitted_values[j], tmp_ptr - values[i]);
            }
          }
          if (ret == 0) {
            permitted = 1;
            break;
          }
        }
        if (permitted == 1) {
          tmp_buffer = make_string("%s%s", param, values[i]);
          ret = add_to_args(args, tmp_buffer);
          free(tmp_buffer);
          if (ret != 0) {
            fprintf(ERRORFILE, "Output buffer too small\n");
            ret = BUFFER_TOO_SMALL;
            goto free_and_exit;
          }
        } else {
          fprintf(ERRORFILE, "Invalid param '%s' requested\n", values[i]);
          ret = -1;
          goto free_and_exit;
        }
      }
    } else {
      fprintf(ERRORFILE, "Invalid param '%s' requested, "
          "permitted values list is empty\n", values[0]);
      ret = -1;
      goto free_and_exit;
    }
  }

free_and_exit:
  free_values(values);
  free_values(permitted_values);
  return ret;
}

static int add_docker_config_param(const struct configuration *command_config, args *args) {
  return add_param_to_command(command_config, "docker-config", "--config=", 1, args);
}

static int validate_volume_name(const char *volume_name) {
  const char *regex_str = "^[a-zA-Z0-9]([a-zA-Z0-9_.-]*)$";
  return execute_regex_match(regex_str, volume_name);
}

static int validate_container_name(const char *container_name) {
  const char *CONTAINER_NAME_PREFIX = "container_";
  if (0 == strncmp(container_name, CONTAINER_NAME_PREFIX, strlen(CONTAINER_NAME_PREFIX))) {
    if (1 == validate_container_id(container_name)) {
      return 0;
    }
  }
  fprintf(ERRORFILE, "Specified container_id=%s is invalid\n", container_name);
  return INVALID_DOCKER_CONTAINER_NAME;
}

int get_max_retries(const struct configuration *conf) {
  int retries = 10;
  char *max_retries = get_configuration_value(DOCKER_INSPECT_MAX_RETRIES_KEY,
      CONTAINER_EXECUTOR_CFG_DOCKER_SECTION, conf);
  if (max_retries != NULL) {
    retries = atoi(max_retries);
    free(max_retries);
  }
  return retries;
}

char *get_docker_binary(const struct configuration *conf) {
  char *docker_binary = NULL;
  docker_binary = get_configuration_value(DOCKER_BINARY_KEY, CONTAINER_EXECUTOR_CFG_DOCKER_SECTION, conf);
  if (docker_binary == NULL) {
    docker_binary = get_configuration_value(DOCKER_BINARY_KEY, "", conf);
    if (docker_binary == NULL) {
      docker_binary = strdup("/usr/bin/docker");
    }
  }
  return docker_binary;
}

int get_use_entry_point_flag() {
  return entry_point;
}

int docker_module_enabled(const struct configuration *conf) {
  struct section *section = get_configuration_section(CONTAINER_EXECUTOR_CFG_DOCKER_SECTION, conf);
  if (section != NULL) {
    return module_enabled(section, CONTAINER_EXECUTOR_CFG_DOCKER_SECTION);
  }
  return 0;
}

int get_docker_command(const char *command_file, const struct configuration *conf, args *args) {
  int ret = 0;
  struct configuration command_config = {0, NULL};

  ret = read_config(command_file, &command_config);
  if (ret != 0) {
    free_configuration(&command_config);
    return INVALID_DOCKER_COMMAND_FILE;
  }

  char *docker = get_docker_binary(conf);
  ret = add_to_args(args, docker);
  free(docker);
  if (ret != 0) {
    free_configuration(&command_config);
    return BUFFER_TOO_SMALL;
  }

  ret = add_docker_config_param(&command_config, args);
  if (ret != 0) {
    free_configuration(&command_config);
    return BUFFER_TOO_SMALL;
  }

  char *command = get_configuration_value("docker-command", DOCKER_COMMAND_FILE_SECTION, &command_config);
  free_configuration(&command_config);
  if (strcmp(DOCKER_INSPECT_COMMAND, command) == 0) {
    ret = get_docker_inspect_command(command_file, conf, args);
  } else if (strcmp(DOCKER_KILL_COMMAND, command) == 0) {
    ret = get_docker_kill_command(command_file, conf, args);
  } else if (strcmp(DOCKER_LOAD_COMMAND, command) == 0) {
    ret = get_docker_load_command(command_file, conf, args);
  } else if (strcmp(DOCKER_PULL_COMMAND, command) == 0) {
    ret = get_docker_pull_command(command_file, conf, args);
  } else if (strcmp(DOCKER_RM_COMMAND, command) == 0) {
    ret = get_docker_rm_command(command_file, conf, args);
  } else if (strcmp(DOCKER_RUN_COMMAND, command) == 0) {
    ret = get_docker_run_command(command_file, conf, args);
  } else if (strcmp(DOCKER_STOP_COMMAND, command) == 0) {
    ret = get_docker_stop_command(command_file, conf, args);
  } else if (strcmp(DOCKER_VOLUME_COMMAND, command) == 0) {
    ret = get_docker_volume_command(command_file, conf, args);
  } else if (strcmp(DOCKER_START_COMMAND, command) == 0) {
    ret = get_docker_start_command(command_file, conf, args);
  } else if (strcmp(DOCKER_EXEC_COMMAND, command) == 0) {
    ret = get_docker_exec_command(command_file, conf, args);
  } else if (strcmp(DOCKER_IMAGES_COMMAND, command) == 0) {
      ret = get_docker_images_command(command_file, conf, args);
  } else {
    ret = UNKNOWN_DOCKER_COMMAND;
  }
  free(command);
  return ret;
}

// check if a key is permitted in the configuration
// return 1 if permitted
static int value_permitted(const struct configuration* executor_cfg,
                           const char* key, const char* value) {
  char **permitted_values = get_configuration_values_delimiter(key,
    CONTAINER_EXECUTOR_CFG_DOCKER_SECTION, executor_cfg, ",");
  if (!permitted_values) {
    return 0;
  }

  char** permitted = permitted_values;
  int found = 0;

  while (*permitted) {
    if (0 == strncmp(*permitted, value, 1024)) {
      found = 1;
      break;
    }
    permitted++;
  }

  free_values(permitted_values);

  return found;
}

int get_docker_volume_command(const char *command_file, const struct configuration *conf, args *args) {
  int ret = 0;
  char *driver = NULL, *volume_name = NULL, *sub_command = NULL, *format = NULL;
  struct configuration command_config = {0, NULL};
  ret = read_and_verify_command_file(command_file, DOCKER_VOLUME_COMMAND, &command_config);
  if (ret != 0) {
    goto cleanup;
  }
  sub_command = get_configuration_value("sub-command", DOCKER_COMMAND_FILE_SECTION, &command_config);

  if ((sub_command == NULL) || ((0 != strcmp(sub_command, "create")) &&
      (0 != strcmp(sub_command, "ls")))) {
    fprintf(ERRORFILE, "\"create/ls\" are the only acceptable sub-command of volume, input sub_command=\"%s\"\n",
       sub_command);
    ret = INVALID_DOCKER_VOLUME_COMMAND;
    goto cleanup;
  }

  ret = add_to_args(args, DOCKER_VOLUME_COMMAND);
  if (ret != 0) {
    goto cleanup;
  }

  if (0 == strcmp(sub_command, "create")) {
    volume_name = get_configuration_value("volume", DOCKER_COMMAND_FILE_SECTION, &command_config);
    if (volume_name == NULL || validate_volume_name(volume_name) != 0) {
      fprintf(ERRORFILE, "%s is not a valid volume name.\n", volume_name);
      ret = INVALID_DOCKER_VOLUME_NAME;
      goto cleanup;
    }

    driver = get_configuration_value("driver", DOCKER_COMMAND_FILE_SECTION, &command_config);
    if (driver == NULL) {
      ret = INVALID_DOCKER_VOLUME_DRIVER;
      goto cleanup;
    }

    ret = add_to_args(args, "create");
    if (ret != 0) {
      goto cleanup;
    }

    char *name_buffer = make_string("--name=%s", volume_name);
    ret = add_to_args(args, name_buffer);
    free(name_buffer);
    if (ret != 0) {
      ret = BUFFER_TOO_SMALL;
      goto cleanup;
    }

    if (!value_permitted(conf, "docker.allowed.volume-drivers", driver)) {
      fprintf(ERRORFILE, "%s is not permitted docker.allowed.volume-drivers\n",
        driver);
      ret = INVALID_DOCKER_VOLUME_DRIVER;
      goto cleanup;
    }

    char *driver_buffer = make_string("--driver=%s", driver);
    ret = add_to_args(args, driver_buffer);
    free(driver_buffer);
    if (ret != 0) {
      ret = BUFFER_TOO_SMALL;
      goto cleanup;
    }
  } else if (0 == strcmp(sub_command, "ls")) {
    format = get_configuration_value("format", DOCKER_COMMAND_FILE_SECTION, &command_config);

    ret = add_to_args(args, "ls");
    if (ret != 0) {
      goto cleanup;
    }
    if (format) {
      char *tmp_buffer = make_string("--format=%s", format);
      ret = add_to_args(args, tmp_buffer);
      free(tmp_buffer);
      if (ret != 0) {
        ret = BUFFER_TOO_SMALL;
        goto cleanup;
      }
    }
  }

cleanup:
  free_configuration(&command_config);
  free(driver);
  free(volume_name);
  free(sub_command);
  free(format);
  return ret;
}

int get_docker_inspect_command(const char *command_file, const struct configuration *conf, args *args) {
  const char *valid_format_strings[] = {"{{.State.Status}}",
                                "{{range(.NetworkSettings.Networks)}}{{.IPAddress}},{{end}}{{.Config.Hostname}}",
                                "{{json .NetworkSettings.Ports}}",
                                "{{.State.Status}},{{.Config.StopSignal}}"};
  int ret = 0, i = 0, valid_format = 0;
  char *format = NULL, *container_name = NULL, *tmp_buffer = NULL;
  struct configuration command_config = {0, NULL};
  ret = read_and_verify_command_file(command_file, DOCKER_INSPECT_COMMAND, &command_config);
  if (ret != 0) {
    goto free_and_exit;
  }

  container_name = get_configuration_value("name", DOCKER_COMMAND_FILE_SECTION, &command_config);
  if (container_name == NULL || validate_container_name(container_name) != 0) {
    ret = INVALID_DOCKER_CONTAINER_NAME;
    goto free_and_exit;
  }

  format = get_configuration_value("format", DOCKER_COMMAND_FILE_SECTION, &command_config);
  if (format == NULL) {
    ret = INVALID_DOCKER_INSPECT_FORMAT;
    goto free_and_exit;
  }

  for (i = 0; i < 4; ++i) {
    if (strcmp(format, valid_format_strings[i]) == 0) {
      valid_format = 1;
      break;
    }
  }
  if (valid_format != 1) {
    fprintf(ERRORFILE, "Invalid format option '%s' not permitted\n", format);
    ret = INVALID_DOCKER_INSPECT_FORMAT;
    goto free_and_exit;
  }

  ret = add_to_args(args, DOCKER_INSPECT_COMMAND);
  if (ret != 0) {
    goto free_and_exit;
  }
  tmp_buffer = make_string("--format=%s", format);
  ret = add_to_args(args, tmp_buffer);
  free(tmp_buffer);
  if (ret != 0) {
    goto free_and_exit;
  }
  ret = add_to_args(args, container_name);
  if (ret != 0) {
    goto free_and_exit;
  }

free_and_exit:
  free_configuration(&command_config);
  free(format);
  free(container_name);
  return ret;
}

int get_docker_load_command(const char *command_file, const struct configuration *conf, args *args) {
  int ret = 0;
  char *image_name = NULL;
  struct configuration command_config = {0, NULL};
  ret = read_and_verify_command_file(command_file, DOCKER_LOAD_COMMAND, &command_config);
  if (ret != 0) {
    goto free_and_exit;
  }

  image_name = get_configuration_value("image", DOCKER_COMMAND_FILE_SECTION, &command_config);
  if (image_name == NULL) {
    ret = INVALID_DOCKER_IMAGE_NAME;
    goto free_and_exit;
  }

  ret = add_to_args(args, DOCKER_LOAD_COMMAND);
  if (ret == 0) {
    char *tmp_buffer = make_string("--i=%s", image_name);
    ret = add_to_args(args, tmp_buffer);
    free(tmp_buffer);
    if (ret != 0) {
      ret = BUFFER_TOO_SMALL;
    }
  }
free_and_exit:
  free(image_name);
  free_configuration(&command_config);
  return ret;
}

static int validate_docker_image_name(const char *image_name) {
  const char *regex_str = "^(([a-zA-Z0-9.-]+)(:[0-9]+)?/)?([a-z0-9_./-]+)(:[a-zA-Z0-9_.-]+)?$";
  return execute_regex_match(regex_str, image_name);
}

int get_docker_pull_command(const char *command_file, const struct configuration *conf, args *args) {
  int ret = 0;
  char *image_name = NULL;
  struct configuration command_config = {0, NULL};
  ret = read_and_verify_command_file(command_file, DOCKER_PULL_COMMAND, &command_config);
  if (ret != 0) {
    goto free_pull;
  }

  image_name = get_configuration_value("image", DOCKER_COMMAND_FILE_SECTION, &command_config);
  if (image_name == NULL || validate_docker_image_name(image_name) != 0) {
    ret = INVALID_DOCKER_IMAGE_NAME;
    goto free_pull;
  }

  ret = add_to_args(args, DOCKER_PULL_COMMAND);
  if (ret == 0) {
    ret = add_to_args(args, image_name);
  }
free_pull:
  free(image_name);
  free_configuration(&command_config);
  return ret;
}

int get_docker_rm_command(const char *command_file, const struct configuration *conf, args *args) {
  int ret = 0;
  char *container_name = NULL;
  struct configuration command_config = {0, NULL};
  ret = read_and_verify_command_file(command_file, DOCKER_RM_COMMAND, &command_config);
  if (ret != 0) {
    goto free_and_exit;
  }

  container_name = get_configuration_value("name", DOCKER_COMMAND_FILE_SECTION, &command_config);
  if (container_name == NULL || validate_container_name(container_name) != 0) {
    ret = INVALID_DOCKER_CONTAINER_NAME;
    goto free_and_exit;
  }

  ret = add_to_args(args, DOCKER_RM_COMMAND);
  if (ret != 0) {
    ret = BUFFER_TOO_SMALL;
    goto free_and_exit;
  }
  ret = add_to_args(args, "-f");
  if (ret != 0) {
    ret = BUFFER_TOO_SMALL;
    goto free_and_exit;
  }
  ret = add_to_args(args, container_name);
  if (ret != 0) {
    ret = BUFFER_TOO_SMALL;
    goto free_and_exit;
  }
free_and_exit:
  free(container_name);
  free_configuration(&command_config);
  return ret;
}

int get_docker_stop_command(const char *command_file, const struct configuration *conf,
                            args *args) {
  int ret = 0;
  size_t len = 0, i = 0;
  char *value = NULL;
  char *container_name = NULL;
  struct configuration command_config = {0, NULL};
  ret = read_and_verify_command_file(command_file, DOCKER_STOP_COMMAND, &command_config);
  if (ret != 0) {
    goto free_and_exit;
  }

  container_name = get_configuration_value("name", DOCKER_COMMAND_FILE_SECTION, &command_config);
  if (container_name == NULL || validate_container_name(container_name) != 0) {
    ret = INVALID_DOCKER_CONTAINER_NAME;
    goto free_and_exit;
  }

  ret = add_to_args(args, DOCKER_STOP_COMMAND);
  if (ret == 0) {
    value = get_configuration_value("time", DOCKER_COMMAND_FILE_SECTION, &command_config);
    if (value != NULL) {
      len = strlen(value);
      for (i = 0; i < len; ++i) {
        if (isdigit(value[i]) == 0) {
          fprintf(ERRORFILE, "Value for time is not a number '%s'\n", value);
          ret = INVALID_DOCKER_STOP_COMMAND;
          goto free_and_exit;
        }
      }
      char *time_buffer = make_string("--time=%s", value);
      ret = add_to_args(args, time_buffer);
      free(time_buffer);
      if (ret != 0) {
        goto free_and_exit;
      }
    }
    ret = add_to_args(args, container_name);
  }
free_and_exit:
  free(value);
  free(container_name);
  free_configuration(&command_config);
  return ret;
}

int get_docker_kill_command(const char *command_file, const struct configuration *conf,
                            args *args) {
  int ret = 0;
  size_t len = 0, i = 0;
  char *value = NULL;
  char *container_name = NULL;
  struct configuration command_config = {0, NULL};
  ret = read_and_verify_command_file(command_file, DOCKER_KILL_COMMAND, &command_config);
  if (ret != 0) {
    goto free_and_exit;
  }

  container_name = get_configuration_value("name", DOCKER_COMMAND_FILE_SECTION, &command_config);
  if (container_name == NULL || validate_container_name(container_name) != 0) {
    ret = INVALID_DOCKER_CONTAINER_NAME;
    goto free_and_exit;
  }

  ret = add_to_args(args, DOCKER_KILL_COMMAND);
  if (ret == 0) {
    value = get_configuration_value("signal", DOCKER_COMMAND_FILE_SECTION, &command_config);
    if (value != NULL) {
      len = strlen(value);
      for (i = 0; i < len; ++i) {
        if (isupper(value[i]) == 0) {
          fprintf(ERRORFILE, "Value for signal contains non-uppercase characters '%s'\n", value);
          ret = INVALID_DOCKER_KILL_COMMAND;
          goto free_and_exit;
        }
      }

      char *signal_buffer = make_string("--signal=%s", value);
      ret = add_to_args(args, signal_buffer);
      free(signal_buffer);
      if (ret != 0) {
        goto free_and_exit;
      }
    }
    ret = add_to_args(args, container_name);
  }
free_and_exit:
  free(value);
  free(container_name);
  free_configuration(&command_config);
  return ret;
}

int get_docker_start_command(const char *command_file, const struct configuration *conf, args *args) {
  int ret = 0;
  char *container_name = NULL;
  struct configuration command_config = {0, NULL};
  ret = read_and_verify_command_file(command_file, DOCKER_START_COMMAND, &command_config);
  if (ret != 0) {
    goto free_and_exit;
  }

  container_name = get_configuration_value("name", DOCKER_COMMAND_FILE_SECTION, &command_config);
  if (container_name == NULL || validate_container_name(container_name) != 0) {
    ret = INVALID_DOCKER_CONTAINER_NAME;
    goto free_and_exit;
  }

  ret = add_to_args(args, DOCKER_START_COMMAND);
  if (ret != 0) {
    goto free_and_exit;
  }
  ret = add_to_args(args, container_name);
free_and_exit:
  free(container_name);
  free_configuration(&command_config);
  return ret;
}

int get_docker_exec_command(const char *command_file, const struct configuration *conf, args *args) {
  int ret = 0, i = 0;
  char *container_name = NULL;
  char **launch_command = NULL;
  struct configuration command_config = {0, NULL};
  ret = read_and_verify_command_file(command_file, DOCKER_EXEC_COMMAND, &command_config);
  if (ret != 0) {
    goto free_and_exit;
  }

  container_name = get_configuration_value("name", DOCKER_COMMAND_FILE_SECTION, &command_config);
  if (container_name == NULL || validate_container_name(container_name) != 0) {
    ret = INVALID_DOCKER_CONTAINER_NAME;
    goto free_and_exit;
  }

  ret = add_to_args(args, DOCKER_EXEC_COMMAND);
  if (ret != 0) {
    goto free_and_exit;
  }

  ret = add_to_args(args, "-it");
  if (ret != 0) {
    goto free_and_exit;
  }

  ret = add_to_args(args, container_name);
  if (ret != 0) {
    goto free_and_exit;
  }

  launch_command = get_configuration_values_delimiter("launch-command", DOCKER_COMMAND_FILE_SECTION, &command_config,
                                                      ",");
  if (launch_command != NULL) {
    for (i = 0; launch_command[i] != NULL; ++i) {
      ret = add_to_args(args, launch_command[i]);
      if (ret != 0) {
        ret = BUFFER_TOO_SMALL;
        goto free_and_exit;
      }
    }
  } else {
    ret = INVALID_DOCKER_COMMAND_FILE;
  }
free_and_exit:
  free(container_name);
  free_configuration(&command_config);
  free_values(launch_command);
  return ret;
}

static int detach_container(const struct configuration *command_config, args *args) {
  return add_param_to_command(command_config, "detach", "-d", 0, args);
}

static int  rm_container_on_exit(const struct configuration *command_config, args *args) {
  return add_param_to_command(command_config, "rm", "--rm", 0, args);
}

static int set_container_workdir(const struct configuration *command_config, args *args) {
  return add_param_to_command(command_config, "workdir", "--workdir=", 1, args);
}

static int set_cgroup_parent(const struct configuration *command_config, args *args) {
  return add_param_to_command(command_config, "cgroup-parent", "--cgroup-parent=", 1, args);
}

static int set_hostname(const struct configuration *command_config, args *args) {
  return add_param_to_command(command_config, "hostname", "--hostname=", 1, args);
}

static int set_group_add(const struct configuration *command_config, args *args) {
  int i = 0, ret = 0;
  char **group_add = get_configuration_values_delimiter("group-add", DOCKER_COMMAND_FILE_SECTION, command_config, ",");
  char *privileged = NULL;

  privileged = get_configuration_value("privileged", DOCKER_COMMAND_FILE_SECTION, command_config);
  if (privileged != NULL && strcasecmp(privileged, "true") == 0 ) {
    goto free_and_exit;
  }

  if (group_add != NULL) {
    for (i = 0; group_add[i] != NULL; ++i) {
      ret = add_to_args(args, "--group-add");
      if (ret != 0) {
        goto free_and_exit;
      }
      ret = add_to_args(args, group_add[i]);
      if (ret != 0) {
        goto free_and_exit;
      }
    }
  }
free_and_exit:
  free_values(group_add);
  free(privileged);
  return ret;
}

static int set_network(const struct configuration *command_config,
                       const struct configuration *conf, args *args) {

  int ret = 0;
  ret = add_param_to_command_if_allowed(command_config, conf, "net",
                                        "docker.allowed.networks", "--net=",
                                        0, 0, args);
  if (ret != 0) {
    fprintf(ERRORFILE, "Could not find requested network in allowed networks\n");
    ret = INVALID_DOCKER_NETWORK;
  }

  return ret;
}

static int set_runtime(const struct configuration *command_config,
                       const struct configuration *conf, args *args) {
  int ret = 0;
  ret = add_param_to_command_if_allowed(command_config, conf, "runtime",
                                        "docker.allowed.runtimes", "--runtime=",
                                        0, 0, args);
  if (ret != 0) {
    fprintf(ERRORFILE, "Could not find requested runtime in allowed runtimes\n");
    ret = INVALID_DOCKER_RUNTIME;
  }
  return ret;
}

int is_service_mode_enabled(const struct configuration *command_config,
                            const struct configuration *executor_cfg, args *args) {
    int ret = 0;
    struct section *section = get_configuration_section(CONTAINER_EXECUTOR_CFG_DOCKER_SECTION, executor_cfg);
    char *value = get_configuration_value("service-mode", DOCKER_COMMAND_FILE_SECTION, command_config);
    if (value != NULL && strcasecmp(value, "true") == 0) {
      if (is_feature_enabled(DOCKER_SERVICE_MODE_ENABLED_KEY, ret, section)) {
        ret = 1;
      } else {
        ret = DOCKER_SERVICE_MODE_DISABLED;
      }
     }
    free(value);
    return ret;
}

static int add_ports_mapping_to_command(const struct configuration *command_config, args *args) {
  int i = 0, ret = 0;
  char *network_type = (char*) malloc(128);
  char *docker_network_command = NULL;
  char *docker_binary = get_docker_binary(command_config);
  char *network_name = get_configuration_value("net", DOCKER_COMMAND_FILE_SECTION, command_config);
  char **ports_mapping_values = get_configuration_values_delimiter("ports-mapping", DOCKER_COMMAND_FILE_SECTION, command_config, ",");
  if (network_name != NULL) {
    docker_network_command = make_string("%s network inspect %s --format='{{.Driver}}'", docker_binary, network_name);
    FILE* docker_network = popen(docker_network_command, "r");
    ret = fscanf(docker_network, "%s", network_type);
    if (pclose (docker_network) != 0 || ret <= 0) {
      fprintf (ERRORFILE, "Could not inspect docker network to get type %s.\n", docker_network_command);
      goto cleanup;
    }
    // other network type exit successfully without ports mapping
    if (strcasecmp(network_type, "bridge") != 0) {
      ret = 0;
      goto cleanup;
    }
    // add -P when not configure ports mapping
    if (ports_mapping_values == NULL) {
      ret = add_to_args(args, "-P");
      if (ret != 0) {
        ret = BUFFER_TOO_SMALL;
      }
    }
  }
  // add -p when configure ports mapping
  if (ports_mapping_values != NULL) {
    for (i = 0; ports_mapping_values[i] != NULL; i++) {
      if (!is_valid_ports_mapping(ports_mapping_values[i])) {
         fprintf (ERRORFILE, "Invalid port mappings:  %s.\n", ports_mapping_values[i]);
         ret = INVALID_DOCKER_PORTS_MAPPING;
         break;
      }
      ret = add_to_args(args, "-p");
      if (ret != 0) {
        ret = BUFFER_TOO_SMALL;
        break;
      }
      ret = add_to_args(args, ports_mapping_values[i]);
      if (ret != 0) {
        ret = BUFFER_TOO_SMALL;
        break;
      }
    }
  }

cleanup:
  free(network_type);
  free(docker_binary);
  free(network_name);
  free(docker_network_command);
  free_values(ports_mapping_values);
  return ret;
}

static int set_pid_namespace(const struct configuration *command_config,
                   const struct configuration *conf, args *args) {
  char *value = get_configuration_value("pid", DOCKER_COMMAND_FILE_SECTION,
      command_config);
  char *pid_host_enabled = get_configuration_value("docker.host-pid-namespace.enabled",
      CONTAINER_EXECUTOR_CFG_DOCKER_SECTION, conf);
  int ret = 0;

  if (value != NULL) {
    if (strcmp(value, "host") == 0) {
      if (pid_host_enabled != NULL) {
        if (strcmp(pid_host_enabled, "1") == 0 ||
            strcasecmp(pid_host_enabled, "True") == 0) {
          ret = add_to_args(args, "--pid=host");
          if (ret != 0) {
            ret = BUFFER_TOO_SMALL;
          }
        } else {
          fprintf(ERRORFILE, "Host pid namespace is disabled\n");
          ret = DOCKER_PID_HOST_DISABLED;
          goto free_and_exit;
        }
      } else {
        fprintf(ERRORFILE, "Host pid namespace is disabled\n");
        ret = DOCKER_PID_HOST_DISABLED;
        goto free_and_exit;
      }
    } else {
      fprintf(ERRORFILE, "Invalid pid namespace\n");
      ret = INVALID_DOCKER_PID_NAMESPACE;
    }
  }

free_and_exit:
  free(value);
  free(pid_host_enabled);
  return ret;
}

static int set_capabilities(const struct configuration *command_config,
                            const struct configuration *conf, args *args) {
  int ret = 0;

  ret = add_to_args(args, "--cap-drop=ALL");
  if (ret != 0) {
    return BUFFER_TOO_SMALL;
  }

  ret = add_param_to_command_if_allowed(command_config, conf, "cap-add",
                                        "docker.allowed.capabilities",
                                        "--cap-add=", 1, 0,
                                        args);
  switch (ret) {
    case 0:
      break;
    case INVALID_DOCKER_IMAGE_TRUST:
      fprintf(ERRORFILE, "Docker capability disabled for untrusted image\n");
      ret = 0;
      break;
    default:
      fprintf(ERRORFILE, "Invalid docker capability requested\n");
      ret = INVALID_DOCKER_CAPABILITY;
  }

  return ret;
}

static int set_devices(const struct configuration *command_config, const struct configuration *conf, args *args) {
  int ret = 0;
  ret = add_param_to_command_if_allowed(command_config, conf, "devices", "docker.allowed.devices", "--device=", 1, ':',
                                        args);
  if (ret != 0) {
    fprintf(ERRORFILE, "Invalid docker device requested\n");
    ret = INVALID_DOCKER_DEVICE;
  }

  return ret;
}

static int set_env(const struct configuration *command_config, struct args *args) {
  int ret = 0;
  // Use envfile method.
  char *envfile = get_configuration_value("environ", DOCKER_COMMAND_FILE_SECTION, command_config);
  if (envfile != NULL) {
    ret = add_to_args(args, "--env-file");
    if (ret != 0) {
      ret = BUFFER_TOO_SMALL;
    }
    ret = add_to_args(args, envfile);
    if (ret != 0) {
      ret = BUFFER_TOO_SMALL;
    }
    free(envfile);
  }
  return ret;
}

static int add_tmpfs_mounts(const struct configuration *command_config, args *args) {
  char **values = get_configuration_values_delimiter("tmpfs", DOCKER_COMMAND_FILE_SECTION, command_config, ",");
  int i = 0, ret = 0;
  if (values == NULL) {
    goto free_and_exit;
  }
  for (i = 0; values[i] != NULL; i++) {
    if (!is_valid_tmpfs_mount(values[i])) {
      fprintf(ERRORFILE, "Invalid docker tmpfs mount '%s'\n", values[i]);
      ret = INVALID_DOCKER_TMPFS_MOUNT;
      goto free_and_exit;
    }
    ret = add_to_args(args, "--tmpfs");
    if (ret != 0) {
      ret = BUFFER_TOO_SMALL;
      goto free_and_exit;
    }
    ret = add_to_args(args, values[i]);
    if (ret != 0) {
      ret = BUFFER_TOO_SMALL;
      goto free_and_exit;
    }
  }

free_and_exit:
  free_values(values);
  return ret;
}



static int check_privileges(const char *user) {
  int ngroups = 0;
  gid_t *groups = NULL;
  struct passwd *pw;
  struct group *gr;
  int ret = 0;
  int waitid = -1;
  int statval = 0;

  pw = getpwnam(user);
  if (pw == NULL) {
    fprintf(ERRORFILE, "User %s does not exist in host OS.\n", user);
    exit(INITIALIZE_USER_FAILED);
  }

#ifdef __linux__
  int rc = getgrouplist(user, pw->pw_gid, groups, &ngroups);
#else
  int rc = getgrouplist(user, pw->pw_gid, (int *)groups, &ngroups);
#endif
  if (rc < 0) {
    groups = (gid_t *) alloc_and_clear_memory(ngroups, sizeof(gid_t));
    if (groups == NULL) {
      fprintf(ERRORFILE, "Failed to allocate buffer for group lookup for user %s.\n", user);
      exit(OUT_OF_MEMORY);
    }
#ifdef __linux__
    if (getgrouplist(user, pw->pw_gid, groups, &ngroups) == -1) {
#else
    if (getgrouplist(user, pw->pw_gid, (int *)groups, &ngroups) == -1) {
#endif
      fprintf(ERRORFILE, "Fail to lookup groups for user %s.\n", user);
      ret = 2;
    }
  }

  if (ret != 2) {
    for (int j = 0; j < ngroups; j++) {
      gr = getgrgid(groups[j]);
      if (gr != NULL) {
        if (strcmp(gr->gr_name, "root")==0 || strcmp(gr->gr_name, "docker")==0) {
          ret = 1;
          break;
        }
      }
    }
  }

  if (ret != 1) {
    int child_pid = fork();
    if (child_pid == 0) {
      execl("/usr/bin/sudo", "sudo", "-U", user, "-n", "-l", "docker", NULL);
      fprintf(ERRORFILE, "could not invoke docker with sudo - %s\n", strerror(errno));
      _exit(INITIALIZE_USER_FAILED);
    } else {
      while ((waitid = waitpid(child_pid, &statval, 0)) != child_pid) {
        if (waitid == -1 && errno != EINTR) {
          fprintf(ERRORFILE, "waitpid failed: %s\n", strerror(errno));
          break;
        }
      }
      if (waitid == child_pid) {
        if (WIFEXITED(statval)) {
          if (WEXITSTATUS(statval) == 0) {
            ret = 1;
          }
        } else if (WIFSIGNALED(statval)) {
          fprintf(ERRORFILE, "sudo terminated by signal %d\n", WTERMSIG(statval));
        }
      }
    }
  }
  free(groups);
  if (ret == 1) {
    fprintf(ERRORFILE, "check privileges passed for user: %s\n", user);
  } else {
    fprintf(ERRORFILE, "check privileges failed for user: %s, error code: %d\n", user, ret);
    ret = 0;
  }
  return ret;
}

static int set_privileged(const struct configuration *command_config, const struct configuration *conf, args *args) {
  char *user = NULL;
  char *value = get_configuration_value("privileged", DOCKER_COMMAND_FILE_SECTION, command_config);
  char *privileged_container_enabled
      = get_configuration_value("docker.privileged-containers.enabled", CONTAINER_EXECUTOR_CFG_DOCKER_SECTION, conf);
  int ret = 0;
  int allowed = 1;

  user = get_configuration_value("user", DOCKER_COMMAND_FILE_SECTION, command_config);
  if (user == NULL) {
    ret = INVALID_DOCKER_USER_NAME;
    goto free_and_exit;
  }

  if (value != NULL && strcasecmp(value, "true") == 0 ) {
    if (privileged_container_enabled != NULL) {
      if (strcmp(privileged_container_enabled, "1") == 0 ||
          strcasecmp(privileged_container_enabled, "True") == 0) {
        // Disable set privileged if entry point mode is disabled
        if (get_use_entry_point_flag() != 1) {
          fprintf(ERRORFILE, "Privileged containers are disabled for non-entry-point mode\n");
          ret = PRIVILEGED_DOCKER_CONTAINERS_DISABLED;
          goto free_and_exit;
        }
        // Disable set privileged if image is not trusted.
        if (check_trusted_image(command_config, conf) != 0) {
          fprintf(ERRORFILE, "Privileged containers are disabled from untrusted source\n");
          ret = PRIVILEGED_DOCKER_CONTAINERS_DISABLED;
          goto free_and_exit;
        }
        allowed = check_privileges(user);
        if (allowed) {
          ret = add_to_args(args, "--privileged");
          if (ret != 0) {
            ret = BUFFER_TOO_SMALL;
          }
        } else {
          fprintf(ERRORFILE, "Privileged containers are disabled for user: %s\n", user);
          ret = PRIVILEGED_DOCKER_CONTAINERS_DISABLED;
          goto free_and_exit;
        }
      } else {
        fprintf(ERRORFILE, "Privileged containers are disabled\n");
        ret = PRIVILEGED_DOCKER_CONTAINERS_DISABLED;
        goto free_and_exit;
      }
    } else {
      fprintf(ERRORFILE, "Privileged containers are disabled\n");
      ret = PRIVILEGED_DOCKER_CONTAINERS_DISABLED;
      goto free_and_exit;
    }
  }

free_and_exit:
  free(value);
  free(privileged_container_enabled);
  free(user);
  return ret;
}



static char* get_docker_mount_source(const char *mount) {
    const char *tmp = strchr(mount, ':');
    if (tmp == NULL) {
        fprintf(ERRORFILE, "Invalid docker mount '%s'\n", mount);
        return NULL;
    }
    size_t len = tmp - mount;
    return strndup(mount, len);
}

static char* get_docker_mount_dest(const char *mount) {
    size_t len;
    const char *start = strchr(mount, ':') + 1;
    if (start == NULL) {
        fprintf(ERRORFILE, "Invalid docker mount '%s'\n", mount);
        return NULL;
    }

    const char *end = strchr(start, ':');
    if (end == NULL) {
        len = strlen(mount) - (start - mount);
    } else {
        len = end - start;
    }
    return strndup(start, len);
}

static mount_options* get_docker_mount_options(const char *mount_string) {
    char **opts = NULL;
    mount_options *options = NULL;
    unsigned int num_opts = 0;
    char *option = NULL;
    int len = 0;

    //+1 because we don't care about the ':'
    const char *option_string = strrchr(mount_string, ':') + 1;
    if (option_string == NULL) {
        fprintf(ERRORFILE, "Invalid docker mount '%s'\n", mount_string);
        return NULL;
    }

    options = (mount_options *) calloc(1, sizeof(*options));
    if (options == NULL) {
        fprintf(ERRORFILE, "Unable to allocate %ld bytes\n", sizeof(*options));
        return NULL;
    }

    len = strlen(option_string);

    if (len == 2) {
        opts = (char **) calloc(1, sizeof(*opts));
        if (opts == NULL) {
            fprintf(ERRORFILE, "Unable to allocate %ld bytes\n", sizeof(*opts));
            free(options);
            return NULL;
        }
        num_opts = 1;
    } else if (len < 8) {
        fprintf(ERRORFILE, "Invalid docker mount. Too many options. '%s'\n", mount_string);
        free(options);
        return NULL;
    } else {
        opts = (char **) calloc(2, sizeof(*opts));
        if (opts == NULL) {
          fprintf(ERRORFILE, "Unable to allocate %ld bytes\n", 2 * sizeof(*opts));
          free(options);
          return NULL;
        }
        num_opts = 2;
    }

    options->opts = opts;
    options->num_opts = num_opts;

    char *option_string_token = strdup(option_string);
    option = strtok(option_string_token, "+");
    for (unsigned int i = 0; i < options->num_opts; i++) {
        if (option == NULL) {
            fprintf(ERRORFILE, "Invalid docker mount options '%s'\n", mount_string);
            free(option_string_token);
            free_mount_options(options);
            return NULL;
        }

        if (strcmp("rw", option) == 0) {
            options->rw = 1;
        } else if (strcmp("ro", option) == 0) {
            options->rw = 0;
        } else if (strcmp("shared", option) != 0 &&
          (strcmp("rshared", option) != 0) &&
          (strcmp("slave", option) != 0) &&
          (strcmp("rslave", option) != 0) &&
          (strcmp("private", option) != 0) &&
          (strcmp("rprivate", option) != 0)) {
            fprintf(ERRORFILE, "Invalid docker mount options '%s'\n", mount_string);
            free(option_string_token);
            free_mount_options(options);
            return NULL;
        }
        options->opts[i] = strdup(option);
        option = strtok(NULL, "+");
    }

    free(option_string_token);
    return options;
}

static char* get_docker_mount_options_string(mount_options *options) {
    char *options_string = NULL;
    int len = 0;
    int idx = 0;
    unsigned int i;

    for (i = 0; i < options->num_opts; i++) {
        len += strlen(options->opts[i]);
    }
    len += i; // i-1 commas plus 1 for NUL termination

    options_string = (char *) calloc(len, sizeof(*options_string));
    if (options_string == NULL) {
      fputs("Unable to allocate memory\n", ERRORFILE);
      return NULL;
    }

    idx += sprintf(options_string, "%s", options->opts[0]);
    for (i = 1; i < options->num_opts; i++) {
        idx += sprintf(options_string + idx, ",%s", options->opts[i]);
    }

  return options_string;
}

static int add_mounts_to_docker_args(mount *mounts, unsigned int num_mounts, args *args) {
    int ret = 0, len;
    unsigned int i;
    char *mount_string = NULL;
    char *options_string = NULL;

    if (mounts == NULL) {
        return ret;
    }

    for (i = 0; i < num_mounts; i++) {
        ret = add_to_args(args, "-v");
        if (ret != 0) {
            ret = BUFFER_TOO_SMALL;
            return ret;
        }
        options_string = get_docker_mount_options_string(mounts[i].options);

        //magic number '+3': +1 for both ':', and +1 for NULL termination
        len = strlen(mounts[i].src) + strlen(mounts[i].dest) + strlen(options_string) + 3;
        mount_string = (char *) calloc(len, sizeof(*mount_string));

        snprintf(mount_string, len, "%s:%s:%s", mounts[i].src, mounts[i].dest, options_string);

        ret = add_to_args(args, mount_string);
        if (ret != 0) {
            ret = BUFFER_TOO_SMALL;
            goto free_and_exit;
        }
        free(mount_string);
        free(options_string);
    }
    return ret;

free_and_exit:
    free(mount_string);
    free(options_string);
    return ret;
}

static int get_docker_mounts(mount *mounts, char **mounts_string, unsigned int num_mounts) {
    unsigned int i;
    int ret = 0;
    char *src, *dest;
    mount_options *options = NULL;

    if (mounts_string == NULL) {
        fprintf(ERRORFILE, "Unable to normalize container-executor.cfg path\n");
        ret = MOUNT_ACCESS_ERROR;
        goto free_and_exit;
    }

    for (i = 0; i < num_mounts; i++) {
        src = get_docker_mount_source(mounts_string[i]);
        if (src == NULL) {
            fprintf(ERRORFILE, "Invalid mount '%s'\n", mounts_string[i]);
            ret = INVALID_MOUNT;
            goto free_and_exit;
        }
        mounts[i].src = src;

        dest = get_docker_mount_dest(mounts_string[i]);
        if (dest == NULL) {
            fprintf(ERRORFILE, "Invalid mount '%s'\n", mounts_string[i]);
            ret = INVALID_MOUNT;
            goto free_and_exit;
        }
        mounts[i].dest = dest;

        options = get_docker_mount_options(mounts_string[i]);
        if (options == NULL) {
            fprintf(ERRORFILE, "Invalid mount '%s'\n", mounts_string[i]);
            ret = INVALID_MOUNT;
            goto free_and_exit;
        }
        mounts[i].options = options;
    }

free_and_exit:
    return ret;
}

static int add_docker_mounts(const struct configuration *command_config, const struct configuration *conf, args *args) {
    mount *mounts = NULL;
    unsigned int num_mounts = 0;
    int ret;
    char **permitted_ro_mounts = NULL;
    char **permitted_rw_mounts = NULL;
    char **mounts_string = NULL;
    permitted_ro_mounts = get_configuration_values_delimiter("docker.allowed.ro-mounts",
                                                             CONTAINER_EXECUTOR_CFG_DOCKER_SECTION, conf, ",");
    permitted_rw_mounts = get_configuration_values_delimiter("docker.allowed.rw-mounts",
                                                             CONTAINER_EXECUTOR_CFG_DOCKER_SECTION, conf, ",");
    mounts_string = get_configuration_values_delimiter("mounts", DOCKER_COMMAND_FILE_SECTION, command_config, ",");

    if (mounts_string == NULL) {
        ret = 0;
        goto free_and_exit;
    }

    while (mounts_string[num_mounts] != NULL) {
        num_mounts++;
    }

    // Disable mount volumes if image is not trusted.
    if (check_trusted_image(command_config, conf) != 0) {
        fprintf(ERRORFILE, "Disable mount volume for untrusted image\n");
        // YARN will implicitly bind node manager local directory to
        // docker image.  This can create file system security holes,
        // if docker container has binary to escalate privileges.
        // For untrusted image, we drop mounting without reporting
        // INVALID_MOUNT messages to allow running untrusted
        // image in a sandbox.
        ret = 0;
        goto free_and_exit;
    }

    mounts = (mount *) calloc(num_mounts, sizeof(*mounts));
    if (mounts == NULL) {
        fprintf(ERRORFILE, "Unable to allocate %ld bytes\n", num_mounts * sizeof(*mounts));
        ret = OUT_OF_MEMORY;
        goto free_and_exit;
    }

    ret = get_docker_mounts(mounts, mounts_string, num_mounts);
    if (ret != 0) {
        goto free_and_exit;
    }

    ret = validate_mounts(permitted_ro_mounts, permitted_rw_mounts, mounts, num_mounts);
    if (ret != 0) {
        goto free_and_exit;
    }

    ret = add_mounts_to_docker_args(mounts, num_mounts, args);
    if (ret != 0) {
        goto free_and_exit;
    }

free_and_exit:
    free_values(permitted_ro_mounts);
    free_values(permitted_rw_mounts);
    free_values(mounts_string);
    free_mounts(mounts, num_mounts);
    return ret;
}

int get_docker_run_command(const char *command_file, const struct configuration *conf, args *args) {
  int ret = 0, i = 0;
  char *container_name = NULL, *user = NULL, *image = NULL;
  char *tmp_buffer = NULL;
  char **launch_command = NULL;
  char *privileged = NULL;
  char *no_new_privileges_enabled = NULL;
  char *use_entry_point = NULL;
  int service_mode_enabled = 0;
  struct configuration command_config = {0, NULL};
  ret = read_and_verify_command_file(command_file, DOCKER_RUN_COMMAND, &command_config);
  if (ret != 0) {
    goto free_and_exit;
  }

  service_mode_enabled = is_service_mode_enabled(&command_config, conf, args);
  if (service_mode_enabled == DOCKER_SERVICE_MODE_DISABLED) {
    ret = DOCKER_SERVICE_MODE_DISABLED;
    goto free_and_exit;
  }

  use_entry_point = get_configuration_value("use-entry-point", DOCKER_COMMAND_FILE_SECTION, &command_config);
  if (use_entry_point != NULL && strcasecmp(use_entry_point, "true") == 0) {
    entry_point = 1;
  }
  free(use_entry_point);

  container_name = get_configuration_value("name", DOCKER_COMMAND_FILE_SECTION, &command_config);
  if (container_name == NULL || validate_container_name(container_name) != 0) {
    ret = INVALID_DOCKER_CONTAINER_NAME;
    goto free_and_exit;
  }

  if (!service_mode_enabled) {
    user = get_configuration_value("user", DOCKER_COMMAND_FILE_SECTION, &command_config);
    if (user == NULL) {
      ret = INVALID_DOCKER_USER_NAME;
      goto free_and_exit;
    }
  }
  image = get_configuration_value("image", DOCKER_COMMAND_FILE_SECTION, &command_config);
  if (image == NULL || validate_docker_image_name(image) != 0) {
    ret = INVALID_DOCKER_IMAGE_NAME;
    goto free_and_exit;
  }

  ret = add_to_args(args, DOCKER_RUN_COMMAND);
  if(ret != 0) {
    ret = BUFFER_TOO_SMALL;
    goto free_and_exit;
  }

  tmp_buffer = make_string("--name=%s", container_name);
  ret = add_to_args(args, tmp_buffer);
  free(tmp_buffer);
  if (ret != 0) {
    ret = BUFFER_TOO_SMALL;
    goto free_and_exit;
  }

  privileged = get_configuration_value("privileged", DOCKER_COMMAND_FILE_SECTION, &command_config);

  if (privileged == NULL || strcmp(privileged, "false") == 0) {
    if (!service_mode_enabled) {
      char *user_buffer = make_string("--user=%s", user);
      ret = add_to_args(args, user_buffer);
      free(user_buffer);
      if (ret != 0) {
        ret = BUFFER_TOO_SMALL;
        goto free_and_exit;
      }
    }
    no_new_privileges_enabled =
        get_configuration_value("docker.no-new-privileges.enabled",
        CONTAINER_EXECUTOR_CFG_DOCKER_SECTION, conf);
    if (no_new_privileges_enabled != NULL &&
        strcasecmp(no_new_privileges_enabled, "True") == 0) {
      ret = add_to_args(args, "--security-opt=no-new-privileges");
      if (ret != 0) {
        ret = BUFFER_TOO_SMALL;
        goto free_and_exit;
      }
    }
  }

  ret = detach_container(&command_config, args);
  if (ret != 0) {
    goto free_and_exit;
  }

  ret = rm_container_on_exit(&command_config, args);
  if (ret != 0) {
    goto free_and_exit;
  }

  ret = set_container_workdir(&command_config, args);
  if (ret != 0) {
    goto free_and_exit;
  }

  ret = set_network(&command_config, conf, args);
  if (ret != 0) {
    goto free_and_exit;
  }

  ret = add_ports_mapping_to_command(&command_config, args);
  if (ret != 0) {
    goto free_and_exit;
  }

  ret = set_pid_namespace(&command_config, conf, args);
  if (ret != 0) {
    goto free_and_exit;
  }

  ret = add_docker_mounts(&command_config, conf, args);
  if (ret != 0) {
    goto free_and_exit;
  }

  ret = add_tmpfs_mounts(&command_config, args);
  if (ret != 0) {
    goto free_and_exit;
  }

  ret = set_cgroup_parent(&command_config, args);
  if (ret != 0) {
    goto free_and_exit;
  }

  ret = set_privileged(&command_config, conf, args);
  if (ret != 0) {
    goto free_and_exit;
  }

  ret = set_capabilities(&command_config, conf, args);
  if (ret != 0) {
    goto free_and_exit;
  }

  ret = set_runtime(&command_config, conf, args);
  if (ret != 0) {
    goto free_and_exit;
  }

  ret = set_hostname(&command_config, args);
  if (ret != 0) {
    goto free_and_exit;
  }

  if (!service_mode_enabled) {
    ret = set_group_add(&command_config, args);
    if (ret != 0) {
      goto free_and_exit;
    }
  }

  ret = set_devices(&command_config, conf, args);
  if (ret != 0) {
    goto free_and_exit;
  }

  ret = set_env(&command_config, args);
  if (ret != 0) {
    goto free_and_exit;
  }

  ret = add_to_args(args, image);
  if (ret != 0) {
    goto free_and_exit;
  }

  launch_command = get_configuration_values_delimiter("launch-command", DOCKER_COMMAND_FILE_SECTION, &command_config,
                                                      ",");
  if (launch_command != NULL) {
    for (i = 0; launch_command[i] != NULL; ++i) {
      ret = add_to_args(args, launch_command[i]);
      if (ret != 0) {
        ret = BUFFER_TOO_SMALL;
        goto free_and_exit;
      }
    }
  }
free_and_exit:
  if (ret != 0) {
    reset_args(args);
  }
  free(user);
  free(image);
  free(privileged);
  free(no_new_privileges_enabled);
  free(container_name);
  free_values(launch_command);
  free_configuration(&command_config);
  return ret;
}

int get_docker_images_command(const char *command_file, const struct configuration *conf, args *args) {
  int ret = 0;
  char *image_name = NULL;

  struct configuration command_config = {0, NULL};
  ret = read_and_verify_command_file(command_file, DOCKER_IMAGES_COMMAND, &command_config);
  if (ret != 0) {
    goto free_and_exit;
  }

  ret = add_to_args(args, DOCKER_IMAGES_COMMAND);
  if (ret != 0) {
    goto free_and_exit;
  }

  image_name = get_configuration_value("image", DOCKER_COMMAND_FILE_SECTION, &command_config);
  if (image_name) {
    if (validate_docker_image_name(image_name) != 0) {
      ret = INVALID_DOCKER_IMAGE_NAME;
       goto free_and_exit;
    }
    ret = add_to_args(args, image_name);
    if (ret != 0) {
      goto free_and_exit;
    }
  }

  ret = add_to_args(args, "--format={{json .}}");
  ret = add_to_args(args, "--filter=dangling=false");

  free_and_exit:
    free(image_name);
    free_configuration(&command_config);
  return ret;
}
