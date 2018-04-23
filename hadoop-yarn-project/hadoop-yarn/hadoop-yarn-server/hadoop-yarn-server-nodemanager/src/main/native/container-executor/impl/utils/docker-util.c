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
#include <grp.h>
#include <pwd.h>
#include <errno.h>

static int read_and_verify_command_file(const char *command_file, const char *docker_command,
                                        struct configuration *command_config) {
  int ret = 0;
  ret = read_config(command_file, command_config);
  if (ret != 0) {
    return INVALID_COMMAND_FILE;
  }
  char *command = get_configuration_value("docker-command", DOCKER_COMMAND_FILE_SECTION, command_config);
  if (command == NULL || (strcmp(command, docker_command) != 0)) {
    ret = INCORRECT_COMMAND;
  }
  free(command);
  return ret;
}

static int add_to_buffer(char *buff, const size_t bufflen, const char *string) {
  size_t current_len = strlen(buff);
  size_t string_len = strlen(string);
  if (current_len + string_len < bufflen - 1) {
    strncpy(buff + current_len, string, string_len);
    buff[current_len + string_len] = '\0';
    return 0;
  }
  return -1;
}

static int add_param_to_command(const struct configuration *command_config, const char *key, const char *param,
                                const int with_argument, char *out, const size_t outlen) {
  size_t tmp_buffer_size = 4096;
  int ret = 0;
  char *tmp_buffer = (char *) alloc_and_clear_memory(tmp_buffer_size, sizeof(char));
  char *value = get_configuration_value(key, DOCKER_COMMAND_FILE_SECTION, command_config);
  if (value != NULL) {
    if (with_argument) {
      quote_and_append_arg(&tmp_buffer, &tmp_buffer_size, param, value);
      ret = add_to_buffer(out, outlen, tmp_buffer);
    } else if (strcmp(value, "true") == 0) {
      ret = add_to_buffer(out, outlen, param);
    }
    free(value);
    if (ret != 0) {
      ret = BUFFER_TOO_SMALL;
    }
  }
  free(tmp_buffer);
  return ret;
}

int check_trusted_image(const struct configuration *command_config, const struct configuration *conf) {
  int found = 0;
  int i = 0;
  int ret = 0;
  char *image_name = get_configuration_value("image", DOCKER_COMMAND_FILE_SECTION, command_config);
  char **privileged_registry = get_configuration_values_delimiter("docker.privileged-containers.registries", CONTAINER_EXECUTOR_CFG_DOCKER_SECTION, conf, ",");
  char *registry_ptr = NULL;
  if (image_name == NULL) {
    ret = INVALID_DOCKER_IMAGE_NAME;
    goto free_and_exit;
  }
  if (privileged_registry != NULL) {
    for (i = 0; privileged_registry[i] != NULL; i++) {
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
  free(image_name);

  free_and_exit:
  free(privileged_registry);
  return ret;
}

static int is_regex(const char *str) {
  // regex should begin with prefix "regex:"
  return (strncmp(str, "regex:", 6) == 0);
}

static int is_volume_name(const char *volume_name) {
  const char *regex_str = "^[a-zA-Z0-9]([a-zA-Z0-9_.-]*)$";
  // execute_regex_match return 0 is matched success
  return execute_regex_match(regex_str, volume_name) == 0;
}

static int is_volume_name_matched_by_regex(const char* requested, const char* pattern) {
  // execute_regex_match return 0 is matched success
  return is_volume_name(requested) && (execute_regex_match(pattern + sizeof("regex:"), requested) == 0);
}

static int add_param_to_command_if_allowed(const struct configuration *command_config,
                                           const struct configuration *executor_cfg,
                                           const char *key, const char *allowed_key, const char *param,
                                           const int multiple_values, const char prefix,
                                           char *out, const size_t outlen) {
  size_t tmp_buffer_size = 4096;
  char *tmp_buffer = (char *) alloc_and_clear_memory(tmp_buffer_size, sizeof(char));
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
        return INVALID_DOCKER_IMAGE_TRUST;
      }
    }

    if (permitted_values != NULL) {
      // Values are user requested.
      for (i = 0; values[i] != NULL; ++i) {
        memset(tmp_buffer, 0, tmp_buffer_size);
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
              size_t offset = tmp_ptr - values[i];
              dst = (char *) alloc_and_clear_memory(offset, sizeof(char));
              strncpy(dst, values[i], offset);
              dst[tmp_ptr - values[i]] = '\0';
              pattern = (char *) alloc_and_clear_memory((size_t)(strlen(permitted_values[j]) - 6), sizeof(char));
              strcpy(pattern, permitted_values[j] + 6);
              ret = execute_regex_match(pattern, dst);
            } else {
              ret = strncmp(values[i], permitted_values[j], tmp_ptr - values[i]);
            }
          }
          if (ret == 0) {
            free(dst);
            free(pattern);
            permitted = 1;
            break;
          }
        }
        if (permitted == 1) {
          quote_and_append_arg(&tmp_buffer, &tmp_buffer_size, param, values[i]);
          ret = add_to_buffer(out, outlen, tmp_buffer);
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
  free(tmp_buffer);
  if (ret != 0) {
    memset(out, 0, outlen);
  }
  return ret;
}

static int add_docker_config_param(const struct configuration *command_config, char *out, const size_t outlen) {
  return add_param_to_command(command_config, "docker-config", "--config=", 1, out, outlen);
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
  fflush(ERRORFILE);
  return INVALID_DOCKER_CONTAINER_NAME;
}

const char *get_docker_error_message(const int error_code) {

  switch (error_code) {
    case INVALID_COMMAND_FILE:
      return "Invalid command file passed";
    case INCORRECT_COMMAND:
      return "Incorrect command";
    case BUFFER_TOO_SMALL:
      return "Command buffer too small";
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
    case INVALID_DOCKER_CAPABILITY:
      return "Invalid docker capability";
    case PRIVILEGED_CONTAINERS_DISABLED:
      return "Privileged containers are disabled";
    case INVALID_DOCKER_MOUNT:
      return "Invalid docker mount";
    case INVALID_DOCKER_RO_MOUNT:
      return "Invalid docker read-only mount";
    case INVALID_DOCKER_RW_MOUNT:
      return "Invalid docker read-write mount";
    case MOUNT_ACCESS_ERROR:
      return "Mount access error";
    case INVALID_DOCKER_DEVICE:
      return "Invalid docker device";
    case INVALID_DOCKER_VOLUME_DRIVER:
      return "Invalid docker volume-driver";
    case INVALID_DOCKER_VOLUME_NAME:
      return "Invalid docker volume name";
    case INVALID_DOCKER_VOLUME_COMMAND:
      return "Invalid docker volume command";
    case PID_HOST_DISABLED:
      return "Host pid namespace is disabled";
    case INVALID_PID_NAMESPACE:
      return "Invalid pid namespace";
    case INVALID_DOCKER_IMAGE_TRUST:
      return "Docker image is not trusted";
    default:
      return "Unknown error";
  }
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

int docker_module_enabled(const struct configuration *conf) {
  struct section *section = get_configuration_section(CONTAINER_EXECUTOR_CFG_DOCKER_SECTION, conf);
  if (section != NULL) {
    return module_enabled(section, CONTAINER_EXECUTOR_CFG_DOCKER_SECTION);
  }
  return 0;
}

int get_docker_command(const char *command_file, const struct configuration *conf, char *out, const size_t outlen) {
  int ret = 0;
  struct configuration command_config = {0, NULL};

  ret = read_config(command_file, &command_config);
  if (ret != 0) {
    return INVALID_COMMAND_FILE;
  }

  char *command = get_configuration_value("docker-command", DOCKER_COMMAND_FILE_SECTION, &command_config);
  if (strcmp(DOCKER_INSPECT_COMMAND, command) == 0) {
    return get_docker_inspect_command(command_file, conf, out, outlen);
  } else if (strcmp(DOCKER_KILL_COMMAND, command) == 0) {
    return get_docker_kill_command(command_file, conf, out, outlen);
  } else if (strcmp(DOCKER_LOAD_COMMAND, command) == 0) {
    return get_docker_load_command(command_file, conf, out, outlen);
  } else if (strcmp(DOCKER_PULL_COMMAND, command) == 0) {
    return get_docker_pull_command(command_file, conf, out, outlen);
  } else if (strcmp(DOCKER_RM_COMMAND, command) == 0) {
    return get_docker_rm_command(command_file, conf, out, outlen);
  } else if (strcmp(DOCKER_RUN_COMMAND, command) == 0) {
    return get_docker_run_command(command_file, conf, out, outlen);
  } else if (strcmp(DOCKER_STOP_COMMAND, command) == 0) {
    return get_docker_stop_command(command_file, conf, out, outlen);
  } else if (strcmp(DOCKER_VOLUME_COMMAND, command) == 0) {
    return get_docker_volume_command(command_file, conf, out, outlen);
  } else if (strcmp(DOCKER_START_COMMAND, command) == 0) {
    return get_docker_start_command(command_file, conf, out, outlen);
  } else {
    return UNKNOWN_DOCKER_COMMAND;
  }
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

int get_docker_volume_command(const char *command_file, const struct configuration *conf, char *out,
                               const size_t outlen) {
  int ret = 0;
  char *driver = NULL, *volume_name = NULL, *sub_command = NULL, *format = NULL;
  struct configuration command_config = {0, NULL};
  ret = read_and_verify_command_file(command_file, DOCKER_VOLUME_COMMAND, &command_config);
  if (ret != 0) {
    return ret;
  }
  sub_command = get_configuration_value("sub-command", DOCKER_COMMAND_FILE_SECTION, &command_config);

  if ((sub_command == NULL) || ((0 != strcmp(sub_command, "create")) &&
      (0 != strcmp(sub_command, "ls")))) {
    fprintf(ERRORFILE, "\"create/ls\" are the only acceptable sub-command of volume, input sub_command=\"%s\"\n",
       sub_command);
    ret = INVALID_DOCKER_VOLUME_COMMAND;
    goto cleanup;
  }

  memset(out, 0, outlen);

  ret = add_docker_config_param(&command_config, out, outlen);
  if (ret != 0) {
    ret = BUFFER_TOO_SMALL;
    goto cleanup;
  }

  ret = add_to_buffer(out, outlen, DOCKER_VOLUME_COMMAND);
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

    ret = add_to_buffer(out, outlen, " create");
    if (ret != 0) {
      goto cleanup;
    }

    ret = add_to_buffer(out, outlen, " --name=");
    if (ret != 0) {
      goto cleanup;
    }

    ret = add_to_buffer(out, outlen, volume_name);
    if (ret != 0) {
      goto cleanup;
    }

    if (!value_permitted(conf, "docker.allowed.volume-drivers", driver)) {
      fprintf(ERRORFILE, "%s is not permitted docker.allowed.volume-drivers\n",
        driver);
      ret = INVALID_DOCKER_VOLUME_DRIVER;
      goto cleanup;
    }

    ret = add_to_buffer(out, outlen, " --driver=");
    if (ret != 0) {
      goto cleanup;
    }

    ret = add_to_buffer(out, outlen, driver);
    if (ret != 0) {
      goto cleanup;
    }
  } else if (0 == strcmp(sub_command, "ls")) {
    format = get_configuration_value("format", DOCKER_COMMAND_FILE_SECTION, &command_config);

    ret = add_to_buffer(out, outlen, " ls");
    if (ret != 0) {
      goto cleanup;
    }

    if (format) {
      ret = add_to_buffer(out, outlen, " --format=");
      if (ret != 0) {
        goto cleanup;
      }
      ret = add_to_buffer(out, outlen, format);
      if (ret != 0) {
        goto cleanup;
      }
    }
  }

cleanup:
  free(driver);
  free(volume_name);
  free(sub_command);
  free(format);

  // clean up out buffer
  if (ret != 0) {
    out[0] = 0;
  }
  return ret;
}

int get_docker_inspect_command(const char *command_file, const struct configuration *conf, char *out,
                               const size_t outlen) {
  const char *valid_format_strings[] = { "{{.State.Status}}",
                                "{{range(.NetworkSettings.Networks)}}{{.IPAddress}},{{end}}{{.Config.Hostname}}" };
  int ret = 0, i = 0, valid_format = 0;
  char *format = NULL, *container_name = NULL;
  struct configuration command_config = {0, NULL};
  ret = read_and_verify_command_file(command_file, DOCKER_INSPECT_COMMAND, &command_config);
  if (ret != 0) {
    return ret;
  }

  container_name = get_configuration_value("name", DOCKER_COMMAND_FILE_SECTION, &command_config);
  if (container_name == NULL || validate_container_name(container_name) != 0) {
    return INVALID_DOCKER_CONTAINER_NAME;
  }

  format = get_configuration_value("format", DOCKER_COMMAND_FILE_SECTION, &command_config);
  if (format == NULL) {
    free(container_name);
    return INVALID_DOCKER_INSPECT_FORMAT;
  }
  for (i = 0; i < 2; ++i) {
    if (strcmp(format, valid_format_strings[i]) == 0) {
      valid_format = 1;
      break;
    }
  }
  if (valid_format != 1) {
    fprintf(ERRORFILE, "Invalid format option '%s' not permitted\n", format);
    free(container_name);
    free(format);
    return INVALID_DOCKER_INSPECT_FORMAT;
  }

  memset(out, 0, outlen);

  ret = add_docker_config_param(&command_config, out, outlen);
  if (ret != 0) {
    free(container_name);
    free(format);
    return BUFFER_TOO_SMALL;
  }

  ret = add_to_buffer(out, outlen, DOCKER_INSPECT_COMMAND);
  if (ret != 0) {
    goto free_and_exit;
  }
  ret = add_to_buffer(out, outlen, " --format=");
  if (ret != 0) {
    goto free_and_exit;
  }
  ret = add_to_buffer(out, outlen, format);
  if (ret != 0) {
    goto free_and_exit;
  }
  ret = add_to_buffer(out, outlen, " ");
  if (ret != 0) {
    goto free_and_exit;
  }
  ret = add_to_buffer(out, outlen, container_name);
  if (ret != 0) {
    goto free_and_exit;
  }
  free(format);
  free(container_name);
  return 0;

  free_and_exit:
  free(format);
  free(container_name);
  return BUFFER_TOO_SMALL;
}

int get_docker_load_command(const char *command_file, const struct configuration *conf, char *out, const size_t outlen) {
  int ret = 0;
  char *image_name = NULL;
  size_t tmp_buffer_size = 1024;
  char *tmp_buffer = NULL;
  struct configuration command_config = {0, NULL};
  ret = read_and_verify_command_file(command_file, DOCKER_LOAD_COMMAND, &command_config);
  if (ret != 0) {
    return ret;
  }

  image_name = get_configuration_value("image", DOCKER_COMMAND_FILE_SECTION, &command_config);
  if (image_name == NULL) {
    return INVALID_DOCKER_IMAGE_NAME;
  }

  memset(out, 0, outlen);

  ret = add_docker_config_param(&command_config, out, outlen);
  if (ret != 0) {
    free(image_name);
    return BUFFER_TOO_SMALL;
  }

  ret = add_to_buffer(out, outlen, DOCKER_LOAD_COMMAND);
  if (ret == 0) {
    tmp_buffer = (char *) alloc_and_clear_memory(tmp_buffer_size, sizeof(char));
    quote_and_append_arg(&tmp_buffer, &tmp_buffer_size, " --i=", image_name);
    ret = add_to_buffer(out, outlen, tmp_buffer);
    free(tmp_buffer);
    free(image_name);
    if (ret != 0) {
      return BUFFER_TOO_SMALL;
    }
    return 0;
  }
  free(image_name);
  return BUFFER_TOO_SMALL;
}

static int validate_docker_image_name(const char *image_name) {
  const char *regex_str = "^(([a-zA-Z0-9.-]+)(:[0-9]+)?/)?([a-z0-9_./-]+)(:[a-zA-Z0-9_.-]+)?$";
  return execute_regex_match(regex_str, image_name);
}

int get_docker_pull_command(const char *command_file, const struct configuration *conf, char *out, const size_t outlen) {
  int ret = 0;
  char *image_name = NULL;
  size_t tmp_buffer_size = 1024;
  char *tmp_buffer = NULL;
  struct configuration command_config = {0, NULL};
  ret = read_and_verify_command_file(command_file, DOCKER_PULL_COMMAND, &command_config);
  if (ret != 0) {
    return ret;
  }

  image_name = get_configuration_value("image", DOCKER_COMMAND_FILE_SECTION, &command_config);
  if (image_name == NULL || validate_docker_image_name(image_name) != 0) {
    return INVALID_DOCKER_IMAGE_NAME;
  }

  memset(out, 0, outlen);

  ret = add_docker_config_param(&command_config, out, outlen);
  if (ret != 0) {
    return BUFFER_TOO_SMALL;
  }

  ret = add_to_buffer(out, outlen, DOCKER_PULL_COMMAND);
  if (ret == 0) {
    tmp_buffer = (char *) alloc_and_clear_memory(tmp_buffer_size, sizeof(char));
    quote_and_append_arg(&tmp_buffer, &tmp_buffer_size, " ", image_name);
    ret = add_to_buffer(out, outlen, tmp_buffer);
    free(tmp_buffer);
    free(image_name);
    if (ret != 0) {
      return BUFFER_TOO_SMALL;
    }
    return 0;
  }
  free(image_name);
  return BUFFER_TOO_SMALL;
}

int get_docker_rm_command(const char *command_file, const struct configuration *conf, char *out, const size_t outlen) {
  int ret = 0;
  char *container_name = NULL;
  struct configuration command_config = {0, NULL};
  ret = read_and_verify_command_file(command_file, DOCKER_RM_COMMAND, &command_config);
  if (ret != 0) {
    return ret;
  }

  container_name = get_configuration_value("name", DOCKER_COMMAND_FILE_SECTION, &command_config);
  if (container_name == NULL || validate_container_name(container_name) != 0) {
    return INVALID_DOCKER_CONTAINER_NAME;
  }

  memset(out, 0, outlen);

  ret = add_docker_config_param(&command_config, out, outlen);
  if (ret != 0) {
    return BUFFER_TOO_SMALL;
  }

  ret = add_to_buffer(out, outlen, DOCKER_RM_COMMAND);
  if (ret == 0) {
    ret = add_to_buffer(out, outlen, " ");
    if (ret == 0) {
      ret = add_to_buffer(out, outlen, container_name);
    }
    free(container_name);
    if (ret != 0) {
      return BUFFER_TOO_SMALL;
    }
    return 0;
  }
  free(container_name);
  return BUFFER_TOO_SMALL;
}

int get_docker_stop_command(const char *command_file, const struct configuration *conf,
                            char *out, const size_t outlen) {
  int ret = 0;
  size_t len = 0, i = 0;
  char *value = NULL;
  char *container_name = NULL;
  struct configuration command_config = {0, NULL};
  ret = read_and_verify_command_file(command_file, DOCKER_STOP_COMMAND, &command_config);
  if (ret != 0) {
    return ret;
  }

  container_name = get_configuration_value("name", DOCKER_COMMAND_FILE_SECTION, &command_config);
  if (container_name == NULL || validate_container_name(container_name) != 0) {
    return INVALID_DOCKER_CONTAINER_NAME;
  }

  memset(out, 0, outlen);

  ret = add_docker_config_param(&command_config, out, outlen);
  if (ret != 0) {
    return BUFFER_TOO_SMALL;
  }

  ret = add_to_buffer(out, outlen, DOCKER_STOP_COMMAND);
  if (ret == 0) {
    value = get_configuration_value("time", DOCKER_COMMAND_FILE_SECTION, &command_config);
    if (value != NULL) {
      len = strlen(value);
      for (i = 0; i < len; ++i) {
        if (isdigit(value[i]) == 0) {
          fprintf(ERRORFILE, "Value for time is not a number '%s'\n", value);
          free(container_name);
          memset(out, 0, outlen);
          return INVALID_DOCKER_STOP_COMMAND;
        }
      }
      ret = add_to_buffer(out, outlen, " --time=");
      if (ret == 0) {
        ret = add_to_buffer(out, outlen, value);
      }
      if (ret != 0) {
        free(container_name);
        return BUFFER_TOO_SMALL;
      }
    }
    ret = add_to_buffer(out, outlen, " ");
    if (ret == 0) {
      ret = add_to_buffer(out, outlen, container_name);
    }
    free(container_name);
    if (ret != 0) {
      return BUFFER_TOO_SMALL;
    }
    return 0;
  }
  free(container_name);
  return BUFFER_TOO_SMALL;
}

int get_docker_kill_command(const char *command_file, const struct configuration *conf,
                            char *out, const size_t outlen) {
  int ret = 0;
  size_t len = 0, i = 0;
  char *value = NULL;
  char *container_name = NULL;
  struct configuration command_config = {0, NULL};
  ret = read_and_verify_command_file(command_file, DOCKER_KILL_COMMAND, &command_config);
  if (ret != 0) {
    return ret;
  }

  container_name = get_configuration_value("name", DOCKER_COMMAND_FILE_SECTION, &command_config);
  if (container_name == NULL || validate_container_name(container_name) != 0) {
    return INVALID_DOCKER_CONTAINER_NAME;
  }

  memset(out, 0, outlen);

  ret = add_docker_config_param(&command_config, out, outlen);
  if (ret != 0) {
    return BUFFER_TOO_SMALL;
  }

  ret = add_to_buffer(out, outlen, DOCKER_KILL_COMMAND);
  if (ret == 0) {
    value = get_configuration_value("signal", DOCKER_COMMAND_FILE_SECTION, &command_config);
    if (value != NULL) {
      len = strlen(value);
      for (i = 0; i < len; ++i) {
        if (isupper(value[i]) == 0) {
          fprintf(ERRORFILE, "Value for signal contains non-uppercase characters '%s'\n", value);
          free(container_name);
          memset(out, 0, outlen);
          return INVALID_DOCKER_KILL_COMMAND;
        }
      }
      ret = add_to_buffer(out, outlen, " --signal=");
      if (ret == 0) {
        ret = add_to_buffer(out, outlen, value);
      }
      if (ret != 0) {
        free(container_name);
        return BUFFER_TOO_SMALL;
      }
    }
    ret = add_to_buffer(out, outlen, " ");
    if (ret == 0) {
      ret = add_to_buffer(out, outlen, container_name);
    }
    free(container_name);
    if (ret != 0) {
      return BUFFER_TOO_SMALL;
    }
    return 0;
  }
  free(container_name);
  return BUFFER_TOO_SMALL;
}

int get_docker_start_command(const char *command_file, const struct configuration *conf, char *out, const size_t outlen) {
  int ret = 0;
  char *container_name = NULL;
  struct configuration command_config = {0, NULL};
  ret = read_and_verify_command_file(command_file, DOCKER_START_COMMAND, &command_config);
  if (ret != 0) {
    return ret;
  }

  container_name = get_configuration_value("name", DOCKER_COMMAND_FILE_SECTION, &command_config);
  if (container_name == NULL || validate_container_name(container_name) != 0) {
    return INVALID_DOCKER_CONTAINER_NAME;
  }

  memset(out, 0, outlen);

  ret = add_docker_config_param(&command_config, out, outlen);
  if (ret != 0) {
    return BUFFER_TOO_SMALL;
  }

  ret = add_to_buffer(out, outlen, DOCKER_START_COMMAND);
  if (ret != 0) {
    goto free_and_exit;
  }
  ret = add_to_buffer(out, outlen, " ");
  if (ret != 0) {
    goto free_and_exit;
  }
  ret = add_to_buffer(out, outlen, container_name);
  if (ret != 0) {
    goto free_and_exit;
  }
free_and_exit:
  free(container_name);
  return ret;
}

static int detach_container(const struct configuration *command_config, char *out, const size_t outlen) {
  return add_param_to_command(command_config, "detach", "-d ", 0, out, outlen);
}

static int  rm_container_on_exit(const struct configuration *command_config, char *out, const size_t outlen) {
  return add_param_to_command(command_config, "rm", "--rm ", 0, out, outlen);
}

static int set_container_workdir(const struct configuration *command_config, char *out, const size_t outlen) {
  return add_param_to_command(command_config, "workdir", "--workdir=", 1, out, outlen);
}

static int set_cgroup_parent(const struct configuration *command_config, char *out, const size_t outlen) {
  return add_param_to_command(command_config, "cgroup-parent", "--cgroup-parent=", 1, out, outlen);
}

static int set_hostname(const struct configuration *command_config, char *out, const size_t outlen) {
  return add_param_to_command(command_config, "hostname", "--hostname=", 1, out, outlen);
}

static int set_group_add(const struct configuration *command_config, char *out, const size_t outlen) {
  int i = 0, ret = 0;
  char **group_add = get_configuration_values_delimiter("group-add", DOCKER_COMMAND_FILE_SECTION, command_config, ",");
  size_t tmp_buffer_size = 4096;
  char *tmp_buffer = NULL;
  char *privileged = NULL;

  privileged = get_configuration_value("privileged", DOCKER_COMMAND_FILE_SECTION, command_config);
  if (privileged != NULL && strcasecmp(privileged, "true") == 0 ) {
    free(privileged);
    return ret;
  }
  free(privileged);

  if (group_add != NULL) {
    for (i = 0; group_add[i] != NULL; ++i) {
      tmp_buffer = (char *) alloc_and_clear_memory(tmp_buffer_size, sizeof(char));
      quote_and_append_arg(&tmp_buffer, &tmp_buffer_size, "--group-add ", group_add[i]);
      ret = add_to_buffer(out, outlen, tmp_buffer);
      if (ret != 0) {
        return BUFFER_TOO_SMALL;
      }
    }
  }
  return ret;
}

static int set_network(const struct configuration *command_config,
                       const struct configuration *conf, char *out,
                       const size_t outlen) {

  int ret = 0;
  ret = add_param_to_command_if_allowed(command_config, conf, "net",
                                        "docker.allowed.networks", "--net=",
                                        0, 0, out, outlen);
  if (ret != 0) {
    fprintf(ERRORFILE, "Could not find requested network in allowed networks\n");
    ret = INVALID_DOCKER_NETWORK;
    memset(out, 0, outlen);
  }

  return ret;
}

static int set_pid_namespace(const struct configuration *command_config,
                   const struct configuration *conf, char *out,
                   const size_t outlen) {
  size_t tmp_buffer_size = 1024;
  char *tmp_buffer = (char *) alloc_and_clear_memory(tmp_buffer_size, sizeof(char));
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
          ret = add_to_buffer(out, outlen, "--pid='host' ");
          if (ret != 0) {
            ret = BUFFER_TOO_SMALL;
          }
        } else {
          fprintf(ERRORFILE, "Host pid namespace is disabled\n");
          ret = PID_HOST_DISABLED;
          goto free_and_exit;
        }
      } else {
        fprintf(ERRORFILE, "Host pid namespace is disabled\n");
        ret = PID_HOST_DISABLED;
        goto free_and_exit;
      }
    } else {
      fprintf(ERRORFILE, "Invalid pid namespace\n");
      ret = INVALID_PID_NAMESPACE;
    }
  }

  free_and_exit:
  free(tmp_buffer);
  free(value);
  free(pid_host_enabled);
  if (ret != 0) {
    memset(out, 0, outlen);
  }
  return ret;
}

static int set_capabilities(const struct configuration *command_config,
                            const struct configuration *conf, char *out,
                            const size_t outlen) {

  int ret = 0;

  ret = add_to_buffer(out, outlen, "--cap-drop='ALL' ");
  if (ret != 0) {
    return BUFFER_TOO_SMALL;
  }

  ret = add_param_to_command_if_allowed(command_config, conf, "cap-add",
                                        "docker.allowed.capabilities",
                                        "--cap-add=", 1, 0,
                                        out, outlen);
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
      memset(out, 0, outlen);
  }

  return ret;
}

static int set_devices(const struct configuration *command_config, const struct configuration *conf, char *out,
                       const size_t outlen) {
  int ret = 0;
  ret = add_param_to_command_if_allowed(command_config, conf, "devices", "docker.allowed.devices", "--device=", 1, ':',
                                        out, outlen);
  if (ret != 0) {
    fprintf(ERRORFILE, "Invalid docker device requested\n");
    ret = INVALID_DOCKER_DEVICE;
    memset(out, 0, outlen);
  }

  return ret;
}

/**
 * Helper function to help normalize mounts for checking if mounts are
 * permitted. The function does the following -
 * 1. Find the canonical path for mount using realpath
 * 2. If the path is a directory, add a '/' at the end (if not present)
 * 3. Return a copy of the canonicalised path(to be freed by the caller)
 * @param mount path to be canonicalised
 * @param isRegexAllowed whether regex matching is allowed for normalize mount
 * @return pointer to canonicalised path, NULL on error
 */
static char* normalize_mount(const char* mount, int isRegexAllowed) {
  int ret = 0;
  struct stat buff;
  char *ret_ptr = NULL, *real_mount = NULL;
  if (mount == NULL) {
    return NULL;
  }
  real_mount = realpath(mount, NULL);
  if (real_mount == NULL) {
    // If mount is a valid named volume, just return it and let docker decide
    if (is_volume_name(mount)) {
      return strdup(mount);
    }
    // we only allow permitted mount to be REGEX, for permitted mount, we check
    // if it's a valid REGEX return; for user mount, we need to strictly check
    if (isRegexAllowed) {
      if (is_regex(mount)) {
        return strdup(mount);
      }
    }
    fprintf(ERRORFILE, "Could not determine real path of mount '%s'\n", mount);
    free(real_mount);
    return NULL;
  }
  ret = stat(real_mount, &buff);
  if (ret == 0) {
    if (S_ISDIR(buff.st_mode)) {
      size_t len = strlen(real_mount);
      if (len <= 0) {
        return NULL;
      }
      if (real_mount[len - 1] != '/') {
        ret_ptr = (char *) alloc_and_clear_memory(len + 2, sizeof(char));
        strncpy(ret_ptr, real_mount, len);
        ret_ptr[len] = '/';
        ret_ptr[len + 1] = '\0';
      } else {
        ret_ptr = strdup(real_mount);
      }
    } else {
      ret_ptr = strdup(real_mount);
    }
  } else {
    fprintf(ERRORFILE, "Could not stat path '%s'\n", real_mount);
    ret_ptr = NULL;
  }
  free(real_mount);
  return ret_ptr;
}

static int normalize_mounts(char **mounts, int isRegexAllowed) {
  int i = 0;
  char *tmp = NULL;
  if (mounts == NULL) {
    return 0;
  }
  for (i = 0; mounts[i] != NULL; ++i) {
    tmp = normalize_mount(mounts[i], isRegexAllowed);
    if (tmp == NULL) {
      return -1;
    }
    free(mounts[i]);
    mounts[i] = tmp;
  }
  return 0;
}

static int check_mount_permitted(const char **permitted_mounts, const char *requested) {
  int i = 0, ret = 0;
  size_t permitted_mount_len = 0;
  char *normalized_path = normalize_mount(requested, 0);
  if (permitted_mounts == NULL) {
    return 0;
  }
  if (normalized_path == NULL) {
    return -1;
  }
  for (i = 0; permitted_mounts[i] != NULL; ++i) {
    if (strcmp(normalized_path, permitted_mounts[i]) == 0) {
      ret = 1;
      break;
    }
    // if (permitted_mounts[i] is a REGEX): use REGEX to compare; return
    if (is_regex(permitted_mounts[i]) &&
    is_volume_name_matched_by_regex(normalized_path, permitted_mounts[i])) {
      ret = 1;
      break;
    }

    // directory check
    permitted_mount_len = strlen(permitted_mounts[i]);
    struct stat path_stat;
    stat(permitted_mounts[i], &path_stat);
    if(S_ISDIR(path_stat.st_mode)) {
      if (strncmp(normalized_path, permitted_mounts[i], permitted_mount_len) == 0) {
        ret = 1;
        break;
      }
    }

  }
  free(normalized_path);
  return ret;
}

static char* get_mount_source(const char *mount) {
  char *src_mount = NULL;
  const char *tmp = NULL;
  tmp = strchr(mount, ':');
  if (tmp == NULL) {
    fprintf(ERRORFILE, "Invalid docker mount '%s'\n", mount);
    return NULL;
  }
  src_mount = strndup(mount, tmp - mount);
  return src_mount;
}

static int add_mounts(const struct configuration *command_config, const struct configuration *conf, const char *key,
                      const int ro, char *out, const size_t outlen) {
  size_t tmp_buffer_size = 1024;
  const char *ro_suffix = "";
  const char *tmp_path_buffer[2] = {NULL, NULL};
  char *tmp_buffer = (char *) alloc_and_clear_memory(tmp_buffer_size, sizeof(char));
  char **permitted_ro_mounts = get_configuration_values_delimiter("docker.allowed.ro-mounts",
                                                                  CONTAINER_EXECUTOR_CFG_DOCKER_SECTION, conf, ",");
  char **permitted_rw_mounts = get_configuration_values_delimiter("docker.allowed.rw-mounts",
                                                                  CONTAINER_EXECUTOR_CFG_DOCKER_SECTION, conf, ",");
  char **values = get_configuration_values_delimiter(key, DOCKER_COMMAND_FILE_SECTION, command_config, ",");
  char *tmp_buffer_2 = NULL, *mount_src = NULL;
  const char *container_executor_cfg_path = normalize_mount(get_config_path(""), 0);
  int i = 0, permitted_rw = 0, permitted_ro = 0, ret = 0;
  if (ro != 0) {
    ro_suffix = ":ro";
  }

  if (values != NULL) {
    // Disable mount volumes if image is not trusted.
    if (check_trusted_image(command_config, conf) != 0) {
      fprintf(ERRORFILE, "Disable mount volume for untrusted image\n");
      // YARN will implicitly bind node manager local directory to
      // docker image.  This can create file system security holes,
      // if docker container has binary to escalate privileges.
      // For untrusted image, we drop mounting without reporting
      // INVALID_DOCKER_MOUNT messages to allow running untrusted
      // image in a sandbox.
      ret = 0;
      goto free_and_exit;
    }
    ret = normalize_mounts(permitted_ro_mounts, 1);
    ret |= normalize_mounts(permitted_rw_mounts, 1);
    if (ret != 0) {
      fprintf(ERRORFILE, "Unable to find permitted docker mounts on disk\n");
      ret = MOUNT_ACCESS_ERROR;
      goto free_and_exit;
    }
    for (i = 0; values[i] != NULL; ++i) {
      mount_src = get_mount_source(values[i]);
      if (mount_src == NULL) {
        fprintf(ERRORFILE, "Invalid docker mount '%s', realpath=%s\n", values[i], mount_src);
        ret = INVALID_DOCKER_MOUNT;
        goto free_and_exit;
      }
      permitted_rw = check_mount_permitted((const char **) permitted_rw_mounts, mount_src);
      permitted_ro = check_mount_permitted((const char **) permitted_ro_mounts, mount_src);
      if (permitted_ro == -1 || permitted_rw == -1) {
        fprintf(ERRORFILE, "Invalid docker mount '%s', realpath=%s\n", values[i], mount_src);
        ret = INVALID_DOCKER_MOUNT;
        goto free_and_exit;
      }
      // rw mount
      if (ro == 0) {
        if (permitted_rw == 0) {
          fprintf(ERRORFILE, "Invalid docker rw mount '%s', realpath=%s\n", values[i], mount_src);
          ret = INVALID_DOCKER_RW_MOUNT;
          goto free_and_exit;
        } else {
          // determine if the user can modify the container-executor.cfg file
          tmp_path_buffer[0] = normalize_mount(mount_src, 0);
          // just re-use the function, flip the args to check if the container-executor path is in the requested
          // mount point
          ret = check_mount_permitted(tmp_path_buffer, container_executor_cfg_path);
          free((void *) tmp_path_buffer[0]);
          if (ret == 1) {
            fprintf(ERRORFILE, "Attempting to mount a parent directory '%s' of container-executor.cfg as read-write\n",
                    values[i]);
            ret = INVALID_DOCKER_RW_MOUNT;
            goto free_and_exit;
          }
        }
      }
      //ro mount
      if (ro != 0 && permitted_ro == 0 && permitted_rw == 0) {
        fprintf(ERRORFILE, "Invalid docker ro mount '%s', realpath=%s\n", values[i], mount_src);
        ret = INVALID_DOCKER_RO_MOUNT;
        goto free_and_exit;
      }
      tmp_buffer_2 = (char *) alloc_and_clear_memory(strlen(values[i]) + strlen(ro_suffix) + 1, sizeof(char));
      strncpy(tmp_buffer_2, values[i], strlen(values[i]));
      strncpy(tmp_buffer_2 + strlen(values[i]), ro_suffix, strlen(ro_suffix));
      quote_and_append_arg(&tmp_buffer, &tmp_buffer_size, "-v ", tmp_buffer_2);
      ret = add_to_buffer(out, outlen, tmp_buffer);
      free(tmp_buffer_2);
      free(mount_src);
      tmp_buffer_2 = NULL;
      mount_src = NULL;
      memset(tmp_buffer, 0, tmp_buffer_size);
      if (ret != 0) {
        ret = BUFFER_TOO_SMALL;
        goto free_and_exit;
      }
    }
  }

  free_and_exit:
  free_values(permitted_ro_mounts);
  free_values(permitted_rw_mounts);
  free_values(values);
  free(mount_src);
  free((void *) container_executor_cfg_path);
  free(tmp_buffer);
  if (ret != 0) {
    memset(out, 0, outlen);
  }
  return ret;
}

static int add_ro_mounts(const struct configuration *command_config, const struct configuration *conf, char *out,
                          const size_t outlen) {
  return add_mounts(command_config, conf, "ro-mounts", 1, out, outlen);
}

static int  add_rw_mounts(const struct configuration *command_config, const struct configuration *conf, char *out,
                          const size_t outlen) {
  return add_mounts(command_config, conf, "rw-mounts", 0, out, outlen);
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

  int rc = getgrouplist(user, pw->pw_gid, groups, &ngroups);
  if (rc < 0) {
    groups = (gid_t *) alloc_and_clear_memory(ngroups, sizeof(gid_t));
    if (groups == NULL) {
      fprintf(ERRORFILE, "Failed to allocate buffer for group lookup for user %s.\n", user);
      exit(OUT_OF_MEMORY);
    }
    if (getgrouplist(user, pw->pw_gid, groups, &ngroups) == -1) {
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
      execl("/bin/sudo", "sudo", "-U", user, "-n", "-l", "docker", NULL);
      exit(INITIALIZE_USER_FAILED);
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

static int set_privileged(const struct configuration *command_config, const struct configuration *conf, char *out,
                          const size_t outlen) {
  size_t tmp_buffer_size = 1024;
  char *user = NULL;
  char *tmp_buffer = (char *) alloc_and_clear_memory(tmp_buffer_size, sizeof(char));
  char *value = get_configuration_value("privileged", DOCKER_COMMAND_FILE_SECTION, command_config);
  char *privileged_container_enabled
      = get_configuration_value("docker.privileged-containers.enabled", CONTAINER_EXECUTOR_CFG_DOCKER_SECTION, conf);
  int ret = 0;
  int allowed = 0;

  user = get_configuration_value("user", DOCKER_COMMAND_FILE_SECTION, command_config);
  if (user == NULL) {
    return INVALID_DOCKER_USER_NAME;
  }

  if (value != NULL && strcasecmp(value, "true") == 0 ) {
    if (privileged_container_enabled != NULL) {
      if (strcmp(privileged_container_enabled, "1") == 0 ||
          strcasecmp(privileged_container_enabled, "True") == 0) {
        // Disable set privileged if image is not trusted.
        if (check_trusted_image(command_config, conf) != 0) {
          fprintf(ERRORFILE, "Privileged containers are disabled from untrusted source\n");
          ret = PRIVILEGED_CONTAINERS_DISABLED;
          goto free_and_exit;
        }
        allowed = check_privileges(user);
        if (allowed) {
          ret = add_to_buffer(out, outlen, "--privileged ");
          if (ret != 0) {
            ret = BUFFER_TOO_SMALL;
          }
        } else {
          fprintf(ERRORFILE, "Privileged containers are disabled for user: %s\n", user);
          ret = PRIVILEGED_CONTAINERS_DISABLED;
          goto free_and_exit;
        }
      } else {
        fprintf(ERRORFILE, "Privileged containers are disabled\n");
        ret = PRIVILEGED_CONTAINERS_DISABLED;
        goto free_and_exit;
      }
    } else {
      fprintf(ERRORFILE, "Privileged containers are disabled\n");
      ret = PRIVILEGED_CONTAINERS_DISABLED;
      goto free_and_exit;
    }
  }

  free_and_exit:
  free(tmp_buffer);
  free(value);
  free(privileged_container_enabled);
  free(user);
  if (ret != 0) {
    memset(out, 0, outlen);
  }
  return ret;
}

int get_docker_run_command(const char *command_file, const struct configuration *conf, char *out, const size_t outlen) {
  int ret = 0, i = 0;
  char *container_name = NULL, *user = NULL, *image = NULL;
  size_t tmp_buffer_size = 1024;
  char *tmp_buffer = NULL;
  char **launch_command = NULL;
  char *privileged = NULL;
  struct configuration command_config = {0, NULL};
  ret = read_and_verify_command_file(command_file, DOCKER_RUN_COMMAND, &command_config);
  if (ret != 0) {
    return ret;
  }

  container_name = get_configuration_value("name", DOCKER_COMMAND_FILE_SECTION, &command_config);
  if (container_name == NULL || validate_container_name(container_name) != 0) {
    return INVALID_DOCKER_CONTAINER_NAME;
  }
  user = get_configuration_value("user", DOCKER_COMMAND_FILE_SECTION, &command_config);
  if (user == NULL) {
    return INVALID_DOCKER_USER_NAME;
  }
  image = get_configuration_value("image", DOCKER_COMMAND_FILE_SECTION, &command_config);
  if (image == NULL || validate_docker_image_name(image) != 0) {
    return INVALID_DOCKER_IMAGE_NAME;
  }

  ret = add_docker_config_param(&command_config, out, outlen);
  if (ret != 0) {
    return BUFFER_TOO_SMALL;
  }

  ret = add_to_buffer(out, outlen, DOCKER_RUN_COMMAND);
  if(ret != 0) {
    return BUFFER_TOO_SMALL;
  }


  tmp_buffer = (char *) alloc_and_clear_memory(tmp_buffer_size, sizeof(char));

  quote_and_append_arg(&tmp_buffer, &tmp_buffer_size, " --name=", container_name);
  ret = add_to_buffer(out, outlen, tmp_buffer);
  if (ret != 0) {
    return BUFFER_TOO_SMALL;
  }
  memset(tmp_buffer, 0, tmp_buffer_size);

  privileged = get_configuration_value("privileged", DOCKER_COMMAND_FILE_SECTION, &command_config);

  if (privileged == NULL || strcasecmp(privileged, "false") == 0) {
      quote_and_append_arg(&tmp_buffer, &tmp_buffer_size, "--user=", user);
      ret = add_to_buffer(out, outlen, tmp_buffer);
      if (ret != 0) {
        return BUFFER_TOO_SMALL;
      }
      memset(tmp_buffer, 0, tmp_buffer_size);
  }
  free(privileged);

  ret = detach_container(&command_config, out, outlen);
  if (ret != 0) {
    return ret;
  }

  ret = rm_container_on_exit(&command_config, out, outlen);
  if (ret != 0) {
    return ret;
  }

  ret = set_container_workdir(&command_config, out, outlen);
  if (ret != 0) {
    return ret;
  }

  ret = set_network(&command_config, conf, out, outlen);
  if (ret != 0) {
    return ret;
  }

  ret = set_pid_namespace(&command_config, conf, out, outlen);
  if (ret != 0) {
    return ret;
  }

  ret = add_ro_mounts(&command_config, conf, out, outlen);
  if (ret != 0) {
    return ret;
  }

  ret = add_rw_mounts(&command_config, conf, out, outlen);
  if (ret != 0) {
    return ret;
  }

  ret = set_cgroup_parent(&command_config, out, outlen);
  if (ret != 0) {
    return ret;
  }

  ret = set_privileged(&command_config, conf, out, outlen);
  if (ret != 0) {
    return ret;
  }

  ret = set_capabilities(&command_config, conf, out, outlen);
  if (ret != 0) {
    return ret;
  }

  ret = set_hostname(&command_config, out, outlen);
  if (ret != 0) {
    return ret;
  }

  ret = set_group_add(&command_config, out, outlen);
  if (ret != 0) {
    return ret;
  }

  ret = set_devices(&command_config, conf, out, outlen);
  if (ret != 0) {
    return ret;
  }

  quote_and_append_arg(&tmp_buffer, &tmp_buffer_size, "", image);
  ret = add_to_buffer(out, outlen, tmp_buffer);
  if (ret != 0) {
    return BUFFER_TOO_SMALL;
  }

  launch_command = get_configuration_values_delimiter("launch-command", DOCKER_COMMAND_FILE_SECTION, &command_config,
                                                      ",");

  if (check_trusted_image(&command_config, conf) != 0) {
    launch_command = NULL;
  }

  if (launch_command != NULL) {
    for (i = 0; launch_command[i] != NULL; ++i) {
      memset(tmp_buffer, 0, tmp_buffer_size);
      quote_and_append_arg(&tmp_buffer, &tmp_buffer_size, "", launch_command[i]);
      ret = add_to_buffer(out, outlen, tmp_buffer);
      if (ret != 0) {
        free_values(launch_command);
        free(tmp_buffer);
        return BUFFER_TOO_SMALL;
      }
    }
    free_values(launch_command);
  }
  free(tmp_buffer);
  return 0;
}



