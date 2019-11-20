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
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "util.h"
#include "utils/cJSON/cJSON.h"
#include "utils/file-utils.h"
#include "utils/string-utils.h"
#include "utils/mount-utils.h"

#include "configuration.h"
#include "container-executor.h"
#include "runc_config.h"
#include "runc_launch_cmd.h"

#define SQUASHFS_MEDIA_TYPE     "application/vnd.squashfs"

static void free_rlc_layers(rlc_layer_spec* layers, unsigned int num_layers) {
  for (unsigned int i = 0; i < num_layers; ++i) {
    free(layers[i].media_type);
    free(layers[i].path);
  }
  free(layers);
}

/**
 * Free a NULL-terminated array of pointers
 */
static void free_ntarray(char** parray) {
  if (parray != NULL) {
    for (char** p = parray; *p != NULL; ++p) {
      free(*p);
    }
    free(parray);
  }
}

/**
 * Free a runC launch command structure and all memory assruncated with it.
 */
void free_runc_launch_cmd(runc_launch_cmd* rlc) {
  if (rlc != NULL) {
    free(rlc->run_as_user);
    free(rlc->username);
    free(rlc->app_id);
    free(rlc->container_id);
    free(rlc->pid_file);
    free(rlc->script_path);
    free(rlc->cred_path);
    free_ntarray(rlc->local_dirs);
    free_ntarray(rlc->log_dirs);
    free_rlc_layers(rlc->layers, rlc->num_layers);
    cJSON_Delete(rlc->config.hostname);
    cJSON_Delete(rlc->config.linux_config);
    cJSON_Delete(rlc->config.mounts);
    cJSON_Delete(rlc->config.process.args);
    cJSON_Delete(rlc->config.process.cwd);
    cJSON_Delete(rlc->config.process.env);
    free(rlc);
  }
}

static cJSON* parse_json_file(const char* filename) {
  char* data = read_file_to_string_as_nm_user(filename);
  if (data == NULL) {
    fprintf(ERRORFILE, "Cannot read command file %s\n", filename);
    return NULL;
  }

  const char* parse_error_location = NULL;
  cJSON* json = cJSON_ParseWithOpts(data, &parse_error_location, 1);
  if (json == NULL) {
    fprintf(ERRORFILE, "Error parsing command file %s at byte offset %ld\n",
        filename, parse_error_location - data);
  }

  free(data);
  return json;
}

static char** parse_dir_list(const cJSON* dirs_json) {
  if (!cJSON_IsArray(dirs_json)) {
    return NULL;
  }

  int num_dirs = cJSON_GetArraySize(dirs_json);
  if (num_dirs <= 0) {
    return NULL;
  }

  char** dirs = calloc(num_dirs + 1, sizeof(*dirs));  // +1 for terminating NULL
  int i = 0;
  const cJSON* e;
  cJSON_ArrayForEach(e, dirs_json) {
    if (!cJSON_IsString(e)) {
      free_ntarray(dirs);
      return NULL;
    }
    dirs[i++] = strdup(e->valuestring);
  }

  return dirs;
}

static bool parse_runc_launch_cmd_layer(rlc_layer_spec* layer_out,
    const cJSON* layer_json) {
  if (!cJSON_IsObject(layer_json)) {
    fputs("runC launch command layer is not an object\n", ERRORFILE);
    return false;
  }

  const cJSON* media_type_json = cJSON_GetObjectItemCaseSensitive(layer_json,
      "mediaType");
  if (!cJSON_IsString(media_type_json)) {
    fputs("Bad/Missing media type for runC launch command layer\n", ERRORFILE);
    return false;
  }

  const cJSON* path_json = cJSON_GetObjectItemCaseSensitive(layer_json, "path");
  if (!cJSON_IsString(path_json)) {
    fputs("Bad/Missing path for runC launch command layer\n", ERRORFILE);
    return false;
  }

  layer_out->media_type = strdup(media_type_json->valuestring);
  layer_out->path = strdup(path_json->valuestring);
  return true;
}

static rlc_layer_spec* parse_runc_launch_cmd_layers(unsigned int* num_layers_out,
    const cJSON* layers_json) {
  if (!cJSON_IsArray(layers_json)) {
    fputs("Bad/Missing runC launch command layers\n", ERRORFILE);
    return NULL;
  }

  unsigned int num_layers = (unsigned int) cJSON_GetArraySize(layers_json);
  if (num_layers <= 0) {
    return NULL;
  }

  rlc_layer_spec* layers = calloc(num_layers, sizeof(*layers));
  if (layers == NULL) {
    fprintf(ERRORFILE, "Cannot allocate memory for %d layers\n",
        num_layers + 1);
    return NULL;
  }

  unsigned int layer_index = 0;
  const cJSON* e;
  cJSON_ArrayForEach(e, layers_json) {
    if (layer_index >= num_layers) {
      fputs("Iterating past end of layer array\n", ERRORFILE);
      free_rlc_layers(layers, layer_index);
      return NULL;
    }

    if (!parse_runc_launch_cmd_layer(&layers[layer_index], e)) {
      free_rlc_layers(layers, layer_index);
      return NULL;
    }

    ++layer_index;
  }

  *num_layers_out = layer_index;
  return layers;
}

static int parse_json_int(cJSON* json) {
  if (!cJSON_IsNumber(json)) {
    fputs("Bad/Missing runC int\n", ERRORFILE);
    return -1;
  }
  return json->valueint;
}

static int parse_runc_launch_cmd_runc_config(runc_config* rc, cJSON* rc_json) {
  if (!cJSON_IsObject(rc_json)) {
    fputs("Bad/Missing runC runtime config in launch command\n", ERRORFILE);
    return -1;
  }
  rc->hostname = cJSON_DetachItemFromObjectCaseSensitive(rc_json, "hostname");
  rc->linux_config = cJSON_DetachItemFromObjectCaseSensitive(rc_json, "linux");
  rc->mounts = cJSON_DetachItemFromObjectCaseSensitive(rc_json, "mounts");

  cJSON* process_json = cJSON_GetObjectItemCaseSensitive(rc_json, "process");
  if (!cJSON_IsObject(process_json)) {
    fputs("Bad/Missing process section in runC config\n", ERRORFILE);
    return -1;
  }
  rc->process.args = cJSON_DetachItemFromObjectCaseSensitive(
      process_json, "args");
  rc->process.cwd = cJSON_DetachItemFromObjectCaseSensitive(
      process_json, "cwd");
  rc->process.env = cJSON_DetachItemFromObjectCaseSensitive(
      process_json, "env");

  return 0;
}

static bool is_valid_layer_media_type(char* media_type) {
  if (media_type == NULL) {
    return false;
  }

  if (strcmp(SQUASHFS_MEDIA_TYPE, media_type)) {
    fprintf(ERRORFILE, "Unrecognized layer media type: %s\n", media_type);
    return false;
  }

  return true;
}

static bool is_valid_runc_launch_cmd_layers(rlc_layer_spec* layers,
    unsigned int num_layers) {
  if (layers == NULL) {
    return false;
  }

  for (unsigned int i = 0; i < num_layers; ++i) {
    if (!is_valid_layer_media_type(layers[i].media_type)) {
      return false;
    }
    if (layers[i].path == NULL) {
      return false;
    }
  }

  return true;
}

static bool is_valid_runc_config_linux_resources(const cJSON* rclr) {
  if (!cJSON_IsObject(rclr)) {
    fputs("runC config linux resources missing or not an object\n", ERRORFILE);
    return false;
  }

  bool all_sections_ok = true;
  const cJSON* e;
  cJSON_ArrayForEach(e, rclr) {
    if (strcmp("blockIO", e->string) == 0) {
      // block I/O settings allowed
    } else if (strcmp("cpu", e->string) == 0) {
      // cpu settings allowed
    } else {
      fprintf(ERRORFILE,
          "Unrecognized runC config linux resources element: %s\n", e->string);
      all_sections_ok = false;
    }
  }

  return all_sections_ok;
}

static bool is_valid_runc_config_linux_seccomp(const cJSON* rcls) {
  if (!cJSON_IsObject(rcls)) {
    fputs("runC config linux seccomp missing or not an object\n", ERRORFILE);
    return false;
  }

  bool all_sections_ok = true;
  const cJSON* e;
  cJSON_ArrayForEach(e, rcls) {
    if (strcmp("defaultAction", e->string) == 0) {
      // defaultAction allowed
    } else if (strcmp("architectures", e->string) == 0) {
      // architecture settings allowed
    } else if (strcmp("flags", e->string) == 0) {
      // flags allowed
    } else if (strcmp("syscalls", e->string) == 0) {
      // syscalls allowed
    } else {
      fprintf(ERRORFILE,
          "Unrecognized runC config linux seccomp element: %s\n", e->string);
      all_sections_ok = false;
    }
  }

  return all_sections_ok;

}

static bool is_valid_runc_config_linux(const cJSON* rcl) {
  if (!cJSON_IsObject(rcl)) {
    fputs("runC config linux section missing or not an object\n", ERRORFILE);
    return false;
  }

  bool all_sections_ok = true;
  const cJSON* e;
  cJSON_ArrayForEach(e, rcl) {
    if (strcmp("cgroupsPath", e->string) == 0) {
      if (!cJSON_IsString(e)) {
        all_sections_ok = false;
      }
    } else if (strcmp("resources", e->string) == 0) {
      all_sections_ok &= is_valid_runc_config_linux_resources(e);
    } else if (strcmp("seccomp", e->string) == 0) {
      all_sections_ok &= is_valid_runc_config_linux_seccomp(e);
    } else {
      fprintf(ERRORFILE, "Unrecognized runC config linux element: %s\n",
          e->string);
      all_sections_ok = false;
    }
  }

  return all_sections_ok;
}

static bool is_valid_mount_type(const char *type) {
  if (strcmp("bind", type)) {
    fprintf(ERRORFILE, "Invalid runC mount type '%s'\n", type);
    return false;
  }
  return true;
}

static mount_options* get_mount_options(const cJSON* mo) {
  if (!cJSON_IsArray(mo)) {
    fputs("runC config mount options not an array\n", ERRORFILE);
    return NULL;
  }

  unsigned int num_options = cJSON_GetArraySize(mo);

  mount_options *options = (mount_options *) calloc(1, sizeof(*options));
  char **options_array = (char **) calloc(num_options + 1, sizeof(char*));

  options->num_opts = num_options;
  options->opts = options_array;

  bool has_rbind = false;
  bool has_rprivate = false;
  int i = 0;
  const cJSON* e;
  cJSON_ArrayForEach(e, mo) {
    if (!cJSON_IsString(e)) {
      fputs("runC config mount option is not a string\n", ERRORFILE);
      free_mount_options(options);
      return NULL;
    }
    if (strcmp("rbind", e->valuestring) == 0) {
      has_rbind = true;
    } else if (strcmp("rprivate", e->valuestring) == 0) {
      has_rprivate = true;
    } else if (strcmp("rw", e->valuestring) == 0) {
      options->rw = 1;
    } else if (strcmp("ro", e->valuestring) == 0) {
      options->rw = 0;
    }

    options->opts[i] = strdup(e->valuestring);
    i++;
  }
  options->opts[i] = NULL;

  if (!has_rbind) {
    fputs("runC config mount options missing rbind\n", ERRORFILE);
    free_mount_options(options);
    return NULL;
  }
  if (!has_rprivate) {
    fputs("runC config mount options missing rprivate\n", ERRORFILE);
    free_mount_options(options);
    return NULL;
  }

  return options;
}

static int get_runc_mounts(mount* mounts, const cJSON* rcm) {
  if (!cJSON_IsArray(rcm)) {
    fputs("runC config mount entry is not an object\n", ERRORFILE);
    return INVALID_MOUNT;
  }

  bool has_type = false;
  const cJSON *e;
  const cJSON *mount;
  int i = 0;
  int ret = 0;
  cJSON_ArrayForEach(mount, rcm) {
    cJSON_ArrayForEach(e, mount) {
      if (strcmp("type", e->string) == 0) {
        if (!cJSON_IsString(e) || !is_valid_mount_type(e->valuestring)) {
          ret = INVALID_MOUNT;
          goto free_and_exit;
        }
        has_type = true;
      } else if (strcmp("source", e->string) == 0) {
        if (!cJSON_IsString(e)) {
          ret = INVALID_MOUNT;
          goto free_and_exit;
        }
        mounts[i].src = strdup(e->valuestring);
      } else if (strcmp("destination", e->string) == 0) {
        if (!cJSON_IsString(e)) {
          ret = INVALID_MOUNT;
          goto free_and_exit;
        }
        mounts[i].dest = strdup(e->valuestring);
      } else if (strcmp("options", e->string) == 0) {
        if (!cJSON_IsArray(e)) {
          ret = INVALID_MOUNT;
          goto free_and_exit;
        }
        mounts[i].options = get_mount_options(e);
      } else {
        fprintf(ERRORFILE, "Unrecognized runC config mount parameter: %s\n",
                e->string);
        ret = INVALID_MOUNT;
        goto free_and_exit;
      }
    }

    if (!has_type) {
      fputs("runC config mount missing mount type\n", ERRORFILE);
      ret = INVALID_MOUNT;
      goto free_and_exit;
    }

    if (mounts[i].src == NULL) {
      fputs("runC config mount missing source\n", ERRORFILE);
      ret = INVALID_MOUNT;
      goto free_and_exit;
    }

    if (mounts[i].dest == NULL) {
      fputs("runC config mount missing destination\n", ERRORFILE);
      ret = INVALID_MOUNT;
      goto free_and_exit;
    }

    if (mounts[i].options == NULL) {
      fputs("runC config mount missing mount options\n", ERRORFILE);
      ret = INVALID_MOUNT;
      goto free_and_exit;
    }

    i++;
  }

free_and_exit:
    return ret;
}

static bool is_valid_runc_config_mounts(const cJSON* rcm) {
  mount *mounts = NULL;
  unsigned int num_mounts = 0;
  int ret = 0;
  bool all_mounts_ok = true;
  char **permitted_ro_mounts = NULL;
  char **permitted_rw_mounts = NULL;

  if (rcm == NULL) {
    return true;  // OK to have no extra mounts
  }
  if (!cJSON_IsArray(rcm)) {
    fputs("runC config mounts is not an array\n", ERRORFILE);
    return false;
  }

  permitted_ro_mounts = get_configuration_values_delimiter("runc.allowed.ro-mounts",
                                                           CONTAINER_EXECUTOR_CFG_RUNC_SECTION, get_cfg(), ",");
  permitted_rw_mounts = get_configuration_values_delimiter("runc.allowed.rw-mounts",
                                                           CONTAINER_EXECUTOR_CFG_RUNC_SECTION, get_cfg(), ",");

  num_mounts = cJSON_GetArraySize(rcm);

  mounts = (mount *) calloc(num_mounts, sizeof(*mounts));
  if (mounts == NULL) {
    fprintf(ERRORFILE, "Unable to allocate %ld bytes\n", num_mounts * sizeof(*mounts));
    all_mounts_ok = false;
    goto free_and_exit;
  }

  ret = get_runc_mounts(mounts, rcm);
  if (ret != 0) {
    all_mounts_ok = false;
    goto free_and_exit;
  }

  ret = validate_mounts(permitted_ro_mounts, permitted_rw_mounts, mounts, num_mounts);
  if (ret != 0) {
    all_mounts_ok = false;
    goto free_and_exit;
  }

free_and_exit:
  free_values(permitted_ro_mounts);
  free_values(permitted_rw_mounts);
  free_mounts(mounts, num_mounts);
  return all_mounts_ok;
}

static bool is_valid_runc_config_process(const runc_config_process* rcp) {
  if (rcp == NULL) {
    return false;
  }

  if (!cJSON_IsArray(rcp->args)) {
    fputs("runC config process args is missing or not an array\n", ERRORFILE);
    return false;
  }

  const cJSON* e;
  cJSON_ArrayForEach(e, rcp->args) {
    if (!cJSON_IsString(e)) {
      fputs("runC config process args has a non-string in array\n", ERRORFILE);
      return false;
    }
  }

  if (!cJSON_IsString(rcp->cwd)) {
    fputs("Bad/Missing runC config process cwd\n", ERRORFILE);
    return false;
  }

  if (!cJSON_IsArray(rcp->env)) {
    fputs("runC config process env is missing or not an array\n", ERRORFILE);
    return false;
  }
  cJSON_ArrayForEach(e, rcp->env) {
    if (!cJSON_IsString(e)) {
      fputs("runC config process env has a non-string in array\n", ERRORFILE);
      return false;
    }
  }

  return true;
}

static bool is_valid_runc_config(const runc_config* rc) {
  bool is_valid = true;
  if (rc->hostname != NULL && !cJSON_IsString(rc->hostname)) {
    fputs("runC config hostname is not a string\n", ERRORFILE);
    is_valid = false;
  }

  is_valid &= is_valid_runc_config_linux(rc->linux_config);
  is_valid &= is_valid_runc_config_mounts(rc->mounts);
  is_valid &= is_valid_runc_config_process(&rc->process);
  return is_valid;
}

bool is_valid_runc_launch_cmd(const runc_launch_cmd* rlc) {
  if (rlc == NULL) {
    return false;
  }

  if (rlc->run_as_user == NULL) {
    fputs("runC command has bad/missing runAsUser\n", ERRORFILE);
    return false;
  }

  if (rlc->username == NULL) {
    fputs("runC command has bad/missing username\n", ERRORFILE);
    return false;
  }

  if (rlc->app_id == NULL) {
    fputs("runC command has bad/missing application ID\n", ERRORFILE);
    return false;
  }

  if (rlc->container_id == NULL) {
    fputs("runC command has bad/missing container ID\n", ERRORFILE);
    return false;
  }
  if (!validate_container_id(rlc->container_id)) {
    fprintf(ERRORFILE, "Bad container id in runC command: %s\n",
        rlc->container_id);
    return false;
  }

  if (rlc->pid_file == NULL) {
    fputs("runC command has bad/missing pid file\n", ERRORFILE);
    return false;
  }
  if (check_pidfile_as_nm(rlc->pid_file) != 0) {
    fprintf(ERRORFILE, "Bad pidfile %s : %s\n", rlc->pid_file,
        strerror(errno));
    return false;
  }

  if (rlc->script_path == NULL) {
    fputs("runC command has bad/missing container script path\n", ERRORFILE);
    return false;
  }

  if (rlc->cred_path == NULL) {
    fputs("runC command has bad/missing container credentials path\n",
        ERRORFILE);
    return false;
  }

  if (rlc->local_dirs == NULL) {
    fputs("runC command has bad/missing local directories\n", ERRORFILE);
    return false;
  }

  if (rlc->log_dirs == NULL) {
    fputs("runC command has bad/missing log directories\n", ERRORFILE);
    return false;
  }

  if (!is_valid_runc_launch_cmd_layers(rlc->layers, rlc->num_layers)) {
    return false;
  }

  if (rlc->num_reap_layers_keep < 0) {
    fprintf(ERRORFILE, "Bad number of layers to preserve: %d\n",
        rlc->num_reap_layers_keep);
    return false;
  }

  return is_valid_runc_config(&rlc->config);
}

/**
 * Read, parse, and validate a runC container launch command.
 *
 * Returns a pointer to the launch command or NULL on error.
 */
runc_launch_cmd* parse_runc_launch_cmd(const char* command_filename) {
  int ret = 0;
  runc_launch_cmd* rlc = NULL;
  cJSON* rlc_json = NULL;

  rlc_json = parse_json_file(command_filename);
  if (rlc_json == NULL) {
    goto cleanup;
  }

  rlc = calloc(1, sizeof(*rlc));
  if (rlc == NULL) {
    fprintf(ERRORFILE, "Unable to allocate %ld bytes\n", sizeof(*rlc));
    goto cleanup;
  }

  char* run_as_user = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(
      rlc_json, "runAsUser"));
  if (run_as_user== NULL) {
    goto fail_and_exit;
  }
  rlc->run_as_user= strdup(run_as_user);

  char* username = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(
      rlc_json, "username"));
  if (username == NULL) {
    goto fail_and_exit;
  }
  rlc->username = strdup(username);

  char* app_id = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(
      rlc_json, "applicationId"));
  if (app_id == NULL) {
    goto fail_and_exit;
  }
  rlc->app_id = strdup(app_id);

  char* container_id = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(
      rlc_json, "containerId"));
  if (container_id == NULL) {
    goto fail_and_exit;
  }
  rlc->container_id = strdup(container_id);

  char* pid_file = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(
      rlc_json, "pidFile"));
  if (pid_file == NULL) {
    goto fail_and_exit;
  }
  rlc->pid_file = strdup(pid_file);

  char* script_path = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(
      rlc_json, "containerScriptPath"));
  if (script_path == NULL) {
    goto fail_and_exit;
  }
  rlc->script_path = strdup(script_path);

  char* cred_path = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(
     rlc_json, "containerCredentialsPath"));
  if (cred_path == NULL) {
    goto fail_and_exit;
  }
  rlc->cred_path = strdup(cred_path);

  rlc->https = parse_json_int(cJSON_GetObjectItemCaseSensitive(rlc_json, "https"));

  char* keystore_path = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(
          rlc_json, "keystorePath"));
  if (keystore_path != NULL) {
    rlc->keystore_path = strdup(keystore_path);
  }

  char* truststore_path = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(
          rlc_json, "truststorePath"));
  if (truststore_path != NULL) {
    rlc->truststore_path = strdup(truststore_path);
  }

  char **local_dirs = parse_dir_list(cJSON_GetObjectItemCaseSensitive(
      rlc_json, "localDirs"));
  if (local_dirs == NULL) {
    goto fail_and_exit;
  }
  rlc->local_dirs = local_dirs;

  char **log_dirs = parse_dir_list(cJSON_GetObjectItemCaseSensitive(
      rlc_json, "logDirs"));
  if (log_dirs == NULL) {
    goto fail_and_exit;
  }
  rlc->log_dirs = log_dirs;

  rlc_layer_spec* layers = parse_runc_launch_cmd_layers(&rlc->num_layers,
      cJSON_GetObjectItemCaseSensitive(rlc_json,"layers"));
  if (layers == NULL) {
    goto fail_and_exit;
  }
  rlc->layers = layers;

  rlc->num_reap_layers_keep = parse_json_int(
      cJSON_GetObjectItemCaseSensitive(rlc_json, "reapLayerKeepCount"));

  ret = parse_runc_launch_cmd_runc_config(&rlc->config,
      cJSON_GetObjectItemCaseSensitive(rlc_json, "ociRuntimeConfig"));
  if (ret < 0) {
    goto fail_and_exit;
  }

cleanup:
  cJSON_Delete(rlc_json);
  return rlc;

fail_and_exit:
  cJSON_Delete(rlc_json);
  free_runc_launch_cmd(rlc);
  return NULL;
}

