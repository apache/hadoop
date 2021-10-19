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
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "util.h"
#include "mount-utils.h"
#include "configuration.h"

/**
 * Function to free an options struct.
 * @param options - options struct to be freed.
 * @return void.
 */
void free_mount_options(mount_options *options) {
  if (options == NULL) {
      return;
  }

  if (options->opts != NULL) {
      for (unsigned int i = 0; i < options->num_opts; i++) {
          free(options->opts[i]);
      }
      free(options->opts);
  }

  free(options);
}

/**
 * Function to free an array of mounts.
 * @param mounts - Array of mounts to be freed.
 * @param num_mounts - Number of mounts to be freed.
 * @return void.
 */
void free_mounts(mount *mounts, const unsigned int num_mounts) {
    if (mounts == NULL) {
        return;
    }

    for (unsigned int i = 0; i < num_mounts; i++) {
       free(mounts[i].src);
       free(mounts[i].dest);
       free_mount_options(mounts[i].options);
    }
    free(mounts);
}

/**
 * Function to determine whether a string is a volume name.
 * @param volume_name - string to check.
 * @return 1 on match, 0 on no match.
 */
static int is_volume_name(const char *volume_name) {
    const char *regex_str = "^[a-zA-Z0-9]([a-zA-Z0-9_.-]*)$";
    // execute_regex_match return 0 is matched success
    return execute_regex_match(regex_str, volume_name) == 0;
}

/**
 * Function to determine whether a string is a valid volume name and if
 * it matches a passed in regex.
 * @param requested - string to check.
 * @param pattern - regular expression to check against.
 * @return 1 on match, 0 on no match.
 */
static int is_volume_name_matched_by_regex(const char* requested, const char* pattern) {
    // execute_regex_match return 0 is matched success
    return is_volume_name(requested) && (execute_regex_match(pattern + sizeof("regex:"), requested) == 0);
}

/**
 * Helper function to help normalize mounts for checking if mounts are
 * permitted. The function does the following -
 * 1. Find the canonical path for mount using realpath
 * 2. If the path is a directory, add a '/' at the end (if not present)
 * 3. Return a copy of the canonicalised path(to be freed by the caller)
 * @param mount path to be canonicalised.
 * @param isRegexAllowed whether regex matching is allowed for normalize mount.
 * @return pointer to canonicalised path, NULL on error.
 */
static char* normalize_mount(const char* mount, const int isRegexAllowed) {
    int ret = 0;
    struct stat buff;
    char *ret_ptr = NULL, *real_mount = NULL;
    if (mount == NULL) {
        return NULL;
    }
    real_mount = realpath(mount, NULL);
    if (real_mount == NULL) {
        // If mount is a valid named volume, just return it and let the container runtime decide
        if (is_volume_name(mount)) {
            ret_ptr = strdup(mount);
            goto free_and_exit;
        }
        // we only allow permitted mount to be REGEX, for permitted mount, we check
        // if it's a valid REGEX return; for user mount, we need to strictly check
        if (isRegexAllowed) {
            if (is_regex(mount)) {
                ret_ptr = strdup(mount);
                goto free_and_exit;
            }
        }
        fprintf(ERRORFILE, "Could not determine real path of mount '%s'\n", mount);
        ret_ptr = NULL;
        goto free_and_exit;
    }
    ret = stat(real_mount, &buff);
    if (ret == 0) {
        if (S_ISDIR(buff.st_mode)) {
            size_t len = strlen(real_mount);
            if (len <= 0) {
                ret_ptr = NULL;
                goto free_and_exit;
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

free_and_exit:
    free(real_mount);
    return ret_ptr;
}

/**
 * Function to normalize an array of strings. Each string in the array will
 * be normalized to its real path in the file system.
 * @param mounts - An array of strings to normalize. The contents of this
 * string array will be modified and replaced with their normalized equivalents.
 * If a string is replaced, the original string will be freed. The caller is responsible
 * for freeing the mounts string array.
 * @param isRegexAllowed - Integer to determine whether or not regex is allowed.
 * for any of the strings. 1 for allowed, 0 for not allowed.
 * @return 0 on success, -1 on failure.
 */
static int normalize_mounts(char **mounts, const int isRegexAllowed) {
    unsigned int i = 0;
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

/**
 * Function to get the normalized path of the container-executor config file.
 * @param container_executor_cfg_path - A pointer to a string. This pointer will
 * point to an allocated string containing the normalized path to the container-executor
 * config file. The caller is responsible for freeing this memory.
 * @return 0 on success, MOUNT_ACCESS_ERROR on failure.
 */
static int get_normalized_config_path(const char **container_executor_cfg_path) {
    char *config_path = NULL;
    int ret = 0;

    config_path = get_config_path("");
    *container_executor_cfg_path = normalize_mount(config_path, 0);
    if (*container_executor_cfg_path == NULL) {
        ret = MOUNT_ACCESS_ERROR;
        goto free_and_exit;
    }

free_and_exit:
    free(config_path);
    return ret;
}

/**
 * Function to determine whether or not a requested string path is allowed as a mount.
 * The requested string will be normalized and checked against the normalized paths in the
 * permitted_mounts string array. Volumes that match a regex in the permitted_mounts list
 * will also be allowed.
 * @param permitted_mounts - An array of strings that define the permitted list of paths
 * for the requested string.
 * @param requested - A string that is requested to be mounted.
 * @return 0 on not permitted, 1 on permitted, -1 on error.
 */
static int check_mount_permitted(const char **permitted_mounts, const char *requested) {
    int ret = 0;
    unsigned int i;
    size_t permitted_mount_len = 0;
    if (permitted_mounts == NULL) {
        return 0;
    }
    char *normalized_path = normalize_mount(requested, 0);
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
        if (S_ISDIR(path_stat.st_mode)) {
            if (strncmp(normalized_path, permitted_mounts[i], permitted_mount_len) == 0) {
                ret = 1;
                break;
            }
        }
    }
    free(normalized_path);
    return ret;
}

/**
 * Function to validate whether a requested mount path is permitted or not. The normalized mount path
 * must be in the correct permitted list based on the type of mount (ro or rw) and must not be a
 * parent of the container-executor config file.
 * @param permitted_ro_mounts - Array of permitted read-only mounts.
 * @param permitted_rw_mounts - Array of permitted read-write mounts.
 * @param requested  - Mount path to be validated
 * @return 0 on valid mount, INVALID_MOUNT, INVALID_RW_MOUNT, INVALID_RO_MOUNT,
 * or MOUNT_ACCESS_ERROR on error.
 */
static int validate_mount(const char **permitted_ro_mounts, const char **permitted_rw_mounts, const mount *requested) {
    const char *container_executor_cfg_path = NULL;
    const char *tmp_path_buffer[2] = {NULL, NULL};
    int permitted_rw, permitted_ro;
    int ret = 0;

    if (requested == NULL) {
        goto free_and_exit;
    }

    ret = get_normalized_config_path(&container_executor_cfg_path);
    if (ret != 0) {
        goto free_and_exit;
    }

    permitted_rw = check_mount_permitted(permitted_rw_mounts, requested->src);
    permitted_ro = check_mount_permitted(permitted_ro_mounts, requested->src);

    if (permitted_ro == -1 || permitted_rw == -1) {
        fprintf(ERRORFILE, "Invalid mount src='%s', dest='%s'\n",
                requested->src, requested->dest);
        ret = INVALID_MOUNT;
        goto free_and_exit;
    }

    if (requested->options != NULL && requested->options->rw == 1) {
        // rw mount
        if (permitted_rw == 0) {
            fprintf(ERRORFILE, "Configuration does not allow mount src='%s', dest='%s'\n",
                    requested->src, requested->dest);
            ret = INVALID_RW_MOUNT;
            goto free_and_exit;
        } else {
            // determine if the user can modify the container-executor.cfg file
            tmp_path_buffer[0] = normalize_mount(requested->src, 0);
            // just re-use the function, flip the args to check if the container-executor path is in the requested
            // mount point
            ret = check_mount_permitted(tmp_path_buffer, container_executor_cfg_path);
            free((void *) tmp_path_buffer[0]);
            if (ret == 1) {
                fprintf(ERRORFILE, "Attempting to mount a parent directory of container-executor.cfg as read-write. src='%s', dest='%s'\n",
                        requested->src, requested->dest);
                ret = INVALID_RW_MOUNT;
                goto free_and_exit;
            }
        }
    } else {
        // ro mount
        if (permitted_ro == 0 && permitted_rw == 0) {
            fprintf(ERRORFILE, "Configuration does not allow mount src='%s', dest='%s'\n",
                    requested->src, requested->dest);
            ret = INVALID_RO_MOUNT;
            goto free_and_exit;
        }
    }

free_and_exit:
    free((void *) container_executor_cfg_path);
    return ret;
}

/**
 * Function to validate an array of mounts.
 * @param permitted_ro_mounts - Array of permitted read-only mounts.
 * @param permitted_rw_mounts - Array of permitted read-write mounts.
 * @param mounts - Array of mounts to be validated.
 * @param num_mounts - Number of mounts to be valildated.
 * @return 0 on valid mounts, INVALID_MOUNT, INVALID_RW_MOUNT, INVALID_RO_MOUNT,
 * or MOUNT_ACCESS_ERROR on error.
 */
int validate_mounts(char **permitted_ro_mounts, char **permitted_rw_mounts, mount *mounts, const unsigned int num_mounts) {
    int ret = 0;
    unsigned int i;

    ret = normalize_mounts(permitted_ro_mounts, 1);
    ret |= normalize_mounts(permitted_rw_mounts, 1);
    if (ret != 0) {
        fprintf(ERRORFILE, "Unable to find permitted mounts on disk\n");
        ret = MOUNT_ACCESS_ERROR;
        goto free_and_exit;
    }

    for (i = 0; i < num_mounts; i++) {
        ret = validate_mount((const char **) permitted_ro_mounts, (const char **) permitted_rw_mounts, &mounts[i]);
        if (ret != 0) {
            goto free_and_exit;
        }
    }

free_and_exit:
    return ret;
}
