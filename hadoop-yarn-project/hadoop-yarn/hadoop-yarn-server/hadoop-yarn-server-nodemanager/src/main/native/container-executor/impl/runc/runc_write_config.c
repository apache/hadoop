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
#include <sys/utsname.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "hadoop_user_info.h"

#include "container-executor.h"
#include "utils/cJSON/cJSON.h"
#include "utils/file-utils.h"
#include "util.h"

#include "runc_launch_cmd.h"
#include "runc_write_config.h"

#define RUNC_CONFIG_FILENAME    "config.json"
#define STARTING_JSON_BUFFER_SIZE  (128*1024)


static cJSON* build_runc_config_root(const char* rootfs_path) {
  cJSON* root = cJSON_CreateObject();
  if (cJSON_AddStringToObject(root, "path", rootfs_path) == NULL) {
    goto fail;
  }
  if (cJSON_AddTrueToObject(root, "readonly") == NULL) {
    goto fail;
  }
  return root;

fail:
  cJSON_Delete(root);
  return NULL;
}

static cJSON* build_runc_config_process_user(const char* username) {
  cJSON* user_json = cJSON_CreateObject();
  struct hadoop_user_info* hui = hadoop_user_info_alloc();
  if (hui == NULL) {
    return NULL;
  }

  int rc = hadoop_user_info_fetch(hui, username);
  if (rc != 0) {
    fprintf(ERRORFILE, "Error looking up user %s : %s\n", username,
        strerror(rc));
    goto fail;
  }

  if (cJSON_AddNumberToObject(user_json, "uid", hui->pwd.pw_uid) == NULL) {
    goto fail;
  }
  if (cJSON_AddNumberToObject(user_json, "gid", hui->pwd.pw_gid) == NULL) {
    goto fail;
  }

  rc = hadoop_user_info_getgroups(hui);
  if (rc != 0) {
    fprintf(ERRORFILE, "Error getting groups for user %s : %s\n", username,
        strerror(rc));
    goto fail;
  }

  if (hui->num_gids > 1) {
    cJSON* garray = cJSON_AddArrayToObject(user_json, "additionalGids");
    if (garray == NULL) {
      goto fail;
    }

    // first gid entry is the primary group which is accounted for above
    for (int i = 1; i < hui->num_gids; ++i) {
      cJSON* g = cJSON_CreateNumber(hui->gids[i]);
      if (g == NULL) {
        goto fail;
      }
      cJSON_AddItemToArray(garray, g);
    }
  }

  return user_json;

fail:
  hadoop_user_info_free(hui);
  cJSON_Delete(user_json);
  return NULL;
}

static cJSON* build_runc_config_process(const runc_launch_cmd* rlc) {
  cJSON* process = cJSON_CreateObject();
  if (process == NULL) {
    return NULL;
  }

  cJSON_AddItemReferenceToObject(process, "args", rlc->config.process.args);
  cJSON_AddItemReferenceToObject(process, "cwd", rlc->config.process.cwd);
  cJSON_AddItemReferenceToObject(process, "env", rlc->config.process.env);
  if (cJSON_AddTrueToObject(process, "noNewPrivileges") == NULL) {
    goto fail;
  }

  cJSON* user_json = build_runc_config_process_user(rlc->run_as_user);
  if (user_json == NULL) {
    goto fail;
  }
  cJSON_AddItemToObjectCS(process, "user", user_json);

  return process;

fail:
  cJSON_Delete(process);
  return NULL;
}

static bool add_mount_opts(cJSON* mount_json, va_list opts) {
  const char* opt = va_arg(opts, const char*);
  if (opt == NULL) {
    return true;
  }

  cJSON* opts_array = cJSON_AddArrayToObject(mount_json, "options");
  if (opts_array == NULL) {
    return false;
  }

  do {
    cJSON* opt_json = cJSON_CreateString(opt);
    if (opt_json == NULL) {
      return false;
    }
    cJSON_AddItemToArray(opts_array, opt_json);
    opt = va_arg(opts, const char*);
  } while (opt != NULL);

  return true;
}

static bool add_mount_json(cJSON* mounts_array, const char* src,
    const char* dest, const char* fstype, ...) {
  bool result = false;
  cJSON* m = cJSON_CreateObject();
  if (cJSON_AddStringToObject(m, "source", src) == NULL) {
    goto cleanup;
  }
  if (cJSON_AddStringToObject(m, "destination", dest) == NULL) {
    goto cleanup;
  }
  if (cJSON_AddStringToObject(m, "type", fstype) == NULL) {
    goto cleanup;
  }

  va_list vargs;
  va_start(vargs, fstype);
  result = add_mount_opts(m, vargs);
  va_end(vargs);

  if (result) {
    cJSON_AddItemToArray(mounts_array, m);
  }

cleanup:
  if (!result) {
    cJSON_Delete(m);
  }
  return result;
}

static bool add_std_mounts_json(cJSON* mounts_array) {
  bool result = true;
  result &= add_mount_json(mounts_array, "proc", "/proc", "proc", NULL);
  result &= add_mount_json(mounts_array, "tmpfs", "/dev", "tmpfs",
      "nosuid", "strictatime", "mode=755", "size=65536k", NULL);
  result &= add_mount_json(mounts_array, "devpts", "/dev/pts", "devpts",
      "nosuid", "noexec", "newinstance", "ptmxmode=0666", "mode=0620", "gid=5",
       NULL);
  result &= add_mount_json(mounts_array, "shm", "/dev/shm", "tmpfs",
      "nosuid", "noexec", "nodev", "mode=1777", "size=8g", NULL);
  result &= add_mount_json(mounts_array, "mqueue", "/dev/mqueue", "mqueue",
      "nosuid", "noexec", "nodev", NULL);
  result &= add_mount_json(mounts_array, "sysfs", "/sys", "sysfs",
      "nosuid", "noexec", "nodev", "ro", NULL);
  result &= add_mount_json(mounts_array, "cgroup", "/sys/fs/cgroup", "cgroup",
      "nosuid", "noexec", "nodev", "relatime", "ro", NULL);
  return result;
}

static cJSON* build_runc_config_mounts(const runc_launch_cmd* rlc) {
  cJSON* mjson = cJSON_CreateArray();
  if (!add_std_mounts_json(mjson)) {
    goto fail;
  }

  cJSON* e;
  cJSON_ArrayForEach(e, rlc->config.mounts) {
    cJSON_AddItemReferenceToArray(mjson, e);
  }

  return mjson;

fail:
  cJSON_Delete(mjson);
  return NULL;
}

static cJSON* get_default_linux_devices_json() {
  cJSON* devs = cJSON_CreateArray();
  if (devs == NULL) {
    return NULL;
  }

  cJSON* o = cJSON_CreateObject();
  if (o == NULL) {
    goto fail;
  }
  cJSON_AddItemToArray(devs, o);

  if (cJSON_AddStringToObject(o, "access", "rwm") == NULL) {
    goto fail;
  }

  if (cJSON_AddFalseToObject(o, "allow") == NULL) {
    goto fail;
  }

  return devs;

fail:
  cJSON_Delete(devs);
  return NULL;
}

static bool add_linux_cgroups_json(cJSON* ljson, const runc_launch_cmd* rlc) {
    cJSON* cj = cJSON_GetObjectItemCaseSensitive(rlc->config.linux_config,
                                                 "cgroupsPath");
    if (cj != NULL) {
        cJSON_AddItemReferenceToObject(ljson, "cgroupsPath", cj);
    }
    return true;
}

static bool add_linux_resources_json(cJSON* ljson, const runc_launch_cmd* rlc) {
  cJSON* robj = cJSON_AddObjectToObject(ljson, "resources");
  if (robj == NULL) {
    return false;
  }

  cJSON* devs = get_default_linux_devices_json();
  if (devs == NULL) {
    return false;
  }
  cJSON_AddItemToObjectCS(robj, "devices", devs);

  const cJSON* rlc_rsrc = cJSON_GetObjectItemCaseSensitive(
      rlc->config.linux_config, "resources");
  cJSON* e;
  cJSON_ArrayForEach(e, rlc_rsrc) {
    if (strcmp("devices", e->string) == 0) {
      cJSON* dev_e;
      cJSON_ArrayForEach(dev_e, e) {
        cJSON_AddItemReferenceToArray(devs, dev_e);
      }
    } else {
      cJSON_AddItemReferenceToObject(robj, e->string, e);
    }
  }

  return true;
}

static bool add_linux_namespace_json(cJSON* ljson, const char* ns_type) {
  cJSON* ns = cJSON_CreateObject();
  if (ns == NULL) {
    return false;
  }
  cJSON_AddItemToArray(ljson, ns);
  return (cJSON_AddStringToObject(ns, "type", ns_type) != NULL);
}

static bool add_linux_namespaces_json(cJSON* ljson) {
  cJSON* ns_array = cJSON_AddArrayToObject(ljson, "namespaces");
  if (ns_array == NULL) {
    return false;
  }
  bool result = add_linux_namespace_json(ns_array, "pid");
  result &= add_linux_namespace_json(ns_array, "ipc");
  result &= add_linux_namespace_json(ns_array, "uts");
  result &= add_linux_namespace_json(ns_array, "mount");
  return result;
}

static const char* runc_masked_paths[] = {
  "/proc/kcore",
  "/proc/latency_stats",
  "/proc/timer_list",
  "/proc/timer_stats",
  "/proc/sched_debug",
  "/proc/scsi",
  "/sys/firmware"
};

static bool add_linux_masked_paths_json(cJSON* ljson) {
  size_t num_paths = sizeof(runc_masked_paths) / sizeof(runc_masked_paths[0]);
  cJSON* paths = cJSON_CreateStringArray(runc_masked_paths, num_paths);
  if (paths == NULL) {
    return false;
  }
  cJSON_AddItemToObject(ljson, "maskedPaths", paths);
  return true;
}

static const char* runc_readonly_paths[] = {
  "/proc/asound",
  "/proc/bus",
  "/proc/fs",
  "/proc/irq",
  "/proc/sys",
  "/proc/sysrq-trigger"
};

static bool add_linux_readonly_paths_json(cJSON* ljson) {
  size_t num_paths = sizeof(runc_readonly_paths) / sizeof(runc_readonly_paths[0]);
  cJSON* paths = cJSON_CreateStringArray(runc_readonly_paths, num_paths);
  if (paths == NULL) {
    return false;
  }
  cJSON_AddItemToObject(ljson, "readonlyPaths", paths);
  return true;
}

static bool add_linux_seccomp_json(cJSON* ljson, const runc_launch_cmd* rlc) {
  cJSON* sj = cJSON_GetObjectItemCaseSensitive(rlc->config.linux_config,
      "seccomp");
  if (sj != NULL) {
    cJSON_AddItemReferenceToObject(ljson, "seccomp", sj);
  }
  return true;
}

static cJSON* build_runc_config_linux(const runc_launch_cmd* rlc) {
  cJSON* ljson = cJSON_CreateObject();
  if (ljson == NULL) {
    return NULL;
  }

  if (!add_linux_cgroups_json(ljson, rlc)) {
      goto fail;
  }

  if (!add_linux_resources_json(ljson, rlc)) {
    goto fail;
  }

  if (!add_linux_namespaces_json(ljson)) {
    goto fail;
  }

  if (!add_linux_masked_paths_json(ljson)) {
    goto fail;
  }

  if (!add_linux_readonly_paths_json(ljson)) {
    goto fail;
  }

  if (!add_linux_seccomp_json(ljson, rlc)) {
    goto fail;
  }

  return ljson;

fail:
  cJSON_Delete(ljson);
  return NULL;
}

static char* build_runc_config(const runc_launch_cmd* rlc,
    const char* rootfs_path) {
  char* json_data = NULL;

  cJSON* rcj = build_runc_config_json(rlc, rootfs_path);

  json_data = cJSON_PrintBuffered(rcj, STARTING_JSON_BUFFER_SIZE, false);

  return json_data;
}

cJSON* build_runc_config_json(const runc_launch_cmd* rlc,
    const char* rootfs_path) {
  cJSON* rcj = cJSON_CreateObject();
  if (rcj == NULL) {
    goto fail;
  }

  if (cJSON_AddStringToObject(rcj, "runcVersion", "1.0.0") == NULL) {
    goto fail;
  }

  struct utsname uts;
  uname(&uts);
  if (cJSON_AddStringToObject(rcj, "hostname", uts.nodename) == NULL) {
    goto fail;
  }

  cJSON* item = build_runc_config_root(rootfs_path);
  if (item == NULL) {
    goto fail;
  }
  cJSON_AddItemToObjectCS(rcj, "root", item);

  item = build_runc_config_process(rlc);
  if (item == NULL) {
    goto fail;
  }
  cJSON_AddItemToObjectCS(rcj, "process", item);

  item = build_runc_config_mounts(rlc);
  if (item == NULL) {
    goto fail;
  }
  cJSON_AddItemToObjectCS(rcj, "mounts", item);

  item = build_runc_config_linux(rlc);
  if (item == NULL) {
    goto fail;
  }
  cJSON_AddItemToObjectCS(rcj, "linux", item);
  return rcj;

fail:
  cJSON_Delete(rcj);
  return NULL;
}

static char* get_runc_config_path(const char* pid_file) {
  char* dir_end = strrchr(pid_file, '/');
  if (dir_end == NULL) {
    fprintf(ERRORFILE, "Error pid file %s has no parent directory\n", pid_file);
    return NULL;
  }

  int dir_len = (dir_end + 1) - pid_file;  // include trailing slash
  char* config_path = malloc(dir_len + strlen(RUNC_CONFIG_FILENAME) + 1);
  if (config_path == NULL) {
    return NULL;
  }

  char* cp = stpncpy(config_path, pid_file, dir_len);
  stpcpy(cp, RUNC_CONFIG_FILENAME);
  return config_path;
}

/**
 * Creates the runC runtime configuration file for a container.
 *
 * Returns the path to the written configuration file or NULL on error.
 */
char* write_runc_runc_config(const runc_launch_cmd* rlc,
    const char* rootfs_path) {
  char* config_data = build_runc_config(rlc, rootfs_path);
  if (config_data == NULL) {
    return NULL;
  }

  char* runc_config_path = get_runc_config_path(rlc->pid_file);
  if (runc_config_path == NULL) {
    fputs("Unable to generate runc config path\n", ERRORFILE);
    free(config_data);
    return NULL;
  }

  bool write_ok = write_file_as_nm(runc_config_path, config_data,
      strlen(config_data));
  free(config_data);
  if (!write_ok) {
    free(runc_config_path);
    return NULL;
  }

  return runc_config_path;
}
