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

#include "configuration.h"
#include "container-executor.h"
#include "utils/docker-util.h"
#include "util.h"
#include "config.h"

#include <inttypes.h>
#include <libgen.h>
#include <dirent.h>
#include <fcntl.h>
#ifdef __sun
#include <sys/param.h>
#define NAME_MAX MAXNAMELEN
#endif
#include <errno.h>
#include <grp.h>
#include <unistd.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <sys/stat.h>
#include <sys/mount.h>
#include <sys/wait.h>
#include <getopt.h>

#ifndef HAVE_FCHMODAT
#include "compat/fchmodat.h"
#endif

#ifndef HAVE_FDOPENDIR
#include "compat/fdopendir.h"
#endif

#ifndef HAVE_FSTATAT
#include "compat/fstatat.h"
#endif

#ifndef HAVE_OPENAT
#include "compat/openat.h"
#endif

#ifndef HAVE_UNLINKAT
#include "compat/unlinkat.h"
#endif

static const int DEFAULT_MIN_USERID = 1000;

static const char* DEFAULT_BANNED_USERS[] = {"yarn", "mapred", "hdfs", "bin", 0};

static const int DEFAULT_DOCKER_SUPPORT_ENABLED = 0;
static const int DEFAULT_TC_SUPPORT_ENABLED = 0;

//location of traffic control binary
static const char* TC_BIN = "/sbin/tc";
static const char* TC_MODIFY_STATE_OPTS [] = { "-b" , NULL};
static const char* TC_READ_STATE_OPTS [] = { "-b", NULL};
static const char* TC_READ_STATS_OPTS [] = { "-s",  "-b", NULL};

//struct to store the user details
struct passwd *user_detail = NULL;

FILE* LOGFILE = NULL;
FILE* ERRORFILE = NULL;

static uid_t nm_uid = -1;
static gid_t nm_gid = -1;

struct configuration CFG = {.size=0, .sections=NULL};
struct section executor_cfg = {.size=0, .kv_pairs=NULL};

char *concatenate(char *concat_pattern, char *return_path_name,
   int numArgs, ...);

void set_nm_uid(uid_t user, gid_t group) {
  nm_uid = user;
  nm_gid = group;
}

//function used to load the configurations present in the secure config
void read_executor_config(const char *file_name) {
  const struct section *tmp = NULL;
  int ret = read_config(file_name, &CFG);
  if (ret == 0) {
    tmp = get_configuration_section("", &CFG);
    if (tmp != NULL) {
      executor_cfg = *tmp;
    }
  }
}

//function used to free executor configuration data
void free_executor_configurations() {
    free_configuration(&CFG);
}

//Lookup nodemanager group from container executor configuration.
char *get_nodemanager_group() {
    return get_section_value(NM_GROUP_KEY, &executor_cfg);
}

int check_executor_permissions(char *executable_file) {

  errno = 0;
#ifdef HAVE_CANONICALIZE_FILE_NAME
  char * resolved_path = canonicalize_file_name(executable_file);
#else
  char * resolved_path = realpath(executable_file, NULL);
#endif
  if (resolved_path == NULL) {
    fprintf(ERRORFILE,
        "Error resolving the canonical name for the executable : %s!",
        strerror(errno));
    return -1;
  }

  struct stat filestat;
  errno = 0;
  if (stat(resolved_path, &filestat) != 0) {
    fprintf(ERRORFILE,
            "Could not stat the executable : %s!.\n", strerror(errno));
    return -1;
  }

  uid_t binary_euid = filestat.st_uid; // Binary's user owner
  gid_t binary_gid = filestat.st_gid; // Binary's group owner

  // Effective uid should be root
  if (binary_euid != 0) {
    fprintf(LOGFILE,
        "The container-executor binary should be user-owned by root.\n");
    return -1;
  }

  if (binary_gid != getgid()) {
    fprintf(LOGFILE, "The configured nodemanager group %d is different from"
            " the group of the executable %d\n", getgid(), binary_gid);
    return -1;
  }

  // check others do not have write/execute permissions
  if ((filestat.st_mode & S_IWOTH) == S_IWOTH ||
      (filestat.st_mode & S_IXOTH) == S_IXOTH) {
    fprintf(LOGFILE,
            "The container-executor binary should not have write or execute "
            "for others.\n");
    return -1;
  }

  // Binary should be setuid executable
  if ((filestat.st_mode & S_ISUID) == 0) {
    fprintf(LOGFILE, "The container-executor binary should be set setuid.\n");
    return -1;
  }

  return 0;
}

/**
 * Change the effective user id to limit damage.
 */
static int change_effective_user(uid_t user, gid_t group) {
  if (geteuid() == user) {
    return 0;
  }
  if (seteuid(0) != 0) {
    return -1;
  }
  if (setegid(group) != 0) {
    fprintf(LOGFILE, "Failed to set effective group id %d - %s\n", group,
            strerror(errno));
    return -1;
  }
  if (seteuid(user) != 0) {
    fprintf(LOGFILE, "Failed to set effective user id %d - %s\n", user,
            strerror(errno));
    return -1;
  }
  return 0;
}

#ifdef __linux
/**
 * Write the pid of the current process to the cgroup file.
 * cgroup_file: Path to cgroup file where pid needs to be written to.
 */
static int write_pid_to_cgroup_as_root(const char* cgroup_file, pid_t pid) {
  uid_t user = geteuid();
  gid_t group = getegid();
  if (change_effective_user(0, 0) != 0) {
    return -1;
  }

  // open
  int cgroup_fd = open(cgroup_file, O_WRONLY | O_APPEND, 0);
  if (cgroup_fd == -1) {
    fprintf(LOGFILE, "Can't open file %s as node manager - %s\n", cgroup_file,
           strerror(errno));
    return -1;
  }

  // write pid
  char pid_buf[21];
  snprintf(pid_buf, sizeof(pid_buf), "%" PRId64, (int64_t)pid);
  ssize_t written = write(cgroup_fd, pid_buf, strlen(pid_buf));
  close(cgroup_fd);
  if (written == -1) {
    fprintf(LOGFILE, "Failed to write pid to file %s - %s\n",
       cgroup_file, strerror(errno));
    return -1;
  }

  // Revert back to the calling user.
  if (change_effective_user(user, group)) {
    return -1;
  }

  return 0;
}
#endif

/**
 * Write the pid of the current process into the pid file.
 * pid_file: Path to pid file where pid needs to be written to
 */
static int write_pid_to_file_as_nm(const char* pid_file, pid_t pid) {
  uid_t user = geteuid();
  gid_t group = getegid();
  if (change_effective_user(nm_uid, nm_gid) != 0) {
    fprintf(ERRORFILE, "Could not change to effective users %d, %d\n", nm_uid, nm_gid);
    fflush(ERRORFILE);
    return -1;
  }

  char *temp_pid_file = concatenate("%s.tmp", "pid_file_path", 1, pid_file);
  fprintf(LOGFILE, "Writing to tmp file %s\n", temp_pid_file);
  fflush(LOGFILE);
  // create with 700
  int pid_fd = open(temp_pid_file, O_WRONLY|O_CREAT|O_EXCL, S_IRWXU);
  if (pid_fd == -1) {
    fprintf(LOGFILE, "Can't open file %s as node manager - %s\n", temp_pid_file,
           strerror(errno));
    fflush(LOGFILE);
    free(temp_pid_file);
    return -1;
  }

  // write pid to temp file
  char pid_buf[21];
  snprintf(pid_buf, 21, "%" PRId64, (int64_t)pid);
  ssize_t written = write(pid_fd, pid_buf, strlen(pid_buf));
  close(pid_fd);
  if (written == -1) {
    fprintf(LOGFILE, "Failed to write pid to file %s as node manager - %s\n",
       temp_pid_file, strerror(errno));
    fflush(LOGFILE);
    free(temp_pid_file);
    return -1;
  }

  // rename temp file to actual pid file
  // use rename as atomic
  if (rename(temp_pid_file, pid_file)) {
    fprintf(LOGFILE, "Can't move pid file from %s to %s as node manager - %s\n",
        temp_pid_file, pid_file, strerror(errno));
    fflush(LOGFILE);
    unlink(temp_pid_file);
    free(temp_pid_file);
    return -1;
  }

  // Revert back to the calling user.
  if (change_effective_user(user, group)) {
	free(temp_pid_file);
    return -1;
  }

  free(temp_pid_file);
  return 0;
}

/**
 * Write the exit code of the container into the exit code file
 * exit_code_file: Path to exit code file where exit code needs to be written
 */
static int write_exit_code_file_as_nm(const char* exit_code_file, int exit_code) {
  uid_t user = geteuid();
  gid_t group = getegid();
  if (change_effective_user(nm_uid, nm_gid) != 0) {
    fprintf(ERRORFILE, "Could not change to effective users %d, %d\n", nm_uid, nm_gid);
    fflush(ERRORFILE);
    return -1;
  }
  char *tmp_ecode_file = concatenate("%s.tmp", "exit_code_path", 1,
      exit_code_file);
  if (tmp_ecode_file == NULL) {
    return -1;
  }

  // create with 700
  int ecode_fd = open(tmp_ecode_file, O_WRONLY|O_CREAT|O_EXCL, S_IRWXU);
  if (ecode_fd == -1) {
    fprintf(LOGFILE, "Can't open file %s - %s\n", tmp_ecode_file,
           strerror(errno));
    free(tmp_ecode_file);
    return -1;
  }

  char ecode_buf[21];
  snprintf(ecode_buf, sizeof(ecode_buf), "%d", exit_code);
  ssize_t written = write(ecode_fd, ecode_buf, strlen(ecode_buf));
  close(ecode_fd);
  if (written == -1) {
    fprintf(LOGFILE, "Failed to write exit code to file %s - %s\n",
       tmp_ecode_file, strerror(errno));
    free(tmp_ecode_file);
    return -1;
  }

  // rename temp file to actual exit code file
  // use rename as atomic
  if (rename(tmp_ecode_file, exit_code_file)) {
    fprintf(LOGFILE, "Can't move exit code file from %s to %s - %s\n",
        tmp_ecode_file, exit_code_file, strerror(errno));
    unlink(tmp_ecode_file);
    free(tmp_ecode_file);
    return -1;
  }

  // always change back
  if (change_effective_user(user, group) != 0) {
    fprintf(ERRORFILE,
       "Could not change to effective users %d, %d\n", user, group);
    fflush(ERRORFILE);
    return -1;
  }
  free(tmp_ecode_file);
  return 0;
}

static int wait_and_get_exit_code(pid_t pid) {
  int child_status = -1;
  int exit_code = -1;
  int waitpid_result;

  do {
      waitpid_result = waitpid(pid, &child_status, 0);
  } while (waitpid_result == -1 && errno == EINTR);

  if (waitpid_result < 0) {
    fprintf(LOGFILE, "error waiting for process %" PRId64 " - %s\n", (int64_t)pid, strerror(errno));
    return -1;
  }

  if (WIFEXITED(child_status)) {
    exit_code = WEXITSTATUS(child_status);
  } else if (WIFSIGNALED(child_status)) {
    exit_code = 0x80 + WTERMSIG(child_status);
  } else {
    fprintf(LOGFILE, "Unable to determine exit status for pid %" PRId64 "\n", (int64_t)pid);
  }

  return exit_code;
}

/**
 * Wait for the container process to exit and write the exit code to
 * the exit code file.
 * Returns the exit code of the container process.
 */
static int wait_and_write_exit_code(pid_t pid, const char* exit_code_file) {
  int exit_code = -1;

  exit_code = wait_and_get_exit_code(pid);
  if (write_exit_code_file_as_nm(exit_code_file, exit_code) < 0) {
    return -1;
  }

  return exit_code;
}

/**
 * Change the real and effective user and group to abandon the super user
 * priviledges.
 */
int change_user(uid_t user, gid_t group) {
  if (user == getuid() && user == geteuid() &&
      group == getgid() && group == getegid()) {
    return 0;
  }

  if (seteuid(0) != 0) {
    fprintf(LOGFILE, "unable to reacquire root - %s\n", strerror(errno));
    fprintf(LOGFILE, "Real: %d:%d; Effective: %d:%d\n",
	    getuid(), getgid(), geteuid(), getegid());
    return SETUID_OPER_FAILED;
  }
  if (setgid(group) != 0) {
    fprintf(LOGFILE, "unable to set group to %d - %s\n", group,
            strerror(errno));
    fprintf(LOGFILE, "Real: %d:%d; Effective: %d:%d\n",
	    getuid(), getgid(), geteuid(), getegid());
    return SETUID_OPER_FAILED;
  }
  if (setuid(user) != 0) {
    fprintf(LOGFILE, "unable to set user to %d - %s\n", user, strerror(errno));
    fprintf(LOGFILE, "Real: %d:%d; Effective: %d:%d\n",
	    getuid(), getgid(), geteuid(), getegid());
    return SETUID_OPER_FAILED;
  }

  return 0;
}

int is_feature_enabled(const char* feature_key, int default_value,
                              struct section *cfg) {
    char *enabled_str = get_section_value(feature_key, cfg);
    int enabled = default_value;

    if (enabled_str != NULL) {
        char *end_ptr = NULL;
        enabled = strtol(enabled_str, &end_ptr, 10);

        if ((enabled_str == end_ptr || *end_ptr != '\0') ||
            (enabled < 0 || enabled > 1)) {
              fprintf(LOGFILE, "Illegal value '%s' for '%s' in configuration. "
              "Using default value: %d.\n", enabled_str, feature_key,
              default_value);
              fflush(LOGFILE);
              free(enabled_str);
              return default_value;
        }

        free(enabled_str);
        return enabled;
    } else {
        return default_value;
    }
}

int is_docker_support_enabled() {
  return is_feature_enabled(DOCKER_SUPPORT_ENABLED_KEY,
                         DEFAULT_DOCKER_SUPPORT_ENABLED, &executor_cfg)
      || docker_module_enabled(&CFG);
}

int is_tc_support_enabled() {
    return is_feature_enabled(TC_SUPPORT_ENABLED_KEY,
    DEFAULT_TC_SUPPORT_ENABLED, &executor_cfg);
}

/**
 * Utility function to concatenate argB to argA using the concat_pattern.
 */
char *concatenate(char *concat_pattern, char *return_path_name,
                  int numArgs, ...) {
  va_list ap;
  va_start(ap, numArgs);
  int strlen_args = 0;
  char *arg = NULL;
  int j;
  for (j = 0; j < numArgs; j++) {
    arg = va_arg(ap, char*);
    if (arg == NULL) {
      fprintf(LOGFILE, "One of the arguments passed for %s is null.\n",
          return_path_name);
      return NULL;
    }
    strlen_args += strlen(arg);
  }
  va_end(ap);

  char *return_path = NULL;
  int str_len = strlen(concat_pattern) + strlen_args + 1;

  return_path = (char *) malloc(str_len);
  if (return_path == NULL) {
    fprintf(LOGFILE, "Unable to allocate memory for %s.\n", return_path_name);
    return NULL;
  }
  va_start(ap, numArgs);
  vsnprintf(return_path, str_len, concat_pattern, ap);
  va_end(ap);
  return return_path;
}

/**
 * Get the app-directory path from nm_root, user name and app-id
 */
char *get_app_directory(const char * nm_root, const char *user,
                        const char *app_id) {
  return concatenate(NM_APP_DIR_PATTERN, "app_dir_path", 3, nm_root, user,
      app_id);
}

/**
 * Get the user directory of a particular user
 */
char *get_user_directory(const char *nm_root, const char *user) {
  int result = check_nm_local_dir(nm_uid, nm_root);
  if (result != 0) {
    return NULL;
  }
  return concatenate(USER_DIR_PATTERN, "user_dir_path", 2, nm_root, user);
}

/**
 * Check node manager local dir permission.
 */
int check_nm_local_dir(uid_t caller_uid, const char *nm_root) {
  struct stat info;
  errno = 0;
  int err = stat(nm_root, &info);
  if (err < 0) {
    fprintf(LOGFILE, "Error checking file stats for %s %d %s.\n", nm_root, err, strerror(errno));
    return 1;
  }
  if (caller_uid != info.st_uid) {
    fprintf(LOGFILE, "Permission mismatch for %s for caller uid: %d, owner uid: %d.\n", nm_root, caller_uid, info.st_uid);
    return 1;
  }
  return 0;
}

/**
 * Get the container directory for the given container_id
 */
char *get_container_work_directory(const char *nm_root, const char *user,
				 const char *app_id, const char *container_id) {
  return concatenate(CONTAINER_DIR_PATTERN, "container_dir_path", 4,
                     nm_root, user, app_id, container_id);
}

char *get_exit_code_file(const char* pid_file) {
  return concatenate("%s.exitcode", "exit_code_file", 1, pid_file);
}

char *get_container_launcher_file(const char* work_dir) {
  return concatenate("%s/%s", "container launcher", 2, work_dir, CONTAINER_SCRIPT);
}

char *get_container_credentials_file(const char* work_dir) {
  return concatenate("%s/%s", "container credentials", 2, work_dir,
      CREDENTIALS_FILENAME);
}

/**
 * Get the app log directory under the given log_root
 */
char* get_app_log_directory(const char *log_root, const char* app_id) {
  return concatenate("%s/%s", "app log dir", 2, log_root,
                             app_id);
}

/**
 * Get the tmp directory under the working directory
 */
char *get_tmp_directory(const char *work_dir) {
  return concatenate("%s/%s", "tmp dir", 2, work_dir, TMP_DIR);
}

/**
 * Ensure that the given path and all of the parent directories are created
 * with the desired permissions.
 */
int mkdirs(const char* path, mode_t perm) {
  struct stat sb;
  char * npath;
  char * p;
  if (stat(path, &sb) == 0) {
    return check_dir(path, sb.st_mode, perm, 1);
  }
  npath = strdup(path);
  if (npath == NULL) {
    fprintf(LOGFILE, "Not enough memory to copy path string");
    return -1;
  }
  /* Skip leading slashes. */
  p = npath;
  while (*p == '/') {
    p++;
  }

  while (NULL != (p = strchr(p, '/'))) {
    *p = '\0';
    if (create_validate_dir(npath, perm, path, 0) == -1) {
      free(npath);
      return -1;
    }
    *p++ = '/'; /* restore slash */
    while (*p == '/')
      p++;
  }

  /* Create the final directory component. */
  if (create_validate_dir(npath, perm, path, 1) == -1) {
    free(npath);
    return -1;
  }
  free(npath);
  return 0;
}

/*
* Create the parent directory if they do not exist. Or check the permission if
* the race condition happens.
* Give 0 or 1 to represent whether this is the final component. If it is, we
* need to check the permission.
*/
int create_validate_dir(const char* npath, mode_t perm, const char* path,
                        int finalComponent) {
  struct stat sb;
  if (stat(npath, &sb) != 0) {
    if (mkdir(npath, perm) != 0) {
      if (errno != EEXIST || stat(npath, &sb) != 0) {
        fprintf(LOGFILE, "Can't create directory %s - %s\n", npath,
                strerror(errno));
        return -1;
      }
      // The directory npath should exist.
      if (check_dir(npath, sb.st_mode, perm, finalComponent) == -1) {
        return -1;
      }
    }
  } else {
    if (check_dir(npath, sb.st_mode, perm, finalComponent) == -1) {
      return -1;
    }
  }
  return 0;
}

// check whether the given path is a directory
// also check the access permissions whether it is the same as desired permissions
int check_dir(const char* npath, mode_t st_mode, mode_t desired, int finalComponent) {
  if (!S_ISDIR(st_mode)) {
    fprintf(LOGFILE, "Path %s is file not dir\n", npath);
    return -1;
  } else if (finalComponent == 1) {
    int filePermInt = st_mode & (S_IRWXU | S_IRWXG | S_IRWXO);
    int desiredInt = desired & (S_IRWXU | S_IRWXG | S_IRWXO);
    if (filePermInt != desiredInt) {
      fprintf(LOGFILE, "Path %s has permission %o but needs permission %o.\n", npath, filePermInt, desiredInt);
      return -1;
    }
  }
  return 0;
}

/**
 * Function to prepare the container directories.
 * It creates the container work and log directories.
 */
static int create_container_directories(const char* user, const char *app_id,
    const char *container_id, char* const* local_dir, char* const* log_dir, const char *work_dir) {
  // create dirs as 0750
  const mode_t perms = S_IRWXU | S_IRGRP | S_IXGRP;
  if (user == NULL || app_id == NULL || container_id == NULL ||
      local_dir == NULL || log_dir == NULL || work_dir == NULL ||
      user_detail == NULL || user_detail->pw_name == NULL) {
    fprintf(LOGFILE,
            "Either app_id, container_id or the user passed is null.\n");
    return ERROR_CREATE_CONTAINER_DIRECTORIES_ARGUMENTS;
  }

  int result = COULD_NOT_CREATE_WORK_DIRECTORIES;
  char* const* local_dir_ptr;
  for(local_dir_ptr = local_dir; *local_dir_ptr != NULL; ++local_dir_ptr) {
    char *container_dir = get_container_work_directory(*local_dir_ptr, user, app_id,
                                                container_id);
    int check = check_nm_local_dir(nm_uid, *local_dir_ptr);
    if (check != 0) {
      free(container_dir);
      continue;
    }
    if (container_dir == NULL) {
      return OUT_OF_MEMORY;
    }
    if (mkdirs(container_dir, perms) == 0) {
      result = 0;
    }
    // continue on to create other work directories
    free(container_dir);

  }
  if (result != 0) {
    return result;
  }

  result = COULD_NOT_CREATE_APP_LOG_DIRECTORIES;
  // also make the directory for the container logs
  char *combined_name = malloc(strlen(app_id) + strlen(container_id) + 2);
  if (combined_name == NULL) {
    fprintf(LOGFILE, "Malloc of combined name failed\n");
    result = OUT_OF_MEMORY;
  } else {
    sprintf(combined_name, "%s/%s", app_id, container_id);

    char* const* log_dir_ptr;
    for(log_dir_ptr = log_dir; *log_dir_ptr != NULL; ++log_dir_ptr) {
      char *container_log_dir = get_app_log_directory(*log_dir_ptr, combined_name);
      int check = check_nm_local_dir(nm_uid, *log_dir_ptr);
      if (check != 0) {
        container_log_dir = NULL;
      }
      if (strstr(container_log_dir, "..") != 0) {
        fprintf(LOGFILE, "Unsupported container log directory path detected.\n");
        container_log_dir = NULL;
      }
      if (container_log_dir == NULL) {
        free(combined_name);
        return OUT_OF_MEMORY;
      } else if (mkdirs(container_log_dir, perms) != 0) {
    	free(container_log_dir);
      } else {
    	result = 0;
    	free(container_log_dir);
      }
    }
    free(combined_name);
  }

  if (result != 0) {
    return result;
  }

  result = COULD_NOT_CREATE_TMP_DIRECTORIES;
  // also make the tmp directory
  char *tmp_dir = get_tmp_directory(work_dir);

  if (tmp_dir == NULL) {
    return OUT_OF_MEMORY;
  }
  if (mkdirs(tmp_dir, perms) == 0) {
    result = 0;
  }
  free(tmp_dir);

  return result;
}

/**
 * Load the user information for a given user name.
 */
static struct passwd* get_user_info(const char* user) {
  size_t string_size = sysconf(_SC_GETPW_R_SIZE_MAX);
  struct passwd *result = NULL;
  if(string_size < 1024) {
    string_size = 1024;
  }
  struct passwd* buffer = malloc(sizeof(struct passwd) + string_size);
  if (NULL == buffer) {
    fprintf(LOGFILE, "Failed malloc in get_user_info");
    return NULL;
  }
  if (getpwnam_r(user, buffer, ((char*)buffer) + sizeof(struct passwd),
        string_size, &result) != 0) {
    free(buffer);
    fprintf(LOGFILE, "Can't get user information %s - %s\n", user,
	    strerror(errno));
    return NULL;
  }
  return result;
}

int is_whitelisted(const char *user) {
  char **whitelist = get_section_values(ALLOWED_SYSTEM_USERS_KEY, &executor_cfg);
  char **users = whitelist;
  if (whitelist != NULL) {
    for(; *users; ++users) {
      if (strncmp(*users, user, sysconf(_SC_LOGIN_NAME_MAX)) == 0) {
        free_values(whitelist);
        return 1;
      }
    }
    free_values(whitelist);
  }
  return 0;
}

/**
 * Is the user a real user account?
 * Checks:
 *   1. Not root
 *   2. UID is above the minimum configured.
 *   3. Not in banned user list
 * Returns NULL on failure
 */
struct passwd* check_user(const char *user) {
  if (strcmp(user, "root") == 0) {
    fprintf(LOGFILE, "Running as root is not allowed\n");
    fflush(LOGFILE);
    return NULL;
  }
  char *min_uid_str = get_section_value(MIN_USERID_KEY, &executor_cfg);
  int min_uid = DEFAULT_MIN_USERID;
  if (min_uid_str != NULL) {
    char *end_ptr = NULL;
    min_uid = strtol(min_uid_str, &end_ptr, 10);
    if (min_uid_str == end_ptr || *end_ptr != '\0') {
      fprintf(LOGFILE, "Illegal value of %s for %s in configuration\n",
	      min_uid_str, MIN_USERID_KEY);
      fflush(LOGFILE);
      free(min_uid_str);
      return NULL;
    }
    free(min_uid_str);
  }
  struct passwd *user_info = get_user_info(user);
  if (NULL == user_info) {
    fprintf(LOGFILE, "User %s not found\n", user);
    fflush(LOGFILE);
    return NULL;
  }
  if (user_info->pw_uid < min_uid && !is_whitelisted(user)) {
    fprintf(LOGFILE, "Requested user %s is not whitelisted and has id %d,"
	    "which is below the minimum allowed %d\n", user, user_info->pw_uid, min_uid);
    fflush(LOGFILE);
    free(user_info);
    return NULL;
  }
  char **banned_users = get_section_values(BANNED_USERS_KEY, &executor_cfg);
  banned_users = banned_users == NULL ?
    (char**) DEFAULT_BANNED_USERS : banned_users;
  char **banned_user = banned_users;
  for(; *banned_user; ++banned_user) {
    if (strcmp(*banned_user, user) == 0) {
      free(user_info);
      if (banned_users != (char**)DEFAULT_BANNED_USERS) {
        free_values(banned_users);
      }
      fprintf(LOGFILE, "Requested user %s is banned\n", user);
      return NULL;
    }
  }
  if (banned_users != NULL && banned_users != (char**)DEFAULT_BANNED_USERS) {
    free_values(banned_users);
  }
  return user_info;
}

/**
 * function used to populate and user_details structure.
 */
int set_user(const char *user) {
  // free any old user
  if (user_detail != NULL) {
    free(user_detail);
    user_detail = NULL;
  }
  user_detail = check_user(user);
  if (user_detail == NULL) {
    return -1;
  }

  if (geteuid() == user_detail->pw_uid) {
    return 0;
  }

  if (initgroups(user, user_detail->pw_gid) != 0) {
    fprintf(LOGFILE, "Error setting supplementary groups for user %s: %s\n",
        user, strerror(errno));
    return -1;
  }

  return change_effective_user(user_detail->pw_uid, user_detail->pw_gid);
}

/**
 * Change the ownership of the given file or directory to the new user.
 */
static int change_owner(const char* path, uid_t user, gid_t group) {
  if (geteuid() == user && getegid() == group) {

  /*
   * On the BSDs, this is not a guaranteed shortcut
   * since group permissions are inherited
   */

#if defined(__FreeBSD__) || defined(__NetBSD__)
    if (chown(path, user, group) != 0) {
      fprintf(LOGFILE, "Can't chown %s to %d:%d - %s\n", path, user, group,
              strerror(errno));
      return -1;
    }
    return 0;
#else
    return 0;
#endif

  } else {
    uid_t old_user = geteuid();
    gid_t old_group = getegid();
    if (change_effective_user(0, group) != 0) {
      return -1;
    }
    if (chown(path, user, group) != 0) {
      fprintf(LOGFILE, "Can't chown %s to %d:%d - %s\n", path, user, group,
	      strerror(errno));
      return -1;
    }
    return change_effective_user(old_user, old_group);
  }
}

/**
 * Create a top level directory for the user.
 * It assumes that the parent directory is *not* writable by the user.
 * It creates directories with 02750 permissions owned by the user
 * and with the group set to the node manager group.
 * return non-0 on failure
 */
int create_directory_for_user(const char* path) {
  // set 2750 permissions and group sticky bit
  mode_t permissions = S_IRWXU | S_IRGRP | S_IXGRP | S_ISGID;
  uid_t user = geteuid();
  gid_t group = getegid();
  uid_t root = 0;
  int ret = 0;

  if(getuid() == root) {
    ret = change_effective_user(root, nm_gid);
  }

  if (ret == 0) {
    if (0 == mkdir(path, permissions) || EEXIST == errno) {
      // need to reassert the group sticky bit
      if (change_owner(path, user, nm_gid) != 0) {
        fprintf(LOGFILE, "Failed to chown %s to %d:%d: %s\n", path, user, nm_gid,
            strerror(errno));
        ret = -1;
      } else if (chmod(path, permissions) != 0) {
        fprintf(LOGFILE, "Can't chmod %s to add the sticky bit - %s\n",
                path, strerror(errno));
        ret = -1;
      }
    } else {
      fprintf(LOGFILE, "Failed to create directory %s - %s\n", path,
              strerror(errno));
      ret = -1;
    }
  }
  if (change_effective_user(user, group) != 0) {
    fprintf(LOGFILE, "Failed to change user to %i - %i\n", user, group);

    ret = -1;
  }
  return ret;
}

/**
 * Open a file as the node manager and return a file descriptor for it.
 * Returns -1 on error
 */
static int open_file_as_nm(const char* filename) {
  uid_t user = geteuid();
  gid_t group = getegid();
  if (change_effective_user(nm_uid, nm_gid) != 0) {
    return -1;
  }
  int result = open(filename, O_RDONLY);
  if (result == -1) {
    fprintf(LOGFILE, "Can't open file %s as node manager - %s\n", filename,
	    strerror(errno));
  }
  if (change_effective_user(user, group)) {
    result = -1;
  }
  return result;
}

/**
 * Copy a file from a fd to a given filename.
 * The new file must not exist and it is created with permissions perm.
 * The input stream is closed.
 * Return 0 if everything is ok.
 */
static int copy_file(int input, const char* in_filename,
		     const char* out_filename, mode_t perm) {
  const int buffer_size = 128*1024;
  char* buffer = malloc(buffer_size);
  if (buffer == NULL) {
    fprintf(LOGFILE, "Failed to allocate buffer while copying file: %s -> %s",
      in_filename, out_filename);
    fflush(LOGFILE);
    return -1;
  }

  int out_fd = open(out_filename, O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW, perm);
  if (out_fd == -1) {
    fprintf(LOGFILE, "Can't open %s for output - %s\n", out_filename,
            strerror(errno));
    fflush(LOGFILE);
    free(buffer);
    return -1;
  }

  ssize_t len = read(input, buffer, buffer_size);
  while (len > 0) {
    ssize_t pos = 0;
    while (pos < len) {
      ssize_t write_result = write(out_fd, buffer + pos, len - pos);
      if (write_result <= 0) {
        fprintf(LOGFILE, "Error writing to %s - %s\n", out_filename,
          strerror(errno));
        close(out_fd);
        free(buffer);
        return -1;
      }
      pos += write_result;
    }
    len = read(input, buffer, buffer_size);
  }
  free(buffer);

  if (len < 0) {
    fprintf(LOGFILE, "Failed to read file %s - %s\n", in_filename,
	    strerror(errno));
    close(out_fd);
    return -1;
  }
  if (close(out_fd) != 0) {
    fprintf(LOGFILE, "Failed to close file %s - %s\n", out_filename,
	    strerror(errno));
    return -1;
  }
  close(input);
  return 0;
}

/**
 * Function to initialize the user directories of a user.
 */
int initialize_user(const char *user, char* const* local_dirs) {

  char *user_dir;
  char* const* local_dir_ptr;
  int failed = 0;
  for(local_dir_ptr = local_dirs; *local_dir_ptr != 0; ++local_dir_ptr) {
    user_dir = get_user_directory(*local_dir_ptr, user);
    if (user_dir == NULL) {
      fprintf(LOGFILE, "Couldn't get userdir directory for %s.\n", user);
      failed = 1;
      break;
    }
    if (create_directory_for_user(user_dir) != 0) {
      failed = 1;
    }
    free(user_dir);
  }
  return failed ? INITIALIZE_USER_FAILED : 0;
}

int create_log_dirs(const char *app_id, char * const * log_dirs) {

  char* const* log_root;
  char *any_one_app_log_dir = NULL;
  for(log_root=log_dirs; *log_root != NULL; ++log_root) {
    char *app_log_dir = get_app_log_directory(*log_root, app_id);
    int result = check_nm_local_dir(nm_uid, *log_root);
    if (result != 0 && app_log_dir != NULL) {
      free(app_log_dir);
      app_log_dir = NULL;
    }
    if (app_log_dir == NULL) {
      // try the next one
    } else if (create_directory_for_user(app_log_dir) != 0) {
      free(app_log_dir);
      return -1;
    } else if (any_one_app_log_dir == NULL) {
      any_one_app_log_dir = app_log_dir;
    } else {
      free(app_log_dir);
    }
  }

  if (any_one_app_log_dir == NULL) {
    fprintf(LOGFILE, "Did not create any app-log directories\n");
    return -1;
  }
  free(any_one_app_log_dir);
  return 0;
}

char* get_container_log_directory(const char *log_root, const char* app_id,
                                  const char *container_id) {
  return concatenate("%s/%s/%s", "container log dir", 3, log_root, app_id,
                     container_id);
}

int create_container_log_dirs(const char *container_id, const char *app_id,
                              char * const * log_dirs) {
  char* const* log_root;
  int created_any_dir = 0;
  for(log_root=log_dirs; *log_root != NULL; ++log_root) {
    char *container_log_dir = get_container_log_directory(*log_root, app_id,
                        container_id);

    if (container_log_dir == NULL) {
      fprintf(LOGFILE,
              "Failed to get container log directory name! Log root directory: %s, App id: %s, Container id: %s\n",
              *log_root, app_id, container_id);
      continue;
    }

    int result = check_nm_local_dir(nm_uid, *log_root);
    if (result != 0 && container_log_dir != NULL) {
      fprintf(LOGFILE, "Unsupported container log directory path (%s) detected.\n",
              container_log_dir);
      free(container_log_dir);
      container_log_dir = NULL;
      continue;
    }

    if (create_directory_for_user(container_log_dir) != 0) {
      fprintf(LOGFILE, "Failed to create container log directory (%s)!\n",
              container_log_dir);
      free(container_log_dir);
      return -1;
    }

    if (!created_any_dir) {
      created_any_dir = 1;
    }

    free(container_log_dir);
  }

  if (!created_any_dir) {
    fprintf(LOGFILE, "Did not create any container log directory.\n");
    return -1;
  }
  return 0;
}

/**
 * Function to prepare the application directories for the container.
 */
int initialize_app(const char *user, const char *app_id,
                   const char *container_id,
                   const char* nmPrivate_credentials_file,
                   char* const* local_dirs, char* const* log_roots,
                   char* const* args) {
  if (app_id == NULL || user == NULL || user_detail == NULL || user_detail->pw_name == NULL) {
    fprintf(LOGFILE, "Either app_id is null or the user passed is null.\n");
    return INVALID_ARGUMENT_NUMBER;
  }

  // create the user directory on all disks
  int result = initialize_user(user, local_dirs);
  if (result != 0) {
    return result;
  }

  // create the log directories for the app on all disks
  int log_create_result = create_log_dirs(app_id, log_roots);
  if (log_create_result != 0) {
    return log_create_result;
  }

  // create the log directories for the container on all disks
  int container_log_create_result = create_container_log_dirs(container_id,
                                    app_id, log_roots);
  if (container_log_create_result != 0) {
    return container_log_create_result;
  }

  // open up the credentials file
  int cred_file = open_file_as_nm(nmPrivate_credentials_file);
  if (cred_file == -1) {
    return -1;
  }

  // give up root privs
  if (change_user(user_detail->pw_uid, user_detail->pw_gid) != 0) {
    return -1;
  }

  // 750
  mode_t permissions = S_IRWXU | S_IRGRP | S_IXGRP;
  char* const* nm_root;
  char *primary_app_dir = NULL;
  for(nm_root=local_dirs; *nm_root != NULL; ++nm_root) {
    char *app_dir = get_app_directory(*nm_root, user, app_id);
    if (app_dir == NULL) {
      // try the next one
    } else if (mkdirs(app_dir, permissions) != 0) {
      free(app_dir);
    } else if (primary_app_dir == NULL) {
      primary_app_dir = app_dir;
    } else {
      free(app_dir);
    }
  }

  if (primary_app_dir == NULL) {
    fprintf(LOGFILE, "Did not create any app directories\n");
    return -1;
  }

  char *nmPrivate_credentials_file_copy = strdup(nmPrivate_credentials_file);
  // TODO: FIXME. The user's copy of creds should go to a path selected by
  // localDirAllocatoir
  char *cred_file_name = concatenate("%s/%s", "cred file", 2,
				   primary_app_dir, basename(nmPrivate_credentials_file_copy));
  if (cred_file_name == NULL) {
	free(nmPrivate_credentials_file_copy);
    return -1;
  }
  if (copy_file(cred_file, nmPrivate_credentials_file,
		  cred_file_name, S_IRUSR|S_IWUSR) != 0){
	free(nmPrivate_credentials_file_copy);
    return -1;
  }

  free(nmPrivate_credentials_file_copy);

  fclose(stdin);
  fflush(LOGFILE);
  if (LOGFILE != stdout) {
    fclose(stdout);
  }
  if (ERRORFILE != stderr) {
    fclose(stderr);
  }
  if (chdir(primary_app_dir) != 0) {
    fprintf(LOGFILE, "Failed to chdir to app dir - %s\n", strerror(errno));
    return -1;
  }
  execvp(args[0], args);
  fprintf(ERRORFILE, "Failure to exec app initialization process - %s\n",
	  strerror(errno));
  return -1;
}

char *construct_docker_command(const char *command_file) {
  int ret = 0;
  size_t command_size = MIN(sysconf(_SC_ARG_MAX), 128*1024);
  char *buffer = alloc_and_clear_memory(command_size, sizeof(char));
  ret = get_docker_command(command_file, &CFG, buffer, command_size);
  if (ret != 0) {
    fprintf(ERRORFILE, "Error constructing docker command, docker error code=%d, error message='%s'\n", ret,
            get_docker_error_message(ret));
    fflush(ERRORFILE);
    exit(DOCKER_RUN_FAILED);
  }
  return buffer;
}

int run_docker(const char *command_file) {
  char* docker_command = construct_docker_command(command_file);
  char* docker_binary = get_docker_binary(&CFG);
  size_t command_size = MIN(sysconf(_SC_ARG_MAX), 128*1024);

  char* docker_command_with_binary = alloc_and_clear_memory(command_size, sizeof(char));
  snprintf(docker_command_with_binary, command_size, "%s %s", docker_binary, docker_command);
  fprintf(LOGFILE, "Invoking '%s'\n", docker_command_with_binary);
  char **args = split_delimiter(docker_command_with_binary, " ");

  int exit_code = -1;
  if (execvp(docker_binary, args) != 0) {
    fprintf(ERRORFILE, "Couldn't execute the container launch with args %s - %s",
              docker_binary, strerror(errno));
      fflush(LOGFILE);
      fflush(ERRORFILE);
      free(docker_binary);
      free(args);
      free(docker_command_with_binary);
      free(docker_command);
      exit_code = DOCKER_RUN_FAILED;
  } else {
    exit_code = 0;
  }
  return exit_code;
}

int create_script_paths(const char *work_dir,
  const char *script_name, const char *cred_file,
  char** script_file_dest, char** cred_file_dest,
  int* container_file_source, int* cred_file_source ) {
  int exit_code = -1;

  *script_file_dest = get_container_launcher_file(work_dir);
  if (script_file_dest == NULL) {
    exit_code = OUT_OF_MEMORY;
    fprintf(ERRORFILE, "Could not create script_file_dest");
    fflush(ERRORFILE);
    return exit_code;
  }

  *cred_file_dest = get_container_credentials_file(work_dir);
  if (NULL == cred_file_dest) {
    exit_code = OUT_OF_MEMORY;
    fprintf(ERRORFILE, "Could not create cred_file_dest");
    fflush(ERRORFILE);
    return exit_code;
  }
  // open launch script
  *container_file_source = open_file_as_nm(script_name);
  if (*container_file_source == -1) {
    exit_code = INVALID_NM_ROOT_DIRS;
    fprintf(ERRORFILE, "Could not open container file");
    fflush(ERRORFILE);
    return exit_code;
  }
  // open credentials
  *cred_file_source = open_file_as_nm(cred_file);
  if (*cred_file_source == -1) {
    exit_code = INVALID_ARGUMENT_NUMBER;
    fprintf(ERRORFILE, "Could not open cred file");
    fflush(ERRORFILE);
    return exit_code;
  }

  exit_code = 0;
  return exit_code;
}

int create_local_dirs(const char * user, const char *app_id,
                       const char *container_id, const char *work_dir,
                       const char *script_name, const char *cred_file,
                       char* const* local_dirs,
                       char* const* log_dirs, int effective_user,
                       char* script_file_dest, char* cred_file_dest,
                       int container_file_source, int cred_file_source) {
  int exit_code = -1;
  // create the user directory on all disks
  int result = initialize_user(user, local_dirs);
  if (result != 0) {
    fprintf(ERRORFILE, "Could not create user dir");
    fflush(ERRORFILE);
    return result;
  }

  // initializing log dirs
  int log_create_result = create_log_dirs(app_id, log_dirs);
  if (log_create_result != 0) {
    fprintf(ERRORFILE, "Could not create log dirs");
    fflush(ERRORFILE);
    return log_create_result;
  }
  if (effective_user == 1) {
    if (change_effective_user(user_detail->pw_uid, user_detail->pw_gid) != 0) {
      fprintf(ERRORFILE, "Could not change to effective users %d, %d\n", user_detail->pw_uid, user_detail->pw_gid);
      fflush(ERRORFILE);
      goto cleanup;
    }
  } else {
   // give up root privs
    if (change_user(user_detail->pw_uid, user_detail->pw_gid) != 0) {
      exit_code = SETUID_OPER_FAILED;
      goto cleanup;
    }
  }
  // Create container specific directories as user. If there are no resources
  // to localize for this container, app-directories and log-directories are
  // also created automatically as part of this call.
  int directory_create_result = create_container_directories(user, app_id,
    container_id, local_dirs, log_dirs, work_dir);
  if (directory_create_result != 0) {
    fprintf(ERRORFILE, "Could not create container dirs");
    fflush(ERRORFILE);
    exit_code = directory_create_result;
    goto cleanup;
  }

  // Copy script file with permissions 700
  if (copy_file(container_file_source, script_name, script_file_dest,S_IRWXU) != 0) {
    fprintf(ERRORFILE, "Could not create copy file %d %s\n", container_file_source, script_file_dest);
    fflush(ERRORFILE);
    exit_code = COULD_NOT_CREATE_SCRIPT_COPY;
    goto cleanup;
  }

  // Copy credential file to permissions 600
  if (copy_file(cred_file_source, cred_file, cred_file_dest,
        S_IRUSR | S_IWUSR) != 0) {
    exit_code = COULD_NOT_CREATE_CREDENTIALS_FILE;
    fprintf(ERRORFILE, "Could not copy file");
    fflush(ERRORFILE);
    goto cleanup;
  }

  if (chdir(work_dir) != 0) {
    fprintf(ERRORFILE, "Can't change directory to %s -%s\n", work_dir,
      strerror(errno));
      fflush(ERRORFILE);
    goto cleanup;
  }
  exit_code = 0;
  cleanup:
  return exit_code;
}

int launch_docker_container_as_user(const char * user, const char *app_id,
                              const char *container_id, const char *work_dir,
                              const char *script_name, const char *cred_file,
                              const char *pid_file, char* const* local_dirs,
                              char* const* log_dirs, const char *command_file,
                              const char *resources_key,
                              char* const* resources_values) {
  int exit_code = -1;
  char *script_file_dest = NULL;
  char *cred_file_dest = NULL;
  char *exit_code_file = NULL;
  char *docker_command_with_binary = NULL;
  char *docker_wait_command = NULL;
  char *docker_logs_command = NULL;
  char *docker_inspect_command = NULL;
  char *docker_rm_command = NULL;
  int container_file_source =-1;
  int cred_file_source = -1;
  int BUFFER_SIZE = 4096;
  char buffer[BUFFER_SIZE];

  size_t command_size = MIN(sysconf(_SC_ARG_MAX), 128*1024);

  docker_command_with_binary = (char *) alloc_and_clear_memory(command_size, sizeof(char));
  docker_wait_command = (char *) alloc_and_clear_memory(command_size, sizeof(char));
  docker_logs_command = (char *) alloc_and_clear_memory(command_size, sizeof(char));
  docker_inspect_command = (char *) alloc_and_clear_memory(command_size, sizeof(char));
  docker_rm_command = (char *) alloc_and_clear_memory(command_size, sizeof(char));

  gid_t user_gid = getegid();
  uid_t prev_uid = geteuid();

  char *docker_command = NULL;
  char *docker_binary = NULL;

  fprintf(LOGFILE, "Creating script paths...\n");
  exit_code = create_script_paths(
    work_dir, script_name, cred_file, &script_file_dest, &cred_file_dest,
    &container_file_source, &cred_file_source);
  if (exit_code != 0) {
    fprintf(ERRORFILE, "Could not create script path\n");
    fflush(ERRORFILE);
    goto cleanup;
  }

  fprintf(LOGFILE, "Creating local dirs...\n");
  exit_code = create_local_dirs(user, app_id, container_id,
    work_dir, script_name, cred_file, local_dirs, log_dirs,
    1, script_file_dest, cred_file_dest,
    container_file_source, cred_file_source);
  if (exit_code != 0) {
    fprintf(ERRORFILE, "Could not create local files and directories %d %d\n", container_file_source, cred_file_source);
    fflush(ERRORFILE);
    goto cleanup;
  }

  docker_command = construct_docker_command(command_file);
  docker_binary = get_docker_binary(&CFG);

  fprintf(LOGFILE, "Getting exit code file...\n");
  exit_code_file = get_exit_code_file(pid_file);
  if (NULL == exit_code_file) {
    exit_code = OUT_OF_MEMORY;
    fprintf(ERRORFILE, "Container out of memory");
    fflush(ERRORFILE);
    goto cleanup;
  }

  fprintf(LOGFILE, "Changing effective user to root...\n");
  if (change_effective_user(0, user_gid) != 0) {
    fprintf(ERRORFILE, "Could not change to effective users %d, %d\n", 0, user_gid);
    fflush(ERRORFILE);
    goto cleanup;
  }

  snprintf(docker_command_with_binary, command_size, "%s %s", docker_binary, docker_command);

  fprintf(LOGFILE, "Launching docker container...\n");
  FILE* start_docker = popen(docker_command_with_binary, "r");
  if (pclose (start_docker) != 0)
  {
    fprintf (ERRORFILE,
     "Could not invoke docker %s.\n", docker_command_with_binary);
    fflush(ERRORFILE);
    exit_code = UNABLE_TO_EXECUTE_CONTAINER_SCRIPT;
    goto cleanup;
  }

  snprintf(docker_inspect_command, command_size,
    "%s inspect --format {{.State.Pid}} %s",
    docker_binary, container_id);

  fprintf(LOGFILE, "Inspecting docker container...\n");
  FILE* inspect_docker = popen(docker_inspect_command, "r");
  int pid = 0;
  int res = fscanf (inspect_docker, "%d", &pid);
  if (pclose (inspect_docker) != 0 || res <= 0)
  {
    fprintf (ERRORFILE,
     "Could not inspect docker to get pid %s.\n", docker_inspect_command);
    fflush(ERRORFILE);
    exit_code = UNABLE_TO_EXECUTE_CONTAINER_SCRIPT;
    goto cleanup;
  }

  if (pid != 0) {
#ifdef __linux
    fprintf(LOGFILE, "Writing to cgroup task files...\n");
    // cgroups-based resource enforcement
    if (resources_key != NULL && ! strcmp(resources_key, "cgroups")) {
      // write pid to cgroups
      char* const* cgroup_ptr;
      for (cgroup_ptr = resources_values; cgroup_ptr != NULL &&
          *cgroup_ptr != NULL; ++cgroup_ptr) {
        if (strcmp(*cgroup_ptr, "none") != 0 &&
             write_pid_to_cgroup_as_root(*cgroup_ptr, pid) != 0) {
          exit_code = WRITE_CGROUP_FAILED;
          goto cleanup;
        }
      }
    }
#endif

    // write pid to pidfile
    fprintf(LOGFILE, "Writing pid file...\n");
    if (pid_file == NULL
        || write_pid_to_file_as_nm(pid_file, (pid_t)pid) != 0) {
      exit_code = WRITE_PIDFILE_FAILED;
      fprintf(ERRORFILE, "Could not write pid to %s", pid_file);
      fflush(ERRORFILE);
      goto cleanup;
    }

    snprintf(docker_wait_command, command_size,
      "%s wait %s", docker_binary, container_id);

    fprintf(LOGFILE, "Waiting for docker container to finish...\n");
    FILE* wait_docker = popen(docker_wait_command, "r");
    res = fscanf (wait_docker, "%d", &exit_code);
    if (pclose (wait_docker) != 0 || res <= 0) {
      fprintf (ERRORFILE,
       "Could not attach to docker; is container dead? %s.\n", docker_wait_command);
      fflush(ERRORFILE);
    }
    if(exit_code != 0) {
      fprintf(ERRORFILE, "Docker container exit code was not zero: %d\n",
      exit_code);
      snprintf(docker_logs_command, command_size, "%s logs --tail=250 %s",
        docker_binary, container_id);
      FILE* logs = popen(docker_logs_command, "r");
      if(logs != NULL) {
        clearerr(logs);
        res = fread(buffer, BUFFER_SIZE, 1, logs);
        if(res < 1) {
          fprintf(ERRORFILE, "%s %d %d\n",
            "Unable to read from docker logs(ferror, feof):", ferror(logs), feof(logs));
          fflush(ERRORFILE);
        }
        else {
          fprintf(ERRORFILE, "%s\n", buffer);
          fflush(ERRORFILE);
        }
      }
      else {
        fprintf(ERRORFILE, "%s\n", "Failed to get output of docker logs");
        fprintf(ERRORFILE, "Command was '%s'\n", docker_logs_command);
        fprintf(ERRORFILE, "%s\n", strerror(errno));
        fflush(ERRORFILE);
      }
      if(pclose(logs) != 0) {
        fprintf(ERRORFILE, "%s\n", "Failed to fetch docker logs");
        fflush(ERRORFILE);
      }
    }
  }

  fprintf(LOGFILE, "Removing docker container post-exit...\n");
  snprintf(docker_rm_command, command_size,
    "%s rm %s", docker_binary, container_id);
  FILE* rm_docker = popen(docker_rm_command, "w");
  if (pclose (rm_docker) != 0)
  {
    fprintf (ERRORFILE,
     "Could not remove container %s.\n", docker_rm_command);
    fflush(ERRORFILE);
    exit_code = UNABLE_TO_EXECUTE_CONTAINER_SCRIPT;
    goto cleanup;
  }

cleanup:

  if (exit_code_file != NULL && write_exit_code_file_as_nm(exit_code_file, exit_code) < 0) {
    fprintf (ERRORFILE,
      "Could not write exit code to file %s.\n", exit_code_file);
    fflush(ERRORFILE);
  }

  // Drop root privileges
  if (change_effective_user(prev_uid, user_gid) != 0) {
    fprintf(ERRORFILE,
      "Could not change to effective users %d, %d\n", prev_uid, user_gid);
    fflush(ERRORFILE);
  }

#if HAVE_FCLOSEALL
  fcloseall();
#else
  // only those fds are opened assuming no bug
  fclose(LOGFILE);
  fclose(ERRORFILE);
  fclose(stdin);
  fclose(stdout);
  fclose(stderr);
#endif
  free(exit_code_file);
  free(script_file_dest);
  free(cred_file_dest);
  free(docker_command_with_binary);
  free(docker_wait_command);
  free(docker_logs_command);
  free(docker_inspect_command);
  free(docker_rm_command);
  return exit_code;
}


int launch_container_as_user(const char *user, const char *app_id,
                   const char *container_id, const char *work_dir,
                   const char *script_name, const char *cred_file,
                   const char* pid_file, char* const* local_dirs,
                   char* const* log_dirs, const char *resources_key,
                   char* const* resources_values) {
  int exit_code = -1;
  char *script_file_dest = NULL;
  char *cred_file_dest = NULL;
  char *exit_code_file = NULL;

  fprintf(LOGFILE, "Getting exit code file...\n");
  exit_code_file = get_exit_code_file(pid_file);
  if (NULL == exit_code_file) {
    exit_code = OUT_OF_MEMORY;
    goto cleanup;
  }

  int container_file_source =-1;
  int cred_file_source = -1;

  fprintf(LOGFILE, "Creating script paths...\n");
  exit_code = create_script_paths(
    work_dir, script_name, cred_file, &script_file_dest, &cred_file_dest,
    &container_file_source, &cred_file_source);
  if (exit_code != 0) {
    fprintf(ERRORFILE, "Could not create local files and directories");
    fflush(ERRORFILE);
    goto cleanup;
  }

  pid_t child_pid = fork();
  if (child_pid != 0) {
    // parent
    exit_code = wait_and_write_exit_code(child_pid, exit_code_file);
    goto cleanup;
  }

  // setsid
  pid_t pid = setsid();
  if (pid == -1) {
    exit_code = SETSID_OPER_FAILED;
    goto cleanup;
  }

  fprintf(LOGFILE, "Writing pid file...\n");
  // write pid to pidfile
  if (pid_file == NULL
      || write_pid_to_file_as_nm(pid_file, pid) != 0) {
    exit_code = WRITE_PIDFILE_FAILED;
    goto cleanup;
  }

#ifdef __linux
  fprintf(LOGFILE, "Writing to cgroup task files...\n");
  // cgroups-based resource enforcement
  if (resources_key != NULL && ! strcmp(resources_key, "cgroups")) {
    // write pid to cgroups
    char* const* cgroup_ptr;
    for (cgroup_ptr = resources_values; cgroup_ptr != NULL &&
         *cgroup_ptr != NULL; ++cgroup_ptr) {
      if (strcmp(*cgroup_ptr, "none") != 0 &&
            write_pid_to_cgroup_as_root(*cgroup_ptr, pid) != 0) {
        exit_code = WRITE_CGROUP_FAILED;
        goto cleanup;
      }
    }
  }
#endif

  fprintf(LOGFILE, "Creating local dirs...\n");
  exit_code = create_local_dirs(user, app_id, container_id,
    work_dir, script_name, cred_file, local_dirs, log_dirs,
    0, script_file_dest, cred_file_dest,
    container_file_source, cred_file_source);
  if (exit_code != 0) {
    fprintf(ERRORFILE, "Could not create local files and directories");
    fflush(ERRORFILE);
    goto cleanup;
  }

  fprintf(LOGFILE, "Launching container...\n");

#if HAVE_FCLOSEALL
  fcloseall();
#else
  // only those fds are opened assuming no bug
  fclose(LOGFILE);
  fclose(ERRORFILE);
  fclose(stdin);
  fclose(stdout);
  fclose(stderr);
#endif
  umask(0027);

  if (execlp(script_file_dest, script_file_dest, NULL) != 0) {
    fprintf(LOGFILE, "Couldn't execute the container launch file %s - %s",
            script_file_dest, strerror(errno));
    exit_code = UNABLE_TO_EXECUTE_CONTAINER_SCRIPT;
    goto cleanup;
  }
  exit_code = 0;

  cleanup:
    free(exit_code_file);
    free(script_file_dest);
    free(cred_file_dest);
    return exit_code;
}

int signal_container_as_user(const char *user, int pid, int sig) {
  if(pid <= 0) {
    return INVALID_CONTAINER_PID;
  }

  if (change_user(user_detail->pw_uid, user_detail->pw_gid) != 0) {
    return SETUID_OPER_FAILED;
  }

  //Don't continue if the process-group is not alive anymore.
  if (kill(-pid,0) < 0) {
    fprintf(LOGFILE, "Error signalling not exist process group %d "
            "with signal %d\n", pid, sig);
    return INVALID_CONTAINER_PID;
  }

  if (kill(-pid, sig) < 0) {
    if(errno != ESRCH) {
      fprintf(LOGFILE,
              "Error signalling process group %d with signal %d - %s\n",
              -pid, sig, strerror(errno));
      fflush(LOGFILE);
      return UNABLE_TO_SIGNAL_CONTAINER;
    } else {
      return INVALID_CONTAINER_PID;
    }
  }
  fprintf(LOGFILE, "Killing process group %d with %d\n", pid, sig);
  return 0;
}

/**
 * Delete a final directory as the node manager user.
 */
static int rmdir_as_nm(const char* path) {
  int user_uid = geteuid();
  int user_gid = getegid();
  int ret = change_effective_user(nm_uid, nm_gid);
  if (ret == 0) {
    if (rmdir(path) != 0 && errno != ENOENT) {
      fprintf(LOGFILE, "rmdir of %s failed - %s\n", path, strerror(errno));
      ret = -1;
    }
  }
  // always change back
  if (change_effective_user(user_uid, user_gid) != 0) {
    ret = -1;
  }
  return ret;
}

static int open_helper(int dirfd, const char *name) {
  int fd;
  if (dirfd >= 0) {
    fd = openat(dirfd, name, O_RDONLY | O_NOFOLLOW);
  } else {
    fd = open(name, O_RDONLY | O_NOFOLLOW);
  }
  if (fd >= 0) {
    return fd;
  }
  return -errno;
}

static int chmod_helper(int dirfd, const char *name, mode_t val) {
  int ret;
  if (dirfd >= 0) {
    ret = fchmodat(dirfd, name, val, 0);
  } else {
    ret = chmod(name, val);
  }
  if (ret >= 0) {
    return 0;
  }
  return errno;
}

static int unlink_helper(int dirfd, const char *name, int flags) {
  int ret;
  if (dirfd >= 0) {
    ret = unlinkat(dirfd, name, flags);
  } else {
    ret = unlink(name);
  }
  if (ret >= 0 || errno == ENOENT) {
    return 0;
  }
  return errno;
}

/**
 * Determine if an entry in a directory is another directory without following
 * symlinks.
 *
 * @param dirfd     The directory file descriptor, or -1 if there is none.
 * @param name      If dirfd is -1, this is the path to examine.
 *                  Otherwise, this is the file name in the directory to
 *                  examine.
 *
 * @return          0 if the entry is a symlink or otherwise not a directory
 *                  1 if the entry is a directory
 *                  A negative errno code if we couldn't access the entry.
 */
static int is_dir_helper(int dirfd, const char *name)
{
  struct stat stat;

  if (dirfd < 0) {
    if (lstat(name, &stat) < 0) {
      return -errno;
    }
  } else {
    if (fstatat(dirfd, name, &stat, AT_SYMLINK_NOFOLLOW) < 0) {
      return -errno;
    }
  }
  return !!S_ISDIR(stat.st_mode);
}

static int recursive_unlink_helper(int dirfd, const char *name,
                                   const char* fullpath)
{
  int fd = -1, ret = 0, unlink_err = 0;
  DIR *dfd = NULL;
  struct stat stat;

  // Check to see if the file is a directory. If not then we can unlink it now.
  ret = is_dir_helper(dirfd, name);
  if (ret < 0) {
    // is_dir_helper failed.
    if (ret == -ENOENT) {
      ret = 0;
      goto done;
    }
    ret = -ret;
    fprintf(LOGFILE, "is_dir_helper(%s) failed: %s\n",
            fullpath, strerror(ret));
    goto done;
  } else if (ret == 0) {
    // is_dir_helper determined that the path is not a directory.
    ret = unlink_helper(dirfd, name, 0);
    if (ret) {
      fprintf(LOGFILE, "failed to unlink %s: %s\n",
              fullpath, strerror(ret));
    }
    goto done;
  }

  // Open the directory. We use O_NOFOLLOW here to ensure that if a symlink was
  // swapped in by an attacker, we will fail to follow it rather than deleting
  // something we potentially should not.
  fd = open_helper(dirfd, name);
  if (fd == -EACCES) {
    ret = chmod_helper(dirfd, name, 0700);
    if (ret) {
      if (ret == ENOENT) {
        ret = 0;
        goto done;
      }
      fprintf(LOGFILE, "chmod(%s) failed: %s\n", fullpath, strerror(ret));
      goto done;
    }
    fd = open_helper(dirfd, name);
  }
  if (fd < 0) {
    ret = -fd;
    if (ret == ENOENT) {
      ret = 0;
      goto done;
    }
    fprintf(LOGFILE, "error opening %s: %s\n", fullpath, strerror(ret));
    goto done;
  }
  if (fstat(fd, &stat) < 0) {
    ret = errno;
    if (ret == ENOENT) {
      ret = 0;
      goto done;
    }
    fprintf(LOGFILE, "failed to stat %s: %s\n", fullpath, strerror(ret));
    goto done;
  }
  if (!(S_ISDIR(stat.st_mode))) {
    ret = unlink_helper(dirfd, name, 0);
    if (ret) {
      fprintf(LOGFILE, "failed to unlink %s: %s\n", fullpath, strerror(ret));
      goto done;
    }
  } else {
    // make sure the directory has full user permissions
    // so entries can be deleted
    if ((stat.st_mode & S_IRWXU) != S_IRWXU) {
      ret = chmod_helper(dirfd, name, 0700);
      if (ret) {
        if (ret == ENOENT) {
          ret = 0;
          goto done;
        }
        fprintf(LOGFILE, "chmod(%s) failed: %s\n", fullpath, strerror(ret));
        goto done;
      }
    }
    dfd = fdopendir(fd);
    if (!dfd) {
      ret = errno;
      if (ret == ENOENT) {
        ret = 0;
        goto done;
      }
      fprintf(LOGFILE, "fopendir(%s) failed: %s\n", fullpath, strerror(ret));
      goto done;
    }
    while (1) {
      struct dirent *de;
      char *new_fullpath = NULL;

      errno = 0;
      de = readdir(dfd);
      if (!de) {
        ret = errno;
        if (ret && ret != ENOENT) {
          fprintf(LOGFILE, "readdir(%s) failed: %s\n", fullpath, strerror(ret));
          goto done;
        }
        break;
      }
      if (!strcmp(de->d_name, ".")) {
        continue;
      }
      if (!strcmp(de->d_name, "..")) {
        continue;
      }
      if (asprintf(&new_fullpath, "%s/%s", fullpath, de->d_name) < 0) {
        fprintf(LOGFILE, "Failed to allocate string for %s/%s.\n",
                fullpath, de->d_name);
        ret = ENOMEM;
        goto done;
      }
      int rc = recursive_unlink_helper(fd, de->d_name, new_fullpath);
      free(new_fullpath);
      if (rc && !unlink_err) {
        unlink_err = rc;
      }
    }
    if (dirfd != -1) {
      ret = unlink_helper(dirfd, name, AT_REMOVEDIR);
      if (ret) {
        fprintf(LOGFILE, "failed to rmdir %s: %s\n", name, strerror(ret));
        goto done;
      }
    }
  }
  ret = unlink_err;
done:
  if (fd >= 0) {
    close(fd);
  }
  if (dfd) {
    closedir(dfd);
  }
  return ret;
}

int recursive_unlink_children(const char *name)
{
  return recursive_unlink_helper(-1, name, name);
}

/**
 * Recursively delete the given path.
 * full_path : the path to delete
 * needs_tt_user: the top level directory must be deleted by the tt user.
 */
static int delete_path(const char *full_path,
                       int needs_tt_user) {
  int ret;

  /* Return an error if the path is null. */
  if (full_path == NULL) {
    fprintf(LOGFILE, "Path is null\n");
    return PATH_TO_DELETE_IS_NULL;
  }
  ret = recursive_unlink_children(full_path);
  if (ret != 0) {
    fprintf(LOGFILE, "Error while deleting %s: %d (%s)\n",
            full_path, ret, strerror(ret));
    return -1;
  }

  /*
   * If required, do the final rmdir as root on the top level.
   * That handles the case where the top level directory is in a directory
   * owned by the node manager.
   */
  if (needs_tt_user) {
    return rmdir_as_nm(full_path);
  }
  /* Otherwise rmdir the top level as the current user. */
  if (rmdir(full_path) != 0) {
    ret = errno;
    if (ret != ENOENT) {
      fprintf(LOGFILE, "Couldn't delete directory %s - %s\n",
              full_path, strerror(ret));
      return -1;
    }
  }
  return 0;
}

/**
 * Delete the given directory as the user from each of the directories
 * user: the user doing the delete
 * subdir: the subdir to delete (if baseDirs is empty, this is treated as
           an absolute path)
 * baseDirs: (optional) the baseDirs where the subdir is located
 */
int delete_as_user(const char *user,
                   const char *subdir,
                   char* const* baseDirs) {
  int ret = 0;
  int subDirEmptyStr = (subdir == NULL || subdir[0] == 0);
  int needs_tt_user = subDirEmptyStr;
  char** ptr;

  // TODO: No switching user? !!!!
  if (baseDirs == NULL || *baseDirs == NULL) {
    return delete_path(subdir, needs_tt_user);
  }
  // do the delete
  for(ptr = (char**)baseDirs; *ptr != NULL; ++ptr) {
    char* full_path = NULL;
    struct stat sb;
    if (stat(*ptr, &sb) != 0) {
      if (errno == ENOENT) {
        // Ignore missing dir. Continue deleting other directories.
        continue;
      } else {
        fprintf(LOGFILE, "Could not stat %s - %s\n", *ptr, strerror(errno));
        return -1;
      }
    }
    if (!S_ISDIR(sb.st_mode)) {
      if (!subDirEmptyStr) {
        fprintf(LOGFILE, "baseDir \"%s\" is a file and cannot contain subdir \"%s\".\n", *ptr, subdir);
        return -1;
      }
      full_path = strdup(*ptr);
      needs_tt_user = 0;
    } else {
      full_path = concatenate("%s/%s", "user subdir", 2, *ptr, subdir);
    }

    if (full_path == NULL) {
      return -1;
    }
    int this_ret = delete_path(full_path, needs_tt_user);
    free(full_path);
    // delete as much as we can, but remember the error
    if (this_ret != 0) {
      ret = this_ret;
    }
  }
  return ret;
}

/**
 * List the files in the given directory as the user.
 * user: the user listing the files
 * target_dir: the directory from which to list files
 */
int list_as_user(const char *target_dir) {
  int ret = 0;
  struct stat sb;

  if (stat(target_dir, &sb) != 0) {
    // If directory doesn't exist or can't be accessed, error out
    fprintf(LOGFILE, "Could not stat %s - %s\n", target_dir,
        strerror(errno));
    ret = -1;
  } else if (!S_ISDIR(sb.st_mode)) {
    // If it's not a directory, list it as the only file
    printf("%s\n", target_dir);
  } else {
    DIR *dir = opendir(target_dir);

    if (dir != NULL) {
      struct dirent *file;

      errno = 0;

      do {
        file = readdir(dir);

        // Ignore the . and .. entries
        if ((file != NULL) &&
            (strcmp(".", file->d_name) != 0) &&
            (strcmp("..", file->d_name) != 0)) {
          printf("%s\n", file->d_name);
        }
      } while (file != NULL);

      // If we ended the directory read early on an error, then error out
      if (errno != 0) {
        fprintf(LOGFILE, "Could not read directory %s - %s\n", target_dir,
            strerror(errno));
        ret = -1;
      }
    } else {
      fprintf(LOGFILE, "Could not open directory %s - %s\n", target_dir,
          strerror(errno));
      ret = -1;
    }
  }

  return ret;
}

void chown_dir_contents(const char *dir_path, uid_t uid, gid_t gid) {
  DIR *dp;
  struct dirent *ep;

  char *path_tmp = malloc(strlen(dir_path) + NAME_MAX + 2);
  if (path_tmp == NULL) {
    return;
  }

  char *buf = stpncpy(path_tmp, dir_path, strlen(dir_path));
  *buf++ = '/';

  dp = opendir(dir_path);
  if (dp != NULL) {
    while ((ep = readdir(dp)) != NULL) {
      stpncpy(buf, ep->d_name, strlen(ep->d_name));
      buf[strlen(ep->d_name)] = '\0';
      change_owner(path_tmp, uid, gid);
    }
    closedir(dp);
  }

  free(path_tmp);
}

/**
 * Mount a cgroup controller at the requested mount point and create
 * a hierarchy for the Hadoop NodeManager to manage.
 * pair: a key-value pair of the form "controller=mount-path"
 * hierarchy: the top directory of the hierarchy for the NM
 */
int mount_cgroup(const char *pair, const char *hierarchy) {
#ifndef __linux
  fprintf(LOGFILE, "Failed to mount cgroup controller, not supported\n");
  return -1;
#else
  size_t len = strlen(pair);
  char *controller = malloc(len);
  char *mount_path = malloc(len);
  char hier_path[EXECUTOR_PATH_MAX];
  int result = 0;
  struct stat sb;

  if (controller == NULL || mount_path == NULL) {
    fprintf(LOGFILE, "Failed to mount cgroup controller; not enough memory\n");
    result = OUT_OF_MEMORY;
  }
  if (get_kv_key(pair, controller, len) < 0 ||
      get_kv_value(pair, mount_path, len) < 0) {
    fprintf(LOGFILE, "Failed to mount cgroup controller; invalid option: %s\n",
              pair);
    result = -1;
  } else {
    if (stat(mount_path, &sb) != 0) {
      // Create mount point, if it does not exist
      const mode_t mount_perms = S_IRWXU | S_IRGRP | S_IXGRP;
      if (mkdirs(mount_path, mount_perms) == 0) {
        fprintf(LOGFILE, "Failed to create cgroup mount point %s at %s\n",
          controller, mount_path);
      }
    }
    if (mount("none", mount_path, "cgroup", 0, controller) == 0) {
      char *buf = stpncpy(hier_path, mount_path, strlen(mount_path));
      *buf++ = '/';
      snprintf(buf, EXECUTOR_PATH_MAX - (buf - hier_path), "%s", hierarchy);

      // create hierarchy as 0750 and chown to Hadoop NM user
      const mode_t perms = S_IRWXU | S_IRGRP | S_IXGRP;
      if (mkdirs(hier_path, perms) == 0) {
        change_owner(hier_path, nm_uid, nm_gid);
        chown_dir_contents(hier_path, nm_uid, nm_gid);
      }
    } else {
      fprintf(LOGFILE, "Failed to mount cgroup controller %s at %s - %s\n",
                controller, mount_path, strerror(errno));
      // if controller is already mounted, don't stop trying to mount others
      if (errno != EBUSY) {
        result = -1;
      }
    }
  }

  free(controller);
  free(mount_path);

  return result;
#endif
}

static int run_traffic_control(const char *opts[], char *command_file) {
  const int max_tc_args = 16;
  const char *args[max_tc_args];
  int i = 0, j = 0;

  args[i++] = TC_BIN;
  while (opts[j] != NULL && i < max_tc_args - 1) {
    args[i] = opts[j];
    ++i, ++j;
  }
  //too many args to tc
  if (i == max_tc_args - 1) {
    fprintf(LOGFILE, "too many args to tc");
    return TRAFFIC_CONTROL_EXECUTION_FAILED;
  }
  args[i++] = command_file;
  args[i] = 0;

  pid_t child_pid = fork();
  if (child_pid != 0) {
    int exit_code = wait_and_get_exit_code(child_pid);
    if (exit_code != 0) {
      fprintf(LOGFILE, "failed to execute tc command!\n");
      return TRAFFIC_CONTROL_EXECUTION_FAILED;
    }
    return 0;
  } else {
    execv(TC_BIN, (char**)args);
    //if we reach here, exec failed
    fprintf(LOGFILE, "failed to execute tc command! error: %s\n", strerror(errno));
    return TRAFFIC_CONTROL_EXECUTION_FAILED;
  }
}

/**
 * Run a batch of tc commands that modify interface configuration. command_file
 * is deleted after being used.
 */
int traffic_control_modify_state(char *command_file) {
  return run_traffic_control(TC_MODIFY_STATE_OPTS, command_file);
}

/**
 * Run a batch of tc commands that read interface configuration. Output is
 * written to standard output and it is expected to be read and parsed by the
 * calling process. command_file is deleted after being used.
 */
int traffic_control_read_state(char *command_file) {
  return run_traffic_control(TC_READ_STATE_OPTS, command_file);
}

/**
 * Run a batch of tc commands that read interface stats. Output is
 * written to standard output and it is expected to be read and parsed by the
 * calling process. command_file is deleted after being used.
 */
int traffic_control_read_stats(char *command_file) {
  return run_traffic_control(TC_READ_STATS_OPTS, command_file);
}

/**
 * FIXME: (wangda) it's better to move executor_cfg out of container-executor.c
 * Now initialize of executor_cfg and data structures are stored inside
 * container-executor which is not a good design.
 */
struct configuration* get_cfg() {
  return &CFG;
}
