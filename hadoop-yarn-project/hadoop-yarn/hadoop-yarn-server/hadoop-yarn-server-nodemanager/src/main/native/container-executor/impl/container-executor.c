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

#include <libgen.h>
#include <dirent.h>
#include <fcntl.h>
#include <fts.h>
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

static const int DEFAULT_MIN_USERID = 1000;

static const char* DEFAULT_BANNED_USERS[] = {"mapred", "hdfs", "bin", 0};

//struct to store the user details
struct passwd *user_detail = NULL;

FILE* LOGFILE = NULL;
FILE* ERRORFILE = NULL;

static uid_t nm_uid = -1;
static gid_t nm_gid = -1;

char *concatenate(char *concat_pattern, char *return_path_name,
   int numArgs, ...);

void set_nm_uid(uid_t user, gid_t group) {
  nm_uid = user;
  nm_gid = group;
}

/**
 * get the executable filename.
 */
char* get_executable() {
  char buffer[PATH_MAX];
  snprintf(buffer, PATH_MAX, "/proc/%u/exe", getpid());
  char *filename = malloc(PATH_MAX);
  ssize_t len = readlink(buffer, filename, PATH_MAX);
  if (len == -1) {
    fprintf(ERRORFILE, "Can't get executable name from %s - %s\n", buffer,
            strerror(errno));
    exit(-1);
  } else if (len >= PATH_MAX) {
    fprintf(ERRORFILE, "Executable name %.*s is longer than %d characters.\n",
            PATH_MAX, filename, PATH_MAX);
    exit(-1);
  }
  filename[len] = '\0';
  return filename;
}

int check_executor_permissions(char *executable_file) {

  errno = 0;
  char * resolved_path = realpath(executable_file, NULL);
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
  snprintf(pid_buf, sizeof(pid_buf), "%d", pid);
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

/**
 * Write the pid of the current process into the pid file.
 * pid_file: Path to pid file where pid needs to be written to
 */
static int write_pid_to_file_as_nm(const char* pid_file, pid_t pid) {
  uid_t user = geteuid();
  gid_t group = getegid();
  if (change_effective_user(nm_uid, nm_gid) != 0) {
    return -1;
  }

  char *temp_pid_file = concatenate("%s.tmp", "pid_file_path", 1, pid_file);

  // create with 700
  int pid_fd = open(temp_pid_file, O_WRONLY|O_CREAT|O_EXCL, S_IRWXU);
  if (pid_fd == -1) {
    fprintf(LOGFILE, "Can't open file %s as node manager - %s\n", temp_pid_file,
           strerror(errno));
    free(temp_pid_file);
    return -1;
  }

  // write pid to temp file
  char pid_buf[21];
  snprintf(pid_buf, 21, "%d", pid);
  ssize_t written = write(pid_fd, pid_buf, strlen(pid_buf));
  close(pid_fd);
  if (written == -1) {
    fprintf(LOGFILE, "Failed to write pid to file %s as node manager - %s\n",
       temp_pid_file, strerror(errno));
    free(temp_pid_file);
    return -1;
  }

  // rename temp file to actual pid file
  // use rename as atomic
  if (rename(temp_pid_file, pid_file)) {
    fprintf(LOGFILE, "Can't move pid file from %s to %s as node manager - %s\n",
        temp_pid_file, pid_file, strerror(errno));
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
static int write_exit_code_file(const char* exit_code_file, int exit_code) {
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

  free(tmp_ecode_file);
  return 0;
}

/**
 * Wait for the container process to exit and write the exit code to
 * the exit code file.
 * Returns the exit code of the container process.
 */
static int wait_and_write_exit_code(pid_t pid, const char* exit_code_file) {
  int child_status = -1;
  int exit_code = -1;
  int waitpid_result;

  if (change_effective_user(nm_uid, nm_gid) != 0) {
    return -1;
  }
  do {
    waitpid_result = waitpid(pid, &child_status, 0);
  } while (waitpid_result == -1 && errno == EINTR);
  if (waitpid_result < 0) {
    fprintf(LOGFILE, "Error waiting for container process %d - %s\n",
        pid, strerror(errno));
    return -1;
  }
  if (WIFEXITED(child_status)) {
    exit_code = WEXITSTATUS(child_status);
  } else if (WIFSIGNALED(child_status)) {
    exit_code = 0x80 + WTERMSIG(child_status);
  } else {
    fprintf(LOGFILE, "Unable to determine exit status for pid %d\n", pid);
  }
  if (write_exit_code_file(exit_code_file, exit_code) < 0) {
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
      fprintf(LOGFILE, "One of the arguments passed for %s in null.\n",
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
  return concatenate(USER_DIR_PATTERN, "user_dir_path", 2, nm_root, user);
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
int create_validate_dir(char* npath, mode_t perm, char* path, int finalComponent) {
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
int check_dir(char* npath, mode_t st_mode, mode_t desired, int finalComponent) {
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
  if (app_id == NULL || container_id == NULL || user == NULL || user_detail == NULL || user_detail->pw_name == NULL) {
    fprintf(LOGFILE, 
            "Either app_id, container_id or the user passed is null.\n");
    return -1;
  }

  int result = -1;
  char* const* local_dir_ptr;
  for(local_dir_ptr = local_dir; *local_dir_ptr != NULL; ++local_dir_ptr) {
    char *container_dir = get_container_work_directory(*local_dir_ptr, user, app_id, 
                                                container_id);
    if (container_dir == NULL) {
      return -1;
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

  result = -1;
  // also make the directory for the container logs
  char *combined_name = malloc(strlen(app_id) + strlen(container_id) + 2);
  if (combined_name == NULL) {
    fprintf(LOGFILE, "Malloc of combined name failed\n");
    result = -1;
  } else {
    sprintf(combined_name, "%s/%s", app_id, container_id);

    char* const* log_dir_ptr;
    for(log_dir_ptr = log_dir; *log_dir_ptr != NULL; ++log_dir_ptr) {
      char *container_log_dir = get_app_log_directory(*log_dir_ptr, combined_name);
      if (container_log_dir == NULL) {
        free(combined_name);
        return -1;
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

  result = -1;
  // also make the tmp directory
  char *tmp_dir = get_tmp_directory(work_dir);

  if (tmp_dir == NULL) {
    return -1;
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
  int string_size = sysconf(_SC_GETPW_R_SIZE_MAX);
  void* buffer = malloc(string_size + sizeof(struct passwd));
  struct passwd *result = NULL;
  if (getpwnam_r(user, buffer, buffer + sizeof(struct passwd), string_size,
		 &result) != 0) {
    free(buffer);
    fprintf(LOGFILE, "Can't get user information %s - %s\n", user,
	    strerror(errno));
    return NULL;
  }
  return result;
}

int is_whitelisted(const char *user) {
  char **whitelist = get_values(ALLOWED_SYSTEM_USERS_KEY);
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
  char *min_uid_str = get_value(MIN_USERID_KEY);
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
  char **banned_users = get_values(BANNED_USERS_KEY);
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
    return 0;
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
      if (chmod(path, permissions) != 0) {
        fprintf(LOGFILE, "Can't chmod %s to add the sticky bit - %s\n",
                path, strerror(errno));
        ret = -1;
      } else if (change_owner(path, user, nm_gid) != 0) {
        fprintf(LOGFILE, "Failed to chown %s to %d:%d: %s\n", path, user, nm_gid,
            strerror(errno));
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
  char buffer[buffer_size];
  int out_fd = open(out_filename, O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW, perm);
  if (out_fd == -1) {
    fprintf(LOGFILE, "Can't open %s for output - %s\n", out_filename, 
            strerror(errno));
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
	return -1;
      }
      pos += write_result;
    }
    len = read(input, buffer, buffer_size);
  }
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


/**
 * Function to prepare the application directories for the container.
 */
int initialize_app(const char *user, const char *app_id,
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

  script_file_dest = get_container_launcher_file(work_dir);
  if (script_file_dest == NULL) {
    exit_code = OUT_OF_MEMORY;
    goto cleanup;
  }
  cred_file_dest = get_container_credentials_file(work_dir);
  if (NULL == cred_file_dest) {
    exit_code = OUT_OF_MEMORY;
    goto cleanup;
  }
  exit_code_file = get_exit_code_file(pid_file);
  if (NULL == exit_code_file) {
    exit_code = OUT_OF_MEMORY;
    goto cleanup;
  }

  // open launch script
  int container_file_source = open_file_as_nm(script_name);
  if (container_file_source == -1) {
    goto cleanup;
  }

  // open credentials
  int cred_file_source = open_file_as_nm(cred_file);
  if (cred_file_source == -1) {
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

  // write pid to pidfile
  if (pid_file == NULL
      || write_pid_to_file_as_nm(pid_file, pid) != 0) {
    exit_code = WRITE_PIDFILE_FAILED;
    goto cleanup;
  }

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

  // create the user directory on all disks
  int result = initialize_user(user, local_dirs);
  if (result != 0) {
    return result;
  }

  // initializing log dirs
  int log_create_result = create_log_dirs(app_id, log_dirs);
  if (log_create_result != 0) {
    return log_create_result;
  }

  // give up root privs
  if (change_user(user_detail->pw_uid, user_detail->pw_gid) != 0) {
    exit_code = SETUID_OPER_FAILED;
    goto cleanup;
  }

  // Create container specific directories as user. If there are no resources
  // to localize for this container, app-directories and log-directories are
  // also created automatically as part of this call.
  if (create_container_directories(user, app_id, container_id, local_dirs,
                                   log_dirs, work_dir) != 0) {
    fprintf(LOGFILE, "Could not create container dirs");
    goto cleanup;
  }


  // 700
  if (copy_file(container_file_source, script_name, script_file_dest,S_IRWXU) != 0) {
    goto cleanup;
  }

  // 600
  if (copy_file(cred_file_source, cred_file, cred_file_dest,
        S_IRUSR | S_IWUSR) != 0) {
    goto cleanup;
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
  umask(0027);
  if (chdir(work_dir) != 0) {
    fprintf(LOGFILE, "Can't change directory to %s -%s\n", work_dir,
	    strerror(errno));
    goto cleanup;
  }
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
      fprintf(stderr, 
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
    if (rmdir(path) != 0) {
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

/**
 * Recursively delete the given path.
 * full_path : the path to delete
 * needs_tt_user: the top level directory must be deleted by the tt user.
 */
static int delete_path(const char *full_path, 
                       int needs_tt_user) {
  int exit_code = 0;

  if (full_path == NULL) {
    fprintf(LOGFILE, "Path is null\n");
    exit_code = UNABLE_TO_BUILD_PATH; // may be malloc failed
  } else {
    char *(paths[]) = {strdup(full_path), 0};
    if (paths[0] == NULL) {
      fprintf(LOGFILE, "Malloc failed in delete_path\n");
      return -1;
    }
    // check to make sure the directory exists
    if (access(full_path, F_OK) != 0) {
      if (errno == ENOENT) {
        free(paths[0]);
        return 0;
      }
    }
    FTS* tree = fts_open(paths, FTS_PHYSICAL | FTS_XDEV, NULL);
    FTSENT* entry = NULL;
    int ret = 0;
    int ret_errno = 0;

    if (tree == NULL) {
      fprintf(LOGFILE,
              "Cannot open file traversal structure for the path %s:%s.\n", 
              full_path, strerror(errno));
      free(paths[0]);
      return -1;
    }
    while (((entry = fts_read(tree)) != NULL) && exit_code == 0) {
      switch (entry->fts_info) {

      case FTS_DP:        // A directory being visited in post-order
        if (!needs_tt_user ||
            strcmp(entry->fts_path, full_path) != 0) {
          if (rmdir(entry->fts_accpath) != 0) {
            fprintf(LOGFILE, "Couldn't delete directory %s - %s\n", 
                    entry->fts_path, strerror(errno));
            if (errno == EROFS) {
              exit_code = -1;
            }
            // record the first errno
            if (errno != ENOENT && ret_errno == 0) {
              ret_errno = errno;
            }
          }
        }
        break;

      case FTS_F:         // A regular file
      case FTS_SL:        // A symbolic link
      case FTS_SLNONE:    // A broken symbolic link
      case FTS_DEFAULT:   // Unknown type of file
        if (unlink(entry->fts_accpath) != 0) {
          fprintf(LOGFILE, "Couldn't delete file %s - %s\n", entry->fts_path,
                  strerror(errno));
          if (errno == EROFS) {
            exit_code = -1;
          }
          // record the first errno
          if (errno != ENOENT && ret_errno == 0) {
            ret_errno = errno;
          }
        }
        break;

      case FTS_DNR:       // Unreadable directory
        fprintf(LOGFILE, "Unreadable directory %s. Skipping..\n", 
                entry->fts_path);
        break;

      case FTS_D:         // A directory in pre-order
        // if the directory isn't readable, chmod it
        if ((entry->fts_statp->st_mode & 0200) == 0) {
          fprintf(LOGFILE, "Unreadable directory %s, chmoding.\n", 
                  entry->fts_path);
          if (chmod(entry->fts_accpath, 0700) != 0) {
            fprintf(LOGFILE, "Error chmoding %s - %s, continuing\n", 
                    entry->fts_path, strerror(errno));
          }
        }
        break;

      case FTS_NS:        // A file with no stat(2) information
        // usually a root directory that doesn't exist
        fprintf(LOGFILE, "Directory not found %s\n", entry->fts_path);
        break;

      case FTS_DC:        // A directory that causes a cycle
      case FTS_DOT:       // A dot directory
      case FTS_NSOK:      // No stat information requested
        break;

      case FTS_ERR:       // Error return
        fprintf(LOGFILE, "Error traversing directory %s - %s\n", 
                entry->fts_path, strerror(entry->fts_errno));
        exit_code = -1;
        break;
        break;
      default:
        exit_code = -1;
        break;
      }
    }
    ret = fts_close(tree);
    if (ret_errno != 0) {
      exit_code = -1;
    }
    if (exit_code == 0 && ret != 0) {
      fprintf(LOGFILE, "Error in fts_close while deleting %s\n", full_path);
      exit_code = -1;
    }
    if (needs_tt_user) {
      // If the delete failed, try a final rmdir as root on the top level.
      // That handles the case where the top level directory is in a directory
      // that is owned by the node manager.
      exit_code = rmdir_as_nm(full_path);
    }
    free(paths[0]);
  }
  return exit_code;
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
    while (ep = readdir(dp)) {
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
  char *controller = malloc(strlen(pair));
  char *mount_path = malloc(strlen(pair));
  char hier_path[PATH_MAX];
  int result = 0;

  if (get_kv_key(pair, controller, strlen(pair)) < 0 ||
      get_kv_value(pair, mount_path, strlen(pair)) < 0) {
    fprintf(LOGFILE, "Failed to mount cgroup controller; invalid option: %s\n",
              pair);
    result = -1; 
  } else {
    if (mount("none", mount_path, "cgroup", 0, controller) == 0) {
      char *buf = stpncpy(hier_path, mount_path, strlen(mount_path));
      *buf++ = '/';
      snprintf(buf, PATH_MAX - (buf - hier_path), "%s", hierarchy);

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

