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
#include <sys/stat.h>

static const int DEFAULT_MIN_USERID = 1000;

static const char* DEFAULT_BANNED_USERS[] = {"mapred", "hdfs", "bin", 0};

//struct to store the user details
struct passwd *user_detail = NULL;

FILE* LOGFILE = NULL;

static uid_t tt_uid = -1;
static gid_t tt_gid = -1;

void set_tasktracker_uid(uid_t user, gid_t group) {
  tt_uid = user;
  tt_gid = group;
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
    fprintf(stderr, "Can't get executable name from %s - %s\n", buffer,
            strerror(errno));
    exit(-1);
  } else if (len >= PATH_MAX) {
    fprintf(LOGFILE, "Executable name %.*s is longer than %d characters.\n",
            PATH_MAX, filename, PATH_MAX);
    exit(-1);
  }
  filename[len] = '\0';
  return filename;
}

/**
 * Check the permissions on taskcontroller to make sure that security is
 * promisable. For this, we need container-executor binary to
 *    * be user-owned by root
 *    * be group-owned by a configured special group.
 *    * others do not have any permissions
 *    * be setuid/setgid
 */
int check_taskcontroller_permissions(char *executable_file) {

  errno = 0;
  char * resolved_path = realpath(executable_file, NULL);
  if (resolved_path == NULL) {
    fprintf(LOGFILE,
        "Error resolving the canonical name for the executable : %s!",
        strerror(errno));
    return -1;
  }

  struct stat filestat;
  errno = 0;
  if (stat(resolved_path, &filestat) != 0) {
    fprintf(LOGFILE, 
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
    fprintf(LOGFILE, "The configured tasktracker group %d is different from"
            " the group of the executable %d\n", getgid(), binary_gid);
    return -1;
  }

  // check others do not have read/write/execute permissions
  if ((filestat.st_mode & S_IROTH) == S_IROTH || (filestat.st_mode & S_IWOTH)
      == S_IWOTH || (filestat.st_mode & S_IXOTH) == S_IXOTH) {
    fprintf(LOGFILE,
            "The container-executor binary should not have read or write or"
            " execute for others.\n");
    return -1;
  }

  // Binary should be setuid/setgid executable
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
 * Get the job-directory path from tt_root, user name and job-id
 */
char *get_job_directory(const char * tt_root, const char *user,
                        const char *jobid) {
  return concatenate(TT_JOB_DIR_PATTERN, "job_dir_path", 3, tt_root, user,
      jobid);
}

/**
 * Get the user directory of a particular user
 */
char *get_user_directory(const char *tt_root, const char *user) {
  return concatenate(USER_DIR_PATTERN, "user_dir_path", 2, tt_root, user);
}

char *get_job_work_directory(const char *job_dir) {
  return concatenate("%s/work", "job work", 1, job_dir);
}

/**
 * Get the attempt directory for the given attempt_id
 */
char *get_attempt_work_directory(const char *tt_root, const char *user,
				 const char *job_id, const char *attempt_id) {
  return concatenate(ATTEMPT_DIR_PATTERN, "attempt_dir_path", 4,
                     tt_root, user, job_id, attempt_id);
}

char *get_task_launcher_file(const char* work_dir) {
  return concatenate("%s/%s", "task launcher", 2, work_dir, TASK_SCRIPT);
}

char *get_task_credentials_file(const char* work_dir) {
  return concatenate("%s/%s", "task crednetials", 2, work_dir,
      CREDENTIALS_FILENAME);
}

/**
 * Get the job log directory under the given log_root
 */
char* get_job_log_directory(const char *log_root, const char* jobid) {
  return concatenate("%s/%s", "job log dir", 2, log_root,
                             jobid);
}

/*
 * Get a user subdirectory.
 */
char *get_user_subdirectory(const char *tt_root,
                            const char *user,
                            const char *subdir) {
  char * user_dir = get_user_directory(tt_root, user);
  char * result = concatenate("%s/%s", "user subdir", 2,
                              user_dir, subdir);
  free(user_dir);
  return result;
}

/**
 * Ensure that the given path and all of the parent directories are created
 * with the desired permissions.
 */
int mkdirs(const char* path, mode_t perm) {
  char *buffer = strdup(path);
  char *token;
  int cwd = open("/", O_RDONLY);
  if (cwd == -1) {
    fprintf(LOGFILE, "Can't open / in %s - %s\n", path, strerror(errno));
    free(buffer);
    return -1;
  }
  for(token = strtok(buffer, "/"); token != NULL; token = strtok(NULL, "/")) {
    if (mkdirat(cwd, token, perm) != 0) {
      if (errno != EEXIST) {
        fprintf(LOGFILE, "Can't create directory %s in %s - %s\n", 
                token, path, strerror(errno));
        close(cwd);
        free(buffer);
        return -1;
      }
    }
    int new_dir = openat(cwd, token, O_RDONLY);
    close(cwd);
    cwd = new_dir;
    if (cwd == -1) {
      fprintf(LOGFILE, "Can't open %s in %s - %s\n", token, path, 
              strerror(errno));
      free(buffer);
      return -1;
    }
  }
  free(buffer);
  close(cwd);
  return 0;
}

/**
 * Function to prepare the attempt directories for the task JVM.
 * It creates the task work and log directories.
 */
static int create_attempt_directories(const char* user, const char *job_id, 
					const char *task_id) {
  // create dirs as 0750
  const mode_t perms = S_IRWXU | S_IRGRP | S_IXGRP;
  if (job_id == NULL || task_id == NULL || user == NULL) {
    fprintf(LOGFILE, 
            "Either task_id is null or the user passed is null.\n");
    return -1;
  }

  int result = -1;

  char **local_dir = get_values(TT_SYS_DIR_KEY);

  if (local_dir == NULL) {
    fprintf(LOGFILE, "%s is not configured.\n", TT_SYS_DIR_KEY);
    return -1;
  }

  char **local_dir_ptr;
  for(local_dir_ptr = local_dir; *local_dir_ptr != NULL; ++local_dir_ptr) {
    char *task_dir = get_attempt_work_directory(*local_dir_ptr, user, job_id, 
                                                task_id);
    if (task_dir == NULL) {
      free_values(local_dir);
      return -1;
    }
    if (mkdirs(task_dir, perms) != 0) {
      // continue on to create other task directories
      free(task_dir);
    } else {
      result = 0;
      free(task_dir);
    }
  }
  free_values(local_dir);
  if (result != 0) {
    return result;
  }

  result = -1;
  // also make the directory for the task logs
  char *job_task_name = malloc(strlen(job_id) + strlen(task_id) + 2);
  if (job_task_name == NULL) {
    fprintf(LOGFILE, "Malloc of job task name failed\n");
    result = -1;
  } else {
    sprintf(job_task_name, "%s/%s", job_id, task_id);

    char **log_dir = get_values(TT_LOG_DIR_KEY);
    if (log_dir == NULL) {
      fprintf(LOGFILE, "%s is not configured.\n", TT_LOG_DIR_KEY);
      return -1;
    }

    char **log_dir_ptr;
    for(log_dir_ptr = log_dir; *log_dir_ptr != NULL; ++log_dir_ptr) {
      char *job_log_dir = get_job_log_directory(*log_dir_ptr, job_task_name);
      if (job_log_dir == NULL) {
        free_values(log_dir);
        return -1;
      } else if (mkdirs(job_log_dir, perms) != 0) {
    	free(job_log_dir);
      } else {
    	result = 0;
    	free(job_log_dir);
      }
    }
    free(job_task_name);
    free_values(log_dir);
  }
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
      free(min_uid_str);
      return NULL;
    }
    free(min_uid_str);
  }
  struct passwd *user_info = get_user_info(user);
  if (NULL == user_info) {
    fprintf(LOGFILE, "User %s not found\n", user);
    return NULL;
  }
  if (user_info->pw_uid < min_uid) {
    fprintf(LOGFILE, "Requested user %s has id %d, which is below the "
	    "minimum allowed %d\n", user, user_info->pw_uid, min_uid);
    free(user_info);
    return NULL;
  }
  char **banned_users = get_values(BANNED_USERS_KEY);
  char **banned_user = (banned_users == NULL) ? 
    (char**) DEFAULT_BANNED_USERS : banned_users;
  for(; *banned_user; ++banned_user) {
    if (strcmp(*banned_user, user) == 0) {
      free(user_info);
      fprintf(LOGFILE, "Requested user %s is banned\n", user);
      return NULL;
    }
  }
  if (banned_users != NULL) {
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
 * and with the group set to the task tracker group.
 * return non-0 on failure
 */
int create_directory_for_user(const char* path) {
  // set 2750 permissions and group sticky bit
  mode_t permissions = S_IRWXU | S_IRGRP | S_IXGRP | S_ISGID;
  uid_t user = geteuid();
  gid_t group = getegid();
  int ret = 0;
  ret = change_effective_user(0, tt_gid);
  if (ret == 0) {
    if (0 == mkdir(path, permissions) || EEXIST == errno) {
      // need to reassert the group sticky bit
      if (chmod(path, permissions) != 0) {
        fprintf(LOGFILE, "Can't chmod %s to add the sticky bit - %s\n",
                path, strerror(errno));
        ret = -1;
      } else if (change_owner(path, user, tt_gid) != 0) {
        fprintf(LOGFILE, "Failed to chown %s to %d:%d: %s\n", path, user, tt_gid,
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
    ret = -1;
  }
  return ret;
}

/**
 * Open a file as the tasktracker and return a file descriptor for it.
 * Returns -1 on error
 */
static int open_file_as_task_tracker(const char* filename) {
  uid_t user = geteuid();
  gid_t group = getegid();
  if (change_effective_user(tt_uid, tt_gid) != 0) {
    return -1;
  }
  int result = open(filename, O_RDONLY);
  if (result == -1) {
    fprintf(LOGFILE, "Can't open file %s as task tracker - %s\n", filename,
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
int initialize_user(const char *user) {
  char **local_dir = get_values(TT_SYS_DIR_KEY);
  if (local_dir == NULL) {
    fprintf(LOGFILE, "%s is not configured.\n", TT_SYS_DIR_KEY);
    return INVALID_TT_ROOT;
  }

  char *user_dir;
  char **local_dir_ptr = local_dir;
  int failed = 0;
  for(local_dir_ptr = local_dir; *local_dir_ptr != 0; ++local_dir_ptr) {
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
  free_values(local_dir);
  return failed ? INITIALIZE_USER_FAILED : 0;
}

/**
 * Function to prepare the job directories for the task JVM.
 */
int initialize_job(const char *user, const char *jobid, 
		   const char* nmPrivate_credentials_file, char* const* args) {
  if (jobid == NULL || user == NULL) {
    fprintf(LOGFILE, "Either jobid is null or the user passed is null.\n");
    return INVALID_ARGUMENT_NUMBER;
  }

  // create the user directory on all disks
  int result = initialize_user(user);
  if (result != 0) {
    return result;
  }

  ////////////// create the log directories for the app on all disks
  char **log_roots = get_values(TT_LOG_DIR_KEY);
  if (log_roots == NULL) {
    return INVALID_CONFIG_FILE;
  }
  char **log_root;
  char *any_one_job_log_dir = NULL;
  for(log_root=log_roots; *log_root != NULL; ++log_root) {
    char *job_log_dir = get_job_log_directory(*log_root, jobid);
    if (job_log_dir == NULL) {
      // try the next one
    } else if (create_directory_for_user(job_log_dir) != 0) {
      free(job_log_dir);
      return -1;
    } else if (any_one_job_log_dir == NULL) {
    	any_one_job_log_dir = job_log_dir;
    } else {
      free(job_log_dir);
    }
  }
  free_values(log_roots);
  if (any_one_job_log_dir == NULL) {
    fprintf(LOGFILE, "Did not create any job-log directories\n");
    return -1;
  }
  free(any_one_job_log_dir);
  ////////////// End of creating the log directories for the app on all disks

  // open up the credentials file
  int cred_file = open_file_as_task_tracker(nmPrivate_credentials_file);
  if (cred_file == -1) {
    return -1;
  }

  // give up root privs
  if (change_user(user_detail->pw_uid, user_detail->pw_gid) != 0) {
    return -1;
  }

  // 750
  mode_t permissions = S_IRWXU | S_IRGRP | S_IXGRP;
  char **tt_roots = get_values(TT_SYS_DIR_KEY);

  if (tt_roots == NULL) {
    return INVALID_CONFIG_FILE;
  }

  char **tt_root;
  char *primary_job_dir = NULL;
  for(tt_root=tt_roots; *tt_root != NULL; ++tt_root) {
    char *job_dir = get_job_directory(*tt_root, user, jobid);
    if (job_dir == NULL) {
      // try the next one
    } else if (mkdirs(job_dir, permissions) != 0) {
      free(job_dir);
    } else if (primary_job_dir == NULL) {
      primary_job_dir = job_dir;
    } else {
      free(job_dir);
    }
  }
  free_values(tt_roots);
  if (primary_job_dir == NULL) {
    fprintf(LOGFILE, "Did not create any job directories\n");
    return -1;
  }

  char *nmPrivate_credentials_file_copy = strdup(nmPrivate_credentials_file);
  // TODO: FIXME. The user's copy of creds should go to a path selected by
  // localDirAllocatoir
  char *cred_file_name = concatenate("%s/%s", "cred file", 2,
				   primary_job_dir, basename(nmPrivate_credentials_file_copy));
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
  fclose(stderr);
  if (chdir(primary_job_dir) != 0) {
    fprintf(LOGFILE, "Failed to chdir to job dir - %s\n", strerror(errno));
    return -1;
  }
  execvp(args[0], args);
  fprintf(LOGFILE, "Failure to exec job initialization process - %s\n",
	  strerror(errno));
  return -1;
}

/*
 * Function used to launch a task as the provided user. It does the following :
 * 1) Creates attempt work dir and log dir to be accessible by the child
 * 2) Copies the script file from the TT to the work directory
 * 3) Sets up the environment
 * 4) Does an execlp on the same in order to replace the current image with
 *    task image.
 */
int run_task_as_user(const char *user, const char *job_id, 
                     const char *task_id, const char *work_dir,
                     const char *script_name, const char *cred_file) {
  int exit_code = -1;
  char *script_file_dest = NULL;
  char *cred_file_dest = NULL;
  script_file_dest = get_task_launcher_file(work_dir);
  if (script_file_dest == NULL) {
    exit_code = OUT_OF_MEMORY;
    goto cleanup;
  }
  cred_file_dest = get_task_credentials_file(work_dir);
  if (NULL == cred_file_dest) {
    exit_code = OUT_OF_MEMORY;
    goto cleanup;
  }

  // open launch script
  int task_file_source = open_file_as_task_tracker(script_name);
  if (task_file_source == -1) {
    goto cleanup;
  }

  // open credentials
  int cred_file_source = open_file_as_task_tracker(cred_file);
  if (cred_file_source == -1) {
    goto cleanup;
  }

  // give up root privs
  if (change_user(user_detail->pw_uid, user_detail->pw_gid) != 0) {
    exit_code = SETUID_OPER_FAILED;
    goto cleanup;
  }

  if (create_attempt_directories(user, job_id, task_id) != 0) {
    fprintf(LOGFILE, "Could not create attempt dirs");
    goto cleanup;
  }

  // 700
  if (copy_file(task_file_source, script_name, script_file_dest,S_IRWXU) != 0) {
    goto cleanup;
  }

  // 600
  if (copy_file(cred_file_source, cred_file, cred_file_dest,
        S_IRUSR | S_IWUSR) != 0) {
    goto cleanup;
  }

  fcloseall();
  umask(0027);
  if (chdir(work_dir) != 0) {
    fprintf(LOGFILE, "Can't change directory to %s -%s\n", work_dir,
	    strerror(errno));
    goto cleanup;
  }
  if (execlp(script_file_dest, script_file_dest, NULL) != 0) {
    fprintf(LOGFILE, "Couldn't execute the task jvm file %s - %s", 
            script_file_dest, strerror(errno));
    exit_code = UNABLE_TO_EXECUTE_TASK_SCRIPT;
    goto cleanup;
  }
  exit_code = 0;

 cleanup:
  free(script_file_dest);
  free(cred_file_dest);
  return exit_code;
}

/**
 * Function used to signal a task launched by the user.
 * The function sends appropriate signal to the process group
 * specified by the task_pid.
 */
int signal_user_task(const char *user, int pid, int sig) {
  if(pid <= 0) {
    return INVALID_TASK_PID;
  }

  if (change_user(user_detail->pw_uid, user_detail->pw_gid) != 0) {
    return SETUID_OPER_FAILED;
  }

  //Don't continue if the process-group is not alive anymore.
  int has_group = 1;
  if (kill(-pid,0) < 0) {
    if (kill(pid, 0) < 0) {
      if (errno == ESRCH) {
        return INVALID_TASK_PID;
      }
      fprintf(LOGFILE, "Error signalling task %d with %d - %s\n",
	      pid, sig, strerror(errno));
      return -1;
    } else {
      has_group = 0;
    }
  }

  if (kill((has_group ? -1 : 1) * pid, sig) < 0) {
    if(errno != ESRCH) {
      fprintf(LOGFILE, 
              "Error signalling process group %d with signal %d - %s\n", 
              -pid, sig, strerror(errno));
      return UNABLE_TO_KILL_TASK;
    } else {
      return INVALID_TASK_PID;
    }
  }
  fprintf(LOGFILE, "Killing process %s%d with %d\n",
	  (has_group ? "group " :""), pid, sig);
  return 0;
}

/**
 * Delete a final directory as the task tracker user.
 */
static int rmdir_as_tasktracker(const char* path) {
  int user_uid = geteuid();
  int user_gid = getegid();
  int ret = change_effective_user(tt_uid, tt_gid);
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
            exit_code = -1;
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
          exit_code = -1;
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
    if (exit_code == 0 && ret != 0) {
      fprintf(LOGFILE, "Error in fts_close while deleting %s\n", full_path);
      exit_code = -1;
    }
    if (needs_tt_user) {
      // If the delete failed, try a final rmdir as root on the top level.
      // That handles the case where the top level directory is in a directory
      // that is owned by the task tracker.
      exit_code = rmdir_as_tasktracker(full_path);
    }
    free(paths[0]);
  }
  return exit_code;
}

/**
 * Delete the given directory as the user from each of the tt_root directories
 * user: the user doing the delete
 * subdir: the subdir to delete (if baseDirs is empty, this is treated as
           an absolute path)
 * baseDirs: (optional) the baseDirs where the subdir is located
 */
int delete_as_user(const char *user,
                   const char *subdir,
                   char* const* baseDirs) {
  int ret = 0;

  char** ptr;

  // TODO: No switching user? !!!!
  if (baseDirs == NULL || *baseDirs == NULL) {
    return delete_path(subdir, strlen(subdir) == 0);
  }
  // do the delete
  for(ptr = (char**)baseDirs; *ptr != NULL; ++ptr) {
    char* full_path = concatenate("%s/%s", "user subdir", 2,
                              *ptr, subdir);
    if (full_path == NULL) {
      return -1;
    }
    int this_ret = delete_path(full_path, strlen(subdir) == 0);
    free(full_path);
    // delete as much as we can, but remember the error
    if (this_ret != 0) {
      ret = this_ret;
    }
  }
  return ret;
}
