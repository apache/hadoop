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
#include "task-controller.h"

//struct to store the user details
struct passwd *user_detail = NULL;

//LOGFILE
FILE *LOGFILE;

//placeholder for global cleanup operations
void cleanup() {
  free_configurations();
}

//change the user to passed user for executing/killing tasks
int change_user(const char * user) {
  if (get_user_details(user) < 0) {
    return -1;
  }

  if(initgroups(user_detail->pw_name, user_detail->pw_gid) != 0) {
	  cleanup();
	  return SETUID_OPER_FAILED;
  }

  errno = 0;

  setgid(user_detail->pw_gid);
  if (errno != 0) {
    fprintf(LOGFILE, "unable to setgid : %s\n", strerror(errno));
    cleanup();
    return SETUID_OPER_FAILED;
  }

  setegid(user_detail->pw_gid);
  if (errno != 0) {
    fprintf(LOGFILE, "unable to setegid : %s\n", strerror(errno));
    cleanup();
    return SETUID_OPER_FAILED;
  }

  setuid(user_detail->pw_uid);
  if (errno != 0) {
    fprintf(LOGFILE, "unable to setuid : %s\n", strerror(errno));
    cleanup();
    return SETUID_OPER_FAILED;
  }

  seteuid(user_detail->pw_uid);
  if (errno != 0) {
    fprintf(LOGFILE, "unable to seteuid : %s\n", strerror(errno));
    cleanup();
    return SETUID_OPER_FAILED;
  }
  return 0;
}

// function to check if the passed tt_root is present in hadoop.tmp.dir
int check_tt_root(const char *tt_root) {
  char ** mapred_local_dir;
  char ** iter;
  int found = -1;

  if (tt_root == NULL) {
    return -1;
  }

  mapred_local_dir = (char **)get_values(TT_SYS_DIR_KEY);
  iter = mapred_local_dir;

  if (mapred_local_dir == NULL) {
    return -1;
  }

  while(*iter != NULL) {
    if(strcmp(*iter, tt_root) == 0) {
      found = 0;
      break;
    }
    ++iter; 
  }
  free(mapred_local_dir);
  return found;
}

/**
 * Function to check if the constructed path and absolute
 * path resolve to one and same.
 */

int check_path_for_relative_components(char *path) {
  char * resolved_path = (char *) canonicalize_file_name(path);
  if(resolved_path == NULL) {
    fprintf(LOGFILE, "Error resolving the path: %s. Passed path: %s\n",
          strerror(errno), path);
    return ERROR_RESOLVING_FILE_PATH;
  }
  if(strcmp(resolved_path, path) !=0) {
    fprintf(LOGFILE,
            "Relative path components in the path: %s. Resolved path: %s\n",
            path, resolved_path);
    free(resolved_path);
    return RELATIVE_PATH_COMPONENTS_IN_FILE_PATH;
  }
  free(resolved_path);
  return 0;
}
/**
 * Function to check if a user actually owns the file.
 */
int check_owner(uid_t uid, char *path) {
  struct stat filestat;
  if(stat(path, &filestat)!=0) {
    return UNABLE_TO_STAT_FILE;
  }
  //check owner.
  if(uid != filestat.st_uid){
    return FILE_NOT_OWNED_BY_TASKTRACKER;
  }
  return 0;
}

/*
 * function to provide path to the task file which is created by the tt
 *
 *Check TT_LOCAL_TASK_SCRIPT_PATTERN for pattern
 */
void get_task_file_path(const char * jobid, const char * taskid,
    const char * tt_root, char **task_script_path) {
  const char ** mapred_local_dir = get_values(TT_SYS_DIR_KEY);
  *task_script_path = NULL;
  int str_len = strlen(TT_LOCAL_TASK_SCRIPT_PATTERN) + strlen(jobid) + (strlen(
      taskid)) + strlen(tt_root);

  if (mapred_local_dir == NULL) {
    return;
  }

  *task_script_path = (char *) malloc(sizeof(char) * (str_len + 1));
  if (*task_script_path == NULL) {
    fprintf(LOGFILE, "Unable to allocate memory for task_script_path \n");
    free(mapred_local_dir);
    return;
  }

  memset(*task_script_path,'\0',str_len+1);
  snprintf(*task_script_path, str_len, TT_LOCAL_TASK_SCRIPT_PATTERN, tt_root,
      jobid, taskid);
#ifdef DEBUG
  fprintf(LOGFILE, "get_task_file_path : task script path = %s\n", *task_script_path);
  fflush(LOGFILE);
#endif
  free(mapred_local_dir);
}

/*
 * Builds the full path of the dir(localTaskDir or localWorkDir)
 * tt_root : is the base path(i.e. mapred-local-dir) sent to task-controller
 * dir_to_be_deleted : is either taskDir($taskId) OR taskWorkDir($taskId/work)
 *
 * Check TT_LOCAL_TASK_DIR_PATTERN for pattern
 */
void get_task_dir_path(const char * tt_root, const char * jobid,
                       const char * dir_to_be_deleted, char **task_dir_path) {
  *task_dir_path = NULL;
  int str_len = strlen(TT_LOCAL_TASK_DIR_PATTERN) + strlen(jobid) + strlen(
      dir_to_be_deleted) + strlen(tt_root);

  *task_dir_path = (char *) malloc(sizeof(char) * (str_len + 1));
  if (*task_dir_path == NULL) {
    fprintf(LOGFILE, "Unable to allocate memory for task_dir_path \n");
    return;
  }

  memset(*task_dir_path,'\0',str_len+1);
  snprintf(*task_dir_path, str_len, TT_LOCAL_TASK_DIR_PATTERN, tt_root,
           jobid, dir_to_be_deleted);
#ifdef DEBUG
  fprintf(LOGFILE, "get_task_dir_path : task dir path = %s\n", *task_dir_path);
  fflush(LOGFILE);
#endif
}

//end of private functions
void display_usage(FILE *stream) {
  fprintf(stream,
      "Usage: task-controller [-l logfile] user command command-args\n");
}

//function used to populate and user_details structure.

int get_user_details(const char *user) {
  if (user_detail == NULL) {
    user_detail = getpwnam(user);
    if (user_detail == NULL) {
      fprintf(LOGFILE, "Invalid user\n");
      return -1;
    }
  }
  return 0;
}

/**
 * Compare ownership of a file with the given ids.
 */
int compare_ownership(uid_t uid, gid_t gid, char *path) {
  struct stat filestat;
  if (stat(path, &filestat) != 0) {
    return UNABLE_TO_STAT_FILE;
  }
  if (uid == filestat.st_uid && gid == filestat.st_gid) {
    return 0;
  }
  return 1;
}

/*
 * Function to check if the TaskTracker actually owns the file.
  */
int check_ownership(char *path) {
  struct stat filestat;
  if (stat(path, &filestat) != 0) {
    return UNABLE_TO_STAT_FILE;
  }
  // check user/group. User should be TaskTracker user, group can either be
  // TaskTracker's primary group or the special group to which binary's
  // permissions are set.
  if (getuid() != filestat.st_uid || (getgid() != filestat.st_gid && getegid()
      != filestat.st_gid)) {
    return FILE_NOT_OWNED_BY_TASKTRACKER;
  }
  return 0;
}

/**
 * Function to change the owner/group of a given path.
 */
static int change_owner(const char *path, uid_t uid, gid_t gid) {
  int exit_code = chown(path, uid, gid);
  if (exit_code != 0) {
    fprintf(LOGFILE, "chown %d:%d for path %s failed: %s.\n", uid, gid, path,
        strerror(errno));
  }
  return exit_code;
}

/**
 * Function to change the mode of a given path.
 */
static int change_mode(const char *path, mode_t mode) {
  int exit_code = chmod(path, mode);
  if (exit_code != 0) {
    fprintf(LOGFILE, "chmod %d of path %s failed: %s.\n", mode, path,
        strerror(errno));
  }
  return exit_code;
}

/**
 * Function to change permissions of the given path. It does the following
 * recursively:
 *    1) changes the owner/group of the paths to the passed owner/group
 *    2) changes the file permission to the passed file_mode and directory
 *       permission to the passed dir_mode
 *
 * should_check_ownership : boolean to enable checking of ownership of each path
 */
static int secure_path(const char *path, uid_t uid, gid_t gid,
    mode_t file_mode, mode_t dir_mode, int should_check_ownership) {
  FTS *tree = NULL; // the file hierarchy
  FTSENT *entry = NULL; // a file in the hierarchy
  char *paths[] = { (char *) path };
  int process_path = 0;
  int dir = 0;
  int error_code = 0;
  int done = 0;

  // Get physical locations and don't resolve the symlinks.
  // Don't change directory while walking the directory.
  int ftsoptions = FTS_PHYSICAL | FTS_NOCHDIR;

  tree = fts_open(paths, ftsoptions, NULL);
  if (tree == NULL) {
    fprintf(LOGFILE,
        "Cannot open file traversal structure for the path %s:%s.\n", path,
        strerror(errno));
    return -1;
  }

  while (((entry = fts_read(tree)) != NULL) && !done) {
    dir = 0;
    switch (entry->fts_info) {
    case FTS_D:
      // A directory being visited in pre-order.
      // We change ownership of directories in post-order.
      // so ignore the pre-order visit.
      process_path = 0;
      break;
    case FTS_DC:
      // A directory that causes a cycle in the tree
      // We don't expect cycles, ignore.
      process_path = 0;
      break;
    case FTS_DNR:
      // A directory which cannot be read
      // Ignore and set error code.
      process_path = 0;
      error_code = -1;
      break;
    case FTS_DOT:
      // "."  or ".."
      process_path = 0;
      break;
    case FTS_F:
      // A regular file
      process_path = 1;
      break;
    case FTS_DP:
      // A directory being visited in post-order
      if (entry->fts_level == 0) {
        // root directory. Done with traversing.
        done = 1;
      }
      process_path = 1;
      dir = 1;
      break;
    case FTS_SL:
      // A symbolic link
      process_path = 1;
      break;
    case FTS_SLNONE:
      // A symbolic link with a nonexistent target
      process_path = 1;
      break;
    case FTS_NS:
      // A  file for which no stat(2) information was available
      // Ignore and set error code
      process_path = 0;
      error_code = -1;
      break;
    case FTS_ERR:
      // An error return. Ignore and set error code.
      process_path = 0;
      error_code = -1;
      break;
    case FTS_DEFAULT:
      // File that doesn't belong to any of the above type. Ignore.
      process_path = 0;
      break;
    default:
      // None of the above. Ignore and set error code
      process_path = 0;
      error_code = -1;
    }

    if (error_code != 0) {
      break;
    }
    if (!process_path) {
      continue;
    }
    if (should_check_ownership &&
        (compare_ownership(uid, gid, entry->fts_path) == 0)) {
      // already set proper permissions.
      // This might happen with distributed cache.
#ifdef DEBUG
      fprintf(
          LOGFILE,
          "already has private permissions. Not trying to change again for %s",
          entry->fts_path);
#endif
      continue;
    }

    if (should_check_ownership && (check_ownership(entry->fts_path) != 0)) {
      fprintf(LOGFILE,
          "Invalid file path. %s not user/group owned by the tasktracker.\n",
          entry->fts_path);
      error_code = -1;
    } else if (change_owner(entry->fts_path, uid, gid) != 0) {
      fprintf(LOGFILE, "couldn't change the ownership of %s\n",
          entry->fts_path);
      error_code = -3;
    } else if (change_mode(entry->fts_path, (dir ? dir_mode : file_mode)) != 0) {
      fprintf(LOGFILE, "couldn't change the permissions of %s\n",
          entry->fts_path);
      error_code = -3;
    }
  }
  if (fts_close(tree) != 0) {
    fprintf(LOGFILE, "couldn't close file traversal structure:%s.\n",
        strerror(errno));
  }
  return error_code;
}

/*
 *Function used to launch a task as the provided user.
 * First the function checks if the tt_root passed is found in
 * hadoop.temp.dir
 * Uses get_task_file_path to fetch the task script file path.
 * Does an execlp on the same in order to replace the current image with
 * task image.
 */

int run_task_as_user(const char * user, const char *jobid, const char *taskid,
    const char *tt_root) {
  char *task_script_path = NULL;
  int exit_code = 0;
  uid_t uid = getuid();

  if(jobid == NULL || taskid == NULL) {
    return INVALID_ARGUMENT_NUMBER;
  }

#ifdef DEBUG
  fprintf(LOGFILE,"run_task_as_user : Job id : %s \n", jobid);
  fprintf(LOGFILE,"run_task_as_user : task id : %s \n", taskid);
  fprintf(LOGFILE,"run_task_as_user : tt_root : %s \n", tt_root);
  fflush(LOGFILE);
#endif
  //Check tt_root before switching the user, as reading configuration
  //file requires privileged access.
  if (check_tt_root(tt_root) < 0) {
    fprintf(LOGFILE, "invalid tt root passed %s\n", tt_root);
    cleanup();
    return INVALID_TT_ROOT;
  }

  //change the user
  fclose(LOGFILE);
  fcloseall();
  umask(0);
  if (change_user(user) != 0) {
    cleanup();
    return SETUID_OPER_FAILED;
  }

  get_task_file_path(jobid, taskid, tt_root, &task_script_path);
  if (task_script_path == NULL) {
    cleanup();
    return INVALID_TASK_SCRIPT_PATH;
  }
  errno = 0;
  exit_code = check_path_for_relative_components(task_script_path);
  if(exit_code != 0) {
    goto cleanup;
  }
  errno = 0;
  exit_code = check_owner(uid, task_script_path);
  if(exit_code != 0) {
    goto cleanup;
  }
  errno = 0;
  cleanup();
  execlp(task_script_path, task_script_path, NULL);
  if (errno != 0) {
    free(task_script_path);
    exit_code = UNABLE_TO_EXECUTE_TASK_SCRIPT;
  }

  return exit_code;

cleanup:
  if (task_script_path != NULL) {
    free(task_script_path);
  }
  // free configurations
  cleanup();
  return exit_code;
}

/**
 * Function used to terminate/kill a task launched by the user.
 * The function sends appropriate signal to the process group
 * specified by the task_pid.
 */

int kill_user_task(const char *user, const char *task_pid, int sig) {
  int pid = 0;

  if(task_pid == NULL) {
    return INVALID_ARGUMENT_NUMBER;
  }
  pid = atoi(task_pid);

  if(pid <= 0) {
    return INVALID_TASK_PID;
  }
  fclose(LOGFILE);
  fcloseall();
  if (change_user(user) != 0) {
    cleanup();
    return SETUID_OPER_FAILED;
  }

  //Don't continue if the process-group is not alive anymore.
  if(kill(-pid,0) < 0) {
    errno = 0;
    return 0;
  }

  if (kill(-pid, sig) < 0) {
    if(errno != ESRCH) {
      fprintf(LOGFILE, "Error is %s\n", strerror(errno));
      cleanup();
      return UNABLE_TO_KILL_TASK;
    }
    errno = 0;
  }
  cleanup();
  return 0;
}

/**
 * Enables the path for deletion by changing the owner, group and permissions
 * of the specified path and all the files/directories in the path recursively.
 *     *  sudo chown user:mapred -R full_path
 *     *  sudo chmod 2777 -R full_path
 * Before changing permissions, makes sure that the given path doesn't contain
 * any relative components.
 * tt_root : is the base path(i.e. mapred-local-dir) sent to task-controller
 * dir_to_be_deleted : is either taskDir OR taskWorkDir that is to be deleted
 */
int enable_task_for_cleanup(char *tt_root, const char *user,
           const char *jobid, const char *dir_to_be_deleted) {
  int exit_code = 0;
  gid_t tasktracker_gid = getegid(); // the group permissions of the binary.

  char * full_path = NULL;
  if (check_tt_root(tt_root) < 0) {
    fprintf(LOGFILE, "invalid tt root passed %s\n", tt_root);
    cleanup();
    return INVALID_TT_ROOT;
  }

  get_task_dir_path(tt_root, jobid, dir_to_be_deleted, &full_path);
  if (full_path == NULL) {
    fprintf(LOGFILE, 
      "Could not build the full path. Not deleting the dir %s\n",
      dir_to_be_deleted);
    exit_code = UNABLE_TO_BUILD_PATH; // may be malloc failed
  }
     // Make sure that the path given is not having any relative components
  else if ((exit_code = check_path_for_relative_components(full_path)) != 0) {
    fprintf(LOGFILE, 
         "Not changing permissions as the path contains relative components.\n",
         full_path);
  }
  else if (get_user_details(user) < 0) {
    fprintf(LOGFILE, "Couldn't get the user details of %s.\n", user);
    exit_code = INVALID_USER_NAME;
  }
  else if (exit_code = secure_path(full_path, user_detail->pw_uid,
               tasktracker_gid, S_IRWXU | S_IRWXG | S_IRWXO,
               S_ISGID | S_IRWXU | S_IRWXG | S_IRWXO, 0) != 0) {
    // No setgid on files and setgid on dirs, 777.
    // set 777 permissions for user, TTgroup for all files/directories in
    // 'full_path' recursively sothat deletion of path by TaskTracker succeeds.

    fprintf(LOGFILE, "Failed to set permissions for %s\n", full_path);
  }

  if (full_path != NULL) {
    free(full_path);
  }
  // free configurations
  cleanup();
  return exit_code;
}
