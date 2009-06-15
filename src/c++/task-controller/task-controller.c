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
  int found = -1;

  if (tt_root == NULL) {
    return -1;
  }

  mapred_local_dir = (char **)get_values(TT_SYS_DIR_KEY);

  if (mapred_local_dir == NULL) {
    return -1;
  }

  while(*mapred_local_dir != NULL) {
    if(strcmp(*mapred_local_dir,tt_root) == 0) {
      found = 0;
      break;
    }
  }
  free(mapred_local_dir);
  return found;
}

/**
 * Function to check if the constructed path and absolute
 * path resolve to one and same.
 */

int check_path(char *path) {
  char * resolved_path = (char *) canonicalize_file_name(path);
  if(resolved_path == NULL) {
    return ERROR_RESOLVING_FILE_PATH;
  }
  if(strcmp(resolved_path, path) !=0) {
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
  exit_code = check_path(task_script_path);
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

