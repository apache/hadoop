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

//hadoop temp dir root which is configured in secure configuration
const char *mapred_local_dir;

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
#ifdef DEBUG
  fprintf(LOGFILE,"change_user : setting user as %s ", user_detail->pw_name);
#endif
  errno = 0;
  setgid(user_detail->pw_gid);
  if (errno != 0) {
    cleanup();
    return SETUID_OPER_FAILED;
  }

  setegid(user_detail->pw_gid);
  if (errno != 0) {
    cleanup();
    return SETUID_OPER_FAILED;
  }

  setuid(user_detail->pw_uid);
  if (errno != 0) {
    cleanup();
    return SETUID_OPER_FAILED;
  }

  seteuid(user_detail->pw_uid);
  if (errno != 0) {
    cleanup();
    return SETUID_OPER_FAILED;
  }
  return 0;
}

//Function to set the hadoop.temp.dir key from configuration.
//would return -1 if the configuration is not proper.

int get_mapred_local_dir() {

  if (mapred_local_dir == NULL) {
    mapred_local_dir = get_value(TT_SYS_DIR_KEY);
  }

  //after the call it should not be null
  if (mapred_local_dir == NULL) {
    return -1;
  } else {
    return 0;
  }

}
// function to check if the passed tt_root is present in hadoop.tmp.dir
int check_tt_root(const char *tt_root) {
  char *token;
  int found = -1;

  if (tt_root == NULL) {
    return -1;
  }

  if (mapred_local_dir == NULL) {
    if (get_mapred_local_dir() < 0) {
      fprintf(LOGFILE, "invalid hadoop config\n");
      return -1;
    }
  }

  token = strtok((char *) mapred_local_dir, ",");
  if (token == NULL && mapred_local_dir != NULL) {
#ifdef DEBUG
    fprintf(LOGFILE,"Single hadoop.tmp.dir configured");
#endif
    token = (char *)mapred_local_dir;
  }

  while (1) {
    if (strcmp(tt_root, token) == 0) {
      found = 0;
      break;
    }
    token = strtok(NULL, ",");
    if (token == NULL) {
      break;
    }
  }

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
 *Function which would return .pid file path which is used while running
 * and killing of the tasks by the user.
 *
 * check TT_SYS_DIR for pattern
 */
void get_pid_path(const char * jobid, const char * taskid, const char *tt_root,
    char ** pid_path) {

  int str_len = strlen(TT_SYS_DIR) + strlen(jobid) + strlen(taskid) + strlen(
      tt_root);
  *pid_path = NULL;

  if (mapred_local_dir == NULL) {
    if (get_mapred_local_dir() < 0) {
      return;
    }
  }

  *pid_path = (char *) malloc(sizeof(char) * (str_len + 1));

  if (*pid_path == NULL) {
    fprintf(LOGFILE, "unable to allocate memory for pid path\n");
    return;
  }
  memset(*pid_path,'\0',str_len+1);
  snprintf(*pid_path, str_len, TT_SYS_DIR, tt_root, jobid, taskid);
#ifdef DEBUG
  fprintf(LOGFILE, "get_pid_path : pid path = %s\n", *pid_path);
  fflush(LOGFILE);
#endif

}

/*
 * function to provide path to the task file which is created by the tt
 *
 *Check TT_LOCAL_TASK_SCRIPT_PATTERN for pattern
 */
void get_task_file_path(const char * jobid, const char * taskid,
    const char * tt_root, char **task_script_path) {
  *task_script_path = NULL;
  int str_len = strlen(TT_LOCAL_TASK_SCRIPT_PATTERN) + strlen(jobid) + (strlen(
      taskid)) + strlen(tt_root);

  if (mapred_local_dir == NULL) {
    if (get_mapred_local_dir() < 0) {
      return;
    }
  }

  *task_script_path = (char *) malloc(sizeof(char) * (str_len + 1));
  if (*task_script_path == NULL) {
    fprintf(LOGFILE, "Unable to allocate memory for task_script_path \n");
    return;
  }

  memset(*task_script_path,'\0',str_len+1);
  snprintf(*task_script_path, str_len, TT_LOCAL_TASK_SCRIPT_PATTERN, tt_root,
      jobid, taskid);
#ifdef DEBUG
  fprintf(LOGFILE, "get_task_file_path : task script path = %s\n", *task_script_path);
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

/*
 *Function used to launch a task as the provided user.
 * First the function checks if the tt_root passed is found in
 * hadoop.temp.dir
 *
 *Then gets the path to which the task has to write its pid from
 *get_pid_path.
 *
 * THen writes its pid into the file.
 *
 * Then changes the permission of the pid file into 600
 *
 * Then uses get_task_file_path to fetch the task script file path.
 *
 * Does an execlp on the same in order to replace the current image with
 * task image.
 *
 */

int run_task_as_user(const char * user, const char *jobid, const char *taskid,
    const char *tt_root) {
  char *task_script_path = NULL;
  char *pid_path = NULL;
  char *task_script = NULL;
  FILE *file_handle = NULL;
  int exit_code = 0;
  uid_t uid = getuid();

#ifdef DEBUG
  fprintf(LOGFILE,"run_task_as_user : Job id : %s \n", jobid);
  fprintf(LOGFILE,"run_task_as_user : task id : %s \n", taskid);
  fprintf(LOGFILE,"run_task_as_user : tt_root : %s \n", tt_root);
  fflush(LOGFILE);
#endif

  if (check_tt_root(tt_root) < 0) {
    fprintf(LOGFILE, "invalid tt root passed %s\n", tt_root);
    cleanup();
    return INVALID_TT_ROOT;
  }

  get_pid_path(jobid, taskid, tt_root, &pid_path);
  if (pid_path == NULL) {
    fprintf(LOGFILE, "Invalid task-pid path provided");
    cleanup();
    return INVALID_PID_PATH;
  }
  errno = 0;
  file_handle = fopen(pid_path, "w");

  if (file_handle == NULL) {
    fprintf(LOGFILE, "Error opening task-pid file %s :%s\n", pid_path,
        strerror(errno));
    exit_code = UNABLE_TO_OPEN_PID_FILE_WRITE_MODE;
    goto cleanup;
  }

  errno = 0;
  if (fprintf(file_handle, "%d\n", getpid()) < 0) {
    fprintf(LOGFILE, "Error writing to task-pid file :%s\n", strerror(errno));
    exit_code = UNABLE_TO_WRITE_TO_PID_FILE;
    goto cleanup;
  }

  fflush(file_handle);
  fclose(file_handle);
  file_handle = NULL;
  //change the permissions of the file
  errno = 0;
  //setting permission to 600
  if (chmod(pid_path, S_IREAD | S_IWRITE) < 0) {
    fprintf(LOGFILE, "Error changing permission of %s task-pid file : %s",
        pid_path, strerror(errno));
    errno = 0;
    if (remove(pid_path) < 0) {
      fprintf(LOGFILE, "Error deleting %s task-pid file : %s", pid_path,
          strerror(errno));
      exit_code = UNABLE_TO_CHANGE_PERMISSION_AND_DELETE_PID_FILE;
    } else {
      exit_code = UNABLE_TO_CHANGE_PERMISSION_OF_PID_FILE;
    }
    goto cleanup;
  }
  //while checking path make sure the target of the path exists otherwise
  //check_paths would fail always. So write out .pid file then check if
  //it correctly resolves. If not delete the pid file and bail out.
  errno = 0;
  exit_code = check_path(pid_path);
  if(exit_code != 0) {
    remove(pid_path);
    goto cleanup;
  }

  //free pid_t path which is allocated
  free(pid_path);
  pid_path = NULL;

  //change the user
  fcloseall();
  fclose(LOGFILE);
  umask(0);
  if (change_user(user) != 0) {
    exit_code = SETUID_OPER_FAILED;
    goto cleanup;
  }

  //change set the launching process as the session leader.
  if(setsid() < 0) {
    exit_code = SETSID_FAILED;
    goto cleanup;
  }

  get_task_file_path(jobid, taskid, tt_root, &task_script_path);

  if (task_script_path == NULL) {
    exit_code = INVALID_TASK_SCRIPT_PATH;
    goto cleanup;
  }
  //resolve paths.
  errno = 0;
  exit_code = check_path(task_script_path);
  if(exit_code !=0) {
    goto cleanup;
  }
  errno = 0;
  //get stat of the task file.
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
  if (pid_path != NULL) {
    free(pid_path);
  }
  if (task_script_path != NULL) {
    free(task_script_path);
  }
  if (file_handle != NULL) {
    fclose(file_handle);
  }
  // free configurations
  cleanup();
  return exit_code;
}
/**
 * Function used to terminate a task launched by the user.
 *
 * The function first checks if the passed tt-root is found in
 * configured hadoop.temp.dir (which is a list of tt_roots).
 *
 * Then gets the task-pid path using function get_pid_path.
 *
 * reads the task-pid from the file which is mentioned by get_pid_path
 *
 * kills the task by sending SIGTERM to that particular process.
 *
 */

int kill_user_task(const char *user, const char *jobid, const char *taskid,
    const char *tt_root) {
  int pid = 0;
  int i = 0;
  char *pid_path = NULL;
  FILE *file_handle = NULL;
  const char *sleep_interval_char;
  int sleep_interval = 0;
  uid_t uid = getuid();
  int exit_code = 0;
#ifdef DEBUG
  fprintf(LOGFILE,"kill_user_task : Job id : %s \n", jobid);
  fprintf(LOGFILE,"kill_user_task : task id : %s \n", taskid);
  fprintf(LOGFILE,"kill_user_task : tt_root : %s \n", tt_root);
  fflush(LOGFILE);
#endif
  if (check_tt_root(tt_root) < 0) {
    fprintf(LOGFILE, "invalid tt root specified");
    cleanup();
    return INVALID_TT_ROOT;
  }
  get_pid_path(jobid, taskid, tt_root, &pid_path);
  if (pid_path == NULL) {
    cleanup();
    return INVALID_PID_PATH;
  }
  errno = 0;
  exit_code = check_path(pid_path);
  if(exit_code != 0) {
    free(pid_path);
    cleanup();
    return exit_code;
  }
  errno = 0;
  exit_code = check_owner(uid, pid_path);

  if(exit_code != 0) {
    free(pid_path);
    cleanup();
    return exit_code;
  }

#ifdef DEBUG
  fprintf(LOGFILE,"kill_user_task : task-pid path :%s \n",pid_path);
  fflush(LOGFILE);
#endif
  errno = 0;
  file_handle = fopen(pid_path, "r");
  if (file_handle == NULL) {
    fprintf(LOGFILE, "unable to open task-pid file :%s \n", pid_path);
    free(pid_path);
    cleanup();
    return UNABLE_TO_OPEN_PID_FILE_READ_MODE;
  }
  fscanf(file_handle, "%d", &pid);
  fclose(file_handle);
  fclose(LOGFILE);
  free(pid_path);
  if (pid == 0) {
    cleanup();
    return UNABLE_TO_READ_PID;
  }
  if (change_user(user) != 0) {
    cleanup();
    return SETUID_OPER_FAILED;
  }
  //kill the entire session.
  if (kill(-pid, SIGTERM) < 0) {
    fprintf(LOGFILE, "%s\n", strerror(errno));
    cleanup();
    return UNABLE_TO_KILL_TASK;
  }
  //get configured interval time.
  sleep_interval_char = get_value("mapred.tasktracker.tasks.sleeptime-before-sigkill");
  if(sleep_interval_char != NULL) {
    sleep_interval = atoi(sleep_interval_char);
  }
  if(sleep_interval == 0) {
    sleep_interval = 5;
  }
  //sleep for configured interval.
  sleep(sleep_interval);
  //check pid exists
  if(kill(-pid,0) == 0) {
    //if pid present then sigkill it
    if(kill(-pid, SIGKILL) <0) {
      //ignore no such pid present.
      if(errno != ESRCH) {
        //log error ,exit unclean
        cleanup();
        return UNABLE_TO_KILL_TASK;
      }
    }
  }
  cleanup();
  return 0;
}
