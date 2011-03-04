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
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <pwd.h>
#include <assert.h>
#include <getopt.h>
#include <sys/stat.h>
#include <sys/signal.h>
#include <getopt.h>
#include <grp.h>
#include "configuration.h"

//command definitions
enum command {
  RUN_TASK,
  KILL_TASK
};

enum errorcodes {
  INVALID_ARGUMENT_NUMBER = 1,
  INVALID_USER_NAME,
  INVALID_COMMAND_PROVIDED,
  SUPER_USER_NOT_ALLOWED_TO_RUN_TASKS,
  OUT_OF_MEMORY,
  INVALID_TT_ROOT,
  INVALID_PID_PATH,
  UNABLE_TO_OPEN_PID_FILE_WRITE_MODE,
  UNABLE_TO_OPEN_PID_FILE_READ_MODE,
  UNABLE_TO_WRITE_TO_PID_FILE,
  UNABLE_TO_CHANGE_PERMISSION_OF_PID_FILE,
  UNABLE_TO_CHANGE_PERMISSION_AND_DELETE_PID_FILE,
  SETUID_OPER_FAILED,
  INVALID_TASK_SCRIPT_PATH,
  UNABLE_TO_EXECUTE_TASK_SCRIPT,
  UNABLE_TO_READ_PID,
  UNABLE_TO_KILL_TASK,
  UNABLE_TO_FIND_PARENT_PID_FILE,
  UNABLE_TO_READ_PARENT_PID,
  SETSID_FAILED,
  ERROR_RESOLVING_FILE_PATH,
  RELATIVE_PATH_COMPONENTS_IN_FILE_PATH,
  UNABLE_TO_STAT_FILE,
  FILE_NOT_OWNED_BY_TASKTRACKER,
  UNABLE_TO_CHANGE_OWNERSHIP_OF_PID_FILE,
  UNABLE_TO_CHANGE_OWNERSHIP_AND_DELETE_PID_FILE
};


#define TT_PID_PATTERN "%s/hadoop-%s-tasktracker.pid"

#define TT_LOCAL_TASK_SCRIPT_PATTERN "%s/taskTracker/jobcache/%s/%s/taskjvm.sh"

#define TT_SYS_DIR "%s/taskTracker/jobcache/%s/%s/.pid"

#define TT_SYS_DIR_KEY "mapred.local.dir"

#define MAX_ITEMS 10

#ifndef HADOOP_CONF_DIR
  #define EXEC_PATTERN "/bin/task-controller"
  extern char * hadoop_conf_dir;
#endif

extern struct passwd *user_detail;

extern FILE *LOGFILE;

void display_usage(FILE *stream);

int run_task_as_user(const char * user, const char *jobid, const char *taskid, const char *tt_root);

int verify_parent();

int kill_user_task(const char *user, const char *jobid, const char *taskid, const char *tt_root);

int get_user_details(const char *user);
