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
#include<grp.h>
#include <fts.h>

#include "configuration.h"

//command definitions
enum command {
  LAUNCH_TASK_JVM,
  TERMINATE_TASK_JVM,
  KILL_TASK_JVM,
  ENABLE_TASK_FOR_CLEANUP
};

enum errorcodes {
  INVALID_ARGUMENT_NUMBER = 1,
  INVALID_USER_NAME, //2
  INVALID_COMMAND_PROVIDED, //3
  SUPER_USER_NOT_ALLOWED_TO_RUN_TASKS, //4
  INVALID_TT_ROOT, //5
  SETUID_OPER_FAILED, //6
  INVALID_TASK_SCRIPT_PATH, //7
  UNABLE_TO_EXECUTE_TASK_SCRIPT, //8
  UNABLE_TO_KILL_TASK, //9
  INVALID_PROCESS_LAUNCHING_TASKCONTROLLER, //10
  INVALID_TASK_PID, //11
  ERROR_RESOLVING_FILE_PATH, //12
  RELATIVE_PATH_COMPONENTS_IN_FILE_PATH, //13
  UNABLE_TO_STAT_FILE, //14
  FILE_NOT_OWNED_BY_TASKTRACKER, //15
  UNABLE_TO_BUILD_PATH //16
};


#define TT_LOCAL_TASK_SCRIPT_PATTERN "%s/taskTracker/jobcache/%s/%s/taskjvm.sh"

#define TT_LOCAL_TASK_DIR_PATTERN    "%s/taskTracker/jobcache/%s/%s"

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

int kill_user_task(const char *user, const char *task_pid, int sig);

int enable_task_for_cleanup(char * base_path, const char *user,
 const char *jobid, const char *dir_to_be_deleted);

int get_user_details(const char *user);
