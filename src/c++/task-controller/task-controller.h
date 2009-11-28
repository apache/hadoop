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
#include <dirent.h>
#include <fcntl.h>
#include <fts.h>

#include "configuration.h"

//command definitions
enum command {
  INITIALIZE_USER,
  INITIALIZE_JOB,
  INITIALIZE_DISTRIBUTEDCACHE,
  LAUNCH_TASK_JVM,
  INITIALIZE_TASK,
  TERMINATE_TASK_JVM,
  KILL_TASK_JVM,
  RUN_DEBUG_SCRIPT,
};

enum errorcodes {
  INVALID_ARGUMENT_NUMBER = 1,
  INVALID_USER_NAME, //2
  INVALID_COMMAND_PROVIDED, //3
  SUPER_USER_NOT_ALLOWED_TO_RUN_TASKS, //4
  INVALID_TT_ROOT, //5
  SETUID_OPER_FAILED, //6
  UNABLE_TO_EXECUTE_TASK_SCRIPT, //7
  UNABLE_TO_KILL_TASK, //8
  INVALID_TASK_PID, //9
  ERROR_RESOLVING_FILE_PATH, //10
  RELATIVE_PATH_COMPONENTS_IN_FILE_PATH, //11
  UNABLE_TO_STAT_FILE, //12
  FILE_NOT_OWNED_BY_TASKTRACKER, //13
  PREPARE_ATTEMPT_DIRECTORIES_FAILED, //14
  INITIALIZE_JOB_FAILED, //15
  PREPARE_TASK_LOGS_FAILED, //16
  INVALID_TT_LOG_DIR, //17
  OUT_OF_MEMORY, //18
  INITIALIZE_DISTCACHE_FAILED, //19
  INITIALIZE_USER_FAILED, //20
  UNABLE_TO_EXECUTE_DEBUG_SCRIPT, //21
};

#define USER_DIR_PATTERN "%s/taskTracker/%s"

#define TT_JOB_DIR_PATTERN USER_DIR_PATTERN"/jobcache/%s"

#define USER_DISTRIBUTED_CACHE_DIR_PATTERN USER_DIR_PATTERN"/distcache"

#define JOB_DIR_TO_JOB_WORK_PATTERN "%s/work"

#define JOB_DIR_TO_ATTEMPT_DIR_PATTERN "%s/%s"

#define ATTEMPT_LOG_DIR_PATTERN "%s/userlogs/%s"

#define TASK_SCRIPT_PATTERN "%s/%s/taskjvm.sh"

#define TT_SYS_DIR_KEY "mapreduce.cluster.local.dir"

#define TT_LOG_DIR_KEY "hadoop.log.dir"

#ifndef HADOOP_CONF_DIR
  #define EXEC_PATTERN "/bin/task-controller"
  extern char * hadoop_conf_dir;
#endif

extern struct passwd *user_detail;

extern FILE *LOGFILE;

int run_task_as_user(const char * user, const char *jobid, const char *taskid,
    const char *tt_root);

int run_debug_script_as_user(const char * user, const char *jobid, const char *taskid,
    const char *tt_root);

int initialize_user(const char *user);

int initialize_task(const char *jobid, const char *taskid, const char *user);

int initialize_job(const char *jobid, const char *user);

int initialize_distributed_cache(const char *user);

int kill_user_task(const char *user, const char *task_pid, int sig);

int prepare_attempt_directory(const char *attempt_dir, const char *user);

// The following functions are exposed for testing

int check_variable_against_config(const char *config_key,
    const char *passed_value);

int get_user_details(const char *user);

char *get_task_launcher_file(const char *job_dir, const char *attempt_dir);
