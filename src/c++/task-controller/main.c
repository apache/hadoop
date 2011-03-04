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


int main(int argc, char **argv) {
  int command;
  int next_option = 0;
  const char * job_id = NULL;
  const char * task_id = NULL;
  const char * dir_to_be_deleted = NULL;
  const char * tt_root = NULL;
  int exit_code = 0;
  const char * task_pid = NULL;
  const char* const short_options = "l:";
  const struct option long_options[] = { { "log", 1, NULL, 'l' }, { NULL, 0,
      NULL, 0 } };

  const char* log_file = NULL;
  char * base_path = NULL;

  //Minimum number of arguments required to run the task-controller
  //command-name user command tt-root
  if (argc < 3) {
    display_usage(stderr);
    return INVALID_ARGUMENT_NUMBER;
  }

#ifndef HADOOP_CONF_DIR
  hadoop_conf_dir = (char *) malloc (sizeof(char) *
      (strlen(argv[0]) - strlen(EXEC_PATTERN)) + 1);
  strncpy(hadoop_conf_dir,argv[0],(strlen(argv[0]) - strlen(EXEC_PATTERN)));
  hadoop_conf_dir[(strlen(argv[0]) - strlen(EXEC_PATTERN))] = '\0';
#endif
  do {
    next_option = getopt_long(argc, argv, short_options, long_options, NULL);
    switch (next_option) {
    case 'l':
      log_file = optarg;
    default:
      break;
    }
  } while (next_option != -1);
  if (log_file == NULL) {
    LOGFILE = stderr;
  } else {
    LOGFILE = fopen(log_file, "a");
    if (LOGFILE == NULL) {
      fprintf(stderr, "Unable to open LOGFILE : %s \n", log_file);
      LOGFILE = stderr;
    }
    if (LOGFILE != stderr) {
      if (chmod(log_file, S_IREAD | S_IEXEC | S_IWRITE | S_IROTH | S_IWOTH
          | S_IRGRP | S_IWGRP) < 0) {
        fprintf(stderr, "Unable to change permission of the log file %s \n",
            log_file);
        fprintf(stderr, "changing log file to stderr");
        LOGFILE = stderr;
      }
    }
  }
  //checks done for user name
  //checks done if the user is root or not.
  if (argv[optind] == NULL) {
    fprintf(LOGFILE, "Invalid user name \n");
    return INVALID_USER_NAME;
  }
  if (get_user_details(argv[optind]) != 0) {
    return INVALID_USER_NAME;
  }
  //implicit conversion to int instead of __gid_t and __uid_t
  if (user_detail->pw_gid == 0 || user_detail->pw_uid == 0) {
    fprintf(LOGFILE, "Cannot run tasks as super user\n");
    return SUPER_USER_NOT_ALLOWED_TO_RUN_TASKS;
  }
  optind = optind + 1;
  command = atoi(argv[optind++]);
#ifdef DEBUG
  fprintf(LOGFILE, "main : command provided %d\n",command);
  fprintf(LOGFILE, "main : user is %s\n", user_detail->pw_name);
#endif
  switch (command) {
  case LAUNCH_TASK_JVM:
    tt_root = argv[optind++];
    job_id = argv[optind++];
    task_id = argv[optind++];
    exit_code
        = run_task_as_user(user_detail->pw_name, job_id, task_id, tt_root);
    break;
  case TERMINATE_TASK_JVM:
    task_pid = argv[optind++];
    exit_code = kill_user_task(user_detail->pw_name, task_pid, SIGTERM);
    break;
  case KILL_TASK_JVM:
    task_pid = argv[optind++];
    exit_code = kill_user_task(user_detail->pw_name, task_pid, SIGKILL);
    break;
  case ENABLE_TASK_FOR_CLEANUP:
    base_path = argv[optind++];
    job_id = argv[optind++];
    dir_to_be_deleted = argv[optind++];
    exit_code = enable_task_for_cleanup(base_path, user_detail->pw_name, job_id,
                                        dir_to_be_deleted);
    break;
  default:
    exit_code = INVALID_COMMAND_PROVIDED;
  }
  fflush(LOGFILE);
  fclose(LOGFILE);
  return exit_code;
}
