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

void open_log_file(const char *log_file) {
  if (log_file == NULL) {
    LOGFILE = stdout;
  } else {
    LOGFILE = fopen(log_file, "a");
    if (LOGFILE == NULL) {
      fprintf(stdout, "Unable to open LOGFILE : %s \n", log_file);
      LOGFILE = stdout;
    }
    if (LOGFILE != stdout) {
      if (chmod(log_file, S_IREAD | S_IEXEC | S_IWRITE | S_IROTH | S_IWOTH
          | S_IRGRP | S_IWGRP) < 0) {
        fprintf(stdout, "Unable to change permission of the log file %s \n",
            log_file);
        fclose(LOGFILE);
        fprintf(stdout, "changing log file to stdout");
        LOGFILE = stdout;
      }
    }
  }
}

void display_usage(FILE *stream) {
  fprintf(stream,
      "Usage: task-controller [-l logfile] user command command-args\n");
}

/**
 * Check the permissions on taskcontroller to make sure that security is
 * promisable. For this, we need task-controller binary to
 *    * be user-owned by root
 *    * be group-owned by a configured special group.
 *    * others do not have write or execute permissions
 *    * be setuid
 */
int check_taskcontroller_permissions(char *executable_file) {

  errno = 0;
  char * resolved_path = (char *) canonicalize_file_name(executable_file);
  if (resolved_path == NULL) {
    fprintf(LOGFILE,
        "Error resolving the canonical name for the executable : %s!",
        strerror(errno));
    return -1;
  }

  struct stat filestat;
  errno = 0;
  if (stat(resolved_path, &filestat) != 0) {
    fprintf(LOGFILE, "Could not stat the executable : %s!.\n", strerror(errno));
    return -1;
  }

  uid_t binary_euid = filestat.st_uid; // Binary's user owner
  gid_t binary_egid = filestat.st_gid; // Binary's group owner

  // Effective uid should be root
  if (binary_euid != 0) {
    fprintf(LOGFILE,
        "The task-controller binary should be user-owned by root.\n");
    return -1;
  }

  // Get the group entry for the special_group
  errno = 0;
  struct group *special_group_entry = getgrgid(binary_egid);
  if (special_group_entry == NULL) {
    fprintf(LOGFILE,
      "Unable to get information for effective group of the binary : %s\n",
      strerror(errno));
    return -1;
  }

  char * binary_group = special_group_entry->gr_name;
  // verify that the group name of the special group 
  // is same as the one in configuration
  if (check_variable_against_config(TT_GROUP_KEY, binary_group) != 0) {
    fprintf(LOGFILE,
      "Group of the binary does not match with that in configuration\n");
    return -1;
  }
  
  // check others do not have write/execute permissions
  if ((filestat.st_mode & S_IWOTH) == S_IWOTH ||
      (filestat.st_mode & S_IXOTH) == S_IXOTH) {
    fprintf(LOGFILE,
      "The task-controller binary should not have write or execute for others.\n");
    return -1;
  }

  // Binary should be setuid executable
  if ((filestat.st_mode & S_ISUID) != S_ISUID) {
     fprintf(LOGFILE,
        "The task-controller binary should be set setuid.\n");
    return -1;
  }
  
  return 0;
}

int main(int argc, char **argv) {
  int command;
  int next_option = 0;
  const char * job_id = NULL;
  const char * task_id = NULL;
  const char * tt_root = NULL;
  const char *log_dir = NULL;
  const char * unique_string = NULL;
  int exit_code = 0;
  const char * task_pid = NULL;
  const char* const short_options = "l:";
  const struct option long_options[] = { { "log", 1, NULL, 'l' }, { NULL, 0,
      NULL, 0 } };

  const char* log_file = NULL;
  char * dir_to_be_deleted = NULL;
  int conf_dir_len = 0;

  char *executable_file = argv[0];
#ifndef HADOOP_CONF_DIR
  conf_dir_len = (strlen(executable_file) - strlen(EXEC_PATTERN)) + 1;
  if (conf_dir_len < 1) {
    // We didn't get an absolute path to our executable_file; bail.
    printf("Cannot find configuration directory.\n");
    printf("This program must be run with its full absolute path.\n");
    return INVALID_CONF_DIR;
  } else {
    hadoop_conf_dir = (char *) malloc (sizeof(char) * conf_dir_len);
    strncpy(hadoop_conf_dir, executable_file,
      (strlen(executable_file) - strlen(EXEC_PATTERN)));
    hadoop_conf_dir[(strlen(executable_file) - strlen(EXEC_PATTERN))] = '\0';
  }
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

  open_log_file(log_file);

  if (check_taskcontroller_permissions(executable_file) != 0) {
    fprintf(LOGFILE, "Invalid permissions on task-controller binary.\n");
    return INVALID_TASKCONTROLLER_PERMISSIONS;
  }

  //Minimum number of arguments required to run the task-controller
  //command-name user command tt-root
  if (argc < 3) {
    display_usage(stdout);
    return INVALID_ARGUMENT_NUMBER;
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

  fprintf(LOGFILE, "main : command provided %d\n",command);
  fprintf(LOGFILE, "main : user is %s\n", user_detail->pw_name);

  switch (command) {
  case INITIALIZE_USER:
    exit_code = initialize_user(user_detail->pw_name);
    break;
  case INITIALIZE_JOB:
    job_id = argv[optind++];
    exit_code = initialize_job(job_id, user_detail->pw_name);
    break;
  case INITIALIZE_DISTRIBUTEDCACHE_FILE:
    tt_root = argv[optind++];
    unique_string = argv[optind++];
    exit_code = initialize_distributed_cache_file(tt_root, unique_string,
        user_detail->pw_name);
    break;
  case LAUNCH_TASK_JVM:
    tt_root = argv[optind++];
    job_id = argv[optind++];
    task_id = argv[optind++];
    exit_code
        = run_task_as_user(user_detail->pw_name, job_id, task_id, tt_root);
    break;
  case INITIALIZE_TASK:
    job_id = argv[optind++];
    task_id = argv[optind++];
    exit_code = initialize_task(job_id, task_id, user_detail->pw_name);
    break;
  case TERMINATE_TASK_JVM:
    task_pid = argv[optind++];
    exit_code = kill_user_task(user_detail->pw_name, task_pid, SIGTERM);
    break;
  case KILL_TASK_JVM:
    task_pid = argv[optind++];
    exit_code = kill_user_task(user_detail->pw_name, task_pid, SIGKILL);
    break;
  case RUN_DEBUG_SCRIPT:
    tt_root = argv[optind++];
    job_id = argv[optind++];
    task_id = argv[optind++];
    exit_code
        = run_debug_script_as_user(user_detail->pw_name, job_id, task_id, tt_root);
    break;
  case SIGQUIT_TASK_JVM:
    task_pid = argv[optind++];
    exit_code = kill_user_task(user_detail->pw_name, task_pid, SIGQUIT);
    break;
  case ENABLE_TASK_FOR_CLEANUP:
    tt_root = argv[optind++];
    job_id = argv[optind++];
    dir_to_be_deleted = argv[optind++];
    exit_code = enable_task_for_cleanup(tt_root, user_detail->pw_name, job_id,
                                        dir_to_be_deleted);
    break;
  case ENABLE_JOB_FOR_CLEANUP:
    tt_root = argv[optind++];
    job_id = argv[optind++];
    exit_code = enable_job_for_cleanup(tt_root, user_detail->pw_name, job_id);
    break;
  default:
    exit_code = INVALID_COMMAND_PROVIDED;
  }
  fflush(LOGFILE);
  fclose(LOGFILE);
  return exit_code;
}
