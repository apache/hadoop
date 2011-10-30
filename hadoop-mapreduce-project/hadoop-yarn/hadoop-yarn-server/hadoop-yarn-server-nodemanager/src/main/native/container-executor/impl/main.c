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

#include <errno.h>
#include <grp.h>
#include <limits.h>
#include <unistd.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#define _STRINGIFY(X) #X
#define STRINGIFY(X) _STRINGIFY(X)
#define CONF_FILENAME "container-executor.cfg"

#ifndef HADOOP_CONF_DIR
  #error HADOOP_CONF_DIR must be defined
#endif

void display_usage(FILE *stream) {
  fprintf(stream,
      "Usage: container-executor user command command-args\n");
  fprintf(stream, "Commands:\n");
  fprintf(stream, "   initialize container: %2d appid tokens cmd app...\n",
	  INITIALIZE_CONTAINER);
  fprintf(stream, "   launch container:    %2d appid containerid workdir container-script tokens\n",
	  LAUNCH_CONTAINER);
  fprintf(stream, "   signal container:    %2d container-pid signal\n",
	  SIGNAL_CONTAINER);
  fprintf(stream, "   delete as user: %2d relative-path\n",
	  DELETE_AS_USER);
}

int main(int argc, char **argv) {
  //Minimum number of arguments required to run the container-executor
  if (argc < 4) {
    display_usage(stdout);
    return INVALID_ARGUMENT_NUMBER;
  }

  LOGFILE = stdout;
  ERRORFILE = stderr;
  int command;
  const char * app_id = NULL;
  const char * container_id = NULL;
  const char * cred_file = NULL;
  const char * script_file = NULL;
  const char * current_dir = NULL;
  const char * pid_file = NULL;

  int exit_code = 0;

  char * dir_to_be_deleted = NULL;

  char *executable_file = get_executable();

  char *orig_conf_file = STRINGIFY(HADOOP_CONF_DIR) "/" CONF_FILENAME;
  char *conf_file = realpath(orig_conf_file, NULL);

  if (conf_file == NULL) {
    fprintf(ERRORFILE, "Configuration file %s not found.\n", orig_conf_file);
    exit(INVALID_CONFIG_FILE);
  }
  if (check_configuration_permissions(conf_file) != 0) {
    exit(INVALID_CONFIG_FILE);
  }
  read_config(conf_file);
  free(conf_file);

  // look up the node manager group in the config file
  char *nm_group = get_value(NM_GROUP_KEY);
  if (nm_group == NULL) {
    fprintf(ERRORFILE, "Can't get configured value for %s.\n", NM_GROUP_KEY);
    exit(INVALID_CONFIG_FILE);
  }
  struct group *group_info = getgrnam(nm_group);
  if (group_info == NULL) {
    fprintf(ERRORFILE, "Can't get group information for %s - %s.\n", nm_group,
            strerror(errno));
    fflush(LOGFILE);
    exit(INVALID_CONFIG_FILE);
  }
  set_nm_uid(getuid(), group_info->gr_gid);
  // if we are running from a setuid executable, make the real uid root
  setuid(0);
  // set the real and effective group id to the node manager group
  setgid(group_info->gr_gid);

  if (check_executor_permissions(executable_file) != 0) {
    fprintf(ERRORFILE, "Invalid permissions on container-executor binary.\n");
    return INVALID_CONTAINER_EXEC_PERMISSIONS;
  }

  //checks done for user name
  if (argv[optind] == NULL) {
    fprintf(ERRORFILE, "Invalid user name.\n");
    return INVALID_USER_NAME;
  }
  int ret = set_user(argv[optind]);
  if (ret != 0) {
    return ret;
  }
 
  optind = optind + 1;
  command = atoi(argv[optind++]);

  fprintf(LOGFILE, "main : command provided %d\n",command);
  fprintf(LOGFILE, "main : user is %s\n", user_detail->pw_name);
  fflush(LOGFILE);

  switch (command) {
  case INITIALIZE_CONTAINER:
    if (argc < 6) {
      fprintf(ERRORFILE, "Too few arguments (%d vs 6) for initialize container\n",
	      argc);
      fflush(ERRORFILE);
      return INVALID_ARGUMENT_NUMBER;
    }
    app_id = argv[optind++];
    cred_file = argv[optind++];
    exit_code = initialize_app(user_detail->pw_name, app_id, cred_file,
                               argv + optind);
    break;
  case LAUNCH_CONTAINER:
    if (argc < 9) {
      fprintf(ERRORFILE, "Too few arguments (%d vs 8) for launch container\n",
	      argc);
      fflush(ERRORFILE);
      return INVALID_ARGUMENT_NUMBER;
    }
    app_id = argv[optind++];
    container_id = argv[optind++];
    current_dir = argv[optind++];
    script_file = argv[optind++];
    cred_file = argv[optind++];
    pid_file = argv[optind++];
    exit_code = launch_container_as_user(user_detail->pw_name, app_id, container_id,
                                 current_dir, script_file, cred_file, pid_file);
    break;
  case SIGNAL_CONTAINER:
    if (argc < 5) {
      fprintf(ERRORFILE, "Too few arguments (%d vs 5) for signal container\n",
	      argc);
      fflush(ERRORFILE);
      return INVALID_ARGUMENT_NUMBER;
    } else {
      char* end_ptr = NULL;
      char* option = argv[optind++];
      int container_pid = strtol(option, &end_ptr, 10);
      if (option == end_ptr || *end_ptr != '\0') {
        fprintf(ERRORFILE, "Illegal argument for container pid %s\n", option);
        fflush(ERRORFILE);
        return INVALID_ARGUMENT_NUMBER;
      }
      option = argv[optind++];
      int signal = strtol(option, &end_ptr, 10);
      if (option == end_ptr || *end_ptr != '\0') {
        fprintf(ERRORFILE, "Illegal argument for signal %s\n", option);
        fflush(ERRORFILE);
        return INVALID_ARGUMENT_NUMBER;
      }
      exit_code = signal_container_as_user(user_detail->pw_name, container_pid, signal);
    }
    break;
  case DELETE_AS_USER:
    dir_to_be_deleted = argv[optind++];
    exit_code= delete_as_user(user_detail->pw_name, dir_to_be_deleted,
                              argv + optind);
    break;
  default:
    fprintf(ERRORFILE, "Invalid command %d not supported.",command);
    fflush(ERRORFILE);
    exit_code = INVALID_COMMAND_PROVIDED;
  }
  fclose(LOGFILE);
  fclose(ERRORFILE);
  return exit_code;
}
