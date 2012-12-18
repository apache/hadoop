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

#include "config.h"
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

#define CONF_FILENAME "container-executor.cfg"

// When building as part of a Maven build this value gets defined by using
// container-executor.conf.dir property. See:
//   hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/pom.xml
// for details.
// NOTE: if this ends up being a relative path it gets resolved relative to
//       the location of the container-executor binary itself, not getwd(3)
#ifndef HADOOP_CONF_DIR
  #error HADOOP_CONF_DIR must be defined
#endif

void display_usage(FILE *stream) {
  fprintf(stream,
          "Usage: container-executor --checksetup\n");
  fprintf(stream,
          "Usage: container-executor --mount-cgroups "\
          "hierarchy controller=path...\n");
  fprintf(stream,
      "Usage: container-executor user command command-args\n");
  fprintf(stream, "Commands:\n");
  fprintf(stream, "   initialize container: %2d appid tokens " \
   "nm-local-dirs nm-log-dirs cmd app...\n", INITIALIZE_CONTAINER);
  fprintf(stream,
      "   launch container:    %2d appid containerid workdir "\
      "container-script tokens pidfile nm-local-dirs nm-log-dirs resources\n",
	  LAUNCH_CONTAINER);
  fprintf(stream, "   signal container:    %2d container-pid signal\n",
	  SIGNAL_CONTAINER);
  fprintf(stream, "   delete as user: %2d relative-path\n",
	  DELETE_AS_USER);
}

int main(int argc, char **argv) {
  int invalid_args = 0; 
  int do_check_setup = 0;
  int do_mount_cgroups = 0;
  
  LOGFILE = stdout;
  ERRORFILE = stderr;

  if (argc > 1) {
    if (strcmp("--mount-cgroups", argv[1]) == 0) {
      do_mount_cgroups = 1;
    }
  }

  // Minimum number of arguments required to run 
  // the std. container-executor commands is 4
  // 4 args not needed for checksetup option
  if (argc < 4 && !do_mount_cgroups) {
    invalid_args = 1;
    if (argc == 2) {
      const char *arg1 = argv[1];
      if (strcmp("--checksetup", arg1) == 0) {
        invalid_args = 0;
        do_check_setup = 1;        
      }      
    }
  }
  
  if (invalid_args != 0) {
    display_usage(stdout);
    return INVALID_ARGUMENT_NUMBER;
  }

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

  char *orig_conf_file = HADOOP_CONF_DIR "/" CONF_FILENAME;
  char *conf_file = resolve_config_path(orig_conf_file, argv[0]);
  char *local_dirs, *log_dirs;
  char *resources, *resources_key, *resources_value;

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

  if (do_check_setup != 0) {
    // basic setup checks done
    // verified configs available and valid
    // verified executor permissions
    return 0;
  }

  if (do_mount_cgroups) {
    optind++;
    char *hierarchy = argv[optind++];
    int result = 0;

    while (optind < argc && result == 0) {
      result = mount_cgroup(argv[optind++], hierarchy);
    }

    return result;
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
    if (argc < 8) {
      fprintf(ERRORFILE, "Too few arguments (%d vs 8) for initialize container\n",
	      argc);
      fflush(ERRORFILE);
      return INVALID_ARGUMENT_NUMBER;
    }
    app_id = argv[optind++];
    cred_file = argv[optind++];
    local_dirs = argv[optind++];// good local dirs as a comma separated list
    log_dirs = argv[optind++];// good log dirs as a comma separated list
    exit_code = initialize_app(user_detail->pw_name, app_id, cred_file,
                               extract_values(local_dirs),
                               extract_values(log_dirs), argv + optind);
    break;
  case LAUNCH_CONTAINER:
    if (argc != 12) {
      fprintf(ERRORFILE, "Wrong number of arguments (%d vs 12) for launch container\n",
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
    local_dirs = argv[optind++];// good local dirs as a comma separated list
    log_dirs = argv[optind++];// good log dirs as a comma separated list
    resources = argv[optind++];// key,value pair describing resources
    char *resources_key = malloc(strlen(resources));
    char *resources_value = malloc(strlen(resources));
    if (get_kv_key(resources, resources_key, strlen(resources)) < 0 ||
        get_kv_value(resources, resources_value, strlen(resources)) < 0) {
        fprintf(ERRORFILE, "Invalid arguments for cgroups resources: %s",
                           resources);
        fflush(ERRORFILE);
        free(resources_key);
        free(resources_value);
        return INVALID_ARGUMENT_NUMBER;
    }
    char** resources_values = extract_values(resources_value);
    exit_code = launch_container_as_user(user_detail->pw_name, app_id,
                    container_id, current_dir, script_file, cred_file,
                    pid_file, extract_values(local_dirs),
                    extract_values(log_dirs), resources_key,
                    resources_values);
    free(resources_key);
    free(resources_value);
    break;
  case SIGNAL_CONTAINER:
    if (argc != 5) {
      fprintf(ERRORFILE, "Wrong number of arguments (%d vs 5) for " \
          "signal container\n", argc);
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
