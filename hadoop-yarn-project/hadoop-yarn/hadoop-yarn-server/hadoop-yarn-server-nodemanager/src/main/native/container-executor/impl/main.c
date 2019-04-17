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
#include "util.h"
#include "get_executable.h"
#include "modules/gpu/gpu-module.h"
#include "modules/fpga/fpga-module.h"
#include "modules/cgroups/cgroups-operations.h"
#include "modules/devices/devices-module.h"
#include "utils/string-utils.h"

#include <errno.h>
#include <grp.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>

static void display_usage(FILE *stream) {
  fprintf(stream,
    "Usage: container-executor --checksetup\n"
    "       container-executor --mount-cgroups <hierarchy> "
    "<controller=path>\n" );

  if(is_tc_support_enabled()) {
    fprintf(stream,
      "       container-executor --tc-modify-state <command-file>\n"
      "       container-executor --tc-read-state <command-file>\n"
      "       container-executor --tc-read-stats <command-file>\n" );
  } else {
    fprintf(stream,
      "[DISABLED] container-executor --tc-modify-state <command-file>\n"
      "[DISABLED] container-executor --tc-read-state <command-file>\n"
      "[DISABLED] container-executor --tc-read-stats <command-file>\n");
  }

  if(is_docker_support_enabled()) {
    fprintf(stream,
      "       container-executor --run-docker <command-file>\n"
      "       container-executor --remove-docker-container [hierarchy] "
      "<container_id>\n"
      "       container-executor --inspect-docker-container <container_id>\n");
  } else {
    fprintf(stream,
      "[DISABLED] container-executor --run-docker <command-file>\n"
      "[DISABLED] container-executor --remove-docker-container [hierarchy] "
      "<container_id>\n"
      "[DISABLED] container-executor --inspect-docker-container "
      "<format> ... <container_id>\n");
  }

  if (is_terminal_support_enabled()) {
    fprintf(stream,
      "       container-executor --exec-container <command-file>\n");
  } else {
    fprintf(stream,
      "[DISABLED] container-executor --exec-container <command-file>\n");
  }

  fprintf(stream,
      "       container-executor <user> <yarn-user> <command> <command-args>\n"
      "       where command and command-args: \n" \
      "            initialize container:  %2d appid tokens nm-local-dirs "
      "nm-log-dirs cmd app...\n"
      "            launch container:      %2d appid containerid workdir "
      "container-script tokens pidfile nm-local-dirs nm-log-dirs resources ",
      INITIALIZE_CONTAINER, LAUNCH_CONTAINER);

  if(is_tc_support_enabled()) {
    fprintf(stream, "optional-tc-command-file\n");
  } else {
    fprintf(stream, "\n");
  }

  if(is_docker_support_enabled()) {
    fprintf(stream,
      "            launch docker container:      %2d appid containerid workdir "
      "container-script tokens pidfile nm-local-dirs nm-log-dirs "
      "docker-command-file resources ", LAUNCH_DOCKER_CONTAINER);
  } else {
    fprintf(stream,
      "[DISABLED]  launch docker container:      %2d appid containerid workdir "
      "container-script tokens pidfile nm-local-dirs nm-log-dirs "
      "docker-command-file resources ", LAUNCH_DOCKER_CONTAINER);
  }

  if(is_tc_support_enabled()) {
    fprintf(stream, "optional-tc-command-file\n");
  } else {
    fprintf(stream, "\n");
  }

  fprintf(stream,
      "            signal container:      %2d container-pid signal\n"
      "            delete as user:        %2d relative-path\n"
      "            list as user:          %2d relative-path\n",
      SIGNAL_CONTAINER, DELETE_AS_USER, LIST_AS_USER);

  if(is_yarn_sysfs_support_enabled()) {
    fprintf(stream,
        "            sync yarn sysfs:       %2d app-id nm-local-dirs\n",
        SYNC_YARN_SYSFS);
  } else {
    fprintf(stream,
        "[DISABLED]  sync yarn sysfs:       %2d app-id nm-local-dirs\n",
        SYNC_YARN_SYSFS);
  }
  fflush(stream);
}

/* Sets up log files for normal/error logging */
static void open_log_files() {
  if (LOGFILE == NULL) {
    LOGFILE = stdout;
  }

  if (ERRORFILE == NULL) {
    ERRORFILE = stderr;
  }

  // There may be a process reading from stdout/stderr, and if it
  // exits, we will crash on a SIGPIPE when we try to write to them.
  // By ignoring SIGPIPE, we can handle the EPIPE instead of crashing.
  signal(SIGPIPE, SIG_IGN);
}

/* Flushes and closes log files */
static void flush_and_close_log_files() {
  if (LOGFILE != NULL) {
    fflush(LOGFILE);
    fclose(LOGFILE);
    LOGFILE = NULL;
  }

  if (ERRORFILE != NULL) {
    fflush(ERRORFILE);
    fclose(ERRORFILE);
    ERRORFILE = NULL;
  }

  free_executor_configurations();
}

/** Validates the current container-executor setup. Causes program exit
in case of validation failures. Also sets up configuration / group information etc.,
This function is to be called in every invocation of container-executor, irrespective
of whether an explicit checksetup operation is requested. */

static void assert_valid_setup(char *argv0) {
  int ret;
  char *executable_file = get_executable(argv0);
  if (!executable_file || executable_file[0] == 0) {
    fprintf(ERRORFILE, "realpath of executable: %s\n",
            errno != 0 ? strerror(errno) : "unknown");
    flush_and_close_log_files();
    exit(INVALID_CONFIG_FILE);
  }

  char *conf_file = get_config_path(argv0);

  if (conf_file == NULL) {
    flush_and_close_log_files();
    exit(INVALID_CONFIG_FILE);
  }

  if (check_configuration_permissions(conf_file) != 0) {
    flush_and_close_log_files();
    exit(INVALID_CONFIG_FILE);
  }
  read_executor_config(conf_file);
  free(conf_file);

  // look up the node manager group in the config file
  char *nm_group = get_nodemanager_group();
  if (nm_group == NULL) {
    free(executable_file);
    fprintf(ERRORFILE, "Can't get configured value for %s.\n", NM_GROUP_KEY);
    flush_and_close_log_files();
    exit(INVALID_CONFIG_FILE);
  }
  struct group *group_info = getgrnam(nm_group);
  if (group_info == NULL) {
    free(executable_file);
    fprintf(ERRORFILE, "Can't get group information for %s - %s.\n", nm_group,
      errno != 0 ? strerror(errno) : "unknown");
    flush_and_close_log_files();
    exit(INVALID_CONFIG_FILE);
  }
  set_nm_uid(getuid(), group_info->gr_gid);
  /*
   * if we are running from a setuid executable, make the real uid root
   * we're going to ignore this result just in case we aren't.
   */
  ret=setuid(0);

  /*
   * set the real and effective group id to the node manager group
   * we're going to ignore this result just in case we aren't
   */
  ret=setgid(group_info->gr_gid);

  /* make the unused var warning to away */
  ret++;

  if (check_executor_permissions(executable_file) != 0) {
    free(executable_file);
    fprintf(ERRORFILE, "Invalid permissions on container-executor binary.\n");
    flush_and_close_log_files();
    exit(INVALID_CONTAINER_EXEC_PERMISSIONS);
  }
  free(executable_file);
}


static void display_feature_disabled_message(const char* name) {
    fprintf(ERRORFILE, "Feature disabled: %s\n", name);
    fflush(ERRORFILE);
}

/* Use to store parsed input parmeters for various operations */
static struct {
  char *cgroups_hierarchy;
  char *traffic_control_command_file;
  const char *run_as_user_name;
  const char *yarn_user_name;
  char *local_dirs;
  char *log_dirs;
  char *resources_key;
  char *resources_value;
  char **resources_values;
  const char *app_id;
  const char *container_id;
  int https;
  const char *keystore_file;
  const char *truststore_file;
  const char *cred_file;
  const char *script_file;
  const char *current_dir;
  const char *pid_file;
  const char *target_dir;
  int container_pid;
  int signal;
  const char *command_file;
} cmd_input;

static int validate_run_as_user_commands(int argc, char **argv, int *operation);

/* Validates that arguments used in the invocation are valid. In case of validation
failure, an 'errorcode' is returned. In case of successful validation, a zero is
returned and 'operation' is populated based on the operation being requested.
Ideally, we should re-factor container-executor to use a more structured, command
line parsing mechanism (e.g getopt). For the time being, we'll use this manual
validation mechanism so that we don't have to change the invocation interface.
*/

static int validate_arguments(int argc, char **argv , int *operation) {
  if (argc < 2) {
    display_usage(stdout);
    return INVALID_ARGUMENT_NUMBER;
  }

  /*
   * Check if it is a known module, if yes, redirect to module
   */
  if (strcmp("--module-gpu", argv[1]) == 0) {
    return handle_gpu_request(&update_cgroups_parameters, "gpu", argc - 1,
           &argv[1]);
  }

  if (strcmp("--module-fpga", argv[1]) == 0) {
    return handle_fpga_request(&update_cgroups_parameters, "fpga", argc - 1,
           &argv[1]);
  }

  if (strcmp("--module-devices", argv[1]) == 0) {
    return handle_devices_request(&update_cgroups_parameters, "devices", argc - 1,
          &argv[1]);
  }

  if (strcmp("--checksetup", argv[1]) == 0) {
    *operation = CHECK_SETUP;
    return 0;
  }

  if (strcmp("--mount-cgroups", argv[1]) == 0) {
    if (is_mount_cgroups_support_enabled()) {
      if (argc < 4) {
        display_usage(stdout);
        return INVALID_ARGUMENT_NUMBER;
      }
      optind++;
      cmd_input.cgroups_hierarchy = argv[optind++];
      *operation = MOUNT_CGROUPS;
      return 0;
    } else {
      display_feature_disabled_message("mount cgroup");
      return FEATURE_DISABLED;
    }
  }

  if (strcmp("--tc-modify-state", argv[1]) == 0) {
    if(is_tc_support_enabled()) {
      if (argc != 3) {
        display_usage(stdout);
        return INVALID_ARGUMENT_NUMBER;
      }
      optind++;
      cmd_input.traffic_control_command_file = argv[optind++];
      *operation = TRAFFIC_CONTROL_MODIFY_STATE;
      return 0;
    } else {
      display_feature_disabled_message("traffic control");
      return FEATURE_DISABLED;
    }
  }

  if (strcmp("--tc-read-state", argv[1]) == 0) {
    if(is_tc_support_enabled()) {
      if (argc != 3) {
        display_usage(stdout);
        return INVALID_ARGUMENT_NUMBER;
      }
      optind++;
      cmd_input.traffic_control_command_file = argv[optind++];
      *operation = TRAFFIC_CONTROL_READ_STATE;
      return 0;
    } else {
      display_feature_disabled_message("traffic control");
      return FEATURE_DISABLED;
    }
  }

  if (strcmp("--tc-read-stats", argv[1]) == 0) {
    if(is_tc_support_enabled()) {
      if (argc != 3) {
        display_usage(stdout);
        return INVALID_ARGUMENT_NUMBER;
      }
      optind++;
      cmd_input.traffic_control_command_file = argv[optind++];
      *operation = TRAFFIC_CONTROL_READ_STATS;
      return 0;
    } else {
      display_feature_disabled_message("traffic control");
      return FEATURE_DISABLED;
    }
  }

  if (strcmp("--exec-container", argv[1]) == 0) {
    if(is_terminal_support_enabled()) {
      if (argc != 3) {
        display_usage(stdout);
        return INVALID_ARGUMENT_NUMBER;
      }
      optind++;
      cmd_input.command_file = argv[optind++];
      *operation = EXEC_CONTAINER;
      return 0;
    } else {
        display_feature_disabled_message("feature.terminal.enabled");
        return FEATURE_DISABLED;
    }
  }

  if (strcmp("--run-docker", argv[1]) == 0) {
    if(is_docker_support_enabled()) {
      if (argc != 3) {
        display_usage(stdout);
        return INVALID_ARGUMENT_NUMBER;
      }
      optind++;
      cmd_input.command_file = argv[optind++];
      *operation = RUN_DOCKER;
      return 0;
    } else {
        display_feature_disabled_message("docker");
        return FEATURE_DISABLED;
    }
  }

  if (strcmp("--remove-docker-container", argv[1]) == 0) {
    if(is_docker_support_enabled()) {
      if ((argc != 3) && (argc != 4)) {
        display_usage(stdout);
        return INVALID_ARGUMENT_NUMBER;
      }
      optind++;
      *operation = REMOVE_DOCKER_CONTAINER;
      return 0;
    } else {
        display_feature_disabled_message("docker");
        return FEATURE_DISABLED;
    }
  }

  if (strcmp("--inspect-docker-container", argv[1]) == 0) {
    if(is_docker_support_enabled()) {
      if (argc != 4) {
        display_usage(stdout);
        return INVALID_ARGUMENT_NUMBER;
      }
      optind++;
      *operation = INSPECT_DOCKER_CONTAINER;
      return 0;
    } else {
        display_feature_disabled_message("docker");
        return FEATURE_DISABLED;
    }
  }

  /* Now we have to validate 'run as user' operations that don't use
    a 'long option' - we should fix this at some point. The validation/argument
    parsing here is extensive enough that it done in a separate function */

  return validate_run_as_user_commands(argc, argv, operation);
}

/* Parse/validate 'run as user' commands */
static int validate_run_as_user_commands(int argc, char **argv, int *operation) {
  /* We need at least the following arguments in order to proceed further :
    <user>, <yarn-user> <command> - i.e at argc should be at least 4 */

  if (argc < 4) {
    display_usage(stdout);
    return INVALID_ARGUMENT_NUMBER;
  }

  cmd_input.run_as_user_name = argv[optind++];
  cmd_input.yarn_user_name = argv[optind++];
  int command = atoi(argv[optind++]);

  fprintf(LOGFILE, "main : command provided %d\n", command);
  fprintf(LOGFILE, "main : run as user is %s\n", cmd_input.run_as_user_name);
  fprintf(LOGFILE, "main : requested yarn user is %s\n", cmd_input.yarn_user_name);
  fflush(LOGFILE);
  char * resources = NULL;// key,value pair describing resources
  char * resources_key = NULL;
  char * resources_value = NULL;
  switch (command) {
  case INITIALIZE_CONTAINER:
    if (argc < 10) {
      fprintf(ERRORFILE, "Too few arguments (%d vs 10) for initialize container\n",
       argc);
      fflush(ERRORFILE);
      return INVALID_ARGUMENT_NUMBER;
    }
    cmd_input.app_id = argv[optind++];
    cmd_input.container_id = argv[optind++];
    if (!validate_container_id(cmd_input.container_id)) {
      fprintf(ERRORFILE, "Invalid container id %s\n", cmd_input.container_id);
      fflush(ERRORFILE);
      return INVALID_CONTAINER_ID;
    }
    cmd_input.cred_file = argv[optind++];
    cmd_input.local_dirs = argv[optind++];// good local dirs as a comma separated list
    cmd_input.log_dirs = argv[optind++];// good log dirs as a comma separated list

    *operation = RUN_AS_USER_INITIALIZE_CONTAINER;
    return 0;
 case LAUNCH_DOCKER_CONTAINER:
   if(is_docker_support_enabled()) {
      //kill me now.
      if (!(argc >= 14 && argc <= 17)) {
        fprintf(ERRORFILE, "Wrong number of arguments (%d vs 14 - 17) for"
          " launch docker container\n", argc);
        fflush(ERRORFILE);
        return INVALID_ARGUMENT_NUMBER;
      }

      cmd_input.app_id = argv[optind++];
      cmd_input.container_id = argv[optind++];
      cmd_input.current_dir = argv[optind++];
      cmd_input.script_file = argv[optind++];
      cmd_input.cred_file = argv[optind++];
      if (strcmp("--https", argv[optind++]) == 0) {
        cmd_input.https = 1;
        cmd_input.keystore_file = argv[optind++];
        cmd_input.truststore_file = argv[optind++];
      } else {
        cmd_input.https = 0;
      }
      cmd_input.pid_file = argv[optind++];
      // good local dirs as a comma separated list
      cmd_input.local_dirs = argv[optind++];
      // good log dirs as a comma separated list
      cmd_input.log_dirs = argv[optind++];
      cmd_input.command_file = argv[optind++];
      //network isolation through tc
      if ((argc == 15 && !cmd_input.https) || (argc == 17 && cmd_input.https)) {
        if(is_tc_support_enabled()) {
          cmd_input.traffic_control_command_file = argv[optind++];
        } else {
        display_feature_disabled_message("traffic control");
        return FEATURE_DISABLED;
        }
      }

      *operation = RUN_AS_USER_LAUNCH_DOCKER_CONTAINER;
      return 0;
   } else {
      display_feature_disabled_message("docker");
      return FEATURE_DISABLED;
   }

  case LAUNCH_CONTAINER:
    //kill me now.
    if (!(argc >= 14 && argc <= 17)) {
      fprintf(ERRORFILE, "Wrong number of arguments (%d vs 14 - 17)"
        " for launch container\n", argc);
      fflush(ERRORFILE);
      return INVALID_ARGUMENT_NUMBER;
    }

    cmd_input.app_id = argv[optind++];
    cmd_input.container_id = argv[optind++];
    cmd_input.current_dir = argv[optind++];
    cmd_input.script_file = argv[optind++];
    cmd_input.cred_file = argv[optind++];
    if (strcmp("--https", argv[optind++]) == 0) {
      cmd_input.https = 1;
      cmd_input.keystore_file = argv[optind++];
      cmd_input.truststore_file = argv[optind++];
    } else {
      cmd_input.https = 0;
    }
    cmd_input.pid_file = argv[optind++];
    cmd_input.local_dirs = argv[optind++];// good local dirs as a comma separated list
    cmd_input.log_dirs = argv[optind++];// good log dirs as a comma separated list
    resources = argv[optind++];// key,value pair describing resources
    resources_key = malloc(strlen(resources));
    resources_value = malloc(strlen(resources));

    if (get_kv_key(resources, resources_key, strlen(resources)) < 0 ||
        get_kv_value(resources, resources_value, strlen(resources)) < 0) {
        fprintf(ERRORFILE, "Invalid arguments for cgroups resources: %s",
                           resources);
        fflush(ERRORFILE);
        free(resources_key);
        free(resources_value);
        return INVALID_ARGUMENT_NUMBER;
    }

    //network isolation through tc
    if ((argc == 15 && !cmd_input.https) || (argc == 17 && cmd_input.https)) {
      if(is_tc_support_enabled()) {
        cmd_input.traffic_control_command_file = argv[optind++];
      } else {
        display_feature_disabled_message("traffic control");
        return FEATURE_DISABLED;
      }
    }

    cmd_input.resources_key = resources_key;
    cmd_input.resources_value = resources_value;
    cmd_input.resources_values = split(resources_value);
    *operation = RUN_AS_USER_LAUNCH_CONTAINER;
    return 0;

  case SIGNAL_CONTAINER:
    if (argc != 6) {
      fprintf(ERRORFILE, "Wrong number of arguments (%d vs 6) for " \
          "signal container\n", argc);
      fflush(ERRORFILE);
      return INVALID_ARGUMENT_NUMBER;
    }

    char* end_ptr = NULL;
    char* option = argv[optind++];
    cmd_input.container_pid = strtol(option, &end_ptr, 10);
    if (option == end_ptr || *end_ptr != '\0') {
      fprintf(ERRORFILE, "Illegal argument for container pid %s\n", option);
      fflush(ERRORFILE);
      return INVALID_ARGUMENT_NUMBER;
    }
    option = argv[optind++];
    cmd_input.signal = strtol(option, &end_ptr, 10);
    if (option == end_ptr || *end_ptr != '\0') {
      fprintf(ERRORFILE, "Illegal argument for signal %s\n", option);
      fflush(ERRORFILE);
      return INVALID_ARGUMENT_NUMBER;
    }

    *operation = RUN_AS_USER_SIGNAL_CONTAINER;
    return 0;

  case DELETE_AS_USER:
    cmd_input.target_dir = argv[optind++];
    *operation = RUN_AS_USER_DELETE;
    return 0;
  case LIST_AS_USER:
    cmd_input.target_dir = argv[optind++];
    *operation = RUN_AS_USER_LIST;
    return 0;
  case SYNC_YARN_SYSFS:
    cmd_input.app_id = argv[optind++];
    cmd_input.local_dirs = argv[optind++];
    *operation = RUN_AS_USER_SYNC_YARN_SYSFS;
    return 0;
  default:
    fprintf(ERRORFILE, "Invalid command %d not supported.",command);
    fflush(ERRORFILE);
    return INVALID_COMMAND_PROVIDED;
  }
}

int main(int argc, char **argv) {
  open_log_files();
  assert_valid_setup(argv[0]);

  int operation = -1;
  int ret = validate_arguments(argc, argv, &operation);

  if (ret != 0) {
    flush_and_close_log_files();
    return ret;
  }

  int exit_code = 0;

  switch (operation) {
  case CHECK_SETUP:
    //we already did this
    exit_code = 0;
    break;
  case MOUNT_CGROUPS:
    exit_code = 0;

    while (optind < argc && exit_code == 0) {
      exit_code = mount_cgroup(argv[optind++], cmd_input.cgroups_hierarchy);
    }

    break;
  case TRAFFIC_CONTROL_MODIFY_STATE:
    exit_code = traffic_control_modify_state(cmd_input.traffic_control_command_file);
    break;
  case TRAFFIC_CONTROL_READ_STATE:
    exit_code = traffic_control_read_state(cmd_input.traffic_control_command_file);
    break;
  case TRAFFIC_CONTROL_READ_STATS:
    exit_code = traffic_control_read_stats(cmd_input.traffic_control_command_file);
    break;
  case EXEC_CONTAINER:
    exit_code = exec_container(cmd_input.command_file);
    break;
  case RUN_DOCKER:
    exit_code = run_docker(cmd_input.command_file);
    break;
  case REMOVE_DOCKER_CONTAINER:
    exit_code = remove_docker_container(argv + optind, argc - optind);
    break;
  case INSPECT_DOCKER_CONTAINER:
    exit_code = exec_docker_command("inspect", argv + optind, argc - optind);
    break;
  case RUN_AS_USER_INITIALIZE_CONTAINER:
    exit_code = set_user(cmd_input.run_as_user_name);
    if (exit_code != 0) {
      break;
    }

    exit_code = initialize_app(cmd_input.yarn_user_name,
                            cmd_input.app_id,
                            cmd_input.container_id,
                            cmd_input.cred_file,
                            split(cmd_input.local_dirs),
                            split(cmd_input.log_dirs),
                            argv + optind);
    break;
  case RUN_AS_USER_LAUNCH_DOCKER_CONTAINER:
     if (cmd_input.traffic_control_command_file != NULL) {
        //apply tc rules before switching users and launching the container
        exit_code = traffic_control_modify_state(cmd_input.traffic_control_command_file);
        if( exit_code != 0) {
          //failed to apply tc rules - break out before launching the container
          break;
        }
      }

      exit_code = set_user(cmd_input.run_as_user_name);
      if (exit_code != 0) {
        break;
      }

      exit_code = launch_docker_container_as_user(cmd_input.yarn_user_name,
                      cmd_input.app_id,
                      cmd_input.container_id,
                      cmd_input.current_dir,
                      cmd_input.script_file,
                      cmd_input.cred_file,
                      cmd_input.https,
                      cmd_input.keystore_file,
                      cmd_input.truststore_file,
                      cmd_input.pid_file,
                      split(cmd_input.local_dirs),
                      split(cmd_input.log_dirs),
                      cmd_input.command_file);
      break;
  case RUN_AS_USER_LAUNCH_CONTAINER:
    if (cmd_input.traffic_control_command_file != NULL) {
      //apply tc rules before switching users and launching the container
      exit_code = traffic_control_modify_state(cmd_input.traffic_control_command_file);
      if( exit_code != 0) {
        //failed to apply tc rules - break out before launching the container
        break;
      }
    }

    exit_code = set_user(cmd_input.run_as_user_name);
    if (exit_code != 0) {
      break;
    }

    exit_code = launch_container_as_user(cmd_input.yarn_user_name,
                    cmd_input.app_id,
                    cmd_input.container_id,
                    cmd_input.current_dir,
                    cmd_input.script_file,
                    cmd_input.cred_file,
                    cmd_input.https,
                    cmd_input.keystore_file,
                    cmd_input.truststore_file,
                    cmd_input.pid_file,
                    split(cmd_input.local_dirs),
                    split(cmd_input.log_dirs),
                    cmd_input.resources_key,
                    cmd_input.resources_values);
    free(cmd_input.resources_key);
    free(cmd_input.resources_value);
    free(cmd_input.resources_values);
    break;
  case RUN_AS_USER_SIGNAL_CONTAINER:
    exit_code = set_user(cmd_input.run_as_user_name);
    if (exit_code != 0) {
      break;
    }

    exit_code = signal_container_as_user(cmd_input.yarn_user_name,
                                  cmd_input.container_pid,
                                  cmd_input.signal);
    break;
  case RUN_AS_USER_DELETE:
    exit_code = set_user(cmd_input.run_as_user_name);
    if (exit_code != 0) {
      break;
    }

    exit_code = delete_as_user(cmd_input.yarn_user_name,
                        cmd_input.target_dir,
                        argv + optind);
    break;
  case RUN_AS_USER_LIST:
    exit_code = set_user(cmd_input.run_as_user_name);

    if (exit_code != 0) {
      break;
    }

    exit_code = list_as_user(cmd_input.target_dir);
    break;
  case RUN_AS_USER_SYNC_YARN_SYSFS:
    exit_code = set_user(cmd_input.run_as_user_name);
    if (exit_code != 0) {
      break;
    }
    if (is_yarn_sysfs_support_enabled()) {
      exit_code = sync_yarn_sysfs(split(cmd_input.local_dirs),
          cmd_input.run_as_user_name, cmd_input.yarn_user_name,
          cmd_input.app_id);
    } else {
      exit_code = FEATURE_DISABLED;
    }
    break;
  }

  flush_and_close_log_files();
  return exit_code;
}
