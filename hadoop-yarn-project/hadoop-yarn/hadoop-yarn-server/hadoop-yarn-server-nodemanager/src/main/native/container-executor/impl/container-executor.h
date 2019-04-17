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

/* FreeBSD protects the getline() prototype. See getline(3) for more */
#ifdef __FreeBSD__
#define _WITH_GETLINE
#endif

#include <pwd.h>
#include <stdio.h>
#include <sys/types.h>

//command definitions
enum command {
  INITIALIZE_CONTAINER = 0,
  LAUNCH_CONTAINER = 1,
  SIGNAL_CONTAINER = 2,
  DELETE_AS_USER = 3,
  LAUNCH_DOCKER_CONTAINER = 4,
  LIST_AS_USER = 5,
  SYNC_YARN_SYSFS = 6
};

enum operations {
  CHECK_SETUP = 1,
  MOUNT_CGROUPS = 2,
  TRAFFIC_CONTROL_MODIFY_STATE = 3,
  TRAFFIC_CONTROL_READ_STATE = 4,
  TRAFFIC_CONTROL_READ_STATS = 5,
  RUN_AS_USER_INITIALIZE_CONTAINER = 6,
  RUN_AS_USER_LAUNCH_CONTAINER = 7,
  RUN_AS_USER_SIGNAL_CONTAINER = 8,
  RUN_AS_USER_DELETE = 9,
  RUN_AS_USER_LAUNCH_DOCKER_CONTAINER = 10,
  RUN_DOCKER = 11,
  RUN_AS_USER_LIST = 12,
  REMOVE_DOCKER_CONTAINER = 13,
  INSPECT_DOCKER_CONTAINER = 14,
  RUN_AS_USER_SYNC_YARN_SYSFS = 15,
  EXEC_CONTAINER = 16
};

#define NM_GROUP_KEY "yarn.nodemanager.linux-container-executor.group"
#define USER_DIR_PATTERN "%s/usercache/%s"
#define USER_FILECACHE_DIR_PATTERN "%s/usercache/%s/filecache"
#define NM_APP_DIR_PATTERN USER_DIR_PATTERN "/appcache/%s"
#define CONTAINER_DIR_PATTERN NM_APP_DIR_PATTERN "/%s"
#define CONTAINER_SCRIPT "launch_container.sh"
#define CREDENTIALS_FILENAME "container_tokens"
#define KEYSTORE_FILENAME "yarn_provided.keystore"
#define TRUSTSTORE_FILENAME "yarn_provided.truststore"
#define MIN_USERID_KEY "min.user.id"
#define BANNED_USERS_KEY "banned.users"
#define ALLOWED_SYSTEM_USERS_KEY "allowed.system.users"
#define TERMINAL_SUPPORT_ENABLED_KEY "feature.terminal.enabled"
#define DOCKER_SUPPORT_ENABLED_KEY "feature.docker.enabled"
#define TC_SUPPORT_ENABLED_KEY "feature.tc.enabled"
#define MOUNT_CGROUP_SUPPORT_ENABLED_KEY "feature.mount-cgroup.enabled"
#define YARN_SYSFS_SUPPORT_ENABLED_KEY "feature.yarn.sysfs.enabled"
#define TMP_DIR "tmp"
#define COMMAND_FILE_SECTION "command-execution"

extern struct passwd *user_detail;

//function used to load the configurations present in the secure config
void read_executor_config(const char* file_name);

//Lookup nodemanager group from container executor configuration.
char *get_nodemanager_group();

/**
 * Check the permissions on the container-executor to make sure that security is
 * permissible. For this, we need container-executor binary to
 *    * be user-owned by root
 *    * be group-owned by a configured special group.
 *    * others do not have any permissions
 *    * be setuid/setgid
 * @param executable_file the file to check
 * @return -1 on error 0 on success.
 */
int check_executor_permissions(char *executable_file);

//function used to load the configurations present in the secure config.
void read_executor_config(const char* file_name);

//function used to free executor configuration data
void free_executor_configurations();

// initialize the application directory
int initialize_app(const char *user, const char *app_id,
                   const char *container_id,
                   const char *credentials, char* const* local_dirs,
                   char* const* log_dirs, char* const* args);

int launch_docker_container_as_user(const char * user, const char *app_id,
                              const char *container_id, const char *work_dir,
                              const char *script_name, const char *cred_file,
                              const int https,
                              const char *keystore_file, const char *truststore_file,
                              const char *pid_file, char* const* local_dirs,
                              char* const* log_dirs,
                              const char *command_file);

/*
 * Function used to launch a container as the provided user. It does the following :
 * 1) Creates container work dir and log dir to be accessible by the child
 * 2) Copies the script file from the NM to the work directory
 * 3) Sets up the environment
 * 4) Does an execlp on the same in order to replace the current image with
 *    container image.
 * @param user the user to become
 * @param app_id the application id
 * @param container_id the container id
 * @param work_dir the working directory for the container.
 * @param script_name the name of the script to be run to launch the container.
 * @param cred_file the credentials file that needs to be copied to the
 * working directory.
 * @param https 1 if a keystore and truststore will be provided, 0 if not
 * @param keystore_file the keystore file that needs to be copied to the
 * working directory.
 * @param truststore_file the truststore file that needs to be copied to the
 * working directory
 * @param pid_file file where pid of process should be written to
 * @param local_dirs nodemanager-local-directories to be used
 * @param log_dirs nodemanager-log-directories to be used
 * @param resources_key type of resource enforcement (none, cgroups)
 * @param resources_value values needed to apply resource enforcement
 * @return -1 or errorcode enum value on error (should never return on success).
 */
int launch_container_as_user(const char * user, const char *app_id,
                     const char *container_id, const char *work_dir,
                     const char *script_name, const char *cred_file,
                     const int https,
                     const char *keystore_file, const char *truststore_file,
                     const char *pid_file, char* const* local_dirs,
                     char* const* log_dirs, const char *resources_key,
                     char* const* resources_value);

/**
 * Function used to signal a container launched by the user.
 * The function sends appropriate signal to the process group
 * specified by the pid.
 * @param user the user to send the signal as.
 * @param pid the process id to send the signal to.
 * @param sig the signal to send.
 * @return an errorcode enum value on error, or 0 on success.
 */
int signal_container_as_user(const char *user, int pid, int sig);

// delete a directory (or file) recursively as the user. The directory
// could optionally be relative to the baseDir set of directories (if the same
// directory appears on multiple disk volumes, the disk volumes should be passed
// as the baseDirs). If baseDirs is not specified, then dir_to_be_deleted is
// assumed as the absolute path
int delete_as_user(const char *user,
                   const char *dir_to_be_deleted,
                   char* const* baseDirs);

// List the files in the given directory on stdout. The target_dir is always
// assumed to be an absolute path.
int list_as_user(const char *target_dir);

// set the uid and gid of the node manager.  This is used when doing some
// priviledged operations for setting the effective uid and gid.
void set_nm_uid(uid_t user, gid_t group);

/**
 * Is the user a real user account?
 * Checks:
 *   1. Not root
 *   2. UID is above the minimum configured.
 *   3. Not in banned user list
 * Returns NULL on failure
 */
struct passwd* check_user(const char *user);

// set the user
int set_user(const char *user);

// methods to get the directories

char *get_user_directory(const char *nm_root, const char *user);

char *get_app_directory(const char * nm_root, const char *user,
                        const char *app_id);

/**
 * Check node manager local dir permission.
 */
int check_nm_local_dir(uid_t caller_uid, const char *nm_root);

char *get_container_work_directory(const char *nm_root, const char *user,
				 const char *app_id, const char *container_id);

char *get_container_launcher_file(const char* work_dir);

char *get_container_credentials_file(const char* work_dir);

char *get_container_keystore_file(const char* work_dir);

char *get_container_truststore_file(const char* work_dir);

/**
 * Get the app log directory under log_root
 */
char* get_app_log_directory(const char* log_root, const char* appid);

char* get_container_log_directory(const char *log_root, const char* app_id,
                                  const char *container_id);
/**
 * Ensure that the given path and all of the parent directories are created
 * with the desired permissions.
 */
int mkdirs(const char* path, mode_t perm);

/**
 * Function to initialize the user directories of a user.
 */
int initialize_user(const char *user, char* const* local_dirs);

/**
 * Create a top level directory for the user.
 * It assumes that the parent directory is *not* writable by the user.
 * It creates directories with 02700 permissions owned by the user
 * and with the group set to the node manager group.
 * return non-0 on failure
 */
int create_directory_for_user(const char* path);

int change_user(uid_t user, gid_t group);

int mount_cgroup(const char *pair, const char *hierarchy);

int check_dir(const char* npath, mode_t st_mode, mode_t desired,
   int finalComponent);

int create_validate_dir(const char* npath, mode_t perm, const char* path,
   int finalComponent);

/** Check if a feature is enabled in the specified configuration. */
int is_feature_enabled(const char* feature_key, int default_value,
                              struct section *cfg);

/** Check if tc (traffic control) support is enabled in configuration. */
int is_tc_support_enabled();

/** Check if cgroup mount support is enabled in configuration. */
int is_mount_cgroups_support_enabled();

/**
 * Run a batch of tc commands that modify interface configuration
 */
int traffic_control_modify_state(char *command_file);

/**
 * Run a batch of tc commands that read interface configuration. Output is
 * written to standard output and it is expected to be read and parsed by the
 * calling process.
 */
int traffic_control_read_state(char *command_file);

/**
 * Run a batch of tc commands that read interface stats. Output is
 * written to standard output and it is expected to be read and parsed by the
 * calling process.
 */
int traffic_control_read_stats(char *command_file);

/** Check if docker support is enabled in configuration. */
int is_docker_support_enabled();

/**
 * Run a docker command passing the command file as an argument
 */
int run_docker(const char *command_file);

/**
 * Run a docker command passing the command file as an argument with terminal.
 */
int run_docker_with_pty(const char *command_file);

/**
 * Run a docker command without a command file
 */
int exec_docker_command(char *docker_command, char **argv, int argc);

/**
 * Exec a container terminal.
 */
int exec_container(const char *command_file);

/** Check if yarn sysfs is enabled in configuration. */
int is_yarn_sysfs_support_enabled();

/**
 * Create YARN SysFS
 */
int create_yarn_sysfs(const char* user, const char *app_id,
    const char *container_id, const char *work_dir, char* const* local_dirs);

/**
 * Sync YARN SysFS
 */
int sync_yarn_sysfs(char* const* local_dirs, const char *running_user,
    const char *end_user, const char *app_id);

/*
 * Compile the regex_str and determine if the input string matches.
 * Return 0 on match, 1 of non-match.
 */
int execute_regex_match(const char *regex_str, const char *input);

/**
 * Validate the docker image name matches the expected input.
 * Return 0 on success.
 */
int validate_docker_image_name(const char *image_name);

struct configuration* get_cfg();

/**
 * Flatten docker launch command
 */
char* flatten(char **args);

/**
 * Remove docker container
 */
int remove_docker_container(char **argv, int argc);

/**
 * Check if terminal feature is enabled
 */
int is_terminal_support_enabled();
