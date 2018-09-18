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
#include "utils/string-utils.h"
#include "util.h"
#include "get_executable.h"
#include "test/test-container-executor-common.h"

#include <inttypes.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/param.h>


static char* username = NULL;
static char* yarn_username = NULL;
static char** local_dirs = NULL;
static char** log_dirs = NULL;
static uid_t nm_uid = -1;

/**
 * Run the command using the effective user id.
 * It can't use system, since bash seems to copy the real user id into the
 * effective id.
 */
void run(const char *cmd) {
  fflush(stdout);
  fflush(stderr);
  pid_t child = fork();
  if (child == -1) {
    printf("FAIL: failed to fork - %s\n", strerror(errno));
  } else if (child == 0) {
    char *cmd_copy = strdup(cmd);
    char *ptr;
    int words = 1;
    for(ptr = strchr(cmd_copy, ' ');  ptr; ptr = strchr(ptr+1, ' ')) {
      words += 1;
    }
    char **argv = malloc(sizeof(char *) * (words + 1));
    ptr = strtok(cmd_copy, " ");
    int i = 0;
    argv[i++] = ptr;
    while (ptr != NULL) {
      ptr = strtok(NULL, " ");
      argv[i++] = ptr;
    }
    if (execvp(argv[0], argv) != 0) {
      printf("FAIL: exec failed in child %s - %s\n", cmd, strerror(errno));
      exit(42);
    }
  } else {
    int status = 0;
    if (waitpid(child, &status, 0) <= 0) {
      printf("FAIL: failed waiting for child process %s pid %" PRId64 " - %s\n",
	     cmd, (int64_t)child, strerror(errno));
      exit(1);
    }
    if (!WIFEXITED(status)) {
      printf("FAIL: process %s pid %" PRId64 " did not exit\n", cmd, (int64_t)child);
      exit(1);
    }
    if (WEXITSTATUS(status) != 0) {
      printf("FAIL: process %s pid %" PRId64 " exited with error status %d\n", cmd,
	     (int64_t)child, WEXITSTATUS(status));
      exit(1);
    }
  }
}

int write_config_file(char *file_name, int banned) {
  FILE *file;
  file = fopen(file_name, "w");
  if (file == NULL) {
    printf("Failed to open %s.\n", file_name);
    return EXIT_FAILURE;
  }
  if (banned != 0) {
    fprintf(file, "banned.users=bannedUser\n");
    fprintf(file, "min.user.id=500\n");
  } else {
    fprintf(file, "min.user.id=0\n");
  }
  fprintf(file, "allowed.system.users=allowedUser,daemon\n");
  fclose(file);
  return 0;
}

void create_nm_roots(char ** nm_roots) {
  char** nm_root;
  for(nm_root=nm_roots; *nm_root != NULL; ++nm_root) {
    if (mkdir(*nm_root, 0755) != 0) {
      printf("FAIL: Can't create directory %s - %s\n", *nm_root,
             strerror(errno));
      exit(1);
    }
    char buffer[100000];
    sprintf(buffer, "%s/usercache", *nm_root);
    if (mkdir(buffer, 0755) != 0) {
      printf("FAIL: Can't create directory %s - %s\n", buffer,
             strerror(errno));
      exit(1);
    }
  }
}

void check_pid_file(const char* pid_file, pid_t mypid) {
  if(access(pid_file, R_OK) != 0) {
    printf("FAIL: failed to create pid file %s\n", pid_file);
    exit(1);
  }
  int pidfd = open(pid_file, O_RDONLY);
  if (pidfd == -1) {
    printf("FAIL: failed to open pid file %s - %s\n", pid_file, strerror(errno));
    exit(1);
  }

  char pidBuf[100];
  ssize_t bytes = read(pidfd, pidBuf, 100);
  if (bytes == -1) {
    printf("FAIL: failed to read from pid file %s - %s\n", pid_file, strerror(errno));
    exit(1);
  }

  char myPidBuf[33];
  snprintf(myPidBuf, 33, "%" PRId64, (int64_t)(mypid + 1));
  if (strncmp(pidBuf, myPidBuf, strlen(myPidBuf)) != 0) {
    printf("FAIL: failed to find matching pid in pid file\n");
    printf("FAIL: Expected pid %" PRId64 " : Got %.*s", (int64_t)mypid,
      (int)bytes, pidBuf);
    exit(1);
  }
}

void test_get_user_directory() {
  char *user_dir = get_user_directory(TEST_ROOT, "user");
  char *expected = TEST_ROOT "/usercache/user";
  if (strcmp(user_dir, expected) != 0) {
    printf("test_get_user_directory expected %s got %s\n", expected, user_dir);
    exit(1);
  }
  free(user_dir);
}

void test_check_nm_local_dir() {
  // check filesystem is same as running user.
  int expected = 0;
  char *local_path = TEST_ROOT "target";
  char *root_path = "/";
  if (mkdirs(local_path, 0700) != 0) {
    printf("FAIL: unble to create node manager local directory: %s\n", local_path);
    exit(1);
  }
  int actual = check_nm_local_dir(nm_uid, local_path);
  if (expected != actual) {
    printf("test_nm_local_dir expected %d got %d\n", expected, actual);
    exit(1);
  }
  // check filesystem is different from running user.
  expected = 1;
  actual = check_nm_local_dir(nm_uid, root_path);
  if (expected != actual && nm_uid != 0) {
    printf("test_nm_local_dir expected %d got %d\n", expected, actual);
    exit(1);
  }
}

void test_get_app_directory() {
  char *expected = TEST_ROOT "/usercache/user/appcache/app_200906101234_0001";
  char *app_dir = (char *) get_app_directory(TEST_ROOT, "user",
      "app_200906101234_0001");
  if (strcmp(app_dir, expected) != 0) {
    printf("test_get_app_directory expected %s got %s\n", expected, app_dir);
    exit(1);
  }
  free(app_dir);
}

void test_get_container_directory() {
  char *container_dir = get_container_work_directory(TEST_ROOT, "owen", "app_1",
						 "container_1");
  char *expected = TEST_ROOT "/usercache/owen/appcache/app_1/container_1";
  if (strcmp(container_dir, expected) != 0) {
    printf("Fail get_container_work_directory got %s expected %s\n",
	   container_dir, expected);
    exit(1);
  }
  free(container_dir);
}

void test_get_container_launcher_file() {
  char *expected_file = (TEST_ROOT "/usercache/user/appcache/app_200906101234_0001"
			 "/launch_container.sh");
  char *app_dir = get_app_directory(TEST_ROOT, "user",
                                    "app_200906101234_0001");
  char *container_file =  get_container_launcher_file(app_dir);
  if (strcmp(container_file, expected_file) != 0) {
    printf("failure to match expected container file %s vs %s\n", container_file,
           expected_file);
    exit(1);
  }
  free(app_dir);
  free(container_file);
}

void test_get_app_log_dir() {
  char *expected = TEST_ROOT "/logs/userlogs/app_200906101234_0001";
  char *logdir = get_app_log_directory(TEST_ROOT "/logs/userlogs","app_200906101234_0001");
  if (strcmp(logdir, expected) != 0) {
    printf("Fail get_app_log_dir got %s expected %s\n", logdir, expected);
    exit(1);
  }
  free(logdir);
}

void test_check_user(int expectedFailure) {
  printf("\nTesting test_check_user\n");
  struct passwd *user = check_user(username);
  if (user == NULL && !expectedFailure) {
    printf("FAIL: failed check for user %s\n", username);
    exit(1);
  }
  free(user);
  if (check_user("lp") != NULL && !expectedFailure) {
    printf("FAIL: failed check for system user lp\n");
    exit(1);
  }
  if (check_user("root") != NULL) {
    printf("FAIL: failed check for system user root\n");
    exit(1);
  }
  if (check_user("daemon") == NULL && !expectedFailure) {
    printf("FAIL: failed check for whitelisted system user daemon\n");
    exit(1);
  }
}

void test_resolve_config_path() {
  printf("\nTesting resolve_config_path\n");
  if (strcmp(resolve_config_path(TEST_ROOT, NULL), TEST_ROOT) != 0) {
    printf("FAIL: failed to resolve config_name on an absolute path name: "
           TEST_ROOT "\n");
    exit(1);
  }
  if (strcmp(resolve_config_path(RELTMPDIR TEST_ROOT, TEST_ROOT), TEST_ROOT) != 0) {
    printf("FAIL: failed to resolve config_name on a relative path name: "
           RELTMPDIR TEST_ROOT " (relative to " TEST_ROOT ")");
    exit(1);
  }
}

void test_check_configuration_permissions() {
  printf("\nTesting check_configuration_permissions\n");
  if (check_configuration_permissions("/etc/passwd") != 0) {
    printf("FAIL: failed permission check on /etc/passwd\n");
    exit(1);
  }
  if (check_configuration_permissions(TEST_ROOT) == 0) {
    printf("FAIL: failed permission check on %s\n", TEST_ROOT);
    exit(1);
  }
}

void test_delete_container() {
  if (initialize_user(yarn_username, local_dirs)) {
    printf("FAIL: failed to initialize user %s\n", yarn_username);
    exit(1);
  }
  char* app_dir = get_app_directory(TEST_ROOT "/local-2", yarn_username, "app_1");
  char* dont_touch = get_app_directory(TEST_ROOT "/local-2", yarn_username,
                                       DONT_TOUCH_FILE);
  char* container_dir = get_container_work_directory(TEST_ROOT "/local-2",
					      yarn_username, "app_1", "container_1");
  char buffer[100000];
  sprintf(buffer, "mkdir -p %s/who/let/the/dogs/out/who/who", container_dir);
  run(buffer);
  sprintf(buffer, "mknod %s/who/let/the/dogs/out/who/who/p p", container_dir);
  run(buffer);
  sprintf(buffer, "touch %s", dont_touch);
  run(buffer);

  // soft link to the canary file from the container directory
  sprintf(buffer, "ln -s %s %s/who/softlink", dont_touch, container_dir);
  run(buffer);
  // hard link to the canary file from the container directory
  sprintf(buffer, "ln %s %s/who/hardlink", dont_touch, container_dir);
  run(buffer);
  // create a dot file in the container directory
  sprintf(buffer, "touch %s/who/let/.dotfile", container_dir);
  run(buffer);
  // create a no permission file
  sprintf(buffer, "touch %s/who/let/protect", container_dir);
  run(buffer);
  sprintf(buffer, "chmod 000 %s/who/let/protect", container_dir);
  run(buffer);
  // create a no execute permission directory
  sprintf(buffer, "chmod 600 %s/who/let/the", container_dir);
  run(buffer);
  // create a no permission directory
  sprintf(buffer, "chmod 000 %s/who/let", container_dir);
  run(buffer);
  // create a no write permission directory
  sprintf(buffer, "chmod 500 %s/who", container_dir);
  run(buffer);

  // delete container directory
  char * dirs[] = {app_dir, 0};
  int ret = delete_as_user(yarn_username, "container_1" , dirs);
  if (ret != 0) {
    printf("FAIL: return code from delete_as_user is %d\n", ret);
    exit(1);
  }

  // check to make sure the container directory is gone
  if (access(container_dir, R_OK) == 0) {
    printf("FAIL: failed to delete the directory - %s\n", container_dir);
    exit(1);
  }
  // check to make sure the app directory is not gone
  if (access(app_dir, R_OK) != 0) {
    printf("FAIL: accidently deleted the directory - %s\n", app_dir);
    exit(1);
  }
  // but that the canary is not gone
  if (access(dont_touch, R_OK) != 0) {
    printf("FAIL: accidently deleted file %s\n", dont_touch);
    exit(1);
  }
  sprintf(buffer, "chmod -R 700 %s", app_dir);
  run(buffer);
  sprintf(buffer, "rm -fr %s", app_dir);
  run(buffer);
  free(app_dir);
  free(container_dir);
  free(dont_touch);
}

void test_delete_app() {
  char* app_dir = get_app_directory(TEST_ROOT "/local-2", yarn_username, "app_2");
  char* dont_touch = get_app_directory(TEST_ROOT "/local-2", yarn_username,
                                       DONT_TOUCH_FILE);
  char* container_dir = get_container_work_directory(TEST_ROOT "/local-2",
					      yarn_username, "app_2", "container_1");
  char buffer[100000];
  sprintf(buffer, "mkdir -p %s/who/let/the/dogs/out/who/who", container_dir);
  run(buffer);
  sprintf(buffer, "touch %s", dont_touch);
  run(buffer);

  // soft link to the canary file from the container directory
  sprintf(buffer, "ln -s %s %s/who/softlink", dont_touch, container_dir);
  run(buffer);
  // hard link to the canary file from the container directory
  sprintf(buffer, "ln %s %s/who/hardlink", dont_touch, container_dir);
  run(buffer);
  // create a dot file in the container directory
  sprintf(buffer, "touch %s/who/let/.dotfile", container_dir);
  run(buffer);
  // create a no permission file
  sprintf(buffer, "touch %s/who/let/protect", container_dir);
  run(buffer);
  sprintf(buffer, "chmod 000 %s/who/let/protect", container_dir);
  run(buffer);
  // create a no permission directory
  sprintf(buffer, "chmod 000 %s/who/let", container_dir);
  run(buffer);

  // delete application directory
  int ret = delete_as_user(yarn_username, app_dir, NULL);
  if (ret != 0) {
    printf("FAIL: return code from delete_as_user is %d\n", ret);
    exit(1);
  }

  // check to make sure the container directory is gone
  if (access(container_dir, R_OK) == 0) {
    printf("FAIL: failed to delete the directory - %s\n", container_dir);
    exit(1);
  }
  // check to make sure the app directory is gone
  if (access(app_dir, R_OK) == 0) {
    printf("FAIL: didn't delete the directory - %s\n", app_dir);
    exit(1);
  }
  // but that the canary is not gone
  if (access(dont_touch, R_OK) != 0) {
    printf("FAIL: accidently deleted file %s\n", dont_touch);
    exit(1);
  }
  // verify attempt to delete a nonexistent directory does not fail
  ret = delete_as_user(yarn_username, app_dir, NULL);
  if (ret != 0) {
    printf("FAIL: return code from delete_as_user is %d\n", ret);
    exit(1);
  }

  free(app_dir);
  free(container_dir);
  free(dont_touch);
}

void validate_feature_enabled_value(int expected_value, const char* key,
    int default_value, struct section *cfg) {
  int value = is_feature_enabled(key, default_value, cfg);

  if (value != expected_value) {
    printf("FAIL: expected value %d for key %s but found %d\n",
    expected_value, key, value);
    exit(1);
  }
}

void test_is_feature_enabled() {
  char* filename = TEST_ROOT "/feature_flag_test.cfg";
  FILE *file = fopen(filename, "w");
  int disabled = 0;
  int enabled = 1;
  struct configuration exec_cfg = {.size=0, .sections=NULL};
  struct section cfg = {.size=0, .kv_pairs=NULL};

  if (file == NULL) {
    printf("FAIL: Could not open configuration file: %s\n", filename);
    exit(1);
  }

  fprintf(file, "feature.name1.enabled=0\n");
  fprintf(file, "feature.name2.enabled=1\n");
  fprintf(file, "feature.name3.enabled=1klajdflkajdsflk\n");
  fprintf(file, "feature.name4.enabled=asdkjfasdkljfklsdjf0\n");
  fprintf(file, "feature.name5.enabled=-1\n");
  fprintf(file, "feature.name6.enabled=2\n");
  fprintf(file, "feature.name7.enabled=true\n");
  fprintf(file, "feature.name8.enabled=True\n");
  fclose(file);
  read_config(filename, &exec_cfg);
  cfg = *(get_configuration_section("", &exec_cfg));

  validate_feature_enabled_value(disabled, "feature.name1.enabled",
      disabled, &cfg);
  validate_feature_enabled_value(enabled, "feature.name2.enabled",
          disabled, &cfg);
  validate_feature_enabled_value(disabled, "feature.name3.enabled",
          disabled, &cfg);
  validate_feature_enabled_value(disabled, "feature.name4.enabled",
          disabled, &cfg);
  validate_feature_enabled_value(enabled, "feature.name5.enabled",
          enabled, &cfg);
  validate_feature_enabled_value(disabled, "feature.name6.enabled",
          disabled, &cfg);
  validate_feature_enabled_value(enabled, "feature.name7.enabled",
          disabled, &cfg);
  validate_feature_enabled_value(enabled, "feature.name8.enabled",
          disabled, &cfg);


  free_configuration(&exec_cfg);
}

void test_delete_user() {
  printf("\nTesting delete_user\n");
  char* app_dir = get_app_directory(TEST_ROOT "/local-1", yarn_username, "app_3");
  if (mkdirs(app_dir, 0700) != 0) {
    exit(1);
  }

  char buffer[100000];
  sprintf(buffer, "%s/test.cfg", app_dir);
  if (write_config_file(buffer, 1) != 0) {
    exit(1);
  }

  char * dirs[] = {buffer, 0};
  int ret = delete_as_user(yarn_username, "file1" , dirs);
  if (ret == 0) {
    printf("FAIL: if baseDir is a file, delete_as_user should fail if a subdir is also passed\n");
    exit(1);
  }

  // Pass a file to delete_as_user in the baseDirs parameter. The file should
  // be deleted.
  ret = delete_as_user(yarn_username, "" , dirs);
  if (ret != 0) {
    printf("FAIL: delete_as_user could not delete baseDir when baseDir is a file: return code is %d\n", ret);
    exit(1);
  }

  sprintf(buffer, "%s", app_dir);
  char missing_dir[20];
  strcpy(missing_dir, "/some/missing/dir");
  char * dirs_with_missing[] = {missing_dir, buffer, 0};
  ret = delete_as_user(yarn_username, "" , dirs_with_missing);
  printf("%d" , ret);
  if (access(buffer, R_OK) == 0) {
    printf("FAIL: directory not deleted\n");
    exit(1);
  }

  sprintf(buffer, "%s/local-1/usercache/%s", TEST_ROOT, yarn_username);
  if (access(buffer, R_OK) != 0) {
    printf("FAIL: directory missing before test\n");
    exit(1);
  }
  if (delete_as_user(yarn_username, buffer, NULL) != 0) {
    exit(1);
  }
  if (access(buffer, R_OK) == 0) {
    printf("FAIL: directory not deleted\n");
    exit(1);
  }
  if (access(TEST_ROOT "/local-1", R_OK) != 0) {
    printf("FAIL: local-1 directory does not exist\n");
    exit(1);
  }
  free(app_dir);
}

/**
 * Read a file and tokenize it on newlines.  Place up to max lines into lines.
 * The max+1st element of lines will be set to NULL.
 *
 * @param file the name of the file to open
 * @param lines the pointer array into which to place the lines
 * @param max the max number of lines to add to lines
 */
void read_lines(const char* file, char **lines, size_t max) {
  char buf[4096];
  size_t nread;

  int fd = open(file, O_RDONLY);

  if (fd < 0) {
    printf("FAIL: failed to open directory listing file: %s\n", file);
    exit(1);
  } else {
    char *cur = buf;
    size_t count = sizeof buf;

    while ((nread = read(fd, cur, count)) > 0) {
      cur += nread;
      count -= nread;
    }

    if (nread < 0) {
      printf("FAIL: failed to read directory listing file: %s\n", file);
      exit(1);
    }

    close(fd);
  }

  char* entity = strtok(buf, "\n");
  int i;

  for (i = 0; i < max; i++) {
    if (entity == NULL) {
      break;
    }

    lines[i] = (char *)malloc(sizeof(char) * (strlen(entity) + 1));
    strcpy(lines[i], entity);
    entity = strtok(NULL, "\n");
  }

  lines[i] = NULL;
}

void test_list_as_user() {
  printf("\nTesting list_as_user\n");
  char buffer[4096];

  char *app_dir =
      get_app_directory(TEST_ROOT "/local-1", "yarn", "app_4");

  if (mkdirs(app_dir, 0700) != 0) {
    printf("FAIL: unble to create application directory: %s\n", app_dir);
    exit(1);
  }

  // Test with empty dir string
  strcpy(buffer, "");
  int ret = list_as_user(buffer);

  if (ret == 0) {
    printf("FAIL: did not fail on empty directory string\n");
    exit(1);
  }

  // Test with a non-existent directory
  sprintf(buffer, "%s/output", app_dir);

  ret = list_as_user(buffer);

  if (ret == 0) {
    printf("FAIL: did not fail on non-existent directory\n");
    exit(1);
  }

  // Write a couple files to list
  sprintf(buffer, "%s/file1", app_dir);

  if (write_config_file(buffer, 1) != 0) {
    exit(1);
  }

  sprintf(buffer, "%s/.file2", app_dir);

  if (write_config_file(buffer, 1) != 0) {
    exit(1);
  }

  // Also create a directory
  sprintf(buffer, "%s/output", app_dir);

  if (mkdirs(buffer, 0700) != 0) {
    exit(1);
  }

  // Test the regular case
  // Store a copy of stdout, then redirect it to a file
  sprintf(buffer, "%s/output/files", app_dir);

  int oldout = dup(STDOUT_FILENO);
  int fd = open(buffer, O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR);

  dup2(fd, STDOUT_FILENO);

  // Now list the files
  ret = list_as_user(app_dir);

  if (ret != 0) {
    printf("FAIL: unable to list files in regular case\n");
    exit(1);
  }

  // Restore stdout
  close(fd);
  dup2(oldout, STDOUT_FILENO);

  // Check the output -- shouldn't be more than a couple lines
  char *lines[16];

  read_lines(buffer, lines, 15);

  int got_file1 = 0;
  int got_file2 = 0;
  int got_output = 0;
  int i;

  for (i = 0; i < sizeof lines; i++) {
    if (lines[i] == NULL) {
      break;
    } else if (strcmp("file1", lines[i]) == 0) {
      got_file1 = 1;
    } else if (strcmp(".file2", lines[i]) == 0) {
      got_file2 = 1;
    } else if (strcmp("output", lines[i]) == 0) {
      got_output = 1;
    } else {
      printf("FAIL: listed extraneous file: %s\n", lines[i]);
      exit(1);
    }

    free(lines[i]);
  }

  if (!got_file1 || !got_file2 || !got_output) {
    printf("FAIL: missing files in listing\n");
    exit(1);
  }

  free(app_dir);
}

void run_test_in_child(const char* test_name, void (*func)()) {
  printf("\nRunning test %s in child process\n", test_name);
  fflush(stdout);
  fflush(stderr);
  pid_t child = fork();
  if (child == -1) {
    printf("FAIL: fork failed\n");
    exit(1);
  } else if (child == 0) {
    func();
    exit(0);
  } else {
    int status = 0;
    if (waitpid(child, &status, 0) == -1) {
      printf("FAIL: waitpid %" PRId64 " failed - %s\n", (int64_t)child, strerror(errno));
      exit(1);
    }
    if (!WIFEXITED(status)) {
      printf("FAIL: child %" PRId64 " didn't exit - %d\n", (int64_t)child, status);
      exit(1);
    }
    if (WEXITSTATUS(status) != 0) {
      printf("FAIL: child %" PRId64 " exited with bad status %d\n",
	     (int64_t)child, WEXITSTATUS(status));
      exit(1);
    }
  }
}

void test_signal_container_group() {
  printf("\nTesting group signal_container\n");
  fflush(stdout);
  fflush(stderr);
  pid_t child = fork();
  if (child == -1) {
    printf("FAIL: fork failed\n");
    exit(1);
  } else if (child == 0) {
    setpgid(0,0);
    if (change_user(user_detail->pw_uid, user_detail->pw_gid) != 0) {
      exit(1);
    }
    sleep(3600);
    exit(0);
  }
  printf("Child container launched as %" PRId64 "\n", (int64_t)child);
  // there's a race condition for child calling change_user and us
  // calling signal_container_as_user, hence sleeping
  sleep(3);
  if (signal_container_as_user(yarn_username, child, SIGKILL) != 0) {
    exit(1);
  }
  int status = 0;
  if (waitpid(child, &status, 0) == -1) {
    printf("FAIL: waitpid failed - %s\n", strerror(errno));
    exit(1);
  }
  if (!WIFSIGNALED(status)) {
    printf("FAIL: child wasn't signalled - %d\n", status);
    exit(1);
  }
  if (WTERMSIG(status) != SIGKILL) {
    printf("FAIL: child was killed with %d instead of %d\n",
	   WTERMSIG(status), SIGKILL);
    exit(1);
  }
}

void test_init_app() {
  printf("\nTesting init app\n");
  if (seteuid(0) != 0) {
    printf("FAIL: seteuid to root failed - %s\n", strerror(errno));
    exit(1);
  }
  FILE* creds = fopen(TEST_ROOT "/creds.txt", "w");
  if (creds == NULL) {
    printf("FAIL: failed to create credentials file - %s\n", strerror(errno));
    exit(1);
  }
  if (fprintf(creds, "secret key\n") < 0) {
    printf("FAIL: fprintf failed - %s\n", strerror(errno));
    exit(1);
  }
  if (fclose(creds) != 0) {
    printf("FAIL: fclose failed - %s\n", strerror(errno));
    exit(1);
  }
  FILE* job_xml = fopen(TEST_ROOT "/job.xml", "w");
  if (job_xml == NULL) {
    printf("FAIL: failed to create job file - %s\n", strerror(errno));
    exit(1);
  }
  if (fprintf(job_xml, "<jobconf/>\n") < 0) {
    printf("FAIL: fprintf failed - %s\n", strerror(errno));
    exit(1);
  }
  if (fclose(job_xml) != 0) {
    printf("FAIL: fclose failed - %s\n", strerror(errno));
    exit(1);
  }
  if (seteuid(user_detail->pw_uid) != 0) {
    printf("FAIL: failed to seteuid back to user - %s\n", strerror(errno));
    exit(1);
  }
  fflush(stdout);
  fflush(stderr);
  pid_t child = fork();
  if (child == -1) {
    printf("FAIL: failed to fork process for init_app - %s\n",
	   strerror(errno));
    exit(1);
  } else if (child == 0) {
    char *final_pgm[] = {"touch", "my-touch-file", 0};
    if (initialize_app(yarn_username, "app_4", "container_1",
                       TEST_ROOT "/creds.txt",
                       local_dirs, log_dirs, final_pgm) != 0) {
      printf("FAIL: failed in child\n");
      exit(42);
    }
    // should never return
    exit(1);
  }
  int status = 0;
  if (waitpid(child, &status, 0) <= 0) {
    printf("FAIL: failed waiting for process %" PRId64 " - %s\n", (int64_t)child,
	   strerror(errno));
    exit(1);
  }
  if (access(TEST_ROOT "/logs/userlogs/app_4", R_OK) != 0) {
    printf("FAIL: failed to create app log directory\n");
    exit(1);
  }
  char* app_dir = get_app_directory(TEST_ROOT "/local-1", yarn_username, "app_4");
  if (access(app_dir, R_OK) != 0) {
    printf("FAIL: failed to create app directory %s\n", app_dir);
    exit(1);
  }
  char buffer[100000];
  sprintf(buffer, "%s/creds.txt", app_dir);
  if (access(buffer, R_OK) != 0) {
    printf("FAIL: failed to create credentials %s\n", buffer);
    exit(1);
  }
  sprintf(buffer, "%s/my-touch-file", app_dir);
  if (access(buffer, R_OK) != 0) {
    printf("FAIL: failed to create touch file %s\n", buffer);
    exit(1);
  }
  free(app_dir);
  app_dir = get_app_log_directory(TEST_ROOT "/logs/userlogs","app_4");
  if (access(app_dir, R_OK) != 0) {
    printf("FAIL: failed to create app log directory %s\n", app_dir);
    exit(1);
  }
  free(app_dir);

  char *container_dir = get_container_log_directory(TEST_ROOT "/logs/userlogs",
                  "app_4", "container_1");
  if (container_dir != NULL && access(container_dir, R_OK) != 0) {
    printf("FAIL: failed to create container log directory %s\n", container_dir);
    exit(1);
  }
  free(container_dir);
}

void test_run_container() {
  printf("\nTesting run container\n");
  if (seteuid(0) != 0) {
    printf("FAIL: seteuid to root failed - %s\n", strerror(errno));
    exit(1);
  }
  FILE* creds = fopen(TEST_ROOT "/creds.txt", "w");
  if (creds == NULL) {
    printf("FAIL: failed to create credentials file - %s\n", strerror(errno));
    exit(1);
  }
  if (fprintf(creds, "secret key\n") < 0) {
    printf("FAIL: fprintf failed - %s\n", strerror(errno));
    exit(1);
  }
  if (fclose(creds) != 0) {
    printf("FAIL: fclose failed - %s\n", strerror(errno));
    exit(1);
  }

  char * cgroups_pids[] = { TEST_ROOT "/cgroups-pid1.txt", TEST_ROOT "/cgroups-pid2.txt", 0 };
  close(creat(cgroups_pids[0], O_RDWR));
  close(creat(cgroups_pids[1], O_RDWR));

  const char* script_name = TEST_ROOT "/container-script";
  FILE* script = fopen(script_name, "w");
  if (script == NULL) {
    printf("FAIL: failed to create script file - %s\n", strerror(errno));
    exit(1);
  }
  if (seteuid(user_detail->pw_uid) != 0) {
    printf("FAIL: failed to seteuid back to user - %s\n", strerror(errno));
    exit(1);
  }
  if (fprintf(script, "#!/usr/bin/env bash\n"
                     "touch foobar\n"
                     "exit 0") < 0) {
    printf("FAIL: fprintf failed - %s\n", strerror(errno));
    exit(1);
  }
  if (fclose(script) != 0) {
    printf("FAIL: fclose failed - %s\n", strerror(errno));
    exit(1);
  }
  fflush(stdout);
  fflush(stderr);
  char* container_dir = get_container_work_directory(TEST_ROOT "/local-1",
					      yarn_username, "app_4", "container_1");
  const char * pid_file = TEST_ROOT "/pid.txt";

  pid_t child = fork();
  if (child == -1) {
    printf("FAIL: failed to fork process for init_app - %s\n",
	   strerror(errno));
    exit(1);
  } else if (child == 0) {
    if (launch_container_as_user(yarn_username, "app_4", "container_1",
          container_dir, script_name, TEST_ROOT "/creds.txt", pid_file,
          local_dirs, log_dirs,
          "cgroups", cgroups_pids) != 0) {
      printf("FAIL: failed in child\n");
      exit(42);
    }
    // should never return
    exit(1);
  }
  int status = 0;
  if (waitpid(child, &status, 0) <= 0) {
    printf("FAIL: failed waiting for process %" PRId64 " - %s\n", (int64_t)child,
	   strerror(errno));
    exit(1);
  }
  if (access(TEST_ROOT "/logs/userlogs/app_4/container_1", R_OK) != 0) {
    printf("FAIL: failed to create container log directory\n");
    exit(1);
  }
  if (access(container_dir, R_OK) != 0) {
    printf("FAIL: failed to create container directory %s\n", container_dir);
    exit(1);
  }
  char buffer[100000];
  sprintf(buffer, "%s/foobar", container_dir);
  if (access(buffer, R_OK) != 0) {
    printf("FAIL: failed to create touch file %s\n", buffer);
    exit(1);
  }
  free(container_dir);
  container_dir = get_app_log_directory(TEST_ROOT "/logs/userlogs", "app_4/container_1");
  if (access(container_dir, R_OK) != 0) {
    printf("FAIL: failed to create app log directory %s\n", container_dir);
    exit(1);
  }
  free(container_dir);

  if (seteuid(0) != 0) {
    printf("FAIL: seteuid to root failed - %s\n", strerror(errno));
    exit(1);
  }

  check_pid_file(pid_file, child);
  check_pid_file(cgroups_pids[0], child);
  check_pid_file(cgroups_pids[1], child);
}

static void mkdir_or_die(const char *path) {
  if (mkdir(path, 0777) < 0) {
    int err = errno;
    printf("mkdir(%s) failed: %s\n", path, strerror(err));
    exit(1);
  }
}

static void touch_or_die(const char *path) {
  FILE* f = fopen(path, "w");
  if (!f) {
    int err = errno;
    printf("fopen(%s, w) failed: %s\n", path, strerror(err));
    exit(1);
  }
  if (fclose(f) < 0) {
    int err = errno;
    printf("fclose(%s) failed: %s\n", path, strerror(err));
    exit(1);
  }
}

static void symlink_or_die(const char *old, const char *new) {
  if (symlink(old, new) < 0) {
    int err = errno;
    printf("symlink(%s, %s) failed: %s\n", old, new, strerror(err));
    exit(1);
  }
}

static void expect_type(const char *path, int mode) {
  struct stat st_buf;

  if (stat(path, &st_buf) < 0) {
    int err = errno;
    if (err == ENOENT) {
      if (mode == 0) {
        return;
      }
      printf("expect_type(%s): stat failed unexpectedly: %s\n",
             path, strerror(err));
      exit(1);
    }
  }
  if (mode == 0) {
    printf("expect_type(%s): we expected the file to be gone, but it "
           "existed.\n", path);
    exit(1);
  }
  if (!(st_buf.st_mode & mode)) {
    printf("expect_type(%s): the file existed, but it had mode 0%4o, "
           "which didn't have bit 0%4o\n", path, st_buf.st_mode, mode);
    exit(1);
  }
}

static void test_delete_race_internal() {
  char* app_dir = get_app_directory(TEST_ROOT "/local-2", yarn_username, "app_1");
  char* container_dir = get_container_work_directory(TEST_ROOT "/local-2",
                          yarn_username, "app_1", "container_1");
  char buffer[100000];

  sprintf(buffer, "mkdir -p %s/a/b/c/d", container_dir);
  run(buffer);
  int i;
  for (i = 0; i < 100; ++i) {
    sprintf(buffer, "%s/a/f%d", container_dir, i);
    touch_or_die(buffer);
    sprintf(buffer, "%s/a/b/f%d", container_dir, i);
    touch_or_die(buffer);
    sprintf(buffer, "%s/a/b/c/f%d", container_dir, i);
    touch_or_die(buffer);
    sprintf(buffer, "%s/a/b/c/d/f%d", container_dir, i);
    touch_or_die(buffer);
  }

  pid_t child = fork();
  if (child == -1) {
    printf("FAIL: fork failed\n");
    exit(1);
  } else if (child == 0) {
    // delete container directory
    char * dirs[] = {app_dir, 0};
    int ret = delete_as_user(yarn_username, "container_1" , dirs);
    if (ret != 0) {
      printf("FAIL: return code from delete_as_user is %d\n", ret);
      exit(1);
    }
    exit(0);
  } else {
    // delete application directory
    int ret = delete_as_user(yarn_username, app_dir, NULL);
    int status = 0;
    if (waitpid(child, &status, 0) == -1) {
      printf("FAIL: waitpid %" PRId64 " failed - %s\n", (int64_t)child, strerror(errno));
      exit(1);
    }
    if (!WIFEXITED(status)) {
      printf("FAIL: child %" PRId64 " didn't exit - %d\n", (int64_t)child, status);
      exit(1);
    }
    if (WEXITSTATUS(status) != 0) {
      printf("FAIL: child %" PRId64 " exited with bad status %d\n",
             (int64_t)child, WEXITSTATUS(status));
      exit(1);
    }
    if (ret != 0) {
      printf("FAIL: return code from delete_as_user is %d\n", ret);
      exit(1);
    }
  }

  // check to make sure the app directory is gone
  if (access(app_dir, R_OK) == 0) {
    printf("FAIL: didn't delete the directory - %s\n", app_dir);
    exit(1);
  }

  free(app_dir);
  free(container_dir);
}

void test_delete_race() {
  if (initialize_user(yarn_username, local_dirs)) {
    printf("FAIL: failed to initialize user %s\n", yarn_username);
    exit(1);
  }
  int i;
  for (i = 0; i < 100; ++i) {
    test_delete_race_internal();
  }
}

int recursive_unlink_children(const char *name);

void test_recursive_unlink_children() {
  int ret;

  mkdir_or_die(TEST_ROOT "/unlinkRoot");
  mkdir_or_die(TEST_ROOT "/unlinkRoot/a");
  touch_or_die(TEST_ROOT "/unlinkRoot/b");
  mkdir_or_die(TEST_ROOT "/unlinkRoot/c");
  touch_or_die(TEST_ROOT "/unlinkRoot/c/d");
  touch_or_die(TEST_ROOT "/external");
  symlink_or_die(TEST_ROOT "/external",
                 TEST_ROOT "/unlinkRoot/c/external");
  ret = recursive_unlink_children(TEST_ROOT "/unlinkRoot");
  if (ret != 0) {
    printf("recursive_unlink_children(%s) failed: error %d\n",
           TEST_ROOT "/unlinkRoot", ret);
    exit(1);
  }
  // unlinkRoot should still exist.
  expect_type(TEST_ROOT "/unlinkRoot", S_IFDIR);
  // Other files under unlinkRoot should have been deleted.
  expect_type(TEST_ROOT "/unlinkRoot/a", 0);
  expect_type(TEST_ROOT "/unlinkRoot/b", 0);
  expect_type(TEST_ROOT "/unlinkRoot/c", 0);
  // We shouldn't have followed the symlink.
  expect_type(TEST_ROOT "/external", S_IFREG);
  // Clean up.
  if (rmdir(TEST_ROOT "/unlinkRoot") < 0) {
    int err = errno;
    printf("failed to rmdir " TEST_ROOT "/unlinkRoot: %s\n", strerror(err));
  }
  if (unlink(TEST_ROOT "/external") < 0) {
    int err = errno;
    printf("failed to unlink " TEST_ROOT "/external: %s\n", strerror(err));
  }
}


/**
 * This test is used to verify that trim() works correctly
 */
void test_trim_function() {
  char* trimmed = NULL;

  printf("\nTesting trim function\n");

  // Check NULL input
  if (trim(NULL) != NULL) {
    printf("FAIL: trim(NULL) should be NULL\n");
    exit(1);
  }

  // Check empty input
  trimmed = trim("");
  if (strcmp(trimmed, "") != 0) {
    printf("FAIL: trim(\"\") should be \"\"\n");
    exit(1);
  }
  free(trimmed);

  // Check single space input
  trimmed = trim(" ");
  if (strcmp(trimmed, "") != 0) {
    printf("FAIL: trim(\" \") should be \"\"\n");
    exit(1);
  }
  free(trimmed);

  // Check multi space input
  trimmed = trim("   ");
  if (strcmp(trimmed, "") != 0) {
    printf("FAIL: trim(\"   \") should be \"\"\n");
    exit(1);
  }
  free(trimmed);

  // Check both side trim input
  trimmed = trim(" foo ");
  if (strcmp(trimmed, "foo") != 0) {
    printf("FAIL: trim(\" foo \") should be \"foo\"\n");
    exit(1);
  }
  free(trimmed);

  // Check left side trim input
  trimmed = trim("foo   ");
  if (strcmp(trimmed, "foo") != 0) {
    printf("FAIL: trim(\"foo   \") should be \"foo\"\n");
    exit(1);
  }
  free(trimmed);

  // Check right side trim input
  trimmed = trim("   foo");
  if (strcmp(trimmed, "foo") != 0) {
    printf("FAIL: trim(\"   foo\") should be \"foo\"\n");
    exit(1);
  }
  free(trimmed);

  // Check no trim input
  trimmed = trim("foo");
  if (strcmp(trimmed, "foo") != 0) {
    printf("FAIL: trim(\"foo\") should be \"foo\"\n");
    exit(1);
  }
  free(trimmed);
}

int is_empty(char *name);

void test_is_empty() {
  printf("\nTesting is_empty function\n");
  if (is_empty("/")) {
    printf("FAIL: / should not be empty\n");
    exit(1);
  }
  char *noexist = TEST_ROOT "/noexist";
  if (is_empty(noexist)) {
    printf("%s should not exist\n", noexist);
    exit(1);
  }
  char *emptydir = TEST_ROOT "/emptydir";
  mkdir(emptydir, S_IRWXU);
  if (!is_empty(emptydir)) {
    printf("FAIL: %s should be empty\n", emptydir);
    exit(1);
  }
}

#define TCE_FAKE_CGROOT TEST_ROOT "/cgroup_root"
#define TCE_NUM_CG_CONTROLLERS 6
extern int clean_docker_cgroups_internal(const char *mount_table,
                                  const char *yarn_hierarchy,
                                  const char* container_id);

void test_cleaning_docker_cgroups() {
  const char *controllers[TCE_NUM_CG_CONTROLLERS] = { "blkio", "cpu", "cpuset", "devices", "memory", "systemd" };
  const char *yarn_hierarchy = "hadoop-yarn";
  const char *fake_mount_table = TEST_ROOT "/fake_mounts";
  const char *container_id = "container_1410901177871_0001_01_000005";
  const char *other_container_id = "container_e17_1410901177871_0001_01_000005";
  char cgroup_paths[TCE_NUM_CG_CONTROLLERS][PATH_MAX];
  char container_paths[TCE_NUM_CG_CONTROLLERS][PATH_MAX];
  char other_container_paths[TCE_NUM_CG_CONTROLLERS][PATH_MAX];

  printf("\nTesting clean_docker_cgroups\n");

  // Setup fake mount table
  FILE *file;
  file = fopen(fake_mount_table, "w");
  if (file == NULL) {
    printf("Failed to open %s.\n", fake_mount_table);
    exit(1);
  }
  fprintf(file, "rootfs " TEST_ROOT "/fake_root rootfs rw 0 0\n");
  fprintf(file, "sysfs " TEST_ROOT "/fake_sys sysfs rw,nosuid,nodev,noexec,relatime 0 0\n");
  fprintf(file, "proc " TEST_ROOT "/fake_proc proc rw,nosuid,nodev,noexec,relatime 0 0\n");
  for (int i = 0; i < TCE_NUM_CG_CONTROLLERS; i++) {
    fprintf(file, "cgroup %s/%s cgroup rw,nosuid,nodev,noexec,relatime,%s 0 0\n",
            TCE_FAKE_CGROOT, controllers[i], controllers[i]);
  }
  fprintf(file, "/dev/vda " TEST_ROOT "/fake_root ext4 rw,relatime,data=ordered 0 0\n");
  fclose(file);

  // Test with null inputs
  int ret = clean_docker_cgroups_internal(NULL, yarn_hierarchy, container_id);
  if (ret != -1) {
    printf("FAIL: clean_docker_cgroups_internal with NULL mount table should fail\n");
    exit(1);
  }
  ret = clean_docker_cgroups_internal(fake_mount_table, NULL, container_id);
  if (ret != -1) {
    printf("FAIL: clean_docker_cgroups_internal with NULL yarn_hierarchy should fail\n");
    exit(1);
  }
  ret = clean_docker_cgroups_internal(fake_mount_table, yarn_hierarchy, NULL);
  if (ret != -1) {
    printf("FAIL: clean_docker_cgroups_internal with NULL container_id should fail\n");
    exit(1);
  }

  // Test with invalid container_id
  ret = clean_docker_cgroups_internal(fake_mount_table, yarn_hierarchy, "not_a_container_123");
  if (ret != -1) {
    printf("FAIL: clean_docker_cgroups_internal with invalid container_id should fail\n");
    exit(1);
  }
  if (mkdir(TCE_FAKE_CGROOT, 0755) != 0) {
    printf("FAIL: failed to mkdir " TCE_FAKE_CGROOT "\n");
    exit(1);
  }
  for (int i = 0; i < TCE_NUM_CG_CONTROLLERS; i++) {
    snprintf(cgroup_paths[i], PATH_MAX, TCE_FAKE_CGROOT "/%s/%s", controllers[i], yarn_hierarchy);
    if (mkdirs(cgroup_paths[i], 0755) != 0) {
      printf("FAIL: failed to mkdir %s\n", cgroup_paths[i]);
      exit(1);
    }
  }
  for (int i = 0; i < TCE_NUM_CG_CONTROLLERS; i++) {
    DIR *dir = NULL;
    dir = opendir(cgroup_paths[i]);
    if (dir == NULL) {
      printf("FAIL: failed to open dir %s\n", cgroup_paths[i]);
      exit(1);
    }
    closedir(dir);
  }
  // Test before creating any containers
  ret = clean_docker_cgroups_internal(fake_mount_table, yarn_hierarchy, container_id);
  if (ret != 0) {
    printf("FAIL: failed to clean cgroups: mt=%s, yh=%s, cId=%s\n",
           fake_mount_table, yarn_hierarchy, container_id);
  }
  // make sure hadoop-yarn dirs are still there
  for (int i = 0; i < TCE_NUM_CG_CONTROLLERS; i++) {
    DIR *dir = NULL;
    dir = opendir(cgroup_paths[i]);
    if (dir == NULL) {
      printf("FAIL: failed to open dir %s\n", cgroup_paths[i]);
      exit(1);
    }
    closedir(dir);
  }
  // Create container dirs
  for (int i = 0; i < TCE_NUM_CG_CONTROLLERS; i++) {
    snprintf(container_paths[i], PATH_MAX, TCE_FAKE_CGROOT "/%s/%s/%s",
            controllers[i], yarn_hierarchy, container_id);
    if (mkdirs(container_paths[i], 0755) != 0) {
      printf("FAIL: failed to mkdir %s\n", container_paths[i]);
      exit(1);
    }
    snprintf(other_container_paths[i], PATH_MAX, TCE_FAKE_CGROOT "/%s/%s/%s",
            controllers[i], yarn_hierarchy, other_container_id);
    if (mkdirs(other_container_paths[i], 0755) != 0) {
      printf("FAIL: failed to mkdir %s\n", other_container_paths[i]);
      exit(1);
    }
  }
  ret = clean_docker_cgroups_internal(fake_mount_table, yarn_hierarchy, container_id);
  // make sure hadoop-yarn dirs are still there
  for (int i = 0; i < TCE_NUM_CG_CONTROLLERS; i++) {
    DIR *dir = NULL;
    dir = opendir(cgroup_paths[i]);
    if (dir == NULL) {
      printf("FAIL: failed to open dir %s\n", cgroup_paths[i]);
      exit(1);
    }
    closedir(dir);
  }
  // make sure container dirs deleted
  for (int i = 0; i < TCE_NUM_CG_CONTROLLERS; i++) {
    DIR *dir = NULL;
    dir = opendir(container_paths[i]);
    if (dir != NULL) {
      printf("FAIL: container cgroup %s not deleted\n", container_paths[i]);
      exit(1);
    }
    closedir(dir);
  }
  // make sure other container dirs are still there
  for (int i = 0; i < TCE_NUM_CG_CONTROLLERS; i++) {
    DIR *dir = NULL;
    dir = opendir(other_container_paths[i]);
    if (dir == NULL) {
      printf("FAIL: container cgroup %s should not be deleted\n", other_container_paths[i]);
      exit(1);
    }
    closedir(dir);
  }
}

// This test is expected to be executed either by a regular
// user or by root. If executed by a regular user it doesn't
// test all the functions that would depend on changing the
// effective user id. If executed by a super-user everything
// gets tested. Here are different ways of execing the test binary:
// 1. regular user assuming user == yarn user
//    $ test-container-executor
// 2. regular user with a given yarn user
//    $ test-container-executor yarn_user
// 3. super user with a given user and assuming user == yarn user
//    # test-container-executor user
// 4. super user with a given user and a given yarn user
//    # test-container-executor user yarn_user
int main(int argc, char **argv) {
  int ret;
  LOGFILE = stdout;
  ERRORFILE = stderr;

  nm_uid = getuid();

  printf("Attempting to clean up from any previous runs\n");
  // clean up any junk from previous run
  if (system("chmod -R u=rwx " TEST_ROOT "; rm -fr " TEST_ROOT)) {
    exit(1);
  }

  if (mkdirs(TEST_ROOT "/logs/userlogs", 0755) != 0) {
    exit(1);
  }

  if (write_config_file(TEST_ROOT "/test.cfg", 1) != 0) {
    exit(1);
  }

  printf("\nOur executable is %s\n",get_executable(argv[0]));

  read_executor_config(TEST_ROOT "/test.cfg");

  local_dirs = split(strdup(NM_LOCAL_DIRS));
  log_dirs = split(strdup(NM_LOG_DIRS));

  create_nm_roots(local_dirs);

  // See the description above of various ways this test
  // can be executed in order to understand the following logic
  char* current_username = strdup(getpwuid(getuid())->pw_name);
  if (getuid() == 0 && (argc == 2 || argc == 3)) {
    username = argv[1];
    yarn_username = (argc == 3) ? argv[2] : argv[1];
  } else {
    username = current_username;
    yarn_username = (argc == 2) ? argv[1] : current_username;
  }
  set_nm_uid(geteuid(), getegid());

  if (set_user(username)) {
    exit(1);
  }

  printf("\nStarting tests\n");

  printf("\ntest_is_empty()\n");
  test_is_empty();

  printf("\nTesting recursive_unlink_children()\n");
  test_recursive_unlink_children();

  printf("\nTesting resolve_config_path()\n");
  test_resolve_config_path();

  printf("\nTesting get_user_directory()\n");
  test_get_user_directory();

  printf("\nTesting check_nm_local_dir()\n");
  test_check_nm_local_dir();

  printf("\nTesting get_app_directory()\n");
  test_get_app_directory();

  printf("\nTesting get_container_directory()\n");
  test_get_container_directory();

  printf("\nTesting get_container_launcher_file()\n");
  test_get_container_launcher_file();

  printf("\nTesting get_app_log_dir()\n");
  test_get_app_log_dir();

  test_check_configuration_permissions();

  printf("\nTesting delete_container()\n");
  test_delete_container();

  printf("\nTesting delete_app()\n");
  test_delete_app();

  printf("\nTesting delete race\n");
  test_delete_race();

  printf("\nTesting is_feature_enabled()\n");
  test_is_feature_enabled();

  test_check_user(0);

  test_cleaning_docker_cgroups();

#ifdef __APPLE__
   printf("OS X: disabling CrashReporter\n");
  /*
   * disable the "unexpectedly quit" dialog box
   * because we know we're going to make our container
   * do exactly that.
   */
  CFStringRef crashType      = CFSTR("DialogType");
  CFStringRef crashModeNone  = CFSTR("None");
  CFStringRef crashAppID     = CFSTR("com.apple.CrashReporter");
  CFStringRef crashOldMode   = CFPreferencesCopyAppValue(CFSTR("DialogType"), CFSTR("com.apple.CrashReporter"));

  CFPreferencesSetAppValue(crashType, crashModeNone, crashAppID);
  CFPreferencesAppSynchronize(crashAppID);
#endif

  // the tests that change user need to be run in a subshell, so that
  // when they change user they don't give up our privs
  run_test_in_child("test_signal_container_group", test_signal_container_group);

#ifdef __APPLE__
  /*
   * put the "unexpectedly quit" dialog back
   */

  CFPreferencesSetAppValue(crashType, crashOldMode, crashAppID);
  CFPreferencesAppSynchronize(crashAppID);
  printf("OS X: CrashReporter re-enabled\n");
#endif

  // init app and run container can't be run if you aren't testing as root
  if (getuid() == 0) {
    // these tests do internal forks so that the change_owner and execs
    // don't mess up our process.
    test_init_app();
    test_run_container();
  }

  /*
   * try to seteuid(0).  if it doesn't work, carry on anyway.
   * we're going to capture the return value to get rid of a 
   * compiler warning.
   */
  ret=seteuid(0);
  ret++;
  // test_delete_user must run as root since that's how we use the delete_as_user
  test_delete_user();
  free_executor_configurations();

  printf("\nTrying banned default user()\n");
  if (write_config_file(TEST_ROOT "/test.cfg", 0) != 0) {
    exit(1);
  }

  read_executor_config(TEST_ROOT "/test.cfg");
#ifdef __APPLE__
  username = "_uucp";
  test_check_user(1);

  username = "_networkd";
  test_check_user(1);
#else
  username = "bin";
  test_check_user(1);

  username = "sys";
  test_check_user(1);
#endif

  test_trim_function();
  printf("\nFinished tests\n");

  free(current_username);
  free_executor_configurations();

  return 0;
}
