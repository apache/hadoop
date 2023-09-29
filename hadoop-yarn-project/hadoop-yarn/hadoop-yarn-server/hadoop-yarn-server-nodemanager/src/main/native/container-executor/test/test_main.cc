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

#include <gtest/gtest.h>
#include <cstdio>
#include <pwd.h>

extern "C" {
#include "util.h"
#include "container-executor.h"
}

#define TMPDIR "/tmp"
#define TEST_ROOT TMPDIR "/test-container-executor"

int write_config_file(const char *file_name, int banned) {
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
  fprintf(file, "feature.yarn.sysfs.enabled=1\n");
  fclose(file);
  return 0;
}

int main(int argc, char **argv) {
  ERRORFILE = stderr;
  LOGFILE = stdout;

  printf("\nMaking test dir\n");
  if (mkdirs(TEST_ROOT, 0755) != 0) {
    exit(1);
  }
  if (chmod(TEST_ROOT, 0755) != 0) {    // in case of umask
    exit(1);
  }

  // We need a valid config before the test really starts for the check_user
  // and set_user calls
  printf("\nCreating test.cfg\n");
  if (write_config_file(TEST_ROOT "/test.cfg", 1) != 0) {
    exit(1);
  }
  printf("\nLoading test.cfg\n");
  read_executor_config(TEST_ROOT "/test.cfg");

  printf("\nDetermining user details\n");
  char* username = strdup(getpwuid(getuid())->pw_name);
  struct passwd *username_info = check_user(username);
  printf("\nSetting NM UID\n");
  set_nm_uid(username_info->pw_uid, username_info->pw_gid);

  // Make sure that username owns all the files now
  printf("\nEnsuring ownership of test dir\n");
  if (chown(TEST_ROOT, username_info->pw_uid, username_info->pw_gid) != 0) {
    exit(1);
  }
  if (chown(TEST_ROOT "/test.cfg",
        username_info->pw_uid, username_info->pw_gid) != 0) {
    exit(1);
  }

  printf("\nSetting effective user\n");
  if (set_user(username)) {
    exit(1);
  }

  testing::InitGoogleTest(&argc, argv);
  int rc = RUN_ALL_TESTS();

  printf("Attempting to clean up from any previous runs\n");
  // clean up any junk from previous run
  if (system("chmod -R u=rwx " TEST_ROOT "; rm -fr " TEST_ROOT)) {
    exit(1);

  return rc;
  }
}
