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

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <gtest/gtest.h>
#include <sstream>

extern "C" {
#include "configuration.h"
#include "container-executor.h"
#include "modules/cgroups/cgroups-operations.h"
#include "test/test-container-executor-common.h"
#include "util.h"
}

namespace ContainerExecutor {

class TestCGroupsModule : public ::testing::Test {
protected:
  virtual void SetUp() {
    if (mkdirs(TEST_ROOT, 0755) != 0) {
      fprintf(ERRORFILE, "Failed to mkdir TEST_ROOT: %s\n", TEST_ROOT);
      exit(1);
    }
    LOGFILE = stdout;
    ERRORFILE = stderr;
  }

  virtual void TearDown() {}
};

TEST_F(TestCGroupsModule, test_cgroups_get_path_without_define_root) {
  // Write config file.
  const char *filename = TEST_ROOT "/test_cgroups_get_path_without_root.cfg";
  FILE *file = fopen(filename, "w");
  if (file == NULL) {
    printf("FAIL: Could not open configuration file: %s\n", filename);
    exit(1);
  }
  fprintf(file, "[cgroups]\n");
  fprintf(file, "yarn-hierarchy=yarn\n");
  fclose(file);

  // Read config file
  read_executor_config(filename);
  reload_cgroups_configuration();

  char* path = get_cgroups_path_to_write("devices", "deny", "container_1");

  ASSERT_TRUE(NULL == path) << "Should fail.\n";

  free_executor_configurations();
}

TEST_F(TestCGroupsModule, test_cgroups_get_path_without_define_yarn_hierarchy) {
  // Write config file.
  const char *filename = TEST_ROOT "/test_cgroups_get_path_without_root.cfg";
  FILE *file = fopen(filename, "w");

  ASSERT_TRUE(file) << "FAIL: Could not open configuration file: " << filename
                    << "\n";
  fprintf(file, "[cgroups]\n");
  fprintf(file, "root=/sys/fs/cgroups\n");
  fclose(file);

  // Read config file
  read_executor_config(filename);
  reload_cgroups_configuration();
  char* path = get_cgroups_path_to_write("devices", "deny", "container_1");

  ASSERT_TRUE(NULL == path) << "Should fail.\n";

  free_executor_configurations();
}

TEST_F(TestCGroupsModule, test_cgroups_get_path_succeeded) {
  // Write config file.
  const char *filename = TEST_ROOT "/test_cgroups_get_path.cfg";
  FILE *file = fopen(filename, "w");

  ASSERT_TRUE(file) << "FAIL: Could not open configuration file\n";
  fprintf(file, "[cgroups]\n");
  fprintf(file, "root=/sys/fs/cgroups \n");
  fprintf(file, "yarn-hierarchy=yarn \n");
  fclose(file);

  // Read config file
  read_executor_config(filename);
  reload_cgroups_configuration();

  char* path = get_cgroups_path_to_write("devices", "deny", "container_1");
  ASSERT_TRUE(NULL != path) << "Should success.\n";

  const char *EXPECTED =
      "/sys/fs/cgroups/devices/yarn/container_1/devices.deny";

  ASSERT_STREQ(EXPECTED, path)
      << "Return cgroup-path-to-write is not expected\n";

  free(path);

  free_executor_configurations();
}
} // namespace ContainerExecutor