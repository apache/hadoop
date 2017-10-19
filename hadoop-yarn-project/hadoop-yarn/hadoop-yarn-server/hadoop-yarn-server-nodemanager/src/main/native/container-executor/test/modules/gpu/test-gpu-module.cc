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

#include <vector>

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
#include "modules/gpu/gpu-module.h"
#include "test/test-container-executor-common.h"
#include "util.h"
}

namespace ContainerExecutor {

class TestGpuModule : public ::testing::Test {
protected:
  virtual void SetUp() {
    if (mkdirs(TEST_ROOT, 0755) != 0) {
      fprintf(ERRORFILE, "Failed to mkdir TEST_ROOT: %s\n", TEST_ROOT);
      exit(1);
    }
    LOGFILE = stdout;
    ERRORFILE = stderr;
  }

  virtual void TearDown() {

  }
};

static std::vector<const char*> cgroups_parameters_invoked;

static int mock_update_cgroups_parameters(
   const char* controller_name,
   const char* param_name,
   const char* group_id,
   const char* value) {
  char* buf = (char*) malloc(128);
  strcpy(buf, controller_name);
  cgroups_parameters_invoked.push_back(buf);

  buf = (char*) malloc(128);
  strcpy(buf, param_name);
  cgroups_parameters_invoked.push_back(buf);

  buf = (char*) malloc(128);
  strcpy(buf, group_id);
  cgroups_parameters_invoked.push_back(buf);

  buf = (char*) malloc(128);
  strcpy(buf, value);
  cgroups_parameters_invoked.push_back(buf);
  return 0;
}

static void verify_param_updated_to_cgroups(
    int argc, const char** argv) {
  ASSERT_EQ(argc, cgroups_parameters_invoked.size());

  int offset = 0;
  while (offset < argc) {
    ASSERT_STREQ(argv[offset], cgroups_parameters_invoked[offset]);
    offset++;
  }
}

static void write_and_load_gpu_module_to_cfg(const char* cfg_filepath, int enabled) {
  FILE *file = fopen(cfg_filepath, "w");
  if (file == NULL) {
    printf("FAIL: Could not open configuration file: %s\n", cfg_filepath);
    exit(1);
  }
  fprintf(file, "[gpu]\n");
  if (enabled) {
    fprintf(file, "module.enabled=true\n");
  } else {
    fprintf(file, "module.enabled=false\n");
  }
  fclose(file);

  // Read config file
  read_executor_config(cfg_filepath);
  reload_gpu_configuration();
}

static void test_gpu_module_enabled_disabled(int enabled) {
  // Write config file.
  const char *filename = TEST_ROOT "/test_cgroups_module_enabled_disabled.cfg";
  write_and_load_gpu_module_to_cfg(filename, enabled);

  char* argv[] = { (char*) "--module-gpu", (char*) "--excluded_gpus", (char*) "0,1",
                   (char*) "--container_id",
                   (char*) "container_1498064906505_0001_01_000001" };

  int rc = handle_gpu_request(&mock_update_cgroups_parameters,
              "gpu", 5, argv);

  int EXPECTED_RC;
  if (enabled) {
    EXPECTED_RC = 0;
  } else {
    EXPECTED_RC = -1;
  }
  ASSERT_EQ(EXPECTED_RC, rc);
}

TEST_F(TestGpuModule, test_verify_gpu_module_calls_cgroup_parameter) {
  // Write config file.
  const char *filename = TEST_ROOT "/test_verify_gpu_module_calls_cgroup_parameter.cfg";
  write_and_load_gpu_module_to_cfg(filename, 1);

  char* container_id = (char*) "container_1498064906505_0001_01_000001";
  char* argv[] = { (char*) "--module-gpu", (char*) "--excluded_gpus", (char*) "0,1",
                   (char*) "--container_id",
                   container_id };

  /* Test case 1: block 2 devices */
  cgroups_parameters_invoked.clear();
  int rc = handle_gpu_request(&mock_update_cgroups_parameters,
     "gpu", 5, argv);
  ASSERT_EQ(0, rc) << "Should success.\n";

  // Verify cgroups parameters
  const char* expected_cgroups_argv[] = { "devices", "deny", container_id, "c 195:0 rwm",
    "devices", "deny", container_id, "c 195:1 rwm"};
  verify_param_updated_to_cgroups(8, expected_cgroups_argv);

  /* Test case 2: block 0 devices */
  cgroups_parameters_invoked.clear();
  char* argv_1[] = { (char*) "--module-gpu", (char*) "--container_id", container_id };
  rc = handle_gpu_request(&mock_update_cgroups_parameters,
     "gpu", 3, argv_1);
  ASSERT_EQ(0, rc) << "Should success.\n";

  // Verify cgroups parameters
  verify_param_updated_to_cgroups(0, NULL);

  /* Test case 3: block 2 non-sequential devices */
  cgroups_parameters_invoked.clear();
  char* argv_2[] = { (char*) "--module-gpu", (char*) "--excluded_gpus", (char*) "1,3",
                   (char*) "--container_id", container_id };
  rc = handle_gpu_request(&mock_update_cgroups_parameters,
     "gpu", 5, argv_2);
  ASSERT_EQ(0, rc) << "Should success.\n";

  // Verify cgroups parameters
  const char* expected_cgroups_argv_2[] = { "devices", "deny", container_id, "c 195:1 rwm",
    "devices", "deny", container_id, "c 195:3 rwm"};
  verify_param_updated_to_cgroups(8, expected_cgroups_argv_2);
}

TEST_F(TestGpuModule, test_illegal_cli_parameters) {
  // Write config file.
  const char *filename = TEST_ROOT "/test_illegal_cli_parameters.cfg";
  write_and_load_gpu_module_to_cfg(filename, 1);

  // Illegal container id - 1
  char* argv[] = { (char*) "--module-gpu", (char*) "--excluded_gpus", (char*) "0,1",
                   (char*) "--container_id", (char*) "xxxx" };
  int rc = handle_gpu_request(&mock_update_cgroups_parameters,
     "gpu", 5, argv);
  ASSERT_NE(0, rc) << "Should fail.\n";

  // Illegal container id - 2
  char* argv_1[] = { (char*) "--module-gpu", (char*) "--excluded_gpus", (char*) "0,1",
                   (char*) "--container_id", (char*) "container_1" };
  rc = handle_gpu_request(&mock_update_cgroups_parameters,
     "gpu", 5, argv_1);
  ASSERT_NE(0, rc) << "Should fail.\n";

  // Illegal container id - 3
  char* argv_2[] = { (char*) "--module-gpu", (char*) "--excluded_gpus", (char*) "0,1" };
  rc = handle_gpu_request(&mock_update_cgroups_parameters,
     "gpu", 3, argv_2);
  ASSERT_NE(0, rc) << "Should fail.\n";
}

TEST_F(TestGpuModule, test_gpu_module_disabled) {
  test_gpu_module_enabled_disabled(0);
}

TEST_F(TestGpuModule, test_gpu_module_enabled) {
  test_gpu_module_enabled_disabled(1);
}
} // namespace ContainerExecutor