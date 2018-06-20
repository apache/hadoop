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

#if __linux

extern "C" {
#include "oom_listener.h"
#include <time.h>
}

#include <gtest/gtest.h>
#include <fstream>
#include <mutex>

#define CGROUP_ROOT "/sys/fs/cgroup/memory/"
#define TEST_ROOT "/tmp/test-oom-listener/"
#define CGROUP_TASKS "tasks"
#define CGROUP_OOM_CONTROL "memory.oom_control"
#define CGROUP_LIMIT_PHYSICAL "memory.limit_in_bytes"
#define CGROUP_LIMIT_SWAP "memory.memsw.limit_in_bytes"
#define CGROUP_EVENT_CONTROL "cgroup.event_control"
#define CGROUP_LIMIT (5 * 1024 * 1024)

// We try multiple cgroup directories
// We try first the official path to test
// in production
// If we are running as a user we fall back
// to mock cgroup
static const char *cgroup_candidates[] = { CGROUP_ROOT, TEST_ROOT };

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

class OOMListenerTest : public ::testing::Test {
private:
  char cgroup[PATH_MAX];
  const char* cgroup_root;
public:
  OOMListenerTest() : cgroup_root(NULL) {}

  virtual ~OOMListenerTest() = default;
  virtual const char* GetCGroup() { return cgroup; }
  virtual void SetUp() {
    struct stat cgroup_memory = {};
    for (unsigned int i = 0; i < GTEST_ARRAY_SIZE_(cgroup_candidates); ++i) {
      cgroup_root = cgroup_candidates[i];

      // Try to create the root.
      // We might not have permission and
      // it may already exist
      mkdir(cgroup_root, 0700);

      if (0 != stat(cgroup_root, &cgroup_memory)) {
        printf("%s missing. Skipping test\n", cgroup_root);
        continue;
      }

      timespec timespec1 = {};
      if (0 != clock_gettime(CLOCK_MONOTONIC, &timespec1)) {
        ASSERT_TRUE(false) << " clock_gettime failed\n";
      }

      if (snprintf(cgroup, sizeof(cgroup), "%s%lx/",
                        cgroup_root, timespec1.tv_nsec) <= 0) {
        cgroup[0] = '\0';
        printf("%s snprintf failed\n", cgroup_root);
        continue;
      }

      // Create a cgroup named the current timestamp
      // to make it quasi unique
      if (0 != mkdir(cgroup, 0700)) {
        printf("%s not writable.\n", cgroup);
        continue;
      }
      break;
    }

    ASSERT_EQ(0, stat(cgroup, &cgroup_memory))
                  << "Cannot use or simulate cgroup " << cgroup;
  }
  virtual void TearDown() {
    if (cgroup[0] != '\0') {
      rmdir(cgroup);
    }
    if (cgroup_root != NULL &&
        cgroup_root != cgroup_candidates[0]) {
      rmdir(cgroup_root);
    }
  }
};

/*
  Unit test for cgroup testing. There are two modes.
  If the unit test is run as root and we have cgroups
  we try to crate a cgroup and generate an OOM.
  If we are not running as root we just sleep instead of
  hogging memory and simulate the OOM by sending
  an event in a mock event fd mock_oom_event_as_user.
*/
TEST_F(OOMListenerTest, test_oom) {
  // Disable OOM killer
  std::ofstream oom_control;
  std::string oom_control_file =
      std::string(GetCGroup()).append(CGROUP_OOM_CONTROL);
  oom_control.open(oom_control_file.c_str(), oom_control.out);
  oom_control << 1 << std::endl;
  oom_control.close();

  // Set a low enough limit for physical
  std::ofstream limit;
  std::string limit_file =
      std::string(GetCGroup()).append(CGROUP_LIMIT_PHYSICAL);
  limit.open(limit_file.c_str(), limit.out);
  limit << CGROUP_LIMIT << std::endl;
  limit.close();

  // Set a low enough limit for physical + swap
  std::ofstream limitSwap;
  std::string limit_swap_file =
      std::string(GetCGroup()).append(CGROUP_LIMIT_SWAP);
  limitSwap.open(limit_swap_file.c_str(), limitSwap.out);
  limitSwap << CGROUP_LIMIT << std::endl;
  limitSwap.close();

  // Event control file to set
  std::string memory_control_file =
      std::string(GetCGroup()).append(CGROUP_EVENT_CONTROL);

  // Tasks file to check
  std::string tasks_file =
      std::string(GetCGroup()).append(CGROUP_TASKS);

  int mock_oom_event_as_user = -1;
  struct stat stat1 = {};
  if (0 != stat(memory_control_file.c_str(), &stat1)) {
    // We cannot tamper with cgroups
    // running as a user, so simulate an
    // oom event
    mock_oom_event_as_user = eventfd(0, 0);
  }
  const int simulate_cgroups =
      mock_oom_event_as_user != -1;

  __pid_t mem_hog_pid = fork();
  if (!mem_hog_pid) {
    // Child process to consume too much memory
    if (simulate_cgroups) {
      std::cout << "Simulating cgroups OOM" << std::endl;
      for (;;) {
        sleep(1);
      }
    } else {
      // Wait until we are added to the cgroup
      // so that it is accounted for our mem
      // usage
      __pid_t cgroupPid;
      do {
        std::ifstream tasks;
        tasks.open(tasks_file.c_str(), tasks.in);
        tasks >> cgroupPid;
        tasks.close();
      } while (cgroupPid != getpid());

      // Start consuming as much memory as we can.
      // cgroup will stop us at CGROUP_LIMIT
      const int bufferSize = 1024 * 1024;
      std::cout << "Consuming too much memory" << std::endl;
      for (;;) {
        auto buffer = (char *) malloc(bufferSize);
        if (buffer != NULL) {
          for (int i = 0; i < bufferSize; ++i) {
            buffer[i] = (char) std::rand();
          }
        }
      }
    }
  } else {
    // Parent test
    ASSERT_GE(mem_hog_pid, 1) << "Fork failed " << errno;

    // Put child into cgroup
    std::ofstream tasks;
    tasks.open(tasks_file.c_str(), tasks.out);
    tasks << mem_hog_pid << std::endl;
    tasks.close();

    // Create pipe to get forwarded eventfd
    int test_pipe[2];
    ASSERT_EQ(0, pipe(test_pipe));

    // Launch OOM listener
    // There is no race condition with the process
    // running out of memory. If oom is 1 at startup
    // oom_listener will send an initial notification
    __pid_t listener = fork();
    if (listener == 0) {
      // child listener forwarding cgroup events
      _oom_listener_descriptors descriptors = {
          "test",
          mock_oom_event_as_user,
          -1,
          -1,
          {0},
          {0},
          {0},
          0,
          100
      };
      int ret = oom_listener(&descriptors, GetCGroup(), test_pipe[1]);
      cleanup(&descriptors);
      close(test_pipe[0]);
      close(test_pipe[1]);
      exit(ret);
    } else {
    // Parent test
      uint64_t event_id = 1;
      if (simulate_cgroups) {
        // We cannot tamper with cgroups
        // running as a user, so simulate an
        // oom event
        ASSERT_EQ(sizeof(event_id),
                  write(mock_oom_event_as_user,
                        &event_id,
                        sizeof(event_id)));
      }
      ASSERT_EQ(sizeof(event_id),
                read(test_pipe[0],
                     &event_id,
                     sizeof(event_id)))
                    << "The event has not arrived";
      close(test_pipe[0]);
      close(test_pipe[1]);

      // Simulate OOM killer
      ASSERT_EQ(0, kill(mem_hog_pid, SIGKILL));

      // Verify that process was killed
      __WAIT_STATUS mem_hog_status = {};
      __pid_t exited0 = wait(mem_hog_status);
      ASSERT_EQ(mem_hog_pid, exited0)
        << "Wrong process exited";
      ASSERT_EQ(NULL, mem_hog_status)
        << "Test process killed with invalid status";

      if (mock_oom_event_as_user != -1) {
        ASSERT_EQ(0, unlink(oom_control_file.c_str()));
        ASSERT_EQ(0, unlink(limit_file.c_str()));
        ASSERT_EQ(0, unlink(limit_swap_file.c_str()));
        ASSERT_EQ(0, unlink(tasks_file.c_str()));
        ASSERT_EQ(0, unlink(memory_control_file.c_str()));
      }
      // Once the cgroup is empty delete it
      ASSERT_EQ(0, rmdir(GetCGroup()))
                << "Could not delete cgroup " << GetCGroup();

      // Check that oom_listener exited on the deletion of the cgroup
      __WAIT_STATUS oom_listener_status = {};
      __pid_t exited1 = wait(oom_listener_status);
      ASSERT_EQ(listener, exited1)
        << "Wrong process exited";
      ASSERT_EQ(NULL, oom_listener_status)
        << "Listener process exited with invalid status";
    }
  }
}

#else
/*
This tool covers Linux specific functionality,
so it is not available for other operating systems
*/
int main() {
  return 1;
}
#endif
