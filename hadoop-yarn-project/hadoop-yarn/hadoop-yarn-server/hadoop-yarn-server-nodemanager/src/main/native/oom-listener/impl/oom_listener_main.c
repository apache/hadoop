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

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>

#include "oom_listener.h"

extern inline void cleanup(_oom_listener_descriptors *descriptors);

void print_usage(void) {
  fprintf(stderr, "oom-listener");
  fprintf(stderr, "Listen to OOM events in a cgroup");
  fprintf(stderr, "usage to listen: oom-listener <cgroup directory>\n");
  fprintf(stderr, "usage to test: oom-listener oom [<pgid>]\n");
  fprintf(stderr, "example listening: oom-listener /sys/fs/cgroup/memory/hadoop-yarn | xxd -c 8\n");
  fprintf(stderr, "example oom to test: bash -c 'echo $$ >/sys/fs/cgroup/memory/hadoop-yarn/tasks;oom-listener oom'\n");
  fprintf(stderr, "example container overload: sudo -u <user> bash -c 'echo $$ && oom-listener oom 0' >/sys/fs/cgroup/memory/hadoop-yarn/<container>/tasks\n");
  exit(EXIT_FAILURE);
}

/*
  Test an OOM situation adding the pid
  to the group pgid and calling malloc in a loop
  This can be used to test OOM listener. See examples above.
*/
void test_oom_infinite(char* pgids) {
  if (pgids != NULL) {
    int pgid = atoi(pgids);
    setpgid(0, pgid);
  }
  while(1) {
    char* p = (char*)malloc(4096);
    if (p != NULL) {
      p[0] = 0xFF;
    } else {
      exit(1);
    }
  }
}

/*
 A command that receives a memory cgroup directory and
 listens to the events in the directory.
 It will print a new line on every out of memory event
 to the standard output.
 usage:
 oom-listener <cgroup>
*/
int main(int argc, char *argv[]) {
  if (argc >= 2 &&
      strcmp(argv[1], "oom") == 0)
    test_oom_infinite(argc < 3 ? NULL : argv[2]);

  if (argc != 2)
    print_usage();

  _oom_listener_descriptors descriptors = {
      .command = argv[0],
      .event_fd = -1,
      .event_control_fd = -1,
      .oom_control_fd = -1,
      .event_control_path = {0},
      .oom_control_path = {0},
      .oom_command = {0},
      .oom_command_len = 0,
      .watch_timeout = 1000
  };

  int ret = oom_listener(&descriptors, argv[1], STDOUT_FILENO);

  cleanup(&descriptors);

  return ret;
}

#else

/*
 This tool uses Linux specific functionality,
 so it is not available for other operating systems
*/
int main() {
  return 1;
}

#endif
