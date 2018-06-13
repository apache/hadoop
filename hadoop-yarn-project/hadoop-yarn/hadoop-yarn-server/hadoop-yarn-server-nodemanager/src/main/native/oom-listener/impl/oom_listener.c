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

#include <sys/param.h>
#include <poll.h>
#include "oom_listener.h"

/*
 * Print an error.
*/
static inline void print_error(const char *file, const char *message,
                        ...) {
  fprintf(stderr, "%s ", file);
  va_list arguments;
  va_start(arguments, message);
  vfprintf(stderr, message, arguments);
  va_end(arguments);
}

/*
 * Listen to OOM events in a memory cgroup. See declaration for details.
 */
int oom_listener(_oom_listener_descriptors *descriptors, const char *cgroup, int fd) {
  const char *pattern =
          cgroup[MAX(strlen(cgroup), 1) - 1] == '/'
          ? "%s%s" :"%s/%s";

  /* Create an event handle, if we do not have one already*/
  if (descriptors->event_fd == -1 &&
      (descriptors->event_fd = eventfd(0, 0)) == -1) {
    print_error(descriptors->command, "eventfd() failed. errno:%d %s\n",
                errno, strerror(errno));
    return EXIT_FAILURE;
  }

  /*
   * open the file to listen to (memory.oom_control)
   * and write the event handle and the file handle
   * to cgroup.event_control
   */
  if (snprintf(descriptors->event_control_path,
               sizeof(descriptors->event_control_path),
               pattern,
               cgroup,
               "cgroup.event_control") < 0) {
    print_error(descriptors->command, "path too long %s\n", cgroup);
    return EXIT_FAILURE;
  }

  if ((descriptors->event_control_fd = open(
      descriptors->event_control_path,
      O_WRONLY|O_CREAT, 0600)) == -1) {
    print_error(descriptors->command, "Could not open %s. errno:%d %s\n",
                descriptors->event_control_path,
                errno, strerror(errno));
    return EXIT_FAILURE;
  }

  if (snprintf(descriptors->oom_control_path,
               sizeof(descriptors->oom_control_path),
               pattern,
               cgroup,
               "memory.oom_control") < 0) {
    print_error(descriptors->command, "path too long %s\n", cgroup);
    return EXIT_FAILURE;
  }

  if ((descriptors->oom_control_fd = open(
      descriptors->oom_control_path,
      O_RDONLY)) == -1) {
    print_error(descriptors->command, "Could not open %s. errno:%d %s\n",
                descriptors->oom_control_path,
                errno, strerror(errno));
    return EXIT_FAILURE;
  }

  if ((descriptors->oom_command_len = (size_t) snprintf(
      descriptors->oom_command,
      sizeof(descriptors->oom_command),
      "%d %d",
      descriptors->event_fd,
      descriptors->oom_control_fd)) < 0) {
    print_error(descriptors->command, "Could print %d %d\n",
                descriptors->event_control_fd,
                descriptors->oom_control_fd);
    return EXIT_FAILURE;
  }

  if (write(descriptors->event_control_fd,
            descriptors->oom_command,
            descriptors->oom_command_len) == -1) {
    print_error(descriptors->command, "Could not write to %s errno:%d\n",
                descriptors->event_control_path, errno);
    return EXIT_FAILURE;
  }

  if (close(descriptors->event_control_fd) == -1) {
    print_error(descriptors->command, "Could not close %s errno:%d\n",
                descriptors->event_control_path, errno);
    return EXIT_FAILURE;
  }
  descriptors->event_control_fd = -1;

  /*
   * Listen to events as long as the cgroup exists
   * and forward them to the fd in the argument.
   */
  for (;;) {
    uint64_t u;
    ssize_t ret = 0;
    struct stat stat_buffer = {0};
    struct pollfd poll_fd = {
        .fd = descriptors->event_fd,
        .events = POLLIN
    };

    ret = poll(&poll_fd, 1, descriptors->watch_timeout);
    if (ret < 0) {
      /* Error calling poll */
      print_error(descriptors->command,
                  "Could not poll eventfd %d errno:%d %s\n", ret,
                  errno, strerror(errno));
      return EXIT_FAILURE;
    }

    if (ret > 0) {
      /* Event counter values are always 8 bytes */
      if ((ret = read(descriptors->event_fd, &u, sizeof(u))) != sizeof(u)) {
        print_error(descriptors->command,
                    "Could not read from eventfd %d errno:%d %s\n", ret,
                    errno, strerror(errno));
        return EXIT_FAILURE;
      }

      /* Forward the value to the caller, typically stdout */
      if ((ret = write(fd, &u, sizeof(u))) != sizeof(u)) {
        print_error(descriptors->command,
                    "Could not write to pipe %d errno:%d %s\n", ret,
                    errno, strerror(errno));
        return EXIT_FAILURE;
      }
    } else if (ret == 0) {
      /* Timeout has elapsed*/

      /* Quit, if the cgroup is deleted */
      if (stat(cgroup, &stat_buffer) != 0) {
        break;
      }
    }
  }
  return EXIT_SUCCESS;
}

#endif
