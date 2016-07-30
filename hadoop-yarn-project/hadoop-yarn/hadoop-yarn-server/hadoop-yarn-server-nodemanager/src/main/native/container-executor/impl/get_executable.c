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

/*
 * This code implements OS-specific ways to get the absolute
 * filename of the executable.  Typically, one would use
 * realpath(argv[0]) (or equivalent), however, because this
 * code runs as setuid and will be used later on to determine
 * relative paths, we want something a big more secure
 * since argv[0] is replaceable by malicious code.
 *
 * NOTE! The value returned will be free()'d later on!
 *
 */

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include "config.h"
#include "configuration.h"
#include "container-executor.h"

/*
 * A generic function to read a link and return
 * the value for use with System V procfs.
 * With much thanks to Tom Killian, Roger Faulkner,
 * and Ron Gomes, this is pretty generic code.
 *
 * The various BSDs do not have (reliably)
 * have /proc. Custom implementations follow.
 */

char *__get_exec_readproc(char *procfn) {
  char *filename;
  ssize_t len;

  filename = malloc(EXECUTOR_PATH_MAX);
  if (!filename) {
    fprintf(ERRORFILE,"cannot allocate memory for filename: %s\n",strerror(errno));
    exit(-1);
  }
  len = readlink(procfn, filename, EXECUTOR_PATH_MAX);
  if (len == -1) {
    fprintf(ERRORFILE,"Can't get executable name from %s - %s\n", procfn,
            strerror(errno));
    exit(-1);
  } else if (len >= EXECUTOR_PATH_MAX) {
    fprintf(ERRORFILE,"Executable name %.*s is longer than %d characters.\n",
            EXECUTOR_PATH_MAX, filename, EXECUTOR_PATH_MAX);
    exit(-1);
  }
  filename[len] = '\0';
  return filename;
}

#ifdef __APPLE__

/*
 * Mac OS X doesn't have a procfs, but there is
 * libproc which we can use instead.  It is available
 * in most modern versions of OS X as of this writing (2016).
 */

#include <libproc.h>

char* get_executable() {
  char *filename;
  pid_t pid;

  filename = malloc(PROC_PIDPATHINFO_MAXSIZE);
  if (!filename) {
    fprintf(ERRORFILE,"cannot allocate memory for filename: %s\n",strerror(errno));
    exit(-1);
  }
  pid = getpid();
  if (proc_pidpath(pid,filename,PROC_PIDPATHINFO_MAXSIZE) <= 0) {
    fprintf(ERRORFILE,"Can't get executable name from pid %u - %s\n", pid,
            strerror(errno));
    exit(-1);
  }
  return filename;
}

#elif defined(__linux)


char* get_executable() {
  return __get_exec_readproc("/proc/self/exe");
}

#elif defined(__sun)

/*
 * It's tempting to use getexecname(), but there is no guarantee
 * we will get a full path and worse, we'd be reliant on getcwd()
 * being where our exec is at. Instead, we'll use the /proc
 * method, using the "invisible" /proc/self link that only the
 * process itself can see. (Anyone that tells you /proc/self
 * doesn't exist on Solaris hasn't read the proc(4) man page.)
 */

char* get_executable() {
  return __get_exec_readproc("/proc/self/path/a.out");
}

#else

#error Cannot safely determine executable path on this operating system.

#endif
