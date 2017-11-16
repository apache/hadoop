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

#include "config.h"
#include "util.h"

#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

#ifdef HAVE_SYS_SYSCTL_H
#include <sys/types.h>
#include <sys/param.h>
#include <sys/sysctl.h>
#endif

/*
 * A generic function to read a link and return
 * the value for use with System V procfs.
 * With much thanks to Tom Killian, Roger Faulkner,
 * and Ron Gomes, this is pretty generic code.
 */

char *__get_exec_readproc(char *procfn) {
  char *filename;
  ssize_t len;

  filename = malloc(EXECUTOR_PATH_MAX);
  if (!filename) {
    fprintf(ERRORFILE,"cannot allocate memory for filename before readlink: %s\n",strerror(errno));
    exit(-1);
  }
  len = readlink(procfn, filename, EXECUTOR_PATH_MAX);
  if (len == -1) {
    fprintf(ERRORFILE,"Can't get executable name from %s - %s\n", procfn,
            strerror(errno));
    exit(-1);
  } else if (len >= EXECUTOR_PATH_MAX) {
    fprintf(ERRORFILE,"Resolved path for %s [%s] is longer than %d characters.\n",
            procfn, filename, EXECUTOR_PATH_MAX);
    exit(-1);
  }
  filename[len] = '\0';
  return filename;
}


#ifdef HAVE_SYSCTL
/*
 * A generic function to ask the kernel via sysctl.
 * This is used by most of the open source BSDs, as
 * many do not reliably have a /proc mounted.
 */

char *__get_exec_sysctl(int *mib)
{
  char buffer[EXECUTOR_PATH_MAX];
  char *filename;
  size_t len;

  len = sizeof(buffer);
  if (sysctl(mib, 4, buffer, &len, NULL, 0) == -1) {
    fprintf(ERRORFILE,"Can't get executable name from kernel: %s\n",
      strerror(errno));
    exit(-1);
  }
  filename=malloc(EXECUTOR_PATH_MAX);
  if (!filename) {
    fprintf(ERRORFILE,"cannot allocate memory for filename after sysctl: %s\n",strerror(errno));
    exit(-1);
  }
  snprintf(filename,EXECUTOR_PATH_MAX,"%s",buffer);
  return filename;
}

#endif /* HAVE_SYSCTL */

#ifdef __APPLE__

/*
 * Mac OS X doesn't have a procfs, but there is
 * libproc which we can use instead.  It is available
 * in most modern versions of OS X as of this writing (2016).
 */

#include <libproc.h>

char* get_executable(char *argv0) {
  char *filename;
  pid_t pid;

  filename = malloc(PROC_PIDPATHINFO_MAXSIZE);
  if (!filename) {
    fprintf(ERRORFILE,"cannot allocate memory for filename before proc_pidpath: %s\n",strerror(errno));
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

#elif defined(__FreeBSD__)

char* get_executable(char *argv0) {
  static int mib[] = {
    CTL_KERN, KERN_PROC, KERN_PROC_PATHNAME, -1
  };
  return __get_exec_sysctl(mib);
}

#elif defined(__linux__)


char* get_executable(char *argv0) {
  return __get_exec_readproc("/proc/self/exe");
}

#elif defined(__NetBSD__) && defined(KERN_PROC_PATHNAME)

/* Only really new NetBSD kernels have KERN_PROC_PATHNAME */

char* get_executable(char *argv0) {
  static int mib[] = {
    CTL_KERN, KERN_PROC_ARGS, -1, KERN_PROC_PATHNAME,
  };
  return __get_exec_sysctl(mib);
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

char* get_executable(char *argv0) {
  return __get_exec_readproc("/proc/self/path/a.out");
}

#elif defined(HADOOP_CONF_DIR_IS_ABS)

/*
 * This is the fallback for operating systems where
 * we don't know how to ask the kernel where the executable
 * is located.  It is only used if the maven property
 * container-executor.conf.dir is set to an absolute path
 * for security reasons.
 */

char* get_executable (char *argv0) {
  char *filename;

#ifdef HAVE_CANONICALIZE_FILE_NAME
  filename=canonicalize_file_name(argv0);
#else
  filename=realpath(argv0,NULL);
#endif

  if (!filename) {
    fprintf(ERRORFILE,"realpath of executable: %s\n",strerror(errno));
    exit(-1);
  }
  return filename;
}

#else

/*
 * If we ended up here, we're on an operating system that doesn't
 * match any of the above. This means either the OS needs to get a
 * code added or the container-executor.conf.dir maven property
 * should be set to an absolute path.
 */

#error Cannot safely determine executable path with a relative HADOOP_CONF_DIR on this operating system.

#endif /* platform checks */
