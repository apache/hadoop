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

#include "util/posix_util.h"

#include <dirent.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <limits.h>

static pthread_mutex_t gTempdirLock = PTHREAD_MUTEX_INITIALIZER;

static int gTempdirNonce = 0;

int recursiveDeleteContents(const char *path)
{
  int ret;
  DIR *dp;
  struct dirent *de;
  char tmp[PATH_MAX];

  dp = opendir(path);
  if (!dp) {
    ret = -errno;
    fprintf(stderr, "recursiveDelete(%s) failed with error %d\n", path, ret);
    return ret;
  }
  while (1) {
    de = readdir(dp);
    if (!de) {
      ret = 0;
      break;
    }
    if ((de->d_name[0] == '.') && (de->d_name[1] == '\0'))
      continue;
    if ((de->d_name[0] == '.') && (de->d_name[1] == '.') &&
        (de->d_name[2] == '\0'))
      continue;
    snprintf(tmp, sizeof(tmp), "%s/%s", path, de->d_name);
    ret = recursiveDelete(tmp);
    if (ret)
      break;
  }
  if (closedir(dp)) {
    ret = -errno;
    fprintf(stderr, "recursiveDelete(%s): closedir failed with "
            "error %d\n", path, ret);
    return ret;
  }
  return ret;
}

/*
 * Simple recursive delete implementation.
 * It could be optimized, but there is no need at the moment.
 * TODO: use fstat, etc.
 */
int recursiveDelete(const char *path)
{
  int ret;
  struct stat stBuf;

  ret = stat(path, &stBuf);
  if (ret != 0) {
    ret = -errno;
    fprintf(stderr, "recursiveDelete(%s): stat failed with "
            "error %d\n", path, ret);
    return ret;
  }
  if (S_ISDIR(stBuf.st_mode)) {
    ret = recursiveDeleteContents(path);
    if (ret)
      return ret;
    ret = rmdir(path);
    if (ret) {
      ret = errno;
      fprintf(stderr, "recursiveDelete(%s): rmdir failed with error %d\n",
              path, ret);
      return ret;
    }
  } else {
    ret = unlink(path);
    if (ret) {
      ret = -errno;
      fprintf(stderr, "recursiveDelete(%s): unlink failed with "
              "error %d\n", path, ret);
      return ret;
    }
  }
  return 0;
}

int createTempDir(char *tempDir, int nameMax, int mode)
{
  char tmp[PATH_MAX];
  int pid, nonce;
  const char *base = getenv("TMPDIR");
  if (!base)
    base = "/tmp";
  if (base[0] != '/') {
    // canonicalize non-absolute TMPDIR
    if (realpath(base, tmp) == NULL) {
      return -errno;
    }
    base = tmp;
  }
  pid = getpid();
  pthread_mutex_lock(&gTempdirLock);
  nonce = gTempdirNonce++;
  pthread_mutex_unlock(&gTempdirLock);
  snprintf(tempDir, nameMax, "%s/temp.%08d.%08d", base, pid, nonce);
  if (mkdir(tempDir, mode) == -1) {
    int ret = errno;
    return -ret;
  }
  return 0;
}

void sleepNoSig(int sec)
{
  int ret;
  struct timespec req, rem;

  rem.tv_sec = sec;
  rem.tv_nsec = 0;
  do {
    req = rem;
    ret = nanosleep(&req, &rem);
  } while ((ret == -1) && (errno == EINTR));
  if (ret == -1) {
    ret = errno;
    fprintf(stderr, "nanosleep error %d (%s)\n", ret, strerror(ret));
  }
}
