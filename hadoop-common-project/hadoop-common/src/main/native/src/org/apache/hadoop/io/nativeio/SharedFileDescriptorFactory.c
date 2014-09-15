/*
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

#include "org_apache_hadoop.h"

#ifdef UNIX

#include "exception.h"
#include "file_descriptor.h"
#include "org_apache_hadoop.h"
#include "org_apache_hadoop_io_nativeio_SharedFileDescriptorFactory.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define ZERO_FULLY_BUF_SIZE 8192

static pthread_mutex_t g_rand_lock = PTHREAD_MUTEX_INITIALIZER;

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_nativeio_SharedFileDescriptorFactory_deleteStaleTemporaryFiles0(
  JNIEnv *env, jclass clazz, jstring jprefix, jstring jpath)
{
  const char *prefix = NULL, *path = NULL;
  char target[PATH_MAX];
  jthrowable jthr;
  DIR *dp = NULL;
  struct dirent *de;

  prefix = (*env)->GetStringUTFChars(env, jprefix, NULL);
  if (!prefix) goto done; // exception raised
  path = (*env)->GetStringUTFChars(env, jpath, NULL);
  if (!path) goto done; // exception raised

  dp = opendir(path);
  if (!dp) {
    int ret = errno;
    jthr = newIOException(env, "opendir(%s) error %d: %s",
                          path, ret, terror(ret));
    (*env)->Throw(env, jthr);
    goto done;
  }
  while ((de = readdir(dp))) {
    if (strncmp(prefix, de->d_name, strlen(prefix)) == 0) {
      int ret = snprintf(target, PATH_MAX, "%s/%s", path, de->d_name);
      if ((0 < ret) && (ret < PATH_MAX)) {
        unlink(target);
      }
    }
  }

done:
  if (dp) {
    closedir(dp);
  }
  if (prefix) {
    (*env)->ReleaseStringUTFChars(env, jprefix, prefix);
  }
  if (path) {
    (*env)->ReleaseStringUTFChars(env, jpath, path);
  }
}

static int zero_fully(int fd, jint length)
{
  char buf[ZERO_FULLY_BUF_SIZE];
  int res;

  memset(buf, 0, sizeof(buf));
  while (length > 0) {
    res = write(fd, buf,
      (length > ZERO_FULLY_BUF_SIZE) ? ZERO_FULLY_BUF_SIZE : length);
    if (res < 0) {
      if (errno == EINTR) continue;
      return errno;
    }
    length -= res;
  }
  return 0;
}

JNIEXPORT jobject JNICALL
Java_org_apache_hadoop_io_nativeio_SharedFileDescriptorFactory_createDescriptor0(
  JNIEnv *env, jclass clazz, jstring jprefix, jstring jpath, jint length)
{
  const char *prefix = NULL, *path = NULL;
  char target[PATH_MAX];
  int ret, fd = -1, rnd;
  jthrowable jthr;
  jobject jret = NULL;

  prefix = (*env)->GetStringUTFChars(env, jprefix, NULL);
  if (!prefix) goto done; // exception raised
  path = (*env)->GetStringUTFChars(env, jpath, NULL);
  if (!path) goto done; // exception raised

  pthread_mutex_lock(&g_rand_lock);
  rnd = rand();
  pthread_mutex_unlock(&g_rand_lock);
  while (1) {
    ret = snprintf(target, PATH_MAX, "%s/%s_%d",
                   path, prefix, rnd);
    if (ret < 0) {
      jthr = newIOException(env, "snprintf error");
      (*env)->Throw(env, jthr);
      goto done;
    } else if (ret >= PATH_MAX) {
      jthr = newIOException(env, "computed path was too long.");
      (*env)->Throw(env, jthr);
      goto done;
    }
    fd = open(target, O_CREAT | O_EXCL | O_RDWR, 0700);
    if (fd >= 0) break; // success
    ret = errno;
    if (ret == EEXIST) {
      // Bad luck -- we got a very rare collision here between us and 
      // another DataNode (or process).  Try again.
      continue;
    } else if (ret == EINTR) {
      // Most of the time, this error is only possible when opening FIFOs.
      // But let's be thorough.
      continue;
    }
    jthr = newIOException(env, "open(%s, O_CREAT | O_EXCL | O_RDWR) "
            "failed: error %d (%s)", target, ret, terror(ret));
    (*env)->Throw(env, jthr);
    goto done;
  }
  if (unlink(target) < 0) {
    jthr = newIOException(env, "unlink(%s) failed: error %d (%s)",
                          path, ret, terror(ret));
    (*env)->Throw(env, jthr);
    goto done;
  }
  ret = zero_fully(fd, length);
  if (ret) {
    jthr = newIOException(env, "zero_fully(%s, %d) failed: error %d (%s)",
                          path, length, ret, terror(ret));
    (*env)->Throw(env, jthr);
    goto done;
  }
  if (lseek(fd, 0, SEEK_SET) < 0) {
    ret = errno;
    jthr = newIOException(env, "lseek(%s, 0, SEEK_SET) failed: error %d (%s)",
                          path, ret, terror(ret));
    (*env)->Throw(env, jthr);
    goto done;
  }
  jret = fd_create(env, fd); // throws exception on error.

done:
  if (prefix) {
    (*env)->ReleaseStringUTFChars(env, jprefix, prefix);
  }
  if (path) {
    (*env)->ReleaseStringUTFChars(env, jpath, path);
  }
  if (!jret) {
    if (fd >= 0) {
      close(fd);
    }
  }
  return jret;
}

#endif
