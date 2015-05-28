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

#include "config.h"
#include "exception.h"
#include "org_apache_hadoop.h"
#include "org_apache_hadoop_net_unix_DomainSocketWatcher.h"

#include <errno.h>
#include <fcntl.h>
#include <jni.h>
#include <poll.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

static jfieldID fd_set_data_fid;

#define FD_SET_DATA_MIN_SIZE 2

struct fd_set_data {
  /**
   * Number of fds we have allocated space for.
   */
  int alloc_size;

  /**
   * Number of fds actually in use.
   */
  int used_size;

  /**
   * Beginning of pollfd data.
   */
  struct pollfd pollfd[0];
};

JNIEXPORT void JNICALL
Java_org_apache_hadoop_net_unix_DomainSocketWatcher_anchorNative(
JNIEnv *env, jclass clazz)
{
  jclass fd_set_class;

  fd_set_class = (*env)->FindClass(env,
          "org/apache/hadoop/net/unix/DomainSocketWatcher$FdSet");
  if (!fd_set_class) return; // exception raised
  fd_set_data_fid = (*env)->GetFieldID(env, fd_set_class, "data", "J");
  if (!fd_set_data_fid) return; // exception raised
}

JNIEXPORT jlong JNICALL
Java_org_apache_hadoop_net_unix_DomainSocketWatcher_00024FdSet_alloc0(
JNIEnv *env, jclass clazz)
{
  struct fd_set_data *sd;

  sd = calloc(1, sizeof(struct fd_set_data) +
              (sizeof(struct pollfd) * FD_SET_DATA_MIN_SIZE));
  if (!sd) {
    (*env)->Throw(env, newRuntimeException(env, "out of memory allocating "
                                            "DomainSocketWatcher#FdSet"));
    return 0L;
  }
  sd->alloc_size = FD_SET_DATA_MIN_SIZE;
  sd->used_size = 0;
  return (jlong)(intptr_t)sd;
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_net_unix_DomainSocketWatcher_00024FdSet_add(
JNIEnv *env, jobject obj, jint fd)
{
  struct fd_set_data *sd, *nd;
  struct pollfd *pollfd;

  sd = (struct fd_set_data*)(intptr_t)(*env)->
    GetLongField(env, obj, fd_set_data_fid);
  if (sd->used_size + 1 > sd->alloc_size) {
    nd = realloc(sd, sizeof(struct fd_set_data) +
            (sizeof(struct pollfd) * sd->alloc_size * 2));
    if (!nd) {
      (*env)->Throw(env, newRuntimeException(env, "out of memory adding "
            "another fd to DomainSocketWatcher#FdSet.  we have %d already",
            sd->alloc_size));
      return;
    }
    nd->alloc_size = nd->alloc_size * 2;
    (*env)->SetLongField(env, obj, fd_set_data_fid, (jlong)(intptr_t)nd);
    sd = nd;
  }
  pollfd = &sd->pollfd[sd->used_size];
  sd->used_size++;
  pollfd->fd = fd;
  pollfd->events = POLLIN | POLLHUP;
  pollfd->revents = 0;
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_net_unix_DomainSocketWatcher_00024FdSet_remove(
JNIEnv *env, jobject obj, jint fd)
{
  struct fd_set_data *sd;
  struct pollfd *pollfd = NULL, *last_pollfd;
  int used_size, i;

  sd = (struct fd_set_data*)(intptr_t)(*env)->
      GetLongField(env, obj, fd_set_data_fid);
  used_size = sd->used_size;
  for (i = 0; i < used_size; i++) {
    if (sd->pollfd[i].fd == fd) {
      pollfd = sd->pollfd + i;
      break;
    }
  }
  if (pollfd == NULL) {
    (*env)->Throw(env, newRuntimeException(env, "failed to remove fd %d "
          "from the FdSet because it was never present.", fd));
    return;
  }
  last_pollfd = sd->pollfd + (used_size - 1);
  if (used_size > 1) {
    // Move last pollfd to the new empty slot if needed
    pollfd->fd = last_pollfd->fd;
    pollfd->events = last_pollfd->events;
    pollfd->revents = last_pollfd->revents;
  }
  memset(last_pollfd, 0, sizeof(struct pollfd));
  sd->used_size--;
}

JNIEXPORT jobject JNICALL
Java_org_apache_hadoop_net_unix_DomainSocketWatcher_00024FdSet_getAndClearReadableFds(
JNIEnv *env, jobject obj)
{
  int *carr = NULL;
  jobject jarr = NULL;
  struct fd_set_data *sd;
  int used_size, num_readable = 0, i, j;
  jthrowable jthr = NULL;

  sd = (struct fd_set_data*)(intptr_t)(*env)->
      GetLongField(env, obj, fd_set_data_fid);
  used_size = sd->used_size;
  for (i = 0; i < used_size; i++) {
    // We check for both POLLIN and POLLHUP, because on some OSes, when a socket
    // is shutdown(), it sends POLLHUP rather than POLLIN.
    if ((sd->pollfd[i].revents & POLLIN) ||
        (sd->pollfd[i].revents & POLLHUP)) {
      num_readable++;
    } else {
      sd->pollfd[i].revents = 0;
    }
  }
  if (num_readable > 0) {
    carr = malloc(sizeof(int) * num_readable);
    if (!carr) {
      jthr = newRuntimeException(env, "failed to allocate a temporary array "
            "of %d ints", num_readable);
      goto done;
    }
    j = 0;
    for (i = 0; ((i < used_size) && (j < num_readable)); i++) {
      if ((sd->pollfd[i].revents & POLLIN) ||
          (sd->pollfd[i].revents & POLLHUP)) {
        carr[j] = sd->pollfd[i].fd;
        j++;
        sd->pollfd[i].revents = 0;
      }
    }
    if (j != num_readable) {
      jthr = newRuntimeException(env, "failed to fill entire carr "
            "array of size %d: only filled %d elements", num_readable, j);
      goto done;
    }
  }
  jarr = (*env)->NewIntArray(env, num_readable);
  if (!jarr) {
    jthr = (*env)->ExceptionOccurred(env);
    (*env)->ExceptionClear(env);
    goto done;
  }
  if (num_readable > 0) {
    (*env)->SetIntArrayRegion(env, jarr, 0, num_readable, carr);
    jthr = (*env)->ExceptionOccurred(env);
    if (jthr) {
      (*env)->ExceptionClear(env);
      goto done;
    }
  }

done:
  free(carr);
  if (jthr) {
    (*env)->DeleteLocalRef(env, jarr);
    (*env)->Throw(env, jthr);
  }
  return jarr;
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_net_unix_DomainSocketWatcher_00024FdSet_close(
JNIEnv *env, jobject obj)
{
  struct fd_set_data *sd;

  sd = (struct fd_set_data*)(intptr_t)(*env)->
      GetLongField(env, obj, fd_set_data_fid);
  if (sd) {
    free(sd);
    (*env)->SetLongField(env, obj, fd_set_data_fid, 0L);
  }
}

JNIEXPORT jint JNICALL
Java_org_apache_hadoop_net_unix_DomainSocketWatcher_doPoll0(
JNIEnv *env, jclass clazz, jint checkMs, jobject fdSet)
{
  struct fd_set_data *sd;
  int ret, err;

  sd = (struct fd_set_data*)(intptr_t)(*env)->
      GetLongField(env, fdSet, fd_set_data_fid);
  ret = poll(sd->pollfd, sd->used_size, checkMs);
  if (ret >= 0) {
    return ret;
  }
  err = errno;
  if (err != EINTR) { // treat EINTR as 0 fds ready
    (*env)->Throw(env, newIOException(env,
            "poll(2) failed with error code %d: %s", err, terror(err)));
  }
  return 0;
}
