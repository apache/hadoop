/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#include <jni.h>
#include "file_descriptor.h"
#include "org_apache_hadoop.h"

// class of java.io.FileDescriptor
static jclass fd_class;
// the internal field for the integer fd
static jfieldID fd_descriptor;
// the no-argument constructor
static jmethodID fd_constructor;

#ifdef WINDOWS
// the internal field for the long handle
static jfieldID fd_handle;
#endif

/*
 * Initialize the java.io.FileDescriptor class/member cache.
 *
 * Besides being called from the NativeIO and DomainSocket
 * initializers, this is also re-invoked lazily by fd_get()/fd_create()
 * in case a previous attempt failed or was torn down.
 *
 * This function is intentionally not thread-safe: two threads racing
 * on a cold cache may both perform the lookups; the last publisher
 * wins and the other threads' global refs leak (bounded by the number
 * of racing threads). A thread observing a partially published cache
 * is caught by the full NULL checks in fd_get()/fd_create(), so the
 * worst outcome of the race is a spurious IllegalStateException. Real
 * world callers are rare and close to serialized, so a lock is not
 * warranted here.
 */
void fd_init(JNIEnv* env)
{
  jclass local_cls;
  jmethodID constructor;
  jfieldID descriptor;
#ifdef WINDOWS
  jfieldID handle;
#endif

  if (fd_class != NULL) return; // already initted

  local_cls = (*env)->FindClass(env, "java/io/FileDescriptor");
  PASS_EXCEPTIONS(env);

  // Resolve every member before publishing anything so that a failure
  // part-way through cannot leave a half-initialized cache behind: all
  // statics stay NULL and a later call is free to retry.
  descriptor = (*env)->GetFieldID(env, local_cls, "fd", "I");
  PASS_EXCEPTIONS_GOTO(env, cleanup);

#ifdef WINDOWS
  handle = (*env)->GetFieldID(env, local_cls, "handle", "J");
  PASS_EXCEPTIONS_GOTO(env, cleanup);
#endif

  constructor = (*env)->GetMethodID(env, local_cls, "<init>", "()V");
  PASS_EXCEPTIONS_GOTO(env, cleanup);

  fd_class = (*env)->NewGlobalRef(env, local_cls);
  if (fd_class == NULL) {
    // Most likely OutOfMemoryError. Nothing has been published yet, so
    // leave all statics NULL and let a later call retry.
    goto cleanup;
  }
  fd_descriptor = descriptor;
  fd_constructor = constructor;
#ifdef WINDOWS
  fd_handle = handle;
#endif

cleanup:
  if (local_cls != NULL) {
    (*env)->DeleteLocalRef(env, local_cls);
  }
}

/*
 * Tear down the cache established by fd_init(). Currently no live
 * caller: retained for symmetry with fd_init() and for explicit
 * shutdown use (e.g. a future JNI_OnUnload). Do not call this from the
 * error paths of unrelated initializers: the cache is shared with
 * DomainSocket, which may have initialized it successfully earlier.
 */
void fd_deinit(JNIEnv *env) {
  if (fd_class != NULL) {
    (*env)->DeleteGlobalRef(env, fd_class);
    fd_class = NULL;
  }
  fd_descriptor = NULL;
#ifdef WINDOWS
  fd_handle = NULL;
#endif
  fd_constructor = NULL;
}

#ifdef UNIX
/*
 * Given an instance 'obj' of java.io.FileDescriptor, return the
 * underlying fd, or throw if unavailable
 */
int fd_get(JNIEnv* env, jobject obj) {
  if (obj == NULL) {
    THROW(env, "java/lang/NullPointerException",
          "FileDescriptor object is null");
    return -1;
  }
  // The member cache may be missing if a previous fd_init() failed
  // part-way. Retry instead of using NULL members (which would crash
  // the JVM inside the JNI call).
  fd_init(env);
  PASS_EXCEPTIONS_RET(env, -1);
  if (fd_descriptor == NULL) {
    THROW(env, "java/lang/IllegalStateException",
          "java.io.FileDescriptor natives not initialized");
    return -1;
  }
  return (*env)->GetIntField(env, obj, fd_descriptor);
}

/*
 * Create a FileDescriptor object corresponding to the given int fd
 */
jobject fd_create(JNIEnv* env, int fd) {
  jobject obj;

  // The class cache may be missing or incomplete for any reason (an
  // earlier fd_init() that failed part-way, or a cache torn down by a
  // partially failing initializer). Never call JNI with NULL class,
  // constructor or field members: doing so crashed the JVM with SIGSEGV
  // inside NewObject(). Retry the initialization and throw a regular
  // Java exception instead.
  fd_init(env);
  PASS_EXCEPTIONS_RET(env, NULL);

  if (fd_class == NULL || fd_constructor == NULL || fd_descriptor == NULL) {
    THROW(env, "java/lang/IllegalStateException",
          "java.io.FileDescriptor natives not initialized");
    return NULL;
  }

  obj = (*env)->NewObject(env, fd_class, fd_constructor);
  PASS_EXCEPTIONS_RET(env, NULL);

  (*env)->SetIntField(env, obj, fd_descriptor, fd);
  return obj;
}
#endif

#ifdef WINDOWS
/*
 * Given an instance 'obj' of java.io.FileDescriptor, return the
 * underlying fd, or throw if unavailable
 */
long fd_get(JNIEnv* env, jobject obj) {
  if (obj == NULL) {
    THROW(env, "java/lang/NullPointerException",
          "FileDescriptor object is null");
    return -1;
  }
  fd_init(env);
  PASS_EXCEPTIONS_RET(env, -1);
  if (fd_handle == NULL) {
    THROW(env, "java/lang/IllegalStateException",
          "java.io.FileDescriptor natives not initialized");
    return -1;
  }
  return (long) (*env)->GetLongField(env, obj, fd_handle);
}

/*
 * Create a FileDescriptor object corresponding to the given int fd
 */
jobject fd_create(JNIEnv *env, long fd) {
  jobject obj;

  fd_init(env);
  PASS_EXCEPTIONS_RET(env, (jobject) NULL);

  if (fd_class == NULL || fd_constructor == NULL || fd_handle == NULL) {
    THROW(env, "java/lang/IllegalStateException",
          "java.io.FileDescriptor natives not initialized");
    return NULL;
  }

  obj = (*env)->NewObject(env, fd_class, fd_constructor);
  PASS_EXCEPTIONS_RET(env, (jobject) NULL);

  (*env)->SetLongField(env, obj, fd_handle, fd);
  return obj;
}
#endif
