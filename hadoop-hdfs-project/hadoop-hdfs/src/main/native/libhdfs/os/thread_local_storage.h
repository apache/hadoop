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

#ifndef LIBHDFS_THREAD_LOCAL_STORAGE_H
#define LIBHDFS_THREAD_LOCAL_STORAGE_H

/*
 * Defines abstraction over platform-specific thread-local storage.  libhdfs
 * currently only needs thread-local storage for a single piece of data: the
 * thread's JNIEnv.  For simplicity, this interface is defined in terms of
 * JNIEnv, not general-purpose thread-local storage of any arbitrary data.
 */

#include <jni.h>

/*
 * Most operating systems support the more efficient __thread construct, which
 * is initialized by the linker.  The following macros use this technique on the
 * operating systems that support it.
 */
#ifdef HAVE_BETTER_TLS
  #define THREAD_LOCAL_STORAGE_GET_QUICK() \
    static __thread JNIEnv *quickTlsEnv = NULL; \
    { \
      if (quickTlsEnv) { \
        return quickTlsEnv; \
      } \
    }

  #define THREAD_LOCAL_STORAGE_SET_QUICK(env) \
    { \
      quickTlsEnv = (env); \
    }
#else
  #define THREAD_LOCAL_STORAGE_GET_QUICK()
  #define THREAD_LOCAL_STORAGE_SET_QUICK(env)
#endif

/**
 * Gets the JNIEnv in thread-local storage for the current thread.  If the call
 * succeeds, and there is a JNIEnv associated with this thread, then returns 0
 * and populates env.  If the call succeeds, but there is no JNIEnv associated
 * with this thread, then returns 0 and sets JNIEnv to NULL.  If the call fails,
 * then returns non-zero.  Only one thread at a time may execute this function.
 * The caller is responsible for enforcing mutual exclusion.
 *
 * @param env JNIEnv out parameter
 * @return 0 if successful, non-zero otherwise
 */
int threadLocalStorageGet(JNIEnv **env);

/**
 * Sets the JNIEnv in thread-local storage for the current thread.
 *
 * @param env JNIEnv to set
 * @return 0 if successful, non-zero otherwise
 */
int threadLocalStorageSet(JNIEnv *env);

#endif
