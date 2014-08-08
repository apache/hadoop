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

#include "os/thread_local_storage.h"

#include <jni.h>
#include <pthread.h>
#include <stdio.h>

/** Key that allows us to retrieve thread-local storage */
static pthread_key_t gTlsKey;

/** nonzero if we succeeded in initializing gTlsKey. Protected by the jvmMutex */
static int gTlsKeyInitialized = 0;

/**
 * The function that is called whenever a thread with libhdfs thread local data
 * is destroyed.
 *
 * @param v         The thread-local data
 */
static void hdfsThreadDestructor(void *v)
{
  JavaVM *vm;
  JNIEnv *env = v;
  jint ret;

  ret = (*env)->GetJavaVM(env, &vm);
  if (ret) {
    fprintf(stderr, "hdfsThreadDestructor: GetJavaVM failed with error %d\n",
      ret);
    (*env)->ExceptionDescribe(env);
  } else {
    (*vm)->DetachCurrentThread(vm);
  }
}

int threadLocalStorageGet(JNIEnv **env)
{
  int ret = 0;
  if (!gTlsKeyInitialized) {
    ret = pthread_key_create(&gTlsKey, hdfsThreadDestructor);
    if (ret) {
      fprintf(stderr,
        "threadLocalStorageGet: pthread_key_create failed with error %d\n",
        ret);
      return ret;
    }
    gTlsKeyInitialized = 1;
  }
  *env = pthread_getspecific(gTlsKey);
  return ret;
}

int threadLocalStorageSet(JNIEnv *env)
{
  int ret = pthread_setspecific(gTlsKey, env);
  if (ret) {
    fprintf(stderr,
      "threadLocalStorageSet: pthread_setspecific failed with error %d\n",
      ret);
    hdfsThreadDestructor(env);
  }
  return ret;
}
