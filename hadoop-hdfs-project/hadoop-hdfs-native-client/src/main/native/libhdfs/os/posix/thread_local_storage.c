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
#include <stdlib.h>
#include <pthread.h>
#include <stdio.h>

#include "exception.h"
#include "jni_helper.h"

#define UNKNOWN "UNKNOWN"
#define MAXTHRID 256

/** Key that allows us to retrieve thread-local storage */
static pthread_key_t gTlsKey;

/** nonzero if we succeeded in initializing gTlsKey. Protected by the jvmMutex */
static int gTlsKeyInitialized = 0;

static void get_current_thread_id(JNIEnv* env, char* id, int max);

/**
 * The function that is called whenever a thread with libhdfs thread local data
 * is destroyed.
 *
 * @param v         The thread-local data
 */
void hdfsThreadDestructor(void *v)
{
  JavaVM *vm;
  struct ThreadLocalState *state = (struct ThreadLocalState*)v;
  JNIEnv *env = state->env;;
  jint ret;
  jthrowable jthr;
  char thr_name[MAXTHRID];

  /* Detach the current thread from the JVM */
  if ((env != NULL) && (*env != NULL)) {
    ret = (*env)->GetJavaVM(env, &vm);

    if (ret != 0) {
      fprintf(stderr, "hdfsThreadDestructor: GetJavaVM failed with error %d\n",
        ret);
      jthr = (*env)->ExceptionOccurred(env);
      if (jthr) {
        (*env)->ExceptionDescribe(env);
        (*env)->ExceptionClear(env);
      }
    } else {
      ret = (*vm)->DetachCurrentThread(vm);

      if (ret != JNI_OK) {
        jthr = (*env)->ExceptionOccurred(env);
        if (jthr) {
          (*env)->ExceptionDescribe(env);
          (*env)->ExceptionClear(env);
        }
        get_current_thread_id(env, thr_name, MAXTHRID);

        fprintf(stderr, "hdfsThreadDestructor: Unable to detach thread %s "
            "from the JVM. Error code: %d\n", thr_name, ret);
      }
    }
  }

  /* Free exception strings */
  if (state->lastExceptionStackTrace) free(state->lastExceptionStackTrace);
  if (state->lastExceptionRootCause) free(state->lastExceptionRootCause);

  /* Free the state itself */
  free(state);
}

static void get_current_thread_id(JNIEnv* env, char* id, int max) {
  jvalue jVal;
  jobject thr = NULL;
  jstring thr_name = NULL;
  jlong thr_id = 0;
  jthrowable jthr = NULL;
  const char *thr_name_str;

  jthr = findClassAndInvokeMethod(env, &jVal, STATIC, NULL, "java/lang/Thread",
          "currentThread", "()Ljava/lang/Thread;");
  if (jthr) {
    snprintf(id, max, "%s", UNKNOWN);
    printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "get_current_thread_id: Thread#currentThread failed: ");
    goto done;
  }
  thr = jVal.l;

  jthr = findClassAndInvokeMethod(env, &jVal, INSTANCE, thr,
          "java/lang/Thread", "getId", "()J");
  if (jthr) {
    snprintf(id, max, "%s", UNKNOWN);
    printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "get_current_thread_id: Thread#getId failed: ");
    goto done;
  }
  thr_id = jVal.j;

  jthr = findClassAndInvokeMethod(env, &jVal, INSTANCE, thr,
          "java/lang/Thread", "toString", "()Ljava/lang/String;");
  if (jthr) {
    snprintf(id, max, "%s:%ld", UNKNOWN, thr_id);
    printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "get_current_thread_id: Thread#toString failed: ");
    goto done;
  }
  thr_name = jVal.l;

  thr_name_str = (*env)->GetStringUTFChars(env, thr_name, NULL);
  if (!thr_name_str) {
    printPendingExceptionAndFree(env, PRINT_EXC_ALL,
            "get_current_thread_id: GetStringUTFChars failed: ");
    snprintf(id, max, "%s:%ld", UNKNOWN, thr_id);
    goto done;
  }

  // Treating the jlong as a long *should* be safe
  snprintf(id, max, "%s:%ld", thr_name_str, thr_id);

  // Release the char*
  (*env)->ReleaseStringUTFChars(env, thr_name, thr_name_str);

done:
  destroyLocalReference(env, thr);
  destroyLocalReference(env, thr_name);

  // Make sure the id is null terminated in case we overflow the max length
  id[max - 1] = '\0';
}

struct ThreadLocalState* threadLocalStorageCreate()
{
  struct ThreadLocalState *state;
  state = (struct ThreadLocalState*)malloc(sizeof(struct ThreadLocalState));
  if (state == NULL) {
    fprintf(stderr,
      "threadLocalStorageCreate: OOM - Unable to allocate thread local state\n");
    return NULL;
  }
  state->lastExceptionStackTrace = NULL;
  state->lastExceptionRootCause = NULL;
  return state;
}

int threadLocalStorageGet(struct ThreadLocalState **state)
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
  *state = pthread_getspecific(gTlsKey);
  return ret;
}

int threadLocalStorageSet(struct ThreadLocalState *state)
{
  int ret = pthread_setspecific(gTlsKey, state);
  if (ret) {
    fprintf(stderr,
      "threadLocalStorageSet: pthread_setspecific failed with error %d\n",
      ret);
    hdfsThreadDestructor(state);
  }
  return ret;
}
