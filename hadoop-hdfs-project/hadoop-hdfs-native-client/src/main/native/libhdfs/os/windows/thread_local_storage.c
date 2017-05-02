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
#include <malloc.h>
#include <stdio.h>
#include <windows.h>

/** Key that allows us to retrieve thread-local storage */
static DWORD gTlsIndex = TLS_OUT_OF_INDEXES;

/**
 * If the current thread has a JNIEnv in thread-local storage, then detaches the
 * current thread from the JVM and also frees up the ThreadLocalState object.
 */
static void detachCurrentThreadFromJvm()
{
  struct ThreadLocalState *state = NULL;
  JNIEnv *env = NULL;
  JavaVM *vm;
  jint ret;
  if (threadLocalStorageGet(&state) || !state) {
    return;
  }
  if (!state->env) {
    return;
  }
  env = state->env;
  ret = (*env)->GetJavaVM(env, &vm);
  if (ret) {
    fprintf(stderr,
      "detachCurrentThreadFromJvm: GetJavaVM failed with error %d\n",
      ret);
    (*env)->ExceptionDescribe(env);
  } else {
    (*vm)->DetachCurrentThread(vm);
  }

  /* Free exception strings */
  if (state->lastExceptionStackTrace) free(state->lastExceptionStackTrace);
  if (state->lastExceptionRootCause) free(state->lastExceptionRootCause);

  /* Free the state itself */
  free(state);
}

void hdfsThreadDestructor(void *v)
{
  // Ignore 'v' since it will contain the state and we will obtain it in the below
  // call anyway.
  detachCurrentThreadFromJvm();
}

/**
 * Unlike pthreads, the Windows API does not seem to provide a convenient way to
 * hook a callback onto thread shutdown.  However, the Windows portable
 * executable format does define a concept of thread-local storage callbacks.
 * Here, we define a function and instruct the linker to set a pointer to that
 * function in the segment for thread-local storage callbacks.  See page 85 of
 * Microsoft Portable Executable and Common Object File Format Specification:
 * http://msdn.microsoft.com/en-us/gg463119.aspx
 * This technique only works for implicit linking (OS loads DLL on demand), not
 * for explicit linking (user code calls LoadLibrary directly).  This effectively
 * means that we have a known limitation: libhdfs may not work correctly if a
 * Windows application attempts to use it via explicit linking.
 *
 * @param h module handle
 * @param reason the reason for calling the callback
 * @param pv reserved, unused
 */
static void NTAPI tlsCallback(PVOID h, DWORD reason, PVOID pv)
{
  DWORD tlsIndex;
  switch (reason) {
  case DLL_THREAD_DETACH:
    detachCurrentThreadFromJvm();
    break;
  case DLL_PROCESS_DETACH:
    detachCurrentThreadFromJvm();
    tlsIndex = gTlsIndex;
    gTlsIndex = TLS_OUT_OF_INDEXES;
    if (!TlsFree(tlsIndex)) {
      fprintf(stderr, "tlsCallback: TlsFree failed with error %d\n",
        GetLastError());
    }
    break;
  default:
    break;
  }
}

/*
 * A variable named _tls_used contains the TLS directory, which contains a list
 * of pointers to callback functions.  Normally, the linker won't retain this
 * variable unless the executable has implicit thread-local variables, defined
 * using the __declspec(thread) extended storage-class modifier.  libhdfs
 * doesn't use __declspec(thread), and we have no guarantee that the executable
 * linked to libhdfs will use __declspec(thread).  By forcing the linker to
 * reference _tls_used, we guarantee that the binary retains the TLS directory.
 * See Microsoft Visual Studio 10.0/VC/crt/src/tlssup.c .
 */
#ifdef _WIN64
#pragma comment(linker, "/INCLUDE:_tls_used")
#else
#pragma comment(linker, "/INCLUDE:__tls_used")
#endif

/*
 * We must retain a pointer to the callback function.  Force the linker to keep
 * this symbol, even though it appears that nothing in our source code uses it.
 */
#ifdef _WIN64
#pragma comment(linker, "/INCLUDE:pTlsCallback")
#else
#pragma comment(linker, "/INCLUDE:_pTlsCallback")
#endif

/*
 * Define constant pointer to our callback, and tell the linker to pin it into
 * the TLS directory so that it receives thread callbacks.  Use external linkage
 * to protect against the linker discarding the seemingly unused symbol.
 */
#pragma const_seg(".CRT$XLB")
extern const PIMAGE_TLS_CALLBACK pTlsCallback;
const PIMAGE_TLS_CALLBACK pTlsCallback = tlsCallback;
#pragma const_seg()

struct ThreadLocalState* threadLocalStorageCreate()
{
  struct ThreadLocalState *state;
  state = (struct ThreadLocalState*)malloc(sizeof(struct ThreadLocalState));
  if (state == NULL) {
    fprintf(stderr,
      "threadLocalStorageSet: OOM - Unable to allocate thread local state\n");
    return NULL;
  }
  state->lastExceptionStackTrace = NULL;
  state->lastExceptionRootCause = NULL;
  return state;
}

int threadLocalStorageGet(struct ThreadLocalState **state)
{
  LPVOID tls;
  DWORD ret;
  if (TLS_OUT_OF_INDEXES == gTlsIndex) {
    gTlsIndex = TlsAlloc();
    if (TLS_OUT_OF_INDEXES == gTlsIndex) {
      fprintf(stderr,
        "threadLocalStorageGet: TlsAlloc failed with error %d\n",
        TLS_OUT_OF_INDEXES);
      return TLS_OUT_OF_INDEXES;
    }
  }
  tls = TlsGetValue(gTlsIndex);
  if (tls) {
    *state = tls;
    return 0;
  } else {
    ret = GetLastError();
    if (ERROR_SUCCESS == ret) {
      /* Thread-local storage contains NULL, because we haven't set it yet. */
      *state = NULL;
      return 0;
    } else {
      /*
       * The API call failed.  According to documentation, TlsGetValue cannot
       * fail as long as the index is a valid index from a successful TlsAlloc
       * call.  This error handling is purely defensive.
       */
      fprintf(stderr,
        "threadLocalStorageGet: TlsGetValue failed with error %d\n", ret);
      return ret;
    }
  }
}

int threadLocalStorageSet(struct ThreadLocalState *state)
{
  DWORD ret = 0;
  if (!TlsSetValue(gTlsIndex, (LPVOID)state)) {
    ret = GetLastError();
    fprintf(stderr,
      "threadLocalStorageSet: TlsSetValue failed with error %d\n",
      ret);
    detachCurrentThreadFromJvm(state);
  }
  return ret;
}
