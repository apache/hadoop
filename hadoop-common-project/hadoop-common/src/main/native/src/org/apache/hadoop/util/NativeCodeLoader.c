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
#include <dlfcn.h>
#include "config.h"
#endif // UNIX

#include <jni.h>

JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_util_NativeCodeLoader_buildSupportsSnappy
  (JNIEnv *env, jclass clazz)
{
#ifdef HADOOP_SNAPPY_LIBRARY
  return JNI_TRUE;
#else
  return JNI_FALSE;
#endif
}

JNIEXPORT jstring JNICALL Java_org_apache_hadoop_util_NativeCodeLoader_getLibraryName
  (JNIEnv *env, jclass clazz)
{
#ifdef UNIX
  Dl_info dl_info;
  int ret = dladdr(
      Java_org_apache_hadoop_util_NativeCodeLoader_getLibraryName,
      &dl_info);
  return (*env)->NewStringUTF(env, ret==0 ? "Unavailable" : dl_info.dli_fname);
#endif

#ifdef WINDOWS
  SIZE_T ret = 0;
  DWORD size = MAX_PATH;
  LPWSTR filename = NULL;
  HMODULE mod = NULL;
  DWORD err = ERROR_SUCCESS;

  MEMORY_BASIC_INFORMATION mbi;
  ret = VirtualQuery(Java_org_apache_hadoop_util_NativeCodeLoader_getLibraryName,
    &mbi, sizeof(mbi));
  if (ret == 0) goto cleanup;
  mod = mbi.AllocationBase;

  do {
    filename = (LPWSTR) realloc(filename, size * sizeof(WCHAR));
    if (filename == NULL) goto cleanup;
    GetModuleFileName(mod, filename, size);
    size <<= 1;
    err = GetLastError();
  } while (err == ERROR_INSUFFICIENT_BUFFER);
  
  if (err != ERROR_SUCCESS) goto cleanup;

  return (*env)->NewString(env, filename, (jsize) wcslen(filename));

cleanup:
  if (filename != NULL) free(filename);
  return (*env)->NewStringUTF(env, "Unavailable");
#endif
}
