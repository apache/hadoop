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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "org_apache_hadoop.h"
#include "../include/erasure_code.h"
#include "org_apache_hadoop_io_erasurecode_ErasureCodeNative.h"

#ifdef UNIX
#include "config.h"
#endif

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_erasurecode_ErasureCodeNative_loadLibrary
(JNIEnv *env, jclass myclass) {
  char errMsg[1024];
  load_erasurecode_lib(errMsg, sizeof(errMsg));
  if (strlen(errMsg) > 0) {
    THROW(env, "java/lang/UnsatisfiedLinkError", errMsg);
  }
}

JNIEXPORT jstring JNICALL
Java_org_apache_hadoop_io_erasurecode_ErasureCodeNative_getLibraryName
(JNIEnv *env, jclass myclass) {
  char* libName = get_library_name();
  if (libName == NULL) {
    libName = "Unavailable";
  }
  return (*env)->NewStringUTF(env, libName);
}
