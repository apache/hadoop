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

#include "org_apache_hadoop_io_compress_snappy.h"

#if defined HADOOP_SNAPPY_LIBRARY

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef UNIX
#include <dlfcn.h>
#include "config.h"
#endif

#include "org_apache_hadoop_io_compress_snappy_SnappyDecompressor.h"

static jfieldID SnappyDecompressor_compressedDirectBuf;
static jfieldID SnappyDecompressor_compressedDirectBufLen;
static jfieldID SnappyDecompressor_uncompressedDirectBuf;
static jfieldID SnappyDecompressor_directBufferSize;

#ifdef UNIX
static snappy_status (*dlsym_snappy_uncompress)(const char*, size_t, char*, size_t*);
#endif

#ifdef WINDOWS
typedef snappy_status (__cdecl *__dlsym_snappy_uncompress)(const char*, size_t, char*, size_t*);
static __dlsym_snappy_uncompress dlsym_snappy_uncompress;
#endif

JNIEXPORT void JNICALL Java_org_apache_hadoop_io_compress_snappy_SnappyDecompressor_initIDs
(JNIEnv *env, jclass clazz){

  // Load libsnappy.so
#ifdef UNIX
  void *libsnappy = dlopen(HADOOP_SNAPPY_LIBRARY, RTLD_LAZY | RTLD_GLOBAL);
  if (!libsnappy) {
    char* msg = (char*)malloc(1000);
    snprintf(msg, 1000, "%s (%s)!", "Cannot load " HADOOP_SNAPPY_LIBRARY, dlerror());
    THROW(env, "java/lang/UnsatisfiedLinkError", msg);
    return;
  }
#endif

#ifdef WINDOWS
  HMODULE libsnappy = LoadLibrary(HADOOP_SNAPPY_LIBRARY);
  if (!libsnappy) {
    THROW(env, "java/lang/UnsatisfiedLinkError", "Cannot load snappy.dll");
    return;
  }
#endif

  // Locate the requisite symbols from libsnappy.so
#ifdef UNIX
  dlerror();                                 // Clear any existing error
  LOAD_DYNAMIC_SYMBOL(dlsym_snappy_uncompress, env, libsnappy, "snappy_uncompress");

#endif

#ifdef WINDOWS
  LOAD_DYNAMIC_SYMBOL(__dlsym_snappy_uncompress, dlsym_snappy_uncompress, env, libsnappy, "snappy_uncompress");
#endif

  SnappyDecompressor_compressedDirectBuf = (*env)->GetFieldID(env,clazz,
                                                           "compressedDirectBuf",
                                                           "Ljava/nio/Buffer;");
  SnappyDecompressor_compressedDirectBufLen = (*env)->GetFieldID(env,clazz,
                                                              "compressedDirectBufLen", "I");
  SnappyDecompressor_uncompressedDirectBuf = (*env)->GetFieldID(env,clazz,
                                                             "uncompressedDirectBuf",
                                                             "Ljava/nio/Buffer;");
  SnappyDecompressor_directBufferSize = (*env)->GetFieldID(env, clazz,
                                                         "directBufferSize", "I");
}

JNIEXPORT jint JNICALL Java_org_apache_hadoop_io_compress_snappy_SnappyDecompressor_decompressBytesDirect
(JNIEnv *env, jobject thisj){
  const char* compressed_bytes = NULL;
  char* uncompressed_bytes = NULL;
  snappy_status ret;
  // Get members of SnappyDecompressor
  jobject compressed_direct_buf = (*env)->GetObjectField(env,thisj, SnappyDecompressor_compressedDirectBuf);
  jint compressed_direct_buf_len = (*env)->GetIntField(env,thisj, SnappyDecompressor_compressedDirectBufLen);
  jobject uncompressed_direct_buf = (*env)->GetObjectField(env,thisj, SnappyDecompressor_uncompressedDirectBuf);
  size_t uncompressed_direct_buf_len = (*env)->GetIntField(env, thisj, SnappyDecompressor_directBufferSize);

  // Get the input direct buffer
  compressed_bytes = (const char*)(*env)->GetDirectBufferAddress(env, compressed_direct_buf);

  if (compressed_bytes == 0) {
    return (jint)0;
  }

  // Get the output direct buffer
  uncompressed_bytes = (char *)(*env)->GetDirectBufferAddress(env, uncompressed_direct_buf);

  if (uncompressed_bytes == 0) {
    return (jint)0;
  }

  ret = dlsym_snappy_uncompress(compressed_bytes, compressed_direct_buf_len,
        uncompressed_bytes, &uncompressed_direct_buf_len);
  if (ret == SNAPPY_BUFFER_TOO_SMALL){
    THROW(env, "java/lang/InternalError", "Could not decompress data. Buffer length is too small.");
  } else if (ret == SNAPPY_INVALID_INPUT){
    THROW(env, "java/lang/InternalError", "Could not decompress data. Input is invalid.");
  } else if (ret != SNAPPY_OK){
    THROW(env, "java/lang/InternalError", "Could not decompress data.");
  }

  (*env)->SetIntField(env, thisj, SnappyDecompressor_compressedDirectBufLen, 0);

  return (jint)uncompressed_direct_buf_len;
}

#endif //define HADOOP_SNAPPY_LIBRARY
