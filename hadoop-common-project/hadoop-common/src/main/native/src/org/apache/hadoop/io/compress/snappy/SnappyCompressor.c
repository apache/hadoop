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


#if defined HADOOP_SNAPPY_LIBRARY

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef UNIX
#include <dlfcn.h>
#include "config.h"
#endif // UNIX

#include "org_apache_hadoop_io_compress_snappy.h"
#include "org_apache_hadoop_io_compress_snappy_SnappyCompressor.h"

#define JINT_MAX 0x7fffffff

static jfieldID SnappyCompressor_clazz;
static jfieldID SnappyCompressor_uncompressedDirectBuf;
static jfieldID SnappyCompressor_uncompressedDirectBufLen;
static jfieldID SnappyCompressor_compressedDirectBuf;
static jfieldID SnappyCompressor_directBufferSize;

static snappy_status (*dlsym_snappy_compress)(const char*, size_t, char*, size_t*);

JNIEXPORT void JNICALL Java_org_apache_hadoop_io_compress_snappy_SnappyCompressor_initIDs
(JNIEnv *env, jclass clazz){

  // Load libsnappy.so
  void *libsnappy = dlopen(HADOOP_SNAPPY_LIBRARY, RTLD_LAZY | RTLD_GLOBAL);
  if (!libsnappy) {
    char msg[1000];
    snprintf(msg, 1000, "%s (%s)!", "Cannot load " HADOOP_SNAPPY_LIBRARY, dlerror());
    THROW(env, "java/lang/UnsatisfiedLinkError", msg);
    return;
  }

  // Locate the requisite symbols from libsnappy.so
  dlerror();                                 // Clear any existing error
  LOAD_DYNAMIC_SYMBOL(dlsym_snappy_compress, env, libsnappy, "snappy_compress");

  SnappyCompressor_clazz = (*env)->GetStaticFieldID(env, clazz, "clazz",
                                                 "Ljava/lang/Class;");
  SnappyCompressor_uncompressedDirectBuf = (*env)->GetFieldID(env, clazz,
                                                           "uncompressedDirectBuf",
                                                           "Ljava/nio/Buffer;");
  SnappyCompressor_uncompressedDirectBufLen = (*env)->GetFieldID(env, clazz,
                                                              "uncompressedDirectBufLen", "I");
  SnappyCompressor_compressedDirectBuf = (*env)->GetFieldID(env, clazz,
                                                         "compressedDirectBuf",
                                                         "Ljava/nio/Buffer;");
  SnappyCompressor_directBufferSize = (*env)->GetFieldID(env, clazz,
                                                       "directBufferSize", "I");
}

JNIEXPORT jint JNICALL Java_org_apache_hadoop_io_compress_snappy_SnappyCompressor_compressBytesDirect
(JNIEnv *env, jobject thisj){
  // Get members of SnappyCompressor
  jobject clazz = (*env)->GetStaticObjectField(env, thisj, SnappyCompressor_clazz);
  jobject uncompressed_direct_buf = (*env)->GetObjectField(env, thisj, SnappyCompressor_uncompressedDirectBuf);
  jint uncompressed_direct_buf_len = (*env)->GetIntField(env, thisj, SnappyCompressor_uncompressedDirectBufLen);
  jobject compressed_direct_buf = (*env)->GetObjectField(env, thisj, SnappyCompressor_compressedDirectBuf);
  jint compressed_direct_buf_len = (*env)->GetIntField(env, thisj, SnappyCompressor_directBufferSize);
  size_t buf_len;

  // Get the input direct buffer
  LOCK_CLASS(env, clazz, "SnappyCompressor");
  const char* uncompressed_bytes = (const char*)(*env)->GetDirectBufferAddress(env, uncompressed_direct_buf);
  UNLOCK_CLASS(env, clazz, "SnappyCompressor");

  if (uncompressed_bytes == 0) {
    return (jint)0;
  }

  // Get the output direct buffer
  LOCK_CLASS(env, clazz, "SnappyCompressor");
  char* compressed_bytes = (char *)(*env)->GetDirectBufferAddress(env, compressed_direct_buf);
  UNLOCK_CLASS(env, clazz, "SnappyCompressor");

  if (compressed_bytes == 0) {
    return (jint)0;
  }

  /* size_t should always be 4 bytes or larger. */
  buf_len = (size_t)compressed_direct_buf_len;
  snappy_status ret = dlsym_snappy_compress(uncompressed_bytes,
        uncompressed_direct_buf_len, compressed_bytes, &buf_len);
  if (ret != SNAPPY_OK){
    THROW(env, "java/lang/InternalError", "Could not compress data. Buffer length is too small.");
    return 0;
  }
  if (buf_len > JINT_MAX) {
    THROW(env, "java/lang/InternalError", "Invalid return buffer length.");
    return 0;
  }

  (*env)->SetIntField(env, thisj, SnappyCompressor_uncompressedDirectBufLen, 0);
  return (jint)buf_len;
}

#endif //define HADOOP_SNAPPY_LIBRARY
