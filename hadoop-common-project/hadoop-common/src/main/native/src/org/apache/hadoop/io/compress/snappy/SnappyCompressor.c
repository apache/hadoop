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
#endif // UNIX

#ifdef WINDOWS
#include "winutils.h"
#endif

#include "org_apache_hadoop_io_compress_snappy_SnappyCompressor.h"

#define JINT_MAX 0x7fffffff

static jfieldID SnappyCompressor_uncompressedDirectBuf;
static jfieldID SnappyCompressor_uncompressedDirectBufLen;
static jfieldID SnappyCompressor_compressedDirectBuf;
static jfieldID SnappyCompressor_directBufferSize;

#ifdef UNIX
static snappy_status (*dlsym_snappy_compress)(const char*, size_t, char*, size_t*);
#endif

#ifdef WINDOWS
typedef snappy_status (__cdecl *__dlsym_snappy_compress)(const char*, size_t, char*, size_t*);
static __dlsym_snappy_compress dlsym_snappy_compress;
#endif

JNIEXPORT void JNICALL Java_org_apache_hadoop_io_compress_snappy_SnappyCompressor_initIDs
(JNIEnv *env, jclass clazz){
#ifdef UNIX
  // Load libsnappy.so
  void *libsnappy = dlopen(HADOOP_SNAPPY_LIBRARY, RTLD_LAZY | RTLD_GLOBAL);
  if (!libsnappy) {
    char msg[1000];
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
  LOAD_DYNAMIC_SYMBOL(dlsym_snappy_compress, env, libsnappy, "snappy_compress");
#endif

#ifdef WINDOWS
  LOAD_DYNAMIC_SYMBOL(__dlsym_snappy_compress, dlsym_snappy_compress, env, libsnappy, "snappy_compress");
#endif

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
  const char* uncompressed_bytes;
  char* compressed_bytes;
  snappy_status ret;
  // Get members of SnappyCompressor
  jobject uncompressed_direct_buf = (*env)->GetObjectField(env, thisj, SnappyCompressor_uncompressedDirectBuf);
  jint uncompressed_direct_buf_len = (*env)->GetIntField(env, thisj, SnappyCompressor_uncompressedDirectBufLen);
  jobject compressed_direct_buf = (*env)->GetObjectField(env, thisj, SnappyCompressor_compressedDirectBuf);
  jint compressed_direct_buf_len = (*env)->GetIntField(env, thisj, SnappyCompressor_directBufferSize);
  size_t buf_len;

  // Get the input direct buffer
  uncompressed_bytes = (const char*)(*env)->GetDirectBufferAddress(env, uncompressed_direct_buf);

  if (uncompressed_bytes == 0) {
    return (jint)0;
  }

  // Get the output direct buffer
  compressed_bytes = (char *)(*env)->GetDirectBufferAddress(env, compressed_direct_buf);

  if (compressed_bytes == 0) {
    return (jint)0;
  }

  /* size_t should always be 4 bytes or larger. */
  buf_len = (size_t)compressed_direct_buf_len;
  ret = dlsym_snappy_compress(uncompressed_bytes, uncompressed_direct_buf_len,
        compressed_bytes, &buf_len);
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

JNIEXPORT jstring JNICALL
Java_org_apache_hadoop_io_compress_snappy_SnappyCompressor_getLibraryName(JNIEnv *env, jclass class) {
#ifdef UNIX
  if (dlsym_snappy_compress) {
    Dl_info dl_info;
    if(dladdr(
        dlsym_snappy_compress,
        &dl_info)) {
      return (*env)->NewStringUTF(env, dl_info.dli_fname);
    }
  }

  return (*env)->NewStringUTF(env, HADOOP_SNAPPY_LIBRARY);
#endif

#ifdef WINDOWS
  LPWSTR filename = NULL;
  GetLibraryName(dlsym_snappy_compress, &filename);
  if (filename != NULL) {
    return (*env)->NewString(env, filename, (jsize) wcslen(filename));
  } else {
    return (*env)->NewStringUTF(env, "Unavailable");
  }
#endif
}
#endif //define HADOOP_SNAPPY_LIBRARY
