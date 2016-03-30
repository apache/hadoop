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
#include "org_apache_hadoop_io_compress_lz4_Lz4Compressor.h"

#ifdef UNIX
#include "config.h"
#endif // UNIX
#include "lz4.h"
#include "lz4hc.h"


static jfieldID Lz4Compressor_uncompressedDirectBuf;
static jfieldID Lz4Compressor_uncompressedDirectBufLen;
static jfieldID Lz4Compressor_compressedDirectBuf;
static jfieldID Lz4Compressor_directBufferSize;


JNIEXPORT void JNICALL Java_org_apache_hadoop_io_compress_lz4_Lz4Compressor_initIDs
(JNIEnv *env, jclass clazz){

  Lz4Compressor_uncompressedDirectBuf = (*env)->GetFieldID(env, clazz,
                                                           "uncompressedDirectBuf",
                                                           "Ljava/nio/Buffer;");
  Lz4Compressor_uncompressedDirectBufLen = (*env)->GetFieldID(env, clazz,
                                                              "uncompressedDirectBufLen", "I");
  Lz4Compressor_compressedDirectBuf = (*env)->GetFieldID(env, clazz,
                                                         "compressedDirectBuf",
                                                         "Ljava/nio/Buffer;");
  Lz4Compressor_directBufferSize = (*env)->GetFieldID(env, clazz,
                                                       "directBufferSize", "I");
}

JNIEXPORT jint JNICALL Java_org_apache_hadoop_io_compress_lz4_Lz4Compressor_compressBytesDirect
(JNIEnv *env, jobject thisj){
  const char* uncompressed_bytes;
  char *compressed_bytes;

  // Get members of Lz4Compressor
  jobject uncompressed_direct_buf = (*env)->GetObjectField(env, thisj, Lz4Compressor_uncompressedDirectBuf);
  jint uncompressed_direct_buf_len = (*env)->GetIntField(env, thisj, Lz4Compressor_uncompressedDirectBufLen);
  jobject compressed_direct_buf = (*env)->GetObjectField(env, thisj, Lz4Compressor_compressedDirectBuf);
  jint compressed_direct_buf_len = (*env)->GetIntField(env, thisj, Lz4Compressor_directBufferSize);

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

  compressed_direct_buf_len = LZ4_compress(uncompressed_bytes, compressed_bytes, uncompressed_direct_buf_len);
  if (compressed_direct_buf_len < 0){
    THROW(env, "java/lang/InternalError", "LZ4_compress failed");
  }

  (*env)->SetIntField(env, thisj, Lz4Compressor_uncompressedDirectBufLen, 0);

  return (jint)compressed_direct_buf_len;
}

JNIEXPORT jstring JNICALL
Java_org_apache_hadoop_io_compress_lz4_Lz4Compressor_getLibraryName(
 JNIEnv *env, jclass class
 ) {
  char version_buf[128];
  snprintf(version_buf, sizeof(version_buf), "revision:%d", LZ4_versionNumber());
  return (*env)->NewStringUTF(env, version_buf);
}

JNIEXPORT jint JNICALL Java_org_apache_hadoop_io_compress_lz4_Lz4Compressor_compressBytesDirectHC
(JNIEnv *env, jobject thisj){
  const char* uncompressed_bytes = NULL;
  char* compressed_bytes = NULL;

  // Get members of Lz4Compressor
  jobject uncompressed_direct_buf = (*env)->GetObjectField(env, thisj, Lz4Compressor_uncompressedDirectBuf);
  jint uncompressed_direct_buf_len = (*env)->GetIntField(env, thisj, Lz4Compressor_uncompressedDirectBufLen);
  jobject compressed_direct_buf = (*env)->GetObjectField(env, thisj, Lz4Compressor_compressedDirectBuf);
  jint compressed_direct_buf_len = (*env)->GetIntField(env, thisj, Lz4Compressor_directBufferSize);

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

  compressed_direct_buf_len = LZ4_compressHC(uncompressed_bytes, compressed_bytes, uncompressed_direct_buf_len);
  if (compressed_direct_buf_len < 0){
    THROW(env, "java/lang/InternalError", "LZ4_compressHC failed");
  }

  (*env)->SetIntField(env, thisj, Lz4Compressor_uncompressedDirectBufLen, 0);

  return (jint)compressed_direct_buf_len;
}
