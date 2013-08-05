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
#include "org_apache_hadoop_io_compress_lz4_Lz4Decompressor.h"

#ifdef UNIX
#include "config.h"
#endif // UNIX
#include "lz4.h"


static jfieldID Lz4Decompressor_clazz;
static jfieldID Lz4Decompressor_compressedDirectBuf;
static jfieldID Lz4Decompressor_compressedDirectBufLen;
static jfieldID Lz4Decompressor_uncompressedDirectBuf;
static jfieldID Lz4Decompressor_directBufferSize;

JNIEXPORT void JNICALL Java_org_apache_hadoop_io_compress_lz4_Lz4Decompressor_initIDs
(JNIEnv *env, jclass clazz){

  Lz4Decompressor_clazz = (*env)->GetStaticFieldID(env, clazz, "clazz",
                                                   "Ljava/lang/Class;");
  Lz4Decompressor_compressedDirectBuf = (*env)->GetFieldID(env,clazz,
                                                           "compressedDirectBuf",
                                                           "Ljava/nio/Buffer;");
  Lz4Decompressor_compressedDirectBufLen = (*env)->GetFieldID(env,clazz,
                                                              "compressedDirectBufLen", "I");
  Lz4Decompressor_uncompressedDirectBuf = (*env)->GetFieldID(env,clazz,
                                                             "uncompressedDirectBuf",
                                                             "Ljava/nio/Buffer;");
  Lz4Decompressor_directBufferSize = (*env)->GetFieldID(env, clazz,
                                                         "directBufferSize", "I");
}

JNIEXPORT jint JNICALL Java_org_apache_hadoop_io_compress_lz4_Lz4Decompressor_decompressBytesDirect
(JNIEnv *env, jobject thisj){
  const char *compressed_bytes;
  char *uncompressed_bytes;

  // Get members of Lz4Decompressor
  jobject clazz = (*env)->GetStaticObjectField(env,thisj, Lz4Decompressor_clazz);
  jobject compressed_direct_buf = (*env)->GetObjectField(env,thisj, Lz4Decompressor_compressedDirectBuf);
  jint compressed_direct_buf_len = (*env)->GetIntField(env,thisj, Lz4Decompressor_compressedDirectBufLen);
  jobject uncompressed_direct_buf = (*env)->GetObjectField(env,thisj, Lz4Decompressor_uncompressedDirectBuf);
  size_t uncompressed_direct_buf_len = (*env)->GetIntField(env, thisj, Lz4Decompressor_directBufferSize);

  // Get the input direct buffer
  LOCK_CLASS(env, clazz, "Lz4Decompressor");
  compressed_bytes = (const char*)(*env)->GetDirectBufferAddress(env, compressed_direct_buf);
  UNLOCK_CLASS(env, clazz, "Lz4Decompressor");

  if (compressed_bytes == 0) {
    return (jint)0;
  }

  // Get the output direct buffer
  LOCK_CLASS(env, clazz, "Lz4Decompressor");
  uncompressed_bytes = (char *)(*env)->GetDirectBufferAddress(env, uncompressed_direct_buf);
  UNLOCK_CLASS(env, clazz, "Lz4Decompressor");

  if (uncompressed_bytes == 0) {
    return (jint)0;
  }

  uncompressed_direct_buf_len = LZ4_decompress_safe(compressed_bytes, uncompressed_bytes, compressed_direct_buf_len, uncompressed_direct_buf_len);
  if (uncompressed_direct_buf_len < 0) {
    THROW(env, "java/lang/InternalError", "LZ4_uncompress_unknownOutputSize failed.");
  }

  (*env)->SetIntField(env, thisj, Lz4Decompressor_compressedDirectBufLen, 0);

  return (jint)uncompressed_direct_buf_len;
}
