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
#include "org_apache_hadoop_util_NativeCrc32.h"

#include <assert.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#ifdef UNIX
#include <inttypes.h>
#include <arpa/inet.h>
#include <unistd.h>
#include "config.h"
#include "gcc_optimizations.h"
#endif // UNIX

#include "bulk_crc32.h"

static void throw_checksum_exception(JNIEnv *env,
    uint32_t got_crc, uint32_t expected_crc,
    jstring j_filename, jlong pos) {
  char message[1024];
  jstring jstr_message;
  char *filename;
  jclass checksum_exception_clazz;
  jmethodID checksum_exception_ctor;
  jthrowable obj;

  // Get filename as C string, or "null" if not provided
  if (j_filename == NULL) {
    filename = strdup("null");
  } else {
    const char *c_filename = (*env)->GetStringUTFChars(env, j_filename, NULL);
    if (c_filename == NULL) {
      return; // OOME already thrown
    }
    filename = strdup(c_filename);
    (*env)->ReleaseStringUTFChars(env, j_filename, c_filename);
  }

  // Format error message
#ifdef WINDOWS
  _snprintf_s(
	message,
	sizeof(message),
	_TRUNCATE,
    "Checksum error: %s at %I64d exp: %d got: %d",
    filename, pos, expected_crc, got_crc);
#else
  snprintf(message, sizeof(message),
    "Checksum error: %s at %"PRId64" exp: %"PRId32" got: %"PRId32,
    filename, pos, expected_crc, got_crc);
#endif // WINDOWS

  if ((jstr_message = (*env)->NewStringUTF(env, message)) == NULL) {
    goto cleanup;
  }
 
  // Throw exception
  checksum_exception_clazz = (*env)->FindClass(
    env, "org/apache/hadoop/fs/ChecksumException");
  if (checksum_exception_clazz == NULL) {
    goto cleanup;
  }

  checksum_exception_ctor = (*env)->GetMethodID(env,
    checksum_exception_clazz, "<init>",
    "(Ljava/lang/String;J)V");
  if (checksum_exception_ctor == NULL) {
    goto cleanup;
  }

  obj = (jthrowable)(*env)->NewObject(env, checksum_exception_clazz,
    checksum_exception_ctor, jstr_message, pos);
  if (obj == NULL) goto cleanup;

  (*env)->Throw(env, obj);

cleanup:
  if (filename != NULL) {
    free(filename);
  }
}

static int convert_java_crc_type(JNIEnv *env, jint crc_type) {
  switch (crc_type) {
    case org_apache_hadoop_util_NativeCrc32_CHECKSUM_CRC32:
      return CRC32_ZLIB_POLYNOMIAL;
    case org_apache_hadoop_util_NativeCrc32_CHECKSUM_CRC32C:
      return CRC32C_POLYNOMIAL;
    default:
      THROW(env, "java/lang/IllegalArgumentException",
        "Invalid checksum type");
      return -1;
  }
}

JNIEXPORT void JNICALL Java_org_apache_hadoop_util_NativeCrc32_nativeVerifyChunkedSums
  (JNIEnv *env, jclass clazz,
    jint bytes_per_checksum, jint j_crc_type,
    jobject j_sums, jint sums_offset,
    jobject j_data, jint data_offset, jint data_len,
    jstring j_filename, jlong base_pos)
{
  uint8_t *sums_addr;
  uint8_t *data_addr;
  uint32_t *sums;
  uint8_t *data;
  int crc_type;
  crc32_error_t error_data;
  int ret;

  if (unlikely(!j_sums || !j_data)) {
    THROW(env, "java/lang/NullPointerException",
      "input ByteBuffers must not be null");
    return;
  }

  // Convert direct byte buffers to C pointers
  sums_addr = (*env)->GetDirectBufferAddress(env, j_sums);
  data_addr = (*env)->GetDirectBufferAddress(env, j_data);

  if (unlikely(!sums_addr || !data_addr)) {
    THROW(env, "java/lang/IllegalArgumentException",
      "input ByteBuffers must be direct buffers");
    return;
  }
  if (unlikely(sums_offset < 0 || data_offset < 0 || data_len < 0)) {
    THROW(env, "java/lang/IllegalArgumentException",
      "bad offsets or lengths");
    return;
  }
  if (unlikely(bytes_per_checksum) <= 0) {
    THROW(env, "java/lang/IllegalArgumentException",
      "invalid bytes_per_checksum");
    return;
  }

  sums = (uint32_t *)(sums_addr + sums_offset);
  data = data_addr + data_offset;

  // Convert to correct internal C constant for CRC type
  crc_type = convert_java_crc_type(env, j_crc_type);
  if (crc_type == -1) return; // exception already thrown

  // Setup complete. Actually verify checksums.
  ret = bulk_verify_crc(data, data_len, sums, crc_type,
                            bytes_per_checksum, &error_data);
  if (likely(ret == CHECKSUMS_VALID)) {
    return;
  } else if (unlikely(ret == INVALID_CHECKSUM_DETECTED)) {
    long pos = base_pos + (error_data.bad_data - data);
    throw_checksum_exception(
      env, error_data.got_crc, error_data.expected_crc,
      j_filename, pos);
  } else {
    THROW(env, "java/lang/AssertionError",
      "Bad response code from native bulk_verify_crc");
  }
}

/**
 * vim: sw=2: ts=2: et:
 */
