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

#if defined HAVE_CONFIG_H
  #include <config.h>
#endif

#if defined HAVE_STDIO_H
  #include <stdio.h>
#else
  #error 'stdio.h not found'
#endif  

#if defined HAVE_STDLIB_H
  #include <stdlib.h>
#else
  #error 'stdlib.h not found'
#endif  

#if defined HAVE_STRING_H
  #include <string.h>
#else
  #error 'string.h not found'
#endif  

#if defined HAVE_DLFCN_H
  #include <dlfcn.h>
#else
  #error 'dlfcn.h not found'
#endif  

#if defined HADOOP_BZIP2_LIBRARY

#include "org_apache_hadoop_io_compress_bzip2.h"
#include "org_apache_hadoop_io_compress_bzip2_Bzip2Compressor.h"

static jfieldID Bzip2Compressor_clazz;
static jfieldID Bzip2Compressor_stream;
static jfieldID Bzip2Compressor_uncompressedDirectBuf;
static jfieldID Bzip2Compressor_uncompressedDirectBufOff;
static jfieldID Bzip2Compressor_uncompressedDirectBufLen;
static jfieldID Bzip2Compressor_compressedDirectBuf;
static jfieldID Bzip2Compressor_directBufferSize;
static jfieldID Bzip2Compressor_finish;
static jfieldID Bzip2Compressor_finished;

static int (*dlsym_BZ2_bzCompressInit)(bz_stream*, int, int, int);
static int (*dlsym_BZ2_bzCompress)(bz_stream*, int);
static int (*dlsym_BZ2_bzCompressEnd)(bz_stream*);

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_compress_bzip2_Bzip2Compressor_initIDs(
                                 JNIEnv *env, jclass class, jstring libname)
{
    const char* bzlib_name = (*env)->GetStringUTFChars(env, libname, NULL);
    if (strcmp(bzlib_name, "system-native") == 0)
      bzlib_name = HADOOP_BZIP2_LIBRARY;
    // Load the native library.
    void *libbz2 = dlopen(bzlib_name, RTLD_LAZY | RTLD_GLOBAL);
    if (!libbz2) {
        THROW(env, "java/lang/UnsatisfiedLinkError",
              "Cannot load bzip2 native library");
        return;
    }

    // Locate the requisite symbols from libbz2.so.
    dlerror();                                 // Clear any existing error.
    LOAD_DYNAMIC_SYMBOL(dlsym_BZ2_bzCompressInit, env, libbz2,
                        "BZ2_bzCompressInit");
    LOAD_DYNAMIC_SYMBOL(dlsym_BZ2_bzCompress, env, libbz2,
                        "BZ2_bzCompress");
    LOAD_DYNAMIC_SYMBOL(dlsym_BZ2_bzCompressEnd, env, libbz2,
                        "BZ2_bzCompressEnd");

    // Initialize the requisite fieldIds.
    Bzip2Compressor_clazz = (*env)->GetStaticFieldID(env, class, "clazz", 
                                                     "Ljava/lang/Class;");
    Bzip2Compressor_stream = (*env)->GetFieldID(env, class, "stream", "J");
    Bzip2Compressor_finish = (*env)->GetFieldID(env, class, "finish", "Z");
    Bzip2Compressor_finished = (*env)->GetFieldID(env, class, "finished", "Z");
    Bzip2Compressor_uncompressedDirectBuf = (*env)->GetFieldID(env, class, 
                                                       "uncompressedDirectBuf",
                                                       "Ljava/nio/Buffer;");
    Bzip2Compressor_uncompressedDirectBufOff = (*env)->GetFieldID(env, class, 
                                                  "uncompressedDirectBufOff",
                                                  "I");
    Bzip2Compressor_uncompressedDirectBufLen = (*env)->GetFieldID(env, class, 
                                                  "uncompressedDirectBufLen",
                                                  "I");
    Bzip2Compressor_compressedDirectBuf = (*env)->GetFieldID(env, class, 
                                                     "compressedDirectBuf", 
                                                     "Ljava/nio/Buffer;");
    Bzip2Compressor_directBufferSize = (*env)->GetFieldID(env, class, 
                                                  "directBufferSize", "I");
}

JNIEXPORT jlong JNICALL
Java_org_apache_hadoop_io_compress_bzip2_Bzip2Compressor_init(
            JNIEnv *env, jclass class, jint blockSize, jint workFactor)
{
    // Create a bz_stream.
    bz_stream *stream = malloc(sizeof(bz_stream));
    if (!stream) {
        THROW(env, "java/lang/OutOfMemoryError", NULL);
        return (jlong)0;
    }
    memset((void*)stream, 0, sizeof(bz_stream));

    // Initialize stream.
    int rv = (*dlsym_BZ2_bzCompressInit)(stream, blockSize, 0, workFactor);
    if (rv != BZ_OK) {
        // Contingency - Report error by throwing appropriate exceptions.
        free(stream);
        stream = NULL;
        
        switch (rv) {
        case BZ_MEM_ERROR: 
            {
                THROW(env, "java/lang/OutOfMemoryError", NULL);
            }
            break;
        case BZ_PARAM_ERROR:
            {
                THROW(env,
                      "java/lang/IllegalArgumentException",
                      NULL);
            }
            break;
        default:
            {
                THROW(env, "java/lang/InternalError", NULL);
            }
            break;
        }
    }
        
    return JLONG(stream);
}

JNIEXPORT jint JNICALL
Java_org_apache_hadoop_io_compress_bzip2_Bzip2Compressor_deflateBytesDirect(
        JNIEnv *env, jobject this)
{
    // Get members of Bzip2Compressor.
    bz_stream *stream = BZSTREAM((*env)->GetLongField(env, this, 
                                              Bzip2Compressor_stream));
    if (!stream) {
        THROW(env, "java/lang/NullPointerException", NULL);
        return (jint)0;
    } 

    jobject clazz = (*env)->GetStaticObjectField(env, this, 
                                                 Bzip2Compressor_clazz);
    jobject uncompressed_direct_buf = (*env)->GetObjectField(env, this, 
                                     Bzip2Compressor_uncompressedDirectBuf);
    jint uncompressed_direct_buf_off = (*env)->GetIntField(env, this, 
                                   Bzip2Compressor_uncompressedDirectBufOff);
    jint uncompressed_direct_buf_len = (*env)->GetIntField(env, this, 
                                   Bzip2Compressor_uncompressedDirectBufLen);

    jobject compressed_direct_buf = (*env)->GetObjectField(env, this, 
                                   Bzip2Compressor_compressedDirectBuf);
    jint compressed_direct_buf_len = (*env)->GetIntField(env, this, 
                                 Bzip2Compressor_directBufferSize);

    jboolean finish = (*env)->GetBooleanField(env, this,
                                              Bzip2Compressor_finish);

    // Get the input and output direct buffers.
    LOCK_CLASS(env, clazz, "Bzip2Compressor");
    char* uncompressed_bytes = (*env)->GetDirectBufferAddress(env, 
                                                uncompressed_direct_buf);
    char* compressed_bytes = (*env)->GetDirectBufferAddress(env, 
                                                compressed_direct_buf);
    UNLOCK_CLASS(env, clazz, "Bzip2Compressor");

    if (!uncompressed_bytes || !compressed_bytes) {
        return (jint)0;
    }
        
    // Re-calibrate the bz_stream.
    stream->next_in = uncompressed_bytes + uncompressed_direct_buf_off;
    stream->avail_in = uncompressed_direct_buf_len;
    stream->next_out = compressed_bytes;
    stream->avail_out = compressed_direct_buf_len;
        
    // Compress.
    int rv = dlsym_BZ2_bzCompress(stream, finish ? BZ_FINISH : BZ_RUN);

    jint no_compressed_bytes = 0;
    switch (rv) {
        // Contingency? - Report error by throwing appropriate exceptions.
    case BZ_STREAM_END:
        {
            (*env)->SetBooleanField(env, this,
                                    Bzip2Compressor_finished,
                                    JNI_TRUE);
        } // cascade
    case BZ_RUN_OK:
    case BZ_FINISH_OK:
        {
            uncompressed_direct_buf_off +=
                uncompressed_direct_buf_len - stream->avail_in;
            (*env)->SetIntField(env, this, 
                                Bzip2Compressor_uncompressedDirectBufOff,
                                uncompressed_direct_buf_off);
            (*env)->SetIntField(env, this, 
                                Bzip2Compressor_uncompressedDirectBufLen,
                                stream->avail_in);
            no_compressed_bytes =
                compressed_direct_buf_len - stream->avail_out;
        }
        break;
    default:
        {
            THROW(env, "java/lang/InternalError", NULL);
        }
        break;
    }
        
    return no_compressed_bytes;
}

JNIEXPORT jlong JNICALL
Java_org_apache_hadoop_io_compress_bzip2_Bzip2Compressor_getBytesRead(
                            JNIEnv *env, jclass class, jlong stream)
{
    const bz_stream* strm = BZSTREAM(stream);
    return ((jlong)strm->total_in_hi32 << 32) | strm->total_in_lo32;
}

JNIEXPORT jlong JNICALL
Java_org_apache_hadoop_io_compress_bzip2_Bzip2Compressor_getBytesWritten(
                                JNIEnv *env, jclass class, jlong stream)
{
    const bz_stream* strm = BZSTREAM(stream);
    return ((jlong)strm->total_out_hi32 << 32) | strm->total_out_lo32;
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_compress_bzip2_Bzip2Compressor_end(
                                JNIEnv *env, jclass class, jlong stream)
{
    if (dlsym_BZ2_bzCompressEnd(BZSTREAM(stream)) != BZ_OK) {
        THROW(env, "java/lang/InternalError", NULL);
    } else {
        free(BZSTREAM(stream));
    }
}

#endif //define HADOOP_BZIP2_LIBRARY

/**
 * vim: sw=2: ts=2: et:
 */

