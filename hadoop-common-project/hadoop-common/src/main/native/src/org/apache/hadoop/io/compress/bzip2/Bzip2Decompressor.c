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

#include <config.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dlfcn.h>

#include "org_apache_hadoop_io_compress_bzip2.h"
#include "org_apache_hadoop_io_compress_bzip2_Bzip2Decompressor.h"

static jfieldID Bzip2Decompressor_stream;
static jfieldID Bzip2Decompressor_compressedDirectBuf;
static jfieldID Bzip2Decompressor_compressedDirectBufOff;
static jfieldID Bzip2Decompressor_compressedDirectBufLen;
static jfieldID Bzip2Decompressor_uncompressedDirectBuf;
static jfieldID Bzip2Decompressor_directBufferSize;
static jfieldID Bzip2Decompressor_finished;

static int (*dlsym_BZ2_bzDecompressInit)(bz_stream*, int, int);
static int (*dlsym_BZ2_bzDecompress)(bz_stream*);
static int (*dlsym_BZ2_bzDecompressEnd)(bz_stream*);

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_compress_bzip2_Bzip2Decompressor_initIDs(
                                 JNIEnv *env, jclass class, jstring libname)
{
    const char *bzlib_name = NULL;
    const char *java_lib_name = (*env)->GetStringUTFChars(env, libname, NULL);
    if (java_lib_name == NULL) {
        // Java code will get OutOfMemoryException thrown by GetStringUTFChars
        goto cleanup;
    }

    if (strcmp(java_lib_name, "system-native") == 0) {
        bzlib_name = HADOOP_BZIP2_LIBRARY;
    } else {
        bzlib_name = java_lib_name;
    }

    // Load the native library.
    void *libbz2 = dlopen(bzlib_name, RTLD_LAZY | RTLD_GLOBAL);
    if (!libbz2) {
        THROW(env, "java/lang/UnsatisfiedLinkError",
              "Cannot load bzip2 native library");
        goto cleanup;
    }

    // Locate the requisite symbols from libbz2.so.
    dlerror();                                 // Clear any existing error.
    LOAD_DYNAMIC_SYMBOL(dlsym_BZ2_bzDecompressInit, env, libbz2,
                        "BZ2_bzDecompressInit");
    LOAD_DYNAMIC_SYMBOL(dlsym_BZ2_bzDecompress, env, libbz2,
                        "BZ2_bzDecompress");
    LOAD_DYNAMIC_SYMBOL(dlsym_BZ2_bzDecompressEnd, env, libbz2,
                        "BZ2_bzDecompressEnd");

    // Initialize the requisite fieldIds.
    Bzip2Decompressor_stream = (*env)->GetFieldID(env, class, "stream", "J");
    Bzip2Decompressor_finished = (*env)->GetFieldID(env, class,
                                                    "finished", "Z");
    Bzip2Decompressor_compressedDirectBuf = (*env)->GetFieldID(env, class, 
                                                        "compressedDirectBuf", 
                                                        "Ljava/nio/Buffer;");
    Bzip2Decompressor_compressedDirectBufOff = (*env)->GetFieldID(env, class, 
                                                "compressedDirectBufOff", "I");
    Bzip2Decompressor_compressedDirectBufLen = (*env)->GetFieldID(env, class, 
                                                "compressedDirectBufLen", "I");
    Bzip2Decompressor_uncompressedDirectBuf = (*env)->GetFieldID(env, class, 
                                                "uncompressedDirectBuf", 
                                                "Ljava/nio/Buffer;");
    Bzip2Decompressor_directBufferSize = (*env)->GetFieldID(env, class, 
                                                "directBufferSize", "I");
cleanup:
    if(java_lib_name != NULL) {
         (*env)->ReleaseStringUTFChars(env,libname,java_lib_name);
         java_lib_name = NULL;
    }
}

JNIEXPORT jlong JNICALL
Java_org_apache_hadoop_io_compress_bzip2_Bzip2Decompressor_init(
                                JNIEnv *env, jclass cls, jint conserveMemory)
{
    bz_stream *stream = malloc(sizeof(bz_stream));
    if (stream == 0) {
        THROW(env, "java/lang/OutOfMemoryError", NULL);
        return (jlong)0;
    } 
    memset((void*)stream, 0, sizeof(bz_stream));
    
    int rv = dlsym_BZ2_bzDecompressInit(stream, 0, conserveMemory);

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
Java_org_apache_hadoop_io_compress_bzip2_Bzip2Decompressor_inflateBytesDirect(
                        JNIEnv *env, jobject this)
{
    // Get members of Bzip2Decompressor.
    bz_stream *stream = BZSTREAM((*env)->GetLongField(env, this,
                                                Bzip2Decompressor_stream));
    if (!stream) {
        THROW(env, "java/lang/NullPointerException", NULL);
        return (jint)0;
    } 

    jarray compressed_direct_buf = (jarray)(*env)->GetObjectField(env,
                                this, Bzip2Decompressor_compressedDirectBuf);
    jint compressed_direct_buf_off = (*env)->GetIntField(env, this, 
                                Bzip2Decompressor_compressedDirectBufOff);
    jint compressed_direct_buf_len = (*env)->GetIntField(env, this, 
                                Bzip2Decompressor_compressedDirectBufLen);

    jarray uncompressed_direct_buf = (jarray)(*env)->GetObjectField(env,
                                this, Bzip2Decompressor_uncompressedDirectBuf);
    jint uncompressed_direct_buf_len = (*env)->GetIntField(env, this, 
                                Bzip2Decompressor_directBufferSize);

    // Get the input and output direct buffers.
    char* compressed_bytes = (*env)->GetDirectBufferAddress(env, 
                                                compressed_direct_buf);
    char* uncompressed_bytes = (*env)->GetDirectBufferAddress(env, 
                                                uncompressed_direct_buf);

    if (!compressed_bytes || !uncompressed_bytes) {
        return (jint)0;
    }
        
    // Re-calibrate the bz_stream.
    stream->next_in  = compressed_bytes + compressed_direct_buf_off;
    stream->avail_in  = compressed_direct_buf_len;
    stream->next_out = uncompressed_bytes;
    stream->avail_out = uncompressed_direct_buf_len;
        
    // Decompress.
    int rv = dlsym_BZ2_bzDecompress(stream);

    // Contingency? - Report error by throwing appropriate exceptions.
    int no_decompressed_bytes = 0;      
    switch (rv) {
    case BZ_STREAM_END:
        {
            (*env)->SetBooleanField(env, this,
                                    Bzip2Decompressor_finished,
                                    JNI_TRUE);
        } // cascade down
    case BZ_OK:
        {
            compressed_direct_buf_off +=
                compressed_direct_buf_len - stream->avail_in;
            (*env)->SetIntField(env, this,
                                Bzip2Decompressor_compressedDirectBufOff, 
                                compressed_direct_buf_off);
            (*env)->SetIntField(env, this,
                                Bzip2Decompressor_compressedDirectBufLen, 
                                stream->avail_in);
            no_decompressed_bytes =
                uncompressed_direct_buf_len - stream->avail_out;
        }
        break;
    case BZ_DATA_ERROR:
    case BZ_DATA_ERROR_MAGIC:
        {
            THROW(env, "java/io/IOException", NULL);
        }
        break;
    case BZ_MEM_ERROR:
        {
            THROW(env, "java/lang/OutOfMemoryError", NULL);
        }
        break;
    default:
        {
            THROW(env, "java/lang/InternalError", NULL);
        }
        break;
    }
    
    return no_decompressed_bytes;
}

JNIEXPORT jlong JNICALL
Java_org_apache_hadoop_io_compress_bzip2_Bzip2Decompressor_getBytesRead(
                                JNIEnv *env, jclass cls, jlong stream)
{
    const bz_stream* strm = BZSTREAM(stream);
    return ((jlong)strm->total_in_hi32 << 32) | strm->total_in_lo32;
}

JNIEXPORT jlong JNICALL
Java_org_apache_hadoop_io_compress_bzip2_Bzip2Decompressor_getBytesWritten(
                                JNIEnv *env, jclass cls, jlong stream)
{
    const bz_stream* strm = BZSTREAM(stream);
    return ((jlong)strm->total_out_hi32 << 32) | strm->total_out_lo32;
}

JNIEXPORT jint JNICALL
Java_org_apache_hadoop_io_compress_bzip2_Bzip2Decompressor_getRemaining(
                                JNIEnv *env, jclass cls, jlong stream)
{
    return (BZSTREAM(stream))->avail_in;
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_compress_bzip2_Bzip2Decompressor_end(
                                JNIEnv *env, jclass cls, jlong stream)
{
    if (dlsym_BZ2_bzDecompressEnd(BZSTREAM(stream)) != BZ_OK) {
        THROW(env, "java/lang/InternalError", 0);
    }

    free(BZSTREAM(stream));

}

/**
 * vim: sw=2: ts=2: et:
 */

