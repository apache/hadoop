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

#include "org_apache_hadoop_io_compress_zstd.h"

#if defined HADOOP_ZSTD_LIBRARY

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef UNIX
#include <dlfcn.h>
#include "config.h"
#endif

#include "org_apache_hadoop_io_compress_zstd_ZStandardDecompressor.h"

static jfieldID ZStandardDecompressor_stream;
static jfieldID ZStandardDecompressor_compressedDirectBufOff;
static jfieldID ZStandardDecompressor_bytesInCompressedBuffer;
static jfieldID ZStandardDecompressor_directBufferSize;
static jfieldID ZStandardDecompressor_finished;
static jfieldID ZStandardDecompressor_remaining;

#ifdef UNIX
static size_t (*dlsym_ZSTD_DStreamOutSize)(void);
static size_t (*dlsym_ZSTD_DStreamInSize)(void);
static ZSTD_DStream* (*dlsym_ZSTD_createDStream)(void);
static size_t (*dlsym_ZSTD_initDStream)(ZSTD_DStream*);
static size_t (*dlsym_ZSTD_freeDStream)(ZSTD_DStream*);
static size_t (*dlsym_ZSTD_resetDStream)(ZSTD_DStream*);
static size_t (*dlsym_ZSTD_decompressStream)(ZSTD_DStream*, ZSTD_outBuffer*, ZSTD_inBuffer*);
static size_t (*dlsym_ZSTD_flushStream)(ZSTD_CStream*, ZSTD_outBuffer*);
static unsigned (*dlsym_ZSTD_isError)(size_t);
static const char * (*dlsym_ZSTD_getErrorName)(size_t);
#endif

#ifdef WINDOWS
typedef size_t (__cdecl *__dlsym_ZSTD_DStreamOutSize)(void);
typedef size_t (__cdecl *__dlsym_ZSTD_DStreamInSize)(void);
typedef ZSTD_DStream* (__cdecl *__dlsym_ZSTD_createDStream)(void);
typedef size_t (__cdecl *__dlsym_ZSTD_initDStream)(ZSTD_DStream*);
typedef size_t (__cdecl *__dlsym_ZSTD_freeDStream)(ZSTD_DStream*);
typedef size_t (__cdecl *__dlsym_ZSTD_resetDStream)(ZSTD_DStream*);
typedef size_t (__cdecl *__dlsym_ZSTD_decompressStream)(ZSTD_DStream*, ZSTD_outBuffer*, ZSTD_inBuffer*);
typedef size_t (__cdecl *__dlsym_ZSTD_flushStream)(ZSTD_CStream*, ZSTD_outBuffer*);
typedef unsigned (__cdecl *__dlsym_ZSTD_isError)(size_t);
typedef const char * (__cdecl *__dlsym_ZSTD_getErrorName)(size_t);

static __dlsym_ZSTD_DStreamOutSize dlsym_ZSTD_DStreamOutSize;
static __dlsym_ZSTD_DStreamInSize dlsym_ZSTD_DStreamInSize;
static __dlsym_ZSTD_createDStream dlsym_ZSTD_createDStream;
static __dlsym_ZSTD_initDStream dlsym_ZSTD_initDStream;
static __dlsym_ZSTD_freeDStream dlsym_ZSTD_freeDStream;
static __dlsym_ZSTD_resetDStream dlsym_ZSTD_resetDStream;
static __dlsym_ZSTD_decompressStream dlsym_ZSTD_decompressStream;
static __dlsym_ZSTD_isError dlsym_ZSTD_isError;
static __dlsym_ZSTD_getErrorName dlsym_ZSTD_getErrorName;
static __dlsym_ZSTD_flushStream dlsym_ZSTD_flushStream;
#endif

JNIEXPORT void JNICALL Java_org_apache_hadoop_io_compress_zstd_ZStandardDecompressor_initIDs (JNIEnv *env, jclass clazz) {
    // Load libzstd.so
#ifdef UNIX
    void *libzstd = dlopen(HADOOP_ZSTD_LIBRARY, RTLD_LAZY | RTLD_GLOBAL);
    if (!libzstd) {
        char* msg = (char*)malloc(1000);
        snprintf(msg, 1000, "%s (%s)!", "Cannot load " HADOOP_ZSTD_LIBRARY, dlerror());
        THROW(env, "java/lang/UnsatisfiedLinkError", msg);
        return;
    }
#endif

#ifdef WINDOWS
    HMODULE libzstd = LoadLibrary(HADOOP_ZSTD_LIBRARY);
    if (!libzstd) {
        THROW(env, "java/lang/UnsatisfiedLinkError", "Cannot load zstd.dll");
        return;
    }
#endif

#ifdef UNIX
    dlerror();
    LOAD_DYNAMIC_SYMBOL(dlsym_ZSTD_DStreamOutSize, env, libzstd, "ZSTD_DStreamOutSize");
    LOAD_DYNAMIC_SYMBOL(dlsym_ZSTD_DStreamInSize, env, libzstd, "ZSTD_DStreamInSize");
    LOAD_DYNAMIC_SYMBOL(dlsym_ZSTD_createDStream, env, libzstd, "ZSTD_createDStream");
    LOAD_DYNAMIC_SYMBOL(dlsym_ZSTD_initDStream, env, libzstd, "ZSTD_initDStream");
    LOAD_DYNAMIC_SYMBOL(dlsym_ZSTD_freeDStream, env, libzstd, "ZSTD_freeDStream");
    LOAD_DYNAMIC_SYMBOL(dlsym_ZSTD_resetDStream, env, libzstd, "ZSTD_resetDStream");
    LOAD_DYNAMIC_SYMBOL(dlsym_ZSTD_decompressStream, env, libzstd, "ZSTD_decompressStream");
    LOAD_DYNAMIC_SYMBOL(dlsym_ZSTD_isError, env, libzstd, "ZSTD_isError");
    LOAD_DYNAMIC_SYMBOL(dlsym_ZSTD_getErrorName, env, libzstd, "ZSTD_getErrorName");
    LOAD_DYNAMIC_SYMBOL(dlsym_ZSTD_flushStream, env, libzstd, "ZSTD_flushStream");
#endif

#ifdef WINDOWS
    LOAD_DYNAMIC_SYMBOL(__dlsym_ZSTD_DStreamOutSize, dlsym_ZSTD_DStreamOutSize, env, libzstd, "ZSTD_DStreamOutSize");
    LOAD_DYNAMIC_SYMBOL(__dlsym_ZSTD_DStreamInSize, dlsym_ZSTD_DStreamInSize, env, libzstd, "ZSTD_DStreamInSize");
    LOAD_DYNAMIC_SYMBOL(__dlsym_ZSTD_createDStream, dlsym_ZSTD_createDStream, env, libzstd, "ZSTD_createDStream");
    LOAD_DYNAMIC_SYMBOL(__dlsym_ZSTD_initDStream, dlsym_ZSTD_initDStream, env, libzstd, "ZSTD_initDStream");
    LOAD_DYNAMIC_SYMBOL(__dlsym_ZSTD_freeDStream, dlsym_ZSTD_freeDStream, env, libzstd, "ZSTD_freeDStream");
    LOAD_DYNAMIC_SYMBOL(__dlsym_ZSTD_resetDStream, dlsym_ZSTD_resetDStream, env, libzstd, "ZSTD_resetDStream");
    LOAD_DYNAMIC_SYMBOL(__dlsym_ZSTD_decompressStream, dlsym_ZSTD_decompressStream, env, libzstd, "ZSTD_decompressStream");
    LOAD_DYNAMIC_SYMBOL(__dlsym_ZSTD_isError, dlsym_ZSTD_isError, env, libzstd, "ZSTD_isError");
    LOAD_DYNAMIC_SYMBOL(__dlsym_ZSTD_getErrorName, dlsym_ZSTD_getErrorName, env, libzstd, "ZSTD_getErrorName");
    LOAD_DYNAMIC_SYMBOL(__dlsym_ZSTD_flushStream, dlsym_ZSTD_flushStream, env, libzstd, "ZSTD_flushStream");
#endif

    ZStandardDecompressor_stream = (*env)->GetFieldID(env, clazz, "stream", "J");
    ZStandardDecompressor_finished = (*env)->GetFieldID(env, clazz, "finished", "Z");
    ZStandardDecompressor_compressedDirectBufOff = (*env)->GetFieldID(env, clazz, "compressedDirectBufOff", "I");
    ZStandardDecompressor_bytesInCompressedBuffer = (*env)->GetFieldID(env, clazz, "bytesInCompressedBuffer", "I");
    ZStandardDecompressor_directBufferSize = (*env)->GetFieldID(env, clazz, "directBufferSize", "I");
    ZStandardDecompressor_remaining = (*env)->GetFieldID(env, clazz, "remaining", "I");
}

JNIEXPORT jlong JNICALL Java_org_apache_hadoop_io_compress_zstd_ZStandardDecompressor_create(JNIEnv *env, jclass clazz) {
    ZSTD_DStream * stream = dlsym_ZSTD_createDStream();
    if (stream == NULL) {
        THROW(env, "java/lang/InternalError", "Error creating stream");
        return (jlong) 0;
    }
    return (jlong) stream;
}

JNIEXPORT void JNICALL Java_org_apache_hadoop_io_compress_zstd_ZStandardDecompressor_init(JNIEnv *env, jclass clazz, jlong stream) {
    size_t result = dlsym_ZSTD_initDStream((ZSTD_DStream *) stream);
    if (dlsym_ZSTD_isError(result)) {
        THROW(env, "java/lang/InternalError", dlsym_ZSTD_getErrorName(result));
        return;
    }
}


JNIEXPORT void JNICALL Java_org_apache_hadoop_io_compress_zstd_ZStandardDecompressor_free(JNIEnv *env, jclass clazz, jlong stream) {
    size_t result = dlsym_ZSTD_freeDStream((ZSTD_DStream *) stream);
    if (dlsym_ZSTD_isError(result)) {
        THROW(env, "java/lang/InternalError", dlsym_ZSTD_getErrorName(result));
        return;
    }
}

JNIEXPORT jint JNICALL Java_org_apache_hadoop_io_compress_zstd_ZStandardDecompressor_inflateBytesDirect
(JNIEnv *env, jobject this, jobject compressed_direct_buf, jint compressed_direct_buf_off, jint compressed_direct_buf_len, jobject uncompressed_direct_buf, jint uncompressed_direct_buf_off, jint uncompressed_direct_buf_len) {
    ZSTD_DStream *stream = (ZSTD_DStream *) (*env)->GetLongField(env, this, ZStandardDecompressor_stream);
    if (!stream) {
        THROW(env, "java/lang/NullPointerException", NULL);
        return (jint)0;
    }

    // Get the input direct buffer
    void * compressed_bytes = (*env)->GetDirectBufferAddress(env, compressed_direct_buf);
    if (!compressed_bytes) {
        THROW(env, "java/lang/InternalError", "Undefined memory address for compressedDirectBuf");
        return (jint) 0;
    }

    // Get the output direct buffer
    void * uncompressed_bytes = (*env)->GetDirectBufferAddress(env, uncompressed_direct_buf);
    if (!uncompressed_bytes) {
        THROW(env, "java/lang/InternalError", "Undefined memory address for uncompressedDirectBuf");
        return (jint) 0;
    }
    uncompressed_bytes = ((char*) uncompressed_bytes) + uncompressed_direct_buf_off;

    ZSTD_inBuffer input = { compressed_bytes, compressed_direct_buf_len, compressed_direct_buf_off };
    ZSTD_outBuffer output = { uncompressed_bytes, uncompressed_direct_buf_len, 0 };

    size_t const size = dlsym_ZSTD_decompressStream(stream, &output, &input);

    // check for errors
    if (dlsym_ZSTD_isError(size)) {
        THROW(env, "java/lang/InternalError", dlsym_ZSTD_getErrorName(size));
        return (jint) 0;
    }
    int remaining = input.size - input.pos;
    (*env)->SetIntField(env, this, ZStandardDecompressor_remaining, remaining);

    // the entire frame has been decoded
    if (size == 0) {
        (*env)->SetBooleanField(env, this, ZStandardDecompressor_finished, JNI_TRUE);
        size_t result = dlsym_ZSTD_initDStream(stream);
        if (dlsym_ZSTD_isError(result)) {
            THROW(env, "java/lang/InternalError", dlsym_ZSTD_getErrorName(result));
            return (jint) 0;
        }
    }
    (*env)->SetIntField(env, this, ZStandardDecompressor_compressedDirectBufOff, input.pos);
    (*env)->SetIntField(env, this, ZStandardDecompressor_bytesInCompressedBuffer, input.size);
    return (jint) output.pos;
}

// returns the max size of the recommended input and output buffers
JNIEXPORT jint JNICALL Java_org_apache_hadoop_io_compress_zstd_ZStandardDecompressor_getStreamSize
(JNIEnv *env, jclass clazz) {
    int x = (int) dlsym_ZSTD_DStreamInSize();
    int y = (int) dlsym_ZSTD_DStreamOutSize();
    return (x >= y) ? x : y;
}

#endif //define HADOOP_ZSTD_LIBRARY
