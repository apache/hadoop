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

#include "org_apache_hadoop_io_compress_zstd_ZStandardCompressor.h"

static jfieldID ZStandardCompressor_stream;
static jfieldID ZStandardCompressor_uncompressedDirectBufOff;
static jfieldID ZStandardCompressor_uncompressedDirectBufLen;
static jfieldID ZStandardCompressor_directBufferSize;
static jfieldID ZStandardCompressor_finish;
static jfieldID ZStandardCompressor_finished;
static jfieldID ZStandardCompressor_bytesWritten;
static jfieldID ZStandardCompressor_bytesRead;

#ifdef UNIX
static size_t (*dlsym_ZSTD_CStreamInSize)(void);
static size_t (*dlsym_ZSTD_CStreamOutSize)(void);
static ZSTD_CStream* (*dlsym_ZSTD_createCStream)(void);
static size_t (*dlsym_ZSTD_initCStream)(ZSTD_CStream*, int);
static size_t (*dlsym_ZSTD_freeCStream)(ZSTD_CStream*);
static size_t (*dlsym_ZSTD_compressStream)(ZSTD_CStream*, ZSTD_outBuffer*, ZSTD_inBuffer*);
static size_t (*dlsym_ZSTD_endStream)(ZSTD_CStream*, ZSTD_outBuffer*);
static size_t (*dlsym_ZSTD_flushStream)(ZSTD_CStream*, ZSTD_outBuffer*);
static unsigned (*dlsym_ZSTD_isError)(size_t);
static const char * (*dlsym_ZSTD_getErrorName)(size_t);
#endif

#ifdef WINDOWS
typedef size_t (__cdecl *__dlsym_ZSTD_CStreamInSize)(void);
typedef size_t (__cdecl *__dlsym_ZSTD_CStreamOutSize)(void);
typedef ZSTD_CStream* (__cdecl *__dlsym_ZSTD_createCStream)(void);
typedef size_t (__cdecl *__dlsym_ZSTD_initCStream)(ZSTD_CStream*, int);
typedef size_t (__cdecl *__dlsym_ZSTD_freeCStream)(ZSTD_CStream*);
typedef size_t (__cdecl *__dlsym_ZSTD_compressStream)(ZSTD_CStream*, ZSTD_outBuffer*, ZSTD_inBuffer*);
typedef size_t (__cdecl *__dlsym_ZSTD_endStream)(ZSTD_CStream*, ZSTD_outBuffer*);
typedef size_t (__cdecl *__dlsym_ZSTD_flushStream)(ZSTD_CStream*, ZSTD_outBuffer*);
typedef unsigned (__cdecl *__dlsym_ZSTD_isError)(size_t);
typedef const char * (__cdecl *__dlsym_ZSTD_getErrorName)(size_t);

static __dlsym_ZSTD_CStreamInSize dlsym_ZSTD_CStreamInSize;
static __dlsym_ZSTD_CStreamOutSize dlsym_ZSTD_CStreamOutSize;
static __dlsym_ZSTD_createCStream dlsym_ZSTD_createCStream;
static __dlsym_ZSTD_initCStream dlsym_ZSTD_initCStream;
static __dlsym_ZSTD_freeCStream dlsym_ZSTD_freeCStream;
static __dlsym_ZSTD_compressStream dlsym_ZSTD_compressStream;
static __dlsym_ZSTD_endStream dlsym_ZSTD_endStream;
static __dlsym_ZSTD_flushStream dlsym_ZSTD_flushStream;
static __dlsym_ZSTD_isError dlsym_ZSTD_isError;
static __dlsym_ZSTD_getErrorName dlsym_ZSTD_getErrorName;
#endif

// Load the libztsd.so from disk
JNIEXPORT void JNICALL Java_org_apache_hadoop_io_compress_zstd_ZStandardCompressor_initIDs (JNIEnv *env, jclass clazz) {
#ifdef UNIX
    // Load libzstd.so
    void *libzstd = dlopen(HADOOP_ZSTD_LIBRARY, RTLD_LAZY | RTLD_GLOBAL);
    if (!libzstd) {
        char* msg = (char*)malloc(10000);
        snprintf(msg, 10000, "%s (%s)!", "Cannot load " HADOOP_ZSTD_LIBRARY, dlerror());
        THROW(env, "java/lang/InternalError", msg);
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
    // load dynamic symbols
    dlerror();
    LOAD_DYNAMIC_SYMBOL(dlsym_ZSTD_CStreamInSize, env, libzstd, "ZSTD_CStreamInSize");
    LOAD_DYNAMIC_SYMBOL(dlsym_ZSTD_CStreamOutSize, env, libzstd, "ZSTD_CStreamOutSize");
    LOAD_DYNAMIC_SYMBOL(dlsym_ZSTD_createCStream, env, libzstd, "ZSTD_createCStream");
    LOAD_DYNAMIC_SYMBOL(dlsym_ZSTD_initCStream, env, libzstd, "ZSTD_initCStream");
    LOAD_DYNAMIC_SYMBOL(dlsym_ZSTD_freeCStream, env, libzstd, "ZSTD_freeCStream");
    LOAD_DYNAMIC_SYMBOL(dlsym_ZSTD_compressStream, env, libzstd, "ZSTD_compressStream");
    LOAD_DYNAMIC_SYMBOL(dlsym_ZSTD_endStream, env, libzstd, "ZSTD_endStream");
    LOAD_DYNAMIC_SYMBOL(dlsym_ZSTD_flushStream, env, libzstd, "ZSTD_flushStream");
    LOAD_DYNAMIC_SYMBOL(dlsym_ZSTD_isError, env, libzstd, "ZSTD_isError");
    LOAD_DYNAMIC_SYMBOL(dlsym_ZSTD_getErrorName, env, libzstd, "ZSTD_getErrorName");
#endif

#ifdef WINDOWS
    LOAD_DYNAMIC_SYMBOL(__dlsym_ZSTD_CStreamInSize, dlsym_ZSTD_CStreamInSize, env, libzstd, "ZSTD_CStreamInSize");
    LOAD_DYNAMIC_SYMBOL(__dlsym_ZSTD_CStreamOutSize, dlsym_ZSTD_CStreamOutSize, env, libzstd, "ZSTD_CStreamOutSize");
    LOAD_DYNAMIC_SYMBOL(__dlsym_ZSTD_createCStream, dlsym_ZSTD_createCStream, env, libzstd, "ZSTD_createCStream");
    LOAD_DYNAMIC_SYMBOL(__dlsym_ZSTD_initCStream, dlsym_ZSTD_initCStream, env, libzstd, "ZSTD_initCStream");
    LOAD_DYNAMIC_SYMBOL(__dlsym_ZSTD_freeCStream, dlsym_ZSTD_freeCStream, env, libzstd, "ZSTD_freeCStream");
    LOAD_DYNAMIC_SYMBOL(__dlsym_ZSTD_compressStream, dlsym_ZSTD_compressStream, env, libzstd, "ZSTD_compressStream");
    LOAD_DYNAMIC_SYMBOL(__dlsym_ZSTD_endStream, dlsym_ZSTD_endStream, env, libzstd, "ZSTD_endStream");
    LOAD_DYNAMIC_SYMBOL(__dlsym_ZSTD_flushStream, dlsym_ZSTD_flushStream, env, libzstd, "ZSTD_flushStream");
    LOAD_DYNAMIC_SYMBOL(__dlsym_ZSTD_isError, dlsym_ZSTD_isError, env, libzstd, "ZSTD_isError");
    LOAD_DYNAMIC_SYMBOL(__dlsym_ZSTD_getErrorName, dlsym_ZSTD_getErrorName, env, libzstd, "ZSTD_getErrorName");
#endif

    // load fields
    ZStandardCompressor_stream = (*env)->GetFieldID(env, clazz, "stream", "J");
    ZStandardCompressor_finish = (*env)->GetFieldID(env, clazz, "finish", "Z");
    ZStandardCompressor_finished = (*env)->GetFieldID(env, clazz, "finished", "Z");
    ZStandardCompressor_uncompressedDirectBufOff = (*env)->GetFieldID(env, clazz, "uncompressedDirectBufOff", "I");
    ZStandardCompressor_uncompressedDirectBufLen = (*env)->GetFieldID(env, clazz, "uncompressedDirectBufLen", "I");
    ZStandardCompressor_directBufferSize = (*env)->GetFieldID(env, clazz, "directBufferSize", "I");
    ZStandardCompressor_bytesRead = (*env)->GetFieldID(env, clazz, "bytesRead", "J");
    ZStandardCompressor_bytesWritten = (*env)->GetFieldID(env, clazz, "bytesWritten", "J");
}

// Create the compression stream
JNIEXPORT jlong JNICALL Java_org_apache_hadoop_io_compress_zstd_ZStandardCompressor_create (JNIEnv *env, jclass clazz) {
    ZSTD_CStream* const stream =  dlsym_ZSTD_createCStream();
    if (stream == NULL) {
        THROW(env, "java/lang/InternalError", "Error creating the stream");
        return (jlong)0;
    }
    return (jlong) stream;
}

// Initialize the compression stream
JNIEXPORT void JNICALL Java_org_apache_hadoop_io_compress_zstd_ZStandardCompressor_init (JNIEnv *env, jclass clazz, jint level, jlong stream) {
    size_t result = dlsym_ZSTD_initCStream((ZSTD_CStream *) stream, level);
    if (dlsym_ZSTD_isError(result)) {
        THROW(env, "java/lang/InternalError", dlsym_ZSTD_getErrorName(result));
        return;
    }
}

// free the compression stream
JNIEXPORT void JNICALL Java_org_apache_hadoop_io_compress_zstd_ZStandardCompressor_end (JNIEnv *env, jclass clazz, jlong stream) {
    size_t result = dlsym_ZSTD_freeCStream((ZSTD_CStream *) stream);
    if (dlsym_ZSTD_isError(result)) {
        THROW(env, "java/lang/InternalError", dlsym_ZSTD_getErrorName(result));
        return;
    }
}

JNIEXPORT jint Java_org_apache_hadoop_io_compress_zstd_ZStandardCompressor_deflateBytesDirect
(JNIEnv *env, jobject this, jobject uncompressed_direct_buf, jint uncompressed_direct_buf_off, jint uncompressed_direct_buf_len, jobject compressed_direct_buf, jint compressed_direct_buf_len ) {
    ZSTD_CStream* const stream = (ZSTD_CStream*) (*env)->GetLongField(env, this, ZStandardCompressor_stream);
    if (!stream) {
        THROW(env, "java/lang/NullPointerException", NULL);
        return (jint)0;
    }

    jlong bytes_read = (*env)->GetLongField(env, this, ZStandardCompressor_bytesRead);
    jlong bytes_written = (*env)->GetLongField(env, this, ZStandardCompressor_bytesWritten);
    jboolean finish = (*env)->GetBooleanField(env, this, ZStandardCompressor_finish);

    // Get the input direct buffer
    void * uncompressed_bytes = (*env)->GetDirectBufferAddress(env, uncompressed_direct_buf);
    if (!uncompressed_bytes) {
        THROW(env, "java/lang/InternalError", "Undefined memory address for uncompressedDirectBuf");
        return (jint) 0;
    }

    // Get the output direct buffer
    void * compressed_bytes = (*env)->GetDirectBufferAddress(env, compressed_direct_buf);
    if (!compressed_bytes) {
        THROW(env, "java/lang/InternalError", "Undefined memory address for compressedDirectBuf");
        return (jint) 0;
    }

    ZSTD_inBuffer input = { uncompressed_bytes, uncompressed_direct_buf_len, uncompressed_direct_buf_off };
    ZSTD_outBuffer output = { compressed_bytes, compressed_direct_buf_len, 0 };

    size_t size;
    if (uncompressed_direct_buf_len != 0) {
        size = dlsym_ZSTD_compressStream(stream, &output, &input);
        if (dlsym_ZSTD_isError(size)) {
            THROW(env, "java/lang/InternalError", dlsym_ZSTD_getErrorName(size));
            return (jint) 0;
        }
    }
    if (finish && input.pos == input.size) {
        // end the stream, flush and  write the frame epilogue
        size = dlsym_ZSTD_endStream(stream, &output);
        if (!size) {
            (*env)->SetBooleanField(env, this, ZStandardCompressor_finished, JNI_TRUE);
        }
    } else {
        // need to flush the output buffer
        // this also updates the output buffer position.
        size = dlsym_ZSTD_flushStream(stream, &output);
    }
    if (dlsym_ZSTD_isError(size)) {
        THROW(env, "java/lang/InternalError", dlsym_ZSTD_getErrorName(size));
        return (jint) 0;
    }

    bytes_read += input.pos;
    bytes_written += output.pos;
    (*env)->SetLongField(env, this, ZStandardCompressor_bytesRead, bytes_read);
    (*env)->SetLongField(env, this, ZStandardCompressor_bytesWritten, bytes_written);

    (*env)->SetIntField(env, this, ZStandardCompressor_uncompressedDirectBufOff, input.pos);
    (*env)->SetIntField(env, this, ZStandardCompressor_uncompressedDirectBufLen, input.size - input.pos);
    return (jint) output.pos;
}

JNIEXPORT jstring JNICALL Java_org_apache_hadoop_io_compress_zstd_ZStandardCompressor_getLibraryName
(JNIEnv *env, jclass clazz) {
#ifdef UNIX
    if (dlsym_ZSTD_isError) {
        Dl_info dl_info;
        if (dladdr( dlsym_ZSTD_isError, &dl_info)) {
            return (*env)->NewStringUTF(env, dl_info.dli_fname);
        }
    }
    return (*env)->NewStringUTF(env, HADOOP_ZSTD_LIBRARY);
#endif
#ifdef WINDOWS
    LPWSTR filename = NULL;
    GetLibraryName(dlsym_ZSTD_isError, &filename);
    if (filename != NULL) {
        return (*env)->NewString(env, filename, (jsize) wcslen(filename));
    } else {
        return (*env)->NewStringUTF(env, "Unavailable");
    }
#endif
}

// returns the max size of the recommended input and output buffers
JNIEXPORT jint JNICALL Java_org_apache_hadoop_io_compress_zstd_ZStandardCompressor_getStreamSize
(JNIEnv *env, jclass clazz) {
    int x = (int) dlsym_ZSTD_CStreamInSize();
    int y = (int) dlsym_ZSTD_CStreamOutSize();
    return (x >= y) ? x : y;
}

#endif //define HADOOP_ZSTD_LIBRARY