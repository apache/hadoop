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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef UNIX
#include <dlfcn.h>
#include "config.h"
#endif

#include "org_apache_hadoop_io_compress_zlib.h"
#include "org_apache_hadoop_io_compress_zlib_ZlibCompressor.h"

static jfieldID ZlibCompressor_clazz;
static jfieldID ZlibCompressor_stream;
static jfieldID ZlibCompressor_uncompressedDirectBuf;
static jfieldID ZlibCompressor_uncompressedDirectBufOff;
static jfieldID ZlibCompressor_uncompressedDirectBufLen;
static jfieldID ZlibCompressor_compressedDirectBuf;
static jfieldID ZlibCompressor_directBufferSize;
static jfieldID ZlibCompressor_finish;
static jfieldID ZlibCompressor_finished;

#ifdef UNIX
static int (*dlsym_deflateInit2_)(z_streamp, int, int, int, int, int, const char *, int);
static int (*dlsym_deflate)(z_streamp, int);
static int (*dlsym_deflateSetDictionary)(z_streamp, const Bytef *, uInt);
static int (*dlsym_deflateReset)(z_streamp);
static int (*dlsym_deflateEnd)(z_streamp);
#endif

#ifdef WINDOWS
#include <Strsafe.h>
typedef int (__cdecl *__dlsym_deflateInit2_) (z_streamp, int, int, int, int, int, const char *, int);
typedef int (__cdecl *__dlsym_deflate) (z_streamp, int);
typedef int (__cdecl *__dlsym_deflateSetDictionary) (z_streamp, const Bytef *, uInt);
typedef int (__cdecl *__dlsym_deflateReset) (z_streamp);
typedef int (__cdecl *__dlsym_deflateEnd) (z_streamp);
static __dlsym_deflateInit2_ dlsym_deflateInit2_;
static __dlsym_deflate dlsym_deflate;
static __dlsym_deflateSetDictionary dlsym_deflateSetDictionary;
static __dlsym_deflateReset dlsym_deflateReset;
static __dlsym_deflateEnd dlsym_deflateEnd;

// Try to load zlib.dll from the dir where hadoop.dll is located.
HANDLE LoadZlibTryHadoopNativeDir() {
  HMODULE libz = NULL;
  PCWSTR HADOOP_DLL = L"hadoop.dll";
  size_t HADOOP_DLL_LEN = 10;
  WCHAR path[MAX_PATH] = { 0 };
  BOOL isPathValid = FALSE;

  // Get hadoop.dll full path
  HMODULE hModule = GetModuleHandle(HADOOP_DLL);
  if (hModule != NULL) {
    if (GetModuleFileName(hModule, path, MAX_PATH) > 0) {
      size_t size = 0;
      if (StringCchLength(path, MAX_PATH, &size) == S_OK) {

        // Update path variable to have the full path to the zlib.dll
        size = size - HADOOP_DLL_LEN;
        if (size >= 0) {
          path[size] = L'\0';
          if (StringCchCat(path, MAX_PATH, HADOOP_ZLIB_LIBRARY) == S_OK) {
            isPathValid = TRUE;
          }
        }
      }
    }
  }

  if (isPathValid) {
    libz = LoadLibrary(path);
  }

  // fallback to system paths
  if (!libz) {
    libz = LoadLibrary(HADOOP_ZLIB_LIBRARY);
  }

  return libz;
}
#endif

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_compress_zlib_ZlibCompressor_initIDs(
	JNIEnv *env, jclass class
	) {
#ifdef UNIX
	// Load libz.so
	void *libz = dlopen(HADOOP_ZLIB_LIBRARY, RTLD_LAZY | RTLD_GLOBAL);
  if (!libz) {
		THROW(env, "java/lang/UnsatisfiedLinkError", "Cannot load libz.so");
	  	return;
	}
#endif

#ifdef WINDOWS
  HMODULE libz = LoadZlibTryHadoopNativeDir();

  if (!libz) {
		THROW(env, "java/lang/UnsatisfiedLinkError", "Cannot load zlib1.dll");
    return;
	}
#endif

#ifdef UNIX
	// Locate the requisite symbols from libz.so
	dlerror();                                 // Clear any existing error
  LOAD_DYNAMIC_SYMBOL(dlsym_deflateInit2_, env, libz, "deflateInit2_");
  LOAD_DYNAMIC_SYMBOL(dlsym_deflate, env, libz, "deflate");
  LOAD_DYNAMIC_SYMBOL(dlsym_deflateSetDictionary, env, libz, "deflateSetDictionary");
  LOAD_DYNAMIC_SYMBOL(dlsym_deflateReset, env, libz, "deflateReset");
  LOAD_DYNAMIC_SYMBOL(dlsym_deflateEnd, env, libz, "deflateEnd");
#endif

#ifdef WINDOWS
  LOAD_DYNAMIC_SYMBOL(__dlsym_deflateInit2_, dlsym_deflateInit2_, env, libz, "deflateInit2_");
	LOAD_DYNAMIC_SYMBOL(__dlsym_deflate, dlsym_deflate, env, libz, "deflate");
	LOAD_DYNAMIC_SYMBOL(__dlsym_deflateSetDictionary, dlsym_deflateSetDictionary, env, libz, "deflateSetDictionary");
	LOAD_DYNAMIC_SYMBOL(__dlsym_deflateReset, dlsym_deflateReset, env, libz, "deflateReset");
	LOAD_DYNAMIC_SYMBOL(__dlsym_deflateEnd, dlsym_deflateEnd, env, libz, "deflateEnd");
#endif

	// Initialize the requisite fieldIds
    ZlibCompressor_clazz = (*env)->GetStaticFieldID(env, class, "clazz",
                                                      "Ljava/lang/Class;");
    ZlibCompressor_stream = (*env)->GetFieldID(env, class, "stream", "J");
    ZlibCompressor_finish = (*env)->GetFieldID(env, class, "finish", "Z");
    ZlibCompressor_finished = (*env)->GetFieldID(env, class, "finished", "Z");
    ZlibCompressor_uncompressedDirectBuf = (*env)->GetFieldID(env, class,
        "uncompressedDirectBuf",
    									"Ljava/nio/Buffer;");
    ZlibCompressor_uncompressedDirectBufOff = (*env)->GetFieldID(env, class,
    										"uncompressedDirectBufOff", "I");
    ZlibCompressor_uncompressedDirectBufLen = (*env)->GetFieldID(env, class,
    										"uncompressedDirectBufLen", "I");
    ZlibCompressor_compressedDirectBuf = (*env)->GetFieldID(env, class,
                      "compressedDirectBuf",
    									"Ljava/nio/Buffer;");
    ZlibCompressor_directBufferSize = (*env)->GetFieldID(env, class,
    										"directBufferSize", "I");
}

JNIEXPORT jlong JNICALL
Java_org_apache_hadoop_io_compress_zlib_ZlibCompressor_init(
	JNIEnv *env, jclass class, jint level, jint strategy, jint windowBits
	) {
    int rv = 0;
    static const int memLevel = 8; 							// See zconf.h
	  // Create a z_stream
    z_stream *stream = malloc(sizeof(z_stream));
    if (!stream) {
		THROW(env, "java/lang/OutOfMemoryError", NULL);
		return (jlong)0;
    }
    memset((void*)stream, 0, sizeof(z_stream));

	// Initialize stream
    rv = (*dlsym_deflateInit2_)(stream, level, Z_DEFLATED, windowBits,
    			memLevel, strategy, ZLIB_VERSION, sizeof(z_stream));

    if (rv != Z_OK) {
	    // Contingency - Report error by throwing appropriate exceptions
	    free(stream);
	    stream = NULL;

		switch (rv) {
			case Z_MEM_ERROR:
			    {
		    		THROW(env, "java/lang/OutOfMemoryError", NULL);
			    }
			break;
			case Z_STREAM_ERROR:
		    	{
		    		THROW(env, "java/lang/IllegalArgumentException", NULL);
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

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_compress_zlib_ZlibCompressor_setDictionary(
	JNIEnv *env, jclass class, jlong stream,
	jarray b, jint off, jint len
	) {
    int rv = 0;
    Bytef *buf = (*env)->GetPrimitiveArrayCritical(env, b, 0);
    if (!buf) {
        return;
    }
    rv = dlsym_deflateSetDictionary(ZSTREAM(stream), buf + off, len);
    (*env)->ReleasePrimitiveArrayCritical(env, b, buf, 0);

    if (rv != Z_OK) {
    	// Contingency - Report error by throwing appropriate exceptions
	    switch (rv) {
		    case Z_STREAM_ERROR:
			{
		    	THROW(env, "java/lang/IllegalArgumentException", NULL);
			}
			break;
	    	default:
			{
				THROW(env, "java/lang/InternalError", (ZSTREAM(stream))->msg);
			}
			break;
	    }
    }
}

JNIEXPORT jint JNICALL
Java_org_apache_hadoop_io_compress_zlib_ZlibCompressor_deflateBytesDirect(
	JNIEnv *env, jobject this
	) {
    jobject clazz = NULL;
    jobject uncompressed_direct_buf = NULL;
    jint uncompressed_direct_buf_off = 0;
    jint uncompressed_direct_buf_len = 0;
    jobject compressed_direct_buf = NULL;
    jint compressed_direct_buf_len = 0;
    jboolean finish;
    Bytef* uncompressed_bytes = NULL;
    Bytef* compressed_bytes = NULL;
    int rv = 0;
    jint no_compressed_bytes = 0;
	// Get members of ZlibCompressor
    z_stream *stream = ZSTREAM(
                (*env)->GetLongField(env, this,
    									ZlibCompressor_stream)
    					);
    if (!stream) {
		THROW(env, "java/lang/NullPointerException", NULL);
		return (jint)0;
    }

    // Get members of ZlibCompressor
    clazz = (*env)->GetStaticObjectField(env, this,
                                                 ZlibCompressor_clazz);
	uncompressed_direct_buf = (*env)->GetObjectField(env, this,
									ZlibCompressor_uncompressedDirectBuf);
	uncompressed_direct_buf_off = (*env)->GetIntField(env, this,
									ZlibCompressor_uncompressedDirectBufOff);
	uncompressed_direct_buf_len = (*env)->GetIntField(env, this,
									ZlibCompressor_uncompressedDirectBufLen);

	compressed_direct_buf = (*env)->GetObjectField(env, this,
									ZlibCompressor_compressedDirectBuf);
	compressed_direct_buf_len = (*env)->GetIntField(env, this,
									ZlibCompressor_directBufferSize);

	finish = (*env)->GetBooleanField(env, this, ZlibCompressor_finish);

    // Get the input direct buffer
    LOCK_CLASS(env, clazz, "ZlibCompressor");
    uncompressed_bytes = (*env)->GetDirectBufferAddress(env,
											uncompressed_direct_buf);
    UNLOCK_CLASS(env, clazz, "ZlibCompressor");

  	if (uncompressed_bytes == 0) {
    	return (jint)0;
	}

    // Get the output direct buffer
    LOCK_CLASS(env, clazz, "ZlibCompressor");
    compressed_bytes = (*env)->GetDirectBufferAddress(env,
										compressed_direct_buf);
    UNLOCK_CLASS(env, clazz, "ZlibCompressor");

  	if (compressed_bytes == 0) {
		return (jint)0;
	}

	// Re-calibrate the z_stream
  	stream->next_in = uncompressed_bytes + uncompressed_direct_buf_off;
  	stream->next_out = compressed_bytes;
  	stream->avail_in = uncompressed_direct_buf_len;
    stream->avail_out = compressed_direct_buf_len;

	// Compress
	rv = dlsym_deflate(stream, finish ? Z_FINISH : Z_NO_FLUSH);

	switch (rv) {
    	// Contingency? - Report error by throwing appropriate exceptions
  		case Z_STREAM_END:
  		{
  			(*env)->SetBooleanField(env, this, ZlibCompressor_finished, JNI_TRUE);
  		} // cascade
      case Z_OK:
	  	{
	  		uncompressed_direct_buf_off += uncompressed_direct_buf_len - stream->avail_in;
			(*env)->SetIntField(env, this,
						ZlibCompressor_uncompressedDirectBufOff, uncompressed_direct_buf_off);
			(*env)->SetIntField(env, this,
						ZlibCompressor_uncompressedDirectBufLen, stream->avail_in);
			no_compressed_bytes = compressed_direct_buf_len - stream->avail_out;
	  	}
	  	break;
  		case Z_BUF_ERROR:
  		break;
  		default:
		{
			THROW(env, "java/lang/InternalError", stream->msg);
		}
		break;
  	}

  	return no_compressed_bytes;
}

JNIEXPORT jlong JNICALL
Java_org_apache_hadoop_io_compress_zlib_ZlibCompressor_getBytesRead(
	JNIEnv *env, jclass class, jlong stream
	) {
    return (ZSTREAM(stream))->total_in;
}

JNIEXPORT jlong JNICALL
Java_org_apache_hadoop_io_compress_zlib_ZlibCompressor_getBytesWritten(
	JNIEnv *env, jclass class, jlong stream
	) {
    return (ZSTREAM(stream))->total_out;
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_compress_zlib_ZlibCompressor_reset(
	JNIEnv *env, jclass class, jlong stream
	) {
    if (dlsym_deflateReset(ZSTREAM(stream)) != Z_OK) {
		THROW(env, "java/lang/InternalError", NULL);
    }
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_compress_zlib_ZlibCompressor_end(
	JNIEnv *env, jclass class, jlong stream
	) {
    if (dlsym_deflateEnd(ZSTREAM(stream)) == Z_STREAM_ERROR) {
		THROW(env, "java/lang/InternalError", NULL);
    } else {
		free(ZSTREAM(stream));
    }
}

/**
 * vim: sw=2: ts=2: et:
 */

