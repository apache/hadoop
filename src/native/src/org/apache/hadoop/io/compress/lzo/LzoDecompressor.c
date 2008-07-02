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

#include "org_apache_hadoop_io_compress_lzo.h"

// The lzo2 library-handle
static void *liblzo2 = NULL;
// lzo2 library version
static jint liblzo2_version = 0;

// The lzo 'decompressors'
static char* lzo_decompressors[] = {
  /** lzo1 decompressors */
  /* 0 */   "lzo1_decompress", 
  
  /** lzo1a compressors */
  /* 1 */   "lzo1a_decompress",

  /** lzo1b compressors */
  /* 2 */   "lzo1b_decompress", 
  /* 3 */   "lzo1b_decompress_safe",

  /** lzo1c compressors */
  /* 4 */   "lzo1c_decompress",
  /* 5 */   "lzo1c_decompress_safe",
  /* 6 */   "lzo1c_decompress_asm",
  /* 7 */   "lzo1c_decompress_asm_safe",
  
  /** lzo1f compressors */
  /* 8 */   "lzo1f_decompress",
  /* 9 */   "lzo1f_decompress_safe",
  /* 10 */  "lzo1f_decompress_asm_fast",
  /* 11 */  "lzo1f_decompress_asm_fast_safe",

  /** lzo1x compressors */
  /* 12 */  "lzo1x_decompress",
  /* 13 */  "lzo1x_decompress_safe",
  /* 14 */  "lzo1x_decompress_asm",
  /* 15 */  "lzo1x_decompress_asm_safe",
  /* 16 */  "lzo1x_decompress_asm_fast",
  /* 17 */  "lzo1x_decompress_asm_fast_safe"
  
  /** lzo1y compressors */
  /* 18 */  "lzo1y_decompress",
  /* 19 */  "lzo1y_decompress_safe",
  /* 20 */  "lzo1y_decompress_asm",
  /* 21 */  "lzo1y_decompress_asm_safe",
  /* 22 */  "lzo1y_decompress_asm_fast",
  /* 23 */  "lzo1y_decompress_asm_fast_safe",

  /** lzo1z compressors */
  /* 24 */  "lzo1z_decompress", 
  /* 25 */  "lzo1z_decompress_safe",

  /** lzo2a compressors */
  /* 26 */  "lzo2a_decompress",
  /* 27 */  "lzo2a_decompress_safe"
};

static jfieldID LzoDecompressor_clazz;
static jfieldID LzoDecompressor_finished;
static jfieldID LzoDecompressor_compressedDirectBuf;
static jfieldID LzoDecompressor_compressedDirectBufLen;
static jfieldID LzoDecompressor_uncompressedDirectBuf;
static jfieldID LzoDecompressor_directBufferSize;
static jfieldID LzoDecompressor_lzoDecompressor;

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_compress_lzo_LzoDecompressor_initIDs(
	JNIEnv *env, jclass class
	) {
	// Load liblzo2.so
	liblzo2 = dlopen(HADOOP_LZO_LIBRARY, RTLD_LAZY | RTLD_GLOBAL);
	if (!liblzo2) {
		THROW(env, "java/lang/UnsatisfiedLinkError", "Cannot load liblzo2.so!");
	  return;
	}
    
  LzoDecompressor_clazz = (*env)->GetStaticFieldID(env, class, "clazz", 
                                                   "Ljava/lang/Class;");
  LzoDecompressor_finished = (*env)->GetFieldID(env, class, "finished", "Z");
  LzoDecompressor_compressedDirectBuf = (*env)->GetFieldID(env, class, 
                                                "compressedDirectBuf", 
                                                "Ljava/nio/Buffer;");
  LzoDecompressor_compressedDirectBufLen = (*env)->GetFieldID(env, class, 
                                                    "compressedDirectBufLen", "I");
  LzoDecompressor_uncompressedDirectBuf = (*env)->GetFieldID(env, class, 
                                                  "uncompressedDirectBuf", 
                                                  "Ljava/nio/Buffer;");
  LzoDecompressor_directBufferSize = (*env)->GetFieldID(env, class, 
                                              "directBufferSize", "I");
  LzoDecompressor_lzoDecompressor = (*env)->GetFieldID(env, class,
                                              "lzoDecompressor", "J");

  // record lzo library version
  void* lzo_version_ptr = NULL;
  LOAD_DYNAMIC_SYMBOL(lzo_version_ptr, env, liblzo2, "lzo_version");
  liblzo2_version = (NULL == lzo_version_ptr) ? 0
    : (jint) ((unsigned (__LZO_CDECL *)())lzo_version_ptr)();
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_compress_lzo_LzoDecompressor_init(
  JNIEnv *env, jobject this, jint decompressor 
  ) {
  const char *lzo_decompressor_function = lzo_decompressors[decompressor];
 
  // Locate the requisite symbols from liblzo2.so
  dlerror();                                 // Clear any existing error

  // Initialize the lzo library 
  void *lzo_init_func_ptr = NULL;
  typedef int (__LZO_CDECL *lzo_init_t) (unsigned,int,int,int,int,int,int,int,int,int);
  LOAD_DYNAMIC_SYMBOL(lzo_init_func_ptr, env, liblzo2, "__lzo_init_v2");
  lzo_init_t lzo_init_function = (lzo_init_t)(lzo_init_func_ptr);
  int rv = lzo_init_function(LZO_VERSION, (int)sizeof(short), (int)sizeof(int), 
              (int)sizeof(long), (int)sizeof(lzo_uint32), (int)sizeof(lzo_uint), 
              (int)lzo_sizeof_dict_t, (int)sizeof(char*), (int)sizeof(lzo_voidp),
              (int)sizeof(lzo_callback_t));
  if (rv != LZO_E_OK) {
    THROW(env, "Ljava/lang/InternalError", "Could not initialize lzo library!");
    return;
  }
  
  // Save the decompressor-function into LzoDecompressor_lzoDecompressor
  void *decompressor_func_ptr = NULL;
  LOAD_DYNAMIC_SYMBOL(decompressor_func_ptr, env, liblzo2,
      lzo_decompressor_function);
  (*env)->SetLongField(env, this, LzoDecompressor_lzoDecompressor,
                       JLONG(decompressor_func_ptr));

  return;
}

JNIEXPORT jint JNICALL
Java_org_apache_hadoop_io_compress_lzo_LzoDecompressor_getLzoLibraryVersion(
    JNIEnv* env, jclass class) {
  return liblzo2_version;
}

JNIEXPORT jint JNICALL
Java_org_apache_hadoop_io_compress_lzo_LzoDecompressor_decompressBytesDirect(
	JNIEnv *env, jobject this, jint decompressor
	) {
  const char *lzo_decompressor_function = lzo_decompressors[decompressor];

	// Get members of LzoDecompressor
	jobject clazz = (*env)->GetStaticObjectField(env, this, 
	                                             LzoDecompressor_clazz);
	jobject compressed_direct_buf = (*env)->GetObjectField(env, this,
                                              LzoDecompressor_compressedDirectBuf);
	lzo_uint compressed_direct_buf_len = (*env)->GetIntField(env, this, 
                        		  							LzoDecompressor_compressedDirectBufLen);

	jobject uncompressed_direct_buf = (*env)->GetObjectField(env, this, 
                            								  LzoDecompressor_uncompressedDirectBuf);
	lzo_uint uncompressed_direct_buf_len = (*env)->GetIntField(env, this,
                                                LzoDecompressor_directBufferSize);

  jlong lzo_decompressor_funcptr = (*env)->GetLongField(env, this,
                                              LzoDecompressor_lzoDecompressor);

    // Get the input direct buffer
    LOCK_CLASS(env, clazz, "LzoDecompressor");
	lzo_bytep uncompressed_bytes = (*env)->GetDirectBufferAddress(env, 
											                    uncompressed_direct_buf);
    UNLOCK_CLASS(env, clazz, "LzoDecompressor");
    
 	if (uncompressed_bytes == 0) {
    return (jint)0;
	}
	
    // Get the output direct buffer
    LOCK_CLASS(env, clazz, "LzoDecompressor");
	lzo_bytep compressed_bytes = (*env)->GetDirectBufferAddress(env, 
										                    compressed_direct_buf);
    UNLOCK_CLASS(env, clazz, "LzoDecompressor");

  if (compressed_bytes == 0) {
		return (jint)0;
	}
	
	// Decompress
  lzo_uint no_uncompressed_bytes = uncompressed_direct_buf_len;
  lzo_decompress_t fptr = (lzo_decompress_t) FUNC_PTR(lzo_decompressor_funcptr);
	int rv = fptr(compressed_bytes, compressed_direct_buf_len,
                uncompressed_bytes, &no_uncompressed_bytes,
                NULL); 

  if (rv == LZO_E_OK) {
    // lzo decompresses all input data
    (*env)->SetIntField(env, this, LzoDecompressor_compressedDirectBufLen, 0);
  } else {
    const int msg_len = 32;
    char exception_msg[msg_len];
    snprintf(exception_msg, msg_len, "%s returned: %d", 
              lzo_decompressor_function, rv);
    THROW(env, "java/lang/InternalError", exception_msg);
  }
  
  return no_uncompressed_bytes;
}

/**
 * vim: sw=2: ts=2: et:
 */

