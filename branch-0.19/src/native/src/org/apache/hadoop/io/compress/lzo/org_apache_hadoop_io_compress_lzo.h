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

#if !defined ORG_APACHE_HADOOP_IO_COMPRESS_LZO_LZO_H
#define ORG_APACHE_HADOOP_IO_COMPRESS_LZO_LZO_H

#if defined HAVE_CONFIG_H
  #include <config.h>
#endif

#if defined HAVE_STDDEF_H
  #include <stddef.h>
#else
  #error 'stddef.h not found'
#endif
    
#if defined HAVE_DLFCN_H
  #include <dlfcn.h>
#else
  #error "dlfcn.h not found"
#endif  

#if defined HAVE_JNI_H    
  #include <jni.h>
#else
  #error 'jni.h not found'
#endif

#if defined HAVE_LZO_LZO1_H
  #include <lzo/lzo1.h>
#else
  #error 'lzo/lzo1.h not found'
#endif

#if defined HAVE_LZO_LZO1A_H
  #include <lzo/lzo1a.h>
#else
  #error 'lzo/lzo1a.h not found'
#endif

#if defined HAVE_LZO_LZO1B_H
  #include <lzo/lzo1b.h>
#else
  #error 'lzo/lzo1b.h not found'
#endif

#if defined HAVE_LZO_LZO1C_H
  #include <lzo/lzo1c.h>
#else
  #error 'lzo/lzo1c.h not found'
#endif

#if defined HAVE_LZO_LZO1F_H
  #include <lzo/lzo1f.h>
#else
  #error 'lzo/lzo1f.h not found'
#endif

#if defined HAVE_LZO_LZO1X_H
  #include <lzo/lzo1x.h>
#else
  #error 'lzo/lzo1x.h not found'
#endif

#if defined HAVE_LZO_LZO1Y_H
  #include <lzo/lzo1y.h>
#else
  #error 'lzo/lzo1y.h not found'
#endif

#if defined HAVE_LZO_LZO1Z_H
  #include <lzo/lzo1z.h>
#else
  #error 'lzo/lzo1z.h not found'
#endif

#if defined HAVE_LZO_LZO2A_H
  #include <lzo/lzo2a.h>
#else
  #error 'lzo/lzo2a.h not found'
#endif

#if defined HAVE_LZO_LZO_ASM_H
  #include <lzo/lzo_asm.h>
#else
  #error 'lzo/lzo_asm.h not found'
#endif

#include "org_apache_hadoop.h"

/* A helper macro to convert the java 'function-pointer' to a void*. */
#define FUNC_PTR(func_ptr) ((void*)((ptrdiff_t)(func_ptr)))

/* A helper macro to convert the void* to the java 'function-pointer'. */
#define JLONG(func_ptr) ((jlong)((ptrdiff_t)(func_ptr)))

#endif //ORG_APACHE_HADOOP_IO_COMPRESS_LZO_LZO_H
