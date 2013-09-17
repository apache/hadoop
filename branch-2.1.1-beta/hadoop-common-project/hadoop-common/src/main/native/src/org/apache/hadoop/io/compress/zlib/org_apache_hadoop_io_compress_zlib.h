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

#if !defined ORG_APACHE_HADOOP_IO_COMPRESS_ZLIB_ZLIB_H
#define ORG_APACHE_HADOOP_IO_COMPRESS_ZLIB_ZLIB_H

#include "org_apache_hadoop.h"

#ifdef UNIX
#include <config.h>
#include <stddef.h>
#include <zlib.h>
#include <zconf.h>
#include <dlfcn.h>
#include <jni.h>
#endif

#ifdef WINDOWS
#include <jni.h>
#define HADOOP_ZLIB_LIBRARY L"zlib1.dll"
#include <zlib.h>
#include <zconf.h>
#endif

/* A helper macro to convert the java 'stream-handle' to a z_stream pointer. */
#define ZSTREAM(stream) ((z_stream*)((ptrdiff_t)(stream)))

/* A helper macro to convert the z_stream pointer to the java 'stream-handle'. */
#define JLONG(stream) ((jlong)((ptrdiff_t)(stream)))

#endif //ORG_APACHE_HADOOP_IO_COMPRESS_ZLIB_ZLIB_H
