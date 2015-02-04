/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
#ifndef HADOOP_MAIN_NATIVE_SRC_EXCEPTION_H
#define HADOOP_MAIN_NATIVE_SRC_EXCEPTION_H

#include <jni.h> /* for jthrowable */
#include <stdarg.h> /* for va_list */
#include "org_apache_hadoop.h"

#ifdef WINDOWS
/*
 * gcc-style type-checked format arguments are not supported on Windows, so just
 * stub this macro.
 */
#define TYPE_CHECKED_PRINTF_FORMAT(formatArg, varArgs)
# else
/* Use gcc type-checked format arguments. */
#define TYPE_CHECKED_PRINTF_FORMAT(formatArg, varArgs) \
  __attribute__((format(printf, formatArg, varArgs)))
#endif

/**
 * Create a new Exception.
 *
 * No exceptions will be pending on return.
 *
 * @param env           The JNI environment
 * @param name          full name of the Java exception class
 * @param fmt           printf-style format string
 * @param ap            printf-style arguments
 *
 * @return              The RuntimeException
 */
jthrowable newExceptionV(JNIEnv* env, const char *name,
                         const char *fmt, va_list ap);

/**
 * Create a new Exception.
 *
 * No exceptions will be pending on return.
 *
 * @param env           The JNI environment
 * @param name          full name of the Java exception class
 * @param fmt           printf-style format string
 * @param ...           printf-style arguments
 *
 * @return              The RuntimeException
 */
jthrowable newException(JNIEnv* env, const char *name, const char *fmt, ...)
    TYPE_CHECKED_PRINTF_FORMAT(3, 4);

/**
 * Create a new RuntimeException.
 *
 * No exceptions will be pending on return.
 *
 * @param env           The JNI environment
 * @param fmt           printf-style format string
 * @param ...           printf-style arguments
 *
 * @return              The RuntimeException
 */
jthrowable newRuntimeException(JNIEnv* env, const char *fmt, ...)
    TYPE_CHECKED_PRINTF_FORMAT(2, 3);

/**
 * Create a new IOException.
 *
 * No exceptions will be pending on return.
 *
 * @param env           The JNI environment
 * @param fmt           printf-style format string
 * @param ...           printf-style arguments
 *
 * @return              The IOException, or another exception if we failed
 *                      to create the NativeIOException.
 */
jthrowable newIOException(JNIEnv* env, const char *fmt, ...)
    TYPE_CHECKED_PRINTF_FORMAT(2, 3);

/**
 * Thread-safe strerror alternative.
 *
 * @param errnum        Error number.
 * @return              Statically allocated error string.
 */
const char* terror(int errnum);

#undef TYPE_CHECKED_PRINTF_FORMAT
#endif
