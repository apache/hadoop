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

#ifndef LIBHDFS_EXCEPTION_H
#define LIBHDFS_EXCEPTION_H

/**
 * Exception handling routines for libhdfs.
 *
 * The convention we follow here is to clear pending exceptions as soon as they
 * are raised.  Never assume that the caller of your function will clean up
 * after you-- do it yourself.  Unhandled exceptions can lead to memory leaks
 * and other undefined behavior.
 *
 * If you encounter an exception, return a local reference to it.  The caller is
 * responsible for freeing the local reference, by calling a function like
 * printExceptionAndFree. (You can also free exceptions directly by calling
 * DeleteLocalRef.  However, that would not produce an error message, so it's
 * usually not what you want.)
 *
 * The root cause and stack trace exception strings retrieved from the last
 * exception that happened on a thread are stored in the corresponding
 * thread local state and are accessed by hdfsGetLastExceptionRootCause and
 * hdfsGetLastExceptionStackTrace respectively.
 */

#include "platform.h"

#include <jni.h>
#include <stdio.h>

#include <stdlib.h>
#include <stdarg.h>
#include <search.h>
#include <errno.h>

/**
 * Exception noprint flags
 *
 * Theses flags determine which exceptions should NOT be printed to stderr by
 * the exception printing routines.  For example, if you expect to see
 * FileNotFound, you might use NOPRINT_EXC_FILE_NOT_FOUND, to avoid filling the
 * logs with messages about routine events.
 *
 * On the other hand, if you don't expect any failures, you might pass
 * PRINT_EXC_ALL.
 *
 * You can OR these flags together to avoid printing multiple classes of
 * exceptions.
 */
#define PRINT_EXC_ALL                           0x00
#define NOPRINT_EXC_FILE_NOT_FOUND              0x01
#define NOPRINT_EXC_ACCESS_CONTROL              0x02
#define NOPRINT_EXC_UNRESOLVED_LINK             0x04
#define NOPRINT_EXC_PARENT_NOT_DIRECTORY        0x08
#define NOPRINT_EXC_ILLEGAL_ARGUMENT            0x10

/**
 * Get information about an exception.
 *
 * @param excName         The Exception name.
 *                        This is a Java class name in JNI format.
 * @param noPrintFlags    Flags which determine which exceptions we should NOT
 *                        print.
 * @param excErrno        (out param) The POSIX error number associated with the
 *                        exception.
 * @param shouldPrint     (out param) Nonzero if we should print this exception,
 *                        based on the noPrintFlags and its name. 
 */
void getExceptionInfo(const char *excName, int noPrintFlags,
                      int *excErrno, int *shouldPrint);

/**
 * Store the information about an exception in the thread-local state and print
 * it and free the jthrowable object.
 *
 * @param env             The JNI environment
 * @param exc             The exception to print and free
 * @param noPrintFlags    Flags which determine which exceptions we should NOT
 *                        print.
 * @param fmt             Printf-style format list
 * @param ap              Printf-style varargs
 *
 * @return                The POSIX error number associated with the exception
 *                        object.
 */
int printExceptionAndFreeV(JNIEnv *env, jthrowable exc, int noPrintFlags,
        const char *fmt, va_list ap);

/**
 * Store the information about an exception in the thread-local state and print
 * it and free the jthrowable object.
 *
 * @param env             The JNI environment
 * @param exc             The exception to print and free
 * @param noPrintFlags    Flags which determine which exceptions we should NOT
 *                        print.
 * @param fmt             Printf-style format list
 * @param ...             Printf-style varargs
 *
 * @return                The POSIX error number associated with the exception
 *                        object.
 */
int printExceptionAndFree(JNIEnv *env, jthrowable exc, int noPrintFlags,
        const char *fmt, ...) TYPE_CHECKED_PRINTF_FORMAT(4, 5);

/**
 * Store the information about the pending exception in the thread-local state
 * and print it and free the jthrowable object.
 *
 * @param env             The JNI environment
 * @param noPrintFlags    Flags which determine which exceptions we should NOT
 *                        print.
 * @param fmt             Printf-style format list
 * @param ...             Printf-style varargs
 *
 * @return                The POSIX error number associated with the exception
 *                        object.
 */
int printPendingExceptionAndFree(JNIEnv *env, int noPrintFlags,
        const char *fmt, ...) TYPE_CHECKED_PRINTF_FORMAT(3, 4);

/**
 * Get a local reference to the pending exception and clear it.
 *
 * Once it is cleared, the exception will no longer be pending.  The caller will
 * have to decide what to do with the exception object.
 *
 * @param env             The JNI environment
 *
 * @return                The exception, or NULL if there was no exception
 */
jthrowable getPendingExceptionAndClear(JNIEnv *env);

/**
 * Create a new runtime error.
 *
 * This creates (but does not throw) a new RuntimeError.
 *
 * @param env             The JNI environment
 * @param fmt             Printf-style format list
 * @param ...             Printf-style varargs
 *
 * @return                A local reference to a RuntimeError
 */
jthrowable newRuntimeError(JNIEnv *env, const char *fmt, ...)
        TYPE_CHECKED_PRINTF_FORMAT(2, 3);

#undef TYPE_CHECKED_PRINTF_FORMAT
#endif
