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

#ifndef LIBHDFS_JNI_HELPER_H
#define LIBHDFS_JNI_HELPER_H

#include <jni.h>
#include <stdio.h>

#include <stdlib.h>
#include <stdarg.h>
#include <errno.h>

#define PATH_SEPARATOR ':'


/** Denote the method we want to invoke as STATIC or INSTANCE */
typedef enum {
    STATIC,
    INSTANCE
} MethType;

/**
 * Create a new malloc'ed C string from a Java string.
 *
 * @param env       The JNI environment
 * @param jstr      The Java string
 * @param out       (out param) the malloc'ed C string
 *
 * @return          NULL on success; the exception otherwise
 */
jthrowable newCStr(JNIEnv *env, jstring jstr, char **out);

/**
 * Create a new Java string from a C string.
 *
 * @param env       The JNI environment
 * @param str       The C string
 * @param out       (out param) the java string
 *
 * @return          NULL on success; the exception otherwise
 */
jthrowable newJavaStr(JNIEnv *env, const char *str, jstring *out);

/**
 * Helper function to destroy a local reference of java.lang.Object
 * @param env: The JNIEnv pointer. 
 * @param jFile: The local reference of java.lang.Object object
 * @return None.
 */
void destroyLocalReference(JNIEnv *env, jobject jObject);

/** invokeMethod: Invoke a Static or Instance method.
 * className: Name of the class where the method can be found
 * methName: Name of the method
 * methSignature: the signature of the method "(arg-types)ret-type"
 * methType: The type of the method (STATIC or INSTANCE)
 * instObj: Required if the methType is INSTANCE. The object to invoke
   the method on.
 * env: The JNIEnv pointer
 * retval: The pointer to a union type which will contain the result of the
   method invocation, e.g. if the method returns an Object, retval will be
   set to that, if the method returns boolean, retval will be set to the
   value (JNI_TRUE or JNI_FALSE), etc.
 * exc: If the methods throws any exception, this will contain the reference
 * Arguments (the method arguments) must be passed after methSignature
 * RETURNS: -1 on error and 0 on success. If -1 is returned, exc will have 
   a valid exception reference, and the result stored at retval is undefined.
 */
jthrowable invokeMethod(JNIEnv *env, jvalue *retval, MethType methType,
                 jobject instObj, const char *className, const char *methName, 
                 const char *methSignature, ...);

jthrowable constructNewObjectOfClass(JNIEnv *env, jobject *out, const char *className, 
                                  const char *ctorSignature, ...);

jthrowable methodIdFromClass(const char *className, const char *methName, 
                            const char *methSignature, MethType methType, 
                            JNIEnv *env, jmethodID *out);

jthrowable globalClassReference(const char *className, JNIEnv *env, jclass *out);

/** classNameOfObject: Get an object's class name.
 * @param jobj: The object.
 * @param env: The JNIEnv pointer.
 * @param name: (out param) On success, will contain a string containing the
 * class name. This string must be freed by the caller.
 * @return NULL on success, or the exception
 */
jthrowable classNameOfObject(jobject jobj, JNIEnv *env, char **name);

/** getJNIEnv: A helper function to get the JNIEnv* for the given thread.
 * It gets this from the ThreadLocalState if it exists. If a ThreadLocalState
 * does not exist, one will be created.
 * If no JVM exists, then one will be created. JVM command line arguments
 * are obtained from the LIBHDFS_OPTS environment variable.
 * @param: None.
 * @return The JNIEnv* corresponding to the thread.
 * */
JNIEnv* getJNIEnv(void);

/**
 * Get the last exception root cause that happened in the context of the
 * current thread.
 *
 * The pointer returned by this function is guaranteed to be valid until
 * the next call to invokeMethod() by the current thread.
 * Users of this function should not free the pointer.
 *
 * @return The root cause as a C-string.
 */
char* getLastTLSExceptionRootCause();

/**
 * Get the last exception stack trace that happened in the context of the
 * current thread.
 *
 * The pointer returned by this function is guaranteed to be valid until
 * the next call to invokeMethod() by the current thread.
 * Users of this function should not free the pointer.
 *
 * @return The stack trace as a C-string.
 */
char* getLastTLSExceptionStackTrace();

/** setTLSExceptionStrings: Sets the 'rootCause' and 'stackTrace' in the
 * ThreadLocalState if one exists for the current thread.
 *
 * @param rootCause A string containing the root cause of an exception.
 * @param stackTrace A string containing the stack trace of an exception.
 * @return None.
 */
void setTLSExceptionStrings(const char *rootCause, const char *stackTrace);

/**
 * Figure out if a Java object is an instance of a particular class.
 *
 * @param env  The Java environment.
 * @param obj  The object to check.
 * @param name The class name to check.
 *
 * @return     -1 if we failed to find the referenced class name.
 *             0 if the object is not of the given class.
 *             1 if the object is of the given class.
 */
int javaObjectIsOfClass(JNIEnv *env, jobject obj, const char *name);

/**
 * Set a value in a configuration object.
 *
 * @param env               The JNI environment
 * @param jConfiguration    The configuration object to modify
 * @param key               The key to modify
 * @param value             The value to set the key to
 *
 * @return                  NULL on success; exception otherwise
 */
jthrowable hadoopConfSetStr(JNIEnv *env, jobject jConfiguration,
        const char *key, const char *value);

/**
 * Fetch an instance of an Enum.
 *
 * @param env               The JNI environment.
 * @param className         The enum class name.
 * @param valueName         The name of the enum value
 * @param out               (out param) on success, a local reference to an
 *                          instance of the enum object.  (Since Java enums are
 *                          singletones, this is also the only instance.)
 *
 * @return                  NULL on success; exception otherwise
 */
jthrowable fetchEnumInstance(JNIEnv *env, const char *className,
                             const char *valueName, jobject *out);

#endif /*LIBHDFS_JNI_HELPER_H*/

/**
 * vim: ts=4: sw=4: et:
 */

