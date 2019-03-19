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

#include "config.h"
#include "exception.h"
#include "jclasses.h"
#include "jni_helper.h"
#include "platform.h"
#include "os/mutexes.h"
#include "os/thread_local_storage.h"

#include <stdio.h> 
#include <string.h> 

/** The Native return types that methods could return */
#define JVOID         'V'
#define JOBJECT       'L'
#define JARRAYOBJECT  '['
#define JBOOLEAN      'Z'
#define JBYTE         'B'
#define JCHAR         'C'
#define JSHORT        'S'
#define JINT          'I'
#define JLONG         'J'
#define JFLOAT        'F'
#define JDOUBLE       'D'

/**
 * Length of buffer for retrieving created JVMs.  (We only ever create one.)
 */
#define VM_BUF_LENGTH 1

void destroyLocalReference(JNIEnv *env, jobject jObject)
{
  if (jObject)
    (*env)->DeleteLocalRef(env, jObject);
}

static jthrowable validateMethodType(JNIEnv *env, MethType methType)
{
    if (methType != STATIC && methType != INSTANCE) {
        return newRuntimeError(env, "validateMethodType(methType=%d): "
            "illegal method type.\n", methType);
    }
    return NULL;
}

jthrowable newJavaStr(JNIEnv *env, const char *str, jstring *out)
{
    jstring jstr;

    if (!str) {
        /* Can't pass NULL to NewStringUTF: the result would be
         * implementation-defined. */
        *out = NULL;
        return NULL;
    }
    jstr = (*env)->NewStringUTF(env, str);
    if (!jstr) {
        /* If NewStringUTF returns NULL, an exception has been thrown,
         * which we need to handle.  Probaly an OOM. */
        return getPendingExceptionAndClear(env);
    }
    *out = jstr;
    return NULL;
}

jthrowable newCStr(JNIEnv *env, jstring jstr, char **out)
{
    const char *tmp;

    if (!jstr) {
        *out = NULL;
        return NULL;
    }
    tmp = (*env)->GetStringUTFChars(env, jstr, NULL);
    if (!tmp) {
        return getPendingExceptionAndClear(env);
    }
    *out = strdup(tmp);
    (*env)->ReleaseStringUTFChars(env, jstr, tmp);
    return NULL;
}

/**
 * Does the work to actually execute a Java method. Takes in an existing jclass
 * object and a va_list of arguments for the Java method to be invoked.
 */
static jthrowable invokeMethodOnJclass(JNIEnv *env, jvalue *retval,
        MethType methType, jobject instObj, jclass cls, const char *className,
        const char *methName, const char *methSignature, va_list args)
{
    jmethodID mid;
    jthrowable jthr;
    const char *str;
    char returnType;

    jthr = methodIdFromClass(cls, className, methName, methSignature, methType,
                             env, &mid);
    if (jthr)
        return jthr;
    str = methSignature;
    while (*str != ')') str++;
    str++;
    returnType = *str;
    if (returnType == JOBJECT || returnType == JARRAYOBJECT) {
        jobject jobj = NULL;
        if (methType == STATIC) {
            jobj = (*env)->CallStaticObjectMethodV(env, cls, mid, args);
        }
        else if (methType == INSTANCE) {
            jobj = (*env)->CallObjectMethodV(env, instObj, mid, args);
        }
        retval->l = jobj;
    }
    else if (returnType == JVOID) {
        if (methType == STATIC) {
            (*env)->CallStaticVoidMethodV(env, cls, mid, args);
        }
        else if (methType == INSTANCE) {
            (*env)->CallVoidMethodV(env, instObj, mid, args);
        }
    }
    else if (returnType == JBOOLEAN) {
        jboolean jbool = 0;
        if (methType == STATIC) {
            jbool = (*env)->CallStaticBooleanMethodV(env, cls, mid, args);
        }
        else if (methType == INSTANCE) {
            jbool = (*env)->CallBooleanMethodV(env, instObj, mid, args);
        }
        retval->z = jbool;
    }
    else if (returnType == JSHORT) {
        jshort js = 0;
        if (methType == STATIC) {
            js = (*env)->CallStaticShortMethodV(env, cls, mid, args);
        }
        else if (methType == INSTANCE) {
            js = (*env)->CallShortMethodV(env, instObj, mid, args);
        }
        retval->s = js;
    }
    else if (returnType == JLONG) {
        jlong jl = -1;
        if (methType == STATIC) {
            jl = (*env)->CallStaticLongMethodV(env, cls, mid, args);
        }
        else if (methType == INSTANCE) {
            jl = (*env)->CallLongMethodV(env, instObj, mid, args);
        }
        retval->j = jl;
    }
    else if (returnType == JINT) {
        jint ji = -1;
        if (methType == STATIC) {
            ji = (*env)->CallStaticIntMethodV(env, cls, mid, args);
        }
        else if (methType == INSTANCE) {
            ji = (*env)->CallIntMethodV(env, instObj, mid, args);
        }
        retval->i = ji;
    }

    jthr = (*env)->ExceptionOccurred(env);
    if (jthr) {
        (*env)->ExceptionClear(env);
        return jthr;
    }
    return NULL;
}

jthrowable findClassAndInvokeMethod(JNIEnv *env, jvalue *retval,
        MethType methType, jobject instObj, const char *className,
        const char *methName, const char *methSignature, ...)
{
    jclass cls = NULL;
    jthrowable jthr = NULL;

    va_list args;
    va_start(args, methSignature);

    jthr = validateMethodType(env, methType);
    if (jthr) {
        goto done;
    }

    cls = (*env)->FindClass(env, className);
    if (!cls) {
        jthr = getPendingExceptionAndClear(env);
        goto done;
    }

    jthr = invokeMethodOnJclass(env, retval, methType, instObj, cls,
            className, methName, methSignature, args);

done:
    va_end(args);
    destroyLocalReference(env, cls);
    return jthr;
}

jthrowable invokeMethod(JNIEnv *env, jvalue *retval, MethType methType,
        jobject instObj, CachedJavaClass class,
        const char *methName, const char *methSignature, ...)
{
    jthrowable jthr;

    va_list args;
    va_start(args, methSignature);

    jthr = invokeMethodOnJclass(env, retval, methType, instObj,
            getJclass(class), getClassName(class), methName, methSignature,
            args);

    va_end(args);
    return jthr;
}

static jthrowable constructNewObjectOfJclass(JNIEnv *env,
        jobject *out, jclass cls, const char *className,
                const char *ctorSignature, va_list args) {
    jmethodID mid;
    jobject jobj;
    jthrowable jthr;

    jthr = methodIdFromClass(cls, className, "<init>", ctorSignature, INSTANCE,
            env, &mid);
    if (jthr)
        return jthr;
    jobj = (*env)->NewObjectV(env, cls, mid, args);
    if (!jobj)
        return getPendingExceptionAndClear(env);
    *out = jobj;
    return NULL;
}

jthrowable constructNewObjectOfClass(JNIEnv *env, jobject *out,
        const char *className, const char *ctorSignature, ...)
{
    va_list args;
    jclass cls;
    jthrowable jthr = NULL;

    cls = (*env)->FindClass(env, className);
    if (!cls) {
        jthr = getPendingExceptionAndClear(env);
        goto done;
    }

    va_start(args, ctorSignature);
    jthr = constructNewObjectOfJclass(env, out, cls, className,
            ctorSignature, args);
    va_end(args);
done:
    destroyLocalReference(env, cls);
    return jthr;
}

jthrowable constructNewObjectOfCachedClass(JNIEnv *env, jobject *out,
        CachedJavaClass cachedJavaClass, const char *ctorSignature, ...)
{
    jthrowable jthr = NULL;
    va_list args;
    va_start(args, ctorSignature);

    jthr = constructNewObjectOfJclass(env, out,
            getJclass(cachedJavaClass), getClassName(cachedJavaClass),
            ctorSignature, args);

    va_end(args);
    return jthr;
}

jthrowable methodIdFromClass(jclass cls, const char *className,
        const char *methName, const char *methSignature, MethType methType,
        JNIEnv *env, jmethodID *out)
{
    jthrowable jthr;
    jmethodID mid = 0;

    jthr = validateMethodType(env, methType);
    if (jthr)
        return jthr;
    if (methType == STATIC) {
        mid = (*env)->GetStaticMethodID(env, cls, methName, methSignature);
    }
    else if (methType == INSTANCE) {
        mid = (*env)->GetMethodID(env, cls, methName, methSignature);
    }
    if (mid == NULL) {
        fprintf(stderr, "could not find method %s from class %s with "
            "signature %s\n", methName, className, methSignature);
        return getPendingExceptionAndClear(env);
    }
    *out = mid;
    return NULL;
}

jthrowable classNameOfObject(jobject jobj, JNIEnv *env, char **name)
{
    jthrowable jthr;
    jclass cls, clsClass = NULL;
    jmethodID mid;
    jstring str = NULL;
    const char *cstr = NULL;
    char *newstr;

    cls = (*env)->GetObjectClass(env, jobj);
    if (cls == NULL) {
        jthr = getPendingExceptionAndClear(env);
        goto done;
    }
    clsClass = (*env)->FindClass(env, "java/lang/Class");
    if (clsClass == NULL) {
        jthr = getPendingExceptionAndClear(env);
        goto done;
    }
    mid = (*env)->GetMethodID(env, clsClass, "getName", "()Ljava/lang/String;");
    if (mid == NULL) {
        jthr = getPendingExceptionAndClear(env);
        goto done;
    }
    str = (*env)->CallObjectMethod(env, cls, mid);
    jthr = (*env)->ExceptionOccurred(env);
    if (jthr) {
        (*env)->ExceptionClear(env);
        goto done;
    }
    if (str == NULL) {
        jthr = getPendingExceptionAndClear(env);
        goto done;
    }
    cstr = (*env)->GetStringUTFChars(env, str, NULL);
    if (!cstr) {
        jthr = getPendingExceptionAndClear(env);
        goto done;
    }
    newstr = strdup(cstr);
    if (newstr == NULL) {
        jthr = newRuntimeError(env, "classNameOfObject: out of memory");
        goto done;
    }
    *name = newstr;
    jthr = NULL;

done:
    destroyLocalReference(env, cls);
    destroyLocalReference(env, clsClass);
    if (str) {
        if (cstr)
            (*env)->ReleaseStringUTFChars(env, str, cstr);
        (*env)->DeleteLocalRef(env, str);
    }
    return jthr;
}

/**
 * Get the global JNI environemnt.
 *
 * We only have to create the JVM once.  After that, we can use it in
 * every thread.  You must be holding the jvmMutex when you call this
 * function.
 *
 * @return          The JNIEnv on success; error code otherwise
 */
static JNIEnv* getGlobalJNIEnv(void)
{
    JavaVM* vmBuf[VM_BUF_LENGTH]; 
    JNIEnv *env;
    jint rv = 0; 
    jint noVMs = 0;
    jthrowable jthr;
    char *hadoopClassPath;
    const char *hadoopClassPathVMArg = "-Djava.class.path=";
    size_t optHadoopClassPathLen;
    char *optHadoopClassPath;
    int noArgs = 1;
    char *hadoopJvmArgs;
    char jvmArgDelims[] = " ";
    char *str, *token, *savePtr;
    JavaVMInitArgs vm_args;
    JavaVM *vm;
    JavaVMOption *options;

    rv = JNI_GetCreatedJavaVMs(&(vmBuf[0]), VM_BUF_LENGTH, &noVMs);
    if (rv != 0) {
        fprintf(stderr, "JNI_GetCreatedJavaVMs failed with error: %d\n", rv);
        return NULL;
    }

    if (noVMs == 0) {
        //Get the environment variables for initializing the JVM
        hadoopClassPath = getenv("CLASSPATH");
        if (hadoopClassPath == NULL) {
            fprintf(stderr, "Environment variable CLASSPATH not set!\n");
            return NULL;
        } 
        optHadoopClassPathLen = strlen(hadoopClassPath) + 
          strlen(hadoopClassPathVMArg) + 1;
        optHadoopClassPath = malloc(sizeof(char)*optHadoopClassPathLen);
        snprintf(optHadoopClassPath, optHadoopClassPathLen,
                "%s%s", hadoopClassPathVMArg, hadoopClassPath);

        // Determine the # of LIBHDFS_OPTS args
        hadoopJvmArgs = getenv("LIBHDFS_OPTS");
        if (hadoopJvmArgs != NULL)  {
          hadoopJvmArgs = strdup(hadoopJvmArgs);
          for (noArgs = 1, str = hadoopJvmArgs; ; noArgs++, str = NULL) {
            token = strtok_r(str, jvmArgDelims, &savePtr);
            if (NULL == token) {
              break;
            }
          }
          free(hadoopJvmArgs);
        }

        // Now that we know the # args, populate the options array
        options = calloc(noArgs, sizeof(JavaVMOption));
        if (!options) {
          fputs("Call to calloc failed\n", stderr);
          free(optHadoopClassPath);
          return NULL;
        }
        options[0].optionString = optHadoopClassPath;
        hadoopJvmArgs = getenv("LIBHDFS_OPTS");
	if (hadoopJvmArgs != NULL)  {
          hadoopJvmArgs = strdup(hadoopJvmArgs);
          for (noArgs = 1, str = hadoopJvmArgs; ; noArgs++, str = NULL) {
            token = strtok_r(str, jvmArgDelims, &savePtr);
            if (NULL == token) {
              break;
            }
            options[noArgs].optionString = token;
          }
        }

        //Create the VM
        vm_args.version = JNI_VERSION_1_2;
        vm_args.options = options;
        vm_args.nOptions = noArgs; 
        vm_args.ignoreUnrecognized = 1;

        rv = JNI_CreateJavaVM(&vm, (void*)&env, &vm_args);

        if (hadoopJvmArgs != NULL)  {
          free(hadoopJvmArgs);
        }
        free(optHadoopClassPath);
        free(options);

        if (rv != 0) {
            fprintf(stderr, "Call to JNI_CreateJavaVM failed "
                    "with error: %d\n", rv);
            return NULL;
        }

        // We use findClassAndInvokeMethod here because the jclasses in
        // jclasses.h have not loaded yet
        jthr = findClassAndInvokeMethod(env, NULL, STATIC, NULL, HADOOP_FS,
                "loadFileSystems", "()V");
        if (jthr) {
            printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                    "FileSystem: loadFileSystems failed");
            return NULL;
        }
    } else {
        //Attach this thread to the VM
        vm = vmBuf[0];
        rv = (*vm)->AttachCurrentThread(vm, (void*)&env, 0);
        if (rv != 0) {
            fprintf(stderr, "Call to AttachCurrentThread "
                    "failed with error: %d\n", rv);
            return NULL;
        }
    }

    return env;
}

/**
 * getJNIEnv: A helper function to get the JNIEnv* for the given thread.
 * If no JVM exists, then one will be created. JVM command line arguments
 * are obtained from the LIBHDFS_OPTS environment variable.
 *
 * Implementation note: we rely on POSIX thread-local storage (tls).
 * This allows us to associate a destructor function with each thread, that
 * will detach the thread from the Java VM when the thread terminates.  If we
 * failt to do this, it will cause a memory leak.
 *
 * However, POSIX TLS is not the most efficient way to do things.  It requires a
 * key to be initialized before it can be used.  Since we don't know if this key
 * is initialized at the start of this function, we have to lock a mutex first
 * and check.  Luckily, most operating systems support the more efficient
 * __thread construct, which is initialized by the linker.
 *
 * @param: None.
 * @return The JNIEnv* corresponding to the thread.
 */
JNIEnv* getJNIEnv(void)
{
    struct ThreadLocalState *state = NULL;
    THREAD_LOCAL_STORAGE_GET_QUICK(&state);
    if (state) return state->env;

    mutexLock(&jvmMutex);
    if (threadLocalStorageGet(&state)) {
      mutexUnlock(&jvmMutex);
      return NULL;
    }
    if (state) {
      mutexUnlock(&jvmMutex);

      // Free any stale exception strings.
      free(state->lastExceptionRootCause);
      free(state->lastExceptionStackTrace);
      state->lastExceptionRootCause = NULL;
      state->lastExceptionStackTrace = NULL;

      return state->env;
    }

    /* Create a ThreadLocalState for this thread */
    state = threadLocalStorageCreate();
    if (!state) {
      mutexUnlock(&jvmMutex);
      fprintf(stderr, "getJNIEnv: Unable to create ThreadLocalState\n");
      return NULL;
    }
    if (threadLocalStorageSet(state)) {
      mutexUnlock(&jvmMutex);
      goto fail;
    }
    THREAD_LOCAL_STORAGE_SET_QUICK(state);

    state->env = getGlobalJNIEnv();
    mutexUnlock(&jvmMutex);

    jthrowable jthr = NULL;
    jthr = initCachedClasses(state->env);
    if (jthr) {
      printExceptionAndFree(state->env, jthr, PRINT_EXC_ALL,
                            "initCachedClasses failed");
      goto fail;
    }

    if (!state->env) {
      goto fail;
    }
    return state->env;

fail:
    fprintf(stderr, "getJNIEnv: getGlobalJNIEnv failed\n");
    hdfsThreadDestructor(state);
    return NULL;
}

char* getLastTLSExceptionRootCause()
{
    struct ThreadLocalState *state = NULL;
    THREAD_LOCAL_STORAGE_GET_QUICK(&state);
    if (!state) {
        mutexLock(&jvmMutex);
        if (threadLocalStorageGet(&state)) {
            mutexUnlock(&jvmMutex);
            return NULL;
        }
        mutexUnlock(&jvmMutex);
    }
    return state->lastExceptionRootCause;
}

char* getLastTLSExceptionStackTrace()
{
    struct ThreadLocalState *state = NULL;
    THREAD_LOCAL_STORAGE_GET_QUICK(&state);
    if (!state) {
        mutexLock(&jvmMutex);
        if (threadLocalStorageGet(&state)) {
            mutexUnlock(&jvmMutex);
            return NULL;
        }
        mutexUnlock(&jvmMutex);
    }
    return state->lastExceptionStackTrace;
}

void setTLSExceptionStrings(const char *rootCause, const char *stackTrace)
{
    struct ThreadLocalState *state = NULL;
    THREAD_LOCAL_STORAGE_GET_QUICK(&state);
    if (!state) {
        mutexLock(&jvmMutex);
        if (threadLocalStorageGet(&state)) {
            mutexUnlock(&jvmMutex);
            return;
        }
        mutexUnlock(&jvmMutex);
    }

    free(state->lastExceptionRootCause);
    free(state->lastExceptionStackTrace);
    state->lastExceptionRootCause = (char*)rootCause;
    state->lastExceptionStackTrace = (char*)stackTrace;
}

int javaObjectIsOfClass(JNIEnv *env, jobject obj, const char *name)
{
    jclass clazz;
    int ret;

    clazz = (*env)->FindClass(env, name);
    if (!clazz) {
        printPendingExceptionAndFree(env, PRINT_EXC_ALL,
            "javaObjectIsOfClass(%s)", name);
        return -1;
    }
    ret = (*env)->IsInstanceOf(env, obj, clazz);
    (*env)->DeleteLocalRef(env, clazz);
    return ret == JNI_TRUE ? 1 : 0;
}

jthrowable hadoopConfSetStr(JNIEnv *env, jobject jConfiguration,
        const char *key, const char *value)
{
    jthrowable jthr;
    jstring jkey = NULL, jvalue = NULL;

    jthr = newJavaStr(env, key, &jkey);
    if (jthr)
        goto done;
    jthr = newJavaStr(env, value, &jvalue);
    if (jthr)
        goto done;
    jthr = invokeMethod(env, NULL, INSTANCE, jConfiguration,
            JC_CONFIGURATION, "set", "(Ljava/lang/String;Ljava/lang/String;)V",
            jkey, jvalue);
    if (jthr)
        goto done;
done:
    (*env)->DeleteLocalRef(env, jkey);
    (*env)->DeleteLocalRef(env, jvalue);
    return jthr;
}

jthrowable fetchEnumInstance(JNIEnv *env, const char *className,
                         const char *valueName, jobject *out)
{
    jclass clazz;
    jfieldID fieldId;
    jobject jEnum;
    char prettyClass[256];

    clazz = (*env)->FindClass(env, className);
    if (!clazz) {
        return getPendingExceptionAndClear(env);
    }
    if (snprintf(prettyClass, sizeof(prettyClass), "L%s;", className)
          >= sizeof(prettyClass)) {
        return newRuntimeError(env, "fetchEnum(%s, %s): class name too long.",
                className, valueName);
    }
    fieldId = (*env)->GetStaticFieldID(env, clazz, valueName, prettyClass);
    if (!fieldId) {
        return getPendingExceptionAndClear(env);
    }
    jEnum = (*env)->GetStaticObjectField(env, clazz, fieldId);
    if (!jEnum) {
        return getPendingExceptionAndClear(env);
    }
    *out = jEnum;
    return NULL;
}

