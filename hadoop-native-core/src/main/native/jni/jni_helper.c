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

#include "common/htable.h"
#include "config.h"
#include "jni/exception.h"
#include "jni/jni_helper.h"

#include <stdio.h> 
#include <string.h> 
#include <uv.h> 

/**
 * JNI Helper functions.
 *
 * These are a bunch of functions to make using JNI more bearable.
 * They should be portable, since we use libuv for most platform-specific
 * things.
 *
 * Currently, once JNI is initialized, it cannot be uninitialized.  This is
 * fine for most apps, but at some point, we should implement a shutdown
 * function.
 */

/**
 * Non-zero if jni_helper.c is initialized.
 */
static int g_jni_init;

/**
 * Once structure initializing jni_helper.c.
 */
static uv_once_t g_jni_init_once = UV_ONCE_INIT;

/**
 * The JNI library.
 */
static uv_lib_t g_jni_lib;

/**
 * The JNI_GetCreatedJavaVMs function.
 */
jint (*GetCreatedJavaVMs)(JavaVM **vmBuf, jsize bufLen, jsize *nVMs);

/**
 * The JNI_CreateJavaVM function.
 */
jint (*CreateJavaVM)(JavaVM **p_vm, void **p_env, void *vm_args);

/**
 * A hash table mapping class names to jclass objects.
 * The keys are strings, and the values are jclass objects.
 * We do not ever delete entries from this table.
 *
 * This may only be accessed under g_name_to_classref_htable_lock.
 */
static struct htable *g_name_to_classref_htable;

/**
 * The lock protecting g_name_to_classref_htable.
 */
static uv_mutex_t g_name_to_classref_htable_lock;

/**
 * Key that allows us to retrieve thread-local storage
 *
 * TODO: use the libuv local storage wrappers, once they supports thread
 * destructors.  (See https://github.com/joyent/libuv/issues/1216)
 **/
static pthread_key_t g_tls_key;

/** The Native return types that methods could return */
#define VOID          'V'
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

/** Pthreads thread-local storage for each library thread. */
struct hdfsTls {
    JNIEnv *env;
};

static void hdfsThreadDestructor(void *v);
static JNIEnv* getGlobalJNIEnv(void);
static uint32_t jni_helper_hash_string(const void *key, uint32_t capacity);
static int jni_helper_compare_string(const void *a, const void *b);

static void jni_helper_init(void)
{
    int ret;
    JNIEnv *env;
    jthrowable jthr;

    if (uv_dlopen(JNI_LIBRARY_NAME, &g_jni_lib) < 0) {
        fprintf(stderr, "jni_helper_init: failed to load " JNI_LIBRARY_NAME
                ": %s\n", uv_dlerror(&g_jni_lib));
        return;
    }
    if (uv_dlsym(&g_jni_lib, "JNI_GetCreatedJavaVMs",
                 (void**)&GetCreatedJavaVMs) < 0) {
        fprintf(stderr, "jni_helper_init: failed to find "
                "JNI_GetCreatedJavaVMs function in "
                JNI_LIBRARY_NAME "\n");
        return;
    }
    if (uv_dlsym(&g_jni_lib, "JNI_CreateJavaVM",
                 (void**)&CreateJavaVM) < 0) {
        fprintf(stderr, "jni_helper_init: failed to find "
                "JNI_CreateJavaVM function in "
                JNI_LIBRARY_NAME "\n");
        return;
    }
    env = getGlobalJNIEnv();
    if (!env) {
        fprintf(stderr, "jni_helper_init: getGlobalJNIENv failed.\n");
        return;
    }
    g_name_to_classref_htable = htable_alloc(128,
            jni_helper_hash_string, jni_helper_compare_string);
    if (!g_name_to_classref_htable) {
        fprintf(stderr, "jni_helper_init: failed to allocate "
                "name-to-classref hash table.\n");
        return;
    }
    if (uv_mutex_init(&g_name_to_classref_htable_lock) < 0) {
        fprintf(stderr, "jni_helper_init: failed to init mutex for "
                "name-to-classref hash table.\n");
        return;
    }
    ret = pthread_key_create(&g_tls_key, hdfsThreadDestructor);
    if (ret) {
        fprintf(stderr, "jni_helper_init: pthread_key_create failed with "
            "error %d\n", ret);
        return;
    }
    jthr = invokeMethod(env, NULL, STATIC, NULL,
                     "org/apache/hadoop/fs/FileSystem",
                     "loadFileSystems", "()V");
    if (jthr) {
        printExceptionAndFree(env, jthr, PRINT_EXC_ALL, "loadFileSystems");
        return;
    }
    g_jni_init = 1;
}

/**
 * The function that is called whenever a thread with libhdfs thread local data
 * is destroyed.
 *
 * @param v         The thread-local data
 */
static void hdfsThreadDestructor(void *v)
{
    struct hdfsTls *tls = v;
    JavaVM *vm;
    JNIEnv *env = tls->env;
    jint ret;

    ret = (*env)->GetJavaVM(env, &vm);
    if (ret) {
        fprintf(stderr, "hdfsThreadDestructor: GetJavaVM failed with "
                "error %d\n", ret);
        (*env)->ExceptionDescribe(env);
    } else {
        (*vm)->DetachCurrentThread(vm);
    }
    free(tls);
}

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

static uint32_t jni_helper_hash_string(const void *key, uint32_t capacity)
{
    const char *str = key;
    uint32_t hash = 0;

    while (*str) {
        hash = (hash * 31) + *str;
        str++;
    }
    return hash % capacity;
}

static int jni_helper_compare_string(const void *a, const void *b)
{
    return strcmp(a, b) == 0;
}

jthrowable invokeMethod(JNIEnv *env, jvalue *retval, MethType methType,
                 jobject instObj, const char *className,
                 const char *methName, const char *methSignature, ...)
{
    va_list args;
    jclass cls;
    jmethodID mid;
    jthrowable jthr;
    const char *str; 
    char returnType;
    
    jthr = validateMethodType(env, methType);
    if (jthr)
        return jthr;
    jthr = globalClassReference(className, env, &cls);
    if (jthr)
        return jthr;
    jthr = methodIdFromClass(className, methName, methSignature, 
                            methType, env, &mid);
    if (jthr)
        return jthr;
    str = methSignature;
    while (*str != ')') str++;
    str++;
    returnType = *str;
    va_start(args, methSignature);
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
    else if (returnType == VOID) {
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
    va_end(args);

    jthr = (*env)->ExceptionOccurred(env);
    if (jthr) {
        (*env)->ExceptionClear(env);
        return jthr;
    }
    return NULL;
}

jthrowable constructNewObjectOfClass(JNIEnv *env, jobject *out, const char *className, 
                                  const char *ctorSignature, ...)
{
    va_list args;
    jclass cls;
    jmethodID mid; 
    jobject jobj;
    jthrowable jthr;

    jthr = globalClassReference(className, env, &cls);
    if (jthr)
        return jthr;
    jthr = methodIdFromClass(className, "<init>", ctorSignature, 
                            INSTANCE, env, &mid);
    if (jthr)
        return jthr;
    va_start(args, ctorSignature);
    jobj = (*env)->NewObjectV(env, cls, mid, args);
    va_end(args);
    if (!jobj)
        return getPendingExceptionAndClear(env);
    *out = jobj;
    return NULL;
}


jthrowable methodIdFromClass(const char *className, const char *methName, 
                            const char *methSignature, MethType methType, 
                            JNIEnv *env, jmethodID *out)
{
    jclass cls;
    jthrowable jthr;

    jthr = globalClassReference(className, env, &cls);
    if (jthr)
        return jthr;
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

jthrowable globalClassReference(const char *className, JNIEnv *env, jclass *out)
{
    jthrowable jthr = NULL;
    jclass local_clazz = NULL;
    jclass clazz = NULL;
    int ret;

    uv_mutex_lock(&g_name_to_classref_htable_lock);
    clazz = htable_get(g_name_to_classref_htable, className);
    if (clazz) {
        *out = clazz;
        goto done;
    }
    local_clazz = (*env)->FindClass(env,className);
    if (!local_clazz) {
        jthr = getPendingExceptionAndClear(env);
        goto done;
    }
    clazz = (*env)->NewGlobalRef(env, local_clazz);
    if (!clazz) {
        jthr = getPendingExceptionAndClear(env);
        goto done;
    }
    ret = htable_put(g_name_to_classref_htable, (char*)className, clazz); 
    if (ret) {
        jthr = newRuntimeError(env, "htable_put failed with error "
                               "code %d\n", ret);
        goto done;
    }
    *out = clazz;
    jthr = NULL;
done:
    uv_mutex_unlock(&g_name_to_classref_htable_lock);
    (*env)->DeleteLocalRef(env, local_clazz);
    if (jthr && clazz) {
        (*env)->DeleteGlobalRef(env, clazz);
    }
    return jthr;
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
    const jsize vmBufLength = 1;
    char *classpath;
    JavaVM* vmBuf[vmBufLength]; 
    JNIEnv *env;
    jint rv = 0; 
    jint noVMs = 0;

    rv = GetCreatedJavaVMs(&(vmBuf[0]), vmBufLength, &noVMs);
    if (rv != 0) {
        fprintf(stderr, "getGlobalJNIEnv: JNI_GetCreatedJavaVMs failed "
                "with error: %d\n", rv);
        return NULL;
    }
    if (noVMs > 0) {
        //Attach this thread to the VM
        JavaVM* vm = vmBuf[0];
        rv = (*vm)->AttachCurrentThread(vm, (void*)&env, 0);
        if (rv != 0) {
            fprintf(stderr, "getGlobalJNIEnv: Call to AttachCurrentThread "
                    "failed with error: %d\n", rv);
            return NULL;
        }
        return env;
    }
    //Get the environment variables for initializing the JVM
    classpath = getenv("CLASSPATH");
    if (!classpath) {
        fprintf(stderr, "getGlobalJNIEnv: The CLASSPATH environment "
                "variable was not set.");
        return NULL;
    } 
    char *hadoopClassPathVMArg = "-Djava.class.path=";
    size_t optHadoopClassPathLen = strlen(classpath) + 
        strlen(hadoopClassPathVMArg) + 1;
    char *optHadoopClassPath = malloc(optHadoopClassPathLen);
    snprintf(optHadoopClassPath, optHadoopClassPathLen,
            "%s%s", hadoopClassPathVMArg, classpath);

    // Determine the # of LIBHDFS_OPTS args
    int noArgs = 1;
    char *hadoopJvmArgs = getenv("LIBHDFS_OPTS");
    char jvmArgDelims[] = " ";
    char *str, *token, *savePtr;
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
    JavaVMOption options[noArgs];
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
    JavaVMInitArgs vm_args;
    JavaVM *vm;
    vm_args.version = JNI_VERSION_1_2;
    vm_args.options = options;
    vm_args.nOptions = noArgs; 
    vm_args.ignoreUnrecognized = 1;

    rv = CreateJavaVM(&vm, (void*)&env, &vm_args);

    if (hadoopJvmArgs != NULL)  {
      free(hadoopJvmArgs);
    }
    free(optHadoopClassPath);
    if (rv != 0) {
        fprintf(stderr, "Call to JNI_CreateJavaVM failed "
                "with error: %d\n", rv);
        return NULL;
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
    JNIEnv *env;
    struct hdfsTls *tls;
    int ret;

#ifdef HAVE_BETTER_TLS
    static __thread struct hdfsTls *quickTls = NULL;
    if (quickTls)
        return quickTls->env;
#endif
    uv_once(&g_jni_init_once, jni_helper_init);
    if (!g_jni_init) {
        fprintf(stderr, "getJNIEnv: failed to initialize jni.\n");
        return NULL;
    }
    tls = pthread_getspecific(g_tls_key);
    if (tls) {
        return tls->env;
    }
    env = getGlobalJNIEnv();
    if (!env) {
        fprintf(stderr, "getJNIEnv: getGlobalJNIEnv failed\n");
        return NULL;
    }
    tls = calloc(1, sizeof(struct hdfsTls));
    if (!tls) {
        fprintf(stderr, "getJNIEnv: OOM allocating %zd bytes\n",
                sizeof(struct hdfsTls));
        return NULL;
    }
    tls->env = env;
    ret = pthread_setspecific(g_tls_key, tls);
    if (ret) {
        fprintf(stderr, "getJNIEnv: pthread_setspecific failed with "
            "error code %d\n", ret);
        hdfsThreadDestructor(tls);
        return NULL;
    }
#ifdef HAVE_BETTER_TLS
    quickTls = tls;
#endif
    return env;
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
            "org/apache/hadoop/conf/Configuration", "set", 
            "(Ljava/lang/String;Ljava/lang/String;)V",
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
        return newRuntimeError(env, "fetchEnum(%s, %s): failed to find class.",
                className, valueName);
    }
    if ((size_t)snprintf(prettyClass, sizeof(prettyClass), "L%s;", className)
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

// vim: ts=4:sw=4:tw=79:et
