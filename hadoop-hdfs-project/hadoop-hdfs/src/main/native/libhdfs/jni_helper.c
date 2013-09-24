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
#include "jni_helper.h"

#include <stdio.h> 
#include <string.h> 

static pthread_mutex_t hdfsHashMutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t jvmMutex = PTHREAD_MUTEX_INITIALIZER;
static volatile int hashTableInited = 0;

#define LOCK_HASH_TABLE() pthread_mutex_lock(&hdfsHashMutex)
#define UNLOCK_HASH_TABLE() pthread_mutex_unlock(&hdfsHashMutex)


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


/**
 * MAX_HASH_TABLE_ELEM: The maximum no. of entries in the hashtable.
 * It's set to 4096 to account for (classNames + No. of threads)
 */
#define MAX_HASH_TABLE_ELEM 4096

/** Key that allows us to retrieve thread-local storage */
static pthread_key_t gTlsKey;

/** nonzero if we succeeded in initializing gTlsKey. Protected by the jvmMutex */
static int gTlsKeyInitialized = 0;

/** Pthreads thread-local storage for each library thread. */
struct hdfsTls {
    JNIEnv *env;
};

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

static int hashTableInit(void)
{
    if (!hashTableInited) {
        LOCK_HASH_TABLE();
        if (!hashTableInited) {
            if (hcreate(MAX_HASH_TABLE_ELEM) == 0) {
                fprintf(stderr, "error creating hashtable, <%d>: %s\n",
                        errno, strerror(errno));
                UNLOCK_HASH_TABLE();
                return 0;
            } 
            hashTableInited = 1;
        }
        UNLOCK_HASH_TABLE();
    }
    return 1;
}


static int insertEntryIntoTable(const char *key, void *data)
{
    ENTRY e, *ep;
    if (key == NULL || data == NULL) {
        return 0;
    }
    if (! hashTableInit()) {
      return -1;
    }
    e.data = data;
    e.key = (char*)key;
    LOCK_HASH_TABLE();
    ep = hsearch(e, ENTER);
    UNLOCK_HASH_TABLE();
    if (ep == NULL) {
        fprintf(stderr, "warn adding key (%s) to hash table, <%d>: %s\n",
                key, errno, strerror(errno));
    }  
    return 0;
}



static void* searchEntryFromTable(const char *key)
{
    ENTRY e,*ep;
    if (key == NULL) {
        return NULL;
    }
    hashTableInit();
    e.key = (char*)key;
    LOCK_HASH_TABLE();
    ep = hsearch(e, FIND);
    UNLOCK_HASH_TABLE();
    if (ep != NULL) {
        return ep->data;
    }
    return NULL;
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
    jclass clsLocalRef;
    jclass cls = searchEntryFromTable(className);
    if (cls) {
        *out = cls;
        return NULL;
    }
    clsLocalRef = (*env)->FindClass(env,className);
    if (clsLocalRef == NULL) {
        return getPendingExceptionAndClear(env);
    }
    cls = (*env)->NewGlobalRef(env, clsLocalRef);
    if (cls == NULL) {
        (*env)->DeleteLocalRef(env, clsLocalRef);
        return getPendingExceptionAndClear(env);
    }
    (*env)->DeleteLocalRef(env, clsLocalRef);
    insertEntryIntoTable(className, cls);
    *out = cls;
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
    JavaVM* vmBuf[vmBufLength]; 
    JNIEnv *env;
    jint rv = 0; 
    jint noVMs = 0;
    jthrowable jthr;

    rv = JNI_GetCreatedJavaVMs(&(vmBuf[0]), vmBufLength, &noVMs);
    if (rv != 0) {
        fprintf(stderr, "JNI_GetCreatedJavaVMs failed with error: %d\n", rv);
        return NULL;
    }

    if (noVMs == 0) {
        //Get the environment variables for initializing the JVM
        char *hadoopClassPath = getenv("CLASSPATH");
        if (hadoopClassPath == NULL) {
            fprintf(stderr, "Environment variable CLASSPATH not set!\n");
            return NULL;
        } 
        char *hadoopClassPathVMArg = "-Djava.class.path=";
        size_t optHadoopClassPathLen = strlen(hadoopClassPath) + 
          strlen(hadoopClassPathVMArg) + 1;
        char *optHadoopClassPath = malloc(sizeof(char)*optHadoopClassPathLen);
        snprintf(optHadoopClassPath, optHadoopClassPathLen,
                "%s%s", hadoopClassPathVMArg, hadoopClassPath);

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

        rv = JNI_CreateJavaVM(&vm, (void*)&env, &vm_args);

        if (hadoopJvmArgs != NULL)  {
          free(hadoopJvmArgs);
        }
        free(optHadoopClassPath);

        if (rv != 0) {
            fprintf(stderr, "Call to JNI_CreateJavaVM failed "
                    "with error: %d\n", rv);
            return NULL;
        }
        jthr = invokeMethod(env, NULL, STATIC, NULL,
                         "org/apache/hadoop/fs/FileSystem",
                         "loadFileSystems", "()V");
        if (jthr) {
            printExceptionAndFree(env, jthr, PRINT_EXC_ALL, "loadFileSystems");
        }
    }
    else {
        //Attach this thread to the VM
        JavaVM* vm = vmBuf[0];
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
    JNIEnv *env;
    struct hdfsTls *tls;
    int ret;

#ifdef HAVE_BETTER_TLS
    static __thread struct hdfsTls *quickTls = NULL;
    if (quickTls)
        return quickTls->env;
#endif
    pthread_mutex_lock(&jvmMutex);
    if (!gTlsKeyInitialized) {
        ret = pthread_key_create(&gTlsKey, hdfsThreadDestructor);
        if (ret) {
            pthread_mutex_unlock(&jvmMutex);
            fprintf(stderr, "getJNIEnv: pthread_key_create failed with "
                "error %d\n", ret);
            return NULL;
        }
        gTlsKeyInitialized = 1;
    }
    tls = pthread_getspecific(gTlsKey);
    if (tls) {
        pthread_mutex_unlock(&jvmMutex);
        return tls->env;
    }

    env = getGlobalJNIEnv();
    pthread_mutex_unlock(&jvmMutex);
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
    ret = pthread_setspecific(gTlsKey, tls);
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

