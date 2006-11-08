/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <string.h> 
#include "hdfsJniHelper.h"

static pthread_mutex_t hdfsHashMutex = PTHREAD_MUTEX_INITIALIZER;
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
 * Helpful macro to convert a pthread_t to a string
 */
#define GET_threadID(threadID, key, keySize) \
    snprintf(key, keySize, "__hdfs_threadID__%u", (unsigned)(threadID)); 
#define threadID_SIZE 32


/**
 * MAX_HASH_TABLE_ELEM: The maximum no. of entries in the hashtable.
 * It's set to 4096 to account for (classNames + No. of threads)
 */
#define MAX_HASH_TABLE_ELEM 4096


#define CHECK_EXCEPTION_IN_METH_INVOC \
    if ((*env)->ExceptionCheck(env)) {\
        (*env)->ExceptionDescribe(env);\
        va_end(args);\
        return -1;\
    }\


static void validateMethodType(MethType methType)
{
    if (methType != STATIC && methType != INSTANCE) {
        fprintf(stderr, "Unimplemented method type\n");
        exit(1);
    }
    return;
}


static void hashTableInit(void)
{
    if (!hashTableInited) {
        LOCK_HASH_TABLE();
        if (!hashTableInited) {
            if (hcreate(MAX_HASH_TABLE_ELEM) == 0) {
                fprintf(stderr, "error creating hashtable, <%d>: %s\n",
                        errno, strerror(errno));
                exit(1);
            } 
            hashTableInited = 1;
        }
        UNLOCK_HASH_TABLE();
    }
}


static void insertEntryIntoTable(const char *key, void *data)
{
    ENTRY e, *ep;
    if (key == NULL || data == NULL) {
        return;
    }
    hashTableInit();
    e.data = data;
    e.key = (char*)key;
    LOCK_HASH_TABLE();
    ep = hsearch(e, ENTER);
    UNLOCK_HASH_TABLE();
    if (ep == NULL) {
        fprintf(stderr, "error adding key (%s) to hash table, <%d>: %s\n",
                key, errno, strerror(errno));
        exit(1);
    }  
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



int invokeMethod(JNIEnv *env, RetVal *retval, MethType methType,
                 jobject instObj, const char *className,
                 const char *methName, const char *methSignature, ...)
{
    va_list args;
    jclass cls;
    jmethodID mid;
    const char *str; 
    char returnType;
    
    validateMethodType(methType);
    cls = globalClassReference(className, env);
    mid = methodIdFromClass(className, methName, methSignature, 
                            methType, env);
    if (mid == NULL) {
        (*env)->ExceptionDescribe(env);
        exit(1);
    }
   
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
        CHECK_EXCEPTION_IN_METH_INVOC
        retval->l = jobj;
    }
    else if (returnType == VOID) {
        if (methType == STATIC) {
            (*env)->CallStaticVoidMethodV(env, cls, mid, args);
        }
        else if (methType == INSTANCE) {
            (*env)->CallVoidMethodV(env, instObj, mid, args);
        }
       CHECK_EXCEPTION_IN_METH_INVOC
    }
    else if (returnType == JBOOLEAN) {
        jboolean jbool = 0;
        if (methType == STATIC) {
            jbool = (*env)->CallStaticBooleanMethodV(env, cls, mid, args);
        }
        else if (methType == INSTANCE) {
            jbool = (*env)->CallBooleanMethodV(env, instObj, mid, args);
        }
        CHECK_EXCEPTION_IN_METH_INVOC
        retval->z = jbool;
    }
    else if (returnType == JLONG) {
        jlong jl = -1;
        if (methType == STATIC) {
            jl = (*env)->CallStaticLongMethodV(env, cls, mid, args);
        }
        else if (methType == INSTANCE) {
            jl = (*env)->CallLongMethodV(env, instObj, mid, args);
        }
        CHECK_EXCEPTION_IN_METH_INVOC
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
        CHECK_EXCEPTION_IN_METH_INVOC
        retval->i = ji;
    }
    va_end(args);
    return 0;
}


jobject constructNewObjectOfClass(JNIEnv *env, const char *className, 
                                  const char *ctorSignature, ...)
{
    va_list args;
    jclass cls;
    jmethodID mid; 
    jobject jobj;

    cls = globalClassReference(className, env);
    mid = methodIdFromClass(className, "<init>", ctorSignature, 
                            INSTANCE, env);
    if (mid == NULL) {
        (*env)->ExceptionDescribe(env);
        exit(1);
    } 
    va_start(args, ctorSignature);
    jobj = (*env)->NewObjectV(env, cls, mid, args);
    va_end(args);
    if ((*env)->ExceptionCheck(env)) {
        (*env)->ExceptionDescribe(env);
    }
    return jobj;
}




jmethodID methodIdFromClass(const char *className, const char *methName, 
                            const char *methSignature, MethType methType, 
                            JNIEnv *env)
{
    jclass cls = globalClassReference(className, env);
    jmethodID mid = 0;
    validateMethodType(methType);
    if (methType == STATIC) {
        mid = (*env)->GetStaticMethodID(env, cls, methName, methSignature);
    }
    else if (methType == INSTANCE) {
        mid = (*env)->GetMethodID(env, cls, methName, methSignature);
    }
    return mid;
}


jclass globalClassReference(const char *className, JNIEnv *env)
{
    jclass clsLocalRef;
    jclass cls = searchEntryFromTable(className);
    if (cls) {
        return cls; 
    }

    clsLocalRef = (*env)->FindClass(env,className);
    if (clsLocalRef == NULL) {
        (*env)->ExceptionDescribe(env);
        exit(1);
    }
    cls = (*env)->NewGlobalRef(env, clsLocalRef);
    if (cls == NULL) {
        (*env)->ExceptionDescribe(env);
        exit(1);
    }
    (*env)->DeleteLocalRef(env, clsLocalRef);
    insertEntryIntoTable(className, cls);
    return cls;
}




/**
 * getJNIEnv: A helper function to get the JNIEnv* for the given thread.
 * @param: None.
 * @return The JNIEnv* corresponding to the thread.
 */
JNIEnv* getJNIEnv(void)
{
    char threadID[threadID_SIZE];

    const jsize vmBufLength = 1;
    JavaVM* vmBuf[vmBufLength]; 
    JNIEnv *env;
    jint rv = 0; 
    jint noVMs = 0;

    //Get the threadID and stringize it 
    GET_threadID(pthread_self(), threadID, sizeof(threadID));

    //See if you already have the JNIEnv* cached...
    env = (JNIEnv*)searchEntryFromTable(threadID);
    if (env != NULL) {
        return env; 
    }

    //All right... some serious work required here!
    //1. Initialize the HashTable
    //2. LOCK!
    //3. Check if any JVMs have been created here
    //      Yes: Use it (we should only have 1 VM)
    //      No: Create the JVM
    //4. UNLOCK

    hashTableInit();

    LOCK_HASH_TABLE();

    rv = JNI_GetCreatedJavaVMs(&(vmBuf[0]), vmBufLength, &noVMs);
    if (rv != 0) {
        fprintf(stderr, "JNI_GetCreatedJavaVMs failed with error: %d\n", rv);
        exit(1);
    }

    if (noVMs == 0) {
        //Get the environment variables for initializing the JVM
        char *hadoopClassPath = getenv("CLASSPATH");
        if (hadoopClassPath == NULL) {
            fprintf(stderr, "Environment variable CLASSPATH not set!\n");
            exit(-1);
        } 
        char *hadoopClassPathVMArg = "-Djava.class.path=";
        size_t optHadoopClassPathLen = strlen(hadoopClassPath) + 
          strlen(hadoopClassPathVMArg) + 1;
        char *optHadoopClassPath = malloc(sizeof(char)*optHadoopClassPathLen);
        snprintf(optHadoopClassPath, optHadoopClassPathLen,
        	"%s%s", hadoopClassPathVMArg, hadoopClassPath);

        //Create the VM
        JavaVMInitArgs vm_args;
        JavaVMOption options[1];
        JavaVM *vm;
        
        // User classes
        options[0].optionString = optHadoopClassPath;
        // Print JNI-related messages      
        //options[2].optionString = "-verbose:jni";

        vm_args.version = JNI_VERSION_1_2;
        vm_args.options = options;
        vm_args.nOptions = 1; 
        vm_args.ignoreUnrecognized = 1;

        rv = JNI_CreateJavaVM(&vm, (void*)&env, &vm_args);
        if (rv != 0) {
            fprintf(stderr, "Call to JNI_CreateJavaVM failed "
                    "with error: %d\n", rv);
            exit(1);
        }

        free(optHadoopClassPath);
    }
    else {
        //Attach this thread to the VM
        JavaVM* vm = vmBuf[0];
        rv = (*vm)->AttachCurrentThread(vm, (void*)&env, 0);
        if (rv != 0) {
            fprintf(stderr, "Call to AttachCurrentThread "
                    "failed with error: %d\n", rv);
            exit(1);
        }
    }

    //Save the threadID -> env mapping
    ENTRY e, *ep;
    e.key = threadID;
    e.data = (void*)(env);
    if ((ep = hsearch(e, ENTER)) == NULL) {
        fprintf(stderr, "Call to hsearch(ENTER) failed\n");
        exit(1);
    }

    UNLOCK_HASH_TABLE();

    return env;
}
