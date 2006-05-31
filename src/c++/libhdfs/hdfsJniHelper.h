/**
 * Copyright 2005 The Apache Software Foundation
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

#include <jni.h>
#include <stdio.h>

#include <stdlib.h>
#include <stdarg.h>
#include <search.h>
#include <pthread.h>

pthread_mutex_t hashTableMutex = PTHREAD_MUTEX_INITIALIZER; 

#define LOCK_HASH_TABLE() pthread_mutex_lock(&hashTableMutex)
#define UNLOCK_HASH_TABLE() pthread_mutex_unlock(&hashTableMutex)

#define PATH_SEPARATOR ':'

#define USER_CLASSPATH "/home/y/libexec/hadoop/conf:/home/y/libexec/hadoop/lib/hadoop-0.1.0.jar"

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

/** Denote the method we want to invoke as STATIC or INSTANCE */
typedef enum { 
    STATIC,
    INSTANCE
} MethType;

/** Used for returning an appropriate return value after invoking
 * a method
 */
typedef jvalue RetVal;

/**
 * MAX_HASH_TABLE_ELEM: The maximum no. of entries in the hashtable.
 * It's set to 4096 to account for (classNames + No. of threads)
 */
#define MAX_HASH_TABLE_ELEM 4096

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
   a valid exception reference.
 */
int invokeMethod(JNIEnv *env, RetVal *retval, jthrowable *exc,
                 MethType methType, jobject instObj,
                 const char *className, const char *methName, 
                 const char *methSignature, ...);

/** constructNewObjectOfClass: Invoke a constructor.
 * className: Name of the class
 * ctorSignature: the signature of the constructor "(arg-types)V"
 * env: The JNIEnv pointer
 * exc: If the ctor throws any exception, this will contain the reference
 * Arguments to the ctor must be passed after ctorSignature 
 */
jobject constructNewObjectOfClass(JNIEnv *env, jthrowable *exc, 
                                  const char *className, 
                                  const char *ctorSignature, ...);

void validateMethodType(MethType methType);

jmethodID methodIdFromClass(const char *className, const char *methName, 
                            const char *methSignature, MethType methType, 
                            JNIEnv *env);

jclass globalClassReference(const char *className, JNIEnv *env);

void insertEntryIntoTable(const char *key, void *data);
void *searchEntryFromTable(const char *key);
void hashTableInit();

#define CHECK_EXCEPTION_IN_METH_INVOC {\
    jthrowable _exc_;\
    if((_exc_ = (*env)->ExceptionOccurred(env))) {\
        (*env)->ExceptionDescribe(env);\
        *exc = _exc_;\
        (*env)->ExceptionClear(env);\
        va_end(args);\
        return -1;\
    }\
}

int invokeMethod(JNIEnv *env, RetVal *retval, jthrowable *exc,
                 MethType methType, jobject instObj,
                 const char *className, const char *methName, 
                 const char *methSignature, ...)
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
    if(mid == NULL) {
        (*env)->ExceptionDescribe(env);
        exit(1);
    }
   
    str = methSignature;
    while(*str != ')') str++;
    str++;
    returnType = *str;
    va_start(args, methSignature);
    if (returnType == JOBJECT || returnType == JARRAYOBJECT) {
        jobject jobj;
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
        jboolean jbool;
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
        jlong jl;
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
        jint ji;
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

void validateMethodType(MethType methType)
{
    if (methType != STATIC && methType != INSTANCE) {
        fprintf(stderr,"Unimplemented method type\n");
        exit(1);
    }
    return;
}

jobject constructNewObjectOfClass(JNIEnv *env, jthrowable *exc, 
                                  const char *className, 
                                  const char *ctorSignature, ...)
{
    va_list args;
    jclass cls;
    jmethodID mid; 
    jobject jobj;
    jthrowable _exc;

    cls = globalClassReference(className, env);
    mid = methodIdFromClass(className, "<init>", ctorSignature, 
            INSTANCE, env);
    if(mid == NULL) {
        (*env)->ExceptionDescribe(env);
        exit(1);
    } 
    va_start(args, ctorSignature);
    jobj = (*env)->NewObjectV(env, cls, mid, args);
    va_end(args);
    if((_exc = (*env)->ExceptionOccurred(env))) {
        (*env)->ExceptionDescribe(env);
        *exc = _exc;
        (*env)->ExceptionClear(env);
    }
    return jobj;
}

jmethodID methodIdFromClass(const char *className, const char *methName, 
                            const char *methSignature, MethType methType, 
                            JNIEnv *env)
{
    jclass cls = globalClassReference(className, env);
    jmethodID mid;
    validateMethodType(methType);
    if(methType == STATIC) {
        mid = (*env)->GetStaticMethodID(env, cls, methName, methSignature);
    }
    else if(methType == INSTANCE) {
        mid = (*env)->GetMethodID(env, cls, methName, methSignature);
    }
    return mid;
}

jclass globalClassReference(const char *className, JNIEnv *env)
{
    jclass clsLocalRef;
    jclass cls = searchEntryFromTable(className);
    if(cls) {
        return cls; 
    }

    clsLocalRef = (*env)->FindClass(env,className);
    if(clsLocalRef == NULL) {
        (*env)->ExceptionDescribe(env);
        exit(1);
    }
    cls = (*env)->NewGlobalRef(env, clsLocalRef);
    if(cls == NULL) {
        (*env)->ExceptionDescribe(env);
        exit(1);
    }
    (*env)->DeleteLocalRef(env, clsLocalRef);
    insertEntryIntoTable(className, cls);
    return cls;
}

void insertEntryIntoTable(const char *key, void *data)
{
    ENTRY e, *ep;
    if(key == NULL || data == NULL) {
        return;
    }
    hashTableInit();
    e.data = data;
    e.key = (char*)key;
    LOCK_HASH_TABLE();
    ep = hsearch(e, ENTER);
    UNLOCK_HASH_TABLE();
    if(ep == NULL) {
        fprintf(stderr,"hsearch(ENTER) returned error\n");
        exit(1);
    }  
}

void *searchEntryFromTable(const char *key)
{
    ENTRY e,*ep;
    if(key == NULL) {
        return NULL;
    }
    hashTableInit();
    e.key = (char*)key;
    LOCK_HASH_TABLE();
    ep = hsearch(e, FIND);
    UNLOCK_HASH_TABLE();
    if(ep != NULL) {
        return ep->data;
    }
    return NULL;
}

void hashTableInit()
{
    static int hash_table_inited = 0;
    LOCK_HASH_TABLE();
    if(!hash_table_inited) {
        if (hcreate(MAX_HASH_TABLE_ELEM) == 0) {
            fprintf(stderr,"hcreate returned error\n");
            exit(1);
        } 
        hash_table_inited = 1;
    }  
    UNLOCK_HASH_TABLE();
}

/**
 * vim: ts=4: sw=4: et:
 */

