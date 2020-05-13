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
#include <jni.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <grp.h>
#include <stdio.h>
#include <pwd.h>
#include <string.h>

#include "exception.h"
#include "org_apache_hadoop_security_JniBasedUnixGroupsMapping.h"
#include "org_apache_hadoop.h"
#include "hadoop_group_info.h"
#include "hadoop_user_info.h"

static jmethodID g_log_error_method;

static jclass g_string_clazz;

extern jobject pw_lock_object;

JNIEXPORT void JNICALL
Java_org_apache_hadoop_security_JniBasedUnixGroupsMapping_anchorNative(
JNIEnv *env, jclass clazz)
{
  jobject string_clazz;

  g_log_error_method = (*env)->GetStaticMethodID(env, clazz, "logError",
        "(ILjava/lang/String;)V");
  if (!g_log_error_method) {
    return; // an exception has been raised
  }
  string_clazz = (*env)->FindClass(env, "java/lang/String");
  if (!string_clazz) {
    return; // an exception has been raised
  }
  g_string_clazz = (*env)->NewGlobalRef(env, string_clazz);
  if (!g_string_clazz) {
    jthrowable jthr = newRuntimeException(env,
        "JniBasedUnixGroupsMapping#anchorNative: failed to make "
        "a global reference to the java.lang.String class\n");
    (*env)->Throw(env, jthr);
    return;
  }
}

/**
 * Log an error about a failure to look up a group ID
 *
 * @param env   The JNI environment
 * @param clazz JniBasedUnixGroupsMapping class
 * @param gid   The gid we failed to look up
 * @param ret   Failure code
 */
static void logError(JNIEnv *env, jclass clazz, jint gid, int ret)
{
  jstring error_msg;

  error_msg = (*env)->NewStringUTF(env, terror(ret));
  if (!error_msg) {
    (*env)->ExceptionClear(env);
    return;
  }
  (*env)->CallStaticVoidMethod(env, clazz, g_log_error_method, gid, error_msg);
  if ((*env)->ExceptionCheck(env)) {
    (*env)->ExceptionClear(env);
    return;
  }
  (*env)->DeleteLocalRef(env, error_msg);
}

JNIEXPORT jobjectArray JNICALL 
Java_org_apache_hadoop_security_JniBasedUnixGroupsMapping_getGroupsForUser 
(JNIEnv *env, jclass clazz, jstring jusername)
{
  const char *username = NULL;
  struct hadoop_user_info *uinfo = NULL;
  struct hadoop_group_info *ginfo = NULL;
  jstring jgroupname = NULL;
  int i, ret, nvalid;
  int pw_lock_locked = 0;
  jobjectArray jgroups = NULL, jnewgroups = NULL;

  if (pw_lock_object != NULL) {
    if ((*env)->MonitorEnter(env, pw_lock_object) != JNI_OK) {
      goto done; // exception thrown
    }
    pw_lock_locked = 1;
  }
  username = (*env)->GetStringUTFChars(env, jusername, NULL);
  if (username == NULL) {
    goto done; // exception thrown
  }
  uinfo = hadoop_user_info_alloc();
  if (!uinfo) {
    THROW(env, "java/lang/OutOfMemoryError", NULL);
    goto done;
  }
  ret = hadoop_user_info_fetch(uinfo, username);
  if (ret) {
    if (ret == ENOENT) {
      jgroups = (*env)->NewObjectArray(env, 0, g_string_clazz, NULL);
    } else { // handle other errors
      (*env)->Throw(env, newRuntimeException(env,
          "getgrouplist: error looking up user. %d (%s)", ret, terror(ret)));
    }
    goto done;
  }

  ginfo = hadoop_group_info_alloc();
  if (!ginfo) {
    THROW(env, "java/lang/OutOfMemoryError", NULL);
    goto done;
  }
  ret = hadoop_user_info_getgroups(uinfo);
  if (ret) {
    if (ret == ENOMEM) {
      THROW(env, "java/lang/OutOfMemoryError", NULL);
    } else {
      (*env)->Throw(env, newRuntimeException(env,
          "getgrouplist: error looking up group. %d (%s)", ret, terror(ret)));
    }
    goto done;
  }
  jgroups = (jobjectArray)(*env)->NewObjectArray(env, uinfo->num_gids,
                                                 g_string_clazz, NULL);
  for (nvalid = 0, i = 0; i < uinfo->num_gids; i++) {
    ret = hadoop_group_info_fetch(ginfo, uinfo->gids[i]);
    if (ret) {
      logError(env, clazz, uinfo->gids[i], ret);
    } else {
      jgroupname = (*env)->NewStringUTF(env, ginfo->group.gr_name);
      if (!jgroupname) { // exception raised
        (*env)->DeleteLocalRef(env, jgroups);
        jgroups = NULL;
        goto done;
      }
      (*env)->SetObjectArrayElement(env, jgroups, nvalid++, jgroupname);
      // We delete the local reference once the element is in the array.
      // This is OK because the array has a reference to it.
      // Technically JNI only mandates that the JVM allow up to 16 local
      // references at a time  (though many JVMs allow more than that.)
      (*env)->DeleteLocalRef(env, jgroupname);
    }
  }
  if (nvalid != uinfo->num_gids) {
    // If some group names could not be looked up, allocate a smaller array
    // with just the entries that could be resolved.  Java has no equivalent to
    // realloc, so we have to do this manually.
    jnewgroups = (jobjectArray)(*env)->NewObjectArray(env, nvalid,
            (*env)->FindClass(env, "java/lang/String"), NULL);
    if (!jnewgroups) { // exception raised
      (*env)->DeleteLocalRef(env, jgroups);
      jgroups = NULL;
      goto done;
    }
    for (i = 0; i < nvalid; i++) {
      jgroupname = (*env)->GetObjectArrayElement(env, jgroups, i);
      (*env)->SetObjectArrayElement(env, jnewgroups, i, jgroupname);
      (*env)->DeleteLocalRef(env, jgroupname);
    }
    (*env)->DeleteLocalRef(env, jgroups);
    jgroups = jnewgroups;
  }

done:
  if (pw_lock_locked) {
    (*env)->MonitorExit(env, pw_lock_object);
  }
  if (username) {
    (*env)->ReleaseStringUTFChars(env, jusername, username);
  }
  if (uinfo) {
    hadoop_user_info_free(uinfo);
  }
  if (ginfo) {
    hadoop_group_info_free(ginfo);
  }
  return jgroups;
}
