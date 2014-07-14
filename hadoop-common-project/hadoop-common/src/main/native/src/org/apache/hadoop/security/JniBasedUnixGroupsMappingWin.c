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
#include "org_apache_hadoop.h"
#include "org_apache_hadoop_security_JniBasedUnixGroupsMapping.h"

#include <assert.h>
#include <Windows.h>
#include "winutils.h"

static jobjectArray emptyGroups = NULL;

JNIEXPORT void JNICALL
Java_org_apache_hadoop_security_JniBasedUnixGroupsMapping_anchorNative(
JNIEnv *env, jclass clazz)
{
  // no-op until full port of HADOOP-9439 to Windows is available
}

/*
 * Throw a java.IO.IOException, generating the message from errno.
 */
static void throw_ioexception(JNIEnv* env, DWORD errnum)
{
  DWORD len = 0;
  LPSTR buffer = NULL;
  const char* message = NULL;

  len = FormatMessageA(
    FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM,
    NULL, *(DWORD*) (&errnum), // reinterpret cast
    MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
    buffer, 0, NULL);

  if (len > 0)
  {
    message = buffer;
  }
  else
  {
    message = "Unknown error.";
  }

  THROW(env, "java/io/IOException", message);

  LocalFree(buffer);

  return;
}

JNIEXPORT jobjectArray JNICALL 
Java_org_apache_hadoop_security_JniBasedUnixGroupsMapping_getGroupsForUser
(JNIEnv *env, jclass clazz, jstring juser) {
  const WCHAR *user = NULL;
  jobjectArray jgroups = NULL;
  DWORD dwRtnCode = ERROR_SUCCESS;

  LPLOCALGROUP_USERS_INFO_0 groups = NULL;
  LPLOCALGROUP_USERS_INFO_0 tmpGroups = NULL;
  DWORD ngroups = 0;

  int i;

  if (emptyGroups == NULL) {
    jobjectArray lEmptyGroups = (jobjectArray)(*env)->NewObjectArray(env, 0,
            (*env)->FindClass(env, "java/lang/String"), NULL);
    if (lEmptyGroups == NULL) {
      goto cleanup;
    }
    emptyGroups = (*env)->NewGlobalRef(env, lEmptyGroups);
    if (emptyGroups == NULL) {
      goto cleanup;
    }
  }
  user = (*env)->GetStringChars(env, juser, NULL);
  if (user == NULL) {
    THROW(env, "java/lang/OutOfMemoryError", "Couldn't allocate memory for user buffer");
    goto cleanup;
  }

  dwRtnCode = GetLocalGroupsForUser(user, &groups, &ngroups);
  if (dwRtnCode != ERROR_SUCCESS) {
    throw_ioexception(env, dwRtnCode);
    goto cleanup;
  }

  jgroups = (jobjectArray)(*env)->NewObjectArray(env, ngroups, 
            (*env)->FindClass(env, "java/lang/String"), NULL);
  if (jgroups == NULL) {
    THROW(env, "java/lang/OutOfMemoryError", "Couldn't allocate memory for group buffer");
    goto cleanup; 
  }

  // use a tmp pointer to iterate over groups and keep the original pointer
  // for memory deallocation
  tmpGroups = groups;

  // fill the output string array
  for (i = 0; i < ngroups; i++) {
    jsize groupStringLen = (jsize)wcslen(tmpGroups->lgrui0_name);
    jstring jgrp = (*env)->NewString(env, tmpGroups->lgrui0_name, groupStringLen);
    if (jgrp == NULL) {
      THROW(env, "java/lang/OutOfMemoryError", "Couldn't allocate memory for groups buffer");
      goto cleanup;
    }
    (*env)->SetObjectArrayElement(env, jgroups, i, jgrp);
    // move on to the next group
    tmpGroups++;
  }

cleanup:
  if (groups != NULL) NetApiBufferFree(groups);

  if (user != NULL) {
    (*env)->ReleaseStringChars(env, juser, user);
  }

  if (dwRtnCode == ERROR_SUCCESS) {
    return jgroups;
  } else {
    return emptyGroups;
  }
}
