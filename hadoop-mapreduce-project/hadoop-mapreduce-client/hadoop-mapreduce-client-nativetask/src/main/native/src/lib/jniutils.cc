/*
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

#include "lib/commons.h"
#include "util/StringUtil.h"
#include "util/SyncUtils.h"
#include "lib/jniutils.h"

using namespace NativeTask;

JavaVM * JNU_GetJVM(void) {
  static JavaVM * gJVM = NULL;
  static Lock GJVMLock;
  if (gJVM != NULL) {
    return gJVM;
  }
  {
    ScopeLock<Lock> autolock(GJVMLock);
    if (gJVM == NULL) {
      jint rv = 0;
      jint noVMs = 0;
      rv = JNI_GetCreatedJavaVMs(&gJVM, 1, &noVMs);
      if (rv != 0) {
        THROW_EXCEPTION(NativeTask::HadoopException, "JNI_GetCreatedJavaVMs failed");
      }
      if (noVMs == 0) {
        char *hadoopClassPath = getenv("CLASSPATH");
        if (hadoopClassPath == NULL) {
          THROW_EXCEPTION(NativeTask::HadoopException, "Environment variable CLASSPATH not set!");
          return NULL;
        }
        const char *hadoopClassPathVMArg = "-Djava.class.path=";
        size_t optHadoopClassPathLen = strlen(hadoopClassPath) + strlen(hadoopClassPathVMArg) + 1;
        char *optHadoopClassPath = (char*)malloc(sizeof(char) * optHadoopClassPathLen);
        snprintf(optHadoopClassPath, optHadoopClassPathLen, "%s%s", hadoopClassPathVMArg,
            hadoopClassPath);
        int noArgs = 1;
        JavaVMOption options[noArgs];
        options[0].optionString = optHadoopClassPath;

        // Create the VM
        JavaVMInitArgs vm_args;
        vm_args.version = JNI_VERSION_1_6;
        vm_args.options = options;
        vm_args.nOptions = noArgs;
        vm_args.ignoreUnrecognized = 1;
        JNIEnv * jenv;
        rv = JNI_CreateJavaVM(&gJVM, (void**)&jenv, &vm_args);
        if (rv != 0) {
          THROW_EXCEPTION(NativeTask::HadoopException, "JNI_CreateJavaVM failed");
          return NULL;
        }
        free(optHadoopClassPath);
      }
    }
  }
  return gJVM;
}

JNIEnv* JNU_GetJNIEnv(void) {
  JNIEnv * env;
  jint rv = JNU_GetJVM()->AttachCurrentThread((void **)&env, NULL);
  if (rv != 0) {
    THROW_EXCEPTION(NativeTask::HadoopException, "Call to AttachCurrentThread failed");
  }
  return env;
}

void JNU_AttachCurrentThread() {
  JNU_GetJNIEnv();
}

void JNU_DetachCurrentThread() {
  jint rv = JNU_GetJVM()->DetachCurrentThread();
  if (rv != 0) {
    THROW_EXCEPTION(NativeTask::HadoopException, "Call to DetachCurrentThread failed");
  }
}

void JNU_ThrowByName(JNIEnv *jenv, const char *name, const char *msg) {
  jclass cls = jenv->FindClass(name);
  if (cls != NULL) {
    jenv->ThrowNew(cls, msg);
  }
  jenv->DeleteLocalRef(cls);
}

std::string JNU_ByteArrayToString(JNIEnv * jenv, jbyteArray src) {
  if (NULL != src) {
    jsize len = jenv->GetArrayLength(src);
    std::string ret(len, '\0');
    jenv->GetByteArrayRegion(src, 0, len, (jbyte*)ret.data());
    return ret;
  }
  return std::string();
}
