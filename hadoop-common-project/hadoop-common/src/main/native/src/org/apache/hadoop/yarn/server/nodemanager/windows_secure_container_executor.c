/**
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements. See the NOTICE file distributed with this
* work for additional information regarding copyright ownership. The ASF
* licenses this file to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
* 
* http://www.apache.org/licenses/LICENSE-2.0
* 
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
* WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
* License for the specific language governing permissions and limitations under
* the License.
*/

#include <jni.h>
#include "org_apache_hadoop.h"
#include "windows_secure_container_executor.h"
#include "winutils.h"
#include "file_descriptor.h"

// class of org.apache.hadoop.yarn.server.nodemanager.WindowsSecureContainerExecutor.Native.WinutilsProcessStub
static jclass wps_class = NULL;

/*
 * private static native void initWsceNative();
 *
 * We rely on this function rather than lazy initialization because
 * the lazy approach may have a race if multiple callers try to
 * init at the same time.
 */
JNIEXPORT void JNICALL
Java_org_apache_hadoop_yarn_server_nodemanager_WindowsSecureContainerExecutor_00024Native_initWsceNative(
  JNIEnv *env, jclass clazz) {

#ifdef WINDOWS
  winutils_process_stub_init(env);
  PASS_EXCEPTIONS_GOTO(env, error);
#endif

  return;
error:
  // these are all idempodent and safe to call even if the
  // class wasn't initted yet
#ifdef WINDOWS
  winutils_process_stub_deinit(env);
#endif
}

static jmethodID wps_constructor = NULL;
static jfieldID wps_hProcess = NULL;
static jfieldID wps_hThread = NULL;
static jfieldID wps_disposed = NULL;

extern void throw_ioe(JNIEnv* env, int errnum);

void winutils_process_stub_init(JNIEnv *env) {
  if (wps_class != NULL) return; // already initted

  wps_class = (*env)->FindClass(env, WINUTILS_PROCESS_STUB_CLASS);
  PASS_EXCEPTIONS(env);

  wps_class = (*env)->NewGlobalRef(env, wps_class);
  PASS_EXCEPTIONS(env);

  wps_hProcess = (*env)->GetFieldID(env, wps_class, "hProcess", "J");
  PASS_EXCEPTIONS(env);  

  wps_hThread = (*env)->GetFieldID(env, wps_class, "hThread", "J");
  PASS_EXCEPTIONS(env);  

  wps_disposed = (*env)->GetFieldID(env, wps_class, "disposed", "Z");
  PASS_EXCEPTIONS(env); 

  wps_constructor = (*env)->GetMethodID(env, wps_class, "<init>", "(JJJJJ)V");
  PASS_EXCEPTIONS(env);

  LogDebugMessage(L"winutils_process_stub_init\n");
}

void winutils_process_stub_deinit(JNIEnv *env) {
  if (wps_class != NULL) {
    (*env)->DeleteGlobalRef(env, wps_class);
    wps_class = NULL;
  }
  wps_hProcess = NULL;
  wps_hThread = NULL;
  wps_disposed = NULL;
  wps_constructor = NULL;
  LogDebugMessage(L"winutils_process_stub_deinit\n");
}

jobject winutils_process_stub_create(JNIEnv *env, 
  jlong hProcess, jlong hThread, jlong hStdIn, jlong hStdOut, jlong hStdErr) {
  jobject obj = (*env)->NewObject(env, wps_class, wps_constructor, 
    hProcess, hThread, hStdIn, hStdOut, hStdErr);
  PASS_EXCEPTIONS_RET(env, NULL);

  LogDebugMessage(L"winutils_process_stub_create: %p\n", obj);

  return obj;
}


/*
 * Class:     org.apache.hadoop.yarn.server.nodemanager.WindowsSecureContainerExecutor$Native
 * Method:    createTaskAsUser
 * Signature: (Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String)Lorg/apache/hadoop/io/nativeio/NativeIO$WinutilsProcessStub
 */
JNIEXPORT jobject JNICALL
Java_org_apache_hadoop_yarn_server_nodemanager_WindowsSecureContainerExecutor_00024Native_createTaskAsUser0(JNIEnv* env,
  jclass clazz, jstring jcwd, jstring jjobName, jstring juser, jstring jpidFile, jstring jcmdLine) {
#ifdef UNIX
    THROW(env, "java/io/IOException",
      "The function createTaskAsUser is not supported on Unix");
    return NULL;
#endif

#ifdef WINDOWS
  LPCWSTR cwd = NULL, jobName = NULL, 
    user = NULL, pidFile = NULL, cmdLine = NULL;
  DWORD dwError = ERROR_SUCCESS;
  HANDLE hProcess = INVALID_HANDLE_VALUE, 
     hThread = INVALID_HANDLE_VALUE, 
     hStdIn = INVALID_HANDLE_VALUE, 
     hStdOut = INVALID_HANDLE_VALUE, 
     hStdErr = INVALID_HANDLE_VALUE;
  jobject ret = NULL;

  cwd = (LPCWSTR) (*env)->GetStringChars(env, jcwd, NULL);
  if (!cwd) goto done; // exception was thrown

  jobName = (LPCWSTR) (*env)->GetStringChars(env, jjobName, NULL);
  if (!jobName) goto done; // exception was thrown

  user = (LPCWSTR) (*env)->GetStringChars(env, juser, NULL);
  if (!user) goto done; // exception was thrown

  pidFile = (LPCWSTR) (*env)->GetStringChars(env, jpidFile, NULL);
  if (!pidFile) goto done; // exception was thrown

  cmdLine = (LPCWSTR) (*env)->GetStringChars(env, jcmdLine, NULL);
  if (!cmdLine) goto done; // exception was thrown

  LogDebugMessage(L"createTaskAsUser: jcwd:%s job:%s juser:%s pid:%s cmd:%s\n",
    cwd, jobName, user, pidFile, cmdLine);
  
  dwError = RpcCall_TaskCreateAsUser(cwd, jobName, user, pidFile, cmdLine, 
    &hProcess, &hThread, &hStdIn, &hStdOut, &hStdErr);

  if (ERROR_SUCCESS == dwError) {
    ret = winutils_process_stub_create(env, (jlong) hProcess, (jlong) hThread,
      (jlong) hStdIn, (jlong) hStdOut, (jlong) hStdErr);

    if (NULL == ret) {
      TerminateProcess(hProcess, EXIT_FAILURE);
      CloseHandle(hThread);
      CloseHandle(hProcess);
      CloseHandle(hStdIn);
      CloseHandle(hStdOut);
      CloseHandle(hStdErr);
    }
  }


  if (dwError != ERROR_SUCCESS) {
    throw_ioe (env, dwError);
  }

done:

  if (cwd)     (*env)->ReleaseStringChars(env, jcwd, cwd);
  if (jobName) (*env)->ReleaseStringChars(env, jjobName, jobName);
  if (user)    (*env)->ReleaseStringChars(env, juser, user);
  if (pidFile) (*env)->ReleaseStringChars(env, jpidFile, pidFile);
  if (cmdLine) (*env)->ReleaseStringChars(env, jcmdLine, cmdLine);

  return ret;
  
#endif
}

/*
 * Class:     org.apache.hadoop.yarn.server.nodemanager.WindowsSecureContainerExecutor$Native$Elevated
 * Method:    elevatedKillTaskImpl
 * Signature: (Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V
 */
JNIEXPORT void JNICALL
Java_org_apache_hadoop_yarn_server_nodemanager_WindowsSecureContainerExecutor_00024Native_00024Elevated_elevatedKillTaskImpl(JNIEnv* env,
  jclass clazz, jstring jtask_name) {
#ifdef UNIX
    THROW(env, "java/io/IOException",
      "The function elevatedSetOwner0 is not supported on Unix");
    return;
#endif

#ifdef WINDOWS

  LPCWSTR task_name = NULL;
  DWORD dwError;

  task_name = (LPCWSTR) (*env)->GetStringChars(env, jtask_name, NULL);
  if (!task_name) goto done; // exception was thrown


  dwError = RpcCall_WinutilsKillTask(task_name);

  if (dwError != ERROR_SUCCESS) {
    throw_ioe (env, dwError);
  }

done:
  if (task_name)     (*env)->ReleaseStringChars(env, jtask_name, task_name);

#endif

}


/*
 * Class:     org.apache.hadoop.yarn.server.nodemanager.WindowsSecureContainerExecutor$Native$Elevated
 * Method:    elevatedChownImpl
 * Signature: (Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V
 */
JNIEXPORT void JNICALL
Java_org_apache_hadoop_yarn_server_nodemanager_WindowsSecureContainerExecutor_00024Native_00024Elevated_elevatedChownImpl(JNIEnv* env,
  jclass clazz, jstring jpath, jstring juser, jstring jgroup) {
#ifdef UNIX
    THROW(env, "java/io/IOException",
      "The function elevatedSetOwner0 is not supported on Unix");
    return;
#endif

#ifdef WINDOWS

  LPCWSTR path = NULL, user = NULL, group = NULL;
  DWORD dwError;

  path = (LPCWSTR) (*env)->GetStringChars(env, jpath, NULL);
  if (!path) goto done; // exception was thrown

  if (juser) {
    user = (LPCWSTR) (*env)->GetStringChars(env, juser, NULL);
    if (!user) goto done; // exception was thrown
  }

  if (jgroup) {
    group = (LPCWSTR) (*env)->GetStringChars(env, jgroup, NULL);
    if (!group) goto done; // exception was thrown
  }

  dwError = RpcCall_WinutilsChown(path, user, group);

  if (dwError != ERROR_SUCCESS) {
    throw_ioe (env, dwError);
  }

done:
  if (path)     (*env)->ReleaseStringChars(env, jpath, path);
  if (user)     (*env)->ReleaseStringChars(env, juser, user);
  if (group)     (*env)->ReleaseStringChars(env, jgroup, group);

#endif

}


/*
 * Class:     org.apache.hadoop.yarn.server.nodemanager.WindowsSecureContainerExecutor$Native$Elevated
 * Method:    elevatedMkDirImpl
 * Signature: (Ljava/lang/String;)V
 */
JNIEXPORT void JNICALL
Java_org_apache_hadoop_yarn_server_nodemanager_WindowsSecureContainerExecutor_00024Native_00024Elevated_elevatedMkDirImpl(JNIEnv* env,
  jclass clazz, jstring jpath) {
#ifdef UNIX
    THROW(env, "java/io/IOException",
      "The function elevatedMkDirImpl is not supported on Unix");
    return;
#endif

#ifdef WINDOWS

  LPCWSTR path = NULL, user = NULL, group = NULL;
  DWORD dwError;

  path = (LPCWSTR) (*env)->GetStringChars(env, jpath, NULL);
  if (!path) goto done; // exception was thrown

  dwError = RpcCall_WinutilsMkDir(path);

  if (dwError != ERROR_SUCCESS) {
    throw_ioe (env, dwError);
  }

done:
  if (path)     (*env)->ReleaseStringChars(env, jpath, path);

#endif

}


/*
 * Class:     org.apache.hadoop.yarn.server.nodemanager.WindowsSecureContainerExecutor$Native$Elevated
 * Method:    elevatedChmodImpl
 * Signature: (Ljava/lang/String;I)V
 */
JNIEXPORT void JNICALL
Java_org_apache_hadoop_yarn_server_nodemanager_WindowsSecureContainerExecutor_00024Native_00024Elevated_elevatedChmodImpl(JNIEnv* env,
  jclass clazz, jstring jpath, jint jmode) {
#ifdef UNIX
    THROW(env, "java/io/IOException",
      "The function elevatedChmodImpl is not supported on Unix");
    return;
#endif

#ifdef WINDOWS

  LPCWSTR path = NULL;
  DWORD dwError;

  path = (LPCWSTR) (*env)->GetStringChars(env, jpath, NULL);
  if (!path) goto done; // exception was thrown

  dwError = RpcCall_WinutilsChmod(path, (int) jmode);

  if (dwError != ERROR_SUCCESS) {
    throw_ioe (env, dwError);
  }

done:
  if (path)     (*env)->ReleaseStringChars(env, jpath, path);

#endif

}


/*
 * Class:     org.apache.hadoop.yarn.server.nodemanager.WindowsSecureContainerExecutor$Native$Elevated
 * Method:    elevatedCopyImpl
 * Signature: (I;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Z)V
 */
JNIEXPORT void JNICALL
Java_org_apache_hadoop_yarn_server_nodemanager_WindowsSecureContainerExecutor_00024Native_00024Elevated_elevatedCopyImpl(JNIEnv* env,
  jclass clazz, jint joperation, jstring jsourcePath, jstring jdestinationPath, jboolean replaceExisting) {
#ifdef UNIX
    THROW(env, "java/io/IOException",
      "The function elevatedCopyImpl is not supported on Unix");
    return;
#endif

#ifdef WINDOWS

  LPCWSTR sourcePath = NULL, destinationPath = NULL;
  DWORD dwError;

  sourcePath = (LPCWSTR) (*env)->GetStringChars(env, jsourcePath, NULL);
  if (!sourcePath) goto done; // exception was thrown

  destinationPath = (LPCWSTR) (*env)->GetStringChars(env, jdestinationPath, NULL);
  if (!destinationPath) goto done; // exception was thrown

  dwError = RpcCall_WinutilsMoveFile((INT) joperation, sourcePath, destinationPath, (BOOL) replaceExisting);

  if (dwError != ERROR_SUCCESS) {
    throw_ioe (env, dwError);
  }

done:
  if (sourcePath)     (*env)->ReleaseStringChars(env, jsourcePath, sourcePath);
  if (destinationPath)     (*env)->ReleaseStringChars(env, jdestinationPath, destinationPath);
#endif
}

/*
 * Class:     org.apache.hadoop.yarn.server.nodemanager.WindowsSecureContainerExecutor$Native$Elevated
 * Method:    elevatedCreateImpl
 * Signature: (Ljava/lang/String;J;J;J;J)J
 */
JNIEXPORT jlong JNICALL
Java_org_apache_hadoop_yarn_server_nodemanager_WindowsSecureContainerExecutor_00024Native_00024Elevated_elevatedCreateImpl(JNIEnv* env,
  jclass clazz, jstring jpath, jlong jdesired_access, jlong jshare_mode, jlong jcreation_disposition, jlong jflags) {
#ifdef UNIX
    THROW(env, "java/io/IOException",
      "The function elevatedCreateImpl is not supported on Unix");
    return 0;
#endif

#ifdef WINDOWS

  LPCWSTR path = NULL;
  DWORD dwError;
  HANDLE hFile = INVALID_HANDLE_VALUE;

  path = (LPCWSTR) (*env)->GetStringChars(env, jpath, NULL);
  if (!path) goto done; // exception was thrown

  dwError = RpcCall_WinutilsCreateFile(path, 
    (DWORD) jdesired_access, (DWORD) jshare_mode, (DWORD) jcreation_disposition, (DWORD) jflags,
    &hFile);

  if (dwError != ERROR_SUCCESS) {
    throw_ioe (env, dwError);
  }

done:
  if (path)     (*env)->ReleaseStringChars(env, jpath, path);
  return (jlong) hFile;
#endif
}

/*
 * Class:     org.apache.hadoop.yarn.server.nodemanager.WindowsSecureContainerExecutor$Native$Elevated
 * Method:    elevatedDeletePathImpl
 * Signature: (Ljava/lang/String;Z)Z
 */
JNIEXPORT jboolean JNICALL
Java_org_apache_hadoop_yarn_server_nodemanager_WindowsSecureContainerExecutor_00024Native_00024Elevated_elevatedDeletePathImpl(JNIEnv* env,
  jclass clazz, jstring jpath, jboolean jIsDir) {
#ifdef UNIX
    THROW(env, "java/io/IOException",
      "The function elevatedDeleteFileImpl is not supported on Unix");
    return  JNI_FALSE;
#endif

#ifdef WINDOWS

  LPCWSTR path = NULL;
  DWORD dwError;
  BOOL deleted = FALSE;

  path = (LPCWSTR) (*env)->GetStringChars(env, jpath, NULL);
  if (!path) goto done; // exception was thrown

  dwError = RpcCall_WinutilsDeletePath(path, (BOOL) jIsDir, &deleted);

  if (dwError != ERROR_SUCCESS) {
    throw_ioe (env, dwError);
  }

done:
  if (path)     (*env)->ReleaseStringChars(env, jpath, path);
  return (jboolean) deleted;
#endif
}




/*
 * native void destroy();
 *
 * The "00024" in the function name is an artifact of how JNI encodes
 * special characters. U+0024 is '$'.
 */
JNIEXPORT void JNICALL 
Java_org_apache_hadoop_yarn_server_nodemanager_WindowsSecureContainerExecutor_00024Native_00024WinutilsProcessStub_destroy(
  JNIEnv *env, jobject objSelf) {

  HANDLE hProcess = (HANDLE)(*env)->GetLongField(env, objSelf, wps_hProcess);
  LogDebugMessage(L"TerminateProcess: %x\n", hProcess);
  TerminateProcess(hProcess, EXIT_FAILURE);
}

/*
 * native void waitFor();
 *
 * The "00024" in the function name is an artifact of how JNI encodes
 * special characters. U+0024 is '$'.
 */
JNIEXPORT void JNICALL 
Java_org_apache_hadoop_yarn_server_nodemanager_WindowsSecureContainerExecutor_00024Native_00024WinutilsProcessStub_waitFor(
  JNIEnv *env, jobject objSelf) {

  HANDLE hProcess = (HANDLE)(*env)->GetLongField(env, objSelf, wps_hProcess);
  LogDebugMessage(L"WaitForSingleObject: %x\n", hProcess);
  WaitForSingleObject(hProcess, INFINITE);
}



/*
 * native void resume();
 *
 * The "00024" in the function name is an artifact of how JNI encodes
 * special characters. U+0024 is '$'.
 */
JNIEXPORT void JNICALL 
Java_org_apache_hadoop_yarn_server_nodemanager_WindowsSecureContainerExecutor_00024Native_00024WinutilsProcessStub_resume(
  JNIEnv *env, jobject objSelf) {

  DWORD dwError;
  HANDLE hThread = (HANDLE)(*env)->GetLongField(env, objSelf, wps_hThread);
  if (-1 == ResumeThread(hThread)) {
    dwError = GetLastError();
    LogDebugMessage(L"ResumeThread: %x error:%d\n", hThread, dwError);
    throw_ioe(env, dwError);
  }
}

/*
 * native int exitValue();
 *
 * The "00024" in the function name is an artifact of how JNI encodes
 * special characters. U+0024 is '$'.
 */
JNIEXPORT jint JNICALL 
Java_org_apache_hadoop_yarn_server_nodemanager_WindowsSecureContainerExecutor_00024Native_00024WinutilsProcessStub_exitValue(
  JNIEnv *env, jobject objSelf) {

  DWORD exitCode;
  DWORD dwError;
  HANDLE hProcess = (HANDLE)(*env)->GetLongField(env, objSelf, wps_hProcess);
  if (!GetExitCodeProcess(hProcess, &exitCode)) {
    dwError = GetLastError();
    throw_ioe(env, dwError);
    return dwError; // exception was thrown, return value doesn't really matter
  }
  LogDebugMessage(L"GetExitCodeProcess: %x :%d\n", hProcess, exitCode);
  
  return exitCode;
}


/*
 * native void dispose();
 *
 * The "00024" in the function name is an artifact of how JNI encodes
 * special characters. U+0024 is '$'.
 */
JNIEXPORT void JNICALL 
Java_org_apache_hadoop_yarn_server_nodemanager_WindowsSecureContainerExecutor_00024Native_00024WinutilsProcessStub_dispose(
  JNIEnv *env, jobject objSelf) {

  HANDLE hProcess = INVALID_HANDLE_VALUE, 
         hThread  = INVALID_HANDLE_VALUE;

  jboolean disposed = (*env)->GetBooleanField(env, objSelf, wps_disposed);

  if (JNI_TRUE != disposed) {
    hProcess = (HANDLE)(*env)->GetLongField(env, objSelf, wps_hProcess);
    hThread = (HANDLE)(*env)->GetLongField(env, objSelf, wps_hThread);

    CloseHandle(hProcess);
    CloseHandle(hThread);
    (*env)->SetBooleanField(env, objSelf, wps_disposed, JNI_TRUE);
    LogDebugMessage(L"disposed: %p\n", objSelf);
  }
}


/*
 * native static FileDescriptor getFileDescriptorFromHandle(long handle);
 *
 * The "00024" in the function name is an artifact of how JNI encodes
 * special characters. U+0024 is '$'.
 */
JNIEXPORT jobject JNICALL 
Java_org_apache_hadoop_yarn_server_nodemanager_WindowsSecureContainerExecutor_00024Native_00024WinutilsProcessStub_getFileDescriptorFromHandle(
  JNIEnv *env, jclass klass, jlong handle) {

  LogDebugMessage(L"getFileDescriptorFromHandle: %x\n", handle);
  return fd_create(env, (long) handle);
}

