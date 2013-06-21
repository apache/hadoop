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

// get the autoconf settings
#include <assert.h>
#include <jni.h>
#include <stdlib.h>
#include <string.h>

#include "org_apache_hadoop.h"
#include "org_apache_hadoop_io_nativeio_NativeIO.h"
#include "file_descriptor.h"

#ifdef UNIX
#include "config.h"
#include <grp.h>
#include <pwd.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/syscall.h>
#include "errno_enum.h"
#endif

#ifdef WINDOWS
#include <Windows.h>
#include "winutils.h"
#endif


// the NativeIO$Stat inner class and its constructor
static jclass stat_clazz;
static jmethodID stat_ctor;

// the NativeIOException class and its constructor
static jclass nioe_clazz;
static jmethodID nioe_ctor;

// Internal functions
static void throw_ioe(JNIEnv* env, int errnum);
#ifdef UNIX
static ssize_t get_pw_buflen();
#endif

#ifdef UNIX
static void stat_init(JNIEnv *env) {
  // Init Stat
  jclass clazz = (*env)->FindClass(env,
    "org/apache/hadoop/io/nativeio/NativeIO$POSIX$Stat");
  PASS_EXCEPTIONS(env);
  stat_clazz = (*env)->NewGlobalRef(env, clazz);
  stat_ctor = (*env)->GetMethodID(env, stat_clazz, "<init>",
    "(Ljava/lang/String;I)V");
}

static void stat_deinit(JNIEnv *env) {
  if (stat_clazz != NULL) {  
    (*env)->DeleteGlobalRef(env, stat_clazz);
    stat_clazz = NULL;
  }
}
#endif

static void nioe_init(JNIEnv *env) {
  // Init NativeIOException
  nioe_clazz = (*env)->FindClass(
    env, "org/apache/hadoop/io/nativeio/NativeIOException");
  PASS_EXCEPTIONS(env);

  nioe_clazz = (*env)->NewGlobalRef(env, nioe_clazz);

#ifdef UNIX
  nioe_ctor = (*env)->GetMethodID(env, nioe_clazz, "<init>",
    "(Ljava/lang/String;Lorg/apache/hadoop/io/nativeio/Errno;)V");
#endif

#ifdef WINDOWS
  nioe_ctor = (*env)->GetMethodID(env, nioe_clazz, "<init>",
    "(Ljava/lang/String;I)V");
#endif
}

static void nioe_deinit(JNIEnv *env) {
  if (nioe_clazz != NULL) {
    (*env)->DeleteGlobalRef(env, nioe_clazz);
    nioe_clazz = NULL;
  }
  nioe_ctor = NULL;
}

/*
 * private static native void initNative();
 *
 * We rely on this function rather than lazy initialization because
 * the lazy approach may have a race if multiple callers try to
 * init at the same time.
 */
JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_nativeio_NativeIO_initNative(
	JNIEnv *env, jclass clazz) {
  nioe_init(env);
  PASS_EXCEPTIONS_GOTO(env, error);
  fd_init(env);
  PASS_EXCEPTIONS_GOTO(env, error);
#ifdef UNIX
  stat_init(env);
  PASS_EXCEPTIONS_GOTO(env, error);
  errno_enum_init(env);
  PASS_EXCEPTIONS_GOTO(env, error);
#endif
  return;
error:
  // these are all idempodent and safe to call even if the
  // class wasn't initted yet
  nioe_deinit(env);
  fd_deinit(env);
#ifdef UNIX
  stat_deinit(env);
  errno_enum_deinit(env);
#endif
}

/*
 * Class:     org_apache_hadoop_io_nativeio_NativeIO_POSIX
 * Method:    fstat
 * Signature: (Ljava/io/FileDescriptor;)Lorg/apache/hadoop/io/nativeio/NativeIO$POSIX$Stat;
 */
JNIEXPORT jobject JNICALL Java_org_apache_hadoop_io_nativeio_NativeIO_00024POSIX_fstat
  (JNIEnv *env, jclass clazz, jobject fd_object)
{
#ifdef UNIX
  jobject ret = NULL;
  char *pw_buf = NULL;

  int fd = fd_get(env, fd_object);
  PASS_EXCEPTIONS_GOTO(env, cleanup);

  struct stat s;
  int rc = fstat(fd, &s);
  if (rc != 0) {
    throw_ioe(env, errno);
    goto cleanup;
  }

  size_t pw_buflen = get_pw_buflen();
  if ((pw_buf = malloc(pw_buflen)) == NULL) {
    THROW(env, "java/lang/OutOfMemoryError", "Couldn't allocate memory for pw buffer");
    goto cleanup;
  }

  // Grab username
  struct passwd pwd, *pwdp;
  while ((rc = getpwuid_r(s.st_uid, &pwd, pw_buf, pw_buflen, &pwdp)) != 0) {
    if (rc != ERANGE) {
      throw_ioe(env, rc);
      goto cleanup;
    }
    free(pw_buf);
    pw_buflen *= 2;
    if ((pw_buf = malloc(pw_buflen)) == NULL) {
      THROW(env, "java/lang/OutOfMemoryError", "Couldn't allocate memory for pw buffer");
      goto cleanup;
    }
  }
  if (rc == 0 && pwdp == NULL) {
    throw_ioe(env, ENOENT);
    goto cleanup;
  }

  jstring jstr_username = (*env)->NewStringUTF(env, pwd.pw_name);
  if (jstr_username == NULL) goto cleanup;

  // Construct result
  ret = (*env)->NewObject(env, stat_clazz, stat_ctor,
    jstr_username, s.st_mode);

cleanup:
  if (pw_buf != NULL) free(pw_buf);
  return ret;
#endif

#ifdef WINDOWS
  THROW(env, "java/io/IOException",
    "The function POSIX.fstat() is not supported on Windows");
  return NULL;
#endif
}

/**
 * public static native void posix_fadvise(
 *   FileDescriptor fd, long offset, long len, int flags);
 */
JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_nativeio_NativeIO_posix_1fadvise(
  JNIEnv *env, jclass clazz,
  jobject fd_object, jlong offset, jlong len, jint flags)
{
#ifndef HAVE_POSIX_FADVISE
  THROW(env, "java/lang/UnsupportedOperationException",
        "fadvise support not available");
#else
  int fd = fd_get(env, fd_object);
  PASS_EXCEPTIONS(env);

  int err = 0;
  if ((err = posix_fadvise(fd, (off_t)offset, (off_t)len, flags))) {
    throw_ioe(env, err);
  }
#endif
}

#if defined(HAVE_SYNC_FILE_RANGE)
#  define my_sync_file_range sync_file_range
#elif defined(SYS_sync_file_range)
// RHEL 5 kernels have sync_file_range support, but the glibc
// included does not have the library function. We can
// still call it directly, and if it's not supported by the
// kernel, we'd get ENOSYS. See RedHat Bugzilla #518581
static int manual_sync_file_range (int fd, __off64_t from, __off64_t to, unsigned int flags)
{
#ifdef __x86_64__
  return syscall( SYS_sync_file_range, fd, from, to, flags);
#else
  return syscall (SYS_sync_file_range, fd,
    __LONG_LONG_PAIR ((long) (from >> 32), (long) from),
    __LONG_LONG_PAIR ((long) (to >> 32), (long) to),
    flags);
#endif
}
#define my_sync_file_range manual_sync_file_range
#endif

/**
 * public static native void sync_file_range(
 *   FileDescriptor fd, long offset, long len, int flags);
 */
JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_nativeio_NativeIO_sync_1file_1range(
  JNIEnv *env, jclass clazz,
  jobject fd_object, jlong offset, jlong len, jint flags)
{
#ifndef my_sync_file_range
  THROW(env, "java/lang/UnsupportedOperationException",
        "sync_file_range support not available");
#else
  int fd = fd_get(env, fd_object);
  PASS_EXCEPTIONS(env);

  if (my_sync_file_range(fd, (off_t)offset, (off_t)len, flags)) {
    if (errno == ENOSYS) {
      // we know the syscall number, but it's not compiled
      // into the running kernel
      THROW(env, "java/lang/UnsupportedOperationException",
            "sync_file_range kernel support not available");
      return;
    } else {
      throw_ioe(env, errno);
    }
  }
#endif
}

/*
 * Class:     org_apache_hadoop_io_nativeio_NativeIO_POSIX
 * Method:    open
 * Signature: (Ljava/lang/String;II)Ljava/io/FileDescriptor;
 */
JNIEXPORT jobject JNICALL Java_org_apache_hadoop_io_nativeio_NativeIO_00024POSIX_open
  (JNIEnv *env, jclass clazz, jstring j_path, jint flags, jint mode)
{
#ifdef UNIX
  jobject ret = NULL;

  const char *path = (*env)->GetStringUTFChars(env, j_path, NULL);
  if (path == NULL) goto cleanup; // JVM throws Exception for us

  int fd;  
  if (flags & O_CREAT) {
    fd = open(path, flags, mode);
  } else {
    fd = open(path, flags);
  }

  if (fd == -1) {
    throw_ioe(env, errno);
    goto cleanup;
  }

  ret = fd_create(env, fd);

cleanup:
  if (path != NULL) {
    (*env)->ReleaseStringUTFChars(env, j_path, path);
  }
  return ret;
#endif

#ifdef WINDOWS
  THROW(env, "java/io/IOException",
    "The function POSIX.open() is not supported on Windows");
  return NULL;
#endif
}

/*
 * Class:     org_apache_hadoop_io_nativeio_NativeIO_Windows
 * Method:    createFile
 * Signature: (Ljava/lang/String;JJJ)Ljava/io/FileDescriptor;
 */
JNIEXPORT jobject JNICALL Java_org_apache_hadoop_io_nativeio_NativeIO_00024Windows_createFile
  (JNIEnv *env, jclass clazz, jstring j_path,
  jlong desiredAccess, jlong shareMode, jlong creationDisposition)
{
#ifdef UNIX
  THROW(env, "java/io/IOException",
    "The function Windows.createFile() is not supported on Unix");
  return NULL;
#endif

#ifdef WINDOWS
  DWORD dwRtnCode = ERROR_SUCCESS;
  BOOL isSymlink = FALSE;
  BOOL isJunction = FALSE;
  DWORD dwFlagsAndAttributes = FILE_ATTRIBUTE_NORMAL | FILE_FLAG_BACKUP_SEMANTICS;
  jobject ret = (jobject) NULL;
  HANDLE hFile = INVALID_HANDLE_VALUE;
  WCHAR *path = (WCHAR *) (*env)->GetStringChars(env, j_path, (jboolean*)NULL);
  if (path == NULL) goto cleanup;

  // Set the flag for a symbolic link or a junctions point only when it exists.
  // According to MSDN if the call to CreateFile() function creates a file,
  // there is no change in behavior. So we do not throw if no file is found.
  //
  dwRtnCode = SymbolicLinkCheck(path, &isSymlink);
  if (dwRtnCode != ERROR_SUCCESS && dwRtnCode != ERROR_FILE_NOT_FOUND) {
    throw_ioe(env, dwRtnCode);
    goto cleanup;
  }
  dwRtnCode = JunctionPointCheck(path, &isJunction);
  if (dwRtnCode != ERROR_SUCCESS && dwRtnCode != ERROR_FILE_NOT_FOUND) {
    throw_ioe(env, dwRtnCode);
    goto cleanup;
  }
  if (isSymlink || isJunction)
    dwFlagsAndAttributes |= FILE_FLAG_OPEN_REPARSE_POINT;

  hFile = CreateFile(path,
    (DWORD) desiredAccess,
    (DWORD) shareMode,
    (LPSECURITY_ATTRIBUTES ) NULL,
    (DWORD) creationDisposition,
    dwFlagsAndAttributes,
    NULL);
  if (hFile == INVALID_HANDLE_VALUE) {
    throw_ioe(env, GetLastError());
    goto cleanup;
  }

  ret = fd_create(env, (long) hFile);
cleanup:
  if (path != NULL) {
    (*env)->ReleaseStringChars(env, j_path, (const jchar*)path);
  }
  return (jobject) ret;
#endif
}

/*
 * Class:     org_apache_hadoop_io_nativeio_NativeIO_POSIX
 * Method:    getUIDforFDOwnerforOwner
 * Signature: (Ljava/io/FileDescriptor;)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_hadoop_io_nativeio_NativeIO_00024POSIX_getUIDforFDOwnerforOwner
  (JNIEnv *env, jclass clazz, jobject fd_object)
{
#ifdef UNIX
  int fd = fd_get(env, fd_object);
  PASS_EXCEPTIONS_GOTO(env, cleanup);

  struct stat s;
  int rc = fstat(fd, &s);
  if (rc != 0) {
    throw_ioe(env, errno);
    goto cleanup;
  }
  return (jlong)(s.st_uid);
cleanup:
  return (jlong)(-1);
#endif

#ifdef WINDOWS
  THROW(env, "java/io/IOException",
    "The function POSIX.getUIDforFDOwnerforOwner() is not supported on Windows");
  return (jlong)(-1);
#endif
}

/*
 * Class:     org_apache_hadoop_io_nativeio_NativeIO_POSIX
 * Method:    getUserName
 * Signature: (J)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_apache_hadoop_io_nativeio_NativeIO_00024POSIX_getUserName
  (JNIEnv *env, jclass clazz, jlong uid)
{
#ifdef UNIX   
  char *pw_buf = NULL;
  int rc;
  size_t pw_buflen = get_pw_buflen();
  if ((pw_buf = malloc(pw_buflen)) == NULL) {
    THROW(env, "java/lang/OutOfMemoryError", "Couldn't allocate memory for pw buffer");
    goto cleanup;
  }

  // Grab username
  struct passwd pwd, *pwdp;
  while ((rc = getpwuid_r((uid_t)uid, &pwd, pw_buf, pw_buflen, &pwdp)) != 0) {
    if (rc != ERANGE) {
      throw_ioe(env, rc);
      goto cleanup;
    }
    free(pw_buf);
    pw_buflen *= 2;
    if ((pw_buf = malloc(pw_buflen)) == NULL) {
      THROW(env, "java/lang/OutOfMemoryError", "Couldn't allocate memory for pw buffer");
      goto cleanup;
    }
  }
  if (rc == 0 && pwdp == NULL) {
    throw_ioe(env, ENOENT);
    goto cleanup;
  }

  jstring jstr_username = (*env)->NewStringUTF(env, pwd.pw_name);

cleanup:
  if (pw_buf != NULL) free(pw_buf);
  return jstr_username;
#endif

#ifdef WINDOWS
  THROW(env, "java/io/IOException",
    "The function POSIX.getUserName() is not supported on Windows");
  return NULL;
#endif
}

/*
 * Class:     org_apache_hadoop_io_nativeio_NativeIO_POSIX
 * Method:    chmod
 * Signature: (Ljava/lang/String;I)V
 */
JNIEXPORT void JNICALL Java_org_apache_hadoop_io_nativeio_NativeIO_00024POSIX_chmodImpl
  (JNIEnv *env, jclass clazz, jstring j_path, jint mode)
{
#ifdef UNIX
  const char *path = (*env)->GetStringUTFChars(env, j_path, NULL);
  if (path == NULL) return; // JVM throws Exception for us

  if (chmod(path, mode) != 0) {
    throw_ioe(env, errno);
  }

  (*env)->ReleaseStringUTFChars(env, j_path, path);
#endif

#ifdef WINDOWS
  DWORD dwRtnCode = ERROR_SUCCESS;
  LPCWSTR path = (LPCWSTR) (*env)->GetStringChars(env, j_path, NULL);
  if (path == NULL) return; // JVM throws Exception for us
  
  if ((dwRtnCode = ChangeFileModeByMask((LPCWSTR) path, mode)) != ERROR_SUCCESS)
  {
    throw_ioe(env, dwRtnCode);
  }

  (*env)->ReleaseStringChars(env, j_path, (const jchar*) path);
#endif
}

/*
 * Throw a java.IO.IOException, generating the message from errno.
 */
static void throw_ioe(JNIEnv* env, int errnum)
{
#ifdef UNIX
  const char* message;
  char buffer[80];
  jstring jstr_message;

  buffer[0] = 0;
#ifdef STRERROR_R_CHAR_P
  // GNU strerror_r
  message = strerror_r(errnum, buffer, sizeof(buffer));
  assert (message != NULL);
#else
  int ret = strerror_r(errnum, buffer, sizeof(buffer));
  if (ret == 0) {
    message = buffer;
  } else {
    message = "Unknown error";
  }
#endif
  jobject errno_obj = errno_to_enum(env, errnum);

  if ((jstr_message = (*env)->NewStringUTF(env, message)) == NULL)
    goto err;

  jthrowable obj = (jthrowable)(*env)->NewObject(env, nioe_clazz, nioe_ctor,
    jstr_message, errno_obj);
  if (obj == NULL) goto err;

  (*env)->Throw(env, obj);
  return;

err:
  if (jstr_message != NULL)
    (*env)->ReleaseStringUTFChars(env, jstr_message, message);
#endif

#ifdef WINDOWS
  DWORD len = 0;
  LPWSTR buffer = NULL;
  const jchar* message = NULL;
  jstring jstr_message = NULL;
  jthrowable obj = NULL;

  len = FormatMessageW(
    FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM,
    NULL, *(DWORD*) (&errnum), // reinterpret cast
    MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
    (LPWSTR) &buffer, 0, NULL);

  if (len > 0)
  {
    message = (const jchar*) buffer;
  }
  else
  {
    message = (const jchar*) L"Unknown error.";
  }

  if ((jstr_message = (*env)->NewString(env, message, len)) == NULL)
    goto err;
  LocalFree(buffer);
  buffer = NULL; // Set buffer to NULL to avoid double free

  obj = (jthrowable)(*env)->NewObject(env, nioe_clazz, nioe_ctor,
    jstr_message, errnum);
  if (obj == NULL) goto err;

  (*env)->Throw(env, obj);
  return;

err:
  if (jstr_message != NULL)
    (*env)->ReleaseStringChars(env, jstr_message, message);
  LocalFree(buffer);
  return;
#endif
}

#ifdef UNIX
/*
 * Determine how big a buffer we need for reentrant getpwuid_r and getgrnam_r
 */
ssize_t get_pw_buflen() {
  size_t ret = 0;
  #ifdef _SC_GETPW_R_SIZE_MAX
  ret = sysconf(_SC_GETPW_R_SIZE_MAX);
  #endif
  return (ret > 512) ? ret : 512;

}
#endif

/*
 * Class:     org_apache_hadoop_io_nativeio_NativeIO_Windows
 * Method:    getOwnerOnWindows
 * Signature: (Ljava/io/FileDescriptor;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_apache_hadoop_io_nativeio_NativeIO_00024Windows_getOwner
  (JNIEnv *env, jclass clazz, jobject fd_object)
{
#ifdef UNIX
  THROW(env, "java/io/IOException",
    "The function Windows.getOwner() is not supported on Unix");
  return NULL;
#endif

#ifdef WINDOWS
  PSID pSidOwner = NULL;
  PSECURITY_DESCRIPTOR pSD = NULL;
  LPWSTR ownerName = (LPWSTR)NULL;
  DWORD dwRtnCode = ERROR_SUCCESS;
  jstring jstr_username = NULL;
  HANDLE hFile = (HANDLE) fd_get(env, fd_object);
  PASS_EXCEPTIONS_GOTO(env, cleanup);

  dwRtnCode = GetSecurityInfo(
    hFile,
    SE_FILE_OBJECT,
    OWNER_SECURITY_INFORMATION,
    &pSidOwner,
    NULL,
    NULL,
    NULL,
    &pSD);
  if (dwRtnCode != ERROR_SUCCESS) {
    throw_ioe(env, dwRtnCode);
    goto cleanup;
  }

  dwRtnCode = GetAccntNameFromSid(pSidOwner, &ownerName);
  if (dwRtnCode != ERROR_SUCCESS) {
    throw_ioe(env, dwRtnCode);
    goto cleanup;
  }

  jstr_username = (*env)->NewString(env, ownerName, (jsize) wcslen(ownerName));
  if (jstr_username == NULL) goto cleanup;

cleanup:
  LocalFree(ownerName);
  LocalFree(pSD);
  return jstr_username;
#endif
}

/*
 * Class:     org_apache_hadoop_io_nativeio_NativeIO_Windows
 * Method:    setFilePointer
 * Signature: (Ljava/io/FileDescriptor;JJ)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_hadoop_io_nativeio_NativeIO_00024Windows_setFilePointer
  (JNIEnv *env, jclass clazz, jobject fd_object, jlong distanceToMove, jlong moveMethod)
{
#ifdef UNIX
  THROW(env, "java/io/IOException",
    "The function setFilePointer(FileDescriptor) is not supported on Unix");
  return NULL;
#endif

#ifdef WINDOWS
  DWORD distanceToMoveLow = (DWORD) distanceToMove;
  LONG distanceToMoveHigh = (LONG) (distanceToMove >> 32);
  DWORD distanceMovedLow = 0;
  HANDLE hFile = (HANDLE) fd_get(env, fd_object);
  PASS_EXCEPTIONS_GOTO(env, cleanup);

  distanceMovedLow = SetFilePointer(hFile,
    distanceToMoveLow, &distanceToMoveHigh, (DWORD) moveMethod);

  if (distanceMovedLow == INVALID_SET_FILE_POINTER) {
     throw_ioe(env, GetLastError());
     return -1;
  }

cleanup:

  return ((jlong) distanceToMoveHigh << 32) | (jlong) distanceMovedLow;
#endif
}

/**
 * vim: sw=2: ts=2: et:
 */

