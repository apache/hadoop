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

#include "org_apache_hadoop.h"
#include "org_apache_hadoop_io_nativeio_NativeIO.h"
#include "org_apache_hadoop_io_nativeio_NativeIO_POSIX.h"
#include "exception.h"

#ifdef UNIX
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <jni.h>
#include <pwd.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#if !(defined(__FreeBSD__) || defined(__MACH__))
#include <sys/sendfile.h>
#endif
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include "config.h"
#endif

#ifdef WINDOWS
#include <assert.h>
#include <Windows.h>
#include "winutils.h"
#endif

#include "file_descriptor.h"
#include "errno_enum.h"

#define MMAP_PROT_READ org_apache_hadoop_io_nativeio_NativeIO_POSIX_MMAP_PROT_READ
#define MMAP_PROT_WRITE org_apache_hadoop_io_nativeio_NativeIO_POSIX_MMAP_PROT_WRITE
#define MMAP_PROT_EXEC org_apache_hadoop_io_nativeio_NativeIO_POSIX_MMAP_PROT_EXEC

#define NATIVE_IO_POSIX_CLASS "org/apache/hadoop/io/nativeio/NativeIO$POSIX"
#define NATIVE_IO_STAT_CLASS "org/apache/hadoop/io/nativeio/NativeIO$POSIX$Stat"

#define SET_INT_OR_RETURN(E, C, F) \
  { \
    setStaticInt(E, C, #F, F); \
    if ((*E)->ExceptionCheck(E)) return; \
  }

// the NativeIO$POSIX$Stat inner class and its constructor
static jclass stat_clazz;
static jmethodID stat_ctor;
static jmethodID stat_ctor2;

// the NativeIOException class and its constructor
static jclass nioe_clazz;
static jmethodID nioe_ctor;

// the monitor used for working around non-threadsafe implementations
// of getpwuid_r, observed on platforms including RHEL 6.0.
// Please see HADOOP-7156 for details.
jobject pw_lock_object;

/*
 * Throw a java.IO.IOException, generating the message from errno.
 * NB. this is also used form windows_secure_container_executor.c
 */
extern void throw_ioe(JNIEnv* env, int errnum);

// Internal functions
#ifdef UNIX
static ssize_t get_pw_buflen();
#endif

/**
 * Returns non-zero if the user has specified that the system
 * has non-threadsafe implementations of getpwuid_r or getgrgid_r.
 **/
static int workaround_non_threadsafe_calls(JNIEnv *env, jclass clazz) {
  jboolean result;
  jfieldID needs_workaround_field = (*env)->GetStaticFieldID(
    env, clazz,
    "workaroundNonThreadSafePasswdCalls",
    "Z");
  PASS_EXCEPTIONS_RET(env, 0);
  assert(needs_workaround_field);

  result = (*env)->GetStaticBooleanField(
    env, clazz, needs_workaround_field);
  return result;
}

/**
 * Sets a static boolean field to the specified value.
 */
static void setStaticBoolean(JNIEnv *env, jclass clazz, char *field,
  jboolean val) {
    jfieldID fid = (*env)->GetStaticFieldID(env, clazz, field, "Z");
    if (fid != NULL) {
      (*env)->SetStaticBooleanField(env, clazz, fid, val);
    }
}

/**
 * Sets a static int field to the specified value.
 */
static void setStaticInt(JNIEnv *env, jclass clazz, char *field,
  jint val) {
    jfieldID fid = (*env)->GetStaticFieldID(env, clazz, field, "I");
    if (fid != NULL) {
      (*env)->SetStaticIntField(env, clazz, fid, val);
    }
}

#ifdef UNIX
/**
 * Initialises a list of java constants that are platform specific.
 * These are only initialized in UNIX.
 * Any exceptions that occur will be dealt at the level above.
**/
static void consts_init(JNIEnv *env) {
  jclass clazz = (*env)->FindClass(env, NATIVE_IO_POSIX_CLASS);
  if (clazz == NULL) {
    return; // exception has been raised
  }
  SET_INT_OR_RETURN(env, clazz, O_RDONLY);
  SET_INT_OR_RETURN(env, clazz, O_WRONLY);
  SET_INT_OR_RETURN(env, clazz, O_RDWR);
  SET_INT_OR_RETURN(env, clazz, O_CREAT);
  SET_INT_OR_RETURN(env, clazz, O_EXCL);
  SET_INT_OR_RETURN(env, clazz, O_NOCTTY);
  SET_INT_OR_RETURN(env, clazz, O_TRUNC);
  SET_INT_OR_RETURN(env, clazz, O_APPEND);
  SET_INT_OR_RETURN(env, clazz, O_NONBLOCK);
  SET_INT_OR_RETURN(env, clazz, O_SYNC);
#ifdef HAVE_POSIX_FADVISE
  setStaticBoolean(env, clazz, "fadvisePossible", JNI_TRUE);
  SET_INT_OR_RETURN(env, clazz, POSIX_FADV_NORMAL);
  SET_INT_OR_RETURN(env, clazz, POSIX_FADV_RANDOM);
  SET_INT_OR_RETURN(env, clazz, POSIX_FADV_SEQUENTIAL);
  SET_INT_OR_RETURN(env, clazz, POSIX_FADV_WILLNEED);
  SET_INT_OR_RETURN(env, clazz, POSIX_FADV_DONTNEED);
  SET_INT_OR_RETURN(env, clazz, POSIX_FADV_NOREUSE);
#else
  setStaticBoolean(env, clazz, "fadvisePossible", JNI_FALSE);
#endif
#ifdef HAVE_SYNC_FILE_RANGE
  SET_INT_OR_RETURN(env, clazz, SYNC_FILE_RANGE_WAIT_BEFORE);
  SET_INT_OR_RETURN(env, clazz, SYNC_FILE_RANGE_WRITE);
  SET_INT_OR_RETURN(env, clazz, SYNC_FILE_RANGE_WAIT_AFTER);
#endif
  clazz = (*env)->FindClass(env, NATIVE_IO_STAT_CLASS);
  if (clazz == NULL) {
    return; // exception has been raised
  }
  SET_INT_OR_RETURN(env, clazz, S_IFMT);
  SET_INT_OR_RETURN(env, clazz, S_IFIFO);
  SET_INT_OR_RETURN(env, clazz, S_IFCHR);
  SET_INT_OR_RETURN(env, clazz, S_IFDIR);
  SET_INT_OR_RETURN(env, clazz, S_IFBLK);
  SET_INT_OR_RETURN(env, clazz, S_IFREG);
  SET_INT_OR_RETURN(env, clazz, S_IFLNK);
  SET_INT_OR_RETURN(env, clazz, S_IFSOCK);
  SET_INT_OR_RETURN(env, clazz, S_ISUID);
  SET_INT_OR_RETURN(env, clazz, S_ISGID);
  SET_INT_OR_RETURN(env, clazz, S_ISVTX);
  SET_INT_OR_RETURN(env, clazz, S_IRUSR);
  SET_INT_OR_RETURN(env, clazz, S_IWUSR);
  SET_INT_OR_RETURN(env, clazz, S_IXUSR);
}
#endif

static void stat_init(JNIEnv *env, jclass nativeio_class) {
  jclass clazz = NULL;
  jclass obj_class = NULL;
  jmethodID  obj_ctor = NULL;
  // Init Stat
  clazz = (*env)->FindClass(env, NATIVE_IO_STAT_CLASS);
  if (!clazz) {
    return; // exception has been raised
  }
  stat_clazz = (*env)->NewGlobalRef(env, clazz);
  if (!stat_clazz) {
    return; // exception has been raised
  }
  stat_ctor = (*env)->GetMethodID(env, stat_clazz, "<init>",
    "(III)V");
  if (!stat_ctor) {
    return; // exception has been raised
  }
  stat_ctor2 = (*env)->GetMethodID(env, stat_clazz, "<init>",
    "(Ljava/lang/String;Ljava/lang/String;I)V");
  if (!stat_ctor2) {
    return; // exception has been raised
  }
  obj_class = (*env)->FindClass(env, "java/lang/Object");
  if (!obj_class) {
    return; // exception has been raised
  }
  obj_ctor = (*env)->GetMethodID(env, obj_class,
    "<init>", "()V");
  if (!obj_ctor) {
    return; // exception has been raised
  }

  if (workaround_non_threadsafe_calls(env, nativeio_class)) {
    pw_lock_object = (*env)->NewObject(env, obj_class, obj_ctor);
    PASS_EXCEPTIONS(env);
    pw_lock_object = (*env)->NewGlobalRef(env, pw_lock_object);

    PASS_EXCEPTIONS(env);
  }
}

static void stat_deinit(JNIEnv *env) {
  if (stat_clazz != NULL) {  
    (*env)->DeleteGlobalRef(env, stat_clazz);
    stat_clazz = NULL;
  }
  if (pw_lock_object != NULL) {
    (*env)->DeleteGlobalRef(env, pw_lock_object);
    pw_lock_object = NULL;
  }
}

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
#ifdef UNIX
  consts_init(env);
  PASS_EXCEPTIONS_GOTO(env, error);
#endif
  stat_init(env, clazz);
  PASS_EXCEPTIONS_GOTO(env, error);
  nioe_init(env);
  PASS_EXCEPTIONS_GOTO(env, error);
  fd_init(env);
  PASS_EXCEPTIONS_GOTO(env, error);
#ifdef UNIX
  errno_enum_init(env);
  PASS_EXCEPTIONS_GOTO(env, error);
#endif
  return;
error:
  // these are all idempodent and safe to call even if the
  // class wasn't initted yet
#ifdef UNIX
  stat_deinit(env);
#endif
  nioe_deinit(env);
  fd_deinit(env);
#ifdef UNIX
  errno_enum_deinit(env);
#endif
}

/*
 * Class:     org_apache_hadoop_io_nativeio_NativeIO_POSIX
 * Method:    fstat
 * Signature: (Ljava/io/FileDescriptor;)Lorg/apache/hadoop/io/nativeio/NativeIO$POSIX$Stat;
 * public static native Stat fstat(FileDescriptor fd);
 *
 * The "00024" in the function name is an artifact of how JNI encodes
 * special characters. U+0024 is '$'.
 */
JNIEXPORT jobject JNICALL
Java_org_apache_hadoop_io_nativeio_NativeIO_00024POSIX_fstat(
  JNIEnv *env, jclass clazz, jobject fd_object)
{
#ifdef UNIX
  jobject ret = NULL;

  int fd = fd_get(env, fd_object);
  PASS_EXCEPTIONS_GOTO(env, cleanup);

  struct stat s;
  int rc = fstat(fd, &s);
  if (rc != 0) {
    throw_ioe(env, errno);
    goto cleanup;
  }

  // Construct result
  ret = (*env)->NewObject(env, stat_clazz, stat_ctor,
    (jint)s.st_uid, (jint)s.st_gid, (jint)s.st_mode);

cleanup:
  return ret;
#endif

#ifdef WINDOWS
  LPWSTR owner = NULL;
  LPWSTR group = NULL;
  int mode = 0;
  jstring jstr_owner = NULL;
  jstring jstr_group = NULL;
  int rc;
  jobject ret = NULL;
  HANDLE hFile = (HANDLE) fd_get(env, fd_object);
  PASS_EXCEPTIONS_GOTO(env, cleanup);

  rc = FindFileOwnerAndPermissionByHandle(hFile, &owner, &group, &mode);
  if (rc != ERROR_SUCCESS) {
    throw_ioe(env, rc);
    goto cleanup;
  }

  jstr_owner = (*env)->NewString(env, owner, (jsize) wcslen(owner));
  if (jstr_owner == NULL) goto cleanup;

  jstr_group = (*env)->NewString(env, group, (jsize) wcslen(group));;
  if (jstr_group == NULL) goto cleanup;

  ret = (*env)->NewObject(env, stat_clazz, stat_ctor2,
    jstr_owner, jstr_group, (jint)mode);

cleanup:
  if (ret == NULL) {
    if (jstr_owner != NULL)
      (*env)->ReleaseStringChars(env, jstr_owner, owner);

    if (jstr_group != NULL)
      (*env)->ReleaseStringChars(env, jstr_group, group);
  }

  LocalFree(owner);
  LocalFree(group);

  return ret;
#endif
}

/*
 * Class:     org_apache_hadoop_io_nativeio_NativeIO_POSIX
 * Method:    stat
 * Signature: (Ljava/lang/String;)Lorg/apache/hadoop/io/nativeio/NativeIO$POSIX$Stat;
 * public static native Stat stat(String path);
 *
 * The "00024" in the function name is an artifact of how JNI encodes
 * special characters. U+0024 is '$'.
 */
JNIEXPORT jobject JNICALL
Java_org_apache_hadoop_io_nativeio_NativeIO_00024POSIX_stat(
  JNIEnv *env, jclass clazz, jstring j_path)
{
#ifdef UNIX
  jobject ret = NULL;

  const char *c_path = (*env)->GetStringUTFChars(env, j_path, NULL);
  if (c_path == NULL) {
    goto cleanup;
  }

  struct stat s;
  int rc = stat(c_path, &s);
  if (rc != 0) {
    throw_ioe(env, errno);
    goto cleanup;
  }

  // Construct result
  ret = (*env)->NewObject(env, stat_clazz, stat_ctor,
    (jint)s.st_uid, (jint)s.st_gid, (jint)s.st_mode);

cleanup:
  if (c_path != NULL) {
    (*env)->ReleaseStringUTFChars(env, j_path, c_path);
  }
  return ret;
#endif

#ifdef WINDOWS
  LPWSTR owner = NULL;
  LPWSTR group = NULL;
  int mode = 0;
  jstring jstr_owner = NULL;
  jstring jstr_group = NULL;
  int rc;
  jobject ret = NULL;

  LPCWSTR path = (LPCWSTR) (*env)->GetStringChars(env, j_path, NULL);
  if (path == NULL) {
    goto cleanup;
  }

  rc = FindFileOwnerAndPermission(path, TRUE, &owner, &group, &mode);
  if (rc != ERROR_SUCCESS) {
    throw_ioe(env, rc);
    goto cleanup;
  }

  jstr_owner = (*env)->NewString(env, owner, (jsize) wcslen(owner));
  if (jstr_owner == NULL) goto cleanup;

  jstr_group = (*env)->NewString(env, group, (jsize) wcslen(group));
  if (jstr_group == NULL) goto cleanup;

  ret = (*env)->NewObject(env, stat_clazz, stat_ctor2,
    jstr_owner, jstr_group, (jint)mode);

cleanup:
  if (path != NULL)
    (*env)->ReleaseStringChars(env, j_path, (const jchar*) path);

  if (ret == NULL) {
    if (jstr_owner != NULL)
      (*env)->ReleaseStringChars(env, jstr_owner, owner);

    if (jstr_group != NULL)
      (*env)->ReleaseStringChars(env, jstr_group, group);
  }

  LocalFree(owner);
  LocalFree(group);

  return ret;
#endif
}

/**
 * public static native void posix_fadvise(
 *   FileDescriptor fd, long offset, long len, int flags);
 *
 * The "00024" in the function name is an artifact of how JNI encodes
 * special characters. U+0024 is '$'.
 */
JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_nativeio_NativeIO_00024POSIX_posix_1fadvise(
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
#ifdef __FreeBSD__
    throw_ioe(env, errno);
#else
    throw_ioe(env, err);
#endif
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
 *
 * The "00024" in the function name is an artifact of how JNI encodes
 * special characters. U+0024 is '$'.
 */
JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_nativeio_NativeIO_00024POSIX_sync_1file_1range(
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

#define CHECK_DIRECT_BUFFER_ADDRESS(buf) \
  { \
    if (!buf) { \
      THROW(env, "java/lang/UnsupportedOperationException", \
        "JNI access to direct buffers not available"); \
      return; \
    } \
  }

/**
 * public static native void mlock_native(
 *   ByteBuffer buffer, long offset);
 *
 * The "00024" in the function name is an artifact of how JNI encodes
 * special characters. U+0024 is '$'.
 */
JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_nativeio_NativeIO_00024POSIX_mlock_1native(
  JNIEnv *env, jclass clazz,
  jobject buffer, jlong len)
{
  void* buf = (void*)(*env)->GetDirectBufferAddress(env, buffer);
  PASS_EXCEPTIONS(env);

#ifdef UNIX
  if (mlock(buf, len)) {
    CHECK_DIRECT_BUFFER_ADDRESS(buf);
    throw_ioe(env, errno);
  }
#endif

#ifdef WINDOWS
  if (!VirtualLock(buf, len)) {
    CHECK_DIRECT_BUFFER_ADDRESS(buf);
    throw_ioe(env, GetLastError());
  }
#endif
}

/*
 * Class:     org_apache_hadoop_io_nativeio_NativeIO_POSIX
 * Method:    open
 * Signature: (Ljava/lang/String;II)Ljava/io/FileDescriptor;
 * public static native FileDescriptor open(String path, int flags, int mode);
 *
 * The "00024" in the function name is an artifact of how JNI encodes
 * special characters. U+0024 is '$'.
 */
JNIEXPORT jobject JNICALL
Java_org_apache_hadoop_io_nativeio_NativeIO_00024POSIX_open(
  JNIEnv *env, jclass clazz, jstring j_path,
  jint flags, jint mode)
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
 * Method:    createDirectoryWithMode0
 * Signature: (Ljava/lang/String;I)V
 *
 * The "00024" in the function name is an artifact of how JNI encodes
 * special characters. U+0024 is '$'.
 */
JNIEXPORT void JNICALL
  Java_org_apache_hadoop_io_nativeio_NativeIO_00024Windows_createDirectoryWithMode0
  (JNIEnv *env, jclass clazz, jstring j_path, jint mode)
{
#ifdef WINDOWS
  DWORD dwRtnCode = ERROR_SUCCESS;

  LPCWSTR path = (LPCWSTR) (*env)->GetStringChars(env, j_path, NULL);
  if (!path) {
    goto done;
  }

  dwRtnCode = CreateDirectoryWithMode(path, mode);

done:
  if (path) {
    (*env)->ReleaseStringChars(env, j_path, (const jchar*) path);
  }
  if (dwRtnCode != ERROR_SUCCESS) {
    throw_ioe(env, dwRtnCode);
  }
#else
  THROW(env, "java/io/IOException",
    "The function Windows.createDirectoryWithMode0() is not supported on this platform");
#endif
}

/*
 * Class:     org_apache_hadoop_io_nativeio_NativeIO_Windows
 * Method:    createFileWithMode0
 * Signature: (Ljava/lang/String;JJJI)Ljava/io/FileDescriptor;
 *
 * The "00024" in the function name is an artifact of how JNI encodes
 * special characters. U+0024 is '$'.
 */
JNIEXPORT jobject JNICALL
  Java_org_apache_hadoop_io_nativeio_NativeIO_00024Windows_createFileWithMode0
  (JNIEnv *env, jclass clazz, jstring j_path,
  jlong desiredAccess, jlong shareMode, jlong creationDisposition, jint mode)
{
#ifdef WINDOWS
  DWORD dwRtnCode = ERROR_SUCCESS;
  HANDLE hFile = INVALID_HANDLE_VALUE;
  jobject fd = NULL;

  LPCWSTR path = (LPCWSTR) (*env)->GetStringChars(env, j_path, NULL);
  if (!path) {
    goto done;
  }

  dwRtnCode = CreateFileWithMode(path, desiredAccess, shareMode,
      creationDisposition, mode, &hFile);
  if (dwRtnCode != ERROR_SUCCESS) {
    goto done;
  }

  fd = fd_create(env, (long) hFile);

done:
  if (path) {
    (*env)->ReleaseStringChars(env, j_path, (const jchar*) path);
  }
  if (dwRtnCode != ERROR_SUCCESS) {
    throw_ioe(env, dwRtnCode);
  }
  return fd;
#else
  THROW(env, "java/io/IOException",
    "The function Windows.createFileWithMode0() is not supported on this platform");
  return NULL;
#endif
}

/*
 * Class:     org_apache_hadoop_io_nativeio_NativeIO_Windows
 * Method:    createFile
 * Signature: (Ljava/lang/String;JJJ)Ljava/io/FileDescriptor;
 *
 * The "00024" in the function name is an artifact of how JNI encodes
 * special characters. U+0024 is '$'.
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
 * static native String getUserName(int uid);
 */
JNIEXPORT jstring JNICALL 
Java_org_apache_hadoop_io_nativeio_NativeIO_00024POSIX_getUserName(
  JNIEnv *env, jclass clazz, jint uid)
{
#ifdef UNIX
  jstring jstr_username = NULL;
  char *pw_buf = NULL;
  int pw_lock_locked = 0;
  if (pw_lock_object != NULL) {
    if ((*env)->MonitorEnter(env, pw_lock_object) != JNI_OK) {
      goto cleanup;
    }
    pw_lock_locked = 1;
  }

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
  if (pwdp == NULL) {
    char msg[80];
    snprintf(msg, sizeof(msg), "uid not found: %d", uid);
    THROW(env, "java/io/IOException", msg);
    goto cleanup;
  }
  if (pwdp != &pwd) {
    char msg[80];
    snprintf(msg, sizeof(msg), "pwd pointer inconsistent with reference. uid: %d", uid);
    THROW(env, "java/lang/IllegalStateException", msg);
    goto cleanup;
  }

  jstr_username = (*env)->NewStringUTF(env, pwd.pw_name);

cleanup:
  if (pw_lock_locked) {
    (*env)->MonitorExit(env, pw_lock_object);
  }
  if (pw_buf != NULL) free(pw_buf);
  return jstr_username;
#endif // UNIX

#ifdef WINDOWS
  THROW(env, "java/io/IOException",
    "The function POSIX.getUserName() is not supported on Windows");
  return NULL;
#endif
}

JNIEXPORT jlong JNICALL 
Java_org_apache_hadoop_io_nativeio_NativeIO_00024POSIX_mmap(
  JNIEnv *env, jclass clazz, jobject jfd, jint jprot,
  jboolean jshared, jlong length)
{
#ifdef UNIX
  void *addr = 0;
  int prot, flags, fd;
  
  prot = ((jprot & MMAP_PROT_READ) ? PROT_READ : 0) |
         ((jprot & MMAP_PROT_WRITE) ? PROT_WRITE : 0) |
         ((jprot & MMAP_PROT_EXEC) ? PROT_EXEC : 0);
  flags = (jshared == JNI_TRUE) ? MAP_SHARED : MAP_PRIVATE;
  fd = fd_get(env, jfd);
  addr = mmap(NULL, length, prot, flags, fd, 0);
  if (addr == MAP_FAILED) {
    throw_ioe(env, errno);
  }
  return (jlong)(intptr_t)addr;
#endif  //   UNIX

#ifdef WINDOWS
  THROW(env, "java/io/IOException",
    "The function POSIX.mmap() is not supported on Windows");
  return (jlong)(intptr_t)NULL;
#endif
}

JNIEXPORT void JNICALL 
Java_org_apache_hadoop_io_nativeio_NativeIO_00024POSIX_munmap(
  JNIEnv *env, jclass clazz, jlong jaddr, jlong length)
{
#ifdef UNIX
  void *addr;

  addr = (void*)(intptr_t)jaddr;
  if (munmap(addr, length) < 0) {
    throw_ioe(env, errno);
  }
#endif  //   UNIX

#ifdef WINDOWS
  THROW(env, "java/io/IOException",
    "The function POSIX.munmap() is not supported on Windows");
#endif
}


/*
 * static native String getGroupName(int gid);
 *
 * The "00024" in the function name is an artifact of how JNI encodes
 * special characters. U+0024 is '$'.
 */
JNIEXPORT jstring JNICALL 
Java_org_apache_hadoop_io_nativeio_NativeIO_00024POSIX_getGroupName(
  JNIEnv *env, jclass clazz, jint gid)
{
#ifdef UNIX
  jstring jstr_groupname = NULL;
  char *pw_buf = NULL;
  int pw_lock_locked = 0;
 
  if (pw_lock_object != NULL) {
    if ((*env)->MonitorEnter(env, pw_lock_object) != JNI_OK) {
      goto cleanup;
    }
    pw_lock_locked = 1;
  }
  
  int rc;
  size_t pw_buflen = get_pw_buflen();
  if ((pw_buf = malloc(pw_buflen)) == NULL) {
    THROW(env, "java/lang/OutOfMemoryError", "Couldn't allocate memory for pw buffer");
    goto cleanup;
  }
  
  // Grab group
  struct group grp, *grpp;
  while ((rc = getgrgid_r((uid_t)gid, &grp, pw_buf, pw_buflen, &grpp)) != 0) {
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
  if (grpp == NULL) {
    char msg[80];
    snprintf(msg, sizeof(msg), "gid not found: %d", gid);
    THROW(env, "java/io/IOException", msg);
    goto cleanup;
  }
  if (grpp != &grp) {
    char msg[80];
    snprintf(msg, sizeof(msg), "pwd pointer inconsistent with reference. gid: %d", gid);
    THROW(env, "java/lang/IllegalStateException", msg);
    goto cleanup;
  }

  jstr_groupname = (*env)->NewStringUTF(env, grp.gr_name);
  PASS_EXCEPTIONS_GOTO(env, cleanup);
  
cleanup:
  if (pw_lock_locked) {
    (*env)->MonitorExit(env, pw_lock_object);
  }
  if (pw_buf != NULL) free(pw_buf);
  return jstr_groupname;
#endif  //   UNIX

#ifdef WINDOWS
  THROW(env, "java/io/IOException",
    "The function POSIX.getUserName() is not supported on Windows");
  return NULL;
#endif
}

/*
 * Throw a java.IO.IOException, generating the message from errno.
 */
void throw_ioe(JNIEnv* env, int errnum)
{
#ifdef UNIX
  char message[80];
  jstring jstr_message;

  snprintf(message,sizeof(message),"%s",terror(errnum));

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
  long ret = 0;
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
 *
 * The "00024" in the function name is an artifact of how JNI encodes
 * special characters. U+0024 is '$'.
 */
JNIEXPORT jstring JNICALL
Java_org_apache_hadoop_io_nativeio_NativeIO_00024Windows_getOwner
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
 *
 * The "00024" in the function name is an artifact of how JNI encodes
 * special characters. U+0024 is '$'.
 */
JNIEXPORT jlong JNICALL
Java_org_apache_hadoop_io_nativeio_NativeIO_00024Windows_setFilePointer
  (JNIEnv *env, jclass clazz, jobject fd_object, jlong distanceToMove, jlong moveMethod)
{
#ifdef UNIX
  THROW(env, "java/io/IOException",
    "The function setFilePointer(FileDescriptor) is not supported on Unix");
  return (jlong)(intptr_t)NULL;
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

/*
 * Class:     org_apache_hadoop_io_nativeio_NativeIO_Windows
 * Method:    access0
 * Signature: (Ljava/lang/String;I)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_io_nativeio_NativeIO_00024Windows_access0
  (JNIEnv *env, jclass clazz, jstring jpath, jint jaccess)
{
#ifdef UNIX
  THROW(env, "java/io/IOException",
    "The function access0(path, access) is not supported on Unix");
  return (jlong)(intptr_t)NULL;
#endif

#ifdef WINDOWS
  LPCWSTR path = NULL;
  DWORD dwRtnCode = ERROR_SUCCESS;
  ACCESS_MASK access = (ACCESS_MASK)jaccess;
  BOOL allowed = FALSE;

  path = (LPCWSTR) (*env)->GetStringChars(env, jpath, NULL);
  if (!path) goto cleanup; // exception was thrown

  dwRtnCode = CheckAccessForCurrentUser(path, access, &allowed);
  if (dwRtnCode != ERROR_SUCCESS) {
    throw_ioe(env, dwRtnCode);
    goto cleanup;
  }

cleanup:
  if (path) (*env)->ReleaseStringChars(env, jpath, path);

  return (jboolean)allowed;
#endif
}

/*
 * Class:     org_apache_hadoop_io_nativeio_NativeIO_Windows
 * Method:    extendWorkingSetSize
 * Signature: (J)V
 *
 * The "00024" in the function name is an artifact of how JNI encodes
 * special characters. U+0024 is '$'.
 */
JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_nativeio_NativeIO_00024Windows_extendWorkingSetSize(
  JNIEnv *env, jclass clazz, jlong delta)
{
#ifdef UNIX
  THROW(env, "java/io/IOException",
    "The function extendWorkingSetSize(delta) is not supported on Unix");
#endif

#ifdef WINDOWS
  SIZE_T min, max;
  HANDLE hProcess = GetCurrentProcess();
  if (!GetProcessWorkingSetSize(hProcess, &min, &max)) {
    throw_ioe(env, GetLastError());
    return;
  }
  if (!SetProcessWorkingSetSizeEx(hProcess, min + delta, max + delta,
      QUOTA_LIMITS_HARDWS_MIN_DISABLE | QUOTA_LIMITS_HARDWS_MAX_DISABLE)) {
    throw_ioe(env, GetLastError());
    return;
  }
  // There is no need to call CloseHandle on the pseudo-handle returned from
  // GetCurrentProcess.
#endif
}

JNIEXPORT void JNICALL 
Java_org_apache_hadoop_io_nativeio_NativeIO_renameTo0(JNIEnv *env, 
jclass clazz, jstring jsrc, jstring jdst)
{
#ifdef UNIX
  const char *src = NULL, *dst = NULL;
  
  src = (*env)->GetStringUTFChars(env, jsrc, NULL);
  if (!src) goto done; // exception was thrown
  dst = (*env)->GetStringUTFChars(env, jdst, NULL);
  if (!dst) goto done; // exception was thrown
  if (rename(src, dst)) {
    throw_ioe(env, errno);
  }

done:
  if (src) (*env)->ReleaseStringUTFChars(env, jsrc, src);
  if (dst) (*env)->ReleaseStringUTFChars(env, jdst, dst);
#endif

#ifdef WINDOWS
  LPCWSTR src = NULL, dst = NULL;

  src = (LPCWSTR) (*env)->GetStringChars(env, jsrc, NULL);
  if (!src) goto done; // exception was thrown
  dst = (LPCWSTR) (*env)->GetStringChars(env, jdst, NULL);
  if (!dst) goto done; // exception was thrown
  if (!MoveFileEx(src, dst, MOVEFILE_REPLACE_EXISTING)) {
    throw_ioe(env, GetLastError());
  }

done:
  if (src) (*env)->ReleaseStringChars(env, jsrc, src);
  if (dst) (*env)->ReleaseStringChars(env, jdst, dst);
#endif
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_nativeio_NativeIO_link0(JNIEnv *env,
jclass clazz, jstring jsrc, jstring jdst)
{
#ifdef UNIX
  const char *src = NULL, *dst = NULL;

  src = (*env)->GetStringUTFChars(env, jsrc, NULL);
  if (!src) goto done; // exception was thrown
  dst = (*env)->GetStringUTFChars(env, jdst, NULL);
  if (!dst) goto done; // exception was thrown
  if (link(src, dst)) {
    throw_ioe(env, errno);
  }

done:
  if (src) (*env)->ReleaseStringUTFChars(env, jsrc, src);
  if (dst) (*env)->ReleaseStringUTFChars(env, jdst, dst);
#endif

#ifdef WINDOWS
  LPCTSTR src = NULL, dst = NULL;

  src = (LPCTSTR) (*env)->GetStringChars(env, jsrc, NULL);
  if (!src) goto done; // exception was thrown
  dst = (LPCTSTR) (*env)->GetStringChars(env, jdst, NULL);
  if (!dst) goto done; // exception was thrown
  if (!CreateHardLink(dst, src, NULL)) {
    throw_ioe(env, GetLastError());
  }

done:
  if (src) (*env)->ReleaseStringChars(env, jsrc, src);
  if (dst) (*env)->ReleaseStringChars(env, jdst, dst);
#endif
}

JNIEXPORT jlong JNICALL
Java_org_apache_hadoop_io_nativeio_NativeIO_getMemlockLimit0(
JNIEnv *env, jclass clazz)
{
#ifdef RLIMIT_MEMLOCK
  struct rlimit rlim;
  int rc = getrlimit(RLIMIT_MEMLOCK, &rlim);
  if (rc != 0) {
    throw_ioe(env, errno);
    return 0;
  }
  return (rlim.rlim_cur == RLIM_INFINITY) ?
    INT64_MAX : rlim.rlim_cur;
#else
  return 0;
#endif
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_nativeio_NativeIO_copyFileUnbuffered0(
JNIEnv *env, jclass clazz, jstring jsrc, jstring jdst)
{
#ifdef UNIX
  THROW(env, "java/lang/UnsupportedOperationException",
    "The function copyFileUnbuffered0 should not be used on Unix. Use FileChannel#transferTo instead.");
#endif

#ifdef WINDOWS
  LPCWSTR src = NULL, dst = NULL;

  src = (LPCWSTR) (*env)->GetStringChars(env, jsrc, NULL);
  if (!src) goto cleanup; // exception was thrown
  dst = (LPCWSTR) (*env)->GetStringChars(env, jdst, NULL);
  if (!dst) goto cleanup; // exception was thrown
  if (!CopyFileEx(src, dst, NULL, NULL, NULL, COPY_FILE_NO_BUFFERING)) {
    throw_ioe(env, GetLastError());
  }

cleanup:
  if (src) (*env)->ReleaseStringChars(env, jsrc, src);
  if (dst) (*env)->ReleaseStringChars(env, jdst, dst);
#endif
}

/**
 * vim: sw=2: ts=2: et:
 */
