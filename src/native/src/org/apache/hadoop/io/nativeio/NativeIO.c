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
#include "config.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <jni.h>
#include <pwd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "org_apache_hadoop.h"
#include "org_apache_hadoop_io_nativeio_NativeIO.h"
#include "file_descriptor.h"
#include "errno_enum.h"

// the NativeIO$Stat inner class and its constructor
static jclass stat_clazz;
static jmethodID stat_ctor;

// the NativeIOException class and its constructor
static jclass nioe_clazz;
static jmethodID nioe_ctor;

// the monitor used for working around non-threadsafe implementations
// of getpwuid_r, observed on platforms including RHEL 6.0.
// Please see HADOOP-7156 for details.
static jobject pw_lock_object;

// Internal functions
static void throw_ioe(JNIEnv* env, int errnum);
static ssize_t get_pw_buflen();

/**
 * Returns non-zero if the user has specified that the system
 * has non-threadsafe implementations of getpwuid_r or getgrgid_r.
 **/
static int workaround_non_threadsafe_calls(JNIEnv *env, jclass clazz) {
  jfieldID needs_workaround_field = (*env)->GetStaticFieldID(env, clazz,
    "workaroundNonThreadSafePasswdCalls", "Z");
  PASS_EXCEPTIONS_RET(env, 0);
  assert(needs_workaround_field);

  jboolean result = (*env)->GetStaticBooleanField(
    env, clazz, needs_workaround_field);
  return result;
}

static void stat_init(JNIEnv *env, jclass nativeio_class) {
  // Init Stat
  jclass clazz = (*env)->FindClass(env, "org/apache/hadoop/io/nativeio/NativeIO$Stat");
  PASS_EXCEPTIONS(env);
  stat_clazz = (*env)->NewGlobalRef(env, clazz);
  stat_ctor = (*env)->GetMethodID(env, stat_clazz, "<init>",
    "(Ljava/lang/String;Ljava/lang/String;I)V");
  
  jclass obj_class = (*env)->FindClass(env, "java/lang/Object");
  assert(obj_class != NULL);
  jmethodID  obj_ctor = (*env)->GetMethodID(env, obj_class,
    "<init>", "()V");
  assert(obj_ctor != NULL);

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
  nioe_ctor = (*env)->GetMethodID(env, nioe_clazz, "<init>",
    "(Ljava/lang/String;Lorg/apache/hadoop/io/nativeio/Errno;)V");
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

  stat_init(env, clazz);
  PASS_EXCEPTIONS_GOTO(env, error);
  nioe_init(env);
  PASS_EXCEPTIONS_GOTO(env, error);
  fd_init(env);
  PASS_EXCEPTIONS_GOTO(env, error);
  errno_enum_init(env);
  PASS_EXCEPTIONS_GOTO(env, error);
  return;
error:
  // these are all idempodent and safe to call even if the
  // class wasn't initted yet
  stat_deinit(env);
  nioe_deinit(env);
  fd_deinit(env);
  errno_enum_deinit(env);
}

/*
 * public static native Stat fstat(FileDescriptor fd);
 */
JNIEXPORT jobject JNICALL
Java_org_apache_hadoop_io_nativeio_NativeIO_fstat(
  JNIEnv *env, jclass clazz, jobject fd_object)
{
  jobject ret = NULL;
  char *pw_buf = NULL;
  int pw_lock_locked = 0;

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

  if (pw_lock_object != NULL) {
    if ((*env)->MonitorEnter(env, pw_lock_object) != JNI_OK) {
      goto cleanup;
    }
    pw_lock_locked = 1;
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
  assert(pwdp == &pwd);

  jstring jstr_username = (*env)->NewStringUTF(env, pwd.pw_name);
  if (jstr_username == NULL) goto cleanup;

  // Grab group
  struct group grp, *grpp;
  while ((rc = getgrgid_r(s.st_gid, &grp, pw_buf, pw_buflen, &grpp)) != 0) {
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
  assert(grpp == &grp);

  jstring jstr_groupname = (*env)->NewStringUTF(env, grp.gr_name);
  PASS_EXCEPTIONS_GOTO(env, cleanup);

  // Construct result
  ret = (*env)->NewObject(env, stat_clazz, stat_ctor,
    jstr_username, jstr_groupname, s.st_mode);

cleanup:
  if (pw_buf != NULL) free(pw_buf);
  if (pw_lock_locked) {
    (*env)->MonitorExit(env, pw_lock_object);
  }
  return ret;
}


/*
 * public static native FileDescriptor open(String path, int flags, int mode);
 */
JNIEXPORT jobject JNICALL
Java_org_apache_hadoop_io_nativeio_NativeIO_open(
  JNIEnv *env, jclass clazz, jstring j_path,
  jint flags, jint mode)
{
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
}

/**
 * public static native void chmod(String path, int mode) throws IOException;
 */
JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_nativeio_NativeIO_chmod(
  JNIEnv *env, jclass clazz, jstring j_path,
  jint mode)
{
  const char *path = (*env)->GetStringUTFChars(env, j_path, NULL);
  if (path == NULL) return; // JVM throws Exception for us

  if (chmod(path, mode) != 0) {
    throw_ioe(env, errno);
  }

  (*env)->ReleaseStringUTFChars(env, j_path, path);
}


/*
 * Throw a java.IO.IOException, generating the message from errno.
 */
static void throw_ioe(JNIEnv* env, int errnum)
{
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
}


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
/**
 * vim: sw=2: ts=2: et:
 */

