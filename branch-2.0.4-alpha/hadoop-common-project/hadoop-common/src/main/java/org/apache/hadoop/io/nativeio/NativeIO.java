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
package org.apache.hadoop.io.nativeio;

import java.io.File;
import java.io.FileDescriptor;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.NativeCodeLoader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/**
 * JNI wrappers for various native IO-related calls not available in Java.
 * These functions should generally be used alongside a fallback to another
 * more portable mechanism.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class NativeIO {
  // Flags for open() call from bits/fcntl.h
  public static final int O_RDONLY   =    00;
  public static final int O_WRONLY   =    01;
  public static final int O_RDWR     =    02;
  public static final int O_CREAT    =  0100;
  public static final int O_EXCL     =  0200;
  public static final int O_NOCTTY   =  0400;
  public static final int O_TRUNC    = 01000;
  public static final int O_APPEND   = 02000;
  public static final int O_NONBLOCK = 04000;
  public static final int O_SYNC   =  010000;
  public static final int O_ASYNC  =  020000;
  public static final int O_FSYNC = O_SYNC;
  public static final int O_NDELAY = O_NONBLOCK;

  // Flags for posix_fadvise() from bits/fcntl.h
  /* No further special treatment.  */
  public static final int POSIX_FADV_NORMAL = 0; 
  /* Expect random page references.  */
  public static final int POSIX_FADV_RANDOM = 1; 
  /* Expect sequential page references.  */
  public static final int POSIX_FADV_SEQUENTIAL = 2; 
  /* Will need these pages.  */
  public static final int POSIX_FADV_WILLNEED = 3; 
  /* Don't need these pages.  */
  public static final int POSIX_FADV_DONTNEED = 4; 
  /* Data will be accessed once.  */
  public static final int POSIX_FADV_NOREUSE = 5; 


  /* Wait upon writeout of all pages
     in the range before performing the
     write.  */
  public static final int SYNC_FILE_RANGE_WAIT_BEFORE = 1;
  /* Initiate writeout of all those
     dirty pages in the range which are
     not presently under writeback.  */
  public static final int SYNC_FILE_RANGE_WRITE = 2;

  /* Wait upon writeout of all pages in
     the range after performing the
     write.  */
  public static final int SYNC_FILE_RANGE_WAIT_AFTER = 4;

  private static final Log LOG = LogFactory.getLog(NativeIO.class);

  private static boolean nativeLoaded = false;
  private static boolean workaroundNonThreadSafePasswdCalls = false;
  private static boolean fadvisePossible = true;
  private static boolean syncFileRangePossible = true;

  static final String WORKAROUND_NON_THREADSAFE_CALLS_KEY =
    "hadoop.workaround.non.threadsafe.getpwuid";
  static final boolean WORKAROUND_NON_THREADSAFE_CALLS_DEFAULT = false;

  private static long cacheTimeout = -1;

  static {
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      try {
        Configuration conf = new Configuration();
        workaroundNonThreadSafePasswdCalls = conf.getBoolean(
          WORKAROUND_NON_THREADSAFE_CALLS_KEY,
          WORKAROUND_NON_THREADSAFE_CALLS_DEFAULT);

        initNative();
        nativeLoaded = true;

        cacheTimeout = conf.getLong(
          CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_KEY,
          CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_DEFAULT) *
          1000;
        LOG.debug("Initialized cache for IDs to User/Group mapping with a" +
          " cache timeout of " + cacheTimeout/1000 + " seconds.");

      } catch (Throwable t) {
        // This can happen if the user has an older version of libhadoop.so
        // installed - in this case we can continue without native IO
        // after warning
        LOG.error("Unable to initialize NativeIO libraries", t);
      }
    }
  }

  /**
   * Return true if the JNI-based native IO extensions are available.
   */
  public static boolean isAvailable() {
    return NativeCodeLoader.isNativeCodeLoaded() && nativeLoaded;
  }

  /** Wrapper around open(2) */
  public static native FileDescriptor open(String path, int flags, int mode) throws IOException;
  /** Wrapper around fstat(2) */
  private static native Stat fstat(FileDescriptor fd) throws IOException;
  /** Wrapper around chmod(2) */
  public static native void chmod(String path, int mode) throws IOException;

  /** Wrapper around posix_fadvise(2) */
  static native void posix_fadvise(
    FileDescriptor fd, long offset, long len, int flags) throws NativeIOException;

  /** Wrapper around sync_file_range(2) */
  static native void sync_file_range(
    FileDescriptor fd, long offset, long nbytes, int flags) throws NativeIOException;

  /** Initialize the JNI method ID and class ID cache */
  private static native void initNative();

  /**
   * Call posix_fadvise on the given file descriptor. See the manpage
   * for this syscall for more information. On systems where this
   * call is not available, does nothing.
   *
   * @throws NativeIOException if there is an error with the syscall
   */
  public static void posixFadviseIfPossible(
      FileDescriptor fd, long offset, long len, int flags)
      throws NativeIOException {
    if (nativeLoaded && fadvisePossible) {
      try {
        posix_fadvise(fd, offset, len, flags);
      } catch (UnsupportedOperationException uoe) {
        fadvisePossible = false;
      } catch (UnsatisfiedLinkError ule) {
        fadvisePossible = false;
      }
    }
  }

  /**
   * Call sync_file_range on the given file descriptor. See the manpage
   * for this syscall for more information. On systems where this
   * call is not available, does nothing.
   *
   * @throws NativeIOException if there is an error with the syscall
   */
  public static void syncFileRangeIfPossible(
      FileDescriptor fd, long offset, long nbytes, int flags)
      throws NativeIOException {
    if (nativeLoaded && syncFileRangePossible) {
      try {
        sync_file_range(fd, offset, nbytes, flags);
      } catch (UnsupportedOperationException uoe) {
        syncFileRangePossible = false;
      } catch (UnsatisfiedLinkError ule) {
        syncFileRangePossible = false;
      }
    }
  }

  /**
   * Result type of the fstat call
   */
  public static class Stat {
    private int ownerId, groupId;
    private String owner, group;
    private int mode;

    // Mode constants
    public static final int S_IFMT = 0170000;      /* type of file */
    public static final int   S_IFIFO  = 0010000;  /* named pipe (fifo) */
    public static final int   S_IFCHR  = 0020000;  /* character special */
    public static final int   S_IFDIR  = 0040000;  /* directory */
    public static final int   S_IFBLK  = 0060000;  /* block special */
    public static final int   S_IFREG  = 0100000;  /* regular */
    public static final int   S_IFLNK  = 0120000;  /* symbolic link */
    public static final int   S_IFSOCK = 0140000;  /* socket */
    public static final int   S_IFWHT  = 0160000;  /* whiteout */
    public static final int S_ISUID = 0004000;  /* set user id on execution */
    public static final int S_ISGID = 0002000;  /* set group id on execution */
    public static final int S_ISVTX = 0001000;  /* save swapped text even after use */
    public static final int S_IRUSR = 0000400;  /* read permission, owner */
    public static final int S_IWUSR = 0000200;  /* write permission, owner */
    public static final int S_IXUSR = 0000100;  /* execute/search permission, owner */

    Stat(int ownerId, int groupId, int mode) {
      this.ownerId = ownerId;
      this.groupId = groupId;
      this.mode = mode;
    }

    @Override
    public String toString() {
      return "Stat(owner='" + owner + "', group='" + group + "'" +
        ", mode=" + mode + ")";
    }

    public String getOwner() {
      return owner;
    }
    public String getGroup() {
      return group;
    }
    public int getMode() {
      return mode;
    }
  }

  static native String getUserName(int uid) throws IOException;

  static native String getGroupName(int uid) throws IOException;

  private static class CachedName {
    final long timestamp;
    final String name;

    public CachedName(String name, long timestamp) {
      this.name = name;
      this.timestamp = timestamp;
    }
  }

  private static final Map<Integer, CachedName> USER_ID_NAME_CACHE =
    new ConcurrentHashMap<Integer, CachedName>();

  private static final Map<Integer, CachedName> GROUP_ID_NAME_CACHE =
    new ConcurrentHashMap<Integer, CachedName>();

  private enum IdCache { USER, GROUP }

  private static String getName(IdCache domain, int id) throws IOException {
    Map<Integer, CachedName> idNameCache = (domain == IdCache.USER)
      ? USER_ID_NAME_CACHE : GROUP_ID_NAME_CACHE;
    String name;
    CachedName cachedName = idNameCache.get(id);
    long now = System.currentTimeMillis();
    if (cachedName != null && (cachedName.timestamp + cacheTimeout) > now) {
      name = cachedName.name;
    } else {
      name = (domain == IdCache.USER) ? getUserName(id) : getGroupName(id);
      if (LOG.isDebugEnabled()) {
        String type = (domain == IdCache.USER) ? "UserName" : "GroupName";
        LOG.debug("Got " + type + " " + name + " for ID " + id +
          " from the native implementation");
      }
      cachedName = new CachedName(name, now);
      idNameCache.put(id, cachedName);
    }
    return name;
  }

  /**
   * Returns the file stat for a file descriptor.
   *
   * @param fd file descriptor.
   * @return the file descriptor file stat.
   * @throws IOException thrown if there was an IO error while obtaining the file stat.
   */
  public static Stat getFstat(FileDescriptor fd) throws IOException {
    Stat stat = fstat(fd);
    stat.owner = getName(IdCache.USER, stat.ownerId);
    stat.group = getName(IdCache.GROUP, stat.groupId);
    return stat;
  }
  
  /**
   * A version of renameTo that throws a descriptive exception when it fails.
   *
   * @param src                  The source path
   * @param dst                  The destination path
   * 
   * @throws NativeIOException   On failure.
   */
  public static void renameTo(File src, File dst)
      throws IOException {
    if (!nativeLoaded) {
      if (!src.renameTo(dst)) {
        throw new IOException("renameTo(src=" + src + ", dst=" +
          dst + ") failed.");
      }
    } else {
      renameTo0(src.getAbsolutePath(), dst.getAbsolutePath());
    }
  }

  /**
   * A version of renameTo that throws a descriptive exception when it fails.
   *
   * @param src                  The source path
   * @param dst                  The destination path
   * 
   * @throws NativeIOException   On failure.
   */
  private static native void renameTo0(String src, String dst)
      throws NativeIOException;
}
