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
package org.apache.hadoop.nfs.nfs3;


/**
 * Some constants for NFSv3
 */
public class Nfs3Constant {
  // The local rpcbind/portmapper port.
  public final static int SUN_RPCBIND = 111;

  // The RPC program number for NFS.
  public final static int PROGRAM = 100003;

  // The program version number that this server implements.
  public final static int VERSION = 3;
  
  // The procedures
  public enum NFSPROC3 {
    // the order of the values below are significant.
    NULL,
    GETATTR,
    SETATTR,
    LOOKUP,
    ACCESS,
    READLINK,
    READ,
    WRITE,
    CREATE(false),
    MKDIR(false),
    SYMLINK(false),
    MKNOD(false),
    REMOVE(false),
    RMDIR(false),
    RENAME(false),
    LINK(false),
    READDIR,
    READDIRPLUS,
    FSSTAT,
    FSINFO,
    PATHCONF,
    COMMIT;

    private final boolean isIdempotent;

    private NFSPROC3(boolean isIdempotent) {
      this.isIdempotent = isIdempotent;
    }

    private NFSPROC3() {
      this(true);
    }

    public boolean isIdempotent() {
      return isIdempotent;
    }

    /** @return the int value representing the procedure. */
    public int getValue() {
      return ordinal();
    }

    /**
     * Convert to NFS procedure.
     * @param value specify the index of NFS procedure
     * @return the procedure corresponding to the value.
     */
    public static NFSPROC3 fromValue(int value) {
      if (value < 0 || value >= values().length) {
        return null;
      }
      return values()[value];
    }
  }
  
  // The maximum size in bytes of the opaque file handle.
  public final static int NFS3_FHSIZE = 64;

  // The byte size of cookie verifier passed by READDIR and READDIRPLUS.
  public final static int NFS3_COOKIEVERFSIZE = 8;

  // The size in bytes of the opaque verifier used for exclusive CREATE.
  public final static int NFS3_CREATEVERFSIZE = 8;

  // The size in bytes of the opaque verifier used for asynchronous WRITE.
  public final static int NFS3_WRITEVERFSIZE = 8;

  /** Access call request mode */
  // File access mode
  public static final int ACCESS_MODE_READ = 0x04;
  public static final int ACCESS_MODE_WRITE = 0x02;
  public static final int ACCESS_MODE_EXECUTE = 0x01;

  /** Access call response rights */
  // Read data from file or read a directory.
  public final static int ACCESS3_READ = 0x0001;
  // Look up a name in a directory (no meaning for non-directory objects).
  public final static int ACCESS3_LOOKUP = 0x0002;
  // Rewrite existing file data or modify existing directory entries.
  public final static int ACCESS3_MODIFY = 0x0004;
  // Write new data or add directory entries.
  public final static int ACCESS3_EXTEND = 0x0008;
  // Delete an existing directory entry.
  public final static int ACCESS3_DELETE = 0x0010;
  // Execute file (no meaning for a directory).
  public final static int ACCESS3_EXECUTE = 0x0020;

  /** File and directory attribute mode bits */
  // Set user ID on execution.
  public final static int MODE_S_ISUID = 0x00800;
  // Set group ID on execution.
  public final static int MODE_S_ISGID = 0x00400;
  // Save swapped text (not defined in POSIX).
  public final static int MODE_S_ISVTX = 0x00200;
  // Read permission for owner.
  public final static int MODE_S_IRUSR = 0x00100;
  // Write permission for owner.
  public final static int MODE_S_IWUSR = 0x00080;
  // Execute permission for owner on a file. Or lookup (search) permission for
  // owner in directory.
  public final static int MODE_S_IXUSR = 0x00040;
  // Read permission for group.
  public final static int MODE_S_IRGRP = 0x00020;
  // Write permission for group.
  public final static int MODE_S_IWGRP = 0x00010;
  // Execute permission for group on a file. Or lookup (search) permission for
  // group in directory.
  public final static int MODE_S_IXGRP = 0x00008;
  // Read permission for others.
  public final static int MODE_S_IROTH = 0x00004;
  // Write permission for others.
  public final static int MODE_S_IWOTH = 0x00002;
  // Execute permission for others on a file. Or lookup (search) permission for
  // others in directory.
  public final static int MODE_S_IXOTH = 0x00001;

  public final static int MODE_ALL = MODE_S_ISUID | MODE_S_ISGID | MODE_S_ISVTX
      | MODE_S_ISVTX | MODE_S_IRUSR | MODE_S_IRUSR | MODE_S_IWUSR
      | MODE_S_IXUSR | MODE_S_IRGRP | MODE_S_IWGRP | MODE_S_IXGRP
      | MODE_S_IROTH | MODE_S_IWOTH | MODE_S_IXOTH;

  /** Write call flavors */
  public enum WriteStableHow {
    // the order of the values below are significant.
    UNSTABLE,
    DATA_SYNC,
    FILE_SYNC;

    public int getValue() {
      return ordinal();
    }

    public static WriteStableHow fromValue(int id) {
      return values()[id];
    }
  }

  /**
   * This is a cookie that the client can use to determine whether the server
   * has changed state between a call to WRITE and a subsequent call to either
   * WRITE or COMMIT. This cookie must be consistent during a single instance of
   * the NFS version 3 protocol service and must be unique between instances of
   * the NFS version 3 protocol server, where uncommitted data may be lost.
   */
  public final static long WRITE_COMMIT_VERF = System.currentTimeMillis();
  
  /** FileSystemProperties */
  public final static int FSF3_LINK = 0x0001;
  public final static int FSF3_SYMLINK = 0x0002;
  public final static int FSF3_HOMOGENEOUS = 0x0008;
  public final static int FSF3_CANSETTIME = 0x0010;

  /** Create options */
  public final static int CREATE_UNCHECKED = 0;
  public final static int CREATE_GUARDED = 1;
  public final static int CREATE_EXCLUSIVE = 2;
  
  /** Size for nfs exports cache */
  public static final String NFS_EXPORTS_CACHE_SIZE_KEY = "nfs.exports.cache.size";
  public static final int NFS_EXPORTS_CACHE_SIZE_DEFAULT = 512;
  /** Expiration time for nfs exports cache entry */
  public static final String NFS_EXPORTS_CACHE_EXPIRYTIME_MILLIS_KEY = "nfs.exports.cache.expirytime.millis";
  public static final long NFS_EXPORTS_CACHE_EXPIRYTIME_MILLIS_DEFAULT = 15 * 60 * 1000; // 15 min
}
