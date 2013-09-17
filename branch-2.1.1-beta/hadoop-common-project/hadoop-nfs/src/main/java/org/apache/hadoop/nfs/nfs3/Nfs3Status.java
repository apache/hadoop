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
 * Success or error status is reported in NFS3 responses.
 */
public class Nfs3Status {
  
  /** Indicates the call completed successfully. */
  public final static int NFS3_OK = 0;
  
  /**
   * The operation was not allowed because the caller is either not a
   * privileged user (root) or not the owner of the target of the operation.
   */
  public final static int NFS3ERR_PERM = 1;
  
  /**
   * No such file or directory. The file or directory name specified does not
   * exist.
   */
  public final static int NFS3ERR_NOENT = 2;
  
  /**
   * I/O error. A hard error (for example, a disk error) occurred while
   * processing the requested operation.
   */
  public final static int NFS3ERR_IO = 5;
  
  /** I/O error. No such device or address. */
  public final static int NFS3ERR_NXIO = 6;
  
  /**
   * Permission denied. The caller does not have the correct permission to
   * perform the requested operation. Contrast this with NFS3ERR_PERM, which
   * restricts itself to owner or privileged user permission failures.
   */
  public final static int NFS3ERR_ACCES = 13;
  
  /** File exists. The file specified already exists. */
  public final static int NFS3ERR_EXIST = 17;
  
  /** Attempt to do a cross-device hard link. */
  public final static int NFS3ERR_XDEV = 18;
  
  /** No such device. */
  public final static int NFS3ERR_NODEV = 19;
  
  /** The caller specified a non-directory in a directory operation. */
  public static int NFS3ERR_NOTDIR = 20;
  
  /** The caller specified a directory in a non-directory operation. */
  public final static int NFS3ERR_ISDIR = 21;
  
  /**
   * Invalid argument or unsupported argument for an operation. Two examples are
   * attempting a READLINK on an object other than a symbolic link or attempting
   * to SETATTR a time field on a server that does not support this operation.
   */
  public final static int NFS3ERR_INVAL = 22;
  
  /**
   * File too large. The operation would have caused a file to grow beyond the
   * server's limit.
   */
  public final static int NFS3ERR_FBIG = 27;
  
  /**
   * No space left on device. The operation would have caused the server's file
   * system to exceed its limit.
   */
  public final static int NFS3ERR_NOSPC = 28;
  
  /**
   * Read-only file system. A modifying operation was attempted on a read-only
   * file system.
   */
  public final static int NFS3ERR_ROFS = 30;
  
  /** Too many hard links. */
  public final static int NFS3ERR_MLINK = 31;
  
  /** The filename in an operation was too long. */
  public final static int NFS3ERR_NAMETOOLONG = 63;
  
  /** An attempt was made to remove a directory that was not empty. */
  public final static int NFS3ERR_NOTEMPTY = 66;
  
  /**
   * Resource (quota) hard limit exceeded. The user's resource limit on the
   * server has been exceeded.
   */
  public final static int NFS3ERR_DQUOT = 69;
  
  /**
   * The file handle given in the arguments was invalid. The file referred to by
   * that file handle no longer exists or access to it has been revoked.
   */
  public final static int NFS3ERR_STALE = 70;
  
  /**
   * The file handle given in the arguments referred to a file on a non-local
   * file system on the server.
   */
  public final static int NFS3ERR_REMOTE = 71;
  
  /** The file handle failed internal consistency checks */
  public final static int NFS3ERR_BADHANDLE = 10001;
  
  /**
   * Update synchronization mismatch was detected during a SETATTR operation.
   */
  public final static int NFS3ERR_NOT_SYNC = 10002;
  
  /** READDIR or READDIRPLUS cookie is stale */
  public final static int NFS3ERR_BAD_COOKIE = 10003;
  
  /** Operation is not supported */
  public final static int NFS3ERR_NOTSUPP = 10004;
  
  /** Buffer or request is too small */
  public final static int NFS3ERR_TOOSMALL = 10005;
  
  /**
   * An error occurred on the server which does not map to any of the legal NFS
   * version 3 protocol error values. The client should translate this into an
   * appropriate error. UNIX clients may choose to translate this to EIO.
   */
  public final static int NFS3ERR_SERVERFAULT = 10006;
  
  /**
   * An attempt was made to create an object of a type not supported by the
   * server.
   */
  public final static int NFS3ERR_BADTYPE = 10007;
  
  /**
   * The server initiated the request, but was not able to complete it in a
   * timely fashion. The client should wait and then try the request with a new
   * RPC transaction ID. For example, this error should be returned from a
   * server that supports hierarchical storage and receives a request to process
   * a file that has been migrated. In this case, the server should start the
   * immigration process and respond to client with this error.
   */
  public final static int NFS3ERR_JUKEBOX = 10008;
}
