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

import org.apache.hadoop.nfs.nfs3.response.NFS3Response;
import org.apache.hadoop.oncrpc.RpcInfo;
import org.apache.hadoop.oncrpc.XDR;

/**
 * RPC procedures as defined in RFC 1813.
 */
public interface Nfs3Interface {

  /** NULL: Do nothing */
  public NFS3Response nullProcedure();

  /** GETATTR: Get file attributes */
  public NFS3Response getattr(XDR xdr, RpcInfo info);

  /** SETATTR: Set file attributes */
  public NFS3Response setattr(XDR xdr, RpcInfo info);

  /** LOOKUP: Lookup filename */
  public NFS3Response lookup(XDR xdr, RpcInfo info);

  /** ACCESS: Check access permission */
  public NFS3Response access(XDR xdr, RpcInfo info);

    /** READLINK: Read from symbolic link */
  public NFS3Response readlink(XDR xdr, RpcInfo info);

  /** READ: Read from file */
  public NFS3Response read(XDR xdr, RpcInfo info);

  /** WRITE: Write to file */
  public NFS3Response write(XDR xdr, RpcInfo info);

  /** CREATE: Create a file */
  public NFS3Response create(XDR xdr, RpcInfo info);

  /** MKDIR: Create a directory */
  public NFS3Response mkdir(XDR xdr, RpcInfo info);

  /** SYMLINK: Create a symbolic link */
  public NFS3Response symlink(XDR xdr, RpcInfo info);

  /** MKNOD: Create a special device */
  public NFS3Response mknod(XDR xdr, RpcInfo info);

  /** REMOVE: Remove a file */
  public NFS3Response remove(XDR xdr, RpcInfo info);

  /** RMDIR: Remove a directory */
  public NFS3Response rmdir(XDR xdr, RpcInfo info);

  /** RENAME: Rename a file or directory */
  public NFS3Response rename(XDR xdr, RpcInfo info);

  /** LINK: create link to an object */
  public NFS3Response link(XDR xdr, RpcInfo info);

  /** READDIR: Read From directory */
  public NFS3Response readdir(XDR xdr, RpcInfo info);

  /** READDIRPLUS: Extended read from directory */
  public NFS3Response readdirplus(XDR xdr, RpcInfo info);
  
  /** FSSTAT: Get dynamic file system information */
  public NFS3Response fsstat(XDR xdr, RpcInfo info);

  /** FSINFO: Get static file system information */
  public NFS3Response fsinfo(XDR xdr, RpcInfo info);

  /** PATHCONF: Retrieve POSIX information */
  public NFS3Response pathconf(XDR xdr, RpcInfo info);

  /** COMMIT: Commit cached data on a server to stable storage */
  public NFS3Response commit(XDR xdr, RpcInfo info);
}
