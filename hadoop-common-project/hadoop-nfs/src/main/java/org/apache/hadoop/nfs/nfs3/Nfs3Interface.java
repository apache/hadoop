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
import org.apache.hadoop.oncrpc.RpcAuthSys;
import org.apache.hadoop.oncrpc.XDR;
import org.jboss.netty.channel.Channel;

/**
 * RPC procedures as defined in RFC 1813.
 */
public interface Nfs3Interface {
  
  /** NULL: Do nothing */
  public NFS3Response nullProcedure();
  
  /** GETATTR: Get file attributes */
  public NFS3Response getattr(XDR xdr, RpcAuthSys authSys);
  
  /** SETATTR: Set file attributes */
  public NFS3Response setattr(XDR xdr, RpcAuthSys authSys);
  
  /** LOOKUP: Lookup filename */
  public NFS3Response lookup(XDR xdr, RpcAuthSys authSys);
  
  /** ACCESS: Check access permission  */
  public NFS3Response access(XDR xdr, RpcAuthSys authSys);
  
  /** READ: Read from file */
  public NFS3Response read(XDR xdr, RpcAuthSys authSys);
  
  /** WRITE: Write to file */
  public NFS3Response write(XDR xdr, Channel channel, int xid, RpcAuthSys authSys);
  
  /** CREATE: Create a file  */
  public NFS3Response create(XDR xdr, RpcAuthSys authSys);
  
  /** MKDIR: Create a directory  */
  public NFS3Response mkdir(XDR xdr, RpcAuthSys authSys);
  
  /** REMOVE: Remove a file  */
  public NFS3Response remove(XDR xdr, RpcAuthSys authSys);
  
  /** RMDIR: Remove a directory  */
  public NFS3Response rmdir(XDR xdr, RpcAuthSys authSys);
  
  /** RENAME: Rename a file or directory */
  public NFS3Response rename(XDR xdr, RpcAuthSys authSys);
  
  /** SYMLINK: Create a symbolic link  */
  public NFS3Response symlink(XDR xdr, RpcAuthSys authSys);
  
  /** READDIR: Read From directory */
  public NFS3Response readdir(XDR xdr, RpcAuthSys authSys);
  
  /** FSSTAT: Get dynamic file system information  */
  public NFS3Response fsstat(XDR xdr, RpcAuthSys authSys);
  
  /** FSINFO: Get static file system information */
  public NFS3Response fsinfo(XDR xdr, RpcAuthSys authSys);
  
  /** PATHCONF: Retrieve POSIX information */
  public NFS3Response pathconf(XDR xdr, RpcAuthSys authSys);
  
  /** COMMIT: Commit cached data on a server to stable storage  */
  public NFS3Response commit(XDR xdr, RpcAuthSys authSys);
}
