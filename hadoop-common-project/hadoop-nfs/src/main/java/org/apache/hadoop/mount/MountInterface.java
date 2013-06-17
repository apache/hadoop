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
package org.apache.hadoop.mount;

import java.net.InetAddress;

import org.apache.hadoop.oncrpc.XDR;

/**
 * This is an interface that should be implemented for handle Mountd related
 * requests. See RFC 1094 for more details.
 */
public interface MountInterface {
  public static int MNTPROC_NULL = 0;
  public static int MNTPROC_MNT = 1;
  public static int MNTPROC_DUMP = 2;
  public static int MNTPROC_UMNT = 3;
  public static int MNTPROC_UMNTALL = 4;
  public static int MNTPROC_EXPORT = 5;
  public static int MNTPROC_EXPORTALL = 6;
  public static int MNTPROC_PATHCONF = 7;

  /** MNTPROC_NULL - Do Nothing */
  public XDR nullOp(XDR out, int xid, InetAddress client);

  /** MNTPROC_MNT - Add mount entry */
  public XDR mnt(XDR xdr, XDR out, int xid, InetAddress client);

  /** MNTPROC_DUMP - Return mount entries */
  public XDR dump(XDR out, int xid, InetAddress client);

  /** MNTPROC_UMNT - Remove mount entry */
  public XDR umnt(XDR xdr, XDR out, int xid, InetAddress client);

  /** MNTPROC_UMNTALL - Remove all mount entries */
  public XDR umntall(XDR out, int xid, InetAddress client);
  
  /** MNTPROC_EXPORT and MNTPROC_EXPORTALL - Return export list */
  //public XDR exportall(XDR out, int xid, InetAddress client);
  
  /** MNTPROC_PATHCONF - POSIX pathconf information */
  //public XDR pathconf(XDR out, int xid, InetAddress client);
}
