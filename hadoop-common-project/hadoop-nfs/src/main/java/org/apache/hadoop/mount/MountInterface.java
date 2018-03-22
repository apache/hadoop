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
  /** Mount procedures */
  public enum MNTPROC {
    // the order of the values below are significant.
    NULL,
    MNT,
    DUMP,
    UMNT,
    UMNTALL,
    EXPORT,
    EXPORTALL,
    PATHCONF;

    /** @return the int value representing the procedure. */
    public int getValue() {
      return ordinal();
    }

    /** The procedure of given value.
     * @param value specifies the procedure index
     * @return the procedure corresponding to the value.
     */
    public static MNTPROC fromValue(int value) {
      if (value < 0 || value >= values().length) {
        return null;
      }
      return values()[value];
    }
  }

  /**
   * MNTPRC_NULL - Do Nothing.
   * @param out XDR response used in NFS protocol
   * @param xid transaction id
   * @param client represents IP address
   * @return XDR response
   */
  public XDR nullOp(XDR out, int xid, InetAddress client);

  /**
   * MNTPROC_MNT - Add mount entry.
   * @param xdr XDR message used in NFS protocol
   * @param out XDR response used in NFS protocol
   * @param xid transaction id
   * @param client represents IP address
   * @return XDR response
   */
  public XDR mnt(XDR xdr, XDR out, int xid, InetAddress client);

  /**
   * MNTPROC_DUMP - Return mount entries.
   * @param out XDR response used in NFS protocol
   * @param xid transaction id
   * @param client represents IP address
   * @return XDR response
   */
  public XDR dump(XDR out, int xid, InetAddress client);

  /**
   * MNTPROC_UMNT - Remove mount entry.
   * @param xdr XDR message used in NFS protocol
   * @param out XDR response used in NFS protocol
   * @param xid transaction id
   * @param client represents IP address
   * @return XDR response
   */
  public XDR umnt(XDR xdr, XDR out, int xid, InetAddress client);

  /**
   * MNTPROC_UMNTALL - Remove all mount entries.
   * @param out XDR response used in NFS protocol
   * @param xid transaction id
   * @param client represents IP address
   * @return XDR response
   */
  public XDR umntall(XDR out, int xid, InetAddress client);
  
  /** MNTPROC_EXPORT and MNTPROC_EXPORTALL - Return export list */
  //public XDR exportall(XDR out, int xid, InetAddress client);
  
  /** MNTPROC_PATHCONF - POSIX pathconf information */
  //public XDR pathconf(XDR out, int xid, InetAddress client);
}
