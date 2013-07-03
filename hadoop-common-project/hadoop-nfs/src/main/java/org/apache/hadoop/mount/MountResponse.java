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

import java.util.List;

import org.apache.hadoop.oncrpc.RpcAcceptedReply;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.oncrpc.RpcAuthInfo.AuthFlavor;

/**
 * Helper class for sending MountResponse
 */
public class MountResponse {
  public final static int MNT_OK = 0;
  
  /** Hidden constructor */
  private MountResponse() {
  }
  
  /** Response for RPC call {@link MountInterface#MNTPROC_MNT} */
  public static XDR writeMNTResponse(int status, XDR xdr, int xid,
      byte[] handle) {
    RpcAcceptedReply.voidReply(xdr, xid);
    xdr.writeInt(status);
    if (status == MNT_OK) {
      xdr.writeInt(handle.length);
      xdr.writeFixedOpaque(handle);
      // Only MountV3 returns a list of supported authFlavors
      xdr.writeInt(1);
      xdr.writeInt(AuthFlavor.AUTH_SYS.getValue());
    }
    return xdr;
  }

  /** Response for RPC call {@link MountInterface#MNTPROC_DUMP} */
  public static XDR writeMountList(XDR xdr, int xid, List<MountEntry> mounts) {
    RpcAcceptedReply.voidReply(xdr, xid);
    for (MountEntry mountEntry : mounts) {
      xdr.writeBoolean(true); // Value follows yes
      xdr.writeString(mountEntry.host());
      xdr.writeString(mountEntry.path());
    }
    xdr.writeBoolean(false); // Value follows no
    return xdr;
  }

  /** Response for RPC call {@link MountInterface#MNTPROC_EXPORT} */
  public static XDR writeExportList(XDR xdr, int xid, List<String> exports) {
    RpcAcceptedReply.voidReply(xdr, xid);
    for (String export : exports) {
      xdr.writeBoolean(true); // Value follows - yes
      xdr.writeString(export);
      xdr.writeInt(0);
    }
    xdr.writeBoolean(false); // Value follows - no
    return xdr;
  }
}
