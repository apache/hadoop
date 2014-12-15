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

import org.apache.commons.io.Charsets;
import org.apache.hadoop.nfs.NfsExports;
import org.apache.hadoop.oncrpc.RpcAcceptedReply;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.oncrpc.security.VerifierNone;
import org.apache.hadoop.oncrpc.security.RpcAuthInfo.AuthFlavor;

/**
 * Helper class for sending MountResponse
 */
public class MountResponse {
  public final static int MNT_OK = 0;
  
  /** Hidden constructor */
  private MountResponse() {
  }
  
  /** Response for RPC call {@link MountInterface.MNTPROC#MNT} */
  public static XDR writeMNTResponse(int status, XDR xdr, int xid,
      byte[] handle) {
    RpcAcceptedReply.getAcceptInstance(xid, new VerifierNone()).write(xdr);
    xdr.writeInt(status);
    if (status == MNT_OK) {
      xdr.writeVariableOpaque(handle);
      // Only MountV3 returns a list of supported authFlavors
      xdr.writeInt(1);
      xdr.writeInt(AuthFlavor.AUTH_SYS.getValue());
    }
    return xdr;
  }

  /** Response for RPC call {@link MountInterface.MNTPROC#DUMP} */
  public static XDR writeMountList(XDR xdr, int xid, List<MountEntry> mounts) {
    RpcAcceptedReply.getAcceptInstance(xid, new VerifierNone()).write(xdr);
    for (MountEntry mountEntry : mounts) {
      xdr.writeBoolean(true); // Value follows yes
      xdr.writeString(mountEntry.getHost());
      xdr.writeString(mountEntry.getPath());
    }
    xdr.writeBoolean(false); // Value follows no
    return xdr;
  }
  
  /** Response for RPC call {@link MountInterface.MNTPROC#EXPORT} */
  public static XDR writeExportList(XDR xdr, int xid, List<String> exports,
      List<NfsExports> hostMatcher) {
    assert (exports.size() == hostMatcher.size());

    RpcAcceptedReply.getAcceptInstance(xid, new VerifierNone()).write(xdr);
    for (int i = 0; i < exports.size(); i++) {
      xdr.writeBoolean(true); // Value follows - yes
      xdr.writeString(exports.get(i));

      // List host groups
      String[] hostGroups = hostMatcher.get(i).getHostGroupList();
      if (hostGroups.length > 0) {
        for (int j = 0; j < hostGroups.length; j++) {
          xdr.writeBoolean(true); // Value follows - yes
          xdr.writeVariableOpaque(hostGroups[j].getBytes(Charsets.UTF_8));
        }
      }
      xdr.writeBoolean(false); // Value follows - no more group
    }
    
    xdr.writeBoolean(false); // Value follows - no
    return xdr;
  }
}
