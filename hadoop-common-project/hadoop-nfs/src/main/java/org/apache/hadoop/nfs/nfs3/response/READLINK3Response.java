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
package org.apache.hadoop.nfs.nfs3.response;

import org.apache.hadoop.nfs.nfs3.Nfs3FileAttributes;
import org.apache.hadoop.nfs.nfs3.Nfs3Status;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.oncrpc.security.Verifier;

/**
 * READLINK3 Response
 */
public class READLINK3Response extends NFS3Response {
  private final Nfs3FileAttributes postOpSymlinkAttr;
  private final byte path[];

  public READLINK3Response(int status) {
    this(status, new Nfs3FileAttributes(), new byte[0]);
  }

  public READLINK3Response(int status, Nfs3FileAttributes postOpAttr,
      byte path[]) {
    super(status);
    this.postOpSymlinkAttr = postOpAttr;
    this.path = new byte[path.length];
    System.arraycopy(path, 0, this.path, 0, path.length);
  }

  public static READLINK3Response deserialize(XDR xdr) {
    int status = xdr.readInt();
    xdr.readBoolean();
    Nfs3FileAttributes postOpSymlinkAttr = Nfs3FileAttributes.deserialize(xdr);
    byte path[] = new byte[0];

    if (status == Nfs3Status.NFS3_OK) {
      path = xdr.readVariableOpaque();
    }

    return new READLINK3Response(status, postOpSymlinkAttr, path);
  }

  @Override
  public XDR serialize(XDR out, int xid, Verifier verifier) {
    super.serialize(out, xid, verifier);
    out.writeBoolean(true); // Attribute follows
    postOpSymlinkAttr.serialize(out);
    if (getStatus() == Nfs3Status.NFS3_OK) {
      out.writeVariableOpaque(path);
    }
    return out;
  }
}