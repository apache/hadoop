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

import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.Nfs3FileAttributes;
import org.apache.hadoop.nfs.nfs3.Nfs3Status;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.oncrpc.security.Verifier;

/**
 * SYMLINK3 Response
 */
public class SYMLINK3Response extends NFS3Response {
  private final FileHandle objFileHandle;
  private final Nfs3FileAttributes objPostOpAttr;
  private final WccData dirWcc;
  
  public SYMLINK3Response(int status) {
    this(status, null, null, new WccData(null, null));
  }
  
  public SYMLINK3Response(int status, FileHandle handle,
      Nfs3FileAttributes attrs, WccData dirWcc) {
    super(status);
    this.objFileHandle = handle;
    this.objPostOpAttr = attrs;
    this.dirWcc = dirWcc;
  }
  
  public FileHandle getObjFileHandle() {
    return objFileHandle;
  }

  public Nfs3FileAttributes getObjPostOpAttr() {
    return objPostOpAttr;
  }

  public WccData getDirWcc() {
    return dirWcc;
  }

  public static SYMLINK3Response deserialize(XDR xdr) {
    int status = xdr.readInt();
    FileHandle objFileHandle = new FileHandle();
    Nfs3FileAttributes objPostOpAttr = null;
    WccData dirWcc;
    if (status == Nfs3Status.NFS3_OK) {
      xdr.readBoolean();
      objFileHandle.deserialize(xdr);
      xdr.readBoolean();
      objPostOpAttr = Nfs3FileAttributes.deserialize(xdr);
    }

    dirWcc = WccData.deserialize(xdr);
    return new SYMLINK3Response(status, objFileHandle, objPostOpAttr, dirWcc);
  }

  @Override
  public XDR serialize(XDR out, int xid, Verifier verifier) {
    super.serialize(out, xid, verifier);
    if (this.getStatus() == Nfs3Status.NFS3_OK) {
      out.writeBoolean(true);
      objFileHandle.serialize(out);
      out.writeBoolean(true);
      objPostOpAttr.serialize(out);
    }
    dirWcc.serialize(out);
    
    return out;
  }
}