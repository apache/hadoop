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
 * CREATE3 Response
 */
public class CREATE3Response extends NFS3Response {
  private final FileHandle objHandle;
  private final Nfs3FileAttributes postOpObjAttr;
  private WccData dirWcc;

  public CREATE3Response(int status) {
    this(status, null, null, null);
  }

  public CREATE3Response(int status, FileHandle handle,
      Nfs3FileAttributes postOpObjAttr, WccData dirWcc) {
    super(status);
    this.objHandle = handle;
    this.postOpObjAttr = postOpObjAttr;
    this.dirWcc = dirWcc;
  }

  public FileHandle getObjHandle() {
    return objHandle;
  }

  public Nfs3FileAttributes getPostOpObjAttr() {
    return postOpObjAttr;
  }

  public WccData getDirWcc() {
    return dirWcc;
  }

  public static CREATE3Response deserialize(XDR xdr) {
    int status = xdr.readInt();
    FileHandle objHandle = new FileHandle();
    Nfs3FileAttributes postOpObjAttr = null;

    if (status == Nfs3Status.NFS3_OK) {
      xdr.readBoolean();
      objHandle.deserialize(xdr);
      xdr.readBoolean();
      postOpObjAttr = Nfs3FileAttributes.deserialize(xdr);
    }

    WccData dirWcc = WccData.deserialize(xdr);
    return new CREATE3Response(status, objHandle, postOpObjAttr, dirWcc);
  }

  @Override
  public XDR serialize(XDR out, int xid, Verifier verifier) {
    super.serialize(out, xid, verifier);
    if (getStatus() == Nfs3Status.NFS3_OK) {
      out.writeBoolean(true); // Handle follows
      objHandle.serialize(out);
      out.writeBoolean(true); // Attributes follow
      postOpObjAttr.serialize(out);
    }
    if (dirWcc == null) {
      dirWcc = new WccData(null, null);
    }
    dirWcc.serialize(out);

    return out;
  }
}