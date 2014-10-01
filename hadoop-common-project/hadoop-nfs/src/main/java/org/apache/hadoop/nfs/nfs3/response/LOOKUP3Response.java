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

import java.io.IOException;

import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.Nfs3FileAttributes;
import org.apache.hadoop.nfs.nfs3.Nfs3Status;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.oncrpc.security.Verifier;

/**
 * LOOKUP3 Response
 */
public class LOOKUP3Response extends NFS3Response {
  private final FileHandle fileHandle;
  private final Nfs3FileAttributes postOpObjAttr; // Can be null
  private final Nfs3FileAttributes postOpDirAttr; // Can be null

  public LOOKUP3Response(int status) {
    this(status, null, new Nfs3FileAttributes(), new Nfs3FileAttributes());
  }

  public LOOKUP3Response(int status, FileHandle fileHandle,
      Nfs3FileAttributes postOpObjAttr, Nfs3FileAttributes postOpDirAttributes) {
    super(status);
    this.fileHandle = fileHandle;
    this.postOpObjAttr = postOpObjAttr;
    this.postOpDirAttr = postOpDirAttributes;
  }

  public LOOKUP3Response(XDR xdr) throws IOException {
    super(-1);
    fileHandle = new FileHandle();
    status = xdr.readInt();
    Nfs3FileAttributes objAttr = null;
    if (status == Nfs3Status.NFS3_OK) {
      if (!fileHandle.deserialize(xdr)) {
        throw new IOException("can't deserialize file handle");
      }
      objAttr = xdr.readBoolean() ? Nfs3FileAttributes.deserialize(xdr) : null;
    }
    postOpObjAttr = objAttr;
    postOpDirAttr = xdr.readBoolean() ? Nfs3FileAttributes.deserialize(xdr)
        : null;
  }

  @Override
  public XDR serialize(XDR out, int xid, Verifier verifier) {
    super.serialize(out, xid, verifier);
    if (this.status == Nfs3Status.NFS3_OK) {
      fileHandle.serialize(out);
      out.writeBoolean(true); // Attribute follows
      postOpObjAttr.serialize(out);
    }

    out.writeBoolean(true); // Attribute follows
    postOpDirAttr.serialize(out);
    return out;
  }
}