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
 * ACCESS3 Response 
 */
public class ACCESS3Response extends NFS3Response {
  /*
   * A bit mask of access permissions indicating access rights for the
   * authentication credentials provided with the request.
   */
  private final int access;
  private final Nfs3FileAttributes postOpAttr;
  
  public ACCESS3Response(int status) {
    this(status, new Nfs3FileAttributes(), 0);
  }
  
  public ACCESS3Response(int status, Nfs3FileAttributes postOpAttr, int access) {
    super(status);
    this.postOpAttr = postOpAttr;
    this.access = access;
  }

  public static ACCESS3Response deserialize(XDR xdr) {
    int status = xdr.readInt();
    Nfs3FileAttributes postOpAttr = null;
    int access = 0;

    if (status == Nfs3Status.NFS3_OK) {
      postOpAttr = Nfs3FileAttributes.deserialize(xdr);
      access = xdr.readInt();
    }
    return new ACCESS3Response(status, postOpAttr, access);
  }

  @Override
  public XDR serialize(XDR out, int xid, Verifier verifier) {
    super.serialize(out, xid, verifier);
    if (this.getStatus() == Nfs3Status.NFS3_OK) {
      out.writeBoolean(true);
      postOpAttr.serialize(out);
      out.writeInt(access);
    } else {
      out.writeBoolean(false);
    }
    return out;
  }
}