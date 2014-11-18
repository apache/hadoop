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
 * GETATTR3 Response
 */
public class GETATTR3Response extends NFS3Response {
  private Nfs3FileAttributes postOpAttr;
  public GETATTR3Response(int status) {
    this(status, new Nfs3FileAttributes());
  }
  
  public GETATTR3Response(int status, Nfs3FileAttributes attrs) {
    super(status);
    this.postOpAttr = attrs;
  }
  
  public void setPostOpAttr(Nfs3FileAttributes postOpAttr) {
    this.postOpAttr = postOpAttr;
  }
  
  public static GETATTR3Response deserialize(XDR xdr) {
    int status = xdr.readInt();
    Nfs3FileAttributes attr = (status == Nfs3Status.NFS3_OK) ? Nfs3FileAttributes
        .deserialize(xdr) : new Nfs3FileAttributes();
    return new GETATTR3Response(status, attr);
  }

  @Override
  public XDR serialize(XDR out, int xid, Verifier verifier) {
    super.serialize(out, xid, verifier);
    if (getStatus() == Nfs3Status.NFS3_OK) {
      postOpAttr.serialize(out);
    }
    return out;
  }
}