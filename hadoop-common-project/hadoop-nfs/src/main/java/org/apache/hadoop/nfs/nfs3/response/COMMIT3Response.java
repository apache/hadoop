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

import org.apache.hadoop.nfs.nfs3.Nfs3Constant;
import org.apache.hadoop.nfs.nfs3.Nfs3Status;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.oncrpc.security.Verifier;

/**
 * COMMIT3 Response
 */
public class COMMIT3Response extends NFS3Response {
  private final WccData fileWcc;
  private final long verf;

  public COMMIT3Response(int status) {
    this(status, new WccData(null, null), Nfs3Constant.WRITE_COMMIT_VERF);
  }

  public COMMIT3Response(int status, WccData fileWcc, long verf) {
    super(status);
    this.fileWcc = fileWcc;
    this.verf = verf;
  }
  
  public WccData getFileWcc() {
    return fileWcc;
  }

  public long getVerf() {
    return verf;
  }

  public static COMMIT3Response deserialize(XDR xdr) {
    int status = xdr.readInt();
    long verf = 0;
    WccData fileWcc = WccData.deserialize(xdr);
    if (status == Nfs3Status.NFS3_OK) {
      verf = xdr.readHyper();
    }
    return new COMMIT3Response(status, fileWcc, verf);
  }

  @Override
  public XDR serialize(XDR out, int xid, Verifier verifier) {
    super.serialize(out, xid, verifier);
    fileWcc.serialize(out);
    if (getStatus() == Nfs3Status.NFS3_OK) {
      out.writeLongAsHyper(verf);
    }
    return out;
  }
}
