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

import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.oncrpc.security.Verifier;

/**
 * RMDIR3 Response
 */
public class RMDIR3Response extends NFS3Response {
  private final WccData dirWcc;

  public RMDIR3Response(int status) {
    this(status, new WccData(null, null));
  }

  public RMDIR3Response(int status, WccData wccData) {
    super(status);
    this.dirWcc = wccData;
  }
  
  public WccData getDirWcc() {
    return dirWcc;
  }

  public static RMDIR3Response deserialize(XDR xdr) {
    int status = xdr.readInt();
    WccData dirWcc = WccData.deserialize(xdr);
    return new RMDIR3Response(status, dirWcc);
  }

  @Override
  public XDR serialize(XDR out, int xid, Verifier verifier) {
    super.serialize(out, xid, verifier);
    dirWcc.serialize(out);
    return out;
  }
}
