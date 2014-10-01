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
 * RENAME3 Response
 */
public class RENAME3Response extends NFS3Response {
  private final WccData fromDirWcc;
  private final WccData toDirWcc;

  public RENAME3Response(int status) {
    this(status, new WccData(null, null), new WccData(null, null));
  }
  
  public RENAME3Response(int status, WccData fromWccData, WccData toWccData) {
    super(status);
    this.fromDirWcc = fromWccData;
    this.toDirWcc = toWccData;
  }

  public WccData getFromDirWcc() {
    return fromDirWcc;
  }

  public WccData getToDirWcc() {
    return toDirWcc;
  }

  public static RENAME3Response deserialize(XDR xdr) {
    int status = xdr.readInt();
    WccData fromDirWcc = WccData.deserialize(xdr);
    WccData toDirWcc = WccData.deserialize(xdr);
    return new RENAME3Response(status, fromDirWcc, toDirWcc);
  }

  @Override
  public XDR serialize(XDR out, int xid, Verifier verifier) {
    super.serialize(out, xid, verifier);
    fromDirWcc.serialize(out);
    toDirWcc.serialize(out);
    return out;
  }
}