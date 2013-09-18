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
import org.apache.hadoop.nfs.nfs3.Nfs3Constant.WriteStableHow;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.oncrpc.security.Verifier;

/**
 * WRITE3 Response
 */
public class WRITE3Response extends NFS3Response {
  private final WccData fileWcc; // return on both success and failure
  private final int count;
  private final WriteStableHow stableHow;
  private final long verifer;

  public WRITE3Response(int status) {
    this(status, new WccData(null, null), 0, WriteStableHow.UNSTABLE,
        Nfs3Constant.WRITE_COMMIT_VERF);
  }
  
  public WRITE3Response(int status, WccData fileWcc, int count,
      WriteStableHow stableHow, long verifier) {
    super(status);
    this.fileWcc = fileWcc;
    this.count = count;
    this.stableHow = stableHow;
    this.verifer = verifier;
  }

  public int getCount() {
    return count;
  }

  public WriteStableHow getStableHow() {
    return stableHow;
  }

  public long getVerifer() {
    return verifer;
  }

  @Override
  public XDR writeHeaderAndResponse(XDR out, int xid, Verifier verifier) {
    super.writeHeaderAndResponse(out, xid, verifier);
    fileWcc.serialize(out);
    if (getStatus() == Nfs3Status.NFS3_OK) {
      out.writeInt(count);
      out.writeInt(stableHow.getValue());
      out.writeLongAsHyper(verifer);
    }
    return out;
  }
}
