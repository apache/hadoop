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
package org.apache.hadoop.nfs.nfs3.request;

import java.io.IOException;

import org.apache.hadoop.nfs.NfsTime;
import org.apache.hadoop.oncrpc.XDR;

/**
 * SETATTR3 Request
 */
public class SETATTR3Request extends RequestWithHandle {
  private final SetAttr3 attr;

  /* A client may request that the server check that the object is in an
   * expected state before performing the SETATTR operation. If guard.check is
   * TRUE, the server must compare the value of ctime to the current ctime of
   * the object. If the values are different, the server must preserve the
   * object attributes and must return a status of NFS3ERR_NOT_SYNC. If check is
   * FALSE, the server will not perform this check.
   */
  private final boolean check;
  private final NfsTime ctime;
  
  public SETATTR3Request(XDR xdr) throws IOException {
    super(xdr);
    attr = new SetAttr3();
    attr.deserialize(xdr);
    check = xdr.readBoolean();
    if (check) {
      ctime = NfsTime.deserialize(xdr);
    } else {
      ctime = null;
    }
  }
  
  public SetAttr3 getAttr() {
    return attr;
  }

  public boolean isCheck() {
    return check;
  }

  public NfsTime getCtime() {
    return ctime;
  }
}