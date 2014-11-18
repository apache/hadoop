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
import org.apache.hadoop.oncrpc.XDR;

/**
 * WccData saved information used by client for weak cache consistency
 */
public class WccData {
  private WccAttr preOpAttr;
  private Nfs3FileAttributes postOpAttr;

  public WccAttr getPreOpAttr() {
    return preOpAttr;
  }

  public void setPreOpAttr(WccAttr preOpAttr) {
    this.preOpAttr = preOpAttr;
  }

  public Nfs3FileAttributes getPostOpAttr() {
    return postOpAttr;
  }

  public void setPostOpAttr(Nfs3FileAttributes postOpAttr) {
    this.postOpAttr = postOpAttr;
  }

  public WccData(WccAttr preOpAttr, Nfs3FileAttributes postOpAttr) {
    this.preOpAttr = (preOpAttr == null) ? new WccAttr() : preOpAttr;
    this.postOpAttr = (postOpAttr == null) ? new Nfs3FileAttributes()
        : postOpAttr;
  }

  public static WccData deserialize(XDR xdr) {
    xdr.readBoolean();
    WccAttr preOpAttr = WccAttr.deserialize(xdr);
    xdr.readBoolean();
    Nfs3FileAttributes postOpAttr = Nfs3FileAttributes.deserialize(xdr);
    return new WccData(preOpAttr, postOpAttr);
  }

  public void serialize(XDR out) {
    out.writeBoolean(true); // attributes follow
    preOpAttr.serialize(out);
    out.writeBoolean(true); // attributes follow
    postOpAttr.serialize(out);
  }
}