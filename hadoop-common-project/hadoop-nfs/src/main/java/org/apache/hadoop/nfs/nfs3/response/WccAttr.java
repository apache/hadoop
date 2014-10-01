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

import org.apache.hadoop.nfs.NfsTime;
import org.apache.hadoop.oncrpc.XDR;

/**
 * WccAttr saves attributes used for weak cache consistency
 */
public class WccAttr {
  long size;
  NfsTime mtime; // in milliseconds
  NfsTime ctime; // in milliseconds

  public long getSize() {
    return size;
  }

  public NfsTime getMtime() {
    return mtime;
  }

  public NfsTime getCtime() {
    return ctime;
  }

  public WccAttr() {
    this.size = 0;
    mtime = null;
    ctime = null;
  }

  public WccAttr(long size, NfsTime mtime, NfsTime ctime) {
    this.size = size;
    this.mtime = mtime;
    this.ctime = ctime;
  }

  public static WccAttr deserialize(XDR xdr) {
    long size = xdr.readHyper();
    NfsTime mtime = NfsTime.deserialize(xdr);
    NfsTime ctime = NfsTime.deserialize(xdr);
    return new WccAttr(size, mtime, ctime);
  }

  public void serialize(XDR out) {
    out.writeLongAsHyper(size);
    if (mtime == null) {
      mtime = new NfsTime(0);
    }
    mtime.serialize(out);
    if (ctime == null) {
      ctime = new NfsTime(0);
    }
    ctime.serialize(out);
  }
}