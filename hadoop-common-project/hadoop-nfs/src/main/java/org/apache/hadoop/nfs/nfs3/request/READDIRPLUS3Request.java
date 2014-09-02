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

import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.oncrpc.XDR;

/**
 * READDIRPLUS3 Request
 */
public class READDIRPLUS3Request extends RequestWithHandle {
  private final long cookie;
  private final long cookieVerf;
  private final int dirCount;
  private final int maxCount;

  public static READDIRPLUS3Request deserialize(XDR xdr) throws IOException {
    FileHandle handle = readHandle(xdr);
    long cookie = xdr.readHyper();
    long cookieVerf = xdr.readHyper();
    int dirCount = xdr.readInt();
    int maxCount = xdr.readInt();
    return new READDIRPLUS3Request(handle, cookie, cookieVerf, dirCount,
        maxCount);
  }

  public READDIRPLUS3Request(FileHandle handle, long cookie, long cookieVerf,
      int dirCount, int maxCount) {
    super(handle);
    this.cookie = cookie;
    this.cookieVerf = cookieVerf;
    this.dirCount = dirCount;
    this.maxCount = maxCount;
  }
  
  public long getCookie() {
    return this.cookie;
  }

  public long getCookieVerf() {
    return this.cookieVerf;
  }

  public int getDirCount() {
    return dirCount;
  }

  public int getMaxCount() {
    return maxCount;
  }

  @Override
  public void serialize(XDR xdr) {
    handle.serialize(xdr);
    xdr.writeLongAsHyper(cookie);
    xdr.writeLongAsHyper(cookieVerf);
    xdr.writeInt(dirCount);
    xdr.writeInt(maxCount);
  }
}