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
 * READDIR3 Request
 */
public class READDIR3Request extends RequestWithHandle {
  private final long cookie;
  private final long cookieVerf;
  private final int count;

  public static READDIR3Request deserialize(XDR xdr) throws IOException {
    FileHandle handle = readHandle(xdr);
    long cookie = xdr.readHyper();
    long cookieVerf = xdr.readHyper();
    int count = xdr.readInt();
    return new READDIR3Request(handle, cookie, cookieVerf, count);
  }
  
  public READDIR3Request(FileHandle handle, long cookie, long cookieVerf,
      int count) {
    super(handle);
    this.cookie = cookie;
    this.cookieVerf = cookieVerf;
    this.count = count;
  }
  
  public long getCookie() {
    return this.cookie;
  }

  public long getCookieVerf() {
    return this.cookieVerf;
  }

  public long getCount() {
    return this.count;
  }

  @Override
  public void serialize(XDR xdr) {
    handle.serialize(xdr);
    xdr.writeLongAsHyper(cookie);
    xdr.writeLongAsHyper(cookieVerf);
    xdr.writeInt(count);
  }
}