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

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * READ3 Request
 */
public class READ3Request extends RequestWithHandle {
  private final long offset;
  private final int count;

  public static READ3Request deserialize(XDR xdr) throws IOException {
    FileHandle handle = readHandle(xdr);
    long offset = xdr.readHyper();
    int count = xdr.readInt();
    return new READ3Request(handle, offset, count);
  }

  @VisibleForTesting
  public READ3Request(FileHandle handle, long offset, int count) {
    super(handle);
    this.offset = offset;
    this.count = count;
  }
  
  public long getOffset() {
    return this.offset;
  }
  
  public int getCount() {
    return this.count;
  }
  
  @Override
  public void serialize(XDR xdr) {
    handle.serialize(xdr);
    xdr.writeLongAsHyper(offset);
    xdr.writeInt(count);
  }
}