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
import java.nio.ByteBuffer;

import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant.WriteStableHow;
import org.apache.hadoop.oncrpc.XDR;

/**
 * WRITE3 Request
 */
public class WRITE3Request extends RequestWithHandle {
  private long offset;
  private int count;
  private final WriteStableHow stableHow;
  private final ByteBuffer data;

  public static WRITE3Request deserialize(XDR xdr) throws IOException {
    FileHandle handle = readHandle(xdr);
    long offset = xdr.readHyper();
    int count = xdr.readInt();
    WriteStableHow stableHow = WriteStableHow.fromValue(xdr.readInt());
    ByteBuffer data = ByteBuffer.wrap(xdr.readFixedOpaque(xdr.readInt()));
    return new WRITE3Request(handle, offset, count, stableHow, data);
  }

  public WRITE3Request(FileHandle handle, final long offset, final int count,
      final WriteStableHow stableHow, final ByteBuffer data) {
    super(handle);
    this.offset = offset;
    this.count = count;
    this.stableHow = stableHow;
    this.data = data;
  }

  public long getOffset() {
    return this.offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }
  
  public int getCount() {
    return this.count;
  }

  public void setCount(int count) {
    this.count = count;
  }
  
  public WriteStableHow getStableHow() {
    return this.stableHow;
  }

  public ByteBuffer getData() {
    return this.data;
  }

  @Override
  public void serialize(XDR xdr) {
    handle.serialize(xdr);
    xdr.writeLongAsHyper(offset);
    xdr.writeInt(count);
    xdr.writeInt(stableHow.getValue());
    xdr.writeInt(count);
    xdr.writeFixedOpaque(data.array(), count);
  }
  
  @Override
  public String toString() {
    return String.format("fileId: %d offset: %d count: %d stableHow: %s",
        handle.getFileId(), offset, count, stableHow.name());
  }
}