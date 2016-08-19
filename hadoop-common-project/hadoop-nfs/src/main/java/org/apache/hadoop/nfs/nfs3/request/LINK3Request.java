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
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.oncrpc.XDR;

/**
 * LINK3 Request
 */
public class LINK3Request extends RequestWithHandle {
  private final FileHandle fromDirHandle;
  private final String fromName;

  public LINK3Request(FileHandle handle, FileHandle fromDirHandle,
      String fromName) {
    super(handle);
    this.fromDirHandle = fromDirHandle;
    this.fromName = fromName;
  }

  public static LINK3Request deserialize(XDR xdr) throws IOException {
    FileHandle handle = readHandle(xdr);
    FileHandle fromDirHandle = readHandle(xdr);
    String fromName = xdr.readString();
    return new LINK3Request(handle, fromDirHandle, fromName);
  }

  public FileHandle getFromDirHandle() {
    return fromDirHandle;
  }

  public String getFromName() {
    return fromName;
  }

  @Override
  public void serialize(XDR xdr) {
    handle.serialize(xdr);
    fromDirHandle.serialize(xdr);
    xdr.writeInt(fromName.length());
    xdr.writeFixedOpaque(fromName.getBytes(StandardCharsets.UTF_8),
        fromName.length());
  }
}
