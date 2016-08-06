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
 * An NFS request that uses {@link FileHandle} to identify a file.
 */
public abstract class NFS3Request {
  
  /**
   * Deserialize a handle from an XDR object
   */
  static FileHandle readHandle(XDR xdr) throws IOException {
    FileHandle handle = new FileHandle();
    if (!handle.deserialize(xdr)) {
      throw new IOException("can't deserialize file handle");
    }
    return handle;
  }
  
  /**
   * Subclass should implement. Usually handle is the first to be serialized
   * @param xdr XDR message
   */
  public abstract void serialize(XDR xdr);
}
