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
package org.apache.hadoop.nfs.nfs3;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.oncrpc.XDR;

/**
 * This is a file handle use by the NFS clients.
 * Server returns this handle to the client, which is used by the client
 * on subsequent operations to reference the file.
 */
public class FileHandle {
  private static final Log LOG = LogFactory.getLog(FileHandle.class);
  private static final String HEXES = "0123456789abcdef";
  private static final int HANDLE_LEN = 32;
  private byte[] handle; // Opaque handle
  private long fileId = -1;

  public FileHandle() {
    handle = null;
  }

  /**
   * Handle is a 32 bytes number. For HDFS, the last 8 bytes is fileId.
   */
  public FileHandle(long v) {
    fileId = v;
    handle = new byte[HANDLE_LEN];
    handle[0] = (byte)(v >>> 56);
    handle[1] = (byte)(v >>> 48);
    handle[2] = (byte)(v >>> 40);
    handle[3] = (byte)(v >>> 32);
    handle[4] = (byte)(v >>> 24);
    handle[5] = (byte)(v >>> 16);
    handle[6] = (byte)(v >>>  8);
    handle[7] = (byte)(v >>>  0);
    for (int i = 8; i < HANDLE_LEN; i++) {
      handle[i] = (byte) 0;
    }
  }
  
  public FileHandle(String s) {
    MessageDigest digest;
    try {
      digest = MessageDigest.getInstance("MD5");
      handle = new byte[HANDLE_LEN];
    } catch (NoSuchAlgorithmException e) {
      LOG.warn("MD5 MessageDigest unavailable.");
      handle = null;
      return;
    }

    byte[] in = s.getBytes();
    for (int i = 0; i < in.length; i++) {
      digest.update(in[i]);
    }

    byte[] digestbytes = digest.digest();
    for (int i = 0; i < 16; i++) {
      handle[i] = (byte) 0;
    }

    for (int i = 16; i < 32; i++) {
      handle[i] = digestbytes[i - 16];
    }
  }

  public boolean serialize(XDR out) {
    out.writeInt(handle.length);
    out.writeFixedOpaque(handle);
    return true;
  }

  private long bytesToLong(byte[] data) {
    ByteBuffer buffer = ByteBuffer.allocate(8);
    for (int i = 0; i < 8; i++) {
      buffer.put(data[i]);
    }
    buffer.flip();// need flip
    return buffer.getLong();
  }
  
  public boolean deserialize(XDR xdr) {
    if (!XDR.verifyLength(xdr, 32)) {
      return false;
    }
    int size = xdr.readInt();
    handle = xdr.readFixedOpaque(size);
    fileId = bytesToLong(handle);
    return true;
  }
  
  private static String hex(byte b) {
    StringBuilder strBuilder = new StringBuilder();
    strBuilder.append(HEXES.charAt((b & 0xF0) >> 4)).append(
        HEXES.charAt((b & 0x0F)));
    return strBuilder.toString();
  }
  
  public long getFileId() {    
    return fileId;
  }
  
  public byte[] getContent() {
    return handle.clone();
  }
  
  @Override
  public String toString() {
    StringBuilder s = new StringBuilder();
    for (int i = 0; i < handle.length; i++) {
      s.append(hex(handle[i]));
    }
    return s.toString();
  }
  
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof FileHandle)) {
      return false;
    }

    FileHandle h = (FileHandle) o;
    return Arrays.equals(handle, h.handle);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(handle);
  }
}
