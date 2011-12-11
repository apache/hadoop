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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.DataInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * An implementation of the abstract class {@link EditLogInputStream},
 * which is used to updates HDFS meta-data state on a backup node.
 * 
 * @see org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol#journal
 * (org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration,
 *  int, int, byte[])
 */
class EditLogBackupInputStream extends EditLogInputStream {
  String address; // sender address 
  private ByteBufferInputStream inner;
  private DataInputStream in;

  /**
   * A ByteArrayInputStream, which lets modify the underlying byte array.
   */
  private static class ByteBufferInputStream extends ByteArrayInputStream {
    ByteBufferInputStream() {
      super(new byte[0]);
    }

    byte[] getData() {
      return super.buf;
    }

    void setData(byte[] newBytes) {
      super.buf = newBytes;
      super.count = newBytes == null ? 0 : newBytes.length;
      super.mark = 0;
      reset();
    }

    /**
     * Number of bytes read from the stream so far.
     */
    int length() {
      return count;
    }
  }

  EditLogBackupInputStream(String name) throws IOException {
    address = name;
    inner = new ByteBufferInputStream();
    in = new DataInputStream(inner);
  }

  @Override // JournalStream
  public String getName() {
    return address;
  }

  @Override // JournalStream
  public JournalType getType() {
    return JournalType.BACKUP;
  }

  @Override
  public int available() throws IOException {
    return in.available();
  }

  @Override
  public int read() throws IOException {
    return in.read();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return in.read(b, off, len);
  }

  @Override
  public void close() throws IOException {
    in.close();
  }

  @Override
  long length() throws IOException {
    // file size + size of both buffers
    return inner.length();
  }

  DataInputStream getDataInputStream() {
    return in;
  }

  void setBytes(byte[] newBytes) throws IOException {
    inner.setData(newBytes);
    in.reset();
  }

  void clear() throws IOException {
    setBytes(null);
  }
}
