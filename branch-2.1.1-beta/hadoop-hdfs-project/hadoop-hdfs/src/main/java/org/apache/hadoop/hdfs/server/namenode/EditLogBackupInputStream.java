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

import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import com.google.common.base.Preconditions;

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
  private FSEditLogOp.Reader reader = null;
  private FSEditLogLoader.PositionTrackingInputStream tracker = null;
  private int version = 0;

  /**
   * A ByteArrayInputStream, which lets modify the underlying byte array.
   */
  private static class ByteBufferInputStream extends ByteArrayInputStream {
    ByteBufferInputStream() {
      super(new byte[0]);
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
    in = null;
    reader = null;
  }

  @Override
  public String getName() {
    return address;
  }

  @Override
  protected FSEditLogOp nextOp() throws IOException {
    Preconditions.checkState(reader != null,
        "Must call setBytes() before readOp()");
    return reader.readOp(false);
  }

  @Override
  protected FSEditLogOp nextValidOp() {
    try {
      return reader.readOp(true);
    } catch (IOException e) {
      throw new RuntimeException("got unexpected IOException " + e, e);
    }
  }

  @Override
  public int getVersion() throws IOException {
    return this.version;
  }

  @Override
  public long getPosition() {
    return tracker.getPos();
  }

  @Override
  public void close() throws IOException {
    in.close();
  }

  @Override
  public long length() throws IOException {
    // file size + size of both buffers
    return inner.length();
  }

  void setBytes(byte[] newBytes, int version) throws IOException {
    inner.setData(newBytes);
    tracker = new FSEditLogLoader.PositionTrackingInputStream(inner);
    in = new DataInputStream(tracker);

    this.version = version;

    reader = new FSEditLogOp.Reader(in, tracker, version);
  }

  void clear() throws IOException {
    setBytes(null, 0);
    reader = null;
    this.version = 0;
  }

  @Override
  public long getFirstTxId() {
    return HdfsConstants.INVALID_TXID;
  }

  @Override
  public long getLastTxId() {
    return HdfsConstants.INVALID_TXID;
  }

  @Override
  public boolean isInProgress() {
    return true;
  }

  @Override
  public void setMaxOpSize(int maxOpSize) {
    reader.setMaxOpSize(maxOpSize);
  }
}
