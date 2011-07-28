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

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;

/**
 * An implementation of the abstract class {@link EditLogOutputStream},
 * which streams edits to a backup node.
 * 
 * @see org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol#journal
 * (org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration,
 *  int, int, byte[])
 */
class EditLogBackupOutputStream extends EditLogOutputStream {
  static int DEFAULT_BUFFER_SIZE = 256;

  private NamenodeProtocol backupNode;          // RPC proxy to backup node
  private NamenodeRegistration bnRegistration;  // backup node registration
  private NamenodeRegistration nnRegistration;  // active node registration
  private EditsDoubleBuffer doubleBuf;
  private DataOutputBuffer out;     // serialized output sent to backup node

  EditLogBackupOutputStream(NamenodeRegistration bnReg, // backup node
                            NamenodeRegistration nnReg) // active name-node
  throws IOException {
    super();
    this.bnRegistration = bnReg;
    this.nnRegistration = nnReg;
    InetSocketAddress bnAddress =
      NetUtils.createSocketAddr(bnRegistration.getAddress());
    Storage.LOG.info("EditLogBackupOutputStream connects to: " + bnAddress);
    try {
      this.backupNode =
        (NamenodeProtocol) RPC.getProxy(NamenodeProtocol.class,
            NamenodeProtocol.versionID, bnAddress, new HdfsConfiguration());
    } catch(IOException e) {
      Storage.LOG.error("Error connecting to: " + bnAddress, e);
      throw e;
    }
    this.doubleBuf = new EditsDoubleBuffer(DEFAULT_BUFFER_SIZE);
    this.out = new DataOutputBuffer(DEFAULT_BUFFER_SIZE);
  }

  @Override // JournalStream
  public String getName() {
    return bnRegistration.getAddress();
  }

  @Override // JournalStream
  public JournalType getType() {
    return JournalType.BACKUP;
  }

  @Override // EditLogOutputStream
  void write(FSEditLogOp op) throws IOException {
    doubleBuf.writeOp(op);
 }

  @Override
  void writeRaw(byte[] bytes, int offset, int length) throws IOException {
    throw new IOException("Not supported");
  }

  /**
   * There is no persistent storage. Just clear the buffers.
   */
  @Override // EditLogOutputStream
  void create() throws IOException {
    assert doubleBuf.isFlushed() : "previous data is not flushed yet";
    this.doubleBuf = new EditsDoubleBuffer(DEFAULT_BUFFER_SIZE);
  }

  @Override // EditLogOutputStream
  public void close() throws IOException {
    // close should have been called after all pending transactions 
    // have been flushed & synced.
    int size = doubleBuf.countBufferedBytes();
    if (size != 0) {
      throw new IOException("BackupEditStream has " + size +
                          " records still to be flushed and cannot be closed.");
    } 
    RPC.stopProxy(backupNode); // stop the RPC threads
    doubleBuf.close();
    doubleBuf = null;
  }

  @Override // EditLogOutputStream
  void setReadyToFlush() throws IOException {
    doubleBuf.setReadyToFlush();
  }

  @Override // EditLogOutputStream
  protected void flushAndSync() throws IOException {
    // XXX: this code won't work in trunk, but it's redone
    // in HDFS-1073 where it's simpler.
    doubleBuf.flushTo(out);
    if (out.size() > 0) {
      send(NamenodeProtocol.JA_JOURNAL);
    }
  }

  /**
   * There is no persistent storage. Therefore length is 0.<p>
   * Length is used to check when it is large enough to start a checkpoint.
   * This criteria should not be used for backup streams.
   */
  @Override // EditLogOutputStream
  long length() throws IOException {
    return 0;
  }

  private void send(int ja) throws IOException {
    try {
      int length = out.getLength();
      out.write(FSEditLogOpCodes.OP_INVALID.getOpCode());
      backupNode.journal(nnRegistration, ja, length, out.getData());
    } finally {
      out.reset();
    }
  }

  /**
   * Get backup node registration.
   */
  NamenodeRegistration getRegistration() {
    return bnRegistration;
  }

  /**
   * Verify that the backup node is alive.
   */
  boolean isAlive() {
    try {
      send(NamenodeProtocol.JA_IS_ALIVE);
    } catch(IOException ei) {
      Storage.LOG.info(bnRegistration.getRole() + " "
                      + bnRegistration.getAddress() + " is not alive. ", ei);
      return false;
    }
    return true;
  }
}
