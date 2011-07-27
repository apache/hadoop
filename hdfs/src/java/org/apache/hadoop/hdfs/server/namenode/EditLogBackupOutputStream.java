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
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.protocol.JournalProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
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

  private JournalProtocol backupNode;        // RPC proxy to backup node
  private NamenodeRegistration bnRegistration;  // backup node registration
  private NamenodeRegistration nnRegistration;  // active node registration
  private ArrayList<JournalRecord> bufCurrent;  // current buffer for writing
  private ArrayList<JournalRecord> bufReady;    // buffer ready for flushing
  private DataOutputBuffer out;     // serialized output sent to backup node

  static class JournalRecord {
    byte op;
    long txid;
    Writable[] args;

    JournalRecord(byte op, long txid, Writable ... writables) {
      this.op = op;
      this.txid = txid;
      this.args = writables;
    }

    void write(DataOutputBuffer out) throws IOException {
      writeChecksummedOp(out, op, txid, args);
    }
  }

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
        RPC.getProxy(JournalProtocol.class,
            JournalProtocol.versionID, bnAddress, new HdfsConfiguration());
    } catch(IOException e) {
      Storage.LOG.error("Error connecting to: " + bnAddress, e);
      throw e;
    }
    this.bufCurrent = new ArrayList<JournalRecord>();
    this.bufReady = new ArrayList<JournalRecord>();
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

  @Override
  void write(byte[] data, int i, int length) throws IOException {
    throw new IOException("Not implemented");
  }

  @Override // EditLogOutputStream
  void write(byte op, long txid, Writable ... writables) throws IOException {
    bufCurrent.add(new JournalRecord(op, txid, writables));
  }

  /**
   * There is no persistent storage. Just clear the buffers.
   */
  @Override // EditLogOutputStream
  void create() throws IOException {
    bufCurrent.clear();
    assert bufReady.size() == 0 : "previous data is not flushed yet";
  }

  @Override // EditLogOutputStream
  public void close() throws IOException {
    // close should have been called after all pending transactions 
    // have been flushed & synced.
    int size = bufCurrent.size();
    if (size != 0) {
      throw new IOException("BackupEditStream has " + size +
                          " records still to be flushed and cannot be closed.");
    } 
    RPC.stopProxy(backupNode); // stop the RPC threads
    bufCurrent = bufReady = null;
  }

  @Override
  public void abort() throws IOException {
    RPC.stopProxy(backupNode);
    bufCurrent = bufReady = null;
  }

  @Override // EditLogOutputStream
  void setReadyToFlush() throws IOException {
    assert bufReady.size() == 0 : "previous data is not flushed yet";
    ArrayList<JournalRecord>  tmp = bufReady;
    bufReady = bufCurrent;
    bufCurrent = tmp;
  }

  @Override // EditLogOutputStream
  protected void flushAndSync() throws IOException {
    assert out.size() == 0 : "Output buffer is not empty";
    for (JournalRecord jRec : bufReady) {
      jRec.write(out);
    }
    if (out.size() > 0) {
      byte[] data = Arrays.copyOf(out.getData(), out.getLength());
      backupNode.journal(nnRegistration,
          bufReady.get(0).txid, bufReady.size(),
          data);
    }
    bufReady.clear();         // erase all data in the buffer
    out.reset();              // reset buffer to the start position
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

  /**
   * Get backup node registration.
   */
  NamenodeRegistration getRegistration() {
    return bnRegistration;
  }

  void startLogSegment(long txId) throws IOException {
    backupNode.startLogSegment(nnRegistration, txId);
  }
}
