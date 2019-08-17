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
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.protocol.JournalInfo;
import org.apache.hadoop.hdfs.server.protocol.JournalProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * An implementation of the abstract class {@link EditLogOutputStream},
 * which streams edits to a backup node.
 * 
 * @see org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol#journal
 * (org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration,
 *  int, int, byte[])
 */
class EditLogBackupOutputStream extends EditLogOutputStream {
  private static final Logger LOG =
      LoggerFactory.getLogger(EditLogFileOutputStream.class);
  static final int DEFAULT_BUFFER_SIZE = 256;

  private final JournalProtocol backupNode;  // RPC proxy to backup node
  private final NamenodeRegistration bnRegistration;  // backup node registration
  private final JournalInfo journalInfo;  // active node registration
  private final DataOutputBuffer out;     // serialized output sent to backup node
  private EditsDoubleBuffer doubleBuf;

  EditLogBackupOutputStream(NamenodeRegistration bnReg, // backup node
                            JournalInfo journalInfo) // active name-node
  throws IOException {
    super();
    this.bnRegistration = bnReg;
    this.journalInfo = journalInfo;
    InetSocketAddress bnAddress =
      NetUtils.createSocketAddr(bnRegistration.getAddress());
    try {
      this.backupNode = NameNodeProxies.createNonHAProxy(new HdfsConfiguration(),
          bnAddress, JournalProtocol.class, UserGroupInformation.getCurrentUser(),
          true).getProxy();
    } catch(IOException e) {
      Storage.LOG.error("Error connecting to: " + bnAddress, e);
      throw e;
    }
    this.doubleBuf = new EditsDoubleBuffer(DEFAULT_BUFFER_SIZE);
    this.out = new DataOutputBuffer(DEFAULT_BUFFER_SIZE);
  }
  
  @Override // EditLogOutputStream
  public void write(FSEditLogOp op) throws IOException {
    doubleBuf.writeOp(op);
 }

  @Override
  public void writeRaw(byte[] bytes, int offset, int length) throws IOException {
    throw new IOException("Not supported");
  }

  /**
   * There is no persistent storage. Just clear the buffers.
   */
  @Override // EditLogOutputStream
  public void create(int layoutVersion) throws IOException {
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

  @Override
  public void abort() throws IOException {
    RPC.stopProxy(backupNode);
    doubleBuf = null;
  }

  @Override // EditLogOutputStream
  public void setReadyToFlush() throws IOException {
    doubleBuf.setReadyToFlush();
  }

  @Override // EditLogOutputStream
  protected void flushAndSync(boolean durable) throws IOException {
    assert out.getLength() == 0 : "Output buffer is not empty";
    
    if (doubleBuf.isFlushed()) {
      LOG.info("Nothing to flush");
      return;
    }

    int numReadyTxns = doubleBuf.countReadyTxns();
    long firstTxToFlush = doubleBuf.getFirstReadyTxId();
    
    doubleBuf.flushTo(out);
    if (out.getLength() > 0) {
      assert numReadyTxns > 0;
      
      byte[] data = Arrays.copyOf(out.getData(), out.getLength());
      out.reset();
      assert out.getLength() == 0 : "Output buffer is not empty";

      backupNode.journal(journalInfo, 0, firstTxToFlush, numReadyTxns, data);
    }
  }

  /**
   * Get backup node registration.
   */
  NamenodeRegistration getRegistration() {
    return bnRegistration;
  }

  void startLogSegment(long txId) throws IOException {
    backupNode.startLogSegment(journalInfo, 0, txId);
  }
}
