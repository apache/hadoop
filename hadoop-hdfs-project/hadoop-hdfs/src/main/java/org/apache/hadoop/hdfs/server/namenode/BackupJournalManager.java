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
import java.util.Collection;

import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.JournalInfo;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;

/**
 * A JournalManager implementation that uses RPCs to log transactions
 * to a BackupNode.
 */
class BackupJournalManager implements JournalManager {
  private final NamenodeRegistration bnReg;
  private final JournalInfo journalInfo;
  
  BackupJournalManager(NamenodeRegistration bnReg,
      NamenodeRegistration nnReg) {
    journalInfo = new JournalInfo(nnReg.getLayoutVersion(),
        nnReg.getClusterID(), nnReg.getNamespaceID());
    this.bnReg = bnReg;
  }

  @Override
  public void format(NamespaceInfo nsInfo) {
    // format() should only get called at startup, before any BNs
    // can register with the NN.
    throw new UnsupportedOperationException(
        "BackupNode journal should never get formatted");
  }
  
  @Override
  public boolean hasSomeData() {
    throw new UnsupportedOperationException();
  }

  
  @Override
  public EditLogOutputStream startLogSegment(long txId, int layoutVersion)
      throws IOException {
    EditLogBackupOutputStream stm = new EditLogBackupOutputStream(bnReg,
        journalInfo);
    stm.startLogSegment(txId);
    return stm;
  }

  @Override
  public void finalizeLogSegment(long firstTxId, long lastTxId)
      throws IOException {
  }

  @Override
  public void setOutputBufferCapacity(int size) {
  }

  @Override
  public void purgeLogsOlderThan(long minTxIdToKeep)
      throws IOException {
  }

  @Override
  public void selectInputStreams(Collection<EditLogInputStream> streams,
      long fromTxnId, boolean inProgressOk) {
    // This JournalManager is never used for input. Therefore it cannot
    // return any transactions
  }

  @Override
  public void recoverUnfinalizedSegments() throws IOException {
  }

  @Override 
  public void close() throws IOException {}

  public boolean matchesRegistration(NamenodeRegistration bnReg) {
    return bnReg.getAddress().equals(this.bnReg.getAddress());
  }

  @Override
  public String toString() {
    return "BackupJournalManager";
  }

  @Override
  public void discardSegments(long startTxId) throws IOException {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public void doPreUpgrade() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void doUpgrade(Storage storage) throws IOException {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public void doFinalize() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean canRollBack(StorageInfo storage, StorageInfo prevStorage,
      int targetLayoutVersion) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void doRollback() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getJournalCTime() throws IOException {
    throw new UnsupportedOperationException();
  }
}
