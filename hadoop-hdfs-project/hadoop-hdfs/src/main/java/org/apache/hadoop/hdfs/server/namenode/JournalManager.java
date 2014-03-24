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

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Storage.FormatConfirmable;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;

/**
 * A JournalManager is responsible for managing a single place of storing
 * edit logs. It may correspond to multiple files, a backup node, etc.
 * Even when the actual underlying storage is rolled, or failed and restored,
 * each conceptual place of storage corresponds to exactly one instance of
 * this class, which is created when the EditLog is first opened.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface JournalManager extends Closeable, LogsPurgeable,
                                        FormatConfirmable {

  /**
   * Format the underlying storage, removing any previously
   * stored data.
   */
  void format(NamespaceInfo ns) throws IOException;

  /**
   * Begin writing to a new segment of the log stream, which starts at
   * the given transaction ID.
   */
  EditLogOutputStream startLogSegment(long txId, int layoutVersion)
      throws IOException;

  /**
   * Mark the log segment that spans from firstTxId to lastTxId
   * as finalized and complete.
   */
  void finalizeLogSegment(long firstTxId, long lastTxId) throws IOException;

  /**
   * Set the amount of memory that this stream should use to buffer edits
   */
  void setOutputBufferCapacity(int size);

  /**
   * Recover segments which have not been finalized.
   */
  void recoverUnfinalizedSegments() throws IOException;
  
  /**
   * Perform any steps that must succeed across all JournalManagers involved in
   * an upgrade before proceeding onto the actual upgrade stage. If a call to
   * any JM's doPreUpgrade method fails, then doUpgrade will not be called for
   * any JM.
   */
  void doPreUpgrade() throws IOException;
  
  /**
   * Perform the actual upgrade of the JM. After this is completed, the NN can
   * begin to use the new upgraded metadata. This metadata may later be either
   * finalized or rolled back to the previous state.
   * 
   * @param storage info about the new upgraded versions.
   */
  void doUpgrade(Storage storage) throws IOException;
  
  /**
   * Finalize the upgrade. JMs should purge any state that they had been keeping
   * around during the upgrade process. After this is completed, rollback is no
   * longer allowed.
   */
  void doFinalize() throws IOException;
  
  /**
   * Return true if this JM can roll back to the previous storage state, false
   * otherwise. The NN will refuse to run the rollback operation unless at least
   * one JM or fsimage storage directory can roll back.
   * 
   * @param storage the storage info for the current state
   * @param prevStorage the storage info for the previous (unupgraded) state
   * @param targetLayoutVersion the layout version we intend to roll back to
   * @return true if this JM can roll back, false otherwise.
   */
  boolean canRollBack(StorageInfo storage, StorageInfo prevStorage,
      int targetLayoutVersion) throws IOException;
  
  /**
   * Perform the rollback to the previous FS state. JMs which do not need to
   * roll back their state should just return without error.
   */
  void doRollback() throws IOException;
  
  /**
   * @return the CTime of the journal manager.
   */
  long getJournalCTime() throws IOException;

  /**
   * Discard the segments whose first txid is >= the given txid.
   * @param startTxId The given txid should be right at the segment boundary, 
   * i.e., it should be the first txid of some segment, if segment corresponding
   * to the txid exists.
   */
  void discardSegments(long startTxId) throws IOException;

  /**
   * Close the journal manager, freeing any resources it may hold.
   */
  @Override
  void close() throws IOException;
  
  /** 
   * Indicate that a journal is cannot be used to load a certain range of 
   * edits.
   * This exception occurs in the case of a gap in the transactions, or a
   * corrupt edit file.
   */
  public static class CorruptionException extends IOException {
    static final long serialVersionUID = -4687802717006172702L;
    
    public CorruptionException(String reason) {
      super(reason);
    }
  }

}
