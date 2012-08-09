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
import java.util.Collection;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.common.Storage.FormatConfirmable;
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
public interface JournalManager extends Closeable, FormatConfirmable {

  /**
   * Format the underlying storage, removing any previously
   * stored data.
   */
  void format(NamespaceInfo ns);

  /**
   * Begin writing to a new segment of the log stream, which starts at
   * the given transaction ID.
   */
  EditLogOutputStream startLogSegment(long txId) throws IOException;

  /**
   * Mark the log segment that spans from firstTxId to lastTxId
   * as finalized and complete.
   */
  void finalizeLogSegment(long firstTxId, long lastTxId) throws IOException;

   /**
   * Get a list of edit log input streams.  The list will start with the
   * stream that contains fromTxnId, and continue until the end of the journal
   * being managed.
   * 
   * @param fromTxnId the first transaction id we want to read
   * @param inProgressOk whether or not in-progress streams should be returned
   *
   * @return a list of streams
   */
  void selectInputStreams(Collection<EditLogInputStream> streams,
      long fromTxnId, boolean inProgressOk);

  /**
   * Set the amount of memory that this stream should use to buffer edits
   */
  void setOutputBufferCapacity(int size);

  /**
   * The JournalManager may archive/purge any logs for transactions less than
   * or equal to minImageTxId.
   *
   * @param minTxIdToKeep the earliest txid that must be retained after purging
   *                      old logs
   * @throws IOException if purging fails
   */
  void purgeLogsOlderThan(long minTxIdToKeep)
    throws IOException;

  /**
   * Recover segments which have not been finalized.
   */
  void recoverUnfinalizedSegments() throws IOException;

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
