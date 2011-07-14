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

import org.apache.hadoop.hdfs.server.namenode.NNStorageArchivalManager.StorageArchiver;

/**
 * A JournalManager is responsible for managing a single place of storing
 * edit logs. It may correspond to multiple files, a backup node, etc.
 * Even when the actual underlying storage is rolled, or failed and restored,
 * each conceptual place of storage corresponds to exactly one instance of
 * this class, which is created when the EditLog is first opened.
 */
public interface JournalManager {
  /**
   * TODO
   */
  EditLogOutputStream startLogSegment(long txId) throws IOException;
  
  void finalizeLogSegment(long firstTxId, long lastTxId) throws IOException;

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
   * @param archiver the archival implementation to use
   * @throws IOException if purging fails
   */
  void archiveLogsOlderThan(long minTxIdToKeep, StorageArchiver archiver)
    throws IOException;

  /**
   * @return an EditLogInputStream that reads from the same log that
   * the edit log is currently writing. May return null if this journal
   * manager does not support this operation.
   */  
  EditLogInputStream getInProgressInputStream(long segmentStartsAtTxId)
    throws IOException;
}
