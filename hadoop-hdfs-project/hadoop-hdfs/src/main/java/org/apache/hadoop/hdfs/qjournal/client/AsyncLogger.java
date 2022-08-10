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
package org.apache.hadoop.hdfs.qjournal.client;

import java.net.InetSocketAddress;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournaledEditsResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournalStateResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.SegmentStateProto;
import org.apache.hadoop.hdfs.qjournal.protocol.RequestInfo;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;

/**
 * Interface for a remote log which is only communicated with asynchronously.
 * This is essentially a wrapper around {@link QJournalProtocol} with the key
 * differences being:
 * 
 * <ul>
 * <li>All methods return {@link ListenableFuture}s instead of synchronous
 * objects.</li>
 * <li>The {@link RequestInfo} objects are created by the underlying
 * implementation.</li>
 * </ul>
 */
interface AsyncLogger {
  
  interface Factory {
    AsyncLogger createLogger(Configuration conf, NamespaceInfo nsInfo,
        String journalId, String nameServiceId, InetSocketAddress addr);
  }

  /**
   * Send a batch of edits to the logger.
   * @param segmentTxId the first txid in the current segment
   * @param firstTxnId the first txid of the edits.
   * @param numTxns the number of transactions in the batch
   * @param data the actual data to be sent
   */
  public ListenableFuture<Void> sendEdits(
      final long segmentTxId, final long firstTxnId,
      final int numTxns, final byte[] data);

  /**
   * Begin writing a new log segment.
   * 
   * @param txid the first txid to be written to the new log
   * @param layoutVersion the LayoutVersion of the log
   */
  public ListenableFuture<Void> startLogSegment(long txid, int layoutVersion);

  /**
   * Finalize a log segment.
   * 
   * @param startTxId the first txid that was written to the segment
   * @param endTxId the last txid that was written to the segment
   */
  public ListenableFuture<Void> finalizeLogSegment(
      long startTxId, long endTxId);

  /**
   * Allow the remote node to purge edit logs earlier than this.
   * @param minTxIdToKeep the min txid which must be retained
   */
  public ListenableFuture<Void> purgeLogsOlderThan(long minTxIdToKeep);

  /**
   * Format the log directory.
   * @param nsInfo the namespace info to format with
   * @param force the force option to format
   */
  public ListenableFuture<Void> format(NamespaceInfo nsInfo, boolean force);

  /**
   * @return whether or not the remote node has any valid data.
   */
  public ListenableFuture<Boolean> isFormatted();
  
  /**
   * @return the state of the last epoch on the target node.
   */
  public ListenableFuture<GetJournalStateResponseProto> getJournalState();

  /**
   * Begin a new epoch on the target node.
   */
  public ListenableFuture<NewEpochResponseProto> newEpoch(long epoch);

  /**
   * Fetch journaled edits from the cache.
   */
  public ListenableFuture<GetJournaledEditsResponseProto> getJournaledEdits(
      long fromTxnId, int maxTransactions);
  
  /**
   * Fetch the list of edit logs available on the remote node.
   */
  public ListenableFuture<RemoteEditLogManifest> getEditLogManifest(
      long fromTxnId, boolean inProgressOk);

  /**
   * Prepare recovery. See the HDFS-3077 design document for details.
   */
  public ListenableFuture<PrepareRecoveryResponseProto> prepareRecovery(
      long segmentTxId);

  /**
   * Accept a recovery proposal. See the HDFS-3077 design document for details.
   */
  public ListenableFuture<Void> acceptRecovery(SegmentStateProto log,
      URL fromUrl);

  /**
   * Set the epoch number used for all future calls.
   */
  public void setEpoch(long e);

  /**
   * Let the logger know the highest committed txid across all loggers in the
   * set. This txid may be higher than the last committed txid for <em>this</em>
   * logger. See HDFS-3863 for details.
   */
  public void setCommittedTxId(long txid);

  /**
   * Build an HTTP URL to fetch the log segment with the given startTxId.
   */
  public URL buildURLToFetchLogs(long segmentTxId);
  
  /**
   * Tear down any resources, connections, etc. The proxy may not be used
   * after this point, and any in-flight RPCs may throw an exception.
   */
  public void close();

  /**
   * Append an HTML-formatted report for this logger's status to the provided
   * StringBuilder. This is displayed on the NN web UI.
   */
  public void appendReport(StringBuilder sb);

  public ListenableFuture<Void> doPreUpgrade();

  public ListenableFuture<Void> doUpgrade(StorageInfo sInfo);

  public ListenableFuture<Void> doFinalize();

  public ListenableFuture<Boolean> canRollBack(StorageInfo storage,
      StorageInfo prevStorage, int targetLayoutVersion);

  public ListenableFuture<Void> doRollback();

  public ListenableFuture<Void> discardSegments(long startTxId);

  public ListenableFuture<Long> getJournalCTime();
}
