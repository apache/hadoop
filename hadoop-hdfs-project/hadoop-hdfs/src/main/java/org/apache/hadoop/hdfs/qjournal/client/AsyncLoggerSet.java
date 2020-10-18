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

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournalStateResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournaledEditsResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.SegmentStateProto;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;

/**
 * Wrapper around a set of Loggers, taking care of fanning out
 * calls to the underlying loggers and constructing corresponding
 * {@link QuorumCall} instances.
 */
class AsyncLoggerSet {
  static final Logger LOG = LoggerFactory.getLogger(AsyncLoggerSet.class);

  private final List<AsyncLogger> loggers;
  
  private static final long INVALID_EPOCH = -1;
  private long myEpoch = INVALID_EPOCH;
  
  public AsyncLoggerSet(List<AsyncLogger> loggers) {
    this.loggers = ImmutableList.copyOf(loggers);
  }
  
  void setEpoch(long e) {
    Preconditions.checkState(!isEpochEstablished(),
        "Epoch already established: epoch=%s", myEpoch);
    myEpoch = e;
    for (AsyncLogger l : loggers) {
      l.setEpoch(e);
    }
  }

  /**
   * Set the highest successfully committed txid seen by the writer.
   * This should be called after a successful write to a quorum, and is used
   * for extra sanity checks against the protocol. See HDFS-3863.
   */
  public void setCommittedTxId(long txid) {
    for (AsyncLogger logger : loggers) {
      logger.setCommittedTxId(txid);
    }
  }

  /**
   * @return true if an epoch has been established.
   */
  boolean isEpochEstablished() {
    return myEpoch != INVALID_EPOCH;
  }
  
  /**
   * @return the epoch number for this writer. This may only be called after
   * a successful call to {@link #createNewUniqueEpoch(NamespaceInfo)}.
   */
  long getEpoch() {
    Preconditions.checkState(myEpoch != INVALID_EPOCH,
        "No epoch created yet");
    return myEpoch;
  }

  /**
   * Close all of the underlying loggers.
   */
  void close() {
    for (AsyncLogger logger : loggers) {
      logger.close();
    }
  }
  
  void purgeLogsOlderThan(long minTxIdToKeep) {
    for (AsyncLogger logger : loggers) {
      logger.purgeLogsOlderThan(minTxIdToKeep);
    }
  }


  /**
   * Wait for a quorum of loggers to respond to the given call. If a quorum
   * can't be achieved, throws a QuorumException.
   * @param q the quorum call
   * @param timeoutMs the number of millis to wait
   * @param operationName textual description of the operation, for logging
   * @return a map of successful results
   * @throws QuorumException if a quorum doesn't respond with success
   * @throws IOException if the thread is interrupted or times out
   */
  <V> Map<AsyncLogger, V> waitForWriteQuorum(QuorumCall<AsyncLogger, V> q,
      int timeoutMs, String operationName) throws IOException {
    int majority = getMajoritySize();
    try {
      q.waitFor(
          loggers.size(), // either all respond 
          majority, // or we get a majority successes
          majority, // or we get a majority failures,
          timeoutMs, operationName);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted waiting " + timeoutMs + "ms for a " +
          "quorum of nodes to respond.");
    } catch (TimeoutException e) {
      throw new IOException("Timed out waiting " + timeoutMs + "ms for a " +
          "quorum of nodes to respond.");
    }
    
    if (q.countSuccesses() < majority) {
      q.rethrowException("Got too many exceptions to achieve quorum size " +
          getMajorityString());
    }
    
    return q.getResults();
  }
  
  /**
   * @return the number of nodes which are required to obtain a quorum.
   */
  int getMajoritySize() {
    return loggers.size() / 2 + 1;
  }
  
  /**
   * @return a textual description of the majority size (eg "2/3" or "3/5")
   */
  String getMajorityString() {
    return getMajoritySize() + "/" + loggers.size();
  }

  /**
   * @return the number of loggers behind this set
   */
  int size() {
    return loggers.size();
  }
  
  @Override
  public String toString() {
    return "[" + Joiner.on(", ").join(loggers) + "]";
  }

  /**
   * Append an HTML-formatted status readout on the current
   * state of the underlying loggers.
   * @param sb the StringBuilder to append to
   */
  void appendReport(StringBuilder sb) {
    for (int i = 0, len = loggers.size(); i < len; ++i) {
      AsyncLogger l = loggers.get(i);
      if (i != 0) {
        sb.append(", ");
      }
      sb.append(l).append(" (");
      l.appendReport(sb);
      sb.append(")");
    }
  }

  /**
   * @return the (mutable) list of loggers, for use in tests to
   * set up spies
   */
  @VisibleForTesting
  List<AsyncLogger> getLoggersForTests() {
    return loggers;
  }
  
  ///////////////////////////////////////////////////////////////////////////
  // The rest of this file is simply boilerplate wrappers which fan-out the
  // various IPC calls to the underlying AsyncLoggers and wrap the result
  // in a QuorumCall.
  ///////////////////////////////////////////////////////////////////////////
  
  public QuorumCall<AsyncLogger, GetJournalStateResponseProto> getJournalState() {
    Map<AsyncLogger, ListenableFuture<GetJournalStateResponseProto>> calls =
        Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      calls.put(logger, logger.getJournalState());
    }
    return QuorumCall.create(calls);    
  }
  
  public QuorumCall<AsyncLogger, Boolean> isFormatted() {
    Map<AsyncLogger, ListenableFuture<Boolean>> calls = Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      calls.put(logger, logger.isFormatted());
    }
    return QuorumCall.create(calls);
  }

  public QuorumCall<AsyncLogger,NewEpochResponseProto> newEpoch(
      NamespaceInfo nsInfo,
      long epoch) {
    Map<AsyncLogger, ListenableFuture<NewEpochResponseProto>> calls =
        Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      calls.put(logger, logger.newEpoch(epoch));
    }
    return QuorumCall.create(calls);    
  }

  public QuorumCall<AsyncLogger, Void> startLogSegment(
      long txid, int layoutVersion) {
    Map<AsyncLogger, ListenableFuture<Void>> calls = Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      calls.put(logger, logger.startLogSegment(txid, layoutVersion));
    }
    return QuorumCall.create(calls);
  }
  
  public QuorumCall<AsyncLogger, Void> finalizeLogSegment(long firstTxId,
      long lastTxId) {
    Map<AsyncLogger, ListenableFuture<Void>> calls = Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      calls.put(logger, logger.finalizeLogSegment(firstTxId, lastTxId));
    }
    return QuorumCall.create(calls);
  }
  
  public QuorumCall<AsyncLogger, Void> sendEdits(
      long segmentTxId, long firstTxnId, int numTxns, byte[] data) {
    Map<AsyncLogger, ListenableFuture<Void>> calls = Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      ListenableFuture<Void> future = 
        logger.sendEdits(segmentTxId, firstTxnId, numTxns, data);
      calls.put(logger, future);
    }
    return QuorumCall.create(calls);
  }

  public QuorumCall<AsyncLogger, GetJournaledEditsResponseProto>
  getJournaledEdits(long fromTxnId, int maxTransactions) {
    Map<AsyncLogger,
        ListenableFuture<GetJournaledEditsResponseProto>> calls
        = Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      ListenableFuture<GetJournaledEditsResponseProto> future =
          logger.getJournaledEdits(fromTxnId, maxTransactions);
      calls.put(logger, future);
    }
    return QuorumCall.create(calls);
  }

  public QuorumCall<AsyncLogger, RemoteEditLogManifest> getEditLogManifest(
      long fromTxnId, boolean inProgressOk) {
    Map<AsyncLogger,
        ListenableFuture<RemoteEditLogManifest>> calls
        = Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      ListenableFuture<RemoteEditLogManifest> future =
          logger.getEditLogManifest(fromTxnId, inProgressOk);
      calls.put(logger, future);
    }
    return QuorumCall.create(calls);
  }

  QuorumCall<AsyncLogger, PrepareRecoveryResponseProto>
      prepareRecovery(long segmentTxId) {
    Map<AsyncLogger,
      ListenableFuture<PrepareRecoveryResponseProto>> calls
      = Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      ListenableFuture<PrepareRecoveryResponseProto> future =
          logger.prepareRecovery(segmentTxId);
      calls.put(logger, future);
    }
    return QuorumCall.create(calls);
  }

  QuorumCall<AsyncLogger,Void>
      acceptRecovery(SegmentStateProto log, URL fromURL) {
    Map<AsyncLogger, ListenableFuture<Void>> calls
      = Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      ListenableFuture<Void> future =
          logger.acceptRecovery(log, fromURL);
      calls.put(logger, future);
    }
    return QuorumCall.create(calls);
  }

  QuorumCall<AsyncLogger, Void> format(NamespaceInfo nsInfo, boolean force) {
    Map<AsyncLogger, ListenableFuture<Void>> calls =
        Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      ListenableFuture<Void> future =
          logger.format(nsInfo, force);
      calls.put(logger, future);
    }
    return QuorumCall.create(calls);
  }
  
  QuorumCall<AsyncLogger, Void> doPreUpgrade() {
    Map<AsyncLogger, ListenableFuture<Void>> calls =
        Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      ListenableFuture<Void> future =
          logger.doPreUpgrade();
      calls.put(logger, future);
    }
    return QuorumCall.create(calls);
  }

  public QuorumCall<AsyncLogger, Void> doUpgrade(StorageInfo sInfo) {
    Map<AsyncLogger, ListenableFuture<Void>> calls =
        Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      ListenableFuture<Void> future =
          logger.doUpgrade(sInfo);
      calls.put(logger, future);
    }
    return QuorumCall.create(calls);
  }

  public QuorumCall<AsyncLogger, Void> doFinalize() {
    Map<AsyncLogger, ListenableFuture<Void>> calls =
        Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      ListenableFuture<Void> future =
          logger.doFinalize();
      calls.put(logger, future);
    }
    return QuorumCall.create(calls);
  }

  public QuorumCall<AsyncLogger, Boolean> canRollBack(StorageInfo storage,
      StorageInfo prevStorage, int targetLayoutVersion) {
    Map<AsyncLogger, ListenableFuture<Boolean>> calls =
        Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      ListenableFuture<Boolean> future =
          logger.canRollBack(storage, prevStorage, targetLayoutVersion);
      calls.put(logger, future);
    }
    return QuorumCall.create(calls);
  }

  public QuorumCall<AsyncLogger, Void> doRollback() {
    Map<AsyncLogger, ListenableFuture<Void>> calls =
        Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      ListenableFuture<Void> future =
          logger.doRollback();
      calls.put(logger, future);
    }
    return QuorumCall.create(calls);
  }

  public QuorumCall<AsyncLogger, Void> discardSegments(long startTxId) {
    Map<AsyncLogger, ListenableFuture<Void>> calls = Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      ListenableFuture<Void> future = logger.discardSegments(startTxId);
      calls.put(logger, future);
    }
    return QuorumCall.create(calls);
  }

  public QuorumCall<AsyncLogger, Long> getJournalCTime() {
    Map<AsyncLogger, ListenableFuture<Long>> calls =
        Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      ListenableFuture<Long> future = logger.getJournalCTime();
      calls.put(logger, future);
    }
    return QuorumCall.create(calls);
  }

}
