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
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournalStateResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.SegmentStateProto;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileInputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.JournalManager;
import org.apache.hadoop.hdfs.server.namenode.JournalSet;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.TextFormat;

/**
 * A JournalManager that writes to a set of remote JournalNodes,
 * requiring a quorum of nodes to ack each write.
 */
@InterfaceAudience.Private
public class QuorumJournalManager implements JournalManager {
  static final Logger LOG = LoggerFactory.getLogger(QuorumJournalManager.class);

  // Timeouts for which the QJM will wait for each of the following actions.
  private final int startSegmentTimeoutMs;
  private final int prepareRecoveryTimeoutMs;
  private final int acceptRecoveryTimeoutMs;
  private final int finalizeSegmentTimeoutMs;
  private final int selectInputStreamsTimeoutMs;
  private final int getJournalStateTimeoutMs;
  private final int newEpochTimeoutMs;
  private final int writeTxnsTimeoutMs;

  // This timeout is used for calls that don't occur during normal operation
  // e.g. format, upgrade operations and a few others. So we can use rather
  // lengthy timeouts by default.
  private final int timeoutMs;
  
  private final Configuration conf;
  private final URI uri;
  private final NamespaceInfo nsInfo;
  private final String nameServiceId;
  private boolean isActiveWriter;
  
  private final AsyncLoggerSet loggers;

  private int outputBufferCapacity = 512 * 1024;
  private final URLConnectionFactory connectionFactory;

  @VisibleForTesting
  public QuorumJournalManager(Configuration conf,
                              URI uri,
                              NamespaceInfo nsInfo) throws IOException {
    this(conf, uri, nsInfo, null, IPCLoggerChannel.FACTORY);
  }
  
  public QuorumJournalManager(Configuration conf,
      URI uri, NamespaceInfo nsInfo, String nameServiceId) throws IOException {
    this(conf, uri, nsInfo, nameServiceId, IPCLoggerChannel.FACTORY);
  }

  @VisibleForTesting
  QuorumJournalManager(Configuration conf,
                       URI uri, NamespaceInfo nsInfo,
                       AsyncLogger.Factory loggerFactory) throws IOException {
    this(conf, uri, nsInfo, null, loggerFactory);

  }

  
  QuorumJournalManager(Configuration conf,
      URI uri, NamespaceInfo nsInfo, String nameServiceId,
      AsyncLogger.Factory loggerFactory) throws IOException {
    Preconditions.checkArgument(conf != null, "must be configured");

    this.conf = conf;
    this.uri = uri;
    this.nsInfo = nsInfo;
    this.nameServiceId = nameServiceId;
    this.loggers = new AsyncLoggerSet(createLoggers(loggerFactory));

    // Configure timeouts.
    this.startSegmentTimeoutMs = conf.getInt(
        DFSConfigKeys.DFS_QJOURNAL_START_SEGMENT_TIMEOUT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_START_SEGMENT_TIMEOUT_DEFAULT);
    this.prepareRecoveryTimeoutMs = conf.getInt(
        DFSConfigKeys.DFS_QJOURNAL_PREPARE_RECOVERY_TIMEOUT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_PREPARE_RECOVERY_TIMEOUT_DEFAULT);
    this.acceptRecoveryTimeoutMs = conf.getInt(
        DFSConfigKeys.DFS_QJOURNAL_ACCEPT_RECOVERY_TIMEOUT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_ACCEPT_RECOVERY_TIMEOUT_DEFAULT);
    this.finalizeSegmentTimeoutMs = conf.getInt(
        DFSConfigKeys.DFS_QJOURNAL_FINALIZE_SEGMENT_TIMEOUT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_FINALIZE_SEGMENT_TIMEOUT_DEFAULT);
    this.selectInputStreamsTimeoutMs = conf.getInt(
        DFSConfigKeys.DFS_QJOURNAL_SELECT_INPUT_STREAMS_TIMEOUT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_SELECT_INPUT_STREAMS_TIMEOUT_DEFAULT);
    this.getJournalStateTimeoutMs = conf.getInt(
        DFSConfigKeys.DFS_QJOURNAL_GET_JOURNAL_STATE_TIMEOUT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_GET_JOURNAL_STATE_TIMEOUT_DEFAULT);
    this.newEpochTimeoutMs = conf.getInt(
        DFSConfigKeys.DFS_QJOURNAL_NEW_EPOCH_TIMEOUT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_NEW_EPOCH_TIMEOUT_DEFAULT);
    this.writeTxnsTimeoutMs = conf.getInt(
        DFSConfigKeys.DFS_QJOURNAL_WRITE_TXNS_TIMEOUT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_WRITE_TXNS_TIMEOUT_DEFAULT);
    this.timeoutMs = (int) conf.getTimeDuration(DFSConfigKeys
            .DFS_QJM_OPERATIONS_TIMEOUT,
        DFSConfigKeys.DFS_QJM_OPERATIONS_TIMEOUT_DEFAULT, TimeUnit
            .MILLISECONDS);

    int connectTimeoutMs = conf.getInt(
        DFSConfigKeys.DFS_QJOURNAL_HTTP_OPEN_TIMEOUT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_HTTP_OPEN_TIMEOUT_DEFAULT);
    int readTimeoutMs = conf.getInt(
        DFSConfigKeys.DFS_QJOURNAL_HTTP_READ_TIMEOUT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_HTTP_READ_TIMEOUT_DEFAULT);
    this.connectionFactory = URLConnectionFactory
        .newDefaultURLConnectionFactory(connectTimeoutMs, readTimeoutMs, conf);
  }
  
  protected List<AsyncLogger> createLoggers(
      AsyncLogger.Factory factory) throws IOException {
    return createLoggers(conf, uri, nsInfo, factory, nameServiceId);
  }

  static String parseJournalId(URI uri) {
    String path = uri.getPath();
    Preconditions.checkArgument(path != null && !path.isEmpty(),
        "Bad URI '%s': must identify journal in path component",
        uri);
    String journalId = path.substring(1);
    checkJournalId(journalId);
    return journalId;
  }
  
  public static void checkJournalId(String jid) {
    Preconditions.checkArgument(jid != null &&
        !jid.isEmpty() &&
        !jid.contains("/") &&
        !jid.startsWith("."),
        "bad journal id: " + jid);
  }

  
  /**
   * Fence any previous writers, and obtain a unique epoch number
   * for write-access to the journal nodes.
   *
   * @return the new, unique epoch number
   */
  Map<AsyncLogger, NewEpochResponseProto> createNewUniqueEpoch()
      throws IOException {
    Preconditions.checkState(!loggers.isEpochEstablished(),
        "epoch already created");
    
    Map<AsyncLogger, GetJournalStateResponseProto> lastPromises =
      loggers.waitForWriteQuorum(loggers.getJournalState(),
          getJournalStateTimeoutMs, "getJournalState()");
    
    long maxPromised = Long.MIN_VALUE;
    for (GetJournalStateResponseProto resp : lastPromises.values()) {
      maxPromised = Math.max(maxPromised, resp.getLastPromisedEpoch());
    }
    assert maxPromised >= 0;
    
    long myEpoch = maxPromised + 1;
    Map<AsyncLogger, NewEpochResponseProto> resps =
        loggers.waitForWriteQuorum(loggers.newEpoch(nsInfo, myEpoch),
            newEpochTimeoutMs, "newEpoch(" + myEpoch + ")");
        
    loggers.setEpoch(myEpoch);
    return resps;
  }
  
  @Override
  public void format(NamespaceInfo nsInfo, boolean force) throws IOException {
    QuorumCall<AsyncLogger, Void> call = loggers.format(nsInfo, force);
    try {
      call.waitFor(loggers.size(), loggers.size(), 0, timeoutMs,
          "format");
    } catch (InterruptedException e) {
      throw new IOException("Interrupted waiting for format() response");
    } catch (TimeoutException e) {
      throw new IOException("Timed out waiting for format() response");
    }
    
    if (call.countExceptions() > 0) {
      call.rethrowException("Could not format one or more JournalNodes");
    }
  }

  @Override
  public boolean hasSomeData() throws IOException {
    QuorumCall<AsyncLogger, Boolean> call =
        loggers.isFormatted();

    try {
      call.waitFor(loggers.size(), 0, 0, timeoutMs, "hasSomeData");
    } catch (InterruptedException e) {
      throw new IOException("Interrupted while determining if JNs have data");
    } catch (TimeoutException e) {
      throw new IOException("Timed out waiting for response from loggers");
    }
    
    if (call.countExceptions() > 0) {
      call.rethrowException(
          "Unable to check if JNs are ready for formatting");
    }
    
    // If any of the loggers returned with a non-empty manifest, then
    // we should prompt for format.
    for (Boolean hasData : call.getResults().values()) {
      if (hasData) {
        return true;
      }
    }

    // Otherwise, none were formatted, we can safely format.
    return false;
  }

  /**
   * Run recovery/synchronization for a specific segment.
   * Postconditions:
   * <ul>
   * <li>This segment will be finalized on a majority
   * of nodes.</li>
   * <li>All nodes which contain the finalized segment will
   * agree on the length.</li>
   * </ul>
   * 
   * @param segmentTxId the starting txid of the segment
   * @throws IOException
   */
  private void recoverUnclosedSegment(long segmentTxId) throws IOException {
    Preconditions.checkArgument(segmentTxId > 0);
    LOG.info("Beginning recovery of unclosed segment starting at txid " +
        segmentTxId);
    
    // Step 1. Prepare recovery
    QuorumCall<AsyncLogger,PrepareRecoveryResponseProto> prepare =
        loggers.prepareRecovery(segmentTxId);
    Map<AsyncLogger, PrepareRecoveryResponseProto> prepareResponses=
        loggers.waitForWriteQuorum(prepare, prepareRecoveryTimeoutMs,
            "prepareRecovery(" + segmentTxId + ")");
    LOG.info("Recovery prepare phase complete. Responses:\n" +
        QuorumCall.mapToString(prepareResponses));

    // Determine the logger who either:
    // a) Has already accepted a previous proposal that's higher than any
    //    other
    //
    //  OR, if no such logger exists:
    //
    // b) Has the longest log starting at this transaction ID
    
    // TODO: we should collect any "ties" and pass the URL for all of them
    // when syncing, so we can tolerate failure during recovery better.
    Entry<AsyncLogger, PrepareRecoveryResponseProto> bestEntry = Collections.max(
        prepareResponses.entrySet(), SegmentRecoveryComparator.INSTANCE); 
    AsyncLogger bestLogger = bestEntry.getKey();
    PrepareRecoveryResponseProto bestResponse = bestEntry.getValue();
    
    // Log the above decision, check invariants.
    if (bestResponse.hasAcceptedInEpoch()) {
      LOG.info("Using already-accepted recovery for segment " +
          "starting at txid " + segmentTxId + ": " +
          bestEntry);
    } else if (bestResponse.hasSegmentState()) {
      LOG.info("Using longest log: " + bestEntry);
    } else {
      // None of the responses to prepareRecovery() had a segment at the given
      // txid. This can happen for example in the following situation:
      // - 3 JNs: JN1, JN2, JN3
      // - writer starts segment 101 on JN1, then crashes before
      //   writing to JN2 and JN3
      // - during newEpoch(), we saw the segment on JN1 and decide to
      //   recover segment 101
      // - before prepare(), JN1 crashes, and we only talk to JN2 and JN3,
      //   neither of which has any entry for this log.
      // In this case, it is allowed to do nothing for recovery, since the
      // segment wasn't started on a quorum of nodes.

      // Sanity check: we should only get here if none of the responses had
      // a log. This should be a postcondition of the recovery comparator,
      // but a bug in the comparator might cause us to get here.
      for (PrepareRecoveryResponseProto resp : prepareResponses.values()) {
        assert !resp.hasSegmentState() :
          "One of the loggers had a response, but no best logger " +
          "was found.";
      }

      LOG.info("None of the responders had a log to recover: " +
          QuorumCall.mapToString(prepareResponses));
      return;
    }
    
    SegmentStateProto logToSync = bestResponse.getSegmentState();
    assert segmentTxId == logToSync.getStartTxId();
    
    // Sanity check: none of the loggers should be aware of a higher
    // txid than the txid we intend to truncate to
    for (Map.Entry<AsyncLogger, PrepareRecoveryResponseProto> e :
         prepareResponses.entrySet()) {
      AsyncLogger logger = e.getKey();
      PrepareRecoveryResponseProto resp = e.getValue();

      if (resp.hasLastCommittedTxId() &&
          resp.getLastCommittedTxId() > logToSync.getEndTxId()) {
        throw new AssertionError("Decided to synchronize log to " + logToSync +
            " but logger " + logger + " had seen txid " +
            resp.getLastCommittedTxId() + " committed");
      }
    }
    
    URL syncFromUrl = bestLogger.buildURLToFetchLogs(segmentTxId);
    
    QuorumCall<AsyncLogger,Void> accept = loggers.acceptRecovery(logToSync, syncFromUrl);
    loggers.waitForWriteQuorum(accept, acceptRecoveryTimeoutMs,
        "acceptRecovery(" + TextFormat.shortDebugString(logToSync) + ")");

    // If one of the loggers above missed the synchronization step above, but
    // we send a finalize() here, that's OK. It validates the log before
    // finalizing. Hence, even if it is not "in sync", it won't incorrectly
    // finalize.
    QuorumCall<AsyncLogger, Void> finalize =
        loggers.finalizeLogSegment(logToSync.getStartTxId(), logToSync.getEndTxId()); 
    loggers.waitForWriteQuorum(finalize, finalizeSegmentTimeoutMs,
        String.format("finalizeLogSegment(%s-%s)",
            logToSync.getStartTxId(),
            logToSync.getEndTxId()));
  }
  
  static List<AsyncLogger> createLoggers(Configuration conf,
                                         URI uri,
                                         NamespaceInfo nsInfo,
                                         AsyncLogger.Factory factory,
                                         String nameServiceId)
      throws IOException {
    List<AsyncLogger> ret = Lists.newArrayList();
    List<InetSocketAddress> addrs = Util.getAddressesList(uri);
    if (addrs.size() % 2 == 0) {
      LOG.warn("Quorum journal URI '" + uri + "' has an even number " +
          "of Journal Nodes specified. This is not recommended!");
    }
    String jid = parseJournalId(uri);
    for (InetSocketAddress addr : addrs) {
      ret.add(factory.createLogger(conf, nsInfo, jid, nameServiceId, addr));
    }
    return ret;
  }
  
  @Override
  public EditLogOutputStream startLogSegment(long txId, int layoutVersion)
      throws IOException {
    Preconditions.checkState(isActiveWriter,
        "must recover segments before starting a new one");
    QuorumCall<AsyncLogger, Void> q = loggers.startLogSegment(txId,
        layoutVersion);
    loggers.waitForWriteQuorum(q, startSegmentTimeoutMs,
        "startLogSegment(" + txId + ")");
    boolean updateCommittedTxId = conf.getBoolean(
        DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY,
        DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_DEFAULT);
    return new QuorumOutputStream(loggers, txId, outputBufferCapacity,
        writeTxnsTimeoutMs, updateCommittedTxId);
  }

  @Override
  public void finalizeLogSegment(long firstTxId, long lastTxId)
      throws IOException {
    QuorumCall<AsyncLogger,Void> q = loggers.finalizeLogSegment(
        firstTxId, lastTxId);
    loggers.waitForWriteQuorum(q, finalizeSegmentTimeoutMs,
        String.format("finalizeLogSegment(%s-%s)", firstTxId, lastTxId));
  }

  @Override
  public void setOutputBufferCapacity(int size) {
    outputBufferCapacity = size;
  }

  @Override
  public void purgeLogsOlderThan(long minTxIdToKeep) throws IOException {
    // This purges asynchronously -- there's no need to wait for a quorum
    // here, because it's always OK to fail.
    LOG.info("Purging remote journals older than txid " + minTxIdToKeep);
    loggers.purgeLogsOlderThan(minTxIdToKeep);
  }

  @Override
  public void recoverUnfinalizedSegments() throws IOException {
    Preconditions.checkState(!isActiveWriter, "already active writer");
    
    LOG.info("Starting recovery process for unclosed journal segments...");
    Map<AsyncLogger, NewEpochResponseProto> resps = createNewUniqueEpoch();
    LOG.info("Successfully started new epoch " + loggers.getEpoch());

    if (LOG.isDebugEnabled()) {
      LOG.debug("newEpoch(" + loggers.getEpoch() + ") responses:\n" +
        QuorumCall.mapToString(resps));
    }
    
    long mostRecentSegmentTxId = Long.MIN_VALUE;
    for (NewEpochResponseProto r : resps.values()) {
      if (r.hasLastSegmentTxId()) {
        mostRecentSegmentTxId = Math.max(mostRecentSegmentTxId,
            r.getLastSegmentTxId());
      }
    }
    
    // On a completely fresh system, none of the journals have any
    // segments, so there's nothing to recover.
    if (mostRecentSegmentTxId != Long.MIN_VALUE) {
      recoverUnclosedSegment(mostRecentSegmentTxId);
    }
    isActiveWriter = true;
  }

  @Override
  public void close() throws IOException {
    loggers.close();
  }

  public void selectInputStreams(Collection<EditLogInputStream> streams,
      long fromTxnId, boolean inProgressOk) throws IOException {
    selectInputStreams(streams, fromTxnId, inProgressOk, false);
  }

  @Override
  public void selectInputStreams(Collection<EditLogInputStream> streams,
      long fromTxnId, boolean inProgressOk,
      boolean onlyDurableTxns) throws IOException {

    QuorumCall<AsyncLogger, RemoteEditLogManifest> q =
        loggers.getEditLogManifest(fromTxnId, inProgressOk);
    Map<AsyncLogger, RemoteEditLogManifest> resps =
        loggers.waitForWriteQuorum(q, selectInputStreamsTimeoutMs,
            "selectInputStreams");
    
    LOG.debug("selectInputStream manifests:\n" +
        Joiner.on("\n").withKeyValueSeparator(": ").join(resps));
    
    final PriorityQueue<EditLogInputStream> allStreams = 
        new PriorityQueue<EditLogInputStream>(64,
            JournalSet.EDIT_LOG_INPUT_STREAM_COMPARATOR);
    for (Map.Entry<AsyncLogger, RemoteEditLogManifest> e : resps.entrySet()) {
      AsyncLogger logger = e.getKey();
      RemoteEditLogManifest manifest = e.getValue();
      long committedTxnId = manifest.getCommittedTxnId();

      for (RemoteEditLog remoteLog : manifest.getLogs()) {
        URL url = logger.buildURLToFetchLogs(remoteLog.getStartTxId());

        long endTxId = remoteLog.getEndTxId();

        // If it's bounded by durable Txns, endTxId could not be larger
        // than committedTxnId. This ensures the consistency.
        // We don't do the following for finalized log segments, since all
        // edits in those are guaranteed to be committed.
        if (onlyDurableTxns && inProgressOk && remoteLog.isInProgress()) {
          endTxId = Math.min(endTxId, committedTxnId);
          if (endTxId < remoteLog.getStartTxId()) {
            LOG.warn("Found endTxId (" + endTxId + ") that is less than " +
                "the startTxId (" + remoteLog.getStartTxId() +
                ") - setting it to startTxId.");
            endTxId = remoteLog.getStartTxId();
          }
        }

        EditLogInputStream elis = EditLogFileInputStream.fromUrl(
            connectionFactory, url, remoteLog.getStartTxId(),
            endTxId, remoteLog.isInProgress());
        allStreams.add(elis);
      }
    }
    JournalSet.chainAndMakeRedundantStreams(streams, allStreams, fromTxnId);
  }
  
  @Override
  public String toString() {
    return "QJM to " + loggers;
  }

  @VisibleForTesting
  AsyncLoggerSet getLoggerSetForTests() {
    return loggers;
  }

  @Override
  public void doPreUpgrade() throws IOException {
    QuorumCall<AsyncLogger, Void> call = loggers.doPreUpgrade();
    try {
      call.waitFor(loggers.size(), loggers.size(), 0, timeoutMs,
          "doPreUpgrade");
      
      if (call.countExceptions() > 0) {
        call.rethrowException("Could not do pre-upgrade of one or more JournalNodes");
      }
    } catch (InterruptedException e) {
      throw new IOException("Interrupted waiting for doPreUpgrade() response");
    } catch (TimeoutException e) {
      throw new IOException("Timed out waiting for doPreUpgrade() response");
    }
  }

  @Override
  public void doUpgrade(Storage storage) throws IOException {
    QuorumCall<AsyncLogger, Void> call = loggers.doUpgrade(storage);
    try {
      call.waitFor(loggers.size(), loggers.size(), 0, timeoutMs,
          "doUpgrade");
      
      if (call.countExceptions() > 0) {
        call.rethrowException("Could not perform upgrade of one or more JournalNodes");
      }
    } catch (InterruptedException e) {
      throw new IOException("Interrupted waiting for doUpgrade() response");
    } catch (TimeoutException e) {
      throw new IOException("Timed out waiting for doUpgrade() response");
    }
  }
  
  @Override
  public void doFinalize() throws IOException {
    QuorumCall<AsyncLogger, Void> call = loggers.doFinalize();
    try {
      call.waitFor(loggers.size(), loggers.size(), 0, timeoutMs,
          "doFinalize");
      
      if (call.countExceptions() > 0) {
        call.rethrowException("Could not finalize one or more JournalNodes");
      }
    } catch (InterruptedException e) {
      throw new IOException("Interrupted waiting for doFinalize() response");
    } catch (TimeoutException e) {
      throw new IOException("Timed out waiting for doFinalize() response");
    }
  }
  
  @Override
  public boolean canRollBack(StorageInfo storage, StorageInfo prevStorage,
      int targetLayoutVersion) throws IOException {
    QuorumCall<AsyncLogger, Boolean> call = loggers.canRollBack(storage,
        prevStorage, targetLayoutVersion);
    try {
      call.waitFor(loggers.size(), loggers.size(), 0, timeoutMs,
          "lockSharedStorage");
      
      if (call.countExceptions() > 0) {
        call.rethrowException("Could not check if roll back possible for"
            + " one or more JournalNodes");
      }
      
      // Either they all return the same thing or this call fails, so we can
      // just return the first result.
      try {
        DFSUtil.assertAllResultsEqual(call.getResults().values());
      } catch (AssertionError ae) {
        throw new IOException("Results differed for canRollBack", ae);
      }
      for (Boolean result : call.getResults().values()) {
        return result;
      }
    } catch (InterruptedException e) {
      throw new IOException("Interrupted waiting for lockSharedStorage() " +
          "response");
    } catch (TimeoutException e) {
      throw new IOException("Timed out waiting for lockSharedStorage() " +
          "response");
    }
    
    throw new AssertionError("Unreachable code.");
  }

  @Override
  public void doRollback() throws IOException {
    QuorumCall<AsyncLogger, Void> call = loggers.doRollback();
    try {
      call.waitFor(loggers.size(), loggers.size(), 0, timeoutMs,
          "doRollback");
      
      if (call.countExceptions() > 0) {
        call.rethrowException("Could not perform rollback of one or more JournalNodes");
      }
    } catch (InterruptedException e) {
      throw new IOException("Interrupted waiting for doFinalize() response");
    } catch (TimeoutException e) {
      throw new IOException("Timed out waiting for doFinalize() response");
    }
  }
  
  @Override
  public void discardSegments(long startTxId) throws IOException {
    QuorumCall<AsyncLogger, Void> call = loggers.discardSegments(startTxId);
    try {
      call.waitFor(loggers.size(), loggers.size(), 0,
          timeoutMs, "discardSegments");
      if (call.countExceptions() > 0) {
        call.rethrowException(
            "Could not perform discardSegments of one or more JournalNodes");
      }
    } catch (InterruptedException e) {
      throw new IOException(
          "Interrupted waiting for discardSegments() response");
    } catch (TimeoutException e) {
      throw new IOException(
          "Timed out waiting for discardSegments() response");
    }
  }
  
  @Override
  public long getJournalCTime() throws IOException {
    QuorumCall<AsyncLogger, Long> call = loggers.getJournalCTime();
    try {
      call.waitFor(loggers.size(), loggers.size(), 0,
          timeoutMs, "getJournalCTime");
      
      if (call.countExceptions() > 0) {
        call.rethrowException("Could not journal CTime for one "
            + "more JournalNodes");
      }
      
      // Either they all return the same thing or this call fails, so we can
      // just return the first result.
      try {
        DFSUtil.assertAllResultsEqual(call.getResults().values());
      } catch (AssertionError ae) {
        throw new IOException("Results differed for getJournalCTime", ae);
      }
      for (Long result : call.getResults().values()) {
        return result;
      }
    } catch (InterruptedException e) {
      throw new IOException("Interrupted waiting for getJournalCTime() " +
          "response");
    } catch (TimeoutException e) {
      throw new IOException("Timed out waiting for getJournalCTime() " +
          "response");
    }
    
    throw new AssertionError("Unreachable code.");
  }
}
