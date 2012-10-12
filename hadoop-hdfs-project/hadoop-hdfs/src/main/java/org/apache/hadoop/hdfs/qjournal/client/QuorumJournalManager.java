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
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournalStateResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.SegmentStateProto;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileInputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.JournalManager;
import org.apache.hadoop.hdfs.server.namenode.JournalSet;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.StringUtils;

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
  static final Log LOG = LogFactory.getLog(QuorumJournalManager.class);

  // Timeouts for which the QJM will wait for each of the following actions.
  private final int startSegmentTimeoutMs;
  private final int prepareRecoveryTimeoutMs;
  private final int acceptRecoveryTimeoutMs;
  private final int finalizeSegmentTimeoutMs;
  private final int selectInputStreamsTimeoutMs;
  private final int getJournalStateTimeoutMs;
  private final int newEpochTimeoutMs;
  private final int writeTxnsTimeoutMs;

  // Since these don't occur during normal operation, we can
  // use rather lengthy timeouts, and don't need to make them
  // configurable.
  private static final int FORMAT_TIMEOUT_MS = 60000;
  private static final int HASDATA_TIMEOUT_MS = 60000;
  
  private final Configuration conf;
  private final URI uri;
  private final NamespaceInfo nsInfo;
  private boolean isActiveWriter;
  
  private final AsyncLoggerSet loggers;

  private int outputBufferCapacity = 512 * 1024;
  
  public QuorumJournalManager(Configuration conf,
      URI uri, NamespaceInfo nsInfo) throws IOException {
    this(conf, uri, nsInfo, IPCLoggerChannel.FACTORY);
  }
  
  QuorumJournalManager(Configuration conf,
      URI uri, NamespaceInfo nsInfo,
      AsyncLogger.Factory loggerFactory) throws IOException {
    Preconditions.checkArgument(conf != null, "must be configured");

    this.conf = conf;
    this.uri = uri;
    this.nsInfo = nsInfo;
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
  }
  
  protected List<AsyncLogger> createLoggers(
      AsyncLogger.Factory factory) throws IOException {
    return createLoggers(conf, uri, nsInfo, factory);
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
  public void format(NamespaceInfo nsInfo) throws IOException {
    QuorumCall<AsyncLogger,Void> call = loggers.format(nsInfo);
    try {
      call.waitFor(loggers.size(), loggers.size(), 0, FORMAT_TIMEOUT_MS,
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
      call.waitFor(loggers.size(), 0, 0, HASDATA_TIMEOUT_MS, "hasSomeData");
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
      URI uri, NamespaceInfo nsInfo, AsyncLogger.Factory factory)
          throws IOException {
    List<AsyncLogger> ret = Lists.newArrayList();
    List<InetSocketAddress> addrs = getLoggerAddresses(uri);
    String jid = parseJournalId(uri);
    for (InetSocketAddress addr : addrs) {
      ret.add(factory.createLogger(conf, nsInfo, jid, addr));
    }
    return ret;
  }
 
  private static List<InetSocketAddress> getLoggerAddresses(URI uri)
      throws IOException {
    String authority = uri.getAuthority();
    Preconditions.checkArgument(authority != null && !authority.isEmpty(),
        "URI has no authority: " + uri);
    
    String[] parts = StringUtils.split(authority, ';');
    for (int i = 0; i < parts.length; i++) {
      parts[i] = parts[i].trim();
    }

    if (parts.length % 2 == 0) {
      LOG.warn("Quorum journal URI '" + uri + "' has an even number " +
          "of Journal Nodes specified. This is not recommended!");
    }
    
    List<InetSocketAddress> addrs = Lists.newArrayList();
    for (String addr : parts) {
      addrs.add(NetUtils.createSocketAddr(
          addr, DFSConfigKeys.DFS_JOURNALNODE_RPC_PORT_DEFAULT));
    }
    return addrs;
  }
  
  @Override
  public EditLogOutputStream startLogSegment(long txId) throws IOException {
    Preconditions.checkState(isActiveWriter,
        "must recover segments before starting a new one");
    QuorumCall<AsyncLogger,Void> q = loggers.startLogSegment(txId);
    loggers.waitForWriteQuorum(q, startSegmentTimeoutMs,
        "startLogSegment(" + txId + ")");
    return new QuorumOutputStream(loggers, txId,
        outputBufferCapacity, writeTxnsTimeoutMs);
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

  @Override
  public void selectInputStreams(Collection<EditLogInputStream> streams,
      long fromTxnId, boolean inProgressOk) throws IOException {

    QuorumCall<AsyncLogger, RemoteEditLogManifest> q =
        loggers.getEditLogManifest(fromTxnId);
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
      
      for (RemoteEditLog remoteLog : manifest.getLogs()) {
        URL url = logger.buildURLToFetchLogs(remoteLog.getStartTxId());

        EditLogInputStream elis = EditLogFileInputStream.fromUrl(
            url, remoteLog.getStartTxId(), remoteLog.getEndTxId(),
            remoteLog.isInProgress());
        allStreams.add(elis);
      }
    }
    JournalSet.chainAndMakeRedundantStreams(
        streams, allStreams, fromTxnId, inProgressOk);
  }
  
  @Override
  public String toString() {
    return "QJM to " + loggers;
  }

  @VisibleForTesting
  AsyncLoggerSet getLoggerSetForTests() {
    return loggers;
  }

}
