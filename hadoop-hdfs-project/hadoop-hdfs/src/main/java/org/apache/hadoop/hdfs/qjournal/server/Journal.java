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
package org.apache.hadoop.hdfs.qjournal.server;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.qjournal.protocol.JournalNotFormattedException;
import org.apache.hadoop.hdfs.qjournal.protocol.JournalOutOfSyncException;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PersistedRecoveryPaxosData;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.SegmentStateProto;
import org.apache.hadoop.hdfs.qjournal.protocol.RequestInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.StorageErrorReporter;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager.EditLogFile;
import org.apache.hadoop.hdfs.server.namenode.JournalManager;
import org.apache.hadoop.hdfs.server.namenode.TransferFsImage;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.util.AtomicFileOutputStream;
import org.apache.hadoop.hdfs.util.BestEffortLongFile;
import org.apache.hadoop.hdfs.util.PersistentLongFile;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.collect.Ranges;
import com.google.protobuf.TextFormat;

/**
 * A JournalNode can manage journals for several clusters at once.
 * Each such journal is entirely independent despite being hosted by
 * the same JVM.
 */
public class Journal implements Closeable {
  static final Log LOG = LogFactory.getLog(Journal.class);


  // Current writing state
  private EditLogOutputStream curSegment;
  private long curSegmentTxId = HdfsConstants.INVALID_TXID;
  private long nextTxId = HdfsConstants.INVALID_TXID;
  private long highestWrittenTxId = 0;
  
  private final String journalId;
  
  private final JNStorage storage;

  /**
   * When a new writer comes along, it asks each node to promise
   * to ignore requests from any previous writer, as identified
   * by epoch number. In order to make such a promise, the epoch
   * number of that writer is stored persistently on disk.
   */
  private PersistentLongFile lastPromisedEpoch;

  /**
   * Each IPC that comes from a given client contains a serial number
   * which only increases from the client's perspective. Whenever
   * we switch epochs, we reset this back to -1. Whenever an IPC
   * comes from a client, we ensure that it is strictly higher
   * than any previous IPC. This guards against any bugs in the IPC
   * layer that would re-order IPCs or cause a stale retry from an old
   * request to resurface and confuse things.
   */
  private long currentEpochIpcSerial = -1;
  
  /**
   * The epoch number of the last writer to actually write a transaction.
   * This is used to differentiate log segments after a crash at the very
   * beginning of a segment. See the the 'testNewerVersionOfSegmentWins'
   * test case.
   */
  private PersistentLongFile lastWriterEpoch;
  
  /**
   * Lower-bound on the last committed transaction ID. This is not
   * depended upon for correctness, but acts as a sanity check
   * during the recovery procedures, and as a visibility mark
   * for clients reading in-progress logs.
   */
  private BestEffortLongFile committedTxnId;
  
  public static final String LAST_PROMISED_FILENAME = "last-promised-epoch";
  public static final String LAST_WRITER_EPOCH = "last-writer-epoch";
  private static final String COMMITTED_TXID_FILENAME = "committed-txid";
  
  private final FileJournalManager fjm;

  private final JournalMetrics metrics;

  /**
   * Time threshold for sync calls, beyond which a warning should be logged to the console.
   */
  private static final int WARN_SYNC_MILLIS_THRESHOLD = 1000;

  Journal(Configuration conf, File logDir, String journalId,
      StartupOption startOpt, StorageErrorReporter errorReporter)
      throws IOException {
    storage = new JNStorage(conf, logDir, startOpt, errorReporter);
    this.journalId = journalId;

    refreshCachedData();
    
    this.fjm = storage.getJournalManager();
    
    this.metrics = JournalMetrics.create(this);
    
    EditLogFile latest = scanStorageForLatestEdits();
    if (latest != null) {
      highestWrittenTxId = latest.getLastTxId();
    }
  }

  /**
   * Reload any data that may have been cached. This is necessary
   * when we first load the Journal, but also after any formatting
   * operation, since the cached data is no longer relevant.
   */
  private synchronized void refreshCachedData() {
    IOUtils.closeStream(committedTxnId);
    
    File currentDir = storage.getSingularStorageDir().getCurrentDir();
    this.lastPromisedEpoch = new PersistentLongFile(
        new File(currentDir, LAST_PROMISED_FILENAME), 0);
    this.lastWriterEpoch = new PersistentLongFile(
        new File(currentDir, LAST_WRITER_EPOCH), 0);
    this.committedTxnId = new BestEffortLongFile(
        new File(currentDir, COMMITTED_TXID_FILENAME),
        HdfsConstants.INVALID_TXID);
  }
  
  /**
   * Scan the local storage directory, and return the segment containing
   * the highest transaction.
   * @return the EditLogFile with the highest transactions, or null
   * if no files exist.
   */
  private synchronized EditLogFile scanStorageForLatestEdits() throws IOException {
    if (!fjm.getStorageDirectory().getCurrentDir().exists()) {
      return null;
    }
    
    LOG.info("Scanning storage " + fjm);
    List<EditLogFile> files = fjm.getLogFiles(0);
    
    while (!files.isEmpty()) {
      EditLogFile latestLog = files.remove(files.size() - 1);
      latestLog.scanLog();
      LOG.info("Latest log is " + latestLog);
      if (latestLog.getLastTxId() == HdfsConstants.INVALID_TXID) {
        // the log contains no transactions
        LOG.warn("Latest log " + latestLog + " has no transactions. " +
            "moving it aside and looking for previous log");
        latestLog.moveAsideEmptyFile();
      } else {
        return latestLog;
      }
    }
    
    LOG.info("No files in " + fjm);
    return null;
  }

  /**
   * Format the local storage with the given namespace.
   */
  void format(NamespaceInfo nsInfo) throws IOException {
    Preconditions.checkState(nsInfo.getNamespaceID() != 0,
        "can't format with uninitialized namespace info: %s",
        nsInfo);
    LOG.info("Formatting " + this + " with namespace info: " +
        nsInfo);
    storage.format(nsInfo);
    refreshCachedData();
  }

  /**
   * Unlock and release resources.
   */
  @Override // Closeable
  public void close() throws IOException {
    storage.close();
    IOUtils.closeStream(committedTxnId);
    IOUtils.closeStream(curSegment);
  }
  
  JNStorage getStorage() {
    return storage;
  }
  
  String getJournalId() {
    return journalId;
  }

  /**
   * @return the last epoch which this node has promised not to accept
   * any lower epoch, or 0 if no promises have been made.
   */
  synchronized long getLastPromisedEpoch() throws IOException {
    checkFormatted();
    return lastPromisedEpoch.get();
  }

  synchronized public long getLastWriterEpoch() throws IOException {
    checkFormatted();
    return lastWriterEpoch.get();
  }
  
  synchronized long getCommittedTxnIdForTests() throws IOException {
    return committedTxnId.get();
  }
  
  synchronized long getCurrentLagTxns() throws IOException {
    long committed = committedTxnId.get();
    if (committed == 0) {
      return 0;
    }
    
    return Math.max(committed - highestWrittenTxId, 0L);
  }
  
  synchronized long getHighestWrittenTxId() {
    return highestWrittenTxId;
  }
  
  @VisibleForTesting
  JournalMetrics getMetricsForTests() {
    return metrics;
  }

  /**
   * Try to create a new epoch for this journal.
   * @param nsInfo the namespace, which is verified for consistency or used to
   * format, if the Journal has not yet been written to.
   * @param epoch the epoch to start
   * @return the status information necessary to begin recovery
   * @throws IOException if the node has already made a promise to another
   * writer with a higher epoch number, if the namespace is inconsistent,
   * or if a disk error occurs.
   */
  synchronized NewEpochResponseProto newEpoch(
      NamespaceInfo nsInfo, long epoch) throws IOException {

    checkFormatted();
    storage.checkConsistentNamespace(nsInfo);

    // Check that the new epoch being proposed is in fact newer than
    // any other that we've promised. 
    if (epoch <= getLastPromisedEpoch()) {
      throw new IOException("Proposed epoch " + epoch + " <= last promise " +
          getLastPromisedEpoch());
    }
    
    updateLastPromisedEpoch(epoch);
    abortCurSegment();
    
    NewEpochResponseProto.Builder builder =
        NewEpochResponseProto.newBuilder();

    EditLogFile latestFile = scanStorageForLatestEdits();

    if (latestFile != null) {
      builder.setLastSegmentTxId(latestFile.getFirstTxId());
    }
    
    return builder.build();
  }

  private void updateLastPromisedEpoch(long newEpoch) throws IOException {
    LOG.info("Updating lastPromisedEpoch from " + lastPromisedEpoch.get() +
        " to " + newEpoch + " for client " + Server.getRemoteIp());
    lastPromisedEpoch.set(newEpoch);
    
    // Since we have a new writer, reset the IPC serial - it will start
    // counting again from 0 for this writer.
    currentEpochIpcSerial = -1;
  }

  private void abortCurSegment() throws IOException {
    if (curSegment == null) {
      return;
    }
    
    curSegment.abort();
    curSegment = null;
    curSegmentTxId = HdfsConstants.INVALID_TXID;
  }

  /**
   * Write a batch of edits to the journal.
   * {@see QJournalProtocol#journal(RequestInfo, long, long, int, byte[])}
   */
  synchronized void journal(RequestInfo reqInfo,
      long segmentTxId, long firstTxnId,
      int numTxns, byte[] records) throws IOException {
    checkFormatted();
    checkWriteRequest(reqInfo);

    checkSync(curSegment != null,
        "Can't write, no segment open");
    
    if (curSegmentTxId != segmentTxId) {
      // Sanity check: it is possible that the writer will fail IPCs
      // on both the finalize() and then the start() of the next segment.
      // This could cause us to continue writing to an old segment
      // instead of rolling to a new one, which breaks one of the
      // invariants in the design. If it happens, abort the segment
      // and throw an exception.
      JournalOutOfSyncException e = new JournalOutOfSyncException(
          "Writer out of sync: it thinks it is writing segment " + segmentTxId
          + " but current segment is " + curSegmentTxId);
      abortCurSegment();
      throw e;
    }
      
    checkSync(nextTxId == firstTxnId,
        "Can't write txid " + firstTxnId + " expecting nextTxId=" + nextTxId);
    
    long lastTxnId = firstTxnId + numTxns - 1;
    if (LOG.isTraceEnabled()) {
      LOG.trace("Writing txid " + firstTxnId + "-" + lastTxnId);
    }

    // If the edit has already been marked as committed, we know
    // it has been fsynced on a quorum of other nodes, and we are
    // "catching up" with the rest. Hence we do not need to fsync.
    boolean isLagging = lastTxnId <= committedTxnId.get();
    boolean shouldFsync = !isLagging;
    
    curSegment.writeRaw(records, 0, records.length);
    curSegment.setReadyToFlush();
    Stopwatch sw = new Stopwatch();
    sw.start();
    curSegment.flush(shouldFsync);
    sw.stop();
    
    metrics.addSync(sw.elapsedTime(TimeUnit.MICROSECONDS));
    if (sw.elapsedTime(TimeUnit.MILLISECONDS) > WARN_SYNC_MILLIS_THRESHOLD) {
      LOG.warn("Sync of transaction range " + firstTxnId + "-" + lastTxnId +
               " took " + sw.elapsedTime(TimeUnit.MILLISECONDS) + "ms");
    }

    if (isLagging) {
      // This batch of edits has already been committed on a quorum of other
      // nodes. So, we are in "catch up" mode. This gets its own metric.
      metrics.batchesWrittenWhileLagging.incr(1);
    }
    
    metrics.batchesWritten.incr(1);
    metrics.bytesWritten.incr(records.length);
    metrics.txnsWritten.incr(numTxns);
    
    highestWrittenTxId = lastTxnId;
    nextTxId = lastTxnId + 1;
  }

  public void heartbeat(RequestInfo reqInfo) throws IOException {
    checkRequest(reqInfo);
  }
  
  /**
   * Ensure that the given request is coming from the correct writer and in-order.
   * @param reqInfo the request info
   * @throws IOException if the request is invalid.
   */
  private synchronized void checkRequest(RequestInfo reqInfo) throws IOException {
    // Invariant 25 from ZAB paper
    if (reqInfo.getEpoch() < lastPromisedEpoch.get()) {
      throw new IOException("IPC's epoch " + reqInfo.getEpoch() +
          " is less than the last promised epoch " +
          lastPromisedEpoch.get());
    } else if (reqInfo.getEpoch() > lastPromisedEpoch.get()) {
      // A newer client has arrived. Fence any previous writers by updating
      // the promise.
      updateLastPromisedEpoch(reqInfo.getEpoch());
    }
    
    // Ensure that the IPCs are arriving in-order as expected.
    checkSync(reqInfo.getIpcSerialNumber() > currentEpochIpcSerial,
        "IPC serial %s from client %s was not higher than prior highest " +
        "IPC serial %s", reqInfo.getIpcSerialNumber(),
        Server.getRemoteIp(),
        currentEpochIpcSerial);
    currentEpochIpcSerial = reqInfo.getIpcSerialNumber();

    if (reqInfo.hasCommittedTxId()) {
      Preconditions.checkArgument(
          reqInfo.getCommittedTxId() >= committedTxnId.get(),
          "Client trying to move committed txid backward from " +
          committedTxnId.get() + " to " + reqInfo.getCommittedTxId());
      
      committedTxnId.set(reqInfo.getCommittedTxId());
    }
  }
  
  private synchronized void checkWriteRequest(RequestInfo reqInfo) throws IOException {
    checkRequest(reqInfo);
    
    if (reqInfo.getEpoch() != lastWriterEpoch.get()) {
      throw new IOException("IPC's epoch " + reqInfo.getEpoch() +
          " is not the current writer epoch  " +
          lastWriterEpoch.get());
    }
  }
  
  public synchronized boolean isFormatted() {
    return storage.isFormatted();
  }

  private void checkFormatted() throws JournalNotFormattedException {
    if (!isFormatted()) {
      throw new JournalNotFormattedException("Journal " +
          storage.getSingularStorageDir() + " not formatted");
    }
  }

  /**
   * @throws JournalOutOfSyncException if the given expression is not true.
   * The message of the exception is formatted using the 'msg' and
   * 'formatArgs' parameters.
   */
  private void checkSync(boolean expression, String msg,
      Object... formatArgs) throws JournalOutOfSyncException {
    if (!expression) {
      throw new JournalOutOfSyncException(String.format(msg, formatArgs));
    }
  }

  /**
   * @throws AssertionError if the given expression is not true.
   * The message of the exception is formatted using the 'msg' and
   * 'formatArgs' parameters.
   * 
   * This should be used in preference to Java's built-in assert in
   * non-performance-critical paths, where a failure of this invariant
   * might cause the protocol to lose data. 
   */
  private void alwaysAssert(boolean expression, String msg,
      Object... formatArgs) {
    if (!expression) {
      throw new AssertionError(String.format(msg, formatArgs));
    }
  }
  
  /**
   * Start a new segment at the given txid. The previous segment
   * must have already been finalized.
   */
  public synchronized void startLogSegment(RequestInfo reqInfo, long txid,
      int layoutVersion) throws IOException {
    assert fjm != null;
    checkFormatted();
    checkRequest(reqInfo);
    
    if (curSegment != null) {
      LOG.warn("Client is requesting a new log segment " + txid + 
          " though we are already writing " + curSegment + ". " +
          "Aborting the current segment in order to begin the new one.");
      // The writer may have lost a connection to us and is now
      // re-connecting after the connection came back.
      // We should abort our own old segment.
      abortCurSegment();
    }

    // Paranoid sanity check: we should never overwrite a finalized log file.
    // Additionally, if it's in-progress, it should have at most 1 transaction.
    // This can happen if the writer crashes exactly at the start of a segment.
    EditLogFile existing = fjm.getLogFile(txid);
    if (existing != null) {
      if (!existing.isInProgress()) {
        throw new IllegalStateException("Already have a finalized segment " +
            existing + " beginning at " + txid);
      }
      
      // If it's in-progress, it should only contain one transaction,
      // because the "startLogSegment" transaction is written alone at the
      // start of each segment. 
      existing.scanLog();
      if (existing.getLastTxId() != existing.getFirstTxId()) {
        throw new IllegalStateException("The log file " +
            existing + " seems to contain valid transactions");
      }
    }
    
    long curLastWriterEpoch = lastWriterEpoch.get();
    if (curLastWriterEpoch != reqInfo.getEpoch()) {
      LOG.info("Updating lastWriterEpoch from " + curLastWriterEpoch +
          " to " + reqInfo.getEpoch() + " for client " +
          Server.getRemoteIp());
      lastWriterEpoch.set(reqInfo.getEpoch());
    }

    // The fact that we are starting a segment at this txid indicates
    // that any previous recovery for this same segment was aborted.
    // Otherwise, no writer would have started writing. So, we can
    // remove the record of the older segment here.
    purgePaxosDecision(txid);
    
    curSegment = fjm.startLogSegment(txid, layoutVersion);
    curSegmentTxId = txid;
    nextTxId = txid;
  }
  
  /**
   * Finalize the log segment at the given transaction ID.
   */
  public synchronized void finalizeLogSegment(RequestInfo reqInfo, long startTxId,
      long endTxId) throws IOException {
    checkFormatted();
    checkRequest(reqInfo);

    boolean needsValidation = true;

    // Finalizing the log that the writer was just writing.
    if (startTxId == curSegmentTxId) {
      if (curSegment != null) {
        curSegment.close();
        curSegment = null;
        curSegmentTxId = HdfsConstants.INVALID_TXID;
      }
      
      checkSync(nextTxId == endTxId + 1,
          "Trying to finalize in-progress log segment %s to end at " +
          "txid %s but only written up to txid %s",
          startTxId, endTxId, nextTxId - 1);
      // No need to validate the edit log if the client is finalizing
      // the log segment that it was just writing to.
      needsValidation = false;
    }
    
    FileJournalManager.EditLogFile elf = fjm.getLogFile(startTxId);
    if (elf == null) {
      throw new JournalOutOfSyncException("No log file to finalize at " +
          "transaction ID " + startTxId);
    }

    if (elf.isInProgress()) {
      if (needsValidation) {
        LOG.info("Validating log segment " + elf.getFile() + " about to be " +
            "finalized");
        elf.scanLog();
  
        checkSync(elf.getLastTxId() == endTxId,
            "Trying to finalize in-progress log segment %s to end at " +
            "txid %s but log %s on disk only contains up to txid %s",
            startTxId, endTxId, elf.getFile(), elf.getLastTxId());
      }
      fjm.finalizeLogSegment(startTxId, endTxId);
    } else {
      Preconditions.checkArgument(endTxId == elf.getLastTxId(),
          "Trying to re-finalize already finalized log " +
              elf + " with different endTxId " + endTxId);
    }

    // Once logs are finalized, a different length will never be decided.
    // During recovery, we treat a finalized segment the same as an accepted
    // recovery. Thus, we no longer need to keep track of the previously-
    // accepted decision. The existence of the finalized log segment is enough.
    purgePaxosDecision(elf.getFirstTxId());
  }
  
  /**
   * @see JournalManager#purgeLogsOlderThan(long)
   */
  public synchronized void purgeLogsOlderThan(RequestInfo reqInfo,
      long minTxIdToKeep) throws IOException {
    checkFormatted();
    checkRequest(reqInfo);
    
    storage.purgeDataOlderThan(minTxIdToKeep);
  }
  
  /**
   * Remove the previously-recorded 'accepted recovery' information
   * for a given log segment, once it is no longer necessary. 
   * @param segmentTxId the transaction ID to purge
   * @throws IOException if the file could not be deleted
   */
  private void purgePaxosDecision(long segmentTxId) throws IOException {
    File paxosFile = storage.getPaxosFile(segmentTxId);
    if (paxosFile.exists()) {
      if (!paxosFile.delete()) {
        throw new IOException("Unable to delete paxos file " + paxosFile);
      }
    }
  }

  /**
   * @see QJournalProtocol#getEditLogManifest(String, long, boolean)
   */
  public RemoteEditLogManifest getEditLogManifest(long sinceTxId,
      boolean inProgressOk) throws IOException {
    // No need to checkRequest() here - anyone may ask for the list
    // of segments.
    checkFormatted();
    
    List<RemoteEditLog> logs = fjm.getRemoteEditLogs(sinceTxId, inProgressOk);
    
    if (inProgressOk) {
      RemoteEditLog log = null;
      for (Iterator<RemoteEditLog> iter = logs.iterator(); iter.hasNext();) {
        log = iter.next();
        if (log.isInProgress()) {
          iter.remove();
          break;
        }
      }
      if (log != null && log.isInProgress()) {
        logs.add(new RemoteEditLog(log.getStartTxId(), getHighestWrittenTxId(),
            true));
      }
    }
    
    return new RemoteEditLogManifest(logs);
  }

  /**
   * @return the current state of the given segment, or null if the
   * segment does not exist.
   */
  @VisibleForTesting
  SegmentStateProto getSegmentInfo(long segmentTxId)
      throws IOException {
    EditLogFile elf = fjm.getLogFile(segmentTxId);
    if (elf == null) {
      return null;
    }
    if (elf.isInProgress()) {
      elf.scanLog();
    }
    if (elf.getLastTxId() == HdfsConstants.INVALID_TXID) {
      LOG.info("Edit log file " + elf + " appears to be empty. " +
          "Moving it aside...");
      elf.moveAsideEmptyFile();
      return null;
    }
    SegmentStateProto ret = SegmentStateProto.newBuilder()
        .setStartTxId(segmentTxId)
        .setEndTxId(elf.getLastTxId())
        .setIsInProgress(elf.isInProgress())
        .build();
    LOG.info("getSegmentInfo(" + segmentTxId + "): " + elf + " -> " +
        TextFormat.shortDebugString(ret));
    return ret;
  }

  /**
   * @see QJournalProtocol#prepareRecovery(RequestInfo, long)
   */
  public synchronized PrepareRecoveryResponseProto prepareRecovery(
      RequestInfo reqInfo, long segmentTxId) throws IOException {
    checkFormatted();
    checkRequest(reqInfo);
    
    abortCurSegment();
    
    PrepareRecoveryResponseProto.Builder builder =
        PrepareRecoveryResponseProto.newBuilder();

    PersistedRecoveryPaxosData previouslyAccepted = getPersistedPaxosData(segmentTxId);
    completeHalfDoneAcceptRecovery(previouslyAccepted);

    SegmentStateProto segInfo = getSegmentInfo(segmentTxId);
    boolean hasFinalizedSegment = segInfo != null && !segInfo.getIsInProgress();

    if (previouslyAccepted != null && !hasFinalizedSegment) {
      SegmentStateProto acceptedState = previouslyAccepted.getSegmentState();
      assert acceptedState.getEndTxId() == segInfo.getEndTxId() :
            "prev accepted: " + TextFormat.shortDebugString(previouslyAccepted)+ "\n" +
            "on disk:       " + TextFormat.shortDebugString(segInfo);
            
      builder.setAcceptedInEpoch(previouslyAccepted.getAcceptedInEpoch())
        .setSegmentState(previouslyAccepted.getSegmentState());
    } else {
      if (segInfo != null) {
        builder.setSegmentState(segInfo);
      }
    }
    
    builder.setLastWriterEpoch(lastWriterEpoch.get());
    if (committedTxnId.get() != HdfsConstants.INVALID_TXID) {
      builder.setLastCommittedTxId(committedTxnId.get());
    }
    
    PrepareRecoveryResponseProto resp = builder.build();
    LOG.info("Prepared recovery for segment " + segmentTxId + ": " +
        TextFormat.shortDebugString(resp));
    return resp;
  }
  
  /**
   * @see QJournalProtocol#acceptRecovery(RequestInfo, QJournalProtocolProtos.SegmentStateProto, URL)
   */
  public synchronized void acceptRecovery(RequestInfo reqInfo,
      SegmentStateProto segment, URL fromUrl)
      throws IOException {
    checkFormatted();
    checkRequest(reqInfo);
    
    abortCurSegment();

    long segmentTxId = segment.getStartTxId();

    // Basic sanity checks that the segment is well-formed and contains
    // at least one transaction.
    Preconditions.checkArgument(segment.getEndTxId() > 0 &&
        segment.getEndTxId() >= segmentTxId,
        "bad recovery state for segment %s: %s",
        segmentTxId, TextFormat.shortDebugString(segment));
    
    PersistedRecoveryPaxosData oldData = getPersistedPaxosData(segmentTxId);
    PersistedRecoveryPaxosData newData = PersistedRecoveryPaxosData.newBuilder()
        .setAcceptedInEpoch(reqInfo.getEpoch())
        .setSegmentState(segment)
        .build();
    
    // If we previously acted on acceptRecovery() from a higher-numbered writer,
    // this call is out of sync. We should never actually trigger this, since the
    // checkRequest() call above should filter non-increasing epoch numbers.
    if (oldData != null) {
      alwaysAssert(oldData.getAcceptedInEpoch() <= reqInfo.getEpoch(),
          "Bad paxos transition, out-of-order epochs.\nOld: %s\nNew: %s\n",
          oldData, newData);
    }
    
    File syncedFile = null;
    
    SegmentStateProto currentSegment = getSegmentInfo(segmentTxId);
    if (currentSegment == null ||
        currentSegment.getEndTxId() != segment.getEndTxId()) {
      if (currentSegment == null) {
        LOG.info("Synchronizing log " + TextFormat.shortDebugString(segment) +
            ": no current segment in place");
        
        // Update the highest txid for lag metrics
        highestWrittenTxId = Math.max(segment.getEndTxId(),
            highestWrittenTxId);
      } else {
        LOG.info("Synchronizing log " + TextFormat.shortDebugString(segment) +
            ": old segment " + TextFormat.shortDebugString(currentSegment) +
            " is not the right length");
        
        // Paranoid sanity check: if the new log is shorter than the log we
        // currently have, we should not end up discarding any transactions
        // which are already Committed.
        if (txnRange(currentSegment).contains(committedTxnId.get()) &&
            !txnRange(segment).contains(committedTxnId.get())) {
          throw new AssertionError(
              "Cannot replace segment " +
              TextFormat.shortDebugString(currentSegment) +
              " with new segment " +
              TextFormat.shortDebugString(segment) + 
              ": would discard already-committed txn " +
              committedTxnId.get());
        }
        
        // Another paranoid check: we should not be asked to synchronize a log
        // on top of a finalized segment.
        alwaysAssert(currentSegment.getIsInProgress(),
            "Should never be asked to synchronize a different log on top of an " +
            "already-finalized segment");
        
        // If we're shortening the log, update our highest txid
        // used for lag metrics.
        if (txnRange(currentSegment).contains(highestWrittenTxId)) {
          highestWrittenTxId = segment.getEndTxId();
        }
      }
      syncedFile = syncLog(reqInfo, segment, fromUrl);
      
    } else {
      LOG.info("Skipping download of log " +
          TextFormat.shortDebugString(segment) +
          ": already have up-to-date logs");
    }
    
    // This is one of the few places in the protocol where we have a single
    // RPC that results in two distinct actions:
    //
    // - 1) Downloads the new log segment data (above)
    // - 2) Records the new Paxos data about the synchronized segment (below)
    //
    // These need to be treated as a transaction from the perspective
    // of any external process. We do this by treating the persistPaxosData()
    // success as the "commit" of an atomic transaction. If we fail before
    // this point, the downloaded edit log will only exist at a temporary
    // path, and thus not change any externally visible state. If we fail
    // after this point, then any future prepareRecovery() call will see
    // the Paxos data, and by calling completeHalfDoneAcceptRecovery() will
    // roll forward the rename of the referenced log file.
    //
    // See also: HDFS-3955
    //
    // The fault points here are exercised by the randomized fault injection
    // test case to ensure that this atomic "transaction" operates correctly.
    JournalFaultInjector.get().beforePersistPaxosData();
    persistPaxosData(segmentTxId, newData);
    JournalFaultInjector.get().afterPersistPaxosData();

    if (syncedFile != null) {
      FileUtil.replaceFile(syncedFile,
          storage.getInProgressEditLog(segmentTxId));
    }

    LOG.info("Accepted recovery for segment " + segmentTxId + ": " +
        TextFormat.shortDebugString(newData));
  }

  private Range<Long> txnRange(SegmentStateProto seg) {
    Preconditions.checkArgument(seg.hasEndTxId(),
        "invalid segment: %s", seg);
    return Ranges.closed(seg.getStartTxId(), seg.getEndTxId());
  }

  /**
   * Synchronize a log segment from another JournalNode. The log is
   * downloaded from the provided URL into a temporary location on disk,
   * which is named based on the current request's epoch.
   *
   * @return the temporary location of the downloaded file
   */
  private File syncLog(RequestInfo reqInfo,
      final SegmentStateProto segment, final URL url) throws IOException {
    final File tmpFile = storage.getSyncLogTemporaryFile(
        segment.getStartTxId(), reqInfo.getEpoch());
    final List<File> localPaths = ImmutableList.of(tmpFile);

    LOG.info("Synchronizing log " +
        TextFormat.shortDebugString(segment) + " from " + url);
    SecurityUtil.doAsLoginUser(
        new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws IOException {
            // We may have lost our ticket since last checkpoint, log in again, just in case
            if (UserGroupInformation.isSecurityEnabled()) {
              UserGroupInformation.getCurrentUser().checkTGTAndReloginFromKeytab();
            }

            boolean success = false;
            try {
              TransferFsImage.doGetUrl(url, localPaths, storage, true);
              assert tmpFile.exists();
              success = true;
            } finally {
              if (!success) {
                if (!tmpFile.delete()) {
                  LOG.warn("Failed to delete temporary file " + tmpFile);
                }
              }
            }
            return null;
          }
        });
    return tmpFile;
  }
  

  /**
   * In the case the node crashes in between downloading a log segment
   * and persisting the associated paxos recovery data, the log segment
   * will be left in its temporary location on disk. Given the paxos data,
   * we can check if this was indeed the case, and &quot;roll forward&quot;
   * the atomic operation.
   * 
   * See the inline comments in
   * {@link #acceptRecovery(RequestInfo, SegmentStateProto, URL)} for more
   * details.
   *
   * @throws IOException if the temporary file is unable to be renamed into
   * place
   */
  private void completeHalfDoneAcceptRecovery(
      PersistedRecoveryPaxosData paxosData) throws IOException {
    if (paxosData == null) {
      return;
    }

    long segmentId = paxosData.getSegmentState().getStartTxId();
    long epoch = paxosData.getAcceptedInEpoch();
    
    File tmp = storage.getSyncLogTemporaryFile(segmentId, epoch);
    
    if (tmp.exists()) {
      File dst = storage.getInProgressEditLog(segmentId);
      LOG.info("Rolling forward previously half-completed synchronization: " +
          tmp + " -> " + dst);
      FileUtil.replaceFile(tmp, dst);
    }
  }

  /**
   * Retrieve the persisted data for recovering the given segment from disk.
   */
  private PersistedRecoveryPaxosData getPersistedPaxosData(long segmentTxId)
      throws IOException {
    File f = storage.getPaxosFile(segmentTxId);
    if (!f.exists()) {
      // Default instance has no fields filled in (they're optional)
      return null;
    }
    
    InputStream in = new FileInputStream(f);
    try {
      PersistedRecoveryPaxosData ret = PersistedRecoveryPaxosData.parseDelimitedFrom(in);
      Preconditions.checkState(ret != null &&
          ret.getSegmentState().getStartTxId() == segmentTxId,
          "Bad persisted data for segment %s: %s",
          segmentTxId, ret);
      return ret;
    } finally {
      IOUtils.closeStream(in);
    }
  }

  /**
   * Persist data for recovering the given segment from disk.
   */
  private void persistPaxosData(long segmentTxId,
      PersistedRecoveryPaxosData newData) throws IOException {
    File f = storage.getPaxosFile(segmentTxId);
    boolean success = false;
    AtomicFileOutputStream fos = new AtomicFileOutputStream(f);
    try {
      newData.writeDelimitedTo(fos);
      fos.write('\n');
      // Write human-readable data after the protobuf. This is only
      // to assist in debugging -- it's not parsed at all.
      OutputStreamWriter writer = new OutputStreamWriter(fos, Charsets.UTF_8);
      
      writer.write(String.valueOf(newData));
      writer.write('\n');
      writer.flush();
      
      fos.flush();
      success = true;
    } finally {
      if (success) {
        IOUtils.closeStream(fos);
      } else {
        fos.abort();
      }
    }
  }

  synchronized void discardSegments(long startTxId) throws IOException {
    storage.getJournalManager().discardSegments(startTxId);
    // we delete all the segments after the startTxId. let's reset committedTxnId 
    committedTxnId.set(startTxId - 1);
  }

  public synchronized void doPreUpgrade() throws IOException {
    // Do not hold file lock on committedTxnId, because the containing
    // directory will be renamed.  It will be reopened lazily on next access.
    IOUtils.cleanup(LOG, committedTxnId);
    storage.getJournalManager().doPreUpgrade();
  }

  public synchronized void doUpgrade(StorageInfo sInfo) throws IOException {
    long oldCTime = storage.getCTime();
    storage.cTime = sInfo.cTime;
    int oldLV = storage.getLayoutVersion();
    storage.layoutVersion = sInfo.layoutVersion;
    LOG.info("Starting upgrade of edits directory: "
        + ".\n   old LV = " + oldLV
        + "; old CTime = " + oldCTime
        + ".\n   new LV = " + storage.getLayoutVersion()
        + "; new CTime = " + storage.getCTime());
    storage.getJournalManager().doUpgrade(storage);
    storage.createPaxosDir();
    
    // Copy over the contents of the epoch data files to the new dir.
    File currentDir = storage.getSingularStorageDir().getCurrentDir();
    File previousDir = storage.getSingularStorageDir().getPreviousDir();
    
    PersistentLongFile prevLastPromisedEpoch = new PersistentLongFile(
        new File(previousDir, LAST_PROMISED_FILENAME), 0);
    PersistentLongFile prevLastWriterEpoch = new PersistentLongFile(
        new File(previousDir, LAST_WRITER_EPOCH), 0);
    BestEffortLongFile prevCommittedTxnId = new BestEffortLongFile(
        new File(previousDir, COMMITTED_TXID_FILENAME),
        HdfsConstants.INVALID_TXID);

    lastPromisedEpoch = new PersistentLongFile(
        new File(currentDir, LAST_PROMISED_FILENAME), 0);
    lastWriterEpoch = new PersistentLongFile(
        new File(currentDir, LAST_WRITER_EPOCH), 0);
    committedTxnId = new BestEffortLongFile(
        new File(currentDir, COMMITTED_TXID_FILENAME),
        HdfsConstants.INVALID_TXID);

    try {
      lastPromisedEpoch.set(prevLastPromisedEpoch.get());
      lastWriterEpoch.set(prevLastWriterEpoch.get());
      committedTxnId.set(prevCommittedTxnId.get());
    } finally {
      IOUtils.cleanup(LOG, prevCommittedTxnId);
    }
  }

  public synchronized void doFinalize() throws IOException {
    LOG.info("Finalizing upgrade for journal " 
          + storage.getRoot() + "."
          + (storage.getLayoutVersion()==0 ? "" :
            "\n   cur LV = " + storage.getLayoutVersion()
            + "; cur CTime = " + storage.getCTime()));
    storage.getJournalManager().doFinalize();
  }

  public Boolean canRollBack(StorageInfo storage, StorageInfo prevStorage,
      int targetLayoutVersion) throws IOException {
    return this.storage.getJournalManager().canRollBack(storage, prevStorage,
        targetLayoutVersion);
  }

  public synchronized void doRollback() throws IOException {
    // Do not hold file lock on committedTxnId, because the containing
    // directory will be renamed.  It will be reopened lazily on next access.
    IOUtils.cleanup(LOG, committedTxnId);
    storage.getJournalManager().doRollback();
  }

  public Long getJournalCTime() throws IOException {
    return storage.getJournalManager().getJournalCTime();
  }
}
