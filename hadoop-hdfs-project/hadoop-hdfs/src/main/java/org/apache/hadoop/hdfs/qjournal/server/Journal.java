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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.qjournal.protocol.JournalNotFormattedException;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PersistedRecoveryPaxosData;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.SegmentStateProto;
import org.apache.hadoop.hdfs.qjournal.protocol.RequestInfo;
import org.apache.hadoop.hdfs.server.common.StorageErrorReporter;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager.EditLogFile;
import org.apache.hadoop.hdfs.server.namenode.JournalManager;
import org.apache.hadoop.hdfs.server.namenode.TransferFsImage;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.util.AtomicFileOutputStream;
import org.apache.hadoop.hdfs.util.PersistentLongFile;
import org.apache.hadoop.io.IOUtils;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;

/**
 * A JournalNode can manage journals for several clusters at once.
 * Each such journal is entirely independent despite being hosted by
 * the same JVM.
 */
class Journal implements Closeable {
  static final Log LOG = LogFactory.getLog(Journal.class);


  // Current writing state
  private EditLogOutputStream curSegment;
  private long curSegmentTxId = HdfsConstants.INVALID_TXID;
  private long nextTxId = HdfsConstants.INVALID_TXID;
  
  private final JNStorage storage;

  /**
   * When a new writer comes along, it asks each node to promise
   * to ignore requests from any previous writer, as identified
   * by epoch number. In order to make such a promise, the epoch
   * number of that writer is stored persistently on disk.
   */
  private PersistentLongFile lastPromisedEpoch;
  private static final String LAST_PROMISED_FILENAME = "last-promised-epoch";

  private final FileJournalManager fjm;

  Journal(File logDir, StorageErrorReporter errorReporter) throws IOException {
    storage = new JNStorage(logDir, errorReporter);

    File currentDir = storage.getSingularStorageDir().getCurrentDir();
    this.lastPromisedEpoch = new PersistentLongFile(
        new File(currentDir, LAST_PROMISED_FILENAME), 0);

    this.fjm = storage.getJournalManager();
  }
  
  /**
   * Iterate over the edit logs stored locally, and set
   * {@link #curSegmentTxId} to refer to the most recently written
   * one.
   */
  private synchronized void scanStorage() throws IOException {
    if (!fjm.getStorageDirectory().getCurrentDir().exists()) {
      return;
    }
    LOG.info("Scanning storage " + fjm);
    List<EditLogFile> files = fjm.getLogFiles(0);
    if (!files.isEmpty()) {
      EditLogFile latestLog = files.get(files.size() - 1);
      LOG.info("Latest log is " + latestLog);
      curSegmentTxId = latestLog.getFirstTxId();
    }
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
  }

  /**
   * Unlock and release resources.
   */
  @Override // Closeable
  public void close() throws IOException {
    storage.close();
  }
  
  JNStorage getStorage() {
    return storage;
  }

  /**
   * @return the last epoch which this node has promised not to accept
   * any lower epoch, or 0 if no promises have been made.
   */
  synchronized long getLastPromisedEpoch() throws IOException {
    checkFormatted();
    return lastPromisedEpoch.get();
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
    
    if (epoch <= getLastPromisedEpoch()) {
      throw new IOException("Proposed epoch " + epoch + " <= last promise " +
          getLastPromisedEpoch());
    }
    
    lastPromisedEpoch.set(epoch);
    if (curSegment != null) {
      curSegment.close();
      curSegment = null;
    }
    
    NewEpochResponseProto.Builder builder =
        NewEpochResponseProto.newBuilder();

    // TODO: we only need to do this once, not on writer switchover.
    scanStorage();

    if (curSegmentTxId != HdfsConstants.INVALID_TXID) {
      builder.setLastSegmentTxId(curSegmentTxId);
    }
    
    return builder.build();
  }

  /**
   * Write a batch of edits to the journal.
   * {@see QJournalProtocol#journal(RequestInfo, long, int, byte[])}
   */
  synchronized void journal(RequestInfo reqInfo, long firstTxnId,
      int numTxns, byte[] records) throws IOException {
    checkRequest(reqInfo);
    checkFormatted();
    
    // TODO: if a JN goes down and comes back up, then it will throw
    // this exception on every edit. We should instead send back
    // a response indicating the log needs to be rolled, which would
    // mark the logger on the client side as "pending" -- and have the
    // NN code look for this condition and trigger a roll when it happens.
    // That way the node can catch back up and rejoin
    Preconditions.checkState(curSegment != null,
        "Can't write, no segment open");
    Preconditions.checkState(nextTxId == firstTxnId,
        "Can't write txid " + firstTxnId + " expecting nextTxId=" + nextTxId);
    
    if (LOG.isTraceEnabled()) {
      LOG.trace("Writing txid " + firstTxnId + "-" + (firstTxnId + numTxns - 1));
    }
    curSegment.writeRaw(records, 0, records.length);
    curSegment.setReadyToFlush();
    curSegment.flush();
    nextTxId += numTxns;
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
    }
    
    // TODO: should other requests check the _exact_ epoch instead of
    // the <= check? <= should probably only be necessary for the
    // first calls
    
    // TODO: some check on serial number that they only increase from a given
    // client
  }
  
  private void checkFormatted() throws JournalNotFormattedException {
    if (!storage.isFormatted()) {
      throw new JournalNotFormattedException("Journal " + storage +
          " not formatted");
    }
  }

  /**
   * Start a new segment at the given txid. The previous segment
   * must have already been finalized.
   */
  public synchronized void startLogSegment(RequestInfo reqInfo, long txid)
      throws IOException {
    assert fjm != null;
    checkRequest(reqInfo);
    checkFormatted();
    
    Preconditions.checkState(curSegment == null,
        "Can't start a log segment, already writing " + curSegment);
    Preconditions.checkState(nextTxId == txid || nextTxId == HdfsConstants.INVALID_TXID,
        "Can't start log segment " + txid + " expecting nextTxId=" + nextTxId);
    curSegment = fjm.startLogSegment(txid);
    curSegmentTxId = txid;
    nextTxId = txid;
  }
  
  /**
   * Finalize the log segment at the given transaction ID.
   */
  public synchronized void finalizeLogSegment(RequestInfo reqInfo, long startTxId,
      long endTxId) throws IOException {
    checkRequest(reqInfo);
    checkFormatted();

    if (startTxId == curSegmentTxId) {
      if (curSegment != null) {
        curSegment.close();
        curSegment = null;
      }
    }
    
    FileJournalManager.EditLogFile elf = fjm.getLogFile(startTxId);
    if (elf.isInProgress()) {
      // TODO: this is slow to validate when in non-recovery cases
      // we already know the length here!

      LOG.info("Validating log about to be finalized: " + elf);
      elf.validateLog();
      
      Preconditions.checkState(elf.getLastTxId() == endTxId,
          "Trying to finalize log %s-%s, but current state of log" +
          "is %s", startTxId, endTxId, elf);
      fjm.finalizeLogSegment(startTxId, endTxId);
    } else {
      Preconditions.checkArgument(endTxId == elf.getLastTxId(),
          "Trying to re-finalize already finalized log " +
              elf + " with different endTxId " + endTxId);
    }
  }
  
  /**
   * @see JournalManager#purgeLogsOlderThan(long)
   */
  public synchronized void purgeLogsOlderThan(RequestInfo reqInfo,
      long minTxIdToKeep) throws IOException {
    checkRequest(reqInfo);
    checkFormatted();
    
    fjm.purgeLogsOlderThan(minTxIdToKeep);
    purgePaxosDecisionsOlderThan(minTxIdToKeep);
  }
  
  private void purgePaxosDecisionsOlderThan(long minTxIdToKeep)
      throws IOException {
    File dir = storage.getPaxosDir();
    for (File f : FileUtil.listFiles(dir)) {
      if (!f.isFile()) continue;
      
      long txid;
      try {
        txid = Long.valueOf(f.getName());
      } catch (NumberFormatException nfe) {
        LOG.warn("Unexpected non-numeric file name for " + f.getAbsolutePath());
        continue;
      }
      
      if (txid < minTxIdToKeep) {
        if (!f.delete()) {
          LOG.warn("Unable to delete no-longer-needed paxos decision record " +
              f);
        }
      }
    }
  }


  /**
   * @see QJournalProtocol#getEditLogManifest(String, long)
   */
  public RemoteEditLogManifest getEditLogManifest(long sinceTxId)
      throws IOException {
    // No need to checkRequest() here - anyone may ask for the list
    // of segments.
    checkFormatted();
    
    RemoteEditLogManifest manifest = new RemoteEditLogManifest(
        fjm.getRemoteEditLogs(sinceTxId));
    return manifest;
  }

  /**
   * @return the current state of the given segment, or null if the
   * segment does not exist.
   */
  private SegmentStateProto getSegmentInfo(long segmentTxId)
      throws IOException {
    EditLogFile elf = fjm.getLogFile(segmentTxId);
    if (elf == null) {
      return null;
    }
    if (elf.isInProgress()) {
      elf.validateLog();
    }
    if (elf.getLastTxId() == HdfsConstants.INVALID_TXID) {
      // no transactions in file
      throw new AssertionError("TODO: no transactions in file " +
          elf);
    }
    SegmentStateProto ret = SegmentStateProto.newBuilder()
        .setStartTxId(segmentTxId)
        .setEndTxId(elf.getLastTxId())
        .setIsInProgress(elf.isInProgress())
        .setMd5Sum(ByteString.EMPTY) // TODO
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
    checkRequest(reqInfo);
    checkFormatted();
    
    PrepareRecoveryResponseProto.Builder builder =
        PrepareRecoveryResponseProto.newBuilder();
    
    PersistedRecoveryPaxosData previouslyAccepted = getPersistedPaxosData(segmentTxId);
    if (previouslyAccepted != null) {
      builder.setAcceptedInEpoch(previouslyAccepted.getAcceptedInEpoch())
        .setSegmentState(previouslyAccepted.getSegmentState());
    } else {
      SegmentStateProto segInfo = getSegmentInfo(segmentTxId);
      if (segInfo != null) {
        builder.setSegmentState(segInfo);
      }
    }
    
    PrepareRecoveryResponseProto resp = builder.build();
    LOG.info("Prepared recovery for segment " + segmentTxId + ": " +
        TextFormat.shortDebugString(resp));
    return resp;
  }

  /**
   * @see QJournalProtocol#acceptRecovery(RequestInfo, SegmentStateProto, URL)
   */
  public synchronized void acceptRecovery(RequestInfo reqInfo,
      SegmentStateProto segment, URL fromUrl)
      throws IOException {
    checkRequest(reqInfo);
    checkFormatted();
    long segmentTxId = segment.getStartTxId();

    // TODO: right now, a recovery of a segment when the log is
    // completely emtpy (ie startLogSegment() but no txns)
    // will fail this assertion here, since endTxId < startTxId
    Preconditions.checkArgument(segment.getEndTxId() > 0 &&
        segment.getEndTxId() >= segmentTxId,
        "bad recovery state for segment %s: %s",
        segmentTxId, TextFormat.shortDebugString(segment));
    
    PersistedRecoveryPaxosData oldData = getPersistedPaxosData(segmentTxId);
    PersistedRecoveryPaxosData newData = PersistedRecoveryPaxosData.newBuilder()
        .setAcceptedInEpoch(reqInfo.getEpoch())
        .setSegmentState(segment)
        .build();
    if (oldData != null) {
      Preconditions.checkState(oldData.getAcceptedInEpoch() <= reqInfo.getEpoch(),
          "Bad paxos transition, out-of-order epochs.\nOld: %s\nNew: %s\n",
          oldData, newData);
    }

    SegmentStateProto currentSegment = getSegmentInfo(segmentTxId);
    // TODO: this can be null, in the case that one of the loggers started
    // the next segment, but others did not! add regression test and null
    // check in next condition below.
    
    // TODO: what if they have the same length but one is finalized and the
    // other isn't! cover that case.
    if (currentSegment.getEndTxId() != segment.getEndTxId()) {
      syncLog(reqInfo, segment, fromUrl);
    } else {
      LOG.info("Skipping download of log " +
          TextFormat.shortDebugString(segment) +
          ": already have up-to-date logs");
    }
    
    // TODO: is it OK that this is non-atomic?
    // we might be left with an older epoch recorded, but a newer log
    
    persistPaxosData(segmentTxId, newData);
    LOG.info("Accepted recovery for segment " + segmentTxId + ": " +
        TextFormat.shortDebugString(newData));
  }

  /**
   * Synchronize a log segment from another JournalNode.
   * @param reqInfo the request info for the recovery IPC
   * @param segment 
   * @param url
   * @throws IOException
   */
  private void syncLog(RequestInfo reqInfo,
      SegmentStateProto segment, URL url) throws IOException {
    String tmpFileName =
        "synclog_" + segment.getStartTxId() + "_" +
        reqInfo.getEpoch() + "." + reqInfo.getIpcSerialNumber();
    
    List<File> localPaths = storage.getFiles(null, tmpFileName);
    assert localPaths.size() == 1;
    File tmpFile = localPaths.get(0);
 
    boolean success = false;

    LOG.info("Synchronizing log " +
        TextFormat.shortDebugString(segment) + " from " + url);
    TransferFsImage.doGetUrl(url, localPaths, storage, true);
    assert tmpFile.exists();
    try {
      success = tmpFile.renameTo(storage.getInProgressEditLog(
          segment.getStartTxId()));
      if (success) {
        // If we're synchronizing the latest segment, update our cached
        // info.
        // TODO: can this be done more generally?
        if (curSegmentTxId == segment.getStartTxId()) {
          nextTxId = segment.getEndTxId() + 1;
        }
      }
    } finally {
      if (!success) {
        if (!tmpFile.delete()) {
          LOG.warn("Failed to delete temporary file " + tmpFile);
        }
      }
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
      OutputStreamWriter writer = new OutputStreamWriter(fos);
      
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
}
