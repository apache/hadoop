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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;

import static org.apache.hadoop.util.ExitUtil.terminate;

import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import static org.apache.hadoop.hdfs.server.common.Util.now;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.security.token.delegation.DelegationKey;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;

import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.*;

/**
 * FSEditLog maintains a log of the namespace modifications.
 * 
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FSEditLog  {

  static final String NO_JOURNAL_STREAMS_WARNING = "!!! WARNING !!!" +
      " File system changes are not persistent. No journal streams.";

  static final Log LOG = LogFactory.getLog(FSEditLog.class);

  /**
   * State machine for edit log.
   * The log starts in UNITIALIZED state upon construction. Once it's
   * initialized, it is usually in IN_SEGMENT state, indicating that edits
   * may be written. In the middle of a roll, or while saving the namespace,
   * it briefly enters the BETWEEN_LOG_SEGMENTS state, indicating that the
   * previous segment has been closed, but the new one has not yet been opened.
   */
  private enum State {
    UNINITIALIZED,
    BETWEEN_LOG_SEGMENTS,
    IN_SEGMENT,
    CLOSED;
  }  
  private State state = State.UNINITIALIZED;


  private List<JournalAndStream> journals = Lists.newArrayList();
    
  // a monotonically increasing counter that represents transactionIds.
  private long txid = 0;

  // stores the last synced transactionId.
  private long synctxid = 0;

  // the first txid of the log that's currently open for writing.
  // If this value is N, we are currently writing to edits_inprogress_N
  private long curSegmentTxId = HdfsConstants.INVALID_TXID;

  // the time of printing the statistics to the log file.
  private long lastPrintTime;

  // is a sync currently running?
  private volatile boolean isSyncRunning;

  // is an automatic sync scheduled?
  private volatile boolean isAutoSyncScheduled = false;
  
  // these are statistics counters.
  private long numTransactions;        // number of transactions
  private long numTransactionsBatchedInSync;
  private long totalTimeTransactions;  // total time for all transactions
  private NameNodeMetrics metrics;

  private NNStorage storage;

  private static class TransactionId {
    public long txid;

    TransactionId(long value) {
      this.txid = value;
    }
  }

  // stores the most current transactionId of this thread.
  private static final ThreadLocal<TransactionId> myTransactionId = new ThreadLocal<TransactionId>() {
    protected synchronized TransactionId initialValue() {
      return new TransactionId(Long.MAX_VALUE);
    }
  };

  FSEditLog(NNStorage storage) {
    isSyncRunning = false;
    this.storage = storage;
    metrics = NameNode.getNameNodeMetrics();
    lastPrintTime = now();
    
    for (StorageDirectory sd : storage.dirIterable(NameNodeDirType.EDITS)) {
      journals.add(new JournalAndStream(new FileJournalManager(sd)));
    }
    
    if (journals.isEmpty()) {
      LOG.error("No edits directories configured!");
    }
    
    state = State.BETWEEN_LOG_SEGMENTS;
  }
  
  /**
   * Initialize the output stream for logging, opening the first
   * log segment.
   */
  synchronized void open() throws IOException {
    Preconditions.checkState(state == State.BETWEEN_LOG_SEGMENTS);

    startLogSegment(getLastWrittenTxId() + 1, true);
    assert state == State.IN_SEGMENT : "Bad state: " + state;
  }
  
  synchronized boolean isOpen() {
    return state == State.IN_SEGMENT;
  }

  /**
   * Shutdown the file store.
   */
  synchronized void close() {
    if (state == State.CLOSED) {
      LOG.debug("Closing log when already closed");
      return;
    }
    
    if (state == State.IN_SEGMENT) {
      assert !journals.isEmpty();
      waitForSyncToFinish();
      endCurrentLogSegment(true);
    }

    state = State.CLOSED;
  }

  /**
   * Write an operation to the edit log. Do not sync to persistent
   * store yet.
   */
  void logEdit(final FSEditLogOp op) {
    synchronized (this) {
      assert state != State.CLOSED;
      
      // wait if an automatic sync is scheduled
      waitIfAutoSyncScheduled();
      
      if (journals.isEmpty()) {
        throw new java.lang.IllegalStateException(NO_JOURNAL_STREAMS_WARNING);
      }
      
      long start = beginTransaction();
      op.setTransactionId(txid);

      mapJournalsAndReportErrors(new JournalClosure() {
        @Override 
        public void apply(JournalAndStream jas) throws IOException {
          if (!jas.isActive()) return;
          jas.stream.write(op);
        }
      }, "logging edit");

      endTransaction(start);
      
      // check if it is time to schedule an automatic sync
      if (!shouldForceSync()) {
        return;
      }
      isAutoSyncScheduled = true;
    }
    
    // sync buffered edit log entries to persistent store
    logSync();
  }

  /**
   * Wait if an automatic sync is scheduled
   * @throws InterruptedException
   */
  synchronized void waitIfAutoSyncScheduled() {
    try {
      while (isAutoSyncScheduled) {
        this.wait(1000);
      }
    } catch (InterruptedException e) {
    }
  }
  
  /**
   * Signal that an automatic sync scheduling is done if it is scheduled
   */
  synchronized void doneWithAutoSyncScheduling() {
    if (isAutoSyncScheduled) {
      isAutoSyncScheduled = false;
      notifyAll();
    }
  }
  
  /**
   * Check if should automatically sync buffered edits to 
   * persistent store
   * 
   * @return true if any of the edit stream says that it should sync
   */
  private boolean shouldForceSync() {
    for (JournalAndStream jas : journals) {
      if (!jas.isActive()) continue;

      if (jas.getCurrentStream().shouldForceSync()) {
        return true;
      }
    }
    return false;
  }
  
  private long beginTransaction() {
    assert Thread.holdsLock(this);
    // get a new transactionId
    txid++;

    //
    // record the transactionId when new data was written to the edits log
    //
    TransactionId id = myTransactionId.get();
    id.txid = txid;
    return now();
  }
  
  private void endTransaction(long start) {
    assert Thread.holdsLock(this);
    
    // update statistics
    long end = now();
    numTransactions++;
    totalTimeTransactions += (end-start);
    if (metrics != null) // Metrics is non-null only when used inside name node
      metrics.addTransaction(end-start);
  }

  /**
   * Return the transaction ID of the last transaction written to the log.
   */
  synchronized long getLastWrittenTxId() {
    return txid;
  }
  
  /**
   * @return the first transaction ID in the current log segment
   */
  synchronized long getCurSegmentTxId() {
    Preconditions.checkState(state == State.IN_SEGMENT,
        "Bad state: %s", state);
    return curSegmentTxId;
  }
  
  /**
   * Set the transaction ID to use for the next transaction written.
   */
  synchronized void setNextTxId(long nextTxId) {
    Preconditions.checkArgument(synctxid <= txid &&
       nextTxId >= txid,
       "May not decrease txid." +
      " synctxid=%s txid=%s nextTxId=%s",
      synctxid, txid, nextTxId);
      
    txid = nextTxId - 1;
  }
    
  /**
   * Blocks until all ongoing edits have been synced to disk.
   * This differs from logSync in that it waits for edits that have been
   * written by other threads, not just edits from the calling thread.
   *
   * NOTE: this should be done while holding the FSNamesystem lock, or
   * else more operations can start writing while this is in progress.
   */
  void logSyncAll() throws IOException {
    // Record the most recent transaction ID as our own id
    synchronized (this) {
      TransactionId id = myTransactionId.get();
      id.txid = txid;
    }
    // Then make sure we're synced up to this point
    logSync();
  }
  
  /**
   * Sync all modifications done by this thread.
   *
   * The internal concurrency design of this class is as follows:
   *   - Log items are written synchronized into an in-memory buffer,
   *     and each assigned a transaction ID.
   *   - When a thread (client) would like to sync all of its edits, logSync()
   *     uses a ThreadLocal transaction ID to determine what edit number must
   *     be synced to.
   *   - The isSyncRunning volatile boolean tracks whether a sync is currently
   *     under progress.
   *
   * The data is double-buffered within each edit log implementation so that
   * in-memory writing can occur in parallel with the on-disk writing.
   *
   * Each sync occurs in three steps:
   *   1. synchronized, it swaps the double buffer and sets the isSyncRunning
   *      flag.
   *   2. unsynchronized, it flushes the data to storage
   *   3. synchronized, it resets the flag and notifies anyone waiting on the
   *      sync.
   *
   * The lack of synchronization on step 2 allows other threads to continue
   * to write into the memory buffer while the sync is in progress.
   * Because this step is unsynchronized, actions that need to avoid
   * concurrency with sync() should be synchronized and also call
   * waitForSyncToFinish() before assuming they are running alone.
   */
  public void logSync() {
    long syncStart = 0;

    // Fetch the transactionId of this thread. 
    long mytxid = myTransactionId.get().txid;
    
    List<JournalAndStream> candidateJournals =
      Lists.newArrayListWithCapacity(journals.size());
    List<JournalAndStream> badJournals = Lists.newArrayList();
    
    boolean sync = false;
    try {
      synchronized (this) {
        try {
        printStatistics(false);
  
        // if somebody is already syncing, then wait
        while (mytxid > synctxid && isSyncRunning) {
          try {
            wait(1000);
          } catch (InterruptedException ie) { 
          }
        }
  
        //
        // If this transaction was already flushed, then nothing to do
        //
        if (mytxid <= synctxid) {
          numTransactionsBatchedInSync++;
          if (metrics != null) // Metrics is non-null only when used inside name node
            metrics.incrTransactionsBatchedInSync();
          return;
        }
     
        // now, this thread will do the sync
        syncStart = txid;
        isSyncRunning = true;
        sync = true;
  
        // swap buffers
        assert !journals.isEmpty() : "no editlog streams";
        
        for (JournalAndStream jas : journals) {
          if (!jas.isActive()) continue;
          try {
            jas.getCurrentStream().setReadyToFlush();
            candidateJournals.add(jas);
          } catch (IOException ie) {
            LOG.error("Unable to get ready to flush.", ie);
            badJournals.add(jas);
          }
        }
        } finally {
          // Prevent RuntimeException from blocking other log edit write 
          doneWithAutoSyncScheduling();
        }
      }
  
      // do the sync
      long start = now();
      for (JournalAndStream jas : candidateJournals) {
        if (!jas.isActive()) continue;
        try {
          jas.getCurrentStream().flush();
        } catch (IOException ie) {
          LOG.error("Unable to sync edit log.", ie);
          //
          // remember the streams that encountered an error.
          //
          badJournals.add(jas);
        }
      }
      long elapsed = now() - start;
      disableAndReportErrorOnJournals(badJournals);
  
      if (metrics != null) { // Metrics non-null only when used inside name node
        metrics.addSync(elapsed);
      }
      
    } finally {
      // Prevent RuntimeException from blocking other log edit sync 
      synchronized (this) {
        if (sync) {
          try {
            if (badJournals.size() >= journals.size() ||
                candidateJournals.isEmpty()) {
              final String msg =
                "Could not sync enough journals to persistent storage. "
                + "Unsynced transactions: " + (txid - synctxid);
              LOG.fatal(msg, new Exception());
              terminate(1, msg);
            }
          } finally {
            synctxid = syncStart;
            // NB: do that if finally block because even if #terminate(2) called above, we must unlock the waiting threads:
            isSyncRunning = false; 
          }
        }
        this.notifyAll();
     }
    }
  }

  //
  // print statistics every 1 minute.
  //
  private void printStatistics(boolean force) {
    long now = now();
    if (lastPrintTime + 60000 > now && !force) {
      return;
    }
    if (journals.isEmpty()) {
      return;
    }
    lastPrintTime = now;
    StringBuilder buf = new StringBuilder();
    buf.append("Number of transactions: ");
    buf.append(numTransactions);
    buf.append(" Total time for transactions(ms): ");
    buf.append(totalTimeTransactions);
    buf.append(" Number of transactions batched in Syncs: ");
    buf.append(numTransactionsBatchedInSync);
    buf.append(" Number of syncs: ");
    for (JournalAndStream jas : journals) {
      if (!jas.isActive()) continue;
      buf.append(jas.getCurrentStream().getNumSync());
      break;
    }

    buf.append(" SyncTimes(ms): ");

    for (JournalAndStream jas : journals) {
      if (!jas.isActive()) continue;
      EditLogOutputStream eStream = jas.getCurrentStream();
      buf.append(eStream.getTotalSyncTime());
      buf.append(" ");
    }
    LOG.info(buf);
  }

  /** 
   * Add open lease record to edit log. 
   * Records the block locations of the last block.
   */
  public void logOpenFile(String path, INodeFileUnderConstruction newNode) {
    AddOp op = AddOp.getInstance()
      .setPath(path)
      .setReplication(newNode.getReplication())
      .setModificationTime(newNode.getModificationTime())
      .setAccessTime(newNode.getAccessTime())
      .setBlockSize(newNode.getPreferredBlockSize())
      .setBlocks(newNode.getBlocks())
      .setPermissionStatus(newNode.getPermissionStatus())
      .setClientName(newNode.getClientName())
      .setClientMachine(newNode.getClientMachine());
    
      logEdit(op);
  }

  /** 
   * Add close lease record to edit log.
   */
  public void logCloseFile(String path, INodeFile newNode) {
    CloseOp op = CloseOp.getInstance()
      .setPath(path)
      .setReplication(newNode.getReplication())
      .setModificationTime(newNode.getModificationTime())
      .setAccessTime(newNode.getAccessTime())
      .setBlockSize(newNode.getPreferredBlockSize())
      .setBlocks(newNode.getBlocks())
      .setPermissionStatus(newNode.getPermissionStatus());
    
    logEdit(op);
  }
  
  /** 
   * Add create directory record to edit log
   */
  public void logMkDir(String path, INode newNode) {
    MkdirOp op = MkdirOp.getInstance()
      .setPath(path)
      .setTimestamp(newNode.getModificationTime())
      .setPermissionStatus(newNode.getPermissionStatus());
    logEdit(op);
  }
  
  /** 
   * Add rename record to edit log
   * TODO: use String parameters until just before writing to disk
   */
  void logRename(String src, String dst, long timestamp) {
    RenameOldOp op = RenameOldOp.getInstance()
      .setSource(src)
      .setDestination(dst)
      .setTimestamp(timestamp);
    logEdit(op);
  }
  
  /** 
   * Add rename record to edit log
   */
  void logRename(String src, String dst, long timestamp, Options.Rename... options) {
    RenameOp op = RenameOp.getInstance()
      .setSource(src)
      .setDestination(dst)
      .setTimestamp(timestamp)
      .setOptions(options);
    logEdit(op);
  }
  
  /** 
   * Add set replication record to edit log
   */
  void logSetReplication(String src, short replication) {
    SetReplicationOp op = SetReplicationOp.getInstance()
      .setPath(src)
      .setReplication(replication);
    logEdit(op);
  }
  
  /** Add set namespace quota record to edit log
   * 
   * @param src the string representation of the path to a directory
   * @param quota the directory size limit
   */
  void logSetQuota(String src, long nsQuota, long dsQuota) {
    SetQuotaOp op = SetQuotaOp.getInstance()
      .setSource(src)
      .setNSQuota(nsQuota)
      .setDSQuota(dsQuota);
    logEdit(op);
  }

  /**  Add set permissions record to edit log */
  void logSetPermissions(String src, FsPermission permissions) {
    SetPermissionsOp op = SetPermissionsOp.getInstance()
      .setSource(src)
      .setPermissions(permissions);
    logEdit(op);
  }

  /**  Add set owner record to edit log */
  void logSetOwner(String src, String username, String groupname) {
    SetOwnerOp op = SetOwnerOp.getInstance()
      .setSource(src)
      .setUser(username)
      .setGroup(groupname);
    logEdit(op);
  }
  
  /**
   * concat(trg,src..) log
   */
  void logConcat(String trg, String [] srcs, long timestamp) {
    ConcatDeleteOp op = ConcatDeleteOp.getInstance()
      .setTarget(trg)
      .setSources(srcs)
      .setTimestamp(timestamp);
    logEdit(op);
  }
  
  /** 
   * Add delete file record to edit log
   */
  void logDelete(String src, long timestamp) {
    DeleteOp op = DeleteOp.getInstance()
      .setPath(src)
      .setTimestamp(timestamp);
    logEdit(op);
  }

  /** 
   * Add generation stamp record to edit log
   */
  void logGenerationStamp(long genstamp) {
    SetGenstampOp op = SetGenstampOp.getInstance()
      .setGenerationStamp(genstamp);
    logEdit(op);
  }

  /** 
   * Add access time record to edit log
   */
  void logTimes(String src, long mtime, long atime) {
    TimesOp op = TimesOp.getInstance()
      .setPath(src)
      .setModificationTime(mtime)
      .setAccessTime(atime);
    logEdit(op);
  }

  /** 
   * Add a create symlink record.
   */
  void logSymlink(String path, String value, long mtime, 
                  long atime, INodeSymlink node) {
    SymlinkOp op = SymlinkOp.getInstance()
      .setPath(path)
      .setValue(value)
      .setModificationTime(mtime)
      .setAccessTime(atime)
      .setPermissionStatus(node.getPermissionStatus());
    logEdit(op);
  }
  
  /**
   * log delegation token to edit log
   * @param id DelegationTokenIdentifier
   * @param expiryTime of the token
   * @return
   */
  void logGetDelegationToken(DelegationTokenIdentifier id,
      long expiryTime) {
    GetDelegationTokenOp op = GetDelegationTokenOp.getInstance()
      .setDelegationTokenIdentifier(id)
      .setExpiryTime(expiryTime);
    logEdit(op);
  }
  
  void logRenewDelegationToken(DelegationTokenIdentifier id,
      long expiryTime) {
    RenewDelegationTokenOp op = RenewDelegationTokenOp.getInstance()
      .setDelegationTokenIdentifier(id)
      .setExpiryTime(expiryTime);
    logEdit(op);
  }
  
  void logCancelDelegationToken(DelegationTokenIdentifier id) {
    CancelDelegationTokenOp op = CancelDelegationTokenOp.getInstance()
      .setDelegationTokenIdentifier(id);
    logEdit(op);
  }
  
  void logUpdateMasterKey(DelegationKey key) {
    UpdateMasterKeyOp op = UpdateMasterKeyOp.getInstance()
      .setDelegationKey(key);
    logEdit(op);
  }

  void logReassignLease(String leaseHolder, String src, String newHolder) {
    ReassignLeaseOp op = ReassignLeaseOp.getInstance()
      .setLeaseHolder(leaseHolder)
      .setPath(src)
      .setNewHolder(newHolder);
    logEdit(op);
  }
  
  /**
   * @return the number of active (non-failed) journals
   */
  private int countActiveJournals() {
    int count = 0;
    for (JournalAndStream jas : journals) {
      if (jas.isActive()) {
        count++;
      }
    }
    return count;
  }
  
  /**
   * Used only by unit tests.
   */
  @VisibleForTesting
  List<JournalAndStream> getJournals() {
    return journals;
  }
  
  /**
   * Return a manifest of what finalized edit logs are available
   */
  public synchronized RemoteEditLogManifest getEditLogManifest(
      long fromTxId) throws IOException {
    // Collect RemoteEditLogs available from each FileJournalManager
    List<RemoteEditLog> allLogs = Lists.newArrayList();
    for (JournalAndStream j : journals) {
      if (j.getManager() instanceof FileJournalManager) {
        FileJournalManager fjm = (FileJournalManager)j.getManager();
        try {
          allLogs.addAll(fjm.getRemoteEditLogs(fromTxId));
        } catch (Throwable t) {
          LOG.warn("Cannot list edit logs in " + fjm, t);
        }
      }
    }
    
    // Group logs by their starting txid
    ImmutableListMultimap<Long, RemoteEditLog> logsByStartTxId =
      Multimaps.index(allLogs, RemoteEditLog.GET_START_TXID);
    long curStartTxId = fromTxId;

    List<RemoteEditLog> logs = Lists.newArrayList();
    while (true) {
      ImmutableList<RemoteEditLog> logGroup = logsByStartTxId.get(curStartTxId);
      if (logGroup.isEmpty()) {
        // we have a gap in logs - for example because we recovered some old
        // storage directory with ancient logs. Clear out any logs we've
        // accumulated so far, and then skip to the next segment of logs
        // after the gap.
        SortedSet<Long> startTxIds = Sets.newTreeSet(logsByStartTxId.keySet());
        startTxIds = startTxIds.tailSet(curStartTxId);
        if (startTxIds.isEmpty()) {
          break;
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Found gap in logs at " + curStartTxId + ": " +
                "not returning previous logs in manifest.");
          }
          logs.clear();
          curStartTxId = startTxIds.first();
          continue;
        }
      }

      // Find the one that extends the farthest forward
      RemoteEditLog bestLog = Collections.max(logGroup);
      logs.add(bestLog);
      // And then start looking from after that point
      curStartTxId = bestLog.getEndTxId() + 1;
    }
    RemoteEditLogManifest ret = new RemoteEditLogManifest(logs);
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Generated manifest for logs since " + fromTxId + ":"
          + ret);      
    }
    return ret;
  }
 
  /**
   * Finalizes the current edit log and opens a new log segment.
   * @return the transaction id of the BEGIN_LOG_SEGMENT transaction
   * in the new log.
   */
  synchronized long rollEditLog() throws IOException {
    LOG.info("Rolling edit logs.");
    endCurrentLogSegment(true);
    
    long nextTxId = getLastWrittenTxId() + 1;
    startLogSegment(nextTxId, true);
    
    assert curSegmentTxId == nextTxId;
    return nextTxId;
  }
  
  /**
   * Start writing to the log segment with the given txid.
   * Transitions from BETWEEN_LOG_SEGMENTS state to IN_LOG_SEGMENT state. 
   */
  synchronized void startLogSegment(final long segmentTxId,
      boolean writeHeaderTxn) throws IOException {
    LOG.info("Starting log segment at " + segmentTxId);
    Preconditions.checkArgument(segmentTxId > 0,
        "Bad txid: %s", segmentTxId);
    Preconditions.checkState(state == State.BETWEEN_LOG_SEGMENTS,
        "Bad state: %s", state);
    Preconditions.checkState(segmentTxId > curSegmentTxId,
        "Cannot start writing to log segment " + segmentTxId +
        " when previous log segment started at " + curSegmentTxId);
    Preconditions.checkArgument(segmentTxId == txid + 1,
        "Cannot start log segment at txid %s when next expected " +
        "txid is %s", segmentTxId, txid + 1);
    
    numTransactions = totalTimeTransactions = numTransactionsBatchedInSync = 0;

    // TODO no need to link this back to storage anymore!
    // See HDFS-2174.
    storage.attemptRestoreRemovedStorage();
    
    mapJournalsAndReportErrors(new JournalClosure() {
      @Override
      public void apply(JournalAndStream jas) throws IOException {
        jas.startLogSegment(segmentTxId);
      }
    }, "starting log segment " + segmentTxId);

    if (countActiveJournals() == 0) {
      throw new IOException("Unable to start log segment " +
          segmentTxId + ": no journals successfully started.");
    }
    
    curSegmentTxId = segmentTxId;
    state = State.IN_SEGMENT;

    if (writeHeaderTxn) {
      logEdit(LogSegmentOp.getInstance(
          FSEditLogOpCodes.OP_START_LOG_SEGMENT));
      logSync();
    }
  }

  /**
   * Finalize the current log segment.
   * Transitions from IN_SEGMENT state to BETWEEN_LOG_SEGMENTS state.
   */
  synchronized void endCurrentLogSegment(boolean writeEndTxn) {
    LOG.info("Ending log segment " + curSegmentTxId);
    Preconditions.checkState(state == State.IN_SEGMENT,
        "Bad state: %s", state);
    
    if (writeEndTxn) {
      logEdit(LogSegmentOp.getInstance(
          FSEditLogOpCodes.OP_END_LOG_SEGMENT));
      logSync();
    }

    printStatistics(true);
    
    final long lastTxId = getLastWrittenTxId();
    
    mapJournalsAndReportErrors(new JournalClosure() {
      @Override
      public void apply(JournalAndStream jas) throws IOException {
        if (jas.isActive()) {
          jas.close(lastTxId);
        }
      }
    }, "ending log segment");
    
    state = State.BETWEEN_LOG_SEGMENTS;
  }
  
  /**
   * Abort all current logs. Called from the backup node.
   */
  synchronized void abortCurrentLogSegment() {
    mapJournalsAndReportErrors(new JournalClosure() {
      
      @Override
      public void apply(JournalAndStream jas) throws IOException {
        jas.abort();
      }
    }, "aborting all streams");
    state = State.BETWEEN_LOG_SEGMENTS;
  }

  /**
   * Archive any log files that are older than the given txid.
   */
  public void purgeLogsOlderThan(final long minTxIdToKeep) {
    synchronized (this) {
      // synchronized to prevent findbugs warning about inconsistent
      // synchronization. This will be JIT-ed out if asserts are
      // off.
      assert curSegmentTxId == HdfsConstants.INVALID_TXID || // on format this is no-op
        minTxIdToKeep <= curSegmentTxId :
        "cannot purge logs older than txid " + minTxIdToKeep +
        " when current segment starts at " + curSegmentTxId;
    }
    
    mapJournalsAndReportErrors(new JournalClosure() {
      @Override
      public void apply(JournalAndStream jas) throws IOException {
        jas.manager.purgeLogsOlderThan(minTxIdToKeep);
      }
    }, "purging logs older than " + minTxIdToKeep);
  }

  
  /**
   * The actual sync activity happens while not synchronized on this object.
   * Thus, synchronized activities that require that they are not concurrent
   * with file operations should wait for any running sync to finish.
   */
  synchronized void waitForSyncToFinish() {
    while (isSyncRunning) {
      try {
        wait(1000);
      } catch (InterruptedException ie) {}
    }
  }

  /**
   * Return the txid of the last synced transaction.
   * For test use only
   */
  synchronized long getSyncTxId() {
    return synctxid;
  }


  // sets the initial capacity of the flush buffer.
  public void setOutputBufferCapacity(int size) {
    for (JournalAndStream jas : journals) {
      jas.manager.setOutputBufferCapacity(size);
    }
  }

  /**
   * Create (or find if already exists) an edit output stream, which
   * streams journal records (edits) to the specified backup node.<br>
   * 
   * The new BackupNode will start receiving edits the next time this
   * NameNode's logs roll.
   * 
   * @param bnReg the backup node registration information.
   * @param nnReg this (active) name-node registration.
   * @throws IOException
   */
  synchronized void registerBackupNode(
      NamenodeRegistration bnReg, // backup node
      NamenodeRegistration nnReg) // active name-node
  throws IOException {
    if(bnReg.isRole(NamenodeRole.CHECKPOINT))
      return; // checkpoint node does not stream edits
    
    JournalAndStream jas = findBackupJournalAndStream(bnReg);
    if (jas != null) {
      // already registered
      LOG.info("Backup node " + bnReg + " re-registers");
      return;
    }
    
    LOG.info("Registering new backup node: " + bnReg);
    BackupJournalManager bjm = new BackupJournalManager(bnReg, nnReg);
    journals.add(new JournalAndStream(bjm));
  }
  
  synchronized void releaseBackupStream(NamenodeRegistration registration) {
    for (Iterator<JournalAndStream> iter = journals.iterator();
         iter.hasNext();) {
      JournalAndStream jas = iter.next();
      if (jas.manager instanceof BackupJournalManager &&
          ((BackupJournalManager)jas.manager).matchesRegistration(
              registration)) {
        jas.abort();        
        LOG.info("Removing backup journal " + jas);
        iter.remove();
      }
    }
  }
  
  /**
   * Find the JournalAndStream associated with this BackupNode.
   * @return null if it cannot be found
   */
  private synchronized JournalAndStream findBackupJournalAndStream(
      NamenodeRegistration bnReg) {
    for (JournalAndStream jas : journals) {
      if (jas.manager instanceof BackupJournalManager) {
        BackupJournalManager bjm = (BackupJournalManager)jas.manager;
        if (bjm.matchesRegistration(bnReg)) {
          return jas;
        }
      }
    }
    return null;
  }

  /**
   * Write an operation to the edit log. Do not sync to persistent
   * store yet.
   */   
  synchronized void logEdit(final int length, final byte[] data) {
    long start = beginTransaction();
    
    mapJournalsAndReportErrors(new JournalClosure() {
      @Override
      public void apply(JournalAndStream jas) throws IOException {
        if (jas.isActive()) {
          jas.getCurrentStream().writeRaw(data, 0, length); // TODO writeRaw
        }
      }      
    }, "Logging edit");

    endTransaction(start);
  }

  //// Iteration across journals
  private interface JournalClosure {
    public void apply(JournalAndStream jas) throws IOException;
  }

  /**
   * Apply the given function across all of the journal managers, disabling
   * any for which the closure throws an IOException.
   * @param status message used for logging errors (e.g. "opening journal")
   */
  private void mapJournalsAndReportErrors(
      JournalClosure closure, String status) {
    List<JournalAndStream> badJAS = Lists.newLinkedList();
    for (JournalAndStream jas : journals) {
      try {
        closure.apply(jas);
      } catch (Throwable t) {
        LOG.error("Error " + status + " (journal " + jas + ")", t);
        badJAS.add(jas);
      }
    }

    disableAndReportErrorOnJournals(badJAS);
  }
  
  /**
   * Called when some journals experience an error in some operation.
   * This propagates errors to the storage level.
   */
  private void disableAndReportErrorOnJournals(List<JournalAndStream> badJournals) {
    if (badJournals == null || badJournals.isEmpty()) {
      return; // nothing to do
    }
 
    for (JournalAndStream j : badJournals) {
      LOG.error("Disabling journal " + j);
      j.abort();
    }
  }

  /**
   * Container for a JournalManager paired with its currently
   * active stream.
   * 
   * If a Journal gets disabled due to an error writing to its
   * stream, then the stream will be aborted and set to null.
   */
  static class JournalAndStream {
    private final JournalManager manager;
    private EditLogOutputStream stream;
    private long segmentStartsAtTxId = HdfsConstants.INVALID_TXID;
    
    private JournalAndStream(JournalManager manager) {
      this.manager = manager;
    }

    private void startLogSegment(long txId) throws IOException {
      Preconditions.checkState(stream == null);
      stream = manager.startLogSegment(txId);
      segmentStartsAtTxId = txId;
    }

    private void close(long lastTxId) throws IOException {
      Preconditions.checkArgument(lastTxId >= segmentStartsAtTxId,
          "invalid segment: lastTxId %s >= " +
          "segment starting txid %s", lastTxId, segmentStartsAtTxId);
          
      if (stream == null) return;
      stream.close();
      manager.finalizeLogSegment(segmentStartsAtTxId, lastTxId);
      stream = null;
    }
    
    @VisibleForTesting
    void abort() {
      if (stream == null) return;
      try {
        stream.abort();
      } catch (IOException ioe) {
        LOG.error("Unable to abort stream " + stream, ioe);
      }
      stream = null;
      segmentStartsAtTxId = HdfsConstants.INVALID_TXID;
    }

    private boolean isActive() {
      return stream != null;
    }

    @VisibleForTesting
    EditLogOutputStream getCurrentStream() {
      return stream;
    }
    
    @Override
    public String toString() {
      return "JournalAndStream(mgr=" + manager +
        ", " + "stream=" + stream + ")";
    }

    @VisibleForTesting
    void setCurrentStreamForTests(EditLogOutputStream stream) {
      this.stream = stream;
    }
    
    @VisibleForTesting
    JournalManager getManager() {
      return manager;
    }

    private EditLogInputStream getInProgressInputStream() throws IOException {
      return manager.getInProgressInputStream(segmentStartsAtTxId);
    }
  }

  /**
   * @return an EditLogInputStream that reads from the same log that
   * the edit log is currently writing. This is used from the BackupNode
   * during edits synchronization.
   * @throws IOException if no valid logs are available.
   */
  synchronized EditLogInputStream getInProgressFileInputStream()
      throws IOException {
    for (JournalAndStream jas : journals) {
      if (!jas.isActive()) continue;
      try {
        EditLogInputStream in = jas.getInProgressInputStream();
        if (in != null) return in;
      } catch (IOException ioe) {
        LOG.warn("Unable to get the in-progress input stream from " + jas,
            ioe);
      }
    }
    throw new IOException("No in-progress stream provided edits");
  }
}
