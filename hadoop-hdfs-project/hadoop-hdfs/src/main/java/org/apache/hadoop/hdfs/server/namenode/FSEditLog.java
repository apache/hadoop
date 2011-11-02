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

import static org.apache.hadoop.hdfs.server.common.Util.now;
import java.net.URI;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.*;
import org.apache.hadoop.hdfs.server.namenode.JournalSet.JournalAndStream;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.conf.Configuration;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * FSEditLog maintains a log of the namespace modifications.
 * 
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FSEditLog  {

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
  
  //initialize
  final private JournalSet journalSet;
  private EditLogOutputStream editLogStream = null;

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
  
  // Used to exit in the event of a failure to sync to all journals. It's a
  // member variable so it can be swapped out for testing.
  private Runtime runtime = Runtime.getRuntime();

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

  final private Collection<URI> editsDirs;

  /**
   * Construct FSEditLog with default configuration, taking editDirs from NNStorage
   * @param storage Storage object used by namenode
   */
  @VisibleForTesting
  FSEditLog(NNStorage storage) {
    this(new Configuration(), storage, Collections.<URI>emptyList());
  }

  /**
   * Constructor for FSEditLog. Add underlying journals are constructed, but 
   * no streams are opened until open() is called.
   * 
   * @param conf The namenode configuration
   * @param storage Storage object used by namenode
   * @param editsDirs List of journals to use
   */
  FSEditLog(Configuration conf, NNStorage storage, Collection<URI> editsDirs) {
    isSyncRunning = false;
    this.storage = storage;
    metrics = NameNode.getNameNodeMetrics();
    lastPrintTime = now();
    
    if (editsDirs.isEmpty()) { 
      // if this is the case, no edit dirs have been explictly configured
      // image dirs are to be used for edits too
      try {
        editsDirs = Lists.newArrayList(storage.getEditsDirectories());
      } catch (IOException ioe) {
        // cannot get list from storage, so the empty editsDirs 
        // will be assigned. an error will be thrown on first use
        // of the editlog, as no journals will exist
      }
      this.editsDirs = editsDirs;
    } else {
      this.editsDirs = Lists.newArrayList(editsDirs);
    }

    this.journalSet = new JournalSet();
    for (URI u : this.editsDirs) {
      StorageDirectory sd = storage.getStorageDirectory(u);
      if (sd != null) {
        journalSet.add(new FileJournalManager(sd));
      }
    }
 
    if (journalSet.isEmpty()) {
      LOG.error("No edits directories configured!");
    } 
    state = State.BETWEEN_LOG_SEGMENTS;
  }

  /**
   * Get the list of URIs the editlog is using for storage
   * @return collection of URIs in use by the edit log
   */
  Collection<URI> getEditURIs() {
    return editsDirs;
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
      LOG.warn("Closing log when already closed", new Exception());
      return;
    }
    if (state == State.IN_SEGMENT) {
      assert editLogStream != null;
      waitForSyncToFinish();
      endCurrentLogSegment(true);
    }
    
    try {
      journalSet.close();
    } catch (IOException ioe) {
      LOG.warn("Error closing journalSet", ioe);
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
      
      long start = beginTransaction();
      op.setTransactionId(txid);

      try {
        editLogStream.write(op);
      } catch (IOException ex) {
        // All journals failed, it is handled in logSync.
      }

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
    return editLogStream.shouldForceSync();
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
  void logSyncAll() {
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
    
    boolean sync = false;
    try {
      EditLogOutputStream logStream = null;
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
            if (metrics != null) {
              // Metrics is non-null only when used inside name node
              metrics.incrTransactionsBatchedInSync();
            }
            return;
          }
     
          // now, this thread will do the sync
          syncStart = txid;
          isSyncRunning = true;
          sync = true;
  
          // swap buffers
          try {
            if (journalSet.isEmpty()) {
              throw new IOException("No journals available to flush");
            }
            editLogStream.setReadyToFlush();
          } catch (IOException e) {
            LOG.fatal("Could not sync any journal to persistent storage. "
                + "Unsynced transactions: " + (txid - synctxid),
                new Exception());
            runtime.exit(1);
          }
        } finally {
          // Prevent RuntimeException from blocking other log edit write 
          doneWithAutoSyncScheduling();
        }
        //editLogStream may become null,
        //so store a local variable for flush.
        logStream = editLogStream;
      }
      
      // do the sync
      long start = now();
      try {
        if (logStream != null) {
          logStream.flush();
        }
      } catch (IOException ex) {
        synchronized (this) {
          LOG.fatal("Could not sync any journal to persistent storage. "
              + "Unsynced transactions: " + (txid - synctxid), new Exception());
          runtime.exit(1);
        }
      }
      long elapsed = now() - start;
  
      if (metrics != null) { // Metrics non-null only when used inside name node
        metrics.addSync(elapsed);
      }
      
    } finally {
      // Prevent RuntimeException from blocking other log edit sync 
      synchronized (this) {
        if (sync) {
          synctxid = syncStart;
          isSyncRunning = false;
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
    lastPrintTime = now;
    StringBuilder buf = new StringBuilder();
    buf.append("Number of transactions: ");
    buf.append(numTransactions);
    buf.append(" Total time for transactions(ms): ");
    buf.append(totalTimeTransactions);
    buf.append("Number of transactions batched in Syncs: ");
    buf.append(numTransactionsBatchedInSync);
    buf.append(" Number of syncs: ");
    buf.append(editLogStream.getNumSync());
    buf.append(" SyncTimes(ms): ");
    buf.append(journalSet.getSyncTimes());
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
   * Used only by unit tests.
   */
  @VisibleForTesting
  List<JournalAndStream> getJournals() {
    return journalSet.getAllJournalStreams();
  }
  
  /**
   * Used only by unit tests.
   */
  @VisibleForTesting
  synchronized void setRuntimeForTesting(Runtime runtime) {
    this.runtime = runtime;
  }
  
  /**
   * Return a manifest of what finalized edit logs are available
   */
  public synchronized RemoteEditLogManifest getEditLogManifest(long fromTxId)
      throws IOException {
    return journalSet.getEditLogManifest(fromTxId);
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
    
    try {
      editLogStream = journalSet.startLogSegment(segmentTxId);
    } catch (IOException ex) {
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
    
    try {
      journalSet.finalizeLogSegment(curSegmentTxId, lastTxId);
      editLogStream = null;
    } catch (IOException e) {
      //All journals have failed, it will be handled in logSync.
    }
    
    state = State.BETWEEN_LOG_SEGMENTS;
  }
  
  /**
   * Abort all current logs. Called from the backup node.
   */
  synchronized void abortCurrentLogSegment() {
    try {
      //Check for null, as abort can be called any time.
      if (editLogStream != null) {
        editLogStream.abort();
        editLogStream = null;
      }
    } catch (IOException e) {
      LOG.warn("All journals failed to abort", e);
    }
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

    try {
      journalSet.purgeLogsOlderThan(minTxIdToKeep);
    } catch (IOException ex) {
      //All journals have failed, it will be handled in logSync.
    }
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
      journalSet.setOutputBufferCapacity(size);
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
    
    JournalManager jas = findBackupJournal(bnReg);
    if (jas != null) {
      // already registered
      LOG.info("Backup node " + bnReg + " re-registers");
      return;
    }
    
    LOG.info("Registering new backup node: " + bnReg);
    BackupJournalManager bjm = new BackupJournalManager(bnReg, nnReg);
    journalSet.add(bjm);
  }
  
  synchronized void releaseBackupStream(NamenodeRegistration registration)
      throws IOException {
    BackupJournalManager bjm = this.findBackupJournal(registration);
    if (bjm != null) {
      LOG.info("Removing backup journal " + bjm);
      journalSet.remove(bjm);
    }
  }
  
  /**
   * Find the JournalAndStream associated with this BackupNode.
   * 
   * @return null if it cannot be found
   */
  private synchronized BackupJournalManager findBackupJournal(
      NamenodeRegistration bnReg) {
    for (JournalManager bjm : journalSet.getJournalManagers()) {
      if ((bjm instanceof BackupJournalManager)
          && ((BackupJournalManager) bjm).matchesRegistration(bnReg)) {
        return (BackupJournalManager) bjm;
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

    try {
      editLogStream.writeRaw(data, 0, length);
    } catch (IOException ex) {
      // All journals have failed, it will be handled in logSync.
    }
    endTransaction(start);
  }

  /**
   * Run recovery on all journals to recover any unclosed segments
   */
  void recoverUnclosedStreams() {
    try {
      journalSet.recoverUnfinalizedSegments();
    } catch (IOException ex) {
      // All journals have failed, it is handled in logSync.
    }
  }

  /**
   * Select a list of input streams to load.
   * @param fromTxId first transaction in the selected streams
   * @param toAtLeast the selected streams must contain this transaction
   */
  Collection<EditLogInputStream> selectInputStreams(long fromTxId,
      long toAtLeastTxId) throws IOException {
    List<EditLogInputStream> streams = new ArrayList<EditLogInputStream>();
    EditLogInputStream stream = journalSet.getInputStream(fromTxId);
    while (stream != null) {
      fromTxId = stream.getLastTxId() + 1;
      streams.add(stream);
      stream = journalSet.getInputStream(fromTxId);
    }
    if (fromTxId <= toAtLeastTxId) {
      closeAllStreams(streams);
      throw new IOException("No non-corrupt logs for txid " 
                            + fromTxId);
    }
    return streams;
  }

  /** 
   * Close all the streams in a collection
   * @param streams The list of streams to close
   */
  static void closeAllStreams(Iterable<EditLogInputStream> streams) {
    for (EditLogInputStream s : streams) {
      IOUtils.closeStream(s);
    }
  }
}
