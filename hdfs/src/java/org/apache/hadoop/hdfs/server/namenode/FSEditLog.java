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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.zip.Checksum;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DeprecatedUTF8;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import static org.apache.hadoop.hdfs.server.common.Util.now;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorageArchivalManager.StorageArchiver;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.util.PureJavaCrc32;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.*;

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
  private long curSegmentTxId = FSConstants.INVALID_TXID;

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

  private static ThreadLocal<Checksum> localChecksum =
    new ThreadLocal<Checksum>() {
    protected Checksum initialValue() {
      return new PureJavaCrc32();
    }
  };

  /** Get a thread local checksum */
  public static Checksum getChecksum() {
    return localChecksum.get();
  }

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
  }
  
  /**
   * Initialize the list of edit journals
   */
  private void initJournals() {
    assert journals.isEmpty();
    Preconditions.checkState(state == State.UNINITIALIZED,
        "Bad state: %s", state);
    
    for (StorageDirectory sd : storage.dirIterable(NameNodeDirType.EDITS)) {
      journals.add(new JournalAndStream(new FileJournalManager(sd)));
    }
    
    if (journals.isEmpty()) {
      LOG.error("No edits directories configured!");
    }
    
    state = State.BETWEEN_LOG_SEGMENTS;
  }
  
  private int getNumEditsDirs() {
   return storage.getNumStorageDirs(NameNodeDirType.EDITS);
  }

  /**
   * Initialize the output stream for logging, opening the first
   * log segment.
   */
  synchronized void open() throws IOException {
    Preconditions.checkState(state == State.UNINITIALIZED);
    initJournals();

    startLogSegment(getLastWrittenTxId() + 1);
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
      assert !journals.isEmpty();
      waitForSyncToFinish();
      endCurrentLogSegment();
    }

    state = State.CLOSED;
  }

  /**
   * Write an operation to the edit log. Do not sync to persistent
   * store yet.
   */
  void logEdit(final FSEditLogOpCodes opCode, final Writable ... writables) {
    assert state != State.CLOSED;
    
    synchronized (this) {
      // wait if an automatic sync is scheduled
      waitIfAutoSyncScheduled();
      
      if (journals.isEmpty()) {
        throw new java.lang.IllegalStateException(NO_JOURNAL_STREAMS_WARNING);
      }
      
      // Only start a new transaction for OPs which will be persisted to disk.
      // Obviously this excludes control op codes.
      long start = now();
      if (opCode.getOpCode() < FSEditLogOpCodes.OP_JSPOOL_START.getOpCode()) {
        start = beginTransaction();
      }

      mapJournalsAndReportErrors(new JournalClosure() {
        @Override 
        public void apply(JournalAndStream jas) throws IOException {
          if (!jas.isActive()) return;
          EditLogOutputStream stream = jas.stream;
          if(!stream.isOperationSupported(opCode.getOpCode()))
            return;
          stream.write(opCode.getOpCode(), txid, writables);
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
    assert synctxid <= txid &&
       nextTxId >= txid : "May not decrease txid." +
      " synctxid=" + synctxid +
      " txid=" + txid +
      " nextTxid=" + nextTxId;
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
    
    List<JournalAndStream> stillGoodJournals =
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
            stillGoodJournals.add(jas);
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
      for (JournalAndStream jas : stillGoodJournals) {
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
  
      if (metrics != null) // Metrics non-null only when used inside name node
        metrics.addSync(elapsed);
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
    if (journals.isEmpty()) {
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

    DeprecatedUTF8 nameReplicationPair[] = new DeprecatedUTF8[] { 
      new DeprecatedUTF8(path), 
      FSEditLog.toLogReplication(newNode.getReplication()),
      FSEditLog.toLogLong(newNode.getModificationTime()),
      FSEditLog.toLogLong(newNode.getAccessTime()),
      FSEditLog.toLogLong(newNode.getPreferredBlockSize())};
    logEdit(OP_ADD,
            new ArrayWritable(DeprecatedUTF8.class, nameReplicationPair), 
            new ArrayWritable(Block.class, newNode.getBlocks()),
            newNode.getPermissionStatus(),
            new DeprecatedUTF8(newNode.getClientName()),
            new DeprecatedUTF8(newNode.getClientMachine()));
  }

  /** 
   * Add close lease record to edit log.
   */
  public void logCloseFile(String path, INodeFile newNode) {
    DeprecatedUTF8 nameReplicationPair[] = new DeprecatedUTF8[] {
      new DeprecatedUTF8(path),
      FSEditLog.toLogReplication(newNode.getReplication()),
      FSEditLog.toLogLong(newNode.getModificationTime()),
      FSEditLog.toLogLong(newNode.getAccessTime()),
      FSEditLog.toLogLong(newNode.getPreferredBlockSize())};
    logEdit(OP_CLOSE,
            new ArrayWritable(DeprecatedUTF8.class, nameReplicationPair),
            new ArrayWritable(Block.class, newNode.getBlocks()),
            newNode.getPermissionStatus());
  }
  
  /** 
   * Add create directory record to edit log
   */
  public void logMkDir(String path, INode newNode) {
    DeprecatedUTF8 info[] = new DeprecatedUTF8[] {
      new DeprecatedUTF8(path),
      FSEditLog.toLogLong(newNode.getModificationTime()),
      FSEditLog.toLogLong(newNode.getAccessTime())
    };
    logEdit(OP_MKDIR,
      new ArrayWritable(DeprecatedUTF8.class, info),
      newNode.getPermissionStatus());
  }
  
  /** 
   * Add rename record to edit log
   * TODO: use String parameters until just before writing to disk
   */
  void logRename(String src, String dst, long timestamp) {
    DeprecatedUTF8 info[] = new DeprecatedUTF8[] { 
      new DeprecatedUTF8(src),
      new DeprecatedUTF8(dst),
      FSEditLog.toLogLong(timestamp)};
    logEdit(OP_RENAME_OLD, new ArrayWritable(DeprecatedUTF8.class, info));
  }
  
  /** 
   * Add rename record to edit log
   */
  void logRename(String src, String dst, long timestamp, Options.Rename... options) {
    DeprecatedUTF8 info[] = new DeprecatedUTF8[] { 
      new DeprecatedUTF8(src),
      new DeprecatedUTF8(dst),
      FSEditLog.toLogLong(timestamp)};
    logEdit(OP_RENAME,
      new ArrayWritable(DeprecatedUTF8.class, info),
      toBytesWritable(options));
  }
  
  /** 
   * Add set replication record to edit log
   */
  void logSetReplication(String src, short replication) {
    logEdit(OP_SET_REPLICATION, 
      new DeprecatedUTF8(src), 
      FSEditLog.toLogReplication(replication));
  }
  
  /** Add set namespace quota record to edit log
   * 
   * @param src the string representation of the path to a directory
   * @param quota the directory size limit
   */
  void logSetQuota(String src, long nsQuota, long dsQuota) {
    logEdit(OP_SET_QUOTA,
      new DeprecatedUTF8(src), 
      new LongWritable(nsQuota), new LongWritable(dsQuota));
  }

  /**  Add set permissions record to edit log */
  void logSetPermissions(String src, FsPermission permissions) {
    logEdit(OP_SET_PERMISSIONS, new DeprecatedUTF8(src), permissions);
  }

  /**  Add set owner record to edit log */
  void logSetOwner(String src, String username, String groupname) {
    DeprecatedUTF8 u = new DeprecatedUTF8(username == null? "": username);
    DeprecatedUTF8 g = new DeprecatedUTF8(groupname == null? "": groupname);
    logEdit(OP_SET_OWNER, new DeprecatedUTF8(src), u, g);
  }
  
  /**
   * concat(trg,src..) log
   */
  void logConcat(String trg, String [] srcs, long timestamp) {
    int size = 1 + srcs.length + 1; // trg, srcs, timestamp
    DeprecatedUTF8 info[] = new DeprecatedUTF8[size];
    int idx = 0;
    info[idx++] = new DeprecatedUTF8(trg);
    for(int i=0; i<srcs.length; i++) {
      info[idx++] = new DeprecatedUTF8(srcs[i]);
    }
    info[idx] = FSEditLog.toLogLong(timestamp);
    logEdit(OP_CONCAT_DELETE, new ArrayWritable(DeprecatedUTF8.class, info));
  }
  
  /** 
   * Add delete file record to edit log
   */
  void logDelete(String src, long timestamp) {
    DeprecatedUTF8 info[] = new DeprecatedUTF8[] { 
      new DeprecatedUTF8(src),
      FSEditLog.toLogLong(timestamp)};
    logEdit(OP_DELETE, new ArrayWritable(DeprecatedUTF8.class, info));
  }

  /** 
   * Add generation stamp record to edit log
   */
  void logGenerationStamp(long genstamp) {
    logEdit(OP_SET_GENSTAMP, new LongWritable(genstamp));
  }

  /** 
   * Add access time record to edit log
   */
  void logTimes(String src, long mtime, long atime) {
    DeprecatedUTF8 info[] = new DeprecatedUTF8[] { 
      new DeprecatedUTF8(src),
      FSEditLog.toLogLong(mtime),
      FSEditLog.toLogLong(atime)};
    logEdit(OP_TIMES, new ArrayWritable(DeprecatedUTF8.class, info));
  }

  /** 
   * Add a create symlink record.
   */
  void logSymlink(String path, String value, long mtime, 
                  long atime, INodeSymlink node) {
    DeprecatedUTF8 info[] = new DeprecatedUTF8[] { 
      new DeprecatedUTF8(path),
      new DeprecatedUTF8(value),
      FSEditLog.toLogLong(mtime),
      FSEditLog.toLogLong(atime)};
    logEdit(OP_SYMLINK, 
      new ArrayWritable(DeprecatedUTF8.class, info),
      node.getPermissionStatus());
  }
  
  /**
   * log delegation token to edit log
   * @param id DelegationTokenIdentifier
   * @param expiryTime of the token
   * @return
   */
  void logGetDelegationToken(DelegationTokenIdentifier id,
      long expiryTime) {
    logEdit(OP_GET_DELEGATION_TOKEN, id, FSEditLog.toLogLong(expiryTime));
  }
  
  void logRenewDelegationToken(DelegationTokenIdentifier id,
      long expiryTime) {
    logEdit(OP_RENEW_DELEGATION_TOKEN, id, FSEditLog.toLogLong(expiryTime));
  }
  
  void logCancelDelegationToken(DelegationTokenIdentifier id) {
    logEdit(OP_CANCEL_DELEGATION_TOKEN, id);
  }
  
  void logUpdateMasterKey(DelegationKey key) {
    logEdit(OP_UPDATE_MASTER_KEY, key);
  }

  void logReassignLease(String leaseHolder, String src, String newHolder) {
    logEdit(OP_REASSIGN_LEASE, new DeprecatedUTF8(leaseHolder),
        new DeprecatedUTF8(src),
        new DeprecatedUTF8(newHolder));
  }
  
  static private DeprecatedUTF8 toLogReplication(short replication) {
    return new DeprecatedUTF8(Short.toString(replication));
  }
  
  static private DeprecatedUTF8 toLogLong(long timestamp) {
    return new DeprecatedUTF8(Long.toString(timestamp));
  }

  /**
   * Return the size of the current EditLog
   */
  // TODO who uses this, does it make sense with transactions?
  synchronized long getEditLogSize() throws IOException {
    assert getNumEditsDirs() <= journals.size() :
        "Number of edits directories should not exceed the number of streams.";
    long size = -1;
        
    List<JournalAndStream> badJournals = Lists.newArrayList();
    
    for (JournalAndStream j : journals) {
      if (!j.isActive()) continue;
      EditLogOutputStream es = j.getCurrentStream();
      try {
        long curSize = es.length();
        assert (size == 0 || size == curSize || curSize ==0) :
          "Wrong streams size";
        size = Math.max(size, curSize);
      } catch (IOException e) {
        LOG.error("getEditLogSize: editstream.length failed. removing journal " + j, e);
        badJournals.add(j);
      }
    }
    disableAndReportErrorOnJournals(badJournals);
    
    assert size != -1;
    return size;
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
  public RemoteEditLogManifest getEditLogManifest(long sinceTxId)
      throws IOException {
    FSImageTransactionalStorageInspector inspector =
        new FSImageTransactionalStorageInspector();

    for (StorageDirectory sd : storage.dirIterable(NameNodeDirType.EDITS)) {
      inspector.inspectDirectory(sd);
    }
    
    return inspector.getEditLogManifest(sinceTxId);
  }
  
  /**
   * Finalizes the current edit log and opens a new log segment.
   * @return the transaction id of the BEGIN_LOG_SEGMENT transaction
   * in the new log.
   */
  synchronized long rollEditLog() throws IOException {
    LOG.info("Rolling edit logs.");
    endCurrentLogSegment();
    
    long nextTxId = getLastWrittenTxId() + 1;
    startLogSegment(nextTxId);
    
    assert curSegmentTxId == nextTxId;
    return nextTxId;
  }
  
  /**
   * Start writing to the log segment with the given txid.
   * Transitions from BETWEEN_LOG_SEGMENTS state to IN_LOG_SEGMENT state. 
   */
  synchronized void startLogSegment(final long txId) {
    LOG.info("Starting log segment at " + txId);
    Preconditions.checkState(state == State.BETWEEN_LOG_SEGMENTS,
        "Bad state: %s", state);
    Preconditions.checkState(txId > curSegmentTxId,
        "Cannot start writing to log segment " + txId +
        " when previous log segment started at " + curSegmentTxId);
    curSegmentTxId = txId;
    
    numTransactions = totalTimeTransactions = numTransactionsBatchedInSync = 0;

    // TODO no need to link this back to storage anymore!
    storage.attemptRestoreRemovedStorage();
    
    mapJournalsAndReportErrors(new JournalClosure() {
      @Override
      public void apply(JournalAndStream jas) throws IOException {
        jas.startLogSegment(txId);
      }
    }, "starting log segment " + txId);

    state = State.IN_SEGMENT;

    logEdit(FSEditLogOpCodes.OP_START_LOG_SEGMENT);
    logSync();    
  }

  /**
   * Finalize the current log segment.
   * Transitions from IN_SEGMENT state to BETWEEN_LOG_SEGMENTS state.
   */
  synchronized void endCurrentLogSegment() {
    LOG.info("Ending log segment " + curSegmentTxId);
    Preconditions.checkState(state == State.IN_SEGMENT,
        "Bad state: %s", state);
    logEdit(FSEditLogOpCodes.OP_END_LOG_SEGMENT);
    waitForSyncToFinish();
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
   * Archive any log files that are older than the given txid.
   */
  public void archiveLogsOlderThan(
      final long minTxIdToKeep, final StorageArchiver archiver) {
    assert curSegmentTxId == FSConstants.INVALID_TXID || // on format this is no-op
      minTxIdToKeep <= curSegmentTxId :
      "cannot archive logs older than txid " + minTxIdToKeep +
      " when current segment starts at " + curSegmentTxId;
    
    mapJournalsAndReportErrors(new JournalClosure() {
      @Override
      public void apply(JournalAndStream jas) throws IOException {
        jas.manager.archiveLogsOlderThan(minTxIdToKeep, archiver);
      }
    }, "archiving logs older than " + minTxIdToKeep);
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
   * Send a record, prescribing to start journal spool.<br>
   * This should be sent via regular stream of journal records so that
   * the backup node new exactly after which record it should start spooling.
   * 
   * @param bnReg the backup node registration information.
   * @param nnReg this (active) name-node registration.
   * @throws IOException
   */
  synchronized void logJSpoolStart(NamenodeRegistration bnReg, // backup node
                      NamenodeRegistration nnReg) // active name-node
  throws IOException {
    /*
    if(bnReg.isRole(NamenodeRole.CHECKPOINT))
      return; // checkpoint node does not stream edits
    if(editStreams == null)
      editStreams = new ArrayList<EditLogOutputStream>();
    EditLogOutputStream boStream = null;
    for(EditLogOutputStream eStream : editStreams) {
      if(eStream.getName().equals(bnReg.getAddress())) {
        boStream = eStream; // already there
        break;
      }
    }
    if(boStream == null) {
      boStream = new EditLogBackupOutputStream(bnReg, nnReg);
      editStreams.add(boStream);
    }
    logEdit(OP_JSPOOL_START, (Writable[])null);
    TODO: backupnode is disabled
    */
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
          jas.getCurrentStream().write(data, 0, length);
        }
      }      
    }, "Logging edit");

    endTransaction(start);
  }

  synchronized void releaseBackupStream(NamenodeRegistration registration) {
    /*
    Iterator<EditLogOutputStream> it =
                                  getOutputStreamIterator(JournalType.BACKUP);
    ArrayList<EditLogOutputStream> errorStreams = null;
    NamenodeRegistration backupNode = null;
    while(it.hasNext()) {
      EditLogBackupOutputStream eStream = (EditLogBackupOutputStream)it.next();
      backupNode = eStream.getRegistration();
      if(backupNode.getAddress().equals(registration.getAddress()) &&
            backupNode.isRole(registration.getRole())) {
        errorStreams = new ArrayList<EditLogOutputStream>(1);
        errorStreams.add(eStream);
        break;
      }
    }
    assert backupNode == null || backupNode.isRole(NamenodeRole.BACKUP) :
      "Not a backup node corresponds to a backup stream";
    disableAndReportErrorOnJournals(errorStreams);
    TODO BN currently disabled
    */
  }

  synchronized boolean checkBackupRegistration(
      NamenodeRegistration registration) {
    /*
    Iterator<EditLogOutputStream> it =
                                  getOutputStreamIterator(JournalType.BACKUP);
    boolean regAllowed = !it.hasNext();
    NamenodeRegistration backupNode = null;
    ArrayList<EditLogOutputStream> errorStreams = null;
    while(it.hasNext()) {
      EditLogBackupOutputStream eStream = (EditLogBackupOutputStream)it.next();
      backupNode = eStream.getRegistration();
      if(backupNode.getAddress().equals(registration.getAddress()) &&
          backupNode.isRole(registration.getRole())) {
        regAllowed = true; // same node re-registers
        break;
      }
      if(!eStream.isAlive()) {
        if(errorStreams == null)
          errorStreams = new ArrayList<EditLogOutputStream>(1);
        errorStreams.add(eStream);
        regAllowed = true; // previous backup node failed
      }
    }
    assert backupNode == null || backupNode.isRole(NamenodeRole.BACKUP) :
      "Not a backup node corresponds to a backup stream";
    disableAndReportErrorOnJournals(errorStreams);
    return regAllowed;
    
    TODO BN currently disabled
    */
    return false;
  }
  
  static BytesWritable toBytesWritable(Options.Rename... options) {
    byte[] bytes = new byte[options.length];
    for (int i = 0; i < options.length; i++) {
      bytes[i] = options[i].value();
    }
    return new BytesWritable(bytes);
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
      try {
        j.abort();
      } catch (IOException ioe) {
        LOG.warn("Failed to abort faulty journal " + j
                 + " before removing it (might be OK)", ioe);
      }
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
    private long segmentStartsAtTxId = FSConstants.INVALID_TXID;
    
    public JournalAndStream(JournalManager manager) {
      this.manager = manager;
    }

    public void startLogSegment(long txId) throws IOException {
      Preconditions.checkState(stream == null);
      stream = manager.startLogSegment(txId);
      segmentStartsAtTxId = txId;
    }

    public void close(long lastTxId) throws IOException {
      if (stream == null) return;
      stream.close();
      manager.finalizeLogSegment(segmentStartsAtTxId, lastTxId);
      stream = null;
    }
    
    public void abort() throws IOException {
      if (stream == null) return;
      try {
        stream.abort();
      } catch (IOException ioe) {
        LOG.error("Unable to abort stream " + stream, ioe);
      }
      stream = null;
      segmentStartsAtTxId = FSConstants.INVALID_TXID;
    }

    boolean isActive() {
      return stream != null;
    }
    
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
  }
}
