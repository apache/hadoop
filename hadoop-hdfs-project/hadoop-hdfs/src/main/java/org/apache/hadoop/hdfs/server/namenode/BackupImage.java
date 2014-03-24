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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.zip.Checksum;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.Storage.StorageState;
import org.apache.hadoop.util.StringUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Extension of FSImage for the backup node.
 * This class handles the setup of the journaling 
 * spool on the backup namenode.
 */
@InterfaceAudience.Private
public class BackupImage extends FSImage {
  /** Backup input stream for loading edits into memory */
  private final EditLogBackupInputStream backupInputStream =
    new EditLogBackupInputStream("Data from remote NameNode");
  
  /**
   * Current state of the BackupNode. The BackupNode's state
   * transitions are as follows:
   * 
   * Initial: DROP_UNTIL_NEXT_ROLL
   * - Transitions to JOURNAL_ONLY the next time the log rolls
   * - Transitions to IN_SYNC in convergeJournalSpool
   * - Transitions back to JOURNAL_ONLY if the log rolls while
   *   stopApplyingOnNextRoll is true.
   */
  volatile BNState bnState;
  static enum BNState {
    /**
     * Edits from the NN should be dropped. On the next log roll,
     * transition to JOURNAL_ONLY state
     */
    DROP_UNTIL_NEXT_ROLL,
    /**
     * Edits from the NN should be written to the local edits log
     * but not applied to the namespace.
     */
    JOURNAL_ONLY,
    /**
     * Edits should be written to the local edits log and applied
     * to the local namespace.
     */
    IN_SYNC;
  }

  /**
   * Flag to indicate that the next time the NN rolls, the BN
   * should transition from to JOURNAL_ONLY state.
   * {@see #freezeNamespaceAtNextRoll()}
   */
  private boolean stopApplyingEditsOnNextRoll = false;
  
  private FSNamesystem namesystem;

  /**
   * Construct a backup image.
   * @param conf Configuration
   * @throws IOException if storage cannot be initialised.
   */
  BackupImage(Configuration conf) throws IOException {
    super(conf);
    storage.setDisablePreUpgradableLayoutCheck(true);
    bnState = BNState.DROP_UNTIL_NEXT_ROLL;
  }
  
  void setNamesystem(FSNamesystem fsn) {
    this.namesystem = fsn;
  }

  /**
   * Analyze backup storage directories for consistency.<br>
   * Recover from incomplete checkpoints if required.<br>
   * Read VERSION and fstime files if exist.<br>
   * Do not load image or edits.
   *
   * @throws IOException if the node should shutdown.
   */
  void recoverCreateRead() throws IOException {
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      StorageState curState;
      try {
        curState = sd.analyzeStorage(HdfsServerConstants.StartupOption.REGULAR, storage);
        // sd is locked but not opened
        switch(curState) {
        case NON_EXISTENT:
          // fail if any of the configured storage dirs are inaccessible
          throw new InconsistentFSStateException(sd.getRoot(),
                "checkpoint directory does not exist or is not accessible.");
        case NOT_FORMATTED:
          // for backup node all directories may be unformatted initially
          LOG.info("Storage directory " + sd.getRoot() + " is not formatted.");
          LOG.info("Formatting ...");
          sd.clearDirectory(); // create empty current
          break;
        case NORMAL:
          break;
        default:  // recovery is possible
          sd.doRecover(curState);
        }
        if(curState != StorageState.NOT_FORMATTED) {
          // read and verify consistency with other directories
          storage.readProperties(sd);
        }
      } catch(IOException ioe) {
        sd.unlock();
        throw ioe;
      }
    }
  }

  /**
   * Save meta-data into fsimage files.
   * and create empty edits.
   */
  void saveCheckpoint() throws IOException {
    saveNamespace(namesystem);
  }

  /**
   * Receive a batch of edits from the NameNode.
   * 
   * Depending on bnState, different actions are taken. See
   * {@link BackupImage.BNState}
   * 
   * @param firstTxId first txid in batch
   * @param numTxns number of transactions
   * @param data serialized journal records.
   * @throws IOException
   * @see #convergeJournalSpool()
   */
  synchronized void journal(long firstTxId, int numTxns, byte[] data) throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Got journal, " +
          "state = " + bnState +
          "; firstTxId = " + firstTxId +
          "; numTxns = " + numTxns);
    }
    
    switch(bnState) {
      case DROP_UNTIL_NEXT_ROLL:
        return;

      case IN_SYNC:
        // update NameSpace in memory
        applyEdits(firstTxId, numTxns, data);
        break;
      
      case JOURNAL_ONLY:
        break;
      
      default:
        throw new AssertionError("Unhandled state: " + bnState);
    }
    
    // write to BN's local edit log.
    logEditsLocally(firstTxId, numTxns, data);
  }

  /**
   * Write the batch of edits to the local copy of the edit logs.
   */
  private void logEditsLocally(long firstTxId, int numTxns, byte[] data) {
    long expectedTxId = editLog.getLastWrittenTxId() + 1;
    Preconditions.checkState(firstTxId == expectedTxId,
        "received txid batch starting at %s but expected txn %s",
        firstTxId, expectedTxId);
    editLog.setNextTxId(firstTxId + numTxns - 1);
    editLog.logEdit(data.length, data);
    editLog.logSync();
  }

  /**
   * Apply the batch of edits to the local namespace.
   */
  private synchronized void applyEdits(long firstTxId, int numTxns, byte[] data)
      throws IOException {
    Preconditions.checkArgument(firstTxId == lastAppliedTxId + 1,
        "Received txn batch starting at %s but expected %s",
        firstTxId, lastAppliedTxId + 1);
    assert backupInputStream.length() == 0 : "backup input stream is not empty";
    try {
      if (LOG.isTraceEnabled()) {
        LOG.debug("data:" + StringUtils.byteToHexString(data));
      }

      FSEditLogLoader logLoader =
          new FSEditLogLoader(namesystem, lastAppliedTxId);
      int logVersion = storage.getLayoutVersion();
      backupInputStream.setBytes(data, logVersion);

      long numTxnsAdvanced = logLoader.loadEditRecords(
          backupInputStream, true, lastAppliedTxId + 1, null, null);
      if (numTxnsAdvanced != numTxns) {
        throw new IOException("Batch of txns starting at txnid " +
            firstTxId + " was supposed to contain " + numTxns +
            " transactions, but we were only able to advance by " +
            numTxnsAdvanced);
      }
      lastAppliedTxId = logLoader.getLastAppliedTxId();

      FSImage.updateCountForQuota(namesystem.dir.rootDir); // inefficient!
    } finally {
      backupInputStream.clear();
    }
  }

  /**
   * Transition the BackupNode from JOURNAL_ONLY state to IN_SYNC state.
   * This is done by repeated invocations of tryConvergeJournalSpool until
   * we are caught up to the latest in-progress edits file.
   */
  void convergeJournalSpool() throws IOException {
    Preconditions.checkState(bnState == BNState.JOURNAL_ONLY,
        "bad state: %s", bnState);

    while (!tryConvergeJournalSpool()) {
      ;
    }
    assert bnState == BNState.IN_SYNC;
  }
  
  private boolean tryConvergeJournalSpool() throws IOException {
    Preconditions.checkState(bnState == BNState.JOURNAL_ONLY,
        "bad state: %s", bnState);
    
    // This section is unsynchronized so we can continue to apply
    // ahead of where we're reading, concurrently. Since the state
    // is JOURNAL_ONLY at this point, we know that lastAppliedTxId
    // doesn't change, and curSegmentTxId only increases

    while (lastAppliedTxId < editLog.getCurSegmentTxId() - 1) {
      long target = editLog.getCurSegmentTxId();
      LOG.info("Loading edits into backupnode to try to catch up from txid "
          + lastAppliedTxId + " to " + target);
      FSImageTransactionalStorageInspector inspector =
        new FSImageTransactionalStorageInspector();
      
      storage.inspectStorageDirs(inspector);

      editLog.recoverUnclosedStreams();
      Iterable<EditLogInputStream> editStreamsAll 
        = editLog.selectInputStreams(lastAppliedTxId, target - 1);
      // remove inprogress
      List<EditLogInputStream> editStreams = Lists.newArrayList();
      for (EditLogInputStream s : editStreamsAll) {
        if (s.getFirstTxId() != editLog.getCurSegmentTxId()) {
          editStreams.add(s);
        }
      }
      loadEdits(editStreams, namesystem);
    }
    
    // now, need to load the in-progress file
    synchronized (this) {
      if (lastAppliedTxId != editLog.getCurSegmentTxId() - 1) {
        LOG.debug("Logs rolled while catching up to current segment");
        return false; // drop lock and try again to load local logs
      }
      
      EditLogInputStream stream = null;
      Collection<EditLogInputStream> editStreams
        = getEditLog().selectInputStreams(
            getEditLog().getCurSegmentTxId(),
            getEditLog().getCurSegmentTxId());
      
      for (EditLogInputStream s : editStreams) {
        if (s.getFirstTxId() == getEditLog().getCurSegmentTxId()) {
          stream = s;
        }
        break;
      }
      if (stream == null) {
        LOG.warn("Unable to find stream starting with " + editLog.getCurSegmentTxId()
                 + ". This indicates that there is an error in synchronization in BackupImage");
        return false;
      }

      try {
        long remainingTxns = getEditLog().getLastWrittenTxId() - lastAppliedTxId;
        
        LOG.info("Going to finish converging with remaining " + remainingTxns
            + " txns from in-progress stream " + stream);
        
        FSEditLogLoader loader =
            new FSEditLogLoader(namesystem, lastAppliedTxId);
        loader.loadFSEdits(stream, lastAppliedTxId + 1);
        lastAppliedTxId = loader.getLastAppliedTxId();
        assert lastAppliedTxId == getEditLog().getLastWrittenTxId();
      } finally {
        FSEditLog.closeAllStreams(editStreams);
      }

      LOG.info("Successfully synced BackupNode with NameNode at txnid " +
          lastAppliedTxId);
      setState(BNState.IN_SYNC);
    }
    return true;
  }

  /**
   * Transition edit log to a new state, logging as necessary.
   */
  private synchronized void setState(BNState newState) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("State transition " + bnState + " -> " + newState);
    }
    bnState = newState;
  }

  /**
   * Receive a notification that the NameNode has begun a new edit log.
   * This causes the BN to also start the new edit log in its local
   * directories.
   */
  synchronized void namenodeStartedLogSegment(long txid)
      throws IOException {
    LOG.info("NameNode started a new log segment at txid " + txid);
    if (editLog.isSegmentOpen()) {
      if (editLog.getLastWrittenTxId() == txid - 1) {
        // We are in sync with the NN, so end and finalize the current segment
        editLog.endCurrentLogSegment(false);
      } else {
        // We appear to have missed some transactions -- the NN probably
        // lost contact with us temporarily. So, mark the current segment
        // as aborted.
        LOG.warn("NN started new log segment at txid " + txid +
            ", but BN had only written up to txid " +
            editLog.getLastWrittenTxId() +
            "in the log segment starting at " + 
        		editLog.getCurSegmentTxId() + ". Aborting this " +
        		"log segment.");
        editLog.abortCurrentLogSegment();
      }
    }
    editLog.setNextTxId(txid);
    editLog.startLogSegment(txid, false);
    if (bnState == BNState.DROP_UNTIL_NEXT_ROLL) {
      setState(BNState.JOURNAL_ONLY);
    }
    
    if (stopApplyingEditsOnNextRoll) {
      if (bnState == BNState.IN_SYNC) {
        LOG.info("Stopped applying edits to prepare for checkpoint.");
        setState(BNState.JOURNAL_ONLY);
      }
      stopApplyingEditsOnNextRoll = false;
      notifyAll();
    }
  }

  /**
   * Request that the next time the BN receives a log roll, it should
   * stop applying the edits log to the local namespace. This is
   * typically followed on by a call to {@link #waitUntilNamespaceFrozen()}
   */
  synchronized void freezeNamespaceAtNextRoll() {
    stopApplyingEditsOnNextRoll = true;
  }

  /**
   * After {@link #freezeNamespaceAtNextRoll()} has been called, wait until
   * the BN receives notification of the next log roll.
   */
  synchronized void waitUntilNamespaceFrozen() throws IOException {
    if (bnState != BNState.IN_SYNC) return;

    LOG.info("Waiting until the NameNode rolls its edit logs in order " +
        "to freeze the BackupNode namespace.");
    while (bnState == BNState.IN_SYNC) {
      Preconditions.checkState(stopApplyingEditsOnNextRoll,
        "If still in sync, we should still have the flag set to " +
        "freeze at next roll");
      try {
        wait();
      } catch (InterruptedException ie) {
        LOG.warn("Interrupted waiting for namespace to freeze", ie);
        throw new IOException(ie);
      }
    }
    LOG.info("BackupNode namespace frozen.");
  }

  /**
   * Override close() so that we don't finalize edit logs.
   */
  @Override
  public synchronized void close() throws IOException {
    editLog.abortCurrentLogSegment();
    storage.close();
  }
}
