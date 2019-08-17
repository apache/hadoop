/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.hdfs.protocol.ZoneReencryptionStatus;
import org.apache.hadoop.hdfs.server.namenode.ReencryptionHandler.ReencryptionBatch;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REENCRYPT_THROTTLE_LIMIT_UPDATER_RATIO_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REENCRYPT_THROTTLE_LIMIT_UPDATER_RATIO_KEY;

/**
 * Class for finalizing re-encrypt EDEK operations, by updating file xattrs with
 * edeks returned from reencryption.
 * <p>
 * The tasks are submitted by ReencryptionHandler.
 * <p>
 * It is assumed only 1 Updater will be running, since updating file xattrs
 * requires namespace write lock, and performance gain from multi-threading
 * is limited.
 */
@InterfaceAudience.Private
public final class ReencryptionUpdater implements Runnable {

  public static final Logger LOG =
      LoggerFactory.getLogger(ReencryptionUpdater.class);

  private volatile boolean shouldPauseForTesting = false;
  private volatile int pauseAfterNthCheckpoint = 0;
  private volatile long pauseZoneId = 0;

  private double throttleLimitRatio;
  private final StopWatch throttleTimerAll = new StopWatch();
  private final StopWatch throttleTimerLocked = new StopWatch();

  private volatile long faultRetryInterval = 60000;
  private volatile boolean isRunning = false;

  /**
   * Class to track re-encryption submissions of a single zone. It contains
   * all the submitted futures, and statistics about how far the futures are
   * processed.
   */
  static final class ZoneSubmissionTracker {
    private boolean submissionDone;
    private LinkedList<Future> tasks;
    private int numCheckpointed;
    private int numFutureDone;

    ZoneSubmissionTracker() {
      submissionDone = false;
      tasks = new LinkedList<>();
      numCheckpointed = 0;
      numFutureDone = 0;
    }

    void reset() {
      submissionDone = false;
      tasks.clear();
      numCheckpointed = 0;
      numFutureDone = 0;
    }

    LinkedList<Future> getTasks() {
      return tasks;
    }

    void cancelAllTasks() {
      if (!tasks.isEmpty()) {
        LOG.info("Cancelling {} re-encryption tasks", tasks.size());
        for (Future f : tasks) {
          f.cancel(true);
        }
      }
    }

    void addTask(final Future task) {
      tasks.add(task);
    }

    private boolean isCompleted() {
      return submissionDone && tasks.isEmpty();
    }

    void setSubmissionDone() {
      submissionDone = true;
    }
  }

  /**
   * Class representing the task for one batch of a re-encryption command. It
   * also contains statistics about how far this single batch has been executed.
   */
  static final class ReencryptionTask {
    private final long zoneId;
    private boolean processed = false;
    private int numFilesUpdated = 0;
    private int numFailures = 0;
    private String lastFile = null;
    private final ReencryptionBatch batch;

    ReencryptionTask(final long id, final int failures,
        final ReencryptionBatch theBatch) {
      zoneId = id;
      numFailures = failures;
      batch = theBatch;
    }
  }

  /**
   * Class that encapsulates re-encryption details of a file. It contains the
   * file inode, stores the initial edek of the file, and the new edek
   * after re-encryption.
   * <p>
   * Assumptions are the object initialization happens when dir lock is held,
   * and inode is valid and is encrypted during initialization.
   * <p>
   * Namespace changes may happen during re-encryption, and if inode is changed
   * the re-encryption is skipped.
   */
  static final class FileEdekInfo {
    private final long inodeId;
    private final EncryptedKeyVersion existingEdek;
    private EncryptedKeyVersion edek = null;

    FileEdekInfo(FSDirectory dir, INodeFile inode) throws IOException {
      assert dir.hasReadLock();
      Preconditions.checkNotNull(inode, "INodeFile is null");
      inodeId = inode.getId();
      final FileEncryptionInfo fei = FSDirEncryptionZoneOp
          .getFileEncryptionInfo(dir, INodesInPath.fromINode(inode));
      Preconditions.checkNotNull(fei,
          "FileEncryptionInfo is null for " + inodeId);
      existingEdek = EncryptedKeyVersion
          .createForDecryption(fei.getKeyName(), fei.getEzKeyVersionName(),
              fei.getIV(), fei.getEncryptedDataEncryptionKey());
    }

    long getInodeId() {
      return inodeId;
    }

    EncryptedKeyVersion getExistingEdek() {
      return existingEdek;
    }

    void setEdek(final EncryptedKeyVersion ekv) {
      assert ekv != null;
      edek = ekv;
    }
  }

  @VisibleForTesting
  synchronized void pauseForTesting() {
    shouldPauseForTesting = true;
    LOG.info("Pausing re-encrypt updater for testing.");
    notify();
  }

  @VisibleForTesting
  synchronized void resumeForTesting() {
    shouldPauseForTesting = false;
    LOG.info("Resuming re-encrypt updater for testing.");
    notify();
  }

  @VisibleForTesting
  void pauseForTestingAfterNthCheckpoint(final long zoneId, final int count) {
    assert pauseAfterNthCheckpoint == 0;
    pauseAfterNthCheckpoint = count;
    pauseZoneId = zoneId;
  }

  @VisibleForTesting
  boolean isRunning() {
    return isRunning;
  }

  private final FSDirectory dir;
  private final CompletionService<ReencryptionTask> batchService;
  private final ReencryptionHandler handler;

  ReencryptionUpdater(final FSDirectory fsd,
      final CompletionService<ReencryptionTask> service,
      final ReencryptionHandler rh, final Configuration conf) {
    dir = fsd;
    batchService = service;
    handler = rh;
    this.throttleLimitRatio =
        conf.getDouble(DFS_NAMENODE_REENCRYPT_THROTTLE_LIMIT_UPDATER_RATIO_KEY,
            DFS_NAMENODE_REENCRYPT_THROTTLE_LIMIT_UPDATER_RATIO_DEFAULT);
    Preconditions.checkArgument(throttleLimitRatio > 0.0f,
        DFS_NAMENODE_REENCRYPT_THROTTLE_LIMIT_UPDATER_RATIO_KEY
            + " is not positive.");
  }

  /**
   * Called by the submission thread to indicate all tasks have been submitted.
   * If this is called but no tasks has been submitted, the re-encryption is
   * considered complete.
   *
   * @param zoneId Id of the zone inode.
   * @throws IOException
   * @throws InterruptedException
   */
  void markZoneSubmissionDone(final long zoneId)
      throws IOException, InterruptedException {
    final ZoneSubmissionTracker tracker = handler.getTracker(zoneId);
    if (tracker != null && !tracker.getTasks().isEmpty()) {
      tracker.submissionDone = true;
    } else {
      // Caller thinks submission is done, but no tasks submitted - meaning
      // no files in the EZ need to be re-encrypted. Complete directly.
      handler.addDummyTracker(zoneId, tracker);
    }
  }

  @Override
  public void run() {
    isRunning = true;
    throttleTimerAll.start();
    while (true) {
      try {
        // Assuming single-threaded updater.
        takeAndProcessTasks();
      } catch (InterruptedException ie) {
        LOG.warn("Re-encryption updater thread interrupted. Exiting.");
        Thread.currentThread().interrupt();
        isRunning = false;
        return;
      } catch (IOException | CancellationException e) {
        LOG.warn("Re-encryption updater thread exception.", e);
      } catch (Throwable t) {
        LOG.error("Re-encryption updater thread exiting.", t);
        isRunning = false;
        return;
      }
    }
  }

  /**
   * Process a completed ReencryptionTask. Each inode id is resolved to an INode
   * object, skip if the inode is deleted.
   * <p>
   * Only file xattr is updated by this method. Re-encryption progress is not
   * updated.
   *
   * @param zoneNodePath full path of the EZ inode.
   * @param task     the completed task.
   * @throws IOException
   * @throws InterruptedException
   */
  private void processTaskEntries(final String zoneNodePath,
      final ReencryptionTask task) throws IOException, InterruptedException {
    assert dir.hasWriteLock();
    if (!task.batch.isEmpty() && task.numFailures == 0) {
      LOG.debug(
          "Updating file xattrs for re-encrypting zone {}," + " starting at {}",
          zoneNodePath, task.batch.getFirstFilePath());
      final int batchSize = task.batch.size();
      for (Iterator<FileEdekInfo> it = task.batch.getBatch().iterator();
           it.hasNext();) {
        FileEdekInfo entry = it.next();
        // resolve the inode again, and skip if it's doesn't exist
        LOG.trace("Updating {} for re-encryption.", entry.getInodeId());
        final INode inode = dir.getInode(entry.getInodeId());
        if (inode == null) {
          LOG.debug("INode {} doesn't exist, skipping re-encrypt.",
              entry.getInodeId());
          // also remove from batch so later it's not saved.
          it.remove();
          continue;
        }

        // Cautiously check file encryption info, and only update if we're sure
        // it's still using the same edek.
        Preconditions.checkNotNull(entry.edek);
        final FileEncryptionInfo fei = FSDirEncryptionZoneOp
            .getFileEncryptionInfo(dir, INodesInPath.fromINode(inode));
        if (!fei.getKeyName().equals(entry.edek.getEncryptionKeyName())) {
          LOG.debug("Inode {} EZ key changed, skipping re-encryption.",
              entry.getInodeId());
          it.remove();
          continue;
        }
        if (fei.getEzKeyVersionName()
            .equals(entry.edek.getEncryptionKeyVersionName())) {
          LOG.debug(
              "Inode {} EZ key version unchanged, skipping re-encryption.",
              entry.getInodeId());
          it.remove();
          continue;
        }
        if (!Arrays.equals(fei.getEncryptedDataEncryptionKey(),
            entry.existingEdek.getEncryptedKeyVersion().getMaterial())) {
          LOG.debug("Inode {} existing edek changed, skipping re-encryption",
              entry.getInodeId());
          it.remove();
          continue;
        }
        FileEncryptionInfo newFei = new FileEncryptionInfo(fei.getCipherSuite(),
            fei.getCryptoProtocolVersion(),
            entry.edek.getEncryptedKeyVersion().getMaterial(),
            entry.edek.getEncryptedKeyIv(), fei.getKeyName(),
            entry.edek.getEncryptionKeyVersionName());
        final INodesInPath iip = INodesInPath.fromINode(inode);
        FSDirEncryptionZoneOp
            .setFileEncryptionInfo(dir, iip, newFei, XAttrSetFlag.REPLACE);
        task.lastFile = iip.getPath();
        ++task.numFilesUpdated;
      }

      LOG.info("Updated xattrs on {}({}) files in zone {} for re-encryption,"
              + " starting:{}.", task.numFilesUpdated, batchSize,
          zoneNodePath, task.batch.getFirstFilePath());
    }
    task.processed = true;
  }

  /**
   * Iterate tasks for the given zone, and update progress accordingly. The
   * checkpoint indicates all files before it are done re-encryption, so it will
   * be updated to the position where all tasks before are completed.
   *
   * @param zoneNode the EZ inode.
   * @param tracker  the zone submission tracker.
   * @return the list containing the last checkpointed xattr. Empty if
   *   no checkpoint happened.
   * @throws ExecutionException
   * @throws IOException
   * @throws InterruptedException
   */
  private List<XAttr> processCheckpoints(final INode zoneNode,
      final ZoneSubmissionTracker tracker)
      throws ExecutionException, IOException, InterruptedException {
    assert dir.hasWriteLock();
    final long zoneId = zoneNode.getId();
    final String zonePath = zoneNode.getFullPathName();
    final ZoneReencryptionStatus status =
        handler.getReencryptionStatus().getZoneStatus(zoneId);
    assert status != null;
    // always start from the beginning, because the checkpoint means all files
    // before it are re-encrypted.
    final LinkedList<Future> tasks = tracker.getTasks();
    final List<XAttr> xAttrs = Lists.newArrayListWithCapacity(1);
    ListIterator<Future> iter = tasks.listIterator();
    synchronized (handler) {
      while (iter.hasNext()) {
        Future<ReencryptionTask> curr = iter.next();
        if (curr.isCancelled()) {
          break;
        }
        if (!curr.isDone() || !curr.get().processed) {
          // still has earlier tasks not completed, skip here.
          break;
        }
        ReencryptionTask task = curr.get();
        LOG.debug("Updating re-encryption checkpoint with completed task."
            + " last: {} size:{}.", task.lastFile, task.batch.size());
        assert zoneId == task.zoneId;
        try {
          final XAttr xattr = FSDirEncryptionZoneOp
              .updateReencryptionProgress(dir, zoneNode, status, task.lastFile,
                  task.numFilesUpdated, task.numFailures);
          xAttrs.clear();
          xAttrs.add(xattr);
        } catch (IOException ie) {
          LOG.warn("Failed to update re-encrypted progress to xattr" +
                  " for zone {}", zonePath, ie);
          ++task.numFailures;
        }
        ++tracker.numCheckpointed;
        iter.remove();
      }
    }
    if (tracker.isCompleted()) {
      LOG.debug("Removed re-encryption tracker for zone {} because it completed"
              + " with {} tasks.", zonePath, tracker.numCheckpointed);
      return handler.completeReencryption(zoneNode);
    }
    return xAttrs;
  }

  private void takeAndProcessTasks() throws Exception {
    final Future<ReencryptionTask> completed = batchService.take();
    throttle();
    checkPauseForTesting();
    if (completed.isCancelled()) {
      // Ignore canceled zones. The cancellation is edit-logged by the handler.
      LOG.debug("Skipped a canceled re-encryption task");
      return;
    }
    final ReencryptionTask task = completed.get();

    boolean shouldRetry;
    do {
      dir.getFSNamesystem().writeLock();
      try {
        throttleTimerLocked.start();
        processTask(task);
        shouldRetry = false;
      } catch (RetriableException | SafeModeException re) {
        // Keep retrying until succeed.
        LOG.info("Exception when processing re-encryption task for zone {}, "
                + "retrying...", task.zoneId, re);
        shouldRetry = true;
        Thread.sleep(faultRetryInterval);
      } catch (IOException ioe) {
        LOG.warn("Failure processing re-encryption task for zone {}",
            task.zoneId, ioe);
        ++task.numFailures;
        task.processed = true;
        shouldRetry = false;
      } finally {
        dir.getFSNamesystem().writeUnlock("reencryptUpdater");
        throttleTimerLocked.stop();
      }
      // logSync regardless, to prevent edit log buffer overflow triggering
      // logSync inside FSN writelock.
      dir.getEditLog().logSync();
    } while (shouldRetry);
  }

  private void processTask(ReencryptionTask task)
      throws InterruptedException, ExecutionException, IOException {
    final List<XAttr> xAttrs;
    final String zonePath;
    dir.writeLock();
    try {
      handler.getTraverser().checkINodeReady(task.zoneId);
      final INode zoneNode = dir.getInode(task.zoneId);
      if (zoneNode == null) {
        // ez removed.
        return;
      }
      zonePath = zoneNode.getFullPathName();
      LOG.info("Processing returned re-encryption task for zone {}({}), "
              + "batch size {}, start:{}", zonePath, task.zoneId,
          task.batch.size(), task.batch.getFirstFilePath());
      final ZoneSubmissionTracker tracker =
          handler.getTracker(zoneNode.getId());
      if (tracker == null) {
        // re-encryption canceled.
        LOG.info("Re-encryption was canceled.");
        return;
      }
      tracker.numFutureDone++;
      EncryptionFaultInjector.getInstance().reencryptUpdaterProcessOneTask();
      processTaskEntries(zonePath, task);
      EncryptionFaultInjector.getInstance().reencryptUpdaterProcessCheckpoint();
      xAttrs = processCheckpoints(zoneNode, tracker);
    } finally {
      dir.writeUnlock();
    }
    FSDirEncryptionZoneOp.saveFileXAttrsForBatch(dir, task.batch.getBatch());
    if (!xAttrs.isEmpty()) {
      dir.getEditLog().logSetXAttrs(zonePath, xAttrs, false);
    }
  }

  private synchronized void checkPauseForTesting() throws InterruptedException {
    assert !dir.hasWriteLock();
    assert !dir.getFSNamesystem().hasWriteLock();
    if (pauseAfterNthCheckpoint != 0) {
      ZoneSubmissionTracker tracker =
          handler.unprotectedGetTracker(pauseZoneId);
      if (tracker != null) {
        if (tracker.numFutureDone == pauseAfterNthCheckpoint) {
          shouldPauseForTesting = true;
          pauseAfterNthCheckpoint = 0;
        }
      }
    }
    while (shouldPauseForTesting) {
      LOG.info("Sleeping in the re-encryption updater for unit test.");
      wait();
      LOG.info("Continuing re-encryption updater after pausing.");
    }
  }

  /**
   * Throttles the ReencryptionUpdater to prevent from contending FSN/FSD write
   * locks. This is done by the configuration.
   */
  private void throttle() throws InterruptedException {
    if (throttleLimitRatio >= 1.0) {
      return;
    }

    final long expect = (long) (throttleTimerAll.now(TimeUnit.MILLISECONDS)
        * throttleLimitRatio);
    final long actual = throttleTimerLocked.now(TimeUnit.MILLISECONDS);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Re-encryption updater throttling expect: {}, actual: {},"
              + " throttleTimerAll:{}", expect, actual,
          throttleTimerAll.now(TimeUnit.MILLISECONDS));
    }
    if (expect - actual < 0) {
      // in case throttleLimitHandlerRatio is very small, expect will be 0.
      // so sleepMs should not be calculated from expect, to really meet the
      // ratio. e.g. if ratio is 0.001, expect = 0 and actual = 1, sleepMs
      // should be 1000 - throttleTimerAll.now()
      final long sleepMs =
          (long) (actual / throttleLimitRatio) - throttleTimerAll
              .now(TimeUnit.MILLISECONDS);
      LOG.debug("Throttling re-encryption, sleeping for {} ms", sleepMs);
      Thread.sleep(sleepMs);
    }
    throttleTimerAll.reset().start();
    throttleTimerLocked.reset();
  }
}