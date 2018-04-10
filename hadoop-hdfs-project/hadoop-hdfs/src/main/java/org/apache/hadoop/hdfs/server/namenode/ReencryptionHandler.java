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
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.ReencryptionStatus;
import org.apache.hadoop.hdfs.protocol.ZoneReencryptionStatus;
import org.apache.hadoop.hdfs.protocol.ZoneReencryptionStatus.State;
import org.apache.hadoop.hdfs.server.namenode.FSTreeTraverser.TraverseInfo;
import org.apache.hadoop.hdfs.server.namenode.ReencryptionUpdater.FileEdekInfo;
import org.apache.hadoop.hdfs.server.namenode.ReencryptionUpdater.ReencryptionTask;
import org.apache.hadoop.hdfs.server.namenode.ReencryptionUpdater.ZoneSubmissionTracker;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REENCRYPT_BATCH_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REENCRYPT_BATCH_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REENCRYPT_SLEEP_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REENCRYPT_SLEEP_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REENCRYPT_THROTTLE_LIMIT_HANDLER_RATIO_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REENCRYPT_THROTTLE_LIMIT_HANDLER_RATIO_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REENCRYPT_EDEK_THREADS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REENCRYPT_EDEK_THREADS_KEY;

/**
 * Class for handling re-encrypt EDEK operations.
 * <p>
 * For each EZ, ReencryptionHandler walks the tree in a depth-first order,
 * and submits batches of (files + existing edeks) as re-encryption tasks
 * to a thread pool. Each thread in the pool then contacts the KMS to
 * re-encrypt the edeks. ReencryptionUpdater tracks the tasks and updates
 * file xattrs with the new edeks.
 * <p>
 * File renames are disabled in the EZ that's being re-encrypted. Newly created
 * files will have new edeks, because the edek cache is drained upon the
 * submission of a re-encryption command.
 * <p>
 * It is assumed only 1 ReencryptionHandler will be running, because:
 *   1. The bottleneck of the entire re-encryption appears to be on the KMS.
 *   2. Even with multiple handlers, since updater requires writelock and is
 * single-threaded, the performance gain is limited.
 * <p>
 * This class uses the FSDirectory lock for synchronization.
 */
@InterfaceAudience.Private
public class ReencryptionHandler implements Runnable {

  public static final Logger LOG =
      LoggerFactory.getLogger(ReencryptionHandler.class);

  // 2000 is based on buffer size = 512 * 1024, and SetXAttr op size is
  // 100 - 200 bytes (depending on the xattr value).
  // The buffer size is hard-coded, see outputBufferCapacity from QJM.
  private static final int MAX_BATCH_SIZE_WITHOUT_FLOODING = 2000;

  private final EncryptionZoneManager ezManager;
  private final FSDirectory dir;
  private final long interval;
  private final int reencryptBatchSize;
  private double throttleLimitHandlerRatio;
  private final int reencryptThreadPoolSize;
  // stopwatches for throttling
  private final StopWatch throttleTimerAll = new StopWatch();
  private final StopWatch throttleTimerLocked = new StopWatch();

  private ExecutorCompletionService<ReencryptionTask> batchService;
  private BlockingQueue<Runnable> taskQueue;
  // protected by ReencryptionHandler object lock
  private final Map<Long, ZoneSubmissionTracker> submissions = new HashMap<>();

  // The current batch that the handler is working on. Handler is designed to
  // be single-threaded, see class javadoc for more details.
  private ReencryptionBatch currentBatch;

  private final ReencryptionPendingInodeIdCollector traverser;

  private final ReencryptionUpdater reencryptionUpdater;
  private ExecutorService updaterExecutor;

  // Vars for unit tests.
  private volatile boolean shouldPauseForTesting = false;
  private volatile int pauseAfterNthSubmission = 0;

  /**
   * Stop the re-encryption updater thread, as well as all EDEK re-encryption
   * tasks submitted.
   */
  void stopThreads() {
    assert dir.hasWriteLock();
    synchronized (this) {
      for (ZoneSubmissionTracker zst : submissions.values()) {
        zst.cancelAllTasks();
      }
    }
    if (updaterExecutor != null) {
      updaterExecutor.shutdownNow();
    }
  }

  /**
   * Start the re-encryption updater thread.
   */
  void startUpdaterThread() {
    updaterExecutor = Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat("reencryptionUpdaterThread #%d").build());
    updaterExecutor.execute(reencryptionUpdater);
  }

  @VisibleForTesting
  synchronized void pauseForTesting() {
    shouldPauseForTesting = true;
    LOG.info("Pausing re-encrypt handler for testing.");
    notify();
  }

  @VisibleForTesting
  synchronized void resumeForTesting() {
    shouldPauseForTesting = false;
    LOG.info("Resuming re-encrypt handler for testing.");
    notify();
  }

  @VisibleForTesting
  void pauseForTestingAfterNthSubmission(final int count) {
    assert pauseAfterNthSubmission == 0;
    pauseAfterNthSubmission = count;
  }

  @VisibleForTesting
  void pauseUpdaterForTesting() {
    reencryptionUpdater.pauseForTesting();
  }

  @VisibleForTesting
  void resumeUpdaterForTesting() {
    reencryptionUpdater.resumeForTesting();
  }

  @VisibleForTesting
  void pauseForTestingAfterNthCheckpoint(final long zoneId, final int count) {
    reencryptionUpdater.pauseForTestingAfterNthCheckpoint(zoneId, count);
  }

  ReencryptionHandler(final EncryptionZoneManager ezMgr,
      final Configuration conf) {
    this.ezManager = ezMgr;
    Preconditions.checkNotNull(ezManager.getProvider(),
        "No provider set, cannot re-encrypt");
    this.dir = ezMgr.getFSDirectory();
    this.interval =
        conf.getTimeDuration(DFS_NAMENODE_REENCRYPT_SLEEP_INTERVAL_KEY,
            DFS_NAMENODE_REENCRYPT_SLEEP_INTERVAL_DEFAULT,
            TimeUnit.MILLISECONDS);
    Preconditions.checkArgument(interval > 0,
        DFS_NAMENODE_REENCRYPT_SLEEP_INTERVAL_KEY + " is not positive.");
    this.reencryptBatchSize = conf.getInt(DFS_NAMENODE_REENCRYPT_BATCH_SIZE_KEY,
        DFS_NAMENODE_REENCRYPT_BATCH_SIZE_DEFAULT);
    Preconditions.checkArgument(reencryptBatchSize > 0,
        DFS_NAMENODE_REENCRYPT_BATCH_SIZE_KEY + " is not positive.");
    if (reencryptBatchSize > MAX_BATCH_SIZE_WITHOUT_FLOODING) {
      LOG.warn("Re-encryption batch size is {}. It could cause edit log buffer "
          + "to be full and trigger a logSync within the writelock, greatly "
          + "impacting namenode throughput.", reencryptBatchSize);
    }
    this.throttleLimitHandlerRatio =
        conf.getDouble(DFS_NAMENODE_REENCRYPT_THROTTLE_LIMIT_HANDLER_RATIO_KEY,
            DFS_NAMENODE_REENCRYPT_THROTTLE_LIMIT_HANDLER_RATIO_DEFAULT);
    LOG.info("Configured throttleLimitHandlerRatio={} for re-encryption",
        throttleLimitHandlerRatio);
    Preconditions.checkArgument(throttleLimitHandlerRatio > 0.0f,
        DFS_NAMENODE_REENCRYPT_THROTTLE_LIMIT_HANDLER_RATIO_KEY
            + " is not positive.");
    this.reencryptThreadPoolSize =
        conf.getInt(DFS_NAMENODE_REENCRYPT_EDEK_THREADS_KEY,
            DFS_NAMENODE_REENCRYPT_EDEK_THREADS_DEFAULT);

    taskQueue = new LinkedBlockingQueue<>();
    ThreadPoolExecutor threadPool =
        new ThreadPoolExecutor(reencryptThreadPoolSize, reencryptThreadPoolSize,
            60, TimeUnit.SECONDS, taskQueue, new Daemon.DaemonFactory() {
              private final AtomicInteger ind = new AtomicInteger(0);

              @Override
              public Thread newThread(Runnable r) {
                Thread t = super.newThread(r);
                t.setName("reencryption edek Thread-" + ind.getAndIncrement());
                return t;
              }
            }, new ThreadPoolExecutor.CallerRunsPolicy() {

              @Override
              public void rejectedExecution(Runnable runnable,
                  ThreadPoolExecutor e) {
                LOG.info("Execution rejected, executing in current thread");
                super.rejectedExecution(runnable, e);
              }
            });

    threadPool.allowCoreThreadTimeOut(true);
    this.batchService = new ExecutorCompletionService(threadPool);
    reencryptionUpdater =
        new ReencryptionUpdater(dir, batchService, this, conf);
    currentBatch = new ReencryptionBatch(reencryptBatchSize);
    traverser = new ReencryptionPendingInodeIdCollector(dir, this, conf);
  }

  ReencryptionStatus getReencryptionStatus() {
    return ezManager.getReencryptionStatus();
  }

  void cancelZone(final long zoneId, final String zoneName) throws IOException {
    assert dir.hasWriteLock();
    final ZoneReencryptionStatus zs =
        getReencryptionStatus().getZoneStatus(zoneId);
    if (zs == null || zs.getState() == State.Completed) {
      throw new IOException("Zone " + zoneName + " is not under re-encryption");
    }
    zs.cancel();
    removeZoneTrackerStopTasks(zoneId);
  }

  void removeZone(final long zoneId) {
    assert dir.hasWriteLock();
    LOG.info("Removing zone {} from re-encryption.", zoneId);
    removeZoneTrackerStopTasks(zoneId);
    getReencryptionStatus().removeZone(zoneId);
  }

  synchronized private void removeZoneTrackerStopTasks(final long zoneId) {
    final ZoneSubmissionTracker zst = submissions.get(zoneId);
    if (zst != null) {
      zst.cancelAllTasks();
      submissions.remove(zoneId);
    }
  }

  ZoneSubmissionTracker getTracker(final long zoneId) {
    assert dir.hasReadLock();
    return unprotectedGetTracker(zoneId);
  }

  /**
   * Get the tracker without holding the FSDirectory lock.
   * The submissions object is protected by object lock.
   */
  synchronized ZoneSubmissionTracker unprotectedGetTracker(final long zoneId) {
    return submissions.get(zoneId);
  }

  /**
   * Add a dummy tracker (with 1 task that has 0 files to re-encrypt)
   * for the zone. This is necessary to complete the re-encryption in case
   * no file in the entire zone needs re-encryption at all. We cannot simply
   * update zone status and set zone xattrs, because in the handler we only hold
   * readlock, and setting xattrs requires upgrading to a writelock.
   *
   * @param zoneId
   */
  void addDummyTracker(final long zoneId, ZoneSubmissionTracker zst) {
    assert dir.hasReadLock();
    if (zst == null) {
      zst = new ZoneSubmissionTracker();
    }
    zst.setSubmissionDone();

    final Future future = batchService.submit(
        new EDEKReencryptCallable(zoneId, new ReencryptionBatch(), this));
    zst.addTask(future);
    synchronized (this) {
      submissions.put(zoneId, zst);
    }
  }

  /**
   * Main loop. It takes at most 1 zone per scan, and executes until the zone
   * is completed.
   * {@see #reencryptEncryptionZoneInt(Long)}.
   */
  @Override
  public void run() {
    LOG.info("Starting up re-encrypt thread with interval={} millisecond.",
        interval);
    while (true) {
      try {
        synchronized (this) {
          wait(interval);
        }
        traverser.checkPauseForTesting();
      } catch (InterruptedException ie) {
        LOG.info("Re-encrypt handler interrupted. Exiting");
        Thread.currentThread().interrupt();
        return;
      }

      final Long zoneId;
      dir.readLock();
      try {
        zoneId = getReencryptionStatus().getNextUnprocessedZone();
        if (zoneId == null) {
          // empty queue.
          continue;
        }
        LOG.info("Executing re-encrypt commands on zone {}. Current zones:{}",
            zoneId, getReencryptionStatus());
        getReencryptionStatus().markZoneStarted(zoneId);
        resetSubmissionTracker(zoneId);
      } finally {
        dir.readUnlock();
      }

      try {
        reencryptEncryptionZone(zoneId);
      } catch (RetriableException | SafeModeException re) {
        LOG.info("Re-encryption caught exception, will retry", re);
        getReencryptionStatus().markZoneForRetry(zoneId);
      } catch (IOException ioe) {
        LOG.warn("IOException caught when re-encrypting zone {}", zoneId, ioe);
      } catch (InterruptedException ie) {
        LOG.info("Re-encrypt handler interrupted. Exiting.");
        Thread.currentThread().interrupt();
        return;
      } catch (Throwable t) {
        LOG.error("Re-encrypt handler thread exiting. Exception caught when"
            + " re-encrypting zone {}.", zoneId, t);
        return;
      }
    }
  }

  /**
   * Re-encrypts a zone by recursively iterating all paths inside the zone,
   * in lexicographic order.
   * Files are re-encrypted, and subdirs are processed during iteration.
   *
   * @param zoneId the Zone's id.
   * @throws IOException
   * @throws InterruptedException
   */
  void reencryptEncryptionZone(final long zoneId)
      throws IOException, InterruptedException {
    throttleTimerAll.reset().start();
    throttleTimerLocked.reset();
    final INode zoneNode;
    final ZoneReencryptionStatus zs;

    traverser.readLock();
    try {
      zoneNode = dir.getInode(zoneId);
      // start re-encrypting the zone from the beginning
      if (zoneNode == null) {
        LOG.info("Directory with id {} removed during re-encrypt, skipping",
            zoneId);
        return;
      }
      if (!zoneNode.isDirectory()) {
        LOG.info("Cannot re-encrypt directory with id {} because it's not a"
            + " directory.", zoneId);
        return;
      }

      zs = getReencryptionStatus().getZoneStatus(zoneId);
      assert zs != null;
      // Only costly log FullPathName here once, and use id elsewhere.
      LOG.info("Re-encrypting zone {}(id={})", zoneNode.getFullPathName(),
          zoneId);
      if (zs.getLastCheckpointFile() == null) {
        // new re-encryption
        traverser.traverseDir(zoneNode.asDirectory(), zoneId,
            HdfsFileStatus.EMPTY_NAME,
            new ZoneTraverseInfo(zs.getEzKeyVersionName()));
      } else {
        // resuming from a past re-encryption
        restoreFromLastProcessedFile(zoneId, zs);
      }
      // save the last batch and mark complete
      traverser.submitCurrentBatch(zoneId);
      LOG.info("Submission completed of zone {} for re-encryption.", zoneId);
      reencryptionUpdater.markZoneSubmissionDone(zoneId);
    } finally {
      traverser.readUnlock();
    }
  }

  /**
   * Reset the zone submission tracker for re-encryption.
   * @param zoneId
   */
  synchronized private void resetSubmissionTracker(final long zoneId) {
    ZoneSubmissionTracker zst = submissions.get(zoneId);
    if (zst == null) {
      zst = new ZoneSubmissionTracker();
      submissions.put(zoneId, zst);
    } else {
      zst.reset();
    }
  }

  List<XAttr> completeReencryption(final INode zoneNode) throws IOException {
    assert dir.hasWriteLock();
    assert dir.getFSNamesystem().hasWriteLock();
    final Long zoneId = zoneNode.getId();
    ZoneReencryptionStatus zs = getReencryptionStatus().getZoneStatus(zoneId);
    assert zs != null;
    LOG.info("Re-encryption completed on zone {}. Re-encrypted {} files,"
            + " failures encountered: {}.", zoneNode.getFullPathName(),
        zs.getFilesReencrypted(), zs.getNumReencryptionFailures());
    synchronized (this) {
      submissions.remove(zoneId);
    }
    return FSDirEncryptionZoneOp
        .updateReencryptionFinish(dir, INodesInPath.fromINode(zoneNode), zs);
  }

  /**
   * Restore the re-encryption from the progress inside ReencryptionStatus.
   * This means start from exactly the lastProcessedFile (LPF), skipping all
   * earlier paths in lexicographic order. Lexicographically-later directories
   * on the LPF parent paths are added to subdirs.
   */
  private void restoreFromLastProcessedFile(final long zoneId,
      final ZoneReencryptionStatus zs)
      throws IOException, InterruptedException {
    final INodeDirectory parent;
    final byte[] startAfter;
    final INodesInPath lpfIIP =
        dir.getINodesInPath(zs.getLastCheckpointFile(), FSDirectory.DirOp.READ);
    parent = lpfIIP.getLastINode().getParent();
    startAfter = lpfIIP.getLastINode().getLocalNameBytes();
    traverser.traverseDir(parent, zoneId, startAfter,
        new ZoneTraverseInfo(zs.getEzKeyVersionName()));
  }

  final class ReencryptionBatch {
    // First file's path, for logging purpose.
    private String firstFilePath;
    private final List<FileEdekInfo> batch;

    ReencryptionBatch() {
      this(reencryptBatchSize);
    }

    ReencryptionBatch(int initialCapacity) {
      batch = new ArrayList<>(initialCapacity);
    }

    void add(final INodeFile inode) throws IOException {
      assert dir.hasReadLock();
      Preconditions.checkNotNull(inode, "INodeFile is null");
      if (batch.isEmpty()) {
        firstFilePath = inode.getFullPathName();
      }
      batch.add(new FileEdekInfo(dir, inode));
    }

    String getFirstFilePath() {
      return firstFilePath;
    }

    boolean isEmpty() {
      return batch.isEmpty();
    }

    int size() {
      return batch.size();
    }

    void clear() {
      batch.clear();
    }

    List<FileEdekInfo> getBatch() {
      return batch;
    }
  }

  /**
   * Simply contacts the KMS for re-encryption. No NN locks held.
   */
  private static class EDEKReencryptCallable
      implements Callable<ReencryptionTask> {
    private final long zoneNodeId;
    private final ReencryptionBatch batch;
    private final ReencryptionHandler handler;

    EDEKReencryptCallable(final long zoneId,
        final ReencryptionBatch currentBatch, final ReencryptionHandler rh) {
      zoneNodeId = zoneId;
      batch = currentBatch;
      handler = rh;
    }

    @Override
    public ReencryptionTask call() {
      LOG.info("Processing batched re-encryption for zone {}, batch size {},"
          + " start:{}", zoneNodeId, batch.size(), batch.getFirstFilePath());
      if (batch.isEmpty()) {
        return new ReencryptionTask(zoneNodeId, 0, batch);
      }
      final Stopwatch kmsSW = new Stopwatch().start();

      int numFailures = 0;
      String result = "Completed";
      if (!reencryptEdeks()) {
        numFailures += batch.size();
        result = "Failed to";
      }
      LOG.info("{} re-encrypting one batch of {} edeks from KMS,"
              + " time consumed: {}, start: {}.", result,
          batch.size(), kmsSW.stop(), batch.getFirstFilePath());
      return new ReencryptionTask(zoneNodeId, numFailures, batch);
    }

    private boolean reencryptEdeks() {
      // communicate with the kms out of lock
      final List<EncryptedKeyVersion> edeks = new ArrayList<>(batch.size());
      for (FileEdekInfo entry : batch.getBatch()) {
        edeks.add(entry.getExistingEdek());
      }
      // provider already has LoadBalancingKMSClientProvider's reties. It that
      // fails, just fail this callable.
      try {
        handler.ezManager.getProvider().reencryptEncryptedKeys(edeks);
        EncryptionFaultInjector.getInstance().reencryptEncryptedKeys();
      } catch (GeneralSecurityException | IOException ex) {
        LOG.warn("Failed to re-encrypt one batch of {} edeks, start:{}",
            batch.size(), batch.getFirstFilePath(), ex);
        return false;
      }
      int i = 0;
      for (FileEdekInfo entry : batch.getBatch()) {
        assert i < edeks.size();
        entry.setEdek(edeks.get(i++));
      }
      return true;
    }
  }


  /**
   * Called when a new zone is submitted for re-encryption. This will interrupt
   * the background thread if it's waiting for the next
   * DFS_NAMENODE_REENCRYPT_SLEEP_INTERVAL_KEY.
   */
  synchronized void notifyNewSubmission() {
    LOG.debug("Notifying handler for new re-encryption command.");
    this.notify();
  }

  public ReencryptionPendingInodeIdCollector getTraverser() {
    return traverser;
  }

  /**
   * ReencryptionPendingInodeIdCollector which throttle based on configured
   * throttle ratio.
   */
  class ReencryptionPendingInodeIdCollector extends FSTreeTraverser {

    private final ReencryptionHandler reencryptionHandler;

    ReencryptionPendingInodeIdCollector(FSDirectory dir,
        ReencryptionHandler rHandler, Configuration conf) {
      super(dir, conf);
      this.reencryptionHandler = rHandler;
    }

    @Override
    protected void checkPauseForTesting()
        throws InterruptedException {
      assert !dir.hasReadLock();
      assert !dir.getFSNamesystem().hasReadLock();
      while (shouldPauseForTesting) {
        LOG.info("Sleeping in the re-encrypt handler for unit test.");
        synchronized (reencryptionHandler) {
          reencryptionHandler.wait(30000);
        }
        LOG.info("Continuing re-encrypt handler after pausing.");
      }
    }

    /**
     * Process an Inode for re-encryption. Add to current batch if it's a file,
     * no-op otherwise.
     *
     * @param inode
     *          the inode
     * @return true if inode is added to currentBatch and should be
     *         re-encrypted. false otherwise: could be inode is not a file, or
     *         inode's edek's key version is not changed.
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean processFileInode(INode inode, TraverseInfo traverseInfo)
        throws IOException, InterruptedException {
      assert dir.hasReadLock();
      if (LOG.isTraceEnabled()) {
        LOG.trace("Processing {} for re-encryption", inode.getFullPathName());
      }
      if (!inode.isFile()) {
        return false;
      }
      FileEncryptionInfo feInfo = FSDirEncryptionZoneOp.getFileEncryptionInfo(
          dir, INodesInPath.fromINode(inode));
      if (feInfo == null) {
        LOG.warn("File {} skipped re-encryption because it is not encrypted! "
            + "This is very likely a bug.", inode.getId());
        return false;
      }
      if (traverseInfo instanceof ZoneTraverseInfo
          && ((ZoneTraverseInfo) traverseInfo).getEzKeyVerName().equals(
              feInfo.getEzKeyVersionName())) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("File {} skipped re-encryption because edek's key version"
              + " name is not changed.", inode.getFullPathName());
        }
        return false;
      }
      currentBatch.add(inode.asFile());
      return true;
    }

    /**
     * Check whether zone is ready for re-encryption. Throws IOE if it's not. 1.
     * If EZ is deleted. 2. if the re-encryption is canceled. 3. If NN is not
     * active or is in safe mode.
     *
     * @throws IOException
     *           if zone does not exist / is cancelled, or if NN is not ready
     *           for write.
     */
    @Override
    protected void checkINodeReady(long zoneId) throws IOException {
      final ZoneReencryptionStatus zs = getReencryptionStatus().getZoneStatus(
          zoneId);
      if (zs == null) {
        throw new IOException("Zone " + zoneId + " status cannot be found.");
      }
      if (zs.isCanceled()) {
        throw new IOException("Re-encryption is canceled for zone " + zoneId);
      }
      dir.getFSNamesystem().checkNameNodeSafeMode(
          "NN is in safe mode, cannot re-encrypt.");
      // re-encryption should be cancelled when NN goes to standby. Just
      // double checking for sanity.
      dir.getFSNamesystem().checkOperation(NameNode.OperationCategory.WRITE);
    }

    /**
     * Submit the current batch to the thread pool.
     *
     * @param zoneId
     *          Id of the EZ INode
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void submitCurrentBatch(final long zoneId) throws IOException,
        InterruptedException {
      if (currentBatch.isEmpty()) {
        return;
      }
      ZoneSubmissionTracker zst;
      synchronized (ReencryptionHandler.this) {
        zst = submissions.get(zoneId);
        if (zst == null) {
          zst = new ZoneSubmissionTracker();
          submissions.put(zoneId, zst);
        }
      }
      Future future = batchService.submit(new EDEKReencryptCallable(zoneId,
          currentBatch, reencryptionHandler));
      zst.addTask(future);
      LOG.info("Submitted batch (start:{}, size:{}) of zone {} to re-encrypt.",
          currentBatch.getFirstFilePath(), currentBatch.size(), zoneId);
      currentBatch = new ReencryptionBatch(reencryptBatchSize);
      // flip the pause flag if this is nth submission.
      // The actual pause need to happen outside of the lock.
      if (pauseAfterNthSubmission > 0) {
        if (--pauseAfterNthSubmission == 0) {
          shouldPauseForTesting = true;
        }
      }
    }

    /**
     * Throttles the ReencryptionHandler in 3 aspects:
     * 1. Prevents generating more Callables than the CPU could possibly
     * handle.
     * 2. Prevents generating more Callables than the ReencryptionUpdater
     * can handle, under its own throttling.
     * 3. Prevents contending FSN/FSD read locks. This is done based
     * on the DFS_NAMENODE_REENCRYPT_THROTTLE_LIMIT_RATIO_KEY configuration.
     * <p>
     * Item 1 and 2 are to control NN heap usage.
     *
     * @throws InterruptedException
     */
    @VisibleForTesting
    @Override
    protected void throttle() throws InterruptedException {
      assert !dir.hasReadLock();
      assert !dir.getFSNamesystem().hasReadLock();
      final int numCores = Runtime.getRuntime().availableProcessors();
      if (taskQueue.size() >= numCores) {
        LOG.debug("Re-encryption handler throttling because queue size {} is"
            + "larger than number of cores {}", taskQueue.size(), numCores);
        while (taskQueue.size() >= numCores) {
          Thread.sleep(100);
        }
      }

      // 2. if tasks are piling up on the updater, don't create new callables
      // until the queue size goes down.
      final int maxTasksPiled = Runtime.getRuntime().availableProcessors() * 2;
      int numTasks = numTasksSubmitted();
      if (numTasks >= maxTasksPiled) {
        LOG.debug("Re-encryption handler throttling because total tasks pending"
            + " re-encryption updater is {}", numTasks);
        while (numTasks >= maxTasksPiled) {
          Thread.sleep(500);
          numTasks = numTasksSubmitted();
        }
      }

      // 3.
      if (throttleLimitHandlerRatio >= 1.0) {
        return;
      }
      final long expect = (long) (throttleTimerAll.now(TimeUnit.MILLISECONDS)
          * throttleLimitHandlerRatio);
      final long actual = throttleTimerLocked.now(TimeUnit.MILLISECONDS);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Re-encryption handler throttling expect: {}, actual: {},"
            + " throttleTimerAll:{}", expect, actual,
            throttleTimerAll.now(TimeUnit.MILLISECONDS));
      }
      if (expect - actual < 0) {
        // in case throttleLimitHandlerRatio is very small, expect will be 0.
        // so sleepMs should not be calculated from expect, to really meet the
        // ratio. e.g. if ratio is 0.001, expect = 0 and actual = 1, sleepMs
        // should be 1000 - throttleTimerAll.now()
        final long sleepMs = (long) (actual / throttleLimitHandlerRatio)
            - throttleTimerAll.now(TimeUnit.MILLISECONDS);
        LOG.debug("Throttling re-encryption, sleeping for {} ms", sleepMs);
        Thread.sleep(sleepMs);
      }
      throttleTimerAll.reset().start();
      throttleTimerLocked.reset();
    }

    private int numTasksSubmitted() {
      int ret = 0;
      synchronized (ReencryptionHandler.this) {
        for (ZoneSubmissionTracker zst : submissions.values()) {
          ret += zst.getTasks().size();
        }
      }
      return ret;
    }

    @Override
    public boolean shouldSubmitCurrentBatch() {
      return currentBatch.size() >= reencryptBatchSize;
    }

    @Override
    public boolean canTraverseDir(INode inode) throws IOException {
      if (ezManager.isEncryptionZoneRoot(inode, inode.getFullPathName())) {
        // nested EZ, ignore.
        LOG.info("{}({}) is a nested EZ, skipping for re-encryption",
            inode.getFullPathName(), inode.getId());
        return false;
      }
      return true;
    }

    @Override
    protected void readLock() {
      super.readLock();
      throttleTimerLocked.start();
    }

    @Override
    protected void readUnlock() {
      super.readUnlock();
      throttleTimerLocked.stop();
    }
  }

  private class ZoneTraverseInfo extends TraverseInfo {
    private String ezKeyVerName;

    ZoneTraverseInfo(String ezKeyVerName) {
      this.ezKeyVerName = ezKeyVerName;
    }

    public String getEzKeyVerName() {
      return ezKeyVerName;
    }
  }
}
