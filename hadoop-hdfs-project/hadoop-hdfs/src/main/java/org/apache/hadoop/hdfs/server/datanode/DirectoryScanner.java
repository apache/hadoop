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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi.ScanInfo;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.hadoop.thirdparty.com.google.common.collect.ListMultimap;

/**
 * Periodically scans the data directories for block and block metadata files.
 * Reconciles the differences with block information maintained in the dataset.
 */
@InterfaceAudience.Private
public class DirectoryScanner implements Runnable {
  private static final Logger LOG =
      LoggerFactory.getLogger(DirectoryScanner.class);

  private static final int DEFAULT_MAP_SIZE = 32768;
  private static final int RECONCILE_BLOCKS_BATCH_SIZE = 1000;
  private final FsDatasetSpi<?> dataset;
  private final ExecutorService reportCompileThreadPool;
  private final ScheduledExecutorService masterThread;
  private final long scanPeriodMsecs;
  private final long throttleLimitMsPerSec;
  private final AtomicBoolean shouldRun = new AtomicBoolean();

  private boolean retainDiffs = false;

  /**
   * Total combined wall clock time (in milliseconds) spent by the report
   * compiler threads executing. Used for testing purposes.
   */
  @VisibleForTesting
  final AtomicLong timeRunningMs = new AtomicLong(0L);

  /**
   * Total combined wall clock time (in milliseconds) spent by the report
   * compiler threads blocked by the throttle. Used for testing purposes.
   */
  @VisibleForTesting
  final AtomicLong timeWaitingMs = new AtomicLong(0L);

  /**
   * The complete list of block differences indexed by block pool ID.
   */
  @VisibleForTesting
  final BlockPoolReport diffs = new BlockPoolReport();

  /**
   * Statistics about the block differences in each blockpool, indexed by block
   * pool ID.
   */
  @VisibleForTesting
  final Map<String, Stats> stats;

  /**
   * Allow retaining diffs for unit test and analysis. Defaults to false (off).
   *
   * @param b whether to retain diffs
   */
  @VisibleForTesting
  public void setRetainDiffs(boolean b) {
    retainDiffs = b;
  }

  /**
   * Stats tracked for reporting and testing, per blockpool
   */
  @VisibleForTesting
  static class Stats {
    final String bpid;
    long totalBlocks = 0;
    long missingMetaFile = 0;
    long missingBlockFile = 0;
    long missingMemoryBlocks = 0;
    long mismatchBlocks = 0;
    long duplicateBlocks = 0;

    /**
     * Create a new Stats object for the given blockpool ID.
     *
     * @param bpid blockpool ID
     */
    public Stats(String bpid) {
      this.bpid = bpid;
    }

    @Override
    public String toString() {
      return "BlockPool " + bpid + " Total blocks: " + totalBlocks
          + ", missing metadata files: " + missingMetaFile
          + ", missing block files: " + missingBlockFile
          + ", missing blocks in memory: " + missingMemoryBlocks
          + ", mismatched blocks: " + mismatchBlocks;
    }
  }

  /**
   * Helper class for compiling block info reports from report compiler threads.
   * Contains a volume, a set of block pool IDs, and a collection of ScanInfo
   * objects. If a block pool exists but has no ScanInfo objects associated with
   * it, there will be no mapping for that particular block pool.
   */
  @VisibleForTesting
  public static class ScanInfoVolumeReport {

    @SuppressWarnings("unused")
    private static final long serialVersionUID = 1L;

    private final FsVolumeSpi volume;

    private final BlockPoolReport blockPoolReport;

    /**
     * Create a new info list.
     *
     * @param volume
     */
    ScanInfoVolumeReport(final FsVolumeSpi volume) {
      this.volume = volume;
      this.blockPoolReport = new BlockPoolReport();
    }

    /**
     * Create a new info list initialized to the given expected size.
     *
     * @param sz initial expected size
     */
    ScanInfoVolumeReport(final FsVolumeSpi volume,
        final Collection<String> blockPools) {
      this.volume = volume;
      this.blockPoolReport = new BlockPoolReport(blockPools);
    }

    public void addAll(final String bpid,
        final Collection<ScanInfo> scanInfos) {
      this.blockPoolReport.addAll(bpid, scanInfos);
    }

    public Set<String> getBlockPoolIds() {
      return this.blockPoolReport.getBlockPoolIds();
    }

    public List<ScanInfo> getScanInfo(final String bpid) {
      return this.blockPoolReport.getScanInfo(bpid);
    }

    public FsVolumeSpi getVolume() {
      return volume;
    }

    @Override
    public String toString() {
      return "ScanInfoVolumeReport [volume=" + volume + ", blockPoolReport="
          + blockPoolReport + "]";
    }
  }

  /**
   * Helper class for compiling block info reports per block pool.
   */
  @VisibleForTesting
  public static class BlockPoolReport {

    @SuppressWarnings("unused")
    private static final long serialVersionUID = 1L;

    private final Set<String> blockPools;

    private final ListMultimap<String, ScanInfo> map;

    /**
     * Create a block pool report.
     *
     * @param volume
     */
    BlockPoolReport() {
      this.blockPools = new HashSet<>(2);
      this.map = ArrayListMultimap.create(2, DEFAULT_MAP_SIZE);
    }

    /**
     * Create a new block pool report initialized to the given expected size.
     *
     * @param blockPools initial list of known block pools
     */
    BlockPoolReport(final Collection<String> blockPools) {
      this.blockPools = new HashSet<>(blockPools);
      this.map = ArrayListMultimap.create(blockPools.size(), DEFAULT_MAP_SIZE);

    }

    public void addAll(final String bpid,
        final Collection<ScanInfo> scanInfos) {
      this.blockPools.add(bpid);
      this.map.putAll(bpid, scanInfos);
    }

    public void sortBlocks() {
      for (final String bpid : this.map.keySet()) {
        final List<ScanInfo> list = this.map.get(bpid);
        // Sort array based on blockId
        Collections.sort(list);
      }
    }

    public Set<String> getBlockPoolIds() {
      return Collections.unmodifiableSet(this.blockPools);
    }

    public List<ScanInfo> getScanInfo(final String bpid) {
      return this.map.get(bpid);
    }

    public Collection<Map.Entry<String, ScanInfo>> getEntries() {
      return Collections.unmodifiableCollection(this.map.entries());
    }

    public void clear() {
      this.map.clear();
      this.blockPools.clear();
    }

    @Override
    public String toString() {
      return "BlockPoolReport [blockPools=" + blockPools + ", map=" + map + "]";
    }
  }

  /**
   * Create a new directory scanner, but don't cycle it running yet.
   *
   * @param dataset the dataset to scan
   * @param conf the Configuration object
   */
  public DirectoryScanner(FsDatasetSpi<?> dataset, Configuration conf) {
    this.dataset = dataset;
    this.stats = new HashMap<>(DEFAULT_MAP_SIZE);
    int interval = (int) conf.getTimeDuration(
        DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY,
        DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_DEFAULT,
        TimeUnit.SECONDS);

    scanPeriodMsecs = TimeUnit.SECONDS.toMillis(interval);

    int throttle = conf.getInt(
        DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THROTTLE_LIMIT_MS_PER_SEC_KEY,
        DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THROTTLE_LIMIT_MS_PER_SEC_DEFAULT);

    if (throttle >= TimeUnit.SECONDS.toMillis(1)) {
      LOG.warn(
          "{} set to value above 1000 ms/sec. Assuming default value of {}",
          DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THROTTLE_LIMIT_MS_PER_SEC_KEY,
          DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THROTTLE_LIMIT_MS_PER_SEC_DEFAULT);
      throttle =
          DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THROTTLE_LIMIT_MS_PER_SEC_DEFAULT;
    }

    throttleLimitMsPerSec = throttle;

    int threads =
        conf.getInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY,
            DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_DEFAULT);

    reportCompileThreadPool =
        Executors.newFixedThreadPool(threads, new Daemon.DaemonFactory());

    masterThread =
        new ScheduledThreadPoolExecutor(1, new Daemon.DaemonFactory());
  }

  /**
   * Start the scanner. The scanner will run every
   * {@link DFSConfigKeys#DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY} seconds.
   */
  void start() {
    shouldRun.set(true);
    long firstScanTime = ThreadLocalRandom.current().nextLong(scanPeriodMsecs);

    LOG.info(
        "Periodic Directory Tree Verification scan starting in {}ms with interval of {}ms and throttle limit of {}ms/s",
        firstScanTime, scanPeriodMsecs, throttleLimitMsPerSec);

    masterThread.scheduleAtFixedRate(this, firstScanTime, scanPeriodMsecs,
        TimeUnit.MILLISECONDS);
  }

  /**
   * Return whether the scanner has been started.
   *
   * @return whether the scanner has been started
   */
  @VisibleForTesting
  boolean getRunStatus() {
    return shouldRun.get();
  }

  /**
   * Clear the current cache of diffs and statistics.
   */
  private void clear() {
    synchronized (diffs) {
      diffs.clear();
    }
    stats.clear();
  }

  /**
   * Main program loop for DirectoryScanner. Runs {@link reconcile()} and
   * handles any exceptions.
   */
  @Override
  public void run() {
    if (!shouldRun.get()) {
      // shutdown has been activated
      LOG.warn(
          "This cycle terminating immediately because 'shouldRun' has been deactivated");
      return;
    }
    try {
      reconcile();
    } catch (Exception e) {
      // Log and continue - allows Executor to run again next cycle
      LOG.error(
          "Exception during DirectoryScanner execution - will continue next cycle",
          e);
    } catch (Error er) {
      // Non-recoverable error - re-throw after logging the problem
      LOG.error(
          "System Error during DirectoryScanner execution - permanently terminating periodic scanner",
          er);
      throw er;
    }
  }

  /**
   * Stops the directory scanner. This method will wait for 1 minute for the
   * main thread to exit and an additional 1 minute for the report compilation
   * threads to exit. If a thread does not exit in that time period, it is left
   * running, and an error is logged.
   */
  void shutdown() {
    LOG.info("Shutdown has been called");
    if (!shouldRun.getAndSet(false)) {
      LOG.warn("Shutdown has been called, but periodic scanner not started");
    }
    if (masterThread != null) {
      masterThread.shutdown();
    }
    if (reportCompileThreadPool != null) {
      reportCompileThreadPool.shutdownNow();
    }
    if (masterThread != null) {
      try {
        masterThread.awaitTermination(1, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        LOG.error(
            "interrupted while waiting for masterThread to " + "terminate", e);
      }
    }
    if (reportCompileThreadPool != null) {
      try {
        reportCompileThreadPool.awaitTermination(1, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        LOG.error("interrupted while waiting for reportCompileThreadPool to "
            + "terminate", e);
      }
    }
    if (!retainDiffs) {
      clear();
    }
  }

  /**
   * Reconcile differences between disk and in-memory blocks
   */
  @VisibleForTesting
  public void reconcile() throws IOException {
    LOG.debug("reconcile start DirectoryScanning");
    scan();

    // HDFS-14476: run checkAndUpadte with batch to avoid holding the lock too
    // long
    int loopCount = 0;
    synchronized (diffs) {
      for (final Map.Entry<String, ScanInfo> entry : diffs.getEntries()) {
        dataset.checkAndUpdate(entry.getKey(), entry.getValue());

        if (loopCount % RECONCILE_BLOCKS_BATCH_SIZE == 0) {
          try {
            Thread.sleep(2000);
          } catch (InterruptedException e) {
            // do nothing
          }
        }
        loopCount++;
      }
    }

    if (!retainDiffs) {
      clear();
    }
  }

  /**
   * Scan for the differences between disk and in-memory blocks Scan only the
   * "finalized blocks" lists of both disk and memory.
   */
  private void scan() {
    BlockPoolReport blockPoolReport = new BlockPoolReport();

    clear();

    Collection<ScanInfoVolumeReport> volumeReports = getVolumeReports();
    for (ScanInfoVolumeReport volumeReport : volumeReports) {
      for (String blockPoolId : volumeReport.getBlockPoolIds()) {
        List<ScanInfo> scanInfos = volumeReport.getScanInfo(blockPoolId);
        blockPoolReport.addAll(blockPoolId, scanInfos);
      }
    }

    // Pre-sort the reports outside of the lock
    blockPoolReport.sortBlocks();

    for (final String bpid : blockPoolReport.getBlockPoolIds()) {
      List<ScanInfo> blockpoolReport = blockPoolReport.getScanInfo(bpid);

      Stats statsRecord = new Stats(bpid);
      stats.put(bpid, statsRecord);
      Collection<ScanInfo> diffRecord = new ArrayList<>();

      statsRecord.totalBlocks = blockpoolReport.size();
      final List<ReplicaInfo> bl;
      bl = dataset.getSortedFinalizedBlocks(bpid);

      int d = 0; // index for blockpoolReport
      int m = 0; // index for memReprot
      while (m < bl.size() && d < blockpoolReport.size()) {
        ReplicaInfo memBlock = bl.get(m);
        ScanInfo info = blockpoolReport.get(d);
        if (info.getBlockId() < memBlock.getBlockId()) {
          if (!dataset.isDeletingBlock(bpid, info.getBlockId())) {
            // Block is missing in memory
            statsRecord.missingMemoryBlocks++;
            addDifference(diffRecord, statsRecord, info);
          }
          d++;
          continue;
        }
        if (info.getBlockId() > memBlock.getBlockId()) {
          // Block is missing on the disk
          addDifference(diffRecord, statsRecord, memBlock.getBlockId(),
              info.getVolume());
          m++;
          continue;
        }
        // Block file and/or metadata file exists on the disk
        // Block exists in memory
        if (info.getBlockFile() == null) {
          // Block metadata file exits and block file is missing
          addDifference(diffRecord, statsRecord, info);
        } else if (info.getGenStamp() != memBlock.getGenerationStamp()
            || info.getBlockLength() != memBlock.getNumBytes()) {
          // Block metadata file is missing or has wrong generation stamp,
          // or block file length is different than expected
          statsRecord.mismatchBlocks++;
          addDifference(diffRecord, statsRecord, info);
        } else if (memBlock.compareWith(info) != 0) {
          // volumeMap record and on-disk files do not match.
          statsRecord.duplicateBlocks++;
          addDifference(diffRecord, statsRecord, info);
        }
        d++;

        if (d < blockpoolReport.size()) {
          // There may be multiple on-disk records for the same block, do not
          // increment the memory record pointer if so.
          ScanInfo nextInfo = blockpoolReport.get(d);
          if (nextInfo.getBlockId() != info.getBlockId()) {
            ++m;
          }
        } else {
          ++m;
        }
      }
      while (m < bl.size()) {
        ReplicaInfo current = bl.get(m++);
        addDifference(diffRecord, statsRecord, current.getBlockId(),
            current.getVolume());
      }
      while (d < blockpoolReport.size()) {
        if (!dataset.isDeletingBlock(bpid,
            blockpoolReport.get(d).getBlockId())) {
          statsRecord.missingMemoryBlocks++;
          addDifference(diffRecord, statsRecord, blockpoolReport.get(d));
        }
        d++;
      }
      synchronized (diffs) {
        diffs.addAll(bpid, diffRecord);
      }
      LOG.info("Scan Results: {}", statsRecord);
    }
  }

  /**
   * Add the ScanInfo object to the list of differences and adjust the stats
   * accordingly. This method is called when a block is found on the disk, but
   * the in-memory block is missing or does not match the block on the disk.
   *
   * @param diffRecord the collection to which to add the info
   * @param statsRecord the stats to update
   * @param info the differing info
   */
  private void addDifference(Collection<ScanInfo> diffRecord, Stats statsRecord,
      ScanInfo info) {
    statsRecord.missingMetaFile += info.getMetaFile() == null ? 1 : 0;
    statsRecord.missingBlockFile += info.getBlockFile() == null ? 1 : 0;
    diffRecord.add(info);
  }

  /**
   * Add a new ScanInfo object to the collection of differences and adjust the
   * stats accordingly. This method is called when a block is not found on the
   * disk.
   *
   * @param diffRecord the collection to which to add the info
   * @param statsRecord the stats to update
   * @param blockId the id of the missing block
   * @param vol the volume that contains the missing block
   */
  private void addDifference(Collection<ScanInfo> diffRecord, Stats statsRecord,
      long blockId, FsVolumeSpi vol) {
    statsRecord.missingBlockFile++;
    statsRecord.missingMetaFile++;
    diffRecord.add(new ScanInfo(blockId, null, null, null, vol));
  }

  /**
   * Get the lists of blocks on the disks in the data set.
   */
  @VisibleForTesting
  public Collection<ScanInfoVolumeReport> getVolumeReports() {
    List<ScanInfoVolumeReport> volReports = new ArrayList<>();
    List<Future<ScanInfoVolumeReport>> compilersInProgress = new ArrayList<>();

    // First get list of data directories
    try (FsDatasetSpi.FsVolumeReferences volumes =
        dataset.getFsVolumeReferences()) {

      for (final FsVolumeSpi volume : volumes) {
        // Disable scanning PROVIDED volumes to keep overhead low
        if (volume.getStorageType() != StorageType.PROVIDED) {
          ReportCompiler reportCompiler = new ReportCompiler(volume);
          Future<ScanInfoVolumeReport> result =
              reportCompileThreadPool.submit(reportCompiler);
          compilersInProgress.add(result);
        }
      }

      for (Future<ScanInfoVolumeReport> future : compilersInProgress) {
        try {
          final ScanInfoVolumeReport result = future.get();
          if (!CollectionUtils.addIgnoreNull(volReports, result)) {
            // This compiler thread were interrupted, give up on this run
            volReports.clear();
            break;
          }
        } catch (Exception ex) {
          LOG.warn("Error compiling report. Continuing.", ex);
        }
      }
    } catch (IOException e) {
      LOG.error("Unexpected IOException by closing FsVolumeReference", e);
    }

    return volReports;
  }

  /**
   * The ReportCompiler class encapsulates the process of searching a datanode's
   * disks for block information. It operates by performing a DFS of the volume
   * to discover block information.
   *
   * When the ReportCompiler discovers block information, it create a new
   * ScanInfo object for it and adds that object to its report list. The report
   * list is returned by the {@link #call()} method.
   */
  public class ReportCompiler implements Callable<ScanInfoVolumeReport> {
    private final FsVolumeSpi volume;
    // Variable for tracking time spent running for throttling purposes
    private final StopWatch throttleTimer = new StopWatch();
    // Variable for tracking time spent running and waiting for testing
    // purposes
    private final StopWatch perfTimer = new StopWatch();

    /**
     * Create a report compiler for the given volume.
     *
     * @param volume the target volume
     */
    public ReportCompiler(FsVolumeSpi volume) {
      this.volume = volume;
    }

    /**
     * Run this report compiler thread.
     *
     * @return the block info report list
     * @throws IOException if the block pool is not found
     */
    @Override
    public ScanInfoVolumeReport call() throws IOException {
      String[] bpList = volume.getBlockPoolList();
      ScanInfoVolumeReport result =
          new ScanInfoVolumeReport(volume, Arrays.asList(bpList));
      perfTimer.start();
      throttleTimer.start();
      for (String bpid : bpList) {
        List<ScanInfo> report = new ArrayList<>(DEFAULT_MAP_SIZE);

        perfTimer.reset().start();
        throttleTimer.reset().start();

        try {
          // ScanInfos are added directly to 'report' list
          volume.compileReport(bpid, report, this);
          result.addAll(bpid, report);
        } catch (InterruptedException ex) {
          // Exit quickly and flag the scanner to do the same
          result = null;
          break;
        }
      }
      LOG.trace("Scanner volume report: {}", result);
      return result;
    }

    /**
     * Called by the thread before each potential disk scan so that a pause can
     * be optionally inserted to limit the number of scans per second. The limit
     * is controlled by
     * {@link DFSConfigKeys#DFS_DATANODE_DIRECTORYSCAN_THROTTLE_LIMIT_MS_PER_SEC_KEY}.
     */
    public void throttle() throws InterruptedException {
      accumulateTimeRunning();

      if (throttleLimitMsPerSec > 0L) {
        final long runningTime = throttleTimer.now(TimeUnit.MILLISECONDS);
        if (runningTime >= throttleLimitMsPerSec) {
          final long sleepTime;
          if (runningTime >= 1000L) {
            LOG.warn("Unable to throttle within the second. Blocking for 1s.");
            sleepTime = 1000L;
          } else {
            // Sleep for the expected time plus any time processing ran over
            final long overTime = runningTime - throttleLimitMsPerSec;
            sleepTime = (1000L - throttleLimitMsPerSec) + overTime;
          }
          Thread.sleep(sleepTime);
          throttleTimer.reset().start();
        }
        accumulateTimeWaiting();
      }
    }

    /**
     * Helper method to measure time running.
     */
    private void accumulateTimeRunning() {
      timeRunningMs.getAndAdd(perfTimer.now(TimeUnit.MILLISECONDS));
      perfTimer.reset().start();
    }

    /**
     * Helper method to measure time waiting.
     */
    private void accumulateTimeWaiting() {
      timeWaitingMs.getAndAdd(perfTimer.now(TimeUnit.MILLISECONDS));
      perfTimer.reset().start();
    }
  }
}
