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

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.time.FastDateFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.AutoCloseableLock;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi.ScanInfo;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StopWatch;
import org.apache.hadoop.util.Time;

/**
 * Periodically scans the data directories for block and block metadata files.
 * Reconciles the differences with block information maintained in the dataset.
 */
@InterfaceAudience.Private
public class DirectoryScanner implements Runnable {
  private static final Log LOG = LogFactory.getLog(DirectoryScanner.class);
  private static final int MILLIS_PER_SECOND = 1000;
  private static final String START_MESSAGE =
      "Periodic Directory Tree Verification scan"
      + " starting at %s with interval of %dms";
  private static final String START_MESSAGE_WITH_THROTTLE = START_MESSAGE
      + " and throttle limit of %dms/s";

  private final FsDatasetSpi<?> dataset;
  private final ExecutorService reportCompileThreadPool;
  private final ScheduledExecutorService masterThread;
  private final long scanPeriodMsecs;
  private final int throttleLimitMsPerSec;
  private volatile boolean shouldRun = false;
  private boolean retainDiffs = false;
  private final DataNode datanode;

  /**
   * Total combined wall clock time (in milliseconds) spent by the report
   * compiler threads executing.  Used for testing purposes.
   */
  @VisibleForTesting
  final AtomicLong timeRunningMs = new AtomicLong(0L);
  /**
   * Total combined wall clock time (in milliseconds) spent by the report
   * compiler threads blocked by the throttle.  Used for testing purposes.
   */
  @VisibleForTesting
  final AtomicLong timeWaitingMs = new AtomicLong(0L);
  /**
   * The complete list of block differences indexed by block pool ID.
   */
  @VisibleForTesting
  final ScanInfoPerBlockPool diffs = new ScanInfoPerBlockPool();
  /**
   * Statistics about the block differences in each blockpool, indexed by
   * block pool ID.
   */
  @VisibleForTesting
  final Map<String, Stats> stats = new HashMap<String, Stats>();
  
  /**
   * Allow retaining diffs for unit test and analysis. Defaults to false (off)
   * @param b whether to retain diffs
   */
  @VisibleForTesting
  void setRetainDiffs(boolean b) {
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
     * @param bpid blockpool ID
     */
    public Stats(String bpid) {
      this.bpid = bpid;
    }
    
    @Override
    public String toString() {
      return "BlockPool " + bpid
      + " Total blocks: " + totalBlocks + ", missing metadata files:"
      + missingMetaFile + ", missing block files:" + missingBlockFile
      + ", missing blocks in memory:" + missingMemoryBlocks
      + ", mismatched blocks:" + mismatchBlocks;
    }
  }

  /**
   * Helper class for compiling block info reports from report compiler threads.
   */
  static class ScanInfoPerBlockPool extends 
                     HashMap<String, LinkedList<ScanInfo>> {
    
    private static final long serialVersionUID = 1L;

    /**
     * Create a new info list.
     */
    ScanInfoPerBlockPool() {super();}

    /**
     * Create a new info list initialized to the given expected size.
     * See {@link java.util.HashMap#HashMap(int)}.
     *
     * @param sz initial expected size
     */
    ScanInfoPerBlockPool(int sz) {super(sz);}
    
    /**
     * Merges {@code that} ScanInfoPerBlockPool into this one
     *
     * @param the ScanInfoPerBlockPool to merge
     */
    public void addAll(ScanInfoPerBlockPool that) {
      if (that == null) return;
      
      for (Entry<String, LinkedList<ScanInfo>> entry : that.entrySet()) {
        String bpid = entry.getKey();
        LinkedList<ScanInfo> list = entry.getValue();
        
        if (this.containsKey(bpid)) {
          //merge that per-bpid linked list with this one
          this.get(bpid).addAll(list);
        } else {
          //add that new bpid and its linked list to this
          this.put(bpid, list);
        }
      }
    }
    
    /**
     * Convert all the LinkedList values in this ScanInfoPerBlockPool map
     * into sorted arrays, and return a new map of these arrays per blockpool
     *
     * @return a map of ScanInfo arrays per blockpool
     */
    public Map<String, ScanInfo[]> toSortedArrays() {
      Map<String, ScanInfo[]> result = 
        new HashMap<String, ScanInfo[]>(this.size());
      
      for (Entry<String, LinkedList<ScanInfo>> entry : this.entrySet()) {
        String bpid = entry.getKey();
        LinkedList<ScanInfo> list = entry.getValue();
        
        // convert list to array
        ScanInfo[] record = list.toArray(new ScanInfo[list.size()]);
        // Sort array based on blockId
        Arrays.sort(record);
        result.put(bpid, record);            
      }
      return result;
    }
  }


  /**
   * Create a new directory scanner, but don't cycle it running yet.
   *
   * @param datanode the parent datanode
   * @param dataset the dataset to scan
   * @param conf the Configuration object
   */
  DirectoryScanner(DataNode datanode, FsDatasetSpi<?> dataset, Configuration conf) {
    this.datanode = datanode;
    this.dataset = dataset;
    int interval = (int) conf.getTimeDuration(
        DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY,
        DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_DEFAULT,
        TimeUnit.SECONDS);
    scanPeriodMsecs = interval * MILLIS_PER_SECOND; //msec

    int throttle =
        conf.getInt(
          DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THROTTLE_LIMIT_MS_PER_SEC_KEY,
          DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THROTTLE_LIMIT_MS_PER_SEC_DEFAULT);

    if ((throttle > MILLIS_PER_SECOND) || (throttle <= 0)) {
      if (throttle > MILLIS_PER_SECOND) {
        LOG.error(
            DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THROTTLE_LIMIT_MS_PER_SEC_KEY
            + " set to value above 1000 ms/sec. Assuming default value of " +
            DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THROTTLE_LIMIT_MS_PER_SEC_DEFAULT);
      } else {
        LOG.error(
            DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THROTTLE_LIMIT_MS_PER_SEC_KEY
            + " set to value below 1 ms/sec. Assuming default value of " +
            DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THROTTLE_LIMIT_MS_PER_SEC_DEFAULT);
      }

      throttleLimitMsPerSec =
          DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THROTTLE_LIMIT_MS_PER_SEC_DEFAULT;
    } else {
      throttleLimitMsPerSec = throttle;
    }

    int threads = 
        conf.getInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY,
                    DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_DEFAULT);

    reportCompileThreadPool = Executors.newFixedThreadPool(threads, 
        new Daemon.DaemonFactory());
    masterThread = new ScheduledThreadPoolExecutor(1,
        new Daemon.DaemonFactory());
  }

  /**
   * Start the scanner.  The scanner will run every
   * {@link DFSConfigKeys#DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY} seconds.
   */
  void start() {
    shouldRun = true;
    long offset = ThreadLocalRandom.current().nextInt(
        (int) (scanPeriodMsecs/MILLIS_PER_SECOND)) * MILLIS_PER_SECOND; //msec
    long firstScanTime = Time.now() + offset;
    String logMsg;

    if (throttleLimitMsPerSec < MILLIS_PER_SECOND) {
      logMsg = String.format(START_MESSAGE_WITH_THROTTLE,
          FastDateFormat.getInstance().format(firstScanTime), scanPeriodMsecs,
          throttleLimitMsPerSec);
    } else {
      logMsg = String.format(START_MESSAGE,
          FastDateFormat.getInstance().format(firstScanTime), scanPeriodMsecs);
    }

    LOG.info(logMsg);
    masterThread.scheduleAtFixedRate(this, offset, scanPeriodMsecs, 
                                     TimeUnit.MILLISECONDS);
  }
  
  /**
   * Return whether the scanner has been started.
   *
   * @return whether the scanner has been started
   */
  @VisibleForTesting
  boolean getRunStatus() {
    return shouldRun;
  }

  /**
   * Clear the current cache of diffs and statistics.
   */
  private void clear() {
    diffs.clear();
    stats.clear();
  }

  /**
   * Main program loop for DirectoryScanner.  Runs {@link reconcile()}
   * and handles any exceptions.
   */
  @Override
  public void run() {
    try {
      if (!shouldRun) {
        //shutdown has been activated
        LOG.warn("this cycle terminating immediately because 'shouldRun' has been deactivated");
        return;
      }

      //We're are okay to run - do it
      reconcile();      
      
    } catch (Exception e) {
      //Log and continue - allows Executor to run again next cycle
      LOG.error("Exception during DirectoryScanner execution - will continue next cycle", e);
    } catch (Error er) {
      //Non-recoverable error - re-throw after logging the problem
      LOG.error("System Error during DirectoryScanner execution - permanently terminating periodic scanner", er);
      throw er;
    }
  }
  
  /**
   * Stops the directory scanner.  This method will wait for 1 minute for the
   * main thread to exit and an additional 1 minute for the report compilation
   * threads to exit.  If a thread does not exit in that time period, it is
   * left running, and an error is logged.
   */
  void shutdown() {
    if (!shouldRun) {
      LOG.warn("DirectoryScanner: shutdown has been called, but periodic scanner not started");
    } else {
      LOG.warn("DirectoryScanner: shutdown has been called");      
    }
    shouldRun = false;
    if (masterThread != null) masterThread.shutdown();

    if (reportCompileThreadPool != null) {
      reportCompileThreadPool.shutdownNow();
    }

    if (masterThread != null) {
      try {
        masterThread.awaitTermination(1, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        LOG.error("interrupted while waiting for masterThread to " +
          "terminate", e);
      }
    }
    if (reportCompileThreadPool != null) {
      try {
        reportCompileThreadPool.awaitTermination(1, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        LOG.error("interrupted while waiting for reportCompileThreadPool to " +
          "terminate", e);
      }
    }
    if (!retainDiffs) clear();
  }

  /**
   * Reconcile differences between disk and in-memory blocks
   */
  @VisibleForTesting
  void reconcile() throws IOException {
    scan();
    for (Entry<String, LinkedList<ScanInfo>> entry : diffs.entrySet()) {
      String bpid = entry.getKey();
      LinkedList<ScanInfo> diff = entry.getValue();
      
      for (ScanInfo info : diff) {
        dataset.checkAndUpdate(bpid, info.getBlockId(), info.getBlockFile(),
            info.getMetaFile(), info.getVolume());
      }
    }
    if (!retainDiffs) clear();
  }

  /**
   * Scan for the differences between disk and in-memory blocks
   * Scan only the "finalized blocks" lists of both disk and memory.
   */
  private void scan() {
    clear();
    Map<String, ScanInfo[]> diskReport = getDiskReport();

    // Hold FSDataset lock to prevent further changes to the block map
    try(AutoCloseableLock lock = dataset.acquireDatasetLock()) {
      for (Entry<String, ScanInfo[]> entry : diskReport.entrySet()) {
        String bpid = entry.getKey();
        ScanInfo[] blockpoolReport = entry.getValue();
        
        Stats statsRecord = new Stats(bpid);
        stats.put(bpid, statsRecord);
        LinkedList<ScanInfo> diffRecord = new LinkedList<ScanInfo>();
        diffs.put(bpid, diffRecord);
        
        statsRecord.totalBlocks = blockpoolReport.length;
        final List<ReplicaInfo> bl = dataset.getFinalizedBlocks(bpid);
        Collections.sort(bl); // Sort based on blockId
  
        int d = 0; // index for blockpoolReport
        int m = 0; // index for memReprot
        while (m < bl.size() && d < blockpoolReport.length) {
          ReplicaInfo memBlock = bl.get(m);
          ScanInfo info = blockpoolReport[d];
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
            addDifference(diffRecord, statsRecord,
                          memBlock.getBlockId(), info.getVolume());
            m++;
            continue;
          }
          // Block file and/or metadata file exists on the disk
          // Block exists in memory
          if (info.getBlockFile() == null) {
            // Block metadata file exits and block file is missing
            addDifference(diffRecord, statsRecord, info);
          } else if (info.getGenStamp() != memBlock.getGenerationStamp()
              || info.getBlockFileLength() != memBlock.getNumBytes()) {
            // Block metadata file is missing or has wrong generation stamp,
            // or block file length is different than expected
            statsRecord.mismatchBlocks++;
            addDifference(diffRecord, statsRecord, info);
          } else if (memBlock.compareWith(info) != 0) {
            // volumeMap record and on-disk files don't match.
            statsRecord.duplicateBlocks++;
            addDifference(diffRecord, statsRecord, info);
          }
          d++;

          if (d < blockpoolReport.length) {
            // There may be multiple on-disk records for the same block, don't increment
            // the memory record pointer if so.
            ScanInfo nextInfo = blockpoolReport[Math.min(d, blockpoolReport.length - 1)];
            if (nextInfo.getBlockId() != info.getBlockId()) {
              ++m;
            }
          } else {
            ++m;
          }
        }
        while (m < bl.size()) {
          ReplicaInfo current = bl.get(m++);
          addDifference(diffRecord, statsRecord,
                        current.getBlockId(), current.getVolume());
        }
        while (d < blockpoolReport.length) {
          if (!dataset.isDeletingBlock(bpid, blockpoolReport[d].getBlockId())) {
            statsRecord.missingMemoryBlocks++;
            addDifference(diffRecord, statsRecord, blockpoolReport[d]);
          }
          d++;
        }
        LOG.info(statsRecord.toString());
      } //end for
    } //end synchronized
  }

  /**
   * Add the ScanInfo object to the list of differences and adjust the stats
   * accordingly.  This method is called when a block is found on the disk,
   * but the in-memory block is missing or does not match the block on the disk.
   *
   * @param diffRecord the list to which to add the info
   * @param statsRecord the stats to update
   * @param info the differing info
   */
  private void addDifference(LinkedList<ScanInfo> diffRecord, 
                             Stats statsRecord, ScanInfo info) {
    statsRecord.missingMetaFile += info.getMetaFile() == null ? 1 : 0;
    statsRecord.missingBlockFile += info.getBlockFile() == null ? 1 : 0;
    diffRecord.add(info);
  }

  /**
   * Add a new ScanInfo object to the list of differences and adjust the stats
   * accordingly.  This method is called when a block is not found on the disk.
   *
   * @param diffRecord the list to which to add the info
   * @param statsRecord the stats to update
   * @param blockId the id of the missing block
   * @param vol the volume that contains the missing block
   */
  private void addDifference(LinkedList<ScanInfo> diffRecord,
                             Stats statsRecord, long blockId,
                             FsVolumeSpi vol) {
    statsRecord.missingBlockFile++;
    statsRecord.missingMetaFile++;
    diffRecord.add(new ScanInfo(blockId, null, null, vol));
  }

  /**
   * Get the lists of blocks on the disks in the dataset, sorted by blockId.
   * The returned map contains one entry per blockpool, keyed by the blockpool
   * ID.
   *
   * @return a map of sorted arrays of block information
   */
  private Map<String, ScanInfo[]> getDiskReport() {
    ScanInfoPerBlockPool list = new ScanInfoPerBlockPool();
    ScanInfoPerBlockPool[] dirReports = null;
    // First get list of data directories
    try (FsDatasetSpi.FsVolumeReferences volumes =
        dataset.getFsVolumeReferences()) {

      // Use an array since the threads may return out of order and
      // compilersInProgress#keySet may return out of order as well.
      dirReports = new ScanInfoPerBlockPool[volumes.size()];

      Map<Integer, Future<ScanInfoPerBlockPool>> compilersInProgress =
          new HashMap<Integer, Future<ScanInfoPerBlockPool>>();

      for (int i = 0; i < volumes.size(); i++) {
        ReportCompiler reportCompiler =
            new ReportCompiler(datanode, volumes.get(i));
        Future<ScanInfoPerBlockPool> result =
            reportCompileThreadPool.submit(reportCompiler);
        compilersInProgress.put(i, result);
      }

      for (Entry<Integer, Future<ScanInfoPerBlockPool>> report :
          compilersInProgress.entrySet()) {
        Integer index = report.getKey();
        try {
          dirReports[index] = report.getValue().get();

          // If our compiler threads were interrupted, give up on this run
          if (dirReports[index] == null) {
            dirReports = null;
            break;
          }
        } catch (Exception ex) {
          FsVolumeSpi fsVolumeSpi = volumes.get(index);
          LOG.error("Error compiling report for the volume, StorageId: "
              + fsVolumeSpi.getStorageID(), ex);
          // Continue scanning the other volumes
        }
      }
    } catch (IOException e) {
      LOG.error("Unexpected IOException by closing FsVolumeReference", e);
    }
    if (dirReports != null) {
      // Compile consolidated report for all the volumes
      for (ScanInfoPerBlockPool report : dirReports) {
        if(report != null){
          list.addAll(report);
        }
      }
    }
    return list.toSortedArrays();
  }

  /**
   * The ReportCompiler class encapsulates the process of searching a datanode's
   * disks for block information.  It operates by performing a DFS of the
   * volume to discover block information.
   *
   * When the ReportCompiler discovers block information, it create a new
   * ScanInfo object for it and adds that object to its report list.  The report
   * list is returned by the {@link #call()} method.
   */
  public class ReportCompiler implements Callable<ScanInfoPerBlockPool> {
    private final FsVolumeSpi volume;
    private final DataNode datanode;
    // Variable for tracking time spent running for throttling purposes
    private final StopWatch throttleTimer = new StopWatch();
    // Variable for tracking time spent running and waiting for testing
    // purposes
    private final StopWatch perfTimer = new StopWatch();

    /**
     * Create a report compiler for the given volume on the given datanode.
     *
     * @param datanode the target datanode
     * @param volume the target volume
     */
    public ReportCompiler(DataNode datanode, FsVolumeSpi volume) {
      this.datanode = datanode;
      this.volume = volume;
    }

    /**
     * Run this report compiler thread.
     *
     * @return the block info report list
     * @throws IOException if the block pool isn't found
     */
    @Override
    public ScanInfoPerBlockPool call() throws IOException {
      String[] bpList = volume.getBlockPoolList();
      ScanInfoPerBlockPool result = new ScanInfoPerBlockPool(bpList.length);
      perfTimer.start();
      throttleTimer.start();
      for (String bpid : bpList) {
        LinkedList<ScanInfo> report = new LinkedList<>();

        try {
          result.put(bpid, volume.compileReport(bpid, report, this));
        } catch (InterruptedException ex) {
          // Exit quickly and flag the scanner to do the same
          result = null;
          break;
        }
      }
      return result;
    }

    /**
     * Called by the thread before each potential disk scan so that a pause
     * can be optionally inserted to limit the number of scans per second.
     * The limit is controlled by
     * {@link DFSConfigKeys#DFS_DATANODE_DIRECTORYSCAN_THROTTLE_LIMIT_MS_PER_SEC_KEY}.
     */
    public void throttle() throws InterruptedException {
      accumulateTimeRunning();

      if ((throttleLimitMsPerSec < 1000) &&
          (throttleTimer.now(TimeUnit.MILLISECONDS) > throttleLimitMsPerSec)) {

        Thread.sleep(MILLIS_PER_SECOND - throttleLimitMsPerSec);
        throttleTimer.reset().start();
      }

      accumulateTimeWaiting();
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

  public enum BlockDirFilter implements FilenameFilter {
    INSTANCE;

    @Override
    public boolean accept(File dir, String name) {
      return name.startsWith(DataStorage.BLOCK_SUBDIR_PREFIX)
          || name.startsWith(DataStorage.STORAGE_DIR_FINALIZED)
          || name.startsWith(Block.BLOCK_FILE_PREFIX);
    }
  }
}
