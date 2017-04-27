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
import java.io.IOException;
import java.util.Arrays;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
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
      + " starting at %dms with interval of %dms";
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
   * Tracks the files and other information related to a block on the disk
   * Missing file is indicated by setting the corresponding member
   * to null.
   * 
   * Because millions of these structures may be created, we try to save
   * memory here.  So instead of storing full paths, we store path suffixes.
   * The block file, if it exists, will have a path like this:
   * <volume_base_path>/<block_path>
   * So we don't need to store the volume path, since we already know what the
   * volume is.
   * 
   * The metadata file, if it exists, will have a path like this:
   * <volume_base_path>/<block_path>_<genstamp>.meta
   * So if we have a block file, there isn't any need to store the block path
   * again.
   * 
   * The accessor functions take care of these manipulations.
   */
  static class ScanInfo implements Comparable<ScanInfo> {
    private final long blockId;
    
    /**
     * The block file path, relative to the volume's base directory.
     * If there was no block file found, this may be null. If 'vol'
     * is null, then this is the full path of the block file.
     */
    private final String blockSuffix;
    
    /**
     * The suffix of the meta file path relative to the block file.
     * If blockSuffix is null, then this will be the entire path relative
     * to the volume base directory, or an absolute path if vol is also
     * null.
     */
    private final String metaSuffix;

    private final FsVolumeSpi volume;

    /**
     * Get the file's length in async block scan
     */
    private final long blockFileLength;

    private final static Pattern CONDENSED_PATH_REGEX =
        Pattern.compile("(?<!^)(\\\\|/){2,}");
    
    private final static String QUOTED_FILE_SEPARATOR = 
        Matcher.quoteReplacement(File.separator);
    
    /**
     * Get the most condensed version of the path.
     *
     * For example, the condensed version of /foo//bar is /foo/bar
     * Unlike {@link File#getCanonicalPath()}, this will never perform I/O
     * on the filesystem.
     *
     * @param path the path to condense
     * @return the condensed path
     */
    private static String getCondensedPath(String path) {
      return CONDENSED_PATH_REGEX.matcher(path).
          replaceAll(QUOTED_FILE_SEPARATOR);
    }

    /**
     * Get a path suffix.
     *
     * @param f            The file to get the suffix for.
     * @param prefix       The prefix we're stripping off.
     *
     * @return             A suffix such that prefix + suffix = path to f
     */
    private static String getSuffix(File f, String prefix) {
      String fullPath = getCondensedPath(f.getAbsolutePath());
      if (fullPath.startsWith(prefix)) {
        return fullPath.substring(prefix.length());
      }
      throw new RuntimeException(prefix + " is not a prefix of " + fullPath);
    }

    /**
     * Create a ScanInfo object for a block. This constructor will examine
     * the block data and meta-data files.
     *
     * @param blockId the block ID
     * @param blockFile the path to the block data file
     * @param metaFile the path to the block meta-data file
     * @param vol the volume that contains the block
     */
    ScanInfo(long blockId, File blockFile, File metaFile, FsVolumeSpi vol) {
      this.blockId = blockId;
      String condensedVolPath = vol == null ? null :
        getCondensedPath(vol.getBasePath());
      this.blockSuffix = blockFile == null ? null :
        getSuffix(blockFile, condensedVolPath);
      this.blockFileLength = (blockFile != null) ? blockFile.length() : 0; 
      if (metaFile == null) {
        this.metaSuffix = null;
      } else if (blockFile == null) {
        this.metaSuffix = getSuffix(metaFile, condensedVolPath);
      } else {
        this.metaSuffix = getSuffix(metaFile,
            condensedVolPath + blockSuffix);
      }
      this.volume = vol;
    }

    /**
     * Returns the block data file.
     *
     * @return the block data file
     */
    File getBlockFile() {
      return (blockSuffix == null) ? null :
        new File(volume.getBasePath(), blockSuffix);
    }

    /**
     * Return the length of the data block. The length returned is the length
     * cached when this object was created.
     *
     * @return the length of the data block
     */
    long getBlockFileLength() {
      return blockFileLength;
    }

    /**
     * Returns the block meta data file or null if there isn't one.
     *
     * @return the block meta data file
     */
    File getMetaFile() {
      if (metaSuffix == null) {
        return null;
      } else if (blockSuffix == null) {
        return new File(volume.getBasePath(), metaSuffix);
      } else {
        return new File(volume.getBasePath(), blockSuffix + metaSuffix);
      }
    }

    /**
     * Returns the block ID.
     *
     * @return the block ID
     */
    long getBlockId() {
      return blockId;
    }

    /**
     * Returns the volume that contains the block that this object describes.
     *
     * @return the volume
     */
    FsVolumeSpi getVolume() {
      return volume;
    }

    @Override // Comparable
    public int compareTo(ScanInfo b) {
      if (blockId < b.blockId) {
        return -1;
      } else if (blockId == b.blockId) {
        return 0;
      } else {
        return 1;
      }
    }

    @Override // Object
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ScanInfo)) {
        return false;
      }
      return blockId == ((ScanInfo) o).blockId;
    }

    @Override // Object
    public int hashCode() {
      return (int)(blockId^(blockId>>>32));
    }

    public long getGenStamp() {
      return metaSuffix != null ? Block.getGenerationStamp(
          getMetaFile().getName()) : 
            GenerationStamp.GRANDFATHER_GENERATION_STAMP;
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
    int interval = conf.getInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY,
        DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_DEFAULT);
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
    long offset = DFSUtil.getRandom().nextInt(
        (int) (scanPeriodMsecs/MILLIS_PER_SECOND)) * MILLIS_PER_SECOND; //msec
    long firstScanTime = Time.now() + offset;
    String logMsg;

    if (throttleLimitMsPerSec < MILLIS_PER_SECOND) {
      logMsg = String.format(START_MESSAGE_WITH_THROTTLE, firstScanTime,
          scanPeriodMsecs, throttleLimitMsPerSec);
    } else {
      logMsg = String.format(START_MESSAGE, firstScanTime, scanPeriodMsecs);
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
    synchronized(dataset) {
      for (Entry<String, ScanInfo[]> entry : diskReport.entrySet()) {
        String bpid = entry.getKey();
        ScanInfo[] blockpoolReport = entry.getValue();
        
        Stats statsRecord = new Stats(bpid);
        stats.put(bpid, statsRecord);
        LinkedList<ScanInfo> diffRecord = new LinkedList<ScanInfo>();
        diffs.put(bpid, diffRecord);
        
        statsRecord.totalBlocks = blockpoolReport.length;
        List<FinalizedReplica> bl = dataset.getFinalizedBlocks(bpid);
        FinalizedReplica[] memReport = bl.toArray(new FinalizedReplica[bl.size()]);
        Arrays.sort(memReport); // Sort based on blockId
  
        int d = 0; // index for blockpoolReport
        int m = 0; // index for memReprot
        while (m < memReport.length && d < blockpoolReport.length) {
          FinalizedReplica memBlock = memReport[m];
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
          } else if (info.getBlockFile().compareTo(memBlock.getBlockFile()) != 0) {
            // volumeMap record and on-disk files don't match.
            statsRecord.duplicateBlocks++;
            addDifference(diffRecord, statsRecord, info);
          }
          d++;

          if (d < blockpoolReport.length) {
            // There may be multiple on-disk records for the same block, don't increment
            // the memory record pointer if so.
            ScanInfo nextInfo = blockpoolReport[Math.min(d, blockpoolReport.length - 1)];
            if (nextInfo.getBlockId() != info.blockId) {
              ++m;
            }
          } else {
            ++m;
          }
        }
        while (m < memReport.length) {
          FinalizedReplica current = memReport[m++];
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

  /** Is the given volume still valid in the dataset? */
  private static boolean isValid(final FsDatasetSpi<?> dataset,
      final FsVolumeSpi volume) {
    for (FsVolumeSpi vol : dataset.getVolumes()) {
      if (vol == volume) {
        return true;
      }
    }
    return false;
  }

  /**
   * Get the lists of blocks on the disks in the dataset, sorted by blockId.
   * The returned map contains one entry per blockpool, keyed by the blockpool
   * ID.
   *
   * @return a map of sorted arrays of block information
   */
  private Map<String, ScanInfo[]> getDiskReport() {
    // First get list of data directories
    final List<? extends FsVolumeSpi> volumes = dataset.getVolumes();

    // Use an array since the threads may return out of order and
    // compilersInProgress#keySet may return out of order as well.
    ScanInfoPerBlockPool[] dirReports = new ScanInfoPerBlockPool[volumes.size()];

    Map<Integer, Future<ScanInfoPerBlockPool>> compilersInProgress =
      new HashMap<Integer, Future<ScanInfoPerBlockPool>>();

    for (int i = 0; i < volumes.size(); i++) {
      if (isValid(dataset, volumes.get(i))) {
        ReportCompiler reportCompiler =
          new ReportCompiler(datanode,volumes.get(i));
        Future<ScanInfoPerBlockPool> result =
          reportCompileThreadPool.submit(reportCompiler);
        compilersInProgress.put(i, result);
      }
    }

    for (Entry<Integer, Future<ScanInfoPerBlockPool>> report :
        compilersInProgress.entrySet()) {
      try {
        dirReports[report.getKey()] = report.getValue().get();

        // If our compiler threads were interrupted, give up on this run
        if (dirReports[report.getKey()] == null) {
          dirReports = null;
          break;
        }
      } catch (Exception ex) {
        LOG.error("Error compiling report", ex);
        // Propagate ex to DataBlockScanner to deal with
        throw new RuntimeException(ex);
      }
    }

    // Compile consolidated report for all the volumes
    ScanInfoPerBlockPool list = new ScanInfoPerBlockPool();
    if (dirReports != null) {
      for (int i = 0; i < volumes.size(); i++) {
        if (isValid(dataset, volumes.get(i))) {
          // volume is still valid
          list.addAll(dirReports[i]);
        }
      }
    }

    return list.toSortedArrays();
  }

  /**
   * Helper method to determine if a file name is consistent with a block.
   * meta-data file
   *
   * @param blockId the block ID
   * @param metaFile the file to check
   * @return whether the file name is a block meta-data file name
   */
  private static boolean isBlockMetaFile(String blockId, String metaFile) {
    return metaFile.startsWith(blockId)
        && metaFile.endsWith(Block.METADATA_EXTENSION);
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
  private class ReportCompiler implements Callable<ScanInfoPerBlockPool> {
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
      for (String bpid : bpList) {
        LinkedList<ScanInfo> report = new LinkedList<>();
        File bpFinalizedDir = volume.getFinalizedDir(bpid);

        perfTimer.start();
        throttleTimer.start();

        try {
          result.put(bpid,
              compileReport(volume, bpFinalizedDir, bpFinalizedDir, report));
        } catch (InterruptedException ex) {
          // Exit quickly and flag the scanner to do the same
          result = null;
          break;
        }
      }
      return result;
    }

    /**
     * Compile a list of {@link ScanInfo} for the blocks in the directory
     * given by {@code dir}.
     *
     * @param vol the volume that contains the directory to scan
     * @param bpFinalizedDir the root directory of the directory to scan
     * @param dir the directory to scan
     * @param report the list onto which blocks reports are placed
     */
    private LinkedList<ScanInfo> compileReport(FsVolumeSpi vol,
        File bpFinalizedDir, File dir, LinkedList<ScanInfo> report)
        throws InterruptedException {

      File[] files;

      throttle();

      try {
        files = FileUtil.listFiles(dir);
      } catch (IOException ioe) {
        LOG.warn("Exception occured while compiling report: ", ioe);
        // Initiate a check on disk failure.
        datanode.checkDiskErrorAsync();
        // Ignore this directory and proceed.
        return report;
      }
      Arrays.sort(files);
      /*
       * Assumption: In the sorted list of files block file appears immediately
       * before block metadata file. This is true for the current naming
       * convention for block file blk_<blockid> and meta file
       * blk_<blockid>_<genstamp>.meta
       */
      for (int i = 0; i < files.length; i++) {
        // Make sure this thread can make a timely exit. With a low throttle
        // rate, completing a run can take a looooong time.
        if (Thread.interrupted()) {
          throw new InterruptedException();
        }

        if (files[i].isDirectory()) {
          compileReport(vol, bpFinalizedDir, files[i], report);
          continue;
        }
        if (!Block.isBlockFilename(files[i])) {
          if (isBlockMetaFile(Block.BLOCK_FILE_PREFIX, files[i].getName())) {
            long blockId = Block.getBlockId(files[i].getName());
            verifyFileLocation(files[i].getParentFile(), bpFinalizedDir,
                blockId);
            report.add(new ScanInfo(blockId, null, files[i], vol));
          }
          continue;
        }
        File blockFile = files[i];
        long blockId = Block.filename2id(blockFile.getName());
        File metaFile = null;

        // Skip all the files that start with block name until
        // getting to the metafile for the block
        while (i + 1 < files.length && files[i + 1].isFile()
            && files[i + 1].getName().startsWith(blockFile.getName())) {
          i++;
          if (isBlockMetaFile(blockFile.getName(), files[i].getName())) {
            metaFile = files[i];
            break;
          }
        }
        verifyFileLocation(blockFile, bpFinalizedDir, blockId);
        report.add(new ScanInfo(blockId, blockFile, metaFile, vol));
      }
      return report;
    }

    /**
     * Verify whether the actual directory location of block file has the
     * expected directory path computed using its block ID.
     */
    private void verifyFileLocation(File actualBlockFile,
        File bpFinalizedDir, long blockId) {
      File blockDir = DatanodeUtil.idToBlockDir(bpFinalizedDir, blockId);
      if (actualBlockFile.getParentFile().compareTo(blockDir) != 0) {
        File expBlockFile = new File(blockDir, actualBlockFile.getName());
        LOG.warn("Block: " + blockId
            + " has to be upgraded to block ID-based layout. "
            + "Actual block file path: " + actualBlockFile
            + ", expected block file path: " + expBlockFile);
      }
    }

    /**
     * Called by the thread before each potential disk scan so that a pause
     * can be optionally inserted to limit the number of scans per second.
     * The limit is controlled by
     * {@link DFSConfigKeys#DFS_DATANODE_DIRECTORYSCAN_THROTTLE_LIMIT_MS_PER_SEC_KEY}.
     */
    private void throttle() throws InterruptedException {
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
}
