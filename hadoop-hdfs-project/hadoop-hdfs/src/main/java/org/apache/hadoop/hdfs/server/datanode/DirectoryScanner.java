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
import org.apache.hadoop.util.Time;

/**
 * Periodically scans the data directories for block and block metadata files.
 * Reconciles the differences with block information maintained in the dataset.
 */
@InterfaceAudience.Private
public class DirectoryScanner implements Runnable {
  private static final Log LOG = LogFactory.getLog(DirectoryScanner.class);

  private final FsDatasetSpi<?> dataset;
  private final ExecutorService reportCompileThreadPool;
  private final ScheduledExecutorService masterThread;
  private final long scanPeriodMsecs;
  private volatile boolean shouldRun = false;
  private boolean retainDiffs = false;

  final ScanInfoPerBlockPool diffs = new ScanInfoPerBlockPool();
  final Map<String, Stats> stats = new HashMap<String, Stats>();
  
  /**
   * Allow retaining diffs for unit test and analysis
   * @param b - defaults to false (off)
   */
  void setRetainDiffs(boolean b) {
    retainDiffs = b;
  }

  /** Stats tracked for reporting and testing, per blockpool */
  static class Stats {
    final String bpid;
    long totalBlocks = 0;
    long missingMetaFile = 0;
    long missingBlockFile = 0;
    long missingMemoryBlocks = 0;
    long mismatchBlocks = 0;
    
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
  
  static class ScanInfoPerBlockPool extends 
                     HashMap<String, LinkedList<ScanInfo>> {
    
    private static final long serialVersionUID = 1L;

    ScanInfoPerBlockPool() {super();}
    
    ScanInfoPerBlockPool(int sz) {super(sz);}
    
    /**
     * Merges {@code that} ScanInfoPerBlockPool into this one
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

    File getBlockFile() {
      return (blockSuffix == null) ? null :
        new File(volume.getBasePath(), blockSuffix);
    }

    long getBlockFileLength() {
      return blockFileLength;
    }

    File getMetaFile() {
      if (metaSuffix == null) {
        return null;
      } else if (blockSuffix == null) {
        return new File(volume.getBasePath(), metaSuffix);
      } else {
        return new File(volume.getBasePath(), blockSuffix + metaSuffix);
      }
    }

    long getBlockId() {
      return blockId;
    }

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

  DirectoryScanner(FsDatasetSpi<?> dataset, Configuration conf) {
    this.dataset = dataset;
    int interval = conf.getInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY,
        DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_DEFAULT);
    scanPeriodMsecs = interval * 1000L; //msec
    int threads = 
        conf.getInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY,
                    DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_DEFAULT);

    reportCompileThreadPool = Executors.newFixedThreadPool(threads, 
        new Daemon.DaemonFactory());
    masterThread = new ScheduledThreadPoolExecutor(1,
        new Daemon.DaemonFactory());
  }

  void start() {
    shouldRun = true;
    long offset = DFSUtil.getRandom().nextInt((int) (scanPeriodMsecs/1000L)) * 1000L; //msec
    long firstScanTime = Time.now() + offset;
    LOG.info("Periodic Directory Tree Verification scan starting at " 
        + firstScanTime + " with interval " + scanPeriodMsecs);
    masterThread.scheduleAtFixedRate(this, offset, scanPeriodMsecs, 
                                     TimeUnit.MILLISECONDS);
  }
  
  // for unit test
  boolean getRunStatus() {
    return shouldRun;
  }

  private void clear() {
    diffs.clear();
    stats.clear();
  }

  /**
   * Main program loop for DirectoryScanner
   * Runs "reconcile()" periodically under the masterThread.
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
  
  void shutdown() {
    if (!shouldRun) {
      LOG.warn("DirectoryScanner: shutdown has been called, but periodic scanner not started");
    } else {
      LOG.warn("DirectoryScanner: shutdown has been called");      
    }
    shouldRun = false;
    if (masterThread != null) masterThread.shutdown();
    if (reportCompileThreadPool != null) reportCompileThreadPool.shutdown();
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
  void reconcile() {
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
  void scan() {
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
          Block memBlock = memReport[Math.min(m, memReport.length - 1)];
          ScanInfo info = blockpoolReport[Math.min(
              d, blockpoolReport.length - 1)];
          if (info.getBlockId() < memBlock.getBlockId()) {
            // Block is missing in memory
            statsRecord.missingMemoryBlocks++;
            addDifference(diffRecord, statsRecord, info);
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
          }
          d++;
          m++;
        }
        while (m < memReport.length) {
          FinalizedReplica current = memReport[m++];
          addDifference(diffRecord, statsRecord,
                        current.getBlockId(), current.getVolume());
        }
        while (d < blockpoolReport.length) {
          statsRecord.missingMemoryBlocks++;
          addDifference(diffRecord, statsRecord, blockpoolReport[d++]);
        }
        LOG.info(statsRecord.toString());
      } //end for
    } //end synchronized
  }

  /**
   * Block is found on the disk. In-memory block is missing or does not match
   * the block on the disk
   */
  private void addDifference(LinkedList<ScanInfo> diffRecord, 
                             Stats statsRecord, ScanInfo info) {
    statsRecord.missingMetaFile += info.getMetaFile() == null ? 1 : 0;
    statsRecord.missingBlockFile += info.getBlockFile() == null ? 1 : 0;
    diffRecord.add(info);
  }

  /** Block is not found on the disk */
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

  /** Get lists of blocks on the disk sorted by blockId, per blockpool */
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
          new ReportCompiler(volumes.get(i));
        Future<ScanInfoPerBlockPool> result = 
          reportCompileThreadPool.submit(reportCompiler);
        compilersInProgress.put(i, result);
      }
    }
    
    for (Entry<Integer, Future<ScanInfoPerBlockPool>> report :
        compilersInProgress.entrySet()) {
      try {
        dirReports[report.getKey()] = report.getValue().get();
      } catch (Exception ex) {
        LOG.error("Error compiling report", ex);
        // Propagate ex to DataBlockScanner to deal with
        throw new RuntimeException(ex);
      }
    }

    // Compile consolidated report for all the volumes
    ScanInfoPerBlockPool list = new ScanInfoPerBlockPool();
    for (int i = 0; i < volumes.size(); i++) {
      if (isValid(dataset, volumes.get(i))) {
        // volume is still valid
        list.addAll(dirReports[i]);
      }
    }

    return list.toSortedArrays();
  }

  private static boolean isBlockMetaFile(String blockId, String metaFile) {
    return metaFile.startsWith(blockId)
        && metaFile.endsWith(Block.METADATA_EXTENSION);
  }

  private static class ReportCompiler 
  implements Callable<ScanInfoPerBlockPool> {
    private final FsVolumeSpi volume;

    public ReportCompiler(FsVolumeSpi volume) {
      this.volume = volume;
    }

    @Override
    public ScanInfoPerBlockPool call() throws Exception {
      String[] bpList = volume.getBlockPoolList();
      ScanInfoPerBlockPool result = new ScanInfoPerBlockPool(bpList.length);
      for (String bpid : bpList) {
        LinkedList<ScanInfo> report = new LinkedList<ScanInfo>();
        File bpFinalizedDir = volume.getFinalizedDir(bpid);
        result.put(bpid, compileReport(volume, bpFinalizedDir, report));
      }
      return result;
    }

    /** Compile list {@link ScanInfo} for the blocks in the directory <dir> */
    private LinkedList<ScanInfo> compileReport(FsVolumeSpi vol, File dir,
        LinkedList<ScanInfo> report) {
      File[] files;
      try {
        files = FileUtil.listFiles(dir);
      } catch (IOException ioe) {
        LOG.warn("Exception occured while compiling report: ", ioe);
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
        if (files[i].isDirectory()) {
          compileReport(vol, files[i], report);
          continue;
        }
        if (!Block.isBlockFilename(files[i])) {
          if (isBlockMetaFile("blk_", files[i].getName())) {
            long blockId = Block.getBlockId(files[i].getName());
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
        report.add(new ScanInfo(blockId, blockFile, metaFile, vol));
      }
      return report;
    }
  }
}
