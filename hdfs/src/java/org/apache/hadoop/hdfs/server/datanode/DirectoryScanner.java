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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.FSVolume;

/**
 * Periodically scans the data directories for block and block metadata files.
 * Reconciles the differences with block information maintained in
 * {@link FSDataset}
 */
@InterfaceAudience.Private
public class DirectoryScanner {
  private static final Log LOG = LogFactory.getLog(DirectoryScanner.class);
  private static final int DEFAULT_SCAN_INTERVAL = 21600;

  private final FSDataset dataset;
  private long scanPeriod;
  private long lastScanTime;
  private ExecutorService reportCompileThreadPool;

  LinkedList<ScanInfo> diff = new LinkedList<ScanInfo>();

  /** Stats tracked for reporting and testing */
  long totalBlocks;
  long missingMetaFile;
  long missingBlockFile;
  long missingMemoryBlocks;
  long mismatchBlocks;

  /**
   * Tracks the files and other information related to a block on the disk
   * Missing file is indicated by setting the corresponding member
   * to null.
   */
  static class ScanInfo implements Comparable<ScanInfo> {
    private final long blockId;
    private final File metaFile;
    private final File blockFile;
    private final FSVolume volume;

    ScanInfo(long blockId) {
      this(blockId, null, null, null);
    }

    ScanInfo(long blockId, File blockFile, File metaFile, FSVolume vol) {
      this.blockId = blockId;
      this.metaFile = metaFile;
      this.blockFile = blockFile;
      this.volume = vol;
    }

    File getMetaFile() {
      return metaFile;
    }

    File getBlockFile() {
      return blockFile;
    }

    long getBlockId() {
      return blockId;
    }

    FSVolume getVolume() {
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
      return metaFile != null ? Block.getGenerationStamp(metaFile.getName()) :
        GenerationStamp.GRANDFATHER_GENERATION_STAMP;
    }
  }

  DirectoryScanner(FSDataset dataset, Configuration conf) {
    this.dataset = dataset;
    int interval = conf.getInt("dfs.datanode.directoryscan.interval",
        DEFAULT_SCAN_INTERVAL);
    scanPeriod = interval * 1000L;
    int threads = 
        conf.getInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY,
                    DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_DEFAULT);

    reportCompileThreadPool = Executors.newFixedThreadPool(threads);

    Random rand = new Random();
    lastScanTime = System.currentTimeMillis() - (rand.nextInt(interval) * 1000L);
    LOG.info("scan starts at " + (lastScanTime + scanPeriod)
        + " with interval " + scanPeriod);
  }

  boolean newScanPeriod(long now) {
    return now > lastScanTime + scanPeriod;
  }

  private void clear() {
    diff.clear();
    totalBlocks = 0;
    missingMetaFile = 0;
    missingBlockFile = 0;
    missingMemoryBlocks = 0;
    mismatchBlocks = 0;
  }
  
  void shutdown() {
    reportCompileThreadPool.shutdown();
  }

  /**
   * Reconcile differences between disk and in-memory blocks
   */
  void reconcile() {
    scan();
    for (ScanInfo info : diff) {
      dataset.checkAndUpdate(info.getBlockId(), info.getBlockFile(), info
          .getMetaFile(), info.getVolume());
    }
  }

  /**
   * Scan for the differences between disk and in-memory blocks
   */
  void scan() {
    clear();
    ScanInfo[] diskReport = getDiskReport();
    totalBlocks = diskReport.length;

    // Hold FSDataset lock to prevent further changes to the block map
    synchronized(dataset) {
      Block[] memReport = dataset.getBlockList(false);
      Arrays.sort(memReport); // Sort based on blockId

      int d = 0; // index for diskReport
      int m = 0; // index for memReprot
      while (m < memReport.length && d < diskReport.length) {
        Block memBlock = memReport[Math.min(m, memReport.length - 1)];
        ScanInfo info = diskReport[Math.min(d, diskReport.length - 1)];
        if (info.getBlockId() < memBlock.getBlockId()) {
          // Block is missing in memory
          missingMemoryBlocks++;
          addDifference(info);
          d++;
          continue;
        }
        if (info.getBlockId() > memBlock.getBlockId()) {
          // Block is missing on the disk
          addDifference(memBlock.getBlockId());
          m++;
          continue;
        }
        // Block file and/or metadata file exists on the disk
        // Block exists in memory
        if (info.getBlockFile() == null) {
          // Block metadata file exits and block file is missing
          addDifference(info);
        } else if (info.getGenStamp() != memBlock.getGenerationStamp()
            || info.getBlockFile().length() != memBlock.getNumBytes()) {
          mismatchBlocks++;
          addDifference(info);
        }
        d++;
        m++;
      }
      while (m < memReport.length) {
        addDifference(memReport[m++].getBlockId());
      }
      while (d < diskReport.length) {
        missingMemoryBlocks++;
        addDifference(diskReport[d++]);
      }
    }
    LOG.info("Total blocks: " + totalBlocks + ", missing metadata files:"
        + missingMetaFile + ", missing block files:" + missingBlockFile
        + ", missing blocks in memory:" + missingMemoryBlocks
        + ", mismatched blocks:" + mismatchBlocks);
    lastScanTime = System.currentTimeMillis();
  }

  /**
   * Block is found on the disk. In-memory block is missing or does not match
   * the block on the disk
   */
  private void addDifference(ScanInfo info) {
    missingMetaFile += info.getMetaFile() == null ? 1 : 0;
    missingBlockFile += info.getBlockFile() == null ? 1 : 0;
    diff.add(info);
  }

  /** Block is not found on the disk */
  private void addDifference(long blockId) {
    missingBlockFile++;
    missingMetaFile++;
    diff.add(new ScanInfo(blockId));
  }

  /** Get list of blocks on the disk sorted by blockId */
  private ScanInfo[] getDiskReport() {
    // First get list of data directories
    FSDataset.FSVolume[] volumes = dataset.volumes.volumes;
    ArrayList<LinkedList<ScanInfo>> dirReports =
      new ArrayList<LinkedList<ScanInfo>>(volumes.length);
    
    Map<Integer, Future<LinkedList<ScanInfo>>> compilersInProgress =
      new HashMap<Integer, Future<LinkedList<ScanInfo>>>();
    for (int i = 0; i < volumes.length; i++) {
      if (!dataset.volumes.isValid(volumes[i])) { // volume is still valid
        dirReports.add(i, null);
      } else {
        ReportCompiler reportCompiler =
          new ReportCompiler(volumes[i], volumes[i].getDir());
        Future<LinkedList<ScanInfo>> result = 
          reportCompileThreadPool.submit(reportCompiler);
        compilersInProgress.put(i, result);
      }
    }
    
    for (Map.Entry<Integer, Future<LinkedList<ScanInfo>>> report :
      compilersInProgress.entrySet()) {
      try {
        dirReports.add(report.getKey(), report.getValue().get());
      } catch (Exception ex) {
        LOG.error("Error compiling report", ex);
        // Propagate ex to DataBlockScanner to deal with
        throw new RuntimeException(ex);
      }
    }

    // Compile consolidated report for all the volumes
    LinkedList<ScanInfo> list = new LinkedList<ScanInfo>();
    for (int i = 0; i < volumes.length; i++) {
      if (dataset.volumes.isValid(volumes[i])) { // volume is still valid
        list.addAll(dirReports.get(i));
      }
    }

    ScanInfo[] report = list.toArray(new ScanInfo[list.size()]);
    // Sort the report based on blockId
    Arrays.sort(report);
    return report;
  }

  private static boolean isBlockMetaFile(String blockId, String metaFile) {
    return metaFile.startsWith(blockId)
        && metaFile.endsWith(Block.METADATA_EXTENSION);
  }

  private static class ReportCompiler implements Callable<LinkedList<ScanInfo>> {
    private FSVolume volume;
    private File dir;

    public ReportCompiler(FSVolume volume, File dir) {
      this.dir = dir;
      this.volume = volume;
    }

    @Override
    public LinkedList<ScanInfo> call() throws Exception {
      LinkedList<ScanInfo> result = new LinkedList<ScanInfo>();
      compileReport(volume, dir, result);
      return result;
    }

    /** Compile list {@link ScanInfo} for the blocks in the directory <dir> */
    private LinkedList<ScanInfo> compileReport(FSVolume vol, File dir,
        LinkedList<ScanInfo> report) {
      File[] files = dir.listFiles();
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
