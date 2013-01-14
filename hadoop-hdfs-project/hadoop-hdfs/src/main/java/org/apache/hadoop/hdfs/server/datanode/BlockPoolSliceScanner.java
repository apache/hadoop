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

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.RollingLogs;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;

/**
 * Scans the block files under a block pool and verifies that the
 * files are not corrupt.
 * This keeps track of blocks and their last verification times.
 * Currently it does not modify the metadata for block.
 */

class BlockPoolSliceScanner {
  
  public static final Log LOG = LogFactory.getLog(BlockPoolSliceScanner.class);
  
  private static final String DATA_FORMAT = "yyyy-MM-dd HH:mm:ss,SSS";

  private static final int MAX_SCAN_RATE = 8 * 1024 * 1024; // 8MB per sec
  private static final int MIN_SCAN_RATE = 1 * 1024 * 1024; // 1MB per sec
  private static final long DEFAULT_SCAN_PERIOD_HOURS = 21*24L; // three weeks

  private static final String VERIFICATION_PREFIX = "dncp_block_verification.log";

  private final String blockPoolId;
  private final long scanPeriod;
  private final AtomicLong lastScanTime = new AtomicLong();

  private final DataNode datanode;
  private final FsDatasetSpi<? extends FsVolumeSpi> dataset;
  
  private final SortedSet<BlockScanInfo> blockInfoSet
      = new TreeSet<BlockScanInfo>();
  private final Map<Block, BlockScanInfo> blockMap
      = new HashMap<Block, BlockScanInfo>();
  
  // processedBlocks keeps track of which blocks are scanned
  // since the last run.
  private volatile HashMap<Long, Integer> processedBlocks;
  
  private long totalScans = 0;
  private long totalScanErrors = 0;
  private long totalTransientErrors = 0;
  private final AtomicInteger totalBlocksScannedInLastRun = new AtomicInteger(); // Used for test only
  
  private long currentPeriodStart = Time.now();
  private long bytesLeft = 0; // Bytes to scan in this period
  private long totalBytesToScan = 0;
  
  private final LogFileHandler verificationLog;
  
  private final DataTransferThrottler throttler = new DataTransferThrottler(
       200, MAX_SCAN_RATE);
  
  private static enum ScanType {
    VERIFICATION_SCAN,     // scanned as part of periodic verfication
    NONE,
  }
  
  static class BlockScanInfo implements Comparable<BlockScanInfo> {
    Block block;
    long lastScanTime = 0;
    ScanType lastScanType = ScanType.NONE; 
    boolean lastScanOk = true;
    
    BlockScanInfo(Block block) {
      this.block = block;
    }
    
    @Override
    public int hashCode() {
      return block.hashCode();
    }
    
    @Override
    public boolean equals(Object other) {
      return other instanceof BlockScanInfo &&
             compareTo((BlockScanInfo)other) == 0;
    }
    
    long getLastScanTime() {
      return (lastScanType == ScanType.NONE) ? 0 : lastScanTime;
    }
    
    @Override
    public int compareTo(BlockScanInfo other) {
      long t1 = lastScanTime;
      long t2 = other.lastScanTime;
      return ( t1 < t2 ) ? -1 : 
                          (( t1 > t2 ) ? 1 : block.compareTo(other.block)); 
    }
  }
  
  BlockPoolSliceScanner(String bpid, DataNode datanode,
      FsDatasetSpi<? extends FsVolumeSpi> dataset, Configuration conf) {
    this.datanode = datanode;
    this.dataset = dataset;
    this.blockPoolId  = bpid;
    
    long hours = conf.getInt(DFSConfigKeys.DFS_DATANODE_SCAN_PERIOD_HOURS_KEY, 
                             DFSConfigKeys.DFS_DATANODE_SCAN_PERIOD_HOURS_DEFAULT);
    if (hours <= 0) {
      hours = DEFAULT_SCAN_PERIOD_HOURS;
    }
    this.scanPeriod = hours * 3600 * 1000;
    LOG.info("Periodic Block Verification Scanner initialized with interval "
        + hours + " hours for block pool " + bpid);

    // get the list of blocks and arrange them in random order
    List<Block> arr = dataset.getFinalizedBlocks(blockPoolId);
    Collections.shuffle(arr);
    
    long scanTime = -1;
    for (Block block : arr) {
      BlockScanInfo info = new BlockScanInfo( block );
      info.lastScanTime = scanTime--; 
      //still keep 'info.lastScanType' to NONE.
      addBlockInfo(info);
    }

    RollingLogs rollingLogs = null;
    try {
       rollingLogs = dataset.createRollingLogs(blockPoolId, VERIFICATION_PREFIX);
    } catch (IOException e) {
      LOG.warn("Could not open verfication log. " +
               "Verification times are not stored.");
    }
    verificationLog = rollingLogs == null? null: new LogFileHandler(rollingLogs);
  }
  
  String getBlockPoolId() {
    return blockPoolId;
  }
  
  private void updateBytesToScan(long len, long lastScanTime) {
    // len could be negative when a block is deleted.
    totalBytesToScan += len;
    if ( lastScanTime < currentPeriodStart ) {
      bytesLeft += len;
    }
    // Should we change throttler bandwidth every time bytesLeft changes?
    // not really required.
  }
  
  private synchronized void addBlockInfo(BlockScanInfo info) {
    boolean added = blockInfoSet.add(info);
    blockMap.put(info.block, info);
    
    if (added) {
      updateBytesToScan(info.block.getNumBytes(), info.lastScanTime);
    }
  }
  
  private synchronized void delBlockInfo(BlockScanInfo info) {
    boolean exists = blockInfoSet.remove(info);
    blockMap.remove(info.block);

    if (exists) {
      updateBytesToScan(-info.block.getNumBytes(), info.lastScanTime);
    }
  }
  
  /** Update blockMap by the given LogEntry */
  private synchronized void updateBlockInfo(LogEntry e) {
    BlockScanInfo info = blockMap.get(new Block(e.blockId, 0, e.genStamp));
    
    if(info != null && e.verificationTime > 0 && 
        info.lastScanTime < e.verificationTime) {
      delBlockInfo(info);
      info.lastScanTime = e.verificationTime;
      info.lastScanType = ScanType.VERIFICATION_SCAN;
      addBlockInfo(info);
    }
  }

  private synchronized long getNewBlockScanTime() {
    /* If there are a lot of blocks, this returns a random time with in 
     * the scan period. Otherwise something sooner.
     */
    long period = Math.min(scanPeriod, 
                           Math.max(blockMap.size(),1) * 600 * 1000L);
    int periodInt = Math.abs((int)period);
    return Time.now() - scanPeriod + 
        DFSUtil.getRandom().nextInt(periodInt);
  }

  /** Adds block to list of blocks */
  synchronized void addBlock(ExtendedBlock block) {
    BlockScanInfo info = blockMap.get(block.getLocalBlock());
    if ( info != null ) {
      LOG.warn("Adding an already existing block " + block);
      delBlockInfo(info);
    }
    
    info = new BlockScanInfo(block.getLocalBlock());    
    info.lastScanTime = getNewBlockScanTime();
    
    addBlockInfo(info);
    adjustThrottler();
  }
  
  /** Deletes the block from internal structures */
  synchronized void deleteBlock(Block block) {
    BlockScanInfo info = blockMap.get(block);
    if ( info != null ) {
      delBlockInfo(info);
    }
  }

  @VisibleForTesting
  long getTotalScans() {
    return totalScans;
  }

  /** @return the last scan time for the block pool. */
  long getLastScanTime() {
    return lastScanTime.get();
  }

  /** @return the last scan time the given block. */
  synchronized long getLastScanTime(Block block) {
    BlockScanInfo info = blockMap.get(block);
    return info == null? 0: info.lastScanTime;
  }

  /** Deletes blocks from internal structures */
  void deleteBlocks(Block[] blocks) {
    for ( Block b : blocks ) {
      deleteBlock(b);
    }
  }
  
  private synchronized void updateScanStatus(Block block, 
                                             ScanType type,
                                             boolean scanOk) {
    BlockScanInfo info = blockMap.get(block);
    
    if ( info != null ) {
      delBlockInfo(info);
    } else {
      // It might already be removed. Thats ok, it will be caught next time.
      info = new BlockScanInfo(block);
    }
    
    long now = Time.now();
    info.lastScanType = type;
    info.lastScanTime = now;
    info.lastScanOk = scanOk;
    addBlockInfo(info);
        
    // Don't update meta data if the verification failed.
    if (!scanOk) {
      return;
    }
    
    if (verificationLog != null) {
      verificationLog.append(now, block.getGenerationStamp(),
          block.getBlockId());
    }
  }
  
  private void handleScanFailure(ExtendedBlock block) {
    LOG.info("Reporting bad " + block);
    try {
      datanode.reportBadBlocks(block);
    } catch (IOException ie) {
      // it is bad, but not bad enough to shutdown the scanner
      LOG.warn("Cannot report bad " + block.getBlockId());
    }
  }
  
  static private class LogEntry {

    long blockId = -1;
    long verificationTime = -1;
    long genStamp = GenerationStamp.GRANDFATHER_GENERATION_STAMP;
    
    /**
     * The format consists of single line with multiple entries. each 
     * entry is in the form : name="value".
     * This simple text and easily extendable and easily parseable with a
     * regex.
     */
    private static Pattern entryPattern = 
      Pattern.compile("\\G\\s*([^=\\p{Space}]+)=\"(.*?)\"\\s*");
    
    static String toString(long verificationTime, long genStamp, long blockId,
        DateFormat dateFormat) {
      return "\ndate=\"" + dateFormat.format(new Date(verificationTime))
          + "\"\t time=\"" + verificationTime
          + "\"\t genstamp=\"" + genStamp
          + "\"\t id=\"" + blockId + "\"";
    }

    static LogEntry parseEntry(String line) {
      LogEntry entry = new LogEntry();
      
      Matcher matcher = entryPattern.matcher(line);
      while (matcher.find()) {
        String name = matcher.group(1);
        String value = matcher.group(2);
        
        try {
          if (name.equals("id")) {
            entry.blockId = Long.valueOf(value);
          } else if (name.equals("time")) {
            entry.verificationTime = Long.valueOf(value);
          } else if (name.equals("genstamp")) {
            entry.genStamp = Long.valueOf(value);
          }
        } catch(NumberFormatException nfe) {
          LOG.warn("Cannot parse line: " + line, nfe);
          return null;
        }
      }
      
      return entry;
    }
  }
  
  private synchronized void adjustThrottler() {
    long timeLeft = currentPeriodStart+scanPeriod - Time.now();
    long bw = Math.max(bytesLeft*1000/timeLeft, MIN_SCAN_RATE);
    throttler.setBandwidth(Math.min(bw, MAX_SCAN_RATE));
  }
  
  @VisibleForTesting
  void verifyBlock(ExtendedBlock block) {
    BlockSender blockSender = null;

    /* In case of failure, attempt to read second time to reduce
     * transient errors. How do we flush block data from kernel 
     * buffers before the second read? 
     */
    for (int i=0; i<2; i++) {
      boolean second = (i > 0);
      
      try {
        adjustThrottler();
        
        blockSender = new BlockSender(block, 0, -1, false, true, true, 
            datanode, null);

        DataOutputStream out = 
                new DataOutputStream(new IOUtils.NullOutputStream());
        
        blockSender.sendBlock(out, null, throttler);

        LOG.info((second ? "Second " : "") +
                 "Verification succeeded for " + block);
        
        if ( second ) {
          totalTransientErrors++;
        }
        
        updateScanStatus(block.getLocalBlock(), ScanType.VERIFICATION_SCAN, true);

        return;
      } catch (IOException e) {
        updateScanStatus(block.getLocalBlock(), ScanType.VERIFICATION_SCAN, false);

        // If the block does not exists anymore, then its not an error
        if (!dataset.contains(block)) {
          LOG.info(block + " is no longer in the dataset");
          deleteBlock(block.getLocalBlock());
          return;
        }

        // If the block exists, the exception may due to a race with write:
        // The BlockSender got an old block path in rbw. BlockReceiver removed
        // the rbw block from rbw to finalized but BlockSender tried to open the
        // file before BlockReceiver updated the VolumeMap. The state of the
        // block can be changed again now, so ignore this error here. If there
        // is a block really deleted by mistake, DirectoryScan should catch it.
        if (e instanceof FileNotFoundException ) {
          LOG.info("Verification failed for " + block +
              " - may be due to race with write");
          deleteBlock(block.getLocalBlock());
          return;
        }

        LOG.warn((second ? "Second " : "First ") + "Verification failed for "
            + block, e);
        
        if (second) {
          totalScanErrors++;
          datanode.getMetrics().incrBlockVerificationFailures();
          handleScanFailure(block);
          return;
        } 
      } finally {
        IOUtils.closeStream(blockSender);
        datanode.getMetrics().incrBlocksVerified();
        totalScans++;
      }
    }
  }
  
  private synchronized long getEarliestScanTime() {
    if (!blockInfoSet.isEmpty()) {
      return blockInfoSet.first().lastScanTime;
    }
    return Long.MAX_VALUE; 
  }
  
  private synchronized boolean isFirstBlockProcessed() {
    if (!blockInfoSet.isEmpty()) {
      long blockId = blockInfoSet.first().block.getBlockId();
      if ((processedBlocks.get(blockId) != null)
          && (processedBlocks.get(blockId) == 1)) {
        return true;
      }
    }
    return false;
  }
  
  // Picks one block and verifies it
  private void verifyFirstBlock() {
    Block block = null;
    synchronized (this) {
      if (!blockInfoSet.isEmpty()) {
        block = blockInfoSet.first().block;
      }
    }
    if ( block != null ) {
      verifyBlock(new ExtendedBlock(blockPoolId, block));
      processedBlocks.put(block.getBlockId(), 1);
    }
  }
  
  // Used for tests only
  int getBlocksScannedInLastRun() {
    return totalBlocksScannedInLastRun.get();
  }

  /**
   * Reads the current and previous log files (if any) and marks the blocks
   * processed if they were processed within last scan period. Copies the log
   * records of recently scanned blocks from previous to current file. 
   * Returns false if the process was interrupted because the thread is marked 
   * to exit.
   */
  private boolean assignInitialVerificationTimes() {
    //First updates the last verification times from the log file.
    if (verificationLog != null) {
      long now = Time.now();
      RollingLogs.LineIterator logIterator = null;
      try {
        logIterator = verificationLog.logs.iterator(false);
        // update verification times from the verificationLog.
        while (logIterator.hasNext()) {
          if (!datanode.shouldRun
              || datanode.blockScanner.blockScannerThread.isInterrupted()) {
            return false;
          }
          LogEntry entry = LogEntry.parseEntry(logIterator.next());
          if (entry != null) {
            updateBlockInfo(entry);
            if (now - entry.verificationTime < scanPeriod) {
              BlockScanInfo info = blockMap.get(new Block(entry.blockId, 0,
                  entry.genStamp));
              if (info != null) {
                if (processedBlocks.get(entry.blockId) == null) {
                  updateBytesLeft(-info.block.getNumBytes());
                  processedBlocks.put(entry.blockId, 1);
                }
                if (logIterator.isPrevious()) {
                  // write the log entry to current file
                  // so that the entry is preserved for later runs.
                  verificationLog.append(entry.verificationTime, entry.genStamp,
                      entry.blockId);
                }
              }
            }
          }
        }
      } catch (IOException e) {
        LOG.warn("Failed to read previous verification times.", e);
      } finally {
        IOUtils.closeStream(logIterator);
      }
    }
    
    
    /* Before this loop, entries in blockInfoSet that are not
     * updated above have lastScanTime of <= 0 . Loop until first entry has
     * lastModificationTime > 0.
     */    
    synchronized (this) {
      final int numBlocks = Math.max(blockMap.size(), 1);
      // Initially spread the block reads over half of scan period
      // so that we don't keep scanning the blocks too quickly when restarted.
      long verifyInterval = Math.min(scanPeriod/(2L * numBlocks), 10*60*1000L);
      long lastScanTime = Time.now() - scanPeriod;

      if (!blockInfoSet.isEmpty()) {
        BlockScanInfo info;
        while ((info =  blockInfoSet.first()).lastScanTime < 0) {
          delBlockInfo(info);        
          info.lastScanTime = lastScanTime;
          lastScanTime += verifyInterval;
          addBlockInfo(info);
        }
      }
    }
    
    return true;
  }
  
  private synchronized void updateBytesLeft(long len) {
    bytesLeft += len;
  }
  
  private synchronized void startNewPeriod() {
    LOG.info("Starting a new period : work left in prev period : "
        + String.format("%.2f%%", totalBytesToScan == 0 ? 0
            : (bytesLeft * 100.0) / totalBytesToScan));

    // reset the byte counts :
    bytesLeft = totalBytesToScan;
    currentPeriodStart = Time.now();
  }
  
  private synchronized boolean workRemainingInCurrentPeriod() {
    if (bytesLeft <= 0 && Time.now() < currentPeriodStart + scanPeriod) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipping scan since bytesLeft=" + bytesLeft + ", Start=" +
                  currentPeriodStart + ", period=" + scanPeriod + ", now=" +
                  Time.now() + " " + blockPoolId);
      }
      return false;
    } else {
      return true;
    }
  }

  void scanBlockPoolSlice() {
    if (!workRemainingInCurrentPeriod()) {
      return;
    }

    // Create a new processedBlocks structure
    processedBlocks = new HashMap<Long, Integer>();
    if (!assignInitialVerificationTimes()) {
      return;
    }
    // Start scanning
    try {
      scan();
    } finally {
      totalBlocksScannedInLastRun.set(processedBlocks.size());
      lastScanTime.set(Time.now());
    }
  }

  /**
   * Shuts down this BlockPoolSliceScanner and releases any internal resources.
   */
  void shutdown() {
    if (verificationLog != null) {
      verificationLog.close();
    }
  }
  
  private void scan() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Starting to scan blockpool: " + blockPoolId);
    }
    try {
      adjustThrottler();
        
      while (datanode.shouldRun
          && !datanode.blockScanner.blockScannerThread.isInterrupted()
          && datanode.isBPServiceAlive(blockPoolId)) {
        long now = Time.now();
        synchronized (this) {
          if ( now >= (currentPeriodStart + scanPeriod)) {
            startNewPeriod();
          }
        }
        if (((now - getEarliestScanTime()) >= scanPeriod)
            || ((!blockInfoSet.isEmpty()) && !(this.isFirstBlockProcessed()))) {
          verifyFirstBlock();
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("All remaining blocks were processed recently, "
                + "so this run is complete");
          }
          break;
        }
      }
    } catch (RuntimeException e) {
      LOG.warn("RuntimeException during BlockPoolScanner.scan()", e);
      throw e;
    } finally {
      rollVerificationLogs();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Done scanning block pool: " + blockPoolId);
      }
    }
  }
  
  private synchronized void rollVerificationLogs() {
    if (verificationLog != null) {
      try {
        verificationLog.logs.roll();
      } catch (IOException ex) {
        LOG.warn("Received exception: ", ex);
        verificationLog.close();
      }
    }
  }

  
  synchronized void printBlockReport(StringBuilder buffer, 
                                     boolean summaryOnly) {
    long oneHour = 3600*1000;
    long oneDay = 24*oneHour;
    long oneWeek = 7*oneDay;
    long fourWeeks = 4*oneWeek;
    
    int inOneHour = 0;
    int inOneDay = 0;
    int inOneWeek = 0;
    int inFourWeeks = 0;
    int inScanPeriod = 0;
    int neverScanned = 0;
    
    DateFormat dateFormat = new SimpleDateFormat(DATA_FORMAT);
    
    int total = blockInfoSet.size();
    
    long now = Time.now();
    
    Date date = new Date();
    
    for(Iterator<BlockScanInfo> it = blockInfoSet.iterator(); it.hasNext();) {
      BlockScanInfo info = it.next();
      
      long scanTime = info.getLastScanTime();
      long diff = now - scanTime;
      
      if (diff <= oneHour) inOneHour++;
      if (diff <= oneDay) inOneDay++;
      if (diff <= oneWeek) inOneWeek++;
      if (diff <= fourWeeks) inFourWeeks++;
      if (diff <= scanPeriod) inScanPeriod++;      
      if (scanTime <= 0) neverScanned++;
      
      if (!summaryOnly) {
        date.setTime(scanTime);
        String scanType = 
          (info.lastScanType == ScanType.VERIFICATION_SCAN) ? "local" : "none"; 
        buffer.append(String.format("%-26s : status : %-6s type : %-6s" +
                                    " scan time : " +
                                    "%-15d %s%n", info.block, 
                                    (info.lastScanOk ? "ok" : "failed"),
                                    scanType, scanTime,
                                    (scanTime <= 0) ? "not yet verified" : 
                                      dateFormat.format(date)));
      }
    }
    
    double pctPeriodLeft = (scanPeriod + currentPeriodStart - now)
                           *100.0/scanPeriod;
    double pctProgress = (totalBytesToScan == 0) ? 100 :
                         (totalBytesToScan-bytesLeft)*100.0/totalBytesToScan;
                         
    buffer.append(String.format("%nTotal Blocks                 : %6d" +
                                "%nVerified in last hour        : %6d" +
                                "%nVerified in last day         : %6d" +
                                "%nVerified in last week        : %6d" +
                                "%nVerified in last four weeks  : %6d" +
                                "%nVerified in SCAN_PERIOD      : %6d" +
                                "%nNot yet verified             : %6d" +
                                "%nVerified since restart       : %6d" +
                                "%nScans since restart          : %6d" +
                                "%nScan errors since restart    : %6d" +
                                "%nTransient scan errors        : %6d" +
                                "%nCurrent scan rate limit KBps : %6d" +
                                "%nProgress this period         : %6.0f%%" +
                                "%nTime left in cur period      : %6.2f%%" +
                                "%n", 
                                total, inOneHour, inOneDay, inOneWeek,
                                inFourWeeks, inScanPeriod, neverScanned,
                                totalScans, totalScans, 
                                totalScanErrors, totalTransientErrors, 
                                Math.round(throttler.getBandwidth()/1024.0),
                                pctProgress, pctPeriodLeft));
  }
  
  /**
   * This class takes care of log file used to store the last verification
   * times of the blocks.
   */
  private static class LogFileHandler {
    private final DateFormat dateFormat = new SimpleDateFormat(DATA_FORMAT);

    private final RollingLogs logs;

    private LogFileHandler(RollingLogs logs)  {
      this.logs = logs;
    }

    void append(long verificationTime, long genStamp, long blockId) {
      final String m = LogEntry.toString(verificationTime, genStamp, blockId,
          dateFormat);
      try {
        logs.appender().append(m);
      } catch (IOException e) {
        LOG.warn("Failed to append to " + logs + ", m=" + m, e);
      }
    }

    void close() {
      try {
        logs.appender().close();
      } catch (IOException e) {
        LOG.warn("Failed to close the appender of " + logs, e);
      }
    }
  }
}
