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

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.FSVolume;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.StringUtils;

/**
 * Performs two types of scanning:
 * <li> Gets block files from the data directories and reconciles the
 * difference between the blocks on the disk and in memory in
 * {@link FSDataset}</li>
 * <li> Scans the data directories for block files under a block pool
 * and verifies that the files are not corrupt</li>
 * This keeps track of blocks and their last verification times.
 * Currently it does not modify the metadata for block.
 */

class BlockPoolSliceScanner {
  
  public static final Log LOG = LogFactory.getLog(BlockPoolSliceScanner.class);
  
  private static final int MAX_SCAN_RATE = 8 * 1024 * 1024; // 8MB per sec
  private static final int MIN_SCAN_RATE = 1 * 1024 * 1024; // 1MB per sec
  
  static final long DEFAULT_SCAN_PERIOD_HOURS = 21*24L; // three weeks
  private final String blockPoolId;
  
  private static final String dateFormatString = "yyyy-MM-dd HH:mm:ss,SSS";
  
  static final String verificationLogFile = "dncp_block_verification.log";
  static final int verficationLogLimit = 5; // * numBlocks.

  private long scanPeriod = DEFAULT_SCAN_PERIOD_HOURS * 3600 * 1000;
  private DataNode datanode;
  private FSDataset dataset;
  
  // sorted set
  private TreeSet<BlockScanInfo> blockInfoSet;
  private HashMap<Block, BlockScanInfo> blockMap;
  
  // processedBlocks keeps track of which blocks are scanned
  // since the last run.
  private HashMap<Long, Integer> processedBlocks;
  
  private long totalScans = 0;
  private long totalScanErrors = 0;
  private long totalTransientErrors = 0;
  private long totalBlocksScannedInLastRun = 0; // Used for test only
  
  private long currentPeriodStart = System.currentTimeMillis();
  private long bytesLeft = 0; // Bytes to scan in this period
  private long totalBytesToScan = 0;
  
  private LogFileHandler verificationLog;
  
  private Random random = new Random();
  
  private DataTransferThrottler throttler = null;
  
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
    
    public int hashCode() {
      return block.hashCode();
    }
    
    public boolean equals(Object other) {
      return other instanceof BlockScanInfo &&
             compareTo((BlockScanInfo)other) == 0;
    }
    
    long getLastScanTime() {
      return (lastScanType == ScanType.NONE) ? 0 : lastScanTime;
    }
    
    public int compareTo(BlockScanInfo other) {
      long t1 = lastScanTime;
      long t2 = other.lastScanTime;
      return ( t1 < t2 ) ? -1 : 
                          (( t1 > t2 ) ? 1 : block.compareTo(other.block)); 
    }
  }
  
  BlockPoolSliceScanner(DataNode datanode, FSDataset dataset, Configuration conf,
      String bpid) {
    this.datanode = datanode;
    this.dataset = dataset;
    this.blockPoolId  = bpid;
    scanPeriod = conf.getInt("dfs.datanode.scan.period.hours", 0);
    if ( scanPeriod <= 0 ) {
      scanPeriod = DEFAULT_SCAN_PERIOD_HOURS;
    }
    scanPeriod *= 3600 * 1000;
    LOG.info("Periodic Block Verification scan initialized with interval " + scanPeriod + ".");
  }
  
  String getBlockPoolId() {
    return blockPoolId;
  }
  
  synchronized boolean isInitialized() {
    return throttler != null;
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

  void init() throws IOException {
    // get the list of blocks and arrange them in random order
    List<Block> arr = dataset.getFinalizedBlocks(blockPoolId);
    Collections.shuffle(arr);
    
    blockInfoSet = new TreeSet<BlockScanInfo>();
    blockMap = new HashMap<Block, BlockScanInfo>();
    
    long scanTime = -1;
    for (Block block : arr) {
      BlockScanInfo info = new BlockScanInfo( block );
      info.lastScanTime = scanTime--; 
      //still keep 'info.lastScanType' to NONE.
      addBlockInfo(info);
    }

    /* Pick the first directory that has any existing scanner log.
     * otherwise, pick the first directory.
     */
    File dir = null;
    List<FSVolume> volumes = dataset.volumes.getVolumes();
    for (FSDataset.FSVolume vol : dataset.volumes.getVolumes()) {
      File bpDir = vol.getBlockPoolSlice(blockPoolId).getDirectory();
      if (LogFileHandler.isFilePresent(bpDir, verificationLogFile)) {
        dir = bpDir;
        break;
      }
    }
    if (dir == null) {
      dir = volumes.get(0).getBlockPoolSlice(blockPoolId).getDirectory();
    }
    
    try {
      // max lines will be updated later during initialization.
      verificationLog = new LogFileHandler(dir, verificationLogFile, 100);
    } catch (IOException e) {
      LOG.warn("Could not open verfication log. " +
               "Verification times are not stored.");
    }
    
    synchronized (this) {
      throttler = new DataTransferThrottler(200, MAX_SCAN_RATE);
    }
  }

  private synchronized long getNewBlockScanTime() {
    /* If there are a lot of blocks, this returns a random time with in 
     * the scan period. Otherwise something sooner.
     */
    long period = Math.min(scanPeriod, 
                           Math.max(blockMap.size(),1) * 600 * 1000L);
    return System.currentTimeMillis() - scanPeriod + 
           random.nextInt((int)period);    
  }

  /** Adds block to list of blocks */
  synchronized void addBlock(ExtendedBlock block) {
    if (!isInitialized()) {
      return;
    }
    
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
    if (!isInitialized()) {
      return;
    }
    BlockScanInfo info = blockMap.get(block);
    if ( info != null ) {
      delBlockInfo(info);
    }
  }

  /** @return the last scan time */
  synchronized long getLastScanTime(Block block) {
    if (!isInitialized()) {
      return 0;
    }
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
    if (!isInitialized()) {
      return;
    }
    BlockScanInfo info = blockMap.get(block);
    
    if ( info != null ) {
      delBlockInfo(info);
    } else {
      // It might already be removed. Thats ok, it will be caught next time.
      info = new BlockScanInfo(block);
    }
    
    long now = System.currentTimeMillis();
    info.lastScanType = type;
    info.lastScanTime = now;
    info.lastScanOk = scanOk;
    addBlockInfo(info);
        
    // Don't update meta data if the verification failed.
    if (!scanOk) {
      return;
    }
    
    LogFileHandler log = verificationLog;
    if (log != null) {
      log.appendLine(now, block.getGenerationStamp(), block.getBlockId());
    }
  }
  
  private void handleScanFailure(ExtendedBlock block) {
    LOG.info("Reporting bad block " + block);
    try {
      datanode.reportBadBlocks(block);
    } catch (IOException ie) {
      // it is bad, but not bad enough to shutdown the scanner
      LOG.warn("Cannot report bad block=" + block.getBlockId());
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
    long timeLeft = currentPeriodStart+scanPeriod - System.currentTimeMillis();
    long bw = Math.max(bytesLeft*1000/timeLeft, MIN_SCAN_RATE);
    throttler.setBandwidth(Math.min(bw, MAX_SCAN_RATE));
  }
  
  private void verifyBlock(ExtendedBlock block) {
    BlockSender blockSender = null;

    /* In case of failure, attempt to read second time to reduce
     * transient errors. How do we flush block data from kernel 
     * buffers before the second read? 
     */
    for (int i=0; i<2; i++) {
      boolean second = (i > 0);
      
      try {
        adjustThrottler();
        
        blockSender = new BlockSender(block, 0, -1, false, false, true,
            datanode);

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
        if ( dataset.getFile(block.getBlockPoolId(), block.getLocalBlock()) == null ) {
          LOG.info("Verification failed for " + block + ". Its ok since " +
          "it not in datanode dataset anymore.");
          deleteBlock(block.getLocalBlock());
          return;
        }

        LOG.warn((second ? "Second " : "First ") + 
                 "Verification failed for " + block + ". Exception : " +
                 StringUtils.stringifyException(e));
        
        if (second) {
          totalScanErrors++;
          datanode.getMetrics().blockVerificationFailures.inc(); 
          handleScanFailure(block);
          return;
        } 
      } finally {
        IOUtils.closeStream(blockSender);
        datanode.getMetrics().blocksVerified.inc();
        totalScans++;
      }
    }
  }
  
  private synchronized long getEarliestScanTime() {
    if ( blockInfoSet.size() > 0 ) {
      return blockInfoSet.first().lastScanTime;
    }
    return Long.MAX_VALUE; 
  }
  
  private synchronized boolean isFirstBlockProcessed() {
    if (blockInfoSet.size() > 0 ) {
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
      if ( blockInfoSet.size() > 0 ) {
        block = blockInfoSet.first().block;
      }
    }
    if ( block != null ) {
      verifyBlock(new ExtendedBlock(blockPoolId, block));
      processedBlocks.put(block.getBlockId(), 1);
    }
  }
  
  // Used for tests only
  long getBlocksScannedInLastRun() {
    return totalBlocksScannedInLastRun;
  }

  /**
   * Reads the current and previous log files (if any) and marks the blocks
   * processed if they were processed within last scan period. Copies the log
   * records of recently scanned blocks from previous to current file. 
   * Returns false if the process was interrupted because the thread is marked 
   * to exit.
   */
  private boolean assignInitialVerificationTimes() {
    int numBlocks = 1;
    LogFileHandler log = null;
    synchronized (this) {
      log = verificationLog;
      numBlocks = Math.max(blockMap.size(), 1);
    }

    long now = System.currentTimeMillis();
    LogFileHandler.Reader logReader[] = new LogFileHandler.Reader[2];
    try {
      if (log != null) {
        logReader[0] = log.getCurrentFileReader();
        logReader[1] = log.getPreviousFileReader();
      }
    } catch (IOException e) {
      LOG.warn("Could not read previous verification times : " +
               StringUtils.stringifyException(e));
    }
    
    try {
      for (LogFileHandler.Reader reader : logReader) {
      // update verification times from the verificationLog.
        while (logReader != null && reader.hasNext()) {
          if (!datanode.shouldRun
              || datanode.blockScanner.blockScannerThread.isInterrupted()) {
            return false;
          }
          LogEntry entry = LogEntry.parseEntry(reader.next());
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
                if (reader.file == log.prevFile) {
                  // write the log entry to current file
                  // so that the entry is preserved for later runs.
                  log.appendLine(entry.verificationTime, entry.genStamp,
                      entry.blockId);
                }
              }
            }
          }
        }
      }
    } finally {
      IOUtils.closeStream(logReader[0]);
      IOUtils.closeStream(logReader[1]);
    }
    
    /* Initially spread the block reads over half of 
     * MIN_SCAN_PERIOD so that we don't keep scanning the 
     * blocks too quickly when restarted.
     */
    long verifyInterval = (long) (Math.min( scanPeriod/2.0/numBlocks,
                                            10*60*1000 ));
    long lastScanTime = System.currentTimeMillis() - scanPeriod;
    
    /* Before this loop, entries in blockInfoSet that are not
     * updated above have lastScanTime of <= 0 . Loop until first entry has
     * lastModificationTime > 0.
     */    
    synchronized (this) {
      if (blockInfoSet.size() > 0 ) {
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

  static File getCurrentFile(FSVolume vol, String bpid) throws IOException {
    return LogFileHandler.getCurrentFile(vol.getBlockPoolSlice(bpid).getDirectory(),
        BlockPoolSliceScanner.verificationLogFile);
  }
  
  private synchronized void startNewPeriod() {
    LOG.info("Starting a new period : work left in prev period : "
        + String.format("%.2f%%", totalBytesToScan == 0 ? 0
            : (bytesLeft * 100.0) / totalBytesToScan));

    // reset the byte counts :
    bytesLeft = totalBytesToScan;
    currentPeriodStart = System.currentTimeMillis();
  }
  
  void scanBlockPoolSlice() {
    startNewPeriod();
    if (processedBlocks != null) {
      totalBlocksScannedInLastRun = processedBlocks.size();
    }
    // Create a new processedBlocks structure
    processedBlocks = new HashMap<Long, Integer>();
    if (verificationLog != null) {
      try {
        verificationLog.openCurFile();
      } catch (FileNotFoundException ex) {
        LOG.warn("Could not open current file");
      }
    }
    if (!assignInitialVerificationTimes()) {
      return;
    }
    // Start scanning
    scan();
  }
  
  public void scan() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Starting to scan blockpool: " + blockPoolId);
    }
    try {
      adjustThrottler();
        
      while (datanode.shouldRun && !Thread.interrupted()
          && datanode.isBPServiceAlive(blockPoolId)) {
        long now = System.currentTimeMillis();
        synchronized (this) {
          if ( now >= (currentPeriodStart + scanPeriod)) {
            startNewPeriod();
          }
        }
        if (((now - getEarliestScanTime()) >= scanPeriod)
            || (!(this.isFirstBlockProcessed()))) {
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
      LOG.warn("RuntimeException during BlockPoolScanner.scan() : " +
               StringUtils.stringifyException(e));
      throw e;
    } finally {
      cleanUp();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Done scanning block pool: " + blockPoolId);
      }
    }
  }
  
  private synchronized void cleanUp() {
    if (verificationLog != null) {
      try {
        verificationLog.roll();
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
    
    DateFormat dateFormat = new SimpleDateFormat(dateFormatString);
    
    int total = blockInfoSet.size();
    
    long now = System.currentTimeMillis();
    
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
                                    "%-15d %s\n", info.block, 
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
                         
    buffer.append(String.format("\nTotal Blocks                 : %6d" +
                                "\nVerified in last hour        : %6d" +
                                "\nVerified in last day         : %6d" +
                                "\nVerified in last week        : %6d" +
                                "\nVerified in last four weeks  : %6d" +
                                "\nVerified in SCAN_PERIOD      : %6d" +
                                "\nNot yet verified             : %6d" +
                                "\nVerified since restart       : %6d" +
                                "\nScans since restart          : %6d" +
                                "\nScan errors since restart    : %6d" +
                                "\nTransient scan errors        : %6d" +
                                "\nCurrent scan rate limit KBps : %6d" +
                                "\nProgress this period         : %6.0f%%" +
                                "\nTime left in cur period      : %6.2f%%" +
                                "\n", 
                                total, inOneHour, inOneDay, inOneWeek,
                                inFourWeeks, inScanPeriod, neverScanned,
                                totalScans, totalScans, 
                                totalScanErrors, totalTransientErrors, 
                                Math.round(throttler.getBandwidth()/1024.0),
                                pctProgress, pctPeriodLeft));
  }
  
  /**
   * This class takes care of log file used to store the last verification
   * times of the blocks. It rolls the current file when it is too big etc.
   * If there is an error while writing, it stops updating with an error
   * message.
   */
  private static class LogFileHandler {
    
    private static final String curFileSuffix = ".curr";
    private static final String prevFileSuffix = ".prev";
    private final DateFormat dateFormat = new SimpleDateFormat(dateFormatString);
    
    static File getCurrentFile(File dir, String filePrefix) {
      return new File(dir, filePrefix + curFileSuffix);
    }
    
    public Reader getPreviousFileReader() throws IOException {
      return new Reader(prevFile);
    }
    
    public Reader getCurrentFileReader() throws IOException {
      return new Reader(curFile);
    }

    static boolean isFilePresent(File dir, String filePrefix) {
      return new File(dir, filePrefix + curFileSuffix).exists() ||
             new File(dir, filePrefix + prevFileSuffix).exists();
    }
    private File curFile;
    private File prevFile;
    
    private PrintStream out;
        
    /**
     * Opens the log file for appending.
     * Note that rolling will happen only after "updateLineCount()" is 
     * called. This is so that line count could be updated in a separate
     * thread without delaying start up.
     * 
     * @param dir where the logs files are located.
     * @param filePrefix prefix of the file.
     * @param maxNumLines max lines in a file (its a soft limit).
     * @throws IOException
     */
    LogFileHandler(File dir, String filePrefix, int maxNumLines) 
                                                throws IOException {
      curFile = new File(dir, filePrefix + curFileSuffix);
      prevFile = new File(dir, filePrefix + prevFileSuffix);
    }
    
    /**
     * Append "\n" + line.
     * If the log file need to be rolled, it will done after 
     * appending the text.
     * This does not throw IOException when there is an error while 
     * appending. Currently does not throw an error even if rolling 
     * fails (may be it should?).
     * return true if append was successful.
     */
    synchronized boolean appendLine(String line) {
      if (out == null) {
        return false;
      }
      out.println();
      out.print(line);
      return true;
    }
    
    boolean appendLine(long verificationTime, long genStamp, long blockId) {
      return appendLine("date=\""
          + dateFormat.format(new Date(verificationTime)) + "\"\t " + "time=\""
          + verificationTime + "\"\t " + "genstamp=\"" + genStamp + "\"\t "
          + "id=\"" + blockId + "\"");
    }
    
    private synchronized void openCurFile() throws FileNotFoundException {
      close();
      out = new PrintStream(new FileOutputStream(curFile, true));
    }
    
    private void roll() throws IOException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Rolling current file: " + curFile.getAbsolutePath()
            + " to previous file: " + prevFile.getAbsolutePath());
      }

      if (!prevFile.delete() && prevFile.exists()) {
        throw new IOException("Could not delete " + prevFile);
      }
      
      close();

      if (!curFile.renameTo(prevFile)) {
        throw new IOException("Could not rename " + curFile + 
                              " to " + prevFile);
      }
    }
    
    synchronized void close() {
      if (out != null) {
        out.close();
        out = null;
      }
    }
    
    /**
     * This is used to read the lines in order.
     * If the data is not read completely (i.e, untill hasNext() returns
     * false), it needs to be explicitly 
     */
    private static class Reader implements Iterator<String>, Closeable {
      
      BufferedReader reader;
      File file;
      String line;
      boolean closed = false;
      
      private Reader(File file) throws IOException {
        reader = null;
        this.file = file;
        readNext();        
      }
      
      private boolean openFile() throws IOException {
        if (file == null) {
          return false;
        }       
        if (reader != null ) {
          reader.close();
          reader = null;
        }
        if (file.exists()) {
          reader = new BufferedReader(new FileReader(file));
          return true;
        } else {
          return false;
        }
      }
      
      // read next line if possible.
      private void readNext() throws IOException {
        line = null;
        if (reader == null) {
          openFile();
        }
        try {
          if (reader != null && (line = reader.readLine()) != null) {
            return;
          }
        } finally {
          if (!hasNext()) {
            close();
          }
        }
      }
      
      public boolean hasNext() {
        return line != null;
      }

      public String next() {
        String curLine = line;
        try {
          readNext();
        } catch (IOException e) {
          LOG.info("Could not reade next line in LogHandler : " +
                   StringUtils.stringifyException(e));
        }
        return curLine;
      }

      public void remove() {
        throw new RuntimeException("remove() is not supported.");
      }

      public void close() throws IOException {
        if (!closed) {
          try {
            if (reader != null) {
              reader.close();
            }
          } finally {
            file = null;
            reader = null;
            closed = true;
          }
        }
      }
    } 
  }
}
