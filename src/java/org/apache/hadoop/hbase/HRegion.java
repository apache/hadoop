/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;

import java.io.*;
import java.util.*;

/**
 * HRegion stores data for a certain region of a table.  It stores all columns
 * for each row. A given table consists of one or more HRegions.
 *
 * <p>We maintain multiple HStores for a single HRegion.
 * 
 * <p>An HStore is a set of rows with some column data; together,
 * they make up all the data for the rows.  
 *
 * <p>Each HRegion has a 'startKey' and 'endKey'.
 *   
 * <p>The first is inclusive, the second is exclusive (except for
 * the final region)  The endKey of region 0 is the same as
 * startKey for region 1 (if it exists).  The startKey for the
 * first region is null. The endKey for the final region is null.
 *
 * <p>The HStores have no locking built-in.  All row-level locking
 * and row-level atomicity is provided by the HRegion.
 * 
 * <p>An HRegion is defined by its table and its key extent.
 * 
 * <p>It consists of at least one HStore.  The number of HStores should be 
 * configurable, so that data which is accessed together is stored in the same
 * HStore.  Right now, we approximate that by building a single HStore for 
 * each column family.  (This config info will be communicated via the 
 * tabledesc.)
 * 
 * The HTableDescriptor contains metainfo about the HRegion's table.
 * regionName is a unique identifier for this HRegion. (startKey, endKey]
 * defines the keyspace for this HRegion.
 */
class HRegion implements HConstants {
  static String SPLITDIR = "splits";
  static String MERGEDIR = "merges";
  static String TMPREGION_PREFIX = "tmpregion_";
  static int MIN_COMMITS_FOR_COMPACTION = 10;
  static Random rand = new Random();

  static final Log LOG = LogFactory.getLog(HRegion.class);

  /**
   * Deletes all the files for a HRegion
   * 
   * @param fs                  - the file system object
   * @param baseDirectory       - base directory for HBase
   * @param regionName          - name of the region to delete
   * @throws IOException
   */
  static void deleteRegion(FileSystem fs, Path baseDirectory,
      Text regionName) throws IOException {
    LOG.debug("Deleting region " + regionName);
    fs.delete(HStoreFile.getHRegionDir(baseDirectory, regionName));
  }
  
  /**
   * Merge two HRegions.  They must be available on the current
   * HRegionServer. Returns a brand-new active HRegion, also
   * running on the current HRegionServer.
   */
  static HRegion closeAndMerge(HRegion srcA, HRegion srcB) throws IOException {

    // Make sure that srcA comes first; important for key-ordering during
    // write of the merged file.
    
    if(srcA.getStartKey() == null) {
      if(srcB.getStartKey() == null) {
        throw new IOException("Cannot merge two regions with null start key");
      }
      // A's start key is null but B's isn't. Assume A comes before B
      
    } else if((srcB.getStartKey() == null)         // A is not null but B is
        || (srcA.getStartKey().compareTo(srcB.getStartKey()) > 0)) { // A > B
      
      HRegion tmp = srcA;
      srcA = srcB;
      srcB = tmp;
    }
    
    if (! srcA.getEndKey().equals(srcB.getStartKey())) {
      throw new IOException("Cannot merge non-adjacent regions");
    }

    FileSystem fs = srcA.getFilesystem();
    Configuration conf = srcA.getConf();
    HTableDescriptor tabledesc = srcA.getTableDesc();
    HLog log = srcA.getLog();
    Path rootDir = srcA.getRootDir();

    Text startKey = srcA.getStartKey();
    Text endKey = srcB.getEndKey();

    Path merges = new Path(srcA.getRegionDir(), MERGEDIR);
    if(! fs.exists(merges)) {
      fs.mkdirs(merges);
    }
    
    HRegionInfo newRegionInfo
      = new HRegionInfo(Math.abs(rand.nextLong()), tabledesc, startKey, endKey);
    
    Path newRegionDir = HStoreFile.getHRegionDir(merges, newRegionInfo.regionName);

    if(fs.exists(newRegionDir)) {
      throw new IOException("Cannot merge; target file collision at " + newRegionDir);
    }

    LOG.info("starting merge of regions: " + srcA.getRegionName() + " and " 
        + srcB.getRegionName() + " new region start key is '" 
        + (startKey == null ? "" : startKey) + "', end key is '" 
        + (endKey == null ? "" : endKey) + "'");
    
    // Flush each of the sources, and merge their files into a single 
    // target for each column family.    
    TreeSet<HStoreFile> alreadyMerged = new TreeSet<HStoreFile>();
    TreeMap<Text, Vector<HStoreFile>> filesToMerge =
      new TreeMap<Text, Vector<HStoreFile>>();
    for(HStoreFile src: srcA.flushcache(true)) {
      Vector<HStoreFile> v = filesToMerge.get(src.getColFamily());
      if(v == null) {
        v = new Vector<HStoreFile>();
        filesToMerge.put(src.getColFamily(), v);
      }
      v.add(src);
    }
    
    for(HStoreFile src: srcB.flushcache(true)) {
      Vector<HStoreFile> v = filesToMerge.get(src.getColFamily());
      if(v == null) {
        v = new Vector<HStoreFile>();
        filesToMerge.put(src.getColFamily(), v);
      }
      v.add(src);
    }
    
    if(LOG.isDebugEnabled()) {
      LOG.debug("merging stores");
    }
    
    for (Map.Entry<Text, Vector<HStoreFile>> es: filesToMerge.entrySet()) {
      Text colFamily = es.getKey();
      Vector<HStoreFile> srcFiles = es.getValue();
      HStoreFile dst = new HStoreFile(conf, merges, newRegionInfo.regionName, 
          colFamily, Math.abs(rand.nextLong()));
      dst.mergeStoreFiles(srcFiles, fs, conf);
      alreadyMerged.addAll(srcFiles);
    }

    // That should have taken care of the bulk of the data.
    // Now close the source HRegions for good, and repeat the above to take care
    // of any last-minute inserts

    if(LOG.isDebugEnabled()) {
      LOG.debug("flushing changes since start of merge for region " 
          + srcA.getRegionName());
    }

    filesToMerge.clear();
    
    for(HStoreFile src: srcA.close()) {
      if(! alreadyMerged.contains(src)) {
        Vector<HStoreFile> v = filesToMerge.get(src.getColFamily());
        if(v == null) {
          v = new Vector<HStoreFile>();
          filesToMerge.put(src.getColFamily(), v);
        }
        v.add(src);
      }
    }
    
    if(LOG.isDebugEnabled()) {
      LOG.debug("flushing changes since start of merge for region " 
          + srcB.getRegionName());
    }
    
    for(HStoreFile src: srcB.close()) {
      if(! alreadyMerged.contains(src)) {
        Vector<HStoreFile> v = filesToMerge.get(src.getColFamily());
        if(v == null) {
          v = new Vector<HStoreFile>();
          filesToMerge.put(src.getColFamily(), v);
        }
        v.add(src);
      }
    }
    
    if(LOG.isDebugEnabled()) {
      LOG.debug("merging changes since start of merge");
    }
    
    for (Map.Entry<Text, Vector<HStoreFile>> es : filesToMerge.entrySet()) {
      Text colFamily = es.getKey();
      Vector<HStoreFile> srcFiles = es.getValue();
      HStoreFile dst = new HStoreFile(conf, merges,
        newRegionInfo.regionName, colFamily, Math.abs(rand.nextLong()));
      dst.mergeStoreFiles(srcFiles, fs, conf);
    }

    // Done
    
    HRegion dstRegion = new HRegion(rootDir, log, fs, conf, newRegionInfo,
        newRegionDir);

    // Get rid of merges directory
    
    fs.delete(merges);

    LOG.info("merge completed. New region is " + dstRegion.getRegionName());
    
    return dstRegion;
  }

  //////////////////////////////////////////////////////////////////////////////
  // Members
  //////////////////////////////////////////////////////////////////////////////

  Map<Text, Long> rowsToLocks = new HashMap<Text, Long>();
  Map<Long, Text> locksToRows = new HashMap<Long, Text>();
  Map<Text, HStore> stores = new HashMap<Text, HStore>();
  Map<Long, TreeMap<Text, byte []>> targetColumns 
    = new HashMap<Long, TreeMap<Text, byte []>>();
  
  HMemcache memcache;

  Path rootDir;
  HLog log;
  FileSystem fs;
  Configuration conf;
  HRegionInfo regionInfo;
  Path regiondir;

  static class WriteState {
    volatile boolean writesOngoing;
    volatile boolean writesEnabled;
    volatile boolean closed;
    WriteState() {
      this.writesOngoing = true;
      this.writesEnabled = true;
      this.closed = false;
    }
  }
  
  volatile WriteState writestate = new WriteState();
  int recentCommits = 0;
  volatile int commitsSinceFlush = 0;

  int maxUnflushedEntries = 0;
  int compactionThreshold = 0;
  private final HLocking lock = new HLocking();
  private long desiredMaxFileSize;

  //////////////////////////////////////////////////////////////////////////////
  // Constructor
  //////////////////////////////////////////////////////////////////////////////

  /**
   * HRegion constructor.
   *
   * @param log The HLog is the outbound log for any updates to the HRegion
   * (There's a single HLog for all the HRegions on a single HRegionServer.)
   * The log file is a logfile from the previous execution that's
   * custom-computed for this HRegion. The HRegionServer computes and sorts the
   * appropriate log info for this HRegion. If there is a previous log file
   * (implying that the HRegion has been written-to before), then read it from
   * the supplied path.
   * 
   * @param rootDir root directory for HBase instance
   * @param log HLog where changes should be committed
   * @param fs is the filesystem.  
   * @param conf is global configuration settings.
   * @param regionInfo - HRegionInfo that describes the region
   * @param initialFiles If there are initial files (implying that the HRegion
   * is new), then read them from the supplied path.
   * 
   * @throws IOException
   */
  HRegion(Path rootDir, HLog log, FileSystem fs, Configuration conf, 
      HRegionInfo regionInfo, Path initialFiles)
  throws IOException {
    
    this.rootDir = rootDir;
    this.log = log;
    this.fs = fs;
    this.conf = conf;
    this.regionInfo = regionInfo;
    this.memcache = new HMemcache();


    this.writestate.writesOngoing = true;
    this.writestate.writesEnabled = true;
    this.writestate.closed = false;

    // Declare the regionName.  This is a unique string for the region, used to 
    // build a unique filename.
    
    this.regiondir = HStoreFile.getHRegionDir(rootDir, this.regionInfo.regionName);
    Path oldLogFile = new Path(regiondir, HREGION_OLDLOGFILE_NAME);

    // Move prefab HStore files into place (if any)
    
    if(initialFiles != null && fs.exists(initialFiles)) {
      fs.rename(initialFiles, regiondir);
    }

    // Load in all the HStores.
    for(Map.Entry<Text, HColumnDescriptor> e :
        this.regionInfo.tableDesc.families().entrySet()) {
      Text colFamily = HStoreKey.extractFamily(e.getKey());
      stores.put(colFamily, new HStore(rootDir, this.regionInfo.regionName,
          e.getValue(), fs, oldLogFile, conf));
    }

    // Get rid of any splits or merges that were lost in-progress
    Path splits = new Path(regiondir, SPLITDIR);
    if (fs.exists(splits)) {
      fs.delete(splits);
    }
    Path merges = new Path(regiondir, MERGEDIR);
    if (fs.exists(merges)) {
      fs.delete(merges);
    }

    // By default, we flush the cache after 10,000 commits
    
    this.maxUnflushedEntries = conf.getInt("hbase.hregion.maxunflushed", 10000);
    
    // By default, we compact the region if an HStore has more than 10 map files
    
    this.compactionThreshold =
      conf.getInt("hbase.hregion.compactionThreshold", 10);
    
    // By default we split region if a file > DEFAULT_MAX_FILE_SIZE.
    this.desiredMaxFileSize =
      conf.getLong("hbase.hregion.max.filesize", DEFAULT_MAX_FILE_SIZE);

    // HRegion is ready to go!
    this.writestate.writesOngoing = false;
    LOG.info("region " + this.regionInfo.regionName + " available");
  }

  /** Returns a HRegionInfo object for this region */
  HRegionInfo getRegionInfo() {
    return this.regionInfo;
  }

  /** returns true if region is closed */
  boolean isClosed() {
    boolean closed = false;
    synchronized(writestate) {
      closed = writestate.closed;
    }
    return closed;
  }
  
  /**
   * Close down this HRegion.  Flush the cache, shut down each HStore, don't 
   * service any more calls.
   *
   * The returned Vector is a list of all the storage files that the HRegion's 
   * component HStores make use of.  It's a list of HStoreFile objects.
   *
   * This method could take some time to execute, so don't call it from a 
   * time-sensitive thread.
   */
  Vector<HStoreFile> close() throws IOException {
    lock.obtainWriteLock();
    try {
      boolean shouldClose = false;
      synchronized(writestate) {
        if(writestate.closed) {
          LOG.info("region " + this.regionInfo.regionName + " closed");
          return new Vector<HStoreFile>();
        }
        while(writestate.writesOngoing) {
          try {
            writestate.wait();
          } catch (InterruptedException iex) {
            // continue
          }
        }
        writestate.writesOngoing = true;
        shouldClose = true;
      }

      if(! shouldClose) {
        return null;
      }
      LOG.info("closing region " + this.regionInfo.regionName);
      Vector<HStoreFile> allHStoreFiles = internalFlushcache();
      for (HStore store: stores.values()) {
        store.close();
      }
      try {
        return allHStoreFiles;

      } finally {
        synchronized (writestate) {
          writestate.closed = true;
          writestate.writesOngoing = false;
        }
        LOG.info("region " + this.regionInfo.regionName + " closed");
      }
    } finally {
      lock.releaseWriteLock();
    }
  }

  /**
   * Split the HRegion to create two brand-new ones.  This will also close the 
   * current HRegion.
   *
   * Returns two brand-new (and open) HRegions
   */
  HRegion[] closeAndSplit(Text midKey, RegionUnavailableListener listener)
      throws IOException {
    
    if(((regionInfo.startKey.getLength() != 0)
        && (regionInfo.startKey.compareTo(midKey) > 0))
        || ((regionInfo.endKey.getLength() != 0)
            && (regionInfo.endKey.compareTo(midKey) < 0))) {
      throw new IOException("Region splitkey must lie within region " +
        "boundaries.");
    }

    LOG.info("Splitting region " + this.regionInfo.regionName);

    Path splits = new Path(regiondir, SPLITDIR);
    if(! fs.exists(splits)) {
      fs.mkdirs(splits);
    }
    
    long regionAId = Math.abs(rand.nextLong());
    HRegionInfo regionAInfo = new HRegionInfo(regionAId, regionInfo.tableDesc, 
        regionInfo.startKey, midKey);
        
    long regionBId = Math.abs(rand.nextLong());
    HRegionInfo regionBInfo
      = new HRegionInfo(regionBId, regionInfo.tableDesc, midKey, null);
    
    Path dirA = HStoreFile.getHRegionDir(splits, regionAInfo.regionName);
    Path dirB = HStoreFile.getHRegionDir(splits, regionBInfo.regionName);

    if(fs.exists(dirA) || fs.exists(dirB)) {
      throw new IOException("Cannot split; target file collision at " + dirA 
          + " or " + dirB);
    }
    
    TreeSet<HStoreFile> alreadySplit = new TreeSet<HStoreFile>();

    // Flush this HRegion out to storage, and turn off flushes
    // or compactions until close() is called.
    
    // TODO: flushcache can come back null if it can't do the flush. FIX.
    Vector<HStoreFile> hstoreFilesToSplit = flushcache(true);
    for(HStoreFile hsf: hstoreFilesToSplit) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Splitting HStore " + hsf.getRegionName() + "/" +
          hsf.getColFamily() + "/" + hsf.fileId());
      }
      HStoreFile dstA = new HStoreFile(conf, splits, regionAInfo.regionName, 
        hsf.getColFamily(), Math.abs(rand.nextLong()));
      HStoreFile dstB = new HStoreFile(conf, splits, regionBInfo.regionName, 
        hsf.getColFamily(), Math.abs(rand.nextLong()));
      hsf.splitStoreFile(midKey, dstA, dstB, fs, conf);
      alreadySplit.add(hsf);
    }

    // We just copied most of the data.
    
    // Notify the caller that we are about to close the region
    listener.closing(this.getRegionName());
    
    // Wait on the last row updates to come in.
    waitOnRowLocks();
    
    // Now close the HRegion
    hstoreFilesToSplit = close();
    
    // Tell listener that region is now closed and that they can therefore
    // clean up any outstanding references.
    listener.closed(this.getRegionName());
    
    // Copy the small remainder
    for(HStoreFile hsf: hstoreFilesToSplit) {
      if(! alreadySplit.contains(hsf)) {
        if(LOG.isDebugEnabled()) {
          LOG.debug("Splitting HStore " + hsf.getRegionName() + "/"
              + hsf.getColFamily() + "/" + hsf.fileId());
        }

        HStoreFile dstA = new HStoreFile(conf, splits, regionAInfo.regionName, 
            hsf.getColFamily(), Math.abs(rand.nextLong()));
        
        HStoreFile dstB = new HStoreFile(conf, splits, regionBInfo.regionName, 
            hsf.getColFamily(), Math.abs(rand.nextLong()));
        
        hsf.splitStoreFile(midKey, dstA, dstB, fs, conf);
      }
    }

    // Done

    HRegion regionA = new HRegion(rootDir, log, fs, conf, regionAInfo, dirA);
    
    HRegion regionB = new HRegion(rootDir, log, fs, conf, regionBInfo, dirB);

    // Cleanup

    fs.delete(splits);                          // Get rid of splits directory
    fs.delete(regiondir);                       // and the directory for the old region
    
    HRegion regions[] = new HRegion[2];
    regions[0] = regionA;
    regions[1] = regionB;
    
    LOG.info("Region split of " + this.regionInfo.regionName + " complete. " +
      "New regions are: " + regions[0].getRegionName() + ", " +
      regions[1].getRegionName());
    
    return regions;
  }

  //////////////////////////////////////////////////////////////////////////////
  // HRegion accessors
  //////////////////////////////////////////////////////////////////////////////

  Text getStartKey() {
    return regionInfo.startKey;
  }
  
  Text getEndKey() {
    return regionInfo.endKey;
  }
  
  long getRegionId() {
    return regionInfo.regionId;
  }

  Text getRegionName() {
    return regionInfo.regionName;
  }
  
  Path getRootDir() {
    return rootDir;
  }
 
  HTableDescriptor getTableDesc() {
    return regionInfo.tableDesc;
  }
  
  HLog getLog() {
    return log;
  }
  
  Configuration getConf() {
    return conf;
  }
  
  Path getRegionDir() {
    return regiondir;
  }
  
  FileSystem getFilesystem() {
    return fs;
  }

  //////////////////////////////////////////////////////////////////////////////
  // HRegion maintenance.  
  //
  // These methods are meant to be called periodically by the HRegionServer for 
  // upkeep.
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Iterates through all the HStores and finds the one with the largest MapFile
   * size. If the size is greater than the (currently hard-coded) threshold,
   * returns true indicating that the region should be split. The midKey for the
   * largest MapFile is returned through the midKey parameter.
   * 
   * @param midKey      - (return value) midKey of the largest MapFile
   * @return            - true if the region should be split
   */
  boolean needsSplit(Text midKey) {
    lock.obtainReadLock();
    try {
      Text key = new Text();
      long maxSize = 0;
      for(HStore store: stores.values()) {
        long size = store.getLargestFileSize(key);
        if(size > maxSize) {                      // Largest so far
          maxSize = size;
          midKey.set(key);
        }
      }

      return (maxSize >
        (this.desiredMaxFileSize + (this.desiredMaxFileSize / 2)));
    } finally {
      lock.releaseReadLock();
    }
  }

  /**
   * @return - returns the size of the largest HStore
   */
  long largestHStore() {
    long maxsize = 0;
    lock.obtainReadLock();
    try {
      Text key = new Text();
      for(HStore h: stores.values()) {
        long size = h.getLargestFileSize(key);

        if(size > maxsize) {                      // Largest so far
          maxsize = size;
        }
      }
      return maxsize;
    
    } finally {
      lock.releaseReadLock();
    }
  }
  
  /**
   * @return true if the region should be compacted.
   */
  boolean needsCompaction() {
    boolean needsCompaction = false;
    this.lock.obtainReadLock();
    try {
      for(HStore store: stores.values()) {
        if(store.getNMaps() > this.compactionThreshold) {
          needsCompaction = true;
          break;
        }
      }
    } finally {
      this.lock.releaseReadLock();
    }
    return needsCompaction;
  }
  
  /**
   * Compact all the stores.  This should be called periodically to make sure 
   * the stores are kept manageable.  
   *
   * This operation could block for a long time, so don't call it from a 
   * time-sensitive thread.
   *
   * If it returns TRUE, the compaction has completed.
   * 
   * If it returns FALSE, the compaction was not carried out, because the 
   * HRegion is busy doing something else storage-intensive (like flushing the 
   * cache).  The caller should check back later.
   */
  boolean compactStores() throws IOException {
    boolean shouldCompact = false;
    lock.obtainReadLock();
    try {
      synchronized (writestate) {
        if ((!writestate.writesOngoing) &&
            writestate.writesEnabled &&
            (!writestate.closed) &&
            recentCommits > MIN_COMMITS_FOR_COMPACTION) {
          writestate.writesOngoing = true;
          shouldCompact = true;
        }
      }

      if (!shouldCompact) {
        LOG.info("not compacting region " + this.regionInfo);
        return false;
      }

      LOG.info("starting compaction on region " + this.regionInfo);
      for (HStore store : stores.values()) {
        store.compact();
      }
      LOG.info("compaction completed on region " + this.regionInfo);
      return true;
      
    } finally {
      lock.releaseReadLock();
      synchronized (writestate) {
        writestate.writesOngoing = false;
        recentCommits = 0;
        writestate.notifyAll();
      }
    }
  }

  /**
   * Each HRegion is given a periodic chance to flush the cache, which it should
   * only take if there have been a lot of uncommitted writes.
   */
  void optionallyFlush() throws IOException {
    if(commitsSinceFlush > maxUnflushedEntries) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Flushing cache. Number of commits is: " + commitsSinceFlush);
      }
      flushcache(false);
    }
  }

  /**
   * Flush the cache.  This is called periodically to minimize the amount of log
   * processing needed upon startup.
   * 
   * The returned Vector is a list of all the files used by the component HStores.
   * It is a list of HStoreFile objects.  If the returned value is NULL, then the
   * flush could not be executed, because the HRegion is busy doing something
   * else storage-intensive.  The caller should check back later.
   *
   * The 'disableFutureWrites' boolean indicates that the caller intends to 
   * close() the HRegion shortly, so the HRegion should not take on any new and 
   * potentially long-lasting disk operations.  This flush() should be the final
   * pre-close() disk operation.
   *
   * This method may block for some time, so it should not be called from a 
   * time-sensitive thread.
   */
  Vector<HStoreFile> flushcache(boolean disableFutureWrites) throws IOException {
    boolean shouldFlush = false;
    synchronized(writestate) {
      if((! writestate.writesOngoing)
          && writestate.writesEnabled
          && (! writestate.closed)) {
        
        writestate.writesOngoing = true;
        shouldFlush = true;
        
        if(disableFutureWrites) {
          writestate.writesEnabled = false;
        }
      }
    }
    
    if(! shouldFlush) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("not flushing cache for region " +
          this.regionInfo.regionName);
      }
      return null;  
    }
    
    try {
      return internalFlushcache();

    } finally {
      synchronized (writestate) {
        writestate.writesOngoing = false;
        writestate.notifyAll();
      }
    }
  }

  /**
   * Flushing the cache is a little tricky. We have a lot of updates in the
   * HMemcache, all of which have also been written to the log. We need to write
   * those updates in the HMemcache out to disk, while being able to process
   * reads/writes as much as possible during the flush operation. Also, the log
   * has to state clearly the point in time at which the HMemcache was flushed.
   * (That way, during recovery, we know when we can rely on the on-disk flushed
   * structures and when we have to recover the HMemcache from the log.)
   * 
   * So, we have a three-step process:
   * 
   * A. Flush the memcache to the on-disk stores, noting the current sequence ID
   * for the log.
   * 
   * B. Write a FLUSHCACHE-COMPLETE message to the log, using the sequence ID
   * that was current at the time of memcache-flush.
   * 
   * C. Get rid of the memcache structures that are now redundant, as they've
   * been flushed to the on-disk HStores.
   * 
   * This method is protected, but can be accessed via several public routes.
   * 
   * This method may block for some time.
   */
  Vector<HStoreFile> internalFlushcache() throws IOException {
    Vector<HStoreFile> allHStoreFiles = new Vector<HStoreFile>();
    
    if(LOG.isDebugEnabled()) {
      LOG.debug("flushing cache for region " + this.regionInfo.regionName);
    }

    // We pass the log to the HMemcache, so we can lock down 
    // both simultaneously.  We only have to do this for a moment:
    // we need the HMemcache state at the time of a known log sequence
    // number.  Since multiple HRegions may write to a single HLog,
    // the sequence numbers may zoom past unless we lock it.
    //
    // When execution returns from snapshotMemcacheForLog()
    // with a non-NULL value, the HMemcache will have a snapshot 
    // object stored that must be explicitly cleaned up using
    // a call to deleteSnapshot().
    
    if(LOG.isDebugEnabled()) {
      LOG.debug("starting memcache snapshot");
    }
    
    HMemcache.Snapshot retval = memcache.snapshotMemcacheForLog(log);
    TreeMap<HStoreKey, byte []> memcacheSnapshot = retval.memcacheSnapshot;
    if(memcacheSnapshot == null) {
      for(HStore hstore: stores.values()) {
        Vector<HStoreFile> hstoreFiles = hstore.getAllMapFiles();
        allHStoreFiles.addAll(0, hstoreFiles);
      }
      return allHStoreFiles;
    }
    
    long logCacheFlushId = retval.sequenceId;

    // A.  Flush memcache to all the HStores.
    
    if(LOG.isDebugEnabled()) {
      LOG.debug("flushing memcache to HStores");
    }
    
    for(Iterator<HStore> it = stores.values().iterator(); it.hasNext(); ) {
      HStore hstore = it.next();
      Vector<HStoreFile> hstoreFiles 
        = hstore.flushCache(memcacheSnapshot, logCacheFlushId);
      
      allHStoreFiles.addAll(0, hstoreFiles);
    }

    // B.  Write a FLUSHCACHE-COMPLETE message to the log.
    //     This tells future readers that the HStores were emitted correctly,
    //     and that all updates to the log for this regionName that have lower 
    //     log-sequence-ids can be safely ignored.
    
    if(LOG.isDebugEnabled()) {
      LOG.debug("writing flush cache complete to log");
    }
    
    log.completeCacheFlush(this.regionInfo.regionName,
        regionInfo.tableDesc.getName(), logCacheFlushId);

    // C. Delete the now-irrelevant memcache snapshot; its contents have been 
    //    dumped to disk-based HStores.
    
    if(LOG.isDebugEnabled()) {
      LOG.debug("deleting memcache snapshot");
    }
    
    memcache.deleteSnapshot();

    if(LOG.isDebugEnabled()) {
      LOG.debug("cache flush complete for region " + this.regionInfo.regionName);
    }
    
    this.commitsSinceFlush = 0;
    return allHStoreFiles;
  }

  //////////////////////////////////////////////////////////////////////////////
  // get() methods for client use.
  //////////////////////////////////////////////////////////////////////////////

  /** Fetch a single data item. */
  byte [] get(Text row, Text column) throws IOException {
    byte [][] results = get(row, column, Long.MAX_VALUE, 1);
    return (results == null || results.length == 0)? null: results[0];
  }
  
  /** Fetch multiple versions of a single data item */
  byte [][] get(Text row, Text column, int numVersions) throws IOException {
    return get(row, column, Long.MAX_VALUE, numVersions);
  }

  /** Fetch multiple versions of a single data item, with timestamp. */
  byte [][] get(Text row, Text column, long timestamp, int numVersions) 
  throws IOException {  
    if(writestate.closed) {
      throw new IOException("HRegion is closed.");
    }

    // Make sure this is a valid row and valid column
    checkRow(row);
    checkColumn(column);

    // Obtain the row-lock
    obtainRowLock(row);
    try {
      // Obtain the -col results
      return get(new HStoreKey(row, column, timestamp), numVersions);
    
    } finally {
      releaseRowLock(row);
    }
  }

  /** Private implementation: get the value for the indicated HStoreKey */
  private byte [][] get(HStoreKey key, int numVersions) throws IOException {

    lock.obtainReadLock();
    try {
      // Check the memcache
      byte [][] result = memcache.get(key, numVersions);
      if(result != null) {
        return result;
      }

      // If unavailable in memcache, check the appropriate HStore

      Text colFamily = HStoreKey.extractFamily(key.getColumn());
      HStore targetStore = stores.get(colFamily);
      if(targetStore == null) {
        return null;
      }

      return targetStore.get(key, numVersions);
      
    } finally {
      lock.releaseReadLock();
    }
  }

  /**
   * Fetch all the columns for the indicated row.
   * Returns a TreeMap that maps column names to values.
   *
   * We should eventually use Bloom filters here, to reduce running time.  If 
   * the database has many column families and is very sparse, then we could be 
   * checking many files needlessly.  A small Bloom for each row would help us 
   * determine which column groups are useful for that row.  That would let us 
   * avoid a bunch of disk activity.
   */
  TreeMap<Text, byte []> getFull(Text row) throws IOException {
    HStoreKey key = new HStoreKey(row, System.currentTimeMillis());

    lock.obtainReadLock();
    try {
      TreeMap<Text, byte []> memResult = memcache.getFull(key);
      for (Text colFamily: stores.keySet()) {
        HStore targetStore = stores.get(colFamily);
        targetStore.getFull(key, memResult);
      }
      return memResult;
    } finally {
      lock.releaseReadLock();
    }
  }

  /**
   * Return an iterator that scans over the HRegion, returning the indicated 
   * columns.  This Iterator must be closed by the caller.
   */
  HInternalScannerInterface getScanner(Text[] cols, Text firstRow)
  throws IOException {
    lock.obtainReadLock();
    try {
      TreeSet<Text> families = new TreeSet<Text>();
      for(int i = 0; i < cols.length; i++) {
        families.add(HStoreKey.extractFamily(cols[i]));
      }

      HStore[] storelist = new HStore[families.size()];
      int i = 0;
      for (Text family: families) {
        storelist[i++] = stores.get(family);
      }
      return new HScanner(cols, firstRow, memcache, storelist);
    } finally {
      lock.releaseReadLock();
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // set() methods for client use.
  //////////////////////////////////////////////////////////////////////////////
  
  /**
   * The caller wants to apply a series of writes to a single row in the
   * HRegion. The caller will invoke startUpdate(), followed by a series of
   * calls to put/delete, then finally either abort() or commit().
   *
   * <p>Note that we rely on the external caller to properly abort() or
   * commit() every transaction.  If the caller is a network client, there
   * should be a lease-system in place that automatically aborts() transactions
   * after a specified quiet period.
   * 
   * @param row Row to update
   * @return lockid
   * @see #put(long, Text, BytesWritable)
   */
  long startUpdate(Text row) throws IOException {
    // We obtain a per-row lock, so other clients will block while one client
    // performs an update.  The read lock is released by the client calling
    // #commit or #abort or if the HRegionServer lease on the lock expires.
    // See HRegionServer#RegionListener for how the expire on HRegionServer
    // invokes a HRegion#abort.
    return obtainRowLock(row);
  }

  /**
   * Put a cell value into the locked row.  The user indicates the row-lock, the
   * target column, and the desired value.  This stuff is set into a temporary 
   * memory area until the user commits the change, at which point it's logged 
   * and placed into the memcache.
   *
   * This method really just tests the input, then calls an internal localput() 
   * method.
   */
  void put(long lockid, Text targetCol, byte [] val) throws IOException {
    if (DELETE_BYTES.compareTo(val) == 0) {
      throw new IOException("Cannot insert value: " + val);
    }
    localput(lockid, targetCol, val);
  }

  /**
   * Delete a value or write a value. This is a just a convenience method for put().
   */
  void delete(long lockid, Text targetCol) throws IOException {
    localput(lockid, targetCol, DELETE_BYTES.get());
  }

  /**
   * Private implementation.
   * 
   * localput() is used for both puts and deletes. We just place the values
   * into a per-row pending area, until a commit() or abort() call is received.
   * (Or until the user's write-lock expires.)
   * 
   * @param lockid
   * @param targetCol
   * @param val Value to enter into cell
   * @throws IOException
   */
  void localput(final long lockid, final Text targetCol,
    final byte [] val)
  throws IOException {
    checkColumn(targetCol);

    Text row = getRowFromLock(lockid);
    if (row == null) {
      throw new LockException("No write lock for lockid " + lockid);
    }

    // This sync block makes localput() thread-safe when multiple
    // threads from the same client attempt an insert on the same 
    // locked row (via lockid).
    synchronized(row) {
      // This check makes sure that another thread from the client
      // hasn't aborted/committed the write-operation.
      if (row != getRowFromLock(lockid)) {
        throw new LockException("Locking error: put operation on lock " +
            lockid + " unexpected aborted by another thread");
      }
      
      TreeMap<Text, byte []> targets = this.targetColumns.get(lockid);
      if (targets == null) {
        targets = new TreeMap<Text, byte []>();
        this.targetColumns.put(lockid, targets);
      }
      targets.put(targetCol, val);
    }
  }

  /**
   * Abort a pending set of writes. This dumps from memory all in-progress
   * writes associated with the given row-lock.  These values have not yet
   * been placed in memcache or written to the log.
   */
  void abort(long lockid) throws IOException {
    Text row = getRowFromLock(lockid);
    if(row == null) {
      throw new LockException("No write lock for lockid " + lockid);
    }
    
    // This sync block makes abort() thread-safe when multiple
    // threads from the same client attempt to operate on the same
    // locked row (via lockid).
    
    synchronized(row) {
      
      // This check makes sure another thread from the client
      // hasn't aborted/committed the write-operation.
      
      if(row != getRowFromLock(lockid)) {
        throw new LockException("Locking error: abort() operation on lock " 
            + lockid + " unexpected aborted by another thread");
      }
      
      this.targetColumns.remove(lockid);
      releaseRowLock(row);
    }
  }

  /**
   * Commit a pending set of writes to the memcache. This also results in
   * writing to the change log.
   *
   * Once updates hit the change log, they are safe.  They will either be moved 
   * into an HStore in the future, or they will be recovered from the log.
   * @param lockid Lock for row we're to commit.
   * @throws IOException
   */
  void commit(final long lockid) throws IOException {
    // Remove the row from the pendingWrites list so 
    // that repeated executions won't screw this up.
    Text row = getRowFromLock(lockid);
    if(row == null) {
      throw new LockException("No write lock for lockid " + lockid);
    }
    
    // This check makes sure that another thread from the client
    // hasn't aborted/committed the write-operation
    synchronized(row) {
      // Add updates to the log and add values to the memcache.
      long commitTimestamp = System.currentTimeMillis();
      TreeMap<Text, byte []> columns =  this.targetColumns.get(lockid);
      if (columns != null && columns.size() > 0) {
        log.append(regionInfo.regionName, regionInfo.tableDesc.getName(),
          row, columns, commitTimestamp);
        memcache.add(row, columns, commitTimestamp);
        // OK, all done!
      }
      targetColumns.remove(lockid);
      releaseRowLock(row);
    }
    recentCommits++;
    this.commitsSinceFlush++;
  }

  //////////////////////////////////////////////////////////////////////////////
  // Support code
  //////////////////////////////////////////////////////////////////////////////

  /** Make sure this is a valid row for the HRegion */
  void checkRow(Text row) throws IOException {
    if(((regionInfo.startKey.getLength() == 0)
        || (regionInfo.startKey.compareTo(row) <= 0))
        && ((regionInfo.endKey.getLength() == 0)
            || (regionInfo.endKey.compareTo(row) > 0))) {
      // all's well
      
    } else {
      throw new WrongRegionException("Requested row out of range for " +
        "HRegion " + regionInfo.regionName + ", startKey='" +
        regionInfo.startKey + "', endKey='" + regionInfo.endKey + "', row='" +
        row + "'");
    }
  }
  
  /** Make sure this is a valid column for the current table */
  void checkColumn(Text columnName) throws IOException {
    Text family = new Text(HStoreKey.extractFamily(columnName) + ":");
    if(! regionInfo.tableDesc.hasFamily(family)) {
      throw new IOException("Requested column family " + family 
          + " does not exist in HRegion " + regionInfo.regionName
          + " for table " + regionInfo.tableDesc.getName());
    }
  }

  /**
   * Obtain a lock on the given row.  Blocks until success.
   *
   * I know it's strange to have two mappings:
   * <pre>
   *   ROWS  ==> LOCKS
   * </pre>
   * as well as
   * <pre>
   *   LOCKS ==> ROWS
   * </pre>
   *
   * But it acts as a guard on the client; a miswritten client just can't
   * submit the name of a row and start writing to it; it must know the correct
   * lockid, which matches the lock list in memory.
   * 
   * <p>It would be more memory-efficient to assume a correctly-written client, 
   * which maybe we'll do in the future.
   * 
   * @param row Name of row to lock.
   * @return The id of the held lock.
   */
  long obtainRowLock(Text row) throws IOException {
    checkRow(row);
    synchronized(rowsToLocks) {
      while(rowsToLocks.get(row) != null) {
        try {
          rowsToLocks.wait();
        } catch (InterruptedException ie) {
          // Empty
        }
      }
      
      long lockid = Math.abs(rand.nextLong());
      rowsToLocks.put(row, lockid);
      locksToRows.put(lockid, row);
      rowsToLocks.notifyAll();
      return lockid;
    }
  }
  
  Text getRowFromLock(long lockid) {
    // Pattern is that all access to rowsToLocks and/or to
    // locksToRows is via a lock on rowsToLocks.
    synchronized(rowsToLocks) {
      return locksToRows.get(lockid);
    }
  }
  
  /** 
   * Release the row lock!
   * @param lock Name of row whose lock we are to release
   */
  void releaseRowLock(Text row) {
    synchronized(rowsToLocks) {
      long lockid = rowsToLocks.remove(row).longValue();
      locksToRows.remove(lockid);
      rowsToLocks.notifyAll();
    }
  }
  
  private void waitOnRowLocks() {
    synchronized (this.rowsToLocks) {
      while (this.rowsToLocks.size() > 0) {
        try {
          this.rowsToLocks.wait();
        } catch (InterruptedException e) {
          // Catch. Let while test determine loop-end.
        }
      }
    }
  }
  
  /**
   * HScanner is an iterator through a bunch of rows in an HRegion.
   */
  private static class HScanner implements HInternalScannerInterface {
    private HInternalScannerInterface[] scanners;
    private TreeMap<Text, byte []>[] resultSets;
    private HStoreKey[] keys;
    private boolean wildcardMatch;
    private boolean multipleMatchers;

    /** Create an HScanner with a handle on many HStores. */
    @SuppressWarnings("unchecked")
    HScanner(Text[] cols, Text firstRow, HMemcache memcache, HStore[] stores)
    throws IOException {  
      long scanTime = System.currentTimeMillis();
      this.scanners = new HInternalScannerInterface[stores.length + 1];
      for(int i = 0; i < this.scanners.length; i++) {
        this.scanners[i] = null;
      }
      
      this.resultSets = new TreeMap[scanners.length];
      this.keys = new HStoreKey[scanners.length];
      this.wildcardMatch = false;
      this.multipleMatchers = false;

      // Advance to the first key in each store.
      // All results will match the required column-set and scanTime.
      
      // NOTE: the memcache scanner should be the first scanner
      try {
        HInternalScannerInterface scanner =
          memcache.getScanner(scanTime, cols, firstRow);
        if(scanner.isWildcardScanner()) {
          this.wildcardMatch = true;
        }
        if(scanner.isMultipleMatchScanner()) {
          this.multipleMatchers = true;
        }
        scanners[0] = scanner;

        for(int i = 0; i < stores.length; i++) {
          scanner = stores[i].getScanner(scanTime, cols, firstRow);
          if(scanner.isWildcardScanner()) {
            this.wildcardMatch = true;
          }
          if(scanner.isMultipleMatchScanner()) {
            this.multipleMatchers = true;
          }
          scanners[i + 1] = scanner;
        }

      } catch(IOException e) {
        for(int i = 0; i < this.scanners.length; i++) {
          if(scanners[i] != null) {
            closeScanner(i);
          }
        }
        throw e;
      }
      for(int i = 0; i < scanners.length; i++) {
        keys[i] = new HStoreKey();
        resultSets[i] = new TreeMap<Text, byte []>();
        if(scanners[i] != null && !scanners[i].next(keys[i], resultSets[i])) {
          closeScanner(i);
        }
      }
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.hbase.HInternalScannerInterface#isWildcardScanner()
     */
    public boolean isWildcardScanner() {
      return wildcardMatch;
    }
    
    /* (non-Javadoc)
     * @see org.apache.hadoop.hbase.HInternalScannerInterface#isMultipleMatchScanner()
     */
    public boolean isMultipleMatchScanner() {
      return multipleMatchers;
    }
    
    /* (non-Javadoc)
     * 
     * Grab the next row's worth of values.  The HScanner will return the most 
     * recent data value for each row that is not newer than the target time.
     *
     * @see org.apache.hadoop.hbase.HInternalScannerInterface#next(org.apache.hadoop.hbase.HStoreKey, java.util.TreeMap)
     */
    public boolean next(HStoreKey key, TreeMap<Text, byte []> results)
    throws IOException {
      // Find the lowest-possible key.
      Text chosenRow = null;
      long chosenTimestamp = -1;
      for(int i = 0; i < this.keys.length; i++) {
        if(scanners[i] != null
            && (chosenRow == null
                || (keys[i].getRow().compareTo(chosenRow) < 0)
                || ((keys[i].getRow().compareTo(chosenRow) == 0)
                    && (keys[i].getTimestamp() > chosenTimestamp)))) {
          
          chosenRow = new Text(keys[i].getRow());
          chosenTimestamp = keys[i].getTimestamp();
        }
      }

      // Store the key and results for each sub-scanner. Merge them as appropriate.
      boolean insertedItem = false;
      if(chosenTimestamp > 0) {
        key.setRow(chosenRow);
        key.setVersion(chosenTimestamp);
        key.setColumn(new Text(""));

        for(int i = 0; i < scanners.length; i++) {
          while((scanners[i] != null)
              && (keys[i].getRow().compareTo(chosenRow) == 0)) {
            // If we are doing a wild card match or there are multiple matchers
            // per column, we need to scan all the older versions of this row
            // to pick up the rest of the family members
            
            if(!wildcardMatch
                && !multipleMatchers
                && (keys[i].getTimestamp() != chosenTimestamp)) {
              break;
            }

            // NOTE: We used to do results.putAll(resultSets[i]);
            //       but this had the effect of overwriting newer
            //       values with older ones. So now we only insert
            //       a result if the map does not contain the key.
            
            for(Map.Entry<Text, byte []> e: resultSets[i].entrySet()) {
              if(!results.containsKey(e.getKey())) {
                results.put(e.getKey(), e.getValue());
                insertedItem = true;
              }
            }

            resultSets[i].clear();
            if(! scanners[i].next(keys[i], resultSets[i])) {
              closeScanner(i);
            }
          }

          // If the current scanner is non-null AND has a lower-or-equal
          // row label, then its timestamp is bad.  We need to advance it.

          while((scanners[i] != null)
              && (keys[i].getRow().compareTo(chosenRow) <= 0)) {
            
            resultSets[i].clear();
            if(! scanners[i].next(keys[i], resultSets[i])) {
              closeScanner(i);
            }
          }
        }
      }
      return insertedItem;
    }

    /** Shut down a single scanner */
    void closeScanner(int i) {
      try {
        scanners[i].close();
      } finally {
        scanners[i] = null;
        keys[i] = null;
        resultSets[i] = null;
      }
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.hbase.HInternalScannerInterface#close()
     */
    public void close() {
      for(int i = 0; i < scanners.length; i++) {
        if(scanners[i] != null) {
          closeScanner(i);
        }
      }
    }
  }
  
  // Utility methods
  
  /**
   * Convenience method creating new HRegions.
   * @param regionId ID to use
   * @param tableDesc Descriptor
   * @param rootDir Root directory of HBase instance
   * @param conf
   * @return New META region (ROOT or META).
   * @throws IOException
   */
  static HRegion createHRegion(final long regionId,
    final HTableDescriptor tableDesc, final Path rootDir,
    final Configuration conf)
  throws IOException {
    return createHRegion(new HRegionInfo(regionId, tableDesc, null, null),
      rootDir, conf, null);
  }
  
  /**
   * Convenience method creating new HRegions. Used by createTable and by the
   * bootstrap code in the HMaster constructor
   * 
   * @param info Info for region to create.
   * @param rootDir Root directory for HBase instance
   * @param conf
   * @param initialFiles InitialFiles to pass new HRegion. Pass null if none.
   * @return new HRegion
   * 
   * @throws IOException
   */
  static HRegion createHRegion(final HRegionInfo info,
    final Path rootDir, final Configuration conf, final Path initialFiles)
  throws IOException {
    Path regionDir = HStoreFile.getHRegionDir(rootDir, info.regionName);
    FileSystem fs = FileSystem.get(conf);
    fs.mkdirs(regionDir);
    return new HRegion(rootDir,
      new HLog(fs, new Path(regionDir, HREGION_LOGDIR_NAME), conf),
      fs, conf, info, initialFiles);
  }
  
  /**
   * Inserts a new region's meta information into the passed
   * <code>meta</code> region. Used by the HMaster bootstrap code adding
   * new table to ROOT table.
   * 
   * @param meta META HRegion to be updated
   * @param r HRegion to add to <code>meta</code>
   *
   * @throws IOException
   */
  static void addRegionToMETA(HRegion meta, HRegion r)
  throws IOException {  
    // The row key is the region name
    long writeid = meta.startUpdate(r.getRegionName());
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream s = new DataOutputStream(bytes);
    r.getRegionInfo().write(s);
    meta.put(writeid, COL_REGIONINFO, bytes.toByteArray());
    meta.commit(writeid);
  }
  
  static void addRegionToMETA(final HClient client,
      final Text table, final HRegion region,
      final HServerAddress serverAddress,
      final long startCode)
  throws IOException {
    client.openTable(table);
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytes);
    region.getRegionInfo().write(out);
    long lockid = client.startUpdate(region.getRegionName());
    client.put(lockid, COL_REGIONINFO, bytes.toByteArray());
    client.put(lockid, COL_SERVER,
      serverAddress.toString().getBytes(UTF8_ENCODING));
    client.put(lockid, COL_STARTCODE,
      String.valueOf(startCode).getBytes(UTF8_ENCODING));
    client.commit(lockid);
    LOG.info("Added region " + region.getRegionName() + " to table " + table);
  }
  
  /**
   * Delete <code>region</code> from META <code>table</code>.
   * @param client Client to use running update.
   * @param table META table we are to delete region from.
   * @param regionName Region to remove.
   * @throws IOException
   */
  static void removeRegionFromMETA(final HClient client,
      final Text table, final Text regionName)
  throws IOException {
    client.openTable(table);
    long lockid = client.startUpdate(regionName);
    client.delete(lockid, COL_REGIONINFO);
    client.delete(lockid, COL_SERVER);
    client.delete(lockid, COL_STARTCODE);
    client.commit(lockid);
    LOG.info("Removed " + regionName + " from table " + table);
  }
  
  /**
   * @param data Map of META row labelled column data.
   * @return Server
   */
  static HRegionInfo getRegionInfo(final TreeMap<Text, byte[]> data)
  throws IOException {
    byte[] bytes = data.get(COL_REGIONINFO);
    if (bytes == null || bytes.length == 0) {
      throw new IOException("no value for " + COL_REGIONINFO);
    }
    DataInputBuffer in = new DataInputBuffer();
    in.reset(bytes, bytes.length);
    HRegionInfo info = new HRegionInfo();
    info.readFields(in);
    return info;
  }
  
  /**
   * @param data Map of META row labelled column data.
   * @return Server
   */
  static String getServerName(final TreeMap<Text, byte[]> data) {
    byte [] bytes = data.get(COL_SERVER);
    String name = null;
    try {
      name = (bytes != null && bytes.length != 0) ?
          new String(bytes, UTF8_ENCODING): null;

    } catch(UnsupportedEncodingException e) {
      assert(false);
    }
    return (name != null)? name.trim(): name;
  }

  /**
   * @param data Map of META row labelled column data.
   * @return Start code.
   */
  static long getStartCode(final TreeMap<Text, byte[]> data) {
    long startCode = -1L;
    byte [] bytes = data.get(COL_STARTCODE);
    if(bytes != null && bytes.length != 0) {
      try {
        startCode = Long.parseLong(new String(bytes, UTF8_ENCODING).trim());
      } catch(NumberFormatException e) {
        LOG.error("Failed getting " + COL_STARTCODE, e);
      } catch(UnsupportedEncodingException e) {
        LOG.error("Failed getting " + COL_STARTCODE, e);
      }
    }
    return startCode;
  }
}
