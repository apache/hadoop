/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;

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
 * <p>The HTableDescriptor contains metainfo about the HRegion's table.
 * regionName is a unique identifier for this HRegion. (startKey, endKey]
 * defines the keyspace for this HRegion.
 */
public class HRegion implements HConstants {
  static String SPLITDIR = "splits";
  static String MERGEDIR = "merges";
  static final Random rand = new Random();
  static final Log LOG = LogFactory.getLog(HRegion.class);
  final AtomicBoolean closed = new AtomicBoolean(false);
  private long noFlushCount = 0;
  
  /**
   * Merge two HRegions.  They must be available on the current
   * HRegionServer. Returns a brand-new active HRegion, also
   * running on the current HRegionServer.
   */
  static HRegion closeAndMerge(final HRegion srcA, final HRegion srcB)
  throws IOException {
    
    HRegion a = srcA;
    HRegion b = srcB;

    // Make sure that srcA comes first; important for key-ordering during
    // write of the merged file.
    FileSystem fs = srcA.getFilesystem();
    if (srcA.getStartKey() == null) {
      if (srcB.getStartKey() == null) {
        throw new IOException("Cannot merge two regions with null start key");
      }
      // A's start key is null but B's isn't. Assume A comes before B
    } else if ((srcB.getStartKey() == null)         // A is not null but B is
        || (srcA.getStartKey().compareTo(srcB.getStartKey()) > 0)) { // A > B
      a = srcB;
      b = srcA;
    }
    
    if (! a.getEndKey().equals(b.getStartKey())) {
      throw new IOException("Cannot merge non-adjacent regions");
    }

    Configuration conf = a.getConf();
    HTableDescriptor tabledesc = a.getTableDesc();
    HLog log = a.getLog();
    Path rootDir = a.getRootDir();
    Text startKey = a.getStartKey();
    Text endKey = b.getEndKey();
    Path merges = new Path(a.getRegionDir(), MERGEDIR);
    if(! fs.exists(merges)) {
      fs.mkdirs(merges);
    }
    
    HRegionInfo newRegionInfo
      = new HRegionInfo(Math.abs(rand.nextLong()), tabledesc, startKey, endKey);
    Path newRegionDir = HRegion.getRegionDir(merges, newRegionInfo.regionName);
    if(fs.exists(newRegionDir)) {
      throw new IOException("Cannot merge; target file collision at " +
        newRegionDir);
    }

    LOG.info("starting merge of regions: " + a.getRegionName() + " and " +
      b.getRegionName() + " into new region " + newRegionInfo.toString());

    Map<Text, Vector<HStoreFile>> byFamily =
      new TreeMap<Text, Vector<HStoreFile>>();
    byFamily = filesByFamily(byFamily, a.close());
    byFamily = filesByFamily(byFamily, b.close());
    for (Map.Entry<Text, Vector<HStoreFile>> es : byFamily.entrySet()) {
      Text colFamily = es.getKey();
      Vector<HStoreFile> srcFiles = es.getValue();
      HStoreFile dst = new HStoreFile(conf, merges, newRegionInfo.regionName,
        colFamily, Math.abs(rand.nextLong()));
      dst.mergeStoreFiles(srcFiles, fs, conf);
    }

    // Done
    // Construction moves the merge files into place under region.
    HRegion dstRegion = new HRegion(rootDir, log, fs, conf, newRegionInfo,
        newRegionDir);

    // Get rid of merges directory
    
    fs.delete(merges);

    LOG.info("merge completed. New region is " + dstRegion.getRegionName());
    
    return dstRegion;
  }
  
  /*
   * Fills a map with a vector of store files keyed by column family. 
   * @param byFamily Map to fill.
   * @param storeFiles Store files to process.
   * @return Returns <code>byFamily</code>
   */
  private static Map<Text, Vector<HStoreFile>> filesByFamily(
      Map<Text, Vector<HStoreFile>> byFamily, Vector<HStoreFile> storeFiles) {
    for(HStoreFile src: storeFiles) {
      Vector<HStoreFile> v = byFamily.get(src.getColFamily());
      if(v == null) {
        v = new Vector<HStoreFile>();
        byFamily.put(src.getColFamily(), v);
      }
      v.add(src);
    }
    return byFamily;
  }

  //////////////////////////////////////////////////////////////////////////////
  // Members
  //////////////////////////////////////////////////////////////////////////////

  Map<Text, Long> rowsToLocks = new HashMap<Text, Long>();
  Map<Long, Text> locksToRows = new HashMap<Long, Text>();
  Map<Text, HStore> stores = new HashMap<Text, HStore>();
  Map<Long, TreeMap<Text, byte []>> targetColumns 
    = new HashMap<Long, TreeMap<Text, byte []>>();
  
  final HMemcache memcache;

  Path rootDir;
  HLog log;
  FileSystem fs;
  Configuration conf;
  HRegionInfo regionInfo;
  Path regiondir;

  static class WriteState {
    // Set while a memcache flush is happening.
    volatile boolean flushing = false;
    // Set while a compaction is running.
    volatile boolean compacting = false;
    // Gets set by last flush before close.  If set, cannot compact or flush
    // again.
    volatile boolean writesEnabled = true;
  }
  
  volatile WriteState writestate = new WriteState();

  final int memcacheFlushSize;
  final int blockingMemcacheSize;
  protected final long threadWakeFrequency;
  protected final int optionalFlushCount;
  private final HLocking lock = new HLocking();
  private long desiredMaxFileSize;
  private final long minSequenceId;

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
   * @param rootDir root directory for HBase instance
   * @param fs is the filesystem.  
   * @param conf is global configuration settings.
   * @param regionInfo - HRegionInfo that describes the region
   * @param initialFiles If there are initial files (implying that the HRegion
   * is new), then read them from the supplied path.
   * 
   * @throws IOException
   */
  public HRegion(Path rootDir, HLog log, FileSystem fs, Configuration conf, 
      HRegionInfo regionInfo, Path initialFiles)
  throws IOException {
    this.rootDir = rootDir;
    this.log = log;
    this.fs = fs;
    this.conf = conf;
    this.regionInfo = regionInfo;
    this.memcache = new HMemcache();
    this.threadWakeFrequency = conf.getLong(THREAD_WAKE_FREQUENCY, 10 * 1000);
    this.optionalFlushCount =
      conf.getInt("hbase.hregion.memcache.optionalflushcount", 10);

    // Declare the regionName.  This is a unique string for the region, used to 
    // build a unique filename.
    this.regiondir = HRegion.getRegionDir(rootDir, this.regionInfo.regionName);
    Path oldLogFile = new Path(regiondir, HREGION_OLDLOGFILE_NAME);

    // Move prefab HStore files into place (if any).  This picks up split files
    // and any merges from splits and merges dirs.
    if(initialFiles != null && fs.exists(initialFiles)) {
      fs.rename(initialFiles, this.regiondir);
    }

    // Load in all the HStores.
    long maxSeqId = -1;
    for(Map.Entry<Text, HColumnDescriptor> e :
        this.regionInfo.tableDesc.families().entrySet()) {
      Text colFamily = HStoreKey.extractFamily(e.getKey());
      
      HStore store = new HStore(rootDir, this.regionInfo.regionName, 
          e.getValue(), fs, oldLogFile, conf); 
      
      stores.put(colFamily, store);
      
      long storeSeqId = store.getMaxSequenceId();
      if (storeSeqId > maxSeqId) {
        maxSeqId = storeSeqId;
      }
    }
    this.minSequenceId = maxSeqId;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Next sequence id for region " + regionInfo.getRegionName() +
        " is " + this.minSequenceId);
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

    // By default, we flush the cache when 16M.
    this.memcacheFlushSize = conf.getInt("hbase.hregion.memcache.flush.size",
      1024*1024*16);
    this.blockingMemcacheSize = this.memcacheFlushSize *
      conf.getInt("hbase.hregion.memcache.block.multiplier", 2);
    
    // By default we split region if a file > DEFAULT_MAX_FILE_SIZE.
    this.desiredMaxFileSize =
      conf.getLong("hbase.hregion.max.filesize", DEFAULT_MAX_FILE_SIZE);

    // HRegion is ready to go!
    this.writestate.compacting = false;
    LOG.info("region " + this.regionInfo.regionName + " available");
  }
  
  /**
   * @return Updates to this region need to have a sequence id that is >= to
   * the this number.
   */
  long getMinSequenceId() {
    return this.minSequenceId;
  }

  /** Returns a HRegionInfo object for this region */
  public HRegionInfo getRegionInfo() {
    return this.regionInfo;
  }

  /** returns true if region is closed */
  boolean isClosed() {
    return this.closed.get();
  }
  
  /**
   * Close down this HRegion.  Flush the cache, shut down each HStore, don't 
   * service any more calls.
   *
   * <p>This method could take some time to execute, so don't call it from a 
   * time-sensitive thread.
   * 
   * @return Vector of all the storage files that the HRegion's component 
   * HStores make use of.  It's a list of all HStoreFile objects. Returns empty
   * vector if already closed and null if judged that it should not close.
   * 
   * @throws IOException
   */
  public Vector<HStoreFile> close() throws IOException {
    return close(false);
  }
  
  /**
   * Close down this HRegion.  Flush the cache unless abort parameter is true,
   * Shut down each HStore, don't service any more calls.
   *
   * This method could take some time to execute, so don't call it from a 
   * time-sensitive thread.
   * 
   * @param abort true if server is aborting (only during testing)
   * @return Vector of all the storage files that the HRegion's component 
   * HStores make use of.  It's a list of HStoreFile objects.  Can be null if
   * we are not to close at this time or we are already closed.
   * 
   * @throws IOException
   */
  Vector<HStoreFile> close(boolean abort) throws IOException {
    if (isClosed()) {
      LOG.info("region " + this.regionInfo.regionName + " already closed");
      return null;
    }
    lock.obtainWriteLock();
    try {
      synchronized(writestate) {
        while(writestate.compacting || writestate.flushing) {
          try {
            writestate.wait();
          } catch (InterruptedException iex) {
            // continue
          }
        }
        // Disable compacting and flushing by background threads for this
        // region.
        writestate.writesEnabled = false;
      }
      
      // Write lock means no more row locks can be given out.  Wait on
      // outstanding row locks to come in before we close so we do not drop
      // outstanding updates.
      waitOnRowLocks();

      if (!abort) {
        // Don't flush the cache if we are aborting during a test.
        internalFlushcache();
      }

      Vector<HStoreFile> result = new Vector<HStoreFile>();
      for (HStore store: stores.values()) {
        result.addAll(store.close());
      }
      this.closed.set(true);
      LOG.info("closed " + this.regionInfo.regionName);
      return result;
    } finally {
      lock.releaseWriteLock();
    }
  }
  
  /*
   * Split the HRegion to create two brand-new ones.  This also closes
   * current HRegion.  Split should be fast since we don't rewrite store files
   * but instead create new 'reference' store files that read off the top and
   * bottom ranges of parent store files.
   * @param midKey Row to split on.
   * @param listener May be null.
   * @return two brand-new (and open) HRegions
   * @throws IOException
   */
  HRegion[] closeAndSplit(final Text midKey,
      final RegionUnavailableListener listener)
  throws IOException {
    checkMidKey(midKey);
    long startTime = System.currentTimeMillis();
    Path splits = getSplitsDir();
    HRegionInfo regionAInfo = new HRegionInfo(Math.abs(rand.nextLong()),
      this.regionInfo.tableDesc, this.regionInfo.startKey, midKey);
    Path dirA = getSplitRegionDir(splits, regionAInfo.regionName);
    if(fs.exists(dirA)) {
      throw new IOException("Cannot split; target file collision at " + dirA);
    }
    HRegionInfo regionBInfo = new HRegionInfo(Math.abs(rand.nextLong()),
      this.regionInfo.tableDesc, midKey, null);
    Path dirB = getSplitRegionDir(splits, regionBInfo.regionName);
    if(this.fs.exists(dirB)) {
      throw new IOException("Cannot split; target file collision at " + dirB);
    }

    // Notify the caller that we are about to close the region. This moves
    // us to the 'retiring' queue. Means no more updates coming in -- just
    // whatever is outstanding.
    if (listener != null) {
      listener.closing(getRegionName());
    }

    // Now close the HRegion.  Close returns all store files or null if not
    // supposed to close (? What to do in this case? Implement abort of close?)
    // Close also does wait on outstanding rows and calls a flush just-in-case.
    Vector<HStoreFile> hstoreFilesToSplit = close();
    if (hstoreFilesToSplit == null) {
      LOG.warn("Close came back null (Implement abort of close?)");
      throw new RuntimeException("close returned empty vector of HStoreFiles");
    }
    
    // Tell listener that region is now closed and that they can therefore
    // clean up any outstanding references.
    if (listener != null) {
      listener.closed(this.getRegionName());
    }
    
    // Split each store file.
    for(HStoreFile h: hstoreFilesToSplit) {
      // A reference to the bottom half of the hsf store file.
      HStoreFile.Reference aReference = new HStoreFile.Reference(
        getRegionName(), h.getFileId(), new HStoreKey(midKey),
        HStoreFile.Range.bottom);
      HStoreFile a = new HStoreFile(this.conf, splits,
        regionAInfo.regionName, h.getColFamily(), Math.abs(rand.nextLong()),
        aReference);
      // Reference to top half of the hsf store file.
      HStoreFile.Reference bReference = new HStoreFile.Reference(
        getRegionName(), h.getFileId(), new HStoreKey(midKey),
        HStoreFile.Range.top);
      HStoreFile b = new HStoreFile(this.conf, splits,
        regionBInfo.regionName, h.getColFamily(), Math.abs(rand.nextLong()),
        bReference);
      h.splitStoreFile(a, b, this.fs);
    }

    // Done!
    // Opening the region copies the splits files from the splits directory
    // under each region.
    HRegion regionA = new HRegion(rootDir, log, fs, conf, regionAInfo, dirA);
    HRegion regionB = new HRegion(rootDir, log, fs, conf, regionBInfo, dirB);

    // Cleanup
    boolean deleted = fs.delete(splits);    // Get rid of splits directory
    if (LOG.isDebugEnabled()) {
      LOG.debug("Cleaned up " + splits.toString() + " " + deleted);
    }
    HRegion regions[] = new HRegion [] {regionA, regionB};
    LOG.info("Region split of " + this.regionInfo.regionName + " complete; " +
      "new regions: " + regions[0].getRegionName() + ", " +
      regions[1].getRegionName() + ". Split took " +
      StringUtils.formatTimeDiff(System.currentTimeMillis(), startTime));
    return regions;
  }
  
  private void checkMidKey(final Text midKey) throws IOException {
    if(((this.regionInfo.startKey.getLength() != 0)
        && (this.regionInfo.startKey.compareTo(midKey) > 0))
        || ((this.regionInfo.endKey.getLength() != 0)
            && (this.regionInfo.endKey.compareTo(midKey) < 0))) {
      throw new IOException("Region splitkey must lie within region " +
        "boundaries.");
    }
  }
  
  private Path getSplitRegionDir(final Path splits, final Text regionName) {
    return HRegion.getRegionDir(splits, regionName);
  }
  
  private Path getSplitsDir() throws IOException {
    Path splits = new Path(this.regiondir, SPLITDIR);
    if(!this.fs.exists(splits)) {
      this.fs.mkdirs(splits);
    }
    return splits;
  }

  //////////////////////////////////////////////////////////////////////////////
  // HRegion accessors
  //////////////////////////////////////////////////////////////////////////////

  /** @return start key for region */
  public Text getStartKey() {
    return this.regionInfo.startKey;
  }

  /** @return end key for region */
  public Text getEndKey() {
    return this.regionInfo.endKey;
  }

  /** @return region id */
  public long getRegionId() {
    return this.regionInfo.regionId;
  }

  /** @return region name */
  public Text getRegionName() {
    return this.regionInfo.regionName;
  }

  /** @return root directory path */
  public Path getRootDir() {
    return rootDir;
  }

  /** @return HTableDescriptor for this region */
  public HTableDescriptor getTableDesc() {
    return this.regionInfo.tableDesc;
  }

  /** @return HLog in use for this region */
  public HLog getLog() {
    return this.log;
  }

  /** @return Configuration object */
  public Configuration getConf() {
    return this.conf;
  }

  /** @return region directory Path */
  public Path getRegionDir() {
    return this.regiondir;
  }

  /** @return FileSystem being used by this region */
  public FileSystem getFilesystem() {
    return this.fs;
  }

  //////////////////////////////////////////////////////////////////////////////
  // HRegion maintenance.  
  //
  // These methods are meant to be called periodically by the HRegionServer for 
  // upkeep.
  //////////////////////////////////////////////////////////////////////////////

  /*
   * Iterates through all the HStores and finds the one with the largest
   * MapFile size. If the size is greater than the (currently hard-coded)
   * threshold, returns true indicating that the region should be split. The
   * midKey for the largest MapFile is returned through the midKey parameter.
   * It is possible for us to rule the region non-splitable even in excess of
   * configured size.  This happens if region contains a reference file.  If
   * a reference file, the region can not be split.
   * @param midKey midKey of the largest MapFile
   * @return true if the region should be split. midKey is set by this method.
   * Check it for a midKey value on return.
   */
  boolean needsSplit(Text midKey) {
    lock.obtainReadLock();
    try {
      HStore.HStoreSize biggest = largestHStore(midKey);
      if (biggest == null) {
        return false;
      }
      long triggerSize =
        this.desiredMaxFileSize + (this.desiredMaxFileSize / 2);
      boolean split = (biggest.getAggregate() >= triggerSize);
      if (split) {
        if (!biggest.isSplitable()) {
          LOG.warn("Region " + getRegionName().toString() +
            " is NOT splitable though its aggregate size is " +
            StringUtils.humanReadableInt(biggest.getAggregate()) +
            " and desired size is " +
            StringUtils.humanReadableInt(this.desiredMaxFileSize));
          split = false;
        } else {
          LOG.info("Splitting " + getRegionName().toString() +
            " because largest aggregate size is " +
            StringUtils.humanReadableInt(biggest.getAggregate()) +
            " and desired size is " +
            StringUtils.humanReadableInt(this.desiredMaxFileSize));
        }
      }
      return split;
    } finally {
      lock.releaseReadLock();
    }
  }
  
  /**
   * @return returns size of largest HStore.  Also returns whether store is
   * splitable or not (Its not splitable if region has a store that has a
   * reference store file).
   */
  HStore.HStoreSize largestHStore(final Text midkey) {
    HStore.HStoreSize biggest = null;
    boolean splitable = true;
    lock.obtainReadLock();
    try {
      for(HStore h: stores.values()) {
        HStore.HStoreSize size = h.size(midkey);
        // If we came across a reference down in the store, then propagate
        // fact that region is not splitable.
        if (splitable) {
          splitable = size.splitable;
        }
        if (biggest == null) {
          biggest = size;
          continue;
        }
        if(size.getAggregate() > biggest.getAggregate()) { // Largest so far
          biggest = size;
        }
      }
      if (biggest != null) {
        biggest.setSplitable(splitable);
      }
      return biggest;
      
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
      for (HStore store: stores.values()) {
        if (store.needsCompaction()) {
          needsCompaction = true;
          LOG.debug(store.toString() + " needs compaction");
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
   * <p>This operation could block for a long time, so don't call it from a 
   * time-sensitive thread.
   *
   * @return Returns TRUE if the compaction has completed.  FALSE, if the
   * compaction was not carried out, because the HRegion is busy doing
   * something else storage-intensive (like flushing the cache). The caller
   * should check back later.
   */
  boolean compactStores() throws IOException {
    boolean shouldCompact = false;
    if (this.closed.get()) {
      return shouldCompact;
    }
    lock.obtainReadLock();
    try {
      synchronized (writestate) {
        if ((!writestate.compacting) &&
            writestate.writesEnabled) {
          writestate.compacting = true;
          shouldCompact = true;
        }
      }

      if (!shouldCompact) {
        LOG.info("NOT compacting region " +
          this.regionInfo.getRegionName().toString());
        return false;
      }

      long startTime = System.currentTimeMillis();
      LOG.info("starting compaction on region " +
        this.regionInfo.getRegionName().toString());
      for (HStore store : stores.values()) {
        store.compact();
      }
      LOG.info("compaction completed on region " +
        this.regionInfo.getRegionName().toString() + ". Took " +
        StringUtils.formatTimeDiff(System.currentTimeMillis(), startTime));
      return true;
      
    } finally {
      lock.releaseReadLock();
      synchronized (writestate) {
        writestate.compacting = false;
        writestate.notifyAll();
      }
    }
  }

  /**
   * Each HRegion is given a periodic chance to flush the cache, which it should
   * only take if there have been a lot of uncommitted writes.
   * @throws IOException
   * @throws DroppedSnapshotException Thrown when replay of hlog is required
   * because a Snapshot was not properly persisted.
   */
  void optionallyFlush() throws IOException {
    if(this.memcache.getSize() > this.memcacheFlushSize) {
      flushcache(false);
    } else if (this.memcache.getSize() > 0) {
      if (this.noFlushCount >= this.optionalFlushCount) {
        LOG.info("Optional flush called " + this.noFlushCount +
            " times when data present without flushing.  Forcing one.");
        flushcache(false);
      } else {
        // Only increment if something in the cache.
        // Gets zero'd when a flushcache is called.
        this.noFlushCount++;
      }
    }
  }

  /**
   * Flush the cache.  This is called periodically to minimize the amount of
   * log processing needed upon startup.
   * 
   * <p>The returned Vector is a list of all the files used by the component
   * HStores. It is a list of HStoreFile objects.  If the returned value is
   * NULL, then the flush could not be executed, because the HRegion is busy
   * doing something else storage-intensive.  The caller should check back
   * later.
   *
   * <p>This method may block for some time, so it should not be called from a 
   * time-sensitive thread.
   * 
   * @param disableFutureWrites indicates that the caller intends to 
   * close() the HRegion shortly, so the HRegion should not take on any new and 
   * potentially long-lasting disk operations. This flush() should be the final
   * pre-close() disk operation.
   * @throws IOException
   * @throws DroppedSnapshotException Thrown when replay of hlog is required
   * because a Snapshot was not properly persisted.
   */
  void flushcache(boolean disableFutureWrites)
  throws IOException {
    if (this.closed.get()) {
      return;
    }
    this.noFlushCount = 0;
    boolean shouldFlush = false;
    synchronized(writestate) {
      if((!writestate.flushing) && writestate.writesEnabled) {
        writestate.flushing = true;
        shouldFlush = true;
        if(disableFutureWrites) {
          writestate.writesEnabled = false;
        }
      }
    }

    if(!shouldFlush) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("NOT flushing memcache for region " +
          this.regionInfo.regionName);
      }
      return;  
    }
    
    try {
      internalFlushcache();
    } finally {
      synchronized (writestate) {
        writestate.flushing = false;
        writestate.notifyAll();
      }
    }
  }

  /**
   * Flushing the cache is a little tricky. We have a lot of updates in the
   * HMemcache, all of which have also been written to the log. We need to
   * write those updates in the HMemcache out to disk, while being able to
   * process reads/writes as much as possible during the flush operation. Also,
   * the log has to state clearly the point in time at which the HMemcache was
   * flushed. (That way, during recovery, we know when we can rely on the
   * on-disk flushed structures and when we have to recover the HMemcache from
   * the log.)
   * 
   * <p>So, we have a three-step process:
   * 
   * <ul><li>A. Flush the memcache to the on-disk stores, noting the current
   * sequence ID for the log.<li>
   * 
   * <li>B. Write a FLUSHCACHE-COMPLETE message to the log, using the sequence
   * ID that was current at the time of memcache-flush.</li>
   * 
   * <li>C. Get rid of the memcache structures that are now redundant, as
   * they've been flushed to the on-disk HStores.</li>
   * </ul>
   * <p>This method is protected, but can be accessed via several public
   * routes.
   * 
   * <p> This method may block for some time.
   * @throws IOException
   * @throws DroppedSnapshotException Thrown when replay of hlog is required
   * because a Snapshot was not properly persisted.
   */
  void internalFlushcache() throws IOException {
    long startTime = -1;
    if(LOG.isDebugEnabled()) {
      startTime = System.currentTimeMillis();
      LOG.debug("Started memcache flush for region " +
        this.regionInfo.regionName + ". Size " +
        StringUtils.humanReadableInt(this.memcache.getSize()));
    }

    // We pass the log to the HMemcache, so we can lock down both
    // simultaneously.  We only have to do this for a moment: we need the
    // HMemcache state at the time of a known log sequence number. Since
    // multiple HRegions may write to a single HLog, the sequence numbers may
    // zoom past unless we lock it.
    //
    // When execution returns from snapshotMemcacheForLog() with a non-NULL
    // value, the HMemcache will have a snapshot object stored that must be
    // explicitly cleaned up using a call to deleteSnapshot() or by calling
    // abort.
    //
    HMemcache.Snapshot retval = memcache.snapshotMemcacheForLog(log);
    if(retval == null || retval.memcacheSnapshot == null) {
      LOG.debug("Finished memcache flush; empty snapshot");
      return;
    }

    // Any failure from here on out will be catastrophic requiring server
    // restart so hlog content can be replayed and put back into the memcache.
    // Otherwise, the snapshot content while backed up in the hlog, it will not
    // be part of the current running servers state.
    try {
      long logCacheFlushId = retval.sequenceId;
      if(LOG.isDebugEnabled()) {
        LOG.debug("Snapshotted memcache for region " +
            this.regionInfo.regionName + " with sequence id " +
            retval.sequenceId + " and entries " +
            retval.memcacheSnapshot.size());
      }

      try {
        // A.  Flush memcache to all the HStores.
        // Keep running vector of all store files that includes both old and the
        // just-made new flush store file.
        for (HStore hstore: stores.values()) {
          hstore.flushCache(retval.memcacheSnapshot, retval.sequenceId);
        }
      } catch (IOException e) {
        // An exception here means that the snapshot was not persisted.
        // The hlog needs to be replayed so its content is restored to memcache.
        // Currently, only a server restart will do this.
        this.log.abortCacheFlush();
        throw new DroppedSnapshotException(e.getMessage());
      }

      // If we get to here, the HStores have been written. If we get an
      // error in completeCacheFlush it will release the lock it is holding

      // B.  Write a FLUSHCACHE-COMPLETE message to the log.
      //     This tells future readers that the HStores were emitted correctly,
      //     and that all updates to the log for this regionName that have lower 
      //     log-sequence-ids can be safely ignored.
      this.log.completeCacheFlush(this.regionInfo.regionName,
          regionInfo.tableDesc.getName(), logCacheFlushId);

    } finally {
      // C. Delete the now-irrelevant memcache snapshot; its contents have been 
      //    dumped to disk-based HStores or, if error, clear aborted snapshot.
      this.memcache.deleteSnapshot();
    }
    
    // D. Finally notify anyone waiting on memcache to clear:
    // e.g. checkResources().
    synchronized(this) {
      notifyAll();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Finished memcache flush for region " +
        this.regionInfo.regionName + " in " +
          (System.currentTimeMillis() - startTime) + "ms");
    }
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
    if (this.closed.get()) {
      throw new IOException("Region " + this.getRegionName().toString() +
        " closed");
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

  private byte [][] get(final HStoreKey key, final int numVersions)
  throws IOException {
    lock.obtainReadLock();
    try {
      // Check the memcache
      byte [][] memcacheResult = this.memcache.get(key, numVersions);
      // If we got sufficient versions from memcache, return.
      if (memcacheResult != null && memcacheResult.length == numVersions) {
        return memcacheResult;
      }

      // Check hstore for more versions.
      Text colFamily = HStoreKey.extractFamily(key.getColumn());
      HStore targetStore = stores.get(colFamily);
      if(targetStore == null) {
        // There are no stores.  Return what we got from memcache.
        return memcacheResult;
      }
      
      // Update the number of versions we need to fetch from the store.
      int amendedNumVersions = numVersions;
      if (memcacheResult != null) {
        amendedNumVersions -= memcacheResult.length;
      }
      byte [][] result =
        targetStore.get(key, amendedNumVersions, this.memcache);
      if (result == null) {
        result = memcacheResult;
      } else if (memcacheResult != null) {
        // We have results from both memcache and from stores.  Put them
        // together in an array in the proper order.
        byte [][] storeResult = result;
        result = new byte [memcacheResult.length + result.length][];
        for (int i = 0; i < memcacheResult.length; i++) {
          result[i] = memcacheResult[i];
        }
        for (int i = 0; i < storeResult.length; i++) {
          result[i + memcacheResult.length] = storeResult[i];
        }
      }
      return result;
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
   * Get all keys matching the origin key's row/column/timestamp and those
   * of an older vintage
   * Default access so can be accessed out of {@link HRegionServer}.
   * @param origin Where to start searching.
   * @return Ordered list of keys going from newest on back.
   * @throws IOException
   */
  List<HStoreKey> getKeys(final HStoreKey origin) throws IOException {
    return getKeys(origin, ALL_VERSIONS);
  }
  
  /**
   * Get <code>versions</code> keys matching the origin key's
   * row/column/timestamp and those of an older vintage
   * Default access so can be accessed out of {@link HRegionServer}.
   * @param origin Where to start searching.
   * @param versions How many versions to return. Pass
   * {@link HConstants.ALL_VERSIONS} to retrieve all.
   * @return Ordered list of <code>versions</code> keys going from newest back.
   * @throws IOException
   */
  List<HStoreKey> getKeys(final HStoreKey origin, final int versions)
  throws IOException {
    List<HStoreKey> keys = this.memcache.getKeys(origin, versions);
    if (versions != ALL_VERSIONS && keys.size() >= versions) {
      return keys;
    }
    // Check hstore for more versions.
    Text colFamily = HStoreKey.extractFamily(origin.getColumn());
    HStore targetStore = stores.get(colFamily);
    if (targetStore != null) {
      // Pass versions without modification since in the store getKeys, it
      // includes the size of the passed <code>keys</code> array when counting.
      keys = targetStore.getKeys(origin, keys, versions);
    }
    return keys;
  }

  /**
   * Return an iterator that scans over the HRegion, returning the indicated 
   * columns for only the rows that match the data filter.  This Iterator must
   * be closed by the caller.
   *
   * @param cols columns to scan. If column name is a column family, all
   * columns of the specified column family are returned.  Its also possible
   * to pass a regex in the column qualifier. A column qualifier is judged to
   * be a regex if it contains at least one of the following characters:
   * <code>\+|^&*$[]]}{)(</code>.
   * @param firstRow row which is the starting point of the scan
   * @param timestamp only return rows whose timestamp is <= this value
   * @param filter row filter
   * @return HScannerInterface
   * @throws IOException
   */
  public HInternalScannerInterface getScanner(Text[] cols, Text firstRow,
      long timestamp, RowFilterInterface filter)
  throws IOException {
    lock.obtainReadLock();
    try {
      TreeSet<Text> families = new TreeSet<Text>();
      for(int i = 0; i < cols.length; i++) {
        families.add(HStoreKey.extractFamily(cols[i]));
      }
      List<HStore> storelist = new ArrayList<HStore>();
      for (Text family: families) {
        HStore s = stores.get(family);
        if (s == null) {
          continue;
        }
        storelist.add(stores.get(family));
      }
      return new HScanner(cols, firstRow, timestamp, memcache,
        storelist.toArray(new HStore [storelist.size()]), filter);
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
   * @return lock id
   * @throws IOException
   * @see #put(long, Text, byte[])
   */
  public long startUpdate(Text row) throws IOException {
    // Do a rough check that we have resources to accept a write.  The check is
    // 'rough' in that between the resource check and the call to obtain a 
    // read lock, resources may run out.  For now, the thought is that this
    // will be extremely rare; we'll deal with it when it happens.
    checkResources();

    // Get a read lock. We will not be able to get one if we are closing or
    // if this region is being split.  In neither case should we be allowing
    // updates.
    this.lock.obtainReadLock();
    if (this.closed.get()) {
      throw new IOException("Region " + this.getRegionName().toString() +
        " closed");
    }
    try {
      // We obtain a per-row lock, so other clients will block while one client
      // performs an update. The read lock is released by the client calling
      // #commit or #abort or if the HRegionServer lease on the lock expires.
      // See HRegionServer#RegionListener for how the expire on HRegionServer
      // invokes a HRegion#abort.
      return obtainRowLock(row);
    } finally {
      this.lock.releaseReadLock();
    }
  }
  
  /*
   * Check if resources to support an update.
   * 
   * For now, just checks memcache saturation.
   * 
   * Here we synchronize on HRegion, a broad scoped lock.  Its appropriate
   * given we're figuring in here whether this region is able to take on
   * writes.  This is only method with a synchronize (at time of writing),
   * this and the synchronize on 'this' inside in internalFlushCache to send
   * the notify.
   */
  private synchronized void checkResources() {
    boolean blocked = false;
    
    while (!checkCommitsSinceFlush()) {
      if (!blocked) {
        LOG.info("Blocking updates for '" + Thread.currentThread().getName() +
            "': Memcache size " +
            StringUtils.humanReadableInt(this.memcache.getSize()) +
            " is >= than blocking " +
            StringUtils.humanReadableInt(this.blockingMemcacheSize) + " size");
      }

      blocked = true;
      try {
        wait(threadWakeFrequency);
      } catch (InterruptedException e) {
        // continue;
      }
    }
    if (blocked) {
      LOG.info("Unblocking updates for region " + getRegionName() + " '" + 
        Thread.currentThread().getName() + "'");
    }
  }
  
  /*
   * @return True if commits since flush is under the blocking threshold.
   */
  private boolean checkCommitsSinceFlush() {
    return this.memcache.getSize() < this.blockingMemcacheSize;
  }

  /**
   * Put a cell value into the locked row.  The user indicates the row-lock, the
   * target column, and the desired value.  This stuff is set into a temporary 
   * memory area until the user commits the change, at which point it's logged 
   * and placed into the memcache.
   *
   * This method really just tests the input, then calls an internal localput() 
   * method.
   *
   * @param lockid lock id obtained from startUpdate
   * @param targetCol name of column to be updated
   * @param val new value for column
   * @throws IOException
   */
  public void put(long lockid, Text targetCol, byte [] val) throws IOException {
    if (HGlobals.deleteBytes.compareTo(val) == 0) {
      throw new IOException("Cannot insert value: " + val);
    }
    localput(lockid, targetCol, val);
  }

  /**
   * Delete a value or write a value.
   * This is a just a convenience method for put().
   * @param lockid lock id obtained from startUpdate
   * @param targetCol name of column to be deleted
   * @throws IOException
   */
  public void delete(long lockid, Text targetCol) throws IOException {
    localput(lockid, targetCol, HGlobals.deleteBytes.get());
  }
  
  /**
   * Delete all cells of the same age as the passed timestamp or older.
   * @param row
   * @param column
   * @param ts Delete all entries that have this timestamp or older
   * @throws IOException
   */
  public void deleteAll(final Text row, final Text column, final long ts)
  throws IOException {
    deleteMultiple(row, column, ts, ALL_VERSIONS);
  }
  
  /**
   * Delete one or many cells.
   * Used to support {@link #deleteAll(Text, Text, long)} and deletion of
   * latest cell.
   * @param row
   * @param column
   * @param ts Timestamp to start search on.
   * @param versions How many versions to delete. Pass
   * {@link HConstants.ALL_VERSIONS} to delete all.
   * @throws IOException
   */
  void deleteMultiple(final Text row, final Text column, final long ts,
    final int versions)
  throws IOException {
    lock.obtainReadLock();
    try {
      checkColumn(column);
      HStoreKey origin = new HStoreKey(row, column, ts);
      synchronized(row) {
        List<HStoreKey> keys = getKeys(origin, versions);
        if (keys.size() > 0) {
          TreeMap<Text, byte []> edits = new TreeMap<Text, byte []>();
          edits.put(column, HGlobals.deleteBytes.get());
          for (HStoreKey key: keys) {
            update(row, key.getTimestamp(), edits);
          }
        }
      }
    } finally {
      lock.releaseReadLock();
    }
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
      Long lid = Long.valueOf(lockid);
      TreeMap<Text, byte []> targets = this.targetColumns.get(lid);
      if (targets == null) {
        targets = new TreeMap<Text, byte []>();
        this.targetColumns.put(lid, targets);
      }
      targets.put(targetCol, val);
    }
  }

  /**
   * Abort a pending set of writes. This dumps from memory all in-progress
   * writes associated with the given row-lock.  These values have not yet
   * been placed in memcache or written to the log.
   *
   * @param lockid lock id obtained from startUpdate
   * @throws IOException
   */
  public void abort(long lockid) throws IOException {
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
      
      this.targetColumns.remove(Long.valueOf(lockid));
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
   * @param timestamp the time to associate with this change.
   * @throws IOException
   */
  public void commit(final long lockid, final long timestamp)
  throws IOException {
    // Remove the row from the pendingWrites list so 
    // that repeated executions won't screw this up.
    Text row = getRowFromLock(lockid);
    if(row == null) {
      throw new LockException("No write lock for lockid " + lockid);
    }
    
    // This check makes sure that another thread from the client
    // hasn't aborted/committed the write-operation
    synchronized(row) {
      Long lid = Long.valueOf(lockid);
      update(row, timestamp, this.targetColumns.get(lid));
      targetColumns.remove(lid);
      releaseRowLock(row);
    }
  }
  
  /**
   * This method for unit testing only.
   * Does each operation individually so can do appropriate
   * {@link HConstants#LATEST_TIMESTAMP} action.  Tries to mimic how
   * {@link HRegionServer#batchUpdate(Text, long, org.apache.hadoop.hbase.io.BatchUpdate)}
   * works when passed a timestamp of LATEST_TIMESTAMP.
   * @param lockid Lock for row we're to commit.
   * @throws IOException 
   * @throws IOException
   * @see {@link #commit(long, long)}
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
      Long lid = Long.valueOf(lockid);
      TreeMap<Text, byte []> updatesByColumn = this.targetColumns.get(lid);
      // Run updates one at a time so we can supply appropriate timestamp
      long now = System.currentTimeMillis();
      for (Map.Entry<Text, byte []>e: updatesByColumn.entrySet()) {
        if (HGlobals.deleteBytes.equals(e.getValue())) {
          // Its a delete.  Delete latest.  deleteMultiple calls update for us.
          // Actually regets the row lock but since we already have it, should
          // be fine.
          deleteMultiple(row, e.getKey(), LATEST_TIMESTAMP, 1);
          continue;
        }
        // Must be a 'put'.
        TreeMap<Text, byte []> putEdit = new TreeMap<Text, byte []>();
        putEdit.put(e.getKey(), e.getValue());
        update(row, now, putEdit);
      }
      this.targetColumns.remove(lid);
      releaseRowLock(row);
    }
  }
   
  /* 
   * Add updates first to the hlog and then add values to memcache.
   * Warning: Assumption is caller has lock on passed in row.
   * @param row Row to update.
   * @param timestamp Timestamp to record the updates against
   * @param updatesByColumn Cell updates by column
   * @throws IOException
   */
  private void update(final Text row, final long timestamp,
    final TreeMap<Text, byte []> updatesByColumn)
  throws IOException {
    if (updatesByColumn == null || updatesByColumn.size() <= 0) {
      return;
    }
    this.log.append(regionInfo.regionName, regionInfo.tableDesc.getName(),
        row, updatesByColumn, timestamp);
    this.memcache.add(row, updatesByColumn, timestamp);
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
  
  /**
   * Make sure this is a valid column for the current table
   * @param columnName
   * @throws IOException
   */
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
      
      Long lid = Long.valueOf(Math.abs(rand.nextLong()));
      rowsToLocks.put(row, lid);
      locksToRows.put(lid, row);
      rowsToLocks.notifyAll();
      return lid.longValue();
    }
  }
  
  Text getRowFromLock(long lockid) {
    // Pattern is that all access to rowsToLocks and/or to
    // locksToRows is via a lock on rowsToLocks.
    synchronized(rowsToLocks) {
      return locksToRows.get(Long.valueOf(lockid));
    }
  }
  
  /** 
   * Release the row lock!
   * @param lock Name of row whose lock we are to release
   */
  void releaseRowLock(Text row) {
    synchronized(rowsToLocks) {
      long lockid = rowsToLocks.remove(row).longValue();
      locksToRows.remove(Long.valueOf(lockid));
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
  
  /** {@inheritDoc} */
  @Override
  public String toString() {
    return getRegionName().toString();
  }

  /**
   * HScanner is an iterator through a bunch of rows in an HRegion.
   */
  private static class HScanner implements HInternalScannerInterface {
    private HInternalScannerInterface[] scanners;
    private TreeMap<Text, byte []>[] resultSets;
    private HStoreKey[] keys;
    private boolean wildcardMatch = false;
    private boolean multipleMatchers = false;
    private RowFilterInterface dataFilter;

    /** Create an HScanner with a handle on many HStores. */
    @SuppressWarnings("unchecked")
    HScanner(Text[] cols, Text firstRow, long timestamp, HMemcache memcache,
        HStore[] stores, RowFilterInterface filter) throws IOException {  
      this.dataFilter = filter;
      if (null != dataFilter) {
        dataFilter.reset();
      }
      this.scanners = new HInternalScannerInterface[stores.length + 1];
      this.resultSets = new TreeMap[scanners.length];
      this.keys = new HStoreKey[scanners.length];

      // Advance to the first key in each store.
      // All results will match the required column-set and scanTime.
      
      // NOTE: the memcache scanner should be the first scanner
      try {
        HInternalScannerInterface scanner =
          memcache.getScanner(timestamp, cols, firstRow);
        if (scanner.isWildcardScanner()) {
          this.wildcardMatch = true;
        }
        if (scanner.isMultipleMatchScanner()) {
          this.multipleMatchers = true;
        }
        scanners[0] = scanner;

        for (int i = 0; i < stores.length; i++) {
          scanner = stores[i].getScanner(timestamp, cols, firstRow);
          if (scanner.isWildcardScanner()) {
            this.wildcardMatch = true;
          }
          if (scanner.isMultipleMatchScanner()) {
            this.multipleMatchers = true;
          }
          scanners[i + 1] = scanner;
        }

      } catch(IOException e) {
        for (int i = 0; i < this.scanners.length; i++) {
          if(scanners[i] != null) {
            closeScanner(i);
          }
        }
        throw e;
      }
      for (int i = 0; i < scanners.length; i++) {
        keys[i] = new HStoreKey();
        resultSets[i] = new TreeMap<Text, byte []>();
        if(scanners[i] != null && !scanners[i].next(keys[i], resultSets[i])) {
          closeScanner(i);
        }
      }
    }

    public boolean isWildcardScanner() {
      return wildcardMatch;
    }
    
    public boolean isMultipleMatchScanner() {
      return multipleMatchers;
    }

    public boolean next(HStoreKey key, SortedMap<Text, byte[]> results)
    throws IOException {
      // Filtered flag is set by filters.  If a cell has been 'filtered out'
      // -- i.e. it is not to be returned to the caller -- the flag is 'true'.
      boolean filtered = true;
      boolean moreToFollow = true;
      while (filtered && moreToFollow) {
        // Find the lowest-possible key.
        Text chosenRow = null;
        long chosenTimestamp = -1;
        for (int i = 0; i < this.keys.length; i++) {
          if (scanners[i] != null &&
              (chosenRow == null ||
              (keys[i].getRow().compareTo(chosenRow) < 0) ||
              ((keys[i].getRow().compareTo(chosenRow) == 0) &&
              (keys[i].getTimestamp() > chosenTimestamp)))) {
            chosenRow = new Text(keys[i].getRow());
            chosenTimestamp = keys[i].getTimestamp();
          }
        }
        
        // Filter whole row by row key?
        filtered = dataFilter != null? dataFilter.filter(chosenRow) : false;

        // Store the key and results for each sub-scanner. Merge them as
        // appropriate.
        if (chosenTimestamp >= 0 && !filtered) {
          // Here we are setting the passed in key with current row+timestamp
          key.setRow(chosenRow);
          key.setVersion(chosenTimestamp);
          key.setColumn(HConstants.EMPTY_TEXT);
          // Keep list of deleted cell keys within this row.  We need this
          // because as we go through scanners, the delete record may be in an
          // early scanner and then the same record with a non-delete, non-null
          // value in a later. Without history of what we've seen, we'll return
          // deleted values. This List should not ever grow too large since we
          // are only keeping rows and columns that match those set on the
          // scanner and which have delete values.  If memory usage becomes a
          // problem, could redo as bloom filter.
          List<HStoreKey> deletes = new ArrayList<HStoreKey>();
          for (int i = 0; i < scanners.length && !filtered; i++) {
            while ((scanners[i] != null
                && !filtered
                && moreToFollow)
                && (keys[i].getRow().compareTo(chosenRow) == 0)) {
              // If we are doing a wild card match or there are multiple
              // matchers per column, we need to scan all the older versions of 
              // this row to pick up the rest of the family members
              if (!wildcardMatch
                  && !multipleMatchers
                  && (keys[i].getTimestamp() != chosenTimestamp)) {
                break;
              }

              // Filter out null criteria columns that are not null
              if (dataFilter != null) {
                filtered = dataFilter.filterNotNull(resultSets[i]);
              }

              // NOTE: We used to do results.putAll(resultSets[i]);
              // but this had the effect of overwriting newer
              // values with older ones. So now we only insert
              // a result if the map does not contain the key.
              HStoreKey hsk = new HStoreKey(key.getRow(), EMPTY_TEXT,
                key.getTimestamp());
              for (Map.Entry<Text, byte[]> e : resultSets[i].entrySet()) {
                hsk.setColumn(e.getKey());
                if (HGlobals.deleteBytes.equals(e.getValue())) {
                  if (!deletes.contains(hsk)) {
                    // Key changes as we cycle the for loop so add a copy to
                    // the set of deletes.
                    deletes.add(new HStoreKey(hsk));
                  }
                } else if (!deletes.contains(hsk) &&
                    !filtered &&
                    moreToFollow &&
                    !results.containsKey(e.getKey())) {
                  if (dataFilter != null) {
                    // Filter whole row by column data?
                    filtered =
                        dataFilter.filter(chosenRow, e.getKey(), e.getValue());
                    if (filtered) {
                      results.clear();
                      break;
                    }
                  }
                  results.put(e.getKey(), e.getValue());
                }
              }
              resultSets[i].clear();
              if (!scanners[i].next(keys[i], resultSets[i])) {
                closeScanner(i);
              }
            }
          }          
        }
        
        for (int i = 0; i < scanners.length; i++) {
          // If the current scanner is non-null AND has a lower-or-equal
          // row label, then its timestamp is bad. We need to advance it.
          while ((scanners[i] != null) &&
              (keys[i].getRow().compareTo(chosenRow) <= 0)) {
            resultSets[i].clear();
            if (!scanners[i].next(keys[i], resultSets[i])) {
              closeScanner(i);
            }
          }
        }

        moreToFollow = chosenTimestamp >= 0;
        
        if (dataFilter != null) {
          if (moreToFollow) {
            dataFilter.rowProcessed(filtered, chosenRow);
          }
          if (dataFilter.filterAllRemaining()) {
            moreToFollow = false;
            LOG.debug("page limit");
          }
        }
        if (LOG.isDebugEnabled()) {
          if (this.dataFilter != null) {
            LOG.debug("ROWKEY = " + chosenRow + ", FILTERED = " + filtered);
          }
        }
        
        if (results.size() <= 0 && !filtered) {
          // There were no results found for this row.  Marked it as 
          // 'filtered'-out otherwise we will not move on to the next row.
          filtered = true;
        }
      }
      
      // If we got no results, then there is no more to follow.
      if (results == null || results.size() <= 0) {
        moreToFollow = false;
      }
      
      // Make sure scanners closed if no more results
      if (!moreToFollow) {
        for (int i = 0; i < scanners.length; i++) {
          if (null != scanners[i]) {
            closeScanner(i);
          }
        }
      }
      
      return moreToFollow;
    }

    
    /** Shut down a single scanner */
    void closeScanner(int i) {
      try {
        try {
          scanners[i].close();
        } catch (IOException e) {
          LOG.warn("Failed closeing scanner " + i, e);
        }
      } finally {
        scanners[i] = null;
        keys[i] = null;
        resultSets[i] = null;
      }
    }

    /**
     * {@inheritDoc}
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
   * Convenience method creating new HRegions. Used by createTable and by the
   * bootstrap code in the HMaster constructor.
   * Note, this method creates an {@link HLog} for the created region. It
   * needs to be closed explicitly.  Use {@link HRegion#getLog()} to get
   * access.
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
    Path regionDir = HRegion.getRegionDir(rootDir, info.regionName);
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
    meta.put(writeid, COL_REGIONINFO, Writables.getBytes(r.getRegionInfo()));
    meta.commit(writeid, System.currentTimeMillis());
  }
  
  /**
   * Deletes all the files for a HRegion
   * 
   * @param fs the file system object
   * @param baseDirectory base directory for HBase
   * @param regionName name of the region to delete
   * @throws IOException
   * @return True if deleted.
   */
  static boolean deleteRegion(FileSystem fs, Path baseDirectory,
      Text regionName) throws IOException {
    Path p = HRegion.getRegionDir(fs.makeQualified(baseDirectory), regionName);
    return fs.delete(p);
  }

  /**
   * Computes the Path of the HRegion
   * 
   * @param dir hbase home directory
   * @param regionName name of the region
   * @return Path of HRegion directory
   */
  public static Path getRegionDir(final Path dir, final Text regionName) {
    return new Path(dir, new Path(HREGIONDIR_PREFIX + regionName));
  }
}
