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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.io.BatchOperation;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
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
 * <p>Locking at the HRegion level serves only one purpose: preventing the
 * region from being closed (and consequently split) while other operations
 * are ongoing. Each row level operation obtains both a row lock and a region
 * read lock for the duration of the operation. While a scanner is being
 * constructed, getScanner holds a read lock. If the scanner is successfully
 * constructed, it holds a read lock until it is closed. A close takes out a
 * write lock and consequently will block for ongoing operations and will block
 * new operations from starting while the close is in progress.
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
  static final String SPLITDIR = "splits";
  static final String MERGEDIR = "merges";
  static final Random rand = new Random();
  static final Log LOG = LogFactory.getLog(HRegion.class);
  final AtomicBoolean closed = new AtomicBoolean(false);

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

    HBaseConfiguration conf = a.getConf();
    HTableDescriptor tabledesc = a.getTableDesc();
    HLog log = a.getLog();
    Path basedir = a.getBaseDir();
    Text startKey = a.getStartKey();
    Text endKey = b.getEndKey();
    Path merges = new Path(a.getRegionDir(), MERGEDIR);
    if(! fs.exists(merges)) {
      fs.mkdirs(merges);
    }

    HRegionInfo newRegionInfo = new HRegionInfo(tabledesc, startKey, endKey);
    Path newRegionDir =
      HRegion.getRegionDir(merges, newRegionInfo.getEncodedName());
    if(fs.exists(newRegionDir)) {
      throw new IOException("Cannot merge; target file collision at " +
          newRegionDir);
    }

    LOG.info("starting merge of regions: " + a.getRegionName() + " and " +
        b.getRegionName() + " into new region " + newRegionInfo.toString());

    Map<Text, List<HStoreFile>> byFamily =
      new TreeMap<Text, List<HStoreFile>>();
    byFamily = filesByFamily(byFamily, a.close());
    byFamily = filesByFamily(byFamily, b.close());
    for (Map.Entry<Text, List<HStoreFile>> es : byFamily.entrySet()) {
      Text colFamily = es.getKey();
      List<HStoreFile> srcFiles = es.getValue();
      HStoreFile dst = new HStoreFile(conf, fs, merges,
          newRegionInfo.getEncodedName(), colFamily, -1, null);
      dst.mergeStoreFiles(srcFiles, fs, conf);
    }

    // Done
    // Construction moves the merge files into place under region.
    HRegion dstRegion = new HRegion(basedir, log, fs, conf, newRegionInfo,
        newRegionDir, null);

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
  private static Map<Text, List<HStoreFile>> filesByFamily(
      Map<Text, List<HStoreFile>> byFamily, List<HStoreFile> storeFiles) {
    for(HStoreFile src: storeFiles) {
      List<HStoreFile> v = byFamily.get(src.getColFamily());
      if(v == null) {
        v = new ArrayList<HStoreFile>();
        byFamily.put(src.getColFamily(), v);
      }
      v.add(src);
    }
    return byFamily;
  }

  //////////////////////////////////////////////////////////////////////////////
  // Members
  //////////////////////////////////////////////////////////////////////////////

  volatile Map<Text, Long> rowsToLocks = new ConcurrentHashMap<Text, Long>();
  volatile Map<Long, Text> locksToRows = new ConcurrentHashMap<Long, Text>();
  volatile Map<Text, HStore> stores = new ConcurrentHashMap<Text, HStore>();
  volatile Map<Long, TreeMap<HStoreKey, byte []>> targetColumns =
    new ConcurrentHashMap<Long, TreeMap<HStoreKey, byte []>>();

  final AtomicLong memcacheSize = new AtomicLong(0);

  final Path basedir;
  final HLog log;
  final FileSystem fs;
  final HBaseConfiguration conf;
  final HRegionInfo regionInfo;
  final Path regiondir;
  private final Path regionCompactionDir;

  /*
   * Data structure of write state flags used coordinating flushes,
   * compactions and closes.
   */
  static class WriteState {
    // Set while a memcache flush is happening.
    volatile boolean flushing = false;
    // Set while a compaction is running.
    volatile boolean compacting = false;
    // Gets set in close. If set, cannot compact or flush again.
    volatile boolean writesEnabled = true;
  }

  volatile WriteState writestate = new WriteState();

  final int memcacheFlushSize;
  private volatile long lastFlushTime;
  final CacheFlushListener flushListener;
  final int blockingMemcacheSize;
  protected final long threadWakeFrequency;
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final Integer updateLock = new Integer(0);
  private final Integer splitLock = new Integer(0);
  private final long desiredMaxFileSize;
  private final long minSequenceId;
  final AtomicInteger activeScannerCount = new AtomicInteger(0);

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
   * @param basedir qualified path of directory where region should be located,
   * usually the table directory.
   * @param fs is the filesystem.  
   * @param conf is global configuration settings.
   * @param regionInfo - HRegionInfo that describes the region
   * @param initialFiles If there are initial files (implying that the HRegion
   * is new), then read them from the supplied path.
   * @param listener an object that implements CacheFlushListener or null
   * 
   * @throws IOException
   */
  public HRegion(Path basedir, HLog log, FileSystem fs, HBaseConfiguration conf, 
      HRegionInfo regionInfo, Path initialFiles, CacheFlushListener listener)
    throws IOException {
    
    this.basedir = basedir;
    this.log = log;
    this.fs = fs;
    this.conf = conf;
    this.regionInfo = regionInfo;
    this.threadWakeFrequency = conf.getLong(THREAD_WAKE_FREQUENCY, 10 * 1000);
    this.regiondir = new Path(basedir, this.regionInfo.getEncodedName());
    Path oldLogFile = new Path(regiondir, HREGION_OLDLOGFILE_NAME);

    this.regionCompactionDir =
      new Path(getCompactionDir(basedir), this.regionInfo.getEncodedName());

    // Move prefab HStore files into place (if any).  This picks up split files
    // and any merges from splits and merges dirs.
    if(initialFiles != null && fs.exists(initialFiles)) {
      fs.rename(initialFiles, this.regiondir);
    }

    // Load in all the HStores.
    long maxSeqId = -1;
    for(HColumnDescriptor c :
      this.regionInfo.getTableDesc().families().values()) {

      HStore store = new HStore(this.basedir, this.regionInfo, c, this.fs,
          oldLogFile, this.conf);

      stores.put(c.getFamilyName(), store);

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

    // By default, we flush the cache when 64M.
    this.memcacheFlushSize = conf.getInt("hbase.hregion.memcache.flush.size",
      1024*1024*64);
    this.flushListener = listener;
    this.blockingMemcacheSize = this.memcacheFlushSize *
      conf.getInt("hbase.hregion.memcache.block.multiplier", 1);

    // By default we split region if a file > DEFAULT_MAX_FILE_SIZE.
    this.desiredMaxFileSize =
      conf.getLong("hbase.hregion.max.filesize", DEFAULT_MAX_FILE_SIZE);

    // HRegion is ready to go!
    this.writestate.compacting = false;
    this.lastFlushTime = System.currentTimeMillis();
    LOG.info("region " + this.regionInfo.getRegionName() + " available");
  }
  
  /**
   * @return Updates to this region need to have a sequence id that is >= to
   * the this number.
   */
  long getMinSequenceId() {
    return this.minSequenceId;
  }

  /** @return a HRegionInfo object for this region */
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
  public List<HStoreFile> close() throws IOException {
    return close(false, null);
  }
  
  /**
   * Close down this HRegion.  Flush the cache unless abort parameter is true,
   * Shut down each HStore, don't service any more calls.
   *
   * This method could take some time to execute, so don't call it from a 
   * time-sensitive thread.
   * 
   * @param abort true if server is aborting (only during testing)
   * @param listener call back to alert caller on close status
   * @return Vector of all the storage files that the HRegion's component 
   * HStores make use of.  It's a list of HStoreFile objects.  Can be null if
   * we are not to close at this time or we are already closed.
   * 
   * @throws IOException
   */
  List<HStoreFile> close(boolean abort,
      final RegionUnavailableListener listener) throws IOException {
    Text regionName = this.regionInfo.getRegionName(); 
    if (isClosed()) {
      LOG.info("region " + regionName + " already closed");
      return null;
    }
    synchronized (splitLock) {
      synchronized (writestate) {
        // Disable compacting and flushing by background threads for this
        // region.
        writestate.writesEnabled = false;
        LOG.debug("compactions and cache flushes disabled for region " +
          regionName);
        while (writestate.compacting || writestate.flushing) {
          LOG.debug("waiting for" +
              (writestate.compacting ? " compaction" : "") +
              (writestate.flushing ?
                  (writestate.compacting ? "," : "") + " cache flush" :
                    ""
              ) + " to complete for region " + regionName
          );
          try {
            writestate.wait();
          } catch (InterruptedException iex) {
            // continue
          }
        }
      }
      lock.writeLock().lock();
      LOG.debug("new updates and scanners for region " + regionName +
          " disabled");
      
      try {
        // Wait for active scanners to finish. The write lock we hold will prevent
        // new scanners from being created.
        synchronized (activeScannerCount) {
          while (activeScannerCount.get() != 0) {
            LOG.debug("waiting for " + activeScannerCount.get() +
                " scanners to finish");
            try {
              activeScannerCount.wait();

            } catch (InterruptedException e) {
              // continue
            }
          }
        }
        LOG.debug("no more active scanners for region " + regionName);

        // Write lock means no more row locks can be given out.  Wait on
        // outstanding row locks to come in before we close so we do not drop
        // outstanding updates.
        waitOnRowLocks();
        LOG.debug("no more row locks outstanding on region " + regionName);
        
        if (listener != null) {
          // If there is a listener, let them know that we have now
          // acquired all the necessary locks and are starting to
          // do the close
          listener.closing(getRegionName());
        }
        
        // Don't flush the cache if we are aborting
        if (!abort) {
          internalFlushcache(snapshotMemcaches());
        }

        List<HStoreFile> result = new ArrayList<HStoreFile>();
        for (HStore store: stores.values()) {
          result.addAll(store.close());
        }
        this.closed.set(true);
        
        if (listener != null) {
          // If there is a listener, tell them that the region is now 
          // closed.
          listener.closed(getRegionName());
        }
        
        LOG.info("closed " + this.regionInfo.getRegionName());
        return result;
      } finally {
        lock.writeLock().unlock();
      }
    }
  }
  
  //////////////////////////////////////////////////////////////////////////////
  // HRegion accessors
  //////////////////////////////////////////////////////////////////////////////

  /** @return start key for region */
  public Text getStartKey() {
    return this.regionInfo.getStartKey();
  }

  /** @return end key for region */
  public Text getEndKey() {
    return this.regionInfo.getEndKey();
  }

  /** @return region id */
  long getRegionId() {
    return this.regionInfo.getRegionId();
  }

  /** @return region name */
  public Text getRegionName() {
    return this.regionInfo.getRegionName();
  }

  /** @return HTableDescriptor for this region */
  HTableDescriptor getTableDesc() {
    return this.regionInfo.getTableDesc();
  }

  /** @return HLog in use for this region */
  public HLog getLog() {
    return this.log;
  }

  /** @return Configuration object */
  HBaseConfiguration getConf() {
    return this.conf;
  }

  /** @return region directory Path */
  Path getRegionDir() {
    return this.regiondir;
  }

  /** @return FileSystem being used by this region */
  FileSystem getFilesystem() {
    return this.fs;
  }

  /** @return the last time the region was flushed */
  public long getLastFlushTime() {
    return this.lastFlushTime;
  }
  
  //////////////////////////////////////////////////////////////////////////////
  // HRegion maintenance.  
  //
  // These methods are meant to be called periodically by the HRegionServer for 
  // upkeep.
  //////////////////////////////////////////////////////////////////////////////

  /**
   * @return returns size of largest HStore.  Also returns whether store is
   * splitable or not (Its not splitable if region has a store that has a
   * reference store file).
   */
  HStore.HStoreSize largestHStore(Text midkey) {
    HStore.HStoreSize biggest = null;
    boolean splitable = true;
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
  }
  
  /*
   * Split the HRegion to create two brand-new ones.  This also closes
   * current HRegion.  Split should be fast since we don't rewrite store files
   * but instead create new 'reference' store files that read off the top and
   * bottom ranges of parent store files.
   * @param listener May be null.
   * @return two brand-new (and open) HRegions or null if a split is not needed
   * @throws IOException
   */
  HRegion[] splitRegion(final RegionUnavailableListener listener)
    throws IOException {
    synchronized (splitLock) {
      Text midKey = new Text();
      if (closed.get() || !needsSplit(midKey)) {
        return null;
      }
      Path splits = new Path(this.regiondir, SPLITDIR);
      if(!this.fs.exists(splits)) {
        this.fs.mkdirs(splits);
      }
      HRegionInfo regionAInfo = new HRegionInfo(this.regionInfo.getTableDesc(),
          this.regionInfo.getStartKey(), midKey);
      Path dirA = new Path(splits, regionAInfo.getEncodedName());
      if(fs.exists(dirA)) {
        throw new IOException("Cannot split; target file collision at " + dirA);
      }
      HRegionInfo regionBInfo = new HRegionInfo(this.regionInfo.getTableDesc(),
          midKey, this.regionInfo.getEndKey());
      Path dirB = new Path(splits, regionBInfo.getEncodedName());
      if(this.fs.exists(dirB)) {
        throw new IOException("Cannot split; target file collision at " + dirB);
      }

      // Now close the HRegion.  Close returns all store files or null if not
      // supposed to close (? What to do in this case? Implement abort of close?)
      // Close also does wait on outstanding rows and calls a flush just-in-case.
      List<HStoreFile> hstoreFilesToSplit = close(false, listener);
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
            this.regionInfo.getEncodedName(), h.getFileId(),
            new HStoreKey(midKey), HStoreFile.Range.bottom);
        HStoreFile a = new HStoreFile(this.conf, fs, splits,
            regionAInfo.getEncodedName(), h.getColFamily(), -1, aReference);
        // Reference to top half of the hsf store file.
        HStoreFile.Reference bReference = new HStoreFile.Reference(
            this.regionInfo.getEncodedName(), h.getFileId(),
            new HStoreKey(midKey), HStoreFile.Range.top);
        HStoreFile b = new HStoreFile(this.conf, fs, splits,
            regionBInfo.getEncodedName(), h.getColFamily(), -1, bReference);
        h.splitStoreFile(a, b, this.fs);
      }

      // Done!
      // Opening the region copies the splits files from the splits directory
      // under each region.
      HRegion regionA =
        new HRegion(basedir, log, fs, conf, regionAInfo, dirA, null);
      regionA.close();
      HRegion regionB =
        new HRegion(basedir, log, fs, conf, regionBInfo, dirB, null);
      regionB.close();

      // Cleanup
      boolean deleted = fs.delete(splits);    // Get rid of splits directory
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cleaned up " + splits.toString() + " " + deleted);
      }
      HRegion regions[] = new HRegion [] {regionA, regionB};
      return regions;
    }
  }
  
  /*
   * Iterates through all the HStores and finds the one with the largest
   * MapFile size. If the size is greater than the (currently hard-coded)
   * threshold, returns true indicating that the region should be split. The
   * midKey for the largest MapFile is returned through the midKey parameter.
   * It is possible for us to rule the region non-splitable even in excess of
   * configured size.  This happens if region contains a reference file.  If
   * a reference file, the region can not be split.
   * 
   * Note that there is no need to do locking in this method because it calls
   * largestHStore which does the necessary locking.
   * 
   * @param midKey midKey of the largest MapFile
   * @return true if the region should be split. midKey is set by this method.
   * Check it for a midKey value on return.
   */
  boolean needsSplit(Text midKey) {
    HStore.HStoreSize biggest = largestHStore(midKey);
    if (biggest == null || midKey.getLength() == 0 || 
      (midKey.equals(getStartKey()) && midKey.equals(getEndKey())) ) {
      return false;
    }
    boolean split = (biggest.getAggregate() >= this.desiredMaxFileSize);
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
  }
  
  /**
   * Only do a compaction if it is necessary
   * 
   * @return
   * @throws IOException
   */
  boolean compactIfNeeded() throws IOException {
    boolean needsCompaction = false;
    for (HStore store: stores.values()) {
      if (store.needsCompaction()) {
        needsCompaction = true;
        if (LOG.isDebugEnabled()) {
          LOG.debug(store.toString() + " needs compaction");
        }
        break;
      }
    }
    if (!needsCompaction) {
      return false;
    }
    return compactStores();
  }
  
  /*
   * @param dir
   * @return compaction directory for the passed in <code>dir</code>
   */
  static Path getCompactionDir(final Path dir) {
   return new Path(dir, "compaction.dir");
  }

  /*
   * Do preparation for pending compaction.
   * Clean out any vestiges of previous failed compactions.
   * @throws IOException
   */
  private void doRegionCompactionPrep() throws IOException {
    doRegionCompactionCleanup();
  }
  
  /*
   * Removes the compaction directory for this Store.
   * @throws IOException
   */
  private void doRegionCompactionCleanup() throws IOException {
    if (this.fs.exists(this.regionCompactionDir)) {
      this.fs.delete(this.regionCompactionDir);
    }
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
   * 
   * Note that no locking is necessary at this level because compaction only
   * conflicts with a region split, and that cannot happen because the region
   * server does them sequentially and not in parallel.
   */
  boolean compactStores() throws IOException {
    if (this.closed.get()) {
      return false;
    }
    try {
      synchronized (writestate) {
        if (!writestate.compacting && writestate.writesEnabled) {
          writestate.compacting = true;
        } else {
          LOG.info("NOT compacting region " +
              this.regionInfo.getRegionName().toString() + ": compacting=" +
              writestate.compacting + ", writesEnabled=" +
              writestate.writesEnabled);
            return false;
        }
      }
      long startTime = System.currentTimeMillis();
      LOG.info("starting compaction on region " +
        this.regionInfo.getRegionName().toString());
      boolean status = true;
      doRegionCompactionPrep();
      for (HStore store : stores.values()) {
        if(!store.compact()) {
          status = false;
        }
      }
      doRegionCompactionCleanup();
      LOG.info("compaction completed on region " +
        this.regionInfo.getRegionName().toString() + ". Took " +
        StringUtils.formatTimeDiff(System.currentTimeMillis(), startTime));
      return status;
      
    } finally {
      synchronized (writestate) {
        writestate.compacting = false;
        writestate.notifyAll();
      }
    }
  }

  /**
   * Flush the cache.
   * 
   * When this method is called the cache will be flushed unless:
   * <ol>
   *   <li>the cache is empty</li>
   *   <li>the region is closed.</li>
   *   <li>a flush is already in progress</li>
   *   <li>writes are disabled</li>
   * </ol>
   *
   * <p>This method may block for some time, so it should not be called from a 
   * time-sensitive thread.
   * 
   * @return true if cache was flushed
   * 
   * @throws IOException
   * @throws DroppedSnapshotException Thrown when replay of hlog is required
   * because a Snapshot was not properly persisted.
   */
  boolean flushcache() throws IOException {
    if (this.closed.get()) {
      return false;
    }
    synchronized (writestate) {
      if (!writestate.flushing && writestate.writesEnabled) {
        writestate.flushing = true;
      } else {
        if(LOG.isDebugEnabled()) {
          LOG.debug("NOT flushing memcache for region " +
              this.regionInfo.getRegionName() + ", flushing=" +
              writestate.flushing + ", writesEnabled=" +
              writestate.writesEnabled);
        }
        return false;  
      }
    }
    try {
      lock.readLock().lock();                      // Prevent splits and closes
      try {
        long startTime = -1;
        synchronized (updateLock) {// Stop updates while we snapshot the memcaches
          startTime = snapshotMemcaches();
        }
        return internalFlushcache(startTime);
      } finally {
        lock.readLock().unlock();
      }
    } finally {
      synchronized (writestate) {
        writestate.flushing = false;
        writestate.notifyAll();
      }
    }
  }

  /*
   * It is assumed that updates are blocked for the duration of this method
   */
  private long snapshotMemcaches() {
    if (this.memcacheSize.get() == 0) {
      return -1;
    }
    long startTime = System.currentTimeMillis();
    
    if(LOG.isDebugEnabled()) {
      LOG.debug("Started memcache flush for region " +
        this.regionInfo.getRegionName() + ". Size " +
        StringUtils.humanReadableInt(this.memcacheSize.get()));
    }

    // We reset the aggregate memcache size here so that subsequent updates
    // will add to the unflushed size
    
    this.memcacheSize.set(0L);
    
    for (HStore hstore: stores.values()) {
      hstore.snapshotMemcache();
    }
    return startTime;
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
   * 
   * @param startTime the time the cache was snapshotted or -1 if a flush is
   * not needed
   * 
   * @return true if the cache was flushed
   * 
   * @throws IOException
   * @throws DroppedSnapshotException Thrown when replay of hlog is required
   * because a Snapshot was not properly persisted.
   */
  private boolean internalFlushcache(long startTime) throws IOException {
    if (startTime == -1) {
      return false;
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
    long sequenceId = log.startCacheFlush();

    // Any failure from here on out will be catastrophic requiring server
    // restart so hlog content can be replayed and put back into the memcache.
    // Otherwise, the snapshot content while backed up in the hlog, it will not
    // be part of the current running servers state.

    try {
      // A.  Flush memcache to all the HStores.
      // Keep running vector of all store files that includes both old and the
      // just-made new flush store file.
      
      for (HStore hstore: stores.values()) {
        hstore.flushCache(sequenceId);
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
    this.log.completeCacheFlush(this.regionInfo.getRegionName(),
        regionInfo.getTableDesc().getName(), sequenceId);

    // D. Finally notify anyone waiting on memcache to clear:
    // e.g. checkResources().
    synchronized (this) {
      notifyAll();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Finished memcache flush for region " +
          this.regionInfo.getRegionName() + " in " +
          (System.currentTimeMillis() - startTime) + "ms, sequenceid=" +
          sequenceId);
    }
    return true;
  }
  
  //////////////////////////////////////////////////////////////////////////////
  // get() methods for client use.
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Fetch a single data item.
   * @param row
   * @param column
   * @return column value
   * @throws IOException
   */
  public byte [] get(Text row, Text column) throws IOException {
    byte [][] results = get(row, column, Long.MAX_VALUE, 1);
    return (results == null || results.length == 0)? null: results[0];
  }
  
  /**
   * Fetch multiple versions of a single data item
   * 
   * @param row
   * @param column
   * @param numVersions
   * @return array of values one element per version
   * @throws IOException
   */
  public byte [][] get(Text row, Text column, int numVersions) throws IOException {
    return get(row, column, Long.MAX_VALUE, numVersions);
  }

  /**
   * Fetch multiple versions of a single data item, with timestamp.
   *
   * @param row
   * @param column
   * @param timestamp
   * @param numVersions
   * @return array of values one element per version that matches the timestamp
   * @throws IOException
   */
  public byte [][] get(Text row, Text column, long timestamp, int numVersions) 
    throws IOException {
    
    if (this.closed.get()) {
      throw new IOException("Region " + this.getRegionName().toString() +
        " closed");
    }

    // Make sure this is a valid row and valid column
    checkRow(row);
    checkColumn(column);

    // Don't need a row lock for a simple get
    
    HStoreKey key = new HStoreKey(row, column, timestamp);
    HStore targetStore = stores.get(HStoreKey.extractFamily(column));
    return targetStore.get(key, numVersions);
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
   *
   * @param row
   * @return Map<columnName, byte[]> values
   * @throws IOException
   */
  public Map<Text, byte []> getFull(Text row) throws IOException {
    return getFull(row, HConstants.LATEST_TIMESTAMP);
  }

  /**
   * Fetch all the columns for the indicated row at a specified timestamp.
   * Returns a TreeMap that maps column names to values.
   *
   * We should eventually use Bloom filters here, to reduce running time.  If 
   * the database has many column families and is very sparse, then we could be 
   * checking many files needlessly.  A small Bloom for each row would help us 
   * determine which column groups are useful for that row.  That would let us 
   * avoid a bunch of disk activity.
   *
   * @param row
   * @param ts
   * @return Map<columnName, byte[]> values
   * @throws IOException
   */
  public Map<Text, byte []> getFull(Text row, long ts) throws IOException {
    HStoreKey key = new HStoreKey(row, ts);
    obtainRowLock(row);
    try {
      TreeMap<Text, byte []> result = new TreeMap<Text, byte[]>();
      for (Text colFamily: stores.keySet()) {
        HStore targetStore = stores.get(colFamily);
        targetStore.getFull(key, result);
      }
      return result;
    } finally {
      releaseRowLock(row);
    }
  }

  /**
   * Return all the data for the row that matches <i>row</i> exactly, 
   * or the one that immediately preceeds it, at or immediately before 
   * <i>ts</i>.
   * 
   * @param row row key
   * @param ts
   * @return map of values
   * @throws IOException
   */
  public Map<Text, byte[]> getClosestRowBefore(final Text row, final long ts)
  throws IOException{
    // look across all the HStores for this region and determine what the
    // closest key is across all column families, since the data may be sparse
    
    HStoreKey key = null;
    checkRow(row);
    lock.readLock().lock();
    try {
      // examine each column family for the preceeding or matching key
      for(Text colFamily : stores.keySet()){
        HStore store = stores.get(colFamily);

        // get the closest key
        Text closestKey = store.getRowKeyAtOrBefore(row, ts);
        
        // if it happens to be an exact match, we can stop looping
        if (row.equals(closestKey)) {
          key = new HStoreKey(closestKey, ts);
          break;
        }

        // otherwise, we need to check if it's the max and move to the next
        if (closestKey != null 
          && (key == null || closestKey.compareTo(key.getRow()) > 0) ) {
          key = new HStoreKey(closestKey, ts);
        }
      }

      if (key == null) {
        return null;
      }
          
      // now that we've found our key, get the values
      TreeMap<Text, byte []> result = new TreeMap<Text, byte[]>();
      for (Text colFamily: stores.keySet()) {
        HStore targetStore = stores.get(colFamily);
        targetStore.getFull(key, result);
      }
      
      return result;
    } finally {
      lock.readLock().unlock();
    }
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
  private List<HStoreKey> getKeys(final HStoreKey origin, final int versions)
    throws IOException {

    List<HStoreKey> keys = null;
    Text colFamily = HStoreKey.extractFamily(origin.getColumn());
    HStore targetStore = stores.get(colFamily);
    if (targetStore != null) {
      // Pass versions without modification since in the store getKeys, it
      // includes the size of the passed <code>keys</code> array when counting.
      keys = targetStore.getKeys(origin, versions);
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
  public HScannerInterface getScanner(Text[] cols, Text firstRow,
      long timestamp, RowFilterInterface filter) throws IOException {
    lock.readLock().lock();
    try {
      if (this.closed.get()) {
        throw new IOException("Region " + this.getRegionName().toString() +
          " closed");
      }
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
      return new HScanner(cols, firstRow, timestamp,
        storelist.toArray(new HStore [storelist.size()]), filter);
    } finally {
      lock.readLock().unlock();
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // set() methods for client use.
  //////////////////////////////////////////////////////////////////////////////
  
  /**
   * @param timestamp
   * @param b
   * @throws IOException
   */
  public void batchUpdate(long timestamp, BatchUpdate b)
    throws IOException {
    // Do a rough check that we have resources to accept a write.  The check is
    // 'rough' in that between the resource check and the call to obtain a 
    // read lock, resources may run out.  For now, the thought is that this
    // will be extremely rare; we'll deal with it when it happens.
    checkResources();

    // We obtain a per-row lock, so other clients will block while one client
    // performs an update. The read lock is released by the client calling
    // #commit or #abort or if the HRegionServer lease on the lock expires.
    // See HRegionServer#RegionListener for how the expire on HRegionServer
    // invokes a HRegion#abort.
    Text row = b.getRow();
    long lockid = obtainRowLock(row);

    long commitTime =
      (timestamp == LATEST_TIMESTAMP) ? System.currentTimeMillis() : timestamp;
      
    try {
      List<Text> deletes = null;
      for (BatchOperation op: b) {
        HStoreKey key = new HStoreKey(row, op.getColumn(), commitTime);
        byte[] val = null;
        if (op.isPut()) {
          val = op.getValue();
          if (HLogEdit.isDeleted(val)) {
            throw new IOException("Cannot insert value: " + val);
          }
        } else {
          if (timestamp == LATEST_TIMESTAMP) {
            // Save off these deletes
            if (deletes == null) {
              deletes = new ArrayList<Text>();
            }
            deletes.add(op.getColumn());
          } else {
            val = HLogEdit.deleteBytes.get();
          }
        }
        if (val != null) {
          localput(lockid, key, val);
        }
      }
      TreeMap<HStoreKey, byte[]> edits =
        this.targetColumns.remove(Long.valueOf(lockid));
      if (edits != null && edits.size() > 0) {
        update(edits);
      }
      
      if (deletes != null && deletes.size() > 0) {
        // We have some LATEST_TIMESTAMP deletes to run.
        for (Text column: deletes) {
          deleteMultiple(row, column, LATEST_TIMESTAMP, 1);
        }
      }

    } catch (IOException e) {
      this.targetColumns.remove(Long.valueOf(lockid));
      throw e;
      
    } finally {
      releaseRowLock(row);
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
    
    while (this.memcacheSize.get() >= this.blockingMemcacheSize) {
      if (!blocked) {
        LOG.info("Blocking updates for '" + Thread.currentThread().getName() +
            "': Memcache size " +
            StringUtils.humanReadableInt(this.memcacheSize.get()) +
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
  
  /**
   * Delete all cells of the same age as the passed timestamp or older.
   * @param row
   * @param column
   * @param ts Delete all entries that have this timestamp or older
   * @throws IOException
   */
  public void deleteAll(final Text row, final Text column, final long ts)
    throws IOException {
    
    checkColumn(column);
    obtainRowLock(row);
    try {
      deleteMultiple(row, column, ts, ALL_VERSIONS);
    } finally {
      releaseRowLock(row);
    }
  }

  /**
   * Delete all cells of the same age as the passed timestamp or older.
   * @param row
   * @param ts Delete all entries that have this timestamp or older
   * @throws IOException
   */
  public void deleteAll(final Text row, final long ts)
    throws IOException {
    
    obtainRowLock(row);    
    
    try {
      for(Map.Entry<Text, HStore> store : stores.entrySet()){
        List<HStoreKey> keys = store.getValue().getKeys(new HStoreKey(row, ts), ALL_VERSIONS);

        TreeMap<HStoreKey, byte []> edits = new TreeMap<HStoreKey, byte []>();
        for (HStoreKey key: keys) {
          edits.put(key, HLogEdit.deleteBytes.get());
        }
        update(edits);
      }
    } finally {
      releaseRowLock(row);
    }
  }

  /**
   * Delete all cells for a row with matching column family with timestamps
   * less than or equal to <i>timestamp</i>.
   *
   * @param row The row to operate on
   * @param family The column family to match
   * @param timestamp Timestamp to match
   * @throws IOException
   */
  public void deleteFamily(Text row, Text family, long timestamp)
  throws IOException{
    obtainRowLock(row);    
    
    try {
      // find the HStore for the column family
      HStore store = stores.get(HStoreKey.extractFamily(family));
      // find all the keys that match our criteria
      List<HStoreKey> keys = store.getKeys(new HStoreKey(row, timestamp), ALL_VERSIONS);
      
      // delete all the cells
      TreeMap<HStoreKey, byte []> edits = new TreeMap<HStoreKey, byte []>();
      for (HStoreKey key: keys) {
        edits.put(key, HLogEdit.deleteBytes.get());
      }
      update(edits);
    } finally {
      releaseRowLock(row);
    }
  }
  
  /**
   * Delete one or many cells.
   * Used to support {@link #deleteAll(Text, Text, long)} and deletion of
   * latest cell.
   * 
   * @param row
   * @param column
   * @param ts Timestamp to start search on.
   * @param versions How many versions to delete. Pass
   * {@link HConstants#ALL_VERSIONS} to delete all.
   * @throws IOException
   */
  private void deleteMultiple(final Text row, final Text column, final long ts,
      final int versions) throws IOException {
    
    HStoreKey origin = new HStoreKey(row, column, ts);
    List<HStoreKey> keys = getKeys(origin, versions);
    if (keys.size() > 0) {
      TreeMap<HStoreKey, byte []> edits = new TreeMap<HStoreKey, byte []>();
      for (HStoreKey key: keys) {
        edits.put(key, HLogEdit.deleteBytes.get());
      }
      update(edits);
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
   * @param key 
   * @param val Value to enter into cell
   * @throws IOException
   */
  private void localput(final long lockid, final HStoreKey key, 
      final byte [] val) throws IOException {
    
    checkColumn(key.getColumn());
    Long lid = Long.valueOf(lockid);
    TreeMap<HStoreKey, byte []> targets = this.targetColumns.get(lid);
    if (targets == null) {
      targets = new TreeMap<HStoreKey, byte []>();
      this.targetColumns.put(lid, targets);
    }
    targets.put(key, val);
  }

  /* 
   * Add updates first to the hlog and then add values to memcache.
   * Warning: Assumption is caller has lock on passed in row.
   * @param row Row to update.
   * @param timestamp Timestamp to record the updates against
   * @param updatesByColumn Cell updates by column
   * @throws IOException
   */
  private void update(final TreeMap<HStoreKey, byte []> updatesByColumn)
    throws IOException {
    
    if (updatesByColumn == null || updatesByColumn.size() <= 0) {
      return;
    }
    synchronized (updateLock) {                         // prevent a cache flush
      this.log.append(regionInfo.getRegionName(),
          regionInfo.getTableDesc().getName(), updatesByColumn);

      long size = 0;
      for (Map.Entry<HStoreKey, byte[]> e: updatesByColumn.entrySet()) {
        HStoreKey key = e.getKey();
        byte[] val = e.getValue();
        size = this.memcacheSize.addAndGet(key.getSize() +
            (val == null ? 0 : val.length));
        stores.get(HStoreKey.extractFamily(key.getColumn())).add(key, val);
      }
      if (this.flushListener != null && size > this.memcacheFlushSize) {
        // Request a cache flush
        this.flushListener.flushRequested(this);
      }
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // Support code
  //////////////////////////////////////////////////////////////////////////////

  /** Make sure this is a valid row for the HRegion */
  private void checkRow(Text row) throws IOException {
    if(((regionInfo.getStartKey().getLength() == 0)
        || (regionInfo.getStartKey().compareTo(row) <= 0))
        && ((regionInfo.getEndKey().getLength() == 0)
            || (regionInfo.getEndKey().compareTo(row) > 0))) {
      // all's well
      
    } else {
      throw new WrongRegionException("Requested row out of range for " +
        "HRegion " + regionInfo.getRegionName() + ", startKey='" +
        regionInfo.getStartKey() + "', getEndKey()='" + regionInfo.getEndKey() +
        "', row='" + row + "'");
    }
  }
  
  /**
   * Make sure this is a valid column for the current table
   * @param columnName
   * @throws IOException
   */
  private void checkColumn(Text columnName) throws IOException {
    Text family = HStoreKey.extractFamily(columnName, true);
    if (!regionInfo.getTableDesc().hasFamily(family)) {
      throw new IOException("Requested column family " + family 
          + " does not exist in HRegion " + regionInfo.getRegionName()
          + " for table " + regionInfo.getTableDesc().getName());
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
   * @throws IOException
   * @return The id of the held lock.
   */
  long obtainRowLock(Text row) throws IOException {
    checkRow(row);
    lock.readLock().lock();
    try {
      if (this.closed.get()) {
        throw new IOException("Region " + this.getRegionName().toString() +
          " closed");
      }
      synchronized (rowsToLocks) {
        while (rowsToLocks.get(row) != null) {
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
    } finally {
      lock.readLock().unlock();
    }
  }
  
  Text getRowFromLock(long lockid) {
    // Pattern is that all access to rowsToLocks and/or to
    // locksToRows is via a lock on rowsToLocks.
    synchronized (rowsToLocks) {
      return locksToRows.get(Long.valueOf(lockid));
    }
  }
  
  /** 
   * Release the row lock!
   * @param row Name of row whose lock we are to release
   */
  void releaseRowLock(Text row) {
    synchronized (rowsToLocks) {
      long lockid = rowsToLocks.remove(row).longValue();
      locksToRows.remove(Long.valueOf(lockid));
      rowsToLocks.notifyAll();
    }
  }
  
  private void waitOnRowLocks() {
    synchronized (rowsToLocks) {
      while (this.rowsToLocks.size() > 0) {
        LOG.debug("waiting for " + this.rowsToLocks.size() + " row locks");
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
    return regionInfo.getRegionName().toString();
  }
  
  private Path getBaseDir() {
    return this.basedir;
  }

  /**
   * HScanner is an iterator through a bunch of rows in an HRegion.
   */
  private class HScanner implements HScannerInterface {
    private HInternalScannerInterface[] scanners;
    private TreeMap<Text, byte []>[] resultSets;
    private HStoreKey[] keys;

    /** Create an HScanner with a handle on many HStores. */
    @SuppressWarnings("unchecked")
    HScanner(Text[] cols, Text firstRow, long timestamp, HStore[] stores,
        RowFilterInterface filter)
    throws IOException {
      this.scanners = new HInternalScannerInterface[stores.length];
      try {
        for (int i = 0; i < stores.length; i++) {
          // TODO: The cols passed in here can include columns from other
          // stores; add filter so only pertinent columns are passed.
          //
          // Also, if more than one store involved, need to replicate filters.
          // At least WhileMatchRowFilter will mess up the scan if only
          // one shared across many rows. See HADOOP-2467.
          scanners[i] = stores[i].getScanner(timestamp, cols, firstRow,
            (i > 0 && filter != null)?
              (RowFilterInterface)WritableUtils.clone(filter, conf): filter);
        }
      } catch(IOException e) {
        for (int i = 0; i < this.scanners.length; i++) {
          if(scanners[i] != null) {
            closeScanner(i);
          }
        }
        throw e;
      }

      // Advance to the first key in each store.
      // All results will match the required column-set and scanTime.
      this.resultSets = new TreeMap[scanners.length];
      this.keys = new HStoreKey[scanners.length];
      for (int i = 0; i < scanners.length; i++) {
        keys[i] = new HStoreKey();
        resultSets[i] = new TreeMap<Text, byte []>();
        if(scanners[i] != null && !scanners[i].next(keys[i], resultSets[i])) {
          closeScanner(i);
        }
      }

      // As we have now successfully completed initialization, increment the
      // activeScanner count.
      activeScannerCount.incrementAndGet();
    }

    /** {@inheritDoc} */
    public boolean next(HStoreKey key, SortedMap<Text, byte[]> results)
    throws IOException {
      boolean moreToFollow = false;

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

      // Store the key and results for each sub-scanner. Merge them as
      // appropriate.
      if (chosenTimestamp >= 0) {
        // Here we are setting the passed in key with current row+timestamp
        key.setRow(chosenRow);
        key.setVersion(chosenTimestamp);
        key.setColumn(HConstants.EMPTY_TEXT);

        for (int i = 0; i < scanners.length; i++) {
          if (scanners[i] != null && keys[i].getRow().compareTo(chosenRow) == 0) {
            // NOTE: We used to do results.putAll(resultSets[i]);
            // but this had the effect of overwriting newer
            // values with older ones. So now we only insert
            // a result if the map does not contain the key.
            for (Map.Entry<Text, byte[]> e : resultSets[i].entrySet()) {
              if (!results.containsKey(e.getKey())) {
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
      if (results == null || results.size() <= 0) {
        // If we got no results, then there is no more to follow.
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
        resultSets[i] = null;
        keys[i] = null;
      }
    }

    /**
     * {@inheritDoc}
     */
    public void close() {
      try {
        for(int i = 0; i < scanners.length; i++) {
          if(scanners[i] != null) {
            closeScanner(i);
          }
        }
      } finally {
        synchronized (activeScannerCount) {
          int count = activeScannerCount.decrementAndGet();
          if (count < 0) {
            LOG.error("active scanner count less than zero: " + count +
                " resetting to zero");
            activeScannerCount.set(0);
            count = 0;
          }
          if (count == 0) {
            activeScannerCount.notifyAll();
          }
        }
      }
    }

    /** {@inheritDoc} */
    public Iterator<Entry<HStoreKey, SortedMap<Text, byte[]>>> iterator() {
      throw new UnsupportedOperationException("Unimplemented serverside. " +
        "next(HStoreKey, StortedMap(...) is more efficient");
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
   * @return new HRegion
   * 
   * @throws IOException
   */
  static HRegion createHRegion(final HRegionInfo info, final Path rootDir,
      final HBaseConfiguration conf) throws IOException {
    Path tableDir =
      HTableDescriptor.getTableDir(rootDir, info.getTableDesc().getName());
    Path regionDir = HRegion.getRegionDir(tableDir, info.getEncodedName());
    FileSystem fs = FileSystem.get(conf);
    fs.mkdirs(regionDir);
    return new HRegion(tableDir,
      new HLog(fs, new Path(regionDir, HREGION_LOGDIR_NAME), conf, null),
      fs, conf, info, null, null);
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
   * @see {@link #removeRegionFromMETA(HRegion, HRegion)}
   */
  static void addRegionToMETA(HRegion meta, HRegion r) throws IOException {
    meta.checkResources();
    // The row key is the region name
    Text row = r.getRegionName();
    meta.obtainRowLock(row);
    try {
      HStoreKey key =
        new HStoreKey(row, COL_REGIONINFO, System.currentTimeMillis());
      TreeMap<HStoreKey, byte[]> edits = new TreeMap<HStoreKey, byte[]>();
      edits.put(key, Writables.getBytes(r.getRegionInfo()));
      meta.update(edits);
    
    } finally {
      meta.releaseRowLock(row);
    }
  }

  /**
   * Delete a region's meta information from the passed
   * <code>meta</code> region.
   * 
   * @param srvr META server to be updated
   * @param metaRegionName Meta region name
   * @param regionNmae HRegion to remove from <code>meta</code>
   *
   * @throws IOException
   * @see {@link #addRegionToMETA(HRegion, HRegion)}
   */
  static void removeRegionFromMETA(final HRegionInterface srvr,
      final Text metaRegionName, final Text regionName)
  throws IOException {
    BatchUpdate b = new BatchUpdate(rand.nextLong());
    long lockid = b.startUpdate(regionName);
    for (int i = 0; i < ALL_META_COLUMNS.length; i++) {
      b.delete(lockid, ALL_META_COLUMNS[i]);
    }
    srvr.batchUpdate(metaRegionName, System.currentTimeMillis(), b);
  }

  /**
   * Utility method used by HMaster marking regions offlined.
   * @param srvr META server to be updated
   * @param metaRegionName Meta region name
   * @param info HRegion to update in <code>meta</code>
   *
   * @throws IOException
   * @see {@link #addRegionToMETA(HRegion, HRegion)}
   */
  static void offlineRegionInMETA(final HRegionInterface srvr,
      final Text metaRegionName, final HRegionInfo info)
  throws IOException {
    BatchUpdate b = new BatchUpdate(rand.nextLong());
    long lockid = b.startUpdate(info.getRegionName());
    info.setOffline(true);
    b.put(lockid, COL_REGIONINFO, Writables.getBytes(info));
    b.delete(lockid, COL_SERVER);
    b.delete(lockid, COL_STARTCODE);
    // If carrying splits, they'll be in place when we show up on new
    // server.
    srvr.batchUpdate(metaRegionName, System.currentTimeMillis(), b);
  }

  /**
   * Deletes all the files for a HRegion
   * 
   * @param fs the file system object
   * @param rootdir qualified path of HBase root directory
   * @param info HRegionInfo for region to be deleted
   * @throws IOException
   * @return True if deleted.
   */
  static boolean deleteRegion(FileSystem fs, Path rootdir, HRegionInfo info)
  throws IOException {
    Path p = HRegion.getRegionDir(rootdir, info);
    if (LOG.isDebugEnabled()) {
      LOG.debug("DELETING region " + p.toString());
    }
    return fs.delete(p);
  }

  /**
   * Computes the Path of the HRegion
   * 
   * @param tabledir qualified path for table
   * @param name region file name ENCODED!
   * @return Path of HRegion directory
   * @see HRegionInfo#encodeRegionName(Text)
   */
  static Path getRegionDir(final Path tabledir, final String name) {
    return new Path(tabledir, name);
  }
  
  /**
   * Computes the Path of the HRegion
   * 
   * @param rootdir qualified path of HBase root directory
   * @param info HRegionInfo for the region
   * @return qualified path of region directory
   */
  public static Path getRegionDir(final Path rootdir, final HRegionInfo info) {
    return new Path(
        HTableDescriptor.getTableDir(rootdir, info.getTableDesc().getName()),
        info.getEncodedName()
    );
  }
}
