 /**
 * Copyright 2008 The Apache Software Foundation
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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.DroppedSnapshotException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionHistorian;
import org.apache.hadoop.hbase.ValueOverMaxLengthException;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.io.BatchOperation;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.HbaseMapWritable;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.io.Reference.Range;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.Progressable;
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
  static final Log LOG = LogFactory.getLog(HRegion.class);
  static final String SPLITDIR = "splits";
  static final String MERGEDIR = "merges";
  final AtomicBoolean closed = new AtomicBoolean(false);
  /* Closing can take some time; use the closing flag if there is stuff we don't want
   * to do while in closing state; e.g. like offer this region up to the master as a region
   * to close if the carrying regionserver is overloaded.  Once set, it is never cleared.
   */
  private final AtomicBoolean closing = new AtomicBoolean(false);
  private final RegionHistorian historian;

  //////////////////////////////////////////////////////////////////////////////
  // Members
  //////////////////////////////////////////////////////////////////////////////

  private final Map<Integer, byte []> locksToRows =
    new ConcurrentHashMap<Integer, byte []>();
  protected final Map<byte [], Store> stores =
    new ConcurrentSkipListMap<byte [], Store>(KeyValue.FAMILY_COMPARATOR);
  final AtomicLong memcacheSize = new AtomicLong(0);

  // This is the table subdirectory.
  final Path basedir;
  final HLog log;
  final FileSystem fs;
  final HBaseConfiguration conf;
  final HRegionInfo regionInfo;
  final Path regiondir;
  private final Path regionCompactionDir;
  KeyValue.KVComparator comparator;
  private KeyValue.KVComparator comparatorIgnoreTimestamp;

  /*
   * Set this when scheduling compaction if want the next compaction to be a
   * major compaction.  Cleared each time through compaction code.
   */
  private volatile boolean forceMajorCompaction = false;

  /*
   * Data structure of write state flags used coordinating flushes,
   * compactions and closes.
   */
  static class WriteState {
    // Set while a memcache flush is happening.
    volatile boolean flushing = false;
    // Set when a flush has been requested.
    volatile boolean flushRequested = false;
    // Set while a compaction is running.
    volatile boolean compacting = false;
    // Gets set in close. If set, cannot compact or flush again.
    volatile boolean writesEnabled = true;
    // Set if region is read-only
    volatile boolean readOnly = false;
    
    /**
     * Set flags that make this region read-only.
     */
    synchronized void setReadOnly(final boolean onOff) {
      this.writesEnabled = !onOff;
      this.readOnly = onOff;
    }
    
    boolean isReadOnly() {
      return this.readOnly;
    }

    boolean isFlushRequested() {
      return this.flushRequested;
    }
  }

  private volatile WriteState writestate = new WriteState();

  final int memcacheFlushSize;
  private volatile long lastFlushTime;
  final FlushRequester flushListener;
  private final int blockingMemcacheSize;
  final long threadWakeFrequency;
  // Used to guard splits and closes
  private final ReentrantReadWriteLock splitsAndClosesLock =
    new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock newScannerLock = 
    new ReentrantReadWriteLock();

  // Stop updates lock
  private final ReentrantReadWriteLock updatesLock =
    new ReentrantReadWriteLock();
  private final Object splitLock = new Object();
  private long minSequenceId;
  final AtomicInteger activeScannerCount = new AtomicInteger(0);
  
  /**
   * Name of the region info file that resides just under the region directory.
   */
  public final static String REGIONINFO_FILE = ".regioninfo";

  /**
   * REGIONINFO_FILE as byte array.
   */
  public final static byte [] REGIONINFO_FILE_BYTES =
    Bytes.toBytes(REGIONINFO_FILE);

  /**
   * HRegion constructor.
   *
   * @param basedir qualified path of directory where region should be located,
   * usually the table directory.
   * @param log The HLog is the outbound log for any updates to the HRegion
   * (There's a single HLog for all the HRegions on a single HRegionServer.)
   * The log file is a logfile from the previous execution that's
   * custom-computed for this HRegion. The HRegionServer computes and sorts the
   * appropriate log info for this HRegion. If there is a previous log file
   * (implying that the HRegion has been written-to before), then read it from
   * the supplied path.
   * @param fs is the filesystem.  
   * @param conf is global configuration settings.
   * @param regionInfo - HRegionInfo that describes the region
   * is new), then read them from the supplied path.
   * @param flushListener an object that implements CacheFlushListener or null
   * making progress to master -- otherwise master might think region deploy
   * failed.  Can be null.
   * @throws IOException 
   */
  public HRegion(Path basedir, HLog log, FileSystem fs, HBaseConfiguration conf, 
      HRegionInfo regionInfo, FlushRequester flushListener) {
    this.basedir = basedir;
    this.comparator = regionInfo.getComparator();
    this.comparatorIgnoreTimestamp =
      this.comparator.getComparatorIgnoringTimestamps();
    this.log = log;
    this.fs = fs;
    this.conf = conf;
    this.regionInfo = regionInfo;
    this.flushListener = flushListener;
    this.threadWakeFrequency = conf.getLong(THREAD_WAKE_FREQUENCY, 10 * 1000);
    String encodedNameStr = Integer.toString(this.regionInfo.getEncodedName());
    this.regiondir = new Path(basedir, encodedNameStr);
    this.historian = RegionHistorian.getInstance();
    if (LOG.isDebugEnabled()) {
      // Write out region name as string and its encoded name.
      LOG.debug("Opening region " + this + "/" +
        this.regionInfo.getEncodedName());
    }
    this.regionCompactionDir =
      new Path(getCompactionDir(basedir), encodedNameStr);
    int flushSize = regionInfo.getTableDesc().getMemcacheFlushSize();
    if (flushSize == HTableDescriptor.DEFAULT_MEMCACHE_FLUSH_SIZE) {
      flushSize = conf.getInt("hbase.hregion.memcache.flush.size",
                      HTableDescriptor.DEFAULT_MEMCACHE_FLUSH_SIZE);
    }
    this.memcacheFlushSize = flushSize;
    this.blockingMemcacheSize = this.memcacheFlushSize *
      conf.getInt("hbase.hregion.memcache.block.multiplier", 1);
  }

  /**
   * Initialize this region and get it ready to roll.
   * Called after construction.
   * 
   * @param initialFiles
   * @param reporter
   * @throws IOException
   */
  public void initialize( Path initialFiles, final Progressable reporter)
  throws IOException {
    Path oldLogFile = new Path(regiondir, HREGION_OLDLOGFILE_NAME);
    
    // Write HRI to a file in case we need to recover .META.
    checkRegioninfoOnFilesystem();

    // Move prefab HStore files into place (if any).  This picks up split files
    // and any merges from splits and merges dirs.
    if (initialFiles != null && fs.exists(initialFiles)) {
      fs.rename(initialFiles, this.regiondir);
    }

    // Load in all the HStores.
    long maxSeqId = -1;
    long minSeqId = Integer.MAX_VALUE;
    for (HColumnDescriptor c : this.regionInfo.getTableDesc().getFamilies()) {
      Store store = instantiateHStore(this.basedir, c, oldLogFile, reporter);
      this.stores.put(c.getName(), store);
      long storeSeqId = store.getMaxSequenceId();
      if (storeSeqId > maxSeqId) {
        maxSeqId = storeSeqId;
      }
      if (storeSeqId < minSeqId) {
        minSeqId = storeSeqId;
      }
    }

    // Play log if one.  Delete when done.
    doReconstructionLog(oldLogFile, minSeqId, maxSeqId, reporter);
    if (fs.exists(oldLogFile)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Deleting old log file: " + oldLogFile);
      }
      fs.delete(oldLogFile, false);
    }
    
    // Add one to the current maximum sequence id so new edits are beyond.
    this.minSequenceId = maxSeqId + 1;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Next sequence id for region " +
        Bytes.toString(regionInfo.getRegionName()) + " is " +
        this.minSequenceId);
    }

    // Get rid of any splits or merges that were lost in-progress
    FSUtils.deleteDirectory(this.fs, new Path(regiondir, SPLITDIR));
    FSUtils.deleteDirectory(this.fs, new Path(regiondir, MERGEDIR));

    // See if region is meant to run read-only.
    if (this.regionInfo.getTableDesc().isReadOnly()) {
      this.writestate.setReadOnly(true);
    }

    // HRegion is ready to go!
    this.writestate.compacting = false;
    this.lastFlushTime = System.currentTimeMillis();
    LOG.info("region " + this + "/" + this.regionInfo.getEncodedName() +
      " available");
  }

  /*
   * Write out an info file under the region directory.  Useful recovering
   * mangled regions.
   * @throws IOException
   */
  private void checkRegioninfoOnFilesystem() throws IOException {
    // Name of this file has two leading and trailing underscores so it doesn't
    // clash w/ a store/family name.  There is possibility, but assumption is
    // that its slim (don't want to use control character in filename because
    // 
    Path regioninfo = new Path(this.regiondir, REGIONINFO_FILE);
    if (this.fs.exists(regioninfo) &&
        this.fs.getFileStatus(regioninfo).getLen() > 0) {
      return;
    }
    FSDataOutputStream out = this.fs.create(regioninfo, true);
    try {
      this.regionInfo.write(out);
      out.write('\n');
      out.write('\n');
      out.write(Bytes.toBytes(this.regionInfo.toString()));
    } finally {
      out.close();
    }
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

  /** @return true if region is closed */
  public boolean isClosed() {
    return this.closed.get();
  }
  
  /**
   * @return True if closing process has started.
   */
  public boolean isClosing() {
    return this.closing.get();
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
  public List<StoreFile> close() throws IOException {
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
  List<StoreFile> close(final boolean abort) throws IOException {
    if (isClosed()) {
      LOG.warn("region " + this + " already closed");
      return null;
    }
    this.closing.set(true);
    synchronized (splitLock) {
      synchronized (writestate) {
        // Disable compacting and flushing by background threads for this
        // region.
        writestate.writesEnabled = false;
        LOG.debug("Closing " + this + ": compactions & flushes disabled ");
        while (writestate.compacting || writestate.flushing) {
          LOG.debug("waiting for" +
              (writestate.compacting ? " compaction" : "") +
              (writestate.flushing ?
                  (writestate.compacting ? "," : "") + " cache flush" :
                    "") + " to complete for region " + this);
          try {
            writestate.wait();
          } catch (InterruptedException iex) {
            // continue
          }
        }
      }
      newScannerLock.writeLock().lock();
      try {
        // Wait for active scanners to finish. The write lock we hold will
        // prevent new scanners from being created.
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
        splitsAndClosesLock.writeLock().lock();
        LOG.debug("Updates disabled for region, no outstanding scanners on " +
          this);
        try {
          // Write lock means no more row locks can be given out.  Wait on
          // outstanding row locks to come in before we close so we do not drop
          // outstanding updates.
          waitOnRowLocks();
          LOG.debug("No more row locks outstanding on region " + this);
  
          // Don't flush the cache if we are aborting
          if (!abort) {
            internalFlushcache();
          }
  
          List<StoreFile> result = new ArrayList<StoreFile>();
          for (Store store: stores.values()) {
            result.addAll(store.close());
          }
          this.closed.set(true);
          LOG.info("Closed " + this);
          return result;
        } finally {
          splitsAndClosesLock.writeLock().unlock();
        }
      } finally {
        newScannerLock.writeLock().unlock();
      }
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // HRegion accessors
  //////////////////////////////////////////////////////////////////////////////

  /** @return start key for region */
  public byte [] getStartKey() {
    return this.regionInfo.getStartKey();
  }

  /** @return end key for region */
  public byte [] getEndKey() {
    return this.regionInfo.getEndKey();
  }

  /** @return region id */
  public long getRegionId() {
    return this.regionInfo.getRegionId();
  }

  /** @return region name */
  public byte [] getRegionName() {
    return this.regionInfo.getRegionName();
  }

  /** @return HTableDescriptor for this region */
  public HTableDescriptor getTableDesc() {
    return this.regionInfo.getTableDesc();
  }

  /** @return HLog in use for this region */
  public HLog getLog() {
    return this.log;
  }

  /** @return Configuration object */
  public HBaseConfiguration getConf() {
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

  /** @return returns size of largest HStore. */
  public long getLargestHStoreSize() {
    long size = 0;
    for (Store h: stores.values()) {
      long storeSize = h.getSize();
      if (storeSize > size) {
        size = storeSize;
      }
    }
    return size;
  }

  /*
   * Split the HRegion to create two brand-new ones.  This also closes
   * current HRegion.  Split should be fast since we don't rewrite store files
   * but instead create new 'reference' store files that read off the top and
   * bottom ranges of parent store files.
   * @param splitRow row on which to split region
   * @return two brand-new (and open) HRegions or null if a split is not needed
   * @throws IOException
   */
  HRegion [] splitRegion(final byte [] splitRow) throws IOException {
    synchronized (splitLock) {
      if (closed.get()) {
        return null;
      }
      // Add start/end key checking: hbase-428.
      byte [] startKey = this.regionInfo.getStartKey();
      byte [] endKey = this.regionInfo.getEndKey();
      if (this.comparator.matchingRows(startKey, 0, startKey.length,
          splitRow, 0, splitRow.length)) {
        LOG.debug("Startkey and midkey are same, not splitting");
        return null;
      }
      if (this.comparator.matchingRows(splitRow, 0, splitRow.length,
          endKey, 0, endKey.length)) {
        LOG.debug("Endkey and midkey are same, not splitting");
        return null;
      }
      LOG.info("Starting split of region " + this);
      Path splits = new Path(this.regiondir, SPLITDIR);
      if(!this.fs.exists(splits)) {
        this.fs.mkdirs(splits);
      }
      // Calculate regionid to use.  Can't be less than that of parent else
      // it'll insert into wrong location over in .META. table: HBASE-710.
      long rid = System.currentTimeMillis();
      if (rid < this.regionInfo.getRegionId()) {
        LOG.warn("Clock skew; parent regions id is " +
          this.regionInfo.getRegionId() + " but current time here is " + rid);
        rid = this.regionInfo.getRegionId() + 1;
      }
      HRegionInfo regionAInfo = new HRegionInfo(this.regionInfo.getTableDesc(),
        startKey, splitRow, false, rid);
      Path dirA =
        new Path(splits, Integer.toString(regionAInfo.getEncodedName()));
      if(fs.exists(dirA)) {
        throw new IOException("Cannot split; target file collision at " + dirA);
      }
      HRegionInfo regionBInfo = new HRegionInfo(this.regionInfo.getTableDesc(),
        splitRow, endKey, false, rid);
      Path dirB =
        new Path(splits, Integer.toString(regionBInfo.getEncodedName()));
      if(this.fs.exists(dirB)) {
        throw new IOException("Cannot split; target file collision at " + dirB);
      }

      // Now close the HRegion.  Close returns all store files or null if not
      // supposed to close (? What to do in this case? Implement abort of close?)
      // Close also does wait on outstanding rows and calls a flush just-in-case.
      List<StoreFile> hstoreFilesToSplit = close(false);
      if (hstoreFilesToSplit == null) {
        LOG.warn("Close came back null (Implement abort of close?)");
        throw new RuntimeException("close returned empty vector of HStoreFiles");
      }

      // Split each store file.
      for(StoreFile h: hstoreFilesToSplit) {
        StoreFile.split(fs,
          Store.getStoreHomedir(splits, regionAInfo.getEncodedName(),
            h.getFamily()),
          h, splitRow, Range.bottom);
        StoreFile.split(fs,
          Store.getStoreHomedir(splits, regionBInfo.getEncodedName(),
            h.getFamily()),
          h, splitRow, Range.top);
      }

      // Done!
      // Opening the region copies the splits files from the splits directory
      // under each region.
      HRegion regionA = new HRegion(basedir, log, fs, conf, regionAInfo, null);
      regionA.initialize(dirA, null);
      regionA.close();
      HRegion regionB = new HRegion(basedir, log, fs, conf, regionBInfo, null);
      regionB.initialize(dirB, null);
      regionB.close();

      // Cleanup
      boolean deleted = fs.delete(splits, true); // Get rid of splits directory
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cleaned up " + FSUtils.getPath(splits) + " " + deleted);
      }
      HRegion regions[] = new HRegion [] {regionA, regionB};
      this.historian.addRegionSplit(this.regionInfo,
        regionA.getRegionInfo(), regionB.getRegionInfo());
      return regions;
    }
  }
  
  /*
   * @param dir
   * @return compaction directory for the passed in <code>dir</code>
   */
  static Path getCompactionDir(final Path dir) {
   return new Path(dir, HREGION_COMPACTIONDIR_NAME);
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
    FSUtils.deleteDirectory(this.fs, this.regionCompactionDir);
  }

  void setForceMajorCompaction(final boolean b) {
    this.forceMajorCompaction = b;
  }

  boolean getForceMajorCompaction() {
    return this.forceMajorCompaction;
  }

  /**
   * Called by compaction thread and after region is opened to compact the
   * HStores if necessary.
   *
   * <p>This operation could block for a long time, so don't call it from a 
   * time-sensitive thread.
   *
   * Note that no locking is necessary at this level because compaction only
   * conflicts with a region split, and that cannot happen because the region
   * server does them sequentially and not in parallel.
   * 
   * @return mid key if split is needed
   * @throws IOException
   */
  public byte [] compactStores() throws IOException {
    boolean majorCompaction = this.forceMajorCompaction;
    this.forceMajorCompaction = false;
    return compactStores(majorCompaction);
  }

  /*
   * Called by compaction thread and after region is opened to compact the
   * HStores if necessary.
   *
   * <p>This operation could block for a long time, so don't call it from a 
   * time-sensitive thread.
   *
   * Note that no locking is necessary at this level because compaction only
   * conflicts with a region split, and that cannot happen because the region
   * server does them sequentially and not in parallel.
   * 
   * @param majorCompaction True to force a major compaction regardless of thresholds
   * @return split row if split is needed
   * @throws IOException
   */
  byte [] compactStores(final boolean majorCompaction)
  throws IOException {
    splitsAndClosesLock.readLock().lock();
    try {
      byte [] splitRow = null;
      if (this.closed.get()) {
        return splitRow;
      }
      try {
        synchronized (writestate) {
          if (!writestate.compacting && writestate.writesEnabled) {
            writestate.compacting = true;
          } else {
            LOG.info("NOT compacting region " + this +
                ": compacting=" + writestate.compacting + ", writesEnabled=" +
                writestate.writesEnabled);
              return splitRow;
          }
        }
        LOG.info("starting " + (majorCompaction? "major" : "") + 
            " compaction on region " + this);
        long startTime = System.currentTimeMillis();
        doRegionCompactionPrep();
        long maxSize = -1;
        for (Store store: stores.values()) {
          final Store.StoreSize ss = store.compact(majorCompaction);
          if (ss != null && ss.getSize() > maxSize) {
            maxSize = ss.getSize();
            splitRow = ss.getSplitRow();
          }
        }
        doRegionCompactionCleanup();
        String timeTaken = StringUtils.formatTimeDiff(System.currentTimeMillis(), 
            startTime);
        LOG.info("compaction completed on region " + this + " in " + timeTaken);
        this.historian.addRegionCompaction(regionInfo, timeTaken);
      } finally {
        synchronized (writestate) {
          writestate.compacting = false;
          writestate.notifyAll();
        }
      }
      return splitRow;
    } finally {
      splitsAndClosesLock.readLock().unlock();
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
  public boolean flushcache() throws IOException {
    if (this.closed.get()) {
      return false;
    }
    synchronized (writestate) {
      if (!writestate.flushing && writestate.writesEnabled) {
        this.writestate.flushing = true;
      } else {
        if(LOG.isDebugEnabled()) {
          LOG.debug("NOT flushing memcache for region " + this +
            ", flushing=" +
              writestate.flushing + ", writesEnabled=" +
              writestate.writesEnabled);
        }
        return false;  
      }
    }
    try {
      // Prevent splits and closes
      splitsAndClosesLock.readLock().lock();
      try {
        return internalFlushcache();
      } finally {
        splitsAndClosesLock.readLock().unlock();
      }
    } finally {
      synchronized (writestate) {
        writestate.flushing = false;
        this.writestate.flushRequested = false;
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
   * 
   * @return true if the region needs compacting
   * 
   * @throws IOException
   * @throws DroppedSnapshotException Thrown when replay of hlog is required
   * because a Snapshot was not properly persisted.
   */
  private boolean internalFlushcache() throws IOException {
    final long startTime = System.currentTimeMillis();
    // Clear flush flag.
    // Record latest flush time
    this.lastFlushTime = startTime;
    // If nothing to flush, return and avoid logging start/stop flush.
    if (this.memcacheSize.get() <= 0) {
      return false;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Started memcache flush for region " + this +
        ". Current region memcache size " +
          StringUtils.humanReadableInt(this.memcacheSize.get()));
    }

    // Stop updates while we snapshot the memcache of all stores. We only have
    // to do this for a moment.  Its quick.  The subsequent sequence id that
    // goes into the HLog after we've flushed all these snapshots also goes
    // into the info file that sits beside the flushed files.
    // We also set the memcache size to zero here before we allow updates
    // again so its value will represent the size of the updates received
    // during the flush
    long sequenceId = -1L;
    long completeSequenceId = -1L;
    this.updatesLock.writeLock().lock();
    // Get current size of memcaches.
    final long currentMemcacheSize = this.memcacheSize.get();
    try {
      for (Store s: stores.values()) {
        s.snapshot();
      }
      sequenceId = log.startCacheFlush();
      completeSequenceId = this.getCompleteCacheFlushSequenceId(sequenceId);
    } finally {
      this.updatesLock.writeLock().unlock();
    }

    // Any failure from here on out will be catastrophic requiring server
    // restart so hlog content can be replayed and put back into the memcache.
    // Otherwise, the snapshot content while backed up in the hlog, it will not
    // be part of the current running servers state.
    boolean compactionRequested = false;
    try {
      // A.  Flush memcache to all the HStores.
      // Keep running vector of all store files that includes both old and the
      // just-made new flush store file.
      for (Store hstore: stores.values()) {
        boolean needsCompaction = hstore.flushCache(completeSequenceId);
        if (needsCompaction) {
          compactionRequested = true;
        }
      }
      // Set down the memcache size by amount of flush.
      this.memcacheSize.addAndGet(-currentMemcacheSize);
    } catch (Throwable t) {
      // An exception here means that the snapshot was not persisted.
      // The hlog needs to be replayed so its content is restored to memcache.
      // Currently, only a server restart will do this.
      // We used to only catch IOEs but its possible that we'd get other
      // exceptions -- e.g. HBASE-659 was about an NPE -- so now we catch
      // all and sundry.
      this.log.abortCacheFlush();
      DroppedSnapshotException dse = new DroppedSnapshotException("region: " +
          Bytes.toString(getRegionName()));
      dse.initCause(t);
      throw dse;
    }

    // If we get to here, the HStores have been written. If we get an
    // error in completeCacheFlush it will release the lock it is holding

    // B.  Write a FLUSHCACHE-COMPLETE message to the log.
    //     This tells future readers that the HStores were emitted correctly,
    //     and that all updates to the log for this regionName that have lower 
    //     log-sequence-ids can be safely ignored.
    this.log.completeCacheFlush(getRegionName(),
        regionInfo.getTableDesc().getName(), completeSequenceId);

    // C. Finally notify anyone waiting on memcache to clear:
    // e.g. checkResources().
    synchronized (this) {
      notifyAll();
    }
    
    if (LOG.isDebugEnabled()) {
      long now = System.currentTimeMillis();
      String timeTaken = StringUtils.formatTimeDiff(now, startTime);
      LOG.debug("Finished memcache flush of ~" +
        StringUtils.humanReadableInt(currentMemcacheSize) + " for region " +
        this + " in " + (now - startTime) + "ms, sequence id=" + sequenceId +
        ", compaction requested=" + compactionRequested);
      if (!regionInfo.isMetaRegion()) {
        this.historian.addRegionFlush(regionInfo, timeTaken);
      }
    }
    return compactionRequested;
  }
  
  /**
   * Get the sequence number to be associated with this cache flush. Used by
   * TransactionalRegion to not complete pending transactions.
   * 
   * 
   * @param currentSequenceId
   * @return sequence id to complete the cache flush with
   */ 
  protected long getCompleteCacheFlushSequenceId(long currentSequenceId) {
    return currentSequenceId;
  }
  
  //////////////////////////////////////////////////////////////////////////////
  // get() methods for client use.
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Fetch multiple versions of a single data item, with timestamp.
   *
   * @param row
   * @param column
   * @param ts
   * @param nv
   * @return Results or null if none.
   * @throws IOException
   */
  public List<KeyValue> get(final byte[] row, final byte[] column, final long ts,
      final int nv) 
  throws IOException {
    long timestamp = ts == -1? HConstants.LATEST_TIMESTAMP : ts;
    int numVersions = nv == -1? 1 : nv;
    splitsAndClosesLock.readLock().lock();
    try {
      if (this.closed.get()) {
        throw new IOException("Region " + this + " closed");
      }
      // Make sure this is a valid row and valid column
      checkRow(row);
      checkColumn(column);
      // Don't need a row lock for a simple get
      List<KeyValue> result = getStore(column).
        get(KeyValue.createFirstOnRow(row, column, timestamp), numVersions);
      // Guarantee that we return null instead of a zero-length array, 
      // if there are no results to return.
      return (result == null || result.isEmpty())? null : result;
    } finally {
      splitsAndClosesLock.readLock().unlock();
    }
  }

  /**
   * Data structure with a counter that is accessible rather than create a
   * new Integer every time we want to up the counter.  Initializes at count 1.
   */
  static class Counter {
    int counter = 1;
  }

  /*
   * Check to see if we've not gone over threshold for this particular
   * column.
   * @param kv
   * @param versions
   * @param versionsCount
   * @return True if its ok to add current value.
   */
  static boolean okToAddResult(final KeyValue kv, final int versions,
      final Map<KeyValue, HRegion.Counter> versionsCount) {
    if (versionsCount == null) {
      return true;
    }
    if (versionsCount.containsKey(kv)) {
      if (versionsCount.get(kv).counter < versions) {
        return true;
      }
    } else {
      return true;
    }
    return false;
  }

  /*
   * Used adding item found to list of results getting.
   * @param kv
   * @param versionsCount
   * @param results
   */
  static void addResult(final KeyValue kv,
      final Map<KeyValue, HRegion.Counter> versionsCount,
      final List<KeyValue> results) {
    // Don't add if already present; i.e. ignore second entry.
    if (results.contains(kv)) return;
    results.add(kv);
    if (versionsCount == null) {
      return;
    }
    if (!versionsCount.containsKey(kv)) {
      versionsCount.put(kv, new HRegion.Counter());
    } else {
      versionsCount.get(kv).counter++;
    }
  }

  /*
   * @param versions Number of versions to get.
   * @param versionsCount May be null.
   * @param columns Columns we want to fetch.
   * @return True if has enough versions.
   */
  static boolean hasEnoughVersions(final int versions,
      final Map<KeyValue, HRegion.Counter> versionsCount,
      final Set<byte []> columns) {
    if (columns == null || versionsCount == null) {
      // Wants all columns so just keep going
      return false;
    }
    if (columns.size() > versionsCount.size()) {
      return false;
    }
    if (versions == 1) {
      return true;
    }
    // Need to look at each to make sure at least versions.
    for (Map.Entry<KeyValue, HRegion.Counter> e: versionsCount.entrySet()) {
      if (e.getValue().counter < versions) {
        return false;
      }
    }
    return true;
  }

  /**
   * Fetch all the columns for the indicated row at a specified timestamp.
   * Returns a HbaseMapWritable that maps column names to values.
   *
   * We should eventually use Bloom filters here, to reduce running time.  If 
   * the database has many column families and is very sparse, then we could be 
   * checking many files needlessly.  A small Bloom for each row would help us 
   * determine which column groups are useful for that row.  That would let us 
   * avoid a bunch of disk activity.
   *
   * @param row
   * @param columns Array of columns you'd like to retrieve. When null, get all.
   * @param ts
   * @param numVersions number of versions to retrieve
   * @param lockid
   * @return HbaseMapWritable<columnName, Cell> values
   * @throws IOException
   */
  public HbaseMapWritable<byte [], Cell> getFull(final byte [] row,
      final NavigableSet<byte []> columns, final long ts,
      final int numVersions, final Integer lockid) 
  throws IOException {
    // Check columns passed
    if (columns != null) {
      for (byte [] column: columns) {
        checkColumn(column);
      }
    }
    List<KeyValue> keyvalues = new ArrayList<KeyValue>();
    Map<KeyValue, Counter> versionCounter =
      new TreeMap<KeyValue, Counter>(this.comparatorIgnoreTimestamp);
    Integer lid = getLock(lockid,row);
    HashSet<Store> storeSet = new HashSet<Store>();
    try {
      // Get the concerned columns or all of them
      if (columns != null) {
        for (byte[] bs : columns) {
          Store store = stores.get(bs);
          if (store != null) {
            storeSet.add(store);
          }
        }
      } else {
        storeSet.addAll(stores.values());
      }
      long timestamp =
        (ts == HConstants.LATEST_TIMESTAMP)? System.currentTimeMillis(): ts;
      KeyValue key = KeyValue.createFirstOnRow(row, timestamp);
      // For each column name that is just a column family, open the store
      // related to it and fetch everything for that row. HBASE-631
      // Also remove each store from storeSet so that these stores
      // won't be opened for no reason. HBASE-783
      if (columns != null) {
        for (byte [] bs : columns) {
          // TODO: Fix so we use comparator in KeyValue that looks at
          // column family portion only.
          if (KeyValue.getFamilyDelimiterIndex(bs, 0, bs.length) == (bs.length - 1)) {
            Store store = stores.get(bs);
            store.getFull(key, null, null, numVersions, versionCounter,
              keyvalues, timestamp);
            storeSet.remove(store);
          }
        }
      }
      for (Store targetStore: storeSet) {
        targetStore.getFull(key, columns, null, numVersions, versionCounter,
          keyvalues, timestamp);
      }
      
      return Cell.createCells(keyvalues);
    } finally {
      if(lockid == null) releaseRowLock(lid);
    }
  }

  /**
   * Return all the data for the row that matches <i>row</i> exactly, 
   * or the one that immediately preceeds it, at or immediately before 
   * <i>ts</i>.
   * 
   * @param row row key
   * @return map of values
   * @throws IOException
   */
  RowResult getClosestRowBefore(final byte [] row)
  throws IOException{
    return getClosestRowBefore(row, HConstants.COLUMN_FAMILY);
  }

  /**
   * Return all the data for the row that matches <i>row</i> exactly, 
   * or the one that immediately preceeds it, at or immediately before 
   * <i>ts</i>.
   * 
   * @param row row key
   * @param columnFamily Must include the column family delimiter character.
   * @return map of values
   * @throws IOException
   */
  public RowResult getClosestRowBefore(final byte [] row,
    final byte [] columnFamily)
  throws IOException{
    // look across all the HStores for this region and determine what the
    // closest key is across all column families, since the data may be sparse
    KeyValue key = null;
    checkRow(row);
    splitsAndClosesLock.readLock().lock();
    try {
      Store store = getStore(columnFamily);
      KeyValue kv = new KeyValue(row, HConstants.LATEST_TIMESTAMP);
      // get the closest key. (HStore.getRowKeyAtOrBefore can return null)
      key = store.getRowKeyAtOrBefore(kv);
      if (key == null) {
        return null;
      }
      List<KeyValue> results = new ArrayList<KeyValue>();
      // This will get all results for this store.  TODO: Do I have to make a
      // new key?
      if (!this.comparator.matchingRows(kv, key)) {
        kv = new KeyValue(key.getRow(), HConstants.LATEST_TIMESTAMP);
      }
      store.getFull(kv, null, null, 1, null, results, System.currentTimeMillis());
      // Convert to RowResult.  TODO: Remove need to do this.
      return RowResult.createRowResult(results);
    } finally {
      splitsAndClosesLock.readLock().unlock();
    }
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
   * @return InternalScanner
   * @throws IOException
   */
  public InternalScanner getScanner(byte[][] cols, byte [] firstRow,
    long timestamp, RowFilterInterface filter) 
  throws IOException {
    newScannerLock.readLock().lock();
    try {
      if (this.closed.get()) {
        throw new IOException("Region " + this + " closed");
      }
      HashSet<Store> storeSet = new HashSet<Store>();
      NavigableSet<byte []> columns =
        new TreeSet<byte []>(Bytes.BYTES_COMPARATOR);
      // Below we make up set of stores we want scanners on and we fill out the
      // list of columns.
      for (int i = 0; i < cols.length; i++) {
        columns.add(cols[i]);
        Store s = stores.get(cols[i]);
        if (s != null) {
          storeSet.add(s);
        }
      }
      return new HScanner(columns, firstRow, timestamp,
        storeSet.toArray(new Store [storeSet.size()]), filter);
    } finally {
      newScannerLock.readLock().unlock();
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // set() methods for client use.
  //////////////////////////////////////////////////////////////////////////////
  
  /**
   * @param b
   * @throws IOException
   */
  public void batchUpdate(BatchUpdate b) throws IOException {
    this.batchUpdate(b, null, true);
  }
  
  /**
   * @param b
   * @param writeToWAL
   * @throws IOException
   */
  public void batchUpdate(BatchUpdate b, boolean writeToWAL) throws IOException {
    this.batchUpdate(b, null, writeToWAL);
  }

  
  /**
   * @param b
   * @param lockid
   * @throws IOException
   */
  public void batchUpdate(BatchUpdate b, Integer lockid) throws IOException {
    this.batchUpdate(b, lockid, true);
  }
  
  /**
   * @param b
   * @param lockid
   * @param writeToWAL if true, then we write this update to the log
   * @throws IOException
   */
  public void batchUpdate(BatchUpdate b, Integer lockid, boolean writeToWAL)
  throws IOException {
    checkReadOnly();
    validateValuesLength(b);

    // Do a rough check that we have resources to accept a write.  The check is
    // 'rough' in that between the resource check and the call to obtain a 
    // read lock, resources may run out.  For now, the thought is that this
    // will be extremely rare; we'll deal with it when it happens.
    checkResources();
    splitsAndClosesLock.readLock().lock();
    try {
      // We obtain a per-row lock, so other clients will block while one client
      // performs an update. The read lock is released by the client calling
      // #commit or #abort or if the HRegionServer lease on the lock expires.
      // See HRegionServer#RegionListener for how the expire on HRegionServer
      // invokes a HRegion#abort.
      byte [] row = b.getRow();
      // If we did not pass an existing row lock, obtain a new one
      Integer lid = getLock(lockid, row);
      long commitTime = b.getTimestamp() == LATEST_TIMESTAMP?
        System.currentTimeMillis(): b.getTimestamp();
      Set<byte []> latestTimestampDeletes = null;
      List<KeyValue> edits = new ArrayList<KeyValue>();
      try {
        for (BatchOperation op: b) {
          byte [] column = op.getColumn();
          checkColumn(column);
          KeyValue kv = null;
          if (op.isPut()) {
            byte [] val = op.getValue();
            if (HLogEdit.isDeleted(val)) {
              throw new IOException("Cannot insert value: " +
                Bytes.toString(val));
            }
            kv = new KeyValue(row, column, commitTime, val);
          } else {
            // Its a delete.
            if (b.getTimestamp() == LATEST_TIMESTAMP) {
              // Save off these deletes of the most recent thing added on the
              // family.
              if (latestTimestampDeletes == null) {
                latestTimestampDeletes =
                  new TreeSet<byte []>(Bytes.BYTES_RAWCOMPARATOR);
              }
              latestTimestampDeletes.add(op.getColumn());
              continue;
            }
            // Its an explicit timestamp delete
            kv = new KeyValue(row, column, commitTime, KeyValue.Type.Delete,
              HConstants.EMPTY_BYTE_ARRAY);
          }
          edits.add(kv);
        }
        if (!edits.isEmpty()) {
          update(edits, writeToWAL);
        }
        if (latestTimestampDeletes != null &&
            !latestTimestampDeletes.isEmpty()) {
          // We have some LATEST_TIMESTAMP deletes to run.  Can't do them inline
          // as edits.  Need to do individually after figuring which is latest
          // timestamp to delete.
          for (byte [] column: latestTimestampDeletes) {
            deleteMultiple(row, column, LATEST_TIMESTAMP, 1);
          }
        }
      } finally {
        if(lockid == null) releaseRowLock(lid);
      }
    } finally {
      splitsAndClosesLock.readLock().unlock();
    }
  }

  /**
   * Performs an atomic check and save operation. Checks if
   * the specified expected values have changed, and if not
   * applies the update.
   * 
   * @param b the update to apply
   * @param expectedValues the expected values to check
   * @param lockid
   * @param writeToWAL whether or not to write to the write ahead log
   * @return true if update was applied
   * @throws IOException
   */
  public boolean checkAndSave(BatchUpdate b,
    HbaseMapWritable<byte[], byte[]> expectedValues, Integer lockid,
    boolean writeToWAL)
  throws IOException {
    // This is basically a copy of batchUpdate with the atomic check and save
    // added in. So you should read this method with batchUpdate. I will
    // comment the areas that I have changed where I have not changed, you
    // should read the comments from the batchUpdate method
    boolean success = true;
    checkReadOnly();
    validateValuesLength(b);
    checkResources();
    splitsAndClosesLock.readLock().lock();
    try {
      byte[] row = b.getRow();
      Integer lid = getLock(lockid,row);
      try {
        NavigableSet<byte []> keySet =
          new TreeSet<byte []>(Bytes.BYTES_COMPARATOR);
        keySet.addAll(expectedValues.keySet());
        Map<byte[],Cell> actualValues = getFull(row, keySet,
          HConstants.LATEST_TIMESTAMP, 1,lid);
        for (byte[] key : keySet) {
          // If test fails exit
          if(!Bytes.equals(actualValues.get(key).getValue(),
              expectedValues.get(key))) {
            success = false;
            break;
          }
        }
        if (success) {
          long commitTime = (b.getTimestamp() == LATEST_TIMESTAMP)?
            System.currentTimeMillis(): b.getTimestamp();
          Set<byte []> latestTimestampDeletes = null;
          List<KeyValue> edits = new ArrayList<KeyValue>();
          for (BatchOperation op: b) {
            byte [] column = op.getColumn();
            KeyValue kv = null;
            if (op.isPut()) {
              byte [] val = op.getValue();
              if (HLogEdit.isDeleted(val)) {
                throw new IOException("Cannot insert value: " +
                  Bytes.toString(val));
              }
              kv = new KeyValue(row, column, commitTime, val);
            } else {
              // Its a delete.
              if (b.getTimestamp() == LATEST_TIMESTAMP) {
                // Save off these deletes of the most recent thing added on
                // the family.
                if (latestTimestampDeletes == null) {
                  latestTimestampDeletes =
                    new TreeSet<byte []>(Bytes.BYTES_RAWCOMPARATOR);
                }
                latestTimestampDeletes.add(op.getColumn());
              } else {
                // Its an explicit timestamp delete
                kv = new KeyValue(row, column, commitTime,
                  KeyValue.Type.Delete, HConstants.EMPTY_BYTE_ARRAY);
              }
            }
            edits.add(kv);
          }
          if (!edits.isEmpty()) {
            update(edits, writeToWAL);
          }
          if (latestTimestampDeletes != null &&
              !latestTimestampDeletes.isEmpty()) {
            // We have some LATEST_TIMESTAMP deletes to run.  Can't do them inline
            // as edits.  Need to do individually after figuring which is latest
            // timestamp to delete.
            for (byte [] column: latestTimestampDeletes) {
              deleteMultiple(row, column, LATEST_TIMESTAMP, 1);
            }
          }
        }
      } finally {
        if(lockid == null) releaseRowLock(lid);
      }
    } finally {
      splitsAndClosesLock.readLock().unlock();
    }
    return success;
  }

  /*
   * Utility method to verify values length
   * @param batchUpdate The update to verify
   * @throws IOException Thrown if a value is too long
   */
  private void validateValuesLength(BatchUpdate batchUpdate)
  throws IOException {
    for (Iterator<BatchOperation> iter = 
      batchUpdate.iterator(); iter.hasNext();) {
      BatchOperation operation = iter.next();
      if (operation.getValue() != null) {
        HColumnDescriptor fam = this.regionInfo.getTableDesc().
          getFamily(operation.getColumn());
        if (fam != null) {
          int maxLength = fam.getMaxValueLength();
          if (operation.getValue().length > maxLength) {
            throw new ValueOverMaxLengthException("Value in column "
                + Bytes.toString(operation.getColumn()) + " is too long. "
                + operation.getValue().length + " instead of " + maxLength);
          }
        }
      }
    }
  }

  /*
   * Check if resources to support an update.
   * 
   * Here we synchronize on HRegion, a broad scoped lock.  Its appropriate
   * given we're figuring in here whether this region is able to take on
   * writes.  This is only method with a synchronize (at time of writing),
   * this and the synchronize on 'this' inside in internalFlushCache to send
   * the notify.
   */
  private void checkResources() {
    boolean blocked = false;
    while (this.memcacheSize.get() > this.blockingMemcacheSize) {
      requestFlush();
      if (!blocked) {
        LOG.info("Blocking updates for '" + Thread.currentThread().getName() +
          "' on region " + Bytes.toString(getRegionName()) +
          ": Memcache size " +
          StringUtils.humanReadableInt(this.memcacheSize.get()) +
          " is >= than blocking " +
          StringUtils.humanReadableInt(this.blockingMemcacheSize) + " size");
      }
      blocked = true;
      synchronized(this) {
        try {
          wait(threadWakeFrequency);
        } catch (InterruptedException e) {
          // continue;
        }
      }
    }
    if (blocked) {
      LOG.info("Unblocking updates for region " + this + " '"
          + Thread.currentThread().getName() + "'");
    }
  }
  
  /**
   * Delete all cells of the same age as the passed timestamp or older.
   * @param row
   * @param column
   * @param ts Delete all entries that have this timestamp or older
   * @param lockid Row lock
   * @throws IOException
   */
  public void deleteAll(final byte [] row, final byte [] column, final long ts,
      final Integer lockid)
  throws IOException {
    checkColumn(column);
    checkReadOnly();
    Integer lid = getLock(lockid,row);
    try {
      // Delete ALL versions rather than column family VERSIONS.  If we just did
      // VERSIONS, then if 2* VERSION cells, subsequent gets would get old stuff.
      deleteMultiple(row, column, ts, ALL_VERSIONS);
    } finally {
      if(lockid == null) releaseRowLock(lid);
    }
  }

  /**
   * Delete all cells of the same age as the passed timestamp or older.
   * @param row
   * @param ts Delete all entries that have this timestamp or older
   * @param lockid Row lock
   * @throws IOException
   */
  public void deleteAll(final byte [] row, final long ts, final Integer lockid)
  throws IOException {
    checkReadOnly();
    Integer lid = getLock(lockid, row);
    long time = ts;
    if (ts == HConstants.LATEST_TIMESTAMP) {
      time = System.currentTimeMillis();
    }
    KeyValue kv = KeyValue.createFirstOnRow(row, time);
    try {
      for (Store store : stores.values()) {
        List<KeyValue> keyvalues = new ArrayList<KeyValue>();
        store.getFull(kv, null, null, ALL_VERSIONS, null, keyvalues, time);
        List<KeyValue> edits = new ArrayList<KeyValue>();
        for (KeyValue key: keyvalues) {
          // This is UGLY. COPY OF KEY PART OF KeyValue.
          edits.add(key.cloneDelete());
        }
        update(edits);
      }
    } finally {
      if (lockid == null) releaseRowLock(lid);
    }
  }
  
  /**
   * Delete all cells for a row with matching columns with timestamps
   * less than or equal to <i>timestamp</i>. 
   * 
   * @param row The row to operate on
   * @param columnRegex The column regex 
   * @param timestamp Timestamp to match
   * @param lockid Row lock
   * @throws IOException
   */
  public void deleteAllByRegex(final byte [] row, final String columnRegex, 
      final long timestamp, final Integer lockid) throws IOException {
    checkReadOnly();
    Pattern columnPattern = Pattern.compile(columnRegex);
    Integer lid = getLock(lockid, row);
    long now = System.currentTimeMillis();
    KeyValue kv = new KeyValue(row, timestamp);
    try {
      for (Store store : stores.values()) {
        List<KeyValue> keyvalues = new ArrayList<KeyValue>();
        store.getFull(kv, null, columnPattern, ALL_VERSIONS, null, keyvalues,
          now);
        List<KeyValue> edits = new ArrayList<KeyValue>();
        for (KeyValue key: keyvalues) {
          edits.add(key.cloneDelete());
        }
        update(edits);
      }
    } finally {
      if(lockid == null) releaseRowLock(lid);
    }
  }

  /**
   * Delete all cells for a row with matching column family with timestamps
   * less than or equal to <i>timestamp</i>.
   *
   * @param row The row to operate on
   * @param family The column family to match
   * @param timestamp Timestamp to match
   * @param lockid Row lock
   * @throws IOException
   */
  public void deleteFamily(byte [] row, byte [] family, long timestamp,
      final Integer lockid)
  throws IOException{
    checkReadOnly();
    Integer lid = getLock(lockid, row);
    long now = System.currentTimeMillis();
    try {
      // find the HStore for the column family
      Store store = getStore(family);
      // find all the keys that match our criteria
      List<KeyValue> keyvalues = new ArrayList<KeyValue>();
      store.getFull(new KeyValue(row, timestamp), null, null, ALL_VERSIONS,
        null, keyvalues, now);
      // delete all the cells
      List<KeyValue> edits = new ArrayList<KeyValue>();
      for (KeyValue kv: keyvalues) {
        edits.add(kv.cloneDelete());
      }
      update(edits);
    } finally {
      if(lockid == null) releaseRowLock(lid);
    }
  }

  /**
   * Delete all cells for a row with all the matching column families by
   * familyRegex with timestamps less than or equal to <i>timestamp</i>.
   * 
   * @param row The row to operate on
   * @param familyRegex The column family regex for matching. This regex
   * expression just match the family name, it didn't include <code>:<code>
   * @param timestamp Timestamp to match
   * @param lockid Row lock
   * @throws IOException
   */
  public void deleteFamilyByRegex(byte [] row, String familyRegex,
      final long timestamp, final Integer lockid)
  throws IOException {
    checkReadOnly();
    // construct the family regex pattern
    Pattern familyPattern = Pattern.compile(familyRegex);
    Integer lid = getLock(lockid, row);
    long now = System.currentTimeMillis();
    KeyValue kv = new KeyValue(row, timestamp);
    try {
      for(Store store: stores.values()) {
        String familyName = Bytes.toString(store.getFamily().getName());
        // check the family name match the family pattern.
        if(!(familyPattern.matcher(familyName).matches())) 
          continue;
        
        List<KeyValue> keyvalues = new ArrayList<KeyValue>();
        store.getFull(kv, null, null, ALL_VERSIONS, null, keyvalues, now);
        List<KeyValue> edits = new ArrayList<KeyValue>();
        for (KeyValue k: keyvalues) {
          edits.add(k.cloneDelete());
        }
        update(edits);
      }
    } finally {
      if(lockid == null) releaseRowLock(lid);
    }
  }
  
  /*
   * Delete one or many cells.
   * Used to support {@link #deleteAll(byte [], byte [], long)} and deletion of
   * latest cell.
   * @param row
   * @param column
   * @param ts Timestamp to start search on.
   * @param versions How many versions to delete. Pass
   * {@link HConstants#ALL_VERSIONS} to delete all.
   * @throws IOException
   */
  private void deleteMultiple(final byte [] row, final byte [] column,
      final long ts, final int versions)
  throws IOException {
    checkReadOnly();
    // We used to have a getKeys method that purportedly only got the keys and
    // not the keys and values.  We now just do getFull.  For memcache values,
    // shouldn't matter if we get key and value since it'll be the entry that
    // is in memcache.  For the keyvalues from storefile, could be saving if
    // we only returned key component. TODO.
    List<KeyValue> keys = get(row, column, ts, versions);
    if (keys != null && keys.size() > 0) {
      // I think the below edits don't have to be storted.  Its deletes.
      // hey don't have to go in in exact sorted order (we don't have to worry
      // about the meta or root sort comparator here).
      List<KeyValue> edits = new ArrayList<KeyValue>();
      for (KeyValue key: keys) {
        edits.add(key.cloneDelete());
      }
      update(edits);
    }
  }

  /**
   * Tests for the existence of any cells for a given coordinate.
   * 
   * @param row the row
   * @param column the column, or null
   * @param timestamp the timestamp, or HConstants.LATEST_VERSION for any
   * @param lockid the existing lock, or null 
   * @return true if cells exist for the row, false otherwise
   * @throws IOException
   */
  public boolean exists(final byte[] row, final byte[] column, 
    final long timestamp, final Integer lockid) 
  throws IOException {
    checkRow(row);
    Integer lid = getLock(lockid, row);
    try {
      NavigableSet<byte []> columns = null;
      if (column != null) {
        columns = new TreeSet<byte []>(Bytes.BYTES_COMPARATOR);
        columns.add(column);
      }
      return !getFull(row, columns, timestamp, 1, lid).isEmpty();
    } finally {
      if (lockid == null) releaseRowLock(lid);
    }
  }

  /**
   * @throws IOException Throws exception if region is in read-only mode.
   */
  protected void checkReadOnly() throws IOException {
    if (this.writestate.isReadOnly()) {
      throw new IOException("region is read only");
    }
  }

  /** 
   * Add updates first to the hlog and then add values to memcache.
   * Warning: Assumption is caller has lock on passed in row.
   * @param edits Cell updates by column
   * @throws IOException
   */
  private void update(final List<KeyValue> edits) throws IOException {
    this.update(edits, true);
  }

  /** 
   * Add updates first to the hlog (if writeToWal) and then add values to memcache.
   * Warning: Assumption is caller has lock on passed in row.
   * @param writeToWAL if true, then we should write to the log
   * @param updatesByColumn Cell updates by column
   * @throws IOException
   */
  private void update(final List<KeyValue> edits, boolean writeToWAL)
  throws IOException {
    if (edits == null || edits.isEmpty()) {
      return;
    }
    boolean flush = false;
    this.updatesLock.readLock().lock();
    try {
      if (writeToWAL) {
        this.log.append(regionInfo.getRegionName(),
          regionInfo.getTableDesc().getName(), edits,
          (regionInfo.isMetaRegion() || regionInfo.isRootRegion()));
      }
      long size = 0;
      for (KeyValue kv: edits) {
        // TODO: Fix -- do I have to do a getColumn here?
        size = this.memcacheSize.addAndGet(getStore(kv.getColumn()).add(kv));
      }
      flush = isFlushSize(size);
    } finally {
      this.updatesLock.readLock().unlock();
    }
    if (flush) {
      // Request a cache flush.  Do it outside update lock.
      requestFlush();
    }
  }

  private void requestFlush() {
    if (this.flushListener == null) {
      return;
    }
    synchronized (writestate) {
      if (this.writestate.isFlushRequested()) {
        return;
      }
      writestate.flushRequested = true;
    }
    // Make request outside of synchronize block; HBASE-818.
    this.flushListener.request(this);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Flush requested on " + this);
    }
  }

  /*
   * @param size
   * @return True if size is over the flush threshold
   */
  private boolean isFlushSize(final long size) {
    return size > this.memcacheFlushSize;
  }
  
  // Do any reconstruction needed from the log
  @SuppressWarnings("unused")
  protected void doReconstructionLog(Path oldLogFile, long minSeqId, long maxSeqId,
    Progressable reporter)
  throws UnsupportedEncodingException, IOException {
    // Nothing to do (Replaying is done in HStores)
  }

  protected Store instantiateHStore(Path baseDir, 
    HColumnDescriptor c, Path oldLogFile, Progressable reporter)
  throws IOException {
    return new Store(baseDir, this.regionInfo, c, this.fs, oldLogFile,
      this.conf, reporter);
  }

  /**
   * Return HStore instance.
   * Use with caution.  Exposed for use of fixup utilities.
   * @param column Name of column family hosted by this region.
   * @return Store that goes with the family on passed <code>column</code>.
   * TODO: Make this lookup faster.
   */
  public Store getStore(final byte [] column) {
    return this.stores.get(column); 
  }

  //////////////////////////////////////////////////////////////////////////////
  // Support code
  //////////////////////////////////////////////////////////////////////////////

  /** Make sure this is a valid row for the HRegion */
  private void checkRow(final byte [] row) throws IOException {
    if(!rowIsInRange(regionInfo, row)) {
      throw new WrongRegionException("Requested row out of range for " +
          "HRegion " + this + ", startKey='" +
          Bytes.toString(regionInfo.getStartKey()) + "', getEndKey()='" +
          Bytes.toString(regionInfo.getEndKey()) + "', row='" +
          Bytes.toString(row) + "'");
    }
  }
  
  /*
   * Make sure this is a valid column for the current table
   * @param columnName
   * @throws NoSuchColumnFamilyException
   */
  private void checkColumn(final byte [] column)
  throws NoSuchColumnFamilyException {
    if (column == null) {
      return;
    }
    if (!regionInfo.getTableDesc().hasFamily(column)) {
      throw new NoSuchColumnFamilyException("Column family on " +
        Bytes.toString(column) + " does not exist in region " + this
          + " in table " + regionInfo.getTableDesc());
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
  Integer obtainRowLock(final byte [] row) throws IOException {
    checkRow(row);
    splitsAndClosesLock.readLock().lock();
    try {
      if (this.closed.get()) {
        throw new NotServingRegionException("Region " + this + " closed");
      }
      Integer key = Bytes.mapKey(row);
      synchronized (locksToRows) {
        while (locksToRows.containsKey(key)) {
          try {
            locksToRows.wait();
          } catch (InterruptedException ie) {
            // Empty
          }
        }
        locksToRows.put(key, row);
        locksToRows.notifyAll();
        return key;
      }
    } finally {
      splitsAndClosesLock.readLock().unlock();
    }
  }
  
  /**
   * Used by unit tests.
   * @param lockid
   * @return Row that goes with <code>lockid</code>
   */
  byte [] getRowFromLock(final Integer lockid) {
    return locksToRows.get(lockid);
  }
  
  /** 
   * Release the row lock!
   * @param row Name of row whose lock we are to release
   */
  void releaseRowLock(final Integer lockid) {
    synchronized (locksToRows) {
      locksToRows.remove(lockid);
      locksToRows.notifyAll();
    }
  }
  
  /**
   * See if row is currently locked.
   * @param lockid
   * @return boolean
   */
  private boolean isRowLocked(final Integer lockid) {
    synchronized (locksToRows) {
      if(locksToRows.containsKey(lockid)) {
        return true;
      }
      return false;
    }
  }
  
  /**
   * Returns existing row lock if found, otherwise
   * obtains a new row lock and returns it.
   * @param lockid
   * @return lockid
   */
  private Integer getLock(Integer lockid, byte [] row) 
  throws IOException {
    Integer lid = null;
    if (lockid == null) {
      lid = obtainRowLock(row);
    } else {
      if (!isRowLocked(lockid)) {
        throw new IOException("Invalid row lock");
      }
      lid = lockid;
    }
    return lid;
  }
  
  private void waitOnRowLocks() {
    synchronized (locksToRows) {
      while (this.locksToRows.size() > 0) {
        LOG.debug("waiting for " + this.locksToRows.size() + " row locks");
        try {
          this.locksToRows.wait();
        } catch (InterruptedException e) {
          // Catch. Let while test determine loop-end.
        }
      }
    }
  }
  
  @Override
  public boolean equals(Object o) {
    return this.hashCode() == ((HRegion)o).hashCode();
  }
  
  @Override
  public int hashCode() {
    return this.regionInfo.getRegionName().hashCode();
  }
  
  @Override
  public String toString() {
    return this.regionInfo.getRegionNameAsString();
  }

  /** @return Path of region base directory */
  public Path getBaseDir() {
    return this.basedir;
  }

  /**
   * HScanner is an iterator through a bunch of rows in an HRegion.
   */
  private class HScanner implements InternalScanner {
    private InternalScanner[] scanners;
    private List<KeyValue> [] resultSets;
    private RowFilterInterface filter;

    /** Create an HScanner with a handle on many HStores. */
    @SuppressWarnings("unchecked")
    HScanner(final NavigableSet<byte []> columns, byte [] firstRow,
      long timestamp, final Store [] stores, final RowFilterInterface filter)
    throws IOException {
      this.filter = filter;
      this.scanners = new InternalScanner[stores.length];
      try {
        for (int i = 0; i < stores.length; i++) {
          // Only pass relevant columns to each store
          NavigableSet<byte[]> columnSubset =
            new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
          for (byte [] c: columns) {
            if (KeyValue.FAMILY_COMPARATOR.compare(stores[i].storeName, c) == 0) {
              columnSubset.add(c);
            }
          }
          RowFilterInterface f = filter;
          if (f != null) {
            // Need to replicate filters.
            // At least WhileMatchRowFilter will mess up the scan if only
            // one shared across many rows. See HADOOP-2467.
            f = WritableUtils.clone(filter, conf);
          }
          scanners[i] = stores[i].getScanner(timestamp, columnSubset, firstRow, f);
        }
      } catch (IOException e) {
        for (int i = 0; i < this.scanners.length; i++) {
          if (scanners[i] != null) {
            closeScanner(i);
          }
        }
        throw e;
      }

      // Advance to the first key in each store.
      // All results will match the required column-set and scanTime.
      this.resultSets = new List[scanners.length];
      for (int i = 0; i < scanners.length; i++) {
        resultSets[i] = new ArrayList<KeyValue>();
        if(scanners[i] != null && !scanners[i].next(resultSets[i])) {
          closeScanner(i);
        }
      }

      // As we have now successfully completed initialization, increment the
      // activeScanner count.
      activeScannerCount.incrementAndGet();
    }

    @SuppressWarnings("null")
    public boolean next(List<KeyValue> results)
    throws IOException {
      boolean moreToFollow = false;
      boolean filtered = false;
      do {
        // Find the lowest key across all stores.
        KeyValue chosen = null;
        long chosenTimestamp = -1;
        for (int i = 0; i < this.scanners.length; i++) {
          if (this.resultSets[i] == null || this.resultSets[i].isEmpty()) {
            continue;
          }
          KeyValue kv = this.resultSets[i].get(0);
          if (chosen == null ||
               (comparator.compareRows(kv, chosen) < 0) ||
               ((comparator.compareRows(kv, chosen) == 0) &&
                 (kv.getTimestamp() > chosenTimestamp))) {
            chosen = kv;
            chosenTimestamp = chosen.getTimestamp();
          }
        }

        // Store results from each sub-scanner.
        if (chosenTimestamp >= 0) {
          for (int i = 0; i < scanners.length; i++) {
            if (this.resultSets[i] == null || this.resultSets[i].isEmpty()) {
              continue;
            }
            KeyValue kv = this.resultSets[i].get(0);
            if (comparator.compareRows(kv, chosen) == 0) {
              results.addAll(this.resultSets[i]);
              resultSets[i].clear();
              if (!scanners[i].next(resultSets[i])) {
                closeScanner(i);
              }
            }
          }
        }

        moreToFollow = chosenTimestamp >= 0;
        if (results == null || results.size() <= 0) {
          // If we got no results, then there is no more to follow.
          moreToFollow = false;
        }

        filtered = filter == null ? false : filter.filterRow(results);
        if (filter != null && filter.filterAllRemaining()) {
          moreToFollow = false;
        }
        
        if (moreToFollow) {
          if (filter != null) {
            filter.rowProcessed(filtered, chosen.getBuffer(), chosen.getRowOffset(),
              chosen.getRowLength());
          }
          if (filtered) {
            results.clear();
          }
        }
      } while(filtered && moreToFollow);

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
          LOG.warn("Failed closing scanner " + i, e);
        }
      } finally {
        scanners[i] = null;
        // These data members can be null if exception in constructor
        if (resultSets != null) {
          resultSets[i] = null;
        }
      }
    }

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

    public boolean isWildcardScanner() {
      throw new UnsupportedOperationException("Unimplemented on HScanner");
    }

    public boolean isMultipleMatchScanner() {
      throw new UnsupportedOperationException("Unimplemented on HScanner");
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
  public static HRegion createHRegion(final HRegionInfo info, final Path rootDir,
    final HBaseConfiguration conf)
  throws IOException {
    Path tableDir =
      HTableDescriptor.getTableDir(rootDir, info.getTableDesc().getName());
    Path regionDir = HRegion.getRegionDir(tableDir, info.getEncodedName());
    FileSystem fs = FileSystem.get(conf);
    fs.mkdirs(regionDir);
    // Note in historian the creation of new region.
    if (!info.isMetaRegion()) {
      RegionHistorian.getInstance().addRegionCreation(info);
    }
    HRegion region = new HRegion(tableDir,
      new HLog(fs, new Path(regionDir, HREGION_LOGDIR_NAME), conf, null),
      fs, conf, info, null);
    region.initialize(null, null);
    return region;
  }
  
  /**
   * Convenience method to open a HRegion outside of an HRegionServer context.
   * @param info Info for region to be opened.
   * @param rootDir Root directory for HBase instance
   * @param log HLog for region to use. This method will call
   * HLog#setSequenceNumber(long) passing the result of the call to
   * HRegion#getMinSequenceId() to ensure the log id is properly kept
   * up.  HRegionStore does this every time it opens a new region.
   * @param conf
   * @return new HRegion
   * 
   * @throws IOException
   */
  public static HRegion openHRegion(final HRegionInfo info, final Path rootDir,
    final HLog log, final HBaseConfiguration conf)
  throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Opening region: " + info);
    }
    if (info == null) {
      throw new NullPointerException("Passed region info is null");
    }
    HRegion r = new HRegion(
        HTableDescriptor.getTableDir(rootDir, info.getTableDesc().getName()),
        log, FileSystem.get(conf), conf, info, null);
    r.initialize(null, null);
    if (log != null) {
      log.setSequenceNumber(r.getMinSequenceId());
    }
    return r;
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
  public static void addRegionToMETA(HRegion meta, HRegion r) 
  throws IOException {
    meta.checkResources();
    // The row key is the region name
    byte [] row = r.getRegionName();
    Integer lid = meta.obtainRowLock(row);
    try {
      List<KeyValue> edits = new ArrayList<KeyValue>();
      edits.add(new KeyValue(row, COL_REGIONINFO, System.currentTimeMillis(),
        Writables.getBytes(r.getRegionInfo())));
      meta.update(edits);
    } finally {
      meta.releaseRowLock(lid);
    }
  }

  /**
   * Delete a region's meta information from the passed
   * <code>meta</code> region.  Removes content in the 'info' column family.
   * Does not remove region historian info.
   * 
   * @param srvr META server to be updated
   * @param metaRegionName Meta region name
   * @param regionName HRegion to remove from <code>meta</code>
   *
   * @throws IOException
   */
  public static void removeRegionFromMETA(final HRegionInterface srvr,
    final byte [] metaRegionName, final byte [] regionName)
  throws IOException {
    srvr.deleteFamily(metaRegionName, regionName, HConstants.COLUMN_FAMILY,
      HConstants.LATEST_TIMESTAMP, -1L);
  }

  /**
   * Utility method used by HMaster marking regions offlined.
   * @param srvr META server to be updated
   * @param metaRegionName Meta region name
   * @param info HRegion to update in <code>meta</code>
   *
   * @throws IOException
   */
  public static void offlineRegionInMETA(final HRegionInterface srvr,
    final byte [] metaRegionName, final HRegionInfo info)
  throws IOException {
    BatchUpdate b = new BatchUpdate(info.getRegionName());
    info.setOffline(true);
    b.put(COL_REGIONINFO, Writables.getBytes(info));
    b.delete(COL_SERVER);
    b.delete(COL_STARTCODE);
    // If carrying splits, they'll be in place when we show up on new
    // server.
    srvr.batchUpdate(metaRegionName, b, -1L);
  }
  
  /**
   * Clean COL_SERVER and COL_STARTCODE for passed <code>info</code> in
   * <code>.META.</code>
   * @param srvr
   * @param metaRegionName
   * @param info
   * @throws IOException
   */
  public static void cleanRegionInMETA(final HRegionInterface srvr,
    final byte [] metaRegionName, final HRegionInfo info)
  throws IOException {
    BatchUpdate b = new BatchUpdate(info.getRegionName());
    b.delete(COL_SERVER);
    b.delete(COL_STARTCODE);
    // If carrying splits, they'll be in place when we show up on new
    // server.
    srvr.batchUpdate(metaRegionName, b, LATEST_TIMESTAMP);
  }

  /**
   * Deletes all the files for a HRegion
   * 
   * @param fs the file system object
   * @param rootdir qualified path of HBase root directory
   * @param info HRegionInfo for region to be deleted
   * @throws IOException
   */
  public static void deleteRegion(FileSystem fs, Path rootdir, HRegionInfo info)
  throws IOException {
    deleteRegion(fs, HRegion.getRegionDir(rootdir, info));
  }

  private static void deleteRegion(FileSystem fs, Path regiondir)
  throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("DELETING region " + regiondir.toString());
    }
    if (!fs.delete(regiondir, true)) {
      LOG.warn("Failed delete of " + regiondir);
    }
  }

  /**
   * Computes the Path of the HRegion
   * 
   * @param tabledir qualified path for table
   * @param name ENCODED region name
   * @return Path of HRegion directory
   */
  public static Path getRegionDir(final Path tabledir, final int name) {
    return new Path(tabledir, Integer.toString(name));
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
      Integer.toString(info.getEncodedName()));
  }

  /**
   * Determines if the specified row is within the row range specified by the
   * specified HRegionInfo
   *  
   * @param info HRegionInfo that specifies the row range
   * @param row row to be checked
   * @return true if the row is within the range specified by the HRegionInfo
   */
  public static boolean rowIsInRange(HRegionInfo info, final byte [] row) {
    return ((info.getStartKey().length == 0) ||
        (Bytes.compareTo(info.getStartKey(), row) <= 0)) &&
        ((info.getEndKey().length == 0) ||
            (Bytes.compareTo(info.getEndKey(), row) > 0));
  }

  /**
   * Make the directories for a specific column family
   * 
   * @param fs the file system
   * @param tabledir base directory where region will live (usually the table dir)
   * @param hri
   * @param colFamily the column family
   * @throws IOException
   */
  public static void makeColumnFamilyDirs(FileSystem fs, Path tabledir,
    final HRegionInfo hri, byte [] colFamily)
  throws IOException {
    Path dir = Store.getStoreHomedir(tabledir, hri.getEncodedName(), colFamily);
    if (!fs.mkdirs(dir)) {
      LOG.warn("Failed to create " + dir);
    }
  }

  /**
   * Merge two HRegions.  The regions must be adjacent and must not overlap.
   * 
   * @param srcA
   * @param srcB
   * @return new merged HRegion
   * @throws IOException
   */
  public static HRegion mergeAdjacent(final HRegion srcA, final HRegion srcB)
  throws IOException {
    HRegion a = srcA;
    HRegion b = srcB;

    // Make sure that srcA comes first; important for key-ordering during
    // write of the merged file.
    if (srcA.getStartKey() == null) {
      if (srcB.getStartKey() == null) {
        throw new IOException("Cannot merge two regions with null start key");
      }
      // A's start key is null but B's isn't. Assume A comes before B
    } else if ((srcB.getStartKey() == null) ||
      (Bytes.compareTo(srcA.getStartKey(), srcB.getStartKey()) > 0)) {
      a = srcB;
      b = srcA;
    }

    if (!(Bytes.compareTo(a.getEndKey(), b.getStartKey()) == 0)) {
      throw new IOException("Cannot merge non-adjacent regions");
    }
    return merge(a, b);
  }

  /**
   * Merge two regions whether they are adjacent or not.
   * 
   * @param a region a
   * @param b region b
   * @return new merged region
   * @throws IOException
   */
  public static HRegion merge(HRegion a, HRegion b) throws IOException {
    if (!a.getRegionInfo().getTableDesc().getNameAsString().equals(
        b.getRegionInfo().getTableDesc().getNameAsString())) {
      throw new IOException("Regions do not belong to the same table");
    }

    FileSystem fs = a.getFilesystem();

    // Make sure each region's cache is empty
    
    a.flushcache();
    b.flushcache();
    
    // Compact each region so we only have one store file per family
    
    a.compactStores(true);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Files for region: " + a);
      listPaths(fs, a.getRegionDir());
    }
    b.compactStores(true);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Files for region: " + b);
      listPaths(fs, b.getRegionDir());
    }
    
    HBaseConfiguration conf = a.getConf();
    HTableDescriptor tabledesc = a.getTableDesc();
    HLog log = a.getLog();
    Path basedir = a.getBaseDir();
    // Presume both are of same region type -- i.e. both user or catalog 
    // table regions.  This way can use comparator.
    final byte [] startKey = a.comparator.matchingRows(a.getStartKey(), 0,
          a.getStartKey().length,
        EMPTY_BYTE_ARRAY, 0, EMPTY_BYTE_ARRAY.length) ||
      b.comparator.matchingRows(b.getStartKey(), 0, b.getStartKey().length,
        EMPTY_BYTE_ARRAY, 0, EMPTY_BYTE_ARRAY.length)?
        EMPTY_BYTE_ARRAY:
          a.comparator.compareRows(a.getStartKey(), 0, a.getStartKey().length, 
          b.getStartKey(), 0, b.getStartKey().length) <= 0?
        a.getStartKey(): b.getStartKey();
    final byte [] endKey = a.comparator.matchingRows(a.getEndKey(), 0,
        a.getEndKey().length, EMPTY_BYTE_ARRAY, 0, EMPTY_BYTE_ARRAY.length) ||
      a.comparator.matchingRows(b.getEndKey(), 0, b.getEndKey().length,
        EMPTY_BYTE_ARRAY, 0, EMPTY_BYTE_ARRAY.length)?
        EMPTY_BYTE_ARRAY:
        a.comparator.compareRows(a.getEndKey(), 0, a.getEndKey().length,
            b.getEndKey(), 0, b.getEndKey().length) <= 0?
                b.getEndKey(): a.getEndKey();

    HRegionInfo newRegionInfo = new HRegionInfo(tabledesc, startKey, endKey);
    LOG.info("Creating new region " + newRegionInfo.toString());
    int encodedName = newRegionInfo.getEncodedName(); 
    Path newRegionDir = HRegion.getRegionDir(a.getBaseDir(), encodedName);
    if(fs.exists(newRegionDir)) {
      throw new IOException("Cannot merge; target file collision at " +
          newRegionDir);
    }
    fs.mkdirs(newRegionDir);

    LOG.info("starting merge of regions: " + a + " and " + b +
      " into new region " + newRegionInfo.toString() +
        " with start key <" + Bytes.toString(startKey) + "> and end key <" +
        Bytes.toString(endKey) + ">");

    // Move HStoreFiles under new region directory
    
    Map<byte [], List<StoreFile>> byFamily =
      new TreeMap<byte [], List<StoreFile>>(Bytes.BYTES_COMPARATOR);
    byFamily = filesByFamily(byFamily, a.close());
    byFamily = filesByFamily(byFamily, b.close());
    for (Map.Entry<byte [], List<StoreFile>> es : byFamily.entrySet()) {
      byte [] colFamily = es.getKey();
      makeColumnFamilyDirs(fs, basedir, newRegionInfo, colFamily);
      
      // Because we compacted the source regions we should have no more than two
      // HStoreFiles per family and there will be no reference store
      List<StoreFile> srcFiles = es.getValue();
      if (srcFiles.size() == 2) {
        long seqA = srcFiles.get(0).getMaxSequenceId();
        long seqB = srcFiles.get(1).getMaxSequenceId();
        if (seqA == seqB) {
          // Can't have same sequenceid since on open of a store, this is what
          // distingushes the files (see the map of stores how its keyed by
          // sequenceid).
          throw new IOException("Files have same sequenceid: " + seqA);
        }
      }
      for (StoreFile hsf: srcFiles) {
        StoreFile.rename(fs, hsf.getPath(),
          StoreFile.getUniqueFile(fs, Store.getStoreHomedir(basedir,
            newRegionInfo.getEncodedName(), colFamily)));
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Files for new region");
      listPaths(fs, newRegionDir);
    }
    HRegion dstRegion = new HRegion(basedir, log, fs, conf, newRegionInfo, null);
    dstRegion.initialize(null, null);
    dstRegion.compactStores();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Files for new region");
      listPaths(fs, dstRegion.getRegionDir());
    }
    deleteRegion(fs, a.getRegionDir());
    deleteRegion(fs, b.getRegionDir());

    LOG.info("merge completed. New region is " + dstRegion);

    return dstRegion;
  }

  /*
   * Fills a map with a vector of store files keyed by column family. 
   * @param byFamily Map to fill.
   * @param storeFiles Store files to process.
   * @param family
   * @return Returns <code>byFamily</code>
   */
  private static Map<byte [], List<StoreFile>> filesByFamily(
      Map<byte [], List<StoreFile>> byFamily, List<StoreFile> storeFiles) {
    for (StoreFile src: storeFiles) {
      byte [] family = src.getFamily();
      List<StoreFile> v = byFamily.get(family);
      if (v == null) {
        v = new ArrayList<StoreFile>();
        byFamily.put(family, v);
      }
      v.add(src);
    }
    return byFamily;
  }

  /*
   * Method to list files in use by region
   */
  static void listFiles(FileSystem fs, HRegion r) throws IOException {
    listPaths(fs, r.getRegionDir());
  }

  /**
   * @return True if needs a mojor compaction.
   * @throws IOException 
   */
  boolean isMajorCompaction() throws IOException {
    for (Store store: this.stores.values()) {
      if (store.isMajorCompaction()) {
        return true;
      }
    }
    return false;
  }
  
  /*
   * List the files under the specified directory
   * 
   * @param fs
   * @param dir
   * @throws IOException
   */
  private static void listPaths(FileSystem fs, Path dir) throws IOException {
    if (LOG.isDebugEnabled()) {
      FileStatus[] stats = fs.listStatus(dir);
      if (stats == null || stats.length == 0) {
        return;
      }
      for (int i = 0; i < stats.length; i++) {
        String path = stats[i].getPath().toString();
        if (stats[i].isDir()) {
          LOG.debug("d " + path);
          listPaths(fs, stats[i].getPath());
        } else {
          LOG.debug("f " + path + " size=" + stats[i].getLen());
        }
      }
    }
  }

  public long incrementColumnValue(byte[] row, byte[] column, long amount)
  throws IOException {
    checkRow(row);
    checkColumn(column);
    
    Integer lid = obtainRowLock(row);
    splitsAndClosesLock.readLock().lock();
    try {
      KeyValue kv = new KeyValue(row, column);
      long ts = System.currentTimeMillis();
      byte [] value = null;

      Store store = getStore(column);

      List<KeyValue> c;
      // Try the memcache first.
      store.lock.readLock().lock();
      try {
        c = store.memcache.get(kv, 1);
      } finally {
        store.lock.readLock().unlock();
      }
      // Pick the latest value out of List<Cell> c:
      if (c.size() >= 1) {
        // Use the memcache timestamp value.
        LOG.debug("Overwriting the memcache value for " + Bytes.toString(row) +
          "/" + Bytes.toString(column));
        ts = c.get(0).getTimestamp();
        value = c.get(0).getValue();
      }

      if (value == null) {
        // Check the store (including disk) for the previous value.
        c = store.get(kv, 1);
        if (c != null && c.size() == 1) {
          LOG.debug("Using HFile previous value for " + Bytes.toString(row) +
            "/" + Bytes.toString(column));
          value = c.get(0).getValue();
        } else if (c != null && c.size() > 1) {
          throw new DoNotRetryIOException("more than 1 value returned in " +
            "incrementColumnValue from Store");
        }
      }
      
      if (value == null) {
        // Doesn't exist
        LOG.debug("Creating new counter value for " + Bytes.toString(row) +
          "/"+ Bytes.toString(column));
        value = Bytes.toBytes(amount);
      } else {
        if (amount == 0) return Bytes.toLong(value);
        value = Bytes.incrementBytes(value, amount);
      }

      BatchUpdate b = new BatchUpdate(row, ts);
      b.put(column, value);
      batchUpdate(b, lid, true);
      return Bytes.toLong(value);
    } finally {
      splitsAndClosesLock.readLock().unlock();
      releaseRowLock(lid);
    }
  }
}
