 /**
 * Copyright 2009 The Apache Software Foundation
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
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.DroppedSnapshotException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionHistorian;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.Reference.Range;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;

/**
 * HRegion stores data for a certain region of a table.  It stores all columns
 * for each row. A given table consists of one or more HRegions.
 *
 * <p>We maintain multiple HStores for a single HRegion.
 * 
 * <p>An Store is a set of rows with some column data; together,
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
 * <p>It consists of at least one Store.  The number of Stores should be
 * configurable, so that data which is accessed together is stored in the same
 * Store.  Right now, we approximate that by building a single Store for 
 * each column family.  (This config info will be communicated via the 
 * tabledesc.)
 * 
 * <p>The HTableDescriptor contains metainfo about the HRegion's table.
 * regionName is a unique identifier for this HRegion. (startKey, endKey]
 * defines the keyspace for this HRegion.
 */
public class HRegion implements HConstants, HeapSize { // , Writable{
  static final Log LOG = LogFactory.getLog(HRegion.class);
  static final String SPLITDIR = "splits";
  static final String MERGEDIR = "merges";
  final AtomicBoolean closed = new AtomicBoolean(false);
  /* Closing can take some time; use the closing flag if there is stuff we don't 
   * want to do while in closing state; e.g. like offer this region up to the 
   * master as a region to close if the carrying regionserver is overloaded.
   * Once set, it is never cleared.
   */
  final AtomicBoolean closing = new AtomicBoolean(false);
  private final RegionHistorian historian;

  //////////////////////////////////////////////////////////////////////////////
  // Members
  //////////////////////////////////////////////////////////////////////////////

  private final Map<Integer, byte []> locksToRows =
    new ConcurrentHashMap<Integer, byte []>();
  protected final Map<byte [], Store> stores =
    new ConcurrentSkipListMap<byte [], Store>(Bytes.BYTES_RAWCOMPARATOR);
  
  //These variable are just used for getting data out of the region, to test on
  //client side
  // private int numStores = 0;
  // private int [] storeSize = null;
  // private byte [] name = null;
  
  final AtomicLong memstoreSize = new AtomicLong(0);

  // This is the table subdirectory.
  final Path basedir;
  final HLog log;
  final FileSystem fs;
  final HBaseConfiguration conf;
  final HRegionInfo regionInfo;
  final Path regiondir;
  private final Path regionCompactionDir;
  KeyValue.KVComparator comparator;

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
    // Set while a memstore flush is happening.
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

  final int memstoreFlushSize;
  private volatile long lastFlushTime;
  final FlushRequester flushListener;
  private final int blockingMemStoreSize;
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
  private boolean splitRequest;
  
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
   * Should only be used for testing purposes
   */
  public HRegion(){
    this.basedir = null;
    this.blockingMemStoreSize = 0;
    this.conf = null;
    this.flushListener = null;
    this.fs = null;
    this.historian = null;
    this.memstoreFlushSize = 0;
    this.log = null;
    this.regionCompactionDir = null;
    this.regiondir = null;
    this.regionInfo = null;
    this.threadWakeFrequency = 0L;
  }
  
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
   */
  public HRegion(Path basedir, HLog log, FileSystem fs, HBaseConfiguration conf, 
      HRegionInfo regionInfo, FlushRequester flushListener) {
    this.basedir = basedir;
    this.comparator = regionInfo.getComparator();
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
      LOG.debug("Opening region " + this + ", encoded=" +
        this.regionInfo.getEncodedName());
    }
    this.regionCompactionDir =
      new Path(getCompactionDir(basedir), encodedNameStr);
    int flushSize = regionInfo.getTableDesc().getMemStoreFlushSize();
    if (flushSize == HTableDescriptor.DEFAULT_MEMSTORE_FLUSH_SIZE) {
      flushSize = conf.getInt("hbase.hregion.memstore.flush.size",
                      HTableDescriptor.DEFAULT_MEMSTORE_FLUSH_SIZE);
    }
    this.memstoreFlushSize = flushSize;
    this.blockingMemStoreSize = this.memstoreFlushSize *
      conf.getInt("hbase.hregion.memstore.block.multiplier", 1);
  }

  /**
   * Initialize this region and get it ready to roll.
   * Called after construction.
   * 
   * @param initialFiles
   * @param reporter
   * @throws IOException
   */
  public void initialize(Path initialFiles, final Progressable reporter)
  throws IOException {
    Path oldLogFile = new Path(regiondir, HREGION_OLDLOGFILE_NAME);

    // Move prefab HStore files into place (if any).  This picks up split files
    // and any merges from splits and merges dirs.
    if (initialFiles != null && fs.exists(initialFiles)) {
      fs.rename(initialFiles, this.regiondir);
    }

    // Write HRI to a file in case we need to recover .META.
    checkRegioninfoOnFilesystem();

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
      " available; sequence id is " + this.minSequenceId);
  }

  /**
   * @return True if this region has references.
   */
  boolean hasReferences() {
    for (Map.Entry<byte [], Store> e: this.stores.entrySet()) {
      for (Map.Entry<Long, StoreFile> ee:
          e.getValue().getStorefiles().entrySet()) {
        // Found a reference, return.
        if (ee.getValue().isReference()) return true;
      }
    }
    return false;
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
  public List<StoreFile> close(final boolean abort) throws IOException {
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

  /** @return region name as string for logging */
  public String getRegionNameAsString() {
    return this.regionInfo.getRegionNameAsString();
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
    prepareToSplit();
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
  
  protected void prepareToSplit() {
    // nothing
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
    if (this.closing.get() || this.closed.get()) {
      LOG.debug("Skipping compaction on " + this + " because closing/closed");
      return null;
    }
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
        LOG.info("Starting" + (majorCompaction? " major " : " ") + 
            "compaction on region " + this);
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
          LOG.debug("NOT flushing memstore for region " + this +
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
   * memstore, all of which have also been written to the log. We need to
   * write those updates in the memstore out to disk, while being able to
   * process reads/writes as much as possible during the flush operation. Also,
   * the log has to state clearly the point in time at which the memstore was
   * flushed. (That way, during recovery, we know when we can rely on the
   * on-disk flushed structures and when we have to recover the memstore from
   * the log.)
   * 
   * <p>So, we have a three-step process:
   * 
   * <ul><li>A. Flush the memstore to the on-disk stores, noting the current
   * sequence ID for the log.<li>
   * 
   * <li>B. Write a FLUSHCACHE-COMPLETE message to the log, using the sequence
   * ID that was current at the time of memstore-flush.</li>
   * 
   * <li>C. Get rid of the memstore structures that are now redundant, as
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
    if (this.memstoreSize.get() <= 0) {
      return false;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Started memstore flush for region " + this +
        ". Current region memstore size " +
          StringUtils.humanReadableInt(this.memstoreSize.get()));
    }

    // Stop updates while we snapshot the memstore of all stores. We only have
    // to do this for a moment.  Its quick.  The subsequent sequence id that
    // goes into the HLog after we've flushed all these snapshots also goes
    // into the info file that sits beside the flushed files.
    // We also set the memstore size to zero here before we allow updates
    // again so its value will represent the size of the updates received
    // during the flush
    long sequenceId = -1L;
    long completeSequenceId = -1L;
    this.updatesLock.writeLock().lock();
    // Get current size of memstores.
    final long currentMemStoreSize = this.memstoreSize.get();
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
    // restart so hlog content can be replayed and put back into the memstore.
    // Otherwise, the snapshot content while backed up in the hlog, it will not
    // be part of the current running servers state.
    boolean compactionRequested = false;
    try {
      // A.  Flush memstore to all the HStores.
      // Keep running vector of all store files that includes both old and the
      // just-made new flush store file.
      for (Store hstore: stores.values()) {
        boolean needsCompaction = hstore.flushCache(completeSequenceId);
        if (needsCompaction) {
          compactionRequested = true;
        }
      }
      // Set down the memstore size by amount of flush.
      this.memstoreSize.addAndGet(-currentMemStoreSize);
    } catch (Throwable t) {
      // An exception here means that the snapshot was not persisted.
      // The hlog needs to be replayed so its content is restored to memstore.
      // Currently, only a server restart will do this.
      // We used to only catch IOEs but its possible that we'd get other
      // exceptions -- e.g. HBASE-659 was about an NPE -- so now we catch
      // all and sundry.
      this.log.abortCacheFlush();
      DroppedSnapshotException dse = new DroppedSnapshotException("region: " +
          Bytes.toStringBinary(getRegionName()));
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

    // C. Finally notify anyone waiting on memstore to clear:
    // e.g. checkResources().
    synchronized (this) {
      notifyAll();
    }
    
    if (LOG.isDebugEnabled()) {
      long now = System.currentTimeMillis();
      String timeTaken = StringUtils.formatTimeDiff(now, startTime);
      LOG.debug("Finished memstore flush of ~" +
        StringUtils.humanReadableInt(currentMemStoreSize) + " for region " +
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
   * Return all the data for the row that matches <i>row</i> exactly, 
   * or the one that immediately preceeds it, at or immediately before 
   * <i>ts</i>.
   * 
   * @param row row key
   * @return map of values
   * @throws IOException
   */
  Result getClosestRowBefore(final byte [] row)
  throws IOException{
    return getClosestRowBefore(row, HConstants.CATALOG_FAMILY);
  }

  /**
   * Return all the data for the row that matches <i>row</i> exactly, 
   * or the one that immediately preceeds it, at or immediately before 
   * <i>ts</i>.
   * 
   * @param row row key
   * @param family
   * @return map of values
   * @throws IOException
   */
  public Result getClosestRowBefore(final byte [] row, final byte [] family)
  throws IOException {
    // look across all the HStores for this region and determine what the
    // closest key is across all column families, since the data may be sparse
    KeyValue key = null;
    checkRow(row);
    splitsAndClosesLock.readLock().lock();
    try {
      Store store = getStore(family);
      KeyValue kv = new KeyValue(row, HConstants.LATEST_TIMESTAMP);
      // get the closest key. (HStore.getRowKeyAtOrBefore can return null)
      key = store.getRowKeyAtOrBefore(kv);
      if (key == null) {
        return null;
      }
      // This will get all results for this store.  TODO: Do we need to do this?
      Get get = new Get(key.getRow());
      List<KeyValue> results = new ArrayList<KeyValue>();
      store.get(get, null, results);
      return new Result(results);
    } finally {
      splitsAndClosesLock.readLock().unlock();
    }
  }

  /**
   * Return an iterator that scans over the HRegion, returning the indicated 
   * columns and rows specified by the {@link Scan}.
   * <p>
   * This Iterator must be closed by the caller.
   *
   * @param scan configured {@link Scan}
   * @return InternalScanner
   * @throws IOException
   */
  public InternalScanner getScanner(Scan scan)
  throws IOException {
   return getScanner(scan, null);
  }
  
  protected InternalScanner getScanner(Scan scan, List<KeyValueScanner> additionalScanners) throws IOException {
    newScannerLock.readLock().lock();
    try {
      if (this.closed.get()) {
        throw new IOException("Region " + this + " closed");
      }
      // Verify families are all valid
      if(scan.hasFamilies()) {
        for(byte [] family : scan.getFamilyMap().keySet()) {
          checkFamily(family);
        }
      } else { // Adding all families to scanner
        for(byte[] family: regionInfo.getTableDesc().getFamiliesKeys()){
          scan.addFamily(family);
        }
      }
      return new RegionScanner(scan, additionalScanners);
      
    } finally {
      newScannerLock.readLock().unlock();
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // set() methods for client use.
  //////////////////////////////////////////////////////////////////////////////
  /**
   * @param delete
   * @param lockid
   * @param writeToWAL
   * @throws IOException
   */
  public void delete(Delete delete, Integer lockid, boolean writeToWAL)
  throws IOException {
    checkReadOnly();
    checkResources();
    splitsAndClosesLock.readLock().lock();
    Integer lid = null;
    try {
      byte [] row = delete.getRow();
      // If we did not pass an existing row lock, obtain a new one
      lid = getLock(lockid, row);

      //Check to see if this is a deleteRow insert
      if(delete.getFamilyMap().isEmpty()){
        for(byte [] family : regionInfo.getTableDesc().getFamiliesKeys()){
          // Don't eat the timestamp
          delete.deleteFamily(family, delete.getTimeStamp());
        }
      } else {
        for(byte [] family : delete.getFamilyMap().keySet()) {
          if(family == null) {
            throw new NoSuchColumnFamilyException("Empty family is invalid");
          }
          checkFamily(family);
        }
      }
      
      for(Map.Entry<byte[], List<KeyValue>> e: delete.getFamilyMap().entrySet()) {
        byte [] family = e.getKey();
        delete(family, e.getValue(), writeToWAL);
      }
    } finally {
      if(lockid == null) releaseRowLock(lid);
      splitsAndClosesLock.readLock().unlock();
    }
  }
  
  
  /**
   * @param family
   * @param kvs
   * @param writeToWAL
   * @throws IOException
   */
  public void delete(byte [] family, List<KeyValue> kvs, boolean writeToWAL)
  throws IOException {
    long now = System.currentTimeMillis();
    byte [] byteNow = Bytes.toBytes(now);
    boolean flush = false;
    this.updatesLock.readLock().lock();

    try {
      if (writeToWAL) {
        this.log.append(regionInfo.getRegionName(),
          regionInfo.getTableDesc().getName(), kvs,
          (regionInfo.isMetaRegion() || regionInfo.isRootRegion()), now);
      }
      long size = 0;
      Store store = getStore(family);
      for (KeyValue kv: kvs) {
        // Check if time is LATEST, change to time of most recent addition if so
        // This is expensive.
        if (kv.isLatestTimestamp() && kv.isDeleteType()) {
          List<KeyValue> result = new ArrayList<KeyValue>(1);
          Get g = new Get(kv.getRow());
          NavigableSet<byte []> qualifiers =
            new TreeSet<byte []>(Bytes.BYTES_COMPARATOR);
          byte [] q = kv.getQualifier();
          if (q != null && q.length > 0) qualifiers.add(kv.getQualifier());
          get(store, g, qualifiers, result);
          if (result.isEmpty()) {
            // Nothing to delete
            continue;
          }
          if (result.size() > 1) {
            throw new RuntimeException("Unexpected size: " + result.size());
          }
          KeyValue getkv = result.get(0);
          Bytes.putBytes(kv.getBuffer(), kv.getTimestampOffset(),
            getkv.getBuffer(), getkv.getTimestampOffset(), Bytes.SIZEOF_LONG);
        } else {
          kv.updateLatestStamp(byteNow);
        }

        size = this.memstoreSize.addAndGet(store.delete(kv));
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
  
  /**
   * @param put
   * @throws IOException
   */
  public void put(Put put) throws IOException {
    this.put(put, null, put.getWriteToWAL());
  }
  
  /**
   * @param put
   * @param writeToWAL
   * @throws IOException
   */
  public void put(Put put, boolean writeToWAL) throws IOException {
    this.put(put, null, writeToWAL);
  }

  /**
   * @param put
   * @param lockid
   * @throws IOException
   */
  public void put(Put put, Integer lockid) throws IOException {
    this.put(put, lockid, put.getWriteToWAL());
  }

  /**
   * @param put
   * @param lockid
   * @param writeToWAL
   * @throws IOException
   */
  public void put(Put put, Integer lockid, boolean writeToWAL)
  throws IOException {
    checkReadOnly();

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
      byte [] row = put.getRow();
      // If we did not pass an existing row lock, obtain a new one
      Integer lid = getLock(lockid, row);
      byte [] now = Bytes.toBytes(System.currentTimeMillis());
      try {
        for (Map.Entry<byte[], List<KeyValue>> entry:
            put.getFamilyMap().entrySet()) {
          byte [] family = entry.getKey();
          checkFamily(family);
          List<KeyValue> puts = entry.getValue();
          if (updateKeys(puts, now)) {
            put(family, puts, writeToWAL);
          }
        }
      } finally {
        if(lockid == null) releaseRowLock(lid);
      }
    } finally {
      splitsAndClosesLock.readLock().unlock();
    }
  }

  
  //TODO, Think that gets/puts and deletes should be refactored a bit so that 
  //the getting of the lock happens before, so that you would just pass it into
  //the methods. So in the case of checkAndPut you could just do lockRow, 
  //get, put, unlockRow or something
  /**
   * 
   * @param row
   * @param family
   * @param qualifier
   * @param expectedValue
   * @param put
   * @param lockId
   * @param writeToWAL
   * @throws IOException
   * @return true if the new put was execute, false otherwise
   */
  public boolean checkAndPut(byte [] row, byte [] family, byte [] qualifier,
      byte [] expectedValue, Put put, Integer lockId, boolean writeToWAL) 
  throws IOException{
    checkReadOnly();
    //TODO, add check for value length or maybe even better move this to the 
    //client if this becomes a global setting
    checkResources();
    splitsAndClosesLock.readLock().lock();
    try {
      Get get = new Get(row, put.getRowLock());
      checkFamily(family);
      get.addColumn(family, qualifier);

      byte [] now = Bytes.toBytes(System.currentTimeMillis());

      // Lock row
      Integer lid = getLock(lockId, get.getRow()); 
      List<KeyValue> result = new ArrayList<KeyValue>();
      try {
        //Getting data
        for(Map.Entry<byte[],NavigableSet<byte[]>> entry:
          get.getFamilyMap().entrySet()) {
          get(this.stores.get(entry.getKey()), get, entry.getValue(), result);
        }
        boolean matches = false;
        if (result.size() == 0 && expectedValue.length == 0) {
          matches = true;
        } else if(result.size() == 1) {
          //Compare the expected value with the actual value
          byte [] actualValue = result.get(0).getValue();
          matches = Bytes.equals(expectedValue, actualValue);
        }
        //If matches put the new put
        if(matches) {
          for(Map.Entry<byte[], List<KeyValue>> entry :
            put.getFamilyMap().entrySet()) {
            byte [] fam = entry.getKey();
            checkFamily(fam);
            List<KeyValue> puts = entry.getValue();
            if(updateKeys(puts, now)) {
              put(fam, puts, writeToWAL);
            }
          }
          return true;  
        }
        return false;
      } finally {
        if(lockId == null) releaseRowLock(lid);
      }
    } finally {
      splitsAndClosesLock.readLock().unlock();
    }    
  }
      
  
  /**
   * Checks if any stamps is Long.MAX_VALUE.  If so, sets them to now.
   * <p>
   * This acts to replace LATEST_TIMESTAMP with now.
   * @param keys
   * @param now
   * @return <code>true</code> when updating the time stamp completed.
   */
  private boolean updateKeys(List<KeyValue> keys, byte [] now) {
    if(keys == null || keys.isEmpty()) {
      return false;
    }
    for(KeyValue key : keys) {
      if(key.getTimestamp() == HConstants.LATEST_TIMESTAMP) {
        key.updateLatestStamp(now);
      }
    }
    return true;
  }
  

//  /*
//   * Utility method to verify values length. 
//   * @param batchUpdate The update to verify
//   * @throws IOException Thrown if a value is too long
//   */
//  private void validateValuesLength(Put put)
//  throws IOException {
//    Map<byte[], List<KeyValue>> families = put.getFamilyMap();
//    for(Map.Entry<byte[], List<KeyValue>> entry : families.entrySet()) {
//      HColumnDescriptor hcd = 
//        this.regionInfo.getTableDesc().getFamily(entry.getKey());
//      int maxLen = hcd.getMaxValueLength();
//      for(KeyValue kv : entry.getValue()) {
//        if(kv.getValueLength() > maxLen) {
//          throw new ValueOverMaxLengthException("Value in column "
//            + Bytes.toString(kv.getColumn()) + " is too long. "
//            + kv.getValueLength() + " > " + maxLen);
//        }
//      }
//    }
//  }

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
    while (this.memstoreSize.get() > this.blockingMemStoreSize) {
      requestFlush();
      if (!blocked) {
        LOG.info("Blocking updates for '" + Thread.currentThread().getName() +
          "' on region " + Bytes.toStringBinary(getRegionName()) +
          ": memstore size " +
          StringUtils.humanReadableInt(this.memstoreSize.get()) +
          " is >= than blocking " +
          StringUtils.humanReadableInt(this.blockingMemStoreSize) + " size");
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
   * @throws IOException Throws exception if region is in read-only mode.
   */
  protected void checkReadOnly() throws IOException {
    if (this.writestate.isReadOnly()) {
      throw new IOException("region is read only");
    }
  }

  /** 
   * Add updates first to the hlog and then add values to memstore.
   * Warning: Assumption is caller has lock on passed in row.
   * @param edits Cell updates by column
   * @praram now
   * @throws IOException
   */
  private void put(final byte [] family, final List<KeyValue> edits)
  throws IOException {
    this.put(family, edits, true);
  }

  /** 
   * Add updates first to the hlog (if writeToWal) and then add values to memstore.
   * Warning: Assumption is caller has lock on passed in row.
   * @param family
   * @param edits
   * @param writeToWAL if true, then we should write to the log
   * @throws IOException
   */
  private void put(final byte [] family, final List<KeyValue> edits, 
      boolean writeToWAL) throws IOException {
    if (edits == null || edits.isEmpty()) {
      return;
    }
    boolean flush = false;
    this.updatesLock.readLock().lock();
    try {
      if (writeToWAL) {
        long now = System.currentTimeMillis();
        this.log.append(regionInfo.getRegionName(),
          regionInfo.getTableDesc().getName(), edits,
          (regionInfo.isMetaRegion() || regionInfo.isRootRegion()), now);
      }
      long size = 0;
      Store store = getStore(family);
      for (KeyValue kv: edits) {
        size = this.memstoreSize.addAndGet(store.add(kv));
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
    return size > this.memstoreFlushSize;
  }

  // Do any reconstruction needed from the log
  protected void doReconstructionLog(Path oldLogFile, long minSeqId, long maxSeqId,
    Progressable reporter)
  throws UnsupportedEncodingException, IOException {
    // Nothing to do (Replaying is done in HStores)
    // Used by subclasses; e.g. THBase.
  }

  protected Store instantiateHStore(Path baseDir, 
    HColumnDescriptor c, Path oldLogFile, Progressable reporter)
  throws IOException {
    return new Store(baseDir, this, c, this.fs, oldLogFile,
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
          Bytes.toStringBinary(regionInfo.getStartKey()) + "', getEndKey()='" +
          Bytes.toStringBinary(regionInfo.getEndKey()) + "', row='" +
          Bytes.toStringBinary(row) + "'");
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
  public Integer obtainRowLock(final byte [] row) throws IOException {
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
   * @param lockid  The lock ID to release.
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
   * RegionScanner is an iterator through a bunch of rows in an HRegion.
   * <p>
   * It is used to combine scanners from multiple Stores (aka column families).
   */
  class RegionScanner implements InternalScanner {
    private final KeyValueHeap storeHeap;
    private final byte [] stopRow;
    private Filter filter;
    private RowFilterInterface oldFilter;
    private List<KeyValue> results = new ArrayList<KeyValue>();

    RegionScanner(Scan scan, List<KeyValueScanner> additionalScanners) {
      this.filter = scan.getFilter();
      this.oldFilter = scan.getOldFilter();
      if (Bytes.equals(scan.getStopRow(), HConstants.EMPTY_END_ROW)) {
        this.stopRow = null;
      } else {
        this.stopRow = scan.getStopRow();
      }
      
      List<KeyValueScanner> scanners = new ArrayList<KeyValueScanner>();
      if (additionalScanners != null) {
        scanners.addAll(additionalScanners);
      }
      for (Map.Entry<byte[], NavigableSet<byte[]>> entry : 
          scan.getFamilyMap().entrySet()) {
        Store store = stores.get(entry.getKey());
        scanners.add(store.getScanner(scan, entry.getValue()));
      }
      this.storeHeap = 
        new KeyValueHeap(scanners.toArray(new KeyValueScanner[0]), comparator);
    }
    
    RegionScanner(Scan scan) {
      this(scan, null);
    }

    private void resetFilters() {
      if (filter != null) {
        filter.reset();
      }
      if (oldFilter != null) {
        oldFilter.reset();
      }
    }

    /**
     * Get the next row of results from this region.
     * @param results list to append results to
     * @return true if there are more rows, false if scanner is done
     * @throws NotServerRegionException If this region is closing or closed
     */
    @Override
    public boolean next(List<KeyValue> outResults) throws IOException {
      if (closing.get() || closed.get()) {
        close();
        throw new NotServingRegionException(regionInfo.getRegionNameAsString() +
          " is closing=" + closing.get() + " or closed=" + closed.get());
      }
      results.clear();
      boolean returnResult = nextInternal();
      if (!returnResult && filter != null && filter.filterRow()) {
        results.clear();
      }
      outResults.addAll(results);
      resetFilters();
      if(filter != null && filter.filterAllRemaining()) {
        return false;
      }
      return returnResult;
    }

    private boolean nextInternal() throws IOException {
      // This method should probably be reorganized a bit... has gotten messy
      KeyValue kv;
      byte[] currentRow = null;
      boolean filterCurrentRow = false;
      while (true) {
        kv = this.storeHeap.peek();
        if (kv == null) {
          return false;
        }
        byte [] row = kv.getRow();
        if (filterCurrentRow && Bytes.equals(currentRow, row)) {
          // filter all columns until row changes
          this.storeHeap.next(results);
          results.clear();
          continue;
        }
        // see if current row should be filtered based on row key
        if ((filter != null && filter.filterRowKey(row, 0, row.length)) ||
            (oldFilter != null && oldFilter.filterRowKey(row, 0, row.length))) {
          if(!results.isEmpty() && !Bytes.equals(currentRow, row)) {
            return true;
          }
          this.storeHeap.next(results);
          results.clear();
          resetFilters();
          filterCurrentRow = true;
          currentRow = row;
          continue;
        }
        if(!Bytes.equals(currentRow, row)) {
          // Continue on the next row:
          currentRow = row;
          filterCurrentRow = false;
          // See if we passed stopRow
          if(stopRow != null &&
              comparator.compareRows(stopRow, 0, stopRow.length, 
                  currentRow, 0, currentRow.length) <= 0) {
            return false;
          }
          // if there are _no_ results or current row should be filtered
          if (results.isEmpty() || filter != null && filter.filterRow()) {
            // make sure results is empty
            results.clear();
            resetFilters();
            continue;
          }
          return true;
        }
        this.storeHeap.next(results);
      }
    }

    public void close() {
      storeHeap.close();
    }

    /**
     * 
     * @param scanner to be closed
     */
    public void close(KeyValueScanner scanner) {
      try {
        scanner.close();
      } catch(NullPointerException npe) {}
    }
    
    /**
     * @return the current storeHeap
     */
    public KeyValueHeap getStoreHeap() {
      return this.storeHeap;
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
      edits.add(new KeyValue(row, CATALOG_FAMILY, REGIONINFO_QUALIFIER,
          System.currentTimeMillis(), Writables.getBytes(r.getRegionInfo())));
      meta.put(HConstants.CATALOG_FAMILY, edits);
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
    Delete delete = new Delete(regionName);
    delete.deleteFamily(HConstants.CATALOG_FAMILY);
    srvr.delete(metaRegionName, delete);
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
    // Puts and Deletes used to be "atomic" here.  We can use row locks if
    // we need to keep that property, or we can expand Puts and Deletes to
    // allow them to be committed at once.
    byte [] row = info.getRegionName();
    Put put = new Put(row);
    info.setOffline(true);
    put.add(CATALOG_FAMILY, REGIONINFO_QUALIFIER, Writables.getBytes(info));
    srvr.put(metaRegionName, put);
    Delete del = new Delete(row);
    del.deleteColumns(CATALOG_FAMILY, SERVER_QUALIFIER);
    del.deleteColumns(CATALOG_FAMILY, STARTCODE_QUALIFIER);
    srvr.delete(metaRegionName, del);
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
    Delete del = new Delete(info.getRegionName());
    del.deleteColumns(CATALOG_FAMILY, SERVER_QUALIFIER);
    del.deleteColumns(CATALOG_FAMILY, STARTCODE_QUALIFIER);
    srvr.delete(metaRegionName, del);
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

  
  //
  // HBASE-880
  //
  /**
   * @param get
   * @param lockid
   * @return result
   * @throws IOException
   */
  public Result get(final Get get, final Integer lockid) throws IOException {
    // Verify families are all valid
    if (get.hasFamilies()) {
      for (byte [] family: get.familySet()) {
        checkFamily(family);
      }
    } else { // Adding all families to scanner
      for (byte[] family: regionInfo.getTableDesc().getFamiliesKeys()) {
        get.addFamily(family);
      }
    }
    // Lock row
    Integer lid = getLock(lockid, get.getRow()); 
    List<KeyValue> result = new ArrayList<KeyValue>();
    try {
      for (Map.Entry<byte[],NavigableSet<byte[]>> entry:
          get.getFamilyMap().entrySet()) {
        get(this.stores.get(entry.getKey()), get, entry.getValue(), result);
      }
    } finally {
      if(lockid == null) releaseRowLock(lid);
    }
    return new Result(result);
  }

  private void get(final Store store, final Get get,
    final NavigableSet<byte []> qualifiers, List<KeyValue> result)
  throws IOException {
    store.get(get, qualifiers, result);
  }

  /**
   * 
   * @param row
   * @param family
   * @param qualifier
   * @param amount
   * @return The new value.
   * @throws IOException
   */
  public long incrementColumnValue(byte [] row, byte [] family,
      byte [] qualifier, long amount, boolean writeToWAL)
  throws IOException {
    checkRow(row);
    boolean flush = false;
    // Lock row
    Integer lid = obtainRowLock(row);
    long result = 0L;
    try {
      Store store = stores.get(family);
      // Determine what to do and perform increment on returned KV, no insertion 
      Store.ICVResult vas =
        store.incrementColumnValue(row, family, qualifier, amount);
      // Write incremented value to WAL before inserting
      if (writeToWAL) {
        long now = System.currentTimeMillis();
        List<KeyValue> edits = new ArrayList<KeyValue>(1);
        edits.add(vas.kv);
        this.log.append(regionInfo.getRegionName(),
          regionInfo.getTableDesc().getName(), edits,
          (regionInfo.isMetaRegion() || regionInfo.isRootRegion()), now);
      }
      // Insert to the Store
      store.add(vas.kv);
      result = vas.value;
      long size = this.memstoreSize.addAndGet(vas.sizeAdded);
      flush = isFlushSize(size);
    } finally {
      releaseRowLock(lid);
    }

    if (flush) {
      // Request a cache flush.  Do it outside update lock.
      requestFlush();
    }

    return result;
  }
    
  
  //
  // New HBASE-880 Helpers
  //
  
  private void checkFamily(final byte [] family) 
  throws NoSuchColumnFamilyException {
    if(!regionInfo.getTableDesc().hasFamily(family)) {
      throw new NoSuchColumnFamilyException("Column family " +
          Bytes.toString(family) + " does not exist in region " + this
            + " in table " + regionInfo.getTableDesc());
    }
  }

  public static final long FIXED_OVERHEAD = ClassSize.align(
      (3 * Bytes.SIZEOF_LONG) + (2 * Bytes.SIZEOF_INT) + Bytes.SIZEOF_BOOLEAN +
      (20 * ClassSize.REFERENCE) + ClassSize.OBJECT);
  
  public static final long DEEP_OVERHEAD = ClassSize.align(FIXED_OVERHEAD +
      ClassSize.OBJECT + (2 * ClassSize.ATOMIC_BOOLEAN) + 
      ClassSize.ATOMIC_LONG + ClassSize.ATOMIC_INTEGER +
      ClassSize.CONCURRENT_HASHMAP + 
      (16 * ClassSize.CONCURRENT_HASHMAP_ENTRY) + 
      (16 * ClassSize.CONCURRENT_HASHMAP_SEGMENT) +
      ClassSize.CONCURRENT_SKIPLISTMAP + ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY +
      RegionHistorian.FIXED_OVERHEAD + HLog.FIXED_OVERHEAD +
      ClassSize.align(ClassSize.OBJECT + (5 * Bytes.SIZEOF_BOOLEAN)) +
      (3 * ClassSize.REENTRANT_LOCK));
  
  @Override
  public long heapSize() {
    long heapSize = DEEP_OVERHEAD;
    for(Store store : this.stores.values()) {
      heapSize += store.heapSize();
    }
    return heapSize;
  }

  /*
   * This method calls System.exit.
   * @param message Message to print out.  May be null.
   */
  private static void printUsageAndExit(final String message) {
    if (message != null && message.length() > 0) System.out.println(message);
    System.out.println("Usage: HRegion CATLALOG_TABLE_DIR [major_compact]");
    System.out.println("Options:");
    System.out.println(" major_compact  Pass this option to major compact " +
      "passed region.");
    System.out.println("Default outputs scan of passed region.");
    System.exit(1);
  }

  /*
   * Process table.
   * Do major compaction or list content.
   * @param fs
   * @param p
   * @param log
   * @param c
   * @param majorCompact
   * @throws IOException
   */
  private static void processTable(final FileSystem fs, final Path p,
      final HLog log, final HBaseConfiguration c,
      final boolean majorCompact)
  throws IOException {
    HRegion region = null;
    String rootStr = Bytes.toString(HConstants.ROOT_TABLE_NAME);
    String metaStr = Bytes.toString(HConstants.META_TABLE_NAME);
    // Currently expects tables have one region only.
    if (p.getName().startsWith(rootStr)) {
      region = new HRegion(p, log, fs, c, HRegionInfo.ROOT_REGIONINFO, null);
    } else if (p.getName().startsWith(metaStr)) {
      region = new HRegion(p, log, fs, c, HRegionInfo.FIRST_META_REGIONINFO,
          null);
    } else {
      throw new IOException("Not a known catalog table: " + p.toString());
    }
    try {
      region.initialize(null, null);
      if (majorCompact) {
        region.compactStores(true);
      } else {
        // Default behavior
        Scan scan = new Scan();
        // scan.addFamily(HConstants.CATALOG_FAMILY);
        InternalScanner scanner = region.getScanner(scan);
        try {
          List<KeyValue> kvs = new ArrayList<KeyValue>();
          boolean done = false;
          do {
            kvs.clear();
            done = scanner.next(kvs);
            if (kvs.size() > 0) LOG.info(kvs);
          } while (done);
        } finally {
          scanner.close();
        }
        // System.out.println(region.getClosestRowBefore(Bytes.toBytes("GeneratedCSVContent2,E3652782193BC8D66A0BA1629D0FAAAB,9993372036854775807")));
      }
    } finally {
      region.close();
    }
  }

  /**
   * For internal use in forcing splits ahead of file size limit.
   * @param b
   * @return previous value
   */
  public boolean shouldSplit(boolean b) {
    boolean old = this.splitRequest;
    this.splitRequest = b;
    return old;
  }

  /**
   * Facility for dumping and compacting catalog tables.
   * Only does catalog tables since these are only tables we for sure know
   * schema on.  For usage run:
   * <pre>
   *   ./bin/hbase org.apache.hadoop.hbase.regionserver.HRegion
   * </pre>
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      printUsageAndExit(null);
    }
    boolean majorCompact = false;
    if (args.length > 1) {
      if (!args[1].toLowerCase().startsWith("major")) {
        printUsageAndExit("ERROR: Unrecognized option <" + args[1] + ">");
      }
      majorCompact = true;
    }
    Path tableDir  = new Path(args[0]);
    HBaseConfiguration c = new HBaseConfiguration();
    FileSystem fs = FileSystem.get(c);
    Path logdir = new Path(c.get("hbase.tmp.dir"),
      "hlog" + tableDir.getName() + System.currentTimeMillis());
    HLog log = new HLog(fs, logdir, c, null);
    try {
      processTable(fs, tableDir, log, c, majorCompact);
     } finally {
       log.close();
       BlockCache bc = StoreFile.getBlockCache(c);
       if (bc != null) bc.shutdown();
     }
  }
}