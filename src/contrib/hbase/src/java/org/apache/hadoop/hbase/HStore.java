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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Vector;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.StringUtils;
import org.onelab.filter.BloomFilter;
import org.onelab.filter.CountingBloomFilter;
import org.onelab.filter.Filter;
import org.onelab.filter.RetouchedBloomFilter;

/**
 * HStore maintains a bunch of data files.  It is responsible for maintaining 
 * the memory/file hierarchy and for periodic flushes to disk and compacting 
 * edits to the file.
 *
 * Locking and transactions are handled at a higher level.  This API should not 
 * be called directly by any writer, but rather by an HRegion manager.
 */
class HStore implements HConstants {
  static final Log LOG = LogFactory.getLog(HStore.class);

  /**
   * The Memcache holds in-memory modifications to the HRegion.  This is really a
   * wrapper around a TreeMap that helps us when staging the Memcache out to disk.
   */
  static class Memcache {

    // Note that since these structures are always accessed with a lock held,
    // no additional synchronization is required.

    @SuppressWarnings("hiding")
    private final SortedMap<HStoreKey, byte[]> memcache =
      Collections.synchronizedSortedMap(new TreeMap<HStoreKey, byte []>());
      
    volatile SortedMap<HStoreKey, byte[]> snapshot;
      
    @SuppressWarnings("hiding")
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Constructor
     */
    Memcache() {
      snapshot = 
        Collections.synchronizedSortedMap(new TreeMap<HStoreKey, byte []>());
    }

    /**
     * Creates a snapshot of the current Memcache
     */
    void snapshot() {
      this.lock.writeLock().lock();
      try {
        synchronized (memcache) {
          if (memcache.size() != 0) {
            snapshot.putAll(memcache);
            memcache.clear();
          }
        }
      } finally {
        this.lock.writeLock().unlock();
      }
    }
    
    /**
     * @return memcache snapshot
     */
    SortedMap<HStoreKey, byte[]> getSnapshot() {
      this.lock.writeLock().lock();
      try {
        SortedMap<HStoreKey, byte[]> currentSnapshot = snapshot;
        snapshot = 
          Collections.synchronizedSortedMap(new TreeMap<HStoreKey, byte []>());
        
        return currentSnapshot;

      } finally {
        this.lock.writeLock().unlock();
      }
    }
    
    /**
     * Store a value.  
     * @param key
     * @param value
     */
    void add(final HStoreKey key, final byte[] value) {
      this.lock.readLock().lock();
      try {
        memcache.put(key, value);
        
      } finally {
        this.lock.readLock().unlock();
      }
    }

    /**
     * Look back through all the backlog TreeMaps to find the target.
     * @param key
     * @param numVersions
     * @return An array of byte arrays ordered by timestamp.
     */
    List<byte[]> get(final HStoreKey key, final int numVersions) {
      this.lock.readLock().lock();
      try {
        List<byte []> results;
        synchronized (memcache) {
          results = internalGet(memcache, key, numVersions);
        }
        synchronized (snapshot) {
          results.addAll(results.size(),
              internalGet(snapshot, key, numVersions - results.size()));
        }
        return results;
        
      } finally {
        this.lock.readLock().unlock();
      }
    }

    /**
     * Return all the available columns for the given key.  The key indicates a 
     * row and timestamp, but not a column name.
     *
     * The returned object should map column names to byte arrays (byte[]).
     * @param key
     * @param results
     */
    void getFull(HStoreKey key, SortedMap<Text, byte[]> results) {
      this.lock.readLock().lock();
      try {
        synchronized (memcache) {
          internalGetFull(memcache, key, results);
        }
        synchronized (snapshot) {
          internalGetFull(snapshot, key, results);
        }

      } finally {
        this.lock.readLock().unlock();
      }
    }

    private void internalGetFull(SortedMap<HStoreKey, byte []> map, HStoreKey key, 
        SortedMap<Text, byte []> results) {

      SortedMap<HStoreKey, byte []> tailMap = map.tailMap(key);
      for (Map.Entry<HStoreKey, byte []> es: tailMap.entrySet()) {
        HStoreKey itKey = es.getKey();
        Text itCol = itKey.getColumn();
        if (results.get(itCol) == null && key.matchesWithoutColumn(itKey)) {
          byte [] val = tailMap.get(itKey);

          if (!HLogEdit.isDeleted(val)) {
            results.put(itCol, val);
          }

        } else if (key.getRow().compareTo(itKey.getRow()) < 0) {
          break;
        }
      }
    }

    /**
     * Examine a single map for the desired key.
     *
     * TODO - This is kinda slow.  We need a data structure that allows for 
     * proximity-searches, not just precise-matches.
     * 
     * @param map
     * @param key
     * @param numVersions
     * @return Ordered list of items found in passed <code>map</code>.  If no
     * matching values, returns an empty list (does not return null).
     */
    private ArrayList<byte []> internalGet(
        final SortedMap<HStoreKey, byte []> map, final HStoreKey key,
        final int numVersions) {

      ArrayList<byte []> result = new ArrayList<byte []>();
      // TODO: If get is of a particular version -- numVersions == 1 -- we
      // should be able to avoid all of the tailmap creations and iterations
      // below.
      HStoreKey curKey = new HStoreKey(key);
      SortedMap<HStoreKey, byte []> tailMap = map.tailMap(curKey);
      for (Map.Entry<HStoreKey, byte []> es: tailMap.entrySet()) {
        HStoreKey itKey = es.getKey();
        if (itKey.matchesRowCol(curKey)) {
          if (!HLogEdit.isDeleted(es.getValue())) {
            result.add(tailMap.get(itKey));
            curKey.setVersion(itKey.getTimestamp() - 1);
          }
        }
        if (numVersions > 0 && result.size() >= numVersions) {
          break;
        }
      }
      return result;
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
    List<HStoreKey> getKeys(final HStoreKey origin, final int versions) {
      this.lock.readLock().lock();
      try {
        List<HStoreKey> results;
        synchronized (memcache) {
          results = internalGetKeys(this.memcache, origin, versions);
        }
        synchronized (snapshot) {
          results.addAll(results.size(), internalGetKeys(snapshot, origin,
              versions == HConstants.ALL_VERSIONS ? versions :
                (versions - results.size())));
        }
        return results;
        
      } finally {
        this.lock.readLock().unlock();
      }
    }

    /*
     * @param origin Where to start searching.
     * @param versions How many versions to return. Pass
     * {@link HConstants.ALL_VERSIONS} to retrieve all.
     * @return List of all keys that are of the same row and column and of
     * equal or older timestamp.  If no keys, returns an empty List. Does not
     * return null.
     */
    private List<HStoreKey> internalGetKeys(final SortedMap<HStoreKey, byte []> map,
        final HStoreKey origin, final int versions) {

      List<HStoreKey> result = new ArrayList<HStoreKey>();
      SortedMap<HStoreKey, byte []> tailMap = map.tailMap(origin);
      for (Map.Entry<HStoreKey, byte []> es: tailMap.entrySet()) {
        HStoreKey key = es.getKey();
    
        // if there's no column name, then compare rows and timestamps
        if (origin.getColumn().toString().equals("")) {
          // if the current and origin row don't match, then we can jump
          // out of the loop entirely.
          if (!key.getRow().equals(origin.getRow())) {
            break;
          }
          // if the rows match but the timestamp is newer, skip it so we can
          // get to the ones we actually want.
          if (key.getTimestamp() > origin.getTimestamp()) {
            continue;
          }
        }
        else{ // compare rows and columns
          // if the key doesn't match the row and column, then we're done, since 
          // all the cells are ordered.
          if (!key.matchesRowCol(origin)) {
            break;
          }
        }

        if (!HLogEdit.isDeleted(es.getValue())) {
          result.add(key);
          if (versions != HConstants.ALL_VERSIONS && result.size() >= versions) {
            // We have enough results.  Return.
            break;
          }
        }
      }
      return result;
    }


    /**
     * @param key
     * @return True if an entry and its content is {@link HGlobals.deleteBytes}.
     * Use checking values in store. On occasion the memcache has the fact that
     * the cell has been deleted.
     */
    boolean isDeleted(final HStoreKey key) {
      return HLogEdit.isDeleted(this.memcache.get(key));
    }

    /**
     * @return a scanner over the keys in the Memcache
     */
    HInternalScannerInterface getScanner(long timestamp,
        Text targetCols[], Text firstRow) throws IOException {

      // Here we rely on ReentrantReadWriteLock's ability to acquire multiple
      // locks by the same thread and to be able to downgrade a write lock to
      // a read lock. We need to hold a lock throughout this method, but only
      // need the write lock while creating the memcache snapshot
      
      this.lock.writeLock().lock(); // hold write lock during memcache snapshot
      snapshot();                       // snapshot memcache
      this.lock.readLock().lock();      // acquire read lock
      this.lock.writeLock().unlock();   // downgrade to read lock
      try {
        // Prevent a cache flush while we are constructing the scanner

        return new MemcacheScanner(timestamp, targetCols, firstRow);
      
      } finally {
        this.lock.readLock().unlock();
      }
    }

    //////////////////////////////////////////////////////////////////////////////
    // MemcacheScanner implements the HScannerInterface.
    // It lets the caller scan the contents of the Memcache.
    //////////////////////////////////////////////////////////////////////////////

    class MemcacheScanner extends HAbstractScanner {
      SortedMap<HStoreKey, byte []> backingMap;
      Iterator<HStoreKey> keyIterator;

      @SuppressWarnings("unchecked")
      MemcacheScanner(final long timestamp, final Text targetCols[],
          final Text firstRow) throws IOException {

        super(timestamp, targetCols);
        try {
          this.backingMap = new TreeMap<HStoreKey, byte[]>();
          this.backingMap.putAll(snapshot);
          this.keys = new HStoreKey[1];
          this.vals = new byte[1][];

          // Generate list of iterators

          HStoreKey firstKey = new HStoreKey(firstRow);
            if (firstRow != null && firstRow.getLength() != 0) {
              keyIterator =
                backingMap.tailMap(firstKey).keySet().iterator();

            } else {
              keyIterator = backingMap.keySet().iterator();
            }

            while (getNext(0)) {
              if (!findFirstRow(0, firstRow)) {
                continue;
              }
              if (columnMatch(0)) {
                break;
              }
            }
        } catch (RuntimeException ex) {
          LOG.error("error initializing Memcache scanner: ", ex);
          close();
          IOException e = new IOException("error initializing Memcache scanner");
          e.initCause(ex);
          throw e;

        } catch(IOException ex) {
          LOG.error("error initializing Memcache scanner: ", ex);
          close();
          throw ex;
        }
      }

      /**
       * The user didn't want to start scanning at the first row. This method
       * seeks to the requested row.
       *
       * @param i which iterator to advance
       * @param firstRow seek to this row
       * @return true if this is the first row
       */
      @Override
      boolean findFirstRow(int i, Text firstRow) {
        return firstRow.getLength() == 0 ||
        keys[i].getRow().compareTo(firstRow) >= 0;
      }

      /**
       * Get the next value from the specified iterator.
       * 
       * @param i Which iterator to fetch next value from
       * @return true if there is more data available
       */
      @Override
      boolean getNext(int i) {
        boolean result = false;
        while (true) {
          if (!keyIterator.hasNext()) {
            closeSubScanner(i);
            break;
          }
          // Check key is < than passed timestamp for this scanner.
          HStoreKey hsk = keyIterator.next();
          if (hsk == null) {
            throw new NullPointerException("Unexpected null key");
          }
          if (hsk.getTimestamp() <= this.timestamp) {
            this.keys[i] = hsk;
            this.vals[i] = backingMap.get(keys[i]);
            result = true;
            break;
          }
        }
        return result;
      }

      /** Shut down an individual map iterator. */
      @Override
      void closeSubScanner(int i) {
        keyIterator = null;
        keys[i] = null;
        vals[i] = null;
        backingMap = null;
      }

      /** Shut down map iterators */
      public void close() {
        if (!scannerClosed) {
          if(keyIterator != null) {
            closeSubScanner(0);
          }
          scannerClosed = true;
        }
      }
    }
  }
  
  static final String COMPACTION_TO_REPLACE = "toreplace";    
  static final String COMPACTION_DONE = "done";
  
  private static final String BLOOMFILTER_FILE_NAME = "filter";

  final Memcache memcache = new Memcache();
  Path dir;
  Text regionName;
  String encodedRegionName;
  HColumnDescriptor family;
  Text familyName;
  SequenceFile.CompressionType compression;
  FileSystem fs;
  HBaseConfiguration conf;
  Path mapdir;
  Path loginfodir;
  Path filterDir;
  Filter bloomFilter;
  private String storeName;
  private final Path compactionDir;

  Integer compactLock = new Integer(0);
  Integer flushLock = new Integer(0);

  final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  final AtomicInteger activeScanners = new AtomicInteger(0);

  /* Sorted Map of readers keyed by sequence id (Most recent should be last in
   * in list).
   */
  SortedMap<Long, HStoreFile> storefiles =
    Collections.synchronizedSortedMap(new TreeMap<Long, HStoreFile>());
  
  /* Sorted Map of readers keyed by sequence id (Most recent should be last in
   * in list).
   */
  TreeMap<Long, MapFile.Reader> readers = new TreeMap<Long, MapFile.Reader>();

  Random rand = new Random();
  
  private volatile long maxSeqId;
  
  private final int compactionThreshold;
  
  private final ReentrantReadWriteLock newScannerLock =
    new ReentrantReadWriteLock();

  /**
   * An HStore is a set of zero or more MapFiles, which stretch backwards over 
   * time.  A given HStore is responsible for a certain set of columns for a
   * row in the HRegion.
   *
   * <p>The HRegion starts writing to its set of HStores when the HRegion's 
   * memcache is flushed.  This results in a round of new MapFiles, one for
   * each HStore.
   *
   * <p>There's no reason to consider append-logging at this level; all logging 
   * and locking is handled at the HRegion level.  HStore just provides
   * services to manage sets of MapFiles.  One of the most important of those
   * services is MapFile-compaction services.
   *
   * <p>The only thing having to do with logs that HStore needs to deal with is
   * the reconstructionLog.  This is a segment of an HRegion's log that might
   * NOT be present upon startup.  If the param is NULL, there's nothing to do.
   * If the param is non-NULL, we need to process the log to reconstruct
   * a TreeMap that might not have been written to disk before the process
   * died.
   *
   * <p>It's assumed that after this constructor returns, the reconstructionLog
   * file will be deleted (by whoever has instantiated the HStore).
   *
   * @param dir log file directory
   * @param regionName
   * @param encodedName
   * @param family name of column family
   * @param fs file system object
   * @param reconstructionLog existing log file to apply if any
   * @param conf configuration object
   * @throws IOException
   */
  HStore(Path dir, Text regionName, String encodedName,
      HColumnDescriptor family, FileSystem fs, Path reconstructionLog,
      HBaseConfiguration conf) throws IOException {  
    
    this.dir = dir;
    this.compactionDir = new Path(HRegion.getRegionDir(dir, encodedName),
      "compaction.dir");
    this.regionName = regionName;
    this.encodedRegionName = encodedName;
    this.family = family;
    this.familyName = HStoreKey.extractFamily(this.family.getName());
    this.compression = SequenceFile.CompressionType.NONE;
    this.storeName = this.encodedRegionName + "/" + this.familyName.toString();
    
    if(family.getCompression() != HColumnDescriptor.CompressionType.NONE) {
      if(family.getCompression() == HColumnDescriptor.CompressionType.BLOCK) {
        this.compression = SequenceFile.CompressionType.BLOCK;
      } else if(family.getCompression() ==
          HColumnDescriptor.CompressionType.RECORD) {
        this.compression = SequenceFile.CompressionType.RECORD;
      } else {
        assert(false);
      }
    }
    
    this.fs = fs;
    this.conf = conf;
    this.mapdir = HStoreFile.getMapDir(dir, encodedRegionName, familyName);
    fs.mkdirs(mapdir);
    this.loginfodir = HStoreFile.getInfoDir(dir, encodedRegionName, familyName);
    fs.mkdirs(loginfodir);
    if(family.getBloomFilter() == null) {
      this.filterDir = null;
      this.bloomFilter = null;
    } else {
      this.filterDir =
        HStoreFile.getFilterDir(dir, encodedRegionName, familyName);
      fs.mkdirs(filterDir);
      loadOrCreateBloomFilter();
    }

    if(LOG.isDebugEnabled()) {
      LOG.debug("starting " + this.regionName + "/" + this.familyName + " ("
          + this.storeName +
          ((reconstructionLog == null || !fs.exists(reconstructionLog)) ?
          ") (no reconstruction log)": " with reconstruction log: (" +
          reconstructionLog.toString()));
    }

    // Go through the 'mapdir' and 'loginfodir' together, make sure that all 
    // MapFiles are in a reliable state.  Every entry in 'mapdir' must have a 
    // corresponding one in 'loginfodir'. Without a corresponding log info
    // file, the entry in 'mapdir' must be deleted.
    List<HStoreFile> hstoreFiles = HStoreFile.loadHStoreFiles(conf, dir,
        encodedRegionName, familyName, fs);
    for(HStoreFile hsf: hstoreFiles) {
      this.storefiles.put(Long.valueOf(hsf.loadInfo(fs)), hsf);
    }

    // Now go through all the HSTORE_LOGINFOFILEs and figure out the
    // most-recent log-seq-ID that's present.  The most-recent such ID means we
    // can ignore all log messages up to and including that ID (because they're
    // already reflected in the TreeMaps).
    //
    // If the HSTORE_LOGINFOFILE doesn't contain a number, just ignore it. That
    // means it was built prior to the previous run of HStore, and so it cannot 
    // contain any updates also contained in the log.
    
    this.maxSeqId = getMaxSequenceId(hstoreFiles);
    if (LOG.isDebugEnabled()) {
      LOG.debug("maximum sequence id for hstore " + regionName + "/" +
          familyName + " (" + storeName + ") is " + this.maxSeqId);
    }
    
    doReconstructionLog(reconstructionLog, maxSeqId);

    // By default, we compact if an HStore has more than
    // MIN_COMMITS_FOR_COMPACTION map files
    this.compactionThreshold =
      conf.getInt("hbase.hstore.compactionThreshold", 3);
    
    // We used to compact in here before bringing the store online.  Instead
    // get it online quick even if it needs compactions so we can start
    // taking updates as soon as possible (Once online, can take updates even
    // during a compaction).

    // Move maxSeqId on by one. Why here?  And not in HRegion?
    this.maxSeqId += 1;
    
    // Finally, start up all the map readers! (There should be just one at this 
    // point, as we've compacted them all.)
    for(Map.Entry<Long, HStoreFile> e: this.storefiles.entrySet()) {
      this.readers.put(e.getKey(),
        e.getValue().getReader(this.fs, this.bloomFilter));
    }
  }
  
  /* 
   * @param hstoreFiles
   * @return Maximum sequence number found or -1.
   * @throws IOException
   */
  private long getMaxSequenceId(final List<HStoreFile> hstoreFiles)
  throws IOException {
    long maxSeqID = -1;
    for (HStoreFile hsf : hstoreFiles) {
      long seqid = hsf.loadInfo(fs);
      if (seqid > 0) {
        if (seqid > maxSeqID) {
          maxSeqID = seqid;
        }
      }
    }
    return maxSeqID;
  }
  
  long getMaxSequenceId() {
    return this.maxSeqId;
  }
  
  /*
   * Read the reconstructionLog to see whether we need to build a brand-new 
   * MapFile out of non-flushed log entries.  
   *
   * We can ignore any log message that has a sequence ID that's equal to or 
   * lower than maxSeqID.  (Because we know such log messages are already 
   * reflected in the MapFiles.)
   */
  private void doReconstructionLog(final Path reconstructionLog,
      final long maxSeqID) throws UnsupportedEncodingException, IOException {
    
    if (reconstructionLog == null || !fs.exists(reconstructionLog)) {
      // Nothing to do.
      return;
    }
    long maxSeqIdInLog = -1;
    TreeMap<HStoreKey, byte []> reconstructedCache =
      new TreeMap<HStoreKey, byte []>();
      
    SequenceFile.Reader login = new SequenceFile.Reader(this.fs,
        reconstructionLog, this.conf);
    
    try {
      HLogKey key = new HLogKey();
      HLogEdit val = new HLogEdit();
      long skippedEdits = 0;
      while (login.next(key, val)) {
        maxSeqIdInLog = Math.max(maxSeqIdInLog, key.getLogSeqNum());
        if (key.getLogSeqNum() <= maxSeqID) {
          skippedEdits++;
          continue;
        }
        if (skippedEdits > 0 && LOG.isDebugEnabled()) {
          LOG.debug("Skipped " + skippedEdits +
            " edits because sequence id <= " + maxSeqID);
        }
        // Check this edit is for me. Also, guard against writing
        // METACOLUMN info such as HBASE::CACHEFLUSH entries
        Text column = val.getColumn();
        if (column.equals(HLog.METACOLUMN)
            || !key.getRegionName().equals(regionName)
            || !HStoreKey.extractFamily(column).equals(this.familyName)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Passing on edit " + key.getRegionName() + ", " +
                column.toString() + ": " + 
                new String(val.getVal(), UTF8_ENCODING) +
                ", my region: " + regionName + ", my column: " +
                this.familyName);
          }
          continue;
        }
        HStoreKey k = new HStoreKey(key.getRow(), column, val.getTimestamp());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Applying edit <" + k.toString() + "=" + val.toString() +
              ">");
        }
        reconstructedCache.put(k, val.getVal());
      }
    } finally {
      login.close();
    }
    
    if (reconstructedCache.size() > 0) {
      // We create a "virtual flush" at maxSeqIdInLog+1.
      if (LOG.isDebugEnabled()) {
        LOG.debug("flushing reconstructionCache");
      }
      internalFlushCache(reconstructedCache, maxSeqIdInLog + 1);
    }
  }
  
  //////////////////////////////////////////////////////////////////////////////
  // Bloom filters
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Called by constructor if a bloom filter is enabled for this column family.
   * If the HStore already exists, it will read in the bloom filter saved
   * previously. Otherwise, it will create a new bloom filter.
   */
  private void loadOrCreateBloomFilter() throws IOException {
    Path filterFile = new Path(filterDir, BLOOMFILTER_FILE_NAME);
    if(fs.exists(filterFile)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("loading bloom filter for " + this.regionName + "/" +
            this.familyName + " (" + this.storeName + ")");
      }
      
      BloomFilterDescriptor.BloomFilterType type =
        family.getBloomFilter().filterType;

      switch(type) {
      
      case BLOOMFILTER:
        bloomFilter = new BloomFilter();
        break;
        
      case COUNTING_BLOOMFILTER:
        bloomFilter = new CountingBloomFilter();
        break;
        
      case RETOUCHED_BLOOMFILTER:
        bloomFilter = new RetouchedBloomFilter();
      }
      FSDataInputStream in = fs.open(filterFile);
      try {
        bloomFilter.readFields(in);
      } finally {
        fs.close();
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("creating bloom filter for " + this.regionName + "/" +
            this.familyName + " (" + this.storeName + ")");
      }

      BloomFilterDescriptor.BloomFilterType type =
        family.getBloomFilter().filterType;

      switch(type) {
      
      case BLOOMFILTER:
        bloomFilter = new BloomFilter(family.getBloomFilter().vectorSize,
            family.getBloomFilter().nbHash);
        break;
        
      case COUNTING_BLOOMFILTER:
        bloomFilter =
          new CountingBloomFilter(family.getBloomFilter().vectorSize,
            family.getBloomFilter().nbHash);
        break;
        
      case RETOUCHED_BLOOMFILTER:
        bloomFilter =
          new RetouchedBloomFilter(family.getBloomFilter().vectorSize,
            family.getBloomFilter().nbHash);
      }
    }
  }

  /**
   * Flushes bloom filter to disk
   * 
   * @throws IOException
   */
  private void flushBloomFilter() throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("flushing bloom filter for " + this.regionName + "/" +
          this.familyName + " (" + this.storeName + ")");
    }
    FSDataOutputStream out =
      fs.create(new Path(filterDir, BLOOMFILTER_FILE_NAME));
    try {
      bloomFilter.write(out);
    } finally {
      out.close();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("flushed bloom filter for " + this.regionName + "/" +
          this.familyName + " (" + this.storeName + ")");
    }
  }
  
  //////////////////////////////////////////////////////////////////////////////
  // End bloom filters
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Adds a value to the memcache
   * 
   * @param key
   * @param value
   */
  void add(HStoreKey key, byte[] value) {
    lock.readLock().lock();
    try {
      this.memcache.add(key, value);
      
    } finally {
      lock.readLock().unlock();
    }
  }
  
  /**
   * Close all the MapFile readers
   * 
   * We don't need to worry about subsequent requests because the HRegion holds
   * a write lock that will prevent any more reads or writes.
   * 
   * @throws IOException
   */
  List<HStoreFile> close() throws IOException {
    ArrayList<HStoreFile> result = null;
    this.lock.writeLock().lock();
    try {
      for (MapFile.Reader reader: this.readers.values()) {
        reader.close();
      }
      this.readers.clear();
      result = new ArrayList<HStoreFile>(storefiles.values());
      this.storefiles.clear();
      LOG.debug("closed " + this.regionName + "/" + this.familyName + " ("
          + this.storeName + ")");
      return result;
    } finally {
      this.lock.writeLock().unlock();
    }
  }


  //////////////////////////////////////////////////////////////////////////////
  // Flush changes to disk
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Prior to doing a cache flush, we need to snapshot the memcache. Locking is
   * handled by the memcache.
   */
  void snapshotMemcache() {
    this.memcache.snapshot();
  }
  
  /**
   * Write out a brand-new set of items to the disk.
   *
   * We should only store key/vals that are appropriate for the data-columns 
   * stored in this HStore.
   *
   * Also, we are not expecting any reads of this MapFile just yet.
   *
   * Return the entire list of HStoreFiles currently used by the HStore.
   *
   * @param logCacheFlushId flush sequence number
   * @throws IOException
   */
  void flushCache(final long logCacheFlushId) throws IOException {
      internalFlushCache(memcache.getSnapshot(), logCacheFlushId);
  }
  
  private void internalFlushCache(SortedMap<HStoreKey, byte []> cache,
      long logCacheFlushId) throws IOException {
    
    synchronized(flushLock) {
      // A. Write the Maps out to the disk
      HStoreFile flushedFile = HStoreFile.obtainNewHStoreFile(conf, dir,
        encodedRegionName, familyName, fs);
      String name = flushedFile.toString();
      MapFile.Writer out = flushedFile.getWriter(this.fs, this.compression,
        this.bloomFilter);
      
      // Here we tried picking up an existing HStoreFile from disk and
      // interlacing the memcache flush compacting as we go.  The notion was
      // that interlacing would take as long as a pure flush with the added
      // benefit of having one less file in the store.  Experiments showed that
      // it takes two to three times the amount of time flushing -- more column
      // families makes it so the two timings come closer together -- but it
      // also complicates the flush. The code was removed.  Needed work picking
      // which file to interlace (favor references first, etc.)
      //
      // Related, looks like 'merging compactions' in BigTable paper interlaces
      // a memcache flush.  We don't.
      try {
        for (Map.Entry<HStoreKey, byte []> es: cache.entrySet()) {
          HStoreKey curkey = es.getKey();
          if (this.familyName.equals(HStoreKey.extractFamily(
              curkey.getColumn()))) {
            out.append(curkey, new ImmutableBytesWritable(es.getValue()));
          }
        }
      } finally {
        out.close();
      }

      // B. Write out the log sequence number that corresponds to this output
      // MapFile.  The MapFile is current up to and including the log seq num.
      flushedFile.writeInfo(fs, logCacheFlushId);
      
      // C. Flush the bloom filter if any
      if (bloomFilter != null) {
        flushBloomFilter();
      }

      // D. Finally, make the new MapFile available.
      this.lock.writeLock().lock();
      try {
        Long flushid = Long.valueOf(logCacheFlushId);
        // Open the map file reader.
        this.readers.put(flushid,
            flushedFile.getReader(this.fs, this.bloomFilter));
        this.storefiles.put(flushid, flushedFile);
        if(LOG.isDebugEnabled()) {
          LOG.debug("Added " + name +
              " with sequence id " + logCacheFlushId + " and size " +
              StringUtils.humanReadableInt(flushedFile.length()) + " for " +
              this.regionName + "/" + this.familyName);
        }
      } finally {
        this.lock.writeLock().unlock();
      }
      return;
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // Compaction
  //////////////////////////////////////////////////////////////////////////////
  
  /**
   * @return True if this store needs compaction.
   */
  boolean needsCompaction() {
    boolean compactionNeeded = false;
    if (this.storefiles != null) {
      compactionNeeded = this.storefiles.size() >= this.compactionThreshold;
      if (LOG.isDebugEnabled()) {
        LOG.debug("compaction for HStore " + regionName + "/" + familyName +
            (compactionNeeded ? " " : " not ") + "needed.");
      }
    }
    return compactionNeeded;
  }

  /**
   * Compact the back-HStores.  This method may take some time, so the calling 
   * thread must be able to block for long periods.
   * 
   * <p>During this time, the HStore can work as usual, getting values from
   * MapFiles and writing new MapFiles from the Memcache.
   * 
   * Existing MapFiles are not destroyed until the new compacted TreeMap is 
   * completely written-out to disk.
   *
   * The compactLock prevents multiple simultaneous compactions.
   * The structureLock prevents us from interfering with other write operations.
   * 
   * We don't want to hold the structureLock for the whole time, as a compact() 
   * can be lengthy and we want to allow cache-flushes during this period.
   * @throws IOException
   * 
   * @return true if compaction completed successfully
   */
  boolean compact() throws IOException {
    synchronized (compactLock) {
      Path curCompactStore = getCompactionDir();
      if (LOG.isDebugEnabled()) {
        LOG.debug("started compaction of " + storefiles.size() +
          " files using " + curCompactStore.toString() + " for " +
          this.regionName + "/" + this.familyName);
      }
      if (this.fs.exists(curCompactStore)) {
        // Clean out its content in prep. for this new compaction.  Has either
        // aborted previous compaction or it has content of a previous
        // compaction.
        Path [] toRemove = this.fs.listPaths(new Path [] {curCompactStore});
        for (int i = 0; i < toRemove.length; i++) {
          this.fs.delete(toRemove[i]);
        }
      }
      // Storefiles are keyed by sequence id. The oldest file comes first.
      // We need to return out of here a List that has the newest file first.
      List<HStoreFile> filesToCompact =
        new ArrayList<HStoreFile>(this.storefiles.values());
      Collections.reverse(filesToCompact);
      if (filesToCompact.size() < 1 ||
        (filesToCompact.size() == 1 && !filesToCompact.get(0).isReference())) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("nothing to compact for " + this.regionName + "/" +
              this.familyName + " (" + this.storeName + ")");
        }
        return false;
      }

      if (!fs.exists(curCompactStore) && !fs.mkdirs(curCompactStore)) {
        LOG.warn("Mkdir on " + curCompactStore.toString() + " for " +
            this.regionName + "/" +
            this.familyName + " failed");
        return false;
      }

      // Step through them, writing to the brand-new TreeMap
      HStoreFile compactedOutputFile = new HStoreFile(conf, this.compactionDir,
        encodedRegionName, familyName, -1);
      MapFile.Writer compactedOut = compactedOutputFile.getWriter(this.fs,
        this.compression, this.bloomFilter);
      try {
        compactHStoreFiles(compactedOut, filesToCompact);
      } finally {
        compactedOut.close();
      }

      // Now, write out an HSTORE_LOGINFOFILE for the brand-new TreeMap.
      // Compute max-sequenceID seen in any of the to-be-compacted TreeMaps.
      long maxId = getMaxSequenceId(filesToCompact);
      compactedOutputFile.writeInfo(fs, maxId);

      // Write out a list of data files that we're replacing
      Path filesToReplace = new Path(curCompactStore, COMPACTION_TO_REPLACE);
      FSDataOutputStream out = fs.create(filesToReplace);
      try {
        out.writeInt(filesToCompact.size());
        for (HStoreFile hsf : filesToCompact) {
          hsf.write(out);
        }
      } finally {
        out.close();
      }

      // Indicate that we're done.
      Path doneFile = new Path(curCompactStore, COMPACTION_DONE);
      fs.create(doneFile).close();

      // Move the compaction into place.
      completeCompaction(curCompactStore);
      return true;
    }
  }
  
  /*
   * Compact passed <code>toCompactFiles</code> into <code>compactedOut</code>.
   * We create a new set of MapFile.Reader objects so we don't screw up the
   * caching associated with the currently-loaded ones. Our iteration-based
   * access pattern is practically designed to ruin the cache.
   * 
   * We work by opening a single MapFile.Reader for each file, and iterating
   * through them in parallel. We always increment the lowest-ranked one.
   * Updates to a single row/column will appear ranked by timestamp. This allows
   * us to throw out deleted values or obsolete versions. @param compactedOut
   * @param toCompactFiles @throws IOException
   */
  private void compactHStoreFiles(final MapFile.Writer compactedOut,
      final List<HStoreFile> toCompactFiles) throws IOException {
    
    int size = toCompactFiles.size();
    CompactionReader[] rdrs = new CompactionReader[size];
    int index = 0;
    for (HStoreFile hsf: toCompactFiles) {
      try {
        rdrs[index++] =
          new MapFileCompactionReader(hsf.getReader(fs, bloomFilter));
      } catch (IOException e) {
        // Add info about which file threw exception. It may not be in the
        // exception message so output a message here where we know the
        // culprit.
        LOG.warn("Failed with " + e.toString() + ": " + hsf.toString() +
          (hsf.isReference() ? " " + hsf.getReference().toString() : "") +
          " for " + this.regionName + "/" + this.familyName);
        closeCompactionReaders(rdrs);
        throw e;
      }
    }
    try {
      HStoreKey[] keys = new HStoreKey[rdrs.length];
      ImmutableBytesWritable[] vals = new ImmutableBytesWritable[rdrs.length];
      boolean[] done = new boolean[rdrs.length];
      for(int i = 0; i < rdrs.length; i++) {
        keys[i] = new HStoreKey();
        vals[i] = new ImmutableBytesWritable();
        done[i] = false;
      }

      // Now, advance through the readers in order.  This will have the
      // effect of a run-time sort of the entire dataset.
      int numDone = 0;
      for(int i = 0; i < rdrs.length; i++) {
        rdrs[i].reset();
        done[i] = ! rdrs[i].next(keys[i], vals[i]);
        if(done[i]) {
          numDone++;
        }
      }

      int timesSeen = 0;
      Text lastRow = new Text();
      Text lastColumn = new Text();
      // Map of a row deletes keyed by column with a list of timestamps for value
      Map<Text, List<Long>> deletes = null;
      while (numDone < done.length) {
        // Find the reader with the smallest key.  If two files have same key
        // but different values -- i.e. one is delete and other is non-delete
        // value -- we will find the first, the one that was written later and
        // therefore the one whose value should make it out to the compacted
        // store file.
        int smallestKey = -1;
        for(int i = 0; i < rdrs.length; i++) {
          if(done[i]) {
            continue;
          }
          if(smallestKey < 0) {
            smallestKey = i;
          } else {
            if(keys[i].compareTo(keys[smallestKey]) < 0) {
              smallestKey = i;
            }
          }
        }

        // Reflect the current key/val in the output
        HStoreKey sk = keys[smallestKey];
        if(lastRow.equals(sk.getRow())
            && lastColumn.equals(sk.getColumn())) {
          timesSeen++;
        } else {
          timesSeen = 1;
          // We are on to a new row.  Create a new deletes list.
          deletes = new HashMap<Text, List<Long>>();
        }

        byte [] value = (vals[smallestKey] == null)?
          null: vals[smallestKey].get();
        if (!isDeleted(sk, value, false, deletes) &&
            timesSeen <= family.getMaxVersions()) {
          // Keep old versions until we have maxVersions worth.
          // Then just skip them.
          if (sk.getRow().getLength() != 0 && sk.getColumn().getLength() != 0) {
            // Only write out objects which have a non-zero length key and
            // value
            compactedOut.append(sk, vals[smallestKey]);
          }
        }

        // Update last-seen items
        lastRow.set(sk.getRow());
        lastColumn.set(sk.getColumn());

        // Advance the smallest key.  If that reader's all finished, then 
        // mark it as done.
        if(!rdrs[smallestKey].next(keys[smallestKey],
            vals[smallestKey])) {
          done[smallestKey] = true;
          rdrs[smallestKey].close();
          rdrs[smallestKey] = null;
          numDone++;
        }
      }
    } finally {
      closeCompactionReaders(rdrs);
    }
  }
  
  private void closeCompactionReaders(final CompactionReader [] rdrs) {
    for (int i = 0; i < rdrs.length; i++) {
      if (rdrs[i] != null) {
        try {
          rdrs[i].close();
        } catch (IOException e) {
          LOG.warn("Exception closing reader for " + this.regionName + "/" +
              this.familyName, e);
        }
      }
    }
  }

  /** Interface for generic reader for compactions */
  interface CompactionReader {
    
    /**
     * Closes the reader
     * @throws IOException
     */
    public void close() throws IOException;
    
    /**
     * Get the next key/value pair
     * 
     * @param key
     * @param val
     * @return true if more data was returned
     * @throws IOException
     */
    public boolean next(WritableComparable key, Writable val)
    throws IOException;
    
    /**
     * Resets the reader
     * @throws IOException
     */
    public void reset() throws IOException;
  }

  /** A compaction reader for MapFile */
  static class MapFileCompactionReader implements CompactionReader {
    final MapFile.Reader reader;
    
    MapFileCompactionReader(final MapFile.Reader r) {
      this.reader = r;
    }
    
    /** {@inheritDoc} */
    public void close() throws IOException {
      this.reader.close();
    }

    /** {@inheritDoc} */
    public boolean next(WritableComparable key, Writable val)
    throws IOException {
      return this.reader.next(key, val);
    }

    /** {@inheritDoc} */
    public void reset() throws IOException {
      this.reader.reset();
    }
  }

  /*
   * Check if this is cell is deleted.
   * If a memcache and a deletes, check key does not have an entry filled.
   * Otherwise, check value is not the <code>HGlobals.deleteBytes</code> value.
   * If passed value IS deleteBytes, then it is added to the passed
   * deletes map.
   * @param hsk
   * @param value
   * @param checkMemcache true if the memcache should be consulted
   * @param deletes Map keyed by column with a value of timestamp. Can be null.
   * If non-null and passed value is HGlobals.deleteBytes, then we add to this
   * map.
   * @return True if this is a deleted cell.  Adds the passed deletes map if
   * passed value is HGlobals.deleteBytes.
  */
  private boolean isDeleted(final HStoreKey hsk, final byte [] value,
      final boolean checkMemcache, final Map<Text, List<Long>> deletes) {
    if (checkMemcache && memcache.isDeleted(hsk)) {
      return true;
    }
    List<Long> timestamps =
      (deletes == null) ? null: deletes.get(hsk.getColumn());
    if (timestamps != null &&
        timestamps.contains(Long.valueOf(hsk.getTimestamp()))) {
      return true;
    }
    if (value == null) {
      // If a null value, shouldn't be in here.  Mark it as deleted cell.
      return true;
    }
    if (!HLogEdit.isDeleted(value)) {
      return false;
    }
    // Cell has delete value.  Save it into deletes.
    if (deletes != null) {
      if (timestamps == null) {
        timestamps = new ArrayList<Long>();
        deletes.put(hsk.getColumn(), timestamps);
      }
      // We know its not already in the deletes array else we'd have returned
      // earlier so no need to test if timestamps already has this value.
      timestamps.add(Long.valueOf(hsk.getTimestamp()));
    }
    return true;
  }
  
  /*
   * It's assumed that the compactLock  will be acquired prior to calling this 
   * method!  Otherwise, it is not thread-safe!
   *
   * It works by processing a compaction that's been written to disk.
   * 
   * <p>It is usually invoked at the end of a compaction, but might also be
   * invoked at HStore startup, if the prior execution died midway through.
   * 
   * <p>Moving the compacted TreeMap into place means:
   * <pre>
   * 1) Wait for active scanners to exit
   * 2) Acquiring the write-lock
   * 3) Figuring out what MapFiles are going to be replaced
   * 4) Moving the new compacted MapFile into place
   * 5) Unloading all the replaced MapFiles.
   * 6) Deleting all the old MapFile files.
   * 7) Loading the new TreeMap.
   * 8) Releasing the write-lock
   * 9) Allow new scanners to proceed.
   * </pre>
   * 
   * @param curCompactStore Compaction to complete.
   */
  private void completeCompaction(final Path curCompactStore)
  throws IOException {
    // 1. Wait for active scanners to exit
    newScannerLock.writeLock().lock();                  // prevent new scanners
    try {
      synchronized (activeScanners) {
        while (activeScanners.get() != 0) {
          try {
            activeScanners.wait();
          } catch (InterruptedException e) {
            // continue
          }
        }

        // 2. Acquiring the HStore write-lock
        this.lock.writeLock().lock();
      }

      try {
        Path doneFile = new Path(curCompactStore, COMPACTION_DONE);
        if (!fs.exists(doneFile)) {
          // The last execution didn't finish the compaction, so there's nothing 
          // we can do.  We'll just have to redo it. Abandon it and return.
          LOG.warn("Redo failed compaction (missing 'done' file) for " +
              this.regionName + "/" + this.familyName);
          return;
        }

        // 3. Load in the files to be deleted.
        Vector<HStoreFile> toCompactFiles = new Vector<HStoreFile>();
        Path filesToReplace = new Path(curCompactStore, COMPACTION_TO_REPLACE);
        DataInputStream in = new DataInputStream(fs.open(filesToReplace));
        try {
          int numfiles = in.readInt();
          for(int i = 0; i < numfiles; i++) {
            HStoreFile hsf = new HStoreFile(conf);
            hsf.readFields(in);
            toCompactFiles.add(hsf);
          }
        } finally {
          in.close();
        }

        // 4. Moving the new MapFile into place.
        HStoreFile compactedFile = new HStoreFile(conf, this.compactionDir,
            encodedRegionName, familyName, -1);
        // obtainNewHStoreFile does its best to generate a filename that does not
        // currently exist.
        HStoreFile finalCompactedFile = HStoreFile.obtainNewHStoreFile(conf, dir,
            encodedRegionName, familyName, fs);
        if(LOG.isDebugEnabled()) {
          LOG.debug("moving " + compactedFile.toString() + " in " +
              this.compactionDir.toString() + " to " +
              finalCompactedFile.toString() + " in " + dir.toString() +
              " for " + this.regionName + "/" + this.familyName);
        }
        if (!compactedFile.rename(this.fs, finalCompactedFile)) {
          LOG.error("Failed move of compacted file " +
              finalCompactedFile.toString() + " for " + this.regionName + "/" +
              this.familyName);
          return;
        }

        // 5. and 6. Unload all the replaced MapFiles, close and delete.
        Vector<Long> toDelete = new Vector<Long>(toCompactFiles.size());
        for (Map.Entry<Long, HStoreFile> e: this.storefiles.entrySet()) {
          if (!toCompactFiles.contains(e.getValue())) {
            continue;
          }
          Long key = e.getKey();
          MapFile.Reader reader = this.readers.remove(key);
          if (reader != null) {
            reader.close();
          }
          toDelete.add(key);
        }

        try {
          for (Long key: toDelete) {
            HStoreFile hsf = this.storefiles.remove(key);
            hsf.delete();
          }

          // 7. Loading the new TreeMap.
          Long orderVal = Long.valueOf(finalCompactedFile.loadInfo(fs));
          this.readers.put(orderVal,
            finalCompactedFile.getReader(this.fs, this.bloomFilter));
          this.storefiles.put(orderVal, finalCompactedFile);
        } catch (IOException e) {
          LOG.error("Failed replacing compacted files for " +
              this.regionName + "/" + this.familyName + ". Compacted file is " +
              finalCompactedFile.toString() + ".  Files replaced are " +
              toCompactFiles.toString() +
              " some of which may have been already removed", e);
        }
      } finally {
        // 8. Releasing the write-lock
        this.lock.writeLock().unlock();
      }
    } finally {
      // 9. Allow new scanners to proceed.
      newScannerLock.writeLock().unlock();
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // Accessors.  
  // (This is the only section that is directly useful!)
  //////////////////////////////////////////////////////////////////////////////
  
  /**
   * Return all the available columns for the given key.  The key indicates a 
   * row and timestamp, but not a column name.
   *
   * The returned object should map column names to byte arrays (byte[]).
   */
  void getFull(HStoreKey key, TreeMap<Text, byte []> results)
    throws IOException {
    Map<Text, List<Long>> deletes = new HashMap<Text, List<Long>>();
    
    this.lock.readLock().lock();
    memcache.getFull(key, results);
    try {
      MapFile.Reader[] maparray = getReaders();
      for (int i = maparray.length - 1; i >= 0; i--) {
        MapFile.Reader map = maparray[i];
        synchronized(map) {
          map.reset();
          ImmutableBytesWritable readval = new ImmutableBytesWritable();
          HStoreKey readkey = (HStoreKey)map.getClosest(key, readval);
          if (readkey == null) {
            continue;
          }
          do {
            Text readcol = readkey.getColumn();
            if (results.get(readcol) == null
                && key.matchesWithoutColumn(readkey)) {
              if(isDeleted(readkey, readval.get(), true, deletes)) {
                break;
              }
              results.put(new Text(readcol), readval.get());
              readval = new ImmutableBytesWritable();
            } else if(key.getRow().compareTo(readkey.getRow()) < 0) {
              break;
            }
            
          } while(map.next(readkey, readval));
        }
      }
      
    } finally {
      this.lock.readLock().unlock();
    }
  }
  
  /*
   * @return Path to the compaction directory for this column family.
   * Compaction dir is a subdirectory of the region.  Needs to have the
   * same regiondir/storefamily path prefix; HStoreFile constructor presumes
   * it (TODO: Fix).
   */
  private Path getCompactionDir() {
    return HStoreFile.getHStoreDir(this.compactionDir,
      this.encodedRegionName, this.familyName);
  }
  
  private MapFile.Reader [] getReaders() {
    return this.readers.values().
      toArray(new MapFile.Reader[this.readers.size()]);
  }

  /**
   * Get the value for the indicated HStoreKey.  Grab the target value and the 
   * previous 'numVersions-1' values, as well.
   *
   * If 'numVersions' is negative, the method returns all available versions.
   * @param key
   * @param numVersions Number of versions to fetch.  Must be > 0.
   * @return values for the specified versions
   * @throws IOException
   */
  byte [][] get(HStoreKey key, int numVersions) throws IOException {
    if (numVersions <= 0) {
      throw new IllegalArgumentException("Number of versions must be > 0");
    }
    
    this.lock.readLock().lock();
    try {
      // Check the memcache
      List<byte[]> results = this.memcache.get(key, numVersions);
      // If we got sufficient versions from memcache, return.
      if (results.size() == numVersions) {
        return ImmutableBytesWritable.toArray(results);
      }

      // Keep a list of deleted cell keys.  We need this because as we go through
      // the store files, the cell with the delete marker may be in one file and
      // the old non-delete cell value in a later store file. If we don't keep
      // around the fact that the cell was deleted in a newer record, we end up
      // returning the old value if user is asking for more than one version.
      // This List of deletes should not large since we are only keeping rows
      // and columns that match those set on the scanner and which have delete
      // values.  If memory usage becomes an issue, could redo as bloom filter.
      Map<Text, List<Long>> deletes = new HashMap<Text, List<Long>>();
      // This code below is very close to the body of the getKeys method.
      MapFile.Reader[] maparray = getReaders();
      for(int i = maparray.length - 1; i >= 0; i--) {
        MapFile.Reader map = maparray[i];
        synchronized(map) {
          map.reset();
          ImmutableBytesWritable readval = new ImmutableBytesWritable();
          HStoreKey readkey = (HStoreKey)map.getClosest(key, readval);
          if (readkey == null) {
            // map.getClosest returns null if the passed key is > than the
            // last key in the map file.  getClosest is a bit of a misnomer
            // since it returns exact match or the next closest key AFTER not
            // BEFORE.
            continue;
          }
          if (!readkey.matchesRowCol(key)) {
            continue;
          }
          if (!isDeleted(readkey, readval.get(), true, deletes)) {
            results.add(readval.get());
            // Perhaps only one version is wanted.  I could let this
            // test happen later in the for loop test but it would cost
            // the allocation of an ImmutableBytesWritable.
            if (hasEnoughVersions(numVersions, results)) {
              break;
            }
          }
          for (readval = new ImmutableBytesWritable();
              map.next(readkey, readval) &&
              readkey.matchesRowCol(key) &&
              !hasEnoughVersions(numVersions, results);
              readval = new ImmutableBytesWritable()) {
            if (!isDeleted(readkey, readval.get(), true, deletes)) {
              results.add(readval.get());
            }
          }
        }
        if (hasEnoughVersions(numVersions, results)) {
          break;
        }
      }
      return results.size() == 0 ?
        null : ImmutableBytesWritable.toArray(results);
    } finally {
      this.lock.readLock().unlock();
    }
  }
  
  private boolean hasEnoughVersions(final int numVersions,
      final List<byte []> results) {
    return numVersions > 0 && results.size() >= numVersions;
  }

  /**
   * Get <code>versions</code> keys matching the origin key's
   * row/column/timestamp and those of an older vintage
   * Default access so can be accessed out of {@link HRegionServer}.
   * @param origin Where to start searching.
   * @param versions How many versions to return. Pass
   * {@link HConstants.ALL_VERSIONS} to retrieve all. Versions will include
   * size of passed <code>allKeys</code> in its count.
   * @param allKeys List of keys prepopulated by keys we found in memcache.
   * This method returns this passed list with all matching keys found in
   * stores appended.
   * @return The passed <code>allKeys</code> with <code>versions</code> of
   * matching keys found in store files appended.
   * @throws IOException
   */
  List<HStoreKey> getKeys(final HStoreKey origin, final int versions)
    throws IOException {
    
    List<HStoreKey> keys = this.memcache.getKeys(origin, versions);
    if (versions != ALL_VERSIONS && keys.size() >= versions) {
      return keys;
    }
    
    // This code below is very close to the body of the get method.
    this.lock.readLock().lock();
    try {
      MapFile.Reader[] maparray = getReaders();
      for(int i = maparray.length - 1; i >= 0; i--) {
        MapFile.Reader map = maparray[i];
        synchronized(map) {
          map.reset();
          
          // do the priming read
          ImmutableBytesWritable readval = new ImmutableBytesWritable();
          HStoreKey readkey = (HStoreKey)map.getClosest(origin, readval);
          if (readkey == null) {
            // map.getClosest returns null if the passed key is > than the
            // last key in the map file.  getClosest is a bit of a misnomer
            // since it returns exact match or the next closest key AFTER not
            // BEFORE.
            continue;
          }
          
          do{
            // if the row matches, we might want this one.
            if(rowMatches(origin, readkey)){
              // if the cell matches, then we definitely want this key.
              if (cellMatches(origin, readkey)) {
                // store the key if it isn't deleted or superceeded by what's
                // in the memcache
                if (!isDeleted(readkey, readval.get(), false, null) &&
                    !keys.contains(readkey)) {
                  keys.add(new HStoreKey(readkey));

                  // if we've collected enough versions, then exit the loop.
                  if (versions != ALL_VERSIONS && keys.size() >= versions) {
                    break;
                  }
                }
              } else {
                // the cell doesn't match, but there might be more with different
                // timestamps, so move to the next key
                continue;
              }
            } else{
              // the row doesn't match, so we've gone too far.
              break;
            }
          }while(map.next(readkey, readval)); // advance to the next key
        }
      }
      
      return keys;
    } finally {
      this.lock.readLock().unlock();
    }
  }
  
  /**
   * Test that the <i>target</i> matches the <i>origin</i>. If the 
   * <i>origin</i> has an empty column, then it's assumed to mean any column 
   * matches and only match on row and timestamp. Otherwise, it compares the
   * keys with HStoreKey.matchesRowCol().
   * @param origin The key we're testing against
   * @param target The key we're testing
   */
  private boolean cellMatches(HStoreKey origin, HStoreKey target){
    // if the origin's column is empty, then we're matching any column
    if (origin.getColumn().equals(new Text())){
      // if the row matches, then...
      if (target.getRow().equals(origin.getRow())) {
        // check the timestamp
        return target.getTimestamp() <= origin.getTimestamp();
      }
      return false;
    }
    // otherwise, we want to match on row and column
    return target.matchesRowCol(origin);
  }
  
  /**
   * Test that the <i>target</i> matches the <i>origin</i>. If the <i>origin</i>
   * has an empty column, then it just tests row equivalence. Otherwise, it uses
   * HStoreKey.matchesRowCol().
   * @param origin Key we're testing against
   * @param target Key we're testing
   */
  private boolean rowMatches(HStoreKey origin, HStoreKey target){
    // if the origin's column is empty, then we're matching any column
    if (origin.getColumn().equals(new Text())){
      // if the row matches, then...
      return target.getRow().equals(origin.getRow());
    }
    // otherwise, we want to match on row and column
    return target.matchesRowCol(origin);
  }
  
  /*
   * Data structure to hold result of a look at store file sizes.
   */
  static class HStoreSize {
    final long aggregate;
    final long largest;
    boolean splitable;
    
    HStoreSize(final long a, final long l, final boolean s) {
      this.aggregate = a;
      this.largest = l;
      this.splitable = s;
    }
    
    long getAggregate() {
      return this.aggregate;
    }
    
    long getLargest() {
      return this.largest;
    }
    
    boolean isSplitable() {
      return this.splitable;
    }
    
    void setSplitable(final boolean s) {
      this.splitable = s;
    }
  }
  
  /**
   * Gets size for the store.
   * 
   * @param midKey Gets set to the middle key of the largest splitable store
   * file or its set to empty if largest is not splitable.
   * @return Sizes for the store and the passed <code>midKey</code> is
   * set to midKey of largest splitable.  Otherwise, its set to empty
   * to indicate we couldn't find a midkey to split on
   */
  HStoreSize size(Text midKey) {
    long maxSize = 0L;
    long aggregateSize = 0L;
    // Not splitable if we find a reference store file present in the store.
    boolean splitable = true;
    if (this.storefiles.size() <= 0) {
      return new HStoreSize(0, 0, splitable);
    }
    
    this.lock.readLock().lock();
    try {
      Long mapIndex = Long.valueOf(0L);
      // Iterate through all the MapFiles
      for(Map.Entry<Long, HStoreFile> e: storefiles.entrySet()) {
        HStoreFile curHSF = e.getValue();
        long size = curHSF.length();
        aggregateSize += size;
        if (maxSize == 0L || size > maxSize) {
          // This is the largest one so far
          maxSize = size;
          mapIndex = e.getKey();
        }
        if (splitable) {
          splitable = !curHSF.isReference();
        }
      }
      MapFile.Reader r = this.readers.get(mapIndex);
      WritableComparable midkey = r.midKey();
      if (midkey != null) {
        midKey.set(((HStoreKey)midkey).getRow());
      }
    } catch(IOException e) {
      LOG.warn("Failed getting store size for " + this.regionName + "/" +
          this.familyName, e);
    } finally {
      this.lock.readLock().unlock();
    }
    return new HStoreSize(aggregateSize, maxSize, splitable);
  }
  
  //////////////////////////////////////////////////////////////////////////////
  // File administration
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Return a scanner for both the memcache and the HStore files
   */
  HInternalScannerInterface getScanner(long timestamp, Text targetCols[],
      Text firstRow, RowFilterInterface filter) throws IOException {

    newScannerLock.readLock().lock();           // ability to create a new
                                                // scanner during a compaction
    try {
      lock.readLock().lock();                   // lock HStore
      try {
        return new HStoreScanner(targetCols, firstRow, timestamp, filter);

      } finally {
        lock.readLock().unlock();
      }
    } finally {
      newScannerLock.readLock().unlock();
    }
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return this.storeName;
  }

  /**
   * A scanner that iterates through the HStore files
   */
  private class StoreFileScanner extends HAbstractScanner {
    @SuppressWarnings("hiding")
    private MapFile.Reader[] readers;
    
    StoreFileScanner(long timestamp, Text[] targetCols, Text firstRow)
    throws IOException {
      super(timestamp, targetCols);
      try {
        this.readers = new MapFile.Reader[storefiles.size()];
        
        // Most recent map file should be first
        
        int i = readers.length - 1;
        for(HStoreFile curHSF: storefiles.values()) {
          readers[i--] = curHSF.getReader(fs, bloomFilter);
        }
        
        this.keys = new HStoreKey[readers.length];
        this.vals = new byte[readers.length][];
        
        // Advance the readers to the first pos.
        for(i = 0; i < readers.length; i++) {
          keys[i] = new HStoreKey();
          
          if(firstRow.getLength() != 0) {
            if(findFirstRow(i, firstRow)) {
              continue;
            }
          }
          
          while(getNext(i)) {
            if(columnMatch(i)) {
              break;
            }
          }
        }
        
      } catch (Exception ex) {
        close();
        IOException e = new IOException("HStoreScanner failed construction");
        e.initCause(ex);
        throw e;
      }
    }

    /**
     * The user didn't want to start scanning at the first row. This method
     * seeks to the requested row.
     *
     * @param i         - which iterator to advance
     * @param firstRow  - seek to this row
     * @return          - true if this is the first row or if the row was not found
     */
    @Override
    boolean findFirstRow(int i, Text firstRow) throws IOException {
      ImmutableBytesWritable ibw = new ImmutableBytesWritable();
      HStoreKey firstKey
        = (HStoreKey)readers[i].getClosest(new HStoreKey(firstRow), ibw);
      if (firstKey == null) {
        // Didn't find it. Close the scanner and return TRUE
        closeSubScanner(i);
        return true;
      }
      this.vals[i] = ibw.get();
      keys[i].setRow(firstKey.getRow());
      keys[i].setColumn(firstKey.getColumn());
      keys[i].setVersion(firstKey.getTimestamp());
      return columnMatch(i);
    }
    
    /**
     * Get the next value from the specified reader.
     * 
     * @param i - which reader to fetch next value from
     * @return - true if there is more data available
     */
    @Override
    boolean getNext(int i) throws IOException {
      boolean result = false;
      ImmutableBytesWritable ibw = new ImmutableBytesWritable();
      while (true) {
        if (!readers[i].next(keys[i], ibw)) {
          closeSubScanner(i);
          break;
        }
        if (keys[i].getTimestamp() <= this.timestamp) {
          vals[i] = ibw.get();
          result = true;
          break;
        }
      }
      return result;
    }
    
    /** Close down the indicated reader. */
    @Override
    void closeSubScanner(int i) {
      try {
        if(readers[i] != null) {
          try {
            readers[i].close();
          } catch(IOException e) {
            LOG.error(regionName + "/" + familyName + " closing sub-scanner", e);
          }
        }
        
      } finally {
        readers[i] = null;
        keys[i] = null;
        vals[i] = null;
      }
    }

    /** Shut it down! */
    public void close() {
      if(! scannerClosed) {
        try {
          for(int i = 0; i < readers.length; i++) {
            if(readers[i] != null) {
              try {
                readers[i].close();
              } catch(IOException e) {
                LOG.error(regionName + "/" + familyName + " closing scanner", e);
              }
            }
          }
          
        } finally {
          scannerClosed = true;
        }
      }
    }
  }
  
  /**
   * Scanner scans both the memcache and the HStore
   */
  private class HStoreScanner implements HInternalScannerInterface {
    private HInternalScannerInterface[] scanners;
    private TreeMap<Text, byte []>[] resultSets;
    private HStoreKey[] keys;
    private boolean wildcardMatch = false;
    private boolean multipleMatchers = false;
    private RowFilterInterface dataFilter;

    /** Create an Scanner with a handle on the memcache and HStore files. */
    @SuppressWarnings("unchecked")
    HStoreScanner(Text[] targetCols, Text firstRow, long timestamp,
        RowFilterInterface filter) throws IOException {
      
      this.dataFilter = filter;
      if (null != dataFilter) {
        dataFilter.reset();
      }
      this.scanners = new HInternalScannerInterface[2];
      this.resultSets = new TreeMap[scanners.length];
      this.keys = new HStoreKey[scanners.length];

      try {
        scanners[0] = memcache.getScanner(timestamp, targetCols, firstRow);
        scanners[1] = new StoreFileScanner(timestamp, targetCols, firstRow);
        
        for (int i = 0; i < scanners.length; i++) {
          if (scanners[i].isWildcardScanner()) {
            this.wildcardMatch = true;
          }
          if (scanners[i].isMultipleMatchScanner()) {
            this.multipleMatchers = true;
          }
        }

      } catch(IOException e) {
        for (int i = 0; i < this.scanners.length; i++) {
          if(scanners[i] != null) {
            closeScanner(i);
          }
        }
        throw e;
      }
      
      // Advance to the first key in each scanner.
      // All results will match the required column-set and scanTime.
      
      for (int i = 0; i < scanners.length; i++) {
        keys[i] = new HStoreKey();
        resultSets[i] = new TreeMap<Text, byte []>();
        if(scanners[i] != null && !scanners[i].next(keys[i], resultSets[i])) {
          closeScanner(i);
        }
      }
      // As we have now successfully completed initialization, increment the
      // activeScanner count.
      activeScanners.incrementAndGet();
    }

    /** @return true if the scanner is a wild card scanner */
    public boolean isWildcardScanner() {
      return wildcardMatch;
    }

    /** @return true if the scanner is a multiple match scanner */
    public boolean isMultipleMatchScanner() {
      return multipleMatchers;
    }

    /** {@inheritDoc} */
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
                if (HLogEdit.isDeleted(e.getValue())) {
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
          LOG.warn(regionName + "/" + familyName + " failed closing scanner "
              + i, e);
        }
      } finally {
        scanners[i] = null;
        keys[i] = null;
        resultSets[i] = null;
      }
    }

    /** {@inheritDoc} */
    public void close() {
      try {
      for(int i = 0; i < scanners.length; i++) {
        if(scanners[i] != null) {
          closeScanner(i);
        }
      }
      } finally {
        synchronized (activeScanners) {
          int numberOfScanners = activeScanners.decrementAndGet();
          if (numberOfScanners < 0) {
            LOG.error(regionName + "/" + familyName +
                " number of active scanners less than zero: " +
                numberOfScanners + " resetting to zero");
            activeScanners.set(0);
            numberOfScanners = 0;
          }
          if (numberOfScanners == 0) {
            activeScanners.notifyAll();
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
}
