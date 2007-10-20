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
import java.io.DataOutputStream;
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

  static final String COMPACTION_TO_REPLACE = "toreplace";    
  static final String COMPACTION_DONE = "done";
  
  private static final String BLOOMFILTER_FILE_NAME = "filter";

  Path dir;
  Text regionName;
  String encodedRegionName;
  HColumnDescriptor family;
  Text familyName;
  SequenceFile.CompressionType compression;
  FileSystem fs;
  Configuration conf;
  Path mapdir;
  Path loginfodir;
  Path filterDir;
  Filter bloomFilter;
  private String storeName;
  private final Path compactionDir;

  Integer compactLock = new Integer(0);
  Integer flushLock = new Integer(0);

  final HLocking lock = new HLocking();

  /* Sorted Map of readers keyed by sequence id (Most recent should be last in
   * in list).
   */
  TreeMap<Long, HStoreFile> storefiles = new TreeMap<Long, HStoreFile>();
  
  /* Sorted Map of readers keyed by sequence id (Most recent should be last in
   * in list).
   */
  TreeMap<Long, MapFile.Reader> readers = new TreeMap<Long, MapFile.Reader>();

  Random rand = new Random();
  
  private long maxSeqId;
  
  private int compactionThreshold;

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
      Configuration conf)
  throws IOException {  
    this.dir = dir;
    this.compactionDir = new Path(dir, "compaction.dir");
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
      LOG.debug("starting " + this.storeName +
        ((reconstructionLog == null || !fs.exists(reconstructionLog))?
          " (no reconstruction log)": " with reconstruction log: " +
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
    
    long maxSeqID = -1;
    for (HStoreFile hsf: hstoreFiles) {
      long seqid = hsf.loadInfo(fs);
      if(seqid > 0) {
        if(seqid > maxSeqID) {
          maxSeqID = seqid;
        }
      }
    }
    this.maxSeqId = maxSeqID;
    if (LOG.isDebugEnabled()) {
      LOG.debug("maximum sequence id for hstore " + storeName + " is " +
          this.maxSeqId);
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
      final long maxSeqID)
  throws UnsupportedEncodingException, IOException {
    if (reconstructionLog == null || !fs.exists(reconstructionLog)) {
      // Nothing to do.
      return;
    }
    long maxSeqIdInLog = -1;
    TreeMap<HStoreKey, byte []> reconstructedCache =
      new TreeMap<HStoreKey, byte []>();
    SequenceFile.Reader login =
      new SequenceFile.Reader(this.fs, reconstructionLog, this.conf);
    try {
      HLogKey key = new HLogKey();
      HLogEdit val = new HLogEdit();
      while (login.next(key, val)) {
        maxSeqIdInLog = Math.max(maxSeqIdInLog, key.getLogSeqNum());
        if (key.getLogSeqNum() <= maxSeqID) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Skipping edit <" + key.toString() + "=" +
                val.toString() + "> key sequence: " + key.getLogSeqNum() +
                " max sequence: " + maxSeqID);
          }
          continue;
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
      flushCacheHelper(reconstructedCache, maxSeqIdInLog + 1, true);
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
        LOG.debug("loading bloom filter for " + this.storeName);
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
      bloomFilter.readFields(in);
      fs.close();
      
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("creating bloom filter for " + this.storeName);
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
      LOG.debug("flushing bloom filter for " + this.storeName);
    }
    FSDataOutputStream out =
      fs.create(new Path(filterDir, BLOOMFILTER_FILE_NAME));
    
    bloomFilter.write(out);
    out.close();
    if (LOG.isDebugEnabled()) {
      LOG.debug("flushed bloom filter for " + this.storeName);
    }
  }
  
  //////////////////////////////////////////////////////////////////////////////
  // End bloom filters
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Close all the MapFile readers
   * @throws IOException
   */
  Vector<HStoreFile> close() throws IOException {
    Vector<HStoreFile> result = null;
    this.lock.obtainWriteLock();
    try {
      for (MapFile.Reader reader: this.readers.values()) {
        reader.close();
      }
      this.readers.clear();
      result = new Vector<HStoreFile>(storefiles.values());
      this.storefiles.clear();
      LOG.debug("closed " + this.storeName);
      return result;
    } finally {
      this.lock.releaseWriteLock();
    }
  }


  //////////////////////////////////////////////////////////////////////////////
  // Flush changes to disk
  //////////////////////////////////////////////////////////////////////////////

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
   * @param inputCache memcache to flush
   * @param logCacheFlushId flush sequence number
   * @throws IOException
   */
  void flushCache(final SortedMap<HStoreKey, byte []> inputCache,
    final long logCacheFlushId)
  throws IOException {
    flushCacheHelper(inputCache, logCacheFlushId, true);
  }
  
  void flushCacheHelper(SortedMap<HStoreKey, byte []> inputCache,
      long logCacheFlushId, boolean addToAvailableMaps)
  throws IOException {
    synchronized(flushLock) {
      // A. Write the TreeMap out to the disk
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
        for (Map.Entry<HStoreKey, byte []> es: inputCache.entrySet()) {
          HStoreKey curkey = es.getKey();
          if (this.familyName.
              equals(HStoreKey.extractFamily(curkey.getColumn()))) {
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
      if(bloomFilter != null) {
        flushBloomFilter();
      }

      // D. Finally, make the new MapFile available.
      if(addToAvailableMaps) {
        this.lock.obtainWriteLock();
        try {
          Long flushid = Long.valueOf(logCacheFlushId);
          // Open the map file reader.
          this.readers.put(flushid,
            flushedFile.getReader(this.fs, this.bloomFilter));
          this.storefiles.put(flushid, flushedFile);
          if(LOG.isDebugEnabled()) {
            LOG.debug("Added " + name +
                " with sequence id " + logCacheFlushId + " and size " +
              StringUtils.humanReadableInt(flushedFile.length()));
          }
        } finally {
          this.lock.releaseWriteLock();
        }
      }
      return;
    }
  }

  /**
   * @return - vector of all the HStore files in use
   */
  Vector<HStoreFile> getAllStoreFiles() {
    this.lock.obtainReadLock();
    try {
      return new Vector<HStoreFile>(storefiles.values());
    } finally {
      this.lock.releaseReadLock();
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // Compaction
  //////////////////////////////////////////////////////////////////////////////
  
  /**
   * @return True if this store needs compaction.
   */
  public boolean needsCompaction() {
    return this.storefiles != null &&
      this.storefiles.size() >= this.compactionThreshold;
  }

  /**
   * Compact the back-HStores.  This method may take some time, so the calling 
   * thread must be able to block for long periods.
   * 
   * <p>During this time, the HStore can work as usual, getting values from
   * MapFiles and writing new MapFiles from given memcaches.
   * 
   * Existing MapFiles are not destroyed until the new compacted TreeMap is 
   * completely written-out to disk.
   *
   * The compactLock block prevents multiple simultaneous compactions.
   * The structureLock prevents us from interfering with other write operations.
   * 
   * We don't want to hold the structureLock for the whole time, as a compact() 
   * can be lengthy and we want to allow cache-flushes during this period.
   * @throws IOException
   */
  void compact() throws IOException {
    compactHelper(false);
  }
  
  void compactHelper(final boolean deleteSequenceInfo)
  throws IOException {
    compactHelper(deleteSequenceInfo, -1);
  }

  /* 
   * @param deleteSequenceInfo True if we are to set the sequence number to -1
   * on compacted file.
   * @param maxSeenSeqID We may have already calculated the maxSeenSeqID.  If
   * so, pass it here.  Otherwise, pass -1 and it will be calculated inside in
   * this method.
   * @param deleteSequenceInfo
   * @param maxSeenSeqID
   * @throws IOException
   */
  void compactHelper(final boolean deleteSequenceInfo, long maxSeenSeqID)
  throws IOException {
    long maxId = maxSeenSeqID;
    synchronized(compactLock) {
      Path curCompactStore = HStoreFile.getHStoreDir(this.compactionDir,
          encodedRegionName, familyName);
      if(LOG.isDebugEnabled()) {
        LOG.debug("started compaction of " + storefiles.size() + " files in " +
          curCompactStore.toString());
      }
      if (this.fs.exists(curCompactStore)) {
        LOG.warn("Cleaning up a previous incomplete compaction at " +
          curCompactStore.toString());
        if (!this.fs.delete(curCompactStore)) {
          LOG.warn("Deleted returned false on " + curCompactStore.toString());
        }
      }
      try {
        List<HStoreFile> toCompactFiles = getFilesToCompact();
        HStoreFile compactedOutputFile = new HStoreFile(conf,
            this.compactionDir, encodedRegionName, familyName, -1);
        if (toCompactFiles.size() < 1 ||
            (toCompactFiles.size() == 1 &&
              !toCompactFiles.get(0).isReference())) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("nothing to compact for " + this.storeName);
          }
          if (deleteSequenceInfo && toCompactFiles.size() == 1) {
            toCompactFiles.get(0).writeInfo(fs, -1);
          }
          return;
        }
        
        if (!fs.mkdirs(curCompactStore)) {
          LOG.warn("Mkdir on " + curCompactStore.toString() + " failed");
        }
        
        // Compute the max-sequenceID seen in any of the to-be-compacted
        // TreeMaps if it hasn't been passed in to us.
        if (maxId == -1) {
          for (HStoreFile hsf: toCompactFiles) {
            long seqid = hsf.loadInfo(fs);
            if(seqid > 0) {
              if(seqid > maxId) {
                maxId = seqid;
              }
            }
          }
        }

        // Step through them, writing to the brand-new TreeMap
        MapFile.Writer compactedOut =
          compactedOutputFile.getWriter(this.fs, this.compression,
            this.bloomFilter);
        try {
          compact(compactedOut, toCompactFiles);
        } finally {
          compactedOut.close();
        }

        // Now, write out an HSTORE_LOGINFOFILE for the brand-new TreeMap.
        if((! deleteSequenceInfo) && maxId >= 0) {
          compactedOutputFile.writeInfo(fs, maxId);
        } else {
          compactedOutputFile.writeInfo(fs, -1);
        }

        // Write out a list of data files that we're replacing
        Path filesToReplace = new Path(curCompactStore, COMPACTION_TO_REPLACE);
        DataOutputStream out = new DataOutputStream(fs.create(filesToReplace));
        try {
          out.writeInt(toCompactFiles.size());
          for(HStoreFile hsf: toCompactFiles) {
            hsf.write(out);
          }
        } finally {
          out.close();
        }

        // Indicate that we're done.
        Path doneFile = new Path(curCompactStore, COMPACTION_DONE);
        (new DataOutputStream(fs.create(doneFile))).close();

        // Move the compaction into place.
        processReadyCompaction();
      } finally {
        // Clean up the parent -- the region dir in the compactions directory.
        if (this.fs.exists(curCompactStore.getParent())) {
          if (!this.fs.delete(curCompactStore.getParent())) {
            LOG.warn("Delete returned false deleting " +
              curCompactStore.getParent().toString());
          }
        }
      }
    }
  }
  
  /*
   * @return list of files to compact sorted so most recent comes first.
   */
  private List<HStoreFile> getFilesToCompact() {
    List<HStoreFile> filesToCompact = null;
    this.lock.obtainWriteLock();
    try {
      // Storefiles are keyed by sequence id.  The oldest file comes first.
      // We need to return out of here a List that has the newest file as
      // first.
      filesToCompact = new ArrayList<HStoreFile>(this.storefiles.values());
      Collections.reverse(filesToCompact);
    } finally {
      this.lock.releaseWriteLock();
    }
    return filesToCompact;
  }
  
  /*
   * Compact passed <code>toCompactFiles</code> into <code>compactedOut</code>. 
   * We create a new set of MapFile.Reader objects so we don't screw up 
   * the caching associated with the currently-loaded ones. Our
   * iteration-based access pattern is practically designed to ruin 
   * the cache.
   *
   * We work by opening a single MapFile.Reader for each file, and 
   * iterating through them in parallel.  We always increment the 
   * lowest-ranked one.  Updates to a single row/column will appear 
   * ranked by timestamp.  This allows us to throw out deleted values or
   * obsolete versions.
   * @param compactedOut
   * @param toCompactFiles
   * @throws IOException
   */
  void compact(final MapFile.Writer compactedOut,
      final List<HStoreFile> toCompactFiles)
  throws IOException {
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
          (hsf.isReference()? " " + hsf.getReference().toString(): ""));
        throw e;
      }
    }
    try {
      compact(compactedOut, rdrs);
    } finally {
      for (int i = 0; i < rdrs.length; i++) {
        if (rdrs[i] != null) {
          try {
            rdrs[i].close();
          } catch (IOException e) {
            LOG.warn("Exception closing reader", e);
          }
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
  
  void compact(final MapFile.Writer compactedOut,
      final Iterator<Entry<HStoreKey, byte []>> iterator,
      final MapFile.Reader reader)
  throws IOException {
    // Make an instance of a CompactionReader that wraps the iterator.
    CompactionReader cr = new CompactionReader() {
      public boolean next(WritableComparable key, Writable val)
          throws IOException {
        boolean result = false;
        while (iterator.hasNext()) {
          Entry<HStoreKey, byte []> e = iterator.next();
          HStoreKey hsk = e.getKey();
          if (familyName.equals(HStoreKey.extractFamily(hsk.getColumn()))) {
            ((HStoreKey)key).set(hsk);
            ((ImmutableBytesWritable)val).set(e.getValue());
            result = true;
            break;
          }
        }
        return result;
      }

      @SuppressWarnings("unused")
      public void reset() throws IOException {
        // noop.
      }
      
      @SuppressWarnings("unused")
      public void close() throws IOException {
        // noop.
      }
    };
    
    compact(compactedOut,
      new CompactionReader [] {cr, new MapFileCompactionReader(reader)});
  }
  
  void compact(final MapFile.Writer compactedOut,
      final CompactionReader [] rdrs)
  throws IOException {
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
      if (!isDeleted(sk, value, null, deletes) &&
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
  }

  /*
   * Check if this is cell is deleted.
   * If a memcache and a deletes, check key does not have an entry filled.
   * Otherwise, check value is not the <code>HGlobals.deleteBytes</code> value.
   * If passed value IS deleteBytes, then it is added to the passed
   * deletes map.
   * @param hsk
   * @param value
   * @param memcache Can be null.
   * @param deletes Map keyed by column with a value of timestamp. Can be null.
   * If non-null and passed value is HGlobals.deleteBytes, then we add to this
   * map.
   * @return True if this is a deleted cell.  Adds the passed deletes map if
   * passed value is HGlobals.deleteBytes.
  */
  private boolean isDeleted(final HStoreKey hsk, final byte [] value,
      final HMemcache memcache, final Map<Text, List<Long>> deletes) {
    if (memcache != null && memcache.isDeleted(hsk)) {
      return true;
    }
    List<Long> timestamps = (deletes == null)?
      null: deletes.get(hsk.getColumn());
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
   * 1) Acquiring the write-lock
   * 2) Figuring out what MapFiles are going to be replaced
   * 3) Moving the new compacted MapFile into place
   * 4) Unloading all the replaced MapFiles.
   * 5) Deleting all the old MapFile files.
   * 6) Loading the new TreeMap.
   * 7) Releasing the write-lock
   * </pre>
   */
  void processReadyCompaction() throws IOException {
    // 1. Acquiring the write-lock
    Path curCompactStore = HStoreFile.getHStoreDir(this.compactionDir,
        encodedRegionName, familyName);
    this.lock.obtainWriteLock();
    try {
      Path doneFile = new Path(curCompactStore, COMPACTION_DONE);
      if (!fs.exists(doneFile)) {
        // The last execution didn't finish the compaction, so there's nothing 
        // we can do.  We'll just have to redo it. Abandon it and return.
        LOG.warn("Redo failed compaction (missing 'done' file)");
        return;
      }

      // 2. Load in the files to be deleted.
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

      // 3. Moving the new MapFile into place.
      HStoreFile compactedFile = new HStoreFile(conf, this.compactionDir,
          encodedRegionName, familyName, -1);
      // obtainNewHStoreFile does its best to generate a filename that does not
      // currently exist.
      HStoreFile finalCompactedFile = HStoreFile.obtainNewHStoreFile(conf, dir,
          encodedRegionName, familyName, fs);
      if(LOG.isDebugEnabled()) {
        LOG.debug("moving " + compactedFile.toString() + " in " +
            this.compactionDir.toString() +
          " to " + finalCompactedFile.toString() + " in " + dir.toString());
      }
      if (!compactedFile.rename(this.fs, finalCompactedFile)) {
        LOG.error("Failed move of compacted file " +
          finalCompactedFile.toString());
        return;
      }

      // 4. and 5. Unload all the replaced MapFiles, close and delete.
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

        // 6. Loading the new TreeMap.
        Long orderVal = Long.valueOf(finalCompactedFile.loadInfo(fs));
        this.readers.put(orderVal,
            finalCompactedFile.getReader(this.fs, this.bloomFilter));
        this.storefiles.put(orderVal, finalCompactedFile);
      } catch (IOException e) {
        LOG.error("Failed replacing compacted files. Compacted file is " +
          finalCompactedFile.toString() + ".  Files replaced are " +
          toCompactFiles.toString() +
          " some of which may have been already removed", e);
      }
    } finally {
      // 7. Releasing the write-lock
      this.lock.releaseWriteLock();
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
    this.lock.obtainReadLock();
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
              if(HLogEdit.isDeleted(readval.get())) {
                break;
              }
              results.put(new Text(readcol), readval.get());
              readval = new ImmutableBytesWritable();
            } else if(key.getRow().compareTo(readkey.getRow()) > 0) {
              break;
            }
            
          } while(map.next(readkey, readval));
        }
      }
      
    } finally {
      this.lock.releaseReadLock();
    }
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
   * @param memcache Checked for deletions
   * @return values for the specified versions
   * @throws IOException
   */
  byte [][] get(HStoreKey key, int numVersions, final HMemcache memcache)
  throws IOException {
    if (numVersions <= 0) {
      throw new IllegalArgumentException("Number of versions must be > 0");
    }
    
    List<byte []> results = new ArrayList<byte []>();
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
    this.lock.obtainReadLock();
    try {
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
          if (!isDeleted(readkey, readval.get(), memcache, deletes)) {
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
            if (!isDeleted(readkey, readval.get(), memcache, deletes)) {
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
      this.lock.releaseReadLock();
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
  List<HStoreKey> getKeys(final HStoreKey origin, List<HStoreKey> allKeys,
      final int versions) throws IOException {
    
    List<HStoreKey> keys = allKeys;
    if (keys == null) {
      keys = new ArrayList<HStoreKey>();
    }
    // This code below is very close to the body of the get method.
    this.lock.obtainReadLock();
    try {
      MapFile.Reader[] maparray = getReaders();
      for(int i = maparray.length - 1; i >= 0; i--) {
        MapFile.Reader map = maparray[i];
        synchronized(map) {
          map.reset();
          ImmutableBytesWritable readval = new ImmutableBytesWritable();
          HStoreKey readkey = (HStoreKey)map.getClosest(origin, readval);
          if (readkey == null) {
            // map.getClosest returns null if the passed key is > than the
            // last key in the map file.  getClosest is a bit of a misnomer
            // since it returns exact match or the next closest key AFTER not
            // BEFORE.
            continue;
          }
          if (!readkey.matchesRowCol(origin)) {
            continue;
          }
          if (!isDeleted(readkey, readval.get(), null, null) &&
              !keys.contains(readkey)) {
            keys.add(new HStoreKey(readkey));
          }
          for (readval = new ImmutableBytesWritable();
              map.next(readkey, readval) &&
              readkey.matchesRowCol(origin);
              readval = new ImmutableBytesWritable()) {
            if (!isDeleted(readkey, readval.get(), null, null) &&
                !keys.contains(readkey)) {
              keys.add(new HStoreKey(readkey));
              if (versions != ALL_VERSIONS && keys.size() >= versions) {
                break;
              }
            }
          }
        }
      }
      return keys;
    } finally {
      this.lock.releaseReadLock();
    }
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
    
    this.lock.obtainReadLock();
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
      LOG.warn("Failed getting store size", e);
    } finally {
      this.lock.releaseReadLock();
    }
    return new HStoreSize(aggregateSize, maxSize, splitable);
  }
  
  /**
   * @return    Returns the number of map files currently in use
   */
  int countOfStoreFiles() {
    this.lock.obtainReadLock();
    try {
      return storefiles.size();
      
    } finally {
      this.lock.releaseReadLock();
    }
  }
  
  boolean hasReferences() {
    boolean result = false;
    this.lock.obtainReadLock();
    try {
        for (HStoreFile hsf: this.storefiles.values()) {
          if (hsf.isReference()) {
            break;
          }
        }
      
    } finally {
      this.lock.releaseReadLock();
    }
    return result;
  }
  
  //////////////////////////////////////////////////////////////////////////////
  // File administration
  //////////////////////////////////////////////////////////////////////////////

  /** Generate a random unique filename suffix */
  String obtainFileLabel(Path prefix) throws IOException {
    String testsuffix = String.valueOf(rand.nextInt(Integer.MAX_VALUE));
    Path testpath = new Path(prefix.toString() + testsuffix);
    while(fs.exists(testpath)) {
      testsuffix = String.valueOf(rand.nextInt(Integer.MAX_VALUE));
      testpath = new Path(prefix.toString() + testsuffix);
    }
    return testsuffix;
  }

  /**
   * Return a set of MapFile.Readers, one for each HStore file.
   * These should be closed after the user is done with them.
   */
  HInternalScannerInterface getScanner(long timestamp, Text targetCols[],
      Text firstRow) throws IOException {
    
    return new HStoreScanner(timestamp, targetCols, firstRow);
  }
  
  /** {@inheritDoc} */
  @Override
  public String toString() {
    return this.storeName;
  }

  //////////////////////////////////////////////////////////////////////////////
  // This class implements the HScannerInterface.
  // It lets the caller scan the contents of this HStore.
  //////////////////////////////////////////////////////////////////////////////
  
  class HStoreScanner extends HAbstractScanner {
    @SuppressWarnings("hiding")
    private MapFile.Reader[] readers;
    
    HStoreScanner(long timestamp, Text[] targetCols, Text firstRow)
    throws IOException {
      super(timestamp, targetCols);
      lock.obtainReadLock();
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
            LOG.error("Sub-scanner close", e);
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
                LOG.error("Scanner close", e);
              }
            }
          }
          
        } finally {
          lock.releaseReadLock();
          scannerClosed = true;
        }
      }
    }
  }
}
