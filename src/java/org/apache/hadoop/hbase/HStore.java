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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import org.onelab.filter.*;

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

  static final String COMPACTION_DIR = "compaction.tmp";
  static final String WORKING_COMPACTION = "compaction.inprogress";
  static final String COMPACTION_TO_REPLACE = "toreplace";    
  static final String COMPACTION_DONE = "done";
  
  private static final String BLOOMFILTER_FILE_NAME = "filter";

  Path dir;
  Text regionName;
  HColumnDescriptor family;
  Text familyName;
  SequenceFile.CompressionType compression;
  FileSystem fs;
  Configuration conf;
  Path mapdir;
  Path compactdir;
  Path loginfodir;
  Path filterDir;
  Filter bloomFilter;

  Integer compactLock = new Integer(0);
  Integer flushLock = new Integer(0);

  final HLocking lock = new HLocking();

  TreeMap<Long, MapFile.Reader> maps = new TreeMap<Long, MapFile.Reader>();
  TreeMap<Long, HStoreFile> mapFiles = new TreeMap<Long, HStoreFile>();

  Random rand = new Random();

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
   * @param regionName name of region
   * @param family name of column family
   * @param fs file system object
   * @param reconstructionLog existing log file to apply if any
   * @param conf configuration object
   * @throws IOException
   */
  HStore(Path dir, Text regionName, HColumnDescriptor family, 
      FileSystem fs, Path reconstructionLog, Configuration conf)
  throws IOException {  
    this.dir = dir;
    this.regionName = regionName;
    this.family = family;
    this.familyName = HStoreKey.extractFamily(this.family.getName());
    this.compression = SequenceFile.CompressionType.NONE;
    
    if(family.getCompression() != HColumnDescriptor.CompressionType.NONE) {
      if(family.getCompression() == HColumnDescriptor.CompressionType.BLOCK) {
        this.compression = SequenceFile.CompressionType.BLOCK;
        
      } else if(family.getCompression() == HColumnDescriptor.CompressionType.RECORD) {
        this.compression = SequenceFile.CompressionType.RECORD;
        
      } else {
        assert(false);
      }
    }
    
    this.fs = fs;
    this.conf = conf;

    this.mapdir = HStoreFile.getMapDir(dir, regionName, familyName);
    fs.mkdirs(mapdir);
    this.loginfodir = HStoreFile.getInfoDir(dir, regionName, familyName);
    fs.mkdirs(loginfodir);
    
    if(family.bloomFilter == null) {
      this.filterDir = null;
      this.bloomFilter = null;
      
    } else {
      this.filterDir = HStoreFile.getFilterDir(dir, regionName, familyName);
      fs.mkdirs(filterDir);
      loadOrCreateBloomFilter();
    }

    if(LOG.isDebugEnabled()) {
      LOG.debug("starting HStore for " + regionName + "/"+ familyName);
    }
    
    // Either restart or get rid of any leftover compaction work.  Either way, 
    // by the time processReadyCompaction() returns, we can get rid of the 
    // existing compaction-dir.

    this.compactdir = new Path(dir, COMPACTION_DIR);
    Path curCompactStore =
      HStoreFile.getHStoreDir(compactdir, regionName, familyName);
    if(fs.exists(curCompactStore)) {
      processReadyCompaction();
      fs.delete(curCompactStore);
    }

    // Go through the 'mapdir' and 'loginfodir' together, make sure that all 
    // MapFiles are in a reliable state.  Every entry in 'mapdir' must have a 
    // corresponding one in 'loginfodir'. Without a corresponding log info
    // file, the entry in 'mapdir' must be deleted.
    Vector<HStoreFile> hstoreFiles 
      = HStoreFile.loadHStoreFiles(conf, dir, regionName, familyName, fs);
    for(HStoreFile hsf: hstoreFiles) {
      mapFiles.put(Long.valueOf(hsf.loadInfo(fs)), hsf);
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

    doReconstructionLog(reconstructionLog, maxSeqID);
    
    // Compact all the MapFiles into a single file.  The resulting MapFile 
    // should be "timeless"; that is, it should not have an associated seq-ID, 
    // because all log messages have been reflected in the TreeMaps at this
    // point.
    if(mapFiles.size() >= 1) {
      compactHelper(true);
    }

    // Finally, start up all the map readers! (There should be just one at this 
    // point, as we've compacted them all.)
    if(LOG.isDebugEnabled()) {
      LOG.debug("starting map readers");
    }
    for(Map.Entry<Long, HStoreFile> e: mapFiles.entrySet()) {
      // TODO - is this really necessary?  Don't I do this inside compact()?
      maps.put(e.getKey(),
        getMapFileReader(e.getValue().getMapFilePath().toString()));
    }
    
    LOG.info("HStore online for " + this.regionName + "/" + this.familyName);
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
    if (LOG.isDebugEnabled()) {
      LOG.debug("reading reconstructionLog");
    }
    if (reconstructionLog == null || !fs.exists(reconstructionLog)) {
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
          continue;
        }
        // Check this edit is for me. Also, guard against writing
        // METACOLUMN info such as HBASE::CACHEFLUSH entries
        Text column = val.getColumn();
        if (column.equals(HLog.METACOLUMN)
            || !key.getRegionName().equals(this.regionName)
            || !HStoreKey.extractFamily(column).equals(this.familyName)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Passing on edit " + key.getRegionName() + ", "
                + column.toString() + ": " + new String(val.getVal())
                + ", my region: " + this.regionName + ", my column: "
                + this.familyName);
          }
          continue;
        }
        HStoreKey k = new HStoreKey(key.getRow(), column, val.getTimestamp());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Applying edit " + k.toString() + "=" +
            new String(val.getVal(), UTF8_ENCODING));
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
        LOG.debug("loading bloom filter for " + family.getName());
      }

      switch(family.bloomFilter.filterType) {
      
      case BloomFilterDescriptor.BLOOMFILTER:
        bloomFilter = new BloomFilter();
        break;
        
      case BloomFilterDescriptor.COUNTING_BLOOMFILTER:
        bloomFilter = new CountingBloomFilter();
        break;
        
      case BloomFilterDescriptor.RETOUCHED_BLOOMFILTER:
        bloomFilter = new RetouchedBloomFilter();
      }
      FSDataInputStream in = fs.open(filterFile);
      bloomFilter.readFields(in);
      fs.close();
      
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("creating bloom filter for " + family.getName());
      }

      switch(family.bloomFilter.filterType) {
      
      case BloomFilterDescriptor.BLOOMFILTER:
        bloomFilter = new BloomFilter(family.bloomFilter.vectorSize,
            family.bloomFilter.nbHash);
        break;
        
      case BloomFilterDescriptor.COUNTING_BLOOMFILTER:
        bloomFilter = new CountingBloomFilter(family.bloomFilter.vectorSize,
            family.bloomFilter.nbHash);
        break;
        
      case BloomFilterDescriptor.RETOUCHED_BLOOMFILTER:
        bloomFilter = new RetouchedBloomFilter(family.bloomFilter.vectorSize,
            family.bloomFilter.nbHash);
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
      LOG.debug("flushing bloom filter for " + family.getName());
    }
    FSDataOutputStream out =
      fs.create(new Path(filterDir, BLOOMFILTER_FILE_NAME));
    
    bloomFilter.write(out);
    out.close();
    if (LOG.isDebugEnabled()) {
      LOG.debug("flushed bloom filter for " + family.getName());
    }
  }

  /** Generates a bloom filter key from the row and column keys */
  Key getBloomFilterKey(HStoreKey k) {
    StringBuilder s = new StringBuilder(k.getRow().toString());
    s.append(k.getColumn().toString());
    
    byte[] bytes = null;
    try {
      bytes = s.toString().getBytes(HConstants.UTF8_ENCODING);
      
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
      assert(false);
    }
    return new Key(bytes);
  }

  /** 
   * Extends MapFile.Reader and overrides get and getClosest to consult the
   * bloom filter before attempting to read from disk.
   */
  private class BloomFilterReader extends MapFile.Reader {
    
    BloomFilterReader(FileSystem fs, String dirName, Configuration conf)
    throws IOException {
      super(fs, dirName, conf);
    }

    /** {@inheritDoc} */
    @Override
    public Writable get(WritableComparable key, Writable val) throws IOException {
      // Note - the key being passed to us is always a HStoreKey
      
      if(bloomFilter.membershipTest(getBloomFilterKey((HStoreKey)key))) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("bloom filter reported that key exists");
        }
        return super.get(key, val);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("bloom filter reported that key does not exist");
      }
      return null;
    }

    /** {@inheritDoc} */
    @Override
    public WritableComparable getClosest(WritableComparable key, Writable val)
    throws IOException {
      // Note - the key being passed to us is always a HStoreKey
      
      if(bloomFilter.membershipTest(getBloomFilterKey((HStoreKey)key))) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("bloom filter reported that key exists");
        }
        return super.getClosest(key, val);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("bloom filter reported that key does not exist");
      }
      return null;
    }
  }

  /**
   * Extends MapFile.Writer and overrides append, so that whenever a MapFile
   * is written to, the key is added to the bloom filter.
   */
  private class BloomFilterWriter extends MapFile.Writer {

    @SuppressWarnings("unchecked")
    BloomFilterWriter(Configuration conf, FileSystem fs, String dirName,
        Class keyClass, Class valClass, SequenceFile.CompressionType compression)
    throws IOException {
      super(conf, fs, dirName, keyClass, valClass, compression);
    }

    /** {@inheritDoc} */
    @Override
    public void append(WritableComparable key, Writable val) throws IOException {
      // Note - the key being passed to us is always a HStoreKey

      bloomFilter.add(getBloomFilterKey((HStoreKey)key));
      super.append(key, val);
    }
  }
  
  /**
   * Get a MapFile reader
   * This allows us to substitute a BloomFilterReader if a bloom filter is enabled
   */
  MapFile.Reader getMapFileReader(String dirName) throws IOException {
    if(bloomFilter != null) {
      return new BloomFilterReader(fs, dirName, conf);
    }
    return new MapFile.Reader(fs, dirName, conf);
  }
  
  /**
   * Get a MapFile writer
   * This allows us to substitute a BloomFilterWriter if a bloom filter is
   * enabled
   * 
   * @param dirName Directory with store files.
   * @return Map file.
   * @throws IOException
   */
  MapFile.Writer getMapFileWriter(String dirName) throws IOException {
    if (bloomFilter != null) {
      return new BloomFilterWriter(conf, fs, dirName, HStoreKey.class,
        ImmutableBytesWritable.class, compression);
    }
    return new MapFile.Writer(conf, fs, dirName, HStoreKey.class,
        ImmutableBytesWritable.class, compression);
  }
  
  //////////////////////////////////////////////////////////////////////////////
  // End bloom filters
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Turn off all the MapFile readers
   * 
   * @throws IOException
   */
  void close() throws IOException {
    LOG.info("closing HStore for " + this.regionName + "/" + this.familyName);
    this.lock.obtainWriteLock();
    try {
      for (MapFile.Reader map: maps.values()) {
        map.close();
      }
      maps.clear();
      mapFiles.clear();
      
      LOG.info("HStore closed for " + this.regionName + "/" + this.familyName);
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
   * @param inputCache          - memcache to flush
   * @param logCacheFlushId     - flush sequence number
   * @return - Vector of all the HStoreFiles in use
   * @throws IOException
   */
  Vector<HStoreFile> flushCache(TreeMap<HStoreKey, byte []> inputCache,
      long logCacheFlushId)
  throws IOException {
    return flushCacheHelper(inputCache, logCacheFlushId, true);
  }
  
  Vector<HStoreFile> flushCacheHelper(TreeMap<HStoreKey, byte []> inputCache,
      long logCacheFlushId, boolean addToAvailableMaps)
  throws IOException {
    
    synchronized(flushLock) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("flushing HStore " + this.regionName + "/" + this.familyName);
      }
      
      // A. Write the TreeMap out to the disk

      HStoreFile flushedFile 
        = HStoreFile.obtainNewHStoreFile(conf, dir, regionName, familyName, fs);
      
      Path mapfile = flushedFile.getMapFilePath();
      if(LOG.isDebugEnabled()) {
        LOG.debug("map file is: " + mapfile.toString());
      }
      
      MapFile.Writer out = getMapFileWriter(mapfile.toString());
      try {
        for (Map.Entry<HStoreKey, byte []> es: inputCache.entrySet()) {
          HStoreKey curkey = es.getKey();
          if (this.familyName.equals(HStoreKey.extractFamily(curkey.getColumn()))) {
            out.append(curkey, new ImmutableBytesWritable(es.getValue()));
          }
        }
        if(LOG.isDebugEnabled()) {
          LOG.debug("HStore " + this.regionName + "/" + this.familyName + " flushed");
        }
        
      } finally {
        out.close();
      }

      // B. Write out the log sequence number that corresponds to this output
      // MapFile.  The MapFile is current up to and including the log seq num.

      if(LOG.isDebugEnabled()) {
        LOG.debug("writing log cache flush id");
      }
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
          maps.put(flushid, getMapFileReader(mapfile.toString()));
          mapFiles.put(flushid, flushedFile);
          if(LOG.isDebugEnabled()) {
            LOG.debug("HStore available for " + this.regionName + "/"
                + this.familyName + " flush id=" + logCacheFlushId);
          }
        } finally {
          this.lock.releaseWriteLock();
        }
      }
      return getAllMapFiles();
    }
  }

  /**
   * @return - vector of all the HStore files in use
   */
  Vector<HStoreFile> getAllMapFiles() {
    this.lock.obtainReadLock();
    try {
      return new Vector<HStoreFile>(mapFiles.values());
      
    } finally {
      this.lock.releaseReadLock();
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // Compaction
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Compact the back-HStores.  This method may take some time, so the calling 
   * thread must be able to block for long periods.
   * 
   * During this time, the HStore can work as usual, getting values from
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
   * 
   * @throws IOException
   */
  void compact() throws IOException {
    compactHelper(false);
  }
  
  void compactHelper(boolean deleteSequenceInfo) throws IOException {
    synchronized(compactLock) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("started compaction of " + this.regionName + "/" + this.familyName);
      }
      
      Path curCompactStore = HStoreFile.getHStoreDir(compactdir, regionName, familyName);
      fs.mkdirs(curCompactStore);
      
      try {
        
        // Grab a list of files to compact.
        
        Vector<HStoreFile> toCompactFiles = null;
        this.lock.obtainWriteLock();
        try {
          toCompactFiles = new Vector<HStoreFile>(mapFiles.values());
          
        } finally {
          this.lock.releaseWriteLock();
        }

        // Compute the max-sequenceID seen in any of the to-be-compacted TreeMaps

        long maxSeenSeqID = -1;
        for (HStoreFile hsf: toCompactFiles) {
          long seqid = hsf.loadInfo(fs);
          if(seqid > 0) {
            if(seqid > maxSeenSeqID) {
              maxSeenSeqID = seqid;
            }
          }
        }
        if(LOG.isDebugEnabled()) {
          LOG.debug("max sequence id: " + maxSeenSeqID);
        }
        
        HStoreFile compactedOutputFile 
          = new HStoreFile(conf, compactdir, regionName, familyName, -1);
        
        if(toCompactFiles.size() == 1) {
          if(LOG.isDebugEnabled()) {
            LOG.debug("nothing to compact for " + this.regionName + "/" + this.familyName);
          }
          
          HStoreFile hsf = toCompactFiles.elementAt(0);
          if(hsf.loadInfo(fs) == -1) {
            return;
          }
        }

        // Step through them, writing to the brand-new TreeMap
        MapFile.Writer compactedOut =
          getMapFileWriter(compactedOutputFile.getMapFilePath().toString());
        try {

          // We create a new set of MapFile.Reader objects so we don't screw up 
          // the caching associated with the currently-loaded ones.
          //
          // Our iteration-based access pattern is practically designed to ruin 
          // the cache.
          //
          // We work by opening a single MapFile.Reader for each file, and 
          // iterating through them in parallel.  We always increment the 
          // lowest-ranked one.  Updates to a single row/column will appear 
          // ranked by timestamp.  This allows us to throw out deleted values or
          // obsolete versions.

          MapFile.Reader[] readers = new MapFile.Reader[toCompactFiles.size()];
          HStoreKey[] keys = new HStoreKey[toCompactFiles.size()];
          ImmutableBytesWritable[] vals =
            new ImmutableBytesWritable[toCompactFiles.size()];
          boolean[] done = new boolean[toCompactFiles.size()];
          int pos = 0;
          for(Iterator<HStoreFile> it = toCompactFiles.iterator(); it.hasNext(); ) {
            HStoreFile hsf = it.next();
            readers[pos] = getMapFileReader(hsf.getMapFilePath().toString());
            keys[pos] = new HStoreKey();
            vals[pos] = new ImmutableBytesWritable();
            done[pos] = false;
            pos++;
          }

          // Now, advance through the readers in order.  This will have the
          // effect of a run-time sort of the entire dataset.

          if(LOG.isDebugEnabled()) {
            LOG.debug("processing HStoreFile readers");
          }
          
          int numDone = 0;
          for(int i = 0; i < readers.length; i++) {
            readers[i].reset();
            done[i] = ! readers[i].next(keys[i], vals[i]);
            if(done[i]) {
              numDone++;
            }
          }
          
          int timesSeen = 0;
          Text lastRow = new Text();
          Text lastColumn = new Text();
          while(numDone < done.length) {

            // Find the reader with the smallest key

            int smallestKey = -1;
            for(int i = 0; i < readers.length; i++) {
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
            }
            
            if(timesSeen <= family.getMaxVersions()) {

              // Keep old versions until we have maxVersions worth.
              // Then just skip them.

              if(sk.getRow().getLength() != 0
                  && sk.getColumn().getLength() != 0) {
                
                // Only write out objects which have a non-zero length key and value

                compactedOut.append(sk, vals[smallestKey]);
              }
              
            }

            //TODO: I don't know what to do about deleted values.  I currently 
            // include the fact that the item was deleted as a legitimate 
            // "version" of the data.  Maybe it should just drop the deleted val?

            // Update last-seen items

            lastRow.set(sk.getRow());
            lastColumn.set(sk.getColumn());

            // Advance the smallest key.  If that reader's all finished, then 
            // mark it as done.

            if(! readers[smallestKey].next(keys[smallestKey], vals[smallestKey])) {
              done[smallestKey] = true;
              readers[smallestKey].close();
              numDone++;
            }
          }
          
          if(LOG.isDebugEnabled()) {
            LOG.debug("all HStores processed");
          }
          
        } finally {
          compactedOut.close();
        }

        if(LOG.isDebugEnabled()) {
          LOG.debug("writing new compacted HStore");
        }

        // Now, write out an HSTORE_LOGINFOFILE for the brand-new TreeMap.

        if((! deleteSequenceInfo) && maxSeenSeqID >= 0) {
          compactedOutputFile.writeInfo(fs, maxSeenSeqID);
          
        } else {
          compactedOutputFile.writeInfo(fs, -1);
        }

        // Write out a list of data files that we're replacing

        Path filesToReplace = new Path(curCompactStore, COMPACTION_TO_REPLACE);
        DataOutputStream out = new DataOutputStream(fs.create(filesToReplace));
        try {
          out.writeInt(toCompactFiles.size());
          for(Iterator<HStoreFile> it = toCompactFiles.iterator(); it.hasNext(); ) {
            HStoreFile hsf = it.next();
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
        
        if(LOG.isDebugEnabled()) {
          LOG.debug("compaction complete for " + this.regionName + "/" + this.familyName);
        }

      } finally {
        fs.delete(compactdir);
      }
    }
  }

  /**
   * It's assumed that the compactLock  will be acquired prior to calling this 
   * method!  Otherwise, it is not thread-safe!
   *
   * It works by processing a compaction that's been written to disk.
   * 
   * It is usually invoked at the end of a compaction, but might also be invoked
   * at HStore startup, if the prior execution died midway through.
   */
  void processReadyCompaction() throws IOException {

    // Move the compacted TreeMap into place.
    // That means:
    // 1) Acquiring the write-lock
    // 2) Figuring out what MapFiles are going to be replaced
    // 3) Unloading all the replaced MapFiles.
    // 4) Deleting all the old MapFile files.
    // 5) Moving the new MapFile into place
    // 6) Loading the new TreeMap.
    // 7) Releasing the write-lock

    // 1. Acquiring the write-lock


    Path curCompactStore = HStoreFile.getHStoreDir(compactdir, regionName, familyName);
    this.lock.obtainWriteLock();
    try {
      Path doneFile = new Path(curCompactStore, COMPACTION_DONE);
      if(! fs.exists(doneFile)) {
        
        // The last execution didn't finish the compaction, so there's nothing 
        // we can do.  We'll just have to redo it. Abandon it and return.
        
        return;
      }

      // OK, there's actually compaction work that needs to be put into place.

      if(LOG.isDebugEnabled()) {
        LOG.debug("compaction starting");
      }
      
      // 2. Load in the files to be deleted.
      //    (Figuring out what MapFiles are going to be replaced)
      
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

      if(LOG.isDebugEnabled()) {
        LOG.debug("loaded files to be deleted");
      }
      
      // 3. Unload all the replaced MapFiles.
      
      Iterator<HStoreFile> it2 = mapFiles.values().iterator();
      for(Iterator<MapFile.Reader> it = maps.values().iterator(); it.hasNext(); ) {
        MapFile.Reader curReader = it.next();
        HStoreFile curMapFile = it2.next();
        if(toCompactFiles.contains(curMapFile)) {
          curReader.close();
          it.remove();
        }
      }
      
      for(Iterator<HStoreFile> it = mapFiles.values().iterator(); it.hasNext(); ) {
        HStoreFile curMapFile = it.next();
        if(toCompactFiles.contains(curMapFile)) {
          it.remove();
        }
      }

      if(LOG.isDebugEnabled()) {
        LOG.debug("unloaded existing MapFiles");
      }
      
      // What if we crash at this point?  No big deal; we will restart
      // processReadyCompaction(), and nothing has been lost.

      // 4. Delete all the old files, no longer needed
      
      for(Iterator<HStoreFile> it = toCompactFiles.iterator(); it.hasNext(); ) {
        HStoreFile hsf = it.next();
        fs.delete(hsf.getMapFilePath());
        fs.delete(hsf.getInfoFilePath());
      }

      if(LOG.isDebugEnabled()) {
        LOG.debug("old files deleted");
      }
      
      // What if we fail now?  The above deletes will fail silently. We'd better
      // make sure not to write out any new files with the same names as 
      // something we delete, though.

      // 5. Moving the new MapFile into place
      
      if(LOG.isDebugEnabled()) {
        LOG.debug("moving new MapFile into place");
      }
      
      HStoreFile compactedFile 
        = new HStoreFile(conf, compactdir, regionName, familyName, -1);
      
      HStoreFile finalCompactedFile 
        = HStoreFile.obtainNewHStoreFile(conf, dir, regionName, familyName, fs);
      
      fs.rename(compactedFile.getMapFilePath(), finalCompactedFile.getMapFilePath());
      
      // Fail here?  No problem.
      
      fs.rename(compactedFile.getInfoFilePath(), finalCompactedFile.getInfoFilePath());

      // Fail here?  No worries.
      
      Long orderVal = Long.valueOf(finalCompactedFile.loadInfo(fs));

      // 6. Loading the new TreeMap.
      
      if(LOG.isDebugEnabled()) {
        LOG.debug("loading new TreeMap");
      }
      
      mapFiles.put(orderVal, finalCompactedFile);
      maps.put(orderVal, getMapFileReader(
          finalCompactedFile.getMapFilePath().toString()));
      
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
      MapFile.Reader[] maparray 
        = maps.values().toArray(new MapFile.Reader[maps.size()]);
      for (int i = maparray.length - 1; i >= 0; i--) {
        MapFile.Reader map = maparray[i];
        synchronized(map) {
          map.reset();
          ImmutableBytesWritable readval = new ImmutableBytesWritable();
          HStoreKey readkey = (HStoreKey)map.getClosest(key, readval);
          do {
            Text readcol = readkey.getColumn();
            if (results.get(readcol) == null
                && key.matchesWithoutColumn(readkey)) {
              if(readval.equals(HConstants.DELETE_BYTES)) {
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

  /**
   * Get the value for the indicated HStoreKey.  Grab the target value and the 
   * previous 'numVersions-1' values, as well.
   *
   * If 'numVersions' is negative, the method returns all available versions.
   */
  byte [][] get(HStoreKey key, int numVersions) throws IOException {
    if (numVersions <= 0) {
      throw new IllegalArgumentException("Number of versions must be > 0");
    }
    
    List<byte []> results = new ArrayList<byte []>();
    this.lock.obtainReadLock();
    try {
      MapFile.Reader[] maparray 
        = maps.values().toArray(new MapFile.Reader[maps.size()]);
      
      for(int i = maparray.length-1; i >= 0; i--) {
        MapFile.Reader map = maparray[i];

        synchronized(map) {
          ImmutableBytesWritable readval = new ImmutableBytesWritable();
          map.reset();
          HStoreKey readkey = (HStoreKey)map.getClosest(key, readval);
          if (readkey == null) {
            // map.getClosest returns null if the passed key is > than the
            // last key in the map file.  getClosest is a bit of a misnomer
            // since it returns exact match or the next closest key AFTER not
            // BEFORE.
            continue;
          }
          if (readkey.matchesRowCol(key)) {
            if(readval.equals(HConstants.DELETE_BYTES)) {
              break;
            }
            results.add(readval.get());
            readval = new ImmutableBytesWritable();
            while(map.next(readkey, readval) && readkey.matchesRowCol(key)) {
              if ((numVersions > 0 && (results.size() >= numVersions))
                  || readval.equals(HConstants.DELETE_BYTES)) {
                break;
              }
              results.add(readval.get());
              readval = new ImmutableBytesWritable();
            }
          }
        }
        if(results.size() >= numVersions) {
          break;
        }
      }

      return results.size() == 0 ?
        null : ImmutableBytesWritable.toArray(results);
    } finally {
      this.lock.releaseReadLock();
    }
  }
  
  /**
   * Gets the size of the largest MapFile and its mid key.
   * 
   * @param midKey      - the middle key for the largest MapFile
   * @return            - size of the largest MapFile
   */
  long getLargestFileSize(Text midKey) {
    long maxSize = 0L;
    if (this.mapFiles.size() <= 0) {
      return maxSize;
    }
    
    this.lock.obtainReadLock();
    try {
      Long mapIndex = Long.valueOf(0L);
      // Iterate through all the MapFiles
      for(Map.Entry<Long, HStoreFile> e: mapFiles.entrySet()) {
        HStoreFile curHSF = e.getValue();
        long size = fs.getFileStatus(
          new Path(curHSF.getMapFilePath(), MapFile.DATA_FILE_NAME)).getLen();
        if(size > maxSize) {              // This is the largest one so far
          maxSize = size;
          mapIndex = e.getKey();
        }
      }

      MapFile.Reader r = maps.get(mapIndex);
      midKey.set(((HStoreKey)r.midKey()).getRow());
    } catch(IOException e) {
      LOG.warn(e);
    } finally {
      this.lock.releaseReadLock();
    }
    return maxSize;
  }
  
  /**
   * @return    Returns the number of map files currently in use
   */
  int getNMaps() {
    this.lock.obtainReadLock();
    try {
      return maps.size();
      
    } finally {
      this.lock.releaseReadLock();
    }
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

  //////////////////////////////////////////////////////////////////////////////
  // This class implements the HScannerInterface.
  // It lets the caller scan the contents of this HStore.
  //////////////////////////////////////////////////////////////////////////////
  
  class HStoreScanner extends HAbstractScanner {
    private MapFile.Reader[] readers;
    
    HStoreScanner(long timestamp, Text[] targetCols, Text firstRow)
        throws IOException {
      
      super(timestamp, targetCols);

      lock.obtainReadLock();
      try {
        this.readers = new MapFile.Reader[mapFiles.size()];
        
        // Most recent map file should be first
        
        int i = readers.length - 1;
        for(Iterator<HStoreFile> it = mapFiles.values().iterator(); it.hasNext(); ) {
          HStoreFile curHSF = it.next();
          readers[i--] = getMapFileReader(curHSF.getMapFilePath().toString());
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
        LOG.error(ex);
        close();
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
      ImmutableBytesWritable ibw = new ImmutableBytesWritable();
      if (!readers[i].next(keys[i], ibw)) {
        closeSubScanner(i);
        return false;
      }
      vals[i] = ibw.get();
      return true;
    }
    
    /** Close down the indicated reader. */
    @Override
    void closeSubScanner(int i) {
      try {
        if(readers[i] != null) {
          try {
            readers[i].close();
            
          } catch(IOException e) {
            LOG.error(e);
          }
        }
        
      } finally {
        readers[i] = null;
        keys[i] = null;
        vals[i] = null;
      }
    }

    /** Shut it down! */
    @Override
    public void close() {
      if(! scannerClosed) {
        try {
          for(int i = 0; i < readers.length; i++) {
            if(readers[i] != null) {
              try {
                readers[i].close();
                
              } catch(IOException e) {
                LOG.error(e);
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
