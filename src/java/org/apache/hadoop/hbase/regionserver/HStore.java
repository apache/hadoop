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
package org.apache.hadoop.hbase.regionserver;

import java.io.EOFException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.io.BloomFilterMapFile;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.HBaseMapFile;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.io.MapFile;
import org.apache.hadoop.hbase.io.SequenceFile;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;

/**
 * HStore maintains a bunch of data files.  It is responsible for maintaining 
 * the memory/file hierarchy and for periodic flushes to disk and compacting 
 * edits to the file.
 *
 * Locking and transactions are handled at a higher level.  This API should not 
 * be called directly by any writer, but rather by an HRegion manager.
 */
public class HStore implements HConstants {
  static final Log LOG = LogFactory.getLog(HStore.class);

  /*
   * Regex that will work for straight filenames and for reference names.
   * If reference, then the regex has more than just one group.  Group 1 is
   * this files id.  Group 2 the referenced region name, etc.
   */
  private static final Pattern REF_NAME_PARSER =
    Pattern.compile("^(\\d+)(?:\\.(.+))?$");
  
  protected final Memcache memcache;
  private final Path basedir;
  private final HRegionInfo info;
  private final HColumnDescriptor family;
  private final SequenceFile.CompressionType compression;
  final FileSystem fs;
  private final HBaseConfiguration conf;
  // ttl in milliseconds.
  protected long ttl;
  private long majorCompactionTime;
  private int maxFilesToCompact;
  private final long desiredMaxFileSize;
  private volatile long storeSize;

  private final Integer flushLock = new Integer(0);

  final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  final byte [] storeName;
  private final String storeNameStr;

  /*
   * Sorted Map of readers keyed by sequence id (Most recent should be last in
   * in list).
   */
  private final SortedMap<Long, HStoreFile> storefiles =
    Collections.synchronizedSortedMap(new TreeMap<Long, HStoreFile>());
  
  /*
   * Sorted Map of readers keyed by sequence id (Most recent is last in list).
   */
  private final SortedMap<Long, MapFile.Reader> readers =
    new TreeMap<Long, MapFile.Reader>();

  // The most-recent log-seq-ID that's present.  The most-recent such ID means
  // we can ignore all log messages up to and including that ID (because they're
  // already reflected in the TreeMaps).
  private volatile long maxSeqId;
  
  private final Path compactionDir;
  private final Integer compactLock = new Integer(0);
  private final int compactionThreshold;
  
  // All access must be synchronized.
  private final CopyOnWriteArraySet<ChangedReadersObserver> changedReaderObservers =
    new CopyOnWriteArraySet<ChangedReadersObserver>();

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
   * @param basedir qualified path under which the region directory lives
   * @param info HRegionInfo for this region
   * @param family HColumnDescriptor for this column
   * @param fs file system object
   * @param reconstructionLog existing log file to apply if any
   * @param conf configuration object
   * @param reporter Call on a period so hosting server can report we're
   * making progress to master -- otherwise master might think region deploy
   * failed.  Can be null.
   * @throws IOException
   */
  protected HStore(Path basedir, HRegionInfo info, HColumnDescriptor family,
      FileSystem fs, Path reconstructionLog, HBaseConfiguration conf,
      final Progressable reporter)
  throws IOException {  
    this.basedir = basedir;
    this.info = info;
    this.family = family;
    this.fs = fs;
    this.conf = conf;
    // getTimeToLive returns ttl in seconds.  Convert to milliseconds.
    this.ttl = family.getTimeToLive();
    if (ttl != HConstants.FOREVER) {
      this.ttl *= 1000;
    }
    this.memcache = new Memcache(this.ttl, info);
    this.compactionDir = HRegion.getCompactionDir(basedir);
    this.storeName = Bytes.toBytes(this.info.getEncodedName() + "/" +
      Bytes.toString(this.family.getName()));
    this.storeNameStr = Bytes.toString(this.storeName);

    // By default, we compact if an HStore has more than
    // MIN_COMMITS_FOR_COMPACTION map files
    this.compactionThreshold =
      conf.getInt("hbase.hstore.compactionThreshold", 3);
    
    // By default we split region if a file > DEFAULT_MAX_FILE_SIZE.
    long maxFileSize = info.getTableDesc().getMaxFileSize();
    if (maxFileSize == HConstants.DEFAULT_MAX_FILE_SIZE) {
      maxFileSize = conf.getLong("hbase.hregion.max.filesize",
        HConstants.DEFAULT_MAX_FILE_SIZE);
    }
    this.desiredMaxFileSize = maxFileSize;

    this.majorCompactionTime =
      conf.getLong(HConstants.MAJOR_COMPACTION_PERIOD, 86400000);
    if (family.getValue(HConstants.MAJOR_COMPACTION_PERIOD) != null) {
      String strCompactionTime =
        family.getValue(HConstants.MAJOR_COMPACTION_PERIOD);
      this.majorCompactionTime = (new Long(strCompactionTime)).longValue();
    }

    this.maxFilesToCompact = conf.getInt("hbase.hstore.compaction.max", 10);
    this.storeSize = 0L;

    if (family.getCompression() == HColumnDescriptor.CompressionType.BLOCK) {
      this.compression = SequenceFile.CompressionType.BLOCK;
    } else if (family.getCompression() ==
      HColumnDescriptor.CompressionType.RECORD) {
      this.compression = SequenceFile.CompressionType.RECORD;
    } else {
      this.compression = SequenceFile.CompressionType.NONE;
    }
    
    Path mapdir = checkdir(HStoreFile.getMapDir(basedir, info.getEncodedName(),
        family.getName()));
    Path infodir = checkdir(HStoreFile.getInfoDir(basedir, info.getEncodedName(),
        family.getName()));
    
    // Go through the 'mapdir' and 'infodir' together, make sure that all 
    // MapFiles are in a reliable state.  Every entry in 'mapdir' must have a 
    // corresponding one in 'loginfodir'. Without a corresponding log info
    // file, the entry in 'mapdir' must be deleted.
    // loadHStoreFiles also computes the max sequence id internally.
    this.maxSeqId = -1L;
    this.storefiles.putAll(loadHStoreFiles(infodir, mapdir));
    if (LOG.isDebugEnabled() && this.storefiles.size() > 0) {
      LOG.debug("Loaded " + this.storefiles.size() + " file(s) in hstore " +
        Bytes.toString(this.storeName) + ", max sequence id " + this.maxSeqId);
    }
    
    // Do reconstruction log.
    runReconstructionLog(reconstructionLog, this.maxSeqId, reporter);
    
    // Finally, start up all the map readers!
    setupReaders();
  }
  
  /*
   * Setup the mapfile readers for this store. There could be more than one
   * since we haven't compacted yet.
   * @throws IOException
   */
  private void setupReaders() throws IOException {
    boolean first = true;
    for(Map.Entry<Long, HStoreFile> e: this.storefiles.entrySet()) {
      MapFile.Reader r = null;
      if (first) {
        // Use a block cache (if configured) for the first reader only
        // so as to control memory usage.
        r = e.getValue().getReader(this.fs, this.family.isBloomfilter(),
          family.isBlockCacheEnabled());
        first = false;
      } else {
        r = e.getValue().getReader(this.fs, this.family.isBloomfilter(),
            false);
      }
      this.readers.put(e.getKey(), r);
    }
  }

  /*
   * @param dir If doesn't exist, create it.
   * @return Passed <code>dir</code>.
   * @throws IOException
   */
  private Path checkdir(final Path dir) throws IOException {
    if (!fs.exists(dir)) {
      fs.mkdirs(dir);
    }
    return dir;
  }

  HColumnDescriptor getFamily() {
    return this.family;
  }
  
  long getMaxSequenceId() {
    return this.maxSeqId;
  }
  
  /*
   * Run reconstuction log
   * @param reconstructionLog
   * @param msid
   * @param reporter
   * @throws IOException
   */
  private void runReconstructionLog(final Path reconstructionLog,
    final long msid, final Progressable reporter)
  throws IOException {
    try {
      doReconstructionLog(reconstructionLog, msid, reporter);
    } catch (EOFException e) {
      // Presume we got here because of lack of HADOOP-1700; for now keep going
      // but this is probably not what we want long term.  If we got here there
      // has been data-loss
      LOG.warn("Exception processing reconstruction log " + reconstructionLog +
        " opening " + this.storeName +
        " -- continuing.  Probably lack-of-HADOOP-1700 causing DATA LOSS!", e);
    } catch (IOException e) {
      // Presume we got here because of some HDFS issue. Don't just keep going.
      // Fail to open the HStore.  Probably means we'll fail over and over
      // again until human intervention but alternative has us skipping logs
      // and losing edits: HBASE-642.
      LOG.warn("Exception processing reconstruction log " + reconstructionLog +
        " opening " + this.storeName, e);
      throw e;
    }
  }

  /*
   * Read the reconstructionLog to see whether we need to build a brand-new 
   * MapFile out of non-flushed log entries.  
   *
   * We can ignore any log message that has a sequence ID that's equal to or 
   * lower than maxSeqID.  (Because we know such log messages are already 
   * reflected in the MapFiles.)
   */
  @SuppressWarnings("unchecked")
  private void doReconstructionLog(final Path reconstructionLog,
    final long maxSeqID, final Progressable reporter)
  throws UnsupportedEncodingException, IOException {
    if (reconstructionLog == null || !fs.exists(reconstructionLog)) {
      // Nothing to do.
      return;
    }
    // Check its not empty.
    FileStatus[] stats = fs.listStatus(reconstructionLog);
    if (stats == null || stats.length == 0) {
      LOG.warn("Passed reconstruction log " + reconstructionLog + " is zero-length");
      return;
    }
    long maxSeqIdInLog = -1;
    TreeMap<HStoreKey, byte []> reconstructedCache =
      new TreeMap<HStoreKey, byte []>(new HStoreKey.HStoreKeyWritableComparator(this.info));
      
    SequenceFile.Reader logReader = new SequenceFile.Reader(this.fs,
        reconstructionLog, this.conf);
    
    try {
      HLogKey key = new HLogKey();
      HLogEdit val = new HLogEdit();
      long skippedEdits = 0;
      long editsCount = 0;
      // How many edits to apply before we send a progress report.
      int reportInterval = this.conf.getInt("hbase.hstore.report.interval.edits", 2000);
      while (logReader.next(key, val)) {
        maxSeqIdInLog = Math.max(maxSeqIdInLog, key.getLogSeqNum());
        if (key.getLogSeqNum() <= maxSeqID) {
          skippedEdits++;
          continue;
        }
        // Check this edit is for me. Also, guard against writing
        // METACOLUMN info such as HBASE::CACHEFLUSH entries
        byte [] column = val.getColumn();
        if (val.isTransactionEntry() || Bytes.equals(column, HLog.METACOLUMN)
            || !Bytes.equals(key.getRegionName(), info.getRegionName())
            || !HStoreKey.matchingFamily(family.getName(), column)) {
          continue;
        }
        HStoreKey k = new HStoreKey(key.getRow(), column, val.getTimestamp(),
          this.info);
        reconstructedCache.put(k, val.getVal());
        editsCount++;
        // Every 2k edits, tell the reporter we're making progress.
        // Have seen 60k edits taking 3minutes to complete.
        if (reporter != null && (editsCount % reportInterval) == 0) {
          reporter.progress();
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Applied " + editsCount + ", skipped " + skippedEdits +
          " because sequence id <= " + maxSeqID);
      }
    } finally {
      logReader.close();
    }
    
    if (reconstructedCache.size() > 0) {
      // We create a "virtual flush" at maxSeqIdInLog+1.
      if (LOG.isDebugEnabled()) {
        LOG.debug("flushing reconstructionCache");
      }
      internalFlushCache(reconstructedCache, maxSeqIdInLog + 1);
    }
  }
  
  /*
   * Creates a series of HStoreFiles loaded from the given directory.
   * There must be a matching 'mapdir' and 'loginfo' pair of files.
   * If only one exists, we'll delete it.  Does other consistency tests
   * checking files are not zero, etc.
   *
   * @param infodir qualified path for info file directory
   * @param mapdir qualified path for map file directory
   * @throws IOException
   */
  private SortedMap<Long, HStoreFile> loadHStoreFiles(Path infodir, Path mapdir)
  throws IOException {
    // Look first at info files.  If a reference, these contain info we need
    // to create the HStoreFile.
    FileStatus infofiles[] = fs.listStatus(infodir);
    SortedMap<Long, HStoreFile> results = new TreeMap<Long, HStoreFile>();
    ArrayList<Path> mapfiles = new ArrayList<Path>(infofiles.length);
    for (int i = 0; i < infofiles.length; i++) {
      Path p = infofiles[i].getPath();
      // Check for empty info file.  Should never be the case but can happen
      // after data loss in hdfs for whatever reason (upgrade, etc.): HBASE-646
      if (this.fs.getFileStatus(p).getLen() <= 0) {
        LOG.warn("Skipping " + p + " because its empty.  DATA LOSS?  Can " +
          "this scenario be repaired?  HBASE-646");
        continue;
      }

      Matcher m = REF_NAME_PARSER.matcher(p.getName());
      /*
       *  *  *  *  *  N O T E  *  *  *  *  *
       *  
       *  We call isReference(Path, Matcher) here because it calls
       *  Matcher.matches() which must be called before Matcher.group(int)
       *  and we don't want to call Matcher.matches() twice.
       *  
       *  *  *  *  *  N O T E  *  *  *  *  *
       */
      boolean isReference = isReference(p, m);
      long fid = Long.parseLong(m.group(1));
      
      HStoreFile curfile = null;
      Reference reference = null;
      if (isReference) {
        reference = HStoreFile.readSplitInfo(p, fs);
      }
      curfile = new HStoreFile(conf, fs, basedir, this.info,
        family.getName(), fid, reference);
      long storeSeqId = -1;
      try {
        storeSeqId = curfile.loadInfo(fs);
        if (storeSeqId > this.maxSeqId) {
          this.maxSeqId = storeSeqId;
        }
      } catch (IOException e) {
        // If the HSTORE_LOGINFOFILE doesn't contain a number, just ignore it.
        // That means it was built prior to the previous run of HStore, and so
        // it cannot contain any updates also contained in the log.
        LOG.info("HSTORE_LOGINFOFILE " + curfile +
          " does not contain a sequence number - ignoring");
      }
      Path mapfile = curfile.getMapFilePath();
      if (!fs.exists(mapfile)) {
        fs.delete(curfile.getInfoFilePath(), false);
        LOG.warn("Mapfile " + mapfile.toString() + " does not exist. " +
          "Cleaned up info file.  Continuing...Probable DATA LOSS!!!");
        continue;
      }
      // References don't have data or index components under mapfile.
      if (!isReference && isEmptyDataFile(mapfile)) {
        curfile.delete();
        // We can have empty data file if data loss in hdfs.
        LOG.warn("Mapfile " + mapfile.toString() + " has empty data. " +
          "Deleting.  Continuing...Probable DATA LOSS!!!  See HBASE-646.");
        continue;
      }
      if (!isReference && isEmptyIndexFile(mapfile)) {
        try {
          // Try fixing this file.. if we can.  Use the hbase version of fix.
          // Need to remove the old index file first else fix won't go ahead.
          this.fs.delete(new Path(mapfile, MapFile.INDEX_FILE_NAME), false);
          // TODO: This is going to fail if we are to rebuild a file from
          // meta because it won't have right comparator: HBASE-848.
          long count = MapFile.fix(this.fs, mapfile, HStoreKey.class,
            HBaseMapFile.VALUE_CLASS, false, this.conf);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Fixed index on " + mapfile.toString() + "; had " +
              count + " entries");
          }
        } catch (Exception e) {
          LOG.warn("Failed fix of " + mapfile.toString() +
            "...continuing; Probable DATA LOSS!!!", e);
          continue;
        }
      }
      long length = curfile.length();
      storeSize += length;
      
      // TODO: Confirm referent exists.
      
      // Found map and sympathetic info file.  Add this hstorefile to result.
      if (LOG.isDebugEnabled()) {
        LOG.debug("loaded " + FSUtils.getPath(p) + ", isReference=" +
          isReference + ", sequence id=" + storeSeqId +
          ", length=" + length + ", majorCompaction=" +
          curfile.isMajorCompaction());
      }
      results.put(Long.valueOf(storeSeqId), curfile);
      // Keep list of sympathetic data mapfiles for cleaning info dir in next
      // section.  Make sure path is fully qualified for compare.
      mapfiles.add(this.fs.makeQualified(mapfile));
    }
    cleanDataFiles(mapfiles, mapdir);
    return results;
  }
  
  /*
   * If no info file delete the sympathetic data file.
   * @param mapfiles List of mapfiles.
   * @param mapdir Directory to check.
   * @throws IOException
   */
  private void cleanDataFiles(final List<Path> mapfiles, final Path mapdir)
  throws IOException {
    // List paths by experience returns fully qualified names -- at least when
    // running on a mini hdfs cluster.
    FileStatus [] datfiles = fs.listStatus(mapdir);
    for (int i = 0; i < datfiles.length; i++) {
      Path p = datfiles[i].getPath();
      // If does not have sympathetic info file, delete.
      Path qualifiedP = fs.makeQualified(p);
      if (!mapfiles.contains(qualifiedP)) {
        fs.delete(p, true);
      }
    }
  }

  /* 
   * @param mapfile
   * @return True if the passed mapfile has a zero-length data component (its
   * broken).
   * @throws IOException
   */
  private boolean isEmptyDataFile(final Path mapfile)
  throws IOException {
    // Mapfiles are made of 'data' and 'index' files.  Confirm 'data' is
    // non-null if it exists (may not have been written to yet).
    return isEmptyFile(new Path(mapfile, MapFile.DATA_FILE_NAME));
  }

  /* 
   * @param mapfile
   * @return True if the passed mapfile has a zero-length index component (its
   * broken).
   * @throws IOException
   */
  private boolean isEmptyIndexFile(final Path mapfile)
  throws IOException {
    // Mapfiles are made of 'data' and 'index' files.  Confirm 'data' is
    // non-null if it exists (may not have been written to yet).
    return isEmptyFile(new Path(mapfile, MapFile.INDEX_FILE_NAME));
  }

  /* 
   * @param f
   * @return True if the passed file does not exist or is zero-length (its
   * broken).
   * @throws IOException
   */
  private boolean isEmptyFile(final Path f)
  throws IOException {
    return !this.fs.exists(f) || this.fs.getFileStatus(f).getLen() == 0;
  }

  /**
   * Adds a value to the memcache
   * 
   * @param key
   * @param value
   * @return memcache size delta
   */
  protected long add(HStoreKey key, byte[] value) {
    lock.readLock().lock();
    try {
      return this.memcache.add(key, value);
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
      synchronized (this.storefiles) {
        result = new ArrayList<HStoreFile>(storefiles.values());
      }
      LOG.debug("closed " + this.storeNameStr);
      return result;
    } finally {
      this.lock.writeLock().unlock();
    }
  }
  
  //////////////////////////////////////////////////////////////////////////////
  // Flush changes to disk
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Snapshot this stores memcache.  Call before running
   * {@link #flushCache(long)} so it has some work to do.
   */
  void snapshot() {
    this.memcache.snapshot();
  }
  
  /**
   * Write out current snapshot.  Presumes {@link #snapshot()} has been called
   * previously.
   * @param logCacheFlushId flush sequence number
   * @return true if a compaction is needed
   * @throws IOException
   */
  boolean flushCache(final long logCacheFlushId) throws IOException {
    // Get the snapshot to flush.  Presumes that a call to
    // this.memcache.snapshot() has happened earlier up in the chain.
    SortedMap<HStoreKey, byte []> cache = this.memcache.getSnapshot();
    boolean compactionNeeded = internalFlushCache(cache, logCacheFlushId);
    // If an exception happens flushing, we let it out without clearing
    // the memcache snapshot.  The old snapshot will be returned when we say
    // 'snapshot', the next time flush comes around.
    this.memcache.clearSnapshot(cache);
    return compactionNeeded;
  }
  
  private boolean internalFlushCache(final SortedMap<HStoreKey, byte []> cache,
    final long logCacheFlushId)
  throws IOException {
    long flushed = 0;
    // Don't flush if there are no entries.
    if (cache.size() == 0) {
      return false;
    }
    
    // TODO:  We can fail in the below block before we complete adding this
    // flush to list of store files.  Add cleanup of anything put on filesystem
    // if we fail.
    synchronized(flushLock) {
      long now = System.currentTimeMillis();
      // A. Write the Maps out to the disk
      HStoreFile flushedFile = new HStoreFile(conf, fs, basedir,
        this.info,  family.getName(), -1L, null);
      MapFile.Writer out = flushedFile.getWriter(this.fs, this.compression,
        this.family.isBloomfilter(), cache.size());
      setIndexInterval(out);
      
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
      int entries = 0;
      try {
        for (Map.Entry<HStoreKey, byte []> es: cache.entrySet()) {
          HStoreKey curkey = es.getKey();
          byte[] bytes = es.getValue();
          if (HStoreKey.matchingFamily(this.family.getName(), curkey.getColumn())) {
            if (!isExpired(curkey, ttl, now)) {
              entries++;
              out.append(curkey, new ImmutableBytesWritable(bytes));
              flushed += this.memcache.heapSize(curkey, bytes, null);
            }
          }
        }
      } finally {
        out.close();
      }
      long newStoreSize = flushedFile.length();
      storeSize += newStoreSize;

      // B. Write out the log sequence number that corresponds to this output
      // MapFile.  The MapFile is current up to and including the log seq num.
      flushedFile.writeInfo(fs, logCacheFlushId);
      
      // C. Finally, make the new MapFile available.
      updateReaders(logCacheFlushId, flushedFile);
      if(LOG.isDebugEnabled()) {
        LOG.debug("Added " + FSUtils.getPath(flushedFile.getMapFilePath()) +
          " with " + entries +
          " entries, sequence id " + logCacheFlushId + ", data size ~" +
          StringUtils.humanReadableInt(flushed) + ", file size " +
          StringUtils.humanReadableInt(newStoreSize) + " to " +
          this.info.getRegionNameAsString());
      }
    }
    return storefiles.size() >= compactionThreshold;
  }
  
  /*
   * Change readers adding into place the Reader produced by this new flush.
   * @param logCacheFlushId
   * @param flushedFile
   * @throws IOException
   */
  private void updateReaders(final long logCacheFlushId,
      final HStoreFile flushedFile)
  throws IOException {
    this.lock.writeLock().lock();
    try {
      Long flushid = Long.valueOf(logCacheFlushId);
      // Open the map file reader.
      this.readers.put(flushid,
        flushedFile.getReader(this.fs, this.family.isBloomfilter(),
        this.family.isBlockCacheEnabled()));
      this.storefiles.put(flushid, flushedFile);
      // Tell listeners of the change in readers.
      notifyChangedReadersObservers();
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  /*
   * Notify all observers that set of Readers has changed.
   * @throws IOException
   */
  private void notifyChangedReadersObservers() throws IOException {
    for (ChangedReadersObserver o: this.changedReaderObservers) {
      o.updateReaders();
    }
  }

  /*
   * @param o Observer who wants to know about changes in set of Readers
   */
  void addChangedReaderObserver(ChangedReadersObserver o) {
    this.changedReaderObservers.add(o);
  }
  
  /*
   * @param o Observer no longer interested in changes in set of Readers.
   */
  void deleteChangedReaderObserver(ChangedReadersObserver o) {
    if (!this.changedReaderObservers.remove(o)) {
      LOG.warn("Not in set" + o);
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // Compaction
  //////////////////////////////////////////////////////////////////////////////

  /*
   * @param files
   * @return True if any of the files in <code>files</code> are References.
   */
  private boolean hasReferences(Collection<HStoreFile> files) {
    if (files != null && files.size() > 0) {
      for (HStoreFile hsf: files) {
        if (hsf.isReference()) {
          return true;
        }
      }
    }
    return false;
  }

  /*
   * Gets lowest timestamp from files in a dir
   * 
   * @param fs
   * @param dir
   * @throws IOException
   */
  private static long getLowestTimestamp(FileSystem fs, Path dir)
  throws IOException {
    FileStatus[] stats = fs.listStatus(dir);
    if (stats == null || stats.length == 0) {
      return 0l;
    }
    long lowTimestamp = Long.MAX_VALUE;   
    for (int i = 0; i < stats.length; i++) {
      long timestamp = stats[i].getModificationTime();
      if (timestamp < lowTimestamp){
        lowTimestamp = timestamp;
      }
    }
    return lowTimestamp;
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
   * 
   * @param majorCompaction True to force a major compaction regardless of
   * thresholds
   * @return mid key if a split is needed, null otherwise
   * @throws IOException
   */
  StoreSize compact(final boolean majorCompaction) throws IOException {
    boolean forceSplit = this.info.shouldSplit(false);
    boolean doMajorCompaction = majorCompaction;
    synchronized (compactLock) {
      long maxId = -1;
      List<HStoreFile> filesToCompact = null;
      synchronized (storefiles) {
        if (this.storefiles.size() <= 0) {
          LOG.debug(this.storeNameStr + ": no store files to compact");
          return null;
        }
        // filesToCompact are sorted oldest to newest.
        filesToCompact = new ArrayList<HStoreFile>(this.storefiles.values());
        
        // The max-sequenceID in any of the to-be-compacted TreeMaps is the 
        // last key of storefiles.
        maxId = this.storefiles.lastKey().longValue();
      }
      // Check to see if we need to do a major compaction on this region.
      // If so, change doMajorCompaction to true to skip the incremental
      // compacting below. Only check if doMajorCompaction is not true.
      if (!doMajorCompaction) {
        doMajorCompaction = isMajorCompaction(filesToCompact);
      }
      boolean references = hasReferences(filesToCompact);
      if (!doMajorCompaction && !references && 
          (forceSplit || (filesToCompact.size() < compactionThreshold))) {
        return checkSplit(forceSplit);
      }
      if (!fs.exists(compactionDir) && !fs.mkdirs(compactionDir)) {
        LOG.warn("Mkdir on " + compactionDir.toString() + " failed");
        return checkSplit(forceSplit);
      }

      // HBASE-745, preparing all store file sizes for incremental compacting
      // selection.
      int countOfFiles = filesToCompact.size();
      long totalSize = 0;
      long[] fileSizes = new long[countOfFiles];
      long skipped = 0;
      int point = 0;
      for (int i = 0; i < countOfFiles; i++) {
        HStoreFile file = filesToCompact.get(i);
        Path path = file.getMapFilePath();
        if (path == null) {
          LOG.warn("Path is null for " + file);
          return null;
        }
        int len = 0;
        for (FileStatus fstatus:fs.listStatus(path)) {
          len += fstatus.getLen();
        }
        fileSizes[i] = len;
        totalSize += len;
      }
      if (!doMajorCompaction && !references) {
        // Here we select files for incremental compaction.  
        // The rule is: if the largest(oldest) one is more than twice the 
        // size of the second, skip the largest, and continue to next...,
        // until we meet the compactionThreshold limit.
        for (point = 0; point < countOfFiles - 1; point++) {
          if ((fileSizes[point] < fileSizes[point + 1] * 2) && 
               (countOfFiles - point) <= maxFilesToCompact) {
            break;
          }
          skipped += fileSizes[point];
        }
        filesToCompact = new ArrayList<HStoreFile>(filesToCompact.subList(point,
          countOfFiles));
        if (filesToCompact.size() <= 1) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Skipped compaction of 1 file; compaction size of " +
              this.storeNameStr + ": " +
              StringUtils.humanReadableInt(totalSize) + "; Skipped " + point +
              " files, size: " + skipped);
          }
          return checkSplit(forceSplit);
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Compaction size of " + this.storeNameStr + ": " +
            StringUtils.humanReadableInt(totalSize) + "; Skipped " + point +
            " file(s), size: " + skipped);
        }
      }

      /*
       * We create a new list of MapFile.Reader objects so we don't screw up
       * the caching associated with the currently-loaded ones. Our iteration-
       * based access pattern is practically designed to ruin the cache.
       */
      List<MapFile.Reader> rdrs = new ArrayList<MapFile.Reader>();
      int nrows = createReaders(rdrs, filesToCompact);

      // Step through them, writing to the brand-new MapFile
      HStoreFile compactedOutputFile = new HStoreFile(conf, fs, 
          this.compactionDir,  this.info, family.getName(), -1L, null);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Started compaction of " + rdrs.size() + " file(s)" +
          (references? ", hasReferences=true,": " ") + " into " +
          FSUtils.getPath(compactedOutputFile.getMapFilePath()));
      }
      MapFile.Writer writer = compactedOutputFile.getWriter(this.fs,
        this.compression, this.family.isBloomfilter(), nrows);
      setIndexInterval(writer);
      try {
        compact(writer, rdrs, doMajorCompaction);
      } finally {
        writer.close();
      }

      // Now, write out an HSTORE_LOGINFOFILE for the brand-new TreeMap.
      compactedOutputFile.writeInfo(fs, maxId, doMajorCompaction);

      // Move the compaction into place.
      completeCompaction(filesToCompact, compactedOutputFile);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Completed " + (doMajorCompaction? "major": "") +
          " compaction of " + this.storeNameStr +
          " store size is " + StringUtils.humanReadableInt(storeSize));
      }
    }
    return checkSplit(forceSplit);
  }  

  /*
   * Set the index interval for the mapfile. There are two sources for
   * configuration information: the HCD, and the global HBase config.
   * If a source returns the default value, it is ignored. Otherwise,
   * the smallest non-default value is preferred. 
   */
  private void setIndexInterval(MapFile.Writer writer) {
    int familyInterval = this.family.getMapFileIndexInterval();
    int interval = this.conf.getInt("hbase.io.index.interval",
        HColumnDescriptor.DEFAULT_MAPFILE_INDEX_INTERVAL);
    if (familyInterval != HColumnDescriptor.DEFAULT_MAPFILE_INDEX_INTERVAL) {
      if (interval != HColumnDescriptor.DEFAULT_MAPFILE_INDEX_INTERVAL) {
        if (familyInterval < interval)
          interval = familyInterval;
      } else {
        interval = familyInterval;
      }
    }
    writer.setIndexInterval(interval);
  }

  /*
   * @return True if we should run a major compaction.
   */
  boolean isMajorCompaction() throws IOException {
    return isMajorCompaction(null);
  }

  /*
   * @param filesToCompact Files to compact. Can be null.
   * @return True if we should run a major compaction.
   */
  private boolean isMajorCompaction(final List<HStoreFile> filesToCompact)
  throws IOException {
    boolean result = false;
    Path mapdir = HStoreFile.getMapDir(this.basedir, this.info.getEncodedName(),
        this.family.getName());
    long lowTimestamp = getLowestTimestamp(fs, mapdir);
    if (lowTimestamp < (System.currentTimeMillis() - this.majorCompactionTime) &&
        lowTimestamp > 0l) {
      // Major compaction time has elapsed.
      long elapsedTime = System.currentTimeMillis() - lowTimestamp;
      if (filesToCompact != null && filesToCompact.size() == 1 &&
          filesToCompact.get(0).isMajorCompaction() &&
          (this.ttl == HConstants.FOREVER || elapsedTime < this.ttl)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skipping major compaction because only one (major) " +
            "compacted file only and elapsedTime " + elapsedTime +
            " is < ttl=" + this.ttl);
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Major compaction triggered on store: " +
              this.storeNameStr + ". Time since last major compaction: " +
              ((System.currentTimeMillis() - lowTimestamp)/1000) + " seconds");
        }
        result = true;
      }
    }
    return result;
  }
  
  /*
   * Create readers for the passed in list of HStoreFiles and add them to
   * <code>readers</code> list.  Used compacting.
   * @param readers Add Readers here.
   * @param files List of HSFs to make Readers for.
   * @return Count of rows for bloom filter sizing.  Returns -1 if no bloom
   * filter wanted.
   */
  private int createReaders(final List<MapFile.Reader> rs,
    final List<HStoreFile> files)
  throws IOException {
    /* We create a new list of MapFile.Reader objects so we don't screw up
     * the caching associated with the currently-loaded ones. Our iteration-
     * based access pattern is practically designed to ruin the cache.
     */
    int nrows = -1;
    for (HStoreFile file: files) {
      try {
        // TODO: Readers are opened without block-cache enabled.  Means we don't
        // get the prefetch that makes the read faster.  But we don't want to
        // enable block-cache for these readers that are about to be closed.
        // The compaction of soon-to-be closed readers will probably force out
        // blocks that may be needed servicing real-time requests whereas
        // compaction runs in background.  TODO: We know we're going to read
        // this file straight through.  Leverage this fact.  Use a big buffer
        // client side to speed things up or read it all up into memory one file
        // at a time or pull local and memory-map the file but leave the writer
        // up in hdfs?
        BloomFilterMapFile.Reader reader = file.getReader(fs, false, false);
        rs.add(reader);
        // Compute the size of the new bloomfilter if needed
        if (this.family.isBloomfilter()) {
          nrows += reader.getBloomFilterSize();
        }
      } catch (IOException e) {
        // Add info about which file threw exception. It may not be in the
        // exception message so output a message here where we know the
        // culprit.
        LOG.warn("Failed with " + e.toString() + ": " + file.toString());
        closeCompactionReaders(rs);
        throw e;
      }
    }
    return nrows;
  }
  
  /*
   * @param r List to reverse
   * @return A reversed array of content of <code>readers</code>
   */
  private MapFile.Reader [] reverse(final List<MapFile.Reader> r) {
    List<MapFile.Reader> copy = new ArrayList<MapFile.Reader>(r);
    Collections.reverse(copy);
    return copy.toArray(new MapFile.Reader[0]);
  }

  /*
   * @param rdrs List of readers
   * @param keys Current keys
   * @param done Which readers are done
   * @return The lowest current key in passed <code>rdrs</code>
   */
  private int getLowestKey(final MapFile.Reader [] rdrs,
      final HStoreKey [] keys, final boolean [] done) {
    int lowestKey = -1;
    for (int i = 0; i < rdrs.length; i++) {
      if (done[i]) {
        continue;
      }
      if (lowestKey < 0) {
        lowestKey = i;
      } else {
        if (keys[i].compareTo(keys[lowestKey]) < 0) {
          lowestKey = i;
        }
      }
    }
    return lowestKey;
  }

  /*
   * Compact a list of MapFile.Readers into MapFile.Writer.
   * 
   * We work by iterating through the readers in parallel looking at newest
   * store file first. We always increment the lowest-ranked one. Updates to a
   * single row/column will appear ranked by timestamp.
   * @param compactedOut Where to write compaction.
   * @param pReaders List of readers sorted oldest to newest.
   * @param majorCompaction True to force a major compaction regardless of
   * thresholds
   * @throws IOException
   */
  private void compact(final MapFile.Writer compactedOut,
      final List<MapFile.Reader> pReaders, final boolean majorCompaction)
  throws IOException {
    // Reverse order so newest store file is first.
    MapFile.Reader[] rdrs = reverse(pReaders);
    try {
      HStoreKey [] keys = new HStoreKey[rdrs.length];
      ImmutableBytesWritable [] vals = new ImmutableBytesWritable[rdrs.length];
      boolean [] done = new boolean[rdrs.length];
      for(int i = 0; i < rdrs.length; i++) {
        keys[i] = new HStoreKey(HConstants.EMPTY_BYTE_ARRAY, this.info);
        vals[i] = new ImmutableBytesWritable();
        done[i] = false;
      }

      // Now, advance through the readers in order.  This will have the
      // effect of a run-time sort of the entire dataset.
      int numDone = 0;
      for (int i = 0; i < rdrs.length; i++) {
        rdrs[i].reset();
        done[i] = !rdrs[i].next(keys[i], vals[i]);
        if (done[i]) {
          numDone++;
        }
      }

      long now = System.currentTimeMillis();
      int timesSeen = 0;
      HStoreKey lastSeen = new HStoreKey();
      HStoreKey lastDelete = null;
      while (numDone < done.length) {
        // Get lowest key in all store files.
        int lowestKey = getLowestKey(rdrs, keys, done);
        HStoreKey sk = keys[lowestKey];
        // If its same row and column as last key, increment times seen.
        if (HStoreKey.equalsTwoRowKeys(info, lastSeen.getRow(), sk.getRow())
            && Bytes.equals(lastSeen.getColumn(), sk.getColumn())) {
          timesSeen++;
          // Reset last delete if not exact timestamp -- lastDelete only stops
          // exactly the same key making it out to the compacted store file.
          if (lastDelete != null &&
              lastDelete.getTimestamp() != sk.getTimestamp()) {
            lastDelete = null;
          }
        } else {
          timesSeen = 1;
          lastDelete = null;
        }

        // Don't write empty rows or columns.  Only remove cells on major
        // compaction.  Remove if expired of > VERSIONS
        if (sk.getRow().length != 0 && sk.getColumn().length != 0) {
          ImmutableBytesWritable value = vals[lowestKey];
          if (!majorCompaction) {
            // Write out all values if not a major compaction.
            compactedOut.append(sk, value);
          } else {
            boolean expired = false;
            boolean deleted = false;
            if (timesSeen <= family.getMaxVersions() &&
                !(expired = isExpired(sk, ttl, now))) {
              // If this value key is same as a deleted key, skip
              if (lastDelete != null && sk.equals(lastDelete)) {
                deleted = true;
              } else if (HLogEdit.isDeleted(value.get())) {
                // If a deleted value, skip
                deleted = true;
                lastDelete = new HStoreKey(sk);
              } else {
                compactedOut.append(sk, vals[lowestKey]);
              }
            }
            if (expired || deleted) {
              // HBASE-855 remove one from timesSeen because it did not make it
              // past expired check -- don't count against max versions.
              timesSeen--;
            }
          }
        }

        // Update last-seen items
        lastSeen = new HStoreKey(sk);

        // Advance the smallest key.  If that reader's all finished, then 
        // mark it as done.
        if (!rdrs[lowestKey].next(keys[lowestKey], vals[lowestKey])) {
          done[lowestKey] = true;
          rdrs[lowestKey].close();
          rdrs[lowestKey] = null;
          numDone++;
        }
      }
    } finally {
      closeCompactionReaders(Arrays.asList(rdrs));
    }
  }

  private void closeCompactionReaders(final List<MapFile.Reader> rdrs) {
    for (MapFile.Reader r: rdrs) {
      try {
        if (r != null) {
          r.close();
        }
      } catch (IOException e) {
        LOG.warn("Exception closing reader for " + this.storeNameStr, e);
      }
    }
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
   * 1) Moving the new compacted MapFile into place
   * 2) Unload all replaced MapFiles, close and collect list to delete.
   * 3) Loading the new TreeMap.
   * 4) Compute new store size
   * </pre>
   * 
   * @param compactedFiles list of files that were compacted
   * @param compactedFile HStoreFile that is the result of the compaction
   * @throws IOException
   */
  private void completeCompaction(final List<HStoreFile> compactedFiles,
    final HStoreFile compactedFile)
  throws IOException {
    this.lock.writeLock().lock();
    try {
      // 1. Moving the new MapFile into place.
      HStoreFile finalCompactedFile = new HStoreFile(conf, fs, basedir,
        this.info, family.getName(), -1, null,
        compactedFile.isMajorCompaction());
      if (LOG.isDebugEnabled()) {
        LOG.debug("moving " + FSUtils.getPath(compactedFile.getMapFilePath()) +
          " to " + FSUtils.getPath(finalCompactedFile.getMapFilePath()));
      }
      if (!compactedFile.rename(this.fs, finalCompactedFile)) {
        LOG.error("Failed move of compacted file " +
          finalCompactedFile.getMapFilePath().toString());
        return;
      }

      // 2. Unload all replaced MapFiles, close and collect list to delete.
      synchronized (storefiles) {
        Map<Long, HStoreFile> toDelete = new HashMap<Long, HStoreFile>();
        for (Map.Entry<Long, HStoreFile> e : this.storefiles.entrySet()) {
          if (!compactedFiles.contains(e.getValue())) {
            continue;
          }
          Long key = e.getKey();
          MapFile.Reader reader = this.readers.remove(key);
          if (reader != null) {
            reader.close();
          }
          toDelete.put(key, e.getValue());
        }

        try {
          // 3. Loading the new TreeMap.
          // Change this.storefiles so it reflects new state but do not
          // delete old store files until we have sent out notification of
          // change in case old files are still being accessed by outstanding
          // scanners.
          for (Long key : toDelete.keySet()) {
            this.storefiles.remove(key);
          }
          // Add new compacted Reader and store file.
          Long orderVal = Long.valueOf(finalCompactedFile.loadInfo(fs));
          this.readers.put(orderVal,
              // Use a block cache (if configured) for this reader since
              // it is the only one.
              finalCompactedFile.getReader(this.fs,
                  this.family.isBloomfilter(),
                  this.family.isBlockCacheEnabled()));
          this.storefiles.put(orderVal, finalCompactedFile);
          // Tell observers that list of Readers has changed.
          notifyChangedReadersObservers();
          // Finally, delete old store files.
          for (HStoreFile hsf : toDelete.values()) {
            hsf.delete();
          }
        } catch (IOException e) {
          e = RemoteExceptionHandler.checkIOException(e);
          LOG.error("Failed replacing compacted files for " +
            this.storeNameStr +
            ". Compacted file is " + finalCompactedFile.toString() +
            ".  Files replaced are " + compactedFiles.toString() +
            " some of which may have been already removed", e);
        }
        // 4. Compute new store size
        storeSize = 0L;
        for (HStoreFile hsf : storefiles.values()) {
          storeSize += hsf.length();
        }
      }
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  // ////////////////////////////////////////////////////////////////////////////
  // Accessors.
  // (This is the only section that is directly useful!)
  //////////////////////////////////////////////////////////////////////////////
  
  /**
   * Return all the available columns for the given key.  The key indicates a 
   * row and timestamp, but not a column name.
   *
   * The returned object should map column names to Cells.
   */
  void getFull(HStoreKey key, final Set<byte []> columns,
      final int numVersions, Map<byte [], Cell> results)
  throws IOException {
    int versions = versionsToReturn(numVersions);

    Map<byte [], Long> deletes =
      new TreeMap<byte [], Long>(Bytes.BYTES_COMPARATOR);

    // if the key is null, we're not even looking for anything. return.
    if (key == null) {
      return;
    }
    
    this.lock.readLock().lock();
    
    // get from the memcache first.
    memcache.getFull(key, columns, versions, deletes, results);
    
    try {
      MapFile.Reader[] maparray = getReaders();
      
      // examine each mapfile
      for (int i = maparray.length - 1; i >= 0; i--) {
        MapFile.Reader map = maparray[i];
        
        // synchronize on the map so that no one else iterates it at the same 
        // time
        getFullFromMapFile(map, key, columns, versions, deletes, results);
      }
    } finally {
      this.lock.readLock().unlock();
    }
  }
  
  private void getFullFromMapFile(MapFile.Reader map, HStoreKey key, 
    Set<byte []> columns, int numVersions, Map<byte [], Long> deletes,
    Map<byte [], Cell> results) 
  throws IOException {
    synchronized(map) {
      long now = System.currentTimeMillis();

      // seek back to the beginning
      map.reset();
      
      // seek to the closest key that should match the row we're looking for
      ImmutableBytesWritable readval = new ImmutableBytesWritable();
      HStoreKey readkey = (HStoreKey)map.getClosest(key, readval);
      if (readkey == null) {
        return;
      }
      
      do {
        byte [] readcol = readkey.getColumn();
        
        // if we're looking for this column (or all of them), and there isn't 
        // already a value for this column in the results map or there is a value
        // but we haven't collected enough versions yet, and the key we 
        // just read matches, then we'll consider it
        if ((columns == null || columns.contains(readcol)) 
          && (!results.containsKey(readcol) 
              || results.get(readcol).getNumValues() < numVersions)
          && key.matchesWithoutColumn(readkey)) {
          // if the value of the cell we're looking at right now is a delete, 
          // we need to treat it differently
          if(HLogEdit.isDeleted(readval.get())) {
            // if it's not already recorded as a delete or recorded with a more
            // recent delete timestamp, record it for later
            if (!deletes.containsKey(readcol) 
              || deletes.get(readcol).longValue() < readkey.getTimestamp()) {
              deletes.put(readcol, Long.valueOf(readkey.getTimestamp()));              
            }
          } else if (!(deletes.containsKey(readcol) 
            && deletes.get(readcol).longValue() >= readkey.getTimestamp()) ) {
            // So the cell itself isn't a delete, but there may be a delete 
            // pending from earlier in our search. Only record this result if
            // there aren't any pending deletes.
            if (!(deletes.containsKey(readcol) &&
                deletes.get(readcol).longValue() >= readkey.getTimestamp())) {
              if (!isExpired(readkey, ttl, now)) {
                if (!results.containsKey(readcol)) {
                  results.put(readcol,
                              new Cell(readval.get(), readkey.getTimestamp()));
                } else {
                  results.get(readcol).add(readval.get(),
                                           readkey.getTimestamp());
                }
                // need to reinstantiate the readval so we can reuse it, 
                // otherwise next iteration will destroy our result
                readval = new ImmutableBytesWritable();
              }
            }
          }
        } else if (HStoreKey.compareTwoRowKeys(info,key.getRow(), readkey.getRow()) < 0) {
          // if we've crossed into the next row, then we can just stop 
          // iterating
          break;
        }
        
      } while(map.next(readkey, readval));
    }
  }

  /**
   * @return Array of readers ordered oldest to newest.
   */
  public MapFile.Reader [] getReaders() {
    return this.readers.values().
      toArray(new MapFile.Reader[this.readers.size()]);
  }

  /*
   * @param wantedVersions How many versions were asked for.
   * @return wantedVersions or this families' MAX_VERSIONS.
   */
  private int versionsToReturn(final int wantedVersions) {
    if (wantedVersions <= 0) {
      throw new IllegalArgumentException("Number of versions must be > 0");
    }
    // Make sure we do not return more than maximum versions for this store.
    return wantedVersions > this.family.getMaxVersions()?
      this.family.getMaxVersions(): wantedVersions;
  }
  
  /**
   * Get the value for the indicated HStoreKey.  Grab the target value and the 
   * previous <code>numVersions - 1</code> values, as well.
   *
   * Use {@link HConstants.ALL_VERSIONS} to retrieve all versions.
   * @param key
   * @param numVersions Number of versions to fetch.  Must be > 0.
   * @return values for the specified versions
   * @throws IOException
   */
  Cell[] get(final HStoreKey key, final int numVersions) throws IOException {
    // This code below is very close to the body of the getKeys method.  Any 
    // changes in the flow below should also probably be done in getKeys.
    // TODO: Refactor so same code used.
    long now = System.currentTimeMillis();
    int versions = versionsToReturn(numVersions);
    // Keep a list of deleted cell keys.  We need this because as we go through
    // the memcache and store files, the cell with the delete marker may be
    // in one store and the old non-delete cell value in a later store.
    // If we don't keep around the fact that the cell was deleted in a newer
    // record, we end up returning the old value if user is asking for more
    // than one version. This List of deletes should not be large since we
    // are only keeping rows and columns that match those set on the get and
    // which have delete values.  If memory usage becomes an issue, could
    // redo as bloom filter.
    Set<HStoreKey> deletes = new HashSet<HStoreKey>();
    this.lock.readLock().lock();
    try {
      // Check the memcache
      List<Cell> results = this.memcache.get(key, versions, deletes, now);
      // If we got sufficient versions from memcache, return. 
      if (results.size() == versions) {
        return results.toArray(new Cell[results.size()]);
      }
      MapFile.Reader[] maparray = getReaders();
      // Returned array is sorted with the most recent addition last.
      for(int i = maparray.length - 1;
          i >= 0 && !hasEnoughVersions(versions, results); i--) {
        MapFile.Reader r = maparray[i];
        synchronized (r) {
          // Do the priming read
          ImmutableBytesWritable readval = new ImmutableBytesWritable();
          HStoreKey readkey = (HStoreKey)r.getClosest(key, readval);
          if (readkey == null) {
            // map.getClosest returns null if the passed key is > than the
            // last key in the map file.  getClosest is a bit of a misnomer
            // since it returns exact match or the next closest key AFTER not
            // BEFORE.  We use getClosest because we're usually passed a
            // key that has a timestamp of maximum long to indicate we want
            // most recent update.
            continue;
          }
          if (!readkey.matchesRowCol(key)) {
            continue;
          }
          if (get(readkey, readval.get(), versions, results, deletes, now)) {
            break;
          }
          for (readval = new ImmutableBytesWritable();
              r.next(readkey, readval) && readkey.matchesRowCol(key);
              readval = new ImmutableBytesWritable()) {
            if (get(readkey, readval.get(), versions, results, deletes, now)) {
              break;
            }
          }
        }
      }
      return results.size() == 0 ?
        null : results.toArray(new Cell[results.size()]);
    } finally {
      this.lock.readLock().unlock();
    }
  }
  
  /*
   * Look at one key/value.
   * @param key
   * @param value
   * @param versions
   * @param results
   * @param deletes
   * @param now
   * @return True if we have enough versions.
   */
  private boolean get(final HStoreKey key, final byte [] value,
      final int versions, final List<Cell> results,
      final Set<HStoreKey> deletes, final long now) {
    if (!HLogEdit.isDeleted(value)) {
      if (notExpiredAndNotInDeletes(this.ttl, key, now, deletes)) {
        results.add(new Cell(value, key.getTimestamp()));
      }
      // Perhaps only one version is wanted.  I could let this
      // test happen later in the for loop test but it would cost
      // the allocation of an ImmutableBytesWritable.
      if (hasEnoughVersions(versions, results)) {
        return true;
      }
    } else {
      // Is this copy necessary?
      deletes.add(new HStoreKey(key));
    }
    return false;
  }

  /*
   * Small method to check if we are over the max number of versions
   * or we acheived this family max versions. 
   * The later happens when we have the situation described in HBASE-621.
   * @param versions
   * @param c
   * @return 
   */
  private boolean hasEnoughVersions(final int versions, final List<Cell> c) {
    return c.size() >= versions;
  }

  /**
   * Get <code>versions</code> of keys matching the origin key's
   * row/column/timestamp and those of an older vintage.
   * @param origin Where to start searching.
   * @param versions How many versions to return. Pass
   * {@link HConstants#ALL_VERSIONS} to retrieve all.
   * @param now
   * @param columnPattern regex pattern for column matching. if columnPattern
   * is not null, we use column pattern to match columns. And the columnPattern
   * only works when origin's column is null or its length is zero.
   * @return Matching keys.
   * @throws IOException
   */
  public List<HStoreKey> getKeys(final HStoreKey origin, final int versions,
    final long now, final Pattern columnPattern)
  throws IOException {
    // This code below is very close to the body of the get method.  Any 
    // changes in the flow below should also probably be done in get.  TODO:
    // Refactor so same code used.
    Set<HStoreKey> deletes = new HashSet<HStoreKey>();
    this.lock.readLock().lock();
    try {
      // Check the memcache
      List<HStoreKey> keys =
        this.memcache.getKeys(origin, versions, deletes, now, columnPattern);
      // If we got sufficient versions from memcache, return. 
      if (keys.size() >= versions) {
        return keys;
      }
      MapFile.Reader[] maparray = getReaders();
      // Returned array is sorted with the most recent addition last.
      for (int i = maparray.length - 1;
          i >= 0 && keys.size() < versions; i--) {
        MapFile.Reader map = maparray[i];
        synchronized(map) {
          map.reset();
          // Do the priming read
          ImmutableBytesWritable readval = new ImmutableBytesWritable();
          HStoreKey readkey = (HStoreKey)map.getClosest(origin, readval);
          if (readkey == null) {
            // map.getClosest returns null if the passed key is > than the
            // last key in the map file.  getClosest is a bit of a misnomer
            // since it returns exact match or the next closest key AFTER not
            // BEFORE.
            continue;
          }
          do {
            // if the row matches, we might want this one.
            if (rowMatches(origin, readkey)) {
              // if the column pattern is not null, we use it for column matching.
              // we will skip the keys whose column doesn't match the pattern.
              if (columnPattern != null) {
                if (!(columnPattern.matcher(Bytes.toString(readkey.getColumn())).matches())) {
                  continue;
                }
              }
              // if the cell address matches, then we definitely want this key.
              if (cellMatches(origin, readkey)) {
                // Store key if isn't deleted or superceded by memcache
                if (!HLogEdit.isDeleted(readval.get())) {
                  if (notExpiredAndNotInDeletes(this.ttl, readkey, now, deletes)) {
                    keys.add(new HStoreKey(readkey));
                  }
                  if (keys.size() >= versions) {
                    break;
                  }
                } else {
                  deletes.add(new HStoreKey(readkey));
                }
              } else {
                // the cell doesn't match, but there might be more with different
                // timestamps, so move to the next key
                continue;
              }
            } else {
              // the row doesn't match, so we've gone too far.
              break;
            }
          } while (map.next(readkey, readval)); // advance to the next key
        }
      }
      return keys;
    } finally {
      this.lock.readLock().unlock();
    }
  }
  
  /**
   * Find the key that matches <i>row</i> exactly, or the one that immediately
   * preceeds it. WARNING: Only use this method on a table where writes occur 
   * with stricly increasing timestamps. This method assumes this pattern of 
   * writes in order to make it reasonably performant.
   * @param row
   * @return Found row
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  byte [] getRowKeyAtOrBefore(final byte [] row)
  throws IOException{
    // Map of HStoreKeys that are candidates for holding the row key that
    // most closely matches what we're looking for. We'll have to update it as
    // deletes are found all over the place as we go along before finally
    // reading the best key out of it at the end.
    SortedMap<HStoreKey, Long> candidateKeys = new TreeMap<HStoreKey, Long>(
      new HStoreKey.HStoreKeyWritableComparator(info));
    
    // Keep a list of deleted cell keys.  We need this because as we go through
    // the store files, the cell with the delete marker may be in one file and
    // the old non-delete cell value in a later store file. If we don't keep
    // around the fact that the cell was deleted in a newer record, we end up
    // returning the old value if user is asking for more than one version.
    // This List of deletes should not be large since we are only keeping rows
    // and columns that match those set on the scanner and which have delete
    // values.  If memory usage becomes an issue, could redo as bloom filter.
    Set<HStoreKey> deletes = new HashSet<HStoreKey>();
    this.lock.readLock().lock();
    try {
      // First go to the memcache.  Pick up deletes and candidates.
      this.memcache.getRowKeyAtOrBefore(row, candidateKeys, deletes);
      
      // Process each store file.  Run through from newest to oldest.
      // This code below is very close to the body of the getKeys method.
      MapFile.Reader[] maparray = getReaders();
      for (int i = maparray.length - 1; i >= 0; i--) {
        // Update the candidate keys from the current map file
        rowAtOrBeforeFromMapFile(maparray[i], row, candidateKeys, deletes);
      }
      // Return the best key from candidateKeys
      byte [] result =
        candidateKeys.isEmpty()? null: candidateKeys.lastKey().getRow();
      return result;
    } finally {
      this.lock.readLock().unlock();
    }
  }
  
  /*
   * Check an individual MapFile for the row at or before a given key 
   * and timestamp
   * @param map
   * @param row
   * @param candidateKeys
   * @throws IOException
   */
  private void rowAtOrBeforeFromMapFile(final MapFile.Reader map,
    final byte [] row, final SortedMap<HStoreKey, Long> candidateKeys,
    final Set<HStoreKey> deletes)
  throws IOException {
    HStoreKey startKey = new HStoreKey();
    ImmutableBytesWritable startValue = new ImmutableBytesWritable();
    synchronized(map) {
      // Don't bother with the rest of this if the file is empty
      map.reset();
      if (!map.next(startKey, startValue)) {
        return;
      }
      startKey.setHRegionInfo(this.info);
      // If start row for this file is beyond passed in row, return; nothing
      // in here is of use to us.
      if (HStoreKey.compareTwoRowKeys(this.info, startKey.getRow(), row) > 0) {
        return;
      }
      long now = System.currentTimeMillis();
      // if there aren't any candidate keys yet, we'll do some things different 
      if (candidateKeys.isEmpty()) {
        rowAtOrBeforeCandidate(startKey, map, row, candidateKeys, deletes, now);
      } else {
        rowAtOrBeforeWithCandidates(startKey, map, row, candidateKeys, deletes,
          now);
      }
    }
  }
  
  /* Find a candidate for row that is at or before passed row in passed
   * mapfile.
   * @param startKey First key in the mapfile.
   * @param map
   * @param row
   * @param candidateKeys
   * @param now
   * @throws IOException
   */
  private void rowAtOrBeforeCandidate(final HStoreKey startKey,
    final MapFile.Reader map, final byte[] row,
    final SortedMap<HStoreKey, Long> candidateKeys,
    final Set<HStoreKey> deletes, final long now) 
  throws IOException {
    // if the row we're looking for is past the end of this mapfile, set the
    // search key to be the last key.  If its a deleted key, then we'll back
    // up to the row before and return that.
    HStoreKey finalKey = getFinalKey(map);
    HStoreKey searchKey = null;
    if (HStoreKey.compareTwoRowKeys(info,finalKey.getRow(), row) < 0) {
      searchKey = finalKey;
    } else {
      searchKey = new HStoreKey(row, this.info);
      if (searchKey.compareTo(startKey) < 0) {
        searchKey = startKey;
      }
    }
    rowAtOrBeforeCandidate(map, searchKey, candidateKeys, deletes, now);
  }

  /* 
   * @param ttlSetting
   * @param hsk
   * @param now
   * @param deletes
   * @return True if key has not expired and is not in passed set of deletes.
   */
  static boolean notExpiredAndNotInDeletes(final long ttl,
      final HStoreKey hsk, final long now, final Set<HStoreKey> deletes) {
    return !isExpired(hsk, ttl, now) &&
      (deletes == null || !deletes.contains(hsk));
  }
  
  static boolean isExpired(final HStoreKey hsk, final long ttl,
      final long now) {
    return ttl != HConstants.FOREVER && now > hsk.getTimestamp() + ttl;
  }

  /* Find a candidate for row that is at or before passed key, sk, in mapfile.
   * @param map
   * @param sk Key to go search the mapfile with.
   * @param candidateKeys
   * @param now
   * @throws IOException
   * @see {@link #rowAtOrBeforeCandidate(HStoreKey, org.apache.hadoop.io.MapFile.Reader, byte[], SortedMap, long)}
   */
  private void rowAtOrBeforeCandidate(final MapFile.Reader map,
    final HStoreKey sk, final SortedMap<HStoreKey, Long> candidateKeys,
    final Set<HStoreKey> deletes, final long now)
  throws IOException {
    HStoreKey searchKey = sk;
    if (searchKey.getHRegionInfo() == null) {
      searchKey.setHRegionInfo(this.info);
    }
    HStoreKey readkey = null;
    ImmutableBytesWritable readval = new ImmutableBytesWritable();
    HStoreKey knownNoGoodKey = null;
    for (boolean foundCandidate = false; !foundCandidate;) {
      // Seek to the exact row, or the one that would be immediately before it
      readkey = (HStoreKey)map.getClosest(searchKey, readval, true);
      if (readkey == null) {
        // If null, we are at the start or end of the file.
        break;
      }
      HStoreKey deletedOrExpiredRow = null;
      do {
        // Set this region into the readkey.
        readkey.setHRegionInfo(this.info);
        // If we have an exact match on row, and it's not a delete, save this
        // as a candidate key
        if (HStoreKey.equalsTwoRowKeys(this.info, readkey.getRow(),
            searchKey.getRow())) {
          if (!HLogEdit.isDeleted(readval.get())) {
            if (handleNonDelete(readkey, now, deletes, candidateKeys)) {
              foundCandidate = true;
              // NOTE! Continue.
              continue;
            }
          }
          HStoreKey copy = addCopyToDeletes(readkey, deletes);
          if (deletedOrExpiredRow == null) {
            deletedOrExpiredRow = copy;
          }
        } else if (HStoreKey.compareTwoRowKeys(this.info, readkey.getRow(),
            searchKey.getRow()) > 0) {
          // if the row key we just read is beyond the key we're searching for,
          // then we're done.
          break;
        } else {
          // So, the row key doesn't match, but we haven't gone past the row
          // we're seeking yet, so this row is a candidate for closest
          // (assuming that it isn't a delete).
          if (!HLogEdit.isDeleted(readval.get())) {
            if (handleNonDelete(readkey, now, deletes, candidateKeys)) {
              foundCandidate = true;
              // NOTE: Continue
              continue;
            }
          }
          HStoreKey copy = addCopyToDeletes(readkey, deletes);
          if (deletedOrExpiredRow == null) {
            deletedOrExpiredRow = copy;
          }
        }
      } while(map.next(readkey, readval) && (knownNoGoodKey == null ||
          readkey.compareTo(knownNoGoodKey) < 0));

      // If we get here and have no candidates but we did find a deleted or
      // expired candidate, we need to look at the key before that
      if (!foundCandidate && deletedOrExpiredRow != null) {
        knownNoGoodKey = deletedOrExpiredRow;
        searchKey = new HStoreKey.BeforeThisStoreKey(deletedOrExpiredRow);
      } else {
        // No candidates and no deleted or expired candidates. Give up.
        break;
      }
    }
    
    // Arriving here just means that we consumed the whole rest of the map
    // without going "past" the key we're searching for. we can just fall
    // through here.
  }
  
  /*
   * @param key Key to copy and add to <code>deletes</code>
   * @param deletes
   * @return Instance of the copy added to <code>deletes</code>
   */
  private HStoreKey addCopyToDeletes(final HStoreKey key,
      final Set<HStoreKey> deletes) {
    HStoreKey copy = new HStoreKey(key);
    deletes.add(copy);
    return copy;
  }
  
  private void rowAtOrBeforeWithCandidates(final HStoreKey startKey,
    final MapFile.Reader map, final byte[] row,
    final SortedMap<HStoreKey, Long> candidateKeys,
    final Set<HStoreKey> deletes, final long now) 
  throws IOException {
    HStoreKey readkey = null;
    ImmutableBytesWritable readval = new ImmutableBytesWritable();

    // if there are already candidate keys, we need to start our search 
    // at the earliest possible key so that we can discover any possible
    // deletes for keys between the start and the search key.  Back up to start
    // of the row in case there are deletes for this candidate in this mapfile
    // BUT do not backup before the first key in the mapfile else getClosest
    // will return null
    HStoreKey searchKey = new HStoreKey(candidateKeys.firstKey().getRow(), this.info);
    if (searchKey.compareTo(startKey) < 0) {
      searchKey = startKey;
    }

    // Seek to the exact row, or the one that would be immediately before it
    readkey = (HStoreKey)map.getClosest(searchKey, readval, true);
    if (readkey == null) {
      // If null, we are at the start or end of the file.
      // Didn't find anything that would match, so return
      return;
    }

    do {
      // if we have an exact match on row, and it's not a delete, save this
      // as a candidate key
      if (Bytes.equals(readkey.getRow(), row)) {
        handleKey(readkey, readval.get(), now, deletes, candidateKeys);
      } else if (HStoreKey.compareTwoRowKeys(info, 
          readkey.getRow(), row) > 0 ) {
        // if the row key we just read is beyond the key we're searching for,
        // then we're done.
        break;
      } else {
        // So, the row key doesn't match, but we haven't gone past the row
        // we're seeking yet, so this row is a candidate for closest 
        // (assuming that it isn't a delete).
        handleKey(readkey, readval.get(), now, deletes, candidateKeys);
      }
    } while(map.next(readkey, readval));    
  }
  
  /*
   * @param readkey
   * @param now
   * @param deletes
   * @param candidateKeys
   */
  private void handleKey(final HStoreKey readkey, final byte [] value,
      final long now, final Set<HStoreKey> deletes,
      final SortedMap<HStoreKey, Long> candidateKeys) {
    if (!HLogEdit.isDeleted(value)) {
      handleNonDelete(readkey, now, deletes, candidateKeys);
    } else {
      // Pass copy because readkey will change next time next is called.
      handleDeleted(new HStoreKey(readkey), candidateKeys, deletes);
    }
  }
  
  /*
   * @param readkey
   * @param now
   * @param deletes
   * @param candidateKeys
   * @return True if we added a candidate.
   */
  private boolean handleNonDelete(final HStoreKey readkey, final long now,
      final Set<HStoreKey> deletes, final Map<HStoreKey, Long> candidateKeys) {
    if (notExpiredAndNotInDeletes(this.ttl, readkey, now, deletes)) {
      candidateKeys.put(stripTimestamp(readkey),
        Long.valueOf(readkey.getTimestamp()));
      return true;
    }
    return false;
  }

  /* Handle keys whose values hold deletes.
   * Add to the set of deletes and then if the candidate keys contain any that
   * might match by timestamp, then check for a match and remove it if it's too
   * young to survive the delete 
   * @param k Be careful; if key was gotten from a Mapfile, pass in a copy.
   * Values gotten by 'nexting' out of Mapfiles will change in each invocation.
   * @param candidateKeys
   * @param deletes
   */
  static void handleDeleted(final HStoreKey k,
      final SortedMap<HStoreKey, Long> candidateKeys,
      final Set<HStoreKey> deletes) {
    deletes.add(k);
    HStoreKey strippedKey = stripTimestamp(k);
    if (candidateKeys.containsKey(strippedKey)) {
      long bestCandidateTs = 
        candidateKeys.get(strippedKey).longValue();
      if (bestCandidateTs <= k.getTimestamp()) {
        candidateKeys.remove(strippedKey);
      }
    }
  }

  /*
   * @param mf MapFile to dig in.
   * @return Final key from passed <code>mf</code>
   * @throws IOException
   */
  private HStoreKey getFinalKey(final MapFile.Reader mf) throws IOException {
    HStoreKey finalKey = new HStoreKey(); 
    mf.finalKey(finalKey);
    finalKey.setHRegionInfo(this.info);
    return finalKey;
  }
  
  static HStoreKey stripTimestamp(HStoreKey key) {
    return new HStoreKey(key.getRow(), key.getColumn(), key.getHRegionInfo());
  }
    
  /*
   * Test that the <i>target</i> matches the <i>origin</i> cell address. If the 
   * <i>origin</i> has an empty column, then it's assumed to mean any column 
   * matches and only match on row and timestamp. Otherwise, it compares the
   * keys with HStoreKey.matchesRowCol().
   * @param origin The key we're testing against
   * @param target The key we're testing
   */
  private boolean cellMatches(HStoreKey origin, HStoreKey target){
    // if the origin's column is empty, then we're matching any column
    if (Bytes.equals(origin.getColumn(), HConstants.EMPTY_BYTE_ARRAY)) {
      // if the row matches, then...
      if (HStoreKey.equalsTwoRowKeys(info, target.getRow(), origin.getRow())) {
        // check the timestamp
        return target.getTimestamp() <= origin.getTimestamp();
      }
      return false;
    }
    // otherwise, we want to match on row and column
    return target.matchesRowCol(origin);
  }
    
  /*
   * Test that the <i>target</i> matches the <i>origin</i>. If the <i>origin</i>
   * has an empty column, then it just tests row equivalence. Otherwise, it uses
   * HStoreKey.matchesRowCol().
   * @param origin Key we're testing against
   * @param target Key we're testing
   */
  private boolean rowMatches(HStoreKey origin, HStoreKey target){
    // if the origin's column is empty, then we're matching any column
    if (Bytes.equals(origin.getColumn(), HConstants.EMPTY_BYTE_ARRAY)) {
      // if the row matches, then...
      return HStoreKey.equalsTwoRowKeys(info, target.getRow(), origin.getRow());
    }
    // otherwise, we want to match on row and column
    return target.matchesRowCol(origin);
  }
  
  /**
   * Determines if HStore can be split
   * @param force Whether to force a split or not.
   * @return a StoreSize if store can be split, null otherwise
   */
  StoreSize checkSplit(final boolean force) {
    if (this.storefiles.size() <= 0) {
      return null;
    }
    if (!force && (storeSize < this.desiredMaxFileSize)) {
      return null;
    }
    this.lock.readLock().lock();
    try {
      // Not splitable if we find a reference store file present in the store.
      boolean splitable = true;
      long maxSize = 0L;
      Long mapIndex = Long.valueOf(0L);
      // Iterate through all the MapFiles
      synchronized (storefiles) {
        for (Map.Entry<Long, HStoreFile> e: storefiles.entrySet()) {
          HStoreFile curHSF = e.getValue();
          if (splitable) {
            splitable = !curHSF.isReference();
            if (!splitable) {
              // RETURN IN MIDDLE OF FUNCTION!!! If not splitable, just return.
              if (LOG.isDebugEnabled()) {
                LOG.debug(curHSF +  " is not splittable");
              }
              return null;
            }
          }
          long size = curHSF.length();
          if (size > maxSize) {
            // This is the largest one so far
            maxSize = size;
            mapIndex = e.getKey();
          }
        }
      }

      // Cast to HbaseReader.
      HBaseMapFile.HBaseReader r =
        (HBaseMapFile.HBaseReader)this.readers.get(mapIndex);
      // Get first, last, and mid keys.
      r.reset();
      HStoreKey firstKey = new HStoreKey();
      HStoreKey lastKey = new HStoreKey();
      r.next(firstKey, new ImmutableBytesWritable());
      r.finalKey(lastKey);
      HStoreKey mk = (HStoreKey)r.midKey();
      if (mk != null) {
        // if the midkey is the same as the first and last keys, then we cannot
        // (ever) split this region. 
        if (HStoreKey.equalsTwoRowKeys(info, mk.getRow(), firstKey.getRow()) && 
            HStoreKey.equalsTwoRowKeys(info, mk.getRow(), lastKey.getRow())) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("cannot split because midkey is the same as first or last row");
          }
          return null;
        }
        return new StoreSize(maxSize, mk.getRow());
      }
    } catch(IOException e) {
      LOG.warn("Failed getting store size for " + this.storeNameStr, e);
    } finally {
      this.lock.readLock().unlock();
    }
    return null;
  }
  
  /** @return aggregate size of HStore */
  public long getSize() {
    return storeSize;
  }
  
  //////////////////////////////////////////////////////////////////////////////
  // File administration
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Return a scanner for both the memcache and the HStore files
   */
  protected InternalScanner getScanner(long timestamp, byte [][] targetCols,
      byte [] firstRow, RowFilterInterface filter)
  throws IOException {
    lock.readLock().lock();
    try {
      return new HStoreScanner(this, targetCols, firstRow, timestamp, filter);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public String toString() {
    return this.storeNameStr;
  }

  /**
   * @param p Path to check.
   * @return True if the path has format of a HStoreFile reference.
   */
  public static boolean isReference(final Path p) {
    return isReference(p, REF_NAME_PARSER.matcher(p.getName()));
  }
 
  private static boolean isReference(final Path p, final Matcher m) {
    if (m == null || !m.matches()) {
      LOG.warn("Failed match of store file name " + p.toString());
      throw new RuntimeException("Failed match of store file name " +
          p.toString());
    }
    return m.groupCount() > 1 && m.group(2) != null;
  }

  /**
   * @return Current list of store files.
   */
  SortedMap<Long, HStoreFile> getStorefiles() {
    synchronized (this.storefiles) {
      SortedMap<Long, HStoreFile> copy =
        new TreeMap<Long, HStoreFile>(this.storefiles);
      return copy;
    }
  }
  
  /**
   * @return Count of store files
   */
  int getStorefilesCount() {
    return this.storefiles.size();
  }

  /**
   * @return The size of the store file indexes, in bytes.
   * @throws IOException if there was a problem getting file sizes from the
   * filesystem
   */
  long getStorefilesIndexSize() throws IOException {
    long size = 0;
    for (HStoreFile s: storefiles.values())
      size += s.indexLength();
    return size;
  }

  /*
   * Datastructure that holds size and key.
   */
  class StoreSize {
    private final long size;
    private final byte[] key;
    StoreSize(long size, byte[] key) {
      this.size = size;
      this.key = new byte[key.length];
      System.arraycopy(key, 0, this.key, 0, key.length);
    }
    /* @return the size */
    long getSize() {
      return size;
    }
    /* @return the key */
    byte[] getKey() {
      return key;
    }
  }

  HRegionInfo getHRegionInfo() {
    return this.info;
  }
}
