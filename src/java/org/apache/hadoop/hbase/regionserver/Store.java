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

import java.io.EOFException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.KeyValue.KeyComparator;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.SequenceFile;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;

/**
  * A Store holds a column family in a Region.  Its a memstore and a set of zero
  * or more StoreFiles, which stretch backwards over time.
  *
  * <p>There's no reason to consider append-logging at this level; all logging 
  * and locking is handled at the HRegion level.  Store just provides
  * services to manage sets of StoreFiles.  One of the most important of those
  * services is compaction services where files are aggregated once they pass
  * a configurable threshold.
  *
  * <p>The only thing having to do with logs that Store needs to deal with is
  * the reconstructionLog.  This is a segment of an HRegion's log that might
  * NOT be present upon startup.  If the param is NULL, there's nothing to do.
  * If the param is non-NULL, we need to process the log to reconstruct
  * a TreeMap that might not have been written to disk before the process
  * died.
  *
  * <p>It's assumed that after this constructor returns, the reconstructionLog
  * file will be deleted (by whoever has instantiated the Store).
 *
 * <p>Locking and transactions are handled at a higher level.  This API should
 * not be called directly but by an HRegion manager.
 */
public class Store implements HConstants {
  static final Log LOG = LogFactory.getLog(Store.class);
  /**
   * Comparator that looks at columns and compares their family portions.
   * Presumes columns have already been checked for presence of delimiter.
   * If no delimiter present, presume the buffer holds a store name so no need
   * of a delimiter.
   */
  protected final MemStore memstore;
  // This stores directory in the filesystem.
  private final Path homedir;
  private final HRegionInfo regioninfo;
  private final HColumnDescriptor family;
  final FileSystem fs;
  private final HBaseConfiguration conf;
  // ttl in milliseconds.
  protected long ttl;
  private long majorCompactionTime;
  private int maxFilesToCompact;
  private final long desiredMaxFileSize;
  private volatile long storeSize = 0L;
  private final Object flushLock = new Object();
  final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  final byte [] storeName;
  private final String storeNameStr;

  /*
   * Sorted Map of readers keyed by maximum edit sequence id (Most recent should
   * be last in in list).  ConcurrentSkipListMap is lazily consistent so no
   * need to lock it down when iterating; iterator view is that of when the
   * iterator was taken out.
   */
  private final NavigableMap<Long, StoreFile> storefiles =
    new ConcurrentSkipListMap<Long, StoreFile>();

  // All access must be synchronized.
  private final CopyOnWriteArraySet<ChangedReadersObserver> changedReaderObservers =
    new CopyOnWriteArraySet<ChangedReadersObserver>();

  // The most-recent log-seq-ID.  The most-recent such ID means we can ignore
  // all log messages up to and including that ID (because they're already
  // reflected in the TreeMaps).
  private volatile long maxSeqId = -1;

  private final Path regionCompactionDir;
  private final Object compactLock = new Object();
  private final int compactionThreshold;
  private final int blocksize;
  private final boolean blockcache;
  private final Compression.Algorithm compression;
  
  // Comparing KeyValues
  final KeyValue.KVComparator comparator;
  final KeyValue.KVComparator comparatorIgnoringType;

  /**
   * Constructor
   * @param basedir qualified path under which the region directory lives;
   * generally the table subdirectory
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
  protected Store(Path basedir, HRegionInfo info, HColumnDescriptor family,
    FileSystem fs, Path reconstructionLog, HBaseConfiguration conf,
    final Progressable reporter)
  throws IOException {  
    this.homedir = getStoreHomedir(basedir, info.getEncodedName(),
      family.getName());
    this.regioninfo = info;
    this.family = family;
    this.fs = fs;
    this.conf = conf;
    this.blockcache = family.isBlockCacheEnabled();
    this.blocksize = family.getBlocksize();
    this.compression = family.getCompression();
    this.comparator = info.getComparator();
    this.comparatorIgnoringType = this.comparator.getComparatorIgnoringType();
    // getTimeToLive returns ttl in seconds.  Convert to milliseconds.
    this.ttl = family.getTimeToLive();
    if (ttl == HConstants.FOREVER) {
      // default is unlimited ttl.
      ttl = Long.MAX_VALUE;
    } else if (ttl == -1) {
      ttl = Long.MAX_VALUE;
    } else {
      // second -> ms adjust for user data
      this.ttl *= 1000;
    }
    this.memstore = new MemStore(this.ttl, this.comparator);
    this.regionCompactionDir = new Path(HRegion.getCompactionDir(basedir), 
        Integer.toString(info.getEncodedName()));
    this.storeName = this.family.getName();
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

    // loadStoreFiles calculates this.maxSeqId. as side-effect.
    this.storefiles.putAll(loadStoreFiles());

    // Do reconstruction log.
    runReconstructionLog(reconstructionLog, this.maxSeqId, reporter);
  }

  HColumnDescriptor getFamily() {
    return this.family;
  }

  long getMaxSequenceId() {
    return this.maxSeqId;
  }

  /**
   * @param tabledir
   * @param encodedName Encoded region name.
   * @param family
   * @return Path to family/Store home directory.
   */
  public static Path getStoreHomedir(final Path tabledir,
      final int encodedName, final byte [] family) {
    return new Path(tabledir, new Path(Integer.toString(encodedName),
      new Path(Bytes.toString(family))));
  }

  /*
   * Run reconstruction log
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
        " opening " + Bytes.toString(this.storeName) +
        " -- continuing.  Probably lack-of-HADOOP-1700 causing DATA LOSS!", e);
    } catch (IOException e) {
      // Presume we got here because of some HDFS issue. Don't just keep going.
      // Fail to open the HStore.  Probably means we'll fail over and over
      // again until human intervention but alternative has us skipping logs
      // and losing edits: HBASE-642.
      LOG.warn("Exception processing reconstruction log " + reconstructionLog +
        " opening " + Bytes.toString(this.storeName), e);
      throw e;
    }
  }

  /*
   * Read the reconstructionLog to see whether we need to build a brand-new 
   * file out of non-flushed log entries.  
   *
   * We can ignore any log message that has a sequence ID that's equal to or 
   * lower than maxSeqID.  (Because we know such log messages are already 
   * reflected in the MapFiles.)
   */
  private void doReconstructionLog(final Path reconstructionLog,
    final long maxSeqID, final Progressable reporter)
  throws UnsupportedEncodingException, IOException {
    if (reconstructionLog == null || !this.fs.exists(reconstructionLog)) {
      // Nothing to do.
      return;
    }
    // Check its not empty.
    FileStatus [] stats = this.fs.listStatus(reconstructionLog);
    if (stats == null || stats.length == 0) {
      LOG.warn("Passed reconstruction log " + reconstructionLog +
        " is zero-length");
      return;
    }
    // TODO: This could grow large and blow heap out.  Need to get it into
    // general memory usage accounting.
    long maxSeqIdInLog = -1;
    // TODO: Move this memstoring over into MemStore.
    ConcurrentSkipListMap<KeyValue, Object> reconstructedCache =
      MemStore.createMap(this.comparator);
    SequenceFile.Reader logReader = new SequenceFile.Reader(this.fs,
      reconstructionLog, this.conf);
    try {
      HLogKey key = new HLogKey();
      KeyValue val = new KeyValue();
      long skippedEdits = 0;
      long editsCount = 0;
      // How many edits to apply before we send a progress report.
      int reportInterval =
        this.conf.getInt("hbase.hstore.report.interval.edits", 2000);
      while (logReader.next(key, val)) {
        maxSeqIdInLog = Math.max(maxSeqIdInLog, key.getLogSeqNum());
        if (key.getLogSeqNum() <= maxSeqID) {
          skippedEdits++;
          continue;
        }
        // Check this edit is for me. Also, guard against writing the special
        // METACOLUMN info such as HBASE::CACHEFLUSH entries
        if (/* commented out for now - stack via jgray key.isTransactionEntry() || */
            val.matchingFamily(HLog.METAFAMILY) ||
          !Bytes.equals(key.getRegionName(), regioninfo.getRegionName()) ||
          !val.matchingFamily(family.getName())) {
          continue;
        }
        // Add anything as value as long as we use same instance each time.
        reconstructedCache.put(val, Boolean.TRUE);
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
   * Creates a series of StoreFile loaded from the given directory.
   * @throws IOException
   */
  private Map<Long, StoreFile> loadStoreFiles()
  throws IOException {
    Map<Long, StoreFile> results = new HashMap<Long, StoreFile>();
    FileStatus files[] = this.fs.listStatus(this.homedir);
    for (int i = 0; files != null && i < files.length; i++) {
      // Skip directories.
      if (files[i].isDir()) {
        continue;
      }
      Path p = files[i].getPath();
      // Check for empty file.  Should never be the case but can happen
      // after data loss in hdfs for whatever reason (upgrade, etc.): HBASE-646
      if (this.fs.getFileStatus(p).getLen() <= 0) {
        LOG.warn("Skipping " + p + " because its empty. HBASE-646 DATA LOSS?");
        continue;
      }
      StoreFile curfile = null;
      try {
        curfile = new StoreFile(fs, p, blockcache, this.conf);
      } catch (IOException ioe) {
        LOG.warn("Failed open of " + p + "; presumption is that file was " +
          "corrupted at flush and lost edits picked up by commit log replay. " +
          "Verify!", ioe);
        continue;
      }
      long storeSeqId = curfile.getMaxSequenceId();
      if (storeSeqId > this.maxSeqId) {
        this.maxSeqId = storeSeqId;
      }
      long length = curfile.getReader().length();
      this.storeSize += length;
      if (LOG.isDebugEnabled()) {
        LOG.debug("loaded " + FSUtils.getPath(p) + ", isReference=" +
          curfile.isReference() + ", sequence id=" + storeSeqId +
          ", length=" + length + ", majorCompaction=" +
          curfile.isMajorCompaction());
      }
      results.put(Long.valueOf(storeSeqId), curfile);
    }
    return results;
  }

  /**
   * Adds a value to the memstore
   * 
   * @param kv
   * @return memstore size delta
   */
  protected long add(final KeyValue kv) {
    lock.readLock().lock();
    try {
      return this.memstore.add(kv);
    } finally {
      lock.readLock().unlock();
    }
  }
  
  /**
   * Adds a value to the memstore
   * 
   * @param kv
   * @return memstore size delta
   */
  protected long delete(final KeyValue kv) {
    lock.readLock().lock();
    try {
      return this.memstore.delete(kv);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * @return All store files.
   */
  NavigableMap<Long, StoreFile> getStorefiles() {
    return this.storefiles;
  }

  /**
   * Close all the readers
   * 
   * We don't need to worry about subsequent requests because the HRegion holds
   * a write lock that will prevent any more reads or writes.
   * 
   * @throws IOException
   */
  List<StoreFile> close() throws IOException {
    this.lock.writeLock().lock();
    try {
      ArrayList<StoreFile> result =
        new ArrayList<StoreFile>(storefiles.values());
      // Clear so metrics doesn't find them.
      this.storefiles.clear();
      for (StoreFile f: result) {
        f.close();
      }
      LOG.debug("closed " + this.storeNameStr);
      return result;
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  /**
   * Snapshot this stores memstore.  Call before running
   * {@link #flushCache(long)} so it has some work to do.
   */
  void snapshot() {
    this.memstore.snapshot();
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
    // this.memstore.snapshot() has happened earlier up in the chain.
    ConcurrentSkipListMap<KeyValue, ?> cache = this.memstore.getSnapshot();
    // If an exception happens flushing, we let it out without clearing
    // the memstore snapshot.  The old snapshot will be returned when we say
    // 'snapshot', the next time flush comes around.
    StoreFile sf = internalFlushCache(cache, logCacheFlushId);
    if (sf == null) {
      return false;
    }
    // Add new file to store files.  Clear snapshot too while we have the
    // Store write lock.
    int size = updateStorefiles(logCacheFlushId, sf, cache);
    return size >= this.compactionThreshold;
  }

  /*
   * @param cache
   * @param logCacheFlushId
   * @return StoreFile created.
   * @throws IOException
   */
  private StoreFile internalFlushCache(final ConcurrentSkipListMap<KeyValue, ?> cache,
    final long logCacheFlushId)
  throws IOException {
    HFile.Writer writer = null;
    long flushed = 0;
    // Don't flush if there are no entries.
    if (cache.size() == 0) {
      return null;
    }
    long oldestTimestamp = System.currentTimeMillis() - ttl;
    // TODO:  We can fail in the below block before we complete adding this
    // flush to list of store files.  Add cleanup of anything put on filesystem
    // if we fail.
    synchronized (flushLock) {
      // A. Write the map out to the disk
      writer = getWriter();
      int entries = 0;
      try {
        for (Map.Entry<KeyValue, ?> entry: cache.entrySet()) {
          KeyValue kv = entry.getKey();
          if (!isExpired(kv, oldestTimestamp)) {
            writer.append(kv);
            entries++;
            flushed += this.memstore.heapSize(kv, true);
          }
        }
        // B. Write out the log sequence number that corresponds to this output
        // MapFile.  The MapFile is current up to and including logCacheFlushId.
        StoreFile.appendMetadata(writer, logCacheFlushId);
      } finally {
        writer.close();
      }
    }
    StoreFile sf = new StoreFile(this.fs, writer.getPath(), blockcache, 
      this.conf);
    this.storeSize += sf.getReader().length();
    if(LOG.isDebugEnabled()) {
      LOG.debug("Added " + sf + ", entries=" + sf.getReader().getEntries() +
        ", sequenceid=" + logCacheFlushId +
        ", memsize=" + StringUtils.humanReadableInt(flushed) +
        ", filesize=" + StringUtils.humanReadableInt(sf.getReader().length()) +
        " to " + this.regioninfo.getRegionNameAsString());
    }
    return sf;
  }

  /**
   * @return Writer for this store.
   * @throws IOException
   */
  HFile.Writer getWriter() throws IOException {
    return getWriter(this.homedir);
  }

  /*
   * @return Writer for this store.
   * @param basedir Directory to put writer in.
   * @throws IOException
   */
  private HFile.Writer getWriter(final Path basedir) throws IOException {
    return StoreFile.getWriter(this.fs, basedir, this.blocksize,
        this.compression, this.comparator.getRawComparator());
  }

  /*
   * Change storefiles adding into place the Reader produced by this new flush.
   * @param logCacheFlushId
   * @param sf
   * @param cache That was used to make the passed file <code>p</code>.
   * @throws IOException
   * @return Count of store files.
   */
  private int updateStorefiles(final long logCacheFlushId,
    final StoreFile sf, final NavigableMap<KeyValue, ?> cache)
  throws IOException {
    int count = 0;
    this.lock.writeLock().lock();
    try {
      this.storefiles.put(Long.valueOf(logCacheFlushId), sf);
      count = this.storefiles.size();
      // Tell listeners of the change in readers.
      notifyChangedReadersObservers();
      this.memstore.clearSnapshot(cache);
      return count;
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
    if(this.changedReaderObservers.size() > 0) {
      if (!this.changedReaderObservers.remove(o)) {
        LOG.warn("Not in set" + o);
      }
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // Compaction
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Compact the StoreFiles.  This method may take some time, so the calling 
   * thread must be able to block for long periods.
   * 
   * <p>During this time, the Store can work as usual, getting values from
   * MapFiles and writing new MapFiles from the memstore.
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
   * @param mc True to force a major compaction regardless of
   * thresholds
   * @return row to split around if a split is needed, null otherwise
   * @throws IOException
   */
  StoreSize compact(final boolean mc) throws IOException {
    boolean forceSplit = this.regioninfo.shouldSplit(false);
    boolean majorcompaction = mc;
    synchronized (compactLock) {
      long maxId = -1;
      // filesToCompact are sorted oldest to newest.
      List<StoreFile> filesToCompact = null;
      filesToCompact = new ArrayList<StoreFile>(this.storefiles.values());
      if (filesToCompact.size() <= 0) {
        LOG.debug(this.storeNameStr + ": no store files to compact");
        return null;
      }
      // The max-sequenceID in any of the to-be-compacted TreeMaps is the 
      // last key of storefiles.
      maxId = this.storefiles.lastKey().longValue();
      // Check to see if we need to do a major compaction on this region.
      // If so, change doMajorCompaction to true to skip the incremental
      // compacting below. Only check if doMajorCompaction is not true.
      if (!majorcompaction) {
        majorcompaction = isMajorCompaction(filesToCompact);
      }
      boolean references = hasReferences(filesToCompact);
      if (!majorcompaction && !references && 
          (forceSplit || (filesToCompact.size() < compactionThreshold))) {
        return checkSplit(forceSplit);
      }
      if (!fs.exists(this.regionCompactionDir) && !fs.mkdirs(this.regionCompactionDir)) {
        LOG.warn("Mkdir on " + this.regionCompactionDir.toString() + " failed");
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
        StoreFile file = filesToCompact.get(i);
        Path path = file.getPath();
        if (path == null) {
          LOG.warn("Path is null for " + file);
          return null;
        }
        long len = file.getReader().length();
        fileSizes[i] = len;
        totalSize += len;
      }
      if (!majorcompaction && !references) {
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
        filesToCompact = new ArrayList<StoreFile>(filesToCompact.subList(point,
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
 
      // Step through them, writing to the brand-new file
      HFile.Writer writer = getWriter(this.regionCompactionDir);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Started compaction of " + filesToCompact.size() + " file(s)" +
          (references? ", hasReferences=true,": " ") + " into " +
          FSUtils.getPath(writer.getPath()));
      }
      try {
        compact(writer, filesToCompact, majorcompaction);
      } finally {
        // Now, write out an HSTORE_LOGINFOFILE for the brand-new TreeMap.
        StoreFile.appendMetadata(writer, maxId, majorcompaction);
        writer.close();
      }

      // Move the compaction into place.
      completeCompaction(filesToCompact, writer);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Completed" + (majorcompaction? " major ": " ") +
          "compaction of " + this.storeNameStr +
          "; store size is " + StringUtils.humanReadableInt(storeSize));
      }
    }
    return checkSplit(forceSplit);
  }

  /*
   * @param files
   * @return True if any of the files in <code>files</code> are References.
   */
  private boolean hasReferences(Collection<StoreFile> files) {
    if (files != null && files.size() > 0) {
      for (StoreFile hsf: files) {
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

  /*
   * @return True if we should run a major compaction.
   */
  boolean isMajorCompaction() throws IOException {
    List<StoreFile> filesToCompact = null;
    // filesToCompact are sorted oldest to newest.
    filesToCompact = new ArrayList<StoreFile>(this.storefiles.values());
    return isMajorCompaction(filesToCompact);
  }

  /*
   * @param filesToCompact Files to compact. Can be null.
   * @return True if we should run a major compaction.
   */
  private boolean isMajorCompaction(final List<StoreFile> filesToCompact)
  throws IOException {
    boolean result = false;
    if (filesToCompact == null || filesToCompact.size() <= 0) {
      return result;
    }
    long lowTimestamp = getLowestTimestamp(fs,
      filesToCompact.get(0).getPath().getParent());
    long now = System.currentTimeMillis();
    if (lowTimestamp > 0l && lowTimestamp < (now - this.majorCompactionTime)) {
      // Major compaction time has elapsed.
      long elapsedTime = now - lowTimestamp;
      if (filesToCompact.size() == 1 &&
          filesToCompact.get(0).isMajorCompaction() &&
          (this.ttl == HConstants.FOREVER || elapsedTime < this.ttl)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skipping major compaction of " + this.storeNameStr +
            " because one (major) compacted file only and elapsedTime " +
            elapsedTime + "ms is < ttl=" + this.ttl);
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Major compaction triggered on store " + this.storeNameStr +
            "; time since last major compaction " + (now - lowTimestamp) + "ms");
        }
        result = true;
      }
    }
    return result;
  }

  /**
   * Do a minor/major compaction.  Uses the scan infrastructure to make it easy.
   * 
   * @param writer output writer
   * @param filesToCompact which files to compact
   * @param majorCompaction true to major compact (prune all deletes, max versions, etc)
   * @throws IOException
   */
  private void compact(HFile.Writer writer,
                       List<StoreFile> filesToCompact,
                       boolean majorCompaction) throws IOException {
    // for each file, obtain a scanner:
    KeyValueScanner [] scanners = new KeyValueScanner[filesToCompact.size()];
    // init:
    for(int i = 0; i < filesToCompact.size(); ++i) {
      // TODO open a new HFile.Reader w/o block cache.
      scanners[i] = new StoreFileScanner(filesToCompact.get(i).getReader().getScanner());
    }

    if (majorCompaction) {
      InternalScanner scanner = null;

      try {
        Scan scan = new Scan();
        scan.setMaxVersions(family.getMaxVersions());
        // TODO pass in the scanners/store files.
        scanner = new StoreScanner(this, scan, null);

        // since scanner.next() can return 'false' but still be delivering data,
        // we have to use a do/while loop.
        ArrayList<KeyValue> row = new ArrayList<KeyValue>();
        boolean more = true;
        while ( more ) {
          more = scanner.next(row);
          // output to writer:
          for (KeyValue kv : row) {
            writer.append(kv);
          }
          row.clear();
        }
      } finally {
        if (scanner != null)
          scanner.close();
      }
    } else {
      MinorCompactingStoreScanner scanner = null;
      try {
        scanner = new MinorCompactingStoreScanner(this, scanners);

        while ( scanner.next(writer) ) { }
      } finally {
        if (scanner != null)
          scanner.close();
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
  private void completeCompaction(final List<StoreFile> compactedFiles,
    final HFile.Writer compactedFile)
  throws IOException {
    // 1. Moving the new files into place.
    Path p = null;
    try {
      p = StoreFile.rename(this.fs, compactedFile.getPath(),
        StoreFile.getRandomFilename(fs, this.homedir));
    } catch (IOException e) {
      LOG.error("Failed move of compacted file " + compactedFile.getPath(), e);
      return;
    }
    StoreFile finalCompactedFile = new StoreFile(this.fs, p, blockcache, 
      this.conf);
    this.lock.writeLock().lock();
    try {
      try {
        // 3. Loading the new TreeMap.
        // Change this.storefiles so it reflects new state but do not
        // delete old store files until we have sent out notification of
        // change in case old files are still being accessed by outstanding
        // scanners.
        for (Map.Entry<Long, StoreFile> e: this.storefiles.entrySet()) {
          if (compactedFiles.contains(e.getValue())) {
            this.storefiles.remove(e.getKey());
          }
        }
        // Add new compacted Reader and store file.
        Long orderVal = Long.valueOf(finalCompactedFile.getMaxSequenceId());
        this.storefiles.put(orderVal, finalCompactedFile);
        // Tell observers that list of Readers has changed.
        notifyChangedReadersObservers();
        // Finally, delete old store files.
        for (StoreFile hsf: compactedFiles) {
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
      this.storeSize = 0L;
      for (StoreFile hsf : this.storefiles.values()) {
        this.storeSize += hsf.getReader().length();
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
   * @return the number of files in this store
   */
  public int getNumberOfstorefiles() {
    return this.storefiles.size();
  }
  

  /*
   * @param wantedVersions How many versions were asked for.
   * @return wantedVersions or this families' VERSIONS.
   */
  int versionsToReturn(final int wantedVersions) {
    if (wantedVersions <= 0) {
      throw new IllegalArgumentException("Number of versions must be > 0");
    }
    // Make sure we do not return more than maximum versions for this store.
    int maxVersions = this.family.getMaxVersions();
    return wantedVersions > maxVersions ? maxVersions: wantedVersions;
  }

  static void expiredOrDeleted(final Map<KeyValue, Object> set, final KeyValue kv) {
    boolean b = set.remove(kv) != null;
    if (LOG.isDebugEnabled()) {
      LOG.debug(kv.toString() + " expired: " + b);
    }
  }

  /**
   * Find the key that matches <i>row</i> exactly, or the one that immediately
   * preceeds it. WARNING: Only use this method on a table where writes occur 
   * with stricly increasing timestamps. This method assumes this pattern of 
   * writes in order to make it reasonably performant.
   * @param targetkey
   * @return Found keyvalue
   * @throws IOException
   */
  KeyValue getRowKeyAtOrBefore(final KeyValue targetkey)
  throws IOException{
    // Map of keys that are candidates for holding the row key that
    // most closely matches what we're looking for. We'll have to update it as
    // deletes are found all over the place as we go along before finally
    // reading the best key out of it at the end.   Use a comparator that
    // ignores key types.  Otherwise, we can't remove deleted items doing
    // set.remove because of the differing type between insert and delete.
    NavigableSet<KeyValue> candidates =
      new TreeSet<KeyValue>(this.comparator.getComparatorIgnoringType());

    // Keep a list of deleted cell keys.  We need this because as we go through
    // the store files, the cell with the delete marker may be in one file and
    // the old non-delete cell value in a later store file. If we don't keep
    // around the fact that the cell was deleted in a newer record, we end up
    // returning the old value if user is asking for more than one version.
    // This List of deletes should not be large since we are only keeping rows
    // and columns that match those set on the scanner and which have delete
    // values.  If memory usage becomes an issue, could redo as bloom filter.
    NavigableSet<KeyValue> deletes =
      new TreeSet<KeyValue>(this.comparatorIgnoringType);
    long now = System.currentTimeMillis();
    this.lock.readLock().lock();
    try {
      // First go to the memstore.  Pick up deletes and candidates.
      this.memstore.getRowKeyAtOrBefore(targetkey, candidates, deletes, now);
      // Process each store file.  Run through from newest to oldest.
      Map<Long, StoreFile> m = this.storefiles.descendingMap();
      for (Map.Entry<Long, StoreFile> e: m.entrySet()) {
        // Update the candidate keys from the current map file
        rowAtOrBeforeFromStoreFile(e.getValue(), targetkey, candidates,
          deletes, now);
      }
      // Return the best key from candidateKeys
      return candidates.isEmpty()? null: candidates.last();
    } finally {
      this.lock.readLock().unlock();
    }
  }

  /*
   * Check an individual MapFile for the row at or before a given key 
   * and timestamp
   * @param f
   * @param targetkey
   * @param candidates Pass a Set with a Comparator that
   * ignores key Type so we can do Set.remove using a delete, i.e. a KeyValue
   * with a different Type to the candidate key.
   * @throws IOException
   */
  private void rowAtOrBeforeFromStoreFile(final StoreFile f,
    final KeyValue targetkey, final NavigableSet<KeyValue> candidates,
    final NavigableSet<KeyValue> deletes, final long now)
  throws IOException {
    // if there aren't any candidate keys yet, we'll do some things different 
    if (candidates.isEmpty()) {
      rowAtOrBeforeCandidate(f, targetkey, candidates, deletes, now);
    } else {
      rowAtOrBeforeWithCandidates(f, targetkey, candidates, deletes, now);
    }
  }

  /* 
   * @param ttlSetting
   * @param hsk
   * @param now
   * @param deletes A Set whose Comparator ignores Type.
   * @return True if key has not expired and is not in passed set of deletes.
   */
  static boolean notExpiredAndNotInDeletes(final long ttl,
      final KeyValue key, final long now, final Set<KeyValue> deletes) {
    return !isExpired(key, now-ttl) && (deletes == null || deletes.isEmpty() ||
        !deletes.contains(key));
  }

  static boolean isExpired(final KeyValue key, final long oldestTimestamp) {
    return key.getTimestamp() < oldestTimestamp;
  }

  /* Find a candidate for row that is at or before passed key, searchkey, in hfile.
   * @param f
   * @param targetkey Key to go search the hfile with.
   * @param candidates
   * @param now
   * @throws IOException
   * @see {@link #rowAtOrBeforeCandidate(HStoreKey, org.apache.hadoop.io.MapFile.Reader, byte[], SortedMap, long)}
   */
  private void rowAtOrBeforeCandidate(final StoreFile f,
    final KeyValue targetkey, final NavigableSet<KeyValue> candidates,
    final NavigableSet<KeyValue> deletes, final long now)
  throws IOException {
    KeyValue search = targetkey;
    // If the row we're looking for is past the end of this mapfile, set the
    // search key to be the last key.  If its a deleted key, then we'll back
    // up to the row before and return that.
    // TODO: Cache last key as KV over in the file.
    byte [] lastkey = f.getReader().getLastKey();
    KeyValue lastKeyValue =
      KeyValue.createKeyValueFromKey(lastkey, 0, lastkey.length);
    if (this.comparator.compareRows(lastKeyValue, targetkey) < 0) {
      search = lastKeyValue;
    }
    KeyValue knownNoGoodKey = null;
    HFileScanner scanner = f.getReader().getScanner();
    for (boolean foundCandidate = false; !foundCandidate;) {
      // Seek to the exact row, or the one that would be immediately before it
      int result = scanner.seekTo(search.getBuffer(), search.getKeyOffset(),
        search.getKeyLength());
      if (result < 0) {
        // Not in file.
        break;
      }
      KeyValue deletedOrExpiredRow = null;
      KeyValue kv = null;
      do {
        kv = scanner.getKeyValue();
        if (this.comparator.compareRows(kv, search) <= 0) {
          if (!kv.isDeleteType()) {
            if (handleNonDelete(kv, now, deletes, candidates)) {
              foundCandidate = true;
              // NOTE! Continue.
              continue;
            }
          }
          deletes.add(kv);
          if (deletedOrExpiredRow == null) {
            deletedOrExpiredRow = kv;
          }
        } else if (this.comparator.compareRows(kv, search) > 0) {
          // if the row key we just read is beyond the key we're searching for,
          // then we're done.
          break;
        } else {
          // So, the row key doesn't match, but we haven't gone past the row
          // we're seeking yet, so this row is a candidate for closest
          // (assuming that it isn't a delete).
          if (!kv.isDeleteType()) {
            if (handleNonDelete(kv, now, deletes, candidates)) {
              foundCandidate = true;
              // NOTE: Continue
              continue;
            }
          }
          deletes.add(kv);
          if (deletedOrExpiredRow == null) {
            deletedOrExpiredRow = kv;
          }
        }
      } while(scanner.next() && (knownNoGoodKey == null ||
          this.comparator.compare(kv, knownNoGoodKey) < 0));

      // If we get here and have no candidates but we did find a deleted or
      // expired candidate, we need to look at the key before that
      if (!foundCandidate && deletedOrExpiredRow != null) {
        knownNoGoodKey = deletedOrExpiredRow;
        if (!scanner.seekBefore(deletedOrExpiredRow.getBuffer(),
            deletedOrExpiredRow.getKeyOffset(),
            deletedOrExpiredRow.getKeyLength())) {
          // Not in file -- what can I do now but break?
          break;
        }
        search = scanner.getKeyValue();
      } else {
        // No candidates and no deleted or expired candidates. Give up.
        break;
      }
    }
    
    // Arriving here just means that we consumed the whole rest of the map
    // without going "past" the key we're searching for. we can just fall
    // through here.
  }

  private void rowAtOrBeforeWithCandidates(final StoreFile f,
    final KeyValue targetkey,
    final NavigableSet<KeyValue> candidates,
    final NavigableSet<KeyValue> deletes, final long now) 
  throws IOException {
    // if there are already candidate keys, we need to start our search 
    // at the earliest possible key so that we can discover any possible
    // deletes for keys between the start and the search key.  Back up to start
    // of the row in case there are deletes for this candidate in this mapfile
    // BUT do not backup before the first key in the store file.
    KeyValue firstCandidateKey = candidates.first();
    KeyValue search = null;
    if (this.comparator.compareRows(firstCandidateKey, targetkey) < 0) {
      search = targetkey;
    } else {
      search = firstCandidateKey;
    }

    // Seek to the exact row, or the one that would be immediately before it
    HFileScanner scanner = f.getReader().getScanner();
    int result = scanner.seekTo(search.getBuffer(), search.getKeyOffset(),
      search.getKeyLength());
    if (result < 0) {
      // Key is before start of this file.  Return.
      return;
    }
    do {
      KeyValue kv = scanner.getKeyValue();
      // if we have an exact match on row, and it's not a delete, save this
      // as a candidate key
      if (this.comparator.matchingRows(kv, targetkey)) {
        handleKey(kv, now, deletes, candidates);
      } else if (this.comparator.compareRows(kv, targetkey) > 0 ) {
        // if the row key we just read is beyond the key we're searching for,
        // then we're done.
        break;
      } else {
        // So, the row key doesn't match, but we haven't gone past the row
        // we're seeking yet, so this row is a candidate for closest 
        // (assuming that it isn't a delete).
        handleKey(kv, now, deletes, candidates);
      }
    } while(scanner.next());
  }

  /*
   * Used calculating keys at or just before a passed key.
   * @param readkey
   * @param now
   * @param deletes Set with Comparator that ignores key type.
   * @param candidate Set with Comprator that ignores key type.
   */
  private void handleKey(final KeyValue readkey, final long now,
      final NavigableSet<KeyValue> deletes,
      final NavigableSet<KeyValue> candidates) {
    if (!readkey.isDeleteType()) {
      handleNonDelete(readkey, now, deletes, candidates);
    } else {
      handleDeletes(readkey, candidates, deletes);
    }
  }

  /*
   * Used calculating keys at or just before a passed key.
   * @param readkey
   * @param now
   * @param deletes Set with Comparator that ignores key type.
   * @param candidates Set with Comparator that ignores key type.
   * @return True if we added a candidate.
   */
  private boolean handleNonDelete(final KeyValue readkey, final long now,
      final NavigableSet<KeyValue> deletes,
      final NavigableSet<KeyValue> candidates) {
    if (notExpiredAndNotInDeletes(this.ttl, readkey, now, deletes)) {
      candidates.add(readkey);
      return true;
    }
    return false;
  }

  /**
   * Handle keys whose values hold deletes.
   * Add to the set of deletes and then if the candidate keys contain any that
   * might match, then check for a match and remove it.  Implies candidates
   * is made with a Comparator that ignores key type.
   * @param k
   * @param candidates
   * @param deletes
   * @return True if we removed <code>k</code> from <code>candidates</code>.
   */
  static boolean handleDeletes(final KeyValue k,
      final NavigableSet<KeyValue> candidates,
      final NavigableSet<KeyValue> deletes) {
    deletes.add(k);
    return candidates.remove(k);
  }

  /**
   * Determines if HStore can be split
   * @param force Whether to force a split or not.
   * @return a StoreSize if store can be split, null otherwise.
   */
  StoreSize checkSplit(final boolean force) {
    this.lock.readLock().lock();
    try {
      // Iterate through all store files
      if (this.storefiles.size() <= 0) {
        return null;
      }
      if (!force && (storeSize < this.desiredMaxFileSize)) {
        return null;
      }
      // Not splitable if we find a reference store file present in the store.
      boolean splitable = true;
      long maxSize = 0L;
      Long mapIndex = Long.valueOf(0L);
      for (Map.Entry<Long, StoreFile> e: storefiles.entrySet()) {
        StoreFile sf = e.getValue();
        if (splitable) {
          splitable = !sf.isReference();
          if (!splitable) {
            // RETURN IN MIDDLE OF FUNCTION!!! If not splitable, just return.
            if (LOG.isDebugEnabled()) {
              LOG.debug(sf +  " is not splittable");
            }
            return null;
          }
        }
        long size = sf.getReader().length();
        if (size > maxSize) {
          // This is the largest one so far
          maxSize = size;
          mapIndex = e.getKey();
        }
      }

      HFile.Reader r = this.storefiles.get(mapIndex).getReader();
      // Get first, last, and mid keys.  Midkey is the key that starts block
      // in middle of hfile.  Has column and timestamp.  Need to return just
      // the row we want to split on as midkey.
      byte [] midkey = r.midkey();
      if (midkey != null) {
        KeyValue mk = KeyValue.createKeyValueFromKey(midkey, 0, midkey.length);
        byte [] fk = r.getFirstKey();
        KeyValue firstKey = KeyValue.createKeyValueFromKey(fk, 0, fk.length);
        byte [] lk = r.getLastKey();
        KeyValue lastKey = KeyValue.createKeyValueFromKey(lk, 0, lk.length);
        // if the midkey is the same as the first and last keys, then we cannot
        // (ever) split this region. 
        if (this.comparator.compareRows(mk, firstKey) == 0 && 
            this.comparator.compareRows(mk, lastKey) == 0) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("cannot split because midkey is the same as first or " +
              "last row");
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
   * Return a scanner for both the memstore and the HStore files
   */
  protected KeyValueScanner getScanner(Scan scan,
      final NavigableSet<byte []> targetCols) {
    lock.readLock().lock();
    try {
      return new StoreScanner(this, scan, targetCols);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public String toString() {
    return this.storeNameStr;
  }

  /**
   * @return Count of store files
   */
  int getStorefilesCount() {
    return this.storefiles.size();
  }

  /**
   * @return The size of the store file indexes, in bytes.
   */
  long getStorefilesIndexSize() {
    long size = 0;
    for (StoreFile s: storefiles.values())
      size += s.getReader().indexSize();
    return size;
  }

  /*
   * Datastructure that holds size and row to split a file around.
   * TODO: Take a KeyValue rather than row.
   */
  static class StoreSize {
    private final long size;
    private final byte [] row;

    StoreSize(long size, byte [] row) {
      this.size = size;
      this.row = row;
    }
    /* @return the size */
    long getSize() {
      return size;
    }

    byte [] getSplitRow() {
      return this.row;
    }
  }

  HRegionInfo getHRegionInfo() {
    return this.regioninfo;
  }

  /**
   * Convenience method that implements the old MapFile.getClosest on top of
   * HFile Scanners.  getClosest used seek to the asked-for key or just after
   * (HFile seeks to the key or just before).
   * @param s Scanner to use
   * @param kv Key to find.
   * @return True if we were able to seek the scanner to <code>b</code> or to
   * the key just after.
   * @throws IOException 
   */
  static boolean getClosest(final HFileScanner s, final KeyValue kv)
  throws IOException {
    // Pass offsets to key content of a KeyValue; thats whats in the hfile index.
    int result = s.seekTo(kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength());
    if (result < 0) {
      // Not in file.  Will the first key do?
      if (!s.seekTo()) {
        return false;
      }
    } else if (result > 0) {
      // Less than what was asked for but maybe < because we're asking for
      // r/c/LATEST_TIMESTAMP -- what was returned was r/c-1/SOME_TS...
      // A next will get us a r/c/SOME_TS.
      if (!s.next()) {
        return false;
      }
    }
    return true;
  }

  /**
   * @param kv
   * @param columns Can be null
   * @return True if column matches.
   */
  static boolean matchingColumns(final KeyValue kv, final Set<byte []> columns) {
    if (columns == null) {
      return true;
    }
    // Only instantiate columns if lots of columns to test.
    if (columns.size() > 100) {
      return columns.contains(kv.getColumn());
    }
    for (byte [] column: columns) {
      if (kv.matchingColumn(column)) {
        return true;
      }
    }
    return false;
  }
  
  //
  // HBASE-880/1249/1304
  //
  
  /**
   * Retrieve results from this store given the specified Get parameters.
   * @param get Get operation
   * @param columns List of columns to match, can be empty (not null)
   * @param result List to add results to 
   * @throws IOException
   */
  public void get(Get get, NavigableSet<byte[]> columns, List<KeyValue> result) 
  throws IOException {
    KeyComparator keyComparator = this.comparator.getRawComparator();

    // Column matching and version enforcement
    QueryMatcher matcher = new QueryMatcher(get, this.family.getName(), columns,
      this.ttl, keyComparator, versionsToReturn(get.getMaxVersions()));
    
    // Read from memstore
    if(this.memstore.get(matcher, result)) {
      // Received early-out from memstore
      return;
    }
    
    // Check if we even have storefiles
    if(this.storefiles.isEmpty()) {
      return;
    }
    
    // Get storefiles for this store
    List<HFileScanner> storefileScanners = new ArrayList<HFileScanner>();
    for(StoreFile sf : this.storefiles.descendingMap().values()) {
      storefileScanners.add(sf.getReader().getScanner());
    }
    
    // StoreFileGetScan will handle reading this store's storefiles
    StoreFileGetScan scanner = new StoreFileGetScan(storefileScanners, matcher);
    
    // Run a GET scan and put results into the specified list 
    scanner.get(result);
  }

  public static class ValueAndSize {
    public long value;
    public long sizeAdded;
    public ValueAndSize(long value, long sizeAdded) {
      this.value = value;
      this.sizeAdded = sizeAdded;
    }
  }
  
  /**
   * Increments the value for the given row/family/qualifier
   * @param row
   * @param f
   * @param qualifier
   * @param amount
   * @return The new value.
   * @throws IOException
   */
  public ValueAndSize incrementColumnValue(byte [] row, byte [] f,
      byte [] qualifier, long amount) throws IOException {
    long value = 0;
    List<KeyValue> result = new ArrayList<KeyValue>();
    KeyComparator keyComparator = this.comparator.getRawComparator();

    // Setting up the QueryMatcher
    Get get = new Get(row);
    NavigableSet<byte[]> qualifiers = 
      new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    qualifiers.add(qualifier);
    QueryMatcher matcher = new QueryMatcher(get, f, qualifiers, this.ttl,
      keyComparator, 1);
    
    // Read from memstore
    if(this.memstore.get(matcher, result)) {
      // Received early-out from memstore
      KeyValue kv = result.get(0);
      byte [] buffer = kv.getBuffer();
      int valueOffset = kv.getValueOffset();
      value = Bytes.toLong(buffer, valueOffset, Bytes.SIZEOF_LONG) + amount;
      Bytes.putBytes(buffer, valueOffset, Bytes.toBytes(value), 0, 
          Bytes.SIZEOF_LONG);
      return new ValueAndSize(value, 0);
    }
    // Check if we even have storefiles
    if(this.storefiles.isEmpty()) {
      return addNewKeyValue(row, f, qualifier, value, amount);
    }
    
    // Get storefiles for this store
    List<HFileScanner> storefileScanners = new ArrayList<HFileScanner>();
    for(StoreFile sf : this.storefiles.descendingMap().values()) {
      storefileScanners.add(sf.getReader().getScanner());
    }
    
    // StoreFileGetScan will handle reading this store's storefiles
    StoreFileGetScan scanner = new StoreFileGetScan(storefileScanners, matcher);
    
    // Run a GET scan and put results into the specified list 
    scanner.get(result);
    if(result.size() > 0) {
      value = Bytes.toLong(result.get(0).getValue());
    }
    return addNewKeyValue(row, f, qualifier, value, amount);
  }
  
  private ValueAndSize addNewKeyValue(byte [] row, byte [] f, byte [] qualifier,
      long value, long amount) {
    long newValue = value + amount;
    KeyValue newKv = new KeyValue(row, f, qualifier,
        System.currentTimeMillis(),
        Bytes.toBytes(newValue));
    add(newKv);
    return new ValueAndSize(newValue, newKv.heapSize());
  }
}