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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.SequenceFile;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;

/**
  * A Store holds a column family in a Region.  Its a memcache and a set of zero
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
  protected final Memcache memcache;
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

  private final Path compactionDir;
  private final Object compactLock = new Object();
  private final int compactionThreshold;
  private final int blocksize;
  private final boolean bloomfilter;
  private final Compression.Algorithm compression;
  
  // Comparing HStoreKeys in byte arrays.
  final HStoreKey.StoreKeyComparator rawcomparator;
  // Comparing HStoreKey objects.
  final Comparator<HStoreKey> comparator;

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
  @SuppressWarnings("unchecked")
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
    this.bloomfilter = family.isBloomfilter();
    this.blocksize = family.getBlocksize();
    this.compression = family.getCompression();
    this.rawcomparator = info.isRootRegion()?
      new HStoreKey.RootStoreKeyComparator(): info.isMetaRegion()?
        new HStoreKey.MetaStoreKeyComparator():
          new HStoreKey.StoreKeyComparator();
    this.comparator = info.isRootRegion()?
      new HStoreKey.HStoreKeyRootComparator(): info.isMetaRegion()?
        new HStoreKey.HStoreKeyMetaComparator():
          new HStoreKey.HStoreKeyComparator();
    // getTimeToLive returns ttl in seconds.  Convert to milliseconds.
    this.ttl = family.getTimeToLive();
    if (ttl != HConstants.FOREVER) {
      this.ttl *= 1000;
    }
    this.memcache = new Memcache(this.ttl, this.comparator, this.rawcomparator);
    this.compactionDir = HRegion.getCompactionDir(basedir);
    this.storeName = Bytes.toBytes(this.regioninfo.getEncodedName() + "/" +
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

    // loadStoreFiles calculates this.maxSeqId. as side-effect.
    this.storefiles.putAll(loadStoreFiles());
    if (LOG.isDebugEnabled() && this.storefiles.size() > 0) {
      LOG.debug("Loaded " + this.storefiles.size() + " file(s) in Store " +
        Bytes.toString(this.storeName) + ", max sequence id " + this.maxSeqId);
    }

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
    NavigableMap<HStoreKey, byte []> reconstructedCache =
      new TreeMap<HStoreKey, byte []>(this.comparator);
    SequenceFile.Reader logReader = null;
    try {
      logReader = new SequenceFile.Reader(this.fs, reconstructionLog, this.conf);
    } catch (IOException e) {
      LOG.warn("Failed opening reconstruction log though check for null-size passed. " +
        "POSSIBLE DATA LOSS!! Soldiering on", e);
      return;
    }

    try {
      HLogKey key = new HLogKey();
      HLogEdit val = new HLogEdit();
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
        // Check this edit is for me. Also, guard against writing
        // METACOLUMN info such as HBASE::CACHEFLUSH entries
        byte [] column = val.getColumn();
        if (val.isTransactionEntry() || Bytes.equals(column, HLog.METACOLUMN)
            || !Bytes.equals(key.getRegionName(), regioninfo.getRegionName())
            || !HStoreKey.matchingFamily(family.getName(), column)) {
          continue;
        }
        HStoreKey k = new HStoreKey(key.getRow(), column, val.getTimestamp());
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
      StoreFile curfile = new StoreFile(fs, p);
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
    // If an exception happens flushing, we let it out without clearing
    // the memcache snapshot.  The old snapshot will be returned when we say
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
  private StoreFile internalFlushCache(final SortedMap<HStoreKey, byte []> cache,
    final long logCacheFlushId)
  throws IOException {
    HFile.Writer writer = null;
    long flushed = 0;
    // Don't flush if there are no entries.
    if (cache.size() == 0) {
      return null;
    }
    long now = System.currentTimeMillis();
    // TODO:  We can fail in the below block before we complete adding this
    // flush to list of store files.  Add cleanup of anything put on filesystem
    // if we fail.
    synchronized (flushLock) {
      // A. Write the map out to the disk
      writer = getWriter();
      int entries = 0;
      try {
        for (Map.Entry<HStoreKey, byte []> es: cache.entrySet()) {
          HStoreKey curkey = es.getKey();
          byte[] bytes = es.getValue();
          if (!isExpired(curkey, ttl, now)) {
            writer.append(curkey.getBytes(), bytes);
            entries++;
            flushed += this.memcache.heapSize(curkey, bytes, null);
          }
        }
        // B. Write out the log sequence number that corresponds to this output
        // MapFile.  The MapFile is current up to and including logCacheFlushId.
        StoreFile.appendMetadata(writer, logCacheFlushId);
      } finally {
        writer.close();
      }
    }
    StoreFile sf = new StoreFile(this.fs, writer.getPath());
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
    return StoreFile.getWriter(this.fs, this.homedir, this.blocksize,
        this.compression, this.rawcomparator, this.bloomfilter);
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
    final StoreFile sf, final SortedMap<HStoreKey, byte []> cache)
  throws IOException {
    int count = 0;
    this.lock.writeLock().lock();
    try {
      this.storefiles.put(Long.valueOf(logCacheFlushId), sf);
      count = this.storefiles.size();
      // Tell listeners of the change in readers.
      notifyChangedReadersObservers();
      this.memcache.clearSnapshot(cache);
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
    if (!this.changedReaderObservers.remove(o)) {
      LOG.warn("Not in set" + o);
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
      HFile.Writer writer = getWriter();
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
        LOG.debug("Completed " + (majorcompaction? "major": "") +
          " compaction of " + this.storeNameStr +
          " store size is " + StringUtils.humanReadableInt(storeSize));
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

  /*
   * @param r StoreFile list to reverse
   * @return A new array of content of <code>readers</code>, reversed.
   */
  private StoreFile [] reverse(final List<StoreFile> r) {
    List<StoreFile> copy = new ArrayList<StoreFile>(r);
    Collections.reverse(copy);
    // MapFile.Reader is instance of StoreFileReader so this should be ok.
    return copy.toArray(new StoreFile[0]);
  }

  /*
   * @param rdrs List of StoreFiles
   * @param keys Current keys
   * @param done Which readers are done
   * @return The lowest current key in passed <code>rdrs</code>
   */
  private int getLowestKey(final HFileScanner [] rdrs, final ByteBuffer [] keys,
      final boolean [] done) {
    int lowestKey = -1;
    for (int i = 0; i < rdrs.length; i++) {
      if (done[i]) {
        continue;
      }
      if (lowestKey < 0) {
        lowestKey = i;
      } else {
        RawComparator<byte []> c = rdrs[i].getReader().getComparator();
        if (c.compare(keys[i].array(), keys[i].arrayOffset(), keys[i].limit(),
            keys[lowestKey].array(), keys[lowestKey].arrayOffset(),
              keys[lowestKey].limit()) < 0) {
          lowestKey = i;
        }
      }
    }
    return lowestKey;
  }

  /*
   * Compact a list of StoreFiles.
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
  private void compact(final HFile.Writer compactedOut,
      final List<StoreFile> pReaders, final boolean majorCompaction)
  throws IOException {
    // Reverse order so newest store file is first.
    StoreFile[] files = reverse(pReaders);
    HFileScanner[] rdrs = new HFileScanner[files.length];
    ByteBuffer[] keys = new ByteBuffer[rdrs.length];
    ByteBuffer[] vals = new ByteBuffer[rdrs.length];
    boolean[] done = new boolean[rdrs.length];
    // Now, advance through the readers in order. This will have the
    // effect of a run-time sort of the entire dataset.
    int numDone = 0;
    for (int i = 0; i < rdrs.length; i++) {
      rdrs[i] = files[i].getReader().getScanner();
      done[i] = !rdrs[i].seekTo();
      if (done[i]) {
        numDone++;
      } else {
        keys[i] = rdrs[i].getKey();
        vals[i] = rdrs[i].getValue();
      }
    }

    long now = System.currentTimeMillis();
    int timesSeen = 0;
    HStoreKey lastSeen = new HStoreKey();
    HStoreKey lastDelete = null;
    while (numDone < done.length) {
      // Get lowest key in all store files.
      int lowestKey = getLowestKey(rdrs, keys, done);
      // TODO: Suboptimal. And below where we are going from ByteBuffer to
      // byte array. FIX!! Can we get rid of HSK instantiations?
      HStoreKey hsk = HStoreKey.create(keys[lowestKey]);
      // If its same row and column as last key, increment times seen.
      if (HStoreKey.equalsTwoRowKeys(lastSeen.getRow(), hsk.getRow())
          && Bytes.equals(lastSeen.getColumn(), hsk.getColumn())) {
        timesSeen++;
        // Reset last delete if not exact timestamp -- lastDelete only stops
        // exactly the same key making it out to the compacted store file.
        if (lastDelete != null
            && lastDelete.getTimestamp() != hsk.getTimestamp()) {
          lastDelete = null;
        }
      } else {
        timesSeen = 1;
        lastDelete = null;
      }

      // Don't write empty rows or columns. Only remove cells on major
      // compaction. Remove if expired of > VERSIONS
      if (hsk.getRow().length != 0 && hsk.getColumn().length != 0) {
        ByteBuffer value = vals[lowestKey];
        if (!majorCompaction) {
          // Write out all values if not a major compaction.
          compactedOut.append(hsk.getBytes(), Bytes.toBytes(value));
        } else {
          boolean expired = false;
          boolean deleted = false;
          if (timesSeen <= family.getMaxVersions()
              && !(expired = isExpired(hsk, ttl, now))) {
            // If this value key is same as a deleted key, skip
            if (lastDelete != null && hsk.equals(lastDelete)) {
              deleted = true;
            } else if (HLogEdit.isDeleted(value)) {
              // If a deleted value, skip
              deleted = true;
              lastDelete = hsk;
            } else {
              compactedOut.append(hsk.getBytes(), Bytes.toBytes(value));
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
      lastSeen = hsk;

      // Advance the smallest key. If that reader's all finished, then
      // mark it as done.
      if (!rdrs[lowestKey].next()) {
        done[lowestKey] = true;
        rdrs[lowestKey] = null;
        numDone++;
      } else {
        keys[lowestKey] = rdrs[lowestKey].getKey();
        vals[lowestKey] = rdrs[lowestKey].getValue();
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
    StoreFile finalCompactedFile = new StoreFile(this.fs, p);
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
   * Return all the available columns for the given key.  The key indicates a 
   * row and timestamp, but not a column name.
   *
   * The returned object should map column names to Cells.
   */
  void getFull(HStoreKey key, final Set<byte []> columns,
      final int numVersions, Map<byte [], Cell> results)
  throws IOException {
    int versions = versionsToReturn(numVersions);
    // This is map of columns to timestamp
    Map<byte [], Long> deletes =
      new TreeMap<byte [], Long>(Bytes.BYTES_COMPARATOR);
    // if the key is null, we're not even looking for anything. return.
    if (key == null) {
      return;
    }
    
    this.lock.readLock().lock();
    // get from the memcache first.
    this.memcache.getFull(key, columns, versions, deletes, results);
    try {
      Map<Long, StoreFile> m = this.storefiles.descendingMap();
      for (Iterator<Map.Entry<Long, StoreFile>> i = m.entrySet().iterator();
          i.hasNext();) {
        getFullFromStoreFile(i.next().getValue(), key, columns, versions, deletes, results);
      }
    } finally {
      this.lock.readLock().unlock();
    }
  }

  private void getFullFromStoreFile(StoreFile f, HStoreKey key, 
    Set<byte []> columns, int numVersions, Map<byte [], Long> deletes,
    Map<byte [], Cell> results) 
  throws IOException {
    long now = System.currentTimeMillis();
    HFileScanner scanner = f.getReader().getScanner();
    if (!getClosest(scanner, key.getBytes())) {
      return;
    }
    do {
      HStoreKey readkey = HStoreKey.create(scanner.getKey());
      byte[] readcol = readkey.getColumn();

      // if we're looking for this column (or all of them), and there isn't
      // already a value for this column in the results map or there is a value
      // but we haven't collected enough versions yet, and the key we
      // just read matches, then we'll consider it
      if ((columns == null || columns.contains(readcol)) &&
          (!results.containsKey(readcol) ||
            results.get(readcol).getNumValues() < numVersions) &&
          key.matchesWithoutColumn(readkey)) {
        // if the value of the cell we're looking at right now is a delete,
        // we need to treat it differently
        ByteBuffer value = scanner.getValue();
        if (HLogEdit.isDeleted(value)) {
          // if it's not already recorded as a delete or recorded with a more
          // recent delete timestamp, record it for later
          if (!deletes.containsKey(readcol)
              || deletes.get(readcol).longValue() < readkey.getTimestamp()) {
            deletes.put(readcol, Long.valueOf(readkey.getTimestamp()));
          }
        } else if (!(deletes.containsKey(readcol) && deletes.get(readcol)
            .longValue() >= readkey.getTimestamp())) {
          // So the cell itself isn't a delete, but there may be a delete
          // pending from earlier in our search. Only record this result if
          // there aren't any pending deletes.
          if (!(deletes.containsKey(readcol) && deletes.get(readcol)
              .longValue() >= readkey.getTimestamp())) {
            if (!isExpired(readkey, ttl, now)) {
              if (!results.containsKey(readcol)) {
                results.put(readcol, new Cell(value, readkey.getTimestamp()));
              } else {
                results.get(readcol).add(Bytes.toBytes(value),
                    readkey.getTimestamp());
              }
            }
          }
        }
      } else if (this.rawcomparator.compareRows(key.getRow(), 0,
            key.getRow().length,
          readkey.getRow(), 0, readkey.getRow().length) < 0) {
        // if we've crossed into the next row, then we can just stop
        // iterating
        break;
      }
    } while (scanner.next());
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
      Map<Long, StoreFile> m = this.storefiles.descendingMap();
      byte [] keyBytes = key.getBytes();
      for (Iterator<Map.Entry<Long, StoreFile>> i = m.entrySet().iterator();
          i.hasNext() && !hasEnoughVersions(versions, results);) {
        StoreFile f = i.next().getValue();
        HFileScanner scanner = f.getReader().getScanner();
        if (!getClosest(scanner, keyBytes)) {
          continue;
        }
        HStoreKey readkey = HStoreKey.create(scanner.getKey());
        if (!readkey.matchesRowCol(key)) {
          continue;
        }
        if (get(readkey, scanner.getValue(), versions, results, deletes, now)) {
          break;
        }
        while (scanner.next()) {
          readkey = HStoreKey.create(scanner.getKey());
          if (!readkey.matchesRowCol(key)) {
            break;
          }
          if (get(readkey, scanner.getValue(), versions, results, deletes, now)) {
            break;
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
  private boolean get(final HStoreKey key, ByteBuffer value,
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
    // changes in the flow below should also probably be done in get.
    // TODO: Refactor so same code used.
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
      Map<Long, StoreFile> m = this.storefiles.descendingMap();
      for (Iterator<Map.Entry<Long, StoreFile>> i = m.entrySet().iterator();
          i.hasNext() && keys.size() < versions;) {
        StoreFile f = i.next().getValue();
        HFileScanner scanner = f.getReader().getScanner();
        if (!getClosest(scanner, origin.getBytes())) {
          continue;
        }
        do {
          HStoreKey readkey = HStoreKey.create(scanner.getKey());
          // if the row and column matches, we might want this one.
          if (rowMatches(origin, readkey)) {
            // if the column pattern is not null, we use it for column matching.
            // we will skip the keys whose column doesn't match the pattern.
            if (columnPattern != null) {
              if (!(columnPattern.
                  matcher(Bytes.toString(readkey.getColumn())).matches())) {
                continue;
              }
            }
            // if the cell address matches, then we definitely want this key.
            if (cellMatches(origin, readkey)) {
              ByteBuffer readval = scanner.getValue();
              // Store key if isn't deleted or superceded by memcache
              if (!HLogEdit.isDeleted(readval)) {
                if (notExpiredAndNotInDeletes(this.ttl, readkey, now, deletes)) {
                  keys.add(readkey);
                }
                if (keys.size() >= versions) {
                  break;
                }
              } else {
                deletes.add(readkey);
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
        } while (scanner.next()); // advance to the next key
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
  byte [] getRowKeyAtOrBefore(final byte [] row)
  throws IOException{
    // Map of HStoreKeys that are candidates for holding the row key that
    // most closely matches what we're looking for. We'll have to update it as
    // deletes are found all over the place as we go along before finally
    // reading the best key out of it at the end.
    NavigableMap<HStoreKey, Long> candidateKeys =
      new TreeMap<HStoreKey, Long>(this.comparator);
    
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
      Map<Long, StoreFile> m = this.storefiles.descendingMap();
      for (Map.Entry<Long, StoreFile> e: m.entrySet()) {
        // Update the candidate keys from the current map file
        rowAtOrBeforeFromStoreFile(e.getValue(), row, candidateKeys, deletes);
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
   * @param f
   * @param row
   * @param candidateKeys
   * @throws IOException
   */
  private void rowAtOrBeforeFromStoreFile(final StoreFile f,
    final byte [] row, final SortedMap<HStoreKey, Long> candidateKeys,
    final Set<HStoreKey> deletes)
  throws IOException {
    HFileScanner scanner = f.getReader().getScanner();
    // TODO: FIX THIS PROFLIGACY!!!
    if (!scanner.seekBefore(new HStoreKey(row).getBytes())) {
      return;
    }
    long now = System.currentTimeMillis();
    HStoreKey startKey = HStoreKey.create(scanner.getKey());
    // if there aren't any candidate keys yet, we'll do some things different 
    if (candidateKeys.isEmpty()) {
      rowAtOrBeforeCandidate(startKey, f, row, candidateKeys, deletes, now);
    } else {
      rowAtOrBeforeWithCandidates(startKey, f, row, candidateKeys, deletes, now);
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
    final StoreFile f, final byte[] row,
    final SortedMap<HStoreKey, Long> candidateKeys,
    final Set<HStoreKey> deletes, final long now) 
  throws IOException {
    // if the row we're looking for is past the end of this mapfile, set the
    // search key to be the last key.  If its a deleted key, then we'll back
    // up to the row before and return that.
    HStoreKey finalKey = HStoreKey.create(f.getReader().getLastKey());
    HStoreKey searchKey = null;
    if (this.rawcomparator.compareRows(finalKey.getRow(), 0,
          finalKey.getRow().length,
        row, 0, row.length) < 0) {
      searchKey = finalKey;
    } else {
      searchKey = new HStoreKey(row);
      if (this.comparator.compare(searchKey, startKey) < 0) {
        searchKey = startKey;
      }
    }
    rowAtOrBeforeCandidate(f, searchKey, candidateKeys, deletes, now);
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
   * @param f
   * @param sk Key to go search the mapfile with.
   * @param candidateKeys
   * @param now
   * @throws IOException
   * @see {@link #rowAtOrBeforeCandidate(HStoreKey, org.apache.hadoop.io.MapFile.Reader, byte[], SortedMap, long)}
   */
  private void rowAtOrBeforeCandidate(final StoreFile f,
    final HStoreKey sk, final SortedMap<HStoreKey, Long> candidateKeys,
    final Set<HStoreKey> deletes, final long now)
  throws IOException {
    HStoreKey searchKey = sk;
    HStoreKey readkey = null;
    HStoreKey knownNoGoodKey = null;
    HFileScanner scanner = f.getReader().getScanner();
    for (boolean foundCandidate = false; !foundCandidate;) {
      // Seek to the exact row, or the one that would be immediately before it
      int result = scanner.seekTo(searchKey.getBytes());
      if (result < 0) {
        // Not in file.
        continue;
      }
      HStoreKey deletedOrExpiredRow = null;
      do {
        readkey = HStoreKey.create(scanner.getKey());
        ByteBuffer value = scanner.getValue();
        // If we have an exact match on row, and it's not a delete, save this
        // as a candidate key
        if (HStoreKey.equalsTwoRowKeys(readkey.getRow(), searchKey.getRow())) {
          if (!HLogEdit.isDeleted(value)) {
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
        } else if (this.rawcomparator.compareRows(readkey.getRow(), 0,
              readkey.getRow().length,
            searchKey.getRow(), 0, searchKey.getRow().length) > 0) {
          // if the row key we just read is beyond the key we're searching for,
          // then we're done.
          break;
        } else {
          // So, the row key doesn't match, but we haven't gone past the row
          // we're seeking yet, so this row is a candidate for closest
          // (assuming that it isn't a delete).
          if (!HLogEdit.isDeleted(value)) {
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
      } while(scanner.next() && (knownNoGoodKey == null ||
          this.comparator.compare(readkey, knownNoGoodKey) < 0));

      // If we get here and have no candidates but we did find a deleted or
      // expired candidate, we need to look at the key before that
      if (!foundCandidate && deletedOrExpiredRow != null) {
        knownNoGoodKey = deletedOrExpiredRow;
        if (!scanner.seekBefore(deletedOrExpiredRow.getBytes())) {
          // Is this right?
          break;
        }
        searchKey = HStoreKey.create(scanner.getKey());
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
    final StoreFile f, final byte[] row,
    final SortedMap<HStoreKey, Long> candidateKeys,
    final Set<HStoreKey> deletes, final long now) 
  throws IOException {
    // if there are already candidate keys, we need to start our search 
    // at the earliest possible key so that we can discover any possible
    // deletes for keys between the start and the search key.  Back up to start
    // of the row in case there are deletes for this candidate in this mapfile
    // BUT do not backup before the first key in the store file.
    // TODO: FIX THIS PROFLIGATE OBJECT MAKING!!!
    HStoreKey firstCandidateKey = candidateKeys.firstKey();
    byte [] searchKey = null;
    HStoreKey.StoreKeyComparator c =
      (HStoreKey.StoreKeyComparator)f.getReader().getComparator();
    if (c.compareRows(firstCandidateKey.getRow(), startKey.getRow()) < 0) {
      searchKey = startKey.getBytes();
    } else {
      searchKey = new HStoreKey(firstCandidateKey.getRow()).getBytes();
    }

    // Seek to the exact row, or the one that would be immediately before it
    HFileScanner scanner = f.getReader().getScanner();
    int result = scanner.seekTo(searchKey);
    if (result < 0) {
      // Key is before start of this file.  Return.
      return;
    }
    do {
      HStoreKey k = HStoreKey.create(scanner.getKey());
      ByteBuffer v = scanner.getValue();
      // if we have an exact match on row, and it's not a delete, save this
      // as a candidate key
      if (HStoreKey.equalsTwoRowKeys(k.getRow(), row)) {
        handleKey(k, v, now, deletes, candidateKeys);
      } else if (this.rawcomparator.compareRows(k.getRow(), row) > 0 ) {
        // if the row key we just read is beyond the key we're searching for,
        // then we're done.
        break;
      } else {
        // So, the row key doesn't match, but we haven't gone past the row
        // we're seeking yet, so this row is a candidate for closest 
        // (assuming that it isn't a delete).
        handleKey(k, v, now, deletes, candidateKeys);
      }
    } while(scanner.next());
  }

  /*
   * @param readkey
   * @param now
   * @param deletes
   * @param candidateKeys
   */
  private void handleKey(final HStoreKey readkey, ByteBuffer value,
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

  static HStoreKey stripTimestamp(HStoreKey key) {
    return new HStoreKey(key.getRow(), key.getColumn());
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
      if (HStoreKey.equalsTwoRowKeys(target.getRow(), origin.getRow())) {
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
  private boolean rowMatches(final HStoreKey origin, final HStoreKey target){
    // if the origin's column is empty, then we're matching any column
    if (Bytes.equals(origin.getColumn(), HConstants.EMPTY_BYTE_ARRAY)) {
      // if the row matches, then...
      return HStoreKey.equalsTwoRowKeys(target.getRow(), origin.getRow());
    }
    // otherwise, we want to match on row and column
    return target.matchesRowCol(origin);
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
        StoreFile curHSF = e.getValue();
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
        long size = curHSF.getReader().length();
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
        HStoreKey mk = HStoreKey.create(midkey);
        HStoreKey firstKey = HStoreKey.create(r.getFirstKey());
        HStoreKey lastKey = HStoreKey.create(r.getLastKey());
        // if the midkey is the same as the first and last keys, then we cannot
        // (ever) split this region. 
        if (HStoreKey.equalsTwoRowKeys(mk.getRow(), firstKey.getRow()) && 
            HStoreKey.equalsTwoRowKeys( mk.getRow(), lastKey.getRow())) {
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
   * Return a scanner for both the memcache and the HStore files
   */
  protected InternalScanner getScanner(long timestamp, byte [][] targetCols,
      byte [] firstRow, RowFilterInterface filter)
  throws IOException {
    lock.readLock().lock();
    try {
      return new StoreScanner(this, targetCols, firstRow, timestamp, filter);
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
   * @throws IOException if there was a problem getting file sizes from the
   * filesystem
   */
  long getStorefilesIndexSize() throws IOException {
    long size = 0;
    for (StoreFile s: storefiles.values())
      size += s.getReader().indexSize();
    return size;
  }

  /*
   * Datastructure that holds size and row to split a file around.
   */
  static class StoreSize {
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
    byte[] getSplitRow() {
      return key;
    }
  }

  HRegionInfo getHRegionInfo() {
    return this.regioninfo;
  }

  /**
   * Convenience method that implements the old MapFile.getClosest on top of
   * HFile Scanners.  getClosest used seek to the asked-for key or just after
   * (HFile seeks to the key or just before).
   * @param s
   * @param b
   * @return True if we were able to seek the scanner to <code>b</code> or to
   * the key just after.
   * @throws IOException 
   */
  static boolean getClosest(final HFileScanner s, final byte [] b)
  throws IOException {
    int result = s.seekTo(b);
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
}
