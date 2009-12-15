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
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.Progressable;

/**
 * HLog stores all the edits to the HStore.  Its the hbase write-ahead-log
 * implementation.
 *
 * It performs logfile-rolling, so external callers are not aware that the
 * underlying file is being rolled.
 *
 * <p>
 * There is one HLog per RegionServer.  All edits for all Regions carried by
 * a particular RegionServer are entered first in the HLog.
 *
 * <p>
 * Each HRegion is identified by a unique long <code>int</code>. HRegions do
 * not need to declare themselves before using the HLog; they simply include
 * their HRegion-id in the <code>append</code> or
 * <code>completeCacheFlush</code> calls.
 *
 * <p>
 * An HLog consists of multiple on-disk files, which have a chronological order.
 * As data is flushed to other (better) on-disk structures, the log becomes
 * obsolete. We can destroy all the log messages for a given HRegion-id up to
 * the most-recent CACHEFLUSH message from that HRegion.
 *
 * <p>
 * It's only practical to delete entire files. Thus, we delete an entire on-disk
 * file F when all of the messages in F have a log-sequence-id that's older
 * (smaller) than the most-recent CACHEFLUSH message for every HRegion that has
 * a message in F.
 *
 * <p>
 * Synchronized methods can never execute in parallel. However, between the
 * start of a cache flush and the completion point, appends are allowed but log
 * rolling is not. To prevent log rolling taking place during this period, a
 * separate reentrant lock is used.
 * 
 * <p>To read an HLog, call {@link #getReader(org.apache.hadoop.fs.FileSystem,
 * org.apache.hadoop.fs.Path, org.apache.hadoop.conf.Configuration)}.
 *
 */
public class HLog implements HConstants, Syncable {
  static final Log LOG = LogFactory.getLog(HLog.class);
  private static final String HLOG_DATFILE = "hlog.dat.";
  public static final byte [] METAFAMILY = Bytes.toBytes("METAFAMILY");
  static final byte [] METAROW = Bytes.toBytes("METAROW");
  private final FileSystem fs;
  private final Path dir;
  private final Configuration conf;
  private final LogRollListener listener;
  private final long optionalFlushInterval;
  private final long blocksize;
  private final int flushlogentries;
  private final AtomicInteger unflushedEntries = new AtomicInteger(0);
  private final short replicationLevel;

  // used to indirectly tell syncFs to force the sync
  private boolean forceSync = false;

  /*
   * Current log file.
   */
  SequenceFile.Writer writer;
  // This is the above writer's output stream. Its private but we use reflection
  // to expose it so we can call sync on it.
  FSDataOutputStream writer_out;

  /*
   * Map of all log files but the current one. 
   */
  final SortedMap<Long, Path> outputfiles =
    Collections.synchronizedSortedMap(new TreeMap<Long, Path>());

  /*
   * Map of region to last sequence/edit id. 
   */
  private final ConcurrentSkipListMap<byte [], Long> lastSeqWritten =
    new ConcurrentSkipListMap<byte [], Long>(Bytes.BYTES_COMPARATOR);

  private volatile boolean closed = false;

  private final AtomicLong logSeqNum = new AtomicLong(0);

  private volatile long filenum = -1;
  
  private final AtomicInteger numEntries = new AtomicInteger(0);

  // Size of edits written so far. Used figuring when to rotate logs.
  private final AtomicLong editsSize = new AtomicLong(0);

  // If > than this size, roll the log.
  private final long logrollsize;

  // This lock prevents starting a log roll during a cache flush.
  // synchronized is insufficient because a cache flush spans two method calls.
  private final Lock cacheFlushLock = new ReentrantLock();

  // We synchronize on updateLock to prevent updates and to prevent a log roll
  // during an update
  private final Object updateLock = new Object();

  private final boolean enabled;

  /*
   * If more than this many logs, force flush of oldest region to oldest edit
   * goes to disk.  If too many and we crash, then will take forever replaying.
   * Keep the number of logs tidy.
   */
  private final int maxLogs;

  /**
   * Thread that handles group commit
   */
  private final LogSyncer logSyncerThread;

  static byte [] COMPLETE_CACHE_FLUSH;
  static {
    try {
      COMPLETE_CACHE_FLUSH = "HBASE::CACHEFLUSH".getBytes(UTF8_ENCODING);
    } catch (UnsupportedEncodingException e) {
      assert(false);
    }
  }

  /**
   * Create an edit log at the given <code>dir</code> location.
   *
   * You should never have to load an existing log. If there is a log at
   * startup, it should have already been processed and deleted by the time the
   * HLog object is started up.
   *
   * @param fs
   * @param dir
   * @param conf
   * @param listener
   * @throws IOException
   */
  public HLog(final FileSystem fs, final Path dir, final HBaseConfiguration conf,
    final LogRollListener listener)
  throws IOException {
    super();
    this.fs = fs;
    this.dir = dir;
    this.conf = conf;
    this.listener = listener;
    this.flushlogentries =
      conf.getInt("hbase.regionserver.flushlogentries", 1);
    this.blocksize = conf.getLong("hbase.regionserver.hlog.blocksize",
      this.fs.getDefaultBlockSize());
    this.replicationLevel = (short) conf.getInt("hbase.regionserver.hlog.replication",
        this.fs.getDefaultReplication());
    // Roll at 95% of block size.
    float multi = conf.getFloat("hbase.regionserver.logroll.multiplier", 0.95f);
    this.logrollsize = (long)(this.blocksize * multi);
    this.optionalFlushInterval =
      conf.getLong("hbase.regionserver.optionallogflushinterval", 1 * 1000);
    if (fs.exists(dir)) {
      throw new IOException("Target HLog directory already exists: " + dir);
    }
    fs.mkdirs(dir);
    this.maxLogs = conf.getInt("hbase.regionserver.maxlogs", 32);
    this.enabled = conf.getBoolean("hbase.regionserver.hlog.enabled", true);
    LOG.info("HLog configuration: blocksize=" + this.blocksize +
      ", rollsize=" + this.logrollsize +
      ", enabled=" + this.enabled +
      ", flushlogentries=" + this.flushlogentries +
      ", optionallogflushinternal=" + this.optionalFlushInterval + "ms");
    rollWriter();
    logSyncerThread = new LogSyncer(this.optionalFlushInterval);
    Threads.setDaemonThreadRunning(logSyncerThread,
        Thread.currentThread().getName() + ".logSyncer");
  }

  /**
   * @return Current state of the monotonically increasing file id.
   */
  public long getFilenum() {
    return this.filenum;
  }

  /**
   * Get the compression type for the hlog files
   * @param c Configuration to use.
   * @return the kind of compression to use
   */
  static CompressionType getCompressionType(final Configuration c) {
    // Compression makes no sense for commit log.  Always return NONE.
    return CompressionType.NONE;
  }

  /**
   * Called by HRegionServer when it opens a new region to ensure that log
   * sequence numbers are always greater than the latest sequence number of the
   * region being brought on-line.
   *
   * @param newvalue We'll set log edit/sequence number to this value if it
   * is greater than the current value.
   */
  public void setSequenceNumber(final long newvalue) {
    for (long id = this.logSeqNum.get(); id < newvalue &&
        !this.logSeqNum.compareAndSet(id, newvalue); id = this.logSeqNum.get()) {
      // This could spin on occasion but better the occasional spin than locking
      // every increment of sequence number.
      LOG.debug("Change sequence number from " + logSeqNum + " to " + newvalue);
    }
  }
  
  /**
   * @return log sequence number
   */
  public long getSequenceNumber() {
    return logSeqNum.get();
  }

  /**
   * Roll the log writer. That is, start writing log messages to a new file.
   *
   * Because a log cannot be rolled during a cache flush, and a cache flush
   * spans two method calls, a special lock needs to be obtained so that a cache
   * flush cannot start when the log is being rolled and the log cannot be
   * rolled during a cache flush.
   *
   * <p>Note that this method cannot be synchronized because it is possible that
   * startCacheFlush runs, obtaining the cacheFlushLock, then this method could
   * start which would obtain the lock on this but block on obtaining the
   * cacheFlushLock and then completeCacheFlush could be called which would wait
   * for the lock on this and consequently never release the cacheFlushLock
   *
   * @return If lots of logs, flush the returned region so next time through
   * we can clean logs. Returns null if nothing to flush.
   * @throws org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException
   * @throws IOException
   */
  public byte [] rollWriter() throws FailedLogCloseException, IOException {
    // Return if nothing to flush.
    if (this.writer != null && this.numEntries.get() <= 0) {
      return null;
    }
    byte [] regionToFlush = null;
    this.cacheFlushLock.lock();
    try {
      if (closed) {
        return regionToFlush;
      }
      synchronized (updateLock) {
        // Clean up current writer.
        Path oldFile = cleanupCurrentWriter(this.filenum);
        this.filenum = System.currentTimeMillis();
        Path newPath = computeFilename(this.filenum);
        this.writer = createWriter(newPath);
        LOG.info((oldFile != null?
            "Roll " + FSUtils.getPath(oldFile) + ", entries=" +
            this.numEntries.get() +
            ", calcsize=" + this.editsSize.get() + ", filesize=" +
            this.fs.getFileStatus(oldFile).getLen() + ". ": "") +
          "New hlog " + FSUtils.getPath(newPath));
        // Can we delete any of the old log files?
        if (this.outputfiles.size() > 0) {
          if (this.lastSeqWritten.size() <= 0) {
            LOG.debug("Last sequence written is empty. Deleting all old hlogs");
            // If so, then no new writes have come in since all regions were
            // flushed (and removed from the lastSeqWritten map). Means can
            // remove all but currently open log file.
            for (Map.Entry<Long, Path> e : this.outputfiles.entrySet()) {
              deleteLogFile(e.getValue(), e.getKey());
            }
            this.outputfiles.clear();
          } else {
            regionToFlush = cleanOldLogs();
          }
        }
        this.numEntries.set(0);
        this.editsSize.set(0);
      }
    } finally {
      this.cacheFlushLock.unlock();
    }
    return regionToFlush;
  }

  protected SequenceFile.Writer createWriter(Path path) throws IOException {
    return createWriter(path, HLogKey.class, KeyValue.class);
  }
  
  /**
   * Hack just to set the correct file length up in SequenceFile.Reader.
   * See HADOOP-6307.  The below is all about setting the right length on the
   * file we are reading.  fs.getFileStatus(file).getLen() is passed down to
   * a private SequenceFile.Reader constructor.  This won't work.  Need to do
   * the available on the stream.  The below is ugly.  It makes getPos, the
   * first time its called, return length of the file -- i.e. tell a lie -- just
   * so this line up in SF.Reader's constructor ends up with right answer:
   * 
   *         this.end = in.getPos() + length;
   */
  private static class WALReader extends SequenceFile.Reader {
    
    WALReader(final FileSystem fs, final Path p, final Configuration c)
    throws IOException {
      super(fs, p, c);
      
    }

    @Override
    protected FSDataInputStream openFile(FileSystem fs, Path file,
      int bufferSize, long length)
    throws IOException {
      return new WALReaderFSDataInputStream(super.openFile(fs, file, bufferSize,
        length), length);
    }

    /**
     * Override just so can intercept first call to getPos.
     */
    static class WALReaderFSDataInputStream extends FSDataInputStream {
      private boolean firstGetPosInvocation = true;
      private long length;

      WALReaderFSDataInputStream(final FSDataInputStream is, final long l)
      throws IOException {
        super(is);
        this.length = l;
      }

      @Override
      public long getPos() throws IOException {
        if (this.firstGetPosInvocation) {
          this.firstGetPosInvocation = false;
          // Tell a lie.  We're doing this just so that this line up in
          // SequenceFile.Reader constructor comes out with the correct length
          // on the file:
          //         this.end = in.getPos() + length;
          // 
          long available = this.in.available();
          // Length gets added up in the SF.Reader constructor so subtract the
          // difference.  If available < this.length, then return this.length.
          // I ain't sure what else to do.
          return available >= this.length? available - this.length: this.length;
        }
        return super.getPos();
      }
    }
  }

  /**
   * Get a Reader for WAL.
   * Reader is a subclass of SequenceFile.Reader.  The subclass has amendments
   * to make it so we see edits up to the last sync (HDFS-265).  Of note, we
   * can only see up to the sync that happened before this file was opened.
   * Will require us doing up our own WAL Reader if we want to keep up with
   * a syncing Writer.
   * @param p
   * @return A WAL Reader.  Close when done with it.
   * @throws IOException
   */
  public static SequenceFile.Reader getReader(final FileSystem fs,
    final Path p, final Configuration c)
  throws IOException {
    return new WALReader(fs, p, c);
  }

  protected SequenceFile.Writer createWriter(Path path,
    Class<? extends HLogKey> keyClass, Class<? extends KeyValue> valueClass)
  throws IOException {
    SequenceFile.Writer writer =
      SequenceFile.createWriter(this.fs, this.conf, path, keyClass,
      valueClass, fs.getConf().getInt("io.file.buffer.size", 4096),
      this.replicationLevel, this.blocksize,
      SequenceFile.CompressionType.NONE, new DefaultCodec(), null,
      new Metadata());
    // Get at the private FSDataOutputStream inside in SequenceFile so we can
    // call sync on it.  Make it accessible.  Stash it aside for call up in
    // the sync method above.
    final Field fields[] = writer.getClass().getDeclaredFields();
    final String fieldName = "out";
    for (int i = 0; i < fields.length; ++i) {
      if (fieldName.equals(fields[i].getName())) {
        try {
          fields[i].setAccessible(true);
          this.writer_out = (FSDataOutputStream)fields[i].get(writer);
          break;
        } catch (IllegalAccessException ex) {
          throw new IOException("Accessing " + fieldName, ex);
        }
      }
    }
    return writer;
  }
  
  /*
   * Clean up old commit logs.
   * @return If lots of logs, flush the returned region so next time through
   * we can clean logs. Returns null if nothing to flush.
   * @throws IOException
   */
  private byte [] cleanOldLogs() throws IOException {
    byte [] regionToFlush = null;
    Long oldestOutstandingSeqNum = getOldestOutstandingSeqNum();
    // Get the set of all log files whose final ID is older than or
    // equal to the oldest pending region operation
    TreeSet<Long> sequenceNumbers =
      new TreeSet<Long>(this.outputfiles.headMap(
        (Long.valueOf(oldestOutstandingSeqNum.longValue() + 1L))).keySet());
    // Now remove old log files (if any)
    byte [] oldestRegion = null;
    if (LOG.isDebugEnabled()) {
      // Find region associated with oldest key -- helps debugging.
      oldestRegion = getOldestRegion(oldestOutstandingSeqNum);
      LOG.debug("Found " + sequenceNumbers.size() + " hlogs to remove " +
        " out of total " + this.outputfiles.size() + "; " +
        "oldest outstanding seqnum is " + oldestOutstandingSeqNum +
        " from region " + Bytes.toString(oldestRegion));
    }
    if (sequenceNumbers.size() > 0) {
      for (Long seq : sequenceNumbers) {
        deleteLogFile(this.outputfiles.remove(seq), seq);
      }
    }
    int countOfLogs = this.outputfiles.size() - sequenceNumbers.size();
    if (countOfLogs > this.maxLogs) {
      regionToFlush = oldestRegion != null?
        oldestRegion: getOldestRegion(oldestOutstandingSeqNum);
      LOG.info("Too many hlogs: logs=" + countOfLogs + ", maxlogs=" +
        this.maxLogs + "; forcing flush of region with oldest edits: " +
        Bytes.toString(regionToFlush));
    }
    return regionToFlush;
  }

  /*
   * @return Logs older than this id are safe to remove.
   */
  private Long getOldestOutstandingSeqNum() {
    return Collections.min(this.lastSeqWritten.values());
  }

  private byte [] getOldestRegion(final Long oldestOutstandingSeqNum) {
    byte [] oldestRegion = null;
    for (Map.Entry<byte [], Long> e: this.lastSeqWritten.entrySet()) {
      if (e.getValue().longValue() == oldestOutstandingSeqNum.longValue()) {
        oldestRegion = e.getKey();
        break;
      }
    }
    return oldestRegion;
  }

  /*
   * Cleans up current writer closing and adding to outputfiles.
   * Presumes we're operating inside an updateLock scope.
   * @return Path to current writer or null if none.
   * @throws IOException
   */
  private Path cleanupCurrentWriter(final long currentfilenum)
  throws IOException {
    Path oldFile = null;
    if (this.writer != null) {
      // Close the current writer, get a new one.
      try {
        this.writer.close();
      } catch (IOException e) {
        // Failed close of log file.  Means we're losing edits.  For now,
        // shut ourselves down to minimize loss.  Alternative is to try and
        // keep going.  See HBASE-930.
        FailedLogCloseException flce =
          new FailedLogCloseException("#" + currentfilenum);
        flce.initCause(e);
        throw e; 
      }
      if (currentfilenum >= 0) {
        oldFile = computeFilename(currentfilenum);
        this.outputfiles.put(Long.valueOf(this.logSeqNum.get() - 1), oldFile);
      }
    }
    return oldFile;
  }

  private void deleteLogFile(final Path p, final Long seqno) throws IOException {
    LOG.info("removing old hlog file " + FSUtils.getPath(p) +
      " whose highest sequence/edit id is " + seqno);
    this.fs.delete(p, true);
  }

  /**
   * This is a convenience method that computes a new filename with a given
   * file-number.
   * @param fn
   * @return Path
   */
  public Path computeFilename(final long fn) {
    if (fn < 0) return null;
    return new Path(dir, HLOG_DATFILE + fn);
  }

  /**
   * Shut down the log and delete the log directory
   *
   * @throws IOException
   */
  public void closeAndDelete() throws IOException {
    close();
    fs.delete(dir, true);
  }

  /**
   * Shut down the log.
   *
   * @throws IOException
   */
  public void close() throws IOException {
    try {
      logSyncerThread.interrupt();
      // Make sure we synced everything
      logSyncerThread.join();
    } catch (InterruptedException e) {
      LOG.error("Exception while waiting for syncer thread to die", e);
    }

    cacheFlushLock.lock();
    try {
      synchronized (updateLock) {
        this.closed = true;
        if (LOG.isDebugEnabled()) {
          LOG.debug("closing hlog writer in " + this.dir.toString());
        }
        this.writer.close();
      }
    } finally {
      cacheFlushLock.unlock();
    }
  }

   /** Append an entry to the log.
   * 
   * @param regionInfo
   * @param logEdit
   * @param now Time of this edit write.
   * @throws IOException
   */
  public void append(HRegionInfo regionInfo, KeyValue logEdit,
    final long now)
  throws IOException {
    byte [] regionName = regionInfo.getRegionName();
    byte [] tableName = regionInfo.getTableDesc().getName();
    this.append(regionInfo, makeKey(regionName, tableName, -1, now), logEdit);
  }

  /**
   * @param now
   * @param regionName
   * @param tableName
   * @return New log key.
   */
  protected HLogKey makeKey(byte[] regionName, byte[] tableName, long seqnum, long now) {
    return new HLogKey(regionName, tableName, seqnum, now);
  }
  
  
  
  /** Append an entry to the log.
   * 
   * @param regionInfo
   * @param logEdit
   * @param logKey
   * @throws IOException
   */
  public void append(HRegionInfo regionInfo, HLogKey logKey, KeyValue logEdit)
  throws IOException {
    if (this.closed) {
      throw new IOException("Cannot append; log is closed");
    }
    byte [] regionName = regionInfo.getRegionName();
    synchronized (updateLock) {
      long seqNum = obtainSeqNum();
      logKey.setLogSeqNum(seqNum);
      // The 'lastSeqWritten' map holds the sequence number of the oldest
      // write for each region. When the cache is flushed, the entry for the
      // region being flushed is removed if the sequence number of the flush
      // is greater than or equal to the value in lastSeqWritten.
      this.lastSeqWritten.putIfAbsent(regionName, Long.valueOf(seqNum));
      doWrite(logKey, logEdit, logKey.getWriteTime());
      this.unflushedEntries.incrementAndGet();
      this.numEntries.incrementAndGet();
    }
    if (this.editsSize.get() > this.logrollsize) {
      if (listener != null) {
        listener.logRollRequested();
      }
    }
  }

  /**
   * Append a set of edits to the log. Log edits are keyed by regionName,
   * rowname, and log-sequence-id.
   *
   * Later, if we sort by these keys, we obtain all the relevant edits for a
   * given key-range of the HRegion (TODO). Any edits that do not have a
   * matching COMPLETE_CACHEFLUSH message can be discarded.
   *
   * <p>
   * Logs cannot be restarted once closed, or once the HLog process dies. Each
   * time the HLog starts, it must create a new log. This means that other
   * systems should process the log appropriately upon each startup (and prior
   * to initializing HLog).
   *
   * synchronized prevents appends during the completion of a cache flush or for
   * the duration of a log roll.
   *
   * @param regionName
   * @param tableName
   * @param edits
   * @param now
   * @throws IOException
   */
  public void append(byte [] regionName, byte [] tableName, List<KeyValue> edits,
    final long now)
  throws IOException {
    if (this.closed) {
      throw new IOException("Cannot append; log is closed");
    }
    long seqNum [] = obtainSeqNum(edits.size());
    synchronized (this.updateLock) {
      // The 'lastSeqWritten' map holds the sequence number of the oldest
      // write for each region. When the cache is flushed, the entry for the
      // region being flushed is removed if the sequence number of the flush
      // is greater than or equal to the value in lastSeqWritten.
      this.lastSeqWritten.putIfAbsent(regionName, Long.valueOf(seqNum[0]));
      int counter = 0;
      for (KeyValue kv: edits) {
        HLogKey logKey = makeKey(regionName, tableName, seqNum[counter++], now);
        doWrite(logKey, kv, now);
        this.numEntries.incrementAndGet();
      }

      // Only count 1 row as an unflushed entry.
      this.unflushedEntries.incrementAndGet();
    }
    if (this.editsSize.get() > this.logrollsize) {
        requestLogRoll();
    }
  }

  /**
   * This thread is responsible to call syncFs and buffer up the writers while
   * it happens.
   */
   class LogSyncer extends Thread {

    // Using fairness to make sure locks are given in order
    private final ReentrantLock lock = new ReentrantLock(true);

    // Condition used to wait until we have something to sync
    private final Condition queueEmpty = lock.newCondition();

    // Condition used to signal that the sync is done
    private final Condition syncDone = lock.newCondition();

    private final long optionalFlushInterval;

    LogSyncer(long optionalFlushInterval) {
      this.optionalFlushInterval = optionalFlushInterval;
    }

    public void run() {
      try {
        lock.lock();
        // awaiting with a timeout doesn't always
        // throw exceptions on interrupt
        while(!this.isInterrupted()) {

          // Wait until something has to be hflushed or do it if we waited
          // enough time (useful if something appends but does not hflush).
          // 0 or less means that it timed out and maybe waited a bit more.
          if (!(queueEmpty.awaitNanos(
              this.optionalFlushInterval*1000000) <= 0)) {
            forceSync = true;
          }

          // We got the signal, let's hflush. We currently own the lock so new
          // writes are waiting to acquire it in addToSyncQueue while the ones
          // we hflush are waiting on await()
          hflush();

          // Release all the clients waiting on the hflush. Notice that we still
          // own the lock until we get back to await at which point all the
          // other threads waiting will first acquire and release locks
          syncDone.signalAll();
        }
      } catch (IOException e) {
        LOG.error("Error while syncing, requesting close of hlog ", e);
        requestLogRoll();
      } catch (InterruptedException e) {
        LOG.debug(getName() + "interrupted while waiting for sync requests");
      } finally {
        syncDone.signalAll();
        lock.unlock();
        LOG.info(getName() + " exiting");
      }
    }

    /**
     * This method first signals the thread that there's a sync needed
     * and then waits for it to happen before returning.
     */
    public void addToSyncQueue(boolean force) {

      // Don't bother if somehow our append was already hflushed
      if (unflushedEntries.get() == 0) {
        return;
      }
      lock.lock();
      try {
        if(force) {
          forceSync = true;
        }
        // Wake the thread
        queueEmpty.signal();

        // Wait for it to hflush
        syncDone.await();
      } catch (InterruptedException e) {
        LOG.debug(getName() + " was interrupted while waiting for sync", e);
      }
      finally {
        lock.unlock();
      }
    }
  }

  public void sync(){
    sync(false);
  }

  /**
   * This method calls the LogSyncer in order to group commit the sync
   * with other threads.
   * @param force For catalog regions, force the sync to happen
   */
  public void sync(boolean force) {
    logSyncerThread.addToSyncQueue(force);
  }

  public void hflush() throws IOException {
    synchronized (this.updateLock) {
      if (this.closed) {
        return;
      }
      if (this.forceSync ||
          this.unflushedEntries.get() >= this.flushlogentries) {
        try {
          this.writer.sync();
          if (this.writer_out != null) {
            this.writer_out.sync();
          }
          this.forceSync = false;
          this.unflushedEntries.set(0);
        } catch (IOException e) {
          LOG.fatal("Could not append. Requesting close of hlog", e);
          requestLogRoll();
          throw e;
        }
      }
    }
  }

  public void hsync() throws IOException {
    // Not yet implemented up in hdfs so just call hflush.
    hflush();
  }

  private void requestLogRoll() {
    if (this.listener != null) {
      this.listener.logRollRequested();
    }
  }
  
  private void doWrite(HLogKey logKey, KeyValue logEdit, final long now)
  throws IOException {
    if (!this.enabled) {
      return;
    }
    try {
      this.editsSize.addAndGet(logKey.heapSize() + logEdit.heapSize());
      if (this.numEntries.get() % this.flushlogentries == 0) LOG.info("edit=" + this.numEntries.get() + ", write=" + logKey.toString());
      this.writer.append(logKey, logEdit);
      long took = System.currentTimeMillis() - now;
      if (took > 1000) {
        LOG.warn(Thread.currentThread().getName() + " took " + took +
          "ms appending an edit to hlog; editcount=" + this.numEntries.get());
      }
    } catch (IOException e) {
      LOG.fatal("Could not append. Requesting close of hlog", e);
      requestLogRoll();
      throw e;
    }
  }

  /** @return How many items have been added to the log */
  int getNumEntries() {
    return numEntries.get();
  }

  /**
   * Obtain a log sequence number.
   */
  private long obtainSeqNum() {
    return this.logSeqNum.incrementAndGet();
  }

  /** @return the number of log files in use */
  int getNumLogFiles() {
    return outputfiles.size();
  }

  /*
   * Obtain a specified number of sequence numbers
   *
   * @param num number of sequence numbers to obtain
   * @return array of sequence numbers
   */
  private long [] obtainSeqNum(int num) {
    long [] results = new long[num];
    for (int i = 0; i < num; i++) {
      results[i] = this.logSeqNum.incrementAndGet();
    }
    return results;
  }

  /**
   * By acquiring a log sequence ID, we can allow log messages to continue while
   * we flush the cache.
   *
   * Acquire a lock so that we do not roll the log between the start and
   * completion of a cache-flush. Otherwise the log-seq-id for the flush will
   * not appear in the correct logfile.
   *
   * @return sequence ID to pass {@link #completeCacheFlush(byte[], byte[], long)}
   * @see #completeCacheFlush(byte[], byte[], long)
   * @see #abortCacheFlush()
   */
  public long startCacheFlush() {
    this.cacheFlushLock.lock();
    return obtainSeqNum();
  }

  /**
   * Complete the cache flush
   *
   * Protected by cacheFlushLock
   *
   * @param regionName
   * @param tableName
   * @param logSeqId
   * @throws IOException
   */
  public void completeCacheFlush(final byte [] regionName, final byte [] tableName,
    final long logSeqId)
  throws IOException {
    try {
      if (this.closed) {
        return;
      }
      synchronized (updateLock) {
        this.writer.append(makeKey(regionName, tableName, logSeqId, System.currentTimeMillis()), 
            completeCacheFlushLogEdit());
        this.numEntries.incrementAndGet();
        Long seq = this.lastSeqWritten.get(regionName);
        if (seq != null && logSeqId >= seq.longValue()) {
          this.lastSeqWritten.remove(regionName);
        }
      }
    } finally {
      this.cacheFlushLock.unlock();
    }
  }

  private KeyValue completeCacheFlushLogEdit() {
    return new KeyValue(METAROW, METAFAMILY, null,
      System.currentTimeMillis(), COMPLETE_CACHE_FLUSH);
  }

  /**
   * Abort a cache flush.
   * Call if the flush fails. Note that the only recovery for an aborted flush
   * currently is a restart of the regionserver so the snapshot content dropped
   * by the failure gets restored to the memstore.
   */
  public void abortCacheFlush() {
    this.cacheFlushLock.unlock();
  }

  /**
   * @param family
   * @return true if the column is a meta column
   */
  public static boolean isMetaFamily(byte [] family) {
    return Bytes.equals(METAFAMILY, family);
  }
  
  /**
   * Split up a bunch of regionserver commit log files that are no longer
   * being written to, into new files, one per region for region to replay on
   * startup. Delete the old log files when finished.
   *
   * @param rootDir qualified root directory of the HBase instance
   * @param srcDir Directory of log files to split: e.g.
   *                <code>${ROOTDIR}/log_HOST_PORT</code>
   * @param fs FileSystem
   * @param conf HBaseConfiguration
   * @throws IOException
   */
  public static List<Path> splitLog(final Path rootDir, final Path srcDir,
      final FileSystem fs, final HBaseConfiguration conf)
  throws IOException {
    long millis = System.currentTimeMillis();
    List<Path> splits = null;
    if (!fs.exists(srcDir)) {
      // Nothing to do
      return splits;
    }
    FileStatus [] logfiles = fs.listStatus(srcDir);
    if (logfiles == null || logfiles.length == 0) {
      // Nothing to do
      return splits;
    }
    LOG.info("Splitting " + logfiles.length + " hlog(s) in " +
      srcDir.toString());
    splits = splitLog(rootDir, logfiles, fs, conf);
    try {
      fs.delete(srcDir, true);
    } catch (IOException e) {
      e = RemoteExceptionHandler.checkIOException(e);
      IOException io = new IOException("Cannot delete: " + srcDir);
      io.initCause(e);
      throw io;
    }
    long endMillis = System.currentTimeMillis();
    LOG.info("hlog file splitting completed in " + (endMillis - millis) +
        " millis for " + srcDir.toString());
    return splits;
  }

  // Private immutable datastructure to hold Writer and its Path.
  private final static class WriterAndPath {
    final Path p;
    final SequenceFile.Writer w;
    WriterAndPath(final Path p, final SequenceFile.Writer w) {
      this.p = p;
      this.w = w;
    }
  }
  
  @SuppressWarnings("unchecked")
  static Class<? extends HLogKey> getKeyClass(HBaseConfiguration conf) {
     return (Class<? extends HLogKey>) 
       conf.getClass("hbase.regionserver.hlog.keyclass", HLogKey.class);
  }
  
  public static HLogKey newKey(HBaseConfiguration conf) throws IOException {
    Class<? extends HLogKey> keyClass = getKeyClass(conf);
    try {
      return keyClass.newInstance();
    } catch (InstantiationException e) {
      throw new IOException("cannot create hlog key");
    } catch (IllegalAccessException e) {
      throw new IOException("cannot create hlog key");
    }
  }

  /*
   * @param rootDir
   * @param logfiles
   * @param fs
   * @param conf
   * @throws IOException
   * @return List of splits made.
   */
  private static List<Path> splitLog(final Path rootDir,
    final FileStatus [] logfiles, final FileSystem fs,
    final HBaseConfiguration conf)
  throws IOException {
    final Map<byte [], WriterAndPath> logWriters =
      new TreeMap<byte [], WriterAndPath>(Bytes.BYTES_COMPARATOR);
    List<Path> splits = null;
    
    // Number of threads to use when log splitting to rewrite the logs.
    // More means faster but bigger mem consumption.
    int logWriterThreads =
      conf.getInt("hbase.regionserver.hlog.splitlog.writer.threads", 3);
    
    // Number of logs to read concurrently when log splitting.
    // More means faster but bigger mem consumption  */
    int concurrentLogReads =
      conf.getInt("hbase.regionserver.hlog.splitlog.reader.threads", 3);
    // Is append supported?
    try {
      int maxSteps = Double.valueOf(Math.ceil((logfiles.length * 1.0) / 
          concurrentLogReads)).intValue();
      for (int step = 0; step < maxSteps; step++) {
        final Map<byte[], LinkedList<HLogEntry>> logEntries = 
          new TreeMap<byte[], LinkedList<HLogEntry>>(Bytes.BYTES_COMPARATOR);
        // Stop at logfiles.length when it's the last step
        int endIndex = step == maxSteps - 1? logfiles.length: 
          step * concurrentLogReads + concurrentLogReads;
        for (int i = (step * concurrentLogReads); i < endIndex; i++) {
          // Check for possibly empty file. With appends, currently Hadoop 
          // reports a zero length even if the file has been sync'd. Revisit if
          // HADOOP-4751 is committed.
          long length = logfiles[i].getLen();
          if (LOG.isDebugEnabled()) {
            LOG.debug("Splitting hlog " + (i + 1) + " of " + logfiles.length +
              ": " + logfiles[i].getPath() + ", length=" + logfiles[i].getLen());
          }
          SequenceFile.Reader in = null;
          int count = 0;
          try {
            in = HLog.getReader(fs, logfiles[i].getPath(), conf);
            try {
              HLogKey key = newKey(conf);
              KeyValue val = new KeyValue();
              while (in.next(key, val)) {
                byte [] regionName = key.getRegionName();
                LinkedList<HLogEntry> queue = logEntries.get(regionName);
                if (queue == null) {
                  queue = new LinkedList<HLogEntry>();
                  LOG.debug("Adding queue for " + Bytes.toString(regionName));
                  logEntries.put(regionName, queue);
                }
                HLogEntry hle = new HLogEntry(val, key);
                queue.push(hle);
                count++;
                // Make the key and value new each time; otherwise same instance
                // is used over and over.
                key = newKey(conf);
                val = new KeyValue();
              }
              LOG.debug("Pushed=" + count + " entries from " +
                logfiles[i].getPath());
            } catch (IOException e) {
              LOG.debug("IOE Pushed=" + count + " entries from " +
                logfiles[i].getPath());
              e = RemoteExceptionHandler.checkIOException(e);
              if (!(e instanceof EOFException)) {
                LOG.warn("Exception processing " + logfiles[i].getPath() +
                    " -- continuing. Possible DATA LOSS!", e);
              }
            }
          } catch (IOException e) {
            if (length <= 0) {
              LOG.warn("Empty hlog, continuing: " + logfiles[i] + " count=" + count, e);
              continue;
            }
            throw e;
          } finally {
            try {
              if (in != null) {
                in.close();
              }
            } catch (IOException e) {
              LOG.warn("Close in finally threw exception -- continuing", e);
            }
            // Delete the input file now so we do not replay edits. We could
            // have gotten here because of an exception. If so, probably
            // nothing we can do about it. Replaying it, it could work but we
            // could be stuck replaying for ever. Just continue though we
            // could have lost some edits.
            fs.delete(logfiles[i].getPath(), true);
          }
        }
        ExecutorService threadPool =
          Executors.newFixedThreadPool(logWriterThreads);
        for (final byte[] key : logEntries.keySet()) {
          Thread thread = new Thread(Bytes.toString(key)) {
            @Override
            public void run() {
              LinkedList<HLogEntry> entries = logEntries.get(key);
              LOG.debug("Thread got " + entries.size() + " to process");
              long threadTime = System.currentTimeMillis();
              try {
                int count = 0;
                // Items were added to the linkedlist oldest first. Pull them
                // out in that order.
                for (ListIterator<HLogEntry> i =
                  entries.listIterator(entries.size());
                    i.hasPrevious();) {
                  HLogEntry logEntry = i.previous();
                  WriterAndPath wap = logWriters.get(key);
                  if (wap == null) {
                    Path logfile = new Path(HRegion.getRegionDir(HTableDescriptor
                        .getTableDir(rootDir, logEntry.getKey().getTablename()),
                        HRegionInfo.encodeRegionName(key)),
                        HREGION_OLDLOGFILE_NAME);
                    Path oldlogfile = null;
                    SequenceFile.Reader old = null;
                    if (fs.exists(logfile)) {
                      FileStatus stat = fs.getFileStatus(logfile);
                      if (stat.getLen() <= 0) {
                        LOG.warn("Old hlog file " + logfile + " is zero " +
                          "length. Deleting existing file");
                        fs.delete(logfile, false);
                      } else {
                        LOG.warn("Old hlog file " + logfile + " already " +
                          "exists. Copying existing file to new file");
                        oldlogfile = new Path(logfile.toString() + ".old");
                        fs.rename(logfile, oldlogfile);
                        old = new SequenceFile.Reader(fs, oldlogfile, conf);
                      }
                    }
                    SequenceFile.Writer w =
                      SequenceFile.createWriter(fs, conf, logfile,
                        getKeyClass(conf), KeyValue.class, getCompressionType(conf));
                    wap = new WriterAndPath(logfile, w);
                    logWriters.put(key, wap);
                    if (LOG.isDebugEnabled()) {
                      LOG.debug("Creating new hlog file writer for path "
                          + logfile + " and region " + Bytes.toString(key));
                    }

                    if (old != null) {
                      // Copy from existing log file
                      HLogKey oldkey = newKey(conf);
                      KeyValue oldval = new KeyValue();
                      for (; old.next(oldkey, oldval); count++) {
                        if (LOG.isDebugEnabled() && count > 0
                            && count % 10000 == 0) {
                          LOG.debug("Copied " + count + " edits");
                        }
                        w.append(oldkey, oldval);
                      }
                      old.close();
                      fs.delete(oldlogfile, true);
                    }
                  }
                  wap.w.append(logEntry.getKey(), logEntry.getEdit());
                  count++;
                }
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Applied " + count + " total edits to "
                      + Bytes.toString(key) + " in "
                      + (System.currentTimeMillis() - threadTime) + "ms");
                }
              } catch (IOException e) {
                e = RemoteExceptionHandler.checkIOException(e);
                LOG.warn("Got while writing region " + Bytes.toString(key)
                    + " log " + e);
                e.printStackTrace();
              }
            }
          };
          threadPool.execute(thread);
        }
        threadPool.shutdown();
        // Wait for all threads to terminate
        try {
          for(int i = 0; !threadPool.awaitTermination(5, TimeUnit.SECONDS); i++) {
            LOG.debug("Waiting for hlog writers to terminate, iteration #" + i);
          }
        }catch(InterruptedException ex) {
          LOG.warn("Hlog writers were interrupted, possible data loss!");
        }
      }
    } finally {
      splits = new ArrayList<Path>(logWriters.size());
      for (WriterAndPath wap : logWriters.values()) {
        wap.w.close();
        LOG.debug("Closed " + wap.p);
        splits.add(wap.p);
      }
    }
    return splits;
  }

  /**
   * Utility class that lets us keep track of the edit with it's key
   * Only used when splitting logs
   */
  public static class HLogEntry {
    private KeyValue edit;
    private HLogKey key;
    /**
     * Constructor for both params
     * @param edit log's edit
     * @param key log's key
     */
    public HLogEntry(KeyValue edit, HLogKey key) {
      super();
      this.edit = edit;
      this.key = key;
    }
    /**
     * Gets the edit
     * @return edit
     */
    public KeyValue getEdit() {
      return edit;
    }
    /**
     * Gets the key
     * @return key
     */
    public HLogKey getKey() {
      return key;
    }

    public String toString() {
      return this.key + "=" + this.edit;
    }
  }

  /**
   * Construct the HLog directory name
   * 
   * @param info HServerInfo for server
   * @return the HLog directory name
   */
  public static String getHLogDirectoryName(HServerInfo info) {
    return getHLogDirectoryName(HServerInfo.getServerName(info));
  }

  /**
   * Construct the HLog directory name
   * 
   * @param serverAddress
   * @param startCode
   * @return the HLog directory name
   */
  public static String getHLogDirectoryName(String serverAddress,
      long startCode) {
    if (serverAddress == null || serverAddress.length() == 0) {
      return null;
    }
    return getHLogDirectoryName(
        HServerInfo.getServerName(serverAddress, startCode));
  }
  
  /**
   * Construct the HLog directory name
   * 
   * @param serverName
   * @return the HLog directory name
   */
  public static String getHLogDirectoryName(String serverName) {
    StringBuilder dirName = new StringBuilder(HConstants.HREGION_LOGDIR_NAME);
    dirName.append("/");
    dirName.append(serverName);
    return dirName.toString();
  }

  private static void usage() {
    System.err.println("Usage: java org.apache.hbase.HLog" +
        " {--dump <logfile>... | --split <logdir>...}");
  }

  /**
   * Pass one or more log file names and it will either dump out a text version
   * on <code>stdout</code> or split the specified log files.
   *
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      usage();
      System.exit(-1);
    }
    boolean dump = true;
    if (args[0].compareTo("--dump") != 0) {
      if (args[0].compareTo("--split") == 0) {
        dump = false;

      } else {
        usage();
        System.exit(-1);
      }
    }
    HBaseConfiguration conf = new HBaseConfiguration();
    FileSystem fs = FileSystem.get(conf);
    Path baseDir = new Path(conf.get(HBASE_DIR));
    for (int i = 1; i < args.length; i++) {
      Path logPath = new Path(args[i]);
      if (!fs.exists(logPath)) {
        throw new FileNotFoundException(args[i] + " does not exist");
      }
      if (dump) {
        if (!fs.isFile(logPath)) {
          throw new IOException(args[i] + " is not a file");
        }
        Reader log = new SequenceFile.Reader(fs, logPath, conf);
        try {
          HLogKey key = new HLogKey();
          KeyValue val = new KeyValue();
          while (log.next(key, val)) {
            System.out.println(key.toString() + " " + val.toString());
          }
        } finally {
          log.close();
        }
      } else {
        if (!fs.getFileStatus(logPath).isDir()) {
          throw new IOException(args[i] + " is not a directory");
        }
        splitLog(baseDir, logPath, fs, conf);
      }
    }
  }

  public static final long FIXED_OVERHEAD = ClassSize.align(
      ClassSize.OBJECT + (5 * ClassSize.REFERENCE) +
      ClassSize.ATOMIC_INTEGER + Bytes.SIZEOF_INT + (3 * Bytes.SIZEOF_LONG));
  
  static class HLogWriter extends SequenceFile.Writer {
    public HLogWriter(FileSystem arg0, Configuration arg1, Path arg2,
        Class<?> arg3, Class<?> arg4, int arg5, short arg6, long arg7,
        Progressable arg8, Metadata arg9) throws IOException {
      super(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9);
    }
    
    void flush() {
      
    }
  }
}
