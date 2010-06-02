/**
 * Copyright 2010 The Apache Software Foundation
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import com.google.common.util.concurrent.NamingThreadFactory;

import static org.apache.hadoop.hbase.util.FSUtils.recoverFileLease;

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
  public static final byte [] METAFAMILY = Bytes.toBytes("METAFAMILY");
  static final byte [] METAROW = Bytes.toBytes("METAROW");
  private final FileSystem fs;
  private final Path dir;
  private final Configuration conf;
  private final LogRollListener listener;
  private final long optionalFlushInterval;
  private final long blocksize;
  private final int flushlogentries;
  private final String prefix;
  private final AtomicInteger unflushedEntries = new AtomicInteger(0);
  private final Path oldLogDir;
  private final List<LogActionsListener> actionListeners =
      Collections.synchronizedList(new ArrayList<LogActionsListener>());


  private static Class<? extends Writer> logWriterClass;
  private static Class<? extends Reader> logReaderClass;

  private OutputStream hdfs_out;     // OutputStream associated with the current SequenceFile.writer
  private int initialReplication;    // initial replication factor of SequenceFile.writer
  private Method getNumCurrentReplicas; // refers to DFSOutputStream.getNumCurrentReplicas
  final static Object [] NO_ARGS = new Object []{};

  // used to indirectly tell syncFs to force the sync
  private boolean forceSync = false;

  public interface Reader {
    void init(FileSystem fs, Path path, Configuration c) throws IOException;
    void close() throws IOException;
    Entry next() throws IOException;
    Entry next(Entry reuse) throws IOException;
    void seek(long pos) throws IOException;
    long getPosition() throws IOException;
  }

  public interface Writer {
    void init(FileSystem fs, Path path, Configuration c) throws IOException;
    void close() throws IOException;
    void sync() throws IOException;
    void append(Entry entry) throws IOException;
    long getLength() throws IOException;
  }

  /*
   * Current log file.
   */
  Writer writer;

  /*
   * Map of all log files but the current one.
   */
  final SortedMap<Long, Path> outputfiles =
    Collections.synchronizedSortedMap(new TreeMap<Long, Path>());

  /*
   * Map of regions to first sequence/edit id in their memstore.
   */
  private final ConcurrentSkipListMap<byte [], Long> lastSeqWritten =
    new ConcurrentSkipListMap<byte [], Long>(Bytes.BYTES_COMPARATOR);

  private volatile boolean closed = false;

  private final AtomicLong logSeqNum = new AtomicLong(0);

  // The timestamp (in ms) when the log file was created.
  private volatile long filenum = -1;

  //number of transactions in the current Hlog.
  private final AtomicInteger numEntries = new AtomicInteger(0);

  // If > than this size, roll the log. This is typically 0.95 times the size
  // of the default Hdfs block size.
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

  /**
   * Pattern used to validate a HLog file name
   */
  private static final Pattern pattern = Pattern.compile(".*\\.\\d*");

  static byte [] COMPLETE_CACHE_FLUSH;
  static {
    try {
      COMPLETE_CACHE_FLUSH = "HBASE::CACHEFLUSH".getBytes(UTF8_ENCODING);
    } catch (UnsupportedEncodingException e) {
      assert(false);
    }
  }

  // For measuring latency of writes
  private static volatile long writeOps;
  private static volatile long writeTime;
  // For measuring latency of syncs
  private static volatile long syncOps;
  private static volatile long syncTime;

  public static long getWriteOps() {
    long ret = writeOps;
    writeOps = 0;
    return ret;
  }

  public static long getWriteTime() {
    long ret = writeTime;
    writeTime = 0;
    return ret;
  }

  public static long getSyncOps() {
    long ret = syncOps;
    syncOps = 0;
    return ret;
  }

  public static long getSyncTime() {
    long ret = syncTime;
    syncTime = 0;
    return ret;
  }

  /**
   * HLog creating with a null actions listener.
   *
   * @param fs filesystem handle
   * @param dir path to where hlogs are stored
   * @param oldLogDir path to where hlogs are archived
   * @param conf configuration to use
   * @param listener listerner used to request log rolls
   * @throws IOException
   */
  public HLog(final FileSystem fs, final Path dir, final Path oldLogDir,
              final Configuration conf, final LogRollListener listener)
  throws IOException {
    this(fs, dir, oldLogDir, conf, listener, null, null);
  }

  /**
   * Create an edit log at the given <code>dir</code> location.
   *
   * You should never have to load an existing log. If there is a log at
   * startup, it should have already been processed and deleted by the time the
   * HLog object is started up.
   *
   * @param fs filesystem handle
   * @param dir path to where hlogs are stored
   * @param oldLogDir path to where hlogs are archived
   * @param conf configuration to use
   * @param listener listerner used to request log rolls
   * @param actionListener optional listener for hlog actions like archiving
   * @param prefix should always be hostname and port in distributed env and
   *        it will be URL encoded before being used.
   *        If prefix is null, "hlog" will be used
   * @throws IOException
   */
  public HLog(final FileSystem fs, final Path dir, final Path oldLogDir,
              final Configuration conf, final LogRollListener listener,
              final LogActionsListener actionListener, final String prefix)
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
    // Roll at 95% of block size.
    float multi = conf.getFloat("hbase.regionserver.logroll.multiplier", 0.95f);
    this.logrollsize = (long)(this.blocksize * multi);
    this.optionalFlushInterval =
      conf.getLong("hbase.regionserver.optionallogflushinterval", 1 * 1000);
    if (fs.exists(dir)) {
      throw new IOException("Target HLog directory already exists: " + dir);
    }
    fs.mkdirs(dir);
    this.oldLogDir = oldLogDir;
    if (!fs.exists(oldLogDir)) {
      fs.mkdirs(this.oldLogDir);
    }
    this.maxLogs = conf.getInt("hbase.regionserver.maxlogs", 32);
    this.enabled = conf.getBoolean("hbase.regionserver.hlog.enabled", true);
    LOG.info("HLog configuration: blocksize=" + this.blocksize +
      ", rollsize=" + this.logrollsize +
      ", enabled=" + this.enabled +
      ", flushlogentries=" + this.flushlogentries +
      ", optionallogflushinternal=" + this.optionalFlushInterval + "ms");
    if (actionListener != null) {
      addLogActionsListerner(actionListener);
    }
    // If prefix is null||empty then just name it hlog
    this.prefix = prefix == null || prefix.isEmpty() ?
        "hlog" : URLEncoder.encode(prefix, "UTF8");
    // rollWriter sets this.hdfs_out if it can.
    rollWriter();

    // handle the reflection necessary to call getNumCurrentReplicas()
    this.getNumCurrentReplicas = null;
    if(this.hdfs_out != null) {
      try {
        this.getNumCurrentReplicas = this.hdfs_out.getClass().
          getMethod("getNumCurrentReplicas", new Class<?> []{});
        this.getNumCurrentReplicas.setAccessible(true);
      } catch (NoSuchMethodException e) {
        // Thrown if getNumCurrentReplicas() function isn't available
      } catch (SecurityException e) {
        // Thrown if we can't get access to getNumCurrentReplicas()
        this.getNumCurrentReplicas = null; // could happen on setAccessible()
      }
    }
    if(this.getNumCurrentReplicas != null) {
      LOG.info("Using getNumCurrentReplicas--HDFS-826");
    } else {
      LOG.info("getNumCurrentReplicas--HDFS-826 not available" );
    }

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

  // usage: see TestLogRolling.java
  OutputStream getOutputStream() {
    return this.hdfs_out;
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
   * @return If lots of logs, flush the returned regions so next time through
   * we can clean logs. Returns null if nothing to flush.
   * @throws org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException
   * @throws IOException
   */
  public byte [][] rollWriter() throws FailedLogCloseException, IOException {
    // Return if nothing to flush.
    if (this.writer != null && this.numEntries.get() <= 0) {
      return null;
    }
    byte [][] regionsToFlush = null;
    this.cacheFlushLock.lock();
    try {
      if (closed) {
        return regionsToFlush;
      }
      synchronized (updateLock) {
        // Clean up current writer.
        Path oldFile = cleanupCurrentWriter(this.filenum);
        this.filenum = System.currentTimeMillis();
        Path newPath = computeFilename();
        this.writer = createWriter(fs, newPath, HBaseConfiguration.create(conf));
        this.initialReplication = fs.getFileStatus(newPath).getReplication();

        // Can we get at the dfsclient outputstream?  If an instance of
        // SFLW, it'll have done the necessary reflection to get at the
        // protected field name.
        this.hdfs_out = null;
        if (this.writer instanceof SequenceFileLogWriter) {
          this.hdfs_out =
            ((SequenceFileLogWriter)this.writer).getDFSCOutputStream();
        }

        LOG.info((oldFile != null?
            "Roll " + FSUtils.getPath(oldFile) + ", entries=" +
            this.numEntries.get() +
            ", filesize=" +
            this.fs.getFileStatus(oldFile).getLen() + ". ": "") +
          "New hlog " + FSUtils.getPath(newPath));
        // Tell our listeners that a new log was created
        if (!this.actionListeners.isEmpty()) {
          for (LogActionsListener list : this.actionListeners) {
            list.logRolled(newPath);
          }
        }
        // Can we delete any of the old log files?
        if (this.outputfiles.size() > 0) {
          if (this.lastSeqWritten.size() <= 0) {
            LOG.debug("Last sequence written is empty. Deleting all old hlogs");
            // If so, then no new writes have come in since all regions were
            // flushed (and removed from the lastSeqWritten map). Means can
            // remove all but currently open log file.
            for (Map.Entry<Long, Path> e : this.outputfiles.entrySet()) {
              archiveLogFile(e.getValue(), e.getKey());
            }
            this.outputfiles.clear();
          } else {
            regionsToFlush = cleanOldLogs();
          }
        }
        this.numEntries.set(0);
      }
    } finally {
      this.cacheFlushLock.unlock();
    }
    return regionsToFlush;
  }

  /**
   * Get a reader for the WAL.
   * @param fs
   * @param path
   * @param conf
   * @return A WAL reader.  Close when done with it.
   * @throws IOException
   */
  public static Reader getReader(final FileSystem fs,
    final Path path, Configuration conf)
  throws IOException {
    try {
      if (logReaderClass == null) {
        logReaderClass =conf.getClass("hbase.regionserver.hlog.reader.impl",
                SequenceFileLogReader.class, Reader.class);
      }

      HLog.Reader reader = logReaderClass.newInstance();
      reader.init(fs, path, conf);
      return reader;
    } catch (IOException e) {
      throw e;
    }
    catch (Exception e) {
      throw new IOException("Cannot get log reader", e);
    }
  }

  /**
   * Get a writer for the WAL.
   * @param path
   * @param conf
   * @return A WAL writer.  Close when done with it.
   * @throws IOException
   */
  public static Writer createWriter(final FileSystem fs,
      final Path path, Configuration conf)
  throws IOException {
    try {
      if (logWriterClass == null) {
        logWriterClass = conf.getClass("hbase.regionserver.hlog.writer.impl",
                SequenceFileLogWriter.class, Writer.class);
      }
      HLog.Writer writer = (HLog.Writer) logWriterClass.newInstance();
      writer.init(fs, path, conf);
      return writer;
    } catch (Exception e) {
      IOException ie = new IOException("cannot get log writer");
      ie.initCause(e);
      throw ie;
    }
  }

  /*
   * Clean up old commit logs.
   * @return If lots of logs, flush the returned region so next time through
   * we can clean logs. Returns null if nothing to flush.
   * @throws IOException
   */
  private byte [][] cleanOldLogs() throws IOException {
    Long oldestOutstandingSeqNum = getOldestOutstandingSeqNum();
    // Get the set of all log files whose final ID is older than or
    // equal to the oldest pending region operation
    TreeSet<Long> sequenceNumbers =
      new TreeSet<Long>(this.outputfiles.headMap(
        (Long.valueOf(oldestOutstandingSeqNum.longValue() + 1L))).keySet());
    // Now remove old log files (if any)
    int logsToRemove = sequenceNumbers.size();
    if (logsToRemove > 0) {
      if (LOG.isDebugEnabled()) {
        // Find associated region; helps debugging.
        byte [] oldestRegion = getOldestRegion(oldestOutstandingSeqNum);
        LOG.debug("Found " + logsToRemove + " hlogs to remove " +
          " out of total " + this.outputfiles.size() + "; " +
          "oldest outstanding seqnum is " + oldestOutstandingSeqNum +
          " from region " + Bytes.toString(oldestRegion));
      }
      for (Long seq : sequenceNumbers) {
        archiveLogFile(this.outputfiles.remove(seq), seq);
      }
    }

    // If too many log files, figure which regions we need to flush.
    byte [][] regions = null;
    int logCount = this.outputfiles.size() - logsToRemove;
    if (logCount > this.maxLogs && this.outputfiles != null &&
        this.outputfiles.size() > 0) {
      regions = findMemstoresWithEditsOlderThan(this.outputfiles.firstKey(),
        this.lastSeqWritten);
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < regions.length; i++) {
        if (i > 0) sb.append(", ");
        sb.append(Bytes.toStringBinary(regions[i]));
      }
      LOG.info("Too many hlogs: logs=" + logCount + ", maxlogs=" +
        this.maxLogs + "; forcing flush of " + regions.length + " regions(s): " +
        sb.toString());
    }
    return regions;
  }

  /**
   * Return regions (memstores) that have edits that are less than the passed
   * <code>oldestWALseqid</code>.
   * @param oldestWALseqid
   * @param regionsToSeqids
   * @return All regions whose seqid is < than <code>oldestWALseqid</code> (Not
   * necessarily in order).  Null if no regions found.
   */
  static byte [][] findMemstoresWithEditsOlderThan(final long oldestWALseqid,
      final Map<byte [], Long> regionsToSeqids) {
    //  This method is static so it can be unit tested the easier.
    List<byte []> regions = null;
    for (Map.Entry<byte [], Long> e: regionsToSeqids.entrySet()) {
      if (e.getValue().longValue() < oldestWALseqid) {
        if (regions == null) regions = new ArrayList<byte []>();
        regions.add(e.getKey());
      }
    }
    return regions == null?
      null: regions.toArray(new byte [][] {HConstants.EMPTY_BYTE_ARRAY});
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
        oldFile = computeFilename();
        this.outputfiles.put(Long.valueOf(this.logSeqNum.get() - 1), oldFile);
      }
    }
    return oldFile;
  }

  private void archiveLogFile(final Path p, final Long seqno) throws IOException {
    Path newPath = getHLogArchivePath(this.oldLogDir, p);
    LOG.info("moving old hlog file " + FSUtils.getPath(p) +
      " whose highest sequence/edit id is " + seqno + " to " +
      FSUtils.getPath(newPath));
    this.fs.rename(p, newPath);
    if (!this.actionListeners.isEmpty()) {
      for (LogActionsListener list : this.actionListeners) {
        list.logArchived(p, newPath);
      }
    }
  }

  /**
   * This is a convenience method that computes a new filename with a given
   * file-number.
   * @return Path
   */
  protected Path computeFilename() {
    if (filenum < 0) {
      throw new RuntimeException("hlog file number can't be < 0");
    }
    return new Path(dir, prefix + "." + filenum);
  }

  /**
   * Shut down the log and delete the log directory
   *
   * @throws IOException
   */
  public void closeAndDelete() throws IOException {
    close();
    FileStatus[] files = fs.listStatus(this.dir);
    for(FileStatus file : files) {
      fs.rename(file.getPath(),
          getHLogArchivePath(this.oldLogDir, file.getPath()));
    }
    LOG.debug("Moved " + files.length + " log files to " +
        FSUtils.getPath(this.oldLogDir));
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
      logSyncerThread.join(this.optionalFlushInterval*2);
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
  public void append(HRegionInfo regionInfo, WALEdit logEdit,
    final long now,
    final boolean isMetaRegion)
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
  public void append(HRegionInfo regionInfo, HLogKey logKey, WALEdit logEdit)
  throws IOException {
    if (this.closed) {
      throw new IOException("Cannot append; log is closed");
    }
    byte [] regionName = regionInfo.getRegionName();
    synchronized (updateLock) {
      long seqNum = obtainSeqNum();
      logKey.setLogSeqNum(seqNum);
      // The 'lastSeqWritten' map holds the sequence number of the oldest
      // write for each region (i.e. the first edit added to the particular
      // memstore). When the cache is flushed, the entry for the
      // region being flushed is removed if the sequence number of the flush
      // is greater than or equal to the value in lastSeqWritten.
      this.lastSeqWritten.putIfAbsent(regionName, Long.valueOf(seqNum));
      doWrite(regionInfo, logKey, logEdit);
      this.unflushedEntries.incrementAndGet();
      this.numEntries.incrementAndGet();
    }

    // sync txn to file system
    this.sync(regionInfo.isMetaRegion());
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
   * @param info
   * @param tableName
   * @param edits
   * @param now
   * @throws IOException
   */
  public void append(HRegionInfo info, byte [] tableName, WALEdit edits,
    final long now)
  throws IOException {
    byte[] regionName = info.getRegionName();
    if (this.closed) {
      throw new IOException("Cannot append; log is closed");
    }
    synchronized (this.updateLock) {
      long seqNum = obtainSeqNum();
      // The 'lastSeqWritten' map holds the sequence number of the oldest
      // write for each region (i.e. the first edit added to the particular
      // memstore). . When the cache is flushed, the entry for the
      // region being flushed is removed if the sequence number of the flush
      // is greater than or equal to the value in lastSeqWritten.
      this.lastSeqWritten.putIfAbsent(regionName, seqNum);
      HLogKey logKey = makeKey(regionName, tableName, seqNum, now);
      doWrite(info, logKey, edits);
      this.numEntries.incrementAndGet();

      // Only count 1 row as an unflushed entry.
      this.unflushedEntries.incrementAndGet();
    }
    // sync txn to file system
    this.sync(info.isMetaRegion());
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

    private boolean syncerShuttingDown = false;

    LogSyncer(long optionalFlushInterval) {
      this.optionalFlushInterval = optionalFlushInterval;
    }

    @Override
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
        syncerShuttingDown = true;
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
        if (syncerShuttingDown) {
          LOG.warn(getName() + " was shut down while waiting for sync");
          return;
        }
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
      boolean logRollRequested = false;
      if (this.forceSync ||
          this.unflushedEntries.get() >= this.flushlogentries) {
        try {
          long now = System.currentTimeMillis();
          this.writer.sync();
          syncTime += System.currentTimeMillis() - now;
          syncOps++;
          this.forceSync = false;
          this.unflushedEntries.set(0);

          // if the number of replicas in HDFS has fallen below the initial
          // value, then roll logs.
          try {
            int numCurrentReplicas = getLogReplication();
            if (numCurrentReplicas != 0 &&
                numCurrentReplicas < this.initialReplication) {
              LOG.warn("HDFS pipeline error detected. " +
                  "Found " + numCurrentReplicas + " replicas but expecting " +
                  this.initialReplication + " replicas. " +
                  " Requesting close of hlog.");
              requestLogRoll();
              logRollRequested = true;
            }
          } catch (Exception e) {
              LOG.warn("Unable to invoke DFSOutputStream.getNumCurrentReplicas" + e +
                       " still proceeding ahead...");
          }
        } catch (IOException e) {
          LOG.fatal("Could not append. Requesting close of hlog", e);
          requestLogRoll();
          throw e;
        }
      }

      if (!logRollRequested && (this.writer.getLength() > this.logrollsize)) {
        requestLogRoll();
      }
    }
  }

  /**
   * This method gets the datanode replication count for the current HLog.
   *
   * If the pipeline isn't started yet or is empty, you will get the default
   * replication factor.  Therefore, if this function returns 0, it means you
   * are not properly running with the HDFS-826 patch.
   *
   * @throws Exception
   */
  int getLogReplication() throws Exception {
    if(this.getNumCurrentReplicas != null && this.hdfs_out != null) {
      Object repl = this.getNumCurrentReplicas.invoke(this.hdfs_out, NO_ARGS);
      if (repl instanceof Integer) {
        return ((Integer)repl).intValue();
      }
    }
    return 0;
  }

  boolean canGetCurReplicas() {
    return this.getNumCurrentReplicas != null;
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

  protected void doWrite(HRegionInfo info, HLogKey logKey, WALEdit logEdit)
  throws IOException {
    if (!this.enabled) {
      return;
    }
    try {
      long now = System.currentTimeMillis();
      this.writer.append(new HLog.Entry(logKey, logEdit));
      long took = System.currentTimeMillis() - now;
      writeTime += took;
      writeOps++;
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

  /**
   * By acquiring a log sequence ID, we can allow log messages to continue while
   * we flush the cache.
   *
   * Acquire a lock so that we do not roll the log between the start and
   * completion of a cache-flush. Otherwise the log-seq-id for the flush will
   * not appear in the correct logfile.
   *
   * @return sequence ID to pass {@link #completeCacheFlush(byte[], byte[], long, boolean)}
   * (byte[], byte[], long)}
   * @see #completeCacheFlush(byte[], byte[], long, boolean)
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
    final long logSeqId,
    final boolean isMetaRegion)
  throws IOException {
    try {
      if (this.closed) {
        return;
      }
      synchronized (updateLock) {
        long now = System.currentTimeMillis();
        WALEdit edit = completeCacheFlushLogEdit();
        HLogKey key = makeKey(regionName, tableName, logSeqId,
            System.currentTimeMillis());
        this.writer.append(new Entry(key, edit));
        writeTime += System.currentTimeMillis() - now;
        writeOps++;
        this.numEntries.incrementAndGet();
        Long seq = this.lastSeqWritten.get(regionName);
        if (seq != null && logSeqId >= seq.longValue()) {
          this.lastSeqWritten.remove(regionName);
        }
      }
      // sync txn to file system
      this.sync(isMetaRegion);

    } finally {
      this.cacheFlushLock.unlock();
    }
  }

  private WALEdit completeCacheFlushLogEdit() {
    KeyValue kv = new KeyValue(METAROW, METAFAMILY, null,
      System.currentTimeMillis(), COMPLETE_CACHE_FLUSH);
    WALEdit e = new WALEdit();
    e.add(kv);
    return e;
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
   * @param oldLogDir directory where processed (split) logs will be archived to
   * @param fs FileSystem
   * @param conf Configuration
   * @throws IOException will throw if corrupted hlogs aren't tolerated
   * @return the list of splits
   */
  public static List<Path> splitLog(final Path rootDir, final Path srcDir,
    Path oldLogDir, final FileSystem fs, final Configuration conf)
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
    splits = splitLog(rootDir, srcDir, oldLogDir, logfiles, fs, conf);
    try {
      LOG.info("Spliting is done. Removing old log dir "+srcDir);
      fs.delete(srcDir, false);
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
    final Writer w;
    WriterAndPath(final Path p, final Writer w) {
      this.p = p;
      this.w = w;
    }
  }

  @SuppressWarnings("unchecked")
  public static Class<? extends HLogKey> getKeyClass(Configuration conf) {
     return (Class<? extends HLogKey>)
       conf.getClass("hbase.regionserver.hlog.keyclass", HLogKey.class);
  }

  public static HLogKey newKey(Configuration conf) throws IOException {
    Class<? extends HLogKey> keyClass = getKeyClass(conf);
    try {
      return keyClass.newInstance();
    } catch (InstantiationException e) {
      throw new IOException("cannot create hlog key");
    } catch (IllegalAccessException e) {
      throw new IOException("cannot create hlog key");
    }
  }

  /**
   * Sorts the HLog edits in the given list of logfiles (that are a mix of edits on multiple regions)
   * by region and then splits them per region directories, in batches of (hbase.hlog.split.batch.size)
   *
   * A batch consists of a set of log files that will be sorted in a single map of edits indexed by region
   * the resulting map will be concurrently written by multiple threads to their corresponding regions
   *
   * Each batch consists of more more log files that are
   *  - recovered (files is opened for append then closed to ensure no process is writing into it)
   *  - parsed (each edit in the log is appended to a list of edits indexed by region
   *    see {@link #parseHLog} for more details)
   *  - marked as either processed or corrupt depending on parsing outcome
   *  - the resulting edits indexed by region are concurrently written to their corresponding region
   *    region directories
   *  - original files are then archived to a different directory
   *
   *
   *
   * @param rootDir  hbase directory
   * @param srcDir   logs directory
   * @param oldLogDir directory where processed logs are archived to
   * @param logfiles the list of log files to split
   * @param fs
   * @param conf
   * @return
   * @throws IOException
   */
  private static List<Path> splitLog(final Path rootDir, final Path srcDir,
    Path oldLogDir, final FileStatus[] logfiles, final FileSystem fs,
    final Configuration conf)
  throws IOException {
    List<Path> processedLogs = new ArrayList<Path>();
    List<Path> corruptedLogs = new ArrayList<Path>();
    final Map<byte [], WriterAndPath> logWriters =
      Collections.synchronizedMap(
        new TreeMap<byte [], WriterAndPath>(Bytes.BYTES_COMPARATOR));
    List<Path> splits = null;

    // Number of logs in a read batch
    // More means faster but bigger mem consumption
    //TODO make a note on the conf rename and update hbase-site.xml if needed
    int logFilesPerStep =
      conf.getInt("hbase.hlog.split.batch.size", 3);
     boolean skipErrors = conf.getBoolean("hbase.hlog.split.skip.errors", false);


    try {
      int i = -1;
      while (i < logfiles.length) {
        final Map<byte[], LinkedList<Entry>> editsByRegion =
          new TreeMap<byte[], LinkedList<Entry>>(Bytes.BYTES_COMPARATOR);
        for (int j = 0; j < logFilesPerStep; j++) {
          i++;
          if (i == logfiles.length) {
            break;
          }
          FileStatus log = logfiles[i];
          Path logPath = log.getPath();
          long logLength = log.getLen();
          LOG.debug("Splitting hlog " + (i + 1) + " of " + logfiles.length +
            ": " + logPath + ", length=" + logLength );
          try {
            recoverFileLease(fs, logPath, conf);
            parseHLog(log, editsByRegion, fs, conf);
            processedLogs.add(logPath);
           } catch (IOException e) {
             if (skipErrors) {
               LOG.warn("Got while parsing hlog " + logPath +
                 ". Marking as corrupted", e);
               corruptedLogs.add(logPath);
             } else {
               throw e;
             }
          }
        }
        writeEditsBatchToRegions(editsByRegion, logWriters, rootDir, fs, conf);
      }
      if (fs.listStatus(srcDir).length > processedLogs.size() + corruptedLogs.size()) {
        throw new IOException("Discovered orphan hlog after split. Maybe " +
          "HRegionServer was not dead when we started");
      }
      archiveLogs(corruptedLogs, processedLogs, oldLogDir, fs, conf);
    } finally {
      splits = new ArrayList<Path>(logWriters.size());
      for (WriterAndPath wap : logWriters.values()) {
        wap.w.close();
        splits.add(wap.p);
        LOG.debug("Closed " + wap.p);
      }
    }
    return splits;
  }


  /**
   * Utility class that lets us keep track of the edit with it's key
   * Only used when splitting logs
   */
  public static class Entry implements Writable {
    private WALEdit edit;
    private HLogKey key;

    public Entry() {
      edit = new WALEdit();
      key = new HLogKey();
    }

    /**
     * Constructor for both params
     * @param edit log's edit
     * @param key log's key
     */
    public Entry(HLogKey key, WALEdit edit) {
      super();
      this.key = key;
      this.edit = edit;
    }
    /**
     * Gets the edit
     * @return edit
     */
    public WALEdit getEdit() {
      return edit;
    }
    /**
     * Gets the key
     * @return key
     */
    public HLogKey getKey() {
      return key;
    }

    @Override
    public String toString() {
      return this.key + "=" + this.edit;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      this.key.write(dataOutput);
      this.edit.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      this.key.readFields(dataInput);
      this.edit.readFields(dataInput);
    }
  }

  /**
   * Construct the HLog directory name
   *
   * @param info HServerInfo for server
   * @return the HLog directory name
   */
  public static String getHLogDirectoryName(HServerInfo info) {
    return getHLogDirectoryName(info.getServerName());
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

  public static boolean validateHLogFilename(String filename) {
    return pattern.matcher(filename).matches();
  }

  private static Path getHLogArchivePath(Path oldLogDir, Path p) {
    return new Path(oldLogDir, p.getName());
  }

  /**
   * Takes splitLogsMap and concurrently writes them to region directories using a thread pool
   *
   * @param splitLogsMap map that contains the log splitting result indexed by region
   * @param logWriters map that contains a writer per region
   * @param rootDir hbase root dir
   * @param fs
   * @param conf
   * @throws IOException
   */
  private static void writeEditsBatchToRegions(
    final Map<byte[], LinkedList<Entry>> splitLogsMap,
    final Map<byte[], WriterAndPath> logWriters,
    final Path rootDir, final FileSystem fs, final Configuration conf)
  throws IOException {
    // Number of threads to use when log splitting to rewrite the logs.
    // More means faster but bigger mem consumption.
    int logWriterThreads =
      conf.getInt("hbase.regionserver.hlog.splitlog.writer.threads", 3);
    boolean skipErrors = conf.getBoolean("hbase.skip.errors", false);
    HashMap<byte[], Future> writeFutureResult = new HashMap<byte[], Future>();
    NamingThreadFactory f  = new NamingThreadFactory(
            "SplitWriter-%1$d", Executors.defaultThreadFactory());
    ThreadPoolExecutor threadPool = (ThreadPoolExecutor)Executors.newFixedThreadPool(logWriterThreads, f);
    for (final byte[] region : splitLogsMap.keySet()) {
      Callable splitter = createNewSplitter(rootDir, logWriters, splitLogsMap, region, fs, conf);
      writeFutureResult.put(region, threadPool.submit(splitter));
    }

    threadPool.shutdown();
    // Wait for all threads to terminate
    try {
      for (int j = 0; !threadPool.awaitTermination(5, TimeUnit.SECONDS); j++) {
        String message = "Waiting for hlog writers to terminate, elapsed " + j * 5 + " seconds";
        if (j < 30) {
          LOG.debug(message);
        } else {
          LOG.info(message);
        }

      }
    } catch(InterruptedException ex) {
      LOG.warn("Hlog writers were interrupted, possible data loss!");
      if (!skipErrors) {
        throw new IOException("Could not finish writing log entries",  ex);
        //TODO  maybe we should fail here regardless if skipErrors is active or not
      }
    }

    for (Map.Entry<byte[], Future> entry : writeFutureResult.entrySet()) {
      try {
        entry.getValue().get();
      } catch (ExecutionException e) {
        throw (new IOException(e.getCause()));
      } catch (InterruptedException e1) {
        LOG.warn("Writer for region " +  Bytes.toString(entry.getKey()) +
                " was interrupted, however the write process should have " +
                "finished. Throwing up ", e1);
        throw (new IOException(e1.getCause()));
      }
    }
  }

  /*
   * Parse a single hlog and put the edits in @splitLogsMap
   *
   * @param logfile to split
   * @param splitLogsMap output parameter: a map with region names as keys and a
   * list of edits as values
   * @param fs the filesystem
   * @param conf the configuration
   * @throws IOException if hlog is corrupted, or can't be open
   */
  private static void parseHLog(final FileStatus logfile,
    final Map<byte[], LinkedList<Entry>> splitLogsMap, final FileSystem fs,
    final Configuration conf)
  throws IOException {
    // Check for possibly empty file. With appends, currently Hadoop reports a
    // zero length even if the file has been sync'd. Revisit if HDFS-376 or
    // HDFS-878 is committed.
    long length = logfile.getLen();
    if (length <= 0) {
      LOG.warn("File " + logfile.getPath() + " might be still open, length is 0");
    }
    Path path = logfile.getPath();
    Reader in;
    int editsCount = 0;
    try {
      in = HLog.getReader(fs, path, conf);
    } catch (EOFException e) {
      if (length <= 0) {
        //TODO should we ignore an empty, not-last log file if skip.errors is false?
        //Either way, the caller should decide what to do. E.g. ignore if this is the last
        //log in sequence.
        //TODO is this scenario still possible if the log has been recovered (i.e. closed)
        LOG.warn("Could not open " + path + " for reading. File is empty" + e);
        return;
      } else {
        throw e;
      }
    }
    try {
      Entry entry;
      while ((entry = in.next()) != null) {
        byte[] region = entry.getKey().getRegionName();
        LinkedList<Entry> queue = splitLogsMap.get(region);
        if (queue == null) {
          queue = new LinkedList<Entry>();
          splitLogsMap.put(region, queue);
        }
        queue.addFirst(entry);
        editsCount++;
      }
      LOG.debug("Pushed=" + editsCount + " entries from " + path);
    } finally {
      try {
        if (in != null) {
          in.close();
        }
      } catch (IOException e) {
        LOG.warn("Close log reader in finally threw exception -- continuing", e);
      }
    }
  }

  private static Callable<Void> createNewSplitter(final Path rootDir,
    final Map<byte[], WriterAndPath> logWriters,
    final Map<byte[], LinkedList<Entry>> logEntries,
    final byte[] region, final FileSystem fs, final Configuration conf) {
    return new Callable<Void>() {
      public String getName() {
        return "Split writer thread for region " + Bytes.toStringBinary(region);
      }

      @Override
      public Void call() throws IOException {
        LinkedList<Entry> entries = logEntries.get(region);
        LOG.debug(this.getName()+" got " + entries.size() + " to process");
        long threadTime = System.currentTimeMillis();
        try {
          int editsCount = 0;
          WriterAndPath wap = logWriters.get(region);
          for (ListIterator<Entry> iterator = entries.listIterator();
               iterator.hasNext();) {
            Entry logEntry =  iterator.next();

            if (wap == null) {
              Path logFile = getRegionLogPath(logEntry, rootDir);
              if (fs.exists(logFile)) {
                LOG.warn("Found existing old hlog file. It could be the result of a previous" +
                        "failed split attempt. Deleting " + logFile +
                        ", length=" + fs.getFileStatus(logFile).getLen());
                fs.delete(logFile, false);
              }
              Writer w = createWriter(fs, logFile, conf);
              wap = new WriterAndPath(logFile, w);
              logWriters.put(region, wap);
              LOG.debug("Creating writer path=" + logFile +
                " region=" + Bytes.toStringBinary(region));
            }

            wap.w.append(logEntry);
            editsCount++;
          }
          LOG.debug(this.getName() + " Applied " + editsCount +
            " total edits to " + Bytes.toStringBinary(region) +
            " in " + (System.currentTimeMillis() - threadTime) + "ms");
        } catch (IOException e) {
          e = RemoteExceptionHandler.checkIOException(e);
          LOG.fatal(this.getName() + " Got while writing log entry to log", e);
          throw e;
        }
        return null;
      }
    };
  }

  /**
   * Moves processed logs to a oldLogDir after successful processing
   * Moves corrupted logs (any log that couldn't be successfully parsed
   * to corruptDir (.corrupt) for later investigation
   *
   * @param corruptedLogs
   * @param processedLogs
   * @param oldLogDir
   * @param fs
   * @param conf
   * @throws IOException
   */
  private static void archiveLogs(final List<Path> corruptedLogs,
    final List<Path> processedLogs, final Path oldLogDir,
    final FileSystem fs, final Configuration conf)
  throws IOException{
    final Path corruptDir = new Path(conf.get(HBASE_DIR),
      conf.get("hbase.regionserver.hlog.splitlog.corrupt.dir", ".corrupt"));

    fs.mkdirs(corruptDir);
    fs.mkdirs(oldLogDir);

    for (Path corrupted: corruptedLogs) {
      Path p = new Path(corruptDir, corrupted.getName());
      LOG.info("Moving corrupted log " + corrupted + " to " + p);
      fs.rename(corrupted, p);
    }

    for (Path p: processedLogs) {
      Path newPath = getHLogArchivePath(oldLogDir, p);
      fs.rename(p, newPath);
      LOG.info("Archived processed log " + p + " to " + newPath);
    }
  }

  private static Path getRegionLogPath(Entry logEntry, Path rootDir) {
    Path tableDir =
      HTableDescriptor.getTableDir(rootDir, logEntry.getKey().getTablename());
    Path regionDir =
            HRegion.getRegionDir(tableDir, HRegionInfo.encodeRegionName(logEntry.getKey().getRegionName()));
    return new Path(regionDir, HREGION_OLDLOGFILE_NAME);
   }








  public void addLogActionsListerner(LogActionsListener list) {
    LOG.info("Adding a listener");
    this.actionListeners.add(list);
  }

  public boolean removeLogActionsListener(LogActionsListener list) {
    return this.actionListeners.remove(list);
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
    Configuration conf = HBaseConfiguration.create();
    FileSystem fs = FileSystem.get(conf);
    Path baseDir = new Path(conf.get(HBASE_DIR));
    Path oldLogDir = new Path(baseDir, HREGION_OLDLOGDIR_NAME);
    for (int i = 1; i < args.length; i++) {
      Path logPath = new Path(args[i]);
      if (!fs.exists(logPath)) {
        throw new FileNotFoundException(args[i] + " does not exist");
      }
      if (dump) {
        if (!fs.isFile(logPath)) {
          throw new IOException(args[i] + " is not a file");
        }
        Reader log = getReader(fs, logPath, conf);
        try {
          HLog.Entry entry;
          while ((entry = log.next()) != null) {
            System.out.println(entry.toString());
          }
        } finally {
          log.close();
        }
      } else {
        if (!fs.getFileStatus(logPath).isDir()) {
          throw new IOException(args[i] + " is not a directory");
        }
        splitLog(baseDir, logPath, oldLogDir, fs, conf);
      }
    }
  }

  public static final long FIXED_OVERHEAD = ClassSize.align(
      ClassSize.OBJECT + (5 * ClassSize.REFERENCE) +
      ClassSize.ATOMIC_INTEGER + Bytes.SIZEOF_INT + (3 * Bytes.SIZEOF_LONG));

}
