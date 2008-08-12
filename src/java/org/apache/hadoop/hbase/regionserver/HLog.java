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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Reader;

/**
 * HLog stores all the edits to the HStore.
 *
 * It performs logfile-rolling, so external callers are not aware that the
 * underlying file is being rolled.
 *
 * <p>
 * A single HLog is used by several HRegions simultaneously.
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
 * <p>
 * TODO: Vuk Ercegovac also pointed out that keeping HBase HRegion edit logs in
 * HDFS is currently flawed. HBase writes edits to logs and to a memcache. The
 * 'atomic' write to the log is meant to serve as insurance against abnormal
 * RegionServer exit: on startup, the log is rerun to reconstruct an HRegion's
 * last wholesome state. But files in HDFS do not 'exist' until they are cleanly
 * closed -- something that will not happen if RegionServer exits without
 * running its 'close'.
 */
public class HLog implements HConstants {
  private static final Log LOG = LogFactory.getLog(HLog.class);
  private static final String HLOG_DATFILE = "hlog.dat.";
  static final byte [] METACOLUMN = Bytes.toBytes("METACOLUMN:");
  static final byte [] METAROW = Bytes.toBytes("METAROW");
  final FileSystem fs;
  final Path dir;
  final Configuration conf;
  final LogRollListener listener;
  final long threadWakeFrequency;
  private final int maxlogentries;

  /*
   * Current log file.
   */
  SequenceFile.Writer writer;

  /*
   * Map of all log files but the current one. 
   */
  final SortedMap<Long, Path> outputfiles = 
    Collections.synchronizedSortedMap(new TreeMap<Long, Path>());

  /*
   * Map of region to last sequence/edit id. 
   */
  private final Map<byte [], Long> lastSeqWritten = Collections.
    synchronizedSortedMap(new TreeMap<byte [], Long>(Bytes.BYTES_COMPARATOR));

  private volatile boolean closed = false;

  private final Integer sequenceLock = new Integer(0);
  private volatile long logSeqNum = 0;

  private volatile long filenum = 0;
  private volatile long old_filenum = -1;
  
  private volatile int numEntries = 0;

  // This lock prevents starting a log roll during a cache flush.
  // synchronized is insufficient because a cache flush spans two method calls.
  private final Lock cacheFlushLock = new ReentrantLock();

  // We synchronize on updateLock to prevent updates and to prevent a log roll
  // during an update
  private final Integer updateLock = new Integer(0);

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
  public HLog(final FileSystem fs, final Path dir, final Configuration conf,
      final LogRollListener listener) throws IOException {
    this.fs = fs;
    this.dir = dir;
    this.conf = conf;
    this.listener = listener;
    this.threadWakeFrequency = conf.getLong(THREAD_WAKE_FREQUENCY, 10 * 1000);
    this.maxlogentries =
      conf.getInt("hbase.regionserver.maxlogentries", 30 * 1000);
    if (fs.exists(dir)) {
      throw new IOException("Target HLog directory already exists: " + dir);
    }
    fs.mkdirs(dir);
    rollWriter();
  }

  /*
   * Accessor for tests.
   * @return Current state of the monotonically increasing file id.
   */
  long getFilenum() {
    return this.filenum;
  }

  /**
   * Get the compression type for the hlog files.
   * @param c Configuration to use.
   * @return the kind of compression to use
   */
  private static CompressionType getCompressionType(final Configuration c) {
    String name = c.get("hbase.io.seqfile.compression.type");
    return name == null? CompressionType.NONE: CompressionType.valueOf(name);
  }

  /**
   * Called by HRegionServer when it opens a new region to ensure that log
   * sequence numbers are always greater than the latest sequence number of the
   * region being brought on-line.
   *
   * @param newvalue We'll set log edit/sequence number to this value if it
   * is greater than the current value.
   */
  void setSequenceNumber(long newvalue) {
    synchronized (sequenceLock) {
      if (newvalue > logSeqNum) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("changing sequence number from " + logSeqNum + " to " +
            newvalue);
        }
        logSeqNum = newvalue;
      }
    }
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
   * @throws IOException
   */
  public void rollWriter() throws IOException {
    this.cacheFlushLock.lock();
    try {
      if (closed) {
        return;
      }
      synchronized (updateLock) {
        if (this.writer != null) {
          // Close the current writer, get a new one.
          this.writer.close();
          Path p = computeFilename(old_filenum);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Closing current log writer " + FSUtils.getPath(p));
          }
          if (filenum > 0) {
            synchronized (this.sequenceLock) {
              this.outputfiles.put(Long.valueOf(this.logSeqNum - 1), p);
            }
          }
        }
        old_filenum = filenum;
        filenum = System.currentTimeMillis();
        Path newPath = computeFilename(filenum);
        this.writer = SequenceFile.createWriter(this.fs, this.conf, newPath,
            HLogKey.class, HLogEdit.class, getCompressionType(this.conf));
        LOG.info("New log writer created at " + FSUtils.getPath(newPath));

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
            // Get oldest edit/sequence id.  If logs are older than this id,
            // then safe to remove.
            Long oldestOutstandingSeqNum =
              Collections.min(this.lastSeqWritten.values());
            // Get the set of all log files whose final ID is older than or
            // equal to the oldest pending region operation
            TreeSet<Long> sequenceNumbers =
              new TreeSet<Long>(this.outputfiles.headMap(
                (Long.valueOf(oldestOutstandingSeqNum.longValue() + 1L))).keySet());
            // Now remove old log files (if any)
            if (LOG.isDebugEnabled()) {
              // Find region associated with oldest key -- helps debugging.
              byte [] oldestRegion = null;
              for (Map.Entry<byte [], Long> e: this.lastSeqWritten.entrySet()) {
                if (e.getValue().longValue() == oldestOutstandingSeqNum.longValue()) {
                  oldestRegion = e.getKey();
                  break;
                }
              }
              if (LOG.isDebugEnabled() && sequenceNumbers.size() > 0) {
                LOG.debug("Found " + sequenceNumbers.size() +
                  " logs to remove " +
                  "using oldest outstanding seqnum of " +
                  oldestOutstandingSeqNum + " from region " +
                  Bytes.toString(oldestRegion));
              }
            }
            if (sequenceNumbers.size() > 0) {
              for (Long seq : sequenceNumbers) {
                deleteLogFile(this.outputfiles.remove(seq), seq);
              }
            }
          }
        }
        this.numEntries = 0;
      }
    } finally {
      this.cacheFlushLock.unlock();
    }
  }
  
  private void deleteLogFile(final Path p, final Long seqno) throws IOException {
    LOG.info("removing old log file " + FSUtils.getPath(p) +
      " whose highest sequence/edit id is " + seqno);
    this.fs.delete(p, true);
  }

  /**
   * This is a convenience method that computes a new filename with a given
   * file-number.
   */
  Path computeFilename(final long fn) {
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
  void close() throws IOException {
    cacheFlushLock.lock();
    try {
      synchronized (updateLock) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("closing log writer in " + this.dir.toString());
        }
        this.writer.close();
        this.closed = true;
      }
    } finally {
      cacheFlushLock.unlock();
    }
  }

  /**
   * Append a set of edits to the log. Log edits are keyed by regionName,
   * rowname, and log-sequence-id.
   *
   * Later, if we sort by these keys, we obtain all the relevant edits for a
   * given key-range of the HRegion (TODO). Any edits that do not have a
   * matching {@link HConstants#COMPLETE_CACHEFLUSH} message can be discarded.
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
   * @param row
   * @param columns
   * @param timestamp
   * @throws IOException
   */
  void append(byte [] regionName, byte [] tableName,
      TreeMap<HStoreKey, byte[]> edits)
  throws IOException {
    if (closed) {
      throw new IOException("Cannot append; log is closed");
    }
    synchronized (updateLock) {
      long seqNum[] = obtainSeqNum(edits.size());
      // The 'lastSeqWritten' map holds the sequence number of the oldest
      // write for each region. When the cache is flushed, the entry for the
      // region being flushed is removed if the sequence number of the flush
      // is greater than or equal to the value in lastSeqWritten.
      if (!this.lastSeqWritten.containsKey(regionName)) {
        this.lastSeqWritten.put(regionName, Long.valueOf(seqNum[0]));
      }
      int counter = 0;
      for (Map.Entry<HStoreKey, byte[]> es : edits.entrySet()) {
        HStoreKey key = es.getKey();
        HLogKey logKey =
          new HLogKey(regionName, tableName, key.getRow(), seqNum[counter++]);
        HLogEdit logEdit =
          new HLogEdit(key.getColumn(), es.getValue(), key.getTimestamp());
        try {
      	  this.writer.append(logKey, logEdit);
      	} catch (IOException e) {
          LOG.fatal("Could not append. Requesting close of log", e);
          requestLogRoll();
          throw e;
      	}
        this.numEntries++;
      }
    }
    if (this.numEntries > this.maxlogentries) {
        requestLogRoll();
    }
  }

  private void requestLogRoll() {
    if (this.listener != null) {
      this.listener.logRollRequested();
    }
  }

  /** @return How many items have been added to the log */
  int getNumEntries() {
    return numEntries;
  }

  /**
   * Obtain a log sequence number.
   */
  private long obtainSeqNum() {
    long value;
    synchronized (sequenceLock) {
      value = logSeqNum++;
    }
    return value;
  }

  /** @return the number of log files in use */
  int getNumLogFiles() {
    return outputfiles.size();
  }

  /**
   * Obtain a specified number of sequence numbers
   *
   * @param num number of sequence numbers to obtain
   * @return array of sequence numbers
   */
  private long[] obtainSeqNum(int num) {
    long[] results = new long[num];
    synchronized (this.sequenceLock) {
      for (int i = 0; i < num; i++) {
        results[i] = this.logSeqNum++;
      }
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
   * @return sequence ID to pass {@link #completeCacheFlush(Text, Text, long)}
   * @see #completeCacheFlush(Text, Text, long)
   * @see #abortCacheFlush()
   */
  long startCacheFlush() {
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
  void completeCacheFlush(final byte [] regionName, final byte [] tableName,
      final long logSeqId) throws IOException {

    try {
      if (this.closed) {
        return;
      }
      synchronized (updateLock) {
        this.writer.append(new HLogKey(regionName, tableName, HLog.METAROW, logSeqId),
            new HLogEdit(HLog.METACOLUMN, HLogEdit.completeCacheFlush.get(),
                System.currentTimeMillis()));
        this.numEntries++;
        Long seq = this.lastSeqWritten.get(regionName);
        if (seq != null && logSeqId >= seq.longValue()) {
          this.lastSeqWritten.remove(regionName);
        }
      }
    } finally {
      this.cacheFlushLock.unlock();
    }
  }

  /**
   * Abort a cache flush.
   * Call if the flush fails. Note that the only recovery for an aborted flush
   * currently is a restart of the regionserver so the snapshot content dropped
   * by the failure gets restored to the memcache.
   */
  void abortCacheFlush() {
    this.cacheFlushLock.unlock();
  }

  /**
   * Split up a bunch of log files, that are no longer being written to, into
   * new files, one per region. Delete the old log files when finished.
   *
   * @param rootDir qualified root directory of the HBase instance
   * @param srcDir Directory of log files to split: e.g.
   *                <code>${ROOTDIR}/log_HOST_PORT</code>
   * @param fs FileSystem
   * @param conf HBaseConfiguration
   * @throws IOException
   */
  public static void splitLog(Path rootDir, Path srcDir, FileSystem fs,
    Configuration conf) throws IOException {
    if (!fs.exists(srcDir)) {
      // Nothing to do
      return;
    }
    FileStatus logfiles[] = fs.listStatus(srcDir);
    if (logfiles == null || logfiles.length == 0) {
      // Nothing to do
      return;
    }
    LOG.info("splitting " + logfiles.length + " log(s) in " +
      srcDir.toString());
    Map<byte [], SequenceFile.Writer> logWriters =
      new TreeMap<byte [], SequenceFile.Writer>(Bytes.BYTES_COMPARATOR);
    try {
      for (int i = 0; i < logfiles.length; i++) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Splitting " + i + " of " + logfiles.length + ": " +
            logfiles[i].getPath());
        }
        // Check for empty file.
        if (logfiles[i].getLen() <= 0) {
          LOG.info("Skipping " + logfiles[i].toString() +
              " because zero length");
          continue;
        }
        HLogKey key = new HLogKey();
        HLogEdit val = new HLogEdit();
        SequenceFile.Reader in =
          new SequenceFile.Reader(fs, logfiles[i].getPath(), conf);
        try {
          int count = 0;
          for (; in.next(key, val); count++) {
            byte [] tableName = key.getTablename();
            byte [] regionName = key.getRegionName();
            SequenceFile.Writer w = logWriters.get(regionName);
            if (w == null) {
              Path logfile = new Path(
                HRegion.getRegionDir(
                  HTableDescriptor.getTableDir(rootDir, tableName),
                  HRegionInfo.encodeRegionName(regionName)),
                HREGION_OLDLOGFILE_NAME);
              Path oldlogfile = null;
              SequenceFile.Reader old = null;
              if (fs.exists(logfile)) {
                LOG.warn("Old log file " + logfile +
                    " already exists. Copying existing file to new file");
                oldlogfile = new Path(logfile.toString() + ".old");
                fs.rename(logfile, oldlogfile);
                old = new SequenceFile.Reader(fs, oldlogfile, conf);
              }
              w = SequenceFile.createWriter(fs, conf, logfile, HLogKey.class,
                HLogEdit.class, getCompressionType(conf));
              // Use copy of regionName; regionName object is reused inside in
              // HStoreKey.getRegionName so its content changes as we iterate.
              logWriters.put(regionName, w);
              if (LOG.isDebugEnabled()) {
                LOG.debug("Creating new log file writer for path " + logfile +
                  " and region " + regionName);
              }
              
              if (old != null) {
                // Copy from existing log file
                HLogKey oldkey = new HLogKey();
                HLogEdit oldval = new HLogEdit();
                for (; old.next(oldkey, oldval); count++) {
                  if (LOG.isDebugEnabled() && count > 0 && count % 10000 == 0) {
                    LOG.debug("Copied " + count + " edits");
                  }
                  w.append(oldkey, oldval);
                }
                old.close();
                fs.delete(oldlogfile, true);
              }
            }
            w.append(key, val);
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Applied " + count + " total edits from " +
              logfiles[i].getPath().toString());
          }
        } catch (IOException e) {
          e = RemoteExceptionHandler.checkIOException(e);
          if (!(e instanceof EOFException)) {
            LOG.warn("Exception processing " + logfiles[i].getPath() +
                " -- continuing. Possible DATA LOSS!", e);
          }
        } finally {
          try {
            in.close();
          } catch (IOException e) {
            LOG.warn("Close in finally threw exception -- continuing", e);
          }
          // Delete the input file now so we do not replay edits.  We could
          // have gotten here because of an exception.  If so, probably
          // nothing we can do about it. Replaying it, it could work but we
          // could be stuck replaying for ever. Just continue though we
          // could have lost some edits.
          fs.delete(logfiles[i].getPath(), true);
        }
      }
    } finally {
      for (SequenceFile.Writer w : logWriters.values()) {
        w.close();
      }
    }

    try {
      fs.delete(srcDir, true);
    } catch (IOException e) {
      e = RemoteExceptionHandler.checkIOException(e);
      IOException io = new IOException("Cannot delete: " + srcDir);
      io.initCause(e);
      throw io;
    }
    LOG.info("log file splitting completed for " + srcDir.toString());
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
    Configuration conf = new HBaseConfiguration();
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
          HLogEdit val = new HLogEdit();
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
}
