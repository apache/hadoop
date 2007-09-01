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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * HLog stores all the edits to the HStore.
 * 
 * It performs logfile-rolling, so external callers are not aware that the 
 * underlying file is being rolled.
 *
 * <p>A single HLog is used by several HRegions simultaneously.
 * 
 * <p>Each HRegion is identified by a unique long <code>int</code>. HRegions do
 * not need to declare themselves before using the HLog; they simply include
 * their HRegion-id in the <code>append</code> or 
 * <code>completeCacheFlush</code> calls.
 *
 * <p>An HLog consists of multiple on-disk files, which have a chronological
 * order. As data is flushed to other (better) on-disk structures, the log
 * becomes obsolete.  We can destroy all the log messages for a given
 * HRegion-id up to the most-recent CACHEFLUSH message from that HRegion.
 *
 * <p>It's only practical to delete entire files.  Thus, we delete an entire 
 * on-disk file F when all of the messages in F have a log-sequence-id that's 
 * older (smaller) than the most-recent CACHEFLUSH message for every HRegion 
 * that has a message in F.
 * 
 * <p>TODO: Vuk Ercegovac also pointed out that keeping HBase HRegion edit logs
 * in HDFS is currently flawed. HBase writes edits to logs and to a memcache.
 * The 'atomic' write to the log is meant to serve as insurance against
 * abnormal RegionServer exit: on startup, the log is rerun to reconstruct an
 * HRegion's last wholesome state. But files in HDFS do not 'exist' until they
 * are cleanly closed -- something that will not happen if RegionServer exits
 * without running its 'close'.
 */
public class HLog implements HConstants {
  private static final Log LOG = LogFactory.getLog(HLog.class);
  
  static final String HLOG_DATFILE = "hlog.dat.";
  static final Text METACOLUMN = new Text("METACOLUMN:");
  static final Text METAROW = new Text("METAROW");

  FileSystem fs;
  Path dir;
  Configuration conf;

  SequenceFile.Writer writer;
  TreeMap<Long, Path> outputfiles = new TreeMap<Long, Path>();
  volatile boolean insideCacheFlush = false;

  TreeMap<Text, Long> regionToLastFlush = new TreeMap<Text, Long>();

  volatile boolean closed = false;
  volatile long logSeqNum = 0;
  long filenum = 0;
  AtomicInteger numEntries = new AtomicInteger(0);

  Integer rollLock = new Integer(0);

  /**
   * Split up a bunch of log files, that are no longer being written to,
   * into new files, one per region.  Delete the old log files when ready.
   * @param rootDir Root directory of the HBase instance
   * @param srcDir Directory of log files to split:
   * e.g. <code>${ROOTDIR}/log_HOST_PORT</code>
   * @param fs FileSystem
   * @param conf HBaseConfiguration
   * @throws IOException
   */
  static void splitLog(Path rootDir, Path srcDir, FileSystem fs,
    Configuration conf) throws IOException {
    Path logfiles[] = fs.listPaths(new Path[] {srcDir});
    LOG.info("splitting " + logfiles.length + " log(s) in " +
      srcDir.toString());
    HashMap<Text, SequenceFile.Writer> logWriters =
      new HashMap<Text, SequenceFile.Writer>();
    try {
      for(int i = 0; i < logfiles.length; i++) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Splitting " + logfiles[i]);
        }
        // Check for empty file.
        if (fs.getFileStatus(logfiles[i]).getLen() <= 0) {
          LOG.warn("Skipping " + logfiles[i].toString() +
            " because zero length");
          continue;
        }
        SequenceFile.Reader in =
          new SequenceFile.Reader(fs, logfiles[i], conf);
        try {
          HLogKey key = new HLogKey();
          HLogEdit val = new HLogEdit();
          while(in.next(key, val)) {
            Text regionName = key.getRegionName();
            SequenceFile.Writer w = logWriters.get(regionName);
            if (w == null) {
              Path logfile = new Path(HRegion.getRegionDir(rootDir,
                  regionName), HREGION_OLDLOGFILE_NAME);
              if (LOG.isDebugEnabled()) {
                LOG.debug("getting new log file writer for path " + logfile);
              }
              w = SequenceFile.createWriter(fs, conf, logfile, HLogKey.class,
                  HLogEdit.class);
              logWriters.put(regionName, w);
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug("Edit " + key.toString() + "=" + val.toString());
            }
            w.append(key, val);
          }
        } finally {
          in.close();
        }
      }
    } finally {
      for (SequenceFile.Writer w: logWriters.values()) {
        w.close();
      }
    }
    
    if(fs.exists(srcDir)) {
      if(! fs.delete(srcDir)) {
        LOG.error("Cannot delete: " + srcDir);
        if(! FileUtil.fullyDelete(new File(srcDir.toString()))) {
          throw new IOException("Cannot delete: " + srcDir);
        }
      }
    }
    LOG.info("log file splitting completed for " + srcDir.toString());
  }

  /**
   * Create an edit log at the given <code>dir</code> location.
   *
   * You should never have to load an existing log.  If there is a log
   * at startup, it should have already been processed and deleted by 
   * the time the HLog object is started up.
   * 
   * @param fs
   * @param dir
   * @param conf
   * @throws IOException
   */
  HLog(FileSystem fs, Path dir, Configuration conf) throws IOException {
    this.fs = fs;
    this.dir = dir;
    this.conf = conf;

    if (fs.exists(dir)) {
      throw new IOException("Target HLog directory already exists: " + dir);
    }
    fs.mkdirs(dir);
    rollWriter();
  }
  
  synchronized void setSequenceNumber(long newvalue) {
    if (newvalue > logSeqNum) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("changing sequence number from " + logSeqNum + " to " +
            newvalue);
      }
      logSeqNum = newvalue;
    }
  }

  /**
   * Roll the log writer.  That is, start writing log messages to a new file.
   *
   * The 'rollLock' prevents us from entering rollWriter() more than
   * once at a time.
   *
   * The 'this' lock limits access to the current writer so
   * we don't append multiple items simultaneously.
   * 
   * @throws IOException
   */
  void rollWriter() throws IOException {
    synchronized(rollLock) {

      // Try to roll the writer to a new file.  We may have to
      // wait for a cache-flush to complete.  In the process,
      // compute a list of old log files that can be deleted.

      Vector<Path> toDeleteList = new Vector<Path>();
      synchronized(this) {
        if(closed) {
          throw new IOException("Cannot roll log; log is closed");
        }

        // Make sure we do not roll the log while inside a
        // cache-flush.  Otherwise, the log sequence number for
        // the CACHEFLUSH operation will appear in a "newer" log file
        // than it should.
        
        while(insideCacheFlush) {
          try {
            wait();
          } catch (InterruptedException ie) {
            // continue;
          }
        }

        // Close the current writer (if any), and grab a new one.
        if(writer != null) {
          writer.close();
          Path p = computeFilename(filenum - 1);
          if(LOG.isDebugEnabled()) {
            LOG.debug("Closing current log writer " + p.toString() +
              " to get a new one");
          }
          if (filenum > 0) {
            outputfiles.put(logSeqNum - 1, p);
          }
        }
        Path newPath = computeFilename(filenum++);
        this.writer = SequenceFile.createWriter(fs, conf, newPath,
          HLogKey.class, HLogEdit.class);
        if(LOG.isDebugEnabled()) {
          LOG.debug("new log writer created at " + newPath);
        }
        
        // Can we delete any of the old log files?
        // First, compute the oldest relevant log operation 
        // over all the regions.

        long oldestOutstandingSeqNum = Long.MAX_VALUE;
        for(Long l: regionToLastFlush.values()) {
          long curSeqNum = l.longValue();
          
          if(curSeqNum < oldestOutstandingSeqNum) {
            oldestOutstandingSeqNum = curSeqNum;
          }
        }

        // Next, remove all files with a final ID that's older
        // than the oldest pending region-operation.
        for(Iterator<Long> it = outputfiles.keySet().iterator(); it.hasNext();) {
          long maxSeqNum = it.next().longValue();
          if(maxSeqNum < oldestOutstandingSeqNum) {
            Path p = outputfiles.get(maxSeqNum);
            it.remove();
            toDeleteList.add(p);
            
          } else {
            break;
          }
        }
      }

      // Actually delete them, if any!
      for(Iterator<Path> it = toDeleteList.iterator(); it.hasNext(); ) {
        Path p = it.next();
        if(LOG.isDebugEnabled()) {
          LOG.debug("removing old log file " + p.toString());
        }
        fs.delete(p);
      }
      this.numEntries.set(0);
    }
  }

  /**
   * This is a convenience method that computes a new filename with
   * a given file-number.
   */
  Path computeFilename(final long fn) {
    return new Path(dir, HLOG_DATFILE + String.format("%1$03d", fn));
  }

  /**
   * Shut down the log and delete the log directory
   * @throws IOException
   */
  synchronized void closeAndDelete() throws IOException {
    close();
    fs.delete(dir);
  }
  
  /**
   * Shut down the log.
   * @throws IOException
   */
  synchronized void close() throws IOException {
    if(LOG.isDebugEnabled()) {
      LOG.debug("closing log writer in " + this.dir.toString());
    }
    this.writer.close();
    this.closed = true;
  }

  /**
   * Append a set of edits to the log. Log edits are keyed by regionName,
   * rowname, and log-sequence-id.
   *
   * Later, if we sort by these keys, we obtain all the relevant edits for
   * a given key-range of the HRegion (TODO).  Any edits that do not have a
   * matching {@link HConstants#COMPLETE_CACHEFLUSH} message can be discarded.
   *
   * <p>Logs cannot be restarted once closed, or once the HLog process dies.
   * Each time the HLog starts, it must create a new log.  This means that
   * other systems should process the log appropriately upon each startup
   * (and prior to initializing HLog).
   *
   * We need to seize a lock on the writer so that writes are atomic.
   * @param regionName
   * @param tableName
   * @param row
   * @param columns
   * @param timestamp
   * @throws IOException
   */
  synchronized void append(Text regionName, Text tableName, Text row,
      TreeMap<Text, byte []> columns, long timestamp)
  throws IOException {
    if(closed) {
      throw new IOException("Cannot append; log is closed");
    }
    
    long seqNum[] = obtainSeqNum(columns.size());

    // The 'regionToLastFlush' map holds the sequence id of the
    // most recent flush for every regionName.  However, for regions
    // that don't have any flush yet, the relevant operation is the
    // first one that's been added.
    if (regionToLastFlush.get(regionName) == null) {
      regionToLastFlush.put(regionName, seqNum[0]);
    }

    int counter = 0;
    for (Map.Entry<Text, byte []> es: columns.entrySet()) {
      HLogKey logKey =
        new HLogKey(regionName, tableName, row, seqNum[counter++]);
      HLogEdit logEdit = new HLogEdit(es.getKey(), es.getValue(), timestamp);
      writer.append(logKey, logEdit);
      numEntries.getAndIncrement();
    }
  }

  /** @return How many items have been added to the log */
  int getNumEntries() {
    return numEntries.get();
  }

  /**
   * Obtain a log sequence number.  This seizes the whole HLog
   * lock, but it shouldn't last too long.
   */
  synchronized long obtainSeqNum() {
    return logSeqNum++;
  }
  
  /**
   * Obtain a specified number of sequence numbers
   * 
   * @param num - number of sequence numbers to obtain
   * @return - array of sequence numbers
   */
  synchronized long[] obtainSeqNum(int num) {
    long[] results = new long[num];
    for (int i = 0; i < num; i++) {
      results[i] = logSeqNum++;
    }
    return results;
  }

  /**
   * By acquiring a log sequence ID, we can allow log messages
   * to continue while we flush the cache.
   *
   * Set a flag so that we do not roll the log between the start
   * and complete of a cache-flush.  Otherwise the log-seq-id for
   * the flush will not appear in the correct logfile.
   * @return sequence ID to pass {@link #completeCacheFlush(Text, Text, long)}
   * @see #completeCacheFlush(Text, Text, long)
   */
  synchronized long startCacheFlush() {
    while (insideCacheFlush) {
      try {
        wait();
      } catch (InterruptedException ie) {
        // continue
      }
    }
    insideCacheFlush = true;
    notifyAll();
    return obtainSeqNum();
  }

  /** Complete the cache flush
   * @param regionName
   * @param tableName
   * @param logSeqId
   * @throws IOException
   */
  synchronized void completeCacheFlush(final Text regionName,
    final Text tableName, final long logSeqId)
  throws IOException {
    if(closed) {
      return;
    }
    
    if(! insideCacheFlush) {
      throw new IOException("Impossible situation: inside " +
        "completeCacheFlush(), but 'insideCacheFlush' flag is false");
    }
    
    writer.append(new HLogKey(regionName, tableName, HLog.METAROW, logSeqId),
      new HLogEdit(HLog.METACOLUMN, HGlobals.completeCacheFlush.get(),
        System.currentTimeMillis()));
    numEntries.getAndIncrement();

    // Remember the most-recent flush for each region.
    // This is used to delete obsolete log files.
    regionToLastFlush.put(regionName, logSeqId);

    insideCacheFlush = false;
    notifyAll();
  }

  private static void usage() {
    System.err.println("Usage: java org.apache.hbase.HLog" +
        " {--dump <logfile>... | --split <logdir>...}");
  }
  
  /**
   * Pass one or more log file names and it will either dump out a text version
   * on <code>stdout</code> or split the specified log files.
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
    Path baseDir = new Path(conf.get(HBASE_DIR, DEFAULT_HBASE_DIR));
    
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
          while(log.next(key, val)) {
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
