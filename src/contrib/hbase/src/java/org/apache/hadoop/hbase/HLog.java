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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;

import java.io.*;
import java.util.*;

/**
 * HLog stores all the edits to the HStore.
 * 
 * It performs logfile-rolling, so external callers are not aware that the 
 * underlying file is being rolled.
 *
 * <p>A single HLog is used by several HRegions simultaneously.
 * 
 * <p>Each HRegion is identified by a unique long int. HRegions do not need to
 * declare themselves before using the HLog; they simply include their
 * HRegion-id in the {@link #append(Text, Text, Text, TreeMap, long)} or 
 * {@link #completeCacheFlush(Text, Text, long)} calls.
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
  boolean insideCacheFlush = false;

  TreeMap<Text, Long> regionToLastFlush = new TreeMap<Text, Long>();

  boolean closed = false;
  transient long logSeqNum = 0;
  long filenum = 0;
  transient int numEntries = 0;

  Integer rollLock = 0;

  /**
   * Bundle up a bunch of log files (which are no longer being written to),
   * into a new file.  Delete the old log files when ready.
   * @param srcDir Directory of log files to bundle:
   * e.g. <code>${REGIONDIR}/log_HOST_PORT</code>
   * @param dstFile Destination file:
   * e.g. <code>${REGIONDIR}/oldlogfile_HOST_PORT</code>
   * @param fs FileSystem
   * @param conf HBaseConfiguration
   * @throws IOException
   */
  public static void consolidateOldLog(Path srcDir, Path dstFile,
      FileSystem fs, Configuration conf)
  throws IOException {
    if(LOG.isDebugEnabled()) {
      LOG.debug("consolidating log files");
    }
    
    Path logfiles[] = fs.listPaths(srcDir);
    SequenceFile.Writer newlog = SequenceFile.createWriter(fs, conf, dstFile,
        HLogKey.class, HLogEdit.class);
    try {
      for(int i = 0; i < logfiles.length; i++) {
        SequenceFile.Reader in =
          new SequenceFile.Reader(fs, logfiles[i], conf);
        try {
          HLogKey key = new HLogKey();
          HLogEdit val = new HLogEdit();
          while(in.next(key, val)) {
            newlog.append(key, val);
          }
          
        } finally {
          in.close();
        }
      }
      
    } finally {
      newlog.close();
    }
    
    if(fs.exists(srcDir)) {
      
      if(! fs.delete(srcDir)) {
        LOG.error("Cannot delete: " + srcDir);
        
        if(! FileUtil.fullyDelete(new File(srcDir.toString()))) {
          throw new IOException("Cannot delete: " + srcDir);
        }
      }
    }
    if(LOG.isDebugEnabled()) {
      LOG.debug("log file consolidation completed");
    }
  }

  /**
   * Create an edit log at the given <code>dir</code> location.
   *
   * You should never have to load an existing log.  If there is a log
   * at startup, it should have already been processed and deleted by 
   * the time the HLog object is started up.
   */
  public HLog(FileSystem fs, Path dir, Configuration conf) throws IOException {
    this.fs = fs;
    this.dir = dir;
    this.conf = conf;

    if (fs.exists(dir)) {
      throw new IOException("Target HLog directory already exists: " + dir);
    }
    fs.mkdirs(dir);
    rollWriter();
  }

  /**
   * Roll the log writer.  That is, start writing log messages to a new file.
   *
   * The 'rollLock' prevents us from entering rollWriter() more than
   * once at a time.
   *
   * The 'this' lock limits access to the current writer so
   * we don't append multiple items simultaneously.
   */
  public void rollWriter() throws IOException {
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
          }
        }
        
        if(LOG.isDebugEnabled()) {
          LOG.debug("closing current log writer and getting a new one");
        }

        // Close the current writer (if any), and grab a new one.
        
        if(writer != null) {
          writer.close();
          
          if(filenum > 0) {
            outputfiles.put(logSeqNum - 1, computeFilename(filenum - 1));
          }
        }
        
        Path newPath = computeFilename(filenum++);
        this.writer = SequenceFile.createWriter(fs, conf, newPath, HLogKey.class, HLogEdit.class);

        if(LOG.isDebugEnabled()) {
          LOG.debug("new log writer created");
        }
        
        // Can we delete any of the old log files?
        // First, compute the oldest relevant log operation 
        // over all the regions.

        long oldestOutstandingSeqNum = Long.MAX_VALUE;
        for(Iterator<Long> it = regionToLastFlush.values().iterator(); it.hasNext(); ) {
          long curSeqNum = it.next().longValue();
          
          if(curSeqNum < oldestOutstandingSeqNum) {
            oldestOutstandingSeqNum = curSeqNum;
          }
        }

        // Next, remove all files with a final ID that's older
        // than the oldest pending region-operation.

        if(LOG.isDebugEnabled()) {
          LOG.debug("removing old log files");
        }
        
        for(Iterator<Long> it = outputfiles.keySet().iterator(); it.hasNext(); ) {
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
        fs.delete(p);
      }

      if(LOG.isDebugEnabled()) {
        LOG.debug("old log files deleted");
      }
      
      this.numEntries = 0;
    }
  }

  /**
   * This is a convenience method that computes a new filename with
   * a given file-number.
   */
  Path computeFilename(long filenum) {
    return new Path(dir, HLOG_DATFILE + String.format("%1$03d", filenum));
  }

  /** Shut down the log. */
  public synchronized void close() throws IOException {
    if(LOG.isDebugEnabled()) {
      LOG.debug("closing log writer");
    }
    this.writer.close();
    this.closed = true;
    if(LOG.isDebugEnabled()) {
      LOG.debug("log writer closed");
    }
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
  public synchronized void append(Text regionName, Text tableName, Text row,
      TreeMap<Text, BytesWritable> columns, long timestamp)
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
    for (Map.Entry<Text, BytesWritable> es: columns.entrySet()) {
      HLogKey logKey =
        new HLogKey(regionName, tableName, row, seqNum[counter++]);
      HLogEdit logEdit = new HLogEdit(es.getKey(), es.getValue(), timestamp);
      writer.append(logKey, logEdit);
      numEntries++;
    }
  }

  /** How many items have been added to the log? */
  public int getNumEntries() {
    return numEntries;
  }

  /**
   * Obtain a log sequence number.  This seizes the whole HLog
   * lock, but it shouldn't last too long.
   */
  synchronized long obtainSeqNum() {
    return logSeqNum++;
  }
  
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
  public synchronized long startCacheFlush() {
    while (insideCacheFlush) {
      try {
        wait();
      } catch (InterruptedException ie) {
      }
    }
    insideCacheFlush = true;
    notifyAll();
    return obtainSeqNum();
  }

  /** Complete the cache flush */
  public synchronized void completeCacheFlush(final Text regionName,
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
      new HLogEdit(HLog.METACOLUMN, COMPLETE_CACHEFLUSH,
        System.currentTimeMillis()));
    numEntries++;

    // Remember the most-recent flush for each region.
    // This is used to delete obsolete log files.
    regionToLastFlush.put(regionName, logSeqId);

    insideCacheFlush = false;
    notifyAll();
  }

  /**
   * Pass a log file and it will dump out a text version on
   * <code>stdout</code>.
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.err.println("Usage: java org.apache.hbase.HLog <logfile>");
      System.exit(-1);
    }
    Configuration conf = new HBaseConfiguration();
    FileSystem fs = FileSystem.get(conf);
    Path logfile = new Path(args[0]);
    if (!fs.exists(logfile)) {
      throw new FileNotFoundException(args[0] + " does not exist");
    }
    if (!fs.isFile(logfile)) {
      throw new IOException(args[0] + " is not a file");
    }
    Reader log = new SequenceFile.Reader(fs, logfile, conf);
    try {
      HLogKey key = new HLogKey();
      HLogEdit val = new HLogEdit();
      while(log.next(key, val)) {
        System.out.println(key.toString() + " " + val.toString());
      }
    } finally {
      log.close();
    }
  }
}
