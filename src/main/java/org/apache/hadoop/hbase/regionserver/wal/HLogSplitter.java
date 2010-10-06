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

import static org.apache.hadoop.hbase.util.FSUtils.recoverFileLease;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Entry;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Reader;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Writer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * This class is responsible for splitting up a bunch of regionserver commit log
 * files that are no longer being written to, into new files, one per region for
 * region to replay on startup. Delete the old log files when finished.
 */
public class HLogSplitter {

  private static final String LOG_SPLITTER_IMPL = "hbase.hlog.splitter.impl";

  static final Log LOG = LogFactory.getLog(HLogSplitter.class);

  /**
   * Name of file that holds recovered edits written by the wal log splitting
   * code, one per region
   */
  public static final String RECOVERED_EDITS = "recovered.edits";

  /**
   * Create a new HLogSplitter using the given {@link Configuration} and the
   * <code>hbase.hlog.splitter.impl</code> property to derived the instance
   * class to use.
   * 
   * @param conf
   * @return New HLogSplitter instance
   */
  public static HLogSplitter createLogSplitter(Configuration conf) {
    @SuppressWarnings("unchecked")
    Class<? extends HLogSplitter> splitterClass = (Class<? extends HLogSplitter>) conf
        .getClass(LOG_SPLITTER_IMPL, HLogSplitter.class);
    try {
      return splitterClass.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
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

  /**
   * Split up a bunch of regionserver commit log files that are no longer being
   * written to, into new files, one per region for region to replay on startup.
   * Delete the old log files when finished.
   * 
   * @param rootDir
   *          qualified root directory of the HBase instance
   * @param srcDir
   *          Directory of log files to split: e.g.
   *          <code>${ROOTDIR}/log_HOST_PORT</code>
   * @param oldLogDir
   *          directory where processed (split) logs will be archived to
   * @param fs
   *          FileSystem
   * @param conf
   *          Configuration
   * @throws IOException
   *           will throw if corrupted hlogs aren't tolerated
   * @return the list of splits
   */
  public List<Path> splitLog(final Path rootDir, final Path srcDir,
      Path oldLogDir, final FileSystem fs, final Configuration conf)
      throws IOException {

    long millis = System.currentTimeMillis();
    List<Path> splits = null;
    if (!fs.exists(srcDir)) {
      // Nothing to do
      return splits;
    }
    FileStatus[] logfiles = fs.listStatus(srcDir);
    if (logfiles == null || logfiles.length == 0) {
      // Nothing to do
      return splits;
    }
    LOG.info("Splitting " + logfiles.length + " hlog(s) in "
        + srcDir.toString());
    splits = splitLog(rootDir, srcDir, oldLogDir, logfiles, fs, conf);
    try {
      FileStatus[] files = fs.listStatus(srcDir);
      for (FileStatus file : files) {
        Path newPath = HLog.getHLogArchivePath(oldLogDir, file.getPath());
        LOG.info("Moving " + FSUtils.getPath(file.getPath()) + " to "
            + FSUtils.getPath(newPath));
        if (!fs.rename(file.getPath(), newPath)) {
          throw new IOException("Unable to rename " + file.getPath() +
            " to " + newPath);
        }
      }
      LOG.debug("Moved " + files.length + " log files to "
          + FSUtils.getPath(oldLogDir));
      if (!fs.delete(srcDir, true)) {
        throw new IOException("Unable to delete " + srcDir);
      }
    } catch (IOException e) {
      e = RemoteExceptionHandler.checkIOException(e);
      IOException io = new IOException("Cannot delete: " + srcDir);
      io.initCause(e);
      throw io;
    }
    long endMillis = System.currentTimeMillis();
    LOG.info("hlog file splitting completed in " + (endMillis - millis)
        + " millis for " + srcDir.toString());
    return splits;
  }
  
  /**
   * Sorts the HLog edits in the given list of logfiles (that are a mix of edits
   * on multiple regions) by region and then splits them per region directories,
   * in batches of (hbase.hlog.split.batch.size)
   * 
   * A batch consists of a set of log files that will be sorted in a single map
   * of edits indexed by region the resulting map will be concurrently written
   * by multiple threads to their corresponding regions
   * 
   * Each batch consists of more more log files that are - recovered (files is
   * opened for append then closed to ensure no process is writing into it) -
   * parsed (each edit in the log is appended to a list of edits indexed by
   * region see {@link #parseHLog} for more details) - marked as either
   * processed or corrupt depending on parsing outcome - the resulting edits
   * indexed by region are concurrently written to their corresponding region
   * region directories - original files are then archived to a different
   * directory
   * 
   * 
   * 
   * @param rootDir
   *          hbase directory
   * @param srcDir
   *          logs directory
   * @param oldLogDir
   *          directory where processed logs are archived to
   * @param logfiles
   *          the list of log files to split
   * @param fs
   * @param conf
   * @return
   * @throws IOException
   */
  private List<Path> splitLog(final Path rootDir, final Path srcDir,
      Path oldLogDir, final FileStatus[] logfiles, final FileSystem fs,
      final Configuration conf) throws IOException {
    List<Path> processedLogs = new ArrayList<Path>();
    List<Path> corruptedLogs = new ArrayList<Path>();
    final Map<byte[], WriterAndPath> logWriters = Collections
        .synchronizedMap(new TreeMap<byte[], WriterAndPath>(
            Bytes.BYTES_COMPARATOR));
    List<Path> splits = null;

    // Number of logs in a read batch
    // More means faster but bigger mem consumption
    // TODO make a note on the conf rename and update hbase-site.xml if needed
    int logFilesPerStep = conf.getInt("hbase.hlog.split.batch.size", 3);
    boolean skipErrors = conf.getBoolean("hbase.hlog.split.skip.errors", false);

    try {
      int i = -1;
      while (i < logfiles.length) {
        final Map<byte[], LinkedList<Entry>> editsByRegion = new TreeMap<byte[], LinkedList<Entry>>(
            Bytes.BYTES_COMPARATOR);
        for (int j = 0; j < logFilesPerStep; j++) {
          i++;
          if (i == logfiles.length) {
            break;
          }
          FileStatus log = logfiles[i];
          Path logPath = log.getPath();
          long logLength = log.getLen();
          LOG.debug("Splitting hlog " + (i + 1) + " of " + logfiles.length
              + ": " + logPath + ", length=" + logLength);
          try {
            recoverFileLease(fs, logPath, conf);
            parseHLog(log, editsByRegion, fs, conf);
            processedLogs.add(logPath);
          } catch (EOFException eof) {
              // truncated files are expected if a RS crashes (see HBASE-2643)
              LOG.info("EOF from hlog " + logPath + ".  continuing");
              processedLogs.add(logPath);
          } catch (IOException e) {
            if (skipErrors) {
              LOG.warn("Got while parsing hlog " + logPath
                  + ". Marking as corrupted", e);
              corruptedLogs.add(logPath);
            } else {
              throw e;
            }
          }
        }
        writeEditsBatchToRegions(editsByRegion, logWriters, rootDir, fs, conf);
      }
      if (fs.listStatus(srcDir).length > processedLogs.size()
          + corruptedLogs.size()) {
        throw new IOException("Discovered orphan hlog after split. Maybe "
            + "HRegionServer was not dead when we started");
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
   * Takes splitLogsMap and concurrently writes them to region directories using a thread pool
   *
   * @param splitLogsMap map that contains the log splitting result indexed by region
   * @param logWriters map that contains a writer per region
   * @param rootDir hbase root dir
   * @param fs
   * @param conf
   * @throws IOException
   */
  private void writeEditsBatchToRegions(
      final Map<byte[], LinkedList<Entry>> splitLogsMap,
      final Map<byte[], WriterAndPath> logWriters, final Path rootDir,
      final FileSystem fs, final Configuration conf) 
	throws IOException {
    // Number of threads to use when log splitting to rewrite the logs.
    // More means faster but bigger mem consumption.
    int logWriterThreads = conf.getInt(
        "hbase.regionserver.hlog.splitlog.writer.threads", 3);
    boolean skipErrors = conf.getBoolean("hbase.skip.errors", false);
    HashMap<byte[], Future> writeFutureResult = new HashMap<byte[], Future>();
    ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
    builder.setNameFormat("SplitWriter-%1$d");
    ThreadFactory factory = builder.build();
    ThreadPoolExecutor threadPool =
      (ThreadPoolExecutor)Executors.newFixedThreadPool(logWriterThreads, factory);
    for (final byte[] region : splitLogsMap.keySet()) {
      Callable splitter = createNewSplitter(rootDir, logWriters, splitLogsMap,
          region, fs, conf);
      writeFutureResult.put(region, threadPool.submit(splitter));
    }

    threadPool.shutdown();
    // Wait for all threads to terminate
    try {
      for (int j = 0; !threadPool.awaitTermination(5, TimeUnit.SECONDS); j++) {
        String message = "Waiting for hlog writers to terminate, elapsed " + j
            * 5 + " seconds";
        if (j < 30) {
          LOG.debug(message);
        } else {
          LOG.info(message);
        }

      }
    } catch (InterruptedException ex) {
      LOG.warn("Hlog writers were interrupted, possible data loss!");
      if (!skipErrors) {
        throw new IOException("Could not finish writing log entries", ex);
        // TODO maybe we should fail here regardless if skipErrors is active or not
      }
    }

    for (Map.Entry<byte[], Future> entry : writeFutureResult.entrySet()) {
      try {
        entry.getValue().get();
      } catch (ExecutionException e) {
        throw (new IOException(e.getCause()));
      } catch (InterruptedException e1) {
        LOG.warn("Writer for region " + Bytes.toString(entry.getKey())
            + " was interrupted, however the write process should have "
            + "finished. Throwing up ", e1);
        throw (new IOException(e1.getCause()));
      }
    }
  }
  
  /**
   * Moves processed logs to a oldLogDir after successful processing Moves
   * corrupted logs (any log that couldn't be successfully parsed to corruptDir
   * (.corrupt) for later investigation
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
      final FileSystem fs, final Configuration conf) throws IOException {
    final Path corruptDir = new Path(conf.get(HConstants.HBASE_DIR), conf.get(
        "hbase.regionserver.hlog.splitlog.corrupt.dir", ".corrupt"));

    if (!fs.mkdirs(corruptDir)) {
      LOG.info("Unable to mkdir " + corruptDir);
    }
    fs.mkdirs(oldLogDir);

    for (Path corrupted : corruptedLogs) {
      Path p = new Path(corruptDir, corrupted.getName());
      if (!fs.rename(corrupted, p)) { 
        LOG.info("Unable to move corrupted log " + corrupted + " to " + p);
      } else {
        LOG.info("Moving corrupted log " + corrupted + " to " + p);
      }
    }

    for (Path p : processedLogs) {
      Path newPath = HLog.getHLogArchivePath(oldLogDir, p);
      if (!fs.rename(p, newPath)) {
        LOG.info("Unable to move  " + p + " to " + newPath);
      } else {
        LOG.info("Archived processed log " + p + " to " + newPath);
      }
    }
  }

  /**
   * Path to a file under RECOVERED_EDITS_DIR directory of the region found in
   * <code>logEntry</code> named for the sequenceid in the passed
   * <code>logEntry</code>: e.g. /hbase/some_table/2323432434/recovered.edits/2332.
   * This method also ensures existence of RECOVERED_EDITS_DIR under the region
   * creating it if necessary.
   * @param fs
   * @param logEntry
   * @param rootDir HBase root dir.
   * @return Path to file into which to dump split log edits.
   * @throws IOException
   */
  static Path getRegionSplitEditsPath(final FileSystem fs,
      final Entry logEntry, final Path rootDir) throws IOException {
    Path tableDir = HTableDescriptor.getTableDir(rootDir, logEntry.getKey()
        .getTablename());
    Path regiondir = HRegion.getRegionDir(tableDir,
        Bytes.toString(logEntry.getKey().getEncodedRegionName()));
    Path dir = HLog.getRegionDirRecoveredEditsDir(regiondir);
    if (!fs.exists(dir)) {
      if (!fs.mkdirs(dir)) LOG.warn("mkdir failed on " + dir);
    }
    return new Path(dir, formatRecoveredEditsFileName(logEntry.getKey()
        .getLogSeqNum()));
  }

  static String formatRecoveredEditsFileName(final long seqid) {
    return String.format("%019d", seqid);
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
  private void parseHLog(final FileStatus logfile,
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
      in = getReader(fs, path, conf);
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
        byte[] region = entry.getKey().getEncodedRegionName();
        LinkedList<Entry> queue = splitLogsMap.get(region);
        if (queue == null) {
          queue = new LinkedList<Entry>();
          splitLogsMap.put(region, queue);
        }
        queue.addLast(entry);
        editsCount++;
      }
    } finally {
      LOG.debug("Pushed=" + editsCount + " entries from " + path);
      try {
        if (in != null) {
          in.close();
        }
      } catch (IOException e) {
        LOG
            .warn("Close log reader in finally threw exception -- continuing",
                e);
      }
    }
  }
  
  private Callable<Void> createNewSplitter(final Path rootDir,
      final Map<byte[], WriterAndPath> logWriters,
      final Map<byte[], LinkedList<Entry>> logEntries, final byte[] region,
      final FileSystem fs, final Configuration conf) {
    return new Callable<Void>() {
      public String getName() {
        return "Split writer thread for region " + Bytes.toStringBinary(region);
      }

      @Override
      public Void call() throws IOException {
        LinkedList<Entry> entries = logEntries.get(region);
        LOG.debug(this.getName() + " got " + entries.size() + " to process");
        long threadTime = System.currentTimeMillis();
        try {
          int editsCount = 0;
          WriterAndPath wap = logWriters.get(region);
          for (Entry logEntry : entries) {
            if (wap == null) {
              Path regionedits = getRegionSplitEditsPath(fs, logEntry, rootDir);
              if (fs.exists(regionedits)) {
                LOG.warn("Found existing old edits file. It could be the "
                    + "result of a previous failed split attempt. Deleting "
                    + regionedits + ", length="
                    + fs.getFileStatus(regionedits).getLen());
                if (!fs.delete(regionedits, false)) {
                  LOG.warn("Failed delete of old " + regionedits);
                }
              }
              Writer w = createWriter(fs, regionedits, conf);
              wap = new WriterAndPath(regionedits, w);
              logWriters.put(region, wap);
              LOG.debug("Creating writer path=" + regionedits + " region="
                  + Bytes.toStringBinary(region));
            }
            wap.w.append(logEntry);
            editsCount++;
          }
          LOG.debug(this.getName() + " Applied " + editsCount
              + " total edits to " + Bytes.toStringBinary(region) + " in "
              + (System.currentTimeMillis() - threadTime) + "ms");
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
   * Create a new {@link Writer} for writing log splits.
   * 
   * @param fs
   * @param logfile
   * @param conf
   * @return A new Writer instance
   * @throws IOException
   */
  protected Writer createWriter(FileSystem fs, Path logfile, Configuration conf)
      throws IOException {
    return HLog.createWriter(fs, logfile, conf);
  }

  /**
   * Create a new {@link Reader} for reading logs to split.
   * 
   * @param fs
   * @param curLogFile
   * @param conf
   * @return A new Reader instance
   * @throws IOException
   */
  protected Reader getReader(FileSystem fs, Path curLogFile, Configuration conf)
      throws IOException {
    return HLog.getReader(fs, curLogFile, conf);
  }

}
