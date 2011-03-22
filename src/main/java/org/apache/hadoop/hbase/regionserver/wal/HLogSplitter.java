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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Entry;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Reader;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Writer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.io.MultipleIOException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * This class is responsible for splitting up a bunch of regionserver commit log
 * files that are no longer being written to, into new files, one per region for
 * region to replay on startup. Delete the old log files when finished.
 */
public class HLogSplitter {

  private static final String LOG_SPLITTER_IMPL = "hbase.hlog.splitter.impl";

  /**
   * Name of file that holds recovered edits written by the wal log splitting
   * code, one per region
   */
  public static final String RECOVERED_EDITS = "recovered.edits";

  
  static final Log LOG = LogFactory.getLog(HLogSplitter.class);

  private boolean hasSplit = false;
  private long splitTime = 0;
  private long splitSize = 0;


  // Parameters for split process
  protected final Path rootDir;
  protected final Path srcDir;
  protected final Path oldLogDir;
  protected final FileSystem fs;
  protected final Configuration conf;
  
  // Major subcomponents of the split process.
  // These are separated into inner classes to make testing easier.
  OutputSink outputSink;
  EntryBuffers entryBuffers;

  // If an exception is thrown by one of the other threads, it will be
  // stored here.
  protected AtomicReference<Throwable> thrown = new AtomicReference<Throwable>();

  // Wait/notify for when data has been produced by the reader thread,
  // consumed by the reader thread, or an exception occurred
  Object dataAvailable = new Object();

  
  /**
   * Create a new HLogSplitter using the given {@link Configuration} and the
   * <code>hbase.hlog.splitter.impl</code> property to derived the instance
   * class to use.
   *
   * @param conf
   * @param rootDir hbase directory
   * @param srcDir logs directory
   * @param oldLogDir directory where processed logs are archived to
   * @param fs FileSystem
   */
  public static HLogSplitter createLogSplitter(Configuration conf,
      final Path rootDir, final Path srcDir,
      Path oldLogDir, final FileSystem fs)  {

    @SuppressWarnings("unchecked")
    Class<? extends HLogSplitter> splitterClass = (Class<? extends HLogSplitter>) conf
        .getClass(LOG_SPLITTER_IMPL, HLogSplitter.class);
    try {
       Constructor<? extends HLogSplitter> constructor =
         splitterClass.getConstructor(
          Configuration.class, // conf
          Path.class, // rootDir
          Path.class, // srcDir
          Path.class, // oldLogDir
          FileSystem.class); // fs
      return constructor.newInstance(conf, rootDir, srcDir, oldLogDir, fs);
    } catch (IllegalArgumentException e) {
      throw new RuntimeException(e);
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e);
    } catch (SecurityException e) {
      throw new RuntimeException(e);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  public HLogSplitter(Configuration conf, Path rootDir, Path srcDir,
      Path oldLogDir, FileSystem fs) {
    this.conf = conf;
    this.rootDir = rootDir;
    this.srcDir = srcDir;
    this.oldLogDir = oldLogDir;
    this.fs = fs;
    
    entryBuffers = new EntryBuffers(
        conf.getInt("hbase.regionserver.hlog.splitlog.buffersize",
            128*1024*1024));
    outputSink = new OutputSink();
  }
  
  /**
   * Split up a bunch of regionserver commit log files that are no longer being
   * written to, into new files, one per region for region to replay on startup.
   * Delete the old log files when finished.
   * 
   * @throws IOException will throw if corrupted hlogs aren't tolerated
   * @return the list of splits
   */
  public List<Path> splitLog()
      throws IOException {
    Preconditions.checkState(!hasSplit,
        "An HLogSplitter instance may only be used once");
    hasSplit = true;

    long startTime = System.currentTimeMillis();
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
    splits = splitLog(logfiles);
    
    splitTime = System.currentTimeMillis() - startTime;
    LOG.info("hlog file splitting completed in " + splitTime +
        " ms for " + srcDir.toString());
    return splits;
  }
  
  /**
   * @return time that this split took
   */
  public long getTime() {
    return this.splitTime;
  }
  
  /**
   * @return aggregate size of hlogs that were split
   */
  public long getSize() {
    return this.splitSize;
  }

  /**
   * @return a map from encoded region ID to the number of edits written out
   * for that region.
   */
  Map<byte[], Long> getOutputCounts() {
    Preconditions.checkState(hasSplit);
    return outputSink.getOutputCounts();
  }
   
  /**
   * Splits the HLog edits in the given list of logfiles (that are a mix of edits
   * on multiple regions) by region and then splits them per region directories,
   * in batches of (hbase.hlog.split.batch.size)
   * 
   * This process is split into multiple threads. In the main thread, we loop
   * through the logs to be split. For each log, we:
   * <ul>
   *   <li> Recover it (take and drop HDFS lease) to ensure no other process can write</li>
   *   <li> Read each edit (see {@link #parseHLog}</li>
   *   <li> Mark as "processed" or "corrupt" depending on outcome</li>
   * </ul>
   * 
   * Each edit is passed into the EntryBuffers instance, which takes care of
   * memory accounting and splitting the edits by region.
   * 
   * The OutputSink object then manages N other WriterThreads which pull chunks
   * of edits from EntryBuffers and write them to the output region directories.
   * 
   * After the process is complete, the log files are archived to a separate
   * directory.
   */
  private List<Path> splitLog(final FileStatus[] logfiles) throws IOException {
    List<Path> processedLogs = new ArrayList<Path>();
    List<Path> corruptedLogs = new ArrayList<Path>();
    List<Path> splits = null;

    boolean skipErrors = conf.getBoolean("hbase.hlog.split.skip.errors", true);

    splitSize = 0;

    outputSink.startWriterThreads(entryBuffers);
    
    try {
      int i = 0;
      for (FileStatus log : logfiles) {
       Path logPath = log.getPath();
        long logLength = log.getLen();
        splitSize += logLength;
        LOG.debug("Splitting hlog " + (i++ + 1) + " of " + logfiles.length
            + ": " + logPath + ", length=" + logLength);
        try {
          recoverFileLease(fs, logPath, conf);
          parseHLog(log, entryBuffers, fs, conf);
          processedLogs.add(logPath);
        } catch (EOFException eof) {
          // truncated files are expected if a RS crashes (see HBASE-2643)
          LOG.info("EOF from hlog " + logPath + ". Continuing");
          processedLogs.add(logPath);
        } catch (FileNotFoundException fnfe) {
          // A file may be missing if the region server was able to archive it
          // before shutting down. This means the edits were persisted already
          LOG.info("A log was missing " + logPath +
              ", probably because it was moved by the" +
              " now dead region server. Continuing");
          processedLogs.add(logPath);
        } catch (IOException e) {
          // If the IOE resulted from bad file format,
          // then this problem is idempotent and retrying won't help
          if (e.getCause() instanceof ParseException) {
            LOG.warn("Parse exception from hlog " + logPath + ".  continuing", e);
            processedLogs.add(logPath);
          } else {
            if (skipErrors) {
              LOG.info("Got while parsing hlog " + logPath +
                ". Marking as corrupted", e);
              corruptedLogs.add(logPath);
            } else {
              throw e;
            }
          }
        }
      }
      if (fs.listStatus(srcDir).length > processedLogs.size()
          + corruptedLogs.size()) {
        throw new OrphanHLogAfterSplitException(
            "Discovered orphan hlog after split. Maybe the "
            + "HRegionServer was not dead when we started");
      }
      archiveLogs(srcDir, corruptedLogs, processedLogs, oldLogDir, fs, conf);      
    } finally {
      splits = outputSink.finishWritingAndClose();
    }
    return splits;
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
  private static void archiveLogs(
      final Path srcDir,
      final List<Path> corruptedLogs,
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
    
    if (!fs.delete(srcDir, true)) {
      throw new IOException("Unable to delete src dir: " + srcDir);
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
    if (!fs.exists(regiondir)) {
      LOG.info("This region's directory doesn't exist: "
          + regiondir.toString() + ". It is very likely that it was" +
          " already split so it's safe to discard those edits.");
      return null;
    }
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
		EntryBuffers entryBuffers, final FileSystem fs,
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
        entryBuffers.appendEntry(entry);
        editsCount++;
      }
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    } finally {
      LOG.debug("Pushed=" + editsCount + " entries from " + path);
      try {
        if (in != null) {
          in.close();
        }
      } catch (IOException e) {
        LOG.warn("Close log reader in finally threw exception -- continuing",
                 e);
      }
    }
  }

  private void writerThreadError(Throwable t) {
    thrown.compareAndSet(null, t);
  }
  
  /**
   * Check for errors in the writer threads. If any is found, rethrow it.
   */
  private void checkForErrors() throws IOException {
    Throwable thrown = this.thrown.get();
    if (thrown == null) return;
    if (thrown instanceof IOException) {
      throw (IOException)thrown;
    } else {
      throw new RuntimeException(thrown);
    }
  }
  /**
   * Create a new {@link Writer} for writing log splits.
   */
  protected Writer createWriter(FileSystem fs, Path logfile, Configuration conf)
      throws IOException {
    return HLog.createWriter(fs, logfile, conf);
  }

  /**
   * Create a new {@link Reader} for reading logs to split.
   */
  protected Reader getReader(FileSystem fs, Path curLogFile, Configuration conf)
      throws IOException {
    return HLog.getReader(fs, curLogFile, conf);
  }


  /**
   * Class which accumulates edits and separates them into a buffer per region
   * while simultaneously accounting RAM usage. Blocks if the RAM usage crosses
   * a predefined threshold.
   * 
   * Writer threads then pull region-specific buffers from this class.
   */
  class EntryBuffers {
    Map<byte[], RegionEntryBuffer> buffers =
      new TreeMap<byte[], RegionEntryBuffer>(Bytes.BYTES_COMPARATOR);
    
    /* Track which regions are currently in the middle of writing. We don't allow
       an IO thread to pick up bytes from a region if we're already writing
       data for that region in a different IO thread. */ 
    Set<byte[]> currentlyWriting = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);

    long totalBuffered = 0;
    long maxHeapUsage;
    
    EntryBuffers(long maxHeapUsage) {
      this.maxHeapUsage = maxHeapUsage;
    }

    /**
     * Append a log entry into the corresponding region buffer.
     * Blocks if the total heap usage has crossed the specified threshold.
     * 
     * @throws InterruptedException
     * @throws IOException 
     */
    void appendEntry(Entry entry) throws InterruptedException, IOException {
      HLogKey key = entry.getKey();
      
      RegionEntryBuffer buffer;
      synchronized (this) {
        buffer = buffers.get(key.getEncodedRegionName());
        if (buffer == null) {
          buffer = new RegionEntryBuffer(key.getTablename(), key.getEncodedRegionName());
          buffers.put(key.getEncodedRegionName(), buffer);
        }
        long incrHeap = buffer.appendEntry(entry);
        totalBuffered += incrHeap;
      }

      // If we crossed the chunk threshold, wait for more space to be available
      synchronized (dataAvailable) {
        while (totalBuffered > maxHeapUsage && thrown == null) {
          LOG.debug("Used " + totalBuffered + " bytes of buffered edits, waiting for IO threads...");
          dataAvailable.wait(3000);
        }
        dataAvailable.notifyAll();
      }
      checkForErrors();
    }

    synchronized RegionEntryBuffer getChunkToWrite() {
      long biggestSize=0;
      byte[] biggestBufferKey=null;

      for (Map.Entry<byte[], RegionEntryBuffer> entry : buffers.entrySet()) {
        long size = entry.getValue().heapSize();
        if (size > biggestSize && !currentlyWriting.contains(entry.getKey())) {
          biggestSize = size;
          biggestBufferKey = entry.getKey();
        }
      }
      if (biggestBufferKey == null) {
        return null;
      }

      RegionEntryBuffer buffer = buffers.remove(biggestBufferKey);
      currentlyWriting.add(biggestBufferKey);
      return buffer;
    }

    void doneWriting(RegionEntryBuffer buffer) {
      synchronized (this) {
        boolean removed = currentlyWriting.remove(buffer.encodedRegionName);
        assert removed;
      }
      long size = buffer.heapSize();

      synchronized (dataAvailable) {
        totalBuffered -= size;
        // We may unblock writers
        dataAvailable.notifyAll();
      }
    }
    
    synchronized boolean isRegionCurrentlyWriting(byte[] region) {
      return currentlyWriting.contains(region);
    }
  }

  /**
   * A buffer of some number of edits for a given region.
   * This accumulates edits and also provides a memory optimization in order to
   * share a single byte array instance for the table and region name.
   * Also tracks memory usage of the accumulated edits.
   */
  static class RegionEntryBuffer implements HeapSize {
    long heapInBuffer = 0;
    List<Entry> entryBuffer;
    byte[] tableName;
    byte[] encodedRegionName;

    RegionEntryBuffer(byte[] table, byte[] region) {
      this.tableName = table;
      this.encodedRegionName = region;
      this.entryBuffer = new LinkedList<Entry>();
    }

    long appendEntry(Entry entry) {
      internify(entry);
      entryBuffer.add(entry);
      long incrHeap = entry.getEdit().heapSize() +
        ClassSize.align(2 * ClassSize.REFERENCE) + // HLogKey pointers
        0; // TODO linkedlist entry
      heapInBuffer += incrHeap;
      return incrHeap;
    }

    private void internify(Entry entry) {
      HLogKey k = entry.getKey();
      k.internTableName(this.tableName);
      k.internEncodedRegionName(this.encodedRegionName);
    }

    public long heapSize() {
      return heapInBuffer;
    }
  }


  class WriterThread extends Thread {
    private volatile boolean shouldStop = false;
    
    WriterThread(int i) {
      super("WriterThread-" + i);
    }
    
    public void run()  {
      try {
        doRun();
      } catch (Throwable t) {
        LOG.error("Error in log splitting write thread", t);
        writerThreadError(t);
      }
    }
    
    private void doRun() throws IOException {
      LOG.debug("Writer thread " + this + ": starting");
      while (true) {
        RegionEntryBuffer buffer = entryBuffers.getChunkToWrite();
        if (buffer == null) {
          // No data currently available, wait on some more to show up
          synchronized (dataAvailable) {
            if (shouldStop) return;
            try {
              dataAvailable.wait(1000);
            } catch (InterruptedException ie) {
              if (!shouldStop) {
                throw new RuntimeException(ie);
              }
            }
          }
          continue;
        }
        
        assert buffer != null;
        try {
          writeBuffer(buffer);
        } finally {
          entryBuffers.doneWriting(buffer);
        }
      }
    }
       
    private void writeBuffer(RegionEntryBuffer buffer) throws IOException {
      List<Entry> entries = buffer.entryBuffer;      
      if (entries.isEmpty()) {
        LOG.warn(this.getName() + " got an empty buffer, skipping");
        return;
      }

      WriterAndPath wap = null;
      
      long startTime = System.nanoTime();
      try {
        int editsCount = 0;

        for (Entry logEntry : entries) {
          if (wap == null) {
            wap = outputSink.getWriterAndPath(logEntry);
            if (wap == null) {
              // getWriterAndPath decided we don't need to write these edits
              // Message was already logged
              return;
            }
          }
          wap.w.append(logEntry);
          editsCount++;
        }
        // Pass along summary statistics
        wap.incrementEdits(editsCount);
        wap.incrementNanoTime(System.nanoTime() - startTime);
      } catch (IOException e) {
        e = RemoteExceptionHandler.checkIOException(e);
        LOG.fatal(this.getName() + " Got while writing log entry to log", e);
        throw e;
      }
    }
    
    void finish() {
      shouldStop = true;
    }
  }

  /**
   * Class that manages the output streams from the log splitting process.
   */
  class OutputSink {
    private final Map<byte[], WriterAndPath> logWriters = Collections.synchronizedMap(
          new TreeMap<byte[], WriterAndPath>(Bytes.BYTES_COMPARATOR));
    private final List<WriterThread> writerThreads = Lists.newArrayList();
    
    /* Set of regions which we've decided should not output edits */
    private final Set<byte[]> blacklistedRegions = Collections.synchronizedSet(
        new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR));
    
    private boolean hasClosed = false;    
    
    /**
     * Start the threads that will pump data from the entryBuffers
     * to the output files.
     * @return the list of started threads
     */
    synchronized void startWriterThreads(EntryBuffers entryBuffers) {
      // More threads could potentially write faster at the expense
      // of causing more disk seeks as the logs are split.
      // 3. After a certain setting (probably around 3) the
      // process will be bound on the reader in the current
      // implementation anyway.
      int numThreads = conf.getInt(
          "hbase.regionserver.hlog.splitlog.writer.threads", 3);

      for (int i = 0; i < numThreads; i++) {
        WriterThread t = new WriterThread(i);
        t.start();
        writerThreads.add(t);
      }
    }
    
    List<Path> finishWritingAndClose() throws IOException {
      LOG.info("Waiting for split writer threads to finish");
      for (WriterThread t : writerThreads) {
        t.finish();
      }
      for (WriterThread t: writerThreads) {
        try {
          t.join();
        } catch (InterruptedException ie) {
          throw new IOException(ie);
        }
        checkForErrors();
      }
      LOG.info("Split writers finished");
      
      return closeStreams();
    }

    /**
     * Close all of the output streams.
     * @return the list of paths written.
     */
    private List<Path> closeStreams() throws IOException {
      Preconditions.checkState(!hasClosed);
      
      List<Path> paths = new ArrayList<Path>();
      List<IOException> thrown = Lists.newArrayList();
      
      for (WriterAndPath wap : logWriters.values()) {
        try {
          wap.w.close();
        } catch (IOException ioe) {
          LOG.error("Couldn't close log at " + wap.p, ioe);
          thrown.add(ioe);
          continue;
        }
        paths.add(wap.p);
        LOG.info("Closed path " + wap.p +" (wrote " + wap.editsWritten + " edits in "
            + (wap.nanosSpent / 1000/ 1000) + "ms)");
      }
      if (!thrown.isEmpty()) {
        throw MultipleIOException.createIOException(thrown);
      }
      
      hasClosed = true;
      return paths;
    }

    /**
     * Get a writer and path for a log starting at the given entry.
     * 
     * This function is threadsafe so long as multiple threads are always
     * acting on different regions.
     * 
     * @return null if this region shouldn't output any logs
     */
    WriterAndPath getWriterAndPath(Entry entry) throws IOException {
    
      byte region[] = entry.getKey().getEncodedRegionName();
      WriterAndPath ret = logWriters.get(region);
      if (ret != null) {
        return ret;
      }
      
      // If we already decided that this region doesn't get any output
      // we don't need to check again.
      if (blacklistedRegions.contains(region)) {
        return null;
      }
      
      // Need to create writer
      Path regionedits = getRegionSplitEditsPath(fs,
          entry, rootDir);
      if (regionedits == null) {
        // Edits dir doesn't exist
        blacklistedRegions.add(region);
        return null;
      }
      deletePreexistingOldEdits(regionedits);
      Writer w = createWriter(fs, regionedits, conf);
      ret = new WriterAndPath(regionedits, w);
      logWriters.put(region, ret);
      LOG.debug("Creating writer path=" + regionedits + " region="
          + Bytes.toStringBinary(region));

      return ret;
    }

    /**
     * If the specified path exists, issue a warning and delete it.
     */
    private void deletePreexistingOldEdits(Path regionedits) throws IOException {
      if (fs.exists(regionedits)) {
        LOG.warn("Found existing old edits file. It could be the "
            + "result of a previous failed split attempt. Deleting "
            + regionedits + ", length="
            + fs.getFileStatus(regionedits).getLen());
        if (!fs.delete(regionedits, false)) {
          LOG.warn("Failed delete of old " + regionedits);
        }
      }
    }

    /**
     * @return a map from encoded region ID to the number of edits written out
     * for that region.
     */
    private Map<byte[], Long> getOutputCounts() {
      TreeMap<byte[], Long> ret = new TreeMap<byte[], Long>(
          Bytes.BYTES_COMPARATOR);
      synchronized (logWriters) {
        for (Map.Entry<byte[], WriterAndPath> entry : logWriters.entrySet()) {
          ret.put(entry.getKey(), entry.getValue().editsWritten);
        }
      }
      return ret;
    }
  }

  /**
   *  Private data structure that wraps a Writer and its Path,
   *  also collecting statistics about the data written to this
   *  output.
   */
  private final static class WriterAndPath {
    final Path p;
    final Writer w;

    /* Count of edits written to this path */
    long editsWritten = 0;
    /* Number of nanos spent writing to this log */
    long nanosSpent = 0;

    WriterAndPath(final Path p, final Writer w) {
      this.p = p;
      this.w = w;
    }

    void incrementEdits(int edits) {
      editsWritten += edits;
    }

    void incrementNanoTime(long nanos) {
      nanosSpent += nanos;
    }
  }
}
