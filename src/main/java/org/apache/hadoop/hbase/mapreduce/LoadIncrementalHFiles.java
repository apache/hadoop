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
package org.apache.hadoop.hbase.mapreduce;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ServerCallable;
import org.apache.hadoop.hbase.io.HalfStoreFileReader;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.Reference.Range;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Tool to load the output of HFileOutputFormat into an existing table.
 * @see #usage()
 */
public class LoadIncrementalHFiles extends Configured implements Tool {

  private static Log LOG = LogFactory.getLog(LoadIncrementalHFiles.class);
  private static final int  TABLE_CREATE_MAX_RETRIES = 20;
  private static final long TABLE_CREATE_SLEEP = 60000;
  static AtomicLong regionCount = new AtomicLong(0);
  private HBaseAdmin hbAdmin;
  private Configuration cfg;

  public static String NAME = "completebulkload";

  public LoadIncrementalHFiles(Configuration conf) throws Exception {
    super(conf);
    this.cfg = conf;
    this.hbAdmin = new HBaseAdmin(conf);
  }

  private void usage() {
    System.err.println("usage: " + NAME +
        " /path/to/hfileoutputformat-output " +
        "tablename");
  }

  /**
   * Represents an HFile waiting to be loaded. An queue is used
   * in this class in order to support the case where a region has
   * split during the process of the load. When this happens,
   * the HFile is split into two physical parts across the new
   * region boundary, and each part is added back into the queue.
   * The import process finishes when the queue is empty.
   */
  static class LoadQueueItem {
    final byte[] family;
    final Path hfilePath;

    public LoadQueueItem(byte[] family, Path hfilePath) {
      this.family = family;
      this.hfilePath = hfilePath;
    }

    public String toString() {
      return "family:"+ Bytes.toString(family) + " path:" + hfilePath.toString();
    }
  }

  /**
   * Walk the given directory for all HFiles, and return a Queue
   * containing all such files.
   */
  private void discoverLoadQueue(Deque<LoadQueueItem> ret, Path hfofDir)
  throws IOException {
    FileSystem fs = hfofDir.getFileSystem(getConf());

    if (!fs.exists(hfofDir)) {
      throw new FileNotFoundException("HFileOutputFormat dir " +
          hfofDir + " not found");
    }

    FileStatus[] familyDirStatuses = fs.listStatus(hfofDir);
    if (familyDirStatuses == null) {
      throw new FileNotFoundException("No families found in " + hfofDir);
    }

    for (FileStatus stat : familyDirStatuses) {
      if (!stat.isDir()) {
        LOG.warn("Skipping non-directory " + stat.getPath());
        continue;
      }
      Path familyDir = stat.getPath();
      // Skip _logs, etc
      if (familyDir.getName().startsWith("_")) continue;
      byte[] family = familyDir.getName().getBytes();
      Path[] hfiles = FileUtil.stat2Paths(fs.listStatus(familyDir));
      for (Path hfile : hfiles) {
        if (hfile.getName().startsWith("_")) continue;
        ret.add(new LoadQueueItem(family, hfile));
      }
    }
  }

  /**
   * Perform a bulk load of the given directory into the given
   * pre-existing table.
   * @param hfofDir the directory that was provided as the output path
   * of a job using HFileOutputFormat
   * @param table the table to load into
   * @throws TableNotFoundException if table does not yet exist
   */
  public void doBulkLoad(Path hfofDir, final HTable table)
    throws TableNotFoundException, IOException
  {
    final HConnection conn = table.getConnection();

    if (!conn.isTableAvailable(table.getTableName())) {
      throw new TableNotFoundException("Table " +
          Bytes.toStringBinary(table.getTableName()) +
          "is not currently available.");
    }

    // initialize thread pools
    int nrThreads = cfg.getInt("hbase.loadincremental.threads.max",
        Runtime.getRuntime().availableProcessors());
    ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
    builder.setNameFormat("LoadIncrementalHFiles-%1$d");
    ExecutorService pool = new ThreadPoolExecutor(nrThreads, nrThreads,
        60, TimeUnit.SECONDS,
        new LinkedBlockingQueue<Runnable>(),
        builder.build());
    ((ThreadPoolExecutor)pool).allowCoreThreadTimeOut(true);

    // LQI queue does not need to be threadsafe -- all operations on this queue
    // happen in this thread
    Deque<LoadQueueItem> queue = new LinkedList<LoadQueueItem>();
    try {
      discoverLoadQueue(queue, hfofDir);
      int count = 0;

      // Assumes that region splits can happen while this occurs.
      while (!queue.isEmpty()) {
        // need to reload split keys each iteration.
        final Pair<byte[][], byte[][]> startEndKeys = table.getStartEndKeys();
        if (count != 0) {
          LOG.info("Split occured while grouping HFiles, retry attempt " +
              + count + " with " + queue.size() + " files remaining to load");
        }
        
        int maxRetries = cfg.getInt("hbase.bulkload.retries.number", 10);
        if (count >= maxRetries) {
          LOG.error("Retry attempted " + count +  " times without completing, bailing out");
          return;
        }
        count++;

        // Using ByteBuffer for byte[] equality semantics
        Multimap<ByteBuffer, LoadQueueItem> regionGroups = groupOrSplitPhase(table,
            pool, queue, startEndKeys);

        bulkLoadPhase(table, conn, pool, queue, regionGroups);

        // NOTE: The next iteration's split / group could happen in parallel to
        // atomic bulkloads assuming that there are splits and no merges, and
        // that we can atomically pull out the groups we want to retry.
      }

    } finally {
      pool.shutdown();
      if (queue != null && !queue.isEmpty()) {
        StringBuilder err = new StringBuilder();
        err.append("-------------------------------------------------\n");
        err.append("Bulk load aborted with some files not yet loaded:\n");
        err.append("-------------------------------------------------\n");
        for (LoadQueueItem q : queue) {
          err.append("  ").append(q.hfilePath).append('\n');
        }
        LOG.error(err);
      }
    }
  }

  /**
   * This takes the LQI's grouped by likely regions and attempts to bulk load
   * them.  Any failures are re-queued for another pass with the
   * groupOrSplitPhase.
   */
  private void bulkLoadPhase(final HTable table, final HConnection conn,
      ExecutorService pool, Deque<LoadQueueItem> queue,
      final Multimap<ByteBuffer, LoadQueueItem> regionGroups) throws IOException {
    // atomically bulk load the groups.
    Set<Future<List<LoadQueueItem>>> loadingFutures = new HashSet<Future<List<LoadQueueItem>>>();
    for (Entry<ByteBuffer, ? extends Collection<LoadQueueItem>> e: regionGroups.asMap().entrySet()) {
      final byte[] first = e.getKey().array();
      final Collection<LoadQueueItem> lqis =  e.getValue();

      final Callable<List<LoadQueueItem>> call = new Callable<List<LoadQueueItem>>() {
        public List<LoadQueueItem> call() throws Exception {
          List<LoadQueueItem> toRetry = tryAtomicRegionLoad(conn, table.getTableName(), first, lqis);
          return toRetry;
        }
      };
      loadingFutures.add(pool.submit(call));
    }

    // get all the results.
    for (Future<List<LoadQueueItem>> future : loadingFutures) {
      try {
        List<LoadQueueItem> toRetry = future.get();
        if (toRetry != null && toRetry.size() != 0) {
          // LQIs that are requeued to be regrouped.
          queue.addAll(toRetry);
        }

      } catch (ExecutionException e1) {
        Throwable t = e1.getCause();
        if (t instanceof IOException) {
          LOG.error("IOException during bulk load", e1);
          throw (IOException)t; // would have been thrown if not parallelized,
        }
        LOG.error("Unexpected execution exception during bulk load", e1);
        throw new IllegalStateException(t);
      } catch (InterruptedException e1) {
        LOG.error("Unexpected interrupted exception during bulk load", e1);
        throw new IllegalStateException(e1);
      }
    }
  }

  /**
   * @return A Multimap<startkey, LoadQueueItem> that groups LQI by likely
   * bulk load region targets.
   */
  private Multimap<ByteBuffer, LoadQueueItem> groupOrSplitPhase(final HTable table,
      ExecutorService pool, Deque<LoadQueueItem> queue,
      final Pair<byte[][], byte[][]> startEndKeys) throws IOException {
    // <region start key, LQI> need synchronized only within this scope of this
    // phase because of the puts that happen in futures.
    Multimap<ByteBuffer, LoadQueueItem> rgs = HashMultimap.create();
    final Multimap<ByteBuffer, LoadQueueItem> regionGroups = Multimaps.synchronizedMultimap(rgs);

    // drain LQIs and figure out bulk load groups
    Set<Future<List<LoadQueueItem>>> splittingFutures = new HashSet<Future<List<LoadQueueItem>>>();
    while (!queue.isEmpty()) {
      final LoadQueueItem item = queue.remove();
      
      final Callable<List<LoadQueueItem>> call = new Callable<List<LoadQueueItem>>() {
        public List<LoadQueueItem> call() throws Exception {
          List<LoadQueueItem> splits = groupOrSplit(regionGroups, item, table, startEndKeys);
          return splits;
        }
      };
      splittingFutures.add(pool.submit(call));
    }
    // get all the results.  All grouping and splitting must finish before
    // we can attempt the atomic loads.
    for (Future<List<LoadQueueItem>> lqis : splittingFutures) {
      try {
        List<LoadQueueItem> splits = lqis.get();
        if (splits != null) {
          queue.addAll(splits);
        }
      } catch (ExecutionException e1) {
        Throwable t = e1.getCause();
        if (t instanceof IOException) {
          LOG.error("IOException during splitting", e1);
          throw (IOException)t; // would have been thrown if not parallelized,
        }
        LOG.error("Unexpected execution exception during splitting", e1);
        throw new IllegalStateException(t);
      } catch (InterruptedException e1) {
        LOG.error("Unexpected interrupted exception during splitting", e1);
        throw new IllegalStateException(e1);
      }
    }
    return regionGroups;
  }

  // unique file name for the table
  String getUniqueName(byte[] tableName) {
    String name = Bytes.toStringBinary(tableName) + "," + regionCount.incrementAndGet();
    return name;
  }

  protected List<LoadQueueItem> splitStoreFile(final LoadQueueItem item,
      final HTable table, byte[] startKey,
      byte[] splitKey) throws IOException {
    final Path hfilePath = item.hfilePath;

    // We use a '_' prefix which is ignored when walking directory trees
    // above.
    final Path tmpDir = new Path(item.hfilePath.getParent(), "_tmp");

    LOG.info("HFile at " + hfilePath + " no longer fits inside a single " +
    "region. Splitting...");

    String uniqueName = getUniqueName(table.getTableName());
    HColumnDescriptor familyDesc = table.getTableDescriptor().getFamily(item.family);
    Path botOut = new Path(tmpDir, uniqueName + ".bottom");
    Path topOut = new Path(tmpDir, uniqueName + ".top");
    splitStoreFile(getConf(), hfilePath, familyDesc, splitKey,
        botOut, topOut);

    // Add these back at the *front* of the queue, so there's a lower
    // chance that the region will just split again before we get there.
    List<LoadQueueItem> lqis = new ArrayList<LoadQueueItem>(2);
    lqis.add(new LoadQueueItem(item.family, botOut));
    lqis.add(new LoadQueueItem(item.family, topOut));

    LOG.info("Successfully split into new HFiles " + botOut + " and " + topOut);
    return lqis;
  }

  /**
   * Attempt to assign the given load queue item into its target region group.
   * If the hfile boundary no longer fits into a region, physically splits
   * the hfile such that the new bottom half will fit and returns the list of
   * LQI's corresponding to the resultant hfiles.
   *
   * protected for testing
   */
  protected List<LoadQueueItem> groupOrSplit(Multimap<ByteBuffer, LoadQueueItem> regionGroups,
      final LoadQueueItem item, final HTable table,
      final Pair<byte[][], byte[][]> startEndKeys)
      throws IOException {
    final Path hfilePath = item.hfilePath;
    final FileSystem fs = hfilePath.getFileSystem(getConf());
    HFile.Reader hfr = HFile.createReader(fs, hfilePath,
        new CacheConfig(getConf()));
    final byte[] first, last;
    try {
      hfr.loadFileInfo();
      first = hfr.getFirstRowKey();
      last = hfr.getLastRowKey();
    }  finally {
      hfr.close();
    }

    LOG.info("Trying to load hfile=" + hfilePath +
        " first=" + Bytes.toStringBinary(first) +
        " last="  + Bytes.toStringBinary(last));
    if (first == null || last == null) {
      assert first == null && last == null;
      // TODO what if this is due to a bad HFile?
      LOG.info("hfile " + hfilePath + " has no entries, skipping");
      return null;
    }
    if (Bytes.compareTo(first, last) > 0) {
      throw new IllegalArgumentException(
      "Invalid range: " + Bytes.toStringBinary(first) +
      " > " + Bytes.toStringBinary(last));
    }
    int idx = Arrays.binarySearch(startEndKeys.getFirst(), first,
        Bytes.BYTES_COMPARATOR);
    if (idx < 0) {
      // not on boundary, returns -(insertion index).  Calculate region it
      // would be in.
      idx = -(idx + 1) - 1;
    }
    final int indexForCallable = idx;
    boolean lastKeyInRange =
      Bytes.compareTo(last, startEndKeys.getSecond()[idx]) < 0 ||
      Bytes.equals(startEndKeys.getSecond()[idx], HConstants.EMPTY_BYTE_ARRAY);
    if (!lastKeyInRange) {
      List<LoadQueueItem> lqis = splitStoreFile(item, table,
          startEndKeys.getFirst()[indexForCallable],
          startEndKeys.getSecond()[indexForCallable]);
      return lqis;
    }

    // group regions.
    regionGroups.put(ByteBuffer.wrap(startEndKeys.getFirst()[idx]), item);
    return null;
  }

  /**
   * Attempts to do an atomic load of many hfiles into a region.  If it fails,
   * it returns a list of hfiles that need to be retried.  If it is successful
   * it will return an empty list.
   * 
   * NOTE: To maintain row atomicity guarantees, region server callable should
   * succeed atomically and fails atomically.
   * 
   * Protected for testing.
   */
  protected List<LoadQueueItem> tryAtomicRegionLoad(final HConnection conn,
      byte[] tableName, final byte[] first, Collection<LoadQueueItem> lqis) {

    final List<Pair<byte[], String>> famPaths =
      new ArrayList<Pair<byte[], String>>(lqis.size());
    for (LoadQueueItem lqi : lqis) {
      famPaths.add(Pair.newPair(lqi.family, lqi.hfilePath.toString()));
    }

    final ServerCallable<Void> svrCallable = new ServerCallable<Void>(conn,
        tableName, first) {
      @Override
      public Void call() throws Exception {
        LOG.debug("Going to connect to server " + location + " for row "
            + Bytes.toStringBinary(row));
        byte[] regionName = location.getRegionInfo().getRegionName();
        server.bulkLoadHFiles(famPaths, regionName);
        return null;
      }
    };

    List<LoadQueueItem> toRetry = new ArrayList<LoadQueueItem>();
    try {
      conn.getRegionServerWithRetries(svrCallable);
    } catch (IOException e) {
      LOG.warn("Attempt to bulk load region containing "
          + Bytes.toStringBinary(first) + " into table "
          + Bytes.toStringBinary(tableName)  + " with files " + lqis
          + " failed");
      toRetry.addAll(lqis);
    }
    return toRetry;
  }

  /**
   * Split a storefile into a top and bottom half, maintaining
   * the metadata, recreating bloom filters, etc.
   */
  static void splitStoreFile(
      Configuration conf, Path inFile,
      HColumnDescriptor familyDesc, byte[] splitKey,
      Path bottomOut, Path topOut) throws IOException
  {
    // Open reader with no block cache, and not in-memory
    Reference topReference = new Reference(splitKey, Range.top);
    Reference bottomReference = new Reference(splitKey, Range.bottom);

    copyHFileHalf(conf, inFile, topOut, topReference, familyDesc);
    copyHFileHalf(conf, inFile, bottomOut, bottomReference, familyDesc);
  }

  /**
   * Copy half of an HFile into a new HFile.
   */
  private static void copyHFileHalf(
      Configuration conf, Path inFile, Path outFile, Reference reference,
      HColumnDescriptor familyDescriptor)
  throws IOException {
    FileSystem fs = inFile.getFileSystem(conf);
    CacheConfig cacheConf = new CacheConfig(conf);
    HalfStoreFileReader halfReader = null;
    StoreFile.Writer halfWriter = null;
    try {
      halfReader = new HalfStoreFileReader(fs, inFile, cacheConf,
          reference);
      Map<byte[], byte[]> fileInfo = halfReader.loadFileInfo();

      int blocksize = familyDescriptor.getBlocksize();
      Algorithm compression = familyDescriptor.getCompression();
      BloomType bloomFilterType = familyDescriptor.getBloomFilterType();

      halfWriter = new StoreFile.Writer(
          fs, outFile, blocksize, compression, conf, cacheConf,
          KeyValue.COMPARATOR, bloomFilterType, 0);
      HFileScanner scanner = halfReader.getScanner(false, false);
      scanner.seekTo();
      do {
        KeyValue kv = scanner.getKeyValue();
        halfWriter.append(kv);
      } while (scanner.next());

      for (Map.Entry<byte[],byte[]> entry : fileInfo.entrySet()) {
        if (shouldCopyHFileMetaKey(entry.getKey())) {
          halfWriter.appendFileInfo(entry.getKey(), entry.getValue());
        }
      }
    } finally {
      if (halfWriter != null) halfWriter.close();
      if (halfReader != null) halfReader.close(cacheConf.shouldEvictOnClose());
    }
  }

  private static boolean shouldCopyHFileMetaKey(byte[] key) {
    return !HFile.isReservedFileInfoKey(key);
  }

  private boolean doesTableExist(String tableName) throws Exception {
    return hbAdmin.tableExists(tableName);
  }
  
  /*
   * Infers region boundaries for a new table.
   * Parameter:
   *   bdryMap is a map between keys to an integer belonging to {+1, -1}
   *     If a key is a start key of a file, then it maps to +1
   *     If a key is an end key of a file, then it maps to -1
   * Algo:
   * 1) Poll on the keys in order: 
   *    a) Keep adding the mapped values to these keys (runningSum) 
   *    b) Each time runningSum reaches 0, add the start Key from when the runningSum had started to a boundary list.
   * 2) Return the boundary list. 
   */
  public static byte[][] inferBoundaries(TreeMap<byte[], Integer> bdryMap) {
    ArrayList<byte[]> keysArray = new ArrayList<byte[]>();
    int runningValue = 0;
    byte[] currStartKey = null;
    boolean firstBoundary = true;
    
    for (Map.Entry<byte[], Integer> item: bdryMap.entrySet()) {
      if (runningValue == 0) currStartKey = item.getKey();
      runningValue += item.getValue();
      if (runningValue == 0) {
        if (!firstBoundary) keysArray.add(currStartKey);
        firstBoundary = false;
      } 
    }
    
    return keysArray.toArray(new byte[0][0]);
  }
 
  /*
   * If the table is created for the first time, then "completebulkload" reads the files twice.
   * More modifications necessary if we want to avoid doing it.
   */
  private void createTable(String tableName, String dirPath) throws Exception {
    Path hfofDir = new Path(dirPath);
    FileSystem fs = hfofDir.getFileSystem(getConf());

    if (!fs.exists(hfofDir)) {
      throw new FileNotFoundException("HFileOutputFormat dir " +
          hfofDir + " not found");
    }

    FileStatus[] familyDirStatuses = fs.listStatus(hfofDir);
    if (familyDirStatuses == null) {
      throw new FileNotFoundException("No families found in " + hfofDir);
    }

    HTableDescriptor htd = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = null;

    // Add column families
    // Build a set of keys
    byte[][] keys = null;
    TreeMap<byte[], Integer> map = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);
    
    for (FileStatus stat : familyDirStatuses) {
      if (!stat.isDir()) {
        LOG.warn("Skipping non-directory " + stat.getPath());
        continue;
      }
      Path familyDir = stat.getPath();
      // Skip _logs, etc
      if (familyDir.getName().startsWith("_")) continue;
      byte[] family = familyDir.getName().getBytes();
     
      hcd = new HColumnDescriptor(family);
      htd.addFamily(hcd);
      
      Path[] hfiles = FileUtil.stat2Paths(fs.listStatus(familyDir));
      for (Path hfile : hfiles) {
        if (hfile.getName().startsWith("_")) continue;
        
        HFile.Reader reader = HFile.createReader(fs, hfile,
            new CacheConfig(getConf()));
        final byte[] first, last;
        try {
          reader.loadFileInfo();
          first = reader.getFirstRowKey();
          last =  reader.getLastRowKey();

          LOG.info("Trying to figure out region boundaries hfile=" + hfile +
            " first=" + Bytes.toStringBinary(first) +
            " last="  + Bytes.toStringBinary(last));
          
          // To eventually infer start key-end key boundaries
          Integer value = map.containsKey(first)?(Integer)map.get(first):0;
          map.put(first, value+1);

          value = map.containsKey(last)?(Integer)map.get(last):0;
          map.put(last, value-1);
        }  finally {
          reader.close();
        }
      }
    }
    
    keys = LoadIncrementalHFiles.inferBoundaries(map);
    try {    
      this.hbAdmin.createTableAsync(htd, keys);
    } catch (java.net.SocketTimeoutException e) {
      System.err.println("Caught Socket timeout.. Mostly caused by a slow region assignment by master!");
    }

    HTable table = new HTable(this.cfg, tableName);

    HConnection conn = table.getConnection();
    int ctr = 0;
    while (!conn.isTableAvailable(table.getTableName()) && (ctr<TABLE_CREATE_MAX_RETRIES)) {
      LOG.info("Table " + tableName + "not yet available... Sleeping for 60 more seconds...");
      /* Every TABLE_CREATE_SLEEP milliseconds, wakes up and checks if the table is available*/
      Thread.sleep(TABLE_CREATE_SLEEP);
      ctr++;
    }
    LOG.info("Table "+ tableName +" is finally available!!");
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      usage();
      return -1;
    }

    String dirPath   = args[0];
    String tableName = args[1];

    boolean tableExists   = this.doesTableExist(tableName);
    if (!tableExists) this.createTable(tableName,dirPath);

    Path hfofDir = new Path(dirPath);
    HTable table = new HTable(this.cfg, tableName);

    doBulkLoad(hfofDir, table);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new LoadIncrementalHFiles(HBaseConfiguration.create()), args);
    System.exit(ret);
  }

}
