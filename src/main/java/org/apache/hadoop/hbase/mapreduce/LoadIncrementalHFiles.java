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
import java.util.Deque;
import java.util.LinkedList;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ServerCallable;
import org.apache.hadoop.hbase.io.HalfStoreFileReader;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.Reference.Range;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Tool to load the output of HFileOutputFormat into an existing table.
 * @see usage()
 */
public class LoadIncrementalHFiles extends Configured implements Tool {

  static Log LOG = LogFactory.getLog(LoadIncrementalHFiles.class);

  public static String NAME = "completebulkload";
  
  public LoadIncrementalHFiles(Configuration conf) {
    super(conf);
  }
  
  public LoadIncrementalHFiles() {
    super();
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
  private static class LoadQueueItem {
    final byte[] family;
    final Path hfilePath;
    
    public LoadQueueItem(byte[] family, Path hfilePath) {
      this.family = family;
      this.hfilePath = hfilePath;
    }
  }

  /**
   * Walk the given directory for all HFiles, and return a Queue
   * containing all such files.
   */
  private Deque<LoadQueueItem> discoverLoadQueue(Path hfofDir)
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
    
    Deque<LoadQueueItem> ret = new LinkedList<LoadQueueItem>();
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
    return ret;
  }

  /**
   * Perform a bulk load of the given directory into the given
   * pre-existing table.
   * @param hfofDir the directory that was provided as the output path
   * of a job using HFileOutputFormat
   * @param table the table to load into
   * @throws TableNotFoundException if table does not yet exist
   */
  public void doBulkLoad(Path hfofDir, HTable table)
    throws TableNotFoundException, IOException
  {
    HConnection conn = table.getConnection();
    
    if (!conn.isTableAvailable(table.getTableName())) {
      throw new TableNotFoundException("Table " + 
          Bytes.toStringBinary(table.getTableName()) +
          "is not currently available.");
    }
    
    Deque<LoadQueueItem> queue = null;
    try {
      queue = discoverLoadQueue(hfofDir);
      while (!queue.isEmpty()) {
        LoadQueueItem item = queue.remove();
        tryLoad(item, conn, table.getTableName(), queue);
      }
    } finally {
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
   * Attempt to load the given load queue item into its target region server.
   * If the hfile boundary no longer fits into a region, physically splits
   * the hfile such that the new bottom half will fit, and adds the two
   * resultant hfiles back into the load queue.
   */
  private void tryLoad(final LoadQueueItem item,
      HConnection conn, final byte[] table,
      final Deque<LoadQueueItem> queue)
  throws IOException {
    final Path hfilePath = item.hfilePath;
    final FileSystem fs = hfilePath.getFileSystem(getConf());
    HFile.Reader hfr = new HFile.Reader(fs, hfilePath, null, false);
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
      LOG.info("hfile " + hfilePath + " has no entries, skipping");
      return;
    }
    
    // We use a '_' prefix which is ignored when walking directory trees
    // above.
    final Path tmpDir = new Path(item.hfilePath.getParent(), "_tmp");

    conn.getRegionServerWithRetries(
      new ServerCallable<Void>(conn, table, first) {
        @Override
        public Void call() throws Exception {
          LOG.debug("Going to connect to server " + location +
              "for row " + Bytes.toStringBinary(row));
          HRegionInfo hri = location.getRegionInfo();
          if (!hri.containsRange(first, last)) {
            LOG.info("HFile at " + hfilePath + " no longer fits inside a single " +
                "region. Splitting...");
            
            HColumnDescriptor familyDesc = hri.getTableDesc().getFamily(item.family); 
            Path botOut = new Path(tmpDir, hri.getEncodedName() + ".bottom");
            Path topOut = new Path(tmpDir, hri.getEncodedName() + ".top");
            splitStoreFile(getConf(), hfilePath, familyDesc, hri.getEndKey(),
                botOut, topOut);

            // Add these back at the *front* of the queue, so there's a lower
            // chance that the region will just split again before we get there.
            queue.addFirst(new LoadQueueItem(item.family, botOut));
            queue.addFirst(new LoadQueueItem(item.family, topOut));
            LOG.info("Successfully split into new HFiles " + botOut + " and " + topOut);
            return null;
          }
          
          byte[] regionName = location.getRegionInfo().getRegionName();
          server.bulkLoadHFile(hfilePath.toString(), regionName, item.family);
          return null;
        }
      });
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
    HalfStoreFileReader halfReader = null;
    HFile.Writer halfWriter = null;
    try {
      halfReader = new HalfStoreFileReader(fs, inFile, null, reference);
      Map<byte[], byte[]> fileInfo = halfReader.loadFileInfo();
      
      int blocksize = familyDescriptor.getBlocksize();
      Algorithm compression = familyDescriptor.getCompression();
      BloomType bloomFilterType = familyDescriptor.getBloomFilterType();
      
      halfWriter = new StoreFile.Writer(
          fs, outFile, blocksize, compression, conf, KeyValue.COMPARATOR,
          bloomFilterType, 0);
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
      if (halfReader != null) halfReader.close();
    }    
  }
  
  private static boolean shouldCopyHFileMetaKey(byte[] key) {
    return !HFile.isReservedFileInfoKey(key);
  }


  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      usage();
      return -1;
    }
    
    Path hfofDir = new Path(args[0]);
    HTable table = new HTable(args[1]);
    
    doBulkLoad(hfofDir, table);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new LoadIncrementalHFiles(), args);
  }
  
}
