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
package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.mapreduce.hadoopbackport.TotalOrderPartitioner;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Preconditions;

/**
 * Writes HFiles. Passed KeyValues must arrive in order.
 * Currently, can only write files to a single column family at a
 * time.  Multiple column families requires coordinating keys cross family.
 * Writes current time as the sequence id for the file. Sets the major compacted
 * attribute on created hfiles.
 * @see KeyValueSortReducer
 */
public class HFileOutputFormat extends FileOutputFormat<ImmutableBytesWritable, KeyValue> {
  static Log LOG = LogFactory.getLog(HFileOutputFormat.class);
  
  public RecordWriter<ImmutableBytesWritable, KeyValue> getRecordWriter(final TaskAttemptContext context)
  throws IOException, InterruptedException {
    // Get the path of the temporary output file
    final Path outputPath = FileOutputFormat.getOutputPath(context);
    final Path outputdir = new FileOutputCommitter(outputPath, context).getWorkPath();
    Configuration conf = context.getConfiguration();
    final FileSystem fs = outputdir.getFileSystem(conf);
    // These configs. are from hbase-*.xml
    final long maxsize = conf.getLong("hbase.hregion.max.filesize", 268435456);
    final int blocksize = conf.getInt("hfile.min.blocksize.size", 65536);
    // Invented config.  Add to hbase-*.xml if other than default compression.
    final String compression = conf.get("hfile.compression",
      Compression.Algorithm.NONE.getName());

    return new RecordWriter<ImmutableBytesWritable, KeyValue>() {
      // Map of families to writers and how much has been output on the writer.
      private final Map<byte [], WriterLength> writers =
        new TreeMap<byte [], WriterLength>(Bytes.BYTES_COMPARATOR);
      private byte [] previousRow = HConstants.EMPTY_BYTE_ARRAY;
      private final byte [] now = Bytes.toBytes(System.currentTimeMillis());

      public void write(ImmutableBytesWritable row, KeyValue kv)
      throws IOException {
        long length = kv.getLength();
        byte [] family = kv.getFamily();
        WriterLength wl = this.writers.get(family);
        if (wl == null || ((length + wl.written) >= maxsize) &&
            Bytes.compareTo(this.previousRow, 0, this.previousRow.length,
              kv.getBuffer(), kv.getRowOffset(), kv.getRowLength()) != 0) {
          // Get a new writer.
          Path basedir = new Path(outputdir, Bytes.toString(family));
          if (wl == null) {
            wl = new WriterLength();
            this.writers.put(family, wl);
            if (this.writers.size() > 1) throw new IOException("One family only");
            // If wl == null, first file in family.  Ensure family dir exits.
            if (!fs.exists(basedir)) fs.mkdirs(basedir);
          }
          wl.writer = getNewWriter(wl.writer, basedir);
          LOG.info("Writer=" + wl.writer.getPath() +
            ((wl.written == 0)? "": ", wrote=" + wl.written));
          wl.written = 0;
        }
        kv.updateLatestStamp(this.now);
        wl.writer.append(kv);
        wl.written += length;
        // Copy the row so we know when a row transition.
        this.previousRow = kv.getRow();
      }

      /* Create a new HFile.Writer. Close current if there is one.
       * @param writer
       * @param familydir
       * @return A new HFile.Writer.
       * @throws IOException
       */
      private HFile.Writer getNewWriter(final HFile.Writer writer,
          final Path familydir)
      throws IOException {
        close(writer);
        return new HFile.Writer(fs,  StoreFile.getUniqueFile(fs, familydir),
          blocksize, compression, KeyValue.KEY_COMPARATOR);
      }

      private void close(final HFile.Writer w) throws IOException {
        if (w != null) {
          w.appendFileInfo(StoreFile.BULKLOAD_TIME_KEY,
              Bytes.toBytes(System.currentTimeMillis()));
          w.appendFileInfo(StoreFile.BULKLOAD_TASK_KEY,
              Bytes.toBytes(context.getTaskAttemptID().toString()));
          w.appendFileInfo(StoreFile.MAJOR_COMPACTION_KEY, 
              Bytes.toBytes(true));
          w.close();
        }
      }

      public void close(TaskAttemptContext c)
      throws IOException, InterruptedException {
        for (Map.Entry<byte [], WriterLength> e: this.writers.entrySet()) {
          close(e.getValue().writer);
        }
      }
    };
  }

  /*
   * Data structure to hold a Writer and amount of data written on it.
   */
  static class WriterLength {
    long written = 0;
    HFile.Writer writer = null;
  }

  /**
   * Return the start keys of all of the regions in this table,
   * as a list of ImmutableBytesWritable.
   */
  private static List<ImmutableBytesWritable> getRegionStartKeys(HTable table)
  throws IOException {
    byte[][] byteKeys = table.getStartKeys();
    ArrayList<ImmutableBytesWritable> ret =
      new ArrayList<ImmutableBytesWritable>(byteKeys.length);
    for (byte[] byteKey : byteKeys) {
      ret.add(new ImmutableBytesWritable(byteKey));
    }
    return ret;
  }

  /**
   * Write out a SequenceFile that can be read by TotalOrderPartitioner
   * that contains the split points in startKeys.
   * @param partitionsPath output path for SequenceFile
   * @param startKeys the region start keys
   */
  private static void writePartitions(Configuration conf, Path partitionsPath,
      List<ImmutableBytesWritable> startKeys) throws IOException {
    Preconditions.checkArgument(!startKeys.isEmpty(), "No regions passed");

    // We're generating a list of split points, and we don't ever
    // have keys < the first region (which has an empty start key)
    // so we need to remove it. Otherwise we would end up with an
    // empty reducer with index 0
    TreeSet<ImmutableBytesWritable> sorted =
      new TreeSet<ImmutableBytesWritable>(startKeys);

    ImmutableBytesWritable first = sorted.first();
    Preconditions.checkArgument(
        first.equals(HConstants.EMPTY_BYTE_ARRAY),
        "First region of table should have empty start key. Instead has: %s",
        Bytes.toStringBinary(first.get()));
    sorted.remove(first);
    
    // Write the actual file
    FileSystem fs = partitionsPath.getFileSystem(conf);
    SequenceFile.Writer writer = SequenceFile.createWriter(fs, 
        conf, partitionsPath, ImmutableBytesWritable.class, NullWritable.class);
    
    try {
      for (ImmutableBytesWritable startKey : sorted) {
        writer.append(startKey, NullWritable.get());
      }
    } finally {
      writer.close();
    }
  }
  
  /**
   * Configure a MapReduce Job to perform an incremental load into the given
   * table. This
   * <ul>
   *   <li>Inspects the table to configure a total order partitioner</li>
   *   <li>Uploads the partitions file to the cluster and adds it to the DistributedCache</li>
   *   <li>Sets the number of reduce tasks to match the current number of regions</li>
   *   <li>Sets the output key/value class to match HFileOutputFormat's requirements</li>
   *   <li>Sets the reducer up to perform the appropriate sorting (either KeyValueSortReducer or
   *     PutSortReducer)</li>
   * </ul> 
   * The user should be sure to set the map output value class to either KeyValue or Put before
   * running this function.
   */
  public static void configureIncrementalLoad(Job job, HTable table) throws IOException {
    Configuration conf = job.getConfiguration();
    job.setPartitionerClass(TotalOrderPartitioner.class);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(KeyValue.class);
    job.setOutputFormatClass(HFileOutputFormat.class);
    
    // Based on the configured map output class, set the correct reducer to properly
    // sort the incoming values.
    // TODO it would be nice to pick one or the other of these formats.
    if (KeyValue.class.equals(job.getMapOutputValueClass())) {
      job.setReducerClass(KeyValueSortReducer.class);
    } else if (Put.class.equals(job.getMapOutputValueClass())) {
      job.setReducerClass(PutSortReducer.class);
    } else {
      LOG.warn("Unknown map output value type:" + job.getMapOutputValueClass());
    }
    
    LOG.info("Looking up current regions for table " + table);
    List<ImmutableBytesWritable> startKeys = getRegionStartKeys(table);
    LOG.info("Configuring " + startKeys.size() + " reduce partitions " +
        "to match current region count");
    job.setNumReduceTasks(startKeys.size());
    
    Path partitionsPath = new Path(job.getWorkingDirectory(),
        "partitions_" + System.currentTimeMillis());
    LOG.info("Writing partition information to " + partitionsPath);

    FileSystem fs = partitionsPath.getFileSystem(conf);
    writePartitions(conf, partitionsPath, startKeys);
    partitionsPath.makeQualified(fs);
    URI cacheUri;
    try {
      cacheUri = new URI(partitionsPath.toString() + "#" +
          TotalOrderPartitioner.DEFAULT_PATH);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
    DistributedCache.addCacheFile(cacheUri, conf);
    DistributedCache.createSymlink(conf);
    
    LOG.info("Incremental table output configured.");
  }
  
}
