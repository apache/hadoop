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
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mortbay.log.Log;

/**
 * Writes HFiles. Passed KeyValues must arrive in order.
 * Currently, can only write files to a single column family at a
 * time.  Multiple column families requires coordinating keys cross family.
 * Writes current time as the sequence id for the file. Sets the major compacted
 * attribute on created hfiles.
 * @see KeyValueSortReducer
 */
public class HFileOutputFormat extends FileOutputFormat<ImmutableBytesWritable, KeyValue> {
  public RecordWriter<ImmutableBytesWritable, KeyValue> getRecordWriter(TaskAttemptContext context)
  throws IOException, InterruptedException {
    // Get the path of the temporary output file 
    final Path outputdir = FileOutputFormat.getOutputPath(context);
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
          Log.info("Writer=" + wl.writer.getPath() +
            ((wl.written == 0)? "": ", wrote=" + wl.written));
          wl.written = 0;
        }
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
          StoreFile.appendMetadata(w, System.currentTimeMillis(), true);
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
}
