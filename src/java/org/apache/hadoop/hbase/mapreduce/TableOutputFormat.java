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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Convert Map/Reduce output and write it to an HBase table.
 */
public class TableOutputFormat extends
    FileOutputFormat<ImmutableBytesWritable, Put> {

  private final Log LOG = LogFactory.getLog(TableOutputFormat.class);
  /** Job parameter that specifies the output table. */
  public static final String OUTPUT_TABLE = "hbase.mapred.outputtable";

  /**
   * Convert Reduce output (key, value) to (HStoreKey, KeyedDataArrayWritable) 
   * and write to an HBase table
   */
  protected static class TableRecordWriter
    extends RecordWriter<ImmutableBytesWritable, Put> {
    
    /** The table to write to. */
    private HTable table;

    /**
     * Instantiate a TableRecordWriter with the HBase HClient for writing.
     * 
     * @param table  The table to write to.
     */
    public TableRecordWriter(HTable table) {
      this.table = table;
    }

    /**
     * Closes the writer, in this case flush table commits.
     * 
     * @param context  The context.
     * @throws IOException When closing the writer fails.
     * @see org.apache.hadoop.mapreduce.RecordWriter#close(org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    @Override
    public void close(TaskAttemptContext context) 
    throws IOException {
      table.flushCommits();
    }

    /**
     * Writes a key/value pair into the table.
     * 
     * @param key  The key.
     * @param value  The value.
     * @throws IOException When writing fails.
     * @see org.apache.hadoop.mapreduce.RecordWriter#write(java.lang.Object, java.lang.Object)
     */
    @Override
    public void write(ImmutableBytesWritable key, Put value) 
    throws IOException {
      table.put(new Put(value));
    }
  }
  
  /**
   * Creates a new record writer.
   * 
   * @param context  The current task context.
   * @return The newly created writer instance.
   * @throws IOException When creating the writer fails.
   * @throws InterruptedException When the jobs is cancelled.
   * @see org.apache.hadoop.mapreduce.lib.output.FileOutputFormat#getRecordWriter(org.apache.hadoop.mapreduce.TaskAttemptContext)
   */
  public RecordWriter<ImmutableBytesWritable, Put> getRecordWriter(
    TaskAttemptContext context) 
  throws IOException, InterruptedException {
    // expecting exactly one path
    String tableName = context.getConfiguration().get(OUTPUT_TABLE);
    HTable table = null;
    try {
      table = new HTable(new HBaseConfiguration(context.getConfiguration()), 
        tableName);
    } catch(IOException e) {
      LOG.error(e);
      throw e;
    }
    table.setAutoFlush(false);
    return new TableRecordWriter(table);
  }
  
}
