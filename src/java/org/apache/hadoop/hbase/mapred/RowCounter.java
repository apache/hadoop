/**
 * Copyright 2008 The Apache Software Foundation
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
package org.apache.hadoop.hbase.mapred;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.HbaseMapWritable;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A job with a map to count rows.
 * Map outputs table rows IF the input row has columns that have content.  
 * Uses an {@link IdentityReducer}
 */
public class RowCounter
extends MapReduceBase
implements TableMap<ImmutableBytesWritable, RowResult>, Tool {
  /* Name of this 'program'
   */
  static final String NAME = "rowcounter";
  
  private Configuration conf;
  private final RowResult EMPTY_RESULT_VALUE = 
        new RowResult(Bytes.toBytes("dummy"),new HbaseMapWritable<byte [], Cell>());
  private static enum Counters {ROWS}
  
  public void map(ImmutableBytesWritable row, RowResult value,
    OutputCollector<ImmutableBytesWritable, RowResult> output,
    Reporter reporter)
  throws IOException {
    boolean content = false;
    for (Map.Entry<byte [], Cell> e: value.entrySet()) {
      Cell cell = e.getValue();
      if (cell != null && cell.getValue().length > 0) {
        content = true;
        break;
      }
    }
    if (!content) {
      return;
    }
    // Give out same value every time.  We're only interested in the row/key
    reporter.incrCounter(Counters.ROWS, 1);
    output.collect(row, EMPTY_RESULT_VALUE);
  }

  /**
   * @param args
   * @return the JobConf
   * @throws IOException
   */
  @SuppressWarnings("unused")
  public JobConf createSubmittableJob(String[] args) throws IOException {
    JobConf c = new JobConf(getConf(), RowCounter.class);
    c.setJobName(NAME);
    // Columns are space delimited
    StringBuilder sb = new StringBuilder();
    final int columnoffset = 2;
    for (int i = columnoffset; i < args.length; i++) {
      if (i > columnoffset) {
        sb.append(" ");
      }
      sb.append(args[i]);
    }
    // Second argument is the table name.
    TableMapReduceUtil.initTableMapJob(args[1], sb.toString(), this.getClass(),
      ImmutableBytesWritable.class, RowResult.class, c);
    c.setReducerClass(IdentityReducer.class);
    // First arg is the output directory.
    FileOutputFormat.setOutputPath(c, new Path(args[0]));
    return c;
  }
  
  static int printUsage() {
    System.out.println(NAME +
      " <outputdir> <tablename> <column1> [<column2>...]");
    return -1;
  }
  
  public int run(final String[] args) throws Exception {
    // Make sure there are at least 3 parameters
    if (args.length < 3) {
      System.err.println("ERROR: Wrong number of parameters: " + args.length);
      return printUsage();
    }
    JobClient.runJob(createSubmittableJob(args));
    return 0;
  }

  public Configuration getConf() {
    return this.conf;
  }

  public void setConf(final Configuration c) {
    this.conf = c;
  }

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    HBaseConfiguration c = new HBaseConfiguration();
    int errCode = ToolRunner.run(c, new RowCounter(), args);
    System.exit(errCode);
  }
}
