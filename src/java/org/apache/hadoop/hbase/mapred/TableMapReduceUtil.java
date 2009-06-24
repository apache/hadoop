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

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;

/**
 * Utility for {@link TableMap} and {@link TableReduce}
 */
@Deprecated
@SuppressWarnings("unchecked")
public class TableMapReduceUtil {
  
  /**
   * Use this before submitting a TableMap job. It will
   * appropriately set up the JobConf.
   * 
   * @param table  The table name to read from.
   * @param columns  The columns to scan.
   * @param mapper  The mapper class to use.
   * @param outputKeyClass  The class of the output key.
   * @param outputValueClass  The class of the output value.
   * @param job  The current job configuration to adjust.
   */
  public static void initTableMapJob(String table, String columns,
    Class<? extends TableMap> mapper, 
    Class<? extends WritableComparable> outputKeyClass, 
    Class<? extends Writable> outputValueClass, JobConf job) {
      
    job.setInputFormat(TableInputFormat.class);
    job.setMapOutputValueClass(outputValueClass);
    job.setMapOutputKeyClass(outputKeyClass);
    job.setMapperClass(mapper);
    FileInputFormat.addInputPaths(job, table);
    job.set(TableInputFormat.COLUMN_LIST, columns);
  }
  
  /**
   * Use this before submitting a TableReduce job. It will
   * appropriately set up the JobConf.
   * 
   * @param table  The output table.
   * @param reducer  The reducer class to use.
   * @param job  The current job configuration to adjust.
   * @throws IOException When determining the region count fails. 
   */
  public static void initTableReduceJob(String table,
    Class<? extends TableReduce> reducer, JobConf job)
  throws IOException {
    initTableReduceJob(table, reducer, job, null);
  }

  /**
   * Use this before submitting a TableReduce job. It will
   * appropriately set up the JobConf.
   * 
   * @param table  The output table.
   * @param reducer  The reducer class to use.
   * @param job  The current job configuration to adjust.
   * @param partitioner  Partitioner to use. Pass <code>null</code> to use 
   * default partitioner.
   * @throws IOException When determining the region count fails. 
   */
  public static void initTableReduceJob(String table,
    Class<? extends TableReduce> reducer, JobConf job, Class partitioner)
  throws IOException {
    job.setOutputFormat(TableOutputFormat.class);
    job.setReducerClass(reducer);
    job.set(TableOutputFormat.OUTPUT_TABLE, table);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(BatchUpdate.class);
    if (partitioner == HRegionPartitioner.class) {
      job.setPartitionerClass(HRegionPartitioner.class);
      HTable outputTable = new HTable(new HBaseConfiguration(job), table);
      int regions = outputTable.getRegionsInfo().size();
      if (job.getNumReduceTasks() > regions) {
        job.setNumReduceTasks(outputTable.getRegionsInfo().size());
      }
    } else if (partitioner != null) {
      job.setPartitionerClass(partitioner);
    }
  }
  
  /**
   * Ensures that the given number of reduce tasks for the given job 
   * configuration does not exceed the number of regions for the given table. 
   * 
   * @param table  The table to get the region count for.
   * @param job  The current job configuration to adjust.
   * @throws IOException When retrieving the table details fails.
   */
  public static void limitNumReduceTasks(String table, JobConf job) 
  throws IOException { 
    HTable outputTable = new HTable(new HBaseConfiguration(job), table);
    int regions = outputTable.getRegionsInfo().size();
    if (job.getNumReduceTasks() > regions)
      job.setNumReduceTasks(regions);
  }

  /**
   * Ensures that the given number of map tasks for the given job 
   * configuration does not exceed the number of regions for the given table. 
   * 
   * @param table  The table to get the region count for.
   * @param job  The current job configuration to adjust.
   * @throws IOException When retrieving the table details fails.
   */
  public static void limitNumMapTasks(String table, JobConf job) 
  throws IOException { 
    HTable outputTable = new HTable(new HBaseConfiguration(job), table);
    int regions = outputTable.getRegionsInfo().size();
    if (job.getNumMapTasks() > regions)
      job.setNumMapTasks(regions);
  }

  /**
   * Sets the number of reduce tasks for the given job configuration to the 
   * number of regions the given table has. 
   * 
   * @param table  The table to get the region count for.
   * @param job  The current job configuration to adjust.
   * @throws IOException When retrieving the table details fails.
   */
  public static void setNumReduceTasks(String table, JobConf job) 
  throws IOException { 
    HTable outputTable = new HTable(new HBaseConfiguration(job), table);
    int regions = outputTable.getRegionsInfo().size();
    job.setNumReduceTasks(regions);
  }
  
  /**
   * Sets the number of map tasks for the given job configuration to the 
   * number of regions the given table has. 
   * 
   * @param table  The table to get the region count for.
   * @param job  The current job configuration to adjust.
   * @throws IOException When retrieving the table details fails.
   */
  public static void setNumMapTasks(String table, JobConf job) 
  throws IOException { 
    HTable outputTable = new HTable(new HBaseConfiguration(job), table);
    int regions = outputTable.getRegionsInfo().size();
    job.setNumMapTasks(regions);
  }

  /**
   * Sets the number of rows to return and cache with each scanner iteration.
   * Higher caching values will enable faster mapreduce jobs at the expense of
   * requiring more heap to contain the cached rows.
   * 
   * @param job The current job configuration to adjust.
   * @param batchSize The number of rows to return in batch with each scanner
   * iteration.
   */
  public static void setScannerCaching(JobConf job, int batchSize) {
    job.setInt("hbase.client.scanner.caching", batchSize);
  }
}