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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Sample Uploader MapReduce
 * <p>
 * This is EXAMPLE code.  You will need to change it to work for your context.
 * <p>
 * Uses {@link TableReducer} to put the data into HBase. Change the InputFormat 
 * to suit your data.  In this example, we are importing a CSV file.
 * <p>
 * <pre>row,family,qualifier,value</pre>
 * <p>
 * The table and columnfamily we're to insert into must preexist.
 * <p>
 * There is no reducer in this example as it is not necessary and adds 
 * significant overhead.  If you need to do any massaging of data before
 * inserting into HBase, you can do this in the map as well.
 * <p>Do the following to start the MR job:
 * <pre>
 * ./bin/hadoop org.apache.hadoop.hbase.mapreduce.SampleUploader /tmp/input.csv TABLE_NAME
 * </pre>
 * <p>
 * This code was written against HBase 0.21 trunk.
 */
public class SampleUploader {

  private static final String NAME = "SampleUploader";
  
  static class Uploader 
  extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

    private long checkpoint = 100;
    private long count = 0;
    
    @Override
    public void map(LongWritable key, Text line, Context context)
    throws IOException {
      
      // Input is a CSV file
      // Each map() is a single line, where the key is the line number
      // Each line is comma-delimited; row,family,qualifier,value
            
      // Split CSV line
      String [] values = line.toString().split(",");
      if(values.length != 4) {
        return;
      }
      
      // Extract each value
      byte [] row = Bytes.toBytes(values[0]);
      byte [] family = Bytes.toBytes(values[1]);
      byte [] qualifier = Bytes.toBytes(values[2]);
      byte [] value = Bytes.toBytes(values[3]);
      
      // Create Put
      Put put = new Put(row);
      put.add(family, qualifier, value);
      
      // Uncomment below to disable WAL. This will improve performance but means
      // you will experience data loss in the case of a RegionServer crash.
      // put.setWriteToWAL(false);
      
      try {
        context.write(new ImmutableBytesWritable(row), put);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      
      // Set status every checkpoint lines
      if(++count % checkpoint == 0) {
        context.setStatus("Emitting Put " + count);
      }
    }
  }
  
  /**
   * Job configuration.
   */
  public static Job configureJob(Configuration conf, String [] args)
  throws IOException {
    Path inputPath = new Path(args[0]);
    String tableName = args[1];
    Job job = new Job(conf, NAME + "_" + tableName);
    job.setJarByClass(Uploader.class);
    FileInputFormat.setInputPaths(job, inputPath);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapperClass(Uploader.class);
    // No reducers.  Just write straight to table.  Call initTableReducerJob
    // because it sets up the TableOutputFormat.
    TableMapReduceUtil.initTableReducerJob(tableName, null, job);
    job.setNumReduceTasks(0);
    return job;
  }

  /**
   * Main entry point.
   * 
   * @param args  The command line parameters.
   * @throws Exception When running the job fails.
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if(otherArgs.length != 2) {
      System.err.println("Wrong number of arguments: " + otherArgs.length);
      System.err.println("Usage: " + NAME + " <input> <tablename>");
      System.exit(-1);
    }
    Job job = configureJob(conf, otherArgs);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
