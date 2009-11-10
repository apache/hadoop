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
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Example map/reduce job to construct index tables that can be used to quickly
 * find a row based on the value of a column. It demonstrates:
 * <ul>
 * <li>Using TableInputFormat and TableMapReduceUtil to use an HTable as input
 * to a map/reduce job.</li>
 * <li>Passing values from main method to children via the configuration.</li>
 * <li>Using MultiTableOutputFormat to output to multiple tables from a
 * map/reduce job.</li>
 * <li>A real use case of building a secondary index over a table.</li>
 * </ul>
 * 
 * <h3>Usage</h3>
 * 
 * <p>
 * Modify ${HADOOP_HOME}/conf/hadoop-env.sh to include the hbase jar, the
 * zookeeper jar, the examples output directory, and the hbase conf directory in
 * HADOOP_CLASSPATH, and then run
 * <tt><strong>bin/hadoop org.apache.hadoop.hbase.mapreduce.IndexBuilder TABLE_NAME COLUMN_FAMILY ATTR [ATTR ...]</strong></tt>
 * </p>
 * 
 * <p>
 * To run with the sample data provided in index-builder-setup.rb, use the
 * arguments <strong><tt>people attributes name email phone</tt></strong>.
 * </p>
 * 
 * <p>
 * This code was written against HBase 0.21 trunk.
 * </p>
 */
public class IndexBuilder {
  /** the column family containing the indexed row key */
  public static final byte[] INDEX_COLUMN = Bytes.toBytes("INDEX");
  /** the qualifier containing the indexed row key */
  public static final byte[] INDEX_QUALIFIER = Bytes.toBytes("ROW");

  /**
   * Internal Mapper to be run by Hadoop.
   */
  public static class Map extends
      Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Writable> {
    private byte[] family;
    private HashMap<byte[], ImmutableBytesWritable> indexes;

    @Override
    protected void map(ImmutableBytesWritable rowKey, Result result, Context context)
        throws IOException, InterruptedException {
      for(java.util.Map.Entry<byte[], ImmutableBytesWritable> index : indexes.entrySet()) {
        byte[] qualifier = index.getKey();
        ImmutableBytesWritable tableName = index.getValue();
        byte[] value = result.getValue(family, qualifier);
        if (value != null) {
          // original: row 123 attribute:phone 555-1212
          // index: row 555-1212 INDEX:ROW 123
          Put put = new Put(value);
          put.add(INDEX_COLUMN, INDEX_QUALIFIER, rowKey.get());
          context.write(tableName, put);
        }
      }
    }

    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      Configuration configuration = context.getConfiguration();
      String tableName = configuration.get("index.tablename");
      String[] fields = configuration.getStrings("index.fields");
      String familyName = configuration.get("index.familyname");
      family = Bytes.toBytes(familyName);
      indexes = new HashMap<byte[], ImmutableBytesWritable>();
      for(String field : fields) {
        // if the table is "people" and the field to index is "email", then the
        // index table will be called "people-email"
        indexes.put(Bytes.toBytes(field),
            new ImmutableBytesWritable(Bytes.toBytes(tableName + "-" + field)));
      }
    }
  }

  /**
   * Job configuration.
   */
  public static Job configureJob(Configuration conf, String [] args)
  throws IOException {
    String tableName = args[0];
    String columnFamily = args[1];
    System.out.println("****" + tableName);
    conf.set(TableInputFormat.SCAN, TableMapReduceUtil.convertScanToString(new Scan()));
    conf.set(TableInputFormat.INPUT_TABLE, tableName);
    conf.set("index.tablename", tableName);
    conf.set("index.familyname", columnFamily);
    String[] fields = new String[args.length - 2];
    for(int i = 0; i < fields.length; i++) {
      fields[i] = args[i + 2];
    }
    conf.setStrings("index.fields", fields);
    conf.set("index.familyname", "attributes");
    Job job = new Job(conf, tableName);
    job.setJarByClass(IndexBuilder.class);
    job.setMapperClass(Map.class);
    job.setNumReduceTasks(0);
    job.setInputFormatClass(TableInputFormat.class);
    job.setOutputFormatClass(MultiTableOutputFormat.class);
    return job;
  }

  public static void main(String[] args) throws Exception {
    HBaseConfiguration conf = new HBaseConfiguration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if(otherArgs.length < 3) {
      System.err.println("Only " + otherArgs.length + " arguments supplied, required: 3");
      System.err.println("Usage: IndexBuilder <TABLE_NAME> <COLUMN_FAMILY> <ATTR> [<ATTR> ...]");
      System.exit(-1);
    }
    Job job = configureJob(conf, otherArgs);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
