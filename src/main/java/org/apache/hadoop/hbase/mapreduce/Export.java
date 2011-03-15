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
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
* Export an HBase table.
* Writes content to sequence files up in HDFS.  Use {@link Import} to read it
* back in again.
*/
public class Export {
  private static final Log LOG = LogFactory.getLog(Export.class);
  final static String NAME = "export";

  /**
   * Mapper.
   */
  static class Exporter
  extends TableMapper<ImmutableBytesWritable, Result> {
    /**
     * @param row  The current table row key.
     * @param value  The columns.
     * @param context  The current context.
     * @throws IOException When something is broken with the data.
     * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
     *   org.apache.hadoop.mapreduce.Mapper.Context)
     */
    @Override
    public void map(ImmutableBytesWritable row, Result value,
      Context context)
    throws IOException {
      try {
        context.write(row, value);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Sets up the actual job.
   *
   * @param conf  The current configuration.
   * @param args  The command line parameters.
   * @return The newly created job.
   * @throws IOException When setting up the job fails.
   */
  public static Job createSubmittableJob(Configuration conf, String[] args)
  throws IOException {
    String tableName = args[0];
    Path outputDir = new Path(args[1]);
    Job job = new Job(conf, NAME + "_" + tableName);
    job.setJobName(NAME + "_" + tableName);
    job.setJarByClass(Exporter.class);
    // Set optional scan parameters
    Scan s = getConfiguredScanForJob(conf, args);
    TableMapReduceUtil.initTableMapperJob(tableName, s, Exporter.class, null,
      null, job);
    // No reducers.  Just write straight to output files.
    job.setNumReduceTasks(0);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(Result.class);
    FileOutputFormat.setOutputPath(job, outputDir);
    return job;
  }

  private static Scan getConfiguredScanForJob(Configuration conf, String[] args) throws IOException {
    Scan s = new Scan();
    // Optional arguments.
    // Set Scan Versions
    int versions = args.length > 2? Integer.parseInt(args[2]): 1;
    s.setMaxVersions(versions);
    // Set Scan Range
    long startTime = args.length > 3? Long.parseLong(args[3]): 0L;
    long endTime = args.length > 4? Long.parseLong(args[4]): Long.MAX_VALUE;
    s.setTimeRange(startTime, endTime);
    // Set cache blocks
    s.setCacheBlocks(false);
    // Set Scan Column Family
    if (conf.get(TableInputFormat.SCAN_COLUMN_FAMILY) != null) {
      s.addFamily(Bytes.toBytes(conf.get(TableInputFormat.SCAN_COLUMN_FAMILY)));
    }
    // Set RowFilter or Prefix Filter if applicable.
    Filter exportFilter = getExportFilter(args);
    if (exportFilter!= null) {
        LOG.info("Setting Scan Filter for Export.");
      s.setFilter(exportFilter);
    }
    LOG.info("verisons=" + versions + ", starttime=" + startTime +
      ", endtime=" + endTime);
    return s;
  }

  private static Filter getExportFilter(String[] args) {
    Filter exportFilter = null;
    String filterCriteria = (args.length > 5) ? args[5]: null;
    if (filterCriteria == null) return null;
    if (filterCriteria.startsWith("^")) {
      String regexPattern = filterCriteria.substring(1, filterCriteria.length());
      exportFilter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(regexPattern));
    } else {
      exportFilter = new PrefixFilter(Bytes.toBytes(filterCriteria));
    }
    return exportFilter;
  }

  /*
   * @param errorMsg Error message.  Can be null.
   */
  private static void usage(final String errorMsg) {
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR: " + errorMsg);
    }
    System.err.println("Usage: Export [-D <property=value>]* <tablename> <outputdir> [<versions> " +
      "[<starttime> [<endtime>]] [^[regex pattern] or [Prefix] to filter]]\n");
    System.err.println("  Note: -D properties will be applied to the conf used. ");
    System.err.println("  For example: ");
    System.err.println("   -D mapred.output.compress=true");
    System.err.println("   -D mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec");
    System.err.println("   -D mapred.output.compression.type=BLOCK");
    System.err.println("  Additionally, the following SCAN properties can be specified");
    System.err.println("  to control/limit what is exported..");
    System.err.println("   -D " + TableInputFormat.SCAN_COLUMN_FAMILY + "=<familyName>");
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
    if (otherArgs.length < 2) {
      usage("Wrong number of arguments: " + otherArgs.length);
      System.exit(-1);
    }
    Job job = createSubmittableJob(conf, otherArgs);
    System.exit(job.waitForCompletion(true)? 0 : 1);
  }
}