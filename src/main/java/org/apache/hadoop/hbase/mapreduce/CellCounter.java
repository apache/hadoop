/**
 * Copyright 2011 The Apache Software Foundation
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;


/**
 * A job with a a map and reduce phase to count cells in a table.
 * The counter lists the following stats for a given table:
 * <pre>
 * 1. Total number of rows in the table
 * 2. Total number of CFs across all rows
 * 3. Total qualifiers across all rows
 * 4. Total occurrence of each CF
 * 5. Total occurrence  of each qualifier
 * 6. Total number of versions of each qualifier.
 * </pre>
 *
 * The cellcounter takes two optional parameters one to use a user
 * supplied row/family/qualifier string to use in the report and
 * second a regex based or prefix based row filter to restrict the
 * count operation to a limited subset of rows from the table.
 */
public class CellCounter {
  private static final Log LOG =
    LogFactory.getLog(CellCounter.class.getName());


  /**
   * Name of this 'program'.
   */
  static final String NAME = "CellCounter";

  /**
   * Mapper that runs the count.
   */
  static class CellCounterMapper
  extends TableMapper<Text, IntWritable> {
    /**
     * Counter enumeration to count the actual rows.
     */
    public static enum Counters {
      ROWS
    }

    /**
     * Maps the data.
     *
     * @param row     The current table row key.
     * @param values  The columns.
     * @param context The current context.
     * @throws IOException When something is broken with the data.
     * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
     *      org.apache.hadoop.mapreduce.Mapper.Context)
     */

    @Override
    public void map(ImmutableBytesWritable row, Result values,
                    Context context)
        throws IOException {
      String currentFamilyName = null;
      String currentQualifierName = null;
      String currentRowKey = null;
      Configuration config = context.getConfiguration();
      String separator = config.get("ReportSeparator",":");

      try {
        if (values != null) {
          context.getCounter(Counters.ROWS).increment(1);
          context.write(new Text("Total ROWS"), new IntWritable(1));
        }

        for (KeyValue value : values.list()) {
          currentRowKey = Bytes.toStringBinary(value.getRow());
          String thisRowFamilyName = Bytes.toStringBinary(value.getFamily());
          if (thisRowFamilyName != null &&
              !thisRowFamilyName.equals(currentFamilyName)) {
            currentFamilyName = thisRowFamilyName;
            context.getCounter("CF", thisRowFamilyName).increment(1);
            context.write(new Text("Total Families Across all Rows"),
              new IntWritable(1));
            context.write(new Text(thisRowFamilyName), new IntWritable(1));
          }
          String thisRowQualifierName =
            thisRowFamilyName + separator + Bytes.toStringBinary(value.getQualifier());
          if (thisRowQualifierName != null &&
              !thisRowQualifierName.equals(currentQualifierName)) {
            currentQualifierName = thisRowQualifierName;
            context.getCounter("CFQL", thisRowQualifierName).increment(1);
            context.write(new Text("Total Qualifiers across all Rows"),
              new IntWritable(1));
            context.write(new Text(thisRowQualifierName), new IntWritable(1));
            // Intialize versions
            context.getCounter("QL_VERSIONS", currentRowKey + separator +
              thisRowQualifierName).increment(1);
            context.write(new Text(currentRowKey + separator + thisRowQualifierName +
              "_Versions"), new IntWritable(1));

          } else {
            // Increment versions
            currentQualifierName = thisRowQualifierName;
            context.getCounter("QL_VERSIONS", currentRowKey + separator +
              thisRowQualifierName).increment(1);
            context.write(new Text(currentRowKey + separator + thisRowQualifierName + "_Versions"),
              new IntWritable(1));
          }
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  static class IntSumReducer<Key> extends Reducer<Key, IntWritable,
      Key, IntWritable> {

    private IntWritable result = new IntWritable();
    public void reduce(Key key, Iterable<IntWritable> values,
      Context context)
    throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  /**
   * Sets up the actual job.
   *
   * @param conf The current configuration.
   * @param args The command line parameters.
   * @return The newly created job.
   * @throws IOException When setting up the job fails.
   */
  public static Job createSubmittableJob(Configuration conf, String[] args)
      throws IOException {
    String tableName = args[0];
    Path outputDir = new Path(args[1]);
    String reportSeparatorString = (args.length > 2) ? args[2]: ":";
    conf.set("ReportSeparator", reportSeparatorString);
    Job job = new Job(conf, NAME + "_" + tableName);
    job.setJarByClass(CellCounter.class);
    Scan scan = getConfiguredScanForJob(conf, args);
    TableMapReduceUtil.initTableMapperJob(tableName, scan,
        CellCounterMapper.class, ImmutableBytesWritable.class, Result.class, job);
    job.setNumReduceTasks(1);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileOutputFormat.setOutputPath(job, outputDir);
    job.setReducerClass(IntSumReducer.class);
    return job;
  }

  private static Scan getConfiguredScanForJob(Configuration conf, String[] args) throws IOException {
    Scan s = new Scan();
    // Set Scan Versions
    s.setMaxVersions(Integer.MAX_VALUE);
    s.setCacheBlocks(false);
    // Set Scan Column Family
    if (conf.get(TableInputFormat.SCAN_COLUMN_FAMILY) != null) {
      s.addFamily(Bytes.toBytes(conf.get(TableInputFormat.SCAN_COLUMN_FAMILY)));
    }
    // Set RowFilter or Prefix Filter if applicable.
    Filter rowFilter = getRowFilter(args);
    if (rowFilter!= null) {
      LOG.info("Setting Row Filter for counter.");
      s.setFilter(rowFilter);
    }
    return s;
  }


  private static Filter getRowFilter(String[] args) {
    Filter rowFilter = null;
    String filterCriteria = (args.length > 3) ? args[3]: null;
    if (filterCriteria == null) return null;
    if (filterCriteria.startsWith("^")) {
      String regexPattern = filterCriteria.substring(1, filterCriteria.length());
      rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regexPattern));
    } else {
      rowFilter = new PrefixFilter(Bytes.toBytes(filterCriteria));
    }
    return rowFilter;
  }

  /**
   * Main entry point.
   *
   * @param args The command line parameters.
   * @throws Exception When running the job fails.
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 1) {
      System.err.println("ERROR: Wrong number of parameters: " + args.length);
      System.err.println("Usage: CellCounter <tablename> <outputDir> <reportSeparator> " +
          "[^[regex pattern] or [Prefix] for row filter]] ");
      System.err.println("  Note: -D properties will be applied to the conf used. ");
      System.err.println("  Additionally, the following SCAN properties can be specified");
      System.err.println("  to get fine grained control on what is counted..");
      System.err.println("   -D " + TableInputFormat.SCAN_COLUMN_FAMILY + "=<familyName>");
      System.err.println(" <reportSeparator> parameter can be used to override the default report separator " +
          "string : used to separate the rowId/column family name and qualifier name.");
      System.err.println(" [^[regex pattern] or [Prefix] parameter can be used to limit the cell counter count " +
          "operation to a limited subset of rows from the table based on regex or prefix pattern.");
      System.exit(-1);
    }
    Job job = createSubmittableJob(conf, otherArgs);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
