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

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.ImportTsv.TsvParser.BadTsvLineException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

/**
 * Tool to import data from a TSV file.
 *
 * This tool is rather simplistic - it doesn't do any quoting or
 * escaping, but is useful for many data loads.
 *
 * @see ImportTsv#usage(String)
 */
public class ImportTsv {
  final static String NAME = "importtsv";

  final static String SKIP_LINES_CONF_KEY = "importtsv.skip.bad.lines";
  final static String BULK_OUTPUT_CONF_KEY = "importtsv.bulk.output";
  final static String COLUMNS_CONF_KEY = "importtsv.columns";

  static class TsvParser {
    /**
     * Column families and qualifiers mapped to the TSV columns
     */
    private byte[][] families;
    private byte[][] qualifiers;

    private int rowKeyColumnIndex;
    
    public static String ROWKEY_COLUMN_SPEC="HBASE_ROW_KEY";

    /**
     * @param columnsSpecification the list of columns to parser out, comma separated.
     * The row key should be the special token TsvParser.ROWKEY_COLUMN_SPEC
     */
    public TsvParser(String columnsSpecification) {
      ArrayList<String> columnStrings = Lists.newArrayList(
        Splitter.on(',').trimResults().split(columnsSpecification));
      
      families = new byte[columnStrings.size()][];
      qualifiers = new byte[columnStrings.size()][];

      for (int i = 0; i < columnStrings.size(); i++) {
        String str = columnStrings.get(i);
        if (ROWKEY_COLUMN_SPEC.equals(str)) {
          rowKeyColumnIndex = i;
          continue;
        }
        String[] parts = str.split(":", 2);
        if (parts.length == 1) {
          families[i] = str.getBytes();
          qualifiers[i] = HConstants.EMPTY_BYTE_ARRAY;
        } else {
          families[i] = parts[0].getBytes();
          qualifiers[i] = parts[1].getBytes();
        }
      }
    }
    
    public int getRowKeyColumnIndex() {
      return rowKeyColumnIndex;
    }
    public byte[] getFamily(int idx) {
      return families[idx];
    }
    public byte[] getQualifier(int idx) {
      return qualifiers[idx];
    }
    
    public ParsedLine parse(byte[] lineBytes, int length)
    throws BadTsvLineException {
      // Enumerate separator offsets
      ArrayList<Integer> tabOffsets = new ArrayList<Integer>(families.length);
      for (int i = 0; i < length; i++) {
        if (lineBytes[i] == '\t') {
          tabOffsets.add(i);
        }
      }
      tabOffsets.add(length);
      if (tabOffsets.size() > families.length) {
        throw new BadTsvLineException("Bad line:\n");
      }

      return new ParsedLine(tabOffsets, lineBytes);
    }
    
    class ParsedLine {
      private final ArrayList<Integer> tabOffsets;
      private byte[] lineBytes;
      
      ParsedLine(ArrayList<Integer> tabOffsets, byte[] lineBytes) {
        this.tabOffsets = tabOffsets;
        this.lineBytes = lineBytes;
      }
      
      public int getRowKeyOffset() {
        return getColumnOffset(rowKeyColumnIndex);
      }
      public int getRowKeyLength() {
        return getColumnLength(rowKeyColumnIndex);
      }
      public int getColumnOffset(int idx) {
        if (idx > 0)
          return tabOffsets.get(idx - 1) + 1;
        else
          return 0;
      }      
      public int getColumnLength(int idx) {
        return tabOffsets.get(idx) - getColumnOffset(idx);
      }
      public int getColumnCount() {
        return tabOffsets.size();
      }
      public byte[] getLineBytes() {
        return lineBytes;
      }
    }
    
    public static class BadTsvLineException extends Exception {
      public BadTsvLineException(String err) {
        super(err);
      }
      private static final long serialVersionUID = 1L;
    }
  }
  
  /**
   * Write table content out to files in hdfs.
   */
  static class TsvImporter
  extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>
  {
    
    /** Timestamp for all inserted rows */
    private long ts;

    /** Should skip bad lines */
    private boolean skipBadLines;
    private Counter badLineCount;

    private TsvParser parser;

    @Override
    protected void setup(Context context) {
      parser = new TsvParser(context.getConfiguration().get(
                               COLUMNS_CONF_KEY));
      if (parser.getRowKeyColumnIndex() == -1) {
        throw new RuntimeException("No row key column specified");
      }
      ts = System.currentTimeMillis();

      skipBadLines = context.getConfiguration().getBoolean(
        SKIP_LINES_CONF_KEY, true);
      badLineCount = context.getCounter("ImportTsv", "Bad Lines");
    }

    /**
     * Convert a line of TSV text into an HBase table row.
     */
    @Override
    public void map(LongWritable offset, Text value,
      Context context)
    throws IOException {
      byte[] lineBytes = value.getBytes();

      try {
        TsvParser.ParsedLine parsed = parser.parse(
            lineBytes, value.getLength());
        ImmutableBytesWritable rowKey =
          new ImmutableBytesWritable(lineBytes,
              parsed.getRowKeyOffset(),
              parsed.getRowKeyLength());

        Put put = new Put(rowKey.copyBytes());
        for (int i = 0; i < parsed.getColumnCount(); i++) {
          if (i == parser.getRowKeyColumnIndex()) continue;
          KeyValue kv = new KeyValue(
              lineBytes, parsed.getRowKeyOffset(), parsed.getRowKeyLength(),
              parser.getFamily(i), 0, parser.getFamily(i).length,
              parser.getQualifier(i), 0, parser.getQualifier(i).length,
              ts,
              KeyValue.Type.Put,
              lineBytes, parsed.getColumnOffset(i), parsed.getColumnLength(i));
          put.add(kv);
        }
        context.write(rowKey, put);
      } catch (BadTsvLineException badLine) {
        if (skipBadLines) {
          System.err.println(
              "Bad line at offset: " + offset.get() + ":\n" +
              badLine.getMessage());
          badLineCount.increment(1);
          return;
        } else {
          throw new IOException(badLine);
        }
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
    Path inputDir = new Path(args[1]);
    Job job = new Job(conf, NAME + "_" + tableName);
    job.setJarByClass(TsvImporter.class);
    FileInputFormat.setInputPaths(job, inputDir);
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapperClass(TsvImporter.class);

    String hfileOutPath = conf.get(BULK_OUTPUT_CONF_KEY);
    if (hfileOutPath != null) {
      HTable table = new HTable(conf, tableName);
      job.setReducerClass(PutSortReducer.class);
      Path outputDir = new Path(hfileOutPath);
      FileOutputFormat.setOutputPath(job, outputDir);
      job.setMapOutputKeyClass(ImmutableBytesWritable.class);
      job.setMapOutputValueClass(Put.class);
      HFileOutputFormat.configureIncrementalLoad(job, table);
    } else {
      // No reducers.  Just write straight to table.  Call initTableReducerJob
      // to set up the TableOutputFormat.
      TableMapReduceUtil.initTableReducerJob(tableName, null, job);
      job.setNumReduceTasks(0);
    }
    
    TableMapReduceUtil.addDependencyJars(job);
    return job;
  }

  /*
   * @param errorMsg Error message.  Can be null.
   */
  private static void usage(final String errorMsg) {
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR: " + errorMsg);
    }
    String usage = 
      "Usage: " + NAME + " -Dimporttsv.columns=a,b,c <tablename> <inputdir>\n" +
      "\n" +
      "Imports the given input directory of TSV data into the specified table.\n" +
      "\n" +
      "The column names of the TSV data must be specified using the -Dimporttsv.columns\n" +
      "option. This option takes the form of comma-separated column names, where each\n" +
      "column name is either a simple column family, or a columnfamily:qualifier. The special\n" +
      "column name HBASE_ROW_KEY is used to designate that this column should be used\n" +
      "as the row key for each imported record. You must specify exactly one column\n" +
      "to be the row key.\n" +
      "\n" +
      "In order to prepare data for a bulk data load, pass the option:\n" +
      "  -D" + BULK_OUTPUT_CONF_KEY + "=/path/for/output\n" +
      "\n" +
      "Other options that may be specified with -D include:\n" +
      "  -D" + SKIP_LINES_CONF_KEY + "=false - fail if encountering an invalid line";
    System.err.println(usage);
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

    // Make sure columns are specified
    String columns[] = conf.getStrings(COLUMNS_CONF_KEY);
    if (columns == null) {
      usage("No columns specified. Please specify with -D" +
            COLUMNS_CONF_KEY+"=...");
      System.exit(-1);
    }

    // Make sure they specify exactly one column as the row key
    int rowkeysFound=0;
    for (String col : columns) {
      if (col.equals(TsvParser.ROWKEY_COLUMN_SPEC)) rowkeysFound++;
    }
    if (rowkeysFound != 1) {
      usage("Must specify exactly one column as " + TsvParser.ROWKEY_COLUMN_SPEC);
      System.exit(-1);
    }

    Job job = createSubmittableJob(conf, otherArgs);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}
