/**
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

package org.apache.hadoop.examples.terasort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.zip.Checksum;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.util.PureJavaCrc32;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Generate the official GraySort input data set.
 * The user specifies the number of rows and the output directory and this
 * class runs a map/reduce program to generate the data.
 * The format of the data is:
 * <ul>
 * <li>(10 bytes key) (constant 2 bytes) (32 bytes rowid) 
 *     (constant 4 bytes) (48 bytes filler) (constant 4 bytes)
 * <li>The rowid is the right justified row id as a hex number.
 * </ul>
 *
 * <p>
 * To run the program: 
 * <b>bin/hadoop jar hadoop-*-examples.jar teragen 10000000000 in-dir</b>
 */
public class TeraGen extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(TeraSort.class);

  public static enum Counters {CHECKSUM}

  public static String NUM_ROWS = "mapreduce.terasort.num-rows";
  /**
   * An input format that assigns ranges of longs to each mapper.
   */
  static class RangeInputFormat 
       implements InputFormat<LongWritable, NullWritable> {
    
    /**
     * An input split consisting of a range on numbers.
     */
    static class RangeInputSplit implements InputSplit {
      long firstRow;
      long rowCount;

      public RangeInputSplit() { }

      public RangeInputSplit(long offset, long length) {
        firstRow = offset;
        rowCount = length;
      }

      public long getLength() throws IOException {
        return 0;
      }

      public String[] getLocations() throws IOException {
        return new String[]{};
      }

      public void readFields(DataInput in) throws IOException {
        firstRow = WritableUtils.readVLong(in);
        rowCount = WritableUtils.readVLong(in);
      }

      public void write(DataOutput out) throws IOException {
        WritableUtils.writeVLong(out, firstRow);
        WritableUtils.writeVLong(out, rowCount);
      }
    }
    
    /**
     * A record reader that will generate a range of numbers.
     */
    static class RangeRecordReader 
          implements RecordReader<LongWritable, NullWritable> {
      long startRow;
      long finishedRows;
      long totalRows;

      public RangeRecordReader(RangeInputSplit split) {
        startRow = split.firstRow;
        finishedRows = 0;
        totalRows = split.rowCount;
      }

      public void close() throws IOException {
        // NOTHING
      }

      public LongWritable createKey() {
        return new LongWritable();
      }

      public NullWritable createValue() {
        return NullWritable.get();
      }

      public long getPos() throws IOException {
        return finishedRows;
      }

      public float getProgress() throws IOException {
        return finishedRows / (float) totalRows;
      }

      public boolean next(LongWritable key, 
                          NullWritable value) {
        if (finishedRows < totalRows) {
          key.set(startRow + finishedRows);
          finishedRows += 1;
          return true;
        } else {
          return false;
        }
      }
      
    }

    public RecordReader<LongWritable, NullWritable> 
      getRecordReader(InputSplit split, JobConf job,
                      Reporter reporter) throws IOException {
      return new RangeRecordReader((RangeInputSplit) split);
    }

    /**
     * Create the desired number of splits, dividing the number of rows
     * between the mappers.
     */
    public InputSplit[] getSplits(JobConf job, 
                                  int numSplits) {
      long totalRows = getNumberOfRows(job);
      LOG.info("Generating " + totalRows + " using " + numSplits);
      InputSplit[] splits = new InputSplit[numSplits];
      long currentRow = 0;
      for(int split=0; split < numSplits; ++split) {
        long goal = (long) Math.ceil(totalRows * (double)(split+1) / numSplits);
        splits[split] = new RangeInputSplit(currentRow, goal - currentRow);
        currentRow = goal;
      }
      return splits;
    }

  }
  
  static long getNumberOfRows(JobConf job) {
    return job.getLong(NUM_ROWS, 0);
  }
  
  static void setNumberOfRows(JobConf job, long numRows) {
    job.setLong(NUM_ROWS, numRows);
  }

  /**
   * The Mapper class that given a row number, will generate the appropriate 
   * output line.
   */
  public static class SortGenMapper extends MapReduceBase 
      implements Mapper<LongWritable, NullWritable, Text, Text> {

    private Text key = new Text();
    private Text value = new Text();
    private Unsigned16 rand = null;
    private Unsigned16 rowId = null;
    private Unsigned16 checksum = new Unsigned16();
    private Checksum crc32 = new PureJavaCrc32();
    private Unsigned16 total = new Unsigned16();
    private static final Unsigned16 ONE = new Unsigned16(1);
    private byte[] buffer = new byte[TeraInputFormat.KEY_LENGTH +
                                     TeraInputFormat.VALUE_LENGTH];
    private Counter checksumCounter;

    public void map(LongWritable row, NullWritable ignored,
                    OutputCollector<Text, Text> output,
                    Reporter reporter) throws IOException {
      if (rand == null) {
        rowId = new Unsigned16(row.get());
        rand = Random16.skipAhead(rowId);
        checksumCounter = reporter.getCounter(Counters.CHECKSUM);
      }
      Random16.nextRand(rand);
      GenSort.generateRecord(buffer, rand, rowId);
      key.set(buffer, 0, TeraInputFormat.KEY_LENGTH);
      value.set(buffer, TeraInputFormat.KEY_LENGTH, 
                TeraInputFormat.VALUE_LENGTH);
      output.collect(key, value);
      crc32.reset();
      crc32.update(buffer, 0, 
                   TeraInputFormat.KEY_LENGTH + TeraInputFormat.VALUE_LENGTH);
      checksum.set(crc32.getValue());
      total.add(checksum);
      rowId.add(ONE);
    }

    @Override
    public void close() {
      checksumCounter.increment(total.getLow8());
    }
  }

  private static void usage() throws IOException {
    System.err.println("teragen <num rows> <output dir>");
  }

  /**
   * Parse a number that optionally has a postfix that denotes a base.
   * @param str an string integer with an option base {k,m,b,t}.
   * @return the expanded value
   */
  private static long parseHumanLong(String str) {
    char tail = str.charAt(str.length() - 1);
    long base = 1;
    switch (tail) {
    case 't':
      base *= 1000 * 1000 * 1000 * 1000;
      break;
    case 'b':
      base *= 1000 * 1000 * 1000;
      break;
    case 'm':
      base *= 1000 * 1000;
      break;
    case 'k':
      base *= 1000;
      break;
    default:
    }
    if (base != 1) {
      str = str.substring(0, str.length() - 1);
    }
    return Long.parseLong(str) * base;
  }
  
  /**
   * @param args the cli arguments
   */
  public int run(String[] args) throws IOException {
    JobConf job = (JobConf) getConf();
    if (args.length != 2) {
      usage();
      return 1;
    }
    setNumberOfRows(job, parseHumanLong(args[0]));
    Path outputDir = new Path(args[1]);
    if (outputDir.getFileSystem(job).exists(outputDir)) {
      throw new IOException("Output directory " + outputDir + 
                            " already exists.");
    }
    FileOutputFormat.setOutputPath(job, outputDir);
    job.setJobName("TeraGen");
    job.setJarByClass(TeraGen.class);
    job.setMapperClass(SortGenMapper.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormat(RangeInputFormat.class);
    job.setOutputFormat(TeraOutputFormat.class);
    JobClient.runJob(job);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new JobConf(), new TeraGen(), args);
    System.exit(res);
  }

}
