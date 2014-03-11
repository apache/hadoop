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

package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A sample MR job that helps with testing large sorts in the MapReduce
 * framework. Mapper generates the specified number of bytes and pipes them
 * to the reducers.
 *
 * <code>mapreduce.large-sorter.mbs-per-map</code> specifies the amount
 * of data (in MBs) to generate per map. By default, this is twice the value
 * of <code>mapreduce.task.io.sort.mb</code> or 1 GB if that is not specified
 * either.
 * <code>mapreduce.large-sorter.map-tasks</code> specifies the number of map
 * tasks to run.
 * <code>mapreduce.large-sorter.reduce-tasks</code> specifies the number of
 * reduce tasks to run.
 */
public class LargeSorter extends Configured implements Tool {
  private static final String LS_PREFIX = "mapreduce.large-sorter.";

  public static final String MBS_PER_MAP = LS_PREFIX + "mbs-per-map";
  public static final String NUM_MAP_TASKS = LS_PREFIX + "map-tasks";
  public static final String NUM_REDUCE_TASKS = LS_PREFIX + "reduce-tasks";

  private static final String MAX_VALUE = LS_PREFIX + "max-value";
  private static final String MIN_VALUE = LS_PREFIX + "min-value";
  private static final String MIN_KEY = LS_PREFIX + "min-key";
  private static final String MAX_KEY = LS_PREFIX + "max-key";
  
  /**
   * User counters
   */
  static enum Counters { RECORDS_WRITTEN, BYTES_WRITTEN }
  
  /**
   * A custom input format that creates virtual inputs of a single string
   * for each map.
   */
  static class RandomInputFormat extends InputFormat<Text, Text> {

    /** 
     * Generate the requested number of file splits, with the filename
     * set to the filename of the output file.
     */
    public List<InputSplit> getSplits(JobContext job) throws IOException {
      List<InputSplit> result = new ArrayList<InputSplit>();
      Path outDir = FileOutputFormat.getOutputPath(job);
      int numSplits = 
            job.getConfiguration().getInt(MRJobConfig.NUM_MAPS, 1);
      for(int i=0; i < numSplits; ++i) {
        result.add(new FileSplit(
            new Path(outDir, "dummy-split-" + i), 0, 1, null));
      }
      return result;
    }

    /**
     * Return a single record (filename, "") where the filename is taken from
     * the file split.
     */
    static class RandomRecordReader extends RecordReader<Text, Text> {
      Path name;
      Text key = null;
      Text value = new Text();
      public RandomRecordReader(Path p) {
        name = p;
      }
      
      public void initialize(InputSplit split,
                             TaskAttemptContext context)
      throws IOException, InterruptedException {
    	  
      }
      
      public boolean nextKeyValue() {
        if (name != null) {
          key = new Text();
          key.set(name.getName());
          name = null;
          return true;
        }
        return false;
      }
      
      public Text getCurrentKey() {
        return key;
      }
      
      public Text getCurrentValue() {
        return value;
      }
      
      public void close() {}

      public float getProgress() {
        return 0.0f;
      }
    }

    public RecordReader<Text, Text> createRecordReader(InputSplit split,
        TaskAttemptContext context) throws IOException, InterruptedException {
      return new RandomRecordReader(((FileSplit) split).getPath());
    }
  }

  static class RandomMapper extends Mapper<WritableComparable, Writable,
                      BytesWritable, BytesWritable> {
    
    private long numBytesToWrite;
    private int minKeySize;
    private int keySizeRange;
    private int minValueSize;
    private int valueSizeRange;
    private Random random = new Random();
    private BytesWritable randomKey = new BytesWritable();
    private BytesWritable randomValue = new BytesWritable();
    
    private void randomizeBytes(byte[] data, int offset, int length) {
      for(int i=offset + length - 1; i >= offset; --i) {
        data[i] = (byte) random.nextInt(256);
      }
    }

    @Override
    public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      numBytesToWrite = 1024 * 1024 * conf.getLong(MBS_PER_MAP,
          2 * conf.getInt(MRJobConfig.IO_SORT_MB, 512));
      minKeySize = conf.getInt(MIN_KEY, 10);
      keySizeRange =
          conf.getInt(MAX_KEY, 1000) - minKeySize;
      minValueSize = conf.getInt(MIN_VALUE, 0);
      valueSizeRange =
          conf.getInt(MAX_VALUE, 20000) - minValueSize;
    }

    /**
     * Given an output filename, write a bunch of random records to it.
     */
    public void map(WritableComparable key, 
                    Writable value,
                    Context context) throws IOException,InterruptedException {
      int itemCount = 0;
      while (numBytesToWrite > 0) {
        int keyLength = minKeySize + 
          (keySizeRange != 0 ? random.nextInt(keySizeRange) : 0);
        randomKey.setSize(keyLength);
        randomizeBytes(randomKey.getBytes(), 0, randomKey.getLength());
        int valueLength = minValueSize +
          (valueSizeRange != 0 ? random.nextInt(valueSizeRange) : 0);
        randomValue.setSize(valueLength);
        randomizeBytes(randomValue.getBytes(), 0, randomValue.getLength());
        context.write(randomKey, randomValue);
        numBytesToWrite -= keyLength + valueLength;
        context.getCounter(Counters.BYTES_WRITTEN).increment(keyLength + valueLength);
        context.getCounter(Counters.RECORDS_WRITTEN).increment(1);
        if (++itemCount % 200 == 0) {
          context.setStatus("wrote record " + itemCount + ". " + 
                             numBytesToWrite + " bytes left.");
        }
      }
      context.setStatus("done with " + itemCount + " records.");
    }
  }

  static class Discarder extends Reducer<BytesWritable, BytesWritable,
      WritableComparable, Writable> {
    @Override
    public void reduce(BytesWritable key, Iterable<BytesWritable> values,
        Context context) throws IOException, InterruptedException {
      // Do nothing
    }
  }

  private void verifyNotZero(Configuration conf, String config) {
    if (conf.getInt(config, 1) <= 0) {
      throw new IllegalArgumentException(config + "should be > 0");
    }
  }

  public int run(String[] args) throws Exception {
    Path outDir = new Path(
        LargeSorter.class.getName() + System.currentTimeMillis());

    Configuration conf = getConf();
    verifyNotZero(conf, MBS_PER_MAP);
    verifyNotZero(conf, NUM_MAP_TASKS);

    conf.setInt(MRJobConfig.NUM_MAPS, conf.getInt(NUM_MAP_TASKS, 2));

    int ioSortMb = conf.getInt(MRJobConfig.IO_SORT_MB, 512);
    int mapMb = Math.max(2 * ioSortMb, conf.getInt(MRJobConfig.MAP_MEMORY_MB,
        MRJobConfig.DEFAULT_MAP_MEMORY_MB));
    conf.setInt(MRJobConfig.MAP_MEMORY_MB, mapMb);
    conf.set(MRJobConfig.MAP_JAVA_OPTS, "-Xmx" + (mapMb - 200) + "m");

    @SuppressWarnings("deprecation")
    Job job = new Job(conf);
    job.setJarByClass(LargeSorter.class);
    job.setJobName("large-sorter");
    FileOutputFormat.setOutputPath(job, outDir);
    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(BytesWritable.class);
    job.setInputFormatClass(RandomInputFormat.class);
    job.setMapperClass(RandomMapper.class);        
    job.setReducerClass(Discarder.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setNumReduceTasks(conf.getInt(NUM_REDUCE_TASKS, 1));

    Date startTime = new Date();
    System.out.println("Job started: " + startTime);
    int ret = 1;
    try {
      ret = job.waitForCompletion(true) ? 0 : 1;
    } finally {
      FileSystem.get(conf).delete(outDir, true);
    }
    Date endTime = new Date();
    System.out.println("Job ended: " + endTime);
    System.out.println("The job took " + 
                       (endTime.getTime() - startTime.getTime()) /1000 + 
                       " seconds.");
    
    return ret;
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new LargeSorter(), args);
    System.exit(res);
  }

}
