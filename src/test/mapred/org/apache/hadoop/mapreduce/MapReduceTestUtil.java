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
import java.text.NumberFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Utility methods used in various Job Control unit tests.
 */
public class MapReduceTestUtil {

  static private Random rand = new Random();

  private static NumberFormat idFormat = NumberFormat.getInstance();

  static {
    idFormat.setMinimumIntegerDigits(4);
    idFormat.setGroupingUsed(false);
  }

  /**
   * Cleans the data from the passed Path in the passed FileSystem.
   * 
   * @param fs FileSystem to delete data from.
   * @param dirPath Path to be deleted.
   * @throws IOException If an error occurs cleaning the data.
   */
  public static void cleanData(FileSystem fs, Path dirPath) 
      throws IOException {
    fs.delete(dirPath, true);
  }

  /**
   * Generates a string of random digits.
   * 
   * @return A random string.
   */
  public static String generateRandomWord() {
    return idFormat.format(rand.nextLong());
  }

  /**
   * Generates a line of random text.
   * 
   * @return A line of random text.
   */
  public static String generateRandomLine() {
    long r = rand.nextLong() % 7;
    long n = r + 20;
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < n; i++) {
      sb.append(generateRandomWord()).append(" ");
    }
    sb.append("\n");
    return sb.toString();
  }

  /**
   * Generates random data consisting of 10000 lines.
   * 
   * @param fs FileSystem to create data in.
   * @param dirPath Path to create the data in.
   * @throws IOException If an error occurs creating the data.
   */
  public static void generateData(FileSystem fs, Path dirPath) 
      throws IOException {
    FSDataOutputStream out = fs.create(new Path(dirPath, "data.txt"));
    for (int i = 0; i < 10000; i++) {
      String line = generateRandomLine();
      out.write(line.getBytes("UTF-8"));
    }
    out.close();
  }

  /**
   * Creates a simple copy job.
   * 
   * @param conf Configuration object
   * @param outdir Output directory.
   * @param indirs Comma separated input directories.
   * @return Job initialized for a data copy job.
   * @throws Exception If an error occurs creating job configuration.
   */
  public static Job createCopyJob(Configuration conf, Path outdir, 
      Path... indirs) throws Exception {
    conf.setInt("mapred.map.tasks", 3);
    Job theJob = new Job(conf);
    theJob.setJobName("DataMoveJob");

    FileInputFormat.setInputPaths(theJob, indirs);
    theJob.setMapperClass(DataCopyMapper.class);
    FileOutputFormat.setOutputPath(theJob, outdir);
    theJob.setOutputKeyClass(Text.class);
    theJob.setOutputValueClass(Text.class);
    theJob.setReducerClass(DataCopyReducer.class);
    theJob.setNumReduceTasks(1);
    return theJob;
  }

  /**
   * Creates a simple fail job.
   * 
   * @param conf Configuration object
   * @param outdir Output directory.
   * @param indirs Comma separated input directories.
   * @return Job initialized for a simple fail job.
   * @throws Exception If an error occurs creating job configuration.
   */
  public static Job createFailJob(Configuration conf, Path outdir, 
      Path... indirs) throws Exception {

    conf.setInt("mapred.map.max.attempts", 2);
    Job theJob = new Job(conf);
    theJob.setJobName("Fail-Job");

    FileInputFormat.setInputPaths(theJob, indirs);
    theJob.setMapperClass(FailMapper.class);
    theJob.setReducerClass(Reducer.class);
    theJob.setNumReduceTasks(0);
    FileOutputFormat.setOutputPath(theJob, outdir);
    theJob.setOutputKeyClass(Text.class);
    theJob.setOutputValueClass(Text.class);
    return theJob;
  }

  /**
   * Creates a simple fail job.
   * 
   * @param conf Configuration object
   * @param outdir Output directory.
   * @param indirs Comma separated input directories.
   * @return Job initialized for a simple kill job.
   * @throws Exception If an error occurs creating job configuration.
   */
  public static Job createKillJob(Configuration conf, Path outdir, 
      Path... indirs) throws Exception {

    Job theJob = new Job(conf);
    theJob.setJobName("Kill-Job");

    FileInputFormat.setInputPaths(theJob, indirs);
    theJob.setMapperClass(KillMapper.class);
    theJob.setReducerClass(Reducer.class);
    theJob.setNumReduceTasks(0);
    FileOutputFormat.setOutputPath(theJob, outdir);
    theJob.setOutputKeyClass(Text.class);
    theJob.setOutputValueClass(Text.class);
    return theJob;
  }

  /**
   * Simple Mapper and Reducer implementation which copies data it reads in.
   */
  public static class DataCopyMapper extends 
      Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context) 
        throws IOException, InterruptedException {
      context.write(new Text(key.toString()), value);
    }
  }

  public static class DataCopyReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, Context context)
    throws IOException, InterruptedException {
      Text dumbKey = new Text("");
      while (values.hasNext()) {
        Text data = (Text) values.next();
        context.write(dumbKey, data);
      }
    }
  }

  // Mapper that fails
  public static class FailMapper extends 
    Mapper<WritableComparable<?>, Writable, WritableComparable<?>, Writable> {

    public void map(WritableComparable<?> key, Writable value, Context context)
        throws IOException {
      throw new RuntimeException("failing map");
    }
  }

  // Mapper that sleeps for a long time.
  // Used for running a job that will be killed
  public static class KillMapper extends 
    Mapper<WritableComparable<?>, Writable, WritableComparable<?>, Writable> {

    public void map(WritableComparable<?> key, Writable value, Context context)
        throws IOException {
      try {
        Thread.sleep(1000000);
      } catch (InterruptedException e) {
        // Do nothing
      }
    }
  }
}
