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

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.NumberFormat;
import java.util.Iterator;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.apache.hadoop.mapred.Utils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Utility methods used in various Job Control unit tests.
 */
public class MapReduceTestUtil {
  public static final Log LOG = 
    LogFactory.getLog(MapReduceTestUtil.class.getName());

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

    FileSystem fs = outdir.getFileSystem(conf);
    if (fs.exists(outdir)) {
      fs.delete(outdir, true);
    }
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

  public static Job createJob(Configuration conf, Path inDir, Path outDir, 
      int numInputFiles, int numReds) throws IOException {
    String input = "The quick brown fox\n" + "has many silly\n"
      + "red fox sox\n";
    return createJob(conf, inDir, outDir, numInputFiles, numReds, input);
  }

  public static Job createJob(Configuration conf, Path inDir, Path outDir, 
      int numInputFiles, int numReds, String input) throws IOException {
    Job job = new Job(conf);
    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(outDir)) {
      fs.delete(outDir, true);
    }
    if (fs.exists(inDir)) {
      fs.delete(inDir, true);
    }
    fs.mkdirs(inDir);
    for (int i = 0; i < numInputFiles; ++i) {
      DataOutputStream file = fs.create(new Path(inDir, "part-" + i));
      file.writeBytes(input);
      file.close();
    }    

    FileInputFormat.setInputPaths(job, inDir);
    FileOutputFormat.setOutputPath(job, outDir);
    job.setNumReduceTasks(numReds);
    return job;
  }

  public static TaskAttemptContext createDummyMapTaskAttemptContext(
      Configuration conf) {
    TaskAttemptID tid = new TaskAttemptID("jt", 1, true, 0, 0);
    conf.set("mapred.task.id", tid.toString());
    return new TaskAttemptContext(conf, tid);    
  }

  public static StatusReporter createDummyReporter() {
    return new StatusReporter() {
      public void setStatus(String s) {
      }
      public void progress() {
      }
      public Counter getCounter(Enum<?> name) {
        return new Counters().findCounter(name);
      }
      public Counter getCounter(String group, String name) {
        return new Counters().findCounter(group, name);
      }
    };
  }
  
  public static String readOutput(Path outDir, Configuration conf) 
      throws IOException {
    FileSystem fs = outDir.getFileSystem(conf);
    StringBuffer result = new StringBuffer();

    Path[] fileList = FileUtil.stat2Paths(fs.listStatus(outDir,
        new Utils.OutputFileUtils.OutputFilesFilter()));
    for (Path outputFile : fileList) {
      LOG.info("Path" + ": "+ outputFile);
      BufferedReader file = 
        new BufferedReader(new InputStreamReader(fs.open(outputFile)));
      String line = file.readLine();
      while (line != null) {
        result.append(line);
        result.append("\n");
        line = file.readLine();
      }
      file.close();
    }
    return result.toString();
  }

}
