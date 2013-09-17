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
package org.apache.hadoop.mapred;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests the old mapred APIs with {@link Reporter#getProgress()}.
 */
public class TestReporter {
  private static final Path rootTempDir =
    new Path(System.getProperty("test.build.data", "/tmp"));
  private static final Path testRootTempDir = 
    new Path(rootTempDir, "TestReporter");
  
  private static FileSystem fs = null;

  @BeforeClass
  public static void setup() throws Exception {
    fs = FileSystem.getLocal(new Configuration());
    fs.delete(testRootTempDir, true);
    fs.mkdirs(testRootTempDir);
  }

  @AfterClass
  public static void cleanup() throws Exception {
    fs.delete(testRootTempDir, true);
  }
  
  // an input with 4 lines
  private static final String INPUT = "Hi\nHi\nHi\nHi\n";
  private static final int INPUT_LINES = INPUT.split("\n").length;
  
  @SuppressWarnings("deprecation")
  static class ProgressTesterMapper extends MapReduceBase 
  implements Mapper<LongWritable, Text, Text, Text> {
    private float progressRange = 0;
    private int numRecords = 0;
    private Reporter reporter = null;
    
    @Override
    public void configure(JobConf job) {
      super.configure(job);
      // set the progress range accordingly
      if (job.getNumReduceTasks() == 0) {
        progressRange = 1f;
      } else {
        progressRange = 0.667f;
      }
    }
    
    @Override
    public void map(LongWritable key, Text value, 
                    OutputCollector<Text, Text> output, Reporter reporter) 
    throws IOException {
      this.reporter = reporter;
      
      // calculate the actual map progress
      float mapProgress = ((float)++numRecords)/INPUT_LINES;
      // calculate the attempt progress based on the progress range
      float attemptProgress = progressRange * mapProgress;
      assertEquals("Invalid progress in map", 
                   attemptProgress, reporter.getProgress(), 0f);
      output.collect(new Text(value.toString() + numRecords), value);
    }
    
    @Override
    public void close() throws IOException {
      super.close();
      assertEquals("Invalid progress in map cleanup", 
                   progressRange, reporter.getProgress(), 0f);
    }
  }

  static class StatusLimitMapper extends
      org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException {
      StringBuilder sb = new StringBuilder(512);
      for (int i = 0; i < 1000; i++) {
        sb.append("a");
      }
      context.setStatus(sb.toString());
      int progressStatusLength = context.getConfiguration().getInt(
          MRConfig.PROGRESS_STATUS_LEN_LIMIT_KEY,
          MRConfig.PROGRESS_STATUS_LEN_LIMIT_DEFAULT);

      if (context.getStatus().length() > progressStatusLength) {
        throw new IOException("Status is not truncated");
      }
    }
  }

  /**
   * Test {@link Reporter}'s progress for a map-only job.
   * This will make sure that only the map phase decides the attempt's progress.
   */
  @SuppressWarnings("deprecation")
  @Test
  public void testReporterProgressForMapOnlyJob() throws IOException {
    Path test = new Path(testRootTempDir, "testReporterProgressForMapOnlyJob");
    
    JobConf conf = new JobConf();
    conf.setMapperClass(ProgressTesterMapper.class);
    conf.setMapOutputKeyClass(Text.class);
    // fail early
    conf.setMaxMapAttempts(1);
    conf.setMaxReduceAttempts(0);
    
    RunningJob job = 
      UtilsForTests.runJob(conf, new Path(test, "in"), new Path(test, "out"), 
                           1, 0, INPUT);
    job.waitForCompletion();
    
    assertTrue("Job failed", job.isSuccessful());
  }
  
  /**
   * A {@link Reducer} implementation that checks the progress on every call
   * to {@link Reducer#reduce(Object, Iterator, OutputCollector, Reporter)}.
   */
  @SuppressWarnings("deprecation")
  static class ProgressTestingReducer extends MapReduceBase 
  implements Reducer<Text, Text, Text, Text> {
    private int recordCount = 0;
    private Reporter reporter = null;
    // reduce task has a fixed split of progress amongst copy, shuffle and 
    // reduce phases.
    private final float REDUCE_PROGRESS_RANGE = 1.0f/3;
    private final float SHUFFLE_PROGRESS_RANGE = 1 - REDUCE_PROGRESS_RANGE;
    
    @Override
    public void configure(JobConf job) {
      super.configure(job);
    }
    
    @Override
    public void reduce(Text key, Iterator<Text> values,
        OutputCollector<Text, Text> output, Reporter reporter)
    throws IOException {
      float reducePhaseProgress = ((float)++recordCount)/INPUT_LINES;
      float weightedReducePhaseProgress = 
              reducePhaseProgress * REDUCE_PROGRESS_RANGE;
      assertEquals("Invalid progress in reduce", 
                   SHUFFLE_PROGRESS_RANGE + weightedReducePhaseProgress, 
                   reporter.getProgress(), 0.02f);
      this.reporter = reporter;
    }
    
    @Override
    public void close() throws IOException {
      super.close();
      assertEquals("Invalid progress in reduce cleanup", 
                   1.0f, reporter.getProgress(), 0f);
    }
  }
  
  /**
   * Test {@link Reporter}'s progress for map-reduce job.
   */
  @Test
  public void testReporterProgressForMRJob() throws IOException {
    Path test = new Path(testRootTempDir, "testReporterProgressForMRJob");
    
    JobConf conf = new JobConf();
    conf.setMapperClass(ProgressTesterMapper.class);
    conf.setReducerClass(ProgressTestingReducer.class);
    conf.setMapOutputKeyClass(Text.class);
    // fail early
    conf.setMaxMapAttempts(1);
    conf.setMaxReduceAttempts(1);

    RunningJob job = 
      UtilsForTests.runJob(conf, new Path(test, "in"), new Path(test, "out"), 
                           1, 1, INPUT);
    job.waitForCompletion();
    
    assertTrue("Job failed", job.isSuccessful());
  }

  @Test
  public void testStatusLimit() throws IOException, InterruptedException,
      ClassNotFoundException {
    Path test = new Path(testRootTempDir, "testStatusLimit");

    Configuration conf = new Configuration();
    Path inDir = new Path(test, "in");
    Path outDir = new Path(test, "out");
    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(inDir)) {
      fs.delete(inDir, true);
    }
    fs.mkdirs(inDir);
    DataOutputStream file = fs.create(new Path(inDir, "part-" + 0));
    file.writeBytes("testStatusLimit");
    file.close();

    if (fs.exists(outDir)) {
      fs.delete(outDir, true);
    }

    Job job = Job.getInstance(conf, "testStatusLimit");

    job.setMapperClass(StatusLimitMapper.class);
    job.setNumReduceTasks(0);

    FileInputFormat.addInputPath(job, inDir);
    FileOutputFormat.setOutputPath(job, outDir);

    job.waitForCompletion(true);

    assertTrue("Job failed", job.isSuccessful());
  }

}