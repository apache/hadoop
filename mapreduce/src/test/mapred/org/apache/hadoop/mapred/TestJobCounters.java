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

import java.io.IOException;
import java.util.Formatter;
import java.util.StringTokenizer;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskCounter;

/**
 * This is an wordcount application that tests the count of records
 * got spilled to disk. It generates simple text input files. Then
 * runs the wordcount map/reduce application on (1) 3 i/p files(with 3 maps
 * and 1 reduce) and verifies the counters and (2) 4 i/p files(with 4 maps
 * and 1 reduce) and verifies counters. Wordcount application reads the
 * text input files, breaks each line into words and counts them. The output
 * is a locally sorted list of words and the count of how often they occurred.
 *
 */
public class TestJobCounters {

  private void validateCounters(Counters counter, long spillRecCnt,
                                long mapInputRecords, long mapOutputRecords) {
      // Check if the numer of Spilled Records is same as expected
      assertEquals(spillRecCnt,
          counter.findCounter(TaskCounter.SPILLED_RECORDS).getCounter());
      assertEquals(mapInputRecords,
    		  counter.findCounter(TaskCounter.MAP_INPUT_RECORDS).getCounter());
      assertEquals(mapOutputRecords,
    		  counter.findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getCounter());
  }

  private void removeWordsFile(Path inpFile, Configuration conf)
      throws IOException {
    final FileSystem fs = inpFile.getFileSystem(conf);
    if (fs.exists(inpFile) && !fs.delete(inpFile, false)) {
      throw new IOException("Failed to delete " + inpFile);
    }
  }

  private static void createWordsFile(Path inpFile, Configuration conf)
      throws IOException {
    final FileSystem fs = inpFile.getFileSystem(conf);
    if (fs.exists(inpFile)) {
      return;
    }
    FSDataOutputStream out = fs.create(inpFile);
    try {
      // 1024*4 unique words --- repeated 5 times => 5*2K words
      int REPLICAS=5, NUMLINES=1024, NUMWORDSPERLINE=4;
      final String WORD = "zymurgy"; // 7 bytes + 4 id bytes
      final Formatter fmt = new Formatter(new StringBuilder());
      for (int i = 0; i < REPLICAS; i++) {
        for (int j = 1; j <= NUMLINES*NUMWORDSPERLINE; j+=NUMWORDSPERLINE) {
          ((StringBuilder)fmt.out()).setLength(0);
          for (int k = 0; k < NUMWORDSPERLINE; ++k) {
            fmt.format("%s%04d ", WORD, j + k);
          }
          ((StringBuilder)fmt.out()).append("\n");
          out.writeBytes(fmt.toString());
        }
      }
    } finally {
      out.close();
    }
  }

  private static Path IN_DIR = null;
  private static Path OUT_DIR = null;
  private static Path testdir = null;

  @BeforeClass
  public static void initPaths() throws IOException {
    final Configuration conf = new Configuration();
    final Path TEST_ROOT_DIR =
      new Path(System.getProperty("test.build.data", "/tmp"));
    testdir = new Path(TEST_ROOT_DIR, "spilledRecords.countertest");
    IN_DIR = new Path(testdir, "in");
    OUT_DIR = new Path(testdir, "out");

    FileSystem fs = FileSystem.getLocal(conf);
    testdir = new Path(TEST_ROOT_DIR, "spilledRecords.countertest");
    if (fs.exists(testdir) && !fs.delete(testdir, true)) {
      throw new IOException("Could not delete " + testdir);
    }
    if (!fs.mkdirs(IN_DIR)) {
      throw new IOException("Mkdirs failed to create " + IN_DIR);
    }
    // create 3 input files each with 5*2k words
    createWordsFile(new Path(IN_DIR, "input5_2k_1"), conf);
    createWordsFile(new Path(IN_DIR, "input5_2k_2"), conf);
    createWordsFile(new Path(IN_DIR, "input5_2k_3"), conf);

  }

  @AfterClass
  public static void cleanup() throws IOException {
    //clean up the input and output files
    final Configuration conf = new Configuration();
    final FileSystem fs = testdir.getFileSystem(conf);
    if (fs.exists(testdir)) {
      fs.delete(testdir, true);
    }
  }

  public static JobConf createConfiguration() throws IOException {
    JobConf baseConf = new JobConf(TestJobCounters.class);

    baseConf.setOutputKeyClass(Text.class);
    baseConf.setOutputValueClass(IntWritable.class);

    baseConf.setMapperClass(WordCount.MapClass.class);
    baseConf.setCombinerClass(WordCount.Reduce.class);
    baseConf.setReducerClass(WordCount.Reduce.class);

    baseConf.setNumReduceTasks(1);
    baseConf.setInt(JobContext.IO_SORT_MB, 1);
    baseConf.set(JobContext.MAP_SORT_SPILL_PERCENT, "0.50");
    baseConf.setInt(JobContext.MAP_COMBINE_MIN_SPILLS, 3);
    return baseConf;
  }

  public static Job createJob() throws IOException {
    final Configuration conf = new Configuration();
    final Job baseJob = Job.getInstance(new Cluster(conf), conf);
    baseJob.setOutputKeyClass(Text.class);
    baseJob.setOutputValueClass(IntWritable.class);
    baseJob.setMapperClass(NewMapTokenizer.class);
    baseJob.setCombinerClass(NewSummer.class);
    baseJob.setReducerClass(NewSummer.class);
    baseJob.setNumReduceTasks(1);
    baseJob.getConfiguration().setInt(JobContext.IO_SORT_MB, 1);
    baseJob.getConfiguration().set(JobContext.MAP_SORT_SPILL_PERCENT, "0.50");
    baseJob.getConfiguration().setInt(JobContext.MAP_COMBINE_MIN_SPILLS, 3);
    org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setMinInputSplitSize(
        baseJob, Long.MAX_VALUE);
    return baseJob;
  }

  @Test
  public void testOldCounterA() throws Exception {
    JobConf conf = createConfiguration();
    conf.setNumMapTasks(3);
    conf.setInt(JobContext.IO_SORT_FACTOR, 2);
    removeWordsFile(new Path(IN_DIR, "input5_2k_4"), conf);
    removeWordsFile(new Path(IN_DIR, "input5_2k_5"), conf);
    FileInputFormat.setInputPaths(conf, IN_DIR);
    FileOutputFormat.setOutputPath(conf, new Path(OUT_DIR, "outputO0"));

    RunningJob myJob = JobClient.runJob(conf);
    Counters c1 = myJob.getCounters();
    // Each record requires 16 bytes of metadata, 16 bytes per serialized rec
    // (vint word len + word + IntWritable) = (1 + 11 + 4)
    // (2^20 buf * .5 spill pcnt) / 32 bytes/record = 2^14 recs per spill
    // Each file contains 5 replicas of 4096 words, so the first spill will
    // contain 4 (2^14 rec / 2^12 rec/replica) replicas, the second just one.

    // Each map spills twice, emitting 4096 records per spill from the
    // combiner per spill. The merge adds an additional 8192 records, as
    // there are too few spills to combine (2 < 3)
    // Each map spills 2^14 records, so maps spill 49152 records, combined.

    // The reduce spill count is composed of the read from one segment and
    // the intermediate merge of the other two. The intermediate merge
    // adds 8192 records per segment read; again, there are too few spills to
    // combine, so all 16834 are written to disk (total 32768 spilled records
    // for the intermediate merge). The merge into the reduce includes only
    // the unmerged segment, size 8192. Total spilled records in the reduce
    // is 32768 from the merge + 8192 unmerged segment = 40960 records

    // Total: map + reduce = 49152 + 40960 = 90112
    // 3 files, 5120 = 5 * 1024 rec/file = 15360 input records
    // 4 records/line = 61440 output records
    validateCounters(c1, 90112, 15360, 61440);

  }

  @Test
  public void testOldCounterB() throws Exception {

    JobConf conf = createConfiguration();
    createWordsFile(new Path(IN_DIR, "input5_2k_4"), conf);
    removeWordsFile(new Path(IN_DIR, "input5_2k_5"), conf);
    conf.setNumMapTasks(4);
    conf.setInt(JobContext.IO_SORT_FACTOR, 2);
    FileInputFormat.setInputPaths(conf, IN_DIR);
    FileOutputFormat.setOutputPath(conf, new Path(OUT_DIR, "outputO1"));

    RunningJob myJob = JobClient.runJob(conf);
    Counters c1 = myJob.getCounters();
    // As above, each map spills 2^14 records, so 4 maps spill 2^16 records

    // In the reduce, there are two intermediate merges before the reduce.
    // 1st merge: read + write = 8192 * 4
    // 2nd merge: read + write = 8192 * 4
    // final merge: 0
    // Total reduce: 65536

    // Total: map + reduce = 2^16 + 2^16 = 131072
    // 4 files, 5120 = 5 * 1024 rec/file = 15360 input records
    // 4 records/line = 81920 output records
    validateCounters(c1, 131072, 20480, 81920);
  }

  @Test
  public void testOldCounterC() throws Exception {
    JobConf conf = createConfiguration();
    createWordsFile(new Path(IN_DIR, "input5_2k_4"), conf);
    createWordsFile(new Path(IN_DIR, "input5_2k_5"), conf);
    conf.setNumMapTasks(4);
    conf.setInt(JobContext.IO_SORT_FACTOR, 3);
    FileInputFormat.setInputPaths(conf, IN_DIR);
    FileOutputFormat.setOutputPath(conf, new Path(OUT_DIR, "outputO2"));
    RunningJob myJob = JobClient.runJob(conf);
    Counters c1 = myJob.getCounters();
    // As above, each map spills 2^14 records, so 5 maps spill 81920

    // 1st merge: read + write = 6 * 8192
    // final merge: unmerged = 2 * 8192
    // Total reduce: 45056
    // 5 files, 5120 = 5 * 1024 rec/file = 15360 input records
    // 4 records/line = 102400 output records
    validateCounters(c1, 147456, 25600, 102400);
  }

  @Test
  public void testNewCounterA() throws Exception {
    final Job job = createJob();
    final Configuration conf = job.getConfiguration();
    conf.setInt(JobContext.IO_SORT_FACTOR, 2);
    removeWordsFile(new Path(IN_DIR, "input5_2k_4"), conf);
    removeWordsFile(new Path(IN_DIR, "input5_2k_5"), conf);
    org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(
        job, IN_DIR);
    org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(
        job, new Path(OUT_DIR, "outputN0"));
    assertTrue(job.waitForCompletion(true));
    final Counters c1 = Counters.downgrade(job.getCounters());
    validateCounters(c1, 90112, 15360, 61440);
  }

  @Test
  public void testNewCounterB() throws Exception {
    final Job job = createJob();
    final Configuration conf = job.getConfiguration();
    conf.setInt(JobContext.IO_SORT_FACTOR, 2);
    createWordsFile(new Path(IN_DIR, "input5_2k_4"), conf);
    removeWordsFile(new Path(IN_DIR, "input5_2k_5"), conf);
    org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(
        job, IN_DIR);
    org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(
        job, new Path(OUT_DIR, "outputN1"));
    assertTrue(job.waitForCompletion(true));
    final Counters c1 = Counters.downgrade(job.getCounters());
    validateCounters(c1, 131072, 20480, 81920);
  }

  @Test
  public void testNewCounterC() throws Exception {
    final Job job = createJob();
    final Configuration conf = job.getConfiguration();
    conf.setInt(JobContext.IO_SORT_FACTOR, 3);
    createWordsFile(new Path(IN_DIR, "input5_2k_4"), conf);
    createWordsFile(new Path(IN_DIR, "input5_2k_5"), conf);
    org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(
        job, IN_DIR);
    org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(
        job, new Path(OUT_DIR, "outputN2"));
    assertTrue(job.waitForCompletion(true));
    final Counters c1 = Counters.downgrade(job.getCounters());
    validateCounters(c1, 147456, 25600, 102400);
  }

  public static class NewMapTokenizer
      extends org.apache.hadoop.mapreduce.Mapper<Object,Text,Text,IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context)
    throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
          word.set(itr.nextToken());
          context.write(word, one);
        }
      }
  }

  public static class NewSummer
      extends org.apache.hadoop.mapreduce.Reducer<Text,IntWritable,
                                                  Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

}
