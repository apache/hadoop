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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter;
import org.apache.hadoop.yarn.util.ResourceCalculatorProcessTree;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

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

  private void validateFileCounters(Counters counter, long fileBytesRead,
      long fileBytesWritten, long mapOutputBytes,
      long mapOutputMaterializedBytes) {
    assertTrue(counter.findCounter(FileInputFormatCounter.BYTES_READ)
        .getValue() != 0);
    assertEquals(fileBytesRead,
        counter.findCounter(FileInputFormatCounter.BYTES_READ).getValue());

    assertTrue(counter.findCounter(FileOutputFormatCounter.BYTES_WRITTEN)
        .getValue() != 0);

    if (mapOutputBytes >= 0) {
      assertTrue(counter.findCounter(TaskCounter.MAP_OUTPUT_BYTES).getValue() != 0);
    }
    if (mapOutputMaterializedBytes >= 0) {
      assertTrue(counter.findCounter(TaskCounter.MAP_OUTPUT_MATERIALIZED_BYTES)
          .getValue() != 0);
    }
  }

  @SuppressWarnings("deprecation")
  private void validateOldFileCounters(Counters counter, long fileBytesRead,
      long fileBytesWritten, long mapOutputBytes,
      long mapOutputMaterializedBytes) {

    assertEquals(fileBytesRead,
      counter.findCounter(FileInputFormat.Counter.BYTES_READ).getValue());

    assertEquals(
      fileBytesRead,
      counter
        .findCounter(
          org.apache.hadoop.mapreduce.lib.input.FileInputFormat.Counter.BYTES_READ)
        .getValue());

    assertEquals(fileBytesWritten,
      counter.findCounter(FileOutputFormat.Counter.BYTES_WRITTEN).getValue());

    assertEquals(
      fileBytesWritten,
      counter
        .findCounter(
          org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.Counter.BYTES_WRITTEN)
        .getValue());

    if (mapOutputBytes >= 0) {
      assertTrue(counter.findCounter(TaskCounter.MAP_OUTPUT_BYTES).getValue() != 0);
    }
    if (mapOutputMaterializedBytes >= 0) {
      assertTrue(counter.findCounter(TaskCounter.MAP_OUTPUT_MATERIALIZED_BYTES)
        .getValue() != 0);
    }
  }
  
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

  private static Path[] inFiles = new Path[5];

  private static long getFileSize(Path path) throws IOException {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    long len = 0;
    len += fs.getFileStatus(path).getLen();
    Path crcPath = new Path(path.getParent(), "." + path.getName() + ".crc");
    if (fs.exists(crcPath)) {
      len += fs.getFileStatus(crcPath).getLen();
    }
    return len;
  }

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

    for (int i = 0; i < inFiles.length; i++) {
      inFiles[i] = new Path(IN_DIR, "input5_2k_" + i);
    }

    // create 3 input files each with 5*2k words
    createWordsFile(inFiles[0], conf);
    createWordsFile(inFiles[1], conf);
    createWordsFile(inFiles[2], conf);
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
    final Job baseJob = Job.getInstance(conf);
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
    removeWordsFile(inFiles[3], conf);
    removeWordsFile(inFiles[4], conf);
    long inputSize = 0;
    inputSize += getFileSize(inFiles[0]);
    inputSize += getFileSize(inFiles[1]);
    inputSize += getFileSize(inFiles[2]);
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

    // The combiner has emitted 24576 records to the reducer; these are all
    // fetched straight to memory from the map side. The intermediate merge
    // adds 8192 records per segment read; again, there are too few spills to
    // combine, so all Total spilled records in the reduce
    // is 8192 records / map * 3 maps = 24576.

    // Total: map + reduce = 49152 + 24576 = 73728
    // 3 files, 5120 = 5 * 1024 rec/file = 15360 input records
    // 4 records/line = 61440 output records
    validateCounters(c1, 73728, 15360, 61440);
    validateFileCounters(c1, inputSize, 0, 0, 0);
    validateOldFileCounters(c1, inputSize, 61928, 0, 0);
  }

  @Test
  public void testOldCounterB() throws Exception {

    JobConf conf = createConfiguration();
    createWordsFile(inFiles[3], conf);
    removeWordsFile(inFiles[4], conf);
    long inputSize = 0;
    inputSize += getFileSize(inFiles[0]);
    inputSize += getFileSize(inFiles[1]);
    inputSize += getFileSize(inFiles[2]);
    inputSize += getFileSize(inFiles[3]);
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
    // Total reduce: 32768

    // Total: map + reduce = 2^16 + 2^15 = 98304
    // 4 files, 5120 = 5 * 1024 rec/file = 15360 input records
    // 4 records/line = 81920 output records
    validateCounters(c1, 98304, 20480, 81920);
    validateFileCounters(c1, inputSize, 0, 0, 0);
  }

  @Test
  public void testOldCounterC() throws Exception {
    JobConf conf = createConfiguration();
    createWordsFile(inFiles[3], conf);
    createWordsFile(inFiles[4], conf);
    long inputSize = 0;
    inputSize += getFileSize(inFiles[0]);
    inputSize += getFileSize(inFiles[1]);
    inputSize += getFileSize(inFiles[2]);
    inputSize += getFileSize(inFiles[3]);
    inputSize += getFileSize(inFiles[4]);
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
    validateCounters(c1, 122880, 25600, 102400);
    validateFileCounters(c1, inputSize, 0, 0, 0);
  }

  @Test
  public void testOldCounterD() throws Exception {
    JobConf conf = createConfiguration();
    conf.setNumMapTasks(3);
    conf.setInt(JobContext.IO_SORT_FACTOR, 2);
    conf.setNumReduceTasks(0);
    removeWordsFile(inFiles[3], conf);
    removeWordsFile(inFiles[4], conf);
    long inputSize = 0;
    inputSize += getFileSize(inFiles[0]);
    inputSize += getFileSize(inFiles[1]);
    inputSize += getFileSize(inFiles[2]);
    FileInputFormat.setInputPaths(conf, IN_DIR);
    FileOutputFormat.setOutputPath(conf, new Path(OUT_DIR, "outputO3"));

    RunningJob myJob = JobClient.runJob(conf);
    Counters c1 = myJob.getCounters();

    // No Reduces. Will go through the direct output collector. Spills=0

    validateCounters(c1, 0, 15360, 61440);
    validateFileCounters(c1, inputSize, 0, -1, -1);
  }

  @Test
  public void testNewCounterA() throws Exception {
    final Job job = createJob();
    final Configuration conf = job.getConfiguration();
    conf.setInt(JobContext.IO_SORT_FACTOR, 2);
    removeWordsFile(inFiles[3], conf);
    removeWordsFile(inFiles[4], conf);
    long inputSize = 0;
    inputSize += getFileSize(inFiles[0]);
    inputSize += getFileSize(inFiles[1]);
    inputSize += getFileSize(inFiles[2]);
    org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(
        job, IN_DIR);
    org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(
        job, new Path(OUT_DIR, "outputN0"));
    assertTrue(job.waitForCompletion(true));
    final Counters c1 = Counters.downgrade(job.getCounters());
    validateCounters(c1, 73728, 15360, 61440);
    validateFileCounters(c1, inputSize, 0, 0, 0);    
  }

  @Test
  public void testNewCounterB() throws Exception {
    final Job job = createJob();
    final Configuration conf = job.getConfiguration();
    conf.setInt(JobContext.IO_SORT_FACTOR, 2);
    createWordsFile(inFiles[3], conf);
    removeWordsFile(inFiles[4], conf);
    long inputSize = 0;
    inputSize += getFileSize(inFiles[0]);
    inputSize += getFileSize(inFiles[1]);
    inputSize += getFileSize(inFiles[2]);
    inputSize += getFileSize(inFiles[3]);
    org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(
        job, IN_DIR);
    org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(
        job, new Path(OUT_DIR, "outputN1"));
    assertTrue(job.waitForCompletion(true));
    final Counters c1 = Counters.downgrade(job.getCounters());
    validateCounters(c1, 98304, 20480, 81920);
    validateFileCounters(c1, inputSize, 0, 0, 0);
  }

  @Test
  public void testNewCounterC() throws Exception {
    final Job job = createJob();
    final Configuration conf = job.getConfiguration();
    conf.setInt(JobContext.IO_SORT_FACTOR, 3);
    createWordsFile(inFiles[3], conf);
    createWordsFile(inFiles[4], conf);
    long inputSize = 0;
    inputSize += getFileSize(inFiles[0]);
    inputSize += getFileSize(inFiles[1]);
    inputSize += getFileSize(inFiles[2]);
    inputSize += getFileSize(inFiles[3]);
    inputSize += getFileSize(inFiles[4]);
    org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(
        job, IN_DIR);
    org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(
        job, new Path(OUT_DIR, "outputN2"));
    assertTrue(job.waitForCompletion(true));
    final Counters c1 = Counters.downgrade(job.getCounters());
    validateCounters(c1, 122880, 25600, 102400);
    validateFileCounters(c1, inputSize, 0, 0, 0);
  }

  @Test
  public void testNewCounterD() throws Exception {
    final Job job = createJob();
    final Configuration conf = job.getConfiguration();
    conf.setInt(JobContext.IO_SORT_FACTOR, 2);
    job.setNumReduceTasks(0);
    removeWordsFile(inFiles[3], conf);
    removeWordsFile(inFiles[4], conf);
    long inputSize = 0;
    inputSize += getFileSize(inFiles[0]);
    inputSize += getFileSize(inFiles[1]);
    inputSize += getFileSize(inFiles[2]);
    org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(job,
        IN_DIR);
    org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job,
        new Path(OUT_DIR, "outputN3"));
    assertTrue(job.waitForCompletion(true));
    final Counters c1 = Counters.downgrade(job.getCounters());
    validateCounters(c1, 0, 15360, 61440);
    validateFileCounters(c1, inputSize, 0, -1, -1);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testOldCounters() throws Exception {
    Counters c1 = new Counters();
    c1.incrCounter(FileInputFormat.Counter.BYTES_READ, 100);
    c1.incrCounter(FileOutputFormat.Counter.BYTES_WRITTEN, 200);
    c1.incrCounter(TaskCounter.MAP_OUTPUT_BYTES, 100);
    c1.incrCounter(TaskCounter.MAP_OUTPUT_MATERIALIZED_BYTES, 100);
    validateFileCounters(c1, 100, 200, 100, 100);
    validateOldFileCounters(c1, 100, 200, 100, 100);
  }

  /** 
   * Increases the JVM's heap usage to the specified target value.
   */
  static class MemoryLoader {
    private static final int DEFAULT_UNIT_LOAD_SIZE = 10 * 1024 * 1024; // 10mb
    
    // the target value to reach
    private long targetValue;
    // a list to hold the load objects
    private List<String> loadObjects = new ArrayList<String>();
    
    MemoryLoader(long targetValue) {
      this.targetValue = targetValue;
    }
    
    /**
     * Loads the memory to the target value.
     */
    void load() {
      while (Runtime.getRuntime().totalMemory() < targetValue) {
        System.out.println("Loading memory with " + DEFAULT_UNIT_LOAD_SIZE 
                           + " characters. Current usage : " 
                           + Runtime.getRuntime().totalMemory());
        // load some objects in the memory
        loadObjects.add(RandomStringUtils.random(DEFAULT_UNIT_LOAD_SIZE));

        // sleep for 100ms
        try {
          Thread.sleep(100);
        } catch (InterruptedException ie) {}
      }
    }
  }
  
  /**
   * A mapper that increases the JVM's heap usage to a target value configured 
   * via {@link MemoryLoaderMapper#TARGET_VALUE} using a {@link MemoryLoader}.
   */
  @SuppressWarnings({"deprecation", "unchecked"})
  static class MemoryLoaderMapper 
  extends MapReduceBase 
  implements Mapper<WritableComparable, Writable, 
                    WritableComparable, Writable> {
    static final String TARGET_VALUE = "map.memory-loader.target-value";
    
    private static MemoryLoader loader = null;
    
    public void map(WritableComparable key, Writable val, 
                    OutputCollector<WritableComparable, Writable> output,
                    Reporter reporter)
    throws IOException {
      assertNotNull("Mapper not configured!", loader);
      
      // load the memory
      loader.load();
      
      // work as identity mapper
      output.collect(key, val);
    }

    public void configure(JobConf conf) {
      loader = new MemoryLoader(conf.getLong(TARGET_VALUE, -1));
    }
  }
  
  /** 
   * A reducer that increases the JVM's heap usage to a target value configured 
   * via {@link MemoryLoaderReducer#TARGET_VALUE} using a {@link MemoryLoader}.
   */
  @SuppressWarnings({"deprecation", "unchecked"})
  static class MemoryLoaderReducer extends MapReduceBase 
  implements Reducer<WritableComparable, Writable, 
                     WritableComparable, Writable> {
    static final String TARGET_VALUE = "reduce.memory-loader.target-value";
    private static MemoryLoader loader = null;
    
    public void reduce(WritableComparable key, Iterator<Writable> val, 
                       OutputCollector<WritableComparable, Writable> output,
                       Reporter reporter)
    throws IOException {
      assertNotNull("Reducer not configured!", loader);
      
      // load the memory
      loader.load();
      
      // work as identity reducer
      output.collect(key, key);
    }

    public void configure(JobConf conf) {
      loader = new MemoryLoader(conf.getLong(TARGET_VALUE, -1));
    }
  }
  
  @SuppressWarnings("deprecation")
  private long getTaskCounterUsage (JobClient client, JobID id, int numReports,
                                    int taskId, TaskType type) 
  throws Exception {
    TaskReport[] reports = null;
    if (TaskType.MAP.equals(type)) {
      reports = client.getMapTaskReports(id);
    } else if (TaskType.REDUCE.equals(type)) {
      reports = client.getReduceTaskReports(id);
    }
    
    assertNotNull("No reports found for task type '" + type.name() 
                  + "' in job " + id, reports);
    // make sure that the total number of reports match the expected
    assertEquals("Mismatch in task id", numReports, reports.length);
    
    Counters counters = reports[taskId].getCounters();
    
    return counters.getCounter(TaskCounter.COMMITTED_HEAP_BYTES);
  }
  
  // set up heap options, target value for memory loader and the output 
  // directory before running the job
  @SuppressWarnings("deprecation")
  private static RunningJob runHeapUsageTestJob(JobConf conf, Path testRootDir,
                              String heapOptions, long targetMapValue,
                              long targetReduceValue, FileSystem fs, 
                              JobClient client, Path inDir) 
  throws IOException {
    // define a job
    JobConf jobConf = new JobConf(conf);
    
    // configure the jobs
    jobConf.setNumMapTasks(1);
    jobConf.setNumReduceTasks(1);
    jobConf.setMapperClass(MemoryLoaderMapper.class);
    jobConf.setReducerClass(MemoryLoaderReducer.class);
    jobConf.setInputFormat(TextInputFormat.class);
    jobConf.setOutputKeyClass(LongWritable.class);
    jobConf.setOutputValueClass(Text.class);
    jobConf.setMaxMapAttempts(1);
    jobConf.setMaxReduceAttempts(1);
    jobConf.set(JobConf.MAPRED_MAP_TASK_JAVA_OPTS, heapOptions);
    jobConf.set(JobConf.MAPRED_REDUCE_TASK_JAVA_OPTS, heapOptions);

    // set the targets
    jobConf.setLong(MemoryLoaderMapper.TARGET_VALUE, targetMapValue);
    jobConf.setLong(MemoryLoaderReducer.TARGET_VALUE, targetReduceValue);
    
    // set the input directory for the job
    FileInputFormat.setInputPaths(jobConf, inDir);
    
    // define job output folder
    Path outDir = new Path(testRootDir, "out");
    fs.delete(outDir, true);
    FileOutputFormat.setOutputPath(jobConf, outDir);
    
    // run the job
    RunningJob job = client.submitJob(jobConf);
    job.waitForCompletion();
    JobID jobID = job.getID();
    assertTrue("Job " + jobID + " failed!", job.isSuccessful());
    
    return job;
  }
  
  /**
   * Tests {@link TaskCounter}'s {@link TaskCounter.COMMITTED_HEAP_BYTES}. 
   * The test consists of running a low-memory job which consumes less heap 
   * memory and then running a high-memory job which consumes more heap memory, 
   * and then ensuring that COMMITTED_HEAP_BYTES of low-memory job is smaller 
   * than that of the high-memory job.
   * @throws IOException
   */
  @Test
  @SuppressWarnings("deprecation")
  public void testHeapUsageCounter() throws Exception {
    JobConf conf = new JobConf();
    // create a local filesystem handle
    FileSystem fileSystem = FileSystem.getLocal(conf);
    
    // define test root directories
    Path rootDir =
      new Path(System.getProperty("test.build.data", "/tmp"));
    Path testRootDir = new Path(rootDir, "testHeapUsageCounter");
    // cleanup the test root directory
    fileSystem.delete(testRootDir, true);
    // set the current working directory
    fileSystem.setWorkingDirectory(testRootDir);
    
    fileSystem.deleteOnExit(testRootDir);
    
    // create a mini cluster using the local file system
    MiniMRCluster mrCluster = 
      new MiniMRCluster(1, fileSystem.getUri().toString(), 1);
    
    try {
      conf = mrCluster.createJobConf();
      JobClient jobClient = new JobClient(conf);

      // define job input
      Path inDir = new Path(testRootDir, "in");
      // create input data
      createWordsFile(inDir, conf);

      // configure and run a low memory job which will run without loading the
      // jvm's heap
      RunningJob lowMemJob = 
        runHeapUsageTestJob(conf, testRootDir, "-Xms32m -Xmx1G", 
                            0, 0, fileSystem, jobClient, inDir);
      JobID lowMemJobID = lowMemJob.getID();
      long lowMemJobMapHeapUsage = getTaskCounterUsage(jobClient, lowMemJobID, 
                                                       1, 0, TaskType.MAP);
      System.out.println("Job1 (low memory job) map task heap usage: " 
                         + lowMemJobMapHeapUsage);
      long lowMemJobReduceHeapUsage =
        getTaskCounterUsage(jobClient, lowMemJobID, 1, 0, TaskType.REDUCE);
      System.out.println("Job1 (low memory job) reduce task heap usage: " 
                         + lowMemJobReduceHeapUsage);

      // configure and run a high memory job which will load the jvm's heap
      RunningJob highMemJob = 
        runHeapUsageTestJob(conf, testRootDir, "-Xms32m -Xmx1G", 
                            lowMemJobMapHeapUsage + 256*1024*1024, 
                            lowMemJobReduceHeapUsage + 256*1024*1024,
                            fileSystem, jobClient, inDir);
      JobID highMemJobID = highMemJob.getID();

      long highMemJobMapHeapUsage = getTaskCounterUsage(jobClient, highMemJobID,
                                                        1, 0, TaskType.MAP);
      System.out.println("Job2 (high memory job) map task heap usage: " 
                         + highMemJobMapHeapUsage);
      long highMemJobReduceHeapUsage =
        getTaskCounterUsage(jobClient, highMemJobID, 1, 0, TaskType.REDUCE);
      System.out.println("Job2 (high memory job) reduce task heap usage: " 
                         + highMemJobReduceHeapUsage);

      assertTrue("Incorrect map heap usage reported by the map task", 
                 lowMemJobMapHeapUsage < highMemJobMapHeapUsage);

      assertTrue("Incorrect reduce heap usage reported by the reduce task", 
                 lowMemJobReduceHeapUsage < highMemJobReduceHeapUsage);
    } finally {
      // shutdown the mr cluster
      mrCluster.shutdown();
      try {
        fileSystem.delete(testRootDir, true);
      } catch (IOException ioe) {} 
    }
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

  /**
   * Test mapper.
   */
  public static class TokenizerMapper extends
      org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable ONE = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, ONE);
      }
    }
  }

  /**
   * Test reducer.
   */
  public static class IntSumReducer extends
      org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable>{
    /**
     * Test customer counter.
     */
    public enum Counters { MY_COUNTER_MAX }
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
      context.getCounter(Counters.MY_COUNTER_MAX).increment(100);
    }
  }

  /**
   * Mock resource reporting.
   */
  public static class MockResourceCalculatorProcessTree
      extends ResourceCalculatorProcessTree {

    public MockResourceCalculatorProcessTree(String root) {
      super(root);
    }

    @Override
    public void updateProcessTree() {
    }

    @Override
    public String getProcessTreeDump() {
      return "";
    }

    @Override
    public long getCumulativeCpuTime() {
      return 0;
    }

    @Override
    public boolean checkPidPgrpidForMatch() {
      return true;
    }

    @Override
    public long getRssMemorySize() {
      return 1024;
    }

    @Override
    public long getVirtualMemorySize() {
      return 2000;
    }

    @Override
    public float getCpuUsagePercent() {
      return 0;
    }
  }

  @Test
  public void testMockResourceCalculatorProcessTree() {
    ResourceCalculatorProcessTree tree;
    tree = ResourceCalculatorProcessTree.getResourceCalculatorProcessTree(
        "1", TestJobCounters.MockResourceCalculatorProcessTree.class,
        new Configuration());
    assertNotNull(tree);
  }

  /**
   * End to end test of maximum counters.
   * @throws IOException test failed
   * @throws ClassNotFoundException test failed
   * @throws InterruptedException test failed
   */
  @Test
  public void testMaxCounter()
      throws IOException, ClassNotFoundException, InterruptedException {
    // Create mapreduce cluster
    MiniMRClientCluster mrCluster = MiniMRClientClusterFactory.create(
        this.getClass(), 2, new Configuration());

    try {
      // Setup input and output paths
      Path rootDir =
          new Path(System.getProperty("test.build.data", "/tmp"));
      Path testRootDir = new Path(rootDir, "testMaxCounter");
      Path testInputDir = new Path(testRootDir, "input");
      Path testOutputDir = new Path(testRootDir, "output");
      FileSystem fs = FileSystem.getLocal(new Configuration());
      fs.mkdirs(testInputDir);
      Path testInputFile = new Path(testInputDir, "file01");
      FSDataOutputStream stream =
          fs.create(testInputFile);
      stream.writeChars("foo");
      stream.writeChars("bar");
      stream.close();
      fs.delete(testOutputDir, true);

      // Run job (1 mapper, 2 reducers)
      Configuration conf = new Configuration();
      conf.setClass(MRConfig.RESOURCE_CALCULATOR_PROCESS_TREE,
          MockResourceCalculatorProcessTree.class,
          ResourceCalculatorProcessTree.class);
      Job job = Job.getInstance(conf, "word count");
      job.setJarByClass(WordCount.class);
      job.setMapperClass(TokenizerMapper.class);
      job.setCombinerClass(IntSumReducer.class);
      job.setReducerClass(IntSumReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      job.setNumReduceTasks(2); // make sure we have double here to test max
      org.apache.hadoop.mapreduce.lib.input.FileInputFormat
          .addInputPath(job, testInputDir);
      org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
          .setOutputPath(job, testOutputDir);
      assertTrue(job.waitForCompletion(true));

      // Verify physical numbers
      org.apache.hadoop.mapreduce.Counter maxMap =
          job.getCounters().findCounter(
              TaskCounter.MAP_PHYSICAL_MEMORY_BYTES_MAX);
      org.apache.hadoop.mapreduce.Counter maxReduce =
          job.getCounters().findCounter(
              TaskCounter.REDUCE_PHYSICAL_MEMORY_BYTES_MAX);
      org.apache.hadoop.mapreduce.Counter allP =
          job.getCounters().findCounter(
              TaskCounter.PHYSICAL_MEMORY_BYTES);
      assertEquals(1024, maxMap.getValue());
      assertEquals(1024, maxReduce.getValue());
      assertEquals(3072, allP.getValue());

      // Verify virtual numbers
      org.apache.hadoop.mapreduce.Counter maxMapV =
          job.getCounters().findCounter(
              TaskCounter.MAP_VIRTUAL_MEMORY_BYTES_MAX);
      org.apache.hadoop.mapreduce.Counter maxReduceV =
          job.getCounters().findCounter(
              TaskCounter.REDUCE_VIRTUAL_MEMORY_BYTES_MAX);
      org.apache.hadoop.mapreduce.Counter allV =
          job.getCounters().findCounter(
              TaskCounter.VIRTUAL_MEMORY_BYTES);
      assertEquals(2000, maxMapV.getValue());
      assertEquals(2000, maxReduceV.getValue());
      assertEquals(6000, allV.getValue());

      // Make sure customer counters are not affected by the _MAX
      // code in FrameworkCountersGroup
      org.apache.hadoop.mapreduce.Counter customerCounter =
          job.getCounters().findCounter(
              IntSumReducer.Counters.MY_COUNTER_MAX);
      assertEquals(200, customerCounter.getValue());

      fs.delete(testInputDir, true);
      fs.delete(testOutputDir, true);
    } finally {
      mrCluster.stop();
    }
  }
}
