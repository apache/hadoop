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
import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.LocalJobRunner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import org.junit.Test;
import junit.framework.TestCase;

/**
 * Stress tests for the LocalJobRunner
 */
public class TestLocalRunner extends TestCase {

  private static final Log LOG = LogFactory.getLog(TestLocalRunner.class);

  private static int INPUT_SIZES[] =
    new int[] { 50000, 500, 500, 20,  5000, 500};
  private static int OUTPUT_SIZES[] =
    new int[] { 1,     500, 500, 500, 500, 500};
  private static int SLEEP_INTERVALS[] =
    new int[] { 10000, 15,  15, 20, 250, 60 };

  private static class StressMapper
      extends Mapper<LongWritable, Text, LongWritable, Text> {

    // Different map tasks operate at different speeds.
    // We define behavior for 6 threads.
    private int threadId;

    // Used to ensure that the compiler doesn't optimize away
    // some code.
    public long exposedState;

    protected void setup(Context context) {
      // Get the thread num from the file number.
      FileSplit split = (FileSplit) context.getInputSplit();
      Path filePath = split.getPath();
      String name = filePath.getName();
      this.threadId = Integer.valueOf(name);

      LOG.info("Thread " + threadId + " : "
          + context.getInputSplit());
    }

    /** Map method with different behavior based on the thread id */
    public void map(LongWritable key, Text val, Context c)
        throws IOException, InterruptedException {

      // Write many values quickly.
      for (int i = 0; i < OUTPUT_SIZES[threadId]; i++) {
        c.write(new LongWritable(0), val);
        if (i % SLEEP_INTERVALS[threadId] == 1) {
          Thread.sleep(1);
        }
      }
    }

    protected void cleanup(Context context) {
      // Output this here, to ensure that the incrementing done in map()
      // cannot be optimized away.
      LOG.debug("Busy loop counter: " + this.exposedState);
    }
  }

  private static class CountingReducer
      extends Reducer<LongWritable, Text, LongWritable, LongWritable> {

    public void reduce(LongWritable key, Iterable<Text> vals, Context context)
        throws IOException, InterruptedException {
      long out = 0;
      for (Text val : vals) {
        out++;
      }

      context.write(key, new LongWritable(out));
    }
  }

  private static class GCMapper
      extends Mapper<LongWritable, Text, LongWritable, Text> {
    public void map(LongWritable key, Text val, Context c)
        throws IOException, InterruptedException {

      // Create a whole bunch of objects.
      List<Integer> lst = new ArrayList<Integer>();
      for (int i = 0; i < 20000; i++) {
        lst.add(new Integer(i));
      }

      // Actually use this list, to ensure that it isn't just optimized away.
      int sum = 0;
      for (int x : lst) {
        sum += x;
      }

      // throw away the list and run a GC.
      lst = null;
      System.gc();

      c.write(new LongWritable(sum), val);
    }
  }

  /**
   * Create a single input file in the input directory.
   * @param dirPath the directory in which the file resides
   * @param id the file id number
   * @param numRecords how many records to write to each file.
   */
  private void createInputFile(Path dirPath, int id, int numRecords)
      throws IOException {
    final String MESSAGE = "This is a line in a file: ";

    Path filePath = new Path(dirPath, "" + id);
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);

    OutputStream os = fs.create(filePath);
    BufferedWriter w = new BufferedWriter(new OutputStreamWriter(os));

    for (int i = 0; i < numRecords; i++) {
      w.write(MESSAGE + id + " " + i + "\n");
    }

    w.close();
  }

  // This is the total number of map output records we expect to generate,
  // based on input file sizes (see createMultiMapsInput()) and the behavior
  // of the different StressMapper threads.
  private static int TOTAL_RECORDS = 0;
  static {
    for (int i = 0; i < 6; i++) {
      TOTAL_RECORDS += INPUT_SIZES[i] * OUTPUT_SIZES[i];
    }
  }

  private final String INPUT_DIR = "multiMapInput";
  private final String OUTPUT_DIR = "multiMapOutput";

  private Path getInputPath() {
    String dataDir = System.getProperty("test.build.data");
    if (null == dataDir) {
      return new Path(INPUT_DIR);
    } else {
      return new Path(new Path(dataDir), INPUT_DIR);
    }
  }

  private Path getOutputPath() {
    String dataDir = System.getProperty("test.build.data");
    if (null == dataDir) {
      return new Path(OUTPUT_DIR);
    } else {
      return new Path(new Path(dataDir), OUTPUT_DIR);
    }
  }

  /**
   * Create the inputs for the MultiMaps test.
   * @return the path to the input directory.
   */
  private Path createMultiMapsInput() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    Path inputPath = getInputPath();

    // Clear the input directory if it exists, first.
    if (fs.exists(inputPath)) {
      fs.delete(inputPath, true);
    }

    // Create input files, with sizes calibrated based on
    // the amount of work done in each mapper.
    for (int i = 0; i < 6; i++) {
      createInputFile(inputPath, i, INPUT_SIZES[i]);
    }
    return inputPath;
  }

  /**
   * Verify that we got the correct amount of output.
   */
  private void verifyOutput(Path outputPath) throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);

    Path outputFile = new Path(outputPath, "part-r-00000");
    InputStream is = fs.open(outputFile);
    BufferedReader r = new BufferedReader(new InputStreamReader(is));

    // Should get a single line of the form "0\t(count)"
    String line = r.readLine().trim();
    assertTrue("Line does not have correct key", line.startsWith("0\t"));
    int count = Integer.valueOf(line.substring(2));
    assertEquals("Incorrect count generated!", TOTAL_RECORDS, count);

    r.close();

  }

  /**
   * Test that the GC counter actually increments when we know that we've
   * spent some time in the GC during the mapper.
   */
  @Test
  public void testGcCounter() throws Exception {
    Path inputPath = getInputPath();
    Path outputPath = getOutputPath();

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);

    // Clear input/output dirs.
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true);
    }

    if (fs.exists(inputPath)) {
      fs.delete(inputPath, true);
    }

    // Create one input file
    createInputFile(inputPath, 0, 20);

    // Now configure and run the job.
    Job job = Job.getInstance();
    job.setMapperClass(GCMapper.class);
    job.setNumReduceTasks(0);
    job.getConfiguration().set(MRJobConfig.IO_SORT_MB, "25");
    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    boolean ret = job.waitForCompletion(true);
    assertTrue("job failed", ret);

    // This job should have done *some* gc work.
    // It had to clean up 400,000 objects.
    // We strongly suspect this will result in a few milliseconds effort.
    Counter gcCounter = job.getCounters().findCounter(
        TaskCounter.GC_TIME_MILLIS);
    assertNotNull(gcCounter);
    assertTrue("No time spent in gc", gcCounter.getValue() > 0);
  }


  /**
   * Run a test with several mappers in parallel, operating at different
   * speeds. Verify that the correct amount of output is created.
   */
  @Test(timeout=120*1000)
  public void testMultiMaps() throws Exception {
    Job job = Job.getInstance();

    Path inputPath = createMultiMapsInput();
    Path outputPath = getOutputPath();

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);

    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true);
    }

    job.setMapperClass(StressMapper.class);
    job.setReducerClass(CountingReducer.class);
    job.setNumReduceTasks(1);
    LocalJobRunner.setLocalMaxRunningMaps(job, 6);
    job.getConfiguration().set(MRJobConfig.IO_SORT_MB, "25");
    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    final Thread toInterrupt = Thread.currentThread();
    Thread interrupter = new Thread() {
      public void run() {
        try {
          Thread.sleep(120*1000); // 2m
          toInterrupt.interrupt();
        } catch (InterruptedException ie) {}
      }
    };
    LOG.info("Submitting job...");
    job.submit();
    LOG.info("Starting thread to interrupt main thread in 2 minutes");
    interrupter.start();
    LOG.info("Waiting for job to complete...");
    try {
      job.waitForCompletion(true);
    } catch (InterruptedException ie) {
      LOG.fatal("Interrupted while waiting for job completion", ie);
      for (int i = 0; i < 10; i++) {
        LOG.fatal("Dumping stacks");
        ReflectionUtils.logThreadInfo(LOG, "multimap threads", 0);
        Thread.sleep(1000);
      }
      throw ie;
    }
    LOG.info("Job completed, stopping interrupter");
    interrupter.interrupt();
    try {
      interrupter.join();
    } catch (InterruptedException ie) {
      // it might interrupt us right as we interrupt it
    }
    LOG.info("Verifying output");

    verifyOutput(outputPath);
  }

  /**
   * Run a test with a misconfigured number of mappers.
   * Expect failure.
   */
  @Test
  public void testInvalidMultiMapParallelism() throws Exception {
    Job job = Job.getInstance();

    Path inputPath = createMultiMapsInput();
    Path outputPath = getOutputPath();

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);

    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true);
    }

    job.setMapperClass(StressMapper.class);
    job.setReducerClass(CountingReducer.class);
    job.setNumReduceTasks(1);
    LocalJobRunner.setLocalMaxRunningMaps(job, -6);
    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    boolean success = job.waitForCompletion(true);
    assertFalse("Job succeeded somehow", success);
  }

  /** An IF that creates no splits */
  private static class EmptyInputFormat extends InputFormat<Object, Object> {
    public List<InputSplit> getSplits(JobContext context) {
      return new ArrayList<InputSplit>();
    }

    public RecordReader<Object, Object> createRecordReader(InputSplit split,
        TaskAttemptContext context) {
      return new EmptyRecordReader();
    }
  }

  private static class EmptyRecordReader extends RecordReader<Object, Object> {
    public void initialize(InputSplit split, TaskAttemptContext context) {
    }

    public Object getCurrentKey() {
      return new Object();
    }

    public Object getCurrentValue() {
      return new Object();
    }

    public float getProgress() {
      return 0.0f;
    }

    public void close() {
    }

    public boolean nextKeyValue() {
      return false;
    }
  }

  /** Test case for zero mappers */
  @Test
  public void testEmptyMaps() throws Exception {
    Job job = Job.getInstance();
    Path outputPath = getOutputPath();

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);

    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true);
    }

    job.setInputFormatClass(EmptyInputFormat.class);
    job.setNumReduceTasks(1);
    FileOutputFormat.setOutputPath(job, outputPath);

    boolean success = job.waitForCompletion(true);
    assertTrue("Empty job should work", success);
  }

  /** @return the directory where numberfiles are written (mapper inputs)  */
  private Path getNumberDirPath() {
    return new Path(getInputPath(), "numberfiles");
  }

  /**
   * Write out an input file containing an integer.
   *
   * @param fileNum the file number to write to.
   * @param value the value to write to the file
   * @return the path of the written file.
   */
  private Path makeNumberFile(int fileNum, int value) throws IOException {
    Path workDir = getNumberDirPath();
    Path filePath = new Path(workDir, "file" + fileNum);

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);

    OutputStream os = fs.create(filePath);
    BufferedWriter w = new BufferedWriter(new OutputStreamWriter(os));
    w.write("" + value);
    w.close();

    return filePath;
  }

  /**
   * Each record received by this mapper is a number 'n'.
   * Emit the values [0..n-1]
   */
  public static class SequenceMapper
      extends Mapper<LongWritable, Text, Text, NullWritable> {

    public void map(LongWritable k, Text v, Context c)
        throws IOException, InterruptedException {
      int max = Integer.valueOf(v.toString());
      for (int i = 0; i < max; i++) {
        c.write(new Text("" + i), NullWritable.get());
      }
    }
  }

  private final static int NUMBER_FILE_VAL = 100;

  /**
   * Tally up the values and ensure that we got as much data
   * out as we put in.
   * Each mapper generated 'NUMBER_FILE_VAL' values (0..NUMBER_FILE_VAL-1).
   * Verify that across all our reducers we got exactly this much
   * data back.
   */
  private void verifyNumberJob(int numMaps) throws Exception {
    Path outputDir = getOutputPath();
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);

    FileStatus [] stats = fs.listStatus(outputDir);
    int valueSum = 0;
    for (FileStatus f : stats) {
      FSDataInputStream istream = fs.open(f.getPath());
      BufferedReader r = new BufferedReader(new InputStreamReader(istream));
      String line = null;
      while ((line = r.readLine()) != null) {
        valueSum += Integer.valueOf(line.trim());
      }
      r.close();
    }

    int maxVal = NUMBER_FILE_VAL - 1;
    int expectedPerMapper = maxVal * (maxVal + 1) / 2;
    int expectedSum = expectedPerMapper * numMaps;
    LOG.info("expected sum: " + expectedSum + ", got " + valueSum);
    assertEquals("Didn't get all our results back", expectedSum, valueSum);
  }

  /**
   * Run a test which creates a SequenceMapper / IdentityReducer
   * job over a set of generated number files.
   */
  private void doMultiReducerTest(int numMaps, int numReduces,
      int parallelMaps, int parallelReduces) throws Exception {

    Path in = getNumberDirPath();
    Path out = getOutputPath();

    // Clear data from any previous tests.
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    if (fs.exists(out)) {
      fs.delete(out, true);
    }

    if (fs.exists(in)) {
      fs.delete(in, true);
    }

    for (int i = 0; i < numMaps; i++) {
      makeNumberFile(i, 100);
    }

    Job job = Job.getInstance();
    job.setNumReduceTasks(numReduces);

    job.setMapperClass(SequenceMapper.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    FileInputFormat.addInputPath(job, in);
    FileOutputFormat.setOutputPath(job, out);

    LocalJobRunner.setLocalMaxRunningMaps(job, parallelMaps);
    LocalJobRunner.setLocalMaxRunningReduces(job, parallelReduces);

    boolean result = job.waitForCompletion(true);
    assertTrue("Job failed!!", result);

    verifyNumberJob(numMaps);
  }
  
  @Test
  public void testOneMapMultiReduce() throws Exception {
    doMultiReducerTest(1, 2, 1, 1);
  }

  @Test
  public void testOneMapMultiParallelReduce() throws Exception {
    doMultiReducerTest(1, 2, 1, 2);
  }

  @Test
  public void testMultiMapOneReduce() throws Exception {
    doMultiReducerTest(4, 1, 2, 1);
  }

  @Test
  public void testMultiMapMultiReduce() throws Exception {
    doMultiReducerTest(4, 4, 2, 2);
  }

}

