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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.LocalJobRunner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.junit.Test;
import junit.framework.TestCase;

/**
 * Stress tests for the LocalJobRunner
 */
public class TestLocalRunner extends TestCase {

  private static final Log LOG = LogFactory.getLog(TestLocalRunner.class);

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

      switch(threadId) {
      case 0:
        // Write a single value and be done.
        c.write(new LongWritable(0), val);
        break;
      case 1:
      case 2:
        // Write many values quickly.
        for (int i = 0; i < 500; i++) {
          c.write(new LongWritable(0), val);
        }
        break;
      case 3:
        // Write many values, using thread sleeps to delay this.
        for (int i = 0; i < 50; i++) {
          for (int j = 0; j < 10; j++) {
            c.write(new LongWritable(0), val);
          }
          Thread.sleep(1);
        }
        break;
      case 4:
        // Write many values, using busy-loops to delay this.
        for (int i = 0; i < 500; i++) {
          for (int j = 0; j < 10000; j++) {
            this.exposedState++;
          }
          c.write(new LongWritable(0), val);
        }
        break;
      case 5:
        // Write many values, using very slow busy-loops to delay this.
        for (int i = 0; i < 500; i++) {
          for (int j = 0; j < 100000; j++) {
            this.exposedState++;
          }
          c.write(new LongWritable(0), val);
        }
        break;
      default:
        // Write a single value and be done.
        c.write(new LongWritable(0), val);
        break;
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
  private final static int TOTAL_RECORDS = 50000
      + (500 * 500)
      + (500 * 500)
      + (20 * 500)
      + (5000 * 500)
      + (500 * 500);

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
    createInputFile(inputPath, 0, 50000);
    createInputFile(inputPath, 1, 500);
    createInputFile(inputPath, 2, 500);
    createInputFile(inputPath, 3, 20);
    createInputFile(inputPath, 4, 5000);
    createInputFile(inputPath, 5, 500);

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
   * Run a test with several mappers in parallel, operating at different
   * speeds. Verify that the correct amount of output is created.
   */
  @Test
  public void testMultiMaps() throws Exception {
    Job job = new Job();

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
    job.getConfiguration().set("io.sort.record.pct", "0.50");
    job.getConfiguration().set("io.sort.mb", "25");
    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    job.waitForCompletion(true);

    verifyOutput(outputPath);
  }

  /**
   * Run a test with a misconfigured number of mappers.
   * Expect failure.
   */
  @Test
  public void testInvalidMultiMapParallelism() throws Exception {
    Job job = new Job();

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
  public void testEmptyMaps() throws Exception {
    Job job = new Job();
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
}