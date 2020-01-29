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

import java.util.Arrays;
import java.util.Collection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

@SuppressWarnings(value={"unchecked", "deprecation"})
/**
 * This test tests the support for a merge operation in Hadoop.  The input files
 * are already sorted on the key.  This test implements an external
 * MapOutputCollector implementation that just copies the records to different
 * partitions while maintaining the sort order in each partition.  The Hadoop
 * framework's merge on the reduce side will merge the partitions created to
 * generate the final output which is sorted on the key.
 */
@RunWith(Parameterized.class)
public class TestMRIntermediateDataEncryption {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestMRIntermediateDataEncryption.class);
  /**
   * Use urandom to avoid the YarnChild  process from hanging on low entropy
   * systems.
   */
  private static final String JVM_SECURITY_EGD_OPT =
      "-Djava.security.egd=file:/dev/./urandom";
  // Where MR job's input will reside.
  private static final Path INPUT_DIR = new Path("/test/input");
  // Where output goes.
  private static final Path OUTPUT = new Path("/test/output");
  private static final int NUM_LINES = 1000;
  private static MiniMRClientCluster mrCluster = null;
  private static MiniDFSCluster dfsCluster = null;
  private static FileSystem fs = null;
  private static final int NUM_NODES = 2;

  private final String testTitle;
  private final int numMappers;
  private final int numReducers;
  private final boolean isUber;

  /**
   * List of arguments to run the JunitTest.
   * @return
   */
  @Parameterized.Parameters(
      name = "{index}: TestMRIntermediateDataEncryption.{0} .. "
          + "mappers:{1}, reducers:{2}, isUber:{3})")
  public static Collection<Object[]> getTestParameters() {
    return Arrays.asList(new Object[][]{
        {"testSingleReducer", 3, 1, false},
        {"testUberMode", 3, 1, true},
        {"testMultipleMapsPerNode", 8, 1, false},
        {"testMultipleReducers", 2, 4, false}
    });
  }

  /**
   * Initialized the parametrized JUnit test.
   * @param testName the name of the unit test to be executed.
   * @param mappers number of mappers in the tests.
   * @param reducers number of the reducers.
   * @param uberEnabled boolean flag for isUber
   */
  public TestMRIntermediateDataEncryption(String testName, int mappers,
      int reducers, boolean uberEnabled) {
    this.testTitle = testName;
    this.numMappers = mappers;
    this.numReducers = reducers;
    this.isUber = uberEnabled;
  }

  @BeforeClass
  public static void setupClass() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(MRJobConfig.MR_ENCRYPTED_INTERMEDIATE_DATA, true);

    // Set the jvm arguments.
    conf.set(MRJobConfig.MR_AM_ADMIN_COMMAND_OPTS,
        JVM_SECURITY_EGD_OPT);
    final String childJVMOpts = JVM_SECURITY_EGD_OPT
        + " " + conf.get("mapred.child.java.opts", " ");
    conf.set("mapred.child.java.opts", childJVMOpts);


    // Start the mini-MR and mini-DFS clusters.
    dfsCluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(NUM_NODES).build();
    mrCluster =
        MiniMRClientClusterFactory.create(
            TestMRIntermediateDataEncryption.class, NUM_NODES, conf);
    mrCluster.start();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (fs != null) {
      fs.close();
    }
    if (mrCluster != null) {
      mrCluster.stop();
    }
    if (dfsCluster != null) {
      dfsCluster.shutdown();
    }
  }

  @Before
  public void setup() throws Exception {
    LOG.info("Starting TestMRIntermediateDataEncryption#{}.......", testTitle);
    fs = dfsCluster.getFileSystem();
    if (fs.exists(INPUT_DIR) && !fs.delete(INPUT_DIR, true)) {
      throw new IOException("Could not delete " + INPUT_DIR);
    }
    if (fs.exists(OUTPUT) && !fs.delete(OUTPUT, true)) {
      throw new IOException("Could not delete " + OUTPUT);
    }
    // Generate input.
    createInput(fs, numMappers, NUM_LINES);
  }

  @After
  public void cleanup() throws IOException {
    if (fs != null) {
      if (fs.exists(OUTPUT)) {
        fs.delete(OUTPUT, true);
      }
      if (fs.exists(INPUT_DIR)) {
        fs.delete(INPUT_DIR, true);
      }
    }
  }

  @Test(timeout=600000)
  public void testMerge() throws Exception {
    JobConf job = new JobConf(mrCluster.getConfig());
    job.setJobName("Test");
    JobClient client = new JobClient(job);
    RunningJob submittedJob = null;
    FileInputFormat.setInputPaths(job, INPUT_DIR);
    FileOutputFormat.setOutputPath(job, OUTPUT);
    job.set("mapreduce.output.textoutputformat.separator", " ");
    job.setInputFormat(TextInputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setMapperClass(TestMRIntermediateDataEncryption.MyMapper.class);
    job.setPartitionerClass(
        TestMRIntermediateDataEncryption.MyPartitioner.class);
    job.setOutputFormat(TextOutputFormat.class);
    job.setNumReduceTasks(numReducers);
    job.setInt("mapreduce.map.maxattempts", 1);
    job.setInt("mapreduce.reduce.maxattempts", 1);
    job.setInt("mapred.test.num_lines", NUM_LINES);
    job.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, isUber);
    job.setBoolean(MRJobConfig.MR_ENCRYPTED_INTERMEDIATE_DATA, true);
    submittedJob = client.submitJob(job);
    submittedJob.waitForCompletion();
    assertTrue("The submitted job is completed", submittedJob.isComplete());
    assertTrue("The submitted job is successful", submittedJob.isSuccessful());
    verifyOutput(fs, numMappers, NUM_LINES);
    client.close();
    // wait for short period to cool down.
    Thread.sleep(1000);
  }

  private void createInput(FileSystem filesystem, int mappers, int numLines)
      throws Exception {
    for (int i = 0; i < mappers; i++) {
      OutputStream os =
          filesystem.create(new Path(INPUT_DIR, "input_" + i + ".txt"));
      Writer writer = new OutputStreamWriter(os);
      for (int j = 0; j < numLines; j++) {
        // Create sorted key, value pairs.
        int k = j + 1;
        String formattedNumber = String.format("%09d", k);
        writer.write(formattedNumber + " " + formattedNumber + "\n");
      }
      writer.close();
      os.close();
    }
  }

  private void verifyOutput(FileSystem fileSystem,
      int mappers, int numLines)
      throws Exception {
    FSDataInputStream dis = null;
    long numValidRecords = 0;
    long numInvalidRecords = 0;
    String prevKeyValue = "000000000";
    Path[] fileList =
        FileUtil.stat2Paths(fileSystem.listStatus(OUTPUT,
            new Utils.OutputFileUtils.OutputFilesFilter()));
    for (Path outFile : fileList) {
      try {
        dis = fileSystem.open(outFile);
        String record;
        while((record = dis.readLine()) != null) {
          // Split the line into key and value.
          int blankPos = record.indexOf(" ");
          String keyString = record.substring(0, blankPos);
          String valueString = record.substring(blankPos+1);
          // Check for sorted output and correctness of record.
          if (keyString.compareTo(prevKeyValue) >= 0
              && keyString.equals(valueString)) {
            prevKeyValue = keyString;
            numValidRecords++;
          } else {
            numInvalidRecords++;
          }
        }
      } finally {
        if (dis != null) {
          dis.close();
          dis = null;
        }
      }
    }
    // Make sure we got all input records in the output in sorted order.
    assertEquals((long)(mappers * numLines), numValidRecords);
    // Make sure there is no extraneous invalid record.
    assertEquals(0, numInvalidRecords);
  }

  /**
   * A mapper implementation that assumes that key text contains valid integers
   * in displayable form.
   */
  public static class MyMapper extends MapReduceBase
      implements Mapper<LongWritable, Text, Text, Text> {
    private Text keyText;
    private Text valueText;

    public MyMapper() {
      keyText = new Text();
      valueText = new Text();
    }

    @Override
    public void map(LongWritable key, Text value,
        OutputCollector<Text, Text> output,
        Reporter reporter) throws IOException {
      String record = value.toString();
      int blankPos = record.indexOf(" ");
      keyText.set(record.substring(0, blankPos));
      valueText.set(record.substring(blankPos + 1));
      output.collect(keyText, valueText);
    }

    public void close() throws IOException {
    }
  }

  /**
   * Partitioner implementation to make sure that output is in total sorted
   * order.  We basically route key ranges to different reducers such that
   * key values monotonically increase with the partition number.  For example,
   * in this test, the keys are numbers from 1 to 1000 in the form "000000001"
   * to "000001000" in each input file.  The keys "000000001" to "000000250" are
   * routed to partition 0, "000000251" to "000000500" are routed to partition 1
   * and so on since we have 4 reducers.
   */
  static class MyPartitioner implements Partitioner<Text, Text> {

    private JobConf job;

    public MyPartitioner() {
    }

    public void configure(JobConf job) {
      this.job = job;
    }

    public int getPartition(Text key, Text value, int numPartitions) {
      int keyValue = 0;
      try {
        keyValue = Integer.parseInt(key.toString());
      } catch (NumberFormatException nfe) {
        keyValue = 0;
      }
      int partitionNumber = (numPartitions * (Math.max(0, keyValue - 1))) / job
          .getInt("mapred.test.num_lines", 10000);
      return partitionNumber;
    }
  }
}
