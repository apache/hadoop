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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

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
public class TestMRIntermediateDataEncryption {
  // Where MR job's input will reside.
  private static final Path INPUT_DIR = new Path("/test/input");
  // Where output goes.
  private static final Path OUTPUT = new Path("/test/output");

  @Test
  public void testSingleReducer() throws Exception {
    doEncryptionTest(3, 1, 2, false);
  }

  @Test
  public void testUberMode() throws Exception {
    doEncryptionTest(3, 1, 2, true);
  }

  @Test
  public void testMultipleMapsPerNode() throws Exception {
    doEncryptionTest(8, 1, 2, false);
  }

  @Test
  public void testMultipleReducers() throws Exception {
    doEncryptionTest(2, 4, 2, false);
  }

  public void doEncryptionTest(int numMappers, int numReducers, int numNodes,
                               boolean isUber) throws Exception {
    doEncryptionTest(numMappers, numReducers, numNodes, 1000, isUber);
  }

  public void doEncryptionTest(int numMappers, int numReducers, int numNodes,
                               int numLines, boolean isUber) throws Exception {
    MiniDFSCluster dfsCluster = null;
    MiniMRClientCluster mrCluster = null;
    FileSystem fileSystem = null;
    try {
      Configuration conf = new Configuration();
      // Start the mini-MR and mini-DFS clusters

      dfsCluster = new MiniDFSCluster.Builder(conf)
          .numDataNodes(numNodes).build();
      fileSystem = dfsCluster.getFileSystem();
      mrCluster = MiniMRClientClusterFactory.create(this.getClass(),
                                                 numNodes, conf);
      // Generate input.
      createInput(fileSystem, numMappers, numLines);
      // Run the test.
      runMergeTest(new JobConf(mrCluster.getConfig()), fileSystem,
              numMappers, numReducers, numLines, isUber);
    } finally {
      if (dfsCluster != null) {
        dfsCluster.shutdown();
      }
      if (mrCluster != null) {
        mrCluster.stop();
      }
    }
  }

  private void createInput(FileSystem fs, int numMappers, int numLines) throws Exception {
    fs.delete(INPUT_DIR, true);
    for (int i = 0; i < numMappers; i++) {
      OutputStream os = fs.create(new Path(INPUT_DIR, "input_" + i + ".txt"));
      Writer writer = new OutputStreamWriter(os);
      for (int j = 0; j < numLines; j++) {
        // Create sorted key, value pairs.
        int k = j + 1;
        String formattedNumber = String.format("%09d", k);
        writer.write(formattedNumber + " " + formattedNumber + "\n");
      }
      writer.close();
    }
  }

  private void runMergeTest(JobConf job, FileSystem fileSystem, int
          numMappers, int numReducers, int numLines, boolean isUber)
          throws Exception {
    fileSystem.delete(OUTPUT, true);
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
    job.setMapperClass(MyMapper.class);
    job.setPartitionerClass(MyPartitioner.class);
    job.setOutputFormat(TextOutputFormat.class);
    job.setNumReduceTasks(numReducers);

    job.setInt("mapreduce.map.maxattempts", 1);
    job.setInt("mapreduce.reduce.maxattempts", 1);
    job.setInt("mapred.test.num_lines", numLines);
    if (isUber) {
      job.setBoolean("mapreduce.job.ubertask.enable", true);
    }
    job.setBoolean(MRJobConfig.MR_ENCRYPTED_INTERMEDIATE_DATA, true);
    try {
      submittedJob = client.submitJob(job);
      try {
        if (! client.monitorAndPrintJob(job, submittedJob)) {
          throw new IOException("Job failed!");
        }
      } catch(InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
    } catch(IOException ioe) {
      System.err.println("Job failed with: " + ioe);
    } finally {
      verifyOutput(submittedJob, fileSystem, numMappers, numLines);
    }
  }

  private void verifyOutput(RunningJob submittedJob, FileSystem fileSystem, int numMappers, int numLines)
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
    assertEquals((long)(numMappers * numLines), numValidRecords);
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
        valueText.set(record.substring(blankPos+1));
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
      } catch(NumberFormatException nfe) {
        keyValue = 0;
      }
      int partitionNumber = (numPartitions*(Math.max(0, keyValue-1)))/job.getInt("mapred.test.num_lines", 10000);
      return partitionNumber;
    }
  }

}
