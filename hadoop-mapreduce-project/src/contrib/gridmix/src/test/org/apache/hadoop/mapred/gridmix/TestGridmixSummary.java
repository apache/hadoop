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
package org.apache.hadoop.mapred.gridmix;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.UtilsForTests;
import org.apache.hadoop.mapred.gridmix.GenerateData.DataStatistics;
import org.apache.hadoop.mapred.gridmix.Statistics.ClusterStats;
import org.apache.hadoop.mapred.gridmix.Statistics.JobStats;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.tools.rumen.JobStory;
import org.apache.hadoop.tools.rumen.JobStoryProducer;
import org.junit.Test;

/**
 * Test {@link ExecutionSummarizer} and {@link ClusterSummarizer}.
 */
public class TestGridmixSummary {
  
  /**
   * Test {@link DataStatistics}.
   */
  @Test
  public void testDataStatistics() throws Exception {
    // test data-statistics getters with compression enabled
    DataStatistics stats = new DataStatistics(10, 2, true);
    assertEquals("Data size mismatch", 10, stats.getDataSize());
    assertEquals("Num files mismatch", 2, stats.getNumFiles());
    assertTrue("Compression configuration mismatch", stats.isDataCompressed());
    
    // test data-statistics getters with compression disabled
    stats = new DataStatistics(100, 5, false);
    assertEquals("Data size mismatch", 100, stats.getDataSize());
    assertEquals("Num files mismatch", 5, stats.getNumFiles());
    assertFalse("Compression configuration mismatch", stats.isDataCompressed());
    
    // test publish data stats
    Configuration conf = new Configuration();
    Path rootTempDir = new Path(System.getProperty("test.build.data", "/tmp"));
    Path testDir = new Path(rootTempDir, "testDataStatistics");
    FileSystem fs = testDir.getFileSystem(conf);
    fs.delete(testDir, true);
    Path testInputDir = new Path(testDir, "test");
    fs.mkdirs(testInputDir);
    
    // test empty folder (compression = true)
    CompressionEmulationUtil.setCompressionEmulationEnabled(conf, true);
    Boolean failed = null;
    try {
      GenerateData.publishDataStatistics(testInputDir, 1024L, conf);
      failed = false;
    } catch (RuntimeException e) {
      failed = true;
    }
    assertNotNull("Expected failure!", failed);
    assertTrue("Compression data publishing error", failed);
    
    // test with empty folder (compression = off)
    CompressionEmulationUtil.setCompressionEmulationEnabled(conf, false);
    stats = GenerateData.publishDataStatistics(testInputDir, 1024L, conf);
    assertEquals("Data size mismatch", 0, stats.getDataSize());
    assertEquals("Num files mismatch", 0, stats.getNumFiles());
    assertFalse("Compression configuration mismatch", stats.isDataCompressed());
    
    // test with some plain input data (compression = off)
    CompressionEmulationUtil.setCompressionEmulationEnabled(conf, false);
    Path inputDataFile = new Path(testInputDir, "test");
    long size = 
      UtilsForTests.createTmpFileDFS(fs, inputDataFile, 
          FsPermission.createImmutable((short)777), "hi hello bye").size();
    stats = GenerateData.publishDataStatistics(testInputDir, -1, conf);
    assertEquals("Data size mismatch", size, stats.getDataSize());
    assertEquals("Num files mismatch", 1, stats.getNumFiles());
    assertFalse("Compression configuration mismatch", stats.isDataCompressed());
    
    // test with some plain input data (compression = on)
    CompressionEmulationUtil.setCompressionEmulationEnabled(conf, true);
    failed = null;
    try {
      GenerateData.publishDataStatistics(testInputDir, 1234L, conf);
      failed = false;
    } catch (RuntimeException e) {
      failed = true;
    }
    assertNotNull("Expected failure!", failed);
    assertTrue("Compression data publishing error", failed);
    
    // test with some compressed input data (compression = off)
    CompressionEmulationUtil.setCompressionEmulationEnabled(conf, false);
    fs.delete(inputDataFile, false);
    inputDataFile = new Path(testInputDir, "test.gz");
    size = 
      UtilsForTests.createTmpFileDFS(fs, inputDataFile, 
          FsPermission.createImmutable((short)777), "hi hello").size();
    stats =  GenerateData.publishDataStatistics(testInputDir, 1234L, conf);
    assertEquals("Data size mismatch", size, stats.getDataSize());
    assertEquals("Num files mismatch", 1, stats.getNumFiles());
    assertFalse("Compression configuration mismatch", stats.isDataCompressed());
    
    // test with some compressed input data (compression = on)
    CompressionEmulationUtil.setCompressionEmulationEnabled(conf, true);
    stats = GenerateData.publishDataStatistics(testInputDir, 1234L, conf);
    assertEquals("Data size mismatch", size, stats.getDataSize());
    assertEquals("Num files mismatch", 1, stats.getNumFiles());
    assertTrue("Compression configuration mismatch", stats.isDataCompressed());
  }
  
  /**
   * A fake {@link JobFactory}.
   */
  @SuppressWarnings("unchecked")
  private static class FakeJobFactory extends JobFactory {
    /**
     * A fake {@link JobStoryProducer} for {@link FakeJobFactory}.
     */
    private static class FakeJobStoryProducer implements JobStoryProducer {
      @Override
      public void close() throws IOException {
      }

      @Override
      public JobStory getNextJob() throws IOException {
        return null;
      }
    }
    
    FakeJobFactory(Configuration conf) {
      super(null, new FakeJobStoryProducer(), null, conf, null, null);
    }
    
    @Override
    public void update(Object item) {
    }
    
    @Override
    protected Thread createReaderThread() {
      return new Thread();
    }
  }
  
  /**
   * Test {@link ExecutionSummarizer}.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testExecutionSummarizer() throws IOException {
    Configuration conf = new Configuration();
    
    ExecutionSummarizer es = new ExecutionSummarizer();
    assertEquals("ExecutionSummarizer init failed", 
                 Summarizer.NA, es.getCommandLineArgsString());
    
    long startTime = System.currentTimeMillis();
    // test configuration parameters
    String[] initArgs = new String[] {"-Xmx20m", "-Dtest.args='test'"};
    es = new ExecutionSummarizer(initArgs);
    
    assertEquals("ExecutionSummarizer init failed", 
                 "-Xmx20m -Dtest.args='test'", 
                 es.getCommandLineArgsString());
    
    // test start time
    assertTrue("Start time mismatch", es.getStartTime() >= startTime);
    assertTrue("Start time mismatch", 
               es.getStartTime() <= System.currentTimeMillis());
    
    // test start() of ExecutionSummarizer
    es.update(null);
    assertEquals("ExecutionSummarizer init failed", 0, 
                 es.getSimulationStartTime());
    testExecutionSummarizer(0, 0, 0, 0, 0, 0, es);
    
    long simStartTime = System.currentTimeMillis();
    es.start(null);
    assertTrue("Simulation start time mismatch", 
               es.getSimulationStartTime() >= simStartTime);
    assertTrue("Simulation start time mismatch", 
               es.getSimulationStartTime() <= System.currentTimeMillis());
    
    // test with job stats
    JobStats stats = generateFakeJobStats(1, 10, true);
    es.update(stats);
    testExecutionSummarizer(1, 10, 0, 1, 1, 0, es);
    
    // test with failed job 
    stats = generateFakeJobStats(5, 1, false);
    es.update(stats);
    testExecutionSummarizer(6, 11, 0, 2, 1, 1, es);
    
    // test finalize
    //  define a fake job factory
    JobFactory factory = new FakeJobFactory(conf);
    
    // fake the num jobs in trace
    factory.numJobsInTrace = 3;
    
    Path rootTempDir = new Path(System.getProperty("test.build.data", "/tmp"));
    Path testDir = new Path(rootTempDir, "testGridmixSummary");
    Path testTraceFile = new Path(testDir, "test-trace.json");
    FileSystem fs = FileSystem.getLocal(conf);
    fs.create(testTraceFile).close();
    
    // finalize the summarizer
    UserResolver resolver = new RoundRobinUserResolver();
    DataStatistics dataStats = new DataStatistics(100, 2, true);
    String policy = GridmixJobSubmissionPolicy.REPLAY.name();
    conf.set(GridmixJobSubmissionPolicy.JOB_SUBMISSION_POLICY, policy);
    es.finalize(factory, testTraceFile.toString(), 1024L, resolver, dataStats, 
                conf);
    
    // test num jobs in trace
    assertEquals("Mismtach in num jobs in trace", 3, es.getNumJobsInTrace());
    
    // test trace signature
    String tid = 
      ExecutionSummarizer.getTraceSignature(testTraceFile.toString());
    assertEquals("Mismatch in trace signature", 
                 tid, es.getInputTraceSignature());
    // test trace location
    Path qPath = fs.makeQualified(testTraceFile);
    assertEquals("Mismatch in trace filename", 
                 qPath.toString(), es.getInputTraceLocation());
    // test expected data size
    assertEquals("Mismatch in expected data size", 
                 "1.0k", es.getExpectedDataSize());
    // test input data statistics
    assertEquals("Mismatch in input data statistics", 
                 ExecutionSummarizer.stringifyDataStatistics(dataStats), 
                 es.getInputDataStatistics());
    // test user resolver
    assertEquals("Mismatch in user resolver", 
                 resolver.getClass().getName(), es.getUserResolver());
    // test policy
    assertEquals("Mismatch in policy", policy, es.getJobSubmissionPolicy());
    
    // test data stringification using large data
    es.finalize(factory, testTraceFile.toString(), 1024*1024*1024*10L, resolver,
                dataStats, conf);
    assertEquals("Mismatch in expected data size", 
                 "10.0g", es.getExpectedDataSize());
    
    // test trace signature uniqueness
    //  touch the trace file
    fs.delete(testTraceFile, false);
    //  sleep for 1 sec
    try {
      Thread.sleep(1000);
    } catch (InterruptedException ie) {}
    fs.create(testTraceFile).close();
    es.finalize(factory, testTraceFile.toString(), 0L, resolver, dataStats, 
                conf);
    // test missing expected data size
    assertEquals("Mismatch in trace data size", 
                 Summarizer.NA, es.getExpectedDataSize());
    assertFalse("Mismatch in trace signature", 
                tid.equals(es.getInputTraceSignature()));
    // get the new identifier
    tid = ExecutionSummarizer.getTraceSignature(testTraceFile.toString());
    assertEquals("Mismatch in trace signature", 
                 tid, es.getInputTraceSignature());
    
    testTraceFile = new Path(testDir, "test-trace2.json");
    fs.create(testTraceFile).close();
    es.finalize(factory, testTraceFile.toString(), 0L, resolver, dataStats, 
                conf);
    assertFalse("Mismatch in trace signature", 
                tid.equals(es.getInputTraceSignature()));
    // get the new identifier
    tid = ExecutionSummarizer.getTraceSignature(testTraceFile.toString());
    assertEquals("Mismatch in trace signature", 
                 tid, es.getInputTraceSignature());
    
    // finalize trace identifier '-' input
    es.finalize(factory, "-", 0L, resolver, dataStats, conf);
    assertEquals("Mismatch in trace signature",
                 Summarizer.NA, es.getInputTraceSignature());
    assertEquals("Mismatch in trace file location", 
                 Summarizer.NA, es.getInputTraceLocation());
  }
  
  // test the ExecutionSummarizer
  private static void testExecutionSummarizer(int numMaps, int numReds,
      int totalJobsInTrace, int totalJobSubmitted, int numSuccessfulJob, 
      int numFailedJobs, ExecutionSummarizer es) {
    assertEquals("ExecutionSummarizer test failed [num-maps]", 
                 numMaps, es.getNumMapTasksLaunched());
    assertEquals("ExecutionSummarizer test failed [num-reducers]", 
                 numReds, es.getNumReduceTasksLaunched());
    assertEquals("ExecutionSummarizer test failed [num-jobs-in-trace]", 
                 totalJobsInTrace, es.getNumJobsInTrace());
    assertEquals("ExecutionSummarizer test failed [num-submitted jobs]", 
                 totalJobSubmitted, es.getNumSubmittedJobs());
    assertEquals("ExecutionSummarizer test failed [num-successful-jobs]", 
                 numSuccessfulJob, es.getNumSuccessfulJobs());
    assertEquals("ExecutionSummarizer test failed [num-failed jobs]", 
                 numFailedJobs, es.getNumFailedJobs());
  }
  
  // generate fake job stats
  @SuppressWarnings("deprecation")
  private static JobStats generateFakeJobStats(final int numMaps, 
      final int numReds, final boolean isSuccessful) 
  throws IOException {
    // A fake job 
    Job fakeJob = new Job() {
      @Override
      public int getNumReduceTasks() {
        return numReds;
      };
      
      @Override
      public boolean isSuccessful() throws IOException, InterruptedException {
        return isSuccessful;
      };
    };
    return new JobStats(numMaps, numReds, fakeJob);
  }
  
  /**
   * Test {@link ClusterSummarizer}.
   */
  @Test
  @SuppressWarnings("deprecation")
  public void testClusterSummarizer() throws IOException {
    ClusterSummarizer cs = new ClusterSummarizer();
    Configuration conf = new Configuration();
    
    String jt = "test-jt:1234";
    String nn = "test-nn:5678";
    conf.set(JTConfig.JT_IPC_ADDRESS, jt);
    conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, nn);
    cs.start(conf);
    
    assertEquals("JT name mismatch", jt, cs.getJobTrackerInfo());
    assertEquals("NN name mismatch", nn, cs.getNamenodeInfo());
    
    ClusterStats cstats = ClusterStats.getClusterStats();
    conf.set(JTConfig.JT_IPC_ADDRESS, "local");
    conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, "local");
    JobClient jc = new JobClient(conf);
    cstats.setClusterMetric(jc.getClusterStatus());
    
    cs.update(cstats);
    
    // test
    assertEquals("Cluster summary test failed!", 1, cs.getMaxMapTasks());
    assertEquals("Cluster summary test failed!", 1, cs.getMaxReduceTasks());
    assertEquals("Cluster summary test failed!", 1, cs.getNumActiveTrackers());
    assertEquals("Cluster summary test failed!", 0, 
                 cs.getNumBlacklistedTrackers());
  }
}