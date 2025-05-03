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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.junit.jupiter.api.Test;

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
    assertEquals(10, stats.getDataSize(), "Data size mismatch");
    assertEquals(2, stats.getNumFiles(), "Num files mismatch");
    assertTrue(stats.isDataCompressed(), "Compression configuration mismatch");
    
    // test data-statistics getters with compression disabled
    stats = new DataStatistics(100, 5, false);
    assertEquals(100, stats.getDataSize(), "Data size mismatch");
    assertEquals(5, stats.getNumFiles(), "Num files mismatch");
    assertFalse(stats.isDataCompressed(), "Compression configuration mismatch");
    
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
    assertNotNull(failed, "Expected failure!");
    assertTrue(failed, "Compression data publishing error");
    
    // test with empty folder (compression = off)
    CompressionEmulationUtil.setCompressionEmulationEnabled(conf, false);
    stats = GenerateData.publishDataStatistics(testInputDir, 1024L, conf);
    assertEquals(0, stats.getDataSize(), "Data size mismatch");
    assertEquals(0, stats.getNumFiles(), "Num files mismatch");
    assertFalse(stats.isDataCompressed(), "Compression configuration mismatch");
    
    // test with some plain input data (compression = off)
    CompressionEmulationUtil.setCompressionEmulationEnabled(conf, false);
    Path inputDataFile = new Path(testInputDir, "test");
    long size = 
      UtilsForTests.createTmpFileDFS(fs, inputDataFile, 
          FsPermission.createImmutable((short)777), "hi hello bye").size();
    stats = GenerateData.publishDataStatistics(testInputDir, -1, conf);
    assertEquals(size, stats.getDataSize(), "Data size mismatch");
    assertEquals(1, stats.getNumFiles(), "Num files mismatch");
    assertFalse(stats.isDataCompressed(), "Compression configuration mismatch");
    
    // test with some plain input data (compression = on)
    CompressionEmulationUtil.setCompressionEmulationEnabled(conf, true);
    failed = null;
    try {
      GenerateData.publishDataStatistics(testInputDir, 1234L, conf);
      failed = false;
    } catch (RuntimeException e) {
      failed = true;
    }
    assertNotNull(failed, "Expected failure!");
    assertTrue(failed, "Compression data publishing error");
    
    // test with some compressed input data (compression = off)
    CompressionEmulationUtil.setCompressionEmulationEnabled(conf, false);
    fs.delete(inputDataFile, false);
    inputDataFile = new Path(testInputDir, "test.gz");
    size = 
      UtilsForTests.createTmpFileDFS(fs, inputDataFile, 
          FsPermission.createImmutable((short)777), "hi hello").size();
    stats =  GenerateData.publishDataStatistics(testInputDir, 1234L, conf);
    assertEquals(size, stats.getDataSize(), "Data size mismatch");
    assertEquals(1, stats.getNumFiles(), "Num files mismatch");
    assertFalse(stats.isDataCompressed(), "Compression configuration mismatch");
    
    // test with some compressed input data (compression = on)
    CompressionEmulationUtil.setCompressionEmulationEnabled(conf, true);
    stats = GenerateData.publishDataStatistics(testInputDir, 1234L, conf);
    assertEquals(size, stats.getDataSize(), "Data size mismatch");
    assertEquals(1, stats.getNumFiles(), "Num files mismatch");
    assertTrue(stats.isDataCompressed(), "Compression configuration mismatch");
  }
  
  /**
   * A fake {@link JobFactory}.
   */
  @SuppressWarnings("rawtypes")
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
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void testExecutionSummarizer() throws IOException {
    Configuration conf = new Configuration();
    
    ExecutionSummarizer es = new ExecutionSummarizer();
    assertEquals(Summarizer.NA, es.getCommandLineArgsString(),
        "ExecutionSummarizer init failed");
    
    long startTime = System.currentTimeMillis();
    // test configuration parameters
    String[] initArgs = new String[] {"-Xmx20m", "-Dtest.args='test'"};
    es = new ExecutionSummarizer(initArgs);
    
    assertEquals("-Xmx20m -Dtest.args='test'",
        es.getCommandLineArgsString(), "ExecutionSummarizer init failed");
    
    // test start time
    assertTrue(es.getStartTime() >= startTime, "Start time mismatch");
    assertTrue(es.getStartTime() <= System.currentTimeMillis(), "Start time mismatch");
    
    // test start() of ExecutionSummarizer
    es.update(null);
    assertEquals(0, es.getSimulationStartTime(),
        "ExecutionSummarizer init failed");
    testExecutionSummarizer(0, 0, 0, 0, 0, 0, 0, es);
    
    long simStartTime = System.currentTimeMillis();
    es.start(null);
    assertTrue(es.getSimulationStartTime() >= simStartTime,
        "Simulation start time mismatch");
    assertTrue(es.getSimulationStartTime() <= System.currentTimeMillis(),
        "Simulation start time mismatch");
    
    // test with job stats
    JobStats stats = generateFakeJobStats(1, 10, true, false);
    es.update(stats);
    testExecutionSummarizer(1, 10, 0, 1, 1, 0, 0, es);
    
    // test with failed job 
    stats = generateFakeJobStats(5, 1, false, false);
    es.update(stats);
    testExecutionSummarizer(6, 11, 0, 2, 1, 1, 0, es);
    
    // test with successful but lost job 
    stats = generateFakeJobStats(1, 1, true, true);
    es.update(stats);
    testExecutionSummarizer(7, 12, 0, 3, 1, 1, 1, es);
    
    // test with failed but lost job 
    stats = generateFakeJobStats(2, 2, false, true);
    es.update(stats);
    testExecutionSummarizer(9, 14, 0, 4, 1, 1, 2, es);
    
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
    assertEquals(3, es.getNumJobsInTrace(), "Mismtach in num jobs in trace");
    
    // test trace signature
    String tid = 
      ExecutionSummarizer.getTraceSignature(testTraceFile.toString());
    assertEquals(tid, es.getInputTraceSignature(), "Mismatch in trace signature");
    // test trace location
    Path qPath = fs.makeQualified(testTraceFile);
    assertEquals(qPath.toString(), es.getInputTraceLocation(), "Mismatch in trace filename");
    // test expected data size
    assertEquals("1 K", es.getExpectedDataSize(), "Mismatch in expected data size");
    // test input data statistics
    assertEquals(ExecutionSummarizer.stringifyDataStatistics(dataStats),
        es.getInputDataStatistics(), "Mismatch in input data statistics");
    // test user resolver
    assertEquals(resolver.getClass().getName(), es.getUserResolver(),
        "Mismatch in user resolver");
    // test policy
    assertEquals(policy, es.getJobSubmissionPolicy(), "Mismatch in policy");
    
    // test data stringification using large data
    es.finalize(factory, testTraceFile.toString(), 1024*1024*1024*10L, resolver,
                dataStats, conf);
    assertEquals("10 G", es.getExpectedDataSize(), "Mismatch in expected data size");
    
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
    assertEquals(Summarizer.NA, es.getExpectedDataSize(),
        "Mismatch in trace data size");
    assertFalse(tid.equals(es.getInputTraceSignature()),
        "Mismatch in trace signature");
    // get the new identifier
    tid = ExecutionSummarizer.getTraceSignature(testTraceFile.toString());
    assertEquals(tid, es.getInputTraceSignature(), "Mismatch in trace signature");
    
    testTraceFile = new Path(testDir, "test-trace2.json");
    fs.create(testTraceFile).close();
    es.finalize(factory, testTraceFile.toString(), 0L, resolver, dataStats, 
                conf);
    assertFalse(tid.equals(es.getInputTraceSignature()),
        "Mismatch in trace signature");
    // get the new identifier
    tid = ExecutionSummarizer.getTraceSignature(testTraceFile.toString());
    assertEquals(tid, es.getInputTraceSignature(), "Mismatch in trace signature");
    
    // finalize trace identifier '-' input
    es.finalize(factory, "-", 0L, resolver, dataStats, conf);
    assertEquals(Summarizer.NA, es.getInputTraceSignature(),
        "Mismatch in trace signature");
    assertEquals(Summarizer.NA, es.getInputTraceLocation(),
        "Mismatch in trace file location");
  }
  
  // test the ExecutionSummarizer
  private static void testExecutionSummarizer(int numMaps, int numReds,
      int totalJobsInTrace, int totalJobSubmitted, int numSuccessfulJob, 
      int numFailedJobs, int numLostJobs, ExecutionSummarizer es) {
    assertEquals(numMaps, es.getNumMapTasksLaunched(),
        "ExecutionSummarizer test failed [num-maps]");
    assertEquals(numReds, es.getNumReduceTasksLaunched(),
        "ExecutionSummarizer test failed [num-reducers]");
    assertEquals(totalJobsInTrace, es.getNumJobsInTrace(),
        "ExecutionSummarizer test failed [num-jobs-in-trace]");
    assertEquals(totalJobSubmitted, es.getNumSubmittedJobs(),
        "ExecutionSummarizer test failed [num-submitted jobs]");
    assertEquals(numSuccessfulJob, es.getNumSuccessfulJobs(),
        "ExecutionSummarizer test failed [num-successful-jobs]");
    assertEquals(numFailedJobs, es.getNumFailedJobs(),
        "ExecutionSummarizer test failed [num-failed jobs]");
    assertEquals(numLostJobs, es.getNumLostJobs(),
        "ExecutionSummarizer test failed [num-lost jobs]");
  }
  
  // generate fake job stats
  @SuppressWarnings("deprecation")
  private static JobStats generateFakeJobStats(final int numMaps, 
      final int numReds, final boolean isSuccessful, final boolean lost) 
  throws IOException {
    // A fake job 
    Job fakeJob = new Job() {
      @Override
      public int getNumReduceTasks() {
        return numReds;
      };
      
      @Override
      public boolean isSuccessful() throws IOException {
        if (lost) {
          throw new IOException("Test failure!");
        }
        return isSuccessful;
      };
    };
    return new JobStats(numMaps, numReds, fakeJob);
  }
  
  /**
   * Test {@link ClusterSummarizer}.
   */
  @Test
  public void testClusterSummarizer() throws IOException {
    ClusterSummarizer cs = new ClusterSummarizer();
    Configuration conf = new Configuration();
    
    String jt = "test-jt:1234";
    String nn = "test-nn:5678";
    conf.set(JTConfig.JT_IPC_ADDRESS, jt);
    conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, nn);
    cs.start(conf);
    
    assertEquals(jt, cs.getJobTrackerInfo(), "JT name mismatch");
    assertEquals(nn, cs.getNamenodeInfo(), "NN name mismatch");
    
    ClusterStats cStats = ClusterStats.getClusterStats();
    conf.set(JTConfig.JT_IPC_ADDRESS, "local");
    conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, "local");
    JobClient jc = new JobClient(conf);
    cStats.setClusterMetric(jc.getClusterStatus());
    
    cs.update(cStats);
    
    // test
    assertEquals(1, cs.getMaxMapTasks(), "Cluster summary test failed!");
    assertEquals(1, cs.getMaxReduceTasks(), "Cluster summary test failed!");
    assertEquals(1, cs.getNumActiveTrackers(), "Cluster summary test failed!");
    assertEquals(0, cs.getNumBlacklistedTrackers(), "Cluster summary test failed!");
  }
}
