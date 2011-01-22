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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.util.LinuxMemoryCalculatorPlugin;
import org.apache.hadoop.util.MemoryCalculatorPlugin;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;

import junit.framework.TestCase;

/**
 * This test class tests the functionality related to configuring, reporting
 * and computing memory related parameters in a Map/Reduce cluster.
 * 
 * Each test sets up a {@link MiniMRCluster} with a locally defined 
 * {@link org.apache.hadoop.mapred.TaskScheduler}. This scheduler validates 
 * the memory related configuration is correctly computed and reported from 
 * the tasktracker in 
 * {@link org.apache.hadoop.mapred.TaskScheduler#assignTasks(TaskTrackerStatus)}.
 */
public class TestTTMemoryReporting extends TestCase {

  static final Log LOG = LogFactory.getLog(TestTTMemoryReporting.class);
  
  private MiniMRCluster miniMRCluster;

  /**
   * Fake scheduler to test the proper reporting of memory values by TT
   */
  public static class FakeTaskScheduler extends JobQueueTaskScheduler {
    
    private boolean hasPassed = true;
    private String message;
    
    public FakeTaskScheduler() {
      super();
    }
    
    public boolean hasTestPassed() {
      return hasPassed;
    }
    
    public String getFailureMessage() {
      return message;
    }
    
    @Override
    public List<Task> assignTasks(TaskTracker taskTracker)
        throws IOException {
      TaskTrackerStatus status = taskTracker.getStatus();
      
      long totalVirtualMemoryOnTT =
          getConf().getLong("totalVmemOnTT", JobConf.DISABLED_MEMORY_LIMIT);
      long totalPhysicalMemoryOnTT =
          getConf().getLong("totalPmemOnTT", JobConf.DISABLED_MEMORY_LIMIT);
      long mapSlotMemorySize =
          getConf().getLong("mapSlotMemorySize", JobConf.DISABLED_MEMORY_LIMIT);
      long reduceSlotMemorySize =
          getConf()
              .getLong("reduceSlotMemorySize", JobConf.DISABLED_MEMORY_LIMIT);

      long reportedTotalVirtualMemoryOnTT =
          status.getResourceStatus().getTotalVirtualMemory();
      long reportedTotalPhysicalMemoryOnTT =
          status.getResourceStatus().getTotalPhysicalMemory();
      long reportedMapSlotMemorySize =
          status.getResourceStatus().getMapSlotMemorySizeOnTT();
      long reportedReduceSlotMemorySize =
          status.getResourceStatus().getReduceSlotMemorySizeOnTT();

      message =
          "expected memory values : (totalVirtualMemoryOnTT, totalPhysicalMemoryOnTT, "
              + "mapSlotMemSize, reduceSlotMemorySize) = ("
              + totalVirtualMemoryOnTT + ", " + totalPhysicalMemoryOnTT + ","
              + mapSlotMemorySize + "," + reduceSlotMemorySize + ")";
      message +=
          "\nreported memory values : (totalVirtualMemoryOnTT, totalPhysicalMemoryOnTT, "
              + "reportedMapSlotMemorySize, reportedReduceSlotMemorySize) = ("
              + reportedTotalVirtualMemoryOnTT
              + ", "
              + reportedTotalPhysicalMemoryOnTT
              + ","
              + reportedMapSlotMemorySize
              + ","
              + reportedReduceSlotMemorySize
              + ")";
      LOG.info(message);
      if (totalVirtualMemoryOnTT != reportedTotalVirtualMemoryOnTT
          || totalPhysicalMemoryOnTT != reportedTotalPhysicalMemoryOnTT
          || mapSlotMemorySize != reportedMapSlotMemorySize
          || reduceSlotMemorySize != reportedReduceSlotMemorySize) {
        hasPassed = false;
      }
      return super.assignTasks(taskTracker);
    }
  }

  /**
   * Test that verifies default values are configured and reported correctly.
   * 
   * @throws Exception
   */
  public void testDefaultMemoryValues()
      throws Exception {
    JobConf conf = new JobConf();
    try {
      // Memory values are disabled by default.
      conf.setClass(
          org.apache.hadoop.mapred.TaskTracker.MAPRED_TASKTRACKER_MEMORY_CALCULATOR_PLUGIN_PROPERTY,
          DummyMemoryCalculatorPlugin.class, MemoryCalculatorPlugin.class);
      setUpCluster(conf);
      runSleepJob(miniMRCluster.createJobConf());
      verifyTestResults();
    } finally {
      tearDownCluster();
    }
  }

  /**
   * Test that verifies that configured values are reported correctly.
   * 
   * @throws Exception
   */
  public void testConfiguredMemoryValues()
      throws Exception {
    JobConf conf = new JobConf();
    conf.setLong("totalVmemOnTT", 4 * 1024 * 1024 * 1024L);
    conf.setLong("totalPmemOnTT", 2 * 1024 * 1024 * 1024L);
    conf.setLong("mapSlotMemorySize", 1 * 512L);
    conf.setLong("reduceSlotMemorySize", 1 * 1024L);

    conf.setClass(
        org.apache.hadoop.mapred.TaskTracker.MAPRED_TASKTRACKER_MEMORY_CALCULATOR_PLUGIN_PROPERTY,
        DummyMemoryCalculatorPlugin.class, MemoryCalculatorPlugin.class);
    conf.setLong(DummyMemoryCalculatorPlugin.MAXVMEM_TESTING_PROPERTY,
        4 * 1024 * 1024 * 1024L);
    conf.setLong(DummyMemoryCalculatorPlugin.MAXPMEM_TESTING_PROPERTY,
        2 * 1024 * 1024 * 1024L);
    conf.setLong(JobTracker.MAPRED_CLUSTER_MAP_MEMORY_MB_PROPERTY,
        512L);
    conf.setLong(
        JobTracker.MAPRED_CLUSTER_REDUCE_MEMORY_MB_PROPERTY, 1024L);
    
    try {
      setUpCluster(conf);
      JobConf jobConf = miniMRCluster.createJobConf();
      jobConf.setMemoryForMapTask(1 * 1024L);
      jobConf.setMemoryForReduceTask(2 * 1024L);
      runSleepJob(jobConf);
      verifyTestResults();
    } finally {
      tearDownCluster();
    }
  }

  /**
   * Test that verifies that total memory values are calculated and reported
   * correctly.
   * 
   * @throws Exception
   */
  public void testMemoryValuesOnLinux()
      throws Exception {
    if (!System.getProperty("os.name").startsWith("Linux")) {
      return;
    }

    JobConf conf = new JobConf();
    LinuxMemoryCalculatorPlugin plugin = new LinuxMemoryCalculatorPlugin();
    conf.setLong("totalVmemOnTT", plugin.getVirtualMemorySize());
    conf.setLong("totalPmemOnTT", plugin.getPhysicalMemorySize());

    try {
      setUpCluster(conf);
      runSleepJob(miniMRCluster.createJobConf());
      verifyTestResults();
    } finally {
      tearDownCluster();
    }
  }

  private void setUpCluster(JobConf conf)
                                throws Exception {
    conf.setClass("mapred.jobtracker.taskScheduler",
        TestTTMemoryReporting.FakeTaskScheduler.class, TaskScheduler.class);
    conf.set("mapred.job.tracker.handler.count", "1");
    miniMRCluster = new MiniMRCluster(1, "file:///", 3, null, null, conf);
  }
  
  private void runSleepJob(JobConf conf) throws Exception {
    String[] args = { "-m", "1", "-r", "1",
                      "-mt", "10", "-rt", "10" };
    ToolRunner.run(conf, new SleepJob(), args);
  }

  private void verifyTestResults() {
    FakeTaskScheduler scheduler = 
      (FakeTaskScheduler)miniMRCluster.getJobTrackerRunner().
                              getJobTracker().getTaskScheduler();
    assertTrue(scheduler.getFailureMessage(), scheduler.hasTestPassed());
  }
  
  private void tearDownCluster() {
    if (miniMRCluster != null) {
      miniMRCluster.shutdown();
    }
  }
}
