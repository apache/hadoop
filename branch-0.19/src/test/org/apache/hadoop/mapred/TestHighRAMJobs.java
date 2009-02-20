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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.util.ToolRunner;

import junit.framework.TestCase;

/**
 * This test class tests the functionality related to configuring, reporting
 * and computing memory related parameters in a Map/Reduce cluster.
 * 
 * Each test sets up a {@link MiniMRCluster} with a locally defined 
 * {@link org.apache.hadoop.mapred.TaskScheduler}. This scheduler validates 
 * the memory related configuration is correctly computed and reported from 
 * the tasktracker in 
 * {@link org.apache.hadoop.mapred.TaskScheduler.assignTasks()}.
 *  
 */
public class TestHighRAMJobs extends TestCase {

  private static final Log LOG = LogFactory.getLog(TestHighRAMJobs.class);

  private static final String DEFAULT_SLEEP_JOB_MAP_COUNT = "1";
  private static final String DEFAULT_SLEEP_JOB_REDUCE_COUNT = "1";
  private static final String DEFAULT_MAP_SLEEP_TIME = "1000";
  private static final String DEFAULT_REDUCE_SLEEP_TIME = "1000";
  private static final long DISABLED_VIRTUAL_MEMORY_LIMIT = -1L;
  
  private MiniDFSCluster miniDFSCluster;
  private MiniMRCluster miniMRCluster;
  
  public static class FakeTaskScheduler extends JobQueueTaskScheduler {
    
    private boolean hasPassed = true;
    private String message;
    private boolean isFirstTime = true;
    
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
    public List<Task> assignTasks(TaskTrackerStatus status) 
                                          throws IOException {
      TestHighRAMJobs.LOG.info("status = " + status.getResourceStatus().getFreeVirtualMemory());

      long initialFreeMemory = getConf().getLong("initialFreeMemory", 0L);
      long totalMemoryOnTT = getConf().getLong("totalMemoryOnTT", 0L);

      if (isFirstTime) {
        isFirstTime = false;
        if (initialFreeMemory != status.getResourceStatus().getFreeVirtualMemory()) {
          hasPassed = false;
          message = "Initial memory expected = " + initialFreeMemory
                      + " reported = " + status.getResourceStatus().getFreeVirtualMemory();
        }
        if (totalMemoryOnTT != status.getResourceStatus().getTotalMemory()) {
          hasPassed = false;
          message = "Total memory on TT expected = " + totalMemoryOnTT
                      + " reported = " 
                      + status.getResourceStatus().getTotalMemory();
        }
      } else if (initialFreeMemory != DISABLED_VIRTUAL_MEMORY_LIMIT) {
        
        long memoryPerTask = getConf().getLong("memoryPerTask", 0L);
          
        long expectedFreeMemory = 0;
        int runningTaskCount = status.countMapTasks() +
                              status.countReduceTasks();
        expectedFreeMemory = initialFreeMemory - 
                                (memoryPerTask * runningTaskCount);

        TestHighRAMJobs.LOG.info("expected free memory = " + 
                                  expectedFreeMemory + ", reported = " + 
                                  status.getResourceStatus().getFreeVirtualMemory());
        if (expectedFreeMemory != status.getResourceStatus().getFreeVirtualMemory()) {
          hasPassed = false;
          message = "Expected free memory after " + runningTaskCount
                      + " tasks are scheduled = " + expectedFreeMemory
                      + ", reported = " + status.getResourceStatus().getFreeVirtualMemory();
        }
      }
      return super.assignTasks(status);
    }
  }
  
  /* Test that verifies default values are configured and reported
   * correctly.
   */
  public void testDefaultValuesForHighRAMJobs() throws Exception {
    long defaultMemoryLimit = DISABLED_VIRTUAL_MEMORY_LIMIT;
    try {
      setUpCluster(defaultMemoryLimit, defaultMemoryLimit, null);
      runJob(defaultMemoryLimit, DEFAULT_MAP_SLEEP_TIME, 
          DEFAULT_REDUCE_SLEEP_TIME, DEFAULT_SLEEP_JOB_MAP_COUNT, 
          DEFAULT_SLEEP_JOB_REDUCE_COUNT);
      verifyTestResults();
    } finally {
      tearDownCluster();
    }
  }
  
  /* Test that verifies default value for memory per task on TT
   * when the number of slots is non-default.
   */
  public void testDefaultMemoryPerTask() throws Exception {
    long maxVmem = 2*1024*1024*1024L;
    JobConf conf = new JobConf();
    conf.setInt("mapred.tasktracker.map.tasks.maximum", 2);
    conf.setInt("mapred.tasktracker.reduce.tasks.maximum", 2);
    // set a different value for the default memory per task
    long defaultMemPerTask = 256*1024*1024L; 
    try {
      setUpCluster(maxVmem, defaultMemPerTask, 
                    defaultMemPerTask, conf);
      runJob(DISABLED_VIRTUAL_MEMORY_LIMIT, "10000",
              DEFAULT_REDUCE_SLEEP_TIME, DEFAULT_SLEEP_JOB_MAP_COUNT,
              DEFAULT_SLEEP_JOB_REDUCE_COUNT);
      verifyTestResults();
    } finally {
      tearDownCluster();
    }
  }
  
  public void testHighRAMJob() throws Exception {
    long maxVmem = 1024*1024*1024L;
    //long defaultMemPerTaskOnTT = maxVmem/4; // 4 = default number of slots.
    /* Set a HIGH RAM requirement for a job. As 4 is the
     * default number of slots, we set up the memory limit
     * per task to be more than 25%. 
     */
    long maxVmemPerTask = maxVmem/3;
    try {
      setUpCluster(maxVmem, maxVmemPerTask, null);
      /* set up sleep limits higher, so the scheduler will see varying
       * number of running tasks at a time. Also modify the number of
       * map tasks so we test the iteration over more than one task.
       */
      runJob(maxVmemPerTask, "10000", "10000", "2", 
                      DEFAULT_SLEEP_JOB_REDUCE_COUNT);
      verifyTestResults();
    } finally {
      tearDownCluster();
    }
  }
  
  private void setUpCluster(long totalMemoryOnTT, long memoryPerTask,
                              JobConf conf) throws Exception {
    this.setUpCluster(totalMemoryOnTT, 512*1024*1024L, 
                          memoryPerTask, conf);
  }
  
  private void setUpCluster(long totalMemoryOnTT, long defaultMemoryPerTask,
                              long memoryPerTask, JobConf conf)
                                throws Exception {
    if (conf == null) {
      conf = new JobConf();
    }
    conf.setClass("mapred.jobtracker.taskScheduler", 
        TestHighRAMJobs.FakeTaskScheduler.class,
        TaskScheduler.class);
    if (totalMemoryOnTT != -1L) {
      conf.setLong("mapred.tasktracker.tasks.maxmemory", totalMemoryOnTT);  
    }
    conf.setLong("mapred.task.default.maxmemory", defaultMemoryPerTask);
    conf.setLong("initialFreeMemory", totalMemoryOnTT);
    conf.setLong("totalMemoryOnTT", totalMemoryOnTT);
    conf.setLong("memoryPerTask", memoryPerTask);
    miniDFSCluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fileSys = miniDFSCluster.getFileSystem();
    String namenode = fileSys.getUri().toString();
    miniMRCluster = new MiniMRCluster(1, namenode, 3, 
                      null, null, conf);    
  }
  
  private void runJob(long memoryPerTask, String mapSleepTime,
                        String reduceSleepTime, String mapTaskCount,
                        String reduceTaskCount) 
                                        throws Exception {
    Configuration sleepJobConf = new Configuration();
    sleepJobConf.set("mapred.job.tracker", "localhost:"
                              + miniMRCluster.getJobTrackerPort());
    if (memoryPerTask != -1L) {
      sleepJobConf.setLong("mapred.task.maxmemory", memoryPerTask);
    }
    launchSleepJob(mapSleepTime, reduceSleepTime, 
                    mapTaskCount, reduceTaskCount, sleepJobConf);    
  }

  private void launchSleepJob(String mapSleepTime, String reduceSleepTime,
                              String mapTaskCount, String reduceTaskCount,
                              Configuration conf) throws Exception {
    String[] args = { "-m", mapTaskCount, "-r", reduceTaskCount,
                      "-mt", mapSleepTime, "-rt", reduceSleepTime };
    ToolRunner.run(conf, new SleepJob(), args);
  }

  private void verifyTestResults() {
    FakeTaskScheduler scheduler = 
      (FakeTaskScheduler)miniMRCluster.getJobTrackerRunner().
                              getJobTracker().getTaskScheduler();
    assertTrue(scheduler.getFailureMessage(), scheduler.hasTestPassed());
  }
  
  private void tearDownCluster() {
    if (miniMRCluster != null) { miniMRCluster.shutdown(); }
    if (miniDFSCluster != null) { miniDFSCluster.shutdown(); }
  }
}
