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
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.MemoryCalculatorPlugin;
import org.apache.hadoop.util.ProcfsBasedProcessTree;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileSystem;

import junit.framework.TestCase;

/**
 * Test class to verify memory management of tasks.
 */
public class TestTaskTrackerMemoryManager extends TestCase {

  private static final Log LOG =
      LogFactory.getLog(TestTaskTrackerMemoryManager.class);
  private MiniDFSCluster miniDFSCluster;
  private MiniMRCluster miniMRCluster;

  private String taskOverLimitPatternString =
      "TaskTree \\[pid=[0-9]*,tipID=.*\\] is running beyond memory-limits. "
          + "Current usage : [0-9]*bytes. Limit : %sbytes. Killing task.";

  private void startCluster(JobConf conf) throws Exception {
    miniDFSCluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fileSys = miniDFSCluster.getFileSystem();
    String namenode = fileSys.getUri().toString();
    miniMRCluster = new MiniMRCluster(1, namenode, 1, null, null, conf);
  }

  @Override
  protected void tearDown() {
    if (miniMRCluster != null) {
      miniMRCluster.shutdown();
    }
    if (miniDFSCluster != null) {
      miniDFSCluster.shutdown();
    }
  }

  private int runSleepJob(JobConf conf) throws Exception {
    String[] args = { "-m", "3", "-r", "1", "-mt", "3000", "-rt", "1000" };
    return ToolRunner.run(conf, new SleepJob(), args);
  }

  private void runAndCheckSuccessfulJob(JobConf conf)
      throws IOException {
    // Set up job.
    JobTracker jt = miniMRCluster.getJobTrackerRunner().getJobTracker();
    conf.set("mapred.job.tracker", jt.getJobTrackerMachine() + ":"
        + jt.getTrackerPort());
    NameNode nn = miniDFSCluster.getNameNode();
    conf.set("fs.default.name", "hdfs://"
        + nn.getNameNodeAddress().getHostName() + ":"
        + nn.getNameNodeAddress().getPort());

    Pattern taskOverLimitPattern =
        Pattern.compile(String.format(taskOverLimitPatternString, "[0-9]*"));
    Matcher mat = null;

    // Start the job.
    int ret;
    try {
      ret = runSleepJob(conf);
    } catch (Exception e) {
      ret = 1;
    }

    // Job has to succeed
    assertTrue(ret == 0);

    JobClient jClient = new JobClient(conf);
    JobStatus[] jStatus = jClient.getAllJobs();
    JobStatus js = jStatus[0]; // Our only job
    RunningJob rj = jClient.getJob(js.getJobID());

    // All events
    TaskCompletionEvent[] taskComplEvents = rj.getTaskCompletionEvents(0);

    for (TaskCompletionEvent tce : taskComplEvents) {
      String[] diagnostics =
          rj.getTaskDiagnostics(tce.getTaskAttemptId());

      if (diagnostics != null) {
        for (String str : diagnostics) {
          mat = taskOverLimitPattern.matcher(str);
          // The error pattern shouldn't be there in any TIP's diagnostics
          assertFalse(mat.find());
        }
      }
    }
  }

  private boolean isProcfsBasedTreeAvailable() {
    try {
      if (!ProcfsBasedProcessTree.isAvailable()) {
        LOG.info("Currently ProcessTree has only one implementation "
            + "ProcfsBasedProcessTree, which is not available on this "
            + "system. Not testing");
        return false;
      }
    } catch (Exception e) {
      LOG.info(StringUtils.stringifyException(e));
      return false;
    }
    return true;
  }

  /**
   * Test for verifying that nothing is killed when memory management is
   * disabled on the TT, even when the tasks run over their limits.
   * 
   * @throws Exception
   */
  public void testTTLimitsDisabled()
      throws Exception {
    // Run the test only if memory management is enabled
    if (!isProcfsBasedTreeAvailable()) {
      return;
    }

    JobConf conf = new JobConf();
    // Task-memory management disabled by default.
    startCluster(conf);
    long PER_TASK_LIMIT = 100L; // Doesn't matter how low.
    conf.setMaxVirtualMemoryForTask(PER_TASK_LIMIT);
    runAndCheckSuccessfulJob(conf);
  }

  /**
   * Test for verifying that tasks with no limits, with the cumulative usage
   * still under TT's limits, succeed.
   * 
   * @throws Exception
   */
  public void testTasksWithNoLimits()
      throws Exception {
    // Run the test only if memory management is enabled
    if (!isProcfsBasedTreeAvailable()) {
      return;
    }

    // Fairly large value for sleepJob to succeed
    long ttLimit = 4 * 1024 * 1024 * 1024L;
    // Start cluster with proper configuration.
    JobConf fConf = new JobConf();

    fConf.setClass(
        TaskTracker.MAPRED_TASKTRACKER_MEMORY_CALCULATOR_PLUGIN_PROPERTY,
        DummyMemoryCalculatorPlugin.class, MemoryCalculatorPlugin.class);
    fConf.setLong(DummyMemoryCalculatorPlugin.MAXVMEM_TESTING_PROPERTY,
        ttLimit);
    fConf.setLong(JobConf.MAPRED_TASK_DEFAULT_MAXVMEM_PROPERTY, ttLimit);
    fConf.setLong(JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY, ttLimit);
    fConf.setLong(
        TaskTracker.MAPRED_TASKTRACKER_VMEM_RESERVED_PROPERTY, 0);
    startCluster(fConf);
    JobConf conf = new JobConf();
    runAndCheckSuccessfulJob(conf);
  }

  /**
   * Test for verifying that tasks within limits, with the cumulative usage also
   * under TT's limits succeed.
   * 
   * @throws Exception
   */
  public void testTasksWithinLimits()
      throws Exception {
    // Run the test only if memory management is enabled
    if (!isProcfsBasedTreeAvailable()) {
      return;
    }

    // Large so that sleepjob goes through and fits total TT usage
    long PER_TASK_LIMIT = 2 * 1024 * 1024 * 1024L;
    long TASK_TRACKER_LIMIT = 4 * 1024 * 1024 * 1024L;

    // Start cluster with proper configuration.
    JobConf fConf = new JobConf();

    fConf.setClass(
        TaskTracker.MAPRED_TASKTRACKER_MEMORY_CALCULATOR_PLUGIN_PROPERTY,
        DummyMemoryCalculatorPlugin.class, MemoryCalculatorPlugin.class);
    fConf.setLong(DummyMemoryCalculatorPlugin.MAXVMEM_TESTING_PROPERTY,
        TASK_TRACKER_LIMIT);
    fConf.setLong(JobConf.MAPRED_TASK_DEFAULT_MAXVMEM_PROPERTY,
        TASK_TRACKER_LIMIT);
    fConf.setLong(JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY,
        TASK_TRACKER_LIMIT);
    fConf.setLong(
        TaskTracker.MAPRED_TASKTRACKER_VMEM_RESERVED_PROPERTY, 0);
    startCluster(fConf);
    JobConf conf = new JobConf();
    conf.setMaxVirtualMemoryForTask(PER_TASK_LIMIT);
    runAndCheckSuccessfulJob(conf);

  }

  /**
   * Test for verifying that tasks that go beyond limits, though the cumulative
   * usage is under TT's limits, get killed.
   * 
   * @throws Exception
   */
  public void testTasksBeyondLimits()
      throws Exception {

    // Run the test only if memory management is enabled
    if (!isProcfsBasedTreeAvailable()) {
      return;
    }

    long PER_TASK_LIMIT = 444; // Low enough to kill off sleepJob tasks.
    long TASK_TRACKER_LIMIT = 4 * 1024 * 1024 * 1024L; // Large so as to fit
    // total usage
    Pattern taskOverLimitPattern =
        Pattern.compile(String.format(taskOverLimitPatternString, String
            .valueOf(PER_TASK_LIMIT)));
    Matcher mat = null;

    // Start cluster with proper configuration.
    JobConf fConf = new JobConf();
    fConf.setClass(
        TaskTracker.MAPRED_TASKTRACKER_MEMORY_CALCULATOR_PLUGIN_PROPERTY,
        DummyMemoryCalculatorPlugin.class, MemoryCalculatorPlugin.class);
    fConf.setLong(DummyMemoryCalculatorPlugin.MAXVMEM_TESTING_PROPERTY,
        TASK_TRACKER_LIMIT);
    fConf.setLong(JobConf.MAPRED_TASK_DEFAULT_MAXVMEM_PROPERTY,
        TASK_TRACKER_LIMIT);
    fConf.setLong(JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY,
        TASK_TRACKER_LIMIT);
    fConf.setLong(
        TaskTracker.MAPRED_TASKTRACKER_VMEM_RESERVED_PROPERTY, 0);

    // very small value, so that no task escapes to successful completion.
    fConf.set("mapred.tasktracker.taskmemorymanager.monitoring-interval",
        String.valueOf(300));
    startCluster(fConf);

    // Set up job.
    JobConf conf = new JobConf();
    conf.setMaxVirtualMemoryForTask(PER_TASK_LIMIT);
    JobTracker jt = miniMRCluster.getJobTrackerRunner().getJobTracker();
    conf.set("mapred.job.tracker", jt.getJobTrackerMachine() + ":"
        + jt.getTrackerPort());
    NameNode nn = miniDFSCluster.getNameNode();
    conf.set("fs.default.name", "hdfs://"
        + nn.getNameNodeAddress().getHostName() + ":"
        + nn.getNameNodeAddress().getPort());

    // Start the job.
    int ret = 0;
    try {
      ret = runSleepJob(conf);
    } catch (Exception e) {
      ret = 1;
    }

    // Job has to fail
    assertTrue(ret != 0);

    JobClient jClient = new JobClient(conf);
    JobStatus[] jStatus = jClient.getAllJobs();
    JobStatus js = jStatus[0]; // Our only job
    RunningJob rj = jClient.getJob(js.getJobID());

    // All events
    TaskCompletionEvent[] taskComplEvents = rj.getTaskCompletionEvents(0);

    for (TaskCompletionEvent tce : taskComplEvents) {
      // Every task HAS to fail
      assert (tce.getTaskStatus() == TaskCompletionEvent.Status.TIPFAILED || tce
          .getTaskStatus() == TaskCompletionEvent.Status.FAILED);

      String[] diagnostics =
          rj.getTaskDiagnostics(tce.getTaskAttemptId());

      // Every task HAS to spit out the out-of-memory errors
      assert (diagnostics != null);

      for (String str : diagnostics) {
        mat = taskOverLimitPattern.matcher(str);
        // Every task HAS to spit out the out-of-memory errors in the same
        // format. And these are the only diagnostic messages.
        assertTrue(mat.find());
      }
    }
  }

  /**
   * Test for verifying that tasks causing cumulative usage to go beyond TT's
   * limit get killed even though they all are under individual limits. Memory
   * management for tasks with disabled task-limits also traverses the same
   * code-path, so we don't need a separate testTaskLimitsDisabled.
   * 
   * @throws Exception
   */
  public void testTasksCumulativelyExceedingTTLimits()
      throws Exception {

    // Run the test only if memory management is enabled
    if (!isProcfsBasedTreeAvailable()) {
      return;
    }

    // Large enough for SleepJob Tasks.
    long PER_TASK_LIMIT = 100000000000L;
    // Very Limited TT. All tasks will be killed.
    long TASK_TRACKER_LIMIT = 100L;
    Pattern taskOverLimitPattern =
        Pattern.compile(String.format(taskOverLimitPatternString, String
            .valueOf(PER_TASK_LIMIT)));
    Pattern trackerOverLimitPattern =
        Pattern
            .compile("Killing one of the least progress tasks - .*, as "
                + "the cumulative memory usage of all the tasks on the TaskTracker"
                + " exceeds virtual memory limit " + TASK_TRACKER_LIMIT + ".");
    Matcher mat = null;

    // Start cluster with proper configuration.
    JobConf fConf = new JobConf();
    fConf.setClass(
        TaskTracker.MAPRED_TASKTRACKER_MEMORY_CALCULATOR_PLUGIN_PROPERTY,
        DummyMemoryCalculatorPlugin.class, MemoryCalculatorPlugin.class);
    fConf.setLong(DummyMemoryCalculatorPlugin.MAXVMEM_TESTING_PROPERTY,
        TASK_TRACKER_LIMIT);
    fConf.setLong(JobConf.MAPRED_TASK_DEFAULT_MAXVMEM_PROPERTY,
        TASK_TRACKER_LIMIT);
    fConf.setLong(JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY,
        TASK_TRACKER_LIMIT);
    fConf.setLong(
        TaskTracker.MAPRED_TASKTRACKER_VMEM_RESERVED_PROPERTY, 0);
    // very small value, so that no task escapes to successful completion.
    fConf.set("mapred.tasktracker.taskmemorymanager.monitoring-interval",
        String.valueOf(300));

    startCluster(fConf);

    // Set up job.
    JobConf conf = new JobConf();
    conf.setMaxVirtualMemoryForTask(PER_TASK_LIMIT);
    JobTracker jt = miniMRCluster.getJobTrackerRunner().getJobTracker();
    conf.set("mapred.job.tracker", jt.getJobTrackerMachine() + ":"
        + jt.getTrackerPort());
    NameNode nn = miniDFSCluster.getNameNode();
    conf.set("fs.default.name", "hdfs://"
        + nn.getNameNodeAddress().getHostName() + ":"
        + nn.getNameNodeAddress().getPort());

    JobClient jClient = new JobClient(conf);
    SleepJob sleepJob = new SleepJob();
    sleepJob.setConf(conf);
    // Start the job
    Job job = sleepJob.createJob(1, 1, 5000, 1, 1000, 1);
    job.submit();
    boolean TTOverFlowMsgPresent = false;
    while (true) {
      // Set-up tasks are the first to be launched.
      TaskReport[] setUpReports = jClient.getSetupTaskReports(
                                    (org.apache.hadoop.mapred.JobID)job.getID());
      for (TaskReport tr : setUpReports) {
        String[] diag = tr.getDiagnostics();
        for (String str : diag) {
          mat = taskOverLimitPattern.matcher(str);
          assertFalse(mat.find());
          mat = trackerOverLimitPattern.matcher(str);
          if (mat.find()) {
            TTOverFlowMsgPresent = true;
          }
        }
      }
      if (TTOverFlowMsgPresent) {
        break;
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // nothing
      }
    }
    // If it comes here without a test-timeout, it means there was a task that
    // was killed because of crossing cumulative TT limit.

    // Test succeeded, kill the job.
    job.killJob();
  }
}
