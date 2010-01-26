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

/**
 * TestJobInProgress is a unit test to test consistency of JobInProgress class
 * data structures under different conditions (speculation/locality) and at
 * different stages (tasks are running/pending/killed)
 */

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.FakeObjectUtilities.FakeJobInProgress;
import org.apache.hadoop.mapred.FakeObjectUtilities.FakeJobTracker;
import org.apache.hadoop.mapred.TaskStatus.Phase;
import org.apache.hadoop.mapred.UtilsForTests.FakeClock;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.StaticMapping;

@SuppressWarnings("deprecation")
public class TestJobInProgress extends TestCase {
  static final Log LOG = LogFactory.getLog(TestJobInProgress.class);

  static FakeJobTracker jobTracker;

  static String trackers[] = new String[] {
    "tracker_tracker1.r1.com:1000", 
    "tracker_tracker2.r1.com:1000",
    "tracker_tracker3.r2.com:1000",
    "tracker_tracker4.r3.com:1000"
  };

  static String[] hosts = new String[] {
    "tracker1.r1.com",
    "tracker2.r1.com",
    "tracker3.r2.com",
    "tracker4.r3.com"
  };

  static String[] racks = new String[] { "/r1", "/r1", "/r2", "/r3" };

  static int numUniqueHosts = hosts.length;
  static int clusterSize = trackers.length;

  public static Test suite() {
    TestSetup setup = new TestSetup(new TestSuite(TestJobInProgress.class)) {
      protected void setUp() throws Exception {
        JobConf conf = new JobConf();
        conf.set(JTConfig.JT_IPC_ADDRESS, "localhost:0");
        conf.set(JTConfig.JT_HTTP_ADDRESS, "0.0.0.0:0");
        conf.setClass("topology.node.switch.mapping.impl", 
            StaticMapping.class, DNSToSwitchMapping.class);
        jobTracker = new FakeJobTracker(conf, new FakeClock(), trackers);
        // Set up the Topology Information
        for (int i = 0; i < hosts.length; i++) {
          StaticMapping.addNodeToRack(hosts[i], racks[i]);
        }
        for (String s: trackers) {
          FakeObjectUtilities.establishFirstContact(jobTracker, s);
        }
      }
    };
    return setup;
  }

  static class MyFakeJobInProgress extends FakeJobInProgress {

    MyFakeJobInProgress(JobConf jc, JobTracker jt) throws IOException {
      super(jc, jt);
    }

    @Override
    TaskSplitMetaInfo[] createSplits(org.apache.hadoop.mapreduce.JobID jobId) {
      // Set all splits to reside on one host. This will ensure that 
      // one tracker gets data local, one gets rack local and two others
      // get non-local maps
      TaskSplitMetaInfo[] splits = new TaskSplitMetaInfo[numMapTasks];
      String[] splitHosts0 = new String[] { hosts[0] };
      for (int i = 0; i < numMapTasks; i++) {
        splits[i] = new TaskSplitMetaInfo(splitHosts0, 0, 0);
      }
      return splits;
    }

    private void makeRunning(TaskAttemptID taskId, TaskInProgress tip, 
        String taskTracker) {
      TaskStatus status = TaskStatus.createTaskStatus(tip.isMapTask(), taskId, 
          0.0f, 1, TaskStatus.State.RUNNING, "", "", taskTracker,
          tip.isMapTask() ? Phase.MAP : Phase.REDUCE, new Counters());
      updateTaskStatus(tip, status);
    }

    private TaskInProgress getTipForTaskID(TaskAttemptID tid, boolean isMap) {
      TaskInProgress result = null;
      TaskID id = tid.getTaskID();
      TaskInProgress[] arrayToLook = isMap ? maps : reduces;

      for (int i = 0; i < arrayToLook.length; i++) {
        TaskInProgress tip = arrayToLook[i];
        if (tip.getTIPId() == id) {
          result = tip;
          break;
        }
      }
      return result;
    }

    /**
     * Find a new Map or a reduce task and mark it as running on the specified
     * tracker
     */
    public TaskAttemptID findAndRunNewTask(boolean isMap, 
        String tt, String host,
        int clusterSize,
        int numUniqueHosts)
    throws IOException {
      TaskTrackerStatus tts = new TaskTrackerStatus(tt, host);
      Task task = isMap ? 
          obtainNewMapTask(tts, clusterSize, numUniqueHosts) : 
            obtainNewReduceTask(tts, clusterSize, numUniqueHosts);
          TaskAttemptID tid = task.getTaskID();
          makeRunning(task.getTaskID(), getTipForTaskID(tid, isMap), tt);
          return tid;
    }
  }

  public void testPendingMapTaskCount() throws Exception {

    int numMaps = 4;
    int numReds = 4;

    JobConf conf = new JobConf();
    conf.setNumMapTasks(numMaps);
    conf.setNumReduceTasks(numReds);
    conf.setSpeculativeExecution(false);
    conf.setBoolean(
        JobContext.SETUP_CLEANUP_NEEDED, false);
    MyFakeJobInProgress job1 = new MyFakeJobInProgress(conf, jobTracker);
    job1.initTasks();

    TaskAttemptID[] tid = new TaskAttemptID[numMaps];

    for (int i = 0; i < numMaps; i++) {
      tid[i] = job1.findAndRunNewTask(true, trackers[i], hosts[i],
          clusterSize, numUniqueHosts);
    }

    // Fail all maps
    for (int i = 0; i < numMaps; i++) {
      job1.failTask(tid[i]);
    }

    MyFakeJobInProgress job2 = new MyFakeJobInProgress(conf, jobTracker);
    job2.initTasks();

    for (int i = 0; i < numMaps; i++) {
      tid[i] = job2.findAndRunNewTask(true, trackers[i], hosts[i],
          clusterSize, numUniqueHosts);
      job2.finishTask(tid[i]);
    }

    for (int i = 0; i < numReds/2; i++) {
      tid[i] = job2.findAndRunNewTask(false, trackers[i], hosts[i],
          clusterSize, numUniqueHosts);
    }

    for (int i = 0; i < numReds/4; i++) {
      job2.finishTask(tid[i]);
    }

    for (int i = numReds/4; i < numReds/2; i++) {
      job2.failTask(tid[i]);
    }

    // Job1. All Maps have failed, no reduces have been scheduled
    checkTaskCounts(job1, 0, numMaps, 0, numReds);

    // Job2. All Maps have completed. One reducer has completed, one has 
    // failed and two others have not been scheduled
    checkTaskCounts(job2, 0, 0, 0, 3 * numReds / 4);
  }

  /**
   * Test if running tasks are correctly maintained for various types of jobs
   */
  static void testRunningTaskCount(boolean speculation)  throws Exception {
    LOG.info("Testing running jobs with speculation : " + speculation); 

    JobConf conf = new JobConf();
    conf.setNumMapTasks(2);
    conf.setNumReduceTasks(2);
    conf.setSpeculativeExecution(speculation);
    MyFakeJobInProgress jip = new MyFakeJobInProgress(conf, jobTracker);
    jip.initTasks();

    TaskAttemptID[] tid = new TaskAttemptID[4];

    for (int i = 0; i < 2; i++) {
      tid[i] = jip.findAndRunNewTask(true, trackers[i], hosts[i],
          clusterSize, numUniqueHosts);
    }

    // check if the running structures are populated
    Set<TaskInProgress> uniqueTasks = new HashSet<TaskInProgress>();
    for (Map.Entry<Node, Set<TaskInProgress>> s : 
      jip.getRunningMapCache().entrySet()) {
      uniqueTasks.addAll(s.getValue());
    }

    // add non local map tasks
    uniqueTasks.addAll(jip.getNonLocalRunningMaps());

    assertEquals("Running map count doesnt match for jobs with speculation " 
        + speculation,
        jip.runningMaps(), uniqueTasks.size());

    for (int i = 0; i < 2; i++ ) {
      tid[i] = jip.findAndRunNewTask(false, trackers[i], hosts[i],
          clusterSize, numUniqueHosts);
    }

    assertEquals("Running reducer count doesnt match for" +
        " jobs with speculation "
        + speculation,
        jip.runningReduces(), jip.getRunningReduces().size());

  }

  public void testRunningTaskCount() throws Exception {
    // test with spec = false 
    testRunningTaskCount(false);

    // test with spec = true
    testRunningTaskCount(true);

  }

  static void checkTaskCounts(JobInProgress jip, int runningMaps,
      int pendingMaps, int runningReduces, int pendingReduces) {
    Counters counter = jip.getJobCounters();
    long totalTaskCount = counter.getCounter(JobCounter.TOTAL_LAUNCHED_MAPS)
    + counter.getCounter(JobCounter.TOTAL_LAUNCHED_REDUCES);

    LOG.info("totalTaskCount is " + totalTaskCount);
    LOG.info(" Running Maps:" + jip.runningMaps() +
        " Pending Maps:" + jip.pendingMaps() + 
        " Running Reds:" + jip.runningReduces() + 
        " Pending Reds:" + jip.pendingReduces());

    assertEquals(jip.getNumTaskCompletionEvents(),totalTaskCount);
    assertEquals(runningMaps, jip.runningMaps());
    assertEquals(pendingMaps, jip.pendingMaps());
    assertEquals(runningReduces, jip.runningReduces());
    assertEquals(pendingReduces, jip.pendingReduces());
  }

}
