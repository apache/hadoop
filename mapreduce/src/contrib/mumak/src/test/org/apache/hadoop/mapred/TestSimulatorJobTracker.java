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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import java.util.HashSet;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.mapred.TaskStatus.Phase;
import org.apache.hadoop.mapred.TaskStatus.State;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapred.FakeJobs;
import org.junit.Test;

public class TestSimulatorJobTracker {

  SimulatorTaskTracker taskTracker;

  public static final Log LOG = LogFactory
      .getLog(TestSimulatorJobTracker.class);

  @SuppressWarnings("deprecation")
  public JobConf createJobConf() {
    JobConf jtConf = new JobConf();
    jtConf.set("mapred.job.tracker", "localhost:8012");
    jtConf.set("mapred.jobtracker.job.history.block.size", "512");
    jtConf.set("mapred.jobtracker.job.history.buffer.size", "512");
    jtConf.setLong("mapred.tasktracker.expiry.interval", 5000);
    jtConf.setInt("mapred.reduce.copy.backoff", 4);
    jtConf.setLong("mapred.job.reuse.jvm.num.tasks", -1);
    jtConf.setUser("mumak");
    jtConf.set("mapred.system.dir", jtConf.get("hadoop.tmp.dir", "/tmp/hadoop-"
        + jtConf.getUser())
        + "/mapred/system");
    jtConf.setBoolean(JTConfig.JT_PERSIST_JOBSTATUS, false);
    System.out.println("Created JobConf");
    return jtConf;
  }

  public static class FakeJobClient {

    ClientProtocol jobTracker;
    int numMaps;
    int numReduces;

    public FakeJobClient(ClientProtocol jobTracker, int numMaps,
        int numReduces) {
      this.jobTracker = jobTracker;
      this.numMaps = numMaps;
      this.numReduces = numReduces;
    }

    public void submitNewJob() throws IOException, InterruptedException {
      org.apache.hadoop.mapreduce.JobID jobId = jobTracker.getNewJobID();
      LOG.info("Obtained from Jobtracker jobid = " + jobId);
      FakeJobs job = new FakeJobs("job1", 0, numMaps, numReduces);

      SimulatorJobCache.put(org.apache.hadoop.mapred.JobID.downgrade(jobId), job);
      jobTracker.submitJob(jobId, "dummy-path", null);
    }
  }

  public static class FakeTaskTracker extends SimulatorTaskTracker {

    boolean firstHeartbeat = true;
    short responseId = 0;
    int now = 0;

    FakeTaskTracker(InterTrackerProtocol jobTracker, Configuration conf) {
      super(jobTracker, conf);

      LOG.info("FakeTaskTracker constructor, taskTrackerName="
          + taskTrackerName);
    }

    private List<TaskStatus> collectAndCloneTaskStatuses() {
      ArrayList<TaskStatus> statuses = new ArrayList<TaskStatus>();
      Set<TaskAttemptID> mark = new HashSet<TaskAttemptID>();
      for (SimulatorTaskInProgress tip : tasks.values()) {
        statuses.add((TaskStatus) tip.getTaskStatus().clone());
        if (tip.getFinalRunState() == State.SUCCEEDED) {
          mark.add(tip.getTaskStatus().getTaskID());
        }
      }

      for (TaskAttemptID taskId : mark) {
        tasks.remove(taskId);
      }

      return statuses;
    }

    public int sendFakeHeartbeat(int current) throws IOException {

      int numLaunchTaskActions = 0;
      this.now = current;
      List<TaskStatus> taskStatuses = collectAndCloneTaskStatuses();
      TaskTrackerStatus taskTrackerStatus = new SimulatorTaskTrackerStatus(
          taskTrackerName, hostName, httpPort, taskStatuses, 0, maxMapSlots,
          maxReduceSlots, this.now);
      // Transmit the heartbeat
      HeartbeatResponse response = null;
      LOG.debug("sending heartbeat at time = " + this.now + " responseId = "
          + responseId);
      response = jobTracker.heartbeat(taskTrackerStatus, false, firstHeartbeat,
          true, responseId);

      firstHeartbeat = false;
      responseId = response.getResponseId();
      numLaunchTaskActions = findLaunchTaskActions(response);

      return numLaunchTaskActions;
    }

    int findLaunchTaskActions(HeartbeatResponse response) {
      TaskTrackerAction[] actions = response.getActions();
      int numLaunchTaskActions = 0;
      // HashSet<> numLaunchTaskActions
      for (TaskTrackerAction action : actions) {
        if (action instanceof SimulatorLaunchTaskAction) {
          Task task = ((SimulatorLaunchTaskAction) action).getTask();

          numLaunchTaskActions++;
          TaskAttemptID taskId = task.getTaskID();
          if (tasks.containsKey(taskId)) {
            // already have this task..do not need to generate new status
            continue;
          }
          TaskStatus status;
          if (task.isMapTask()) {
            status = new MapTaskStatus(taskId, 0f, 1, State.RUNNING, "", "",
                taskTrackerName, Phase.MAP, new Counters());
          } else {
            status = new ReduceTaskStatus(taskId, 0f, 1, State.RUNNING, "", "",
                taskTrackerName, Phase.SHUFFLE, new Counters());
          }
          status.setRunState(State.SUCCEEDED);
          status.setStartTime(this.now);
          SimulatorTaskInProgress tip = new SimulatorTaskInProgress(
              (SimulatorLaunchTaskAction) action, status, this.now);
          tasks.put(taskId, tip);
        }
      }
      return numLaunchTaskActions;
    }

  }
  
  @Test
  public void testTrackerInteraction() throws IOException, InterruptedException {
    LOG.info("Testing Inter Tracker protocols");
    int now = 0;
    JobConf jtConf = createJobConf();
    int NoMaps = 2;
    int NoReduces = 10;

    // jtConf.set("mapred.jobtracker.taskScheduler",
    // DummyTaskScheduler.class.getName());
    jtConf.set("fs.default.name", "file:///");
    jtConf.set("mapred.jobtracker.taskScheduler", JobQueueTaskScheduler.class
        .getName());
    SimulatorJobTracker sjobTracker = SimulatorJobTracker.startTracker(jtConf,
        0);
    System.out.println("Created the SimulatorJobTracker successfully");
    sjobTracker.offerService();

    FakeJobClient jbc = new FakeJobClient(sjobTracker, NoMaps, NoReduces);
    int NoJobs = 1;
    for (int i = 0; i < NoJobs; i++) {
      jbc.submitNewJob();
    }
    org.apache.hadoop.mapreduce.JobStatus[] allJobs = sjobTracker.getAllJobs();
    Assert.assertTrue("allJobs queue length is " + allJobs.length, allJobs.length >= 1);
    for (org.apache.hadoop.mapreduce.JobStatus js : allJobs) {
      LOG.info("From JTQueue: job id = " + js.getJobID());
    }

    Configuration ttConf = new Configuration();
    ttConf.set("mumak.tasktracker.tracker.name", 
               "tracker_host1.foo.com:localhost/127.0.0.1:9010");
    ttConf.set("mumak.tasktracker.host.name", "host1.foo.com"); 
    ttConf.setInt("mapred.tasktracker.map.tasks.maximum", 10); 
    ttConf.setInt("mapred.tasktracker.reduce.tasks.maximum", 10);
    ttConf.setInt("mumak.tasktracker.heartbeat.fuzz", -1); 
    FakeTaskTracker fakeTracker = new FakeTaskTracker(sjobTracker, ttConf);
    
    int numLaunchTaskActions = 0;

    for (int i = 0; i < NoMaps * 2; ++i) { // we should be able to assign all
                                           // tasks within 2X of NoMaps
                                           // heartbeats
      numLaunchTaskActions += fakeTracker.sendFakeHeartbeat(now);
      if (numLaunchTaskActions >= NoMaps) {
        break;
      }
      now += 5;
      LOG.debug("Number of MapLaunchTasks=" + numLaunchTaskActions + " now = "
          + now);
    }

    Assert.assertTrue("Failed to launch all maps: " + numLaunchTaskActions,
        numLaunchTaskActions >= NoMaps);

    // sending the completed status
    LOG.info("Sending task completed status");
    numLaunchTaskActions += fakeTracker.sendFakeHeartbeat(now);
    // now for the reduce tasks
    for (int i = 0; i < NoReduces * 2; ++i) { // we should be able to assign all
                                              // tasks within 2X of NoReduces
                                              // heartbeats
      if (numLaunchTaskActions >= NoMaps + NoReduces) {
        break;
      }
      numLaunchTaskActions += fakeTracker.sendFakeHeartbeat(now);
      now += 5;
      LOG.debug("Number of ReduceLaunchTasks=" + numLaunchTaskActions
          + " now = " + now);
    }
    Assert.assertTrue("Failed to launch all reduces: " + numLaunchTaskActions,
        numLaunchTaskActions >= NoMaps + NoReduces);

    // sending the reduce completion
    numLaunchTaskActions += fakeTracker.sendFakeHeartbeat(now);
  }
}
