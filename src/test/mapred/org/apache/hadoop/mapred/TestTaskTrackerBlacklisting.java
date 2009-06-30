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
import java.util.HashMap;
import java.util.HashSet;

import java.util.List;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobTracker.ReasonForBlackListing;
import org.apache.hadoop.mapred.TaskStatus.Phase;
import org.apache.hadoop.mapred.TaskTrackerStatus.TaskTrackerHealthStatus;

import junit.framework.TestCase;

public class TestTaskTrackerBlacklisting extends TestCase {

  private static volatile Log LOG = LogFactory
      .getLog(TestTaskTrackerBlacklisting.class);

  static String trackers[] = new String[] { "tracker_tracker1:1000",
      "tracker_tracker2:1000", "tracker_tracker3:1000" };

  static String hosts[] = new String[] { "tracker1", "tracker2", "tracker3" };

  private FakeJobTracker jobTracker;

  private FakeJobTrackerClock clock;

  private short responseId;

  private static HashSet<ReasonForBlackListing> nodeUnHealthyReasonSet = new HashSet<ReasonForBlackListing>();

  private static HashSet<ReasonForBlackListing> exceedsFailuresReasonSet = new HashSet<ReasonForBlackListing>();

  private static HashSet<ReasonForBlackListing> unhealthyAndExceedsFailure = new HashSet<ReasonForBlackListing>();

  static {
    nodeUnHealthyReasonSet.add(ReasonForBlackListing.NODE_UNHEALTHY);
    exceedsFailuresReasonSet.add(ReasonForBlackListing.EXCEEDING_FAILURES);
    unhealthyAndExceedsFailure.add(ReasonForBlackListing.NODE_UNHEALTHY);
    unhealthyAndExceedsFailure.add(ReasonForBlackListing.EXCEEDING_FAILURES);
  }
  private static final long aDay = 24 * 60 * 60 * 1000;

  private class FakeJobTrackerClock extends Clock {
    boolean jumpADay = false;

    @Override
    long getTime() {
      if (!jumpADay) {
        return super.getTime();
      } else {
        long now = super.getTime();
        return now + aDay;
      }
    }
  }

  static class FakeJobTracker extends
      org.apache.hadoop.mapred.TestSpeculativeExecution.FakeJobTracker {
    // initialize max{Map/Reduce} task capacities to twice the clustersize
    int totalSlots = trackers.length * 4;

    FakeJobTracker(JobConf conf, Clock clock) throws IOException,
        InterruptedException {
      super(conf, clock);
    }

    @Override
    public ClusterStatus getClusterStatus(boolean detailed) {
      return new ClusterStatus(trackers.length, 0, 0, 0, 0, totalSlots / 2,
          totalSlots / 2, JobTracker.State.RUNNING, 0);
    }

    public void setNumSlots(int totalSlots) {
      this.totalSlots = totalSlots;
    }

    @Override
    synchronized void finalizeJob(JobInProgress job) {
      List<String> blackListedTrackers = job.getBlackListedTrackers();
      for (String tracker : blackListedTrackers) {
        incrementFaults(tracker);
      }
    }
  }

  static class FakeJobInProgress extends
      org.apache.hadoop.mapred.TestSpeculativeExecution.FakeJobInProgress {
    HashMap<String, Integer> trackerToFailureMap;

    FakeJobInProgress(JobConf jobConf, JobTracker tracker) throws IOException {
      super(jobConf, tracker);
      // initObjects(tracker, numMaps, numReduces);
      trackerToFailureMap = new HashMap<String, Integer>();
    }

    public void failTask(TaskAttemptID taskId) throws Exception {
      TaskInProgress tip = jobtracker.taskidToTIPMap.get(taskId);
      TaskStatus status = TaskStatus.createTaskStatus(tip.isMapTask(), taskId,
          1.0f, 1, TaskStatus.State.FAILED, "", "", tip
              .machineWhereTaskRan(taskId), tip.isMapTask() ? Phase.MAP
              : Phase.REDUCE, new Counters());
      updateTaskStatus(tip, status);
      addFailuresToTrackers(tip.machineWhereTaskRan(taskId));
    }

    public void addFailuresToTrackers(String trackerName) {
      Integer numOfFailures = trackerToFailureMap.get(trackerName);
      if (numOfFailures == null) {
        numOfFailures = 0;
      }
      trackerToFailureMap.put(trackerName, numOfFailures + 1);
    }

    public List<String> getBlackListedTrackers() {
      ArrayList<String> blackListedTrackers = new ArrayList<String>();
      for (Entry<String, Integer> entry : trackerToFailureMap.entrySet()) {
        Integer failures = entry.getValue();
        String tracker = entry.getKey();
        if (failures.intValue() >= this.getJobConf()
            .getMaxTaskFailuresPerTracker()) {
          blackListedTrackers.add(JobInProgress
              .convertTrackerNameToHostName(tracker));
        }
      }
      return blackListedTrackers;
    }
  }

  protected void setUp() throws Exception {
    responseId = 0;
    JobConf conf = new JobConf();
    conf.set("mapred.job.tracker", "localhost:0");
    conf.set("mapred.job.tracker.http.address", "0.0.0.0:0");
    conf.setInt("mapred.max.tracker.blacklists", 1);
    jobTracker = new FakeJobTracker(conf, (clock = new FakeJobTrackerClock()));
    sendHeartBeat(null, true);
  }

  private void sendHeartBeat(TaskTrackerHealthStatus status, boolean initialContact) throws IOException {
    for (String tracker : trackers) {
      TaskTrackerStatus tts = new TaskTrackerStatus(tracker, JobInProgress
          .convertTrackerNameToHostName(tracker));
      if (status != null) {
        TaskTrackerHealthStatus healthStatus = tts.getHealthStatus();
        healthStatus.setNodeHealthy(status.isNodeHealthy());
        healthStatus.setHealthReport(status.getHealthReport());
        healthStatus.setLastReported(status.getLastReported());
      }
      jobTracker.heartbeat(tts, false, initialContact, false, (short) responseId);
    }
    responseId++;
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
  }

  public void testTrackerBlacklistingForJobFailures() throws Exception {
    runBlackListingJob();
    assertEquals("Tracker 1 not blacklisted", jobTracker
        .getBlacklistedTrackerCount(), 1);
    checkReasonForBlackListing(hosts[0], exceedsFailuresReasonSet);
    clock.jumpADay = true;
    sendHeartBeat(null, false);
    assertEquals("Tracker 1 still blacklisted after a day", jobTracker
        .getBlacklistedTrackerCount(), 0);
    clock.jumpADay = false;
  }

  public void testNodeHealthBlackListing() throws Exception {
    TaskTrackerHealthStatus status = new TaskTrackerHealthStatus();
    status.setNodeHealthy(false);
    status.setLastReported(System.currentTimeMillis());
    status.setHealthReport("ERROR");
    sendHeartBeat(status, false);
    for (String host : hosts) {
      checkReasonForBlackListing(host, nodeUnHealthyReasonSet);
    }
    status.setNodeHealthy(true);
    status.setLastReported(System.currentTimeMillis());
    status.setHealthReport("");
    sendHeartBeat(status, false);
    assertEquals("Trackers still blacklisted after healthy report", jobTracker
        .getBlacklistedTrackerCount(), 0);
  }

  public void testBlackListingWithFailuresAndHealthStatus() throws Exception {
    runBlackListingJob();
    assertEquals("Tracker 1 not blacklisted", jobTracker
        .getBlacklistedTrackerCount(), 1);
    checkReasonForBlackListing(hosts[0], exceedsFailuresReasonSet);
    TaskTrackerHealthStatus status = new TaskTrackerHealthStatus();
    status.setNodeHealthy(false);
    status.setLastReported(System.currentTimeMillis());
    status.setHealthReport("ERROR");
    sendHeartBeat(status, false);

    assertEquals("All trackers not blacklisted", 
        jobTracker.getBlacklistedTrackerCount(), 3);
    checkReasonForBlackListing(hosts[0], unhealthyAndExceedsFailure);
    checkReasonForBlackListing(hosts[1], nodeUnHealthyReasonSet);
    checkReasonForBlackListing(hosts[2], nodeUnHealthyReasonSet);
    
    clock.jumpADay = true;
    sendHeartBeat(status, false);
    
    assertEquals("All trackers not blacklisted", 
        jobTracker.getBlacklistedTrackerCount(), 3);
    
    for (String host : hosts) {
      checkReasonForBlackListing(host, nodeUnHealthyReasonSet);
    }
    sendHeartBeat(null, false);
    
    assertEquals("All trackers not white listed", 
        jobTracker.getBlacklistedTrackerCount(), 0);
    
    clock.jumpADay = false;
  }

  private void runBlackListingJob() throws IOException, Exception {
    TaskAttemptID[] taskAttemptID = new TaskAttemptID[3];
    JobConf conf = new JobConf();
    conf.setSpeculativeExecution(false);
    conf.setNumMapTasks(0);
    conf.setNumReduceTasks(5);
    conf.set("mapred.max.reduce.failures.percent", ".70");
    conf.setMaxTaskFailuresPerTracker(1);
    FakeJobInProgress job = new FakeJobInProgress(conf, jobTracker);
    job.initTasks();

    taskAttemptID[0] = job.findReduceTask(trackers[0]);
    taskAttemptID[1] = job.findReduceTask(trackers[1]);
    taskAttemptID[2] = job.findReduceTask(trackers[2]);
    job.finishTask(taskAttemptID[1]);
    job.failTask(taskAttemptID[0]);
    taskAttemptID[0] = job.findReduceTask(trackers[0]);
    job.failTask(taskAttemptID[0]);
    taskAttemptID[0] = job.findReduceTask(trackers[1]);
    job.finishTask(taskAttemptID[2]);
    job.finishTask(taskAttemptID[0]);
    jobTracker.finalizeJob(job);
  }

  private void checkReasonForBlackListing(String host,
      Set<ReasonForBlackListing> reasonsForBlackListing) {
    Set<ReasonForBlackListing> rfbs = jobTracker.getReasonForBlackList(host);
    assertEquals("Reasons for blacklisting of " + host + " does not match",
        reasonsForBlackListing, rfbs);
  }

}
