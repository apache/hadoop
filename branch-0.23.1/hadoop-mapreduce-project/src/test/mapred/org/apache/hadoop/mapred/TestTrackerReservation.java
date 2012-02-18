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

import javax.security.auth.login.LoginException;

import org.apache.hadoop.mapred.FakeObjectUtilities.FakeJobInProgress;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class TestTrackerReservation extends TestCase {

  static String[] trackers = new String[] { "tracker_tracker1:1000",
      "tracker_tracker2:1000", "tracker_tracker3:1000" };
  private static FakeJobTracker jobTracker;

  private static class FakeJobTracker extends
      org.apache.hadoop.mapred.FakeObjectUtilities.FakeJobTracker {

    FakeJobTracker(JobConf conf, Clock clock, String[] tts) throws IOException,
        InterruptedException, LoginException {
      super(conf, clock, tts);
    }

    @Override
    synchronized void finalizeJob(JobInProgress job) {
      // Do nothing
    }
  }


  public static Test suite() {
    TestSetup setup = new TestSetup(new TestSuite(TestTrackerReservation.class)) {
      protected void setUp() throws Exception {
        JobConf conf = new JobConf();
        conf.set(JTConfig.JT_IPC_ADDRESS, "localhost:0");
        conf.set(JTConfig.JT_HTTP_ADDRESS, "0.0.0.0:0");
        jobTracker = new FakeJobTracker(conf, new Clock(), trackers);
        for (String tracker : trackers) {
          FakeObjectUtilities.establishFirstContact(jobTracker, tracker);
        }
      }

      protected void tearDown() throws Exception {
      }
    };
    return setup;
  }

  /**
   * Test case to test if task tracker reservation.
   * <ol>
   * <li>Run a cluster with 3 trackers.</li>
   * <li>Submit a job which reserves all the slots in two
   * trackers.</li>
   * <li>Run the job on another tracker which has 
   * no reservations</li>
   * <li>Finish the job and observe the reservations are
   * successfully canceled</li>
   * </ol>
   * 
   * @throws Exception
   */
  public void testTaskTrackerReservation() throws Exception {
    JobConf conf = new JobConf();
    
    conf.setNumMapTasks(1);
    conf.setNumReduceTasks(1);
    conf.setSpeculativeExecution(false);
    
    conf.setBoolean(JobContext.SETUP_CLEANUP_NEEDED, false);
    
    //Set task tracker objects for reservation.
    TaskTracker tt1 = jobTracker.getTaskTracker(trackers[0]);
    TaskTracker tt2 = jobTracker.getTaskTracker(trackers[1]);
    TaskTracker tt3 = jobTracker.getTaskTracker(trackers[2]);
    TaskTrackerStatus status1 = new TaskTrackerStatus(
        trackers[0],JobInProgress.convertTrackerNameToHostName(
            trackers[0]),0,new ArrayList<TaskStatus>(), 0, 2, 2);
    TaskTrackerStatus status2 = new TaskTrackerStatus(
        trackers[1],JobInProgress.convertTrackerNameToHostName(
            trackers[1]),0,new ArrayList<TaskStatus>(), 0, 2, 2);
    TaskTrackerStatus status3 = new TaskTrackerStatus(
        trackers[1],JobInProgress.convertTrackerNameToHostName(
            trackers[1]),0,new ArrayList<TaskStatus>(), 0, 2, 2);
    tt1.setStatus(status1);
    tt2.setStatus(status2);
    tt3.setStatus(status3);
    
    FakeJobInProgress fjob = new FakeJobInProgress(conf, jobTracker);
    fjob.setClusterSize(3);
    fjob.initTasks();
    
    tt1.reserveSlots(TaskType.MAP, fjob, 2);
    tt1.reserveSlots(TaskType.REDUCE, fjob, 2);
    tt3.reserveSlots(TaskType.MAP, fjob, 2);
    tt3.reserveSlots(TaskType.REDUCE, fjob, 2);
    
    assertEquals("Trackers not reserved for the job : maps", 
        2, fjob.getNumReservedTaskTrackersForMaps());
    assertEquals("Trackers not reserved for the job : reduces", 
        2, fjob.getNumReservedTaskTrackersForReduces());
    ClusterMetrics metrics = jobTracker.getClusterMetrics();
    assertEquals("reserved map slots do not match",
          4, metrics.getReservedMapSlots());
    assertEquals("reserved reduce slots do not match",
          4, metrics.getReservedReduceSlots());
    
    TaskAttemptID mTid = fjob.findMapTask(trackers[1]);
    TaskAttemptID rTid = fjob.findReduceTask(trackers[1]);

    fjob.finishTask(mTid);
    fjob.finishTask(rTid);
    
    assertEquals("Job didnt complete successfully complete", fjob.getStatus()
        .getRunState(), JobStatus.SUCCEEDED);
    
    assertEquals("Reservation for the job not released: Maps", 
        0, fjob.getNumReservedTaskTrackersForMaps());
    assertEquals("Reservation for the job not released : Reduces", 
        0, fjob.getNumReservedTaskTrackersForReduces());
    metrics = jobTracker.getClusterMetrics();
    assertEquals("reserved map slots do not match",
        0, metrics.getReservedMapSlots());
    assertEquals("reserved reduce slots do not match",
        0, metrics.getReservedReduceSlots());
  }
  
  /**
   * Test case to check task tracker reservation for a job which 
   * has a job blacklisted tracker.
   * <ol>
   * <li>Run a job which fails on one of the tracker.</li>
   * <li>Check if the job succeeds and has no reservation.</li>
   * </ol>
   * 
   * @throws Exception
   */
  
  public void testTrackerReservationWithJobBlackListedTracker() throws Exception {
    FakeJobInProgress job = TestTaskTrackerBlacklisting.runBlackListingJob(
        jobTracker, trackers);
    assertEquals("Job has no blacklisted trackers", 1, job
        .getBlackListedTrackers().size());
    assertTrue("Tracker 1 not blacklisted for the job", job
        .getBlackListedTrackers().contains(
            JobInProgress.convertTrackerNameToHostName(trackers[0])));
    assertEquals("Job didnt complete successfully complete", job.getStatus()
        .getRunState(), JobStatus.SUCCEEDED);
    assertEquals("Reservation for the job not released: Maps", 
        0, job.getNumReservedTaskTrackersForMaps());
    assertEquals("Reservation for the job not released : Reduces", 
        0, job.getNumReservedTaskTrackersForReduces());
    ClusterMetrics metrics = jobTracker.getClusterMetrics();
    assertEquals("reserved map slots do not match",
        0, metrics.getReservedMapSlots());
    assertEquals("reserved reduce slots do not match",
        0, metrics.getReservedReduceSlots());
  }
  
  /**
   * Test case to check if the job reservation is handled properly if the 
   * job has a reservation on a black listed tracker.
   * 
   * @throws Exception
   */
  public void testReservationOnBlacklistedTracker() throws Exception {
    TaskAttemptID[] taskAttemptID = new TaskAttemptID[3];
    JobConf conf = new JobConf();
    conf.setSpeculativeExecution(false);
    conf.setNumMapTasks(2);
    conf.setNumReduceTasks(2);
    conf.set(JobContext.REDUCE_FAILURES_MAXPERCENT, ".70");
    conf.set(JobContext.MAP_FAILURES_MAX_PERCENT, ".70");
    conf.setBoolean(JobContext.SETUP_CLEANUP_NEEDED, false);
    conf.setMaxTaskFailuresPerTracker(1);
    FakeJobInProgress job = new FakeJobInProgress(conf, jobTracker);
    job.setClusterSize(trackers.length);
    job.initTasks();
    
    TaskTracker tt1 = jobTracker.getTaskTracker(trackers[0]);
    TaskTracker tt2 = jobTracker.getTaskTracker(trackers[1]);
    TaskTracker tt3 = jobTracker.getTaskTracker(trackers[2]);
    TaskTrackerStatus status1 = new TaskTrackerStatus(
        trackers[0],JobInProgress.convertTrackerNameToHostName(
            trackers[0]),0,new ArrayList<TaskStatus>(), 0, 2, 2);
    TaskTrackerStatus status2 = new TaskTrackerStatus(
        trackers[1],JobInProgress.convertTrackerNameToHostName(
            trackers[1]),0,new ArrayList<TaskStatus>(), 0, 2, 2);
    TaskTrackerStatus status3 = new TaskTrackerStatus(
        trackers[1],JobInProgress.convertTrackerNameToHostName(
            trackers[1]),0,new ArrayList<TaskStatus>(), 0, 2, 2);
    tt1.setStatus(status1);
    tt2.setStatus(status2);
    tt3.setStatus(status3);
    
    tt1.reserveSlots(TaskType.MAP, job, 2);
    tt1.reserveSlots(TaskType.REDUCE, job, 2);
    tt3.reserveSlots(TaskType.MAP, job, 2);
    tt3.reserveSlots(TaskType.REDUCE, job, 2);
    
    assertEquals("Trackers not reserved for the job : maps", 
        2, job.getNumReservedTaskTrackersForMaps());
    assertEquals("Trackers not reserved for the job : reduces", 
        2, job.getNumReservedTaskTrackersForReduces());
    ClusterMetrics metrics = jobTracker.getClusterMetrics();
    assertEquals("reserved map slots do not match",
        4, metrics.getReservedMapSlots());
    assertEquals("reserved reduce slots do not match",
        4, metrics.getReservedReduceSlots());
  
    /*
     * FakeJobInProgress.findMapTask does not handle
     * task failures. So working around it by failing
     * reduce and blacklisting tracker.
     * Then finish the map task later. 
     */
    TaskAttemptID mTid = job.findMapTask(trackers[0]);
    TaskAttemptID rTid = job.findReduceTask(trackers[0]);
    //Task should blacklist the tasktracker.
    job.failTask(rTid);
    
    assertEquals("Tracker 0 not blacklisted for the job", 1, 
        job.getBlackListedTrackers().size());
    assertEquals("Extra Trackers reserved for the job : maps", 
        1, job.getNumReservedTaskTrackersForMaps());
    assertEquals("Extra Trackers reserved for the job : reduces", 
        1, job.getNumReservedTaskTrackersForReduces());
    metrics = jobTracker.getClusterMetrics();
    assertEquals("reserved map slots do not match",
        2, metrics.getReservedMapSlots());
    assertEquals("reserved reduce slots do not match",
        2, metrics.getReservedReduceSlots());

    //Finish the map task on the tracker 1. Finishing it here to work
    //around bug in the FakeJobInProgress object
    job.finishTask(mTid);
    mTid = job.findMapTask(trackers[1]);
    rTid = job.findReduceTask(trackers[1]);
    job.finishTask(mTid);
    job.finishTask(rTid);
    rTid = job.findReduceTask(trackers[1]);
    job.finishTask(rTid);
    assertEquals("Job didnt complete successfully complete", job.getStatus()
        .getRunState(), JobStatus.SUCCEEDED);
    assertEquals("Trackers not unreserved for the job : maps", 
        0, job.getNumReservedTaskTrackersForMaps());
    assertEquals("Trackers not unreserved for the job : reduces", 
        0, job.getNumReservedTaskTrackersForReduces());
    metrics = jobTracker.getClusterMetrics();
    assertEquals("reserved map slots do not match",
        0, metrics.getReservedMapSlots());
    assertEquals("reserved reduce slots do not match",
        0, metrics.getReservedReduceSlots());
  }
}
  