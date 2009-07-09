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

import org.apache.hadoop.mapreduce.TaskType;
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
        InterruptedException {
      super(conf, clock, tts);
    }

    @Override
    synchronized void finalizeJob(JobInProgress job) {
      // Do nothing
    }
  }

  private static class FakeJobInProgress extends
      org.apache.hadoop.mapred.FakeObjectUtilities.FakeJobInProgress {
    TaskInProgress cleanup[] = new TaskInProgress[0];
    TaskInProgress setup[] = new TaskInProgress[0];

    FakeJobInProgress(JobConf jobConf, JobTracker tracker) throws IOException {
      super(jobConf, tracker);
    }

    @Override
    public synchronized void initTasks() throws IOException {
      super.initTasks();
      tasksInited.set(true);
    }

    @Override
    public void cleanUpMetrics() {
    }
  }

  public static Test suite() {
    TestSetup setup = new TestSetup(new TestSuite(TestTrackerReservation.class)) {
      protected void setUp() throws Exception {
        JobConf conf = new JobConf();
        conf.set("mapred.job.tracker", "localhost:0");
        conf.set("mapred.job.tracker.http.address", "0.0.0.0:0");
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
    
    conf.setBoolean(
        "mapred.committer.job.setup.cleanup.needed", false);
    
    //Set task tracker objects for reservation.
    TaskTracker tt1 = new TaskTracker(trackers[0]);
    TaskTracker tt2 = new TaskTracker(trackers[1]);
    TaskTracker tt3 = new TaskTracker(trackers[2]);
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
    fjob.initTasks();
    
    tt1.reserveSlots(TaskType.MAP, fjob, 2);
    tt1.reserveSlots(TaskType.REDUCE, fjob, 2);
    tt3.reserveSlots(TaskType.MAP, fjob, 2);
    tt3.reserveSlots(TaskType.REDUCE, fjob, 2);
    
    assertEquals("Trackers not reserved for the job : maps", 
        2, fjob.getNumReservedTaskTrackersForMaps());
    assertEquals("Trackers not reserved for the job : reduces", 
        2, fjob.getNumReservedTaskTrackersForReduces());
    
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
  }
}
