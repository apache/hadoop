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

import junit.framework.TestCase;

import org.apache.hadoop.mapred.FakeObjectUtilities.FakeJobInProgress;
import org.apache.hadoop.mapred.FakeObjectUtilities.FakeJobTracker;
import org.apache.hadoop.mapred.UtilsForTests.FakeClock;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;

/**
 * A test to verify JobTracker's resilience to lost task trackers. 
 * 
 */
@SuppressWarnings("deprecation")
public class TestLostTracker extends TestCase {

  FakeJobInProgress job;
  static FakeJobTracker jobTracker;
 
  static FakeClock clock;
  
  static String trackers[] = new String[] {"tracker_tracker1:1000",
      "tracker_tracker2:1000"};

  @Override
  protected void setUp() throws Exception {
    JobConf conf = new JobConf();
    conf.set(JTConfig.JT_IPC_ADDRESS, "localhost:0");
    conf.set(JTConfig.JT_HTTP_ADDRESS, "0.0.0.0:0");
    conf.setLong(JTConfig.JT_TRACKER_EXPIRY_INTERVAL, 1000);
    conf.set(JTConfig.JT_MAX_TRACKER_BLACKLISTS, "1");
    jobTracker = new FakeJobTracker(conf, (clock = new FakeClock()), trackers);
    jobTracker.startExpireTrackersThread();
  }
  
  @Override
  protected void tearDown() throws Exception {
    jobTracker.stopExpireTrackersThread();
  }

  public void testLostTracker() throws IOException {
    // Tracker 0 contacts JT
    FakeObjectUtilities.establishFirstContact(jobTracker, trackers[0]);

    TaskAttemptID[] tid = new TaskAttemptID[2];
    JobConf conf = new JobConf();
    conf.setNumMapTasks(1);
    conf.setNumReduceTasks(1);
    FakeJobInProgress job = new FakeJobInProgress(conf, jobTracker);
    job.initTasks();
    
    // Tracker 0 gets the map task
    tid[0] = job.findMapTask(trackers[0]);

    job.finishTask(tid[0]);

    // Advance clock. Tracker 0 would have got lost
    clock.advance(8 * 1000);

    jobTracker.checkExpiredTrackers();
    
    // Tracker 1 establishes contact with JT 
    FakeObjectUtilities.establishFirstContact(jobTracker, trackers[1]);
    
    // Tracker1 should get assigned the lost map task
    tid[1] =  job.findMapTask(trackers[1]);

    assertNotNull("Map Task from Lost Tracker did not get reassigned", tid[1]);
    
    assertEquals("Task ID of reassigned map task does not match",
        tid[0].getTaskID().toString(), tid[1].getTaskID().toString());
    
    job.finishTask(tid[1]);
    
  }
  
  /**
   * Test whether the tracker gets blacklisted after its lost.
   */
  public void testLostTrackerBeforeBlacklisting() throws Exception {
    FakeObjectUtilities.establishFirstContact(jobTracker, trackers[0]);
    TaskAttemptID[] tid = new TaskAttemptID[3];
    JobConf conf = new JobConf();
    conf.setNumMapTasks(1);
    conf.setNumReduceTasks(1);
    conf.set(MRJobConfig.MAX_TASK_FAILURES_PER_TRACKER, "1");
    conf.set(MRJobConfig.SETUP_CLEANUP_NEEDED, "false");
    FakeJobInProgress job = new FakeJobInProgress(conf, jobTracker);
    job.initTasks();
    job.setClusterSize(4);
    
    // Tracker 0 gets the map task
    tid[0] = job.findMapTask(trackers[0]);

    job.finishTask(tid[0]);

    // validate the total tracker count
    assertEquals("Active tracker count mismatch", 
                 1, jobTracker.getClusterStatus(false).getTaskTrackers());
    
    // lose the tracker
    clock.advance(1100);
    jobTracker.checkExpiredTrackers();
    assertFalse("Tracker 0 not lost", 
        jobTracker.getClusterStatus(false).getActiveTrackerNames()
                  .contains(trackers[0]));
    
    // validate the total tracker count
    assertEquals("Active tracker count mismatch", 
                 0, jobTracker.getClusterStatus(false).getTaskTrackers());
    
    // Tracker 1 establishes contact with JT 
    FakeObjectUtilities.establishFirstContact(jobTracker, trackers[1]);
    
    // Tracker1 should get assigned the lost map task
    tid[1] =  job.findMapTask(trackers[1]);

    assertNotNull("Map Task from Lost Tracker did not get reassigned", tid[1]);
    
    assertEquals("Task ID of reassigned map task does not match",
        tid[0].getTaskID().toString(), tid[1].getTaskID().toString());
    
    // finish the map task
    job.finishTask(tid[1]);

    // finish the reduce task
    tid[2] =  job.findReduceTask(trackers[1]);
    job.finishTask(tid[2]);
    
    // check if job is successful
    assertEquals("Job not successful", 
                 JobStatus.SUCCEEDED, job.getStatus().getRunState());
    
    // check if the tracker is lost
    // validate the total tracker count
    assertEquals("Active tracker count mismatch", 
                 1, jobTracker.getClusterStatus(false).getTaskTrackers());
    // validate blacklisted count .. since we lost one blacklisted tracker
    assertEquals("Blacklisted tracker count mismatch", 
                0, jobTracker.getClusterStatus(false).getBlacklistedTrackers());
  }

  /**
   * Test whether the tracker gets lost after its blacklisted.
   */
  public void testLostTrackerAfterBlacklisting() throws Exception {
    FakeObjectUtilities.establishFirstContact(jobTracker, trackers[0]);
    clock.advance(600);
    TaskAttemptID[] tid = new TaskAttemptID[2];
    JobConf conf = new JobConf();
    conf.setNumMapTasks(1);
    conf.setNumReduceTasks(0);
    conf.set(MRJobConfig.MAX_TASK_FAILURES_PER_TRACKER, "1");
    conf.set(MRJobConfig.SETUP_CLEANUP_NEEDED, "false");
    FakeJobInProgress job = new FakeJobInProgress(conf, jobTracker);
    job.initTasks();
    job.setClusterSize(4);
    
    // check if the tracker count is correct
    assertEquals("Active tracker count mismatch", 
                 1, jobTracker.taskTrackers().size());
    
    // Tracker 0 gets the map task
    tid[0] = job.findMapTask(trackers[0]);
    // Fail the task
    job.failTask(tid[0]);
    
    // Tracker 1 establishes contact with JT
    FakeObjectUtilities.establishFirstContact(jobTracker, trackers[1]);
    // check if the tracker count is correct
    assertEquals("Active tracker count mismatch", 
                 2, jobTracker.taskTrackers().size());
    
    // Tracker 1 gets the map task
    tid[1] = job.findMapTask(trackers[1]);
    // Finish the task and also the job
    job.finishTask(tid[1]);

    // check if job is successful
    assertEquals("Job not successful", 
                 JobStatus.SUCCEEDED, job.getStatus().getRunState());
    
    // check if the trackers 1 got blacklisted
    assertTrue("Tracker 0 not blacklisted", 
               jobTracker.getBlacklistedTrackers()[0].getTaskTrackerName()
                 .equals(trackers[0]));
    // check if the tracker count is correct
    assertEquals("Active tracker count mismatch", 
                 2, jobTracker.taskTrackers().size());
    // validate blacklisted count
    assertEquals("Blacklisted tracker count mismatch", 
                1, jobTracker.getClusterStatus(false).getBlacklistedTrackers());
    
    // Advance clock. Tracker 0 should be lost
    clock.advance(500);
    jobTracker.checkExpiredTrackers();
    
    // check if the task tracker is lost
    assertFalse("Tracker 0 not lost", 
            jobTracker.getClusterStatus(false).getActiveTrackerNames()
                      .contains(trackers[0]));
    
    // check if the lost tracker has removed from the jobtracker
    assertEquals("Active tracker count mismatch", 
                 1, jobTracker.taskTrackers().size());
    // validate blacklisted count
    assertEquals("Blacklisted tracker count mismatch", 
                0, jobTracker.getClusterStatus(false).getBlacklistedTrackers());
    
  }
}