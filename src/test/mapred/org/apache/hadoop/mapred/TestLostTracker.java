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
}