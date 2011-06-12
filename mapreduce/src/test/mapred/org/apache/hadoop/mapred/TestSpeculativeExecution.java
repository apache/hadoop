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

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.mapred.FakeObjectUtilities.FakeJobInProgress;
import org.apache.hadoop.mapred.FakeObjectUtilities.FakeJobTracker;
import org.apache.hadoop.mapred.UtilsForTests.FakeClock;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;

public class TestSpeculativeExecution extends TestCase {

  FakeJobInProgress job;
  static FakeJobTracker jobTracker;
  static class SpecFakeClock extends FakeClock {
    long SPECULATIVE_LAG = TaskInProgress.SPECULATIVE_LAG;
    public void advanceBySpeculativeLag() {
      time += SPECULATIVE_LAG;
    }
  };
  static SpecFakeClock clock;
  static final Log LOG = LogFactory.getLog(TestSpeculativeExecution.class);

  
  static String trackers[] = new String[] {"tracker_tracker1:1000", 
      "tracker_tracker2:1000", "tracker_tracker3:1000",
      "tracker_tracker4:1000", "tracker_tracker5:1000"};

  public static Test suite() {
    TestSetup setup = 
      new TestSetup(new TestSuite(TestSpeculativeExecution.class)) {
      protected void setUp() throws Exception {
        JobConf conf = new JobConf();
        conf.set(JTConfig.JT_IPC_ADDRESS, "localhost:0");
        conf.set(JTConfig.JT_HTTP_ADDRESS, "0.0.0.0:0");
        jobTracker = new FakeJobTracker(conf, (clock = new SpecFakeClock()),
            trackers);
        for (String tracker : trackers) {
          FakeObjectUtilities.establishFirstContact(jobTracker, tracker);
        }
      }
      protected void tearDown() throws Exception {
        //delete the build/test/logs/ dir
      }
    };
    return setup;
  }

  public void testRunningTaskCountWithSpeculation() throws IOException {
    TaskAttemptID[] taskAttemptID = new TaskAttemptID[8];
    JobConf conf = new JobConf();
    conf.setSpeculativeExecution(true);
    conf.setNumMapTasks(3);
    conf.setNumReduceTasks(3);
    conf.setFloat(JobContext.SPECULATIVE_SLOWTASK_THRESHOLD, 0.5f);
    FakeJobInProgress job = new FakeJobInProgress(conf, jobTracker);
    job.initTasks();

    //Check for runningMap counts first
    //schedule maps
    taskAttemptID[0] = job.findMapTask(trackers[0]);
    taskAttemptID[1] = job.findMapTask(trackers[1]);
    taskAttemptID[2] = job.findMapTask(trackers[2]);

    clock.advance(5000);
    job.finishTask(taskAttemptID[0]);
    clock.advance(1000);
    job.finishTask(taskAttemptID[1]);
    clock.advanceBySpeculativeLag();

    //we should get a speculative task now
    taskAttemptID[3] = job.findMapTask(trackers[3]);
    int oldRunningMap = job.runningMaps();
    LOG.info("No of running maps before fail was " + oldRunningMap);
    job.failTask(taskAttemptID[2]);
    assertEquals(
      "Running maps count should be updated from " + oldRunningMap + " to " +
        (oldRunningMap - 1), job.runningMaps(), oldRunningMap - 1);
    LOG.info(" Job running maps after fail " + job.runningMaps());

    clock.advance(5000);
    job.finishTask(taskAttemptID[3]);

    //check for runningReduce count.
    taskAttemptID[4] = job.findReduceTask(trackers[0]);
    taskAttemptID[5] = job.findReduceTask(trackers[1]);
    taskAttemptID[6] = job.findReduceTask(trackers[2]);

    clock.advance(5000);
    job.finishTask(taskAttemptID[4]);
    clock.advance(1000);
    job.finishTask(taskAttemptID[5]);

    clock.advanceBySpeculativeLag();
    taskAttemptID[7] = job.findReduceTask(trackers[4]);

    int oldRunningReduces = job.runningReduces();
    job.failTask(taskAttemptID[6]);
    LOG.info(
      " No of running Reduces before fail " + oldRunningReduces);
    LOG.info(
      " No of runing reduces after fail " + job.runningReduces());
    assertEquals(
      "Running reduces count should be updated from " + oldRunningReduces +
        " to " + (oldRunningReduces - 1), job.runningReduces(),
      oldRunningReduces - 1);
    
    job.finishTask(taskAttemptID[7]);
  }

  public void testIsSlowTracker() throws IOException {
    TaskAttemptID[] taskAttemptID = new TaskAttemptID[20];
    JobConf conf = new JobConf();
    conf.setSpeculativeExecution(true);
    conf.setNumMapTasks(10);
    conf.setNumReduceTasks(0);
    FakeJobInProgress job = new FakeJobInProgress(conf, jobTracker);    
    job.initTasks();
    //schedule some tasks
    taskAttemptID[0] = job.findMapTask(trackers[0]);
    taskAttemptID[1] = job.findMapTask(trackers[0]);
    taskAttemptID[2] = job.findMapTask(trackers[0]);
    taskAttemptID[3] = job.findMapTask(trackers[1]);
    taskAttemptID[4] = job.findMapTask(trackers[1]);
    taskAttemptID[5] = job.findMapTask(trackers[1]);
    taskAttemptID[6] = job.findMapTask(trackers[2]);
    taskAttemptID[7] = job.findMapTask(trackers[2]);
    taskAttemptID[8] = job.findMapTask(trackers[2]);
    clock.advance(1000);
    //Some tasks finish in 1 second (on trackers[0])
    job.finishTask(taskAttemptID[0]);
    job.finishTask(taskAttemptID[1]);
    job.finishTask(taskAttemptID[2]);
    clock.advance(1000);
    //Some tasks finish in 2 second (on trackers[1])
    job.finishTask(taskAttemptID[3]);
    job.finishTask(taskAttemptID[4]);
    job.finishTask(taskAttemptID[5]);
    assertEquals("Tracker "+ trackers[0] + " expected to be not slow ",
        job.isSlowTracker(trackers[0]), false);
    clock.advance(100000);
    //After a long time, some tasks finished on trackers[2]
    job.finishTask(taskAttemptID[6]);
    job.finishTask(taskAttemptID[7]);
    job.finishTask(taskAttemptID[8]);
    assertEquals("Tracker "+ trackers[2] + " expected to be slow ",
        job.isSlowTracker(trackers[2]), true);
  }
  
  public void testTaskToSpeculate() throws IOException {
    TaskAttemptID[] taskAttemptID = new TaskAttemptID[6];
    JobConf conf = new JobConf();
    conf.setSpeculativeExecution(true);
    conf.setNumMapTasks(5);
    conf.setNumReduceTasks(5);
    conf.setFloat(JobContext.SPECULATIVE_SLOWTASK_THRESHOLD, 0.5f);
    FakeJobInProgress job = new FakeJobInProgress(conf, jobTracker);    
    job.initTasks();
    //schedule maps
    taskAttemptID[0] = job.findReduceTask(trackers[0]);
    taskAttemptID[1] = job.findReduceTask(trackers[1]);
    taskAttemptID[2] = job.findReduceTask(trackers[2]);
    taskAttemptID[3] = job.findReduceTask(trackers[3]);
    taskAttemptID[4] = job.findReduceTask(trackers[3]);
    clock.advance(5000);
    job.finishTask(taskAttemptID[0]);
    clock.advance(1000);
    job.finishTask(taskAttemptID[1]);
    clock.advance(20000);
    clock.advanceBySpeculativeLag();
    //we should get a speculative task now
    taskAttemptID[5] = job.findReduceTask(trackers[4]);
    assertEquals(taskAttemptID[5].getTaskID().getId(),2);
    clock.advance(5000);
    job.finishTask(taskAttemptID[5]);
    
    taskAttemptID[5] = job.findReduceTask(trackers[4]);
    assertEquals(taskAttemptID[5].getTaskID().getId(),3);
    
  }
  
  /*
   * Tests the fact that we choose tasks with lesser progress
   * among the possible candidates for speculation
   */
  public void testTaskLATEScheduling() throws IOException {
    TaskAttemptID[] taskAttemptID = new TaskAttemptID[20];
    JobConf conf = new JobConf();
    conf.setSpeculativeExecution(true);
    conf.setNumMapTasks(5);
    conf.setNumReduceTasks(0);
    conf.setFloat(JobContext.SPECULATIVE_SLOWTASK_THRESHOLD, 0.5f);
    FakeJobInProgress job = new FakeJobInProgress(conf, jobTracker);
    job.initTasks();

    taskAttemptID[0] = job.findMapTask(trackers[0]);
    taskAttemptID[1] = job.findMapTask(trackers[1]);
    taskAttemptID[2] = job.findMapTask(trackers[2]);
    taskAttemptID[3] = job.findMapTask(trackers[3]);
    clock.advance(2000);
    job.finishTask(taskAttemptID[0]);
    job.finishTask(taskAttemptID[1]);
    job.finishTask(taskAttemptID[2]);
    clock.advance(250000);
    taskAttemptID[4] = job.findMapTask(trackers[3]);
    clock.advanceBySpeculativeLag();
    //by doing the above clock adjustments, we bring the progress rate of 
    //taskID 3 lower than 4. For taskID 3, the rate is 85/317000
    //and for taskID 4, the rate is 20/65000. But when we ask for a spec task
    //now, we should get back taskID 4 (since that is expected to complete
    //later than taskID 3).
    job.progressMade(taskAttemptID[3], 0.85f);
    job.progressMade(taskAttemptID[4], 0.20f);
    taskAttemptID[5] = job.findMapTask(trackers[4]);
    assertEquals(taskAttemptID[5].getTaskID().getId(),4);
  }

  /*
   * Tests the fact that we only launch a limited number of speculative tasks,
   * even though we have a lot of tasks in RUNNING state
   */
  public void testAtSpeculativeCap() throws IOException {
    //The expr which is evaluated for determining whether 
    //atSpeculativeCap should
    //return true or false is
    //(#speculative-tasks < max (10, 0.01*#slots, 0.1*#running-tasks)
    
    //Tests the fact that the max tasks launched is 0.1 * #running-tasks
    assertEquals(speculativeCap(1200,800,20), 40);
    //Tests the fact that the max tasks launched is 10
    assertEquals(speculativeCap(1200,1150,20), 10);
    //Tests the fact that the max tasks launched is 0.01 * #slots
    assertEquals(speculativeCap(1200,1150,4000), 20);
  }
  
  private int speculativeCap(int totalTasks, int numEarlyComplete, int slots)
  throws IOException {
    TaskAttemptID[] taskAttemptID = new TaskAttemptID[1500];
    JobConf conf = new JobConf();
    conf.setSpeculativeExecution(true);
    conf.setNumMapTasks(totalTasks);
    conf.setNumReduceTasks(0);
    jobTracker.setNumSlots(slots);
    FakeJobInProgress job = new FakeJobInProgress(conf, jobTracker);
    job.initTasks();
    int i;
    for (i = 0; i < totalTasks; i++) {
      taskAttemptID[i] = job.findMapTask(trackers[0]);
    }
    clock.advance(5000);
    for (i = 0; i < numEarlyComplete; i++) {
      job.finishTask(taskAttemptID[i]);
    }

    clock.advanceBySpeculativeLag();

    for (i = numEarlyComplete; i < totalTasks; i++) {
      job.progressMade(taskAttemptID[i], 0.85f);
    }
    clock.advance(50000);
    for (i = 0; i < (totalTasks - numEarlyComplete); i++) {
      taskAttemptID[i] = job.findMapTask(trackers[1]);
      clock.advance(2000);
      if (taskAttemptID[i] != null) {
        //add some good progress constantly for the different
        //task-attempts so that
        //the tasktracker doesn't get into the slow trackers category
        job.progressMade(taskAttemptID[i], 0.99f);
      } else {
        break;
      }
    }
    return i;
  }
}
