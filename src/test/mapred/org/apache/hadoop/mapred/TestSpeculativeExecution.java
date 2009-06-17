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

import org.apache.hadoop.mapred.TaskStatus.Phase;

import java.util.Collection;
import java.util.Iterator;

import org.apache.hadoop.mapred.UtilsForTests.FakeClock;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class TestSpeculativeExecution extends TestCase {

  FakeJobInProgress job;
  static FakeJobTracker jobTracker;
  static String jtIdentifier = "test";
  private static int jobCounter;
  static class SpecFakeClock extends FakeClock {
    long SPECULATIVE_LAG = TaskInProgress.SPECULATIVE_LAG;
    @Override
    public void advance(long millis) {
      time += millis + SPECULATIVE_LAG;
    }
  };
  static SpecFakeClock clock;
  
  static String trackers[] = new String[] {"tracker_tracker1:1000", 
      "tracker_tracker2:1000", "tracker_tracker3:1000",
      "tracker_tracker4:1000", "tracker_tracker5:1000"};

  public static Test suite() {
    TestSetup setup = 
      new TestSetup(new TestSuite(TestSpeculativeExecution.class)) {
      protected void setUp() throws Exception {
        JobConf conf = new JobConf();
        conf.set("mapred.job.tracker", "localhost:0");
        conf.set("mapred.job.tracker.http.address", "0.0.0.0:0");
        jobTracker = new FakeJobTracker(conf, (clock = new SpecFakeClock()));
        for (String tracker : trackers) {
          jobTracker.heartbeat(new TaskTrackerStatus(tracker,
              JobInProgress.convertTrackerNameToHostName(tracker)), false, 
              true, false, (short)0);
        }
      }
      protected void tearDown() throws Exception {
        //delete the build/test/logs/ dir
      }
    };
    return setup;
  }
  
  /*
   * This class is required mainly to check the speculative cap
   * based on cluster size
   */
  static class FakeJobTracker extends JobTracker {
    //initialize max{Map/Reduce} task capacities to twice the clustersize
    int totalSlots = trackers.length * 4;
    FakeJobTracker(JobConf conf, Clock clock) throws IOException, 
    InterruptedException {
      super(conf, clock);
    }
    @Override
    public ClusterStatus getClusterStatus(boolean detailed) {
      return new ClusterStatus(trackers.length,
          0, 0, 0, 0, totalSlots/2, totalSlots/2, JobTracker.State.RUNNING, 0);
    }
    public void setNumSlots(int totalSlots) {
      this.totalSlots = totalSlots;
    }
  }

  static class FakeJobInProgress extends JobInProgress {
    JobClient.RawSplit[] rawSplits;
    FakeJobInProgress(JobConf jobConf, JobTracker tracker) throws IOException {
      super(new JobID(jtIdentifier, ++jobCounter), jobConf, tracker);
      //initObjects(tracker, numMaps, numReduces);
    }
    @Override
    public synchronized void initTasks() throws IOException {
      maps = new TaskInProgress[numMapTasks];
      for (int i = 0; i < numMapTasks; i++) {
        JobClient.RawSplit split = new JobClient.RawSplit();
        split.setLocations(new String[0]);
        maps[i] = new TaskInProgress(getJobID(), "test", 
            split, jobtracker, getJobConf(), this, i);
        nonLocalMaps.add(maps[i]);
      }
      reduces = new TaskInProgress[numReduceTasks];
      for (int i = 0; i < numReduceTasks; i++) {
        reduces[i] = new TaskInProgress(getJobID(), "test", 
                                        numMapTasks, i, 
                                        jobtracker, getJobConf(), this);
        nonRunningReduces.add(reduces[i]);
      }
    }
    private TaskAttemptID findTask(String trackerName, String trackerHost,
        Collection<TaskInProgress> nonRunningTasks, 
        Collection<TaskInProgress> runningTasks)
    throws IOException {
      TaskInProgress tip = null;
      Iterator<TaskInProgress> iter = nonRunningTasks.iterator();
      //look for a non-running task first
      while (iter.hasNext()) {
        TaskInProgress t = iter.next();
        if (t.isRunnable() && !t.isRunning()) {
          runningTasks.add(t);
          iter.remove();
          tip = t;
          break;
        }
      }
      if (tip == null) {
        if (getJobConf().getSpeculativeExecution()) {
          tip = findSpeculativeTask(runningTasks, trackerName, trackerHost);
        }
      }
      if (tip != null) {
        TaskAttemptID tId = tip.getTaskToRun(trackerName).getTaskID();
        if (tip.isMapTask()) {
          scheduleMap(tip);
        } else {
          scheduleReduce(tip);
        }
        //Set it to RUNNING
        makeRunning(tId, tip, trackerName);
        return tId;
      }
      return null;
    }
    public TaskAttemptID findMapTask(String trackerName)
    throws IOException {
      return findTask(trackerName, 
          JobInProgress.convertTrackerNameToHostName(trackerName),
          nonLocalMaps, nonLocalRunningMaps);
    }
    public TaskAttemptID findReduceTask(String trackerName) 
    throws IOException {
      return findTask(trackerName, 
          JobInProgress.convertTrackerNameToHostName(trackerName),
          nonRunningReduces, runningReduces);
    }
    public void finishTask(TaskAttemptID taskId) {
      TaskInProgress tip = jobtracker.taskidToTIPMap.get(taskId);
      TaskStatus status = TaskStatus.createTaskStatus(tip.isMapTask(), taskId, 
          1.0f, TaskStatus.State.SUCCEEDED, "", "", tip.machineWhereTaskRan(taskId), 
          tip.isMapTask() ? Phase.MAP : Phase.REDUCE, new Counters());
      updateTaskStatus(tip, status);
    }
    private void makeRunning(TaskAttemptID taskId, TaskInProgress tip, 
        String taskTracker) {
      addRunningTaskToTIP(tip, taskId, new TaskTrackerStatus(taskTracker,
          JobInProgress.convertTrackerNameToHostName(taskTracker)), true);
      TaskStatus status = TaskStatus.createTaskStatus(tip.isMapTask(), taskId, 
          0.0f, TaskStatus.State.RUNNING, "", "", taskTracker,
          tip.isMapTask() ? Phase.MAP : Phase.REDUCE, new Counters());
      updateTaskStatus(tip, status);
    }
    public void progressMade(TaskAttemptID taskId, float progress) {
      TaskInProgress tip = jobtracker.taskidToTIPMap.get(taskId);
      TaskStatus status = TaskStatus.createTaskStatus(tip.isMapTask(), taskId, 
          progress, TaskStatus.State.RUNNING, "", "", tip.machineWhereTaskRan(taskId), 
          tip.isMapTask() ? Phase.MAP : Phase.REDUCE, new Counters());
      updateTaskStatus(tip, status);
    }
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
    conf.setFloat("mapred.speculative.execution.slowTaskThreshold", 0.5f);
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
    conf.setFloat("mapred.speculative.execution.slowTaskThreshold", 0.5f);
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
    clock.advance(28000);
    taskAttemptID[4] = job.findMapTask(trackers[3]);
    clock.advance(5000);
    //by doing the above clock adjustments, we bring the progress rate of 
    //taskID 3 lower than 4. For taskID 3, the rate is 85/35000
    //and for taskID 4, the rate is 20/5000. But when we ask for a spec task
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
    //The expr which is evaluated for determining whether atSpeculativeCap should
    //return true or false is
    //(#speculative-tasks < max (10, 0.01*#slots, 0.1*#running-tasks)
    
    //Tests the fact that the max tasks launched is 0.1 * #running-tasks
    assertEquals(speculativeCap(1200,800,20), 40);
    //Tests the fact that the max tasks launched is 10
    assertEquals(speculativeCap(1200,1150,20), 10);
    //Tests the fact that the max tasks launched is 0.01 * #slots
    assertEquals(speculativeCap(1200,1150,2000), 20);
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
    for (i = numEarlyComplete; i < totalTasks; i++) {
      job.progressMade(taskAttemptID[i], 0.85f);
    }
    clock.advance(50000);
    for (i = 0; i < (totalTasks - numEarlyComplete); i++) {
      taskAttemptID[i] = job.findMapTask(trackers[1]);
      clock.advance(2000);
      if (taskAttemptID[i] != null) {
        //add some good progress constantly for the different task-attempts so that
        //the tasktracker doesn't get into the slow trackers category
        job.progressMade(taskAttemptID[i], 0.99f);
      } else {
        break;
      }
    }
    return i;
  }
}