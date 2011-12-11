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

import org.apache.hadoop.mapred.FakeObjectUtilities.FakeJobInProgress;
import org.apache.hadoop.mapred.FakeObjectUtilities.FakeTaskInProgress;
import org.apache.hadoop.mapred.FakeObjectUtilities.FakeJobTracker;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.mapreduce.split.JobSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import junit.framework.TestCase;

public class TestSetupTaskScheduling extends TestCase {

  public static final Log LOG = 
    LogFactory.getLog(TestSetupTaskScheduling.class);

  static String[] trackers = new String[] { "tracker_tracker1:1000",
    "tracker_tracker2:1000", "tracker_tracker3:1000" };
  private static FakeJobTracker jobTracker;

  /**
   * Fake JobInProgress that can return a hardcoded setup or
   * cleanup task depending on the slot type passed in.
   */
  static class FakeJobWithSetupTask 
    extends FakeObjectUtilities.FakeJobInProgress {
    
    FakeJobWithSetupTask(JobConf jobConf, 
                          JobTracker tracker) throws IOException {
      super(jobConf, tracker);
    }

    /**
     * Initialize tasks, including setup. 
     */
    @Override
    public synchronized void initTasks() throws IOException {
      super.initTasks();
      JobSplit.TaskSplitMetaInfo emptySplit = new JobSplit.TaskSplitMetaInfo();
      setup = new TaskInProgress[2];
      setup[0] = new TaskInProgress(getJobID(), "test",  emptySplit,
          jobtracker, getJobConf(), this, numMapTasks + 1, 1);
      setup[1] = new TaskInProgress(getJobID(), "test", numMapTasks,
          numReduceTasks + 1, jobtracker, getJobConf(), this, 1);
    }
    
    /**
     * Obtain a setup task on a map slot or reduce slot
     * depending on what is free.
     * 
     * Every call to this will return either a map or reduce
     * setup task. No check is done to see if the task is already
     * returned
     */
    @Override
    public Task obtainJobSetupTask(TaskTrackerStatus tts, 
                                             int clusterSize, 
                                             int numUniqueHosts,
                                             boolean isMapSlot) 
                                  throws IOException{
      TaskInProgress tip = null;
      if (isMapSlot) {
        tip = setup[0];
      } else {
        tip = setup[1];
      }
      Task t = tip.getTaskToRun(tts.getHost());
      t.setJobSetupTask();
      return t;
    }
  }

  static class FakeJobWithTaskCleanupTask 
  extends FakeObjectUtilities.FakeJobInProgress {

    FakeJobWithTaskCleanupTask(JobConf jobConf, 
        JobTracker tracker) throws IOException {
      super(jobConf, tracker);
    }

    /**
     * Initialize tasks(1 map and 1 reduce task each needs 2 slots, similar to
     * tasks of a high RAM job). 
     */
    @Override
    public synchronized void initTasks() throws IOException {
      super.initTasks();

      final int numSlotsPerTask = 2;
      maps = new TaskInProgress[1];
      reduces = new TaskInProgress[1];
      
      maps[0] = new FakeTaskInProgress(getJobID(), "test",  
          JobSplit.EMPTY_TASK_SPLIT,
          jobtracker, getJobConf(), this, 0, numSlotsPerTask);
      TaskAttemptID attemptId = new TaskAttemptID(maps[0].getTIPId(), 0);
      
      // make this task a taskCleanup task of a map task
      mapCleanupTasks.add(attemptId);
      TaskStatus stat = new MapTaskStatus(attemptId, 0.01f, 2,
          TaskStatus.State.FAILED_UNCLEAN, "", "", trackers[0],
          TaskStatus.Phase.MAP, new Counters());
      maps[0].updateStatus(stat);
      
      //similarly for reduce task's taskCleanup task
      reduces[0] = new FakeTaskInProgress(getJobID(), "test", 1,
          0, jobtracker, getJobConf(), this, numSlotsPerTask);
      attemptId = new TaskAttemptID(reduces[0].getTIPId(), 0);
      
      // make this task a taskCleanup task of a reduce task
      reduceCleanupTasks.add(attemptId);
      stat = new ReduceTaskStatus(attemptId, 0.01f, 2,
          TaskStatus.State.FAILED_UNCLEAN, "", "", trackers[0],
          TaskStatus.Phase.REDUCE, new Counters());
      reduces[0].updateStatus(stat);
    }
  }

  public void setUp() throws Exception {
    JobConf conf = new JobConf();
    conf.set(JTConfig.JT_IPC_ADDRESS, "localhost:0");
    conf.set(JTConfig.JT_HTTP_ADDRESS, "0.0.0.0:0");
    jobTracker = new FakeJobTracker(conf, new Clock(), trackers);
    for (String tracker : trackers) {
      FakeObjectUtilities.establishFirstContact(jobTracker, tracker);
    }
  }

  // create a job for testing setup tasks and reservations
  FakeJobInProgress createJob(TaskType taskType) throws IOException {
    JobConf conf = new JobConf();
    conf.setSpeculativeExecution(false);
    conf.setNumMapTasks(2);
    conf.setNumReduceTasks(2);
    conf.set(JobContext.REDUCE_FAILURES_MAXPERCENT, ".70");
    conf.set(JobContext.MAP_FAILURES_MAX_PERCENT, ".70");
    FakeJobInProgress job = null;
    if (taskType == null) {
      conf.setBoolean(JobContext.SETUP_CLEANUP_NEEDED, false);
      job = new FakeJobInProgress(conf, jobTracker);
    } else if (taskType == TaskType.JOB_SETUP) {
      job = new FakeJobWithSetupTask(conf, jobTracker);
    } else if (taskType == TaskType.TASK_CLEANUP) {
      job = new FakeJobWithTaskCleanupTask(conf, jobTracker);
    }
    job.setClusterSize(trackers.length);
    job.initTasks();
    return job;
  }
  
  // create a new TaskStatus and add to a list of status objects.
  // useMapSlot param is needed only when taskType is TASK_CLEANUP.
  void addNewTaskStatus(FakeJobInProgress job, TaskType taskType,
        boolean useMapSlot, String tracker, List<TaskStatus> reports) 
        throws IOException {
    TaskAttemptID task = null;
    TaskStatus status = null;
    if (taskType == TaskType.MAP) {
      task = job.findMapTask(tracker);
      status = new MapTaskStatus(task, 0.01f, 2,
            TaskStatus.State.RUNNING, "", "", tracker,
            TaskStatus.Phase.MAP, new Counters());
    } else if (taskType == TaskType.TASK_CLEANUP) {
      if (useMapSlot) {
        status = job.maps[0].taskStatuses.get(
          new TaskAttemptID(job.maps[0].getTIPId(), 0));
      } else {
        status = job.reduces[0].taskStatuses.get(
              new TaskAttemptID(job.reduces[0].getTIPId(), 0));
      }
    } else {
      task = job.findReduceTask(tracker);
      status = new ReduceTaskStatus(task, 0.01f, 2,
            TaskStatus.State.RUNNING, "", "", tracker,
            TaskStatus.Phase.REDUCE, new Counters());
    }
    reports.add(status);
  }
  
  // create a TaskTrackerStatus
  TaskTrackerStatus createTaskTrackerStatus(String tracker, 
      List<TaskStatus> reports) {
    TaskTrackerStatus ttStatus =
      new TaskTrackerStatus(tracker, 
          JobInProgress.convertTrackerNameToHostName(tracker),
          0, reports, 0, 2, 2);
    return ttStatus;
  }

  /**
   * Test that a setup task can be run against a map slot
   * if it is free.
   * @throws IOException
   */
  public void testSetupTaskReturnedForFreeMapSlots() throws IOException {
    // create a job with a setup task.
    FakeJobInProgress job = createJob(TaskType.JOB_SETUP);
    jobTracker.jobs.put(job.getJobID(), job);
    
    // create a status simulating a free tasktracker
    List<TaskStatus> reports = new ArrayList<TaskStatus>();
    TaskTrackerStatus ttStatus 
      = createTaskTrackerStatus(trackers[2], reports);
    
    // verify that a setup task can be assigned to a map slot.
    List<Task> tasks = jobTracker.getSetupAndCleanupTasks(ttStatus);
    assertEquals(1, tasks.size());
    assertTrue(tasks.get(0).isJobSetupTask());
    assertTrue(tasks.get(0).isMapTask());
    jobTracker.jobs.clear();
  }

  /**
   * Test to check that map slots are counted when returning
   * a setup task.
   * @throws IOException
   */
  public void testMapSlotsCountedForSetup() throws IOException {
    // create a job with a setup task.
    FakeJobInProgress job = createJob(TaskType.JOB_SETUP);
    jobTracker.jobs.put(job.getJobID(), job);
    
    // create another job for reservation
    FakeJobInProgress job1 = createJob(null);
    jobTracker.jobs.put(job1.getJobID(), job1);
   
    // create TT status for testing getSetupAndCleanupTasks
    List<TaskStatus> taskStatuses = new ArrayList<TaskStatus>();
    addNewTaskStatus(job, TaskType.MAP, true, trackers[0], taskStatuses);
    TaskTrackerStatus ttStatus 
      = createTaskTrackerStatus(trackers[0], taskStatuses);
    
    // test that there should be no map setup task returned.
    List<Task> tasks = jobTracker.getSetupAndCleanupTasks(ttStatus);
    assertEquals(1, tasks.size());
    assertTrue(tasks.get(0).isJobSetupTask());
    assertFalse(tasks.get(0).isMapTask());
    jobTracker.jobs.clear();
  }

  /**
   * Test to check that reduce slots are also counted when returning
   * a setup task.
   * @throws IOException
   */
  public void testReduceSlotsCountedForSetup() throws IOException {
    // create a job with a setup task.
    FakeJobInProgress job = createJob(TaskType.JOB_SETUP);
    jobTracker.jobs.put(job.getJobID(), job);
    
    // create another job for reservation
    FakeJobInProgress job1 = createJob(null);
    jobTracker.jobs.put(job1.getJobID(), job1);
    
    // create TT status for testing getSetupAndCleanupTasks
    List<TaskStatus> reports = new ArrayList<TaskStatus>();
    // because free map slots are checked first in code,
    // we fill up map slots also.
    addNewTaskStatus(job1, TaskType.MAP, true, trackers[1], reports);
    addNewTaskStatus(job1, TaskType.REDUCE, false,trackers[1], reports);
    TaskTrackerStatus ttStatus 
      = createTaskTrackerStatus(trackers[1], reports);

    // test that there should be no setup task returned,
    // as both map and reduce slots are occupied.
    List<Task> tasks = jobTracker.getSetupAndCleanupTasks(ttStatus);
    assertNull(tasks);
    jobTracker.jobs.clear();
  }

  void validateNumSlotsUsedForTaskCleanup(TaskTrackerStatus ttStatus)
       throws IOException {
    List<Task> tasks = jobTracker.getSetupAndCleanupTasks(ttStatus);

    assertEquals("Actual number of taskCleanup tasks is not same as expected", 1, tasks.size());
    LOG.info("taskCleanup task is " + tasks.get(0));
    assertTrue(tasks.get(0).isTaskCleanupTask());

    // slots needed for taskCleanup task should be 1(even for high RAM jobs)
    assertEquals("TaskCleanup task should not need more than 1 slot.",
                 1, tasks.get(0).getNumSlotsRequired());
  }
  
  /**
   * Test to check that map slots are counted when returning
   * a taskCleanup task.
   * @throws IOException
   */
  public void testNumSlotsUsedForTaskCleanup() throws IOException {
    // Create a high RAM job with a map task's cleanup task and a reduce task's
    // cleanup task. Make this Fake job a high RAM job by setting the slots
    // required for map/reduce task to 2.
    FakeJobInProgress job = createJob(TaskType.TASK_CLEANUP);
    jobTracker.jobs.put(job.getJobID(), job);
   
    // create TT status for testing getSetupAndCleanupTasks
    List<TaskStatus> taskStatuses = new ArrayList<TaskStatus>();
    TaskTrackerStatus ttStatus =
      createTaskTrackerStatus(trackers[0], taskStatuses);//create dummy status
    addNewTaskStatus(job, TaskType.TASK_CLEANUP, true, trackers[0],
                     taskStatuses);// status of map task's cleanup task
    addNewTaskStatus(job, TaskType.TASK_CLEANUP, false, trackers[0],
                     taskStatuses);// status of reduce task's cleanup task
    ttStatus = createTaskTrackerStatus(trackers[0], taskStatuses);
    
    // validate mapTaskCleanup task
    validateNumSlotsUsedForTaskCleanup(ttStatus);
    
    // validate reduceTaskCleanup task
    validateNumSlotsUsedForTaskCleanup(ttStatus);
    
    jobTracker.jobs.clear();
  }
}
