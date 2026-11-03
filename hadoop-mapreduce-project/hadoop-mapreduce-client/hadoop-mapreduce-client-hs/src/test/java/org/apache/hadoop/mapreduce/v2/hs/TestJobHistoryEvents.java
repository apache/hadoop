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

package org.apache.hadoop.mapreduce.v2.hs;

import java.util.Map;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEventHandler;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.MRApp;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestJobHistoryEvents {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestJobHistoryEvents.class);

  @Test
  public void testHistoryEvents() throws Exception {
    Configuration conf = new Configuration();
    MRApp app = new MRAppWithHistory(2, 1, true, this.getClass().getName(), true);
    app.submit(conf);
    Job job = app.getContext().getAllJobs().values().iterator().next();
    JobId jobId = job.getID();
    LOG.info("JOBID is " + TypeConverter.fromYarn(jobId).toString());
    app.waitForState(job, JobState.SUCCEEDED);
    
    //make sure all events are flushed 
    app.waitForState(Service.STATE.STOPPED);
    /*
     * Use HistoryContext to read logged events and verify the number of 
     * completed maps 
    */
    HistoryContext context = new JobHistory();
    // test start and stop states
    ((JobHistory)context).init(conf);
    ((JobHistory)context).start();
    assertTrue(context.getStartTime() > 0);
    assertEquals(((JobHistory) context).getServiceState(), Service.STATE.STARTED);

    // get job before stopping JobHistory
    Job parsedJob = context.getJob(jobId);

    // stop JobHistory
    ((JobHistory)context).stop();
    assertEquals(((JobHistory) context).getServiceState(), Service.STATE.STOPPED);


    assertEquals(2, parsedJob.getCompletedMaps(),
        "CompletedMaps not correct");
    assertEquals(System.getProperty("user.name"), parsedJob.getUserName());
    
    Map<TaskId, Task> tasks = parsedJob.getTasks();
    assertEquals(3, tasks.size(), "No of tasks not correct");
    for (Task task : tasks.values()) {
      verifyTask(task);
    }
    
    Map<TaskId, Task> maps = parsedJob.getTasks(TaskType.MAP);
    assertEquals(2, maps.size(), "No of maps not correct");
    
    Map<TaskId, Task> reduces = parsedJob.getTasks(TaskType.REDUCE);
    assertEquals(1, reduces.size(), "No of reduces not correct");


    assertEquals(1, parsedJob.getCompletedReduces(), "CompletedReduce not correct");

    assertEquals(JobState.SUCCEEDED, parsedJob.getState(), "Job state not correct");
  }

  /**
   * Verify that all the events are flushed on stopping the HistoryHandler
   * @throws Exception
   */
  @Test
  public void testEventsFlushOnStop() throws Exception {

    Configuration conf = new Configuration();
    MRApp app = new MRAppWithSpecialHistoryHandler(1, 0, true, this
        .getClass().getName(), true);
    app.submit(conf);
    Job job = app.getContext().getAllJobs().values().iterator().next();
    JobId jobId = job.getID();
    LOG.info("JOBID is " + TypeConverter.fromYarn(jobId).toString());
    app.waitForState(job, JobState.SUCCEEDED);

    // make sure all events are flushed
    app.waitForState(Service.STATE.STOPPED);
    /*
     * Use HistoryContext to read logged events and verify the number of
     * completed maps
     */
    HistoryContext context = new JobHistory();
    ((JobHistory) context).init(conf);
    Job parsedJob = context.getJob(jobId);
    assertEquals(1, parsedJob.getCompletedMaps(), "CompletedMaps not correct");

    Map<TaskId, Task> tasks = parsedJob.getTasks();
    assertEquals(1, tasks.size(), "No of tasks not correct");
    verifyTask(tasks.values().iterator().next());

    Map<TaskId, Task> maps = parsedJob.getTasks(TaskType.MAP);
    assertEquals(1, maps.size(), "No of maps not correct");

    assertEquals(JobState.SUCCEEDED, parsedJob.getState(), "Job state not correct");
  }

  @Test
  public void testJobHistoryEventHandlerIsFirstServiceToStop() {
    MRApp app = new MRAppWithSpecialHistoryHandler(1, 0, true, this
        .getClass().getName(), true);
    Configuration conf = new Configuration();
    app.init(conf);
    Service[] services = app.getServices().toArray(new Service[0]);
    // Verifying that it is the last to be added is same as verifying that it is
    // the first to be stopped. CompositeService related tests already validate
    // this.
    assertEquals("JobHistoryEventHandler",
        services[services.length - 1].getName());
  }
  
  @Test
  public void testAssignedQueue() throws Exception {
    Configuration conf = new Configuration();
    MRApp app = new MRAppWithHistory(2, 1, true, this.getClass().getName(),
        true, "assignedQueue");
    app.submit(conf);
    Job job = app.getContext().getAllJobs().values().iterator().next();
    JobId jobId = job.getID();
    LOG.info("JOBID is " + TypeConverter.fromYarn(jobId).toString());
    app.waitForState(job, JobState.SUCCEEDED);
    
    //make sure all events are flushed 
    app.waitForState(Service.STATE.STOPPED);
    /*
     * Use HistoryContext to read logged events and verify the number of 
     * completed maps 
    */
    HistoryContext context = new JobHistory();
    // test start and stop states
    ((JobHistory)context).init(conf);
    ((JobHistory)context).start();
    assertTrue(context.getStartTime() > 0);
    assertThat(((JobHistory)context).getServiceState())
        .isEqualTo(Service.STATE.STARTED);

    // get job before stopping JobHistory
    Job parsedJob = context.getJob(jobId);

    // stop JobHistory
    ((JobHistory)context).stop();
    assertThat(((JobHistory)context).getServiceState())
        .isEqualTo(Service.STATE.STOPPED);

    assertEquals("assignedQueue", parsedJob.getQueueName(),
        "QueueName not correct");
  }

  private void verifyTask(Task task) {
    assertEquals(TaskState.SUCCEEDED, task.getState(), "Task state not correct");
    Map<TaskAttemptId, TaskAttempt> attempts = task.getAttempts();
    assertEquals(1, attempts.size(), "No of attempts not correct");
    for (TaskAttempt attempt : attempts.values()) {
      verifyAttempt(attempt);
    }
  }

  private void verifyAttempt(TaskAttempt attempt) {
    assertEquals(TaskAttemptState.SUCCEEDED, attempt.getState(),
        "TaskAttempt state not correct");
    assertNotNull(attempt.getAssignedContainerID());
    // Verify the wrong ctor is not being used. Remove after mrv1 is removed.
    ContainerId fakeCid = MRApp.newContainerId(-1, -1, -1, -1);
    assertNotEquals(attempt.getAssignedContainerID(), fakeCid);
    //Verify complete containerManagerAddress
    assertEquals(MRApp.NM_HOST + ":" + MRApp.NM_PORT,
        attempt.getAssignedContainerMgrAddress());
  }

  static class MRAppWithHistory extends MRApp {
    public MRAppWithHistory(int maps, int reduces, boolean autoComplete,
        String testName, boolean cleanOnStart) {
      super(maps, reduces, autoComplete, testName, cleanOnStart);
    }

    public MRAppWithHistory(int maps, int reduces, boolean autoComplete,
        String testName, boolean cleanOnStart, String assignedQueue) {
      super(maps, reduces, autoComplete, testName, cleanOnStart, assignedQueue);
    }

    @Override
    protected EventHandler<JobHistoryEvent> createJobHistoryHandler(
        AppContext context) {
      return new JobHistoryEventHandler(
              context, getStartCount());
    }
  }

  /**
   * MRapp with special HistoryEventHandler that writes events only during stop.
   * This is to simulate events that don't get written by the eventHandling
   * thread due to say a slow DFS and verify that they are flushed during stop.
   */
  private static class MRAppWithSpecialHistoryHandler extends MRApp {

    public MRAppWithSpecialHistoryHandler(int maps, int reduces,
        boolean autoComplete, String testName, boolean cleanOnStart) {
      super(maps, reduces, autoComplete, testName, cleanOnStart);
    }

    @Override
    protected EventHandler<JobHistoryEvent> createJobHistoryHandler(
        AppContext context) {
      return new JobHistoryEventHandler(context, getStartCount()) {
        @Override
        protected void serviceStart() {
          // Don't start any event draining thread.
          super.eventHandlingThread = new Thread();
          super.eventHandlingThread.start();
        }
      };
    }

  }

  public static void main(String[] args) throws Exception {
    TestJobHistoryEvents t = new TestJobHistoryEvents();
    t.testHistoryEvents();
    t.testEventsFlushOnStop();
    t.testJobHistoryEventHandlerIsFirstServiceToStop();
  }
}
