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

import org.junit.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.junit.Test;

public class TestJobHistoryEvents {
  private static final Log LOG = LogFactory.getLog(TestJobHistoryEvents.class);

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
    Assert.assertTrue( context.getStartTime()>0);
    Assert.assertEquals(((JobHistory)context).getServiceState(),Service.STATE.STARTED);

    // get job before stopping JobHistory
    Job parsedJob = context.getJob(jobId);

    // stop JobHistory
    ((JobHistory)context).stop();
    Assert.assertEquals(((JobHistory)context).getServiceState(),Service.STATE.STOPPED);


    Assert.assertEquals("CompletedMaps not correct", 2,
        parsedJob.getCompletedMaps());
    Assert.assertEquals(System.getProperty("user.name"), parsedJob.getUserName());
    
    Map<TaskId, Task> tasks = parsedJob.getTasks();
    Assert.assertEquals("No of tasks not correct", 3, tasks.size());
    for (Task task : tasks.values()) {
      verifyTask(task);
    }
    
    Map<TaskId, Task> maps = parsedJob.getTasks(TaskType.MAP);
    Assert.assertEquals("No of maps not correct", 2, maps.size());
    
    Map<TaskId, Task> reduces = parsedJob.getTasks(TaskType.REDUCE);
    Assert.assertEquals("No of reduces not correct", 1, reduces.size());
    
    
    Assert.assertEquals("CompletedReduce not correct", 1,
        parsedJob.getCompletedReduces());
    
    Assert.assertEquals("Job state not currect", JobState.SUCCEEDED,
        parsedJob.getState());
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
    Assert.assertEquals("CompletedMaps not correct", 1, parsedJob
        .getCompletedMaps());

    Map<TaskId, Task> tasks = parsedJob.getTasks();
    Assert.assertEquals("No of tasks not correct", 1, tasks.size());
    verifyTask(tasks.values().iterator().next());

    Map<TaskId, Task> maps = parsedJob.getTasks(TaskType.MAP);
    Assert.assertEquals("No of maps not correct", 1, maps.size());

    Assert.assertEquals("Job state not currect", JobState.SUCCEEDED,
        parsedJob.getState());
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
    Assert.assertEquals("JobHistoryEventHandler",
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
    Assert.assertTrue( context.getStartTime()>0);
    Assert.assertEquals(((JobHistory)context).getServiceState(),Service.STATE.STARTED);

    // get job before stopping JobHistory
    Job parsedJob = context.getJob(jobId);

    // stop JobHistory
    ((JobHistory)context).stop();
    Assert.assertEquals(((JobHistory)context).getServiceState(),Service.STATE.STOPPED);

    Assert.assertEquals("QueueName not correct", "assignedQueue",
        parsedJob.getQueueName());
  }

  private void verifyTask(Task task) {
    Assert.assertEquals("Task state not currect", TaskState.SUCCEEDED,
        task.getState());
    Map<TaskAttemptId, TaskAttempt> attempts = task.getAttempts();
    Assert.assertEquals("No of attempts not correct", 1, attempts.size());
    for (TaskAttempt attempt : attempts.values()) {
      verifyAttempt(attempt);
    }
  }

  private void verifyAttempt(TaskAttempt attempt) {
    Assert.assertEquals("TaskAttempt state not currect", 
        TaskAttemptState.SUCCEEDED, attempt.getState());
    Assert.assertNotNull(attempt.getAssignedContainerID());
  //Verify the wrong ctor is not being used. Remove after mrv1 is removed.
    ContainerId fakeCid = MRApp.newContainerId(-1, -1, -1, -1);
    Assert.assertFalse(attempt.getAssignedContainerID().equals(fakeCid));
    //Verify complete contianerManagerAddress
    Assert.assertEquals(MRApp.NM_HOST + ":" + MRApp.NM_PORT,
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
