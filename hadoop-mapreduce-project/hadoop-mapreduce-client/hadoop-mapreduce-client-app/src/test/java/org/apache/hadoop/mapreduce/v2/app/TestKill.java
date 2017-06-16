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

package org.apache.hadoop.mapreduce.v2.app;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.service.Service;
import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.JobStateInternal;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskTAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.impl.JobImpl;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.junit.Test;

/**
 * Tests the state machine with respect to Job/Task/TaskAttempt kill scenarios.
 *
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class TestKill {

  @Test
  public void testKillJob() throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);
    
    MRApp app = new BlockingMRApp(1, 0, latch);
    //this will start the job but job won't complete as task is
    //blocked
    Job job = app.submit(new Configuration());
    
    //wait and vailidate for Job to become RUNNING
    app.waitForInternalState((JobImpl) job, JobStateInternal.RUNNING);

    //send the kill signal to Job
    app.getContext().getEventHandler().handle(
        new JobEvent(job.getID(), JobEventType.JOB_KILL));
    
    //unblock Task
    latch.countDown();

    //wait and validate for Job to be KILLED
    app.waitForState(job, JobState.KILLED);
    // make sure all events are processed. The AM is stopped
    // only when all tasks and task attempts have been killed
    app.waitForState(Service.STATE.STOPPED);

    Map<TaskId,Task> tasks = job.getTasks();
    Assert.assertEquals("No of tasks is not correct", 1, 
        tasks.size());
    Task task = tasks.values().iterator().next();
    Assert.assertEquals("Task state not correct", TaskState.KILLED, 
        task.getReport().getTaskState());
    Map<TaskAttemptId, TaskAttempt> attempts = 
      tasks.values().iterator().next().getAttempts();
    Assert.assertEquals("No of attempts is not correct", 1, 
        attempts.size());
    Iterator<TaskAttempt> it = attempts.values().iterator();
    Assert.assertEquals("Attempt state not correct", TaskAttemptState.KILLED, 
          it.next().getReport().getTaskAttemptState());
  }

  @Test
  public void testKillTask() throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);
    MRApp app = new BlockingMRApp(2, 0, latch);
    //this will start the job but job won't complete as Task is blocked
    Job job = app.submit(new Configuration());
    
    //wait and vailidate for Job to become RUNNING
    app.waitForInternalState((JobImpl) job, JobStateInternal.RUNNING);
    Map<TaskId,Task> tasks = job.getTasks();
    Assert.assertEquals("No of tasks is not correct", 2, 
        tasks.size());
    Iterator<Task> it = tasks.values().iterator();
    Task task1 = it.next();
    Task task2 = it.next();
    
    //send the kill signal to the first Task
    app.getContext().getEventHandler().handle(
          new TaskEvent(task1.getID(), TaskEventType.T_KILL));
    
    //unblock Task
    latch.countDown();
    
    //wait and validate for Job to become SUCCEEDED
    app.waitForState(job, JobState.SUCCEEDED);
    
    //first Task is killed and second is Succeeded
    //Job is succeeded
    
    Assert.assertEquals("Task state not correct", TaskState.KILLED, 
        task1.getReport().getTaskState());
    Assert.assertEquals("Task state not correct", TaskState.SUCCEEDED, 
        task2.getReport().getTaskState());
    Map<TaskAttemptId, TaskAttempt> attempts = task1.getAttempts();
    Assert.assertEquals("No of attempts is not correct", 1, 
        attempts.size());
    Iterator<TaskAttempt> iter = attempts.values().iterator();
    Assert.assertEquals("Attempt state not correct", TaskAttemptState.KILLED, 
          iter.next().getReport().getTaskAttemptState());

    attempts = task2.getAttempts();
    Assert.assertEquals("No of attempts is not correct", 1, 
        attempts.size());
    iter = attempts.values().iterator();
    Assert.assertEquals("Attempt state not correct", TaskAttemptState.SUCCEEDED, 
          iter.next().getReport().getTaskAttemptState());
  }

  @Test
  public void testKillTaskWait() throws Exception {
    final Dispatcher dispatcher = new AsyncDispatcher() {
      private TaskAttemptEvent cachedKillEvent;
      @Override
      protected void dispatch(Event event) {
        if (event instanceof TaskAttemptEvent) {
          TaskAttemptEvent killEvent = (TaskAttemptEvent) event;
          if (killEvent.getType() == TaskAttemptEventType.TA_KILL) {
            TaskAttemptId taID = killEvent.getTaskAttemptID();
            if (taID.getTaskId().getTaskType() == TaskType.REDUCE
                && taID.getTaskId().getId() == 0 && taID.getId() == 0) {
              // Task is asking the reduce TA to kill itself. 'Create' a race
              // condition. Make the task succeed and then inform the task that
              // TA has succeeded. Once Task gets the TA succeeded event at
              // KILL_WAIT, then relay the actual kill signal to TA
              super.dispatch(new TaskAttemptEvent(taID,
                TaskAttemptEventType.TA_DONE));
              super.dispatch(new TaskAttemptEvent(taID,
                TaskAttemptEventType.TA_CONTAINER_COMPLETED));
              super.dispatch(new TaskTAttemptEvent(taID,
                TaskEventType.T_ATTEMPT_SUCCEEDED));
              this.cachedKillEvent = killEvent;
              return;
            }
          }
        } else if (event instanceof TaskEvent) {
          TaskEvent taskEvent = (TaskEvent) event;
          if (taskEvent.getType() == TaskEventType.T_ATTEMPT_SUCCEEDED
              && this.cachedKillEvent != null) {
            // When the TA comes and reports that it is done, send the
            // cachedKillEvent
            super.dispatch(this.cachedKillEvent);
            return;
          }

        }
        super.dispatch(event);
      }
    };
    MRApp app = new MRApp(1, 1, false, this.getClass().getName(), true) {
      @Override
      public Dispatcher createDispatcher() {
        return dispatcher;
      }
    };
    Job job = app.submit(new Configuration());
    JobId jobId = app.getJobId();
    app.waitForState(job, JobState.RUNNING);
    Assert.assertEquals("Num tasks not correct", 2, job.getTasks().size());
    Iterator<Task> it = job.getTasks().values().iterator();
    Task mapTask = it.next();
    Task reduceTask = it.next();
    app.waitForState(mapTask, TaskState.RUNNING);
    app.waitForState(reduceTask, TaskState.RUNNING);
    TaskAttempt mapAttempt = mapTask.getAttempts().values().iterator().next();
    app.waitForState(mapAttempt, TaskAttemptState.RUNNING);
    TaskAttempt reduceAttempt = reduceTask.getAttempts().values().iterator().next();
    app.waitForState(reduceAttempt, TaskAttemptState.RUNNING);

    // Finish map
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            mapAttempt.getID(),
            TaskAttemptEventType.TA_DONE));
    app.waitForState(mapTask, TaskState.SUCCEEDED);

    // Now kill the job
    app.getContext().getEventHandler()
      .handle(new JobEvent(jobId, JobEventType.JOB_KILL));

    app.waitForInternalState((JobImpl) job, JobStateInternal.KILLED);
  }

  @Test
  public void testKillTaskWaitKillJobAfterTA_DONE() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    final Dispatcher dispatcher = new MyAsyncDispatch(latch, TaskAttemptEventType.TA_DONE);
    MRApp app = new MRApp(1, 1, false, this.getClass().getName(), true) {
      @Override
      public Dispatcher createDispatcher() {
        return dispatcher;
      }
    };
    Job job = app.submit(new Configuration());
    JobId jobId = app.getJobId();
    app.waitForState(job, JobState.RUNNING);
    Assert.assertEquals("Num tasks not correct", 2, job.getTasks().size());
    Iterator<Task> it = job.getTasks().values().iterator();
    Task mapTask = it.next();
    Task reduceTask = it.next();
    app.waitForState(mapTask, TaskState.RUNNING);
    app.waitForState(reduceTask, TaskState.RUNNING);
    TaskAttempt mapAttempt = mapTask.getAttempts().values().iterator().next();
    app.waitForState(mapAttempt, TaskAttemptState.RUNNING);
    TaskAttempt reduceAttempt = reduceTask.getAttempts().values().iterator().next();
    app.waitForState(reduceAttempt, TaskAttemptState.RUNNING);

    // The order in the dispatch event queue, from first to last
    // TA_DONE
    // JobEventType.JOB_KILL
    // TaskAttemptEventType.TA_CONTAINER_COMPLETED ( from TA_DONE handling )
    // TaskEventType.T_KILL ( from JobEventType.JOB_KILL handling )
    // TaskEventType.T_ATTEMPT_SUCCEEDED ( from TA_CONTAINER_COMPLETED handling )

    // Finish map
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            mapAttempt.getID(),
            TaskAttemptEventType.TA_DONE));

    // Now kill the job
    app.getContext().getEventHandler()
        .handle(new JobEvent(jobId, JobEventType.JOB_KILL));

    //unblock
    latch.countDown();

    app.waitForInternalState((JobImpl)job, JobStateInternal.KILLED);
  }


  @Test
  public void testKillTaskWaitKillJobBeforeTA_DONE() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    final Dispatcher dispatcher = new MyAsyncDispatch(latch, JobEventType.JOB_KILL);
    MRApp app = new MRApp(1, 1, false, this.getClass().getName(), true) {
      @Override
      public Dispatcher createDispatcher() {
        return dispatcher;
      }
    };
    Job job = app.submit(new Configuration());
    JobId jobId = app.getJobId();
    app.waitForState(job, JobState.RUNNING);
    Assert.assertEquals("Num tasks not correct", 2, job.getTasks().size());
    Iterator<Task> it = job.getTasks().values().iterator();
    Task mapTask = it.next();
    Task reduceTask = it.next();
    app.waitForState(mapTask, TaskState.RUNNING);
    app.waitForState(reduceTask, TaskState.RUNNING);
    TaskAttempt mapAttempt = mapTask.getAttempts().values().iterator().next();
    app.waitForState(mapAttempt, TaskAttemptState.RUNNING);
    TaskAttempt reduceAttempt = reduceTask.getAttempts().values().iterator().next();
    app.waitForState(reduceAttempt, TaskAttemptState.RUNNING);

    // The order in the dispatch event queue, from first to last
    // JobEventType.JOB_KILL
    // TA_DONE
    // TaskEventType.T_KILL ( from JobEventType.JOB_KILL handling )
    // TaskAttemptEventType.TA_CONTAINER_COMPLETED ( from TA_DONE handling )
    // TaskAttemptEventType.TA_KILL ( from TaskEventType.T_KILL handling )
    // TaskEventType.T_ATTEMPT_SUCCEEDED ( from TA_CONTAINER_COMPLETED handling )
    // TaskEventType.T_ATTEMPT_KILLED ( from TA_KILL handling )

    // Now kill the job
    app.getContext().getEventHandler()
        .handle(new JobEvent(jobId, JobEventType.JOB_KILL));

    // Finish map
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            mapAttempt.getID(),
            TaskAttemptEventType.TA_DONE));

    //unblock
    latch.countDown();

    app.waitForInternalState((JobImpl)job, JobStateInternal.KILLED);
  }

  static class MyAsyncDispatch extends AsyncDispatcher {
    private CountDownLatch latch;
    private TaskAttemptEventType attemptEventTypeToWait;
    private JobEventType jobEventTypeToWait;
    MyAsyncDispatch(CountDownLatch latch, TaskAttemptEventType attemptEventTypeToWait) {
      super();
      this.latch = latch;
      this.attemptEventTypeToWait = attemptEventTypeToWait;
    }

    MyAsyncDispatch(CountDownLatch latch, JobEventType jobEventTypeToWait) {
      super();
      this.latch = latch;
      this.jobEventTypeToWait = jobEventTypeToWait;
    }

    @Override
    protected void dispatch(Event event) {
      if (event instanceof TaskAttemptEvent) {
        TaskAttemptEvent attemptEvent = (TaskAttemptEvent) event;
        TaskAttemptId attemptID = ((TaskAttemptEvent) event).getTaskAttemptID();
        if (attemptEvent.getType() == this.attemptEventTypeToWait
            && attemptID.getTaskId().getId() == 0 && attemptID.getId() == 0 ) {
          try {
            latch.await();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      } else if ( event instanceof JobEvent) {
        JobEvent jobEvent = (JobEvent) event;
        if (jobEvent.getType() == this.jobEventTypeToWait) {
          try {
            latch.await();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }

      super.dispatch(event);
    }
  }

  @Test
  public void testKillTaskAttempt() throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);
    MRApp app = new BlockingMRApp(2, 0, latch);
    //this will start the job but job won't complete as Task is blocked
    Job job = app.submit(new Configuration());
    
    //wait and vailidate for Job to become RUNNING
    app.waitForState(job, JobState.RUNNING);
    Map<TaskId,Task> tasks = job.getTasks();
    Assert.assertEquals("No of tasks is not correct", 2, 
        tasks.size());
    Iterator<Task> it = tasks.values().iterator();
    Task task1 = it.next();
    Task task2 = it.next();
    
    //wait for tasks to become running
    app.waitForState(task1, TaskState.SCHEDULED);
    app.waitForState(task2, TaskState.SCHEDULED);
    
    //send the kill signal to the first Task's attempt
    TaskAttempt attempt = task1.getAttempts().values().iterator().next();
    app.getContext().getEventHandler().handle(
          new TaskAttemptEvent(attempt.getID(), TaskAttemptEventType.TA_KILL));
    
    //unblock
    latch.countDown();
    
    //wait and validate for Job to become SUCCEEDED
    //job will still succeed
    app.waitForState(job, JobState.SUCCEEDED);
    
    //first Task will have two attempts 1st is killed, 2nd Succeeds
    //both Tasks and Job succeeds
    Assert.assertEquals("Task state not correct", TaskState.SUCCEEDED, 
        task1.getReport().getTaskState());
    Assert.assertEquals("Task state not correct", TaskState.SUCCEEDED, 
        task2.getReport().getTaskState());
 
    Map<TaskAttemptId, TaskAttempt> attempts = task1.getAttempts();
    Assert.assertEquals("No of attempts is not correct", 2, 
        attempts.size());
    Iterator<TaskAttempt> iter = attempts.values().iterator();
    Assert.assertEquals("Attempt state not correct", TaskAttemptState.KILLED, 
          iter.next().getReport().getTaskAttemptState());
    Assert.assertEquals("Attempt state not correct", TaskAttemptState.SUCCEEDED, 
        iter.next().getReport().getTaskAttemptState());
    
    attempts = task2.getAttempts();
    Assert.assertEquals("No of attempts is not correct", 1, 
        attempts.size());
    iter = attempts.values().iterator();
    Assert.assertEquals("Attempt state not correct", TaskAttemptState.SUCCEEDED, 
          iter.next().getReport().getTaskAttemptState());
  }

  static class BlockingMRApp extends MRApp {
    private CountDownLatch latch;
    BlockingMRApp(int maps, int reduces, CountDownLatch latch) {
      super(maps, reduces, true, "testKill", true);
      this.latch = latch;
    }

    @Override
    protected void attemptLaunched(TaskAttemptId attemptID) {
      if (attemptID.getTaskId().getId() == 0 && attemptID.getId() == 0) {
        //this blocks the first task's first attempt
        //the subsequent ones are completed
        try {
          latch.await();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      } else {
        getContext().getEventHandler().handle(
            new TaskAttemptEvent(attemptID,
                TaskAttemptEventType.TA_DONE));
      }
    }
  }

  public static void main(String[] args) throws Exception {
    TestKill t = new TestKill();
    t.testKillJob();
    t.testKillTask();
    t.testKillTaskAttempt();
  }
}
