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

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEventType;
import org.junit.Test;

/**
 * Tests the state machine with respect to Job/Task/TaskAttempt kill scenarios.
 *
 */
public class TestKill {

  @Test
  public void testKillJob() throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);
    
    MRApp app = new BlockingMRApp(1, 0, latch);
    //this will start the job but job won't complete as task is
    //blocked
    Job job = app.submit(new Configuration());
    
    //wait and vailidate for Job to become RUNNING
    app.waitForState(job, JobState.RUNNING);
    
    //send the kill signal to Job
    app.getContext().getEventHandler().handle(
        new JobEvent(job.getID(), JobEventType.JOB_KILL));
    
    //unblock Task
    latch.countDown();

    //wait and validate for Job to be KILLED
    app.waitForState(job, JobState.KILLED);
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
    app.waitForState(job, JobState.RUNNING);
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
