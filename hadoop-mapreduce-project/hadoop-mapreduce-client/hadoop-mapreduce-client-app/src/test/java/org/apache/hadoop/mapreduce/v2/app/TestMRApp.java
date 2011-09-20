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

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEventType;
import org.junit.Test;

/**
 * Tests the state machine of MR App.
 */
public class TestMRApp {

  @Test
  public void testMapReduce() throws Exception {
    MRApp app = new MRApp(2, 2, true, this.getClass().getName(), true);
    Job job = app.submit(new Configuration());
    app.waitForState(job, JobState.SUCCEEDED);
    app.verifyCompleted();
    Assert.assertEquals(System.getProperty("user.name"),job.getUserName());
  }

  @Test
  public void testZeroMaps() throws Exception {
    MRApp app = new MRApp(0, 1, true, this.getClass().getName(), true);
    Job job = app.submit(new Configuration());
    app.waitForState(job, JobState.SUCCEEDED);
    app.verifyCompleted();
  }
  
  @Test
  public void testZeroMapReduces() throws Exception{
    MRApp app = new MRApp(0, 0, true, this.getClass().getName(), true);
    Job job = app.submit(new Configuration());
    app.waitForState(job, JobState.SUCCEEDED);
  }
  
  @Test
  public void testCommitPending() throws Exception {
    MRApp app = new MRApp(1, 0, false, this.getClass().getName(), true);
    Job job = app.submit(new Configuration());
    app.waitForState(job, JobState.RUNNING);
    Assert.assertEquals("Num tasks not correct", 1, job.getTasks().size());
    Iterator<Task> it = job.getTasks().values().iterator();
    Task task = it.next();
    app.waitForState(task, TaskState.RUNNING);
    TaskAttempt attempt = task.getAttempts().values().iterator().next();
    app.waitForState(attempt, TaskAttemptState.RUNNING);

    //send the commit pending signal to the task
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            attempt.getID(),
            TaskAttemptEventType.TA_COMMIT_PENDING));

    //wait for first attempt to commit pending
    app.waitForState(attempt, TaskAttemptState.COMMIT_PENDING);

    //send the done signal to the task
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            task.getAttempts().values().iterator().next().getID(),
            TaskAttemptEventType.TA_DONE));

    app.waitForState(job, JobState.SUCCEEDED);
  }

  //@Test
  public void testCompletedMapsForReduceSlowstart() throws Exception {
    MRApp app = new MRApp(2, 1, false, this.getClass().getName(), true);
    Configuration conf = new Configuration();
    //after half of the map completion, reduce will start
    conf.setFloat(MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART, 0.5f);
    //uberization forces full slowstart (1.0), so disable that
    conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false);
    Job job = app.submit(conf);
    app.waitForState(job, JobState.RUNNING);
    //all maps would be running
    Assert.assertEquals("Num tasks not correct", 3, job.getTasks().size());
    Iterator<Task> it = job.getTasks().values().iterator();
    Task mapTask1 = it.next();
    Task mapTask2 = it.next();
    Task reduceTask = it.next();
    
    // all maps must be running
    app.waitForState(mapTask1, TaskState.RUNNING);
    app.waitForState(mapTask2, TaskState.RUNNING);
    
    TaskAttempt task1Attempt = mapTask1.getAttempts().values().iterator().next();
    TaskAttempt task2Attempt = mapTask2.getAttempts().values().iterator().next();
    
    //before sending the TA_DONE, event make sure attempt has come to 
    //RUNNING state
    app.waitForState(task1Attempt, TaskAttemptState.RUNNING);
    app.waitForState(task2Attempt, TaskAttemptState.RUNNING);
    
    // reduces must be in NEW state
    Assert.assertEquals("Reduce Task state not correct",
        TaskState.NEW, reduceTask.getReport().getTaskState());
    
    //send the done signal to the 1st map task
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            mapTask1.getAttempts().values().iterator().next().getID(),
            TaskAttemptEventType.TA_DONE));
    
    //wait for first map task to complete
    app.waitForState(mapTask1, TaskState.SUCCEEDED);

    //Once the first map completes, it will schedule the reduces
    //now reduce must be running
    app.waitForState(reduceTask, TaskState.RUNNING);
    
    //send the done signal to 2nd map and the reduce to complete the job
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            mapTask2.getAttempts().values().iterator().next().getID(),
            TaskAttemptEventType.TA_DONE));
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            reduceTask.getAttempts().values().iterator().next().getID(),
            TaskAttemptEventType.TA_DONE));
    
    app.waitForState(job, JobState.SUCCEEDED);
  }

  @Test
  public void testJobError() throws Exception {
    MRApp app = new MRApp(1, 0, false, this.getClass().getName(), true);
    Job job = app.submit(new Configuration());
    app.waitForState(job, JobState.RUNNING);
    Assert.assertEquals("Num tasks not correct", 1, job.getTasks().size());
    Iterator<Task> it = job.getTasks().values().iterator();
    Task task = it.next();
    app.waitForState(task, TaskState.RUNNING);

    //send an invalid event on task at current state
    app.getContext().getEventHandler().handle(
        new TaskEvent(
            task.getID(), TaskEventType.T_SCHEDULE));

    //this must lead to job error
    app.waitForState(job, JobState.ERROR);
  }

  @Test
  public void checkJobStateTypeConversion() {
    //verify that all states can be converted without 
    // throwing an exception
    for (JobState state : JobState.values()) {
      TypeConverter.fromYarn(state);
    }
  }

  @Test
  public void checkTaskStateTypeConversion() {
    //verify that all states can be converted without 
    // throwing an exception
    for (TaskState state : TaskState.values()) {
      TypeConverter.fromYarn(state);
    }
  }

  public static void main(String[] args) throws Exception {
    TestMRApp t = new TestMRApp();
    t.testMapReduce();
    t.testZeroMapReduces();
    t.testCommitPending();
    t.testCompletedMapsForReduceSlowstart();
    t.testJobError();
  }
}
