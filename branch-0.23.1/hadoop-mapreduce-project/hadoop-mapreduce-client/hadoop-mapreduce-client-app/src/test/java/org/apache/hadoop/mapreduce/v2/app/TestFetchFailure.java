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

import java.util.Arrays;
import java.util.Iterator;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEventStatus;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobTaskAttemptFetchFailureEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.junit.Test;

public class TestFetchFailure {

  @Test
  public void testFetchFailure() throws Exception {
    MRApp app = new MRApp(1, 1, false, this.getClass().getName(), true);
    Configuration conf = new Configuration();
    // map -> reduce -> fetch-failure -> map retry is incompatible with
    // sequential, single-task-attempt approach in uber-AM, so disable:
    conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false);
    Job job = app.submit(conf);
    app.waitForState(job, JobState.RUNNING);
    //all maps would be running
    Assert.assertEquals("Num tasks not correct",
       2, job.getTasks().size());
    Iterator<Task> it = job.getTasks().values().iterator();
    Task mapTask = it.next();
    Task reduceTask = it.next();
    
    //wait for Task state move to RUNNING
    app.waitForState(mapTask, TaskState.RUNNING);
    TaskAttempt mapAttempt1 = mapTask.getAttempts().values().iterator().next();
    app.waitForState(mapAttempt1, TaskAttemptState.RUNNING);

    //send the done signal to the map attempt
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(mapAttempt1.getID(),
            TaskAttemptEventType.TA_DONE));
    
    // wait for map success
    app.waitForState(mapTask, TaskState.SUCCEEDED);
    
    TaskAttemptCompletionEvent[] events = 
      job.getTaskAttemptCompletionEvents(0, 100);
    Assert.assertEquals("Num completion events not correct",
        1, events.length);
    Assert.assertEquals("Event status not correct",
        TaskAttemptCompletionEventStatus.SUCCEEDED, events[0].getStatus());
    
    // wait for reduce to start running
    app.waitForState(reduceTask, TaskState.RUNNING);
    TaskAttempt reduceAttempt = 
      reduceTask.getAttempts().values().iterator().next();
    app.waitForState(reduceAttempt, TaskAttemptState.RUNNING);
    
    //send 3 fetch failures from reduce to trigger map re execution
    sendFetchFailure(app, reduceAttempt, mapAttempt1);
    sendFetchFailure(app, reduceAttempt, mapAttempt1);
    sendFetchFailure(app, reduceAttempt, mapAttempt1);
    
    //wait for map Task state move back to RUNNING
    app.waitForState(mapTask, TaskState.RUNNING);
    
    //map attempt must have become FAILED
    Assert.assertEquals("Map TaskAttempt state not correct",
        TaskAttemptState.FAILED, mapAttempt1.getState());

    Assert.assertEquals("Num attempts in Map Task not correct",
        2, mapTask.getAttempts().size());
    
    Iterator<TaskAttempt> atIt = mapTask.getAttempts().values().iterator();
    atIt.next();
    TaskAttempt mapAttempt2 = atIt.next();
    
    app.waitForState(mapAttempt2, TaskAttemptState.RUNNING);
   //send the done signal to the second map attempt
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(mapAttempt2.getID(),
            TaskAttemptEventType.TA_DONE));
    
    // wait for map success
    app.waitForState(mapTask, TaskState.SUCCEEDED);
    
    //send done to reduce
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(reduceAttempt.getID(),
            TaskAttemptEventType.TA_DONE));

    app.waitForState(job, JobState.SUCCEEDED);
    
    //previous completion event now becomes obsolete
    Assert.assertEquals("Event status not correct",
        TaskAttemptCompletionEventStatus.OBSOLETE, events[0].getStatus());
    
    events = job.getTaskAttemptCompletionEvents(0, 100);
    Assert.assertEquals("Num completion events not correct",
        4, events.length);
    Assert.assertEquals("Event map attempt id not correct",
        mapAttempt1.getID(), events[0].getAttemptId());
    Assert.assertEquals("Event map attempt id not correct",
        mapAttempt1.getID(), events[1].getAttemptId());
    Assert.assertEquals("Event map attempt id not correct",
        mapAttempt2.getID(), events[2].getAttemptId());
    Assert.assertEquals("Event redude attempt id not correct",
        reduceAttempt.getID(), events[3].getAttemptId());
    Assert.assertEquals("Event status not correct for map attempt1",
        TaskAttemptCompletionEventStatus.OBSOLETE, events[0].getStatus());
    Assert.assertEquals("Event status not correct for map attempt1",
        TaskAttemptCompletionEventStatus.FAILED, events[1].getStatus());
    Assert.assertEquals("Event status not correct for map attempt2",
        TaskAttemptCompletionEventStatus.SUCCEEDED, events[2].getStatus());
    Assert.assertEquals("Event status not correct for reduce attempt1",
        TaskAttemptCompletionEventStatus.SUCCEEDED, events[3].getStatus());
  }

  private void sendFetchFailure(MRApp app, TaskAttempt reduceAttempt, 
      TaskAttempt mapAttempt) {
    app.getContext().getEventHandler().handle(
        new JobTaskAttemptFetchFailureEvent(
            reduceAttempt.getID(), 
            Arrays.asList(new TaskAttemptId[] {mapAttempt.getID()})));
  }
}
