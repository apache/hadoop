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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEventHandler;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.Phase;
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
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent;
import org.apache.hadoop.yarn.event.EventHandler;
import org.junit.Assert;
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

    TaskCompletionEvent mapEvents[] =
        job.getMapAttemptCompletionEvents(0, 2);
    TaskCompletionEvent convertedEvents[] = TypeConverter.fromYarn(events);
    Assert.assertEquals("Incorrect number of map events", 2, mapEvents.length);
    Assert.assertArrayEquals("Unexpected map events",
        Arrays.copyOfRange(convertedEvents, 0, 2), mapEvents);
    mapEvents = job.getMapAttemptCompletionEvents(2, 200);
    Assert.assertEquals("Incorrect number of map events", 1, mapEvents.length);
    Assert.assertEquals("Unexpected map event", convertedEvents[2],
        mapEvents[0]);
  }
  
  /**
   * This tests that if a map attempt was failed (say due to fetch failures),
   * then it gets re-run. When the next map attempt is running, if the AM dies,
   * then, on AM re-run, the AM does not incorrectly remember the first failed
   * attempt. Currently recovery does not recover running tasks. Effectively,
   * the AM re-runs the maps from scratch.
   */
  @Test
  public void testFetchFailureWithRecovery() throws Exception {
    int runCount = 0;
    MRApp app = new MRAppWithHistory(1, 1, false, this.getClass().getName(), true, ++runCount);
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

    // Crash the app again.
    app.stop();

    //rerun
    app =
      new MRAppWithHistory(1, 1, false, this.getClass().getName(), false,
          ++runCount);
    conf = new Configuration();
    conf.setBoolean(MRJobConfig.MR_AM_JOB_RECOVERY_ENABLE, true);
    conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false);
    job = app.submit(conf);
    app.waitForState(job, JobState.RUNNING);
    //all maps would be running
    Assert.assertEquals("Num tasks not correct",
        2, job.getTasks().size());
    it = job.getTasks().values().iterator();
    mapTask = it.next();
    reduceTask = it.next();

    // the map is not in a SUCCEEDED state after restart of AM
    app.waitForState(mapTask, TaskState.RUNNING);
    mapAttempt1 = mapTask.getAttempts().values().iterator().next();
    app.waitForState(mapAttempt1, TaskAttemptState.RUNNING);

    //send the done signal to the map attempt
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(mapAttempt1.getID(),
          TaskAttemptEventType.TA_DONE));

    // wait for map success
    app.waitForState(mapTask, TaskState.SUCCEEDED);

    reduceAttempt = reduceTask.getAttempts().values().iterator().next();
    //send done to reduce
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(reduceAttempt.getID(),
          TaskAttemptEventType.TA_DONE));

    app.waitForState(job, JobState.SUCCEEDED);
    events = job.getTaskAttemptCompletionEvents(0, 100);
    Assert.assertEquals("Num completion events not correct", 2, events.length);
  }
  
  @Test
  public void testFetchFailureMultipleReduces() throws Exception {
    MRApp app = new MRApp(1, 3, false, this.getClass().getName(), true);
    Configuration conf = new Configuration();
    // map -> reduce -> fetch-failure -> map retry is incompatible with
    // sequential, single-task-attempt approach in uber-AM, so disable:
    conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false);
    Job job = app.submit(conf);
    app.waitForState(job, JobState.RUNNING);
    //all maps would be running
    Assert.assertEquals("Num tasks not correct",
       4, job.getTasks().size());
    Iterator<Task> it = job.getTasks().values().iterator();
    Task mapTask = it.next();
    Task reduceTask = it.next();
    Task reduceTask2 = it.next();
    Task reduceTask3 = it.next();
    
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
    app.waitForState(reduceTask2, TaskState.RUNNING);
    app.waitForState(reduceTask3, TaskState.RUNNING);
    TaskAttempt reduceAttempt = 
      reduceTask.getAttempts().values().iterator().next();
    app.waitForState(reduceAttempt, TaskAttemptState.RUNNING);
    
    updateStatus(app, reduceAttempt, Phase.SHUFFLE);
    
    TaskAttempt reduceAttempt2 = 
      reduceTask2.getAttempts().values().iterator().next();
    app.waitForState(reduceAttempt2, TaskAttemptState.RUNNING);
    updateStatus(app, reduceAttempt2, Phase.SHUFFLE);
    
    TaskAttempt reduceAttempt3 = 
      reduceTask3.getAttempts().values().iterator().next();
    app.waitForState(reduceAttempt3, TaskAttemptState.RUNNING);
    updateStatus(app, reduceAttempt3, Phase.SHUFFLE);
    
    //send 3 fetch failures from reduce to trigger map re execution
    sendFetchFailure(app, reduceAttempt, mapAttempt1);
    sendFetchFailure(app, reduceAttempt, mapAttempt1);
    sendFetchFailure(app, reduceAttempt, mapAttempt1);
    
    //We should not re-launch the map task yet
    assertEquals(TaskState.SUCCEEDED, mapTask.getState());
    updateStatus(app, reduceAttempt2, Phase.REDUCE);
    updateStatus(app, reduceAttempt3, Phase.REDUCE);
    
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
    
    //send done to reduce
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(reduceAttempt2.getID(),
            TaskAttemptEventType.TA_DONE));
    
    //send done to reduce
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(reduceAttempt3.getID(),
            TaskAttemptEventType.TA_DONE));

    app.waitForState(job, JobState.SUCCEEDED);
    
    //previous completion event now becomes obsolete
    Assert.assertEquals("Event status not correct",
        TaskAttemptCompletionEventStatus.OBSOLETE, events[0].getStatus());
    
    events = job.getTaskAttemptCompletionEvents(0, 100);
    Assert.assertEquals("Num completion events not correct",
        6, events.length);
    Assert.assertEquals("Event map attempt id not correct",
        mapAttempt1.getID(), events[0].getAttemptId());
    Assert.assertEquals("Event map attempt id not correct",
        mapAttempt1.getID(), events[1].getAttemptId());
    Assert.assertEquals("Event map attempt id not correct",
        mapAttempt2.getID(), events[2].getAttemptId());
    Assert.assertEquals("Event reduce attempt id not correct",
        reduceAttempt.getID(), events[3].getAttemptId());
    Assert.assertEquals("Event status not correct for map attempt1",
        TaskAttemptCompletionEventStatus.OBSOLETE, events[0].getStatus());
    Assert.assertEquals("Event status not correct for map attempt1",
        TaskAttemptCompletionEventStatus.FAILED, events[1].getStatus());
    Assert.assertEquals("Event status not correct for map attempt2",
        TaskAttemptCompletionEventStatus.SUCCEEDED, events[2].getStatus());
    Assert.assertEquals("Event status not correct for reduce attempt1",
        TaskAttemptCompletionEventStatus.SUCCEEDED, events[3].getStatus());

    TaskCompletionEvent mapEvents[] =
        job.getMapAttemptCompletionEvents(0, 2);
    TaskCompletionEvent convertedEvents[] = TypeConverter.fromYarn(events);
    Assert.assertEquals("Incorrect number of map events", 2, mapEvents.length);
    Assert.assertArrayEquals("Unexpected map events",
        Arrays.copyOfRange(convertedEvents, 0, 2), mapEvents);
    mapEvents = job.getMapAttemptCompletionEvents(2, 200);
    Assert.assertEquals("Incorrect number of map events", 1, mapEvents.length);
    Assert.assertEquals("Unexpected map event", convertedEvents[2],
        mapEvents[0]);
  }
  

  private void updateStatus(MRApp app, TaskAttempt attempt, Phase phase) {
    TaskAttemptStatusUpdateEvent.TaskAttemptStatus status = new TaskAttemptStatusUpdateEvent.TaskAttemptStatus();
    status.counters = new Counters();
    status.fetchFailedMaps = new ArrayList<TaskAttemptId>();
    status.id = attempt.getID();
    status.mapFinishTime = 0;
    status.phase = phase;
    status.progress = 0.5f;
    status.shuffleFinishTime = 0;
    status.sortFinishTime = 0;
    status.stateString = "OK";
    status.taskState = attempt.getState();
    TaskAttemptStatusUpdateEvent event = new TaskAttemptStatusUpdateEvent(attempt.getID(),
        status);
    app.getContext().getEventHandler().handle(event);
  }

  private void sendFetchFailure(MRApp app, TaskAttempt reduceAttempt, 
      TaskAttempt mapAttempt) {
    app.getContext().getEventHandler().handle(
        new JobTaskAttemptFetchFailureEvent(
            reduceAttempt.getID(), 
            Arrays.asList(new TaskAttemptId[] {mapAttempt.getID()})));
  }
  
  static class MRAppWithHistory extends MRApp {
    public MRAppWithHistory(int maps, int reduces, boolean autoComplete,
        String testName, boolean cleanOnStart, int startCount) {
      super(maps, reduces, autoComplete, testName, cleanOnStart, startCount);
    }

    @Override
    protected EventHandler<JobHistoryEvent> createJobHistoryHandler(
        AppContext context) {
      JobHistoryEventHandler eventHandler = new JobHistoryEventHandler(context, 
          getStartCount());
      return eventHandler;
    }
  }

}
