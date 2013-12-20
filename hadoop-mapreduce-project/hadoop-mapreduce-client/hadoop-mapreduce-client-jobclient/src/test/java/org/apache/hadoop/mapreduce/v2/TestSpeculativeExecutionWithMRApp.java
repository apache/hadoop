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

package org.apache.hadoop.mapreduce.v2;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.app.ControlledClock;
import org.apache.hadoop.mapreduce.v2.app.MRApp;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent.TaskAttemptStatus;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.junit.Test;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class TestSpeculativeExecutionWithMRApp {

  private static final int NUM_MAPPERS = 5;
  private static final int NUM_REDUCERS = 0;

  @Test(timeout = 60000)
  public void testSpeculateSuccessfulWithoutUpdateEvents() throws Exception {

    Clock actualClock = new SystemClock();
    ControlledClock clock = new ControlledClock(actualClock);
    clock.setTime(System.currentTimeMillis());

    MRApp app =
        new MRApp(NUM_MAPPERS, NUM_REDUCERS, false, "test", true, clock);
    Job job = app.submit(new Configuration(), true, true);
    app.waitForState(job, JobState.RUNNING);

    Map<TaskId, Task> tasks = job.getTasks();
    Assert.assertEquals("Num tasks is not correct", NUM_MAPPERS + NUM_REDUCERS,
      tasks.size());
    Iterator<Task> taskIter = tasks.values().iterator();
    while (taskIter.hasNext()) {
      app.waitForState(taskIter.next(), TaskState.RUNNING);
    }

    // Process the update events
    clock.setTime(System.currentTimeMillis() + 2000);
    EventHandler appEventHandler = app.getContext().getEventHandler();
    for (Map.Entry<TaskId, Task> mapTask : tasks.entrySet()) {
      for (Map.Entry<TaskAttemptId, TaskAttempt> taskAttempt : mapTask
        .getValue().getAttempts().entrySet()) {
        TaskAttemptStatus status =
            createTaskAttemptStatus(taskAttempt.getKey(), (float) 0.8,
              TaskAttemptState.RUNNING);
        TaskAttemptStatusUpdateEvent event =
            new TaskAttemptStatusUpdateEvent(taskAttempt.getKey(), status);
        appEventHandler.handle(event);
      }
    }

    Random generator = new Random();
    Object[] taskValues = tasks.values().toArray();
    Task taskToBeSpeculated =
        (Task) taskValues[generator.nextInt(taskValues.length)];

    // Other than one random task, finish every other task.
    for (Map.Entry<TaskId, Task> mapTask : tasks.entrySet()) {
      for (Map.Entry<TaskAttemptId, TaskAttempt> taskAttempt : mapTask
        .getValue().getAttempts().entrySet()) {
        if (mapTask.getKey() != taskToBeSpeculated.getID()) {
          appEventHandler.handle(new TaskAttemptEvent(taskAttempt.getKey(),
            TaskAttemptEventType.TA_DONE));
          appEventHandler.handle(new TaskAttemptEvent(taskAttempt.getKey(),
            TaskAttemptEventType.TA_CONTAINER_CLEANED));
          app.waitForState(taskAttempt.getValue(), TaskAttemptState.SUCCEEDED);
        }
      }
    }

    int maxTimeWait = 10;
    boolean successfullySpeculated = false;
    TaskAttempt[] ta = null;
    while (maxTimeWait > 0 && !successfullySpeculated) {
      if (taskToBeSpeculated.getAttempts().size() != 2) {
        Thread.sleep(1000);
        clock.setTime(System.currentTimeMillis() + 20000);
      } else {
        successfullySpeculated = true;
        // finish 1st TA, 2nd will be killed
        ta = makeFirstAttemptWin(appEventHandler, taskToBeSpeculated);
      }
      maxTimeWait--;
    }
    Assert
      .assertTrue("Couldn't speculate successfully", successfullySpeculated);
    verifySpeculationMessage(app, ta);
  }

  @Test(timeout = 60000)
  public void testSepculateSuccessfulWithUpdateEvents() throws Exception {

    Clock actualClock = new SystemClock();
    ControlledClock clock = new ControlledClock(actualClock);
    clock.setTime(System.currentTimeMillis());

    MRApp app =
        new MRApp(NUM_MAPPERS, NUM_REDUCERS, false, "test", true, clock);
    Job job = app.submit(new Configuration(), true, true);
    app.waitForState(job, JobState.RUNNING);

    Map<TaskId, Task> tasks = job.getTasks();
    Assert.assertEquals("Num tasks is not correct", NUM_MAPPERS + NUM_REDUCERS,
      tasks.size());
    Iterator<Task> taskIter = tasks.values().iterator();
    while (taskIter.hasNext()) {
      app.waitForState(taskIter.next(), TaskState.RUNNING);
    }

    // Process the update events
    clock.setTime(System.currentTimeMillis() + 1000);
    EventHandler appEventHandler = app.getContext().getEventHandler();
    for (Map.Entry<TaskId, Task> mapTask : tasks.entrySet()) {
      for (Map.Entry<TaskAttemptId, TaskAttempt> taskAttempt : mapTask
        .getValue().getAttempts().entrySet()) {
        TaskAttemptStatus status =
            createTaskAttemptStatus(taskAttempt.getKey(), (float) 0.5,
              TaskAttemptState.RUNNING);
        TaskAttemptStatusUpdateEvent event =
            new TaskAttemptStatusUpdateEvent(taskAttempt.getKey(), status);
        appEventHandler.handle(event);
      }
    }

    Task speculatedTask = null;
    int numTasksToFinish = NUM_MAPPERS + NUM_REDUCERS - 1;
    clock.setTime(System.currentTimeMillis() + 1000);
    for (Map.Entry<TaskId, Task> task : tasks.entrySet()) {
      for (Map.Entry<TaskAttemptId, TaskAttempt> taskAttempt : task.getValue()
        .getAttempts().entrySet()) {
        if (numTasksToFinish > 0) {
          appEventHandler.handle(new TaskAttemptEvent(taskAttempt.getKey(),
            TaskAttemptEventType.TA_DONE));
          appEventHandler.handle(new TaskAttemptEvent(taskAttempt.getKey(),
            TaskAttemptEventType.TA_CONTAINER_CLEANED));
          numTasksToFinish--;
          app.waitForState(taskAttempt.getValue(), TaskAttemptState.SUCCEEDED);
        } else {
          // The last task is chosen for speculation
          TaskAttemptStatus status =
              createTaskAttemptStatus(taskAttempt.getKey(), (float) 0.75,
                TaskAttemptState.RUNNING);
          speculatedTask = task.getValue();
          TaskAttemptStatusUpdateEvent event =
              new TaskAttemptStatusUpdateEvent(taskAttempt.getKey(), status);
          appEventHandler.handle(event);
        }
      }
    }

    clock.setTime(System.currentTimeMillis() + 15000);
    for (Map.Entry<TaskId, Task> task : tasks.entrySet()) {
      for (Map.Entry<TaskAttemptId, TaskAttempt> taskAttempt : task.getValue()
        .getAttempts().entrySet()) {
        if (taskAttempt.getValue().getState() != TaskAttemptState.SUCCEEDED) {
          TaskAttemptStatus status =
              createTaskAttemptStatus(taskAttempt.getKey(), (float) 0.75,
                TaskAttemptState.RUNNING);
          TaskAttemptStatusUpdateEvent event =
              new TaskAttemptStatusUpdateEvent(taskAttempt.getKey(), status);
          appEventHandler.handle(event);
        }
      }
    }

    int maxTimeWait = 5;
    boolean successfullySpeculated = false;
    TaskAttempt[] ta = null;
    while (maxTimeWait > 0 && !successfullySpeculated) {
      if (speculatedTask.getAttempts().size() != 2) {
        Thread.sleep(1000);
      } else {
        successfullySpeculated = true;
        ta = makeFirstAttemptWin(appEventHandler, speculatedTask);
      }
      maxTimeWait--;
    }
    Assert
      .assertTrue("Couldn't speculate successfully", successfullySpeculated);
    verifySpeculationMessage(app, ta);
  }

  private static TaskAttempt[] makeFirstAttemptWin(
      EventHandler appEventHandler, Task speculatedTask) {

    // finish 1st TA, 2nd will be killed
    Collection<TaskAttempt> attempts = speculatedTask.getAttempts().values();
    TaskAttempt[] ta = new TaskAttempt[attempts.size()];
    attempts.toArray(ta);
    appEventHandler.handle(
        new TaskAttemptEvent(ta[0].getID(), TaskAttemptEventType.TA_DONE));
    appEventHandler.handle(new TaskAttemptEvent(ta[0].getID(),
        TaskAttemptEventType.TA_CONTAINER_CLEANED));
    return ta;
  }

  private static void verifySpeculationMessage(MRApp app, TaskAttempt[] ta)
      throws Exception {
    app.waitForState(ta[0], TaskAttemptState.SUCCEEDED);
    app.waitForState(ta[1], TaskAttemptState.KILLED);
    boolean foundSpecMsg = false;
    for (String msg : ta[1].getDiagnostics()) {
      if (msg.contains("Speculation")) {
        foundSpecMsg = true;
        break;
      }
    }
    Assert.assertTrue("No speculation diagnostics!", foundSpecMsg);
  }

  private TaskAttemptStatus createTaskAttemptStatus(TaskAttemptId id,
      float progress, TaskAttemptState state) {
    TaskAttemptStatus status = new TaskAttemptStatus();
    status.id = id;
    status.progress = progress;
    status.taskState = state;
    return status;
  }
}
