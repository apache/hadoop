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

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.app.speculate.LegacyTaskRuntimeEstimator;
import org.apache.hadoop.mapreduce.v2.app.speculate.SimpleExponentialTaskRuntimeEstimator;
import org.apache.hadoop.mapreduce.v2.app.speculate.TaskRuntimeEstimator;
import org.junit.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.app.MRApp;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent.TaskAttemptStatus;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.ControlledClock;
import org.junit.Before;
import org.junit.Test;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * The type Test speculative execution with mr app.
 * It test the speculation behavior given a list of estimator classes.
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
@RunWith(Parameterized.class)
public class TestSpeculativeExecutionWithMRApp {
  private static final int NUM_MAPPERS = 5;
  private static final int NUM_REDUCERS = 0;

  /**
   * Get test parameters.
   *
   * @return the test parameters
   */
  @Parameterized.Parameters(name = "{index}: TaskEstimator(EstimatorClass {0})")
  public static Collection<Object[]> getTestParameters() {
    return Arrays.asList(new Object[][] {
        {SimpleExponentialTaskRuntimeEstimator.class},
        {LegacyTaskRuntimeEstimator.class}
    });
  }

  private Class<? extends TaskRuntimeEstimator> estimatorClass;

  private final ControlledClock controlledClk;
  /**
   * Instantiates a new Test speculative execution with mr app.
   *
   * @param estimatorKlass the estimator klass
   */
  public TestSpeculativeExecutionWithMRApp(
      Class<? extends TaskRuntimeEstimator>  estimatorKlass) {
    this.estimatorClass = estimatorKlass;
    this.controlledClk = new ControlledClock();
  }

  @Before
  public void setup() {
    this.controlledClk.setTime(System.currentTimeMillis());
  }

  /**
   * Test speculate successful without update events.
   *
   * @throws Exception the exception
   */
  @Test (timeout = 360000)
  public void testSpeculateSuccessfulWithoutUpdateEvents() throws Exception {
    MRApp app =
        new MRApp(NUM_MAPPERS, NUM_REDUCERS, false, "test", true,
            controlledClk);
    Job job = app.submit(createConfiguration(), true, true);
    app.waitForState(job, JobState.RUNNING);

    Map<TaskId, Task> tasks = job.getTasks();
    Assert.assertEquals("Num tasks is not correct", NUM_MAPPERS + NUM_REDUCERS,
      tasks.size());
    Iterator<Task> taskIter = tasks.values().iterator();
    while (taskIter.hasNext()) {
      app.waitForState(taskIter.next(), TaskState.RUNNING);
    }

    // Process the update events.
    controlledClk.tickMsec(1000L);
    EventHandler appEventHandler = app.getContext().getEventHandler();
    for (Map.Entry<TaskId, Task> mapTask : tasks.entrySet()) {
      for (Map.Entry<TaskAttemptId, TaskAttempt> taskAttempt : mapTask
        .getValue().getAttempts().entrySet()) {
        updateTaskProgress(appEventHandler, taskAttempt.getValue(), 0.8f);
      }
    }

    Random generator = new Random();
    Object[] taskValues = tasks.values().toArray();
    final Task taskToBeSpeculated =
        (Task) taskValues[generator.nextInt(taskValues.length)];

    // Other than one random task, finish every other task.
    for (Map.Entry<TaskId, Task> mapTask : tasks.entrySet()) {
      if (mapTask.getKey() != taskToBeSpeculated.getID()) {
        for (Map.Entry<TaskAttemptId, TaskAttempt> taskAttempt : mapTask
            .getValue().getAttempts().entrySet()) {
          TaskAttemptId taId = taskAttempt.getKey();
          if (taId.getId() > 0) {
            // in case the speculator started a speculative TA, then skip it.
            continue;
          }
          markTACompleted(appEventHandler, taskAttempt.getValue());
          waitForTAState(taskAttempt.getValue(), TaskAttemptState.SUCCEEDED,
              controlledClk);
        }
      }
    }
    controlledClk.tickMsec(2000L);
    waitForSpeculation(taskToBeSpeculated, controlledClk);
    // finish 1st TA, 2nd will be killed
    TaskAttempt[] ta = makeFirstAttemptWin(appEventHandler, taskToBeSpeculated);
    waitForTAState(ta[0], TaskAttemptState.SUCCEEDED, controlledClk);
    waitForAppStop(app, controlledClk);
  }

  /**
   * Test speculate successful with update events.
   *
   * @throws Exception the exception
   */
  @Test (timeout = 360000)
  public void testSpeculateSuccessfulWithUpdateEvents() throws Exception {
    MRApp app =
        new MRApp(NUM_MAPPERS, NUM_REDUCERS, false, "test", true,
            controlledClk);
    Job job = app.submit(createConfiguration(), true, true);
    app.waitForState(job, JobState.RUNNING);

    Map<TaskId, Task> tasks = job.getTasks();
    Assert.assertEquals("Num tasks is not correct", NUM_MAPPERS + NUM_REDUCERS,
      tasks.size());
    Iterator<Task> taskIter = tasks.values().iterator();
    while (taskIter.hasNext()) {
      app.waitForState(taskIter.next(), TaskState.RUNNING);
    }

    // process the update events. Note that we should avoid advancing the clock
    // by a value that triggers a speculation scan while updating the task
    // progress, because the speculator may concurrently speculate tasks before
    // we update their progress.
    controlledClk.tickMsec(2000L);
    EventHandler appEventHandler = app.getContext().getEventHandler();
    for (Map.Entry<TaskId, Task> mapTask : tasks.entrySet()) {
      for (Map.Entry<TaskAttemptId, TaskAttempt> taskAttempt : mapTask
        .getValue().getAttempts().entrySet()) {
        updateTaskProgress(appEventHandler, taskAttempt.getValue(), 0.5f);
      }
    }

    Task speculatedTask = null;
    int numTasksToFinish = NUM_MAPPERS + NUM_REDUCERS - 1;
    controlledClk.tickMsec(1000L);
    for (Map.Entry<TaskId, Task> task : tasks.entrySet()) {
      for (Map.Entry<TaskAttemptId, TaskAttempt> taskAttempt : task.getValue()
        .getAttempts().entrySet()) {
        TaskAttemptId taId = taskAttempt.getKey();
        if (numTasksToFinish > 0 && taId.getId() == 0) {
          // Skip speculative attempts if any.
          markTACompleted(appEventHandler, taskAttempt.getValue());
          numTasksToFinish--;
          waitForTAState(taskAttempt.getValue(), TaskAttemptState.SUCCEEDED,
              controlledClk);
        } else {
          // The last task is chosen for speculation
          speculatedTask = task.getValue();
          updateTaskProgress(appEventHandler, taskAttempt.getValue(), 0.75f);
        }
      }
    }

    controlledClk.tickMsec(15000L);

    for (Map.Entry<TaskId, Task> task : tasks.entrySet()) {
      for (Map.Entry<TaskAttemptId, TaskAttempt> taskAttempt : task.getValue()
        .getAttempts().entrySet()) {
        // Skip task attempts that are finished or killed.
        if (!(taskAttempt.getValue().getState() == TaskAttemptState.SUCCEEDED
            || taskAttempt.getValue().getState() == TaskAttemptState.KILLED)) {
          updateTaskProgress(appEventHandler, taskAttempt.getValue(), 0.75f);
        }
      }
    }

    final Task speculatedTaskConst = speculatedTask;
    waitForSpeculation(speculatedTaskConst, controlledClk);

    TaskAttempt[] ta = makeFirstAttemptWin(appEventHandler, speculatedTask);
    waitForTAState(ta[0], TaskAttemptState.SUCCEEDED, controlledClk);
    waitForAppStop(app, controlledClk);
  }

  private static TaskAttempt[] makeFirstAttemptWin(
      EventHandler appEventHandler, Task speculatedTask) {
    // finish 1st TA, 2nd will be killed
    Collection<TaskAttempt> attempts = speculatedTask.getAttempts().values();
    TaskAttempt[] ta = new TaskAttempt[attempts.size()];
    attempts.toArray(ta);
    markTACompleted(appEventHandler, ta[0]);
    return ta;
  }

  private static void markTACompleted(
      EventHandler appEventHandler, TaskAttempt attempt) {
    appEventHandler.handle(
        new TaskAttemptEvent(attempt.getID(), TaskAttemptEventType.TA_DONE));
    appEventHandler.handle(new TaskAttemptEvent(attempt.getID(),
        TaskAttemptEventType.TA_CONTAINER_COMPLETED));
  }

  private TaskAttemptStatus createTaskAttemptStatus(TaskAttemptId id,
      float progress, TaskAttemptState state) {
    TaskAttemptStatus status = new TaskAttemptStatus();
    status.id = id;
    status.progress = progress;
    status.taskState = state;
    return status;
  }

  private Configuration createConfiguration() {
    Configuration conf = new Configuration();
    conf.setClass(MRJobConfig.MR_AM_TASK_ESTIMATOR,
        estimatorClass,
        TaskRuntimeEstimator.class);
    if (SimpleExponentialTaskRuntimeEstimator.class.equals(estimatorClass)) {
      // set configurations specific to SimpleExponential estimator
      conf.setInt(
          MRJobConfig.MR_AM_TASK_ESTIMATOR_SIMPLE_SMOOTH_SKIP_INITIALS, 1);
      conf.setLong(
          MRJobConfig.MR_AM_TASK_ESTIMATOR_SIMPLE_SMOOTH_LAMBDA_MS,
          1000L * 10);
    }
    conf.setLong(MRJobConfig.SPECULATIVE_RETRY_AFTER_NO_SPECULATE,
        3000L);
    return conf;
  }

  /**
   * Wait for MRapp to stop while incrementing the controlled clock.
   * @param app the MRApp to be stopped.
   * @param cClock the controlled clock of the test.
   * @throws TimeoutException
   * @throws InterruptedException
   */
  private void waitForAppStop(final MRApp app, final ControlledClock cClock)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> {
      if (app.getServiceState() != Service.STATE.STOPPED) {
        cClock.tickMsec(250L);
        return false;
      }
      return true;
    }, 250, 60000);
  }

  /**
   * Wait for the task to trigger a new speculation.
   * @param speculatedTask the task we are monitoring.
   * @param cClock the controlled clock of the test.
   * @throws TimeoutException
   * @throws InterruptedException
   */
  private void waitForSpeculation(final Task speculatedTask,
      final ControlledClock cClock)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> {
      if (speculatedTask.getAttempts().size() != 2) {
        cClock.tickMsec(250L);
        return false;
      }
      return true;
    }, 250, 60000);
  }

  public void waitForTAState(TaskAttempt attempt,
      TaskAttemptState finalState, final ControlledClock cClock)
      throws Exception {
    GenericTestUtils.waitFor(() -> {
      if (attempt.getReport().getTaskAttemptState() != finalState) {
        cClock.tickMsec(250L);
        return false;
      }
      return true;
    }, 250, 10000);
  }

  private void updateTaskProgress(EventHandler appEventHandler,
      TaskAttempt attempt, float newProgress) {
    TaskAttemptStatus status =
        createTaskAttemptStatus(attempt.getID(), newProgress,
            TaskAttemptState.RUNNING);
    TaskAttemptStatusUpdateEvent event =
        new TaskAttemptStatusUpdateEvent(attempt.getID(),
            new AtomicReference<>(status));
    appEventHandler.handle(event);
  }

}
