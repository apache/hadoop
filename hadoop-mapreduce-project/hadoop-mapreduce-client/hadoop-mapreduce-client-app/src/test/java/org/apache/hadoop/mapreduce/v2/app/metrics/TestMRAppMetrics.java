/*
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
package org.apache.hadoop.mapreduce.v2.app.metrics;

import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;

import static org.apache.hadoop.test.MetricsAsserts.*;

import org.junit.Test;

import static org.mockito.Mockito.*;

public class TestMRAppMetrics {

  @Test public void testNames() {
    Job job = mock(Job.class);
    Task mapTask = mock(Task.class);
    when(mapTask.getType()).thenReturn(TaskType.MAP);
    Task reduceTask = mock(Task.class);
    when(reduceTask.getType()).thenReturn(TaskType.REDUCE);
    MRAppMetrics metrics = MRAppMetrics.create();

    metrics.submittedJob(job);
    metrics.waitingTask(mapTask);
    metrics.waitingTask(reduceTask);
    metrics.preparingJob(job);
    metrics.submittedJob(job);
    metrics.waitingTask(mapTask);
    metrics.waitingTask(reduceTask);
    metrics.preparingJob(job);
    metrics.submittedJob(job);
    metrics.waitingTask(mapTask);
    metrics.waitingTask(reduceTask);
    metrics.preparingJob(job);
    metrics.endPreparingJob(job);
    metrics.endPreparingJob(job);
    metrics.endPreparingJob(job);

    metrics.runningJob(job);
    metrics.launchedTask(mapTask);
    metrics.runningTask(mapTask);
    metrics.failedTask(mapTask);
    metrics.endWaitingTask(reduceTask);
    metrics.endRunningTask(mapTask);
    metrics.endRunningJob(job);
    metrics.failedJob(job);

    metrics.runningJob(job);
    metrics.launchedTask(mapTask);
    metrics.runningTask(mapTask);
    metrics.killedTask(mapTask);
    metrics.endWaitingTask(reduceTask);
    metrics.endRunningTask(mapTask);
    metrics.endRunningJob(job);
    metrics.killedJob(job);

    metrics.runningJob(job);
    metrics.launchedTask(mapTask);
    metrics.runningTask(mapTask);
    metrics.completedTask(mapTask);
    metrics.endRunningTask(mapTask);
    metrics.launchedTask(reduceTask);
    metrics.runningTask(reduceTask);
    metrics.completedTask(reduceTask);
    metrics.endRunningTask(reduceTask);
    metrics.endRunningJob(job);
    metrics.completedJob(job);

    checkMetrics(/*job*/3, 1, 1, 1, 0, 0,
                 /*map*/3, 1, 1, 1, 0, 0,
                 /*reduce*/1, 1, 0, 0, 0, 0);
  }

  private void checkMetrics(int jobsSubmitted, int jobsCompleted,
      int jobsFailed, int jobsKilled, int jobsPreparing, int jobsRunning,
      int mapsLaunched, int mapsCompleted, int mapsFailed, int mapsKilled,
      int mapsRunning, int mapsWaiting, int reducesLaunched,
      int reducesCompleted, int reducesFailed, int reducesKilled,
      int reducesRunning, int reducesWaiting) {
    MetricsRecordBuilder rb = getMetrics("MRAppMetrics");
    assertCounter("JobsSubmitted", jobsSubmitted, rb);
    assertCounter("JobsCompleted", jobsCompleted, rb);
    assertCounter("JobsFailed", jobsFailed, rb);
    assertCounter("JobsKilled", jobsKilled, rb);
    assertGauge("JobsPreparing", jobsPreparing, rb);
    assertGauge("JobsRunning", jobsRunning, rb);

    assertCounter("MapsLaunched", mapsLaunched, rb);
    assertCounter("MapsCompleted", mapsCompleted, rb);
    assertCounter("MapsFailed", mapsFailed, rb);
    assertCounter("MapsKilled", mapsKilled, rb);
    assertGauge("MapsRunning", mapsRunning, rb);
    assertGauge("MapsWaiting", mapsWaiting, rb);

    assertCounter("ReducesLaunched", reducesLaunched, rb);
    assertCounter("ReducesCompleted", reducesCompleted, rb);
    assertCounter("ReducesFailed", reducesFailed, rb);
    assertCounter("ReducesKilled", reducesKilled, rb);
    assertGauge("ReducesRunning", reducesRunning, rb);
    assertGauge("ReducesWaiting", reducesWaiting, rb);
  }
}
