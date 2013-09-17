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
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.source.JvmMetrics;

@Metrics(about="MR App Metrics", context="mapred")
public class MRAppMetrics {
  @Metric MutableCounterInt jobsSubmitted;
  @Metric MutableCounterInt jobsCompleted;
  @Metric MutableCounterInt jobsFailed;
  @Metric MutableCounterInt jobsKilled;
  @Metric MutableGaugeInt jobsPreparing;
  @Metric MutableGaugeInt jobsRunning;

  @Metric MutableCounterInt mapsLaunched;
  @Metric MutableCounterInt mapsCompleted;
  @Metric MutableCounterInt mapsFailed;
  @Metric MutableCounterInt mapsKilled;
  @Metric MutableGaugeInt mapsRunning;
  @Metric MutableGaugeInt mapsWaiting;

  @Metric MutableCounterInt reducesLaunched;
  @Metric MutableCounterInt reducesCompleted;
  @Metric MutableCounterInt reducesFailed;
  @Metric MutableCounterInt reducesKilled;
  @Metric MutableGaugeInt reducesRunning;
  @Metric MutableGaugeInt reducesWaiting;
  
  public static MRAppMetrics create() {
    return create(DefaultMetricsSystem.instance());
  }

  public static MRAppMetrics create(MetricsSystem ms) {
    JvmMetrics.initSingleton("MRAppMaster", null);
    return ms.register(new MRAppMetrics());
  }

  // potential instrumentation interface methods

  public void submittedJob(Job job) {
    jobsSubmitted.incr();
  }

  public void completedJob(Job job) {
    jobsCompleted.incr();
  }

  public void failedJob(Job job) {
    jobsFailed.incr();
  }

  public void killedJob(Job job) {
    jobsKilled.incr();
  }

  public void preparingJob(Job job) {
    jobsPreparing.incr();
  }

  public void endPreparingJob(Job job) {
    jobsPreparing.decr();
  }

  public void runningJob(Job job) {
    jobsRunning.incr();
  }

  public void endRunningJob(Job job) {
    jobsRunning.decr();
  }

  public void launchedTask(Task task) {
    switch (task.getType()) {
      case MAP:
        mapsLaunched.incr();
        break;
      case REDUCE:
        reducesLaunched.incr();
        break;
    }
    endWaitingTask(task);
  }

  public void completedTask(Task task) {
    switch (task.getType()) {
      case MAP:
        mapsCompleted.incr();
        break;
      case REDUCE:
        reducesCompleted.incr();
        break;
    }
  }

  public void failedTask(Task task) {
    switch (task.getType()) {
      case MAP:
        mapsFailed.incr();
        break;
      case REDUCE:
        reducesFailed.incr();
        break;
    }
  }

  public void killedTask(Task task) {
    switch (task.getType()) {
      case MAP:
        mapsKilled.incr();
        break;
      case REDUCE:
        reducesKilled.incr();
        break;
    }
  }

  public void runningTask(Task task) {
    switch (task.getType()) {
      case MAP:
        mapsRunning.incr();
        break;
      case REDUCE:
        reducesRunning.incr();
        break;
    }
  }

  public void endRunningTask(Task task) {
    switch (task.getType()) {
      case MAP:
        mapsRunning.decr();
        break;
      case REDUCE:
        reducesRunning.decr();
        break;
    }
  }

  public void waitingTask(Task task) {
    switch (task.getType()) {
      case MAP:
        mapsWaiting.incr();
        break;
      case REDUCE:
        reducesWaiting.incr();
    }
  }

  public void endWaitingTask(Task task) {
    switch (task.getType()) {
      case MAP:
        mapsWaiting.decr();
        break;
      case REDUCE:
        reducesWaiting.decr();
        break;
    }
  }
}