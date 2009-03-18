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
package org.apache.hadoop.mapred;

import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.jvm.JvmMetrics;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingInt;

class JobTrackerMetricsInst extends JobTrackerInstrumentation implements Updater {
  private final MetricsRecord metricsRecord;

  private int numMapTasksLaunched = 0;
  private int numMapTasksCompleted = 0;
  private int numMapTasksFailed = 0;
  private int numReduceTasksLaunched = 0;
  private int numReduceTasksCompleted = 0;
  private int numReduceTasksFailed = 0;
  private int numJobsSubmitted = 0;
  private int numJobsCompleted = 0;
  private int numWaitingTasks = 0;
    
  public JobTrackerMetricsInst(JobTracker tracker, JobConf conf) {
    super(tracker, conf);
    String sessionId = conf.getSessionId();
    // Initiate JVM Metrics
    JvmMetrics.init("JobTracker", sessionId);
    // Create a record for map-reduce metrics
    MetricsContext context = MetricsUtil.getContext("mapred");
    metricsRecord = MetricsUtil.createRecord(context, "jobtracker");
    metricsRecord.setTag("sessionId", sessionId);
    context.registerUpdater(this);
  }
    
  /**
   * Since this object is a registered updater, this method will be called
   * periodically, e.g. every 5 seconds.
   */
  public void doUpdates(MetricsContext unused) {
    synchronized (this) {
      metricsRecord.incrMetric("maps_launched", numMapTasksLaunched);
      metricsRecord.incrMetric("maps_completed", numMapTasksCompleted);
      metricsRecord.incrMetric("maps_failed", numMapTasksFailed);
      metricsRecord.incrMetric("reduces_launched", numReduceTasksLaunched);
      metricsRecord.incrMetric("reduces_completed", numReduceTasksCompleted);
      metricsRecord.incrMetric("reduces_failed", numReduceTasksFailed);
      metricsRecord.incrMetric("jobs_submitted", numJobsSubmitted);
      metricsRecord.incrMetric("jobs_completed", numJobsCompleted);
      metricsRecord.incrMetric("waiting_tasks", numWaitingTasks);

      numMapTasksLaunched = 0;
      numMapTasksCompleted = 0;
      numMapTasksFailed = 0;
      numReduceTasksLaunched = 0;
      numReduceTasksCompleted = 0;
      numReduceTasksFailed = 0;
      numWaitingTasks = 0;
      numJobsSubmitted = 0;
      numJobsCompleted = 0;
    }
    metricsRecord.update();

    if (tracker != null) {
      for (JobInProgress jip : tracker.getRunningJobs()) {
        jip.updateMetrics();
      }
    }
  }

  @Override
  public synchronized void launchMap(TaskAttemptID taskAttemptID) {
    ++numMapTasksLaunched;
    decWaiting(taskAttemptID.getJobID(), 1);
  }

  @Override
  public synchronized void completeMap(TaskAttemptID taskAttemptID) {
    ++numMapTasksCompleted;
  }

  @Override
  public synchronized void failedMap(TaskAttemptID taskAttemptID) {
    ++numMapTasksFailed;
    addWaiting(taskAttemptID.getJobID(), 1);
  }

  @Override
  public synchronized void launchReduce(TaskAttemptID taskAttemptID) {
    ++numReduceTasksLaunched;
    decWaiting(taskAttemptID.getJobID(), 1);
  }

  @Override
  public synchronized void completeReduce(TaskAttemptID taskAttemptID) {
    ++numReduceTasksCompleted;
  }

  @Override
  public synchronized void failedReduce(TaskAttemptID taskAttemptID) {
    ++numReduceTasksFailed;
    addWaiting(taskAttemptID.getJobID(), 1);
  }

  @Override
  public synchronized void submitJob(JobConf conf, JobID id) {
    ++numJobsSubmitted;
  }

  @Override
  public synchronized void completeJob(JobConf conf, JobID id) {
    ++numJobsCompleted;
  }

  @Override
  public synchronized void addWaiting(JobID id, int tasks) {
    numWaitingTasks += tasks;
  }

  @Override
  public synchronized void decWaiting(JobID id, int tasks) {
    numWaitingTasks -= tasks;
  }
}
