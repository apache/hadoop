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

class JobTrackerMetricsInst extends JobTrackerInstrumentation implements Updater {
  private MetricsRecord metricsRecord = null;
  int numMapTasksLaunched = 0;
  int numMapTasksCompleted = 0;
  int numReduceTasksLaunched = 0;
  int numReduceTasksCompleted = 0;
  private int numJobsSubmitted = 0;
  private int numJobsCompleted = 0;
    
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
      metricsRecord.incrMetric("reduces_launched", numReduceTasksLaunched);
      metricsRecord.incrMetric("reduces_completed", numReduceTasksCompleted);
      metricsRecord.incrMetric("jobs_submitted", numJobsSubmitted);
      metricsRecord.incrMetric("jobs_completed", numJobsCompleted);
            
      numMapTasksLaunched = 0;
      numMapTasksCompleted = 0;
      numReduceTasksLaunched = 0;
      numReduceTasksCompleted = 0;
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

  public synchronized void launchMap(TaskAttemptID taskAttemptID) {
    ++numMapTasksLaunched;
  }
    
  public synchronized void completeMap(TaskAttemptID taskAttemptID) {
    ++numMapTasksCompleted;
  }
    
  public synchronized void launchReduce(TaskAttemptID taskAttemptID) {
    ++numReduceTasksLaunched;
  }
    
  public synchronized void completeReduce(TaskAttemptID taskAttemptID) {
    ++numReduceTasksCompleted;
  }
    
  public synchronized void submitJob(JobConf conf, JobID id) {
    ++numJobsSubmitted;
  }
    
  public synchronized void completeJob(JobConf conf, JobID id) {
    ++numJobsCompleted;
  }
}
