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
package org.apache.hadoop.mapreduce.task.reduce;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

@Metrics(context="mapred")
class ShuffleClientMetrics {

  final MetricsRegistry registry = new MetricsRegistry("shuffleInput");

  @Metric MutableCounterInt failedFetches;
  @Metric MutableCounterInt successFetches;
  @Metric MutableCounterLong inputBytes;

  private int numThreadsBusy = 0;
  private final int numCopiers;

  ShuffleClientMetrics(TaskAttemptID reduceId, JobConf jobConf) {
    this.numCopiers = jobConf.getInt(MRJobConfig.SHUFFLE_PARALLEL_COPIES, 5);
    registry.tag("User", "User name", jobConf.getUser())
            .tag("JobName", "Job name", jobConf.getJobName())
            .tag("JobId", "Job ID", reduceId.getJobID().toString())
            .tag("TaskId", "Task ID", reduceId.toString())
            .tag("SessionId", "Session ID", jobConf.getSessionId());
    DefaultMetricsSystem.instance().register(this);
  }
  public void inputBytes(long numBytes) {
    inputBytes.incr(numBytes);
  }
  public synchronized void failedFetch() {
    failedFetches.incr();
  }
  public synchronized void successFetch() {
    successFetches.incr();
  }
  public synchronized void threadBusy() {
    ++numThreadsBusy;
  }
  public synchronized void threadFree() {
    --numThreadsBusy;
  }
  @Metric synchronized float getFetchersBusyPercent() {
    return numCopiers == 0 ? 0f : 100f * numThreadsBusy / numCopiers;
  }
}
