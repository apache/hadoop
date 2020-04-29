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

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;

import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;


import java.util.concurrent.ThreadLocalRandom;

import static org.apache.hadoop.metrics2.lib.Interns.info;

/**
 * Metric for Shuffle client.
 */
@SuppressWarnings("checkstyle:finalclass")
@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
@Metrics(name="ShuffleClientMetrics", context="mapred")
public class ShuffleClientMetrics {

  private static final MetricsInfo RECORD_INFO =
      info("ShuffleClientMetrics", "Metrics for Shuffle client");

  @Metric
  private MutableCounterInt numFailedFetches;
  @Metric
  private MutableCounterInt numSuccessFetches;
  @Metric
  private MutableCounterLong numBytes;
  @Metric
  private MutableGaugeInt numThreadsBusy;

  private final MetricsRegistry metricsRegistry =
      new MetricsRegistry(RECORD_INFO);

  private ShuffleClientMetrics() {
  }

  public static ShuffleClientMetrics create(
      TaskAttemptID reduceId,
      JobConf jobConf) {
    MetricsSystem ms = DefaultMetricsSystem.initialize("JobTracker");

    ShuffleClientMetrics shuffleClientMetrics = new ShuffleClientMetrics();
    shuffleClientMetrics.addTags(reduceId, jobConf);

    return ms.register("ShuffleClientMetrics-" +
        ThreadLocalRandom.current().nextInt(), null,
            shuffleClientMetrics);
  }

  public void inputBytes(long bytes) {
    numBytes.incr(bytes);
  }
  public void failedFetch() {
    numFailedFetches.incr();
  }
  public void successFetch() {
    numSuccessFetches.incr();
  }
  public void threadBusy() {
    numThreadsBusy.incr();
  }
  public void threadFree() {
    numThreadsBusy.decr();
  }

  private void addTags(TaskAttemptID reduceId, JobConf jobConf) {
    metricsRegistry.tag("user", "", jobConf.getUser())
        .tag("jobName", "", jobConf.getJobName())
        .tag("jobId", "", reduceId.getJobID().toString())
        .tag("taskId", "", reduceId.toString());
  }

  @VisibleForTesting
  MetricsRegistry getMetricsRegistry() {
    return metricsRegistry;
  }
}
