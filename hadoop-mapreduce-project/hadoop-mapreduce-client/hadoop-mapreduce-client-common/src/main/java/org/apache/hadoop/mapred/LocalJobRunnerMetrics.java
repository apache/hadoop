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
package org.apache.hadoop.mapred;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;

import java.util.concurrent.ThreadLocalRandom;

@Metrics(name="LocalJobRunnerMetrics", context="mapred")
final class LocalJobRunnerMetrics {

  @Metric
  private MutableCounterInt numMapTasksLaunched;
  @Metric
  private MutableCounterInt numMapTasksCompleted;
  @Metric
  private MutableCounterInt numReduceTasksLaunched;
  @Metric
  private MutableGaugeInt numReduceTasksCompleted;

  private LocalJobRunnerMetrics() {
  }

  public static LocalJobRunnerMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.initialize("JobTracker");
    return ms.register("LocalJobRunnerMetrics-" +
            ThreadLocalRandom.current().nextInt(), null,
        new LocalJobRunnerMetrics());
  }

  public synchronized void launchMap(TaskAttemptID taskAttemptID) {
    numMapTasksLaunched.incr();
  }

  public void completeMap(TaskAttemptID taskAttemptID) {
    numMapTasksCompleted.incr();
  }

  public synchronized void launchReduce(TaskAttemptID taskAttemptID) {
    numReduceTasksLaunched.incr();
  }

  public void completeReduce(TaskAttemptID taskAttemptID) {
    numReduceTasksCompleted.incr();
  }
}
