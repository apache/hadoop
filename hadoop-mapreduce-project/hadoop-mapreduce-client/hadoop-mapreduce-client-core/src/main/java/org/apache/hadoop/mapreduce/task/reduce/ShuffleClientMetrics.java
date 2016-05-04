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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;

import java.util.concurrent.ThreadLocalRandom;

@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
@Metrics(name="ShuffleClientMetrics", context="mapred")
public class ShuffleClientMetrics {

  @Metric
  private MutableCounterInt numFailedFetches;
  @Metric
  private MutableCounterInt numSuccessFetches;
  @Metric
  private MutableCounterLong numBytes;
  @Metric
  private MutableGaugeInt numThreadsBusy;

  private ShuffleClientMetrics() {
  }

  public static ShuffleClientMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.initialize("JobTracker");
    return ms.register("ShuffleClientMetrics-" +
        ThreadLocalRandom.current().nextInt(), null,
        new ShuffleClientMetrics());
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
}
