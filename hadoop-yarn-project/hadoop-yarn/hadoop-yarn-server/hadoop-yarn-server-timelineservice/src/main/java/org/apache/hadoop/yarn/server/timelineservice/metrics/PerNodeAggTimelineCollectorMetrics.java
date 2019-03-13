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

package org.apache.hadoop.yarn.server.timelineservice.metrics;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.metrics2.lib.Interns.info;

/**
 * Metrics class for TimelineCollectorWebService
 * running on each NM.
 */
@Metrics(about = "Aggregated metrics of TimelineCollector's running on each NM",
    context = "timelineservice")
final public class PerNodeAggTimelineCollectorMetrics {

  private static final MetricsInfo METRICS_INFO =
      info("PerNodeAggTimelineCollectorMetrics",
      "Aggregated Metrics for TimelineCollector's running on each NM");
  private static AtomicBoolean isInitialized = new AtomicBoolean(false);
  private static PerNodeAggTimelineCollectorMetrics
      instance = null;

  @Metric(about = "PUT entities failure latency", valueName = "latency")
  private MutableQuantiles putEntitiesFailureLatency;
  @Metric(about = "PUT entities success latency", valueName = "latency")
  private MutableQuantiles putEntitiesSuccessLatency;

  @Metric(about = "async PUT entities failure latency", valueName = "latency")
  private MutableQuantiles asyncPutEntitiesFailureLatency;
  @Metric(about = "async PUT entities success latency", valueName = "latency")
  private MutableQuantiles asyncPutEntitiesSuccessLatency;

  private PerNodeAggTimelineCollectorMetrics() {
  }

  public static PerNodeAggTimelineCollectorMetrics getInstance() {
    if (!isInitialized.get()) {
      synchronized (PerNodeAggTimelineCollectorMetrics.class) {
        if (instance == null) {
          instance =
              DefaultMetricsSystem.instance().register(
                  METRICS_INFO.name(), METRICS_INFO.description(),
                  new PerNodeAggTimelineCollectorMetrics());
          isInitialized.set(true);
        }
      }
    }
    return instance;
  }

  public synchronized static void destroy() {
    isInitialized.set(false);
    instance = null;
  }

  @VisibleForTesting
  public MutableQuantiles getPutEntitiesSuccessLatency() {
    return putEntitiesSuccessLatency;
  }

  @VisibleForTesting
  public MutableQuantiles getPutEntitiesFailureLatency() {
    return putEntitiesFailureLatency;
  }

  @VisibleForTesting
  public MutableQuantiles getAsyncPutEntitiesSuccessLatency() {
    return asyncPutEntitiesSuccessLatency;
  }

  @VisibleForTesting
  public MutableQuantiles getAsyncPutEntitiesFailureLatency() {
    return asyncPutEntitiesFailureLatency;
  }

  public void addPutEntitiesLatency(
      long durationMs, boolean succeeded) {
    if (succeeded) {
      putEntitiesSuccessLatency.add(durationMs);
    } else {
      putEntitiesFailureLatency.add(durationMs);
    }
  }

  public void addAsyncPutEntitiesLatency(
      long durationMs, boolean succeeded) {
    if (succeeded) {
      asyncPutEntitiesSuccessLatency.add(durationMs);
    } else {
      asyncPutEntitiesFailureLatency.add(durationMs);
    }
  }
}