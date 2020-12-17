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

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.metrics2.lib.Interns.info;

/**
 * Metrics class for TimelineReader.
 */
@Metrics(about = "Metrics for timeline reader", context = "timelineservice")
public class TimelineReaderMetrics {

  private final static MetricsInfo METRICS_INFO = info("TimelineReaderMetrics",
      "Metrics for TimelineReader");
  private static AtomicBoolean isInitialized = new AtomicBoolean(false);
  private static TimelineReaderMetrics instance = null;

  @Metric(about = "GET entities failure latency", valueName = "latency")
  private MutableQuantiles getEntitiesFailureLatency;
  @Metric(about = "GET entities success latency", valueName = "latency")
  private MutableQuantiles getEntitiesSuccessLatency;

  @Metric(about = "GET entity types failure latency", valueName = "latency")
  private MutableQuantiles getEntityTypesFailureLatency;
  @Metric(about = "GET entity types success latency", valueName = "latency")
  private MutableQuantiles getEntityTypesSuccessLatency;

  @VisibleForTesting
  protected TimelineReaderMetrics() {
  }

  public static TimelineReaderMetrics getInstance() {
    if (!isInitialized.get()) {
      synchronized (TimelineReaderMetrics.class) {
        if (instance == null) {
          instance =
              DefaultMetricsSystem.initialize("TimelineService").register(
                  METRICS_INFO.name(), METRICS_INFO.description(),
                  new TimelineReaderMetrics());
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
  public MutableQuantiles getGetEntitiesSuccessLatency() {
    return getEntitiesSuccessLatency;
  }

  @VisibleForTesting
  public MutableQuantiles getGetEntitiesFailureLatency() {
    return getEntitiesFailureLatency;
  }

  @VisibleForTesting
  public MutableQuantiles getGetEntityTypesSuccessLatency() {
    return getEntityTypesSuccessLatency;
  }

  @VisibleForTesting
  public MutableQuantiles getGetEntityTypesFailureLatency() {
    return getEntityTypesFailureLatency;
  }

  public void addGetEntitiesLatency(
      long durationMs, boolean succeeded) {
    if (succeeded) {
      getEntitiesSuccessLatency.add(durationMs);
    } else {
      getEntitiesFailureLatency.add(durationMs);
    }
  }

  public void addGetEntityTypesLatency(
      long durationMs, boolean succeeded) {
    if (succeeded) {
      getEntityTypesSuccessLatency.add(durationMs);
    } else {
      getEntityTypesFailureLatency.add(durationMs);
    }
  }
}