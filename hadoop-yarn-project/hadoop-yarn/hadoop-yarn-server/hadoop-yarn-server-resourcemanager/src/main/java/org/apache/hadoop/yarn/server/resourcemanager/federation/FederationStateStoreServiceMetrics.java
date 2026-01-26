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
package org.apache.hadoop.yarn.server.resourcemanager.federation;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.metrics2.lib.Interns.info;

@Metrics(about = "Metrics for FederationStateStoreService", context = "fedr")
public final class FederationStateStoreServiceMetrics {

  public static final Logger LOG =
      LoggerFactory.getLogger(FederationStateStoreServiceMetrics.class);

  private static final MetricsInfo RECORD_INFO =
      info("FederationStateStoreServiceMetrics", "Metrics for the RM FederationStateStoreService");

  private static volatile FederationStateStoreServiceMetrics instance = null;
  private MetricsRegistry registry;

  private final static Method[] STATESTORE_API_METHODS = FederationStateStore.class.getMethods();

  // Map method names to counter objects
  private static final Map<String, MutableCounterLong> FAILED_CALLS = new HashMap<>();
  private static final Map<String, MutableRate> SUCCESSFUL_CALLS = new HashMap<>();
  // Provide quantile latency for each api call.
  private static final Map<String, MutableQuantiles> QUANTILE_METRICS = new HashMap<>();

  // Error string templates for logging calls from methods not in
  // FederationStateStore API
  private static final String UNKOWN_FAIL_ERROR_MSG =
      "Not recording failed call for unknown FederationStateStore method {}";
  private static final String UNKNOWN_SUCCESS_ERROR_MSG =
      "Not recording successful call for unknown FederationStateStore method {}";

  /**
   * Initialize the singleton instance.
   *
   * @return the singleton
   */
  public static FederationStateStoreServiceMetrics getMetrics() {
    synchronized (FederationStateStoreServiceMetrics.class) {
      if (instance == null) {
        instance = DefaultMetricsSystem.instance()
            .register(new FederationStateStoreServiceMetrics());
      }
    }
    return instance;
  }

  private FederationStateStoreServiceMetrics() {
    registry = new MetricsRegistry(RECORD_INFO);
    registry.tag(RECORD_INFO, "FederationStateStoreServiceMetrics");

    // Create the metrics for each method and put them into the map
    for (Method m : STATESTORE_API_METHODS) {
      String methodName = m.getName();
      LOG.debug("Registering Federation StateStore Service metrics for {}", methodName);

      // This metric only records the number of failed calls; it does not
      // capture latency information
      FAILED_CALLS.put(methodName, registry.newCounter(methodName + "NumFailedCalls",
          "# failed calls to " + methodName, 0L));

      // This metric records both the number and average latency of successful
      // calls.
      SUCCESSFUL_CALLS.put(methodName, registry.newRate(methodName + "SuccessfulCalls",
          "# successful calls and latency(ms) for" + methodName));

      // This metric records the quantile-based latency of each successful call,
      // re-sampled every 10 seconds.
      QUANTILE_METRICS.put(methodName, registry.newQuantiles(methodName + "Latency",
          "Quantile latency (ms) for " + methodName, "ops", "latency", 10));
    }
  }

  // Aggregate metrics are shared, and don't have to be looked up per call
  @Metric("Total number of successful calls and latency(ms)")
  private static MutableRate totalSucceededCalls;

  @Metric("Total number of failed StateStore calls")
  private static MutableCounterLong totalFailedCalls;

  public static void failedStateStoreServiceCall() {
    String methodName = Thread.currentThread().getStackTrace()[2].getMethodName();
    MutableCounterLong methodMetric = FAILED_CALLS.get(methodName);

    if (methodMetric == null) {
      LOG.error(UNKOWN_FAIL_ERROR_MSG, methodName);
      return;
    }

    totalFailedCalls.incr();
    methodMetric.incr();
  }

  public static void failedStateStoreServiceCall(String methodName) {
    MutableCounterLong methodMetric = FAILED_CALLS.get(methodName);
    if (methodMetric == null) {
      LOG.error(UNKOWN_FAIL_ERROR_MSG, methodName);
      return;
    }
    totalFailedCalls.incr();
    methodMetric.incr();
  }

  public static void succeededStateStoreServiceCall(long duration) {
    StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
    if (ArrayUtils.isNotEmpty(stackTraceElements) && stackTraceElements.length > 2) {
      String methodName = Thread.currentThread().getStackTrace()[2].getMethodName();
      if(SUCCESSFUL_CALLS.containsKey(methodName)) {
        succeededStateStoreServiceCall(methodName, duration);
      } else {
        LOG.error(UNKNOWN_SUCCESS_ERROR_MSG, methodName);
      }
    } else {
      LOG.error("stackTraceElements is empty or length < 2.");
    }
  }

  public static void succeededStateStoreServiceCall(String methodName, long duration) {
    if (SUCCESSFUL_CALLS.containsKey(methodName)) {
      MutableRate methodMetric = SUCCESSFUL_CALLS.get(methodName);
      MutableQuantiles methodQuantileMetric = QUANTILE_METRICS.get(methodName);
      if (methodMetric == null || methodQuantileMetric == null) {
        LOG.error(UNKNOWN_SUCCESS_ERROR_MSG, methodName);
        return;
      }
      totalSucceededCalls.add(duration);
      methodMetric.add(duration);
      methodQuantileMetric.add(duration);
    }
  }

  // Getters for unit testing
  @VisibleForTesting
  public static long getNumFailedCallsForMethod(String methodName) {
    return FAILED_CALLS.get(methodName).value();
  }

  @VisibleForTesting
  public static long getNumSucceessfulCallsForMethod(String methodName) {
    return SUCCESSFUL_CALLS.get(methodName).lastStat().numSamples();
  }

  @VisibleForTesting
  public static double getLatencySucceessfulCallsForMethod(String methodName) {
    return SUCCESSFUL_CALLS.get(methodName).lastStat().mean();
  }

  @VisibleForTesting
  public static long getNumFailedCalls() {
    return totalFailedCalls.value();
  }

  @VisibleForTesting
  public static long getNumSucceededCalls() {
    return totalSucceededCalls.lastStat().numSamples();
  }

  @VisibleForTesting
  public static double getLatencySucceededCalls() {
    return totalSucceededCalls.lastStat().mean();
  }
}
