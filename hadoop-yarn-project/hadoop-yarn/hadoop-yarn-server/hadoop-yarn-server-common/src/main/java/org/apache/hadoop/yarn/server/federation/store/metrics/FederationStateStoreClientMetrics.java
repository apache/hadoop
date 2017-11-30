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

package org.apache.hadoop.yarn.server.federation.store.metrics;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsSource;
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

import com.google.common.annotations.VisibleForTesting;

/**
 * Performance metrics for FederationStateStore implementations.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
@Metrics(about = "Performance and usage metrics for Federation StateStore",
         context = "fedr")
public final class FederationStateStoreClientMetrics implements MetricsSource {
  public static final Logger LOG =
      LoggerFactory.getLogger(FederationStateStoreClientMetrics.class);

  private static final MetricsRegistry REGISTRY =
      new MetricsRegistry("FederationStateStoreClientMetrics");
  private final static Method[] STATESTORE_API_METHODS =
      FederationStateStore.class.getMethods();

  // Map method names to counter objects
  private static final Map<String, MutableCounterLong> API_TO_FAILED_CALLS =
      new HashMap<String, MutableCounterLong>();
  private static final Map<String, MutableRate> API_TO_SUCCESSFUL_CALLS =
      new HashMap<String, MutableRate>();

  // Provide quantile latency for each api call.
  private static final Map<String, MutableQuantiles> API_TO_QUANTILE_METRICS =
      new HashMap<String, MutableQuantiles>();

  // Error string templates for logging calls from methods not in
  // FederationStateStore API
  private static final String UNKOWN_FAIL_ERROR_MSG =
      "Not recording failed call for unknown FederationStateStore method {}";
  private static final String UNKNOWN_SUCCESS_ERROR_MSG =
      "Not recording successful call for unknown "
          + "FederationStateStore method {}";

  // Aggregate metrics are shared, and don't have to be looked up per call
  @Metric("Total number of successful calls and latency(ms)")
  private static MutableRate totalSucceededCalls;

  @Metric("Total number of failed StateStore calls")
  private static MutableCounterLong totalFailedCalls;

  // This after the static members are initialized, or the constructor will
  // throw a NullPointerException
  private static final FederationStateStoreClientMetrics S_INSTANCE =
      DefaultMetricsSystem.instance()
          .register(new FederationStateStoreClientMetrics());

  synchronized public static FederationStateStoreClientMetrics getInstance() {
    return S_INSTANCE;
  }

  private FederationStateStoreClientMetrics() {
    // Create the metrics for each method and put them into the map
    for (Method m : STATESTORE_API_METHODS) {
      String methodName = m.getName();
      LOG.debug("Registering Federation StateStore Client metrics for {}",
          methodName);

      // This metric only records the number of failed calls; it does not
      // capture latency information
      API_TO_FAILED_CALLS.put(methodName,
          REGISTRY.newCounter(methodName + "_numFailedCalls",
              "# failed calls to " + methodName, 0L));

      // This metric records both the number and average latency of successful
      // calls.
      API_TO_SUCCESSFUL_CALLS.put(methodName,
          REGISTRY.newRate(methodName + "_successfulCalls",
              "# successful calls and latency(ms) for" + methodName));

      // This metric records the quantile-based latency of each successful call,
      // re-sampled every 10 seconds.
      API_TO_QUANTILE_METRICS.put(methodName,
          REGISTRY.newQuantiles(methodName + "Latency",
              "Quantile latency (ms) for " + methodName, "ops", "latency", 10));
    }
  }

  public static void failedStateStoreCall() {
    String methodName =
        Thread.currentThread().getStackTrace()[2].getMethodName();
    MutableCounterLong methodMetric = API_TO_FAILED_CALLS.get(methodName);
    if (methodMetric == null) {
      LOG.error(UNKOWN_FAIL_ERROR_MSG, methodName);
      return;
    }

    totalFailedCalls.incr();
    methodMetric.incr();
  }

  public static void succeededStateStoreCall(long duration) {
    String methodName =
        Thread.currentThread().getStackTrace()[2].getMethodName();
    MutableRate methodMetric = API_TO_SUCCESSFUL_CALLS.get(methodName);
    MutableQuantiles methodQuantileMetric =
        API_TO_QUANTILE_METRICS.get(methodName);
    if (methodMetric == null || methodQuantileMetric == null) {
      LOG.error(UNKNOWN_SUCCESS_ERROR_MSG, methodName);
      return;
    }

    totalSucceededCalls.add(duration);
    methodMetric.add(duration);
    methodQuantileMetric.add(duration);
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    REGISTRY.snapshot(collector.addRecord(REGISTRY.info()), all);
  }

  // Getters for unit testing
  @VisibleForTesting
  static long getNumFailedCallsForMethod(String methodName) {
    return API_TO_FAILED_CALLS.get(methodName).value();
  }

  @VisibleForTesting
  static long getNumSucceessfulCallsForMethod(String methodName) {
    return API_TO_SUCCESSFUL_CALLS.get(methodName).lastStat().numSamples();
  }

  @VisibleForTesting
  static double getLatencySucceessfulCallsForMethod(String methodName) {
    return API_TO_SUCCESSFUL_CALLS.get(methodName).lastStat().mean();
  }

  @VisibleForTesting
  static long getNumFailedCalls() {
    return totalFailedCalls.value();
  }

  @VisibleForTesting
  static long getNumSucceededCalls() {
    return totalSucceededCalls.lastStat().numSamples();
  }

  @VisibleForTesting
  static double getLatencySucceededCalls() {
    return totalSucceededCalls.lastStat().mean();
  }
}
