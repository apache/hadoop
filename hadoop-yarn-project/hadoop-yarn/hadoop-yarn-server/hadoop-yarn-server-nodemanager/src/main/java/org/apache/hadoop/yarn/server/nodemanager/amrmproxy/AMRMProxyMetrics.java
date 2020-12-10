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

package org.apache.hadoop.yarn.server.nodemanager.amrmproxy;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableRate;

import static org.apache.hadoop.metrics2.lib.Interns.info;

@Metrics(about = "Metrics for AMRMProxy", context = "fedr")
public final class AMRMProxyMetrics {

  private static final MetricsInfo RECORD_INFO =
      info("AMRMProxyMetrics", "Metrics for the AMRMProxy");
  @Metric("# of failed applications start requests")
  private MutableGaugeLong failedAppStartRequests;
  @Metric("# of failed register AM requests")
  private MutableGaugeLong failedRegisterAMRequests;
  @Metric("# of failed finish AM requests")
  private MutableGaugeLong failedFinishAMRequests;
  @Metric("# of failed allocate requests ")
  private MutableGaugeLong failedAllocateRequests;
  @Metric("# of failed application recoveries")
  private MutableGaugeLong failedAppRecoveryCount;
  // Aggregate metrics are shared, and don't have to be looked up per call
  @Metric("Application start request latency(ms)")
  private MutableRate totalSucceededAppStartRequests;
  @Metric("Register application master latency(ms)")
  private MutableRate totalSucceededRegisterAMRequests;
  @Metric("Finish application master latency(ms)")
  private MutableRate totalSucceededFinishAMRequests;
  @Metric("Allocate latency(ms)")
  private MutableRate totalSucceededAllocateRequests;
  // Quantile latency in ms - this is needed for SLA (95%, 99%, etc)
  private MutableQuantiles applicationStartLatency;
  private MutableQuantiles registerAMLatency;
  private MutableQuantiles finishAMLatency;
  private MutableQuantiles allocateLatency;
  private static volatile AMRMProxyMetrics instance = null;
  private MetricsRegistry registry;

  private AMRMProxyMetrics() {
    registry = new MetricsRegistry(RECORD_INFO);
    registry.tag(RECORD_INFO, "AMRMProxy");

    applicationStartLatency = registry
        .newQuantiles("applicationStartLatency", "latency of app start", "ops",
            "latency", 10);
    registerAMLatency = registry
        .newQuantiles("registerAMLatency", "latency of register AM", "ops",
            "latency", 10);
    finishAMLatency = registry
        .newQuantiles("finishAMLatency", "latency of finish AM", "ops",
            "latency", 10);
    allocateLatency = registry
        .newQuantiles("allocateLatency", "latency of allocate", "ops",
            "latency", 10);
  }

  /**
   * Initialize the singleton instance.
   *
   * @return the singleton
   */
  public static AMRMProxyMetrics getMetrics() {
    synchronized (AMRMProxyMetrics.class) {
      if (instance == null) {
        instance = DefaultMetricsSystem.instance()
            .register("AMRMProxyMetrics", "Metrics for the Yarn AMRMProxy",
                new AMRMProxyMetrics());
      }
    }
    return instance;
  }

  @VisibleForTesting
  long getNumSucceededAppStartRequests() {
    return totalSucceededAppStartRequests.lastStat().numSamples();
  }

  @VisibleForTesting
  double getLatencySucceededAppStartRequests() {
    return totalSucceededAppStartRequests.lastStat().mean();
  }

  public void succeededAppStartRequests(long duration) {
    totalSucceededAppStartRequests.add(duration);
    applicationStartLatency.add(duration);
  }

  @VisibleForTesting
  long getNumSucceededRegisterAMRequests() {
    return totalSucceededRegisterAMRequests.lastStat().numSamples();
  }

  @VisibleForTesting
  double getLatencySucceededRegisterAMRequests() {
    return totalSucceededRegisterAMRequests.lastStat().mean();
  }

  public void succeededRegisterAMRequests(long duration) {
    totalSucceededRegisterAMRequests.add(duration);
    registerAMLatency.add(duration);
  }

  @VisibleForTesting
  long getNumSucceededFinishAMRequests() {
    return totalSucceededFinishAMRequests.lastStat().numSamples();
  }

  @VisibleForTesting
  double getLatencySucceededFinishAMRequests() {
    return totalSucceededFinishAMRequests.lastStat().mean();
  }

  public void succeededFinishAMRequests(long duration) {
    totalSucceededFinishAMRequests.add(duration);
    finishAMLatency.add(duration);
  }

  @VisibleForTesting
  long getNumSucceededAllocateRequests() {
    return totalSucceededAllocateRequests.lastStat().numSamples();
  }

  @VisibleForTesting
  double getLatencySucceededAllocateRequests() {
    return totalSucceededAllocateRequests.lastStat().mean();
  }

  public void succeededAllocateRequests(long duration) {
    totalSucceededAllocateRequests.add(duration);
    allocateLatency.add(duration);
  }

  long getFailedAppStartRequests() {
    return failedAppStartRequests.value();
  }

  public void incrFailedAppStartRequests() {
    failedAppStartRequests.incr();
  }

  long getFailedRegisterAMRequests() {
    return failedRegisterAMRequests.value();
  }

  public void incrFailedRegisterAMRequests() {
    failedRegisterAMRequests.incr();
  }

  long getFailedFinishAMRequests() {
    return failedFinishAMRequests.value();
  }

  public void incrFailedFinishAMRequests() {
    failedFinishAMRequests.incr();
  }

  long getFailedAllocateRequests() {
    return failedAllocateRequests.value();
  }

  public void incrFailedAllocateRequests() {
    failedAllocateRequests.incr();
  }

  long getFailedAppRecoveryCount() {
    return failedAppRecoveryCount.value();
  }

  public void incrFailedAppRecoveryCount() {
    failedAppRecoveryCount.incr();
  }
}
