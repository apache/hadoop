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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableRate;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.metrics2.lib.Interns.info;

/**
 * Metrics for capacity scheduler.
 */
@InterfaceAudience.Private
@Metrics(context="yarn")
public class CapacitySchedulerMetrics {

  private static AtomicBoolean isInitialized = new AtomicBoolean(false);

  private static final MetricsInfo RECORD_INFO =
      info("CapacitySchedulerMetrics",
          "Metrics for the Yarn Capacity Scheduler");

  @Metric("Scheduler allocate containers") MutableRate allocate;
  @Metric("Scheduler commit success") MutableRate commitSuccess;
  @Metric("Scheduler commit failure") MutableRate commitFailure;
  @Metric("Scheduler node update") MutableRate nodeUpdate;

  private static volatile CapacitySchedulerMetrics INSTANCE = null;
  private static MetricsRegistry registry;

  public static CapacitySchedulerMetrics getMetrics() {
    if(!isInitialized.get()){
      synchronized (CapacitySchedulerMetrics.class) {
        if(INSTANCE == null){
          INSTANCE = new CapacitySchedulerMetrics();
          registerMetrics();
          isInitialized.set(true);
        }
      }
    }
    return INSTANCE;
  }

  private static void registerMetrics() {
    registry = new MetricsRegistry(RECORD_INFO);
    registry.tag(RECORD_INFO, "ResourceManager");
    MetricsSystem ms = DefaultMetricsSystem.instance();
    if (ms != null) {
      ms.register("CapacitySchedulerMetrics",
          "Metrics for the Yarn Capacity Scheduler", INSTANCE);
    }
  }

  @VisibleForTesting
  public synchronized static void destroy() {
    isInitialized.set(false);
    INSTANCE = null;
    MetricsSystem ms = DefaultMetricsSystem.instance();
    if (ms != null) {
      ms.unregisterSource("CapacitySchedulerMetrics");
    }
  }

  public void addAllocate(long latency) {
    this.allocate.add(latency);
  }

  public void addCommitSuccess(long latency) {
    this.commitSuccess.add(latency);
  }

  public void addCommitFailure(long latency) {
    this.commitFailure.add(latency);
  }

  public void addNodeUpdate(long latency) {
    this.nodeUpdate.add(latency);
  }

  @VisibleForTesting
  public long getNumOfNodeUpdate() {
    return this.nodeUpdate.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumOfAllocates() {
    return this.allocate.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumOfCommitSuccess() {
    return this.commitSuccess.lastStat().numSamples();
  }
}
