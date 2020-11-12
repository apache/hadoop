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

package org.apache.hadoop.yarn.server.metrics;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.metrics2.lib.Interns.info;

/**
 * Metrics for Opportunistic Scheduler.
 */
@InterfaceAudience.Private
@Metrics(context="yarn")
public class OpportunisticSchedulerMetrics {
  // CHECKSTYLE:OFF:VisibilityModifier
  private static AtomicBoolean isInitialized = new AtomicBoolean(false);

  private static final MetricsInfo RECORD_INFO =
      info("OpportunisticSchedulerMetrics",
          "Metrics for the Yarn Opportunistic Scheduler");

  private static volatile OpportunisticSchedulerMetrics INSTANCE = null;
  private static MetricsRegistry registry;

  public static OpportunisticSchedulerMetrics getMetrics() {
    if(!isInitialized.get()){
      synchronized (OpportunisticSchedulerMetrics.class) {
        if(INSTANCE == null){
          INSTANCE = new OpportunisticSchedulerMetrics();
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
      ms.register("OpportunisticSchedulerMetrics",
          "Metrics for the Yarn Opportunistic Scheduler", INSTANCE);
    }
  }

  @Metric("# of allocated opportunistic containers")
  MutableGaugeInt allocatedOContainers;
  @Metric("Aggregate # of allocated opportunistic containers")
  MutableCounterLong aggregateOContainersAllocated;
  @Metric("Aggregate # of released opportunistic containers")
  MutableCounterLong aggregateOContainersReleased;

  @Metric("Aggregate # of allocated node-local opportunistic containers")
  MutableCounterLong aggregateNodeLocalOContainersAllocated;
  @Metric("Aggregate # of allocated rack-local opportunistic containers")
  MutableCounterLong aggregateRackLocalOContainersAllocated;
  @Metric("Aggregate # of allocated off-switch opportunistic containers")
  MutableCounterLong aggregateOffSwitchOContainersAllocated;

  @Metric("Aggregate latency for opportunistic container allocation")
  MutableQuantiles allocateLatencyOQuantiles;

  @VisibleForTesting
  public int getAllocatedContainers() {
    return allocatedOContainers.value();
  }

  @VisibleForTesting
  public long getAggregatedAllocatedContainers() {
    return aggregateOContainersAllocated.value();
  }

  @VisibleForTesting
  public long getAggregatedReleasedContainers() {
    return aggregateOContainersReleased.value();
  }

  @VisibleForTesting
  public long getAggregatedNodeLocalContainers() {
    return aggregateNodeLocalOContainersAllocated.value();
  }

  @VisibleForTesting
  public long getAggregatedRackLocalContainers() {
    return aggregateRackLocalOContainersAllocated.value();
  }

  @VisibleForTesting
  public long getAggregatedOffSwitchContainers() {
    return aggregateOffSwitchOContainersAllocated.value();
  }

  // Opportunistic Containers
  public void incrAllocatedOppContainers(int numContainers) {
    allocatedOContainers.incr(numContainers);
    aggregateOContainersAllocated.incr(numContainers);
  }

  public void incrReleasedOppContainers(int numContainers) {
    aggregateOContainersReleased.incr(numContainers);
    allocatedOContainers.decr(numContainers);
  }

  public void incrNodeLocalOppContainers() {
    aggregateNodeLocalOContainersAllocated.incr();
  }

  public void incrRackLocalOppContainers() {
    aggregateRackLocalOContainersAllocated.incr();
  }

  public void incrOffSwitchOppContainers() {
    aggregateOffSwitchOContainersAllocated.incr();
  }

  public void addAllocateOLatencyEntry(long latency) {
    allocateLatencyOQuantiles.add(latency);
  }
}
