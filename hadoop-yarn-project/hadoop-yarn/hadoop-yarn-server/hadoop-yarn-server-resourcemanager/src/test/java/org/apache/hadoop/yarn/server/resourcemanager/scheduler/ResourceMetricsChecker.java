/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.assertGauge;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricType.COUNTER_LONG;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricType.GAUGE_INT;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricType.GAUGE_LONG;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricsKey.AGGREGATE_CONTAINERS_ALLOCATED;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricsKey.AGGREGATE_CONTAINERS_RELEASED;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricsKey.ALLOCATED_CONTAINERS;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricsKey.ALLOCATED_MB;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricsKey.ALLOCATED_V_CORES;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricsKey.AVAILABLE_MB;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricsKey.AVAILABLE_V_CORES;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricsKey.PENDING_CONTAINERS;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricsKey.PENDING_MB;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricsKey.PENDING_V_CORES;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricsKey.RESERVED_CONTAINERS;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricsKey.RESERVED_MB;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricsKey.RESERVED_V_CORES;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricsKey.ALLOCATED_CUSTOM_RES1;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricsKey.ALLOCATED_CUSTOM_RES2;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricsKey.AVAILABLE_CUSTOM_RES1;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricsKey.AVAILABLE_CUSTOM_RES2;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricsKey.PENDING_CUSTOM_RES1;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricsKey.PENDING_CUSTOM_RES2;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricsKey.RESERVED_CUSTOM_RES1;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricsKey.RESERVED_CUSTOM_RES2;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricsKey.AGGREGATE_PREEMPTED_SECONDS_CUSTOM_RES1;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricsKey.AGGREGATE_PREEMPTED_SECONDS_CUSTOM_RES2;

final class ResourceMetricsChecker {
  private final static Logger LOG =
          LoggerFactory.getLogger(ResourceMetricsChecker.class);

  enum ResourceMetricType {
    GAUGE_INT, GAUGE_LONG, COUNTER_INT, COUNTER_LONG
  }

  private static final ResourceMetricsChecker INITIAL_MANDATORY_RES_CHECKER =
    new ResourceMetricsChecker().gaugeLong(ALLOCATED_MB, 0)
        .gaugeInt(ALLOCATED_V_CORES, 0).gaugeInt(ALLOCATED_CONTAINERS, 0)
        .counter(AGGREGATE_CONTAINERS_ALLOCATED, 0)
        .counter(AGGREGATE_CONTAINERS_RELEASED, 0).gaugeLong(AVAILABLE_MB, 0)
        .gaugeInt(AVAILABLE_V_CORES, 0).gaugeLong(PENDING_MB, 0)
        .gaugeInt(PENDING_V_CORES, 0).gaugeInt(PENDING_CONTAINERS, 0)
        .gaugeLong(RESERVED_MB, 0).gaugeInt(RESERVED_V_CORES, 0)
        .gaugeInt(RESERVED_CONTAINERS, 0);

  private static final ResourceMetricsChecker INITIAL_CHECKER =
    new ResourceMetricsChecker().gaugeLong(ALLOCATED_MB, 0)
        .gaugeInt(ALLOCATED_V_CORES, 0).gaugeInt(ALLOCATED_CONTAINERS, 0)
        .counter(AGGREGATE_CONTAINERS_ALLOCATED, 0)
        .counter(AGGREGATE_CONTAINERS_RELEASED, 0).gaugeLong(AVAILABLE_MB, 0)
        .gaugeInt(AVAILABLE_V_CORES, 0).gaugeLong(PENDING_MB, 0)
        .gaugeInt(PENDING_V_CORES, 0).gaugeInt(PENDING_CONTAINERS, 0)
        .gaugeLong(RESERVED_MB, 0).gaugeInt(RESERVED_V_CORES, 0)
        .gaugeInt(RESERVED_CONTAINERS, 0).gaugeLong(ALLOCATED_CUSTOM_RES1, 0)
        .gaugeLong(ALLOCATED_CUSTOM_RES2, 0).gaugeLong(AVAILABLE_CUSTOM_RES1, 0)
        .gaugeLong(AVAILABLE_CUSTOM_RES2, 0).gaugeLong(PENDING_CUSTOM_RES1, 0)
        .gaugeLong(PENDING_CUSTOM_RES2, 0).gaugeLong(RESERVED_CUSTOM_RES1, 0)
        .gaugeLong(RESERVED_CUSTOM_RES2, 0)
        .gaugeLong(AGGREGATE_PREEMPTED_SECONDS_CUSTOM_RES1, 0)
        .gaugeLong(AGGREGATE_PREEMPTED_SECONDS_CUSTOM_RES2, 0);



  enum ResourceMetricsKey {
    ALLOCATED_MB("AllocatedMB", GAUGE_LONG),
    ALLOCATED_V_CORES("AllocatedVCores", GAUGE_INT),
    ALLOCATED_CONTAINERS("AllocatedContainers", GAUGE_INT),
    AGGREGATE_CONTAINERS_ALLOCATED("AggregateContainersAllocated",
        COUNTER_LONG),
    AGGREGATE_CONTAINERS_RELEASED("AggregateContainersReleased",
        COUNTER_LONG),
    AVAILABLE_MB("AvailableMB", GAUGE_LONG),
    AVAILABLE_V_CORES("AvailableVCores", GAUGE_INT),
    PENDING_MB("PendingMB", GAUGE_LONG),
    PENDING_V_CORES("PendingVCores", GAUGE_INT),
    PENDING_CONTAINERS("PendingContainers", GAUGE_INT),
    RESERVED_MB("ReservedMB", GAUGE_LONG),
    RESERVED_V_CORES("ReservedVCores", GAUGE_INT),
    RESERVED_CONTAINERS("ReservedContainers", GAUGE_INT),
    AGGREGATE_VCORE_SECONDS_PREEMPTED(
        "AggregateVcoreSecondsPreempted", COUNTER_LONG),
    AGGREGATE_MEMORY_MB_SECONDS_PREEMPTED(
        "AggregateMemoryMBSecondsPreempted", COUNTER_LONG),
    ALLOCATED_CUSTOM_RES1("AllocatedResource.custom_res_1", GAUGE_LONG),
    ALLOCATED_CUSTOM_RES2("AllocatedResource.custom_res_2", GAUGE_LONG),
    AVAILABLE_CUSTOM_RES1("AvailableResource.custom_res_1", GAUGE_LONG),
    AVAILABLE_CUSTOM_RES2("AvailableResource.custom_res_2", GAUGE_LONG),
    PENDING_CUSTOM_RES1("PendingResource.custom_res_1",GAUGE_LONG),
    PENDING_CUSTOM_RES2("PendingResource.custom_res_2",GAUGE_LONG),
    RESERVED_CUSTOM_RES1("ReservedResource.custom_res_1",GAUGE_LONG),
    RESERVED_CUSTOM_RES2("ReservedResource.custom_res_2", GAUGE_LONG),
    AGGREGATE_PREEMPTED_SECONDS_CUSTOM_RES1("AggregatePreemptedSeconds.custom_res_1", GAUGE_LONG),
    AGGREGATE_PREEMPTED_SECONDS_CUSTOM_RES2("AggregatePreemptedSeconds.custom_res_2", GAUGE_LONG);


    private String value;
    private ResourceMetricType type;

    ResourceMetricsKey(String value, ResourceMetricType type) {
      this.value = value;
      this.type = type;
    }

    public String getValue() {
      return value;
    }

    public ResourceMetricType getType() {
      return type;
    }
  }

  private final Map<ResourceMetricsKey, Long> gaugesLong;
  private final Map<ResourceMetricsKey, Integer> gaugesInt;
  private final Map<ResourceMetricsKey, Long> counters;

  private ResourceMetricsChecker() {
    this.gaugesLong = Maps.newHashMap();
    this.gaugesInt = Maps.newHashMap();
    this.counters = Maps.newHashMap();
  }

  private ResourceMetricsChecker(ResourceMetricsChecker checker) {
    this.gaugesLong = Maps.newHashMap(checker.gaugesLong);
    this.gaugesInt = Maps.newHashMap(checker.gaugesInt);
    this.counters = Maps.newHashMap(checker.counters);
  }

  public static ResourceMetricsChecker createFromChecker(
          ResourceMetricsChecker checker) {
    return new ResourceMetricsChecker(checker);
  }

  public static ResourceMetricsChecker create() {
    return new ResourceMetricsChecker(INITIAL_CHECKER);
  }

  public static ResourceMetricsChecker createMandatoryResourceChecker() {
    return new ResourceMetricsChecker(INITIAL_MANDATORY_RES_CHECKER);
  }

  ResourceMetricsChecker gaugeLong(ResourceMetricsKey key, long value) {
    ensureTypeIsCorrect(key, GAUGE_LONG);
    gaugesLong.put(key, value);
    return this;
  }

  ResourceMetricsChecker gaugeInt(ResourceMetricsKey key, int value) {
    ensureTypeIsCorrect(key, GAUGE_INT);
    gaugesInt.put(key, value);
    return this;
  }

  ResourceMetricsChecker counter(ResourceMetricsKey key, long value) {
    ensureTypeIsCorrect(key, COUNTER_LONG);
    counters.put(key, value);
    return this;
  }

  private void ensureTypeIsCorrect(ResourceMetricsKey
      key, ResourceMetricType actualType) {
    if (key.type != actualType) {
      throw new IllegalStateException("Metrics type should be " + key.type
          + " instead of " + actualType + " for metrics: " + key.value);
    }
  }

  ResourceMetricsChecker checkAgainst(MetricsSource source) {
    if (source == null) {
      throw new IllegalStateException("MetricsSource should not be null!");
    }
    MetricsRecordBuilder recordBuilder = getMetrics(source);
    logAssertingMessage(source);

    for (Map.Entry<ResourceMetricsKey, Long> gauge : gaugesLong.entrySet()) {
      assertGauge(gauge.getKey().value, gauge.getValue(), recordBuilder);
    }

    for (Map.Entry<ResourceMetricsKey, Integer> gauge : gaugesInt.entrySet()) {
      assertGauge(gauge.getKey().value, gauge.getValue(), recordBuilder);
    }

    for (Map.Entry<ResourceMetricsKey, Long> counter : counters.entrySet()) {
      assertCounter(counter.getKey().value, counter.getValue(), recordBuilder);
    }
    return this;
  }

  private void logAssertingMessage(MetricsSource source) {
    String queueName = ((QueueMetrics) source).queueName;
    Map<String, QueueMetrics> users = ((QueueMetrics) source).users;

    if (LOG.isDebugEnabled()) {
      LOG.debug("Asserting Resource metrics.. QueueName: " + queueName
          + ", users: " + (users != null && !users.isEmpty() ? users : ""));
    }
  }
}
