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
package org.apache.hadoop.hdds.scm.container.placement.algorithms;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

/**
 * This class is for maintaining Topology aware container placement statistics.
 */
@Metrics(about="SCM Container Placement Metrics", context = "ozone")
public class SCMContainerPlacementMetrics implements MetricsSource {
  public static final String SOURCE_NAME =
      SCMContainerPlacementMetrics.class.getSimpleName();
  private static final MetricsInfo RECORD_INFO = Interns.info(SOURCE_NAME,
      "SCM Container Placement Metrics");
  private static MetricsRegistry registry;

  // total datanode allocation request count
  @Metric private MutableCounterLong datanodeRequestCount;
  // datanode allocation attempt count, including success, fallback and failed
  @Metric private MutableCounterLong datanodeChooseAttemptCount;
  // datanode successful allocation count
  @Metric private MutableCounterLong datanodeChooseSuccessCount;
  // datanode allocated with some allocation constrains compromised
  @Metric private MutableCounterLong datanodeChooseFallbackCount;

  public SCMContainerPlacementMetrics() {
  }

  public static SCMContainerPlacementMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    registry = new MetricsRegistry(RECORD_INFO);
    return ms.register(SOURCE_NAME, "SCM Container Placement Metrics",
        new SCMContainerPlacementMetrics());
  }

  public void incrDatanodeRequestCount(long count) {
    this.datanodeRequestCount.incr(count);
  }

  public void incrDatanodeChooseSuccessCount() {
    this.datanodeChooseSuccessCount.incr(1);
  }

  public void incrDatanodeChooseFallbackCount() {
    this.datanodeChooseFallbackCount.incr(1);
  }

  public void incrDatanodeChooseAttemptCount() {
    this.datanodeChooseAttemptCount.incr(1);
  }

  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  @VisibleForTesting
  public long getDatanodeRequestCount() {
    return this.datanodeRequestCount.value();
  }

  @VisibleForTesting
  public long getDatanodeChooseSuccessCount() {
    return this.datanodeChooseSuccessCount.value();
  }

  @VisibleForTesting
  public long getDatanodeChooseFallbackCount() {
    return this.datanodeChooseFallbackCount.value();
  }

  @VisibleForTesting
  public long getDatanodeChooseAttemptCount() {
    return this.datanodeChooseAttemptCount.value();
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    registry.snapshot(collector.addRecord(registry.info().name()), true);
  }
}