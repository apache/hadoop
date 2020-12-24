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
package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;

import static org.apache.hadoop.metrics2.lib.Interns.info;
import org.apache.hadoop.metrics2.lib.MutableRate;

/**
 * Class to capture the performance metrics of ZKRMStateStore.
 * This should be a singleton.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
@Metrics(context="ZKRMStateStore-op-durations")
public final class ZKRMStateStoreOpDurations implements MetricsSource {

  @Metric("Duration for a load state call")
  MutableRate loadStateCall;

  @Metric("Duration for a store application state call")
  MutableRate storeApplicationStateCall;

  @Metric("Duration for a update application state call")
  MutableRate updateApplicationStateCall;

  @Metric("Duration to handle a remove application state call")
  MutableRate removeApplicationStateCall;

  protected static final MetricsInfo RECORD_INFO =
      info("ZKRMStateStoreOpDurations", "Durations of ZKRMStateStore calls");

  private final MetricsRegistry registry;

  private static final ZKRMStateStoreOpDurations INSTANCE
      = new ZKRMStateStoreOpDurations();

  public static ZKRMStateStoreOpDurations getInstance() {
    return INSTANCE;
  }

  private ZKRMStateStoreOpDurations() {
    registry = new MetricsRegistry(RECORD_INFO);
    registry.tag(RECORD_INFO, "ZKRMStateStoreOpDurations");

    MetricsSystem ms = DefaultMetricsSystem.instance();
    if (ms != null) {
      ms.register(RECORD_INFO.name(), RECORD_INFO.description(), this);
    }
  }

  @Override
  public synchronized void getMetrics(MetricsCollector collector, boolean all) {
    registry.snapshot(collector.addRecord(registry.info()), all);
  }

  public void addLoadStateCallDuration(long value) {
    loadStateCall.add(value);
  }

  public void addStoreApplicationStateCallDuration(long value) {
    storeApplicationStateCall.add(value);
  }

  public void addUpdateApplicationStateCallDuration(long value) {
    updateApplicationStateCall.add(value);
  }

  public void addRemoveApplicationStateCallDuration(long value) {
    removeApplicationStateCall.add(value);
  }
}
