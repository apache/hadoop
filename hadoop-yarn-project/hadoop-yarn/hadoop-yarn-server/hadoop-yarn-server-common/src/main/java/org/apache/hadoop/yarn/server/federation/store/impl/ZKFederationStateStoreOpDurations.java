/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.yarn.server.federation.store.impl;

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
import org.apache.hadoop.metrics2.lib.MutableRate;

import static org.apache.hadoop.metrics2.lib.Interns.info;

@InterfaceAudience.Private
@InterfaceStability.Unstable
@Metrics(context="ZKFederationStateStore-op-durations")
public final class ZKFederationStateStoreOpDurations implements MetricsSource {

  @Metric("Duration for a add application homeSubcluster call")
  private MutableRate addAppHomeSubCluster;

  @Metric("Duration for a update application homeSubcluster call")
  private MutableRate updateAppHomeSubCluster;

  @Metric("Duration for a get application homeSubcluster call")
  private MutableRate getAppHomeSubCluster;

  @Metric("Duration for a get applications homeSubcluster call")
  private MutableRate getAppsHomeSubCluster;

  @Metric("Duration for a delete applications homeSubcluster call")
  private MutableRate deleteAppHomeSubCluster;

  @Metric("Duration for a register subCluster call")
  private MutableRate registerSubCluster;

  @Metric("Duration for a deregister subCluster call")
  private MutableRate deregisterSubCluster;

  @Metric("Duration for a subCluster Heartbeat call")
  private MutableRate subClusterHeartbeat;

  @Metric("Duration for a get SubCluster call")
  private MutableRate getSubCluster;

  @Metric("Duration for a get SubClusters call")
  private MutableRate getSubClusters;

  @Metric("Duration for a get PolicyConfiguration call")
  private MutableRate getPolicyConfiguration;

  @Metric("Duration for a set PolicyConfiguration call")
  private MutableRate setPolicyConfiguration;

  @Metric("Duration for a get PolicyConfigurations call")
  private MutableRate getPoliciesConfigurations;

  protected static final MetricsInfo RECORD_INFO =
      info("ZKFederationStateStoreOpDurations", "Durations of ZKFederationStateStore calls");

  private final MetricsRegistry registry;

  private static final ZKFederationStateStoreOpDurations INSTANCE =
      new ZKFederationStateStoreOpDurations();

  public static ZKFederationStateStoreOpDurations getInstance() {
    return INSTANCE;
  }

  private ZKFederationStateStoreOpDurations() {
    registry = new MetricsRegistry(RECORD_INFO);
    registry.tag(RECORD_INFO, "ZKFederationStateStoreOpDurations");

    MetricsSystem ms = DefaultMetricsSystem.instance();
    if (ms != null) {
      ms.register(RECORD_INFO.name(), RECORD_INFO.description(), this);
    }
  }

  @Override
  public synchronized void getMetrics(MetricsCollector collector, boolean all) {
    registry.snapshot(collector.addRecord(registry.info()), all);
  }

  public void addAppHomeSubClusterDuration(long endTime, long startTime) {
    addAppHomeSubCluster.add(endTime - startTime);
  }

  public void addUpdateAppHomeSubClusterDuration(long endTime, long startTime) {
    updateAppHomeSubCluster.add(endTime - startTime);
  }

  public void addGetAppHomeSubClusterDuration(long endTime, long startTime) {
    getAppHomeSubCluster.add(endTime - startTime);
  }

  public void addGetAppsHomeSubClusterDuration(long endTime, long startTime) {
    getAppsHomeSubCluster.add(endTime - startTime);
  }

  public void addDeleteAppHomeSubClusterDuration(long endTime, long startTime) {
    deleteAppHomeSubCluster.add(endTime - startTime);
  }

  public void addRegisterSubClusterDuration(long endTime, long startTime) {
    registerSubCluster.add(endTime - startTime);
  }

  public void addDeregisterSubClusterDuration(long endTime, long startTime) {
    deregisterSubCluster.add(endTime - startTime);
  }

  public void addSubClusterHeartbeatDuration(long endTime, long startTime) {
    subClusterHeartbeat.add(endTime - startTime);
  }

  public void addGetSubClusterDuration(long endTime, long startTime) {
    getSubCluster.add(endTime - startTime);
  }

  public void addGetSubClustersDuration(long endTime, long startTime) {
    getSubClusters.add(endTime - startTime);
  }

  public void addGetPolicyConfigurationDuration(long endTime, long startTime) {
    getPolicyConfiguration.add(endTime - startTime);
  }

  public void addSetPolicyConfigurationDuration(long endTime, long startTime) {
    setPolicyConfiguration.add(endTime - startTime);
  }

  public void addGetPoliciesConfigurationsDuration(long endTime, long startTime) {
    getPoliciesConfigurations.add(endTime - startTime);
  }
}
