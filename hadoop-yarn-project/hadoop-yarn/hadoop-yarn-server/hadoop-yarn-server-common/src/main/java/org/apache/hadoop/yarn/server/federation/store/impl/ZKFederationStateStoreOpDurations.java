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

  @Metric("Duration for a add reservation homeSubCluster call")
  private MutableRate addReservationHomeSubCluster;

  @Metric("Duration for a get reservation homeSubCluster call")
  private MutableRate getReservationHomeSubCluster;

  @Metric("Duration for a get reservations homeSubCluster call")
  private MutableRate getReservationsHomeSubCluster;

  @Metric("Duration for a delete reservation homeSubCluster call")
  private MutableRate deleteReservationHomeSubCluster;

  @Metric("Duration for a update reservation homeSubCluster call")
  private MutableRate updateReservationHomeSubCluster;

  @Metric("Duration for a store new master key call")
  private MutableRate storeNewMasterKey;

  @Metric("Duration for a remove new master key call")
  private MutableRate removeStoredMasterKey;

  @Metric("Duration for a get master key by delegation key call")
  private MutableRate getMasterKeyByDelegationKey;

  @Metric("Duration for a store new token call")
  private MutableRate storeNewToken;

  @Metric("Duration for a update stored token call")
  private MutableRate updateStoredToken;

  @Metric("Duration for a remove stored token call")
  private MutableRate removeStoredToken;

  @Metric("Duration for a get token by router store token call")
  private MutableRate getTokenByRouterStoreToken;

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

  public void addAppHomeSubClusterDuration(long startTime, long endTime) {
    addAppHomeSubCluster.add(endTime - startTime);
  }

  public void addUpdateAppHomeSubClusterDuration(long startTime, long endTime) {
    updateAppHomeSubCluster.add(endTime - startTime);
  }

  public void addGetAppHomeSubClusterDuration(long startTime, long endTime) {
    getAppHomeSubCluster.add(endTime - startTime);
  }

  public void addGetAppsHomeSubClusterDuration(long startTime, long endTime) {
    getAppsHomeSubCluster.add(endTime - startTime);
  }

  public void addDeleteAppHomeSubClusterDuration(long startTime, long endTime) {
    deleteAppHomeSubCluster.add(endTime - startTime);
  }

  public void addRegisterSubClusterDuration(long startTime, long endTime) {
    registerSubCluster.add(endTime - startTime);
  }

  public void addDeregisterSubClusterDuration(long startTime, long endTime) {
    deregisterSubCluster.add(endTime - startTime);
  }

  public void addSubClusterHeartbeatDuration(long startTime, long endTime) {
    subClusterHeartbeat.add(endTime - startTime);
  }

  public void addGetSubClusterDuration(long startTime, long endTime) {
    getSubCluster.add(endTime - startTime);
  }

  public void addGetSubClustersDuration(long startTime, long endTime) {
    getSubClusters.add(endTime - startTime);
  }

  public void addGetPolicyConfigurationDuration(long startTime, long endTime) {
    getPolicyConfiguration.add(endTime - startTime);
  }

  public void addSetPolicyConfigurationDuration(long startTime, long endTime) {
    setPolicyConfiguration.add(endTime - startTime);
  }

  public void addGetPoliciesConfigurationsDuration(long startTime, long endTime) {
    getPoliciesConfigurations.add(endTime - startTime);
  }

  public void addReservationHomeSubClusterDuration(long startTime, long endTime) {
    addReservationHomeSubCluster.add(endTime - startTime);
  }

  public void addGetReservationHomeSubClusterDuration(long startTime, long endTime) {
    getReservationHomeSubCluster.add(endTime - startTime);
  }

  public void addGetReservationsHomeSubClusterDuration(long startTime, long endTime) {
    getReservationsHomeSubCluster.add(endTime - startTime);
  }

  public void addDeleteReservationHomeSubClusterDuration(long startTime, long endTime) {
    deleteReservationHomeSubCluster.add(endTime - startTime);
  }

  public void addUpdateReservationHomeSubClusterDuration(long startTime, long endTime) {
    updateReservationHomeSubCluster.add(endTime - startTime);
  }

  public void addStoreNewMasterKeyDuration(long startTime, long endTime) {
    storeNewMasterKey.add(endTime - startTime);
  }

  public void removeStoredMasterKeyDuration(long startTime, long endTime) {
    removeStoredMasterKey.add(endTime - startTime);
  }

  public void getMasterKeyByDelegationKeyDuration(long startTime, long endTime) {
    getMasterKeyByDelegationKey.add(endTime - startTime);
  }

  public void getStoreNewTokenDuration(long startTime, long endTime) {
    storeNewToken.add(endTime - startTime);
  }

  public void updateStoredTokenDuration(long startTime, long endTime) {
    updateStoredToken.add(endTime - startTime);
  }

  public void removeStoredTokenDuration(long startTime, long endTime) {
    removeStoredToken.add(endTime - startTime);
  }

  public void getTokenByRouterStoreTokenDuration(long startTime, long endTime) {
    getTokenByRouterStoreToken.add(endTime - startTime);
  }
}
