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

package org.apache.hadoop.yarn.server.federation.policies.amrmproxy;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.AMRMClientRelayer;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.scheduler.ResourceRequestSet;
import org.apache.hadoop.yarn.server.scheduler.ResourceRequestSetKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used by {@link LocalityMulticastAMRMProxyPolicy} to re-balance the pending
 * asks among all sub-clusters.
 */
public class ContainerAsksBalancer implements Configurable {
  public static final Logger LOG =
      LoggerFactory.getLogger(ContainerAsksBalancer.class);

  private Configuration conf;

  // Holds the pending requests and allocation history of all sub-clusters
  private Map<SubClusterId, AMRMClientRelayer> clientRelayers;

  private long subclusterAskTimeOut;
  private Map<SubClusterId, Map<ExecutionType, Long>> lastRelaxCandidateCount;

  public ContainerAsksBalancer() {
    this.clientRelayers = new ConcurrentHashMap<>();
    this.lastRelaxCandidateCount = new ConcurrentHashMap<>();
  }

  @Override
  public void setConf(Configuration config) {
    this.conf = config;
    this.subclusterAskTimeOut = this.conf.getTimeDuration(
        YarnConfiguration.DOWNGRADE_TO_OTHER_SUBCLUSTER_INTERVAL,
        YarnConfiguration.DEFAULT_DOWNGRADE_TO_OTHER_SUBCLUSTER_INTERVAL,
        TimeUnit.MILLISECONDS);
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  public void shutdown() {
    this.lastRelaxCandidateCount.clear();
    this.clientRelayers.clear();
  }

  /**
   * Pass in the AMRMClientRelayer for a new sub-cluster.
   *
   * @param subClusterId sub-cluster id
   * @param relayer      the AMRMClientRelayer for this sub-cluster
   * @throws YarnException if fails
   */
  public void addAMRMClientRelayer(SubClusterId subClusterId,
      AMRMClientRelayer relayer) throws YarnException {
    if (this.clientRelayers.containsKey(subClusterId)) {
      LOG.warn("AMRMClientRelayer already exists for {} ", subClusterId);
    }
    this.clientRelayers.put(subClusterId, relayer);

    Map<ExecutionType, Long> map = new ConcurrentHashMap<>();
    for (ExecutionType type : ExecutionType.values()) {
      map.put(type, 0L);
    }
    this.lastRelaxCandidateCount.put(subClusterId, map);
  }

  /**
   * Modify the output from split-merge (AMRMProxyPolicy). Adding and removing
   * asks to balance the pending asks in all sub-clusters.
   */
  public void adjustAsks() {
    Map<ResourceRequestSetKey, ResourceRequestSet> pendingAsks =
        new HashMap<>();
    Map<ResourceRequestSetKey, Long> pendingTime = new HashMap<>();
    for (Entry<SubClusterId, AMRMClientRelayer> relayerEntry : this.clientRelayers
        .entrySet()) {
      SubClusterId scId = relayerEntry.getKey();

      pendingAsks.clear();
      pendingTime.clear();
      AMRMClientRelayer relayer = relayerEntry.getValue();
      relayer.gatherReadOnlyPendingAsksInfo(pendingAsks, pendingTime);

      Map<ExecutionType, Long> currentCandidateCount = new HashMap<>();
      for (Entry<ResourceRequestSetKey, Long> pendingTimeEntry : pendingTime
          .entrySet()) {
        if (pendingTimeEntry.getValue() < this.subclusterAskTimeOut) {
          continue;
        }
        ResourceRequestSetKey askKey = pendingTimeEntry.getKey();
        ResourceRequestSet askSet = pendingAsks.get(askKey);
        if (!askSet.isANYRelaxable()) {
          continue;
        }
        long value = askSet.getNumContainers();
        if (currentCandidateCount.containsKey(askKey.getExeType())) {
          value += currentCandidateCount.get(askKey.getExeType());
        }
        currentCandidateCount.put(askKey.getExeType(), value);
      }

      // Update the pending metrics for the sub-cluster
      updateRelaxCandidateMetrics(scId, currentCandidateCount);
    }
  }

  protected void updateRelaxCandidateMetrics(SubClusterId scId,
      Map<ExecutionType, Long> currentCandidateCount) {
    Map<ExecutionType, Long> lastValueMap =
        this.lastRelaxCandidateCount.get(scId);
    for (ExecutionType type : ExecutionType.values()) {
      long newValue = 0;
      if (currentCandidateCount.containsKey(type)) {
        newValue = currentCandidateCount.get(type);
      }
      long lastValue = lastValueMap.get(type);
      lastValueMap.put(type, newValue);
      LOG.debug("updating SCRelaxable {} asks in {} from {} to {}", type, scId,
          lastValue, newValue);
    }
  }
}