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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationsHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationsHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterInfoResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPoliciesConfigurationsRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPoliciesConfigurationsResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPolicyConfigurationResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SetSubClusterPolicyConfigurationResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterHeartbeatRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterHeartbeatResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.util.MonotonicClock;

/**
 * In-memory implementation of {@link FederationStateStore}.
 */
public class MemoryFederationStateStore implements FederationStateStore {

  private Map<SubClusterId, SubClusterInfo> membership;
  private Map<ApplicationId, SubClusterId> applications;
  private Map<String, SubClusterPolicyConfiguration> policies;

  private final MonotonicClock clock = new MonotonicClock();

  @Override
  public void init(Configuration conf) {
    membership = new ConcurrentHashMap<SubClusterId, SubClusterInfo>();
    applications = new ConcurrentHashMap<ApplicationId, SubClusterId>();
    policies = new ConcurrentHashMap<String, SubClusterPolicyConfiguration>();
  }

  @Override
  public void close() {
    membership = null;
    applications = null;
    policies = null;
  }

  @Override
  public SubClusterRegisterResponse registerSubCluster(
      SubClusterRegisterRequest request) throws YarnException {
    SubClusterInfo subClusterInfo = request.getSubClusterInfo();
    membership.put(subClusterInfo.getSubClusterId(), subClusterInfo);
    return SubClusterRegisterResponse.newInstance();
  }

  @Override
  public SubClusterDeregisterResponse deregisterSubCluster(
      SubClusterDeregisterRequest request) throws YarnException {
    SubClusterInfo subClusterInfo = membership.get(request.getSubClusterId());
    if (subClusterInfo == null) {
      throw new YarnException(
          "SubCluster " + request.getSubClusterId().toString() + " not found");
    } else {
      subClusterInfo.setState(request.getState());
    }

    return SubClusterDeregisterResponse.newInstance();
  }

  @Override
  public SubClusterHeartbeatResponse subClusterHeartbeat(
      SubClusterHeartbeatRequest request) throws YarnException {

    SubClusterId subClusterId = request.getSubClusterId();
    SubClusterInfo subClusterInfo = membership.get(subClusterId);

    if (subClusterInfo == null) {
      throw new YarnException("Subcluster " + subClusterId.toString()
          + " does not exist; cannot heartbeat");
    }

    subClusterInfo.setLastHeartBeat(clock.getTime());
    subClusterInfo.setState(request.getState());
    subClusterInfo.setCapability(request.getCapability());

    return SubClusterHeartbeatResponse.newInstance();
  }

  @Override
  public GetSubClusterInfoResponse getSubCluster(
      GetSubClusterInfoRequest request) throws YarnException {
    SubClusterId subClusterId = request.getSubClusterId();
    if (!membership.containsKey(subClusterId)) {
      throw new YarnException(
          "Subcluster " + subClusterId.toString() + " does not exist");
    }

    return GetSubClusterInfoResponse.newInstance(membership.get(subClusterId));
  }

  @Override
  public GetSubClustersInfoResponse getSubClusters(
      GetSubClustersInfoRequest request) throws YarnException {
    List<SubClusterInfo> result = new ArrayList<SubClusterInfo>();

    for (SubClusterInfo info : membership.values()) {
      if (!request.getFilterInactiveSubClusters()
          || info.getState().isActive()) {
        result.add(info);
      }
    }
    return GetSubClustersInfoResponse.newInstance(result);
  }

  // FederationApplicationHomeSubClusterStore methods

  @Override
  public AddApplicationHomeSubClusterResponse addApplicationHomeSubCluster(
      AddApplicationHomeSubClusterRequest request) throws YarnException {
    ApplicationId appId =
        request.getApplicationHomeSubCluster().getApplicationId();

    if (!applications.containsKey(appId)) {
      applications.put(appId,
          request.getApplicationHomeSubCluster().getHomeSubCluster());
    }

    return AddApplicationHomeSubClusterResponse
        .newInstance(applications.get(appId));
  }

  @Override
  public UpdateApplicationHomeSubClusterResponse updateApplicationHomeSubCluster(
      UpdateApplicationHomeSubClusterRequest request) throws YarnException {
    ApplicationId appId =
        request.getApplicationHomeSubCluster().getApplicationId();
    if (!applications.containsKey(appId)) {
      throw new YarnException("Application " + appId + " does not exist");
    }

    applications.put(appId,
        request.getApplicationHomeSubCluster().getHomeSubCluster());
    return UpdateApplicationHomeSubClusterResponse.newInstance();
  }

  @Override
  public GetApplicationHomeSubClusterResponse getApplicationHomeSubCluster(
      GetApplicationHomeSubClusterRequest request) throws YarnException {
    ApplicationId appId = request.getApplicationId();
    if (!applications.containsKey(appId)) {
      throw new YarnException("Application " + appId + " does not exist");
    }

    return GetApplicationHomeSubClusterResponse.newInstance(
        ApplicationHomeSubCluster.newInstance(appId, applications.get(appId)));
  }

  @Override
  public GetApplicationsHomeSubClusterResponse getApplicationsHomeSubCluster(
      GetApplicationsHomeSubClusterRequest request) throws YarnException {
    List<ApplicationHomeSubCluster> result =
        new ArrayList<ApplicationHomeSubCluster>();
    for (Entry<ApplicationId, SubClusterId> e : applications.entrySet()) {
      result
          .add(ApplicationHomeSubCluster.newInstance(e.getKey(), e.getValue()));
    }

    GetApplicationsHomeSubClusterResponse.newInstance(result);
    return GetApplicationsHomeSubClusterResponse.newInstance(result);
  }

  @Override
  public DeleteApplicationHomeSubClusterResponse deleteApplicationHomeSubCluster(
      DeleteApplicationHomeSubClusterRequest request) throws YarnException {
    ApplicationId appId = request.getApplicationId();
    if (!applications.containsKey(appId)) {
      throw new YarnException("Application " + appId + " does not exist");
    }

    applications.remove(appId);
    return DeleteApplicationHomeSubClusterResponse.newInstance();
  }

  @Override
  public GetSubClusterPolicyConfigurationResponse getPolicyConfiguration(
      GetSubClusterPolicyConfigurationRequest request) throws YarnException {
    String queue = request.getQueue();
    if (!policies.containsKey(queue)) {
      throw new YarnException("Policy for queue " + queue + " does not exist");
    }

    return GetSubClusterPolicyConfigurationResponse
        .newInstance(policies.get(queue));
  }

  @Override
  public SetSubClusterPolicyConfigurationResponse setPolicyConfiguration(
      SetSubClusterPolicyConfigurationRequest request) throws YarnException {
    policies.put(request.getPolicyConfiguration().getQueue(),
        request.getPolicyConfiguration());
    return SetSubClusterPolicyConfigurationResponse.newInstance();
  }

  @Override
  public GetSubClusterPoliciesConfigurationsResponse getPoliciesConfigurations(
      GetSubClusterPoliciesConfigurationsRequest request) throws YarnException {
    ArrayList<SubClusterPolicyConfiguration> result =
        new ArrayList<SubClusterPolicyConfiguration>();
    for (SubClusterPolicyConfiguration policy : policies.values()) {
      result.add(policy);
    }
    return GetSubClusterPoliciesConfigurationsResponse.newInstance(result);
  }

  @Override
  public Version getCurrentVersion() {
    return null;
  }

  @Override
  public Version loadVersion() {
    return null;
  }

}
