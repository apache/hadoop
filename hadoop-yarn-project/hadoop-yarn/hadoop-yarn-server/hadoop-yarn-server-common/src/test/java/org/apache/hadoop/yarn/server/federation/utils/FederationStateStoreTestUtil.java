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

package org.apache.hadoop.yarn.server.federation.utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPolicyConfigurationResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.util.MonotonicClock;

/**
 * Utility class for FederationStateStore unit tests.
 */
public class FederationStateStoreTestUtil {

  private static final MonotonicClock CLOCK = new MonotonicClock();

  public static final String SC_PREFIX = "SC-";
  public static final String Q_PREFIX = "queue-";
  public static final String POLICY_PREFIX = "policy-";
  public static final String INVALID = "dummy";

  private FederationStateStore stateStore;

  public FederationStateStoreTestUtil(FederationStateStore stateStore) {
    this.stateStore = stateStore;
  }

  private SubClusterInfo createSubClusterInfo(SubClusterId subClusterId) {

    String amRMAddress = "1.2.3.4:1";
    String clientRMAddress = "1.2.3.4:2";
    String rmAdminAddress = "1.2.3.4:3";
    String webAppAddress = "1.2.3.4:4";

    return SubClusterInfo.newInstance(subClusterId, amRMAddress,
        clientRMAddress, rmAdminAddress, webAppAddress,
        SubClusterState.SC_RUNNING, CLOCK.getTime(), "capability");
  }

  public void registerSubCluster(SubClusterId subClusterId)
      throws YarnException {

    SubClusterInfo subClusterInfo = createSubClusterInfo(subClusterId);
    stateStore.registerSubCluster(
        SubClusterRegisterRequest.newInstance(subClusterInfo));
  }

  public void registerSubClusters(int numSubClusters) throws YarnException {

    for (int i = 0; i < numSubClusters; i++) {
      registerSubCluster(SubClusterId.newInstance(SC_PREFIX + i));
    }
  }

  private void addApplicationHomeSC(ApplicationId appId,
      SubClusterId subClusterId) throws YarnException {
    ApplicationHomeSubCluster ahsc =
        ApplicationHomeSubCluster.newInstance(appId, subClusterId);
    AddApplicationHomeSubClusterRequest request =
        AddApplicationHomeSubClusterRequest.newInstance(ahsc);
    stateStore.addApplicationHomeSubCluster(request);
  }

  public void addAppsHomeSC(long clusterTs, int numApps) throws YarnException {
    for (int i = 0; i < numApps; i++) {
      addApplicationHomeSC(ApplicationId.newInstance(clusterTs, i),
          SubClusterId.newInstance(SC_PREFIX + i));
    }
  }

  public List<SubClusterId> getAllSubClusterIds(
      boolean filterInactiveSubclusters) throws YarnException {

    List<SubClusterInfo> infos = stateStore
        .getSubClusters(
            GetSubClustersInfoRequest.newInstance(filterInactiveSubclusters))
        .getSubClusters();
    List<SubClusterId> ids = new ArrayList<>();
    for (SubClusterInfo s : infos) {
      ids.add(s.getSubClusterId());
    }

    return ids;
  }

  private SubClusterPolicyConfiguration createSCPolicyConf(String queueName,
      String policyType) {
    return SubClusterPolicyConfiguration.newInstance(queueName, policyType,
        ByteBuffer.allocate(1));
  }

  private void setPolicyConf(String queue, String policyType)
      throws YarnException {
    SetSubClusterPolicyConfigurationRequest request =
        SetSubClusterPolicyConfigurationRequest
            .newInstance(createSCPolicyConf(queue, policyType));
    stateStore.setPolicyConfiguration(request);
  }

  public void addPolicyConfigs(int numQueues) throws YarnException {

    for (int i = 0; i < numQueues; i++) {
      setPolicyConf(Q_PREFIX + i, POLICY_PREFIX + i);
    }
  }

  public SubClusterInfo querySubClusterInfo(SubClusterId subClusterId)
      throws YarnException {
    GetSubClusterInfoRequest request =
        GetSubClusterInfoRequest.newInstance(subClusterId);
    return stateStore.getSubCluster(request).getSubClusterInfo();
  }

  public SubClusterId queryApplicationHomeSC(ApplicationId appId)
      throws YarnException {
    GetApplicationHomeSubClusterRequest request =
        GetApplicationHomeSubClusterRequest.newInstance(appId);

    GetApplicationHomeSubClusterResponse response =
        stateStore.getApplicationHomeSubCluster(request);

    return response.getApplicationHomeSubCluster().getHomeSubCluster();
  }

  public SubClusterPolicyConfiguration queryPolicyConfiguration(String queue)
      throws YarnException {
    GetSubClusterPolicyConfigurationRequest request =
        GetSubClusterPolicyConfigurationRequest.newInstance(queue);

    GetSubClusterPolicyConfigurationResponse result =
        stateStore.getPolicyConfiguration(request);
    return result.getPolicyConfiguration();
  }

  public void deregisterAllSubClusters() throws YarnException {
    for (SubClusterId sc : getAllSubClusterIds(true)) {
      deRegisterSubCluster(sc);
    }
  }

  private void deRegisterSubCluster(SubClusterId subClusterId)
      throws YarnException {
    stateStore.deregisterSubCluster(SubClusterDeregisterRequest
        .newInstance(subClusterId, SubClusterState.SC_UNREGISTERED));
  }

}
