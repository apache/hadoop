/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.policies.router;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.ConfigurableFederationPolicy;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.federation.store.records.ReservationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.utils.FederationPoliciesTestUtil;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.when;

/**
 * Simple test class for the {@link LoadBasedRouterPolicy}. Test that the load
 * is properly considered for allocation.
 */
public class TestLoadBasedRouterPolicy extends BaseRouterPoliciesTest {

  @Before
  public void setUp() throws Exception {
    setPolicy(new LoadBasedRouterPolicy());
    setPolicyInfo(new WeightedPolicyInfo());
    Map<SubClusterIdInfo, Float> routerWeights = new HashMap<>();
    Map<SubClusterIdInfo, Float> amrmWeights = new HashMap<>();

    long now = Time.now();

    // simulate 20 active subclusters
    for (int i = 0; i < 20; i++) {
      SubClusterIdInfo sc = new SubClusterIdInfo(String.format("sc%02d", i));
      SubClusterInfo federationSubClusterInfo = SubClusterInfo.newInstance(
          sc.toId(), "dns1:80", "dns1:81", "dns1:82", "dns1:83",
          now - 1000, SubClusterState.SC_RUNNING, now - 2000, generateClusterMetricsInfo(i));
      getActiveSubclusters().put(sc.toId(), federationSubClusterInfo);
      float weight = getRand().nextInt(2);
      if (i == 5) {
        weight = 1.0f;
      }

      // 5% chance we omit one of the weights
      if (i <= 5 || getRand().nextFloat() > 0.05f) {
        routerWeights.put(sc, weight);
        amrmWeights.put(sc, weight);
      }
    }
    getPolicyInfo().setRouterPolicyWeights(routerWeights);
    getPolicyInfo().setAMRMPolicyWeights(amrmWeights);

    // initialize policy with context
    setupContext();
  }

  public String generateClusterMetricsInfo(int id) {

    long mem = 1024 * getRand().nextInt(277 * 100 - 1);
    // plant a best cluster
    if (id == 5) {
      mem = 1024 * 277 * 100;
    }
    String clusterMetrics =
        "{\"clusterMetrics\":{\"appsSubmitted\":65," + "\"appsCompleted\":64,"
            + "\"appsPending\":0,\"appsRunning\":0,\"appsFailed\":0,"
            + "\"appsKilled\":1,\"reservedMB\":0,\"availableMB\":" + mem + ","
            + "\"allocatedMB\":0,\"reservedVirtualCores\":0,"
            + "\"availableVirtualCores\":2216,\"allocatedVirtualCores\":0,"
            + "\"containersAllocated\":0,\"containersReserved\":0,"
            + "\"containersPending\":0,\"totalMB\":28364800,"
            + "\"totalVirtualCores\":2216,\"totalNodes\":278,\"lostNodes\":1,"
            + "\"unhealthyNodes\":0,\"decommissionedNodes\":0,"
            + "\"rebootedNodes\":0,\"activeNodes\":277}}\n";

    return clusterMetrics;

  }

  @Test
  public void testLoadIsRespected() throws YarnException {

    SubClusterId chosen = ((FederationRouterPolicy) getPolicy())
        .getHomeSubcluster(getApplicationSubmissionContext(), null);

    // check the "planted" best cluster is chosen
    Assert.assertEquals("sc05", chosen.getId());
  }

  @Test
  public void testIfNoSubclustersWithWeightOne() throws Exception {
    setPolicy(new LoadBasedRouterPolicy());
    setPolicyInfo(new WeightedPolicyInfo());
    Map<SubClusterIdInfo, Float> routerWeights = new HashMap<>();
    Map<SubClusterIdInfo, Float> amrmWeights = new HashMap<>();
    // update subcluster with weight 0
    SubClusterIdInfo sc = new SubClusterIdInfo(String.format("sc%02d", 0));
    SubClusterInfo federationSubClusterInfo = SubClusterInfo.newInstance(
        sc.toId(), null, null, null, null, -1, SubClusterState.SC_RUNNING, -1,
        generateClusterMetricsInfo(0));
    getActiveSubclusters().clear();
    getActiveSubclusters().put(sc.toId(), federationSubClusterInfo);
    routerWeights.put(sc, 0.0f);
    amrmWeights.put(sc, 0.0f);
    getPolicyInfo().setRouterPolicyWeights(routerWeights);
    getPolicyInfo().setAMRMPolicyWeights(amrmWeights);


    ConfigurableFederationPolicy policy = getPolicy();
    FederationPoliciesTestUtil.initializePolicyContext(policy,
        getPolicyInfo(), getActiveSubclusters());

    LambdaTestUtils.intercept(YarnException.class, "Zero Active Subcluster with weight 1.",
        () ->  ((FederationRouterPolicy) policy).
        getHomeSubcluster(getApplicationSubmissionContext(), null));
  }

  @Test
  public void testUpdateReservation() throws YarnException {
    long now = Time.now();
    ReservationSubmissionRequest resReq = getReservationSubmissionRequest();
    when(resReq.getQueue()).thenReturn("queue1");
    when(resReq.getReservationId()).thenReturn(ReservationId.newInstance(now, 1));

    // first we invoke a reservation placement
    FederationRouterPolicy routerPolicy = (FederationRouterPolicy) getPolicy();
    SubClusterId chosen = routerPolicy.getReservationHomeSubcluster(resReq);

    // add this to the store
    FederationStateStoreFacade facade =
        getFederationPolicyContext().getFederationStateStoreFacade();
    ReservationHomeSubCluster subCluster =
        ReservationHomeSubCluster.newInstance(resReq.getReservationId(), chosen);
    facade.addReservationHomeSubCluster(subCluster);

    // get all activeSubClusters
    Map<SubClusterId, SubClusterInfo> activeSubClusters = getActiveSubclusters();

    // Update ReservationHomeSubCluster
    // Cannot be randomly selected, SubCluster with Weight >= 1.0 needs to be selected
    WeightedPolicyInfo weightedPolicyInfo = this.getPolicyInfo();
    Map<SubClusterIdInfo, Float> routerPolicyWeights = weightedPolicyInfo.getRouterPolicyWeights();

    List<SubClusterId> subClusterIds = new ArrayList<>();
    for (Map.Entry<SubClusterIdInfo, Float> entry : routerPolicyWeights.entrySet()) {
      SubClusterIdInfo subClusterIdInfo = entry.getKey();
      Float subClusterWeight = entry.getValue();
      if (subClusterWeight >= 1.0) {
        subClusterIds.add(subClusterIdInfo.toId());
      }
    }

    SubClusterId chosen2 = subClusterIds.get(this.getRand().nextInt(subClusterIds.size()));
    ReservationHomeSubCluster subCluster2 =
        ReservationHomeSubCluster.newInstance(resReq.getReservationId(), chosen2);
    facade.updateReservationHomeSubCluster(subCluster2);

    // route an application that uses this app
    ApplicationSubmissionContext applicationSubmissionContext =
        ApplicationSubmissionContext.newInstance(
            ApplicationId.newInstance(now, 1), "app1", "queue1", Priority.newInstance(1),
                null, false, false, 1, null, null, false);

    applicationSubmissionContext.setReservationID(resReq.getReservationId());
    SubClusterId chosen3 = routerPolicy.getHomeSubcluster(
        applicationSubmissionContext, new ArrayList<>());

    Assert.assertEquals(chosen2, chosen3);
  }
}
