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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.federation.utils.FederationPoliciesTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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

    // simulate 20 active subclusters
    for (int i = 0; i < 20; i++) {
      SubClusterIdInfo sc = new SubClusterIdInfo(String.format("sc%02d", i));
      SubClusterInfo federationSubClusterInfo =
          SubClusterInfo.newInstance(sc.toId(), null, null, null, null, -1,
              SubClusterState.SC_RUNNING, -1, generateClusterMetricsInfo(i));
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

    FederationPoliciesTestUtil.initializePolicyContext(getPolicy(),
        getPolicyInfo(), getActiveSubclusters());

  }

  private String generateClusterMetricsInfo(int id) {

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

}
