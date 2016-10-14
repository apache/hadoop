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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.BaseFederationPoliciesTest;
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
 * Simple test class for the {@link WeightedRandomRouterPolicy}. Generate large
 * number of randomized tests to check we are weighiting correctly even if
 * clusters go inactive.
 */
public class TestWeightedRandomRouterPolicy extends BaseFederationPoliciesTest {

  @Before
  public void setUp() throws Exception {
    setPolicy(new WeightedRandomRouterPolicy());
    setPolicyInfo(new WeightedPolicyInfo());
    Map<SubClusterIdInfo, Float> routerWeights = new HashMap<>();
    Map<SubClusterIdInfo, Float> amrmWeights = new HashMap<>();

    // simulate 20 subclusters with a 5% chance of being inactive
    for (int i = 0; i < 20; i++) {
      SubClusterIdInfo sc = new SubClusterIdInfo("sc" + i);
      // with 5% omit a subcluster
      if (getRand().nextFloat() < 0.95f) {
        SubClusterInfo sci = mock(SubClusterInfo.class);
        when(sci.getState()).thenReturn(SubClusterState.SC_RUNNING);
        when(sci.getSubClusterId()).thenReturn(sc.toId());
        getActiveSubclusters().put(sc.toId(), sci);
      }
      // 5% chance we omit one of the weights
      float weight = getRand().nextFloat();
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

  @Test
  public void testClusterChosenWithRightProbability() throws YarnException {

    Map<SubClusterId, AtomicLong> counter = new HashMap<>();
    for (SubClusterIdInfo id : getPolicyInfo().getRouterPolicyWeights()
        .keySet()) {
      counter.put(id.toId(), new AtomicLong(0));
    }

    float numberOfDraws = 1000000;

    for (float i = 0; i < numberOfDraws; i++) {
      SubClusterId chosenId = ((FederationRouterPolicy) getPolicy())
          .getHomeSubcluster(getApplicationSubmissionContext());
      counter.get(chosenId).incrementAndGet();
    }

    float totalActiveWeight = 0;
    for (SubClusterId id : getActiveSubclusters().keySet()) {
      SubClusterIdInfo idInfo = new SubClusterIdInfo(id);
      if (getPolicyInfo().getRouterPolicyWeights().containsKey(idInfo)) {
        totalActiveWeight +=
            getPolicyInfo().getRouterPolicyWeights().get(idInfo);
      }
    }

    for (Map.Entry<SubClusterId, AtomicLong> counterEntry : counter
        .entrySet()) {
      float expectedWeight = getPolicyInfo().getRouterPolicyWeights()
          .get(new SubClusterIdInfo(counterEntry.getKey())) / totalActiveWeight;
      float actualWeight = counterEntry.getValue().floatValue() / numberOfDraws;

      // make sure that the weights is respected among active subclusters
      // and no jobs are routed to inactive subclusters.
      if (getActiveSubclusters().containsKey(counterEntry.getKey())) {
        Assert.assertTrue(
            "Id " + counterEntry.getKey() + " Actual weight: " + actualWeight
                + " expected weight: " + expectedWeight,
            expectedWeight == 0 || (actualWeight / expectedWeight) < 1.1
                && (actualWeight / expectedWeight) > 0.9);
      } else {
        Assert
            .assertTrue(
                "Id " + counterEntry.getKey() + " Actual weight: "
                    + actualWeight + " expected weight: " + expectedWeight,
                actualWeight == 0);

      }
    }
  }
}
