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

package org.apache.hadoop.yarn.server.federation.policies.manager;

import org.apache.hadoop.yarn.server.federation.policies.amrmproxy.LocalityMulticastAMRMProxyPolicy;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.policies.router.LocalityRouterPolicy;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Simple test of {@link WeightedLocalityPolicyManager}.
 */
public class TestWeightedLocalityPolicyManager extends
    BasePolicyManagerTest {

  private WeightedPolicyInfo policyInfo;

  @Before
  public void setup() {
    // configure a policy

    wfp = new WeightedLocalityPolicyManager();
    wfp.setQueue("queue1");
    SubClusterId sc1 = SubClusterId.newInstance("sc1");
    SubClusterId sc2 = SubClusterId.newInstance("sc2");
    policyInfo = new WeightedPolicyInfo();

    Map<SubClusterIdInfo, Float> routerWeights = new HashMap<>();
    routerWeights.put(new SubClusterIdInfo(sc1), 0.2f);
    routerWeights.put(new SubClusterIdInfo(sc2), 0.8f);
    policyInfo.setRouterPolicyWeights(routerWeights);

    Map<SubClusterIdInfo, Float> amrmWeights = new HashMap<>();
    amrmWeights.put(new SubClusterIdInfo(sc1), 0.2f);
    amrmWeights.put(new SubClusterIdInfo(sc2), 0.8f);
    policyInfo.setAMRMPolicyWeights(amrmWeights);

    ((WeightedLocalityPolicyManager) wfp).setWeightedPolicyInfo(
        policyInfo);

    //set expected params that the base test class will use for tests
    expectedPolicyManager = WeightedLocalityPolicyManager.class;
    expectedAMRMProxyPolicy = LocalityMulticastAMRMProxyPolicy.class;
    expectedRouterPolicy = LocalityRouterPolicy.class;
  }

  @Test
  public void testPolicyInfoSetCorrectly() throws Exception {
    serializeAndDeserializePolicyManager(wfp, expectedPolicyManager,
                                         expectedAMRMProxyPolicy,
                                         expectedRouterPolicy);

    //check the policyInfo propagates through ser/der correctly
    Assert.assertEquals(((WeightedLocalityPolicyManager) wfp)
                            .getWeightedPolicyInfo(), policyInfo);
  }
}
