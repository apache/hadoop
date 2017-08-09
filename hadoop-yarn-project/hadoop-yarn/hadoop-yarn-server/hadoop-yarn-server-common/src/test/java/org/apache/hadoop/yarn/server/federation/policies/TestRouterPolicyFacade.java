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

package org.apache.hadoop.yarn.server.federation.policies;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.policies.manager.PriorityBroadcastPolicyManager;
import org.apache.hadoop.yarn.server.federation.policies.manager.UniformBroadcastPolicyManager;
import org.apache.hadoop.yarn.server.federation.policies.router.PriorityRouterPolicy;
import org.apache.hadoop.yarn.server.federation.policies.router.UniformRandomRouterPolicy;
import org.apache.hadoop.yarn.server.federation.resolver.SubClusterResolver;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.impl.MemoryFederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.SetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.utils.FederationPoliciesTestUtil;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Simple test of {@link RouterPolicyFacade}.
 */
public class TestRouterPolicyFacade {

  private RouterPolicyFacade routerFacade;
  private List<SubClusterId> subClusterIds;
  private FederationStateStore store;
  private String queue1 = "queue1";
  private String defQueueKey = YarnConfiguration.DEFAULT_FEDERATION_POLICY_KEY;

  @Before
  public void setup() throws YarnException {

    // setting up a store and its facade (with caching off)
    FederationStateStoreFacade fedFacade =
        FederationStateStoreFacade.getInstance();
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.FEDERATION_CACHE_TIME_TO_LIVE_SECS, "0");
    store = new MemoryFederationStateStore();
    store.init(conf);
    fedFacade.reinitialize(store, conf);

    FederationStateStoreTestUtil storeTestUtil =
        new FederationStateStoreTestUtil(store);
    storeTestUtil.registerSubClusters(10);

    subClusterIds = storeTestUtil.getAllSubClusterIds(true);
    store.setPolicyConfiguration(SetSubClusterPolicyConfigurationRequest
        .newInstance(getUniformPolicy(queue1)));

    SubClusterResolver resolver = FederationPoliciesTestUtil.initResolver();
    routerFacade = new RouterPolicyFacade(new YarnConfiguration(), fedFacade,
        resolver, subClusterIds.get(0));
  }

  @Test
  public void testConfigurationUpdate() throws YarnException {

    // in this test we see what happens when the configuration is changed
    // between calls. We achieve this by changing what is in the store.

    ApplicationSubmissionContext applicationSubmissionContext =
        mock(ApplicationSubmissionContext.class);
    when(applicationSubmissionContext.getQueue()).thenReturn(queue1);

    // first call runs using standard UniformRandomRouterPolicy
    SubClusterId chosen =
        routerFacade.getHomeSubcluster(applicationSubmissionContext, null);
    Assert.assertTrue(subClusterIds.contains(chosen));
    Assert.assertTrue(routerFacade.globalPolicyMap
        .get(queue1) instanceof UniformRandomRouterPolicy);

    // then the operator changes how queue1 is routed setting it to
    // PriorityRouterPolicy with weights favoring the first subcluster in
    // subClusterIds.
    store.setPolicyConfiguration(SetSubClusterPolicyConfigurationRequest
        .newInstance(getPriorityPolicy(queue1)));

    // second call is routed by new policy PriorityRouterPolicy
    chosen = routerFacade.getHomeSubcluster(applicationSubmissionContext, null);
    Assert.assertTrue(chosen.equals(subClusterIds.get(0)));
    Assert.assertTrue(routerFacade.globalPolicyMap
        .get(queue1) instanceof PriorityRouterPolicy);
  }

  @Test
  public void testGetHomeSubcluster() throws YarnException {

    ApplicationSubmissionContext applicationSubmissionContext =
        mock(ApplicationSubmissionContext.class);
    when(applicationSubmissionContext.getQueue()).thenReturn(queue1);

    // the facade only contains the fallback behavior
    Assert.assertTrue(routerFacade.globalPolicyMap.containsKey(defQueueKey)
        && routerFacade.globalPolicyMap.size() == 1);

    // when invoked it returns the expected SubClusterId.
    SubClusterId chosen =
        routerFacade.getHomeSubcluster(applicationSubmissionContext, null);
    Assert.assertTrue(subClusterIds.contains(chosen));

    // now the caching of policies must have added an entry for this queue
    Assert.assertTrue(routerFacade.globalPolicyMap.size() == 2);

    // after the facade is used the policyMap contains the expected policy type.
    Assert.assertTrue(routerFacade.globalPolicyMap
        .get(queue1) instanceof UniformRandomRouterPolicy);

    // the facade is again empty after reset
    routerFacade.reset();
    // the facade only contains the fallback behavior
    Assert.assertTrue(routerFacade.globalPolicyMap.containsKey(defQueueKey)
        && routerFacade.globalPolicyMap.size() == 1);

  }

  @Test
  public void testFallbacks() throws YarnException {

    // this tests the behavior of the system when the queue requested is
    // not configured (or null) and there is no default policy configured
    // for DEFAULT_FEDERATION_POLICY_KEY (*). This is our second line of
    // defense.

    ApplicationSubmissionContext applicationSubmissionContext =
        mock(ApplicationSubmissionContext.class);

    // The facade answers also for non-initialized policies (using the
    // defaultPolicy)
    String uninitQueue = "non-initialized-queue";
    when(applicationSubmissionContext.getQueue()).thenReturn(uninitQueue);
    SubClusterId chosen =
        routerFacade.getHomeSubcluster(applicationSubmissionContext, null);
    Assert.assertTrue(subClusterIds.contains(chosen));
    Assert.assertFalse(routerFacade.globalPolicyMap.containsKey(uninitQueue));

    // empty string
    when(applicationSubmissionContext.getQueue()).thenReturn("");
    chosen = routerFacade.getHomeSubcluster(applicationSubmissionContext, null);
    Assert.assertTrue(subClusterIds.contains(chosen));
    Assert.assertFalse(routerFacade.globalPolicyMap.containsKey(uninitQueue));

    // null queue also falls back to default
    when(applicationSubmissionContext.getQueue()).thenReturn(null);
    chosen = routerFacade.getHomeSubcluster(applicationSubmissionContext, null);
    Assert.assertTrue(subClusterIds.contains(chosen));
    Assert.assertFalse(routerFacade.globalPolicyMap.containsKey(uninitQueue));

  }

  public static SubClusterPolicyConfiguration getUniformPolicy(String queue)
      throws FederationPolicyInitializationException {

    // we go through standard lifecycle instantiating a policyManager and
    // configuring it and serializing it to a conf.
    UniformBroadcastPolicyManager wfp = new UniformBroadcastPolicyManager();
    wfp.setQueue(queue);

    SubClusterPolicyConfiguration fpc = wfp.serializeConf();

    return fpc;
  }

  public SubClusterPolicyConfiguration getPriorityPolicy(String queue)
      throws FederationPolicyInitializationException {

    // we go through standard lifecycle instantiating a policyManager and
    // configuring it and serializing it to a conf.
    PriorityBroadcastPolicyManager wfp = new PriorityBroadcastPolicyManager();

    // equal weight to all subcluster
    Map<SubClusterIdInfo, Float> routerWeights = new HashMap<>();
    for (SubClusterId s : subClusterIds) {
      routerWeights.put(new SubClusterIdInfo(s), 0.9f / subClusterIds.size());
    }

    // beside the first one who gets more weight
    SubClusterIdInfo favorite = new SubClusterIdInfo((subClusterIds.get(0)));
    routerWeights.put(favorite, (0.1f + 0.9f / subClusterIds.size()));

    WeightedPolicyInfo policyInfo = new WeightedPolicyInfo();
    policyInfo.setRouterPolicyWeights(routerWeights);
    wfp.setWeightedPolicyInfo(policyInfo);
    wfp.setQueue(queue);

    // serializeConf it in a context
    SubClusterPolicyConfiguration fpc = wfp.serializeConf();

    return fpc;
  }

}
