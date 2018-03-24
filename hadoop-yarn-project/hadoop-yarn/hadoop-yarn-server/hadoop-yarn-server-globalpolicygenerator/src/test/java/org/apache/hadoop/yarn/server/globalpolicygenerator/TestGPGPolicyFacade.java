/**
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

package org.apache.hadoop.yarn.server.globalpolicygenerator;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.manager.FederationPolicyManager;
import org.apache.hadoop.yarn.server.federation.policies.manager.WeightedLocalityPolicyManager;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.impl.MemoryFederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPolicyConfigurationResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Unit test for GPG Policy Facade.
 */
public class TestGPGPolicyFacade {

  private Configuration conf;
  private FederationStateStore stateStore;
  private FederationStateStoreFacade facade =
      FederationStateStoreFacade.getInstance();
  private GPGPolicyFacade policyFacade;

  private Set<SubClusterId> subClusterIds;

  private SubClusterPolicyConfiguration testConf;

  private static final String TEST_QUEUE = "test-queue";

  public TestGPGPolicyFacade() {
    conf = new Configuration();
    conf.setInt(YarnConfiguration.FEDERATION_CACHE_TIME_TO_LIVE_SECS, 0);
    subClusterIds = new HashSet<>();
    subClusterIds.add(SubClusterId.newInstance("sc0"));
    subClusterIds.add(SubClusterId.newInstance("sc1"));
    subClusterIds.add(SubClusterId.newInstance("sc2"));
  }

  @Before
  public void setUp() throws IOException, YarnException {
    stateStore = new MemoryFederationStateStore();
    stateStore.init(conf);
    facade.reinitialize(stateStore, conf);
    policyFacade = new GPGPolicyFacade(facade, conf);
    WeightedLocalityPolicyManager manager =
        new WeightedLocalityPolicyManager();
    // Add a test policy for test queue
    manager.setQueue(TEST_QUEUE);
    manager.getWeightedPolicyInfo().setAMRMPolicyWeights(
        GPGUtils.createUniformWeights(subClusterIds));
    manager.getWeightedPolicyInfo().setRouterPolicyWeights(
        GPGUtils.createUniformWeights(subClusterIds));
    testConf = manager.serializeConf();
    stateStore.setPolicyConfiguration(SetSubClusterPolicyConfigurationRequest
        .newInstance(testConf));
  }

  @After
  public void tearDown() throws Exception {
    stateStore.close();
    stateStore = null;
  }

  @Test
  public void testGetPolicy() throws YarnException {
    WeightedLocalityPolicyManager manager =
        (WeightedLocalityPolicyManager) policyFacade
            .getPolicyManager(TEST_QUEUE);
    Assert.assertEquals(testConf, manager.serializeConf());
  }

  /**
   * Test that new policies are written into the state store.
   */
  @Test
  public void testSetNewPolicy() throws YarnException {
    WeightedLocalityPolicyManager manager =
        new WeightedLocalityPolicyManager();
    manager.setQueue(TEST_QUEUE + 0);
    manager.getWeightedPolicyInfo().setAMRMPolicyWeights(
        GPGUtils.createUniformWeights(subClusterIds));
    manager.getWeightedPolicyInfo().setRouterPolicyWeights(
        GPGUtils.createUniformWeights(subClusterIds));
    SubClusterPolicyConfiguration policyConf = manager.serializeConf();
    policyFacade.setPolicyManager(manager);

    manager =
        (WeightedLocalityPolicyManager) policyFacade
            .getPolicyManager(TEST_QUEUE + 0);
    Assert.assertEquals(policyConf, manager.serializeConf());
  }

  /**
   * Test that overwriting policies are updated in the state store.
   */
  @Test
  public void testOverwritePolicy() throws YarnException {
    subClusterIds.add(SubClusterId.newInstance("sc3"));
    WeightedLocalityPolicyManager manager =
        new WeightedLocalityPolicyManager();
    manager.setQueue(TEST_QUEUE);
    manager.getWeightedPolicyInfo().setAMRMPolicyWeights(
        GPGUtils.createUniformWeights(subClusterIds));
    manager.getWeightedPolicyInfo().setRouterPolicyWeights(
        GPGUtils.createUniformWeights(subClusterIds));
    SubClusterPolicyConfiguration policyConf = manager.serializeConf();
    policyFacade.setPolicyManager(manager);

    manager =
        (WeightedLocalityPolicyManager) policyFacade
            .getPolicyManager(TEST_QUEUE);
    Assert.assertEquals(policyConf, manager.serializeConf());
  }

  /**
   * Test that the write through cache works.
   */
  @Test
  public void testWriteCache() throws YarnException {
    stateStore = mock(MemoryFederationStateStore.class);
    facade.reinitialize(stateStore, conf);
    when(stateStore.getPolicyConfiguration(Matchers.any(
        GetSubClusterPolicyConfigurationRequest.class))).thenReturn(
        GetSubClusterPolicyConfigurationResponse.newInstance(testConf));
    policyFacade = new GPGPolicyFacade(facade, conf);

    // Query once to fill the cache
    FederationPolicyManager manager = policyFacade.getPolicyManager(TEST_QUEUE);
    // State store should be contacted once
    verify(stateStore, times(1)).getPolicyConfiguration(
        Matchers.any(GetSubClusterPolicyConfigurationRequest.class));

    // If we set the same policy, the state store should be untouched
    policyFacade.setPolicyManager(manager);
    verify(stateStore, times(0)).setPolicyConfiguration(
        Matchers.any(SetSubClusterPolicyConfigurationRequest.class));
  }

  /**
   * Test that when read only is enabled, the state store is not changed.
   */
  @Test
  public void testReadOnly() throws YarnException {
    conf.setBoolean(YarnConfiguration.GPG_POLICY_GENERATOR_READONLY, true);
    stateStore = mock(MemoryFederationStateStore.class);
    facade.reinitialize(stateStore, conf);
    when(stateStore.getPolicyConfiguration(Matchers.any(
        GetSubClusterPolicyConfigurationRequest.class))).thenReturn(
        GetSubClusterPolicyConfigurationResponse.newInstance(testConf));
    policyFacade = new GPGPolicyFacade(facade, conf);

    // If we set a policy, the state store should be untouched
    WeightedLocalityPolicyManager manager =
        new WeightedLocalityPolicyManager();
    // Add a test policy for test queue
    manager.setQueue(TEST_QUEUE);
    manager.getWeightedPolicyInfo().setAMRMPolicyWeights(
        GPGUtils.createUniformWeights(subClusterIds));
    manager.getWeightedPolicyInfo().setRouterPolicyWeights(
        GPGUtils.createUniformWeights(subClusterIds));
    policyFacade.setPolicyManager(manager);
    verify(stateStore, times(0)).setPolicyConfiguration(
        Matchers.any(SetSubClusterPolicyConfigurationRequest.class));
  }

}
