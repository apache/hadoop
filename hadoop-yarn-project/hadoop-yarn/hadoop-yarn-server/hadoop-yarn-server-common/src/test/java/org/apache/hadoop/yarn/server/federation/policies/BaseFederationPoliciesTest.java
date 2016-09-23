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

package org.apache.hadoop.yarn.server.federation.policies;

import static org.mockito.Mockito.mock;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.NoActiveSubclustersException;
import org.apache.hadoop.yarn.server.federation.policies.router.FederationRouterPolicy;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.utils.FederationPoliciesTestUtil;
import org.junit.Test;

/**
 * Base class for policies tests, tests for common reinitialization cases.
 */
public abstract class BaseFederationPoliciesTest {

  private ConfigurableFederationPolicy policy;
  private WeightedPolicyInfo policyInfo;
  private Map<SubClusterId, SubClusterInfo> activeSubclusters = new HashMap<>();
  private FederationPolicyInitializationContext federationPolicyContext;
  private ApplicationSubmissionContext applicationSubmissionContext =
      mock(ApplicationSubmissionContext.class);
  private Random rand = new Random();

  @Test
  public void testReinitilialize() throws YarnException {
    FederationPolicyInitializationContext fpc =
        new FederationPolicyInitializationContext();
    ByteBuffer buf = getPolicyInfo().toByteBuffer();
    fpc.setSubClusterPolicyConfiguration(SubClusterPolicyConfiguration
        .newInstance("queue1", getPolicy().getClass().getCanonicalName(), buf));
    fpc.setFederationSubclusterResolver(
        FederationPoliciesTestUtil.initResolver());
    fpc.setFederationStateStoreFacade(FederationPoliciesTestUtil.initFacade());
    getPolicy().reinitialize(fpc);
  }

  @Test(expected = FederationPolicyInitializationException.class)
  public void testReinitilializeBad1() throws YarnException {
    getPolicy().reinitialize(null);
  }

  @Test(expected = FederationPolicyInitializationException.class)
  public void testReinitilializeBad2() throws YarnException {
    FederationPolicyInitializationContext fpc =
        new FederationPolicyInitializationContext();
    getPolicy().reinitialize(fpc);
  }

  @Test(expected = FederationPolicyInitializationException.class)
  public void testReinitilializeBad3() throws YarnException {
    FederationPolicyInitializationContext fpc =
        new FederationPolicyInitializationContext();
    ByteBuffer buf = mock(ByteBuffer.class);
    fpc.setSubClusterPolicyConfiguration(SubClusterPolicyConfiguration
        .newInstance("queue1", "WrongPolicyName", buf));
    fpc.setFederationSubclusterResolver(
        FederationPoliciesTestUtil.initResolver());
    fpc.setFederationStateStoreFacade(FederationPoliciesTestUtil.initFacade());
    getPolicy().reinitialize(fpc);
  }

  @Test(expected = NoActiveSubclustersException.class)
  public void testNoSubclusters() throws YarnException {
    // empty the activeSubclusters map
    FederationPoliciesTestUtil.initializePolicyContext(getPolicy(),
        getPolicyInfo(), new HashMap<>());

    ConfigurableFederationPolicy currentPolicy = getPolicy();
    if (currentPolicy instanceof FederationRouterPolicy) {
      ((FederationRouterPolicy) currentPolicy)
          .getHomeSubcluster(getApplicationSubmissionContext());
    }
  }

  public ConfigurableFederationPolicy getPolicy() {
    return policy;
  }

  public void setPolicy(ConfigurableFederationPolicy policy) {
    this.policy = policy;
  }

  public WeightedPolicyInfo getPolicyInfo() {
    return policyInfo;
  }

  public void setPolicyInfo(WeightedPolicyInfo policyInfo) {
    this.policyInfo = policyInfo;
  }

  public Map<SubClusterId, SubClusterInfo> getActiveSubclusters() {
    return activeSubclusters;
  }

  public void setActiveSubclusters(
      Map<SubClusterId, SubClusterInfo> activeSubclusters) {
    this.activeSubclusters = activeSubclusters;
  }

  public FederationPolicyInitializationContext getFederationPolicyContext() {
    return federationPolicyContext;
  }

  public void setFederationPolicyContext(
      FederationPolicyInitializationContext federationPolicyContext) {
    this.federationPolicyContext = federationPolicyContext;
  }

  public ApplicationSubmissionContext getApplicationSubmissionContext() {
    return applicationSubmissionContext;
  }

  public void setApplicationSubmissionContext(
      ApplicationSubmissionContext applicationSubmissionContext) {
    this.applicationSubmissionContext = applicationSubmissionContext;
  }

  public Random getRand() {
    return rand;
  }

  public void setRand(Random rand) {
    this.rand = rand;
  }

}
