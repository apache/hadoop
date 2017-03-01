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

import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContext;
import org.apache.hadoop.yarn.server.federation.policies.amrmproxy.FederationAMRMProxyPolicy;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.policies.router.FederationRouterPolicy;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.utils.FederationPoliciesTestUtil;
import org.junit.Assert;
import org.junit.Test;

/**
 * This class provides common test methods for testing {@code
 * FederationPolicyManager}s.
 */
public abstract class BasePolicyManagerTest {

  @SuppressWarnings("checkstyle:visibilitymodifier")
  protected FederationPolicyManager wfp = null;
  @SuppressWarnings("checkstyle:visibilitymodifier")
  protected Class expectedPolicyManager;
  @SuppressWarnings("checkstyle:visibilitymodifier")
  protected Class expectedAMRMProxyPolicy;
  @SuppressWarnings("checkstyle:visibilitymodifier")
  protected Class expectedRouterPolicy;

  @Test
  public void testSerializeAndInstantiate() throws Exception {
    serializeAndDeserializePolicyManager(wfp, expectedPolicyManager,
        expectedAMRMProxyPolicy, expectedRouterPolicy);
  }

  @Test(expected = FederationPolicyInitializationException.class)
  public void testSerializeAndInstantiateBad1() throws Exception {
    serializeAndDeserializePolicyManager(wfp, String.class,
        expectedAMRMProxyPolicy, expectedRouterPolicy);
  }

  @Test(expected = AssertionError.class)
  public void testSerializeAndInstantiateBad2() throws Exception {
    serializeAndDeserializePolicyManager(wfp, expectedPolicyManager,
        String.class, expectedRouterPolicy);
  }

  @Test(expected = AssertionError.class)
  public void testSerializeAndInstantiateBad3() throws Exception {
    serializeAndDeserializePolicyManager(wfp, expectedPolicyManager,
        expectedAMRMProxyPolicy, String.class);
  }

  protected static void serializeAndDeserializePolicyManager(
      FederationPolicyManager wfp, Class policyManagerType,
      Class expAMRMProxyPolicy, Class expRouterPolicy) throws Exception {

    // serializeConf it in a context
    SubClusterPolicyConfiguration fpc = wfp.serializeConf();
    fpc.setType(policyManagerType.getCanonicalName());
    FederationPolicyInitializationContext context =
        new FederationPolicyInitializationContext();
    context.setSubClusterPolicyConfiguration(fpc);
    context
        .setFederationStateStoreFacade(FederationPoliciesTestUtil.initFacade());
    context.setFederationSubclusterResolver(
        FederationPoliciesTestUtil.initResolver());
    context.setHomeSubcluster(SubClusterId.newInstance("homesubcluster"));

    // based on the "context" created instantiate new class and use it
    Class c = Class.forName(wfp.getClass().getCanonicalName());
    FederationPolicyManager wfp2 = (FederationPolicyManager) c.newInstance();

    FederationAMRMProxyPolicy federationAMRMProxyPolicy =
        wfp2.getAMRMPolicy(context, null);

    FederationRouterPolicy federationRouterPolicy =
        wfp2.getRouterPolicy(context, null);

    Assert.assertEquals(federationAMRMProxyPolicy.getClass(),
        expAMRMProxyPolicy);

    Assert.assertEquals(federationRouterPolicy.getClass(), expRouterPolicy);
  }

}
