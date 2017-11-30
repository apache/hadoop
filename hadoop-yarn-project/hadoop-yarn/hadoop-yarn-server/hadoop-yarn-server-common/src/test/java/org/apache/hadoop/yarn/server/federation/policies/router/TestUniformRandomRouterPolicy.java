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
 * Simple test class for the {@link UniformRandomRouterPolicy}. Tests that one
 * of the active subcluster is chosen.
 */
public class TestUniformRandomRouterPolicy extends BaseRouterPoliciesTest {

  @Before
  public void setUp() throws Exception {
    setPolicy(new UniformRandomRouterPolicy());
    // needed for base test to work
    setPolicyInfo(mock(WeightedPolicyInfo.class));
    for (int i = 1; i <= 2; i++) {
      SubClusterIdInfo sc = new SubClusterIdInfo("sc" + i);
      SubClusterInfo sci = mock(SubClusterInfo.class);
      when(sci.getState()).thenReturn(SubClusterState.SC_RUNNING);
      when(sci.getSubClusterId()).thenReturn(sc.toId());
      getActiveSubclusters().put(sc.toId(), sci);
    }

    FederationPoliciesTestUtil.initializePolicyContext(getPolicy(),
        mock(WeightedPolicyInfo.class), getActiveSubclusters());
  }

  @Test
  public void testOneSubclusterIsChosen() throws YarnException {
    SubClusterId chosen = ((FederationRouterPolicy) getPolicy())
        .getHomeSubcluster(getApplicationSubmissionContext(), null);
    Assert.assertTrue(getActiveSubclusters().keySet().contains(chosen));
  }

}
