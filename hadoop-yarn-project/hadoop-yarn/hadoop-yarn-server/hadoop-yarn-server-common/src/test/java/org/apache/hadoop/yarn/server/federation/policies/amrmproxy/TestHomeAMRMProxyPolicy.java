/*
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

package org.apache.hadoop.yarn.server.federation.policies.amrmproxy;

import static org.apache.hadoop.yarn.server.federation.utils.FederationPoliciesTestUtil.createResourceRequests;
import static org.apache.hadoop.yarn.server.federation.utils.FederationPoliciesTestUtil.initializePolicyContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.BaseFederationPoliciesTest;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.junit.Before;
import org.junit.Test;

/**
 * Simple test class for the {@link HomeAMRMProxyPolicy}.
 */
public class TestHomeAMRMProxyPolicy extends BaseFederationPoliciesTest {

  private static final int NUM_SUBCLUSTERS = 4;

  private static final String HOME_SC_NAME = "sc2";
  private static final SubClusterId HOME_SC_ID =
      SubClusterId.newInstance(HOME_SC_NAME);

  @Before
  public void setUp() throws Exception {
    setPolicy(new HomeAMRMProxyPolicy());
    // needed for base test to work
    setPolicyInfo(mock(WeightedPolicyInfo.class));

    for (int i = 0; i < NUM_SUBCLUSTERS; i++) {
      SubClusterIdInfo sc = new SubClusterIdInfo("sc" + i);
      SubClusterInfo sci = mock(SubClusterInfo.class);
      when(sci.getState()).thenReturn(SubClusterState.SC_RUNNING);
      when(sci.getSubClusterId()).thenReturn(sc.toId());
      getActiveSubclusters().put(sc.toId(), sci);
    }

    initializePolicyContext(getPolicy(), mock(WeightedPolicyInfo.class),
        getActiveSubclusters(), HOME_SC_NAME);
  }

  @Test
  public void testSplitAllocateRequest() throws YarnException {

    // Verify the request only goes to the home subcluster
    String[] hosts = new String[] {"host0", "host1", "host2", "host3"};
    List<ResourceRequest> resourceRequests = createResourceRequests(
        hosts, 2 * 1024, 2, 1, 3, null, false);

    HomeAMRMProxyPolicy federationPolicy =
        (HomeAMRMProxyPolicy) getPolicy();
    Map<SubClusterId, List<ResourceRequest>> response = federationPolicy
        .splitResourceRequests(resourceRequests, new HashSet<SubClusterId>());
    assertEquals(1, response.size());
    assertNotNull(response.get(HOME_SC_ID));
    assertEquals(9, response.get(HOME_SC_ID).size());
  }

  @Test
  public void testHomeSubclusterNotActive() throws YarnException {

    // We setup the home subcluster to a non-existing one
    initializePolicyContext(getPolicy(), mock(WeightedPolicyInfo.class),
        getActiveSubclusters(), "badsc");

    // Verify the request fails because the home subcluster is not available
    try {
      String[] hosts = new String[] {"host0", "host1", "host2", "host3"};
      List<ResourceRequest> resourceRequests = createResourceRequests(
          hosts, 2 * 1024, 2, 1, 3, null, false);
      HomeAMRMProxyPolicy federationPolicy = (HomeAMRMProxyPolicy)getPolicy();
      federationPolicy.splitResourceRequests(resourceRequests,
          new HashSet<SubClusterId>());
      fail("It should fail when the home subcluster is not active");
    } catch(FederationPolicyException e) {
      GenericTestUtils.assertExceptionContains("is not active", e);
    }
  }
}
