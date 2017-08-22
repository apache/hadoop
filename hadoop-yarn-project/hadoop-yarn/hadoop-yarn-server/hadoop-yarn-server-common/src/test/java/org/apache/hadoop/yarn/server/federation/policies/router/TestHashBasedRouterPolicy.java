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

import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.utils.FederationPoliciesTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Simple test class for the {@link HashBasedRouterPolicy}. Tests that one of
 * the active sub-cluster is chosen.
 */
public class TestHashBasedRouterPolicy extends BaseRouterPoliciesTest {

  private int numSubclusters = 10;

  @Before
  public void setUp() throws Exception {

    // set policy in base class
    setPolicy(new HashBasedRouterPolicy());

    // setting up the active sub-clusters for this test
    setMockActiveSubclusters(numSubclusters);

    // initialize policy with context
    FederationPoliciesTestUtil.initializePolicyContext(getPolicy(),
        getPolicyInfo(), getActiveSubclusters());
  }

  @Test
  public void testHashSpreadUniformlyAmongSubclusters() throws YarnException {
    SubClusterId chosen;

    Map<SubClusterId, AtomicLong> counter = new HashMap<>();
    for (SubClusterId id : getActiveSubclusters().keySet()) {
      counter.put(id, new AtomicLong(0));
    }

    long jobPerSub = 100;

    ApplicationSubmissionContext applicationSubmissionContext =
        mock(ApplicationSubmissionContext.class);
    for (int i = 0; i < jobPerSub * numSubclusters; i++) {
      when(applicationSubmissionContext.getQueue()).thenReturn("queue" + i);
      chosen = ((FederationRouterPolicy) getPolicy())
          .getHomeSubcluster(applicationSubmissionContext, null);
      counter.get(chosen).addAndGet(1);
    }

    // hash spread the jobs equally among the subclusters
    for (AtomicLong a : counter.values()) {
      Assert.assertEquals(a.get(), jobPerSub);
    }

  }
}
