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

package org.apache.hadoop.yarn.server.federation.policies.router;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.BaseFederationPoliciesTest;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyUtils;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.utils.FederationPoliciesTestUtil;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Test;

/**
 * Base class for router policies tests, tests for null input cases.
 */
public abstract class BaseRouterPoliciesTest
    extends BaseFederationPoliciesTest {

  @Test
  public void testNullQueueRouting() throws YarnException {
    FederationRouterPolicy localPolicy = (FederationRouterPolicy) getPolicy();
    ApplicationSubmissionContext applicationSubmissionContext =
        ApplicationSubmissionContext.newInstance(null, null, null, null, null,
            false, false, 0, Resources.none(), null, false, null, null);
    SubClusterId chosen =
        localPolicy.getHomeSubcluster(applicationSubmissionContext, null);
    Assert.assertNotNull(chosen);
  }

  @Test(expected = FederationPolicyException.class)
  public void testNullAppContext() throws YarnException {
    ((FederationRouterPolicy) getPolicy()).getHomeSubcluster(null, null);
  }

  @Test
  public void testBlacklistSubcluster() throws YarnException {
    FederationRouterPolicy localPolicy = (FederationRouterPolicy) getPolicy();
    ApplicationSubmissionContext applicationSubmissionContext =
        ApplicationSubmissionContext.newInstance(null, null, null, null, null,
            false, false, 0, Resources.none(), null, false, null, null);
    Map<SubClusterId, SubClusterInfo> activeSubClusters =
        getActiveSubclusters();
    if (activeSubClusters != null && activeSubClusters.size() > 1
        && !(localPolicy instanceof RejectRouterPolicy)) {
      // blacklist all the active subcluster but one.
      Random random = new Random();
      List<SubClusterId> blacklistSubclusters =
          new ArrayList<SubClusterId>(activeSubClusters.keySet());
      SubClusterId removed = blacklistSubclusters
          .remove(random.nextInt(blacklistSubclusters.size()));
      // bias LoadBasedRouterPolicy
      getPolicyInfo().getRouterPolicyWeights()
          .put(new SubClusterIdInfo(removed), 1.0f);
      FederationPoliciesTestUtil.initializePolicyContext(getPolicy(),
          getPolicyInfo(), getActiveSubclusters());

      SubClusterId chosen = localPolicy.getHomeSubcluster(
          applicationSubmissionContext, blacklistSubclusters);

      // check that the selected sub-cluster is only one not blacklisted
      Assert.assertNotNull(chosen);
      Assert.assertEquals(removed, chosen);
    }
  }

  /**
   * This test validates the correctness of blacklist logic in case the cluster
   * has no active subclusters.
   */
  @Test
  public void testAllBlacklistSubcluster() throws YarnException {
    FederationRouterPolicy localPolicy = (FederationRouterPolicy) getPolicy();
    ApplicationSubmissionContext applicationSubmissionContext =
        ApplicationSubmissionContext.newInstance(null, null, null, null, null,
            false, false, 0, Resources.none(), null, false, null, null);
    Map<SubClusterId, SubClusterInfo> activeSubClusters =
        getActiveSubclusters();
    if (activeSubClusters != null && activeSubClusters.size() > 1
        && !(localPolicy instanceof RejectRouterPolicy)) {
      List<SubClusterId> blacklistSubclusters =
          new ArrayList<SubClusterId>(activeSubClusters.keySet());
      try {
        localPolicy.getHomeSubcluster(applicationSubmissionContext,
            blacklistSubclusters);
        Assert.fail();
      } catch (YarnException e) {
        Assert.assertTrue(e.getMessage()
            .equals(FederationPolicyUtils.NO_ACTIVE_SUBCLUSTER_AVAILABLE));
      }
    }
  }
}
