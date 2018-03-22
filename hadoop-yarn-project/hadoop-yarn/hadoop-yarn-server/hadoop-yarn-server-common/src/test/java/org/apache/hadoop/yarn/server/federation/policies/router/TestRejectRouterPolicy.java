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

import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyException;
import org.apache.hadoop.yarn.server.federation.utils.FederationPoliciesTestUtil;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Before;
import org.junit.Test;

/**
 * Simple test class for the {@link RejectRouterPolicy}. Tests that one of the
 * active subcluster is chosen.
 */
public class TestRejectRouterPolicy extends BaseRouterPoliciesTest {

  @Before
  public void setUp() throws Exception {
    setPolicy(new RejectRouterPolicy());

    // setting up the active sub-clusters for this test
    setMockActiveSubclusters(2);

    // initialize policy with context
    FederationPoliciesTestUtil.initializePolicyContext(getPolicy(),
        getPolicyInfo(), getActiveSubclusters());

  }

  @Test(expected = FederationPolicyException.class)
  public void testNoClusterIsChosen() throws YarnException {
    ((FederationRouterPolicy) getPolicy())
        .getHomeSubcluster(getApplicationSubmissionContext(), null);
  }

  @Override
  @Test(expected = FederationPolicyException.class)
  public void testNullQueueRouting() throws YarnException {
    FederationRouterPolicy localPolicy = (FederationRouterPolicy) getPolicy();
    ApplicationSubmissionContext applicationSubmissionContext =
        ApplicationSubmissionContext.newInstance(null, null, null, null, null,
            false, false, 0, Resources.none(), null, false, null, null);
    localPolicy.getHomeSubcluster(applicationSubmissionContext, null);
  }

}
