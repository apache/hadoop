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

package org.apache.hadoop.yarn.server.router.clientrm;

import org.apache.hadoop.yarn.server.federation.policies.amrmproxy.BroadcastAMRMProxyPolicy;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.policies.manager.AbstractPolicyManager;

/**
 * This PolicyManager is used for testing and will contain the
 * {@link TestSequentialRouterPolicy} policy.
 *
 * When we test FederationClientInterceptor Retry,
 * we hope that SubCluster can return in a certain order, not randomly.
 * We can view the policy description by linking to TestSequentialRouterPolicy.
 */
public class TestSequentialBroadcastPolicyManager extends AbstractPolicyManager {
  public TestSequentialBroadcastPolicyManager() {
    // this structurally hard-codes two compatible policies for Router and
    // AMRMProxy.
    routerFederationPolicy = TestSequentialRouterPolicy.class;
    amrmProxyFederationPolicy = BroadcastAMRMProxyPolicy.class;
  }

  @Override
  public WeightedPolicyInfo getWeightedPolicyInfo() {
    return null;
  }

  @Override
  public void setWeightedPolicyInfo(WeightedPolicyInfo weightedPolicyInfo) {
  }

  @Override
  public boolean isSupportWeightedPolicyInfo() {
    return false;
  }
}
