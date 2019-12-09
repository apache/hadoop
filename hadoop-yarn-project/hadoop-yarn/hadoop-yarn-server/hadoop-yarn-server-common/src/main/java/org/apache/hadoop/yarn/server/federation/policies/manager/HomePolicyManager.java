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

import java.nio.ByteBuffer;
import java.util.Collections;

import org.apache.hadoop.yarn.server.federation.policies.amrmproxy.HomeAMRMProxyPolicy;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.policies.router.UniformRandomRouterPolicy;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;

/**
 * Policy manager which uses the {@link UniformRandomRouterPolicy} for the
 * Router and {@link HomeAMRMProxyPolicy} as the AMRMProxy policy to find the
 * RM.
 */
public class HomePolicyManager extends AbstractPolicyManager {

  /** Imaginary configuration to fulfill the super class. */
  private WeightedPolicyInfo weightedPolicyInfo;

  public HomePolicyManager() {

    weightedPolicyInfo = new WeightedPolicyInfo();
    weightedPolicyInfo.setRouterPolicyWeights(
        Collections.singletonMap(new SubClusterIdInfo(""), 1.0f));
    weightedPolicyInfo.setAMRMPolicyWeights(
        Collections.singletonMap(new SubClusterIdInfo(""), 1.0f));

    // Hard-codes two compatible policies for Router and AMRMProxy.
    routerFederationPolicy = UniformRandomRouterPolicy.class;
    amrmProxyFederationPolicy = HomeAMRMProxyPolicy.class;
  }

  @Override
  public SubClusterPolicyConfiguration serializeConf()
      throws FederationPolicyInitializationException {

    ByteBuffer buf = weightedPolicyInfo.toByteBuffer();
    return SubClusterPolicyConfiguration.newInstance(
        getQueue(), this.getClass().getCanonicalName(), buf);
  }
}
