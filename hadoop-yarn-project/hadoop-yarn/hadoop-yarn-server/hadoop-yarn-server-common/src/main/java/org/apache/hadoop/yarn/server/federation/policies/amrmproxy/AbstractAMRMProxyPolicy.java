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

import java.util.Map;

import org.apache.hadoop.yarn.server.federation.policies.AbstractConfigurableFederationPolicy;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;

/**
 * Base abstract class for {@link FederationAMRMProxyPolicy} implementations,
 * that provides common validation for reinitialization.
 */
public abstract class AbstractAMRMProxyPolicy extends
    AbstractConfigurableFederationPolicy implements FederationAMRMProxyPolicy {

  @Override
  public void validate(WeightedPolicyInfo newPolicyInfo)
      throws FederationPolicyInitializationException {
    super.validate(newPolicyInfo);
    Map<SubClusterIdInfo, Float> newWeights =
        newPolicyInfo.getAMRMPolicyWeights();
    if (newWeights == null || newWeights.size() < 1) {
      throw new FederationPolicyInitializationException(
          "Weight vector cannot be null/empty.");
    }
  }

}
