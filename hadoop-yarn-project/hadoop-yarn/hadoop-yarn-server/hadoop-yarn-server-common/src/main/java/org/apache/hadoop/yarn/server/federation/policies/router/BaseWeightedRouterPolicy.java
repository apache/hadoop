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

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContext;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContextValidator;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.NoActiveSubclustersException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;

import java.util.Map;

/**
 * Abstract class provides common validation of reinitialize(), for all
 * policies that are "weight-based".
 */
public abstract class BaseWeightedRouterPolicy
    implements FederationRouterPolicy {

  private WeightedPolicyInfo policyInfo = null;
  private FederationPolicyInitializationContext policyContext;

  public BaseWeightedRouterPolicy() {
  }

  @Override
  public void reinitialize(FederationPolicyInitializationContext
      federationPolicyContext)
      throws FederationPolicyInitializationException {
    FederationPolicyInitializationContextValidator
        .validate(federationPolicyContext, this.getClass().getCanonicalName());

    // perform consistency checks
    WeightedPolicyInfo newPolicyInfo = WeightedPolicyInfo
        .fromByteBuffer(
            federationPolicyContext.getSubClusterPolicyConfiguration()
                .getParams());

    // if nothing has changed skip the rest of initialization
    if (policyInfo != null && policyInfo.equals(newPolicyInfo)) {
      return;
    }

    validate(newPolicyInfo);
    setPolicyInfo(newPolicyInfo);
    this.policyContext = federationPolicyContext;
  }

  /**
   * Overridable validation step for the policy configuration.
   * @param newPolicyInfo the configuration to test.
   * @throws FederationPolicyInitializationException if the configuration is
   * not valid.
   */
  public void validate(WeightedPolicyInfo newPolicyInfo) throws
      FederationPolicyInitializationException {
    if (newPolicyInfo == null) {
      throw new FederationPolicyInitializationException("The policy to "
          + "validate should not be null.");
    }
    Map<SubClusterIdInfo, Float> newWeights =
        newPolicyInfo.getRouterPolicyWeights();
    if (newWeights == null || newWeights.size() < 1) {
      throw new FederationPolicyInitializationException(
          "Weight vector cannot be null/empty.");
    }
  }


  /**
   * Getter method for the configuration weights.
   *
   * @return the {@link WeightedPolicyInfo} representing the policy
   * configuration.
   */
  public WeightedPolicyInfo getPolicyInfo() {
    return policyInfo;
  }

  /**
   * Setter method for the configuration weights.
   *
   * @param policyInfo the {@link WeightedPolicyInfo} representing the policy
   *                   configuration.
   */
  public void setPolicyInfo(
      WeightedPolicyInfo policyInfo) {
    this.policyInfo = policyInfo;
  }

  /**
   * Getter method for the {@link FederationPolicyInitializationContext}.
   * @return the context for this policy.
   */
  public FederationPolicyInitializationContext getPolicyContext() {
    return policyContext;
  }

  /**
   * Setter method for the {@link FederationPolicyInitializationContext}.
   * @param policyContext the context to assign to this policy.
   */
  public void setPolicyContext(
      FederationPolicyInitializationContext policyContext) {
    this.policyContext = policyContext;
  }

  /**
   * This methods gets active subclusters map from the {@code
   * FederationStateStoreFacade} and validate it not being null/empty.
   *
   * @return the map of ids to info for all active subclusters.
   * @throws YarnException if we can't get the list.
   */
  protected Map<SubClusterId, SubClusterInfo> getActiveSubclusters()
      throws YarnException {

    Map<SubClusterId, SubClusterInfo> activeSubclusters = getPolicyContext()
        .getFederationStateStoreFacade().getSubClusters(true);

    if (activeSubclusters == null || activeSubclusters.size() < 1) {
      throw new NoActiveSubclustersException(
          "Zero active subclusters, cannot pick where to send job.");
    }
    return activeSubclusters;
  }



}
