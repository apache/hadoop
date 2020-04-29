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

package org.apache.hadoop.yarn.server.federation.policies;

import java.util.Map;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.NoActiveSubclustersException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;

/**
 * Base abstract class for a weighted {@link ConfigurableFederationPolicy}.
 */
public abstract class AbstractConfigurableFederationPolicy
    implements ConfigurableFederationPolicy {

  private WeightedPolicyInfo policyInfo = null;
  private FederationPolicyInitializationContext policyContext;
  private boolean isDirty;

  public AbstractConfigurableFederationPolicy() {
  }

  @Override
  public void reinitialize(
      FederationPolicyInitializationContext initializationContext)
      throws FederationPolicyInitializationException {
    isDirty = true;
    FederationPolicyInitializationContextValidator
        .validate(initializationContext, this.getClass().getCanonicalName());

    // perform consistency checks
    WeightedPolicyInfo newPolicyInfo = WeightedPolicyInfo.fromByteBuffer(
        initializationContext.getSubClusterPolicyConfiguration().getParams());

    // if nothing has changed skip the rest of initialization
    // and signal to childs that the reinit is free via isDirty var.
    if (policyInfo != null && policyInfo.equals(newPolicyInfo)) {
      isDirty = false;
      return;
    }

    validate(newPolicyInfo);
    setPolicyInfo(newPolicyInfo);
    this.policyContext = initializationContext;
  }

  /**
   * Overridable validation step for the policy configuration.
   *
   * @param newPolicyInfo the configuration to test.
   *
   * @throws FederationPolicyInitializationException if the configuration is not
   *           valid.
   */
  public void validate(WeightedPolicyInfo newPolicyInfo)
      throws FederationPolicyInitializationException {
    if (newPolicyInfo == null) {
      throw new FederationPolicyInitializationException(
          "The policy to " + "validate should not be null.");
    }
  }

  /**
   * Returns true whether the last reinitialization requires actual changes, or
   * was "free" as the weights have not changed. This is used by subclasses
   * overriding reinitialize and calling super.reinitialize() to know whether to
   * quit early.
   *
   * @return whether more work is needed to initialize.
   */
  public boolean getIsDirty() {
    return isDirty;
  }

  /**
   * Getter method for the configuration weights.
   *
   * @return the {@link WeightedPolicyInfo} representing the policy
   *         configuration.
   */
  public WeightedPolicyInfo getPolicyInfo() {
    return policyInfo;
  }

  /**
   * Setter method for the configuration weights.
   *
   * @param policyInfo the {@link WeightedPolicyInfo} representing the policy
   *          configuration.
   */
  public void setPolicyInfo(WeightedPolicyInfo policyInfo) {
    this.policyInfo = policyInfo;
  }

  /**
   * Getter method for the {@link FederationPolicyInitializationContext}.
   *
   * @return the context for this policy.
   */
  public FederationPolicyInitializationContext getPolicyContext() {
    return policyContext;
  }

  /**
   * Setter method for the {@link FederationPolicyInitializationContext}.
   *
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
   *
   * @throws YarnException if we can't get the list.
   */
  protected Map<SubClusterId, SubClusterInfo> getActiveSubclusters()
      throws YarnException {

    Map<SubClusterId, SubClusterInfo> activeSubclusters =
        getPolicyContext().getFederationStateStoreFacade().getSubClusters(true);

    if (activeSubclusters == null || activeSubclusters.size() < 1) {
      throw new NoActiveSubclustersException(
          "Zero active subclusters, cannot pick where to send job.");
    }
    return activeSubclusters;
  }

}
