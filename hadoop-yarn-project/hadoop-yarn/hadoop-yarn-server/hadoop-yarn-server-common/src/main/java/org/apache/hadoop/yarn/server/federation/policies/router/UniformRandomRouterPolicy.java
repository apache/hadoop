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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContext;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContextValidator;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyUtils;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;

/**
 * This simple policy picks at uniform random among any of the currently active
 * subclusters. This policy is easy to use and good for testing.
 *
 * NOTE: this is "almost" subsumed by the {@code WeightedRandomRouterPolicy}.
 * Behavior only diverges when there are active sub-clusters that are not part
 * of the "weights", in which case the {@link UniformRandomRouterPolicy} send
 * load to them, while {@code WeightedRandomRouterPolicy} does not.
 */
public class UniformRandomRouterPolicy extends AbstractRouterPolicy {

  private Random rand;

  public UniformRandomRouterPolicy() {
    rand = new Random(System.currentTimeMillis());
  }

  @Override
  public void reinitialize(FederationPolicyInitializationContext policyContext)
      throws FederationPolicyInitializationException {
    FederationPolicyInitializationContextValidator.validate(policyContext,
        this.getClass().getCanonicalName());

    // note: this overrides AbstractRouterPolicy and ignores the weights

    setPolicyContext(policyContext);
  }

  /**
   * Simply picks a random active subCluster to start the AM (this does NOT
   * depend on the weights in the policy).
   *
   * @param appSubmissionContext the {@link ApplicationSubmissionContext} that
   *          has to be routed to an appropriate subCluster for execution.
   *
   * @param blackListSubClusters the list of subClusters as identified by
   *          {@link SubClusterId} to blackList from the selection of the home
   *          subCluster.
   *
   * @return a randomly chosen subcluster.
   *
   * @throws YarnException if there are no active subclusters.
   */
  @Override
  public SubClusterId getHomeSubcluster(
      ApplicationSubmissionContext appSubmissionContext,
      List<SubClusterId> blackListSubClusters) throws YarnException {

    // null checks and default-queue behavior
    validate(appSubmissionContext);

    Map<SubClusterId, SubClusterInfo> activeSubclusters =
        getActiveSubclusters();

    List<SubClusterId> list = new ArrayList<>(activeSubclusters.keySet());

    FederationPolicyUtils.validateSubClusterAvailability(list,
        blackListSubClusters);

    if (blackListSubClusters != null) {

      // Remove from the active SubClusters from StateStore the blacklisted ones
      for (SubClusterId scId : blackListSubClusters) {
        list.remove(scId);
      }
    }

    return list.get(rand.nextInt(list.size()));
  }

}
