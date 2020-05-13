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

import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyUtils;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;

/**
 * This implements a policy that interprets "weights" as a ordered list of
 * preferences among sub-clusters. Highest weight among active subclusters is
 * chosen.
 */
public class PriorityRouterPolicy extends AbstractRouterPolicy {

  @Override
  public SubClusterId getHomeSubcluster(
      ApplicationSubmissionContext appSubmissionContext,
      List<SubClusterId> blacklist) throws YarnException {

    // null checks and default-queue behavior
    validate(appSubmissionContext);

    Map<SubClusterId, SubClusterInfo> activeSubclusters =
        getActiveSubclusters();

    FederationPolicyUtils.validateSubClusterAvailability(
        new ArrayList<SubClusterId>(activeSubclusters.keySet()), blacklist);

    // This finds the sub-cluster with the highest weight among the
    // currently active ones.
    Map<SubClusterIdInfo, Float> weights =
        getPolicyInfo().getRouterPolicyWeights();
    SubClusterId chosen = null;
    Float currentBest = Float.MIN_VALUE;
    for (SubClusterId id : activeSubclusters.keySet()) {
      SubClusterIdInfo idInfo = new SubClusterIdInfo(id);
      if (blacklist != null && blacklist.contains(id)) {
        continue;
      }
      if (weights.containsKey(idInfo) && weights.get(idInfo) > currentBest) {
        currentBest = weights.get(idInfo);
        chosen = id;
      }
    }
    if (chosen == null) {
      throw new FederationPolicyException(
          "No Active Subcluster with weight vector greater than zero");
    }

    return chosen;
  }

}