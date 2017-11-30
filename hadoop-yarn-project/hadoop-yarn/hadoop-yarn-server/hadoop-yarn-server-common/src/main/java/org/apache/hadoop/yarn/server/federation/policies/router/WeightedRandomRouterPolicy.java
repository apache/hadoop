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

import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyUtils;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;

/**
 * This policy implements a weighted random sample among currently active
 * sub-clusters.
 */
public class WeightedRandomRouterPolicy extends AbstractRouterPolicy {

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

    // note: we cannot pre-compute the weights, as the set of activeSubcluster
    // changes dynamically (and this would unfairly spread the load to
    // sub-clusters adjacent to an inactive one), hence we need to count/scan
    // the list and based on weight pick the next sub-cluster.
    Map<SubClusterIdInfo, Float> weights =
        getPolicyInfo().getRouterPolicyWeights();

    ArrayList<Float> weightList = new ArrayList<>();
    ArrayList<SubClusterId> scIdList = new ArrayList<>();
    for (Map.Entry<SubClusterIdInfo, Float> entry : weights.entrySet()) {
      if (blacklist != null && blacklist.contains(entry.getKey().toId())) {
        continue;
      }
      if (entry.getKey() != null
          && activeSubclusters.containsKey(entry.getKey().toId())) {
        weightList.add(entry.getValue());
        scIdList.add(entry.getKey().toId());
      }
    }

    int pickedIndex = FederationPolicyUtils.getWeightedRandom(weightList);
    if (pickedIndex == -1) {
      throw new FederationPolicyException(
          "No positive weight found on active subclusters");
    }
    return scIdList.get(pickedIndex);
  }

}
