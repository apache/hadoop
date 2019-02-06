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
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContext;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyUtils;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyException;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * This implements a simple load-balancing policy. The policy "weights" are
 * binary 0/1 values that enable/disable each sub-cluster, and the policy peaks
 * the sub-cluster with the least load to forward this application.
 */
public class LoadBasedRouterPolicy extends AbstractRouterPolicy {

  @Override
  public void reinitialize(FederationPolicyInitializationContext policyContext)
      throws FederationPolicyInitializationException {

    // remember old policyInfo
    WeightedPolicyInfo tempPolicy = getPolicyInfo();

    // attempt new initialization
    super.reinitialize(policyContext);

    // check extra constraints
    for (Float weight : getPolicyInfo().getRouterPolicyWeights().values()) {
      if (weight != 0 && weight != 1) {
        // reset to old policyInfo if check fails
        setPolicyInfo(tempPolicy);
        throw new FederationPolicyInitializationException(
            this.getClass().getCanonicalName()
                + " policy expects all weights to be either "
                + "\"0\" or \"1\"");
      }
    }
  }

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

    Map<SubClusterIdInfo, Float> weights =
        getPolicyInfo().getRouterPolicyWeights();
    SubClusterIdInfo chosen = null;
    long currBestMem = -1;
    for (Map.Entry<SubClusterId, SubClusterInfo> entry : activeSubclusters
        .entrySet()) {
      if (blacklist != null && blacklist.contains(entry.getKey())) {
        continue;
      }
      SubClusterIdInfo id = new SubClusterIdInfo(entry.getKey());
      if (weights.containsKey(id) && weights.get(id) > 0) {
        long availableMemory = getAvailableMemory(entry.getValue());
        if (availableMemory > currBestMem) {
          currBestMem = availableMemory;
          chosen = id;
        }
      }
    }
    if (chosen == null) {
      throw new FederationPolicyException(
          "Zero Active Subcluster with weight 1.");
    }
    return chosen.toId();
  }

  private long getAvailableMemory(SubClusterInfo value) throws YarnException {
    try {
      long mem = -1;
      JSONObject obj = new JSONObject(value.getCapability());
      mem = obj.getJSONObject("clusterMetrics").getLong("availableMB");
      return mem;
    } catch (JSONException j) {
      throw new YarnException("FederationSubCluserInfo cannot be parsed", j);
    }
  }
}