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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;

import java.util.Map;
import java.util.Random;

/**
 * This policy implements a weighted random sample among currently active
 * sub-clusters.
 */
public class WeightedRandomRouterPolicy
    extends BaseWeightedRouterPolicy {

  private static final Log LOG =
      LogFactory.getLog(WeightedRandomRouterPolicy.class);
  private Random rand = new Random(System.currentTimeMillis());

  @Override
  public SubClusterId getHomeSubcluster(
      ApplicationSubmissionContext appSubmissionContext)
      throws YarnException {

    Map<SubClusterId, SubClusterInfo> activeSubclusters =
        getActiveSubclusters();

    // note: we cannot pre-compute the weights, as the set of activeSubcluster
    // changes dynamically (and this would unfairly spread the load to
    // sub-clusters adjacent to an inactive one), hence we need to count/scan
    // the list and based on weight pick the next sub-cluster.
    Map<SubClusterIdInfo, Float> weights = getPolicyInfo()
        .getRouterPolicyWeights();

    float totActiveWeight = 0;
    for(Map.Entry<SubClusterIdInfo, Float> entry : weights.entrySet()){
      if(entry.getKey()!=null && activeSubclusters.containsKey(entry.getKey()
          .toId())){
        totActiveWeight += entry.getValue();
      }
    }
    float lookupValue = rand.nextFloat() * totActiveWeight;

    for (SubClusterId id : activeSubclusters.keySet()) {
      SubClusterIdInfo idInfo = new SubClusterIdInfo(id);
      if (weights.containsKey(idInfo)) {
        lookupValue -= weights.get(idInfo);
      }
      if (lookupValue <= 0) {
        return id;
      }
    }
    //should never happen
    return null;
  }
}
