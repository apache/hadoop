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
package org.apache.hadoop.yarn.server.globalpolicygenerator.policygenerator;

import org.apache.hadoop.yarn.server.federation.policies.manager.FederationPolicyManager;
import org.apache.hadoop.yarn.server.federation.policies.manager.WeightedLocalityPolicyManager;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GPGUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Simple policy that generates and updates uniform weighted locality
 * policies.
 */
public class UniformWeightedLocalityGlobalPolicy extends GlobalPolicy {

  private static final Logger LOG =
      LoggerFactory.getLogger(UniformWeightedLocalityGlobalPolicy.class);

  @Override
  protected FederationPolicyManager updatePolicy(String queueName,
      Map<SubClusterId, Map<Class, Object>> clusterInfo, FederationPolicyManager currentManager){

    if(currentManager == null){
      // Set uniform weights for all SubClusters
      LOG.info("Creating uniform weighted policy queue {}.", queueName);
      WeightedLocalityPolicyManager manager = new WeightedLocalityPolicyManager();
      manager.setQueue(queueName);
      Map<SubClusterIdInfo, Float> policyWeights =
          GPGUtils.createUniformWeights(clusterInfo.keySet());
      manager.getWeightedPolicyInfo().setAMRMPolicyWeights(policyWeights);
      manager.getWeightedPolicyInfo().setRouterPolicyWeights(policyWeights);
      currentManager = manager;
    }

    if(currentManager instanceof WeightedLocalityPolicyManager){
      LOG.info("Updating policy for queue {} to default weights.", queueName);
      WeightedLocalityPolicyManager wlpmanager = (WeightedLocalityPolicyManager) currentManager;
      Map<SubClusterIdInfo, Float> uniformWeights =
          GPGUtils.createUniformWeights(clusterInfo.keySet());
      wlpmanager.getWeightedPolicyInfo().setAMRMPolicyWeights(uniformWeights);
      wlpmanager.getWeightedPolicyInfo().setRouterPolicyWeights(uniformWeights);
    } else {
      LOG.info("Policy for queue {} is of type {}, expected {}",
          queueName, currentManager.getClass(), WeightedLocalityPolicyManager.class);
    }
    return currentManager;
  }
}
