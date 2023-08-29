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

package org.apache.hadoop.yarn.server.globalpolicygenerator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyUtils;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.policies.manager.WeightedLocalityPolicyManager;
import org.apache.hadoop.yarn.server.federation.policies.router.FederationRouterPolicy;
import org.apache.hadoop.yarn.server.federation.policies.amrmproxy.FederationAMRMProxyPolicy;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.policies.manager.FederationPolicyManager;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * A utility class for the GPG Policy Generator to read and write policies
 * into the FederationStateStore. Policy specific logic is abstracted away in
 * this class, so the PolicyGenerator can avoid dealing with policy
 * construction, reinitialization, and serialization.
 *
 * There are only two exposed methods:
 *
 * {@link #getPolicyManager(String)}
 * Gets the PolicyManager via queue name. Null if there is no policy
 * configured for the specified queue. The PolicyManager can be used to
 * extract the {@link FederationRouterPolicy} and
 * {@link FederationAMRMProxyPolicy}, as well as any policy specific parameters
 *
 * {@link #setPolicyManager(FederationPolicyManager)}
 * Sets the PolicyManager. If the policy configuration is the same, no change
 * occurs. Otherwise, the internal cache is updated and the new configuration
 * is written into the FederationStateStore
 *
 * This class assumes that the GPG is the only service
 * writing policies. Thus, the only FederationStateStore reads occur the first
 * time a queue policy is retrieved - after that, the GPG only writes to the
 * FederationStateStore.
 *
 * The class uses a PolicyManager cache and a SubClusterPolicyConfiguration
 * cache. The primary use for these caches are to serve reads, and to
 * identify when the PolicyGenerator has actually changed the policy
 * so unnecessary FederationStateStore policy writes can be avoided.
 */

public class GPGPolicyFacade {

  private static final Logger LOG =
      LoggerFactory.getLogger(GPGPolicyFacade.class);

  private FederationStateStoreFacade stateStore;

  private Map<String, FederationPolicyManager> policyManagerMap;
  private Map<String, SubClusterPolicyConfiguration> policyConfMap;

  private boolean readOnly;

  public GPGPolicyFacade(FederationStateStoreFacade stateStore,
      Configuration conf) {
    this.stateStore = stateStore;
    this.policyManagerMap = new HashMap<>();
    this.policyConfMap = new HashMap<>();
    this.readOnly =
        conf.getBoolean(YarnConfiguration.GPG_POLICY_GENERATOR_READONLY,
            YarnConfiguration.DEFAULT_GPG_POLICY_GENERATOR_READONLY);
  }

  /**
   * Provides a utility for the policy generator to read the policy manager
   * from the FederationStateStore. Because the policy generator should be the
   * only component updating the policy, this implementation does not use the
   * reinitialization feature.
   *
   * @param queueName the name of the queue we want the policy manager for.
   * @return the policy manager responsible for the queue policy.
   * @throws YarnException exceptions from yarn servers.
   */
  public FederationPolicyManager getPolicyManager(String queueName)
      throws YarnException {
    FederationPolicyManager policyManager = policyManagerMap.get(queueName);
    // If we don't have the policy manager cached, pull configuration
    // from the FederationStateStore to create and cache it
    if (policyManager == null) {
      try {
        // If we don't have the configuration cached, pull it
        // from the stateStore
        SubClusterPolicyConfiguration conf = policyConfMap.get(queueName);
        if (conf == null) {
          conf = stateStore.getPolicyConfiguration(queueName);
        }
        // If configuration is still null, it does not exist in the
        // FederationStateStore
        if (conf == null) {
          LOG.info("Read null policy for queue {}", queueName);
          return null;
        }
        policyManager =
            FederationPolicyUtils.instantiatePolicyManager(conf.getType());
        policyManager.setQueue(queueName);

        // TODO there is currently no way to cleanly deserialize a policy
        // manager sub type from just the configuration
        if (policyManager instanceof WeightedLocalityPolicyManager) {
          WeightedPolicyInfo wpinfo =
              WeightedPolicyInfo.fromByteBuffer(conf.getParams());
          WeightedLocalityPolicyManager wlpmanager =
              (WeightedLocalityPolicyManager) policyManager;
          LOG.info("Updating policy for queue {} to configured weights router: "
                  + "{}, amrmproxy: {}", queueName,
              wpinfo.getRouterPolicyWeights(),
              wpinfo.getAMRMPolicyWeights());
          wlpmanager.setWeightedPolicyInfo(wpinfo);
        } else {
          LOG.warn("Warning: FederationPolicyManager of unsupported type {}, "
              + "initialization may be incomplete ", policyManager.getClass());
        }

        policyManagerMap.put(queueName, policyManager);
        policyConfMap.put(queueName, conf);
      } catch (YarnException e) {
        LOG.error("Error reading SubClusterPolicyConfiguration from state "
            + "store for queue: {}", queueName);
        throw e;
      }
    }
    return policyManager;
  }

  /**
   * Provides a utility for the policy generator to write a policy manager
   * into the FederationStateStore. The facade keeps a cache and will only write
   * into the FederationStateStore if the policy configuration has changed.
   *
   * @param policyManager The policy manager we want to update into the state
   *                      store. It contains policy information as well as
   *                      the queue name we will update for.
   * @throws YarnException  exceptions from yarn servers.
   */
  public void setPolicyManager(FederationPolicyManager policyManager)
      throws YarnException {
    if (policyManager == null) {
      LOG.warn("Attempting to set null policy manager");
      return;
    }
    // Extract the configuration from the policy manager
    String queue = policyManager.getQueue();
    SubClusterPolicyConfiguration conf;
    try {
      conf = policyManager.serializeConf();
    } catch (FederationPolicyInitializationException e) {
      LOG.warn("Error serializing policy for queue {}", queue);
      throw e;
    }
    if (conf == null) {
      // State store does not currently support setting a policy back to null
      // because it reads the queue name to set from the policy!
      LOG.warn("Skip setting policy to null for queue {} into state store",
          queue);
      return;
    }
    // Compare with configuration cache, if different, write the conf into
    // store and update our conf and manager cache
    if (!confCacheEqual(queue, conf)) {
      try {
        if (readOnly) {
          LOG.info("[read-only] Skipping policy update for queue {}", queue);
          return;
        }
        LOG.info("Updating policy for queue {} into state store", queue);
        stateStore.setPolicyConfiguration(conf);
        policyConfMap.put(queue, conf);
        policyManagerMap.put(queue, policyManager);
      } catch (YarnException e) {
        LOG.warn("Error writing SubClusterPolicyConfiguration to state "
            + "store for queue: {}", queue);
        throw e;
      }
    } else {
      LOG.info("Setting unchanged policy - state store write skipped");
    }
  }

  /**
   * @param queue the queue to check the cached policy configuration for
   * @param conf the new policy configuration
   * @return whether or not the conf is equal to the cached conf
   */
  private boolean confCacheEqual(String queue,
      SubClusterPolicyConfiguration conf) {
    SubClusterPolicyConfiguration cachedConf = policyConfMap.get(queue);
    if (conf == null && cachedConf == null) {
      return true;
    } else if (conf != null && cachedConf != null) {
      if (conf.equals(cachedConf)) {
        return true;
      }
    }
    return false;
  }
}
