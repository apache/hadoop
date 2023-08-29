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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.manager.FederationPolicyManager;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GPGContext;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GPGUtils;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The PolicyGenerator runs periodically and updates the policy configuration
 * for each queue into the FederationStateStore. The policy update behavior is
 * defined by the GlobalPolicy instance that is used.
 */

public class PolicyGenerator implements Runnable, Configurable {

  private static final Logger LOG =
      LoggerFactory.getLogger(PolicyGenerator.class);

  private GPGContext gpgContext;
  private Configuration conf;

  // Information request map
  private Map<Class, String> pathMap = new HashMap<>();

  // Global policy instance
  @VisibleForTesting
  private GlobalPolicy policy;

  /**
   * The PolicyGenerator periodically reads SubCluster load and updates
   * policies into the FederationStateStore.
   *
   * @param conf Configuration.
   * @param context GPG Context.
   */
  public PolicyGenerator(Configuration conf, GPGContext context) {
    setConf(conf);
    init(context);
  }

  private void init(GPGContext context) {
    this.gpgContext = context;
    LOG.info("Initialized PolicyGenerator");
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    this.policy = FederationStateStoreFacade.createInstance(conf,
        YarnConfiguration.GPG_GLOBAL_POLICY_CLASS,
        YarnConfiguration.DEFAULT_GPG_GLOBAL_POLICY_CLASS, GlobalPolicy.class);
    policy.setConf(conf);
    pathMap.putAll(policy.registerPaths());
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public final void run() {
    Map<SubClusterId, SubClusterInfo> activeSubClusters;
    try {
      activeSubClusters = gpgContext.getStateStoreFacade().getSubClusters(true);
    } catch (YarnException e) {
      LOG.error("Error retrieving active sub-clusters", e);
      return;
    }

    // Parse the scheduler information from all the SCs
    Map<SubClusterId, SchedulerInfo> schedInfo = getSchedulerInfo(activeSubClusters);

    // Extract and enforce that all the schedulers have matching type
    Set<String> queueNames = extractQueues(schedInfo);

    // Remove black listed SubClusters
    activeSubClusters.keySet().removeAll(getBlackList());
    LOG.info("Active non-blacklist sub-clusters: {}",
        activeSubClusters.keySet());

    // Get cluster metrics information from non-black listed RMs - later used
    // to evaluate SubCluster load
    Map<SubClusterId, Map<Class, Object>> clusterInfo =
        getInfos(activeSubClusters);

    // Update into the FederationStateStore
    for (String queueName : queueNames) {
      // Retrieve the manager from the policy facade
      FederationPolicyManager manager;
      try {
        manager = this.gpgContext.getPolicyFacade().getPolicyManager(queueName);
      } catch (YarnException e) {
        LOG.error("GetPolicy for queue {} failed.", queueName, e);
        continue;
      }
      LOG.info("Updating policy for queue {}.", queueName);
      manager = policy.updatePolicy(queueName, clusterInfo, manager);
      try {
        this.gpgContext.getPolicyFacade().setPolicyManager(manager);
      } catch (YarnException e) {
        LOG.error("SetPolicy for queue {} failed.", queueName, e);
      }
    }
  }

  /**
   * Helper to retrieve metrics from the RM REST endpoints.
   *
   * @param activeSubClusters A map of active SubCluster IDs to info
   * @return Mapping relationship between SubClusterId and Metric.
   */
  @VisibleForTesting
  protected Map<SubClusterId, Map<Class, Object>> getInfos(
      Map<SubClusterId, SubClusterInfo> activeSubClusters) {

    Map<SubClusterId, Map<Class, Object>> clusterInfo = new HashMap<>();
    for (SubClusterInfo sci : activeSubClusters.values()) {
      for (Map.Entry<Class, String> e : this.pathMap.entrySet()) {
        if (!clusterInfo.containsKey(sci.getSubClusterId())) {
          clusterInfo.put(sci.getSubClusterId(), new HashMap<>());
        }
        Object ret = GPGUtils.invokeRMWebService(sci.getRMWebServiceAddress(),
            e.getValue(), e.getKey(), getConf());
        clusterInfo.get(sci.getSubClusterId()).put(e.getKey(), ret);
      }
    }

    return clusterInfo;
  }

  /**
   * Helper to retrieve SchedulerInfos.
   *
   * @param activeSubClusters A map of active SubCluster IDs to info
   * @return Mapping relationship between SubClusterId and SubClusterInfo.
   */
  @VisibleForTesting
  protected Map<SubClusterId, SchedulerInfo> getSchedulerInfo(
      Map<SubClusterId, SubClusterInfo> activeSubClusters) {
    Map<SubClusterId, SchedulerInfo> schedInfo =
        new HashMap<>();
    for (SubClusterInfo sci : activeSubClusters.values()) {
      SchedulerTypeInfo sti = GPGUtils
          .invokeRMWebService(sci.getRMWebServiceAddress(),
              RMWSConsts.SCHEDULER, SchedulerTypeInfo.class, getConf());
      if(sti != null){
        schedInfo.put(sci.getSubClusterId(), sti.getSchedulerInfo());
      } else {
        LOG.warn("Skipped null scheduler info from SubCluster {}.", sci.getSubClusterId());
      }
    }
    return schedInfo;
  }

  /**
   * Helper to get a set of blacklisted SubCluster Ids from configuration.
   */
  private Set<SubClusterId> getBlackList() {
    String blackListParam =
        conf.get(YarnConfiguration.GPG_POLICY_GENERATOR_BLACKLIST);
    if(blackListParam == null){
      return Collections.emptySet();
    }
    Set<SubClusterId> blackList = new HashSet<>();
    for (String id : blackListParam.split(",")) {
      blackList.add(SubClusterId.newInstance(id));
    }
    return blackList;
  }

  /**
   * Given the scheduler information for all RMs, extract the union of
   * queue names - right now we only consider instances of capacity scheduler.
   *
   * @param schedInfo the scheduler information
   * @return a set of queue names
   */
  private Set<String> extractQueues(Map<SubClusterId, SchedulerInfo> schedInfo) {
    Set<String> queueNames = new HashSet<>();
    for (Map.Entry<SubClusterId, SchedulerInfo> entry : schedInfo.entrySet()) {
      if (entry.getValue() instanceof CapacitySchedulerInfo) {
        // Flatten the queue structure and get only non leaf queues
        queueNames.addAll(flattenQueue((CapacitySchedulerInfo) entry.getValue())
            .get(CapacitySchedulerQueueInfo.class));
      } else {
        LOG.warn("Skipping SubCluster {}, not configured with capacity scheduler.",
            entry.getKey());
      }
    }
    return queueNames;
  }

  // Helpers to flatten the queue structure into a multimap of
  // queue type to set of queue names
  private Map<Class, Set<String>> flattenQueue(CapacitySchedulerInfo csi) {
    Map<Class, Set<String>> flattened = new HashMap<>();
    addOrAppend(flattened, csi.getClass(), csi.getQueueName());
    for (CapacitySchedulerQueueInfo csqi : csi.getQueues().getQueueInfoList()) {
      flattenQueue(csqi, flattened);
    }
    return flattened;
  }

  private void flattenQueue(CapacitySchedulerQueueInfo csi,
      Map<Class, Set<String>> flattened) {
    addOrAppend(flattened, csi.getClass(), csi.getQueueName());
    if (csi.getQueues() != null) {
      for (CapacitySchedulerQueueInfo csqi : csi.getQueues().getQueueInfoList()) {
        flattenQueue(csqi, flattened);
      }
    }
  }

  private <K, V> void addOrAppend(Map<K, Set<V>> multimap, K key, V value) {
    if (!multimap.containsKey(key)) {
      multimap.put(key, new HashSet<>());
    }
    multimap.get(key).add(value);
  }

  public GlobalPolicy getPolicy() {
    return policy;
  }

  public void setPolicy(GlobalPolicy policy) {
    this.policy = policy;
  }
}
