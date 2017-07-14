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

package org.apache.hadoop.yarn.server.federation.policies;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyException;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.policies.manager.FederationPolicyManager;
import org.apache.hadoop.yarn.server.federation.policies.router.FederationRouterPolicy;
import org.apache.hadoop.yarn.server.federation.resolver.SubClusterResolver;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * This class provides a facade to the policy subsystem, and handles the
 * lifecycle of policies (e.g., refresh from remote, default behaviors etc.).
 */
public class RouterPolicyFacade {

  private static final Logger LOG =
      LoggerFactory.getLogger(RouterPolicyFacade.class);

  private final SubClusterResolver subClusterResolver;
  private final FederationStateStoreFacade federationFacade;
  private Map<String, SubClusterPolicyConfiguration> globalConfMap;

  @VisibleForTesting
  Map<String, FederationRouterPolicy> globalPolicyMap;

  public RouterPolicyFacade(Configuration conf,
      FederationStateStoreFacade facade, SubClusterResolver resolver,
      SubClusterId homeSubcluster)
      throws FederationPolicyInitializationException {

    this.federationFacade = facade;
    this.subClusterResolver = resolver;
    this.globalConfMap = new ConcurrentHashMap<>();
    this.globalPolicyMap = new ConcurrentHashMap<>();

    // load default behavior from store if possible
    String defaultKey = YarnConfiguration.DEFAULT_FEDERATION_POLICY_KEY;
    SubClusterPolicyConfiguration configuration = null;
    try {
      configuration = federationFacade.getPolicyConfiguration(defaultKey);
    } catch (YarnException e) {
      LOG.warn("No fallback behavior defined in store, defaulting to XML "
          + "configuration fallback behavior.");
    }

    // or from XML conf otherwise.
    if (configuration == null) {
      String defaultFederationPolicyManager =
          conf.get(YarnConfiguration.FEDERATION_POLICY_MANAGER,
              YarnConfiguration.DEFAULT_FEDERATION_POLICY_MANAGER);
      String defaultPolicyParamString =
          conf.get(YarnConfiguration.FEDERATION_POLICY_MANAGER_PARAMS,
              YarnConfiguration.DEFAULT_FEDERATION_POLICY_MANAGER_PARAMS);
      ByteBuffer defaultPolicyParam = ByteBuffer
          .wrap(defaultPolicyParamString.getBytes(StandardCharsets.UTF_8));

      configuration = SubClusterPolicyConfiguration.newInstance(defaultKey,
          defaultFederationPolicyManager, defaultPolicyParam);
    }

    // construct the required policy manager
    FederationPolicyInitializationContext fallbackContext =
        new FederationPolicyInitializationContext(configuration,
            subClusterResolver, federationFacade, homeSubcluster);
    FederationPolicyManager fallbackPolicyManager =
        FederationPolicyUtils.instantiatePolicyManager(configuration.getType());
    fallbackPolicyManager.setQueue(defaultKey);

    // add to the cache the fallback behavior
    globalConfMap.put(defaultKey,
        fallbackContext.getSubClusterPolicyConfiguration());
    globalPolicyMap.put(defaultKey,
        fallbackPolicyManager.getRouterPolicy(fallbackContext, null));

  }

  /**
   * This method provides a wrapper of all policy functionalities for routing .
   * Internally it manages configuration changes, and policy init/reinit.
   *
   * @param appSubmissionContext the {@link ApplicationSubmissionContext} that
   *          has to be routed to an appropriate subCluster for execution.
   *
   * @param blackListSubClusters the list of subClusters as identified by
   *          {@link SubClusterId} to blackList from the selection of the home
   *          subCluster.
   *
   * @return the {@link SubClusterId} that will be the "home" for this
   *         application.
   *
   * @throws YarnException if there are issues initializing policies, or no
   *           valid sub-cluster id could be found for this app.
   */
  public SubClusterId getHomeSubcluster(
      ApplicationSubmissionContext appSubmissionContext,
      List<SubClusterId> blackListSubClusters) throws YarnException {

    // the maps are concurrent, but we need to protect from reset()
    // reinitialization mid-execution by creating a new reference local to this
    // method.
    Map<String, SubClusterPolicyConfiguration> cachedConfs = globalConfMap;
    Map<String, FederationRouterPolicy> policyMap = globalPolicyMap;

    if (appSubmissionContext == null) {
      throw new FederationPolicyException(
          "The ApplicationSubmissionContext " + "cannot be null.");
    }

    String queue = appSubmissionContext.getQueue();

    // respecting YARN behavior we assume default queue if the queue is not
    // specified. This also ensures that "null" can be used as a key to get the
    // default behavior.
    if (queue == null) {
      queue = YarnConfiguration.DEFAULT_QUEUE_NAME;
    }

    // the facade might cache this request, based on its parameterization
    SubClusterPolicyConfiguration configuration = null;

    try {
      configuration = federationFacade.getPolicyConfiguration(queue);
    } catch (YarnException e) {
      String errMsg = "There is no policy configured for the queue: " + queue
          + ", falling back to defaults.";
      LOG.warn(errMsg, e);
    }

    // If there is no policy configured for this queue, fallback to the baseline
    // policy that is configured either in the store or via XML config (and
    // cached)
    if (configuration == null) {
      LOG.warn("There is no policies configured for queue: " + queue + " we"
          + " fallback to default policy for: "
          + YarnConfiguration.DEFAULT_FEDERATION_POLICY_KEY);

      queue = YarnConfiguration.DEFAULT_FEDERATION_POLICY_KEY;
      try {
        configuration = federationFacade.getPolicyConfiguration(queue);
      } catch (YarnException e) {
        String errMsg = "Cannot retrieve policy configured for the queue: "
            + queue + ", falling back to defaults.";
        LOG.warn(errMsg, e);

      }
    }

    // the fallback is not configure via store, but via XML, using
    // previously loaded configuration.
    if (configuration == null) {
      configuration =
          cachedConfs.get(YarnConfiguration.DEFAULT_FEDERATION_POLICY_KEY);
    }

    // if the configuration has changed since last loaded, reinit the policy
    // based on current configuration
    if (!cachedConfs.containsKey(queue)
        || !cachedConfs.get(queue).equals(configuration)) {
      singlePolicyReinit(policyMap, cachedConfs, queue, configuration);
    }

    FederationRouterPolicy policy = policyMap.get(queue);
    if (policy == null) {
      // this should never happen, as the to maps are updated together
      throw new FederationPolicyException("No FederationRouterPolicy found "
          + "for queue: " + appSubmissionContext.getQueue() + " (for "
          + "application: " + appSubmissionContext.getApplicationId() + ") "
          + "and no default specified.");
    }

    return policy.getHomeSubcluster(appSubmissionContext, blackListSubClusters);
  }

  /**
   * This method reinitializes a policy and loads it in the policyMap.
   *
   * @param queue the queue to initialize a policy for.
   * @param conf the configuration to use for initalization.
   *
   * @throws FederationPolicyInitializationException if initialization fails.
   */
  private void singlePolicyReinit(Map<String, FederationRouterPolicy> policyMap,
      Map<String, SubClusterPolicyConfiguration> cachedConfs, String queue,
      SubClusterPolicyConfiguration conf)
      throws FederationPolicyInitializationException {

    FederationPolicyInitializationContext context =
        new FederationPolicyInitializationContext(conf, subClusterResolver,
            federationFacade, null);
    String newType = context.getSubClusterPolicyConfiguration().getType();
    FederationRouterPolicy routerPolicy = policyMap.get(queue);

    FederationPolicyManager federationPolicyManager =
        FederationPolicyUtils.instantiatePolicyManager(newType);
    // set queue, reinit policy if required (implementation lazily check
    // content of conf), and cache it
    federationPolicyManager.setQueue(queue);
    routerPolicy =
        federationPolicyManager.getRouterPolicy(context, routerPolicy);

    // we need the two put to be atomic (across multiple threads invoking
    // this and reset operations)
    synchronized (this) {
      policyMap.put(queue, routerPolicy);
      cachedConfs.put(queue, conf);
    }
  }

  /**
   * This method flushes all cached configurations and policies. This should be
   * invoked if the facade remains activity after very large churn of queues in
   * the system.
   */
  public synchronized void reset() {

    // remember the fallBack
    SubClusterPolicyConfiguration conf =
        globalConfMap.get(YarnConfiguration.DEFAULT_FEDERATION_POLICY_KEY);
    FederationRouterPolicy policy =
        globalPolicyMap.get(YarnConfiguration.DEFAULT_FEDERATION_POLICY_KEY);

    globalConfMap = new ConcurrentHashMap<>();
    globalPolicyMap = new ConcurrentHashMap<>();

    // add to the cache a fallback with keyword null
    globalConfMap.put(YarnConfiguration.DEFAULT_FEDERATION_POLICY_KEY, conf);
    globalPolicyMap.put(YarnConfiguration.DEFAULT_FEDERATION_POLICY_KEY,
        policy);

  }

}
