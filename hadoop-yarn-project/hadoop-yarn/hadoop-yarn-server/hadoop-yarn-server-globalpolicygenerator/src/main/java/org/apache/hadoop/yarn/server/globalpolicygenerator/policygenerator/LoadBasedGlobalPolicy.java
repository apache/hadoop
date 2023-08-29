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

import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.federation.policies.manager.FederationPolicyManager;
import org.apache.hadoop.yarn.server.federation.policies.manager.WeightedLocalityPolicyManager;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GPGUtils;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.FEDERATION_GPG_LOAD_BASED_MIN_PENDING;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_FEDERATION_GPG_LOAD_BASED_MIN_PENDING;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.FEDERATION_GPG_LOAD_BASED_MAX_PENDING;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_FEDERATION_GPG_LOAD_BASED_MAX_PENDING;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.FEDERATION_GPG_LOAD_BASED_MIN_WEIGHT;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_FEDERATION_GPG_LOAD_BASED_MIN_WEIGHT;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.FEDERATION_GPG_LOAD_BASED_MAX_EDIT;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_FEDERATION_GPG_LOAD_BASED_MAX_EDIT;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.FEDERATION_GPG_LOAD_BASED_SCALING;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_FEDERATION_GPG_LOAD_BASED_SCALING;

/**
 * Load based policy that generates weighted policies by scaling
 * the cluster load (based on pending) to a weight from 0.0 to 1.0.
 */
public class LoadBasedGlobalPolicy extends GlobalPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(LoadBasedGlobalPolicy.class);

  public enum Scaling {
    LINEAR,
    QUADRATIC,
    LOG,
    NONE
  }

  // Minimum pending count before the policy starts scaling down the weights
  private int minPending;
  // Maximum pending count before policy stops scaling down the weights
  // (they'll be set to min weight)
  private int maxPending;
  // Minimum weight that a sub cluster will be assigned
  private float minWeight;
  // Maximum number of weights that can be scaled down simultaneously
  private int maxEdit;
  // Scaling type
  private Scaling scaling = Scaling.NONE;

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    minPending = conf.getInt(FEDERATION_GPG_LOAD_BASED_MIN_PENDING,
        DEFAULT_FEDERATION_GPG_LOAD_BASED_MIN_PENDING);
    maxPending = conf.getInt(FEDERATION_GPG_LOAD_BASED_MAX_PENDING,
        DEFAULT_FEDERATION_GPG_LOAD_BASED_MAX_PENDING);
    minWeight = conf.getFloat(FEDERATION_GPG_LOAD_BASED_MIN_WEIGHT,
        DEFAULT_FEDERATION_GPG_LOAD_BASED_MIN_WEIGHT);
    maxEdit = conf.getInt(FEDERATION_GPG_LOAD_BASED_MAX_EDIT,
        DEFAULT_FEDERATION_GPG_LOAD_BASED_MAX_EDIT);

    try {
      scaling = Scaling.valueOf(conf.get(FEDERATION_GPG_LOAD_BASED_SCALING,
          DEFAULT_FEDERATION_GPG_LOAD_BASED_SCALING));
    } catch (IllegalArgumentException e) {
      LOG.warn("Invalid scaling mode provided", e);
    }

    // Check that all configuration values are valid
    if (!(minPending <= maxPending)) {
      throw new YarnRuntimeException("minPending = " + minPending
          + " must be less than or equal to maxPending=" + maxPending);
    }
    if (!(minWeight >= 0 && minWeight < 1)) {
      throw new YarnRuntimeException(
          "minWeight = " + minWeight + " must be within range [0,1)");
    }
  }

  @Override
  protected Map<Class<?>, String> registerPaths() {
    // Register for the endpoints we want to receive information on
    Map<Class<?>, String> map = new HashMap<>();
    map.put(ClusterMetricsInfo.class, RMWSConsts.METRICS);
    return map;
  }

  /**
   * Update the policy of the queue.
   *
   * @param queueName   name of the queue
   * @param clusterInfo subClusterId map to cluster information about the
   *                    SubCluster used to make policy decisions
   * @param currentManager the FederationPolicyManager for the queue's existing
   * policy the manager may be null, in which case the policy
   * will need to be created.
   *
   * @return FederationPolicyManager.
   */
  @Override
  protected FederationPolicyManager updatePolicy(String queueName,
      Map<SubClusterId, Map<Class, Object>> clusterInfo,
      FederationPolicyManager currentManager) {
    if (currentManager == null) {
      LOG.info("Creating load based weighted policy queue {}.", queueName);
      currentManager = getWeightedLocalityPolicyManager(queueName, clusterInfo);
    } else if (currentManager instanceof WeightedLocalityPolicyManager) {
      LOG.info("Updating load based weighted policy queue {}.", queueName);
      currentManager = getWeightedLocalityPolicyManager(queueName, clusterInfo);
    } else {
      LOG.warn("Policy for queue {} is of type {}, expected {}.", queueName,
          currentManager.getClass(), WeightedLocalityPolicyManager.class);
    }
    return currentManager;
  }

  /**
   * GPG can help update the policy of the queue.
   *
   * We automatically generate the weight of the subCluster
   * according to the clusterMetrics of the subCluster.
   *
   * @param queue queueName.
   * @param subClusterMetricInfos Metric information of the subCluster.
   * @return WeightedLocalityPolicyManager.
   */
  protected WeightedLocalityPolicyManager getWeightedLocalityPolicyManager(String queue,
      Map<SubClusterId, Map<Class, Object>> subClusterMetricInfos) {

    // Parse the metric information of the subCluster.
    Map<SubClusterId, ClusterMetricsInfo> clusterMetrics =
        getSubClustersMetricsInfo(subClusterMetricInfos);

    if (MapUtils.isEmpty(clusterMetrics)) {
      return null;
    }

    // Get the new weight of the subCluster.
    WeightedLocalityPolicyManager manager = new WeightedLocalityPolicyManager();
    Map<SubClusterIdInfo, Float> weights = getTargetWeights(clusterMetrics);
    manager.setQueue(queue);
    manager.getWeightedPolicyInfo().setAMRMPolicyWeights(weights);
    manager.getWeightedPolicyInfo().setRouterPolicyWeights(weights);
    return manager;
  }

  /**
   * Get the ClusterMetric information of the subCluster.
   *
   * @param subClusterMetricsInfo subCluster Metric Information.
   * @return Mapping relationship between subCluster and Metric.
   */
  protected Map<SubClusterId, ClusterMetricsInfo> getSubClustersMetricsInfo(
      Map<SubClusterId, Map<Class, Object>> subClusterMetricsInfo) {

    // Check whether the Metric information of the sub-cluster is empty,
    // if it is empty, we will directly return null.
    if(MapUtils.isEmpty(subClusterMetricsInfo)) {
      LOG.warn("The metric info of the subCluster is empty.");
      return null;
    }

    Map<SubClusterId, ClusterMetricsInfo> clusterMetrics = new HashMap<>();
    for (Map.Entry<SubClusterId, Map<Class, Object>> entry : subClusterMetricsInfo.entrySet()) {
      SubClusterId subClusterId = entry.getKey();
      Map<Class, Object> subClusterMetrics = entry.getValue();
      ClusterMetricsInfo clusterMetricsInfo = (ClusterMetricsInfo)
          subClusterMetrics.getOrDefault(ClusterMetricsInfo.class, null);
      clusterMetrics.put(subClusterId, clusterMetricsInfo);
    }

    // return subCluster Metric Information.
    return clusterMetrics;
  }

  /**
   * Get subCluster target weight.
   *
   * @param clusterMetrics Metric of the subCluster.
   * @return subCluster Weights.
   */
  @VisibleForTesting
  protected Map<SubClusterIdInfo, Float> getTargetWeights(
      Map<SubClusterId, ClusterMetricsInfo> clusterMetrics) {
    Map<SubClusterIdInfo, Float> weights = GPGUtils.createUniformWeights(clusterMetrics.keySet());

    List<SubClusterId> scs = new ArrayList<>(clusterMetrics.keySet());
    // Sort the sub clusters into descending order based on pending load
    scs.sort(new SortByDescendingLoad(clusterMetrics));

    // Keep the top N loaded sub clusters
    scs = scs.subList(0, Math.min(maxEdit, scs.size()));

    for (SubClusterId sc : scs) {
      LOG.info("Updating weight for sub cluster {}", sc.toString());
      int pending = clusterMetrics.get(sc).getAppsPending();
      if (pending <= minPending) {
        LOG.info("Load ({}) is lower than minimum ({}), skipping", pending, minPending);
      } else if (pending < maxPending) {
        // The different scaling strategies should all map values from the
        // range min_pending+1 to max_pending to the range min_weight to 1.0f
        // so we pre-process and simplify the domain to some value [1, MAX-MIN)
        int val = pending - minPending;
        int maxVal = maxPending - minPending;

        // Scale the weights to respect the config minimum
        float weight = getWeightByScaling(maxVal, val);
        weight = weight * (1.0f - minWeight);
        weight += minWeight;
        weights.put(new SubClusterIdInfo(sc), weight);
        LOG.info("Load ({}) is within maximum ({}), setting weights via {} "
            + "scale to {}", pending, maxPending, scaling, weight);
      } else {
        weights.put(new SubClusterIdInfo(sc), minWeight);
        LOG.info("Load ({}) exceeded maximum ({}), setting weight to minimum: {}",
            pending, maxPending, minWeight);
      }
    }
    validateWeights(weights);
    return weights;
  }

  /**
   * Get weight information.
   * We will calculate the weight information according to different Scaling.
   *
   * NONE: No calculation is required, and the weight is 1 at this time.
   *
   * LINEAR: For linear computation, we will use (maxPendingVal - curPendingVal) / (maxPendingVal).
   *
   * QUADRATIC: Calculated using quadratic,
   * We will calculate quadratic for maxPendingVal, curPendingVal,
   * then use this formula = (maxPendingVal - curPendingVal) / (maxPendingVal).
   *
   * LOG(LOGARITHM): Calculated using logarithm,
   * We will calculate logarithm for maxPendingVal, curPendingVal,
   * then use this formula = (maxPendingVal - curPendingVal) / (maxPendingVal).
   *
   * @param maxPendingVal maxPending - minPending
   * @param curPendingVal pending - minPending
   * @return Calculated weight information.
   */
  protected float getWeightByScaling(int maxPendingVal, int curPendingVal) {
    float weight = 1.0f;
    switch (scaling) {
    case NONE:
      break;
    case LINEAR:
      weight = (float) (maxPendingVal - curPendingVal) / (float) (maxPendingVal);
      break;
    case QUADRATIC:
      double maxValQuad = Math.pow(maxPendingVal, 2);
      double valQuad = Math.pow(curPendingVal, 2);
      weight = (float) (maxValQuad - valQuad) / (float) (maxValQuad);
      break;
    case LOG:
      double maxValLog = Math.log(maxPendingVal);
      double valLog = Math.log(curPendingVal);
      weight = (float) (maxValLog - valLog) / (float) (maxValLog);
      break;
    default:
      LOG.warn("No suitable scaling found, Skip.");
      break;
    }
    return weight;
  }

  /**
   * Helper to avoid all zero weights. If weights are all zero, they're reset
   * to one
   * @param weights weights to validate
   */
  private void validateWeights(Map<SubClusterIdInfo, Float> weights) {
    for(Float w : weights.values()) {
      // If we find a nonzero weight, we're validated
      if(w > 0.0f) {
        return;
      }
    }
    LOG.warn("All {} generated weights were 0.0f. Resetting to 1.0f.", weights.size());
    // All weights were zero. Reset all back to 1.0
    weights.replaceAll((i, v) -> 1.0f);
  }

  private static final class SortByDescendingLoad
      implements Comparator<SubClusterId> {

    private Map<SubClusterId, ClusterMetricsInfo> clusterMetrics;

    private SortByDescendingLoad(
        Map<SubClusterId, ClusterMetricsInfo> clusterMetrics) {
      this.clusterMetrics = clusterMetrics;
    }

    public int compare(SubClusterId a, SubClusterId b) {
      // Sort by pending load
      return clusterMetrics.get(b).getAppsPending() - clusterMetrics.get(a)
          .getAppsPending();
    }
  }
}