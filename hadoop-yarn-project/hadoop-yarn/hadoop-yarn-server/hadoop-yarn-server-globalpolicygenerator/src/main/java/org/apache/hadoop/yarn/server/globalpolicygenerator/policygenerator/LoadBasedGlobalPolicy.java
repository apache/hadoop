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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
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

/**
 * Load based policy that generates weighted policies by scaling
 * the cluster load (based on pending) to a weight from 0.0 to 1.0.
 */
public class LoadBasedGlobalPolicy extends GlobalPolicy {
  private static final Logger LOG =
      LoggerFactory.getLogger(LoadBasedGlobalPolicy.class);

  private static final String FEDERATION_GPG_LOAD_BASED_PREFIX =
      YarnConfiguration.FEDERATION_GPG_PREFIX + "policy.generator.load-based.";

  public static final String FEDERATION_GPG_LOAD_BASED_MIN_PENDING =
      FEDERATION_GPG_LOAD_BASED_PREFIX + "pending.minimum";
  public static final int DEFAULT_FEDERATION_GPG_LOAD_BASED_MIN_PENDING = 100;

  public static final String FEDERATION_GPG_LOAD_BASED_MAX_PENDING =
      FEDERATION_GPG_LOAD_BASED_PREFIX + "pending.maximum";
  public static final int DEFAULT_FEDERATION_GPG_LOAD_BASED_MAX_PENDING = 1000;

  public static final String FEDERATION_GPG_LOAD_BASED_MIN_WEIGHT =
      FEDERATION_GPG_LOAD_BASED_PREFIX + "weight.minimum";
  public static final float DEFAULT_FEDERATION_GPG_LOAD_BASED_MIN_WEIGHT = 0.0f;

  public static final String FEDERATION_GPG_LOAD_BASED_MAX_EDIT =
      FEDERATION_GPG_LOAD_BASED_PREFIX + "edit.maximum";
  public static final int DEFAULT_FEDERATION_GPG_LOAD_BASED_MAX_EDIT = 3;

  public static final String FEDERATION_GPG_LOAD_BASED_SCALING =
      FEDERATION_GPG_LOAD_BASED_PREFIX + "scaling";
  public static final String DEFAULT_FEDERATION_GPG_LOAD_BASED_SCALING =
      Scaling.LINEAR.name();

  public enum Scaling {
    LINEAR,
    QUADRATIC,
    LOG,
    NONE
  }

  // Minimum pending count before the policy starts scaling down the weights
  private int minPending;
  // Maximum pending count before policy stops scaling down the weights
  //(they'll be set to min weight)
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
      throw new YarnRuntimeException("minPending=" + minPending
          + " must be less than or equal to maxPending=" + maxPending);
    }
    if (!(minWeight >= 0 && minWeight < 1)) {
      throw new YarnRuntimeException(
          "minWeight=" + minWeight + " must be within range [0,1)");
    }
  }

  @Override
  protected Map<Class<?>, String> registerPaths() {
    // Register for the endpoints we want to receive information on
    Map<Class<?>, String> map = new HashMap<>();
    map.put(ClusterMetricsInfo.class, RMWSConsts.METRICS);
    return map;
  }

  @Override
  protected FederationPolicyManager updatePolicy(String queueName,
      Map<SubClusterId, Map<Class, Object>> clusterInfo,
      FederationPolicyManager currentManager) {
    Map<SubClusterId, ClusterMetricsInfo> clusterMetrics = new HashMap<>();
    for (Map.Entry<SubClusterId, Map<Class, Object>> e : clusterInfo
        .entrySet()) {
      clusterMetrics.put(e.getKey(),
          (ClusterMetricsInfo) e.getValue().get(ClusterMetricsInfo.class));
    }
    if (currentManager == null) {
      LOG.info("Creating load based weighted policy queue {}", queueName);
      Map<SubClusterIdInfo, Float> weights = getTargetWeights(clusterMetrics);
      WeightedLocalityPolicyManager manager =
          new WeightedLocalityPolicyManager();
      manager.setQueue(queueName);
      manager.getWeightedPolicyInfo().setAMRMPolicyWeights(weights);
      manager.getWeightedPolicyInfo().setRouterPolicyWeights(weights);
      currentManager = manager;
    } else if (currentManager instanceof WeightedLocalityPolicyManager) {
      Map<SubClusterIdInfo, Float> weights = getTargetWeights(clusterMetrics);
      LOG.info("Updating policy for queue {} based on cluster load to: {}",
          queueName, weights);
      WeightedLocalityPolicyManager manager =
          (WeightedLocalityPolicyManager) currentManager;
      manager.getWeightedPolicyInfo().setAMRMPolicyWeights(weights);
      manager.getWeightedPolicyInfo().setRouterPolicyWeights(weights);
    } else {
      LOG.warn("Policy for queue {} is of type {}, expected {}", queueName,
          currentManager.getClass(), WeightedLocalityPolicyManager.class);
    }
    return currentManager;
  }

  @VisibleForTesting
  protected Map<SubClusterIdInfo, Float> getTargetWeights(
      Map<SubClusterId, ClusterMetricsInfo> clusterMetrics) {
    Map<SubClusterIdInfo, Float> weights =
        GPGUtils.createUniformWeights(clusterMetrics.keySet());
    List<SubClusterId> scs = new ArrayList<>(clusterMetrics.keySet());
    // Sort the sub clusters into descending order based on pending load
    scs.sort(new SortByDescendingLoad(clusterMetrics));
    // Keep the top N loaded sub clusters
    scs = scs.subList(0, Math.min(maxEdit, scs.size()));
    for (SubClusterId sc : scs) {
      LOG.info("Updating weight for sub cluster {}", sc.toString());
      int pending = clusterMetrics.get(sc).getAppsPending();
      if (pending <= minPending) {
        LOG.info("Load ({}) is lower than minimum ({}), skipping", pending,
            minPending);
      } else if (pending < maxPending) {
        float weight = 1.0f;
        // The different scaling strategies should all map values from the
        // range min_pending+1 to max_pending to the range min_weight to 1.0f
        // so we pre process and simplify the domain to some value [1, MAX-MIN)
        int val = pending - minPending;
        int maxVal = maxPending - minPending;
        switch (scaling) {
        case NONE:
          break;
        case LINEAR:
          weight = (float) (maxVal - val) / (float) (maxVal);
          break;
        case QUADRATIC:
          double maxValQuad = Math.pow(maxVal, 2);
          double valQuad = Math.pow(val, 2);
          weight = (float) (maxValQuad - valQuad) / (float) (maxValQuad);
          break;
        case LOG:
          double maxValLog = Math.log(maxVal);
          double valLog = Math.log(val);
          weight = (float) (maxValLog - valLog) / (float) (maxValLog);
          break;
        }
        // Scale the weights to respect the config minimum
        weight = weight * (1.0f - minWeight);
        weight += minWeight;
        weights.put(new SubClusterIdInfo(sc), weight);
        LOG.info("Load ({}) is within maximum ({}), setting weights via {} "
            + "scale to {}", pending, maxPending, scaling, weight);
      } else {
        weights.put(new SubClusterIdInfo(sc), minWeight);
        LOG.info(
            "Load ({}) exceeded maximum ({}), setting weight to minimum: {}",
            pending, maxPending, minWeight);
      }
    }
    validateWeights(weights);
    return weights;
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
    LOG.warn("All " + weights.size()
        + " generated weights were 0.0f. Resetting to 1.0f");
    // All weights were zero. Reset all back to 1.0
    for(SubClusterIdInfo id : weights.keySet()) {
      weights.put(id, 1.0f);
    }
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