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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.FEDERATION_GPG_LOAD_BASED_MAX_EDIT;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.FEDERATION_GPG_LOAD_BASED_MIN_PENDING;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.FEDERATION_GPG_LOAD_BASED_MAX_PENDING;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.FEDERATION_GPG_LOAD_BASED_MIN_WEIGHT;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.FEDERATION_GPG_LOAD_BASED_SCALING;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_FEDERATION_GPG_LOAD_BASED_SCALING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for the Load Based Global Policy.
 */
public class TestLoadBasedGlobalPolicy {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestLoadBasedGlobalPolicy.class);

  private static final int NUM_SC = 3;
  private static final float DELTA = 0.00001f;

  private static final int MIN_PENDING = 100;
  private static final int MAX_PENDING = 500;

  private List<SubClusterId> subClusterIds;
  private Map<SubClusterId, ClusterMetricsInfo> clusterMetricsInfos;
  private Map<SubClusterIdInfo, Float> weights;

  private final Configuration conf;
  private final LoadBasedGlobalPolicy policyGenerator;

  public TestLoadBasedGlobalPolicy() {
    conf = new Configuration();
    policyGenerator = new LoadBasedGlobalPolicy();
  }

  @Before
  public void setUp() {

    conf.setInt(FEDERATION_GPG_LOAD_BASED_MAX_EDIT, 2);
    conf.setInt(FEDERATION_GPG_LOAD_BASED_MIN_PENDING, MIN_PENDING);
    conf.setInt(FEDERATION_GPG_LOAD_BASED_MAX_PENDING, MAX_PENDING);
    conf.setFloat(FEDERATION_GPG_LOAD_BASED_MIN_WEIGHT, 0.0f);
    conf.set(FEDERATION_GPG_LOAD_BASED_SCALING, LoadBasedGlobalPolicy.Scaling.LINEAR.name());
    policyGenerator.setConf(conf);

    subClusterIds = new ArrayList<>();
    clusterMetricsInfos = new HashMap<>();
    // Set up sub clusters
    for (int i = 0; i < NUM_SC; ++i) {
      // subClusterId
      SubClusterId id = SubClusterId.newInstance("sc" + i);
      subClusterIds.add(id);

      // Cluster metrics info
      ClusterMetricsInfo metricsInfo = new ClusterMetricsInfo();
      metricsInfo.setAppsPending(50);
      clusterMetricsInfos.put(id, metricsInfo);
    }
  }

  @Test
  public void testSimpleTargetWeights() {
    weights = policyGenerator.getTargetWeights(clusterMetricsInfos);
    assertEquals(weights.size(), 3);
    assertEquals(1.0, getWeight(0), DELTA);
    assertEquals(1.0, getWeight(1), DELTA);
    assertEquals(1.0, getWeight(2), DELTA);
  }

  @Test
  public void testLoadTargetWeights() {
    getMetric(0).setAppsPending(100);
    weights = policyGenerator.getTargetWeights(clusterMetricsInfos);
    assertEquals(weights.size(), 3);
    assertEquals(1.0, getWeight(0), DELTA);
    assertEquals(1.0, getWeight(1), DELTA);
    assertEquals(1.0, getWeight(2), DELTA);
    getMetric(0).setAppsPending(500);
    weights = policyGenerator.getTargetWeights(clusterMetricsInfos);
    assertEquals(weights.size(), 3);
    assertEquals(0.0, getWeight(0), DELTA);
    assertEquals(1.0, getWeight(1), DELTA);
    assertEquals(1.0, getWeight(2), DELTA);
  }

  @Test
  public void testMaxEdit() {
    // The policy should be able to edit 2 weights
    getMetric(0).setAppsPending(MAX_PENDING + 200);
    getMetric(1).setAppsPending(MAX_PENDING + 100);
    weights = policyGenerator.getTargetWeights(clusterMetricsInfos);
    assertEquals(weights.size(), 3);
    assertEquals(0.0, getWeight(0), DELTA);
    assertEquals(0.0, getWeight(1), DELTA);
    assertEquals(1.0, getWeight(2), DELTA);
    // After updating the config, it should only edit the most loaded
    conf.setInt(FEDERATION_GPG_LOAD_BASED_MAX_EDIT, 1);
    policyGenerator.setConf(conf);
    weights = policyGenerator.getTargetWeights(clusterMetricsInfos);
    assertEquals(weights.size(), 3);
    assertEquals(0.0, getWeight(0), DELTA);
    assertEquals(1.0, getWeight(1), DELTA);
    assertEquals(1.0, getWeight(2), DELTA);
  }

  @Test
  public void testMinWeight() {
    // If a minimum weight is set, the generator should not go below it
    conf.setFloat(FEDERATION_GPG_LOAD_BASED_MIN_WEIGHT, 0.5f);
    policyGenerator.setConf(conf);
    getMetric(0).setAppsPending(Integer.MAX_VALUE);
    weights = policyGenerator.getTargetWeights(clusterMetricsInfos);
    assertEquals(weights.size(), 3);
    assertEquals(0.5, getWeight(0), DELTA);
    assertEquals(1.0, getWeight(1), DELTA);
    assertEquals(1.0, getWeight(2), DELTA);
  }

  @Test
  public void testScaling() {
    LOG.info("Testing that the generator weights are monotonically"
        + " decreasing regardless of scaling method");
    for (LoadBasedGlobalPolicy.Scaling scaling :
        new LoadBasedGlobalPolicy.Scaling[] {LoadBasedGlobalPolicy.Scaling.LINEAR,
            LoadBasedGlobalPolicy.Scaling.QUADRATIC, LoadBasedGlobalPolicy.Scaling.LOG }) {
      LOG.info("Testing {} scaling...", scaling);
      conf.set(DEFAULT_FEDERATION_GPG_LOAD_BASED_SCALING, scaling.name());
      policyGenerator.setConf(conf);
      // Test a continuous range for scaling
      float prevWeight = 1.01f;
      for (int load = 0; load < MAX_PENDING * 2; ++load) {
        getMetric(0).setAppsPending(load);
        weights = policyGenerator.getTargetWeights(clusterMetricsInfos);
        if (load < MIN_PENDING) {
          // Below the minimum load, it should stay 1.0f
          assertEquals(1.0f, getWeight(0), DELTA);
        } else if (load < MAX_PENDING) {
          // In the specified range, the weight should consistently decrease
          float weight = getWeight(0);
          assertTrue(weight < prevWeight);
          prevWeight = weight;
        } else {
          // Above the maximum load, it should stay 0.0f
          assertEquals(0.0f, getWeight(0), DELTA);
        }
      }
    }
  }

  @Test
  public void testNonZero() {
    // If all generated weights are zero, they should be set back to one
    conf.setFloat(FEDERATION_GPG_LOAD_BASED_MIN_WEIGHT, 0.0f);
    conf.setInt(FEDERATION_GPG_LOAD_BASED_MAX_EDIT, 3);
    policyGenerator.setConf(conf);
    getMetric(0).setAppsPending(Integer.MAX_VALUE);
    getMetric(1).setAppsPending(Integer.MAX_VALUE);
    getMetric(2).setAppsPending(Integer.MAX_VALUE);
    weights = policyGenerator.getTargetWeights(clusterMetricsInfos);
    assertEquals(weights.size(), 3);
    assertEquals(1.0, getWeight(0), DELTA);
    assertEquals(1.0, getWeight(1), DELTA);
    assertEquals(1.0, getWeight(2), DELTA);
  }

  private float getWeight(int sc) {
    return weights.get(new SubClusterIdInfo(subClusterIds.get(sc)));
  }

  private ClusterMetricsInfo getMetric(int sc) {
    return clusterMetricsInfos.get(subClusterIds.get(sc));
  }
}
