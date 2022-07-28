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
package org.apache.hadoop.hdfs.server.federation.fairness;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.federation.router.FederationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;

import static org.apache.hadoop.hdfs.server.federation.fairness.RouterRpcFairnessConstants.CONCURRENT_NS;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_ELASTIC_PERMITS_PERCENT_DEFAULT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_ELASTIC_PERMITS_PERCENT_DEFAULT_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_ELASTIC_PERMITS_PERCENT_KEY_PREFIX;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HANDLER_COUNT_KEY;

/**
 * A subclass of RouterRpcFairnessPolicyController to flexible control
 * the number of permits that each nameservice can use.
 */
public class ElasticRouterRpcFairnessPolicyController
    extends AbstractRouterRpcFairnessPolicyController {

  private static final Logger LOG =
      LoggerFactory.getLogger(ElasticRouterRpcFairnessPolicyController.class);
  private Semaphore totalElasticPermits = null;

  public ElasticRouterRpcFairnessPolicyController(Configuration conf, int version) {
    super(version);
    init(conf);
  }

  private void init(Configuration conf) {
    if (conf == null) {
      conf = new HdfsConfiguration();
    }
    int newVersion = getVersion();
    Map<String, AbstractPermitManager> permitManagerMap = new HashMap<>();
    int handlerCount = conf.getInt(DFS_ROUTER_HANDLER_COUNT_KEY,
        DFS_ROUTER_HANDLER_COUNT_DEFAULT);

    LOG.info("Handlers available for fairness assignment {} ", handlerCount);

    Set<String> allConfiguredNS = FederationUtil.getAllConfiguredNS(conf);
    allConfiguredNS.add(CONCURRENT_NS);
    int totalDedicatedHandlers = validateHandlersCount(
        conf, handlerCount, allConfiguredNS);
    int totalElasticHandlers = handlerCount - totalDedicatedHandlers;

    if (totalElasticHandlers > 0) {
      this.totalElasticPermits = new Semaphore(totalElasticHandlers);
    }

    for (String nsId : allConfiguredNS) {
      int maximumETPermitsCanUse = getMaximumETPermitCanUseForTheNS(
          conf, nsId, totalElasticHandlers);
      int dedicatedHandlers = getDedicatedHandlers(conf, nsId);

      AbstractPermitManager permitManager = new ElasticPermitManager(
          nsId, dedicatedHandlers, maximumETPermitsCanUse,
          this.totalElasticPermits, newVersion);

      permitManagerMap.put(nsId, permitManager);
      LOG.info("Assigned {} dedicatedPermits and {} maximumETPermits for nsId {} ",
          dedicatedHandlers, maximumETPermitsCanUse, nsId);
    }
    initPermits(permitManagerMap);
  }

  /**
   * Get the maximum of elastic Permits that the ns can use.
   * @param totalETPermits total number of elastic permits.
   * @return long
   */
  private int getMaximumETPermitCanUseForTheNS(
      Configuration conf, String nsId, int totalETPermits) {
    int defaultElasticPermitPercent = conf.getInt(
        DFS_ROUTER_ELASTIC_PERMITS_PERCENT_DEFAULT_KEY,
        DFS_ROUTER_ELASTIC_PERMITS_PERCENT_DEFAULT);
    int maxPercent = conf.getInt(
        DFS_ROUTER_ELASTIC_PERMITS_PERCENT_KEY_PREFIX + nsId,
        defaultElasticPermitPercent);
    if (maxPercent < 0) {
      maxPercent = 0;
    }
    return (int) Math.ceil(totalETPermits * (1.0 * Math.min(maxPercent, 100) / 100));
  }

  @Override
  public void shutdown() {
    super.shutdown();
    if (this.totalElasticPermits != null) {
      totalElasticPermits.drainPermits();
    }
  }

  @VisibleForTesting
  int getTotalElasticAvailableHandler() {
    int available = 0;
    if (this.totalElasticPermits != null) {
      available = this.totalElasticPermits.availablePermits();
    }
    return available;
  }
}
