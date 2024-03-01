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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.router.FederationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Set;

import static org.apache.hadoop.hdfs.server.federation.fairness.RouterRpcFairnessConstants.CONCURRENT_NS;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FAIR_HANDLER_PROPORTION_DEFAULT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FAIR_HANDLER_PROPORTION_KEY_PREFIX;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HANDLER_COUNT_KEY;

/**
 * Proportion fairness policy extending {@link AbstractRouterRpcFairnessPolicyController}
 * and fetching proportion of handlers from configuration for all available name services,
 * based on the proportion and the total number of handlers, calculate the handlers of all ns.
 * The handlers count will not change for this controller.
 */
public class ProportionRouterRpcFairnessPolicyController extends
    AbstractRouterRpcFairnessPolicyController{

  private static final Logger LOG =
      LoggerFactory.getLogger(ProportionRouterRpcFairnessPolicyController.class);
  // For unregistered ns, the default ns is used,
  // so the configuration can be simplified if the handler ratio of all ns is 1,
  // and transparent expansion of new ns can be supported.
  private static final String DEFAULT_NS = "default_ns";

  public ProportionRouterRpcFairnessPolicyController(Configuration conf){
    init(conf);
  }

  @Override
  public void init(Configuration conf) {
    super.init(conf);
    // Total handlers configured to process all incoming Rpc.
    int handlerCount = conf.getInt(DFS_ROUTER_HANDLER_COUNT_KEY, DFS_ROUTER_HANDLER_COUNT_DEFAULT);

    LOG.info("Handlers available for fairness assignment {} ", handlerCount);

    // Get all name services configured
    Set<String> allConfiguredNS = FederationUtil.getAllConfiguredNS(conf);

    // Insert the concurrent nameservice into the set to process together
    allConfiguredNS.add(CONCURRENT_NS);

    // Insert the default nameservice into the set to process together
    allConfiguredNS.add(DEFAULT_NS);
    for (String nsId : allConfiguredNS) {
      double dedicatedHandlerProportion = conf.getDouble(
          DFS_ROUTER_FAIR_HANDLER_PROPORTION_KEY_PREFIX + nsId,
                DFS_ROUTER_FAIR_HANDLER_PROPORTION_DEFAULT);
      int dedicatedHandlers = (int) (dedicatedHandlerProportion * handlerCount);
      LOG.info("Dedicated handlers {} for ns {} ", dedicatedHandlers, nsId);
      // Each NS should have at least one handler assigned.
      if (dedicatedHandlers <= 0) {
        dedicatedHandlers = 1;
      }
      insertNameServiceWithPermits(nsId, dedicatedHandlers);
      LOG.info("Assigned {} handlers to nsId {} ", dedicatedHandlers, nsId);
    }
  }

  @Override
  public boolean acquirePermit(String nsId) {
    if (contains(nsId)) {
      return super.acquirePermit(nsId);
    }
    return super.acquirePermit(DEFAULT_NS);
  }

  @Override
  public void releasePermit(String nsId) {
    if (contains(nsId)) {
      super.releasePermit(nsId);
    }
    super.releasePermit(DEFAULT_NS);
  }
}
