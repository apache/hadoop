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
import java.util.HashSet;

import static org.apache.hadoop.hdfs.server.federation.fairness.RouterRpcFairnessConstants.CONCURRENT_NS;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX;

/**
 * Static fairness policy extending @AbstractRouterRpcFairnessPolicyController
 * and fetching handlers from configuration for all available name services.
 * The handlers count will not change for this controller.
 */
public class StaticRouterRpcFairnessPolicyController extends
    AbstractRouterRpcFairnessPolicyController {

  private static final Logger LOG =
      LoggerFactory.getLogger(StaticRouterRpcFairnessPolicyController.class);

  public static final String ERROR_MSG = "Configured handlers "
      + DFS_ROUTER_HANDLER_COUNT_KEY + '='
      + " %d is less than the minimum required handlers %d";

  public StaticRouterRpcFairnessPolicyController(Configuration conf) {
    init(conf);
  }

  public void init(Configuration conf) throws IllegalArgumentException {
    super.init(conf);
    // Total handlers configured to process all incoming Rpc.
    int handlerCount = conf.getInt(DFS_ROUTER_HANDLER_COUNT_KEY, DFS_ROUTER_HANDLER_COUNT_DEFAULT);

    LOG.info("Handlers available for fairness assignment {} ", handlerCount);

    // Get all name services configured
    Set<String> allConfiguredNS = FederationUtil.getAllConfiguredNS(conf);

    // Set to hold name services that are not
    // configured with dedicated handlers.
    Set<String> unassignedNS = new HashSet<>();

    // Insert the concurrent nameservice into the set to process together
    allConfiguredNS.add(CONCURRENT_NS);
    validateHandlersCount(conf, handlerCount, allConfiguredNS);
    for (String nsId : allConfiguredNS) {
      int dedicatedHandlers = conf.getInt(DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX + nsId, 0);
      LOG.info("Dedicated handlers {} for ns {} ", dedicatedHandlers, nsId);
      if (dedicatedHandlers > 0) {
        handlerCount -= dedicatedHandlers;
        insertNameServiceWithPermits(nsId, dedicatedHandlers);
        logAssignment(nsId, dedicatedHandlers);
      } else {
        unassignedNS.add(nsId);
      }
    }

    // Assign remaining handlers equally to remaining name services and
    // general pool if applicable.
    if (!unassignedNS.isEmpty()) {
      LOG.info("Unassigned ns {}", unassignedNS);
      int handlersPerNS = handlerCount / unassignedNS.size();
      LOG.info("Handlers available per ns {}", handlersPerNS);
      for (String nsId : unassignedNS) {
        insertNameServiceWithPermits(nsId, handlersPerNS);
        logAssignment(nsId, handlersPerNS);
      }
    }

    // Assign remaining handlers if any to fan out calls.
    int leftOverHandlers = unassignedNS.isEmpty() ? handlerCount :
        handlerCount % unassignedNS.size();
    int existingPermits = getAvailablePermits(CONCURRENT_NS);
    if (leftOverHandlers > 0) {
      LOG.info("Assigned extra {} handlers to commons pool", leftOverHandlers);
      insertNameServiceWithPermits(CONCURRENT_NS, existingPermits + leftOverHandlers);
    }
    LOG.info("Final permit allocation for concurrent ns: {}", getAvailablePermits(CONCURRENT_NS));
  }

  private static void logAssignment(String nsId, int count) {
    LOG.info("Assigned {} handlers to nsId {} ", count, nsId);
  }

  private void validateHandlersCount(Configuration conf,
      int handlerCount, Set<String> allConfiguredNS) {
    int totalDedicatedHandlers = 0;
    for (String nsId : allConfiguredNS) {
      int dedicatedHandlers = conf.getInt(DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX + nsId, 0);
      if (dedicatedHandlers > 0) {
        // Total handlers should not be less than sum of dedicated handlers.
        totalDedicatedHandlers += dedicatedHandlers;
      } else {
        // Each NS should have at least one handler assigned.
        totalDedicatedHandlers++;
      }
    }
    if (totalDedicatedHandlers > handlerCount) {
      String msg = String.format(ERROR_MSG, handlerCount, totalDedicatedHandlers);
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }
  }

}
