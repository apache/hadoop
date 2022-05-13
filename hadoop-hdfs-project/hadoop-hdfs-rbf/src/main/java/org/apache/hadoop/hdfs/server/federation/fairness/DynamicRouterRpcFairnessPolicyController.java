/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.federation.fairness;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.utils.AdjustableSemaphore;
import org.apache.hadoop.util.concurrent.HadoopExecutors;

import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_DYNAMIC_FAIRNESS_CONTROLLER_REFRESH_INTERVAL_SECONDS_DEFAULT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_DYNAMIC_FAIRNESS_CONTROLLER_REFRESH_INTERVAL_SECONDS_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FAIR_MINIMUM_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FAIR_MINIMUM_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HANDLER_COUNT_KEY;

/**
 * Dynamic fairness policy extending {@link StaticRouterRpcFairnessPolicyController}
 * and fetching handlers from configuration for all available name services.
 * The handlers count changes according to traffic to namespaces.
 * Total handlers might NOT strictly add up to the value defined by DFS_ROUTER_HANDLER_COUNT_KEY
 * but will not exceed initial handler count + number of nameservices.
 */
public class DynamicRouterRpcFairnessPolicyController
    extends StaticRouterRpcFairnessPolicyController {

  private static final Logger LOG =
      LoggerFactory.getLogger(DynamicRouterRpcFairnessPolicyController.class);

  private static final ScheduledExecutorService SCHEDULED_EXECUTOR = HadoopExecutors
      .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true)
          .setNameFormat("DynamicRouterRpcFairnessPolicyControllerPermitsResizer").build());
  private PermitsResizerService permitsResizerService;
  private ScheduledFuture<?> refreshTask;
  private int handlerCount;
  private int minimumHandlerPerNs;

  /**
   * Initializes using the same logic as {@link StaticRouterRpcFairnessPolicyController}
   * and starts a periodic semaphore resizer thread.
   *
   * @param conf configuration
   */
  public DynamicRouterRpcFairnessPolicyController(Configuration conf) {
    super(conf);
    minimumHandlerPerNs = conf.getInt(DFS_ROUTER_FAIR_MINIMUM_HANDLER_COUNT_KEY,
        DFS_ROUTER_FAIR_MINIMUM_HANDLER_COUNT_DEFAULT);
    handlerCount = conf.getInt(DFS_ROUTER_HANDLER_COUNT_KEY, DFS_ROUTER_HANDLER_COUNT_DEFAULT);
    long refreshInterval =
        conf.getTimeDuration(DFS_ROUTER_DYNAMIC_FAIRNESS_CONTROLLER_REFRESH_INTERVAL_SECONDS_KEY,
            DFS_ROUTER_DYNAMIC_FAIRNESS_CONTROLLER_REFRESH_INTERVAL_SECONDS_DEFAULT,
            TimeUnit.SECONDS);
    permitsResizerService = new PermitsResizerService();
    refreshTask = SCHEDULED_EXECUTOR
        .scheduleWithFixedDelay(permitsResizerService, refreshInterval, refreshInterval,
            TimeUnit.SECONDS);
  }

  @VisibleForTesting
  public DynamicRouterRpcFairnessPolicyController(Configuration conf, long refreshInterval) {
    super(conf);
    minimumHandlerPerNs = conf.getInt(DFS_ROUTER_FAIR_MINIMUM_HANDLER_COUNT_KEY,
        DFS_ROUTER_FAIR_MINIMUM_HANDLER_COUNT_DEFAULT);
    handlerCount = conf.getInt(DFS_ROUTER_HANDLER_COUNT_KEY, DFS_ROUTER_HANDLER_COUNT_DEFAULT);
    permitsResizerService = new PermitsResizerService();
    refreshTask = SCHEDULED_EXECUTOR
        .scheduleWithFixedDelay(permitsResizerService, refreshInterval, refreshInterval,
            TimeUnit.SECONDS);
  }

  @VisibleForTesting
  public void refreshPermitsCap() {
    permitsResizerService.run();
  }

  @Override
  public void shutdown() {
    super.shutdown();
    if (refreshTask != null) {
      refreshTask.cancel(true);
    }
    if (SCHEDULED_EXECUTOR != null) {
      SCHEDULED_EXECUTOR.shutdown();
    }
  }

  class PermitsResizerService implements Runnable {

    @Override
    public synchronized void run() {
      if ((getRejectedPermitsPerNs() == null) || (getAcceptedPermitsPerNs() == null)) {
        return;
      }
      long totalOps = 0;
      Map<String, Long> nsOps = new HashMap<>();
      for (Map.Entry<String, AdjustableSemaphore> entry : getPermits().entrySet()) {
        long ops = (getRejectedPermitsPerNs().containsKey(entry.getKey()) ?
            getRejectedPermitsPerNs().get(entry.getKey()).longValue() :
            0) + (getAcceptedPermitsPerNs().containsKey(entry.getKey()) ?
            getAcceptedPermitsPerNs().get(entry.getKey()).longValue() :
            0);
        nsOps.put(entry.getKey(), ops);
        totalOps += ops;
      }

      List<String> underMinimumNss = new ArrayList<>();
      List<String> overMinimumNss = new ArrayList<>();
      int effectiveOps = 0;

      // First iteration: split namespaces into those underused and those that are not.
      for (Map.Entry<String, AdjustableSemaphore> entry : getPermits().entrySet()) {
        String ns = entry.getKey();
        int newPermitCap = (int) Math.ceil((float) nsOps.get(ns) / totalOps * handlerCount);

        if (newPermitCap <= minimumHandlerPerNs) {
          underMinimumNss.add(ns);
        } else {
          overMinimumNss.add(ns);
          effectiveOps += nsOps.get(ns);
        }
      }

      // Second iteration part 1: assign minimum handlers
      for (String ns: underMinimumNss) {
        resizeNsHandlerCapacity(ns, minimumHandlerPerNs);
      }
      // Second iteration part 2: assign handlers to the rest
      int leftoverPermits = handlerCount - minimumHandlerPerNs * underMinimumNss.size();
      for (String ns: overMinimumNss) {
        int newPermitCap = (int) Math.ceil((float) nsOps.get(ns) / effectiveOps * leftoverPermits);
        resizeNsHandlerCapacity(ns, newPermitCap);
      }
    }

    private void resizeNsHandlerCapacity(String ns, int newPermitCap) {
      AdjustableSemaphore semaphore = getPermits().get(ns);
      int oldPermitCap = getPermitSizes().get(ns);
      if (newPermitCap <= minimumHandlerPerNs) {
        newPermitCap = minimumHandlerPerNs;
      }
      getPermitSizes().put(ns, newPermitCap);
      if (newPermitCap > oldPermitCap) {
        semaphore.release(newPermitCap - oldPermitCap);
      } else if (newPermitCap < oldPermitCap) {
        semaphore.reducePermits(oldPermitCap - newPermitCap);
      }
      LOG.info("Resized handlers for nsId {} from {} to {}", ns, oldPermitCap, newPermitCap);
    }
  }
}
