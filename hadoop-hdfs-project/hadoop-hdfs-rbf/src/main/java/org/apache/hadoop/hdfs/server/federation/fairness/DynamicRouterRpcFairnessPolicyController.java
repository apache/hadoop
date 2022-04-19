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

import java.util.HashMap;
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

import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_DYNAMIC_FAIRNESS_CONTROLLER_REFRESH_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_DYNAMIC_FAIRNESS_CONTROLLER_REFRESH_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HANDLER_COUNT_KEY;

/**
 * Dynamic fairness policy extending {@link StaticRouterRpcFairnessPolicyController}
 * and fetching handlers from configuration for all available name services.
 * The handlers count changes according to traffic to namespaces.
 * Total handlers might NOT strictly add up to the value defined by DFS_ROUTER_HANDLER_COUNT_KEY.
 */
public class DynamicRouterRpcFairnessPolicyController
    extends StaticRouterRpcFairnessPolicyController {

  private static final Logger LOG =
      LoggerFactory.getLogger(DynamicRouterRpcFairnessPolicyController.class);

  private static final ScheduledExecutorService scheduledExecutor = HadoopExecutors
      .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true)
          .setNameFormat("DynamicRouterRpcFairnessPolicyControllerPermitsResizer").build());
  private PermitsResizerService permitsResizerService;
  private ScheduledFuture<?> refreshTask;
  private int handlerCount;

  /**
   * Initializes using the same logic as {@link StaticRouterRpcFairnessPolicyController}
   * and starts a periodic semaphore resizer thread
   *
   * @param conf configuration
   */
  public DynamicRouterRpcFairnessPolicyController(Configuration conf) {
    super(conf);
    handlerCount = conf.getInt(DFS_ROUTER_HANDLER_COUNT_KEY, DFS_ROUTER_HANDLER_COUNT_DEFAULT);
    long refreshInterval =
        conf.getTimeDuration(DFS_ROUTER_DYNAMIC_FAIRNESS_CONTROLLER_REFRESH_INTERVAL_KEY,
            DFS_ROUTER_DYNAMIC_FAIRNESS_CONTROLLER_REFRESH_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS);
    permitsResizerService = new PermitsResizerService();
    refreshTask = scheduledExecutor
        .scheduleWithFixedDelay(permitsResizerService, refreshInterval, refreshInterval,
            TimeUnit.MILLISECONDS);
  }

  @VisibleForTesting
  public DynamicRouterRpcFairnessPolicyController(Configuration conf, long refreshInterval) {
    super(conf);
    handlerCount = conf.getInt(DFS_ROUTER_HANDLER_COUNT_KEY, DFS_ROUTER_HANDLER_COUNT_DEFAULT);
    permitsResizerService = new PermitsResizerService();
    refreshTask = scheduledExecutor
        .scheduleWithFixedDelay(permitsResizerService, refreshInterval, refreshInterval,
            TimeUnit.MILLISECONDS);
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
    if (scheduledExecutor != null) {
      scheduledExecutor.shutdown();
    }
  }

  class PermitsResizerService implements Runnable {

    @Override
    public synchronized void run() {
      long totalOps = 0;
      Map<String, Long> nsOps = new HashMap<>();
      for (Map.Entry<String, AdjustableSemaphore> entry : permits.entrySet()) {
        long ops = (rejectedPermitsPerNs.containsKey(entry.getKey()) ?
            rejectedPermitsPerNs.get(entry.getKey()).longValue() :
            0) + (acceptedPermitsPerNs.containsKey(entry.getKey()) ?
            acceptedPermitsPerNs.get(entry.getKey()).longValue() :
            0);
        nsOps.put(entry.getKey(), ops);
        totalOps += ops;
      }

      for (Map.Entry<String, AdjustableSemaphore> entry : permits.entrySet()) {
        String ns = entry.getKey();
        AdjustableSemaphore semaphore = entry.getValue();
        int oldPermitCap = permitSizes.get(ns);
        int newPermitCap = (int) Math.ceil((float) nsOps.get(ns) / totalOps * handlerCount);
        // Leave at least 1 handler even if there's no traffic
        if (newPermitCap == 0) {
          newPermitCap = 1;
        }
        permitSizes.put(ns, newPermitCap);
        if (newPermitCap > oldPermitCap) {
          semaphore.release(newPermitCap - oldPermitCap);
        } else if (newPermitCap < oldPermitCap) {
          semaphore.reducePermits(oldPermitCap - newPermitCap);
        }
        LOG.info("Resized handlers for nsId {} from {} to {}", ns, oldPermitCap, newPermitCap);
      }
    }
  }
}
