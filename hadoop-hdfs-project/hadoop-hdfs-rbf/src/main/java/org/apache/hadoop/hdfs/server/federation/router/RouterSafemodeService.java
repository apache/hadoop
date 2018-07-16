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
package org.apache.hadoop.hdfs.server.federation.router;

import static org.apache.hadoop.util.Time.now;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service to periodically check if the {@link org.apache.hadoop.hdfs.server.
 * federation.store.StateStoreService StateStoreService} cached information in
 * the {@link Router} is up to date. This is for performance and removes the
 * {@link org.apache.hadoop.hdfs.server.federation.store.StateStoreService
 * StateStoreService} from the critical path in common operations.
 */
public class RouterSafemodeService extends PeriodicService {

  private static final Logger LOG =
      LoggerFactory.getLogger(RouterSafemodeService.class);

  /** Router to manage safe mode. */
  private final Router router;

  /**
   * If we are in safe mode, fail requests as if a standby NN.
   * Router can enter safe mode in two different ways:
   *   1. upon start up: router enters this mode after service start, and will
   *      exit after certain time threshold;
   *   2. via admin command: router enters this mode via admin command:
   *        dfsrouteradmin -safemode enter
   *      and exit after admin command:
   *        dfsrouteradmin -safemode leave
   */

  /** Whether Router is in safe mode */
  private volatile boolean safeMode;

  /** Whether the Router safe mode is set manually (i.e., via Router admin) */
  private volatile boolean isSafeModeSetManually;

  /** Interval in ms to wait post startup before allowing RPC requests. */
  private long startupInterval;
  /** Interval in ms after which the State Store cache is too stale. */
  private long staleInterval;
  /** Start time in ms of this service. */
  private long startupTime;

  /** The time the Router enters safe mode in milliseconds. */
  private long enterSafeModeTime = now();


  /**
   * Create a new Cache update service.
   *
   * @param router Router containing the cache.
   */
  public RouterSafemodeService(Router router) {
    super(RouterSafemodeService.class.getSimpleName());
    this.router = router;
  }

  /**
   * Return whether the current Router is in safe mode.
   */
  boolean isInSafeMode() {
    return this.safeMode;
  }

  /**
   * Set the flag to indicate that the safe mode for this Router is set manually
   * via the Router admin command.
   */
  void setManualSafeMode(boolean mode) {
    this.safeMode = mode;
    this.isSafeModeSetManually = mode;
  }

  /**
   * Enter safe mode.
   */
  private void enter() {
    LOG.info("Entering safe mode");
    enterSafeModeTime = now();
    safeMode = true;
    router.updateRouterState(RouterServiceState.SAFEMODE);
  }

  /**
   * Leave safe mode.
   */
  private void leave() {
    // Cache recently updated, leave safemode
    long timeInSafemode = now() - enterSafeModeTime;
    LOG.info("Leaving safe mode after {} milliseconds", timeInSafemode);
    RouterMetrics routerMetrics = router.getRouterMetrics();
    if (routerMetrics == null) {
      LOG.error("The Router metrics are not enabled");
    } else {
      routerMetrics.setSafeModeTime(timeInSafemode);
    }
    safeMode = false;
    router.updateRouterState(RouterServiceState.RUNNING);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {

    // Use same interval as cache update service
    this.setIntervalMs(conf.getTimeDuration(
        RBFConfigKeys.DFS_ROUTER_CACHE_TIME_TO_LIVE_MS,
        RBFConfigKeys.DFS_ROUTER_CACHE_TIME_TO_LIVE_MS_DEFAULT,
        TimeUnit.MILLISECONDS));

    this.startupInterval = conf.getTimeDuration(
        RBFConfigKeys.DFS_ROUTER_SAFEMODE_EXTENSION,
        RBFConfigKeys.DFS_ROUTER_SAFEMODE_EXTENSION_DEFAULT,
        TimeUnit.MILLISECONDS);
    LOG.info("Leave startup safe mode after {} ms", this.startupInterval);

    this.staleInterval = conf.getTimeDuration(
        RBFConfigKeys.DFS_ROUTER_SAFEMODE_EXPIRATION,
        RBFConfigKeys.DFS_ROUTER_SAFEMODE_EXPIRATION_DEFAULT,
        TimeUnit.MILLISECONDS);
    LOG.info("Enter safe mode after {} ms without reaching the State Store",
        this.staleInterval);

    this.startupTime = Time.now();

    // Initializing the RPC server in safe mode, it will disable it later
    enter();

    super.serviceInit(conf);
  }

  @Override
  public void periodicInvoke() {
    long now = Time.now();
    long delta = now - startupTime;
    if (delta < startupInterval) {
      LOG.info("Delaying safemode exit for {} milliseconds...",
          this.startupInterval - delta);
      return;
    }
    StateStoreService stateStore = router.getStateStore();
    long cacheUpdateTime = stateStore.getCacheUpdateTime();
    boolean isCacheStale = (now - cacheUpdateTime) > this.staleInterval;

    // Always update to indicate our cache was updated
    if (isCacheStale) {
      if (!safeMode) {
        enter();
      }
    } else if (safeMode && !isSafeModeSetManually) {
      // Cache recently updated, leave safe mode
      leave();
    }
  }
}