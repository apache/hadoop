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
package org.apache.hadoop.hdfs.server.federation.store;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.federation.router.PeriodicService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service to periodically update the {@link StateStoreService}
 * cached information in the
 * {@link org.apache.hadoop.hdfs.server.federation.router.Router Router}.
 * This is for performance and removes the State Store from the critical path
 * in common operations.
 */
public class StateStoreCacheUpdateService extends PeriodicService {

  private static final Logger LOG =
      LoggerFactory.getLogger(StateStoreCacheUpdateService.class);

  /** The service that manages the State Store connection. */
  private final StateStoreService stateStore;


  /**
   * Create a new Cache update service.
   *
   * @param stateStore Implementation of the state store
   */
  public StateStoreCacheUpdateService(StateStoreService stateStore) {
    super(StateStoreCacheUpdateService.class.getSimpleName());
    this.stateStore = stateStore;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {

    this.setIntervalMs(conf.getTimeDuration(
        DFSConfigKeys.DFS_ROUTER_CACHE_TIME_TO_LIVE_MS,
        DFSConfigKeys.DFS_ROUTER_CACHE_TIME_TO_LIVE_MS_DEFAULT,
        TimeUnit.MILLISECONDS));

    super.serviceInit(conf);
  }

  @Override
  public void periodicInvoke() {
    LOG.debug("Updating State Store cache");
    stateStore.refreshCaches();
  }
}