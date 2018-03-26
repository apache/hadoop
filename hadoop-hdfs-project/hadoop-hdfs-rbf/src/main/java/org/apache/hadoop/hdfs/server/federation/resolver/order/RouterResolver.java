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
package org.apache.hadoop.hdfs.server.federation.resolver.order;

import static org.apache.hadoop.util.Time.monotonicNow;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.resolver.PathLocation;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.hdfs.server.federation.store.MembershipStore;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The order resolver that depends upon the Router service.
 *
 * @param <K> The key type of subcluster mapping info queried from Router.
 * @param <V> The value type of subcluster mapping info queried from Router.
 */
public abstract class RouterResolver<K, V> implements OrderedResolver {

  private static final Logger LOG =
      LoggerFactory.getLogger(RouterResolver.class);

  /** Configuration key to set the minimum time to update subcluster info. */
  public static final String MIN_UPDATE_PERIOD_KEY =
      RBFConfigKeys.FEDERATION_ROUTER_PREFIX + "router-resolver.update-period";
  /** 10 seconds by default. */
  private static final long MIN_UPDATE_PERIOD_DEFAULT = TimeUnit.SECONDS
      .toMillis(10);

  /** Router service. */
  private final Router router;
  /** Minimum update time. */
  private final long minUpdateTime;

  /** K -> T template mapping. */
  private Map<K, V> subclusterMapping = null;
  /** Last time the subcluster mapping was updated. */
  private long lastUpdated;

  public RouterResolver(final Configuration conf, final Router routerService) {
    this.minUpdateTime = conf.getTimeDuration(MIN_UPDATE_PERIOD_KEY,
        MIN_UPDATE_PERIOD_DEFAULT, TimeUnit.MILLISECONDS);
    this.router = routerService;
  }

  @Override
  public String getFirstNamespace(String path, PathLocation loc) {
    updateSubclusterMapping();
    return chooseFirstNamespace(path, loc);
  }

  /**
   * The implementation for getting desired subcluster mapping info.
   *
   * @param membershipStore Membership store the resolver queried from.
   * @return The map of desired type info.
   */
  protected abstract Map<K, V> getSubclusterInfo(
      MembershipStore membershipStore);

  /**
   * Choose the first namespace from queried subcluster mapping info.
   *
   * @param path Path to check.
   * @param loc Federated location with multiple destinations.
   * @return First namespace out of the locations.
   */
  protected abstract String chooseFirstNamespace(String path, PathLocation loc);

  /**
   * Update <NamespaceId, Subcluster Info> mapping info periodically.
   */
  private synchronized void updateSubclusterMapping() {
    if (subclusterMapping == null
        || (monotonicNow() - lastUpdated) > minUpdateTime) {
      // Fetch the mapping asynchronously
      Thread updater = new Thread(new Runnable() {
        @Override
        public void run() {
          final MembershipStore membershipStore = getMembershipStore();
          if (membershipStore == null) {
            LOG.error("Cannot access the Membership store.");
            return;
          }

          subclusterMapping = getSubclusterInfo(membershipStore);
          lastUpdated = monotonicNow();
        }
      });
      updater.start();

      // Wait until initialized
      if (subclusterMapping == null) {
        try {
          LOG.debug("Wait to get the mapping for the first time");
          updater.join();
        } catch (InterruptedException e) {
          LOG.error("Cannot wait for the updater to finish");
        }
      }
    }
  }

  /**
   * Get the Router RPC server.
   *
   * @return Router RPC server. Null if not possible.
   */
  protected RouterRpcServer getRpcServer() {
    if (this.router == null) {
      return null;
    }
    return router.getRpcServer();
  }

  /**
   * Get the Membership store.
   *
   * @return Membership store.
   */
  protected MembershipStore getMembershipStore() {
    StateStoreService stateStore = router.getStateStore();
    if (stateStore == null) {
      return null;
    }
    return stateStore.getRegisteredRecordStore(MembershipStore.class);
  }

  /**
   * Get subcluster mapping info.
   *
   * @return The map of subcluster info.
   */
  protected Map<K, V> getSubclusterMapping() {
    return this.subclusterMapping;
  }
}
