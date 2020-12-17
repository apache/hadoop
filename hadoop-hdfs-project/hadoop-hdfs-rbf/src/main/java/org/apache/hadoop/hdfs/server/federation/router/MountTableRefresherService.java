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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.store.MountTableStore;
import org.apache.hadoop.hdfs.server.federation.store.RouterStore;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreUnavailableException;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreUtils;
import org.apache.hadoop.hdfs.server.federation.store.records.RouterState;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.hadoop.thirdparty.com.google.common.cache.LoadingCache;
import org.apache.hadoop.thirdparty.com.google.common.cache.RemovalListener;
import org.apache.hadoop.thirdparty.com.google.common.cache.RemovalNotification;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * This service is invoked from {@link MountTableStore} when there is change in
 * mount table entries and it updates mount table entry cache on local router as
 * well as on all remote routers. Refresh on local router is done by calling
 * {@link MountTableStore#loadCache(boolean)}} API directly, no RPC call
 * involved, but on remote routers refresh is done through RouterClient(RPC
 * call). To improve performance, all routers are refreshed in separate thread
 * and all connection are cached. Cached connections are removed from
 * cache and closed when their max live time is elapsed.
 */
public class MountTableRefresherService extends AbstractService {
  private static final String ROUTER_CONNECT_ERROR_MSG =
      "Router {} connection failed. Mount table cache will not refresh.";
  private static final Logger LOG =
      LoggerFactory.getLogger(MountTableRefresherService.class);

  /** Local router. */
  private final Router router;
  /** Mount table store. */
  private MountTableStore mountTableStore;
  /** Local router admin address in the form of host:port. */
  private String localAdminAddress;
  /** Timeout in ms to update mount table cache on all the routers. */
  private long cacheUpdateTimeout;

  /**
   * All router admin clients cached. So no need to create the client again and
   * again. Router admin address(host:port) is used as key to cache RouterClient
   * objects.
   */
  private LoadingCache<String, RouterClient> routerClientsCache;

  /**
   * Removes expired RouterClient from routerClientsCache.
   */
  private ScheduledExecutorService clientCacheCleanerScheduler;

  /**
   * Create a new service to refresh mount table cache when there is change in
   * mount table entries.
   *
   * @param router whose mount table cache will be refreshed
   */
  public MountTableRefresherService(Router router) {
    super(MountTableRefresherService.class.getSimpleName());
    this.router = router;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    this.mountTableStore = getMountTableStore();
    // Attach this service to mount table store.
    this.mountTableStore.setRefreshService(this);
    this.localAdminAddress =
        StateStoreUtils.getHostPortString(router.getAdminServerAddress());
    this.cacheUpdateTimeout = conf.getTimeDuration(
        RBFConfigKeys.MOUNT_TABLE_CACHE_UPDATE_TIMEOUT,
        RBFConfigKeys.MOUNT_TABLE_CACHE_UPDATE_TIMEOUT_DEFAULT,
        TimeUnit.MILLISECONDS);
    long routerClientMaxLiveTime = conf.getTimeDuration(
        RBFConfigKeys.MOUNT_TABLE_CACHE_UPDATE_CLIENT_MAX_TIME,
        RBFConfigKeys.MOUNT_TABLE_CACHE_UPDATE_CLIENT_MAX_TIME_DEFAULT,
        TimeUnit.MILLISECONDS);
    routerClientsCache = CacheBuilder.newBuilder()
        .expireAfterWrite(routerClientMaxLiveTime, TimeUnit.MILLISECONDS)
        .removalListener(getClientRemover()).build(getClientCreator());

    initClientCacheCleaner(routerClientMaxLiveTime);
  }

  private void initClientCacheCleaner(long routerClientMaxLiveTime) {
    clientCacheCleanerScheduler =
        Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
        .setNameFormat("MountTableRefresh_ClientsCacheCleaner")
        .setDaemon(true).build());
    /*
     * When cleanUp() method is called, expired RouterClient will be removed and
     * closed.
     */
    clientCacheCleanerScheduler.scheduleWithFixedDelay(
        () -> routerClientsCache.cleanUp(), routerClientMaxLiveTime,
        routerClientMaxLiveTime, TimeUnit.MILLISECONDS);
  }

  /**
   * Create cache entry remove listener.
   */
  private RemovalListener<String, RouterClient> getClientRemover() {
    return new RemovalListener<String, RouterClient>() {
      @Override
      public void onRemoval(
          RemovalNotification<String, RouterClient> notification) {
          closeRouterClient(notification.getValue());
      }
    };
  }

  @VisibleForTesting
  protected void closeRouterClient(RouterClient client) {
    try {
      client.close();
    } catch (IOException e) {
      LOG.error("Error while closing RouterClient", e);
    }
  }

  /**
   * Creates RouterClient and caches it.
   */
  private CacheLoader<String, RouterClient> getClientCreator() {
    return new CacheLoader<String, RouterClient>() {
      public RouterClient load(String adminAddress) throws IOException {
        InetSocketAddress routerSocket =
            NetUtils.createSocketAddr(adminAddress);
        Configuration config = getConfig();
        return createRouterClient(routerSocket, config);
      }
    };
  }

  @VisibleForTesting
  protected RouterClient createRouterClient(InetSocketAddress routerSocket,
      Configuration config) throws IOException {
    return SecurityUtil.doAsLoginUser(() -> {
      if (UserGroupInformation.isSecurityEnabled()) {
        UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
      }
      return new RouterClient(routerSocket, config);
    });
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
    clientCacheCleanerScheduler.shutdown();
    // remove and close all admin clients
    routerClientsCache.invalidateAll();
  }

  private MountTableStore getMountTableStore() throws IOException {
    MountTableStore mountTblStore =
        router.getStateStore().getRegisteredRecordStore(MountTableStore.class);
    if (mountTblStore == null) {
      throw new IOException("Mount table state store is not available.");
    }
    return mountTblStore;
  }

  /**
   * Refresh mount table cache of this router as well as all other routers.
   */
  public void refresh() throws StateStoreUnavailableException {
    RouterStore routerStore = router.getRouterStateManager();

    try {
      routerStore.loadCache(true);
    } catch (IOException e) {
      LOG.warn("RouterStore load cache failed,", e);
    }

    List<RouterState> cachedRecords = routerStore.getCachedRecords();
    List<MountTableRefresherThread> refreshThreads = new ArrayList<>();
    for (RouterState routerState : cachedRecords) {
      String adminAddress = routerState.getAdminAddress();
      if (adminAddress == null || adminAddress.length() == 0) {
        // this router has not enabled router admin.
        continue;
      }
      // No use of calling refresh on router which is not running state
      if (routerState.getStatus() != RouterServiceState.RUNNING) {
        LOG.info(
            "Router {} is not running. Mount table cache will not refresh.",
            routerState.getAddress());
        // remove if RouterClient is cached.
        removeFromCache(adminAddress);
      } else if (isLocalAdmin(adminAddress)) {
        /*
         * Local router's cache update does not require RPC call, so no need for
         * RouterClient
         */
        refreshThreads.add(getLocalRefresher(adminAddress));
      } else {
        try {
          RouterClient client = routerClientsCache.get(adminAddress);
          refreshThreads.add(new MountTableRefresherThread(
              client.getMountTableManager(), adminAddress));
        } catch (ExecutionException execExcep) {
          // Can not connect, seems router is stopped now.
          LOG.warn(ROUTER_CONNECT_ERROR_MSG, adminAddress, execExcep);
        }
      }
    }
    if (!refreshThreads.isEmpty()) {
      invokeRefresh(refreshThreads);
    }
  }

  @VisibleForTesting
  protected MountTableRefresherThread getLocalRefresher(String adminAddress) {
    return new MountTableRefresherThread(router.getAdminServer(), adminAddress);
  }

  private void removeFromCache(String adminAddress) {
    routerClientsCache.invalidate(adminAddress);
  }

  private void invokeRefresh(List<MountTableRefresherThread> refreshThreads) {
    CountDownLatch countDownLatch = new CountDownLatch(refreshThreads.size());
    // start all the threads
    for (MountTableRefresherThread refThread : refreshThreads) {
      refThread.setCountDownLatch(countDownLatch);
      refThread.start();
    }
    try {
      /*
       * Wait for all the thread to complete, await method returns false if
       * refresh is not finished within specified time
       */
      boolean allReqCompleted =
          countDownLatch.await(cacheUpdateTimeout, TimeUnit.MILLISECONDS);
      if (!allReqCompleted) {
        LOG.warn("Not all router admins updated their cache");
      }
    } catch (InterruptedException e) {
      LOG.error("Mount table cache refresher was interrupted.", e);
    }
    logResult(refreshThreads);
  }

  private boolean isLocalAdmin(String adminAddress) {
    return adminAddress.contentEquals(localAdminAddress);
  }

  private void logResult(List<MountTableRefresherThread> refreshThreads) {
    int successCount = 0;
    int failureCount = 0;
    for (MountTableRefresherThread mountTableRefreshThread : refreshThreads) {
      if (mountTableRefreshThread.isSuccess()) {
        successCount++;
      } else {
        failureCount++;
        // remove RouterClient from cache so that new client is created
        removeFromCache(mountTableRefreshThread.getAdminAddress());
      }
    }
    LOG.info(
        "Mount table entries cache refresh successCount={},failureCount={}",
        successCount, failureCount);
  }
}
