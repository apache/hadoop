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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.FederationTestUtils;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.resolver.FileSubclusterResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.store.RouterStore;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RefreshMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RefreshMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * This test class verifies that mount table cache is updated on all the routers
 * when MountTableRefreshService is enabled and there is a change in mount table
 * entries.
 */
public class TestRouterMountTableCacheRefresh {
  private static TestingServer curatorTestingServer;
  private static MiniRouterDFSCluster cluster;
  private static RouterContext routerContext;
  private static MountTableManager mountTableManager;

  public static Collection<Object> data() {
    return Arrays.asList(new Object[] {true, false});
  }

  public void initTestRouterMountTableCacheRefresh(boolean pUseIpForHeartbeats)
      throws Exception {
    // Initialize only once per parameter
    if (curatorTestingServer != null) {
      return;
    }
    curatorTestingServer = new TestingServer();
    curatorTestingServer.start();
    final String connectString = curatorTestingServer.getConnectString();
    int numNameservices = 2;
    cluster = new MiniRouterDFSCluster(false, numNameservices);
    Configuration conf = new RouterConfigBuilder().refreshCache().admin().rpc()
        .heartbeat().build();
    conf.setClass(RBFConfigKeys.FEDERATION_FILE_RESOLVER_CLIENT_CLASS,
        RBFConfigKeys.FEDERATION_FILE_RESOLVER_CLIENT_CLASS_DEFAULT,
        FileSubclusterResolver.class);
    conf.set(RBFConfigKeys.FEDERATION_STORE_ZK_ADDRESS, connectString);
    conf.setBoolean(RBFConfigKeys.DFS_ROUTER_STORE_ENABLE, true);
    conf.setBoolean(RBFConfigKeys.DFS_ROUTER_HEARTBEAT_WITH_IP_ENABLE, pUseIpForHeartbeats);
    cluster.addRouterOverrides(conf);
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp();
    routerContext = cluster.getRandomRouter();
    RouterStore routerStateManager =
        routerContext.getRouter().getRouterStateManager();
    mountTableManager = routerContext.getAdminClient().getMountTableManager();
    // wait for one minute for all the routers to get registered
    FederationTestUtils.waitRouterRegistered(routerStateManager,
        numNameservices, 60000);
  }

  @AfterEach
  public void destroy() {
    try {
      if (curatorTestingServer != null) {
        curatorTestingServer.close();
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    } catch (IOException e) {
      // do nothing
    } finally {
      cluster = null;
      routerContext = null;
      mountTableManager = null;
      curatorTestingServer = null;
    }
  }

  @AfterEach
  public void tearDown() throws IOException {
    clearEntries();
  }

  private void clearEntries() throws IOException {
    List<MountTable> result = getMountTableEntries();
    for (MountTable mountTable : result) {
      RemoveMountTableEntryResponse removeMountTableEntry =
          mountTableManager.removeMountTableEntry(RemoveMountTableEntryRequest
              .newInstance(mountTable.getSourcePath()));
      assertTrue(removeMountTableEntry.getStatus());
    }
  }

  /**
   * addMountTableEntry API should internally update the cache on all the
   * routers.
   */
  @MethodSource("data")
  @ParameterizedTest
  public void testMountTableEntriesCacheUpdatedAfterAddAPICall(boolean pUseIpForHeartbeats)
      throws Exception {

    initTestRouterMountTableCacheRefresh(pUseIpForHeartbeats);
    // Existing mount table size
    int existingEntriesCount = getNumMountTableEntries();
    String srcPath = "/addPath";
    MountTable newEntry = MountTable.newInstance(srcPath,
        Collections.singletonMap("ns0", "/addPathDest"), Time.now(),
        Time.now());
    addMountTableEntry(mountTableManager, newEntry);

    // When Add entry is done, all the routers must have updated its mount table
    // entry
    List<RouterContext> routers = getRouters();
    for (RouterContext rc : routers) {
      List<MountTable> result =
          getMountTableEntries(rc.getAdminClient().getMountTableManager());
      assertEquals(1 + existingEntriesCount, result.size());
      MountTable mountTableResult = result.get(0);
      assertEquals(srcPath, mountTableResult.getSourcePath());
    }
  }

  /**
   * removeMountTableEntry API should internally update the cache on all the
   * routers.
   */
  @MethodSource("data")
  @ParameterizedTest
  public void testMountTableEntriesCacheUpdatedAfterRemoveAPICall(boolean pUseIpForHeartbeats)
      throws Exception {
    initTestRouterMountTableCacheRefresh(pUseIpForHeartbeats);
    // add
    String srcPath = "/removePathSrc";
    MountTable newEntry = MountTable.newInstance(srcPath,
        Collections.singletonMap("ns0", "/removePathDest"), Time.now(),
        Time.now());
    addMountTableEntry(mountTableManager, newEntry);
    int addCount = getNumMountTableEntries();
    assertEquals(1, addCount);

    // remove
    RemoveMountTableEntryResponse removeMountTableEntry =
        mountTableManager.removeMountTableEntry(
            RemoveMountTableEntryRequest.newInstance(srcPath));
    assertTrue(removeMountTableEntry.getStatus());

    int removeCount = getNumMountTableEntries();
    assertEquals(addCount - 1, removeCount);
  }

  /**
   * updateMountTableEntry API should internally update the cache on all the
   * routers.
   */
  @MethodSource("data")
  @ParameterizedTest
  public void testMountTableEntriesCacheUpdatedAfterUpdateAPICall(boolean pUseIpForHeartbeats)
      throws Exception {
    initTestRouterMountTableCacheRefresh(pUseIpForHeartbeats);
    // add
    String srcPath = "/updatePathSrc";
    MountTable newEntry = MountTable.newInstance(srcPath,
        Collections.singletonMap("ns0", "/updatePathDest"), Time.now(),
        Time.now());
    addMountTableEntry(mountTableManager, newEntry);
    int addCount = getNumMountTableEntries();
    assertEquals(1, addCount);

    // update
    String key = "ns1";
    String value = "/updatePathDest2";
    MountTable upateEntry = MountTable.newInstance(srcPath,
        Collections.singletonMap(key, value), Time.now(), Time.now());
    UpdateMountTableEntryResponse updateMountTableEntry =
        mountTableManager.updateMountTableEntry(
            UpdateMountTableEntryRequest.newInstance(upateEntry));
    assertTrue(updateMountTableEntry.getStatus());
    MountTable updatedMountTable = getMountTableEntry(srcPath);
    assertNotNull(updatedMountTable, "Updated mount table entrty cannot be null");
    assertEquals(1, updatedMountTable.getDestinations().size());
    assertEquals(key,
        updatedMountTable.getDestinations().get(0).getNameserviceId());
    assertEquals(value, updatedMountTable.getDestinations().get(0).getDest());
  }

  /**
   * After caching RouterClient if router goes down, refresh should be
   * successful on other available router. The router which is not running
   * should be ignored.
   */
  @MethodSource("data")
  @ParameterizedTest
  public void testCachedRouterClientBehaviourAfterRouterStoped(boolean pUseIpForHeartbeats)
      throws Exception {
    initTestRouterMountTableCacheRefresh(pUseIpForHeartbeats);
    String srcPath = "/addPathClientCache";
    MountTable newEntry = MountTable.newInstance(srcPath,
        Collections.singletonMap("ns0", "/addPathClientCacheDest"), Time.now(),
        Time.now());
    addMountTableEntry(mountTableManager, newEntry);

    // When Add entry is done, all the routers must have updated its mount table
    // entry
    List<RouterContext> routers = getRouters();
    for (RouterContext rc : routers) {
      List<MountTable> result =
          getMountTableEntries(rc.getAdminClient().getMountTableManager());
      assertEquals(1, result.size());
      MountTable mountTableResult = result.get(0);
      assertEquals(srcPath, mountTableResult.getSourcePath());
    }

    // Lets stop one router
    for (RouterContext rc : routers) {
      InetSocketAddress adminServerAddress =
          rc.getRouter().getAdminServerAddress();
      if (!routerContext.getRouter().getAdminServerAddress()
          .equals(adminServerAddress)) {
        cluster.stopRouter(rc);
        break;
      }
    }

    srcPath = "/addPathClientCache2";
    newEntry = MountTable.newInstance(srcPath,
        Collections.singletonMap("ns0", "/addPathClientCacheDest2"), Time.now(),
        Time.now());
    addMountTableEntry(mountTableManager, newEntry);
    for (RouterContext rc : getRouters()) {
      List<MountTable> result =
          getMountTableEntries(rc.getAdminClient().getMountTableManager());
      assertEquals(2, result.size());
    }
  }

  private List<RouterContext> getRouters() {
    List<RouterContext> result = new ArrayList<>();
    for (RouterContext rc : cluster.getRouters()) {
      if (rc.getRouter().getServiceState() == STATE.STARTED) {
        result.add(rc);
      }
    }
    return result;
  }

  @MethodSource("data")
  @ParameterizedTest
  public void testRefreshMountTableEntriesAPI(boolean pUseIpForHeartbeats) throws Exception {
    initTestRouterMountTableCacheRefresh(pUseIpForHeartbeats);
    RefreshMountTableEntriesRequest request =
        RefreshMountTableEntriesRequest.newInstance();
    RefreshMountTableEntriesResponse refreshMountTableEntriesRes =
        mountTableManager.refreshMountTableEntries(request);
    // refresh should be successful
    assertTrue(refreshMountTableEntriesRes.getResult());
  }

  /**
   * Verify cache update timeouts when any of the router takes more time than
   * the configured timeout period.
   */
  @MethodSource("data")
  @ParameterizedTest
  @Timeout(value = 100)
  public void testMountTableEntriesCacheUpdateTimeout(boolean pUseIpForHeartbeats)
      throws Exception {
    initTestRouterMountTableCacheRefresh(pUseIpForHeartbeats);
    // Resources will be closed when router is closed
    @SuppressWarnings("resource")
    MountTableRefresherService mountTableRefresherService =
        new MountTableRefresherService(routerContext.getRouter()) {
          @Override
          protected MountTableRefresherThread getLocalRefresher(
              String adminAddress) {
            return new MountTableRefresherThread(null, adminAddress) {
              @Override
              public void run() {
                try {
                  // Sleep 1 minute
                  Thread.sleep(60000);
                } catch (InterruptedException e) {
                  // Do nothing
                }
              }
            };
          }
        };
    Configuration config = routerContext.getRouter().getConfig();
    config.setTimeDuration(RBFConfigKeys.MOUNT_TABLE_CACHE_UPDATE_TIMEOUT, 5,
        TimeUnit.SECONDS);
    mountTableRefresherService.init(config);
    // One router is not responding for 1 minute, still refresh should
    // finish in 5 second as cache update timeout is set 5 second.
    mountTableRefresherService.refresh();
    // Test case timeout is asserted for this test case.
  }

  /**
   * Verify Cached RouterClient connections are removed from cache and closed
   * when their max live time is elapsed.
   */
  @MethodSource("data")
  @ParameterizedTest
  public void testRouterClientConnectionExpiration(boolean pUseIpForHeartbeats) throws Exception {
    initTestRouterMountTableCacheRefresh(pUseIpForHeartbeats);
    final AtomicInteger createCounter = new AtomicInteger();
    final AtomicInteger removeCounter = new AtomicInteger();
    // Resources will be closed when router is closed
    @SuppressWarnings("resource")
    MountTableRefresherService mountTableRefresherService =
        new MountTableRefresherService(routerContext.getRouter()) {
          @Override
          protected void closeRouterClient(RouterClient client) {
            super.closeRouterClient(client);
            removeCounter.incrementAndGet();
          }

          @Override
          protected RouterClient createRouterClient(
              InetSocketAddress routerSocket, Configuration config)
              throws IOException {
            createCounter.incrementAndGet();
            return super.createRouterClient(routerSocket, config);
          }
        };
    int clientCacheTime = 2000;
    Configuration config = routerContext.getRouter().getConfig();
    config.setTimeDuration(
        RBFConfigKeys.MOUNT_TABLE_CACHE_UPDATE_CLIENT_MAX_TIME, clientCacheTime,
        TimeUnit.MILLISECONDS);
    mountTableRefresherService.init(config);
    // Do refresh to created RouterClient
    mountTableRefresherService.refresh();
    assertNotEquals(0, createCounter.get(), "No RouterClient is created.");
    /*
     * Wait for clients to expire. Let's wait triple the cache eviction period.
     * After cache eviction period all created client must be removed and
     * closed.
     */
    GenericTestUtils.waitFor(() -> createCounter.get() == removeCounter.get(),
        100, 3 * clientCacheTime);
  }

  private int getNumMountTableEntries() throws IOException {
    List<MountTable> records = getMountTableEntries();
    int oldEntriesCount = records.size();
    return oldEntriesCount;
  }

  private MountTable getMountTableEntry(String srcPath) throws IOException {
    List<MountTable> mountTableEntries = getMountTableEntries();
    for (MountTable mountTable : mountTableEntries) {
      String sourcePath = mountTable.getSourcePath();
      if (srcPath.equals(sourcePath)) {
        return mountTable;
      }
    }
    return null;
  }

  private void addMountTableEntry(MountTableManager mountTableMgr,
      MountTable newEntry) throws IOException {
    AddMountTableEntryRequest addRequest =
        AddMountTableEntryRequest.newInstance(newEntry);
    AddMountTableEntryResponse addResponse =
        mountTableMgr.addMountTableEntry(addRequest);
    assertTrue(addResponse.getStatus());
  }

  private List<MountTable> getMountTableEntries() throws IOException {
    return getMountTableEntries(mountTableManager);
  }

  private List<MountTable> getMountTableEntries(
      MountTableManager mountTableManagerParam) throws IOException {
    GetMountTableEntriesRequest request =
        GetMountTableEntriesRequest.newInstance("/");
    return mountTableManagerParam.getMountTableEntries(request).getEntries();
  }
}