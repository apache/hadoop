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

import static org.apache.hadoop.fs.contract.router.SecurityConfUtil.initSecurity;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.server.federation.FederationTestUtils;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.resolver.FileSubclusterResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.store.RouterStore;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.driver.impl.StateStoreZooKeeperImpl;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test class verifies that mount table cache is updated on all the routers
 * when MountTableRefreshService and security mode are enabled and there is a
 * change in mount table entries.
 */
public class TestRouterMountTableCacheRefreshSecure {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRouterMountTableCacheRefreshSecure.class);

  private static TestingServer curatorTestingServer;
  private static MiniRouterDFSCluster cluster;
  private static RouterContext routerContext;
  private static MountTableManager mountTableManager;

  @BeforeClass
  public static void setUp() throws Exception {
    curatorTestingServer = new TestingServer();
    curatorTestingServer.start();
    final String connectString = curatorTestingServer.getConnectString();
    int numNameservices = 2;
    Configuration conf = new RouterConfigBuilder().refreshCache().admin().rpc()
        .heartbeat().build();
    conf.addResource(initSecurity());
    conf.setClass(RBFConfigKeys.FEDERATION_STORE_DRIVER_CLASS,
        StateStoreZooKeeperImpl.class, StateStoreDriver.class);
    conf.setClass(RBFConfigKeys.FEDERATION_FILE_RESOLVER_CLIENT_CLASS,
        RBFConfigKeys.FEDERATION_FILE_RESOLVER_CLIENT_CLASS_DEFAULT,
        FileSubclusterResolver.class);
    conf.set(CommonConfigurationKeys.ZK_ADDRESS, connectString);
    conf.setBoolean(RBFConfigKeys.DFS_ROUTER_STORE_ENABLE, true);
    cluster = new MiniRouterDFSCluster(false, numNameservices, conf);
    cluster.addRouterOverrides(conf);
    cluster.startCluster(conf);
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

  @AfterClass
  public static void destory() {
    try {
      curatorTestingServer.close();
      cluster.shutdown();
    } catch (IOException e) {
      LOG.error("Found error when destroy, caused by: {}", e.getMessage());
    }
  }

  @After
  public void tearDown() throws IOException {
    clearEntries();
  }

  private void clearEntries() throws IOException {
    List<MountTable> result = getMountTableEntries();
    for (MountTable mountTable : result) {
      RemoveMountTableEntryResponse removeMountTableEntry = mountTableManager.
          removeMountTableEntry(RemoveMountTableEntryRequest.
          newInstance(mountTable.getSourcePath()));
      assertTrue(removeMountTableEntry.getStatus());
    }
  }

  /**
   * addMountTableEntry API should internally update the cache on all the
   * routers.
   */
  @Test
  public void testMountTableEntriesCacheUpdatedAfterAddAPICall()
      throws IOException {
    // Existing mount table size
    String srcPath = "/addPath";
    MountTable newEntry = MountTable.newInstance(srcPath,
        Collections.singletonMap("ns0", "/addPathDest"), Time.now(),
        Time.now());
    addMountTableEntry(mountTableManager, newEntry);

    // When add entry is done, all the routers must have updated its mount table
    // entry
    List<RouterContext> routers = getRouters();
    for (RouterContext rc : routers) {
      List<MountTable> result = getMountTableEntries(rc.getAdminClient()
          .getMountTableManager());
      assertEquals(1, result.size());
      MountTable mountTableResult = result.get(0);
      assertEquals(srcPath, mountTableResult.getSourcePath());
    }
  }

  /**
   * removeMountTableEntry API should internally update the cache on all the
   * routers.
   */
  @Test
  public void testMountTableEntriesCacheUpdatedAfterRemoveAPICall()
      throws IOException {
    // add
    String srcPath = "/removePathSrc";
    MountTable newEntry = MountTable.newInstance(srcPath,
        Collections.singletonMap("ns0", "/removePathDest"), Time.now(),
        Time.now());
    addMountTableEntry(mountTableManager, newEntry);

    // When add entry is done, all the routers must have updated its mount
    // table entry
    List<RouterContext> routers = getRouters();
    for (RouterContext rc : routers) {
      List<MountTable> result =
          getMountTableEntries(rc.getAdminClient().getMountTableManager());
      assertEquals(1, result.size());
      MountTable mountTableResult = result.get(0);
      assertEquals(srcPath, mountTableResult.getSourcePath());
    }

    // remove
    RemoveMountTableEntryResponse removeMountTableEntry =
        mountTableManager.removeMountTableEntry(
            RemoveMountTableEntryRequest.newInstance(srcPath));
    assertTrue(removeMountTableEntry.getStatus());

    // When remove entry is done, all the routers must have removed its mount
    // table entry
    routers = getRouters();
    for (RouterContext rc : routers) {
      List<MountTable> result =
          getMountTableEntries(rc.getAdminClient().getMountTableManager());
      assertEquals(0, result.size());
    }
  }

  /**
   * updateMountTableEntry API should internally update the cache on all the
   * routers.
   */
  @Test
  public void testMountTableEntriesCacheUpdatedAfterUpdateAPICall()
      throws IOException {
    // add
    String srcPath = "/updatePathSrc";
    String dstPath = "/updatePathDest";
    String nameServiceId = "ns0";
    MountTable newEntry = MountTable.newInstance(srcPath,
        Collections.singletonMap("ns0", "/updatePathDest"), Time.now(),
        Time.now());
    addMountTableEntry(mountTableManager, newEntry);

    // When add entry is done, all the routers must have updated its mount table
    // entry
    List<RouterContext> routers = getRouters();
    for (RouterContext rc : routers) {
      List<MountTable> result =
          getMountTableEntries(rc.getAdminClient().getMountTableManager());
      assertEquals(1, result.size());
      MountTable mountTableResult = result.get(0);
      assertEquals(srcPath, mountTableResult.getSourcePath());
      assertEquals(nameServiceId,
          mountTableResult.getDestinations().get(0).getNameserviceId());
      assertEquals(dstPath,
          mountTableResult.getDestinations().get(0).getDest());
    }

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
    assertNotNull("Updated mount table entrty cannot be null",
        updatedMountTable);

    // When update entry is done, all the routers must have updated its mount
    // table entry
    routers = getRouters();
    for (RouterContext rc : routers) {
      List<MountTable> result =
          getMountTableEntries(rc.getAdminClient().getMountTableManager());
      assertEquals(1, result.size());
      MountTable mountTableResult = result.get(0);
      assertEquals(srcPath, mountTableResult.getSourcePath());
      assertEquals(key, updatedMountTable.getDestinations().get(0)
          .getNameserviceId());
      assertEquals(value, updatedMountTable.getDestinations().get(0).getDest());
    }
  }

  /**
   * After caching RouterClient if router goes down, refresh should be
   * successful on other available router. The router which is not running
   * should be ignored.
   */
  @Test
  public void testCachedRouterClientBehaviourAfterRouterStoped()
      throws IOException {
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
      InetSocketAddress adminServerAddress = rc.getRouter()
          .getAdminServerAddress();
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
