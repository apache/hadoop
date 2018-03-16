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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.RouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.impl.MountTableStoreImpl;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.tools.federation.RouterAdmin;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
/**
 * Tests Router admin commands.
 */
public class TestRouterAdminCLI {
  private static StateStoreDFSCluster cluster;
  private static RouterContext routerContext;
  private static StateStoreService stateStore;

  private static RouterAdmin admin;
  private static RouterClient client;

  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private static final PrintStream OLD_OUT = System.out;

  @BeforeClass
  public static void globalSetUp() throws Exception {
    cluster = new StateStoreDFSCluster(false, 1);
    // Build and start a router with State Store + admin + RPC
    Configuration conf = new RouterConfigBuilder()
        .stateStore()
        .admin()
        .rpc()
        .build();
    cluster.addRouterOverrides(conf);

    // Start routers
    cluster.startRouters();

    routerContext = cluster.getRandomRouter();
    Router router = routerContext.getRouter();
    stateStore = router.getStateStore();

    Configuration routerConf = new Configuration();
    InetSocketAddress routerSocket = router.getAdminServerAddress();
    routerConf.setSocketAddr(DFSConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_KEY,
        routerSocket);
    admin = new RouterAdmin(routerConf);
    client = routerContext.getAdminClient();
  }

  @AfterClass
  public static void tearDownCluster() {
    cluster.stopRouter(routerContext);
    cluster.shutdown();
    cluster = null;
  }

  @After
  public void tearDown() {
    // set back system out
    System.setOut(OLD_OUT);
  }

  @Test
  public void testAddMountTable() throws Exception {
    String nsId = "ns0";
    String src = "/test-addmounttable";
    String dest = "/addmounttable";
    String[] argv = new String[] {"-add", src, nsId, dest};
    Assert.assertEquals(0, ToolRunner.run(admin, argv));

    stateStore.loadCache(MountTableStoreImpl.class, true);
    GetMountTableEntriesRequest getRequest = GetMountTableEntriesRequest
        .newInstance(src);
    GetMountTableEntriesResponse getResponse = client.getMountTableManager()
        .getMountTableEntries(getRequest);
    MountTable mountTable = getResponse.getEntries().get(0);

    List<RemoteLocation> destinations = mountTable.getDestinations();
    assertEquals(1, destinations.size());

    assertEquals(src, mountTable.getSourcePath());
    assertEquals(nsId, destinations.get(0).getNameserviceId());
    assertEquals(dest, destinations.get(0).getDest());
    assertFalse(mountTable.isReadOnly());

    // test mount table update behavior
    dest = dest + "-new";
    argv = new String[] {"-add", src, nsId, dest, "-readonly"};
    Assert.assertEquals(0, ToolRunner.run(admin, argv));
    stateStore.loadCache(MountTableStoreImpl.class, true);

    getResponse = client.getMountTableManager()
        .getMountTableEntries(getRequest);
    mountTable = getResponse.getEntries().get(0);
    assertEquals(2, mountTable.getDestinations().size());
    assertEquals(nsId, mountTable.getDestinations().get(1).getNameserviceId());
    assertEquals(dest, mountTable.getDestinations().get(1).getDest());
    assertTrue(mountTable.isReadOnly());
  }

  @Test
  public void testListMountTable() throws Exception {
    String nsId = "ns0";
    String src = "/test-lsmounttable";
    String dest = "/lsmounttable";
    String[] argv = new String[] {"-add", src, nsId, dest};
    assertEquals(0, ToolRunner.run(admin, argv));

    // re-set system out for testing
    System.setOut(new PrintStream(out));
    stateStore.loadCache(MountTableStoreImpl.class, true);
    argv = new String[] {"-ls", src};
    assertEquals(0, ToolRunner.run(admin, argv));
    assertTrue(out.toString().contains(src));

    out.reset();
    GetMountTableEntriesRequest getRequest = GetMountTableEntriesRequest
        .newInstance("/");
    GetMountTableEntriesResponse getResponse = client.getMountTableManager()
        .getMountTableEntries(getRequest);

    // Test ls command without input path, it will list
    // mount table under root path.
    argv = new String[] {"-ls"};
    assertEquals(0, ToolRunner.run(admin, argv));
    assertTrue(out.toString().contains(src));
    String outStr = out.toString();
    // verify if all the mount table are listed
    for(MountTable entry: getResponse.getEntries()) {
      assertTrue(outStr.contains(entry.getSourcePath()));
    }
  }

  @Test
  public void testRemoveMountTable() throws Exception {
    String nsId = "ns0";
    String src = "/test-rmmounttable";
    String dest = "/rmmounttable";
    String[] argv = new String[] {"-add", src, nsId, dest};
    assertEquals(0, ToolRunner.run(admin, argv));

    stateStore.loadCache(MountTableStoreImpl.class, true);
    GetMountTableEntriesRequest getRequest = GetMountTableEntriesRequest
        .newInstance(src);
    GetMountTableEntriesResponse getResponse = client.getMountTableManager()
        .getMountTableEntries(getRequest);
    // ensure mount table added successfully
    MountTable mountTable = getResponse.getEntries().get(0);
    assertEquals(src, mountTable.getSourcePath());

    argv = new String[] {"-rm", src};
    assertEquals(0, ToolRunner.run(admin, argv));

    stateStore.loadCache(MountTableStoreImpl.class, true);
    getResponse = client.getMountTableManager()
        .getMountTableEntries(getRequest);
    assertEquals(0, getResponse.getEntries().size());

    // remove an invalid mount table
    String invalidPath = "/invalid";
    System.setOut(new PrintStream(out));
    argv = new String[] {"-rm", invalidPath};
    assertEquals(0, ToolRunner.run(admin, argv));
    assertTrue(out.toString().contains(
        "Cannot remove mount point " + invalidPath));
  }
}