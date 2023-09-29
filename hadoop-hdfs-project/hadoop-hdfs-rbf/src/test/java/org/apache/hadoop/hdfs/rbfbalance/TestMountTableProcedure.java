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
package org.apache.hadoop.hdfs.rbfbalance;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.impl.MountTableStoreImpl;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.Time;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.createNamenodeReport;
import static org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils.synchronizeRecords;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Basic tests of MountTableProcedure.
 */
public class TestMountTableProcedure {

  private static StateStoreDFSCluster cluster;
  private static RouterContext routerContext;
  private static Configuration routerConf;
  private static List<MountTable> mockMountTable;
  private static StateStoreService stateStore;

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
    cluster.startRouters();
    routerContext = cluster.getRandomRouter();
    mockMountTable = cluster.generateMockMountTable();
    Router router = routerContext.getRouter();
    stateStore = router.getStateStore();

    // Add two name services for testing
    ActiveNamenodeResolver membership = router.getNamenodeResolver();
    membership.registerNamenode(createNamenodeReport("ns0", "nn1",
        HAServiceProtocol.HAServiceState.ACTIVE));
    membership.registerNamenode(createNamenodeReport("ns1", "nn1",
        HAServiceProtocol.HAServiceState.ACTIVE));
    stateStore.refreshCaches(true);

    routerConf = new Configuration();
    InetSocketAddress routerSocket = router.getAdminServerAddress();
    routerConf.setSocketAddr(RBFConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_KEY,
        routerSocket);
  }

  @AfterClass
  public static void tearDown() {
    cluster.stopRouter(routerContext);
  }

  @Before
  public void testSetup() throws Exception {
    assertTrue(
        synchronizeRecords(stateStore, mockMountTable, MountTable.class));
    // Avoid running with random users
    routerContext.resetAdminClient();
  }

  @Test
  public void testUpdateMountpoint() throws Exception {
    // Firstly add mount entry: /test-path->{ns0,/test-path}.
    String mount = "/test-path";
    String dst = "/test-dst";
    MountTable newEntry = MountTable
        .newInstance(mount, Collections.singletonMap("ns0", mount),
            Time.now(), Time.now());
    MountTableManager mountTable =
        routerContext.getAdminClient().getMountTableManager();
    AddMountTableEntryRequest addRequest =
        AddMountTableEntryRequest.newInstance(newEntry);
    AddMountTableEntryResponse addResponse =
        mountTable.addMountTableEntry(addRequest);
    assertTrue(addResponse.getStatus());
    // verify the mount entry is added successfully.
    GetMountTableEntriesRequest request =
        GetMountTableEntriesRequest.newInstance("/");
    stateStore.loadCache(MountTableStoreImpl.class, true); // load cache.
    GetMountTableEntriesResponse response =
        mountTable.getMountTableEntries(request);
    assertEquals(3, response.getEntries().size());

    // set the mount table to readonly.
    MountTableProcedure.disableWrite(mount, routerConf);

    // test MountTableProcedure updates the mount point.
    String dstNs = "ns1";
    MountTableProcedure smtp =
        new MountTableProcedure("single-mount-table-procedure", null,
            1000, mount, dst, dstNs, routerConf);
    assertTrue(smtp.execute());
    stateStore.loadCache(MountTableStoreImpl.class, true); // load cache.
    // verify the mount entry is updated to /
    MountTable entry =
        MountTableProcedure.getMountEntry(mount, mountTable);
    assertNotNull(entry);
    assertEquals(1, entry.getDestinations().size());
    String nsId = entry.getDestinations().get(0).getNameserviceId();
    String dstPath = entry.getDestinations().get(0).getDest();
    assertEquals(dstNs, nsId);
    assertEquals(dst, dstPath);
    // Verify the mount table is not readonly.
    URI address = routerContext.getFileSystemURI();
    DFSClient routerClient = new DFSClient(address, routerConf);
    MountTableProcedure.enableWrite(mount, routerConf);
    intercept(RemoteException.class, "No namenode available to invoke mkdirs",
        "Expect no namenode exception.", () -> routerClient
            .mkdirs(mount + "/file", new FsPermission(020), false));
  }

  @Test
  public void testDisableAndEnableWrite() throws Exception {
    // Firstly add mount entry: /test-write->{ns0,/test-write}.
    String mount = "/test-write";
    MountTable newEntry = MountTable
        .newInstance(mount, Collections.singletonMap("ns0", mount),
            Time.now(), Time.now());
    MountTableManager mountTable =
        routerContext.getAdminClient().getMountTableManager();
    AddMountTableEntryRequest addRequest =
        AddMountTableEntryRequest.newInstance(newEntry);
    AddMountTableEntryResponse addResponse =
        mountTable.addMountTableEntry(addRequest);
    assertTrue(addResponse.getStatus());
    stateStore.loadCache(MountTableStoreImpl.class, true); // load cache.

    // Construct client.
    URI address = routerContext.getFileSystemURI();
    DFSClient routerClient = new DFSClient(address, routerConf);
    // Verify the mount point is not readonly.
    intercept(RemoteException.class, "No namenode available to invoke mkdirs",
        "Expect no namenode exception.", () -> routerClient
            .mkdirs(mount + "/file", new FsPermission(020), false));

    // Verify disable write.
    MountTableProcedure.disableWrite(mount, routerConf);
    intercept(RemoteException.class, "is in a read only mount point",
        "Expect readonly exception.", () -> routerClient
            .mkdirs(mount + "/dir", new FsPermission(020), false));

    // Verify enable write.
    MountTableProcedure.enableWrite(mount, routerConf);
    intercept(RemoteException.class, "No namenode available to invoke mkdirs",
        "Expect no namenode exception.", () -> routerClient
            .mkdirs(mount + "/file", new FsPermission(020), false));
  }

  @Test
  public void testSeDeserialize() throws Exception {
    String fedPath = "/test-path";
    String dst = "/test-dst";
    String dstNs = "ns1";
    MountTableProcedure smtp =
        new MountTableProcedure("single-mount-table-procedure", null,
            1000, fedPath, dst, dstNs, routerConf);
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    DataOutput dataOut = new DataOutputStream(bao);
    smtp.write(dataOut);
    smtp = new MountTableProcedure();
    smtp.readFields(
        new DataInputStream(new ByteArrayInputStream(bao.toByteArray())));
    assertEquals(fedPath, smtp.getMount());
    assertEquals(dst, smtp.getDstPath());
    assertEquals(dstNs, smtp.getDstNs());
  }
}