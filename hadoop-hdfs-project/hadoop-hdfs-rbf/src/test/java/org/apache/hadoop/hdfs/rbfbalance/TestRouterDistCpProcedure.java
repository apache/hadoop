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
package org.apache.hadoop.hdfs.rbfbalance;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
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
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.tools.fedbalance.DistCpProcedure.Stage;
import org.apache.hadoop.tools.fedbalance.FedBalanceContext;
import org.apache.hadoop.tools.fedbalance.TestDistCpProcedure;
import org.apache.hadoop.util.Time;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collections;

import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.createNamenodeReport;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.Assert.assertTrue;


public class TestRouterDistCpProcedure extends TestDistCpProcedure {
  private static StateStoreDFSCluster cluster;
  private static MiniRouterDFSCluster.RouterContext routerContext;
  private static Configuration routerConf;
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
    Router router = routerContext.getRouter();
    stateStore = router.getStateStore();

    // Add one name services for testing
    ActiveNamenodeResolver membership = router.getNamenodeResolver();
    membership.registerNamenode(createNamenodeReport("ns0", "nn1",
        HAServiceProtocol.HAServiceState.ACTIVE));
    stateStore.refreshCaches(true);

    routerConf = new Configuration();
    InetSocketAddress routerSocket = router.getAdminServerAddress();
    routerConf.setSocketAddr(RBFConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_KEY,
        routerSocket);
  }

  @Override
  public void testDisableWrite() throws Exception {
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

    FedBalanceContext context = new FedBalanceContext
        .Builder(null, null, mount, routerConf).build();
    RouterDistCpProcedure dcProcedure = new RouterDistCpProcedure();
    executeProcedure(dcProcedure, Stage.FINAL_DISTCP,
        () -> dcProcedure.disableWrite(context));
    intercept(RemoteException.class, "is in a read only mount point",
        "Expect readonly exception.", () -> routerClient
        .mkdirs(mount + "/dir", new FsPermission(020), false));
  }

  @AfterClass
  public static void tearDown() {
    cluster.stopRouter(routerContext);
  }
}
