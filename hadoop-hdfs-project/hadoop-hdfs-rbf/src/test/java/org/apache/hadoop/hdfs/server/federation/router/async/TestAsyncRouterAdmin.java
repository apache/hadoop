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
package org.apache.hadoop.hdfs.server.federation.router.async;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.router.RemoteMethod;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.hdfs.server.federation.router.TestRouterAdmin;
import org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil;
import org.apache.hadoop.util.Lists;
import org.junit.jupiter.api.BeforeAll;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.createNamenodeReport;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_ADMIN_MOUNT_CHECK_ENABLE;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_ASYNC_RPC_ENABLE_KEY;

public class TestAsyncRouterAdmin extends TestRouterAdmin {

  @BeforeAll
  public static void globalSetUp() throws Exception {
    cluster = new StateStoreDFSCluster(false, 1);
    // Build and start a router with State Store + admin + RPC.
    Configuration conf = new RouterConfigBuilder()
        .stateStore()
        .admin()
        .rpc()
        .build();
    conf.setBoolean(DFS_ROUTER_ADMIN_MOUNT_CHECK_ENABLE, true);
    conf.setBoolean(DFS_ROUTER_ASYNC_RPC_ENABLE_KEY, true);

    cluster.addRouterOverrides(conf);
    cluster.startRouters();
    routerContext = cluster.getRandomRouter();
    mockMountTable = cluster.generateMockMountTable();
    Router router = routerContext.getRouter();
    stateStore = router.getStateStore();

    // Add two name services for testing disabling.
    ActiveNamenodeResolver membership = router.getNamenodeResolver();
    membership.registerNamenode(
        createNamenodeReport("ns0", "nn1", HAServiceProtocol.HAServiceState.ACTIVE));
    membership.registerNamenode(
        createNamenodeReport("ns1", "nn1", HAServiceProtocol.HAServiceState.ACTIVE));
    stateStore.refreshCaches(true);

    setUpMocks();
  }

  private static void setUpMocks()
      throws IOException, NoSuchFieldException, IllegalAccessException {
    RouterRpcServer spyRpcServer =
        Mockito.spy(routerContext.getRouter().createRpcServer());
    // Used reflection to set the 'rpcServer field'.
    setField(routerContext.getRouter(), "rpcServer", spyRpcServer);
    Mockito.doReturn(null).when(spyRpcServer).getFileInfo(Mockito.anyString());

    // Mock rpc client for destination check when editing mount tables.
    // Spy RPC client and used reflection to set the 'rpcClient' field.
    mockRpcClient = Mockito.spy(spyRpcServer.getRPCClient());
    setField(spyRpcServer, "rpcClient", mockRpcClient);
    RemoteLocation remoteLocation0 =
        new RemoteLocation("ns0", "/testdir", null);
    RemoteLocation remoteLocation1 =
        new RemoteLocation("ns1", "/", null);
    final Map<RemoteLocation, HdfsFileStatus> mockResponse0 = new HashMap<>();
    final Map<RemoteLocation, HdfsFileStatus> mockResponse1 = new HashMap<>();
    mockResponse0.put(remoteLocation0,
        new HdfsFileStatus.Builder().build());
    Mockito.doAnswer(invocationOnMock -> {
      AsyncUtil.asyncComplete(mockResponse0);
      return null;
    }).when(mockRpcClient).invokeConcurrent(
        Mockito.eq(Lists.newArrayList(remoteLocation0)),
        Mockito.any(RemoteMethod.class),
        Mockito.eq(false),
        Mockito.eq(false),
        Mockito.eq(HdfsFileStatus.class)
    );
    mockResponse1.put(remoteLocation1,
        new HdfsFileStatus.Builder().build());
    Mockito.doAnswer(invocationOnMock -> {
      AsyncUtil.asyncComplete(mockResponse1);
      return null;
    }).when(mockRpcClient).invokeConcurrent(
        Mockito.eq(Lists.newArrayList(remoteLocation1)),
        Mockito.any(RemoteMethod.class),
        Mockito.eq(false),
        Mockito.eq(false),
        Mockito.eq(HdfsFileStatus.class)
    );
  }
}
