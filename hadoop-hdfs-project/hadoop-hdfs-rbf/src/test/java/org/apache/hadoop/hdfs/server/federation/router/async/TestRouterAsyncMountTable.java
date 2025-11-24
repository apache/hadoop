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
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.hdfs.server.federation.router.TestRouterMountTable;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test a router end-to-end including the MountTable using async rpc.
 */
public class TestRouterAsyncMountTable extends TestRouterMountTable {
  public static final Logger LOG = LoggerFactory.getLogger(TestRouterAsyncMountTable.class);

  @BeforeAll
  public static void globalSetUp() throws Exception {
    startTime = Time.now();

    // Build and start a federated cluster.
    cluster = new StateStoreDFSCluster(false, 2);
    Configuration conf = new RouterConfigBuilder()
        .stateStore()
        .admin()
        .rpc()
        .build();
    conf.setInt(RBFConfigKeys.DFS_ROUTER_ADMIN_MAX_COMPONENT_LENGTH_KEY, 20);
    conf.setBoolean(RBFConfigKeys.DFS_ROUTER_ASYNC_RPC_ENABLE_KEY, true);
    cluster.addRouterOverrides(conf);
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp();

    // Get the end points.
    nnContext0 = cluster.getNamenode("ns0", null);
    nnContext1 = cluster.getNamenode("ns1", null);
    nnFs0 = nnContext0.getFileSystem();
    nnFs1 = nnContext1.getFileSystem();
    routerContext = cluster.getRandomRouter();
    routerFs = routerContext.getFileSystem();
    Router router = routerContext.getRouter();
    routerProtocol = routerContext.getClient().getNamenode();
    mountTable = (MountTableResolver) router.getSubclusterResolver();
  }
}
