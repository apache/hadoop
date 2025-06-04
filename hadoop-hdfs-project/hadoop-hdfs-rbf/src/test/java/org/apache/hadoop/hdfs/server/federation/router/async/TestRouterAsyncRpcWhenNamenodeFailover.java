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
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;

import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.transitionClusterNSToActive;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.transitionClusterNSToStandby;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestRouterAsyncRpcWhenNamenodeFailover {

  private StateStoreDFSCluster cluster;

  private void setupCluster(boolean ha)
      throws Exception {
    // Build and start a federated cluster.
    cluster = new StateStoreDFSCluster(ha, 2);
    Configuration routerConf = new RouterConfigBuilder()
        .stateStore()
        .metrics()
        .admin()
        .rpc()
        .heartbeat()
        .build();

    routerConf.setBoolean(RBFConfigKeys.DFS_ROUTER_ASYNC_RPC_ENABLE_KEY, true);

    cluster.addRouterOverrides(routerConf);
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp();
  }

  @Test
  public void testGetFileInfoWhenNsFailover() throws Exception {
    setupCluster(true);
    Configuration conf = cluster.getRouterClientConf();
    conf.setInt("dfs.client.retry.max.attempts", 2);
    DFSClient routerClient = new DFSClient(new URI("hdfs://fed"), conf);
    transitionClusterNSToActive(cluster, 0);

    String basePath = "/ARR/testGetFileInfo";
    routerClient.mkdirs(basePath);
    DirectoryListing directoryListing = routerClient.listPaths("/ARR", new byte[0]);
    assertEquals(1, directoryListing.getPartialListing().length);

    transitionClusterNSToStandby(cluster);

    assertThrows(IOException.class, () -> {
      HdfsFileStatus fileInfo = routerClient.getFileInfo(basePath + 1);
    });
  }
}

