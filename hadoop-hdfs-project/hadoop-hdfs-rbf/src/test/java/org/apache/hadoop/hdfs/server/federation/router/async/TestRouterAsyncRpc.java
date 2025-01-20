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
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.router.TestRouterRpc;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_ASYNC_RPC_ENABLE_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.syncReturn;
import static org.junit.Assert.assertArrayEquals;

/**
 * Testing the asynchronous RPC functionality of the router.
 */
public class TestRouterAsyncRpc extends TestRouterRpc {
  private static MiniRouterDFSCluster cluster;
  private MiniRouterDFSCluster.RouterContext rndRouter;

  @BeforeClass
  public static void globalSetUp() throws Exception {
    // Start routers with only an RPC service
    Configuration routerConf = new RouterConfigBuilder()
        .metrics()
        .rpc()
        .build();
    // We decrease the DN cache times to make the test faster
    routerConf.setTimeDuration(
        RBFConfigKeys.DN_REPORT_CACHE_EXPIRE, 1, TimeUnit.SECONDS);
    // use async router.
    routerConf.setBoolean(DFS_ROUTER_ASYNC_RPC_ENABLE_KEY, true);
    setUp(routerConf);
  }

  @Before
  public void testSetup() throws Exception {
    super.testSetup();
    cluster = super.getCluster();
    // Random router for this test
    rndRouter = cluster.getRandomRouter();
  }

  @Test
  @Override
  public void testgetGroupsForUser() throws Exception {
    String[] group = new String[] {"bar", "group2"};
    UserGroupInformation.createUserForTesting("user",
        new String[] {"bar", "group2"});
    rndRouter.getRouter().getRpcServer().getGroupsForUser("user");
    String[] result = syncReturn(String[].class);
    assertArrayEquals(group, result);
  }
}
