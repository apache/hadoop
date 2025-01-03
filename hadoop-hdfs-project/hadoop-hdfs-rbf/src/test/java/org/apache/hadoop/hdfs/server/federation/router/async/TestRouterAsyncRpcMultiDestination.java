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
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.router.TestRouterRpcMultiDestination;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_MAX_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_WITH_REMOTE_PORT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_RPC_ENABLE_ASYNC;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.syncReturn;
import static org.junit.Assert.assertArrayEquals;

public class TestRouterAsyncRpcMultiDestination extends TestRouterRpcMultiDestination {
  private static MiniRouterDFSCluster cluster;
  private MiniRouterDFSCluster.RouterContext rndRouter;

  @BeforeClass
  public static void globalSetUp() throws Exception {
    Configuration namenodeConf = new Configuration();
    namenodeConf.setBoolean(DFSConfigKeys.HADOOP_CALLER_CONTEXT_ENABLED_KEY,
        true);
    namenodeConf.set(HADOOP_CALLER_CONTEXT_MAX_SIZE_KEY, "256");
    // It's very easy to become overloaded for some specific dn in this small
    // cluster, which will cause the EC file block allocation failure. To avoid
    // this issue, we disable considerLoad option.
    namenodeConf.setBoolean(DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY, false);
    namenodeConf.setBoolean(DFS_NAMENODE_AUDIT_LOG_WITH_REMOTE_PORT_KEY, true);
    cluster = new MiniRouterDFSCluster(false, NUM_SUBCLUSTERS);
    cluster.setNumDatanodesPerNameservice(NUM_DNS);
    cluster.addNamenodeOverrides(namenodeConf);
    cluster.setIndependentDNs();

    Configuration conf = new Configuration();
    // Setup proxy users.
    conf.set("hadoop.proxyuser.testRealUser.groups", "*");
    conf.set("hadoop.proxyuser.testRealUser.hosts", "*");
    String loginUser = UserGroupInformation.getLoginUser().getUserName();
    conf.set(String.format("hadoop.proxyuser.%s.groups", loginUser), "*");
    conf.set(String.format("hadoop.proxyuser.%s.hosts", loginUser), "*");
    // Enable IP proxy users.
    conf.set(DFSConfigKeys.DFS_NAMENODE_IP_PROXY_USERS, "placeholder");
    conf.setInt(DFSConfigKeys.DFS_LIST_LIMIT, 5);
    cluster.addNamenodeOverrides(conf);
    // Start NNs and DNs and wait until ready
    cluster.startCluster();

    // Start routers with only an RPC service
    Configuration routerConf = new RouterConfigBuilder()
        .metrics()
        .rpc()
        .build();
    // We decrease the DN cache times to make the test faster
    routerConf.setTimeDuration(
        RBFConfigKeys.DN_REPORT_CACHE_EXPIRE, 1, TimeUnit.SECONDS);
    routerConf.setBoolean(DFS_ROUTER_RPC_ENABLE_ASYNC, true);
    cluster.addRouterOverrides(routerConf);
    cluster.startRouters();

    // Register and verify all NNs with all routers
    cluster.registerNamenodes();
    cluster.waitNamenodeRegistration();

    // We decrease the DN heartbeat expire interval to make them dead faster
    cluster.getCluster().getNamesystem(0).getBlockManager()
        .getDatanodeManager().setHeartbeatInterval(1);
    cluster.getCluster().getNamesystem(1).getBlockManager()
        .getDatanodeManager().setHeartbeatInterval(1);
    cluster.getCluster().getNamesystem(0).getBlockManager()
        .getDatanodeManager().setHeartbeatExpireInterval(3000);
    cluster.getCluster().getNamesystem(1).getBlockManager()
        .getDatanodeManager().setHeartbeatExpireInterval(3000);
    setCluster(cluster);
  }

  public static void tearDown() {
    cluster.shutdown();
  }

  @Before
  public void testSetup() throws Exception {
    super.testSetup();
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
