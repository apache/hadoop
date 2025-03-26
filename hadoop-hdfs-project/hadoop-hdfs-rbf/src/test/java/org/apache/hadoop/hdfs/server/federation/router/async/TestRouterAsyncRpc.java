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
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.fairness.RouterAsyncRpcFairnessPolicyController;
import org.apache.hadoop.hdfs.server.federation.fairness.RouterRpcFairnessPolicyController;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.router.TestRouterRpc;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.verifyFileExists;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_ASYNC_RPC_ENABLE_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_ASYNC_RPC_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FAIRNESS_POLICY_CONTROLLER_CLASS;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.syncReturn;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Testing the asynchronous RPC functionality of the router.
 */
public class TestRouterAsyncRpc extends TestRouterRpc {
  public static final Logger LOG = LoggerFactory.getLogger(TestRouterAsyncRpc.class);
  private static MiniRouterDFSCluster cluster;
  private MiniRouterDFSCluster.RouterContext rndRouter;

  @BeforeClass
  public static void globalSetUp() throws Exception {
    // Start routers with only an RPC service.
    Configuration routerConf = new RouterConfigBuilder()
        .metrics()
        .rpc()
        .build();
    // We decrease the DN cache times to make the test faster.
    routerConf.setTimeDuration(
        RBFConfigKeys.DN_REPORT_CACHE_EXPIRE, 1, TimeUnit.SECONDS);
    // Use async router.
    routerConf.setBoolean(DFS_ROUTER_ASYNC_RPC_ENABLE_KEY, true);
    // Use RouterAsyncRpcFairnessPolicyController as the fairness controller.
    routerConf.setClass(DFS_ROUTER_FAIRNESS_POLICY_CONTROLLER_CLASS,
        RouterAsyncRpcFairnessPolicyController.class,
        RouterRpcFairnessPolicyController.class);
    routerConf.setInt(DFS_ROUTER_ASYNC_RPC_HANDLER_COUNT_KEY, 1);
    setUp(routerConf);
  }

  @Before
  public void testSetup() throws Exception {
    super.testSetup();
    cluster = super.getCluster();
    // Random router for this test.
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

  @Test
  public void testCallerContextNotResetByAsyncHandler() throws IOException {
    GenericTestUtils.LogCapturer auditLog =
        GenericTestUtils.LogCapturer.captureLogs(FSNamesystem.AUDIT_LOG);
    String dirPath = "/test";
    
    // The reason we start this child thread is that CallContext use InheritableThreadLocal.
    Thread t1 = new Thread(() -> {
      // Set flag async:true.
      CallerContext.setCurrent(
          new CallerContext.Builder("async:true").build());
      // Issue some RPCs via the router to populate the CallerContext of async handler thread.
      for (int i = 0; i < 5; i++) {
        try {
          routerProtocol.mkdirs(dirPath, new FsPermission("755"), false);
          assertTrue(verifyFileExists(routerFS, dirPath));
          routerProtocol.delete(dirPath, true);
          assertFalse(verifyFileExists(routerFS, dirPath));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      // The audit log should contains async:true.
      assertTrue(auditLog.getOutput().contains("async:true"));
      auditLog.clearOutput();
      assertFalse(auditLog.getOutput().contains("async:true"));
    });
    
    t1.start();
    try {
      t1.join();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    routerProtocol.getFileInfo(dirPath);
    // The audit log should not contain async:true.
    assertFalse(auditLog.getOutput().contains("async:true"));
  }
}
