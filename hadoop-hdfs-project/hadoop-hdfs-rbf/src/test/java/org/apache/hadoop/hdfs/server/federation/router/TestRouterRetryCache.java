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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.retry.RetryInvocationHandler;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_IP_PROXY_USERS;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.NAMENODES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestRouterRetryCache {
  /** Federated HDFS cluster. */
  private MiniRouterDFSCluster cluster;

  @Before
  public  void setup() throws Exception {
    UserGroupInformation routerUser = UserGroupInformation.getLoginUser();
    Configuration conf = new Configuration();
    String adminUser = routerUser.getUserName();
    conf.set("hadoop.proxyuser." + adminUser + ".hosts", "*");
    conf.set("hadoop.proxyuser." + adminUser + ".groups", "*");
    conf.set("hadoop.proxyuser.fake_joe.hosts", "*");
    conf.set("hadoop.proxyuser.fake_joe.groups", "*");
    conf.set(DFS_NAMENODE_IP_PROXY_USERS, routerUser.getShortUserName());
    cluster = new MiniRouterDFSCluster(true, 1, conf);
    cluster.addNamenodeOverrides(conf);

    // Start NNs and DNs and wait until ready
    cluster.startCluster();

    // Start routers with only an RPC service
    cluster.startRouters();

    // Register and verify all NNs with all routers
    cluster.registerNamenodes();
    cluster.waitNamenodeRegistration();

    // Setup the mount table
    cluster.installMockLocations();

    // Making one Namenodes active per nameservice
    if (cluster.isHighAvailability()) {
      for (String ns : cluster.getNameservices()) {
        cluster.switchToActive(ns, NAMENODES[0]);
        cluster.switchToStandby(ns, NAMENODES[1]);
      }
    }
    cluster.waitActiveNamespaces();
  }

  @After
  public void teardown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testRetryCacheWithOneLevelProxyUser() throws Exception {
    internalTestRetryCache(false);
  }

  @Test
  public void testRetryCacheWithTwoLevelProxyUser() throws Exception {
    internalTestRetryCache(true);
  }

  /**
   * Test RetryCache through RBF with proxyUser and non-ProxyUser respectively.
   *
   * 1. Start cluster with current user.
   * 2. Create one test directory by the admin user.
   * 3. Create one Router FileSystem with one mocked user, one proxyUser or non-ProxyUser.
   * 4. Try to create one test directory by the router fileSystem.
   * 5. Try to rename the new test directory to one test destination directory
   * 6. Then failover the active to the standby
   * 7. Try to rename the source directory to the destination directory again with the same callId
   * 8. Try to
   */
  private void internalTestRetryCache(boolean twoLevelProxyUGI) throws Exception {
    RetryInvocationHandler.SET_CALL_ID_FOR_TEST.set(false);
    FileSystem routerFS = cluster.getRandomRouter().getFileSystem();
    Path testDir = new Path("/target-ns0/testdir");
    routerFS.mkdirs(testDir);
    routerFS.setPermission(testDir, FsPermission.getDefault());

    // Run as fake joe to authorize the test
    UserGroupInformation joe = UserGroupInformation.createUserForTesting("fake_joe",
        new String[] {"fake_group"});
    if (twoLevelProxyUGI) {
      joe = UserGroupInformation.createProxyUser("fake_proxy_joe", joe);
    }
    FileSystem joeFS = joe.doAs((PrivilegedExceptionAction<FileSystem>) () ->
        FileSystem.newInstance(routerFS.getUri(), routerFS.getConf()));

    Path renameSrc = new Path(testDir, "renameSrc");
    Path renameDst = new Path(testDir, "renameDst");
    joeFS.mkdirs(renameSrc);

    assertEquals(HAServiceProtocol.HAServiceState.ACTIVE,
        cluster.getCluster().getNamesystem(0).getState());

    int callId = Client.nextCallId();
    Client.setCallIdAndRetryCount(callId, 0, null);
    assertTrue(joeFS.rename(renameSrc, renameDst));

    Client.setCallIdAndRetryCount(callId, 0, null);
    assertTrue(joeFS.rename(renameSrc, renameDst));

    String ns0 = cluster.getNameservices().get(0);
    cluster.switchToStandby(ns0, NAMENODES[0]);
    cluster.switchToActive(ns0, NAMENODES[1]);

    assertEquals(HAServiceProtocol.HAServiceState.ACTIVE,
        cluster.getCluster().getNamesystem(1).getState());

    Client.setCallIdAndRetryCount(callId, 0, null);
    assertTrue(joeFS.rename(renameSrc, renameDst));

    FileStatus fileStatus = joeFS.getFileStatus(renameDst);
    if (twoLevelProxyUGI) {
      assertEquals("fake_proxy_joe", fileStatus.getOwner());
    } else {
      assertEquals("fake_joe", fileStatus.getOwner());
    }

    joeFS.delete(renameDst, true);
  }

  @Test
  public void testParseSpecialValue() {
    String mockContent = "mockContent,clientIp:127.0.0.1," +
        "clientCallId:12345,clientId:mockClientId";
    String clientIp = NameNode.parseSpecialValue(mockContent, "clientIp:");
    assertEquals("127.0.0.1", clientIp);

    String clientCallId = NameNode.parseSpecialValue(
        mockContent, "clientCallId:");
    assertEquals("12345", clientCallId);

    String clientId = NameNode.parseSpecialValue(mockContent, "clientId:");
    assertEquals("mockClientId", clientId);

    String clientRetryNum = NameNode.parseSpecialValue(
        mockContent, "clientRetryNum:");
    assertNull(clientRetryNum);
  }
}
