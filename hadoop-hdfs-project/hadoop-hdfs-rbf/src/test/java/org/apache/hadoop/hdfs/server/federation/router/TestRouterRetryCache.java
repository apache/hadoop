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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.io.retry.RetryInvocationHandler;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_IP_PROXY_USERS;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.NAMENODES;

public class TestRouterRetryCache {
  /** Federated HDFS cluster. */
  private MiniRouterDFSCluster cluster;

  @Before
  public  void setup() throws Exception {
    Configuration namenodeConf = new Configuration();
    namenodeConf.set(DFS_NAMENODE_IP_PROXY_USERS, "fake_joe");
    cluster = new MiniRouterDFSCluster(true, 1);
    cluster.addNamenodeOverrides(namenodeConf);

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
  public void testRetryCache() throws Exception {
    RetryInvocationHandler.setCallIdForTest.set(false);
    FileSystem routerFS = cluster.getRandomRouter().getFileSystem();
    Path testDir = new Path("/target-ns0/testdir");
    routerFS.mkdirs(testDir);
    routerFS.setPermission(testDir, FsPermission.getDefault());

    // Run as fake joe to authorize the test
    UserGroupInformation joe =
        UserGroupInformation.createUserForTesting("fake_joe",
            new String[]{"fake_group"});
    FileSystem joeFS = joe.doAs(
        (PrivilegedExceptionAction<FileSystem>) () ->
            FileSystem.newInstance(routerFS.getUri(), routerFS.getConf()));

    Path renameSrc = new Path(testDir, "renameSrc");
    Path renameDst = new Path(testDir, "renameDst");
    joeFS.mkdirs(renameSrc);

    Assert.assertEquals(HAServiceProtocol.HAServiceState.ACTIVE,
        cluster.getCluster().getNamesystem(0).getState());

    int callId = Client.nextCallId();
    Client.setCallIdAndRetryCount(callId, 0, null);
    Assert.assertTrue(joeFS.rename(renameSrc, renameDst));

    Client.setCallIdAndRetryCount(callId, 0, null);
    Assert.assertTrue(joeFS.rename(renameSrc, renameDst));

    String ns0 = cluster.getNameservices().get(0);
    cluster.switchToStandby(ns0, NAMENODES[0]);
    cluster.switchToActive(ns0, NAMENODES[1]);

    Assert.assertEquals(HAServiceProtocol.HAServiceState.ACTIVE,
        cluster.getCluster().getNamesystem(1).getState());

    Client.setCallIdAndRetryCount(callId, 0, null);
    Assert.assertTrue(joeFS.rename(renameSrc, renameDst));
  }
}
