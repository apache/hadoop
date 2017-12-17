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

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.RouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.impl.MountTableStoreImpl;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.tools.federation.RouterAdmin;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
/**
 * Tests Router admin commands.
 */
public class TestRouterAdminCLI {
  private static StateStoreDFSCluster cluster;
  private static RouterContext routerContext;
  private static StateStoreService stateStore;

  private static RouterAdmin admin;
  private static RouterClient client;

  private static final String TEST_USER = "test-user";

  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private static final PrintStream OLD_OUT = System.out;

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

    // Start routers
    cluster.startRouters();

    routerContext = cluster.getRandomRouter();
    Router router = routerContext.getRouter();
    stateStore = router.getStateStore();

    Configuration routerConf = new Configuration();
    InetSocketAddress routerSocket = router.getAdminServerAddress();
    routerConf.setSocketAddr(DFSConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_KEY,
        routerSocket);
    admin = new RouterAdmin(routerConf);
    client = routerContext.getAdminClient();
  }

  @AfterClass
  public static void tearDown() {
    cluster.stopRouter(routerContext);
    cluster.shutdown();
    cluster = null;
  }

  @Test
  public void testMountTableDefaultACL() throws Exception {
    String[] argv = new String[] {"-add", "/testpath0", "ns0", "/testdir0"};
    Assert.assertEquals(0, ToolRunner.run(admin, argv));

    stateStore.loadCache(MountTableStoreImpl.class, true);
    GetMountTableEntriesRequest getRequest = GetMountTableEntriesRequest
        .newInstance("/testpath0");
    GetMountTableEntriesResponse getResponse = client.getMountTableManager()
        .getMountTableEntries(getRequest);
    MountTable mountTable = getResponse.getEntries().get(0);

    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    String group = ugi.getGroups().isEmpty() ? ugi.getShortUserName()
        : ugi.getPrimaryGroupName();
    assertEquals(ugi.getShortUserName(), mountTable.getOwnerName());
    assertEquals(group, mountTable.getGroupName());
    assertEquals((short) 0755, mountTable.getMode().toShort());
  }

  @Test
  public void testMountTablePermissions() throws Exception {
    // re-set system out for testing
    System.setOut(new PrintStream(out));
    // use superuser to add new mount table with only read permission
    String[] argv = new String[] {"-add", "/testpath2-1", "ns0", "/testdir2-1",
        "-owner", TEST_USER, "-group", TEST_USER, "-mode", "0455"};
    assertEquals(0, ToolRunner.run(admin, argv));

    String superUser = UserGroupInformation.
        getCurrentUser().getShortUserName();
    // use normal user as current user to test
    UserGroupInformation remoteUser = UserGroupInformation
        .createRemoteUser(TEST_USER);
    UserGroupInformation.setLoginUser(remoteUser);

    // verify read permission by executing other commands
    verifyExecutionResult("/testpath2-1", true, -1, -1);

    // add new mount table with only write permission
    argv = new String[] {"-add", "/testpath2-2", "ns0", "/testdir2-2",
        "-owner", TEST_USER, "-group", TEST_USER, "-mode", "0255"};
    assertEquals(0, ToolRunner.run(admin, argv));
    verifyExecutionResult("/testpath2-2", false, 0, 0);

    // set mount table entry with read and write permission
    argv = new String[] {"-add", "/testpath2-3", "ns0", "/testdir2-3",
        "-owner", TEST_USER, "-group", TEST_USER, "-mode", "0755"};
    assertEquals(0, ToolRunner.run(admin, argv));
    verifyExecutionResult("/testpath2-3", true, 0, 0);

    // set back system out and login user
    System.setOut(OLD_OUT);
    remoteUser = UserGroupInformation.createRemoteUser(superUser);
    UserGroupInformation.setLoginUser(remoteUser);
  }

  /**
   * Verify router admin commands execution result.
   *
   * @param mount
   *          target mount table
   * @param canRead
   *          whether can list mount tables under specified mount
   * @param addCommandCode
   *          expected return code of add command executed for specified mount
   * @param rmCommandCode
   *          expected return code of rm command executed for specified mount
   * @throws Exception
   */
  private void verifyExecutionResult(String mount, boolean canRead,
      int addCommandCode, int rmCommandCode) throws Exception {
    String[] argv = null;
    stateStore.loadCache(MountTableStoreImpl.class, true);

    out.reset();
    // execute ls command
    argv = new String[] {"-ls", mount};
    assertEquals(0, ToolRunner.run(admin, argv));
    assertEquals(canRead, out.toString().contains(mount));

    // execute add/update command
    argv = new String[] {"-add", mount, "ns0", mount + "newdir"};
    assertEquals(addCommandCode, ToolRunner.run(admin, argv));

    stateStore.loadCache(MountTableStoreImpl.class, true);
    // execute remove command
    argv = new String[] {"-rm", mount};
    assertEquals(rmCommandCode, ToolRunner.run(admin, argv));
  }
}