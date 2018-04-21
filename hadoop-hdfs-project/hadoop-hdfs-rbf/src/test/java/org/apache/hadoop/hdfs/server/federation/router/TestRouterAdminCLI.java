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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.resolver.order.DestinationOrder;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.impl.DisabledNameserviceStoreImpl;
import org.apache.hadoop.hdfs.server.federation.store.impl.MountTableStoreImpl;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.tools.federation.RouterAdmin;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Supplier;

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
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();
  private static final PrintStream OLD_OUT = System.out;
  private static final PrintStream OLD_ERR = System.err;

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
    routerConf.setSocketAddr(RBFConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_KEY,
        routerSocket);
    admin = new RouterAdmin(routerConf);
    client = routerContext.getAdminClient();
  }

  @AfterClass
  public static void tearDownCluster() {
    cluster.stopRouter(routerContext);
    cluster.shutdown();
    cluster = null;
  }

  @After
  public void tearDown() {
    // set back system out/err
    System.setOut(OLD_OUT);
    System.setErr(OLD_ERR);
  }

  @Test
  public void testAddMountTable() throws Exception {
    String nsId = "ns0";
    String src = "/test-addmounttable";
    String dest = "/addmounttable";
    String[] argv = new String[] {"-add", src, nsId, dest};
    assertEquals(0, ToolRunner.run(admin, argv));

    stateStore.loadCache(MountTableStoreImpl.class, true);
    GetMountTableEntriesRequest getRequest = GetMountTableEntriesRequest
        .newInstance(src);
    GetMountTableEntriesResponse getResponse = client.getMountTableManager()
        .getMountTableEntries(getRequest);
    MountTable mountTable = getResponse.getEntries().get(0);

    List<RemoteLocation> destinations = mountTable.getDestinations();
    assertEquals(1, destinations.size());

    assertEquals(src, mountTable.getSourcePath());
    assertEquals(nsId, destinations.get(0).getNameserviceId());
    assertEquals(dest, destinations.get(0).getDest());
    assertFalse(mountTable.isReadOnly());

    // test mount table update behavior
    dest = dest + "-new";
    argv = new String[] {"-add", src, nsId, dest, "-readonly"};
    assertEquals(0, ToolRunner.run(admin, argv));
    stateStore.loadCache(MountTableStoreImpl.class, true);

    getResponse = client.getMountTableManager()
        .getMountTableEntries(getRequest);
    mountTable = getResponse.getEntries().get(0);
    assertEquals(2, mountTable.getDestinations().size());
    assertEquals(nsId, mountTable.getDestinations().get(1).getNameserviceId());
    assertEquals(dest, mountTable.getDestinations().get(1).getDest());
    assertTrue(mountTable.isReadOnly());
  }

  @Test
  public void testAddOrderMountTable() throws Exception {
    testAddOrderMountTable(DestinationOrder.HASH);
    testAddOrderMountTable(DestinationOrder.LOCAL);
    testAddOrderMountTable(DestinationOrder.RANDOM);
    testAddOrderMountTable(DestinationOrder.HASH_ALL);
  }

  private void testAddOrderMountTable(DestinationOrder order)
      throws Exception {
    final String mnt = "/" + order;
    final String nsId = "ns0,ns1";
    final String dest = "/";
    String[] argv = new String[] {
        "-add", mnt, nsId, dest, "-order", order.toString()};
    assertEquals(0, ToolRunner.run(admin, argv));

    // Check the state in the State Store
    stateStore.loadCache(MountTableStoreImpl.class, true);
    MountTableManager mountTable = client.getMountTableManager();
    GetMountTableEntriesRequest request =
        GetMountTableEntriesRequest.newInstance(mnt);
    GetMountTableEntriesResponse response =
        mountTable.getMountTableEntries(request);
    List<MountTable> entries = response.getEntries();
    assertEquals(1, entries.size());
    assertEquals(2, entries.get(0).getDestinations().size());
    assertEquals(order, response.getEntries().get(0).getDestOrder());
  }

  @Test
  public void testListMountTable() throws Exception {
    String nsId = "ns0";
    String src = "/test-lsmounttable";
    String dest = "/lsmounttable";
    String[] argv = new String[] {"-add", src, nsId, dest};
    assertEquals(0, ToolRunner.run(admin, argv));

    // re-set system out for testing
    System.setOut(new PrintStream(out));
    stateStore.loadCache(MountTableStoreImpl.class, true);
    argv = new String[] {"-ls", src};
    assertEquals(0, ToolRunner.run(admin, argv));
    assertTrue(out.toString().contains(src));

    out.reset();
    GetMountTableEntriesRequest getRequest = GetMountTableEntriesRequest
        .newInstance("/");
    GetMountTableEntriesResponse getResponse = client.getMountTableManager()
        .getMountTableEntries(getRequest);

    // Test ls command without input path, it will list
    // mount table under root path.
    argv = new String[] {"-ls"};
    assertEquals(0, ToolRunner.run(admin, argv));
    assertTrue(out.toString().contains(src));
    String outStr = out.toString();
    // verify if all the mount table are listed
    for(MountTable entry: getResponse.getEntries()) {
      assertTrue(outStr.contains(entry.getSourcePath()));
    }
  }

  @Test
  public void testRemoveMountTable() throws Exception {
    String nsId = "ns0";
    String src = "/test-rmmounttable";
    String dest = "/rmmounttable";
    String[] argv = new String[] {"-add", src, nsId, dest};
    assertEquals(0, ToolRunner.run(admin, argv));

    stateStore.loadCache(MountTableStoreImpl.class, true);
    GetMountTableEntriesRequest getRequest = GetMountTableEntriesRequest
        .newInstance(src);
    GetMountTableEntriesResponse getResponse = client.getMountTableManager()
        .getMountTableEntries(getRequest);
    // ensure mount table added successfully
    MountTable mountTable = getResponse.getEntries().get(0);
    assertEquals(src, mountTable.getSourcePath());

    argv = new String[] {"-rm", src};
    assertEquals(0, ToolRunner.run(admin, argv));

    stateStore.loadCache(MountTableStoreImpl.class, true);
    getResponse = client.getMountTableManager()
        .getMountTableEntries(getRequest);
    assertEquals(0, getResponse.getEntries().size());

    // remove an invalid mount table
    String invalidPath = "/invalid";
    System.setOut(new PrintStream(out));
    argv = new String[] {"-rm", invalidPath};
    assertEquals(0, ToolRunner.run(admin, argv));
    assertTrue(out.toString().contains(
        "Cannot remove mount point " + invalidPath));
  }

  @Test
  public void testMountTableDefaultACL() throws Exception {
    String[] argv = new String[] {"-add", "/testpath0", "ns0", "/testdir0"};
    assertEquals(0, ToolRunner.run(admin, argv));

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

    // set back login user
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

  @Test
  public void testSetAndClearQuota() throws Exception {
    String nsId = "ns0";
    String src = "/test-QuotaMounttable";
    String dest = "/QuotaMounttable";
    String[] argv = new String[] {"-add", src, nsId, dest};
    assertEquals(0, ToolRunner.run(admin, argv));

    stateStore.loadCache(MountTableStoreImpl.class, true);
    GetMountTableEntriesRequest getRequest = GetMountTableEntriesRequest
        .newInstance(src);
    GetMountTableEntriesResponse getResponse = client.getMountTableManager()
        .getMountTableEntries(getRequest);
    MountTable mountTable = getResponse.getEntries().get(0);
    RouterQuotaUsage quotaUsage = mountTable.getQuota();

    // verify the default quota set
    assertEquals(RouterQuotaUsage.QUOTA_USAGE_COUNT_DEFAULT,
        quotaUsage.getFileAndDirectoryCount());
    assertEquals(HdfsConstants.QUOTA_DONT_SET, quotaUsage.getQuota());
    assertEquals(RouterQuotaUsage.QUOTA_USAGE_COUNT_DEFAULT,
        quotaUsage.getSpaceConsumed());
    assertEquals(HdfsConstants.QUOTA_DONT_SET, quotaUsage.getSpaceQuota());

    long nsQuota = 50;
    long ssQuota = 100;
    argv = new String[] {"-setQuota", src, "-nsQuota", String.valueOf(nsQuota),
        "-ssQuota", String.valueOf(ssQuota)};
    assertEquals(0, ToolRunner.run(admin, argv));

    stateStore.loadCache(MountTableStoreImpl.class, true);
    getResponse = client.getMountTableManager()
        .getMountTableEntries(getRequest);
    mountTable = getResponse.getEntries().get(0);
    quotaUsage = mountTable.getQuota();

    // verify if the quota is set
    assertEquals(nsQuota, quotaUsage.getQuota());
    assertEquals(ssQuota, quotaUsage.getSpaceQuota());

    // use quota string for setting ss quota
    String newSsQuota = "2m";
    argv = new String[] {"-setQuota", src, "-ssQuota", newSsQuota};
    assertEquals(0, ToolRunner.run(admin, argv));

    stateStore.loadCache(MountTableStoreImpl.class, true);
    getResponse = client.getMountTableManager()
        .getMountTableEntries(getRequest);
    mountTable = getResponse.getEntries().get(0);
    quotaUsage = mountTable.getQuota();
    // verify if ns quota keeps quondam value
    assertEquals(nsQuota, quotaUsage.getQuota());
    // verify if ss quota is correctly set
    assertEquals(2 * 1024 * 1024, quotaUsage.getSpaceQuota());

    // test clrQuota command
    argv = new String[] {"-clrQuota", src};
    assertEquals(0, ToolRunner.run(admin, argv));

    stateStore.loadCache(MountTableStoreImpl.class, true);
    getResponse = client.getMountTableManager()
        .getMountTableEntries(getRequest);
    mountTable = getResponse.getEntries().get(0);
    quotaUsage = mountTable.getQuota();

    // verify if quota unset successfully
    assertEquals(HdfsConstants.QUOTA_DONT_SET, quotaUsage.getQuota());
    assertEquals(HdfsConstants.QUOTA_DONT_SET, quotaUsage.getSpaceQuota());
  }

  @Test
  public void testManageSafeMode() throws Exception {
    // ensure the Router become RUNNING state
    waitState(RouterServiceState.RUNNING);
    assertFalse(routerContext.getRouter().getRpcServer().isInSafeMode());
    assertEquals(0, ToolRunner.run(admin,
        new String[] {"-safemode", "enter"}));
    // verify state
    assertEquals(RouterServiceState.SAFEMODE,
        routerContext.getRouter().getRouterState());
    assertTrue(routerContext.getRouter().getRpcServer().isInSafeMode());

    System.setOut(new PrintStream(out));
    assertEquals(0, ToolRunner.run(admin,
        new String[] {"-safemode", "get"}));
    assertTrue(out.toString().contains("true"));

    assertEquals(0, ToolRunner.run(admin,
        new String[] {"-safemode", "leave"}));
    // verify state
    assertEquals(RouterServiceState.RUNNING,
        routerContext.getRouter().getRouterState());
    assertFalse(routerContext.getRouter().getRpcServer().isInSafeMode());

    out.reset();
    assertEquals(0, ToolRunner.run(admin,
        new String[] {"-safemode", "get"}));
    assertTrue(out.toString().contains("false"));
  }

  @Test
  public void testCreateInvalidEntry() throws Exception {
    String[] argv = new String[] {
        "-add", "test-createInvalidEntry", "ns0", "/createInvalidEntry"};
    assertEquals(-1, ToolRunner.run(admin, argv));

    argv = new String[] {
        "-add", "/test-createInvalidEntry", "ns0", "createInvalidEntry"};
    assertEquals(-1, ToolRunner.run(admin, argv));

    argv = new String[] {
        "-add", null, "ns0", "/createInvalidEntry"};
    assertEquals(-1, ToolRunner.run(admin, argv));

    argv = new String[] {
        "-add", "/test-createInvalidEntry", "ns0", null};
    assertEquals(-1, ToolRunner.run(admin, argv));

    argv = new String[] {
        "-add", "", "ns0", "/createInvalidEntry"};
    assertEquals(-1, ToolRunner.run(admin, argv));

    argv = new String[] {
        "-add", "/test-createInvalidEntry", null, "/createInvalidEntry"};
    assertEquals(-1, ToolRunner.run(admin, argv));

    argv = new String[] {
        "-add", "/test-createInvalidEntry", "", "/createInvalidEntry"};
    assertEquals(-1, ToolRunner.run(admin, argv));
  }

  @Test
  public void testNameserviceManager() throws Exception {
    // Disable a name service and check if it's disabled
    assertEquals(0, ToolRunner.run(admin,
        new String[] {"-nameservice", "disable", "ns0"}));

    stateStore.loadCache(DisabledNameserviceStoreImpl.class, true);
    System.setOut(new PrintStream(out));
    assertEquals(0, ToolRunner.run(admin,
        new String[] {"-getDisabledNameservices"}));
    assertTrue("ns0 should be reported: " + out,
        out.toString().contains("ns0"));

    // Enable a name service and check if it's there
    assertEquals(0, ToolRunner.run(admin,
        new String[] {"-nameservice", "enable", "ns0"}));

    out.reset();
    stateStore.loadCache(DisabledNameserviceStoreImpl.class, true);
    assertEquals(0, ToolRunner.run(admin,
        new String[] {"-getDisabledNameservices"}));
    assertFalse("ns0 should not be reported: " + out,
        out.toString().contains("ns0"));

    // Wrong commands
    System.setErr(new PrintStream(err));
    assertEquals(-1, ToolRunner.run(admin,
        new String[] {"-nameservice", "enable"}));
    String msg = "Not enough parameters specificed for cmd -nameservice";
    assertTrue("Got error: " + err.toString(),
        err.toString().startsWith(msg));

    err.reset();
    assertEquals(-1, ToolRunner.run(admin,
        new String[] {"-nameservice", "wrong", "ns0"}));
    assertTrue("Got error: " + err.toString(),
        err.toString().startsWith("nameservice: Unknown command: wrong"));
  }

  /**
   * Wait for the Router transforming to expected state.
   * @param expectedState Expected Router state.
   * @throws Exception
   */
  private void waitState(final RouterServiceState expectedState)
      throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return expectedState == routerContext.getRouter().getRouterState();
      }
    }, 1000, 30000);
  }
}