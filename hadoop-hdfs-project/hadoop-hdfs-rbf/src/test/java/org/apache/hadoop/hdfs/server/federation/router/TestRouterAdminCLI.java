/*
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

import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.createNamenodeReport;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.metrics.RBFMetrics;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.MultipleDestinationMountTableResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.resolver.order.DestinationOrder;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.impl.DisabledNameserviceStoreImpl;
import org.apache.hadoop.hdfs.server.federation.store.impl.MountTableStoreImpl;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipState;
import org.apache.hadoop.hdfs.server.federation.store.records.MockStateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.tools.federation.RouterAdmin;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.Whitebox;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.function.Supplier;

/**
 * Tests Router admin commands.
 */
public class TestRouterAdminCLI {
  private static StateStoreDFSCluster cluster;
  private static RouterContext routerContext;
  private static StateStoreService stateStore;

  private static RouterAdmin admin;
  private static RouterClient client;
  private static Router router;

  private static final String TEST_USER = "test-user";

  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();
  private static final PrintStream OLD_OUT = System.out;
  private static final PrintStream OLD_ERR = System.err;

  @BeforeClass
  public static void globalSetUp() throws Exception {
    cluster = new StateStoreDFSCluster(false, 1,
        MultipleDestinationMountTableResolver.class);
    // Build and start a router with State Store + admin + RPC
    Configuration conf = new RouterConfigBuilder()
        .stateStore()
        .metrics()
        .admin()
        .rpc()
        .quota()
        .safemode()
        .build();
    cluster.addRouterOverrides(conf);

    // Start routers
    cluster.startRouters();

    routerContext = cluster.getRandomRouter();
    router = routerContext.getRouter();
    stateStore = router.getStateStore();

    Configuration routerConf = new Configuration();
    InetSocketAddress routerSocket = router.getAdminServerAddress();
    routerConf.setSocketAddr(RBFConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_KEY,
        routerSocket);
    admin = new RouterAdmin(routerConf);
    client = routerContext.getAdminClient();

    // Add two fake name services to testing disabling them
    ActiveNamenodeResolver membership = router.getNamenodeResolver();
    membership.registerNamenode(
        createNamenodeReport("ns0", "nn1", HAServiceState.ACTIVE));
    membership.registerNamenode(
        createNamenodeReport("ns1", "nn1", HAServiceState.ACTIVE));
    stateStore.refreshCaches(true);

    // Mock the quota module since no real namenode is started up.
    Quota quota = Mockito
        .spy(routerContext.getRouter().createRpcServer().getQuotaModule());
    Mockito.doNothing().when(quota)
        .setQuota(Mockito.anyString(), Mockito.anyLong(), Mockito.anyLong(),
            Mockito.any(), Mockito.anyBoolean());
    Whitebox.setInternalState(
        routerContext.getRouter().getRpcServer(), "quotaCall", quota);

    RouterRpcServer spyRpcServer =
        Mockito.spy(routerContext.getRouter().createRpcServer());
    Whitebox
        .setInternalState(routerContext.getRouter(), "rpcServer", spyRpcServer);

    Mockito.doReturn(null).when(spyRpcServer).getFileInfo(Mockito.anyString());

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
    String nsId = "ns0,ns1";
    String src = "/test-addmounttable";
    String dest = "/addmounttable";
    String[] argv = new String[] {"-add", src, nsId, dest};
    assertEquals(0, ToolRunner.run(admin, argv));
    assertEquals(-1, ToolRunner.run(admin, argv));

    stateStore.loadCache(MountTableStoreImpl.class, true);
    verifyMountTableContents(src, dest);

    GetMountTableEntriesRequest getRequest = GetMountTableEntriesRequest
        .newInstance(src);
    GetMountTableEntriesResponse getResponse = client.getMountTableManager()
        .getMountTableEntries(getRequest);
    MountTable mountTable = getResponse.getEntries().get(0);

    List<RemoteLocation> destinations = mountTable.getDestinations();
    assertEquals(2, destinations.size());

    assertEquals(src, mountTable.getSourcePath());
    assertEquals("ns0", destinations.get(0).getNameserviceId());
    assertEquals(dest, destinations.get(0).getDest());
    assertEquals("ns1", destinations.get(1).getNameserviceId());
    assertEquals(dest, destinations.get(1).getDest());
    assertFalse(mountTable.isReadOnly());
    assertFalse(mountTable.isFaultTolerant());

    // test mount table update behavior
    dest = dest + "-new";
    argv = new String[] {"-add", src, nsId, dest, "-readonly",
        "-faulttolerant", "-order", "HASH_ALL"};
    assertEquals(0, ToolRunner.run(admin, argv));
    stateStore.loadCache(MountTableStoreImpl.class, true);

    getResponse = client.getMountTableManager()
        .getMountTableEntries(getRequest);
    mountTable = getResponse.getEntries().get(0);
    assertEquals(4, mountTable.getDestinations().size());
    RemoteLocation loc2 = mountTable.getDestinations().get(2);
    assertEquals("ns0", loc2.getNameserviceId());
    assertEquals(dest, loc2.getDest());
    RemoteLocation loc3 = mountTable.getDestinations().get(3);
    assertEquals("ns1", loc3.getNameserviceId());
    assertEquals(dest, loc3.getDest());
    assertTrue(mountTable.isReadOnly());
    assertTrue(mountTable.isFaultTolerant());
  }

  private void verifyMountTableContents(String src, String dest) throws Exception {
    String[] argv = new String[] {"-ls", "/"};
    System.setOut(new PrintStream(out));
    assertEquals(0, ToolRunner.run(admin, argv));
    String response = out.toString();
    assertTrue("The response should have " + src + ": " + response, response.contains(src));
    assertTrue("The response should have " + dest + ": " + response, response.contains(dest));
  }

  @Test
  public void testAddMountTableNotNormalized() throws Exception {
    String nsId = "ns0";
    String src = "/test-addmounttable-notnormalized";
    String srcWithSlash = src + "/";
    String dest = "/addmounttable-notnormalized";
    String[] argv = new String[] {"-add", srcWithSlash, nsId, dest};
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
    assertFalse(mountTable.isFaultTolerant());

    // test mount table update behavior
    dest = dest + "-new";
    argv = new String[] {"-add", srcWithSlash, nsId, dest, "-readonly"};
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
    testAddOrderMountTable(DestinationOrder.SPACE);
  }

  @Test
  public void testAddOrderErrorMsg() throws Exception {
    DestinationOrder order = DestinationOrder.HASH;
    final String mnt = "/newAdd1" + order;
    final String nsId = "ns0,ns1";
    final String dest = "/changAdd";

    String[] argv1 = new String[] {"-add", mnt, nsId, dest, "-order",
        order.toString()};
    assertEquals(0, ToolRunner.run(admin, argv1));

    // Add the order with wrong command
    String[] argv = new String[] {"-add", mnt, nsId, dest, "-orde",
        order.toString()};
    assertEquals(-1, ToolRunner.run(admin, argv));

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
    String srcWithSlash = src + "/";
    String dest = "/lsmounttable";
    String[] argv = new String[] {"-add", src, nsId, dest};
    assertEquals(0, ToolRunner.run(admin, argv));

    // re-set system out for testing
    System.setOut(new PrintStream(out));
    stateStore.loadCache(MountTableStoreImpl.class, true);
    argv = new String[] {"-ls", src};
    assertEquals(0, ToolRunner.run(admin, argv));
    String response = out.toString();
    assertTrue("Wrong response: " + response, response.contains(src));

    // Test with not-normalized src input
    argv = new String[] {"-ls", srcWithSlash};
    assertEquals(0, ToolRunner.run(admin, argv));
    response = out.toString();
    assertTrue("Wrong response: " + response, response.contains(src));

    // Test with wrong number of arguments
    argv = new String[] {"-ls", srcWithSlash, "check", "check2"};
    System.setErr(new PrintStream(err));
    ToolRunner.run(admin, argv);
    response = err.toString();
    assertTrue("Wrong response: " + response,
        response.contains("Too many arguments, Max=2 argument allowed"));

    out.reset();
    GetMountTableEntriesRequest getRequest = GetMountTableEntriesRequest
        .newInstance("/");
    GetMountTableEntriesResponse getResponse = client.getMountTableManager()
        .getMountTableEntries(getRequest);

    // Test ls command without input path, it will list
    // mount table under root path.
    argv = new String[] {"-ls"};
    assertEquals(0, ToolRunner.run(admin, argv));
    response = out.toString();
    assertTrue("Wrong response: " + response, response.contains(src));
    // verify if all the mount table are listed
    for (MountTable entry : getResponse.getEntries()) {
      assertTrue("Wrong response: " + response,
          response.contains(entry.getSourcePath()));
    }
  }

  @Test
  public void testListWithDetails() throws Exception {
    // Create mount entry.
    String[] argv = new String[] {"-add", "/testLsWithDetails", "ns0,ns1",
        "/dest", "-order", "HASH_ALL", "-readonly", "-faulttolerant"};
    assertEquals(0, ToolRunner.run(admin, argv));
    System.setOut(new PrintStream(out));
    stateStore.loadCache(MountTableStoreImpl.class, true);

    // Test list with detail for a mount entry.
    argv = new String[] {"-ls", "-d", "/testLsWithDetails"};
    assertEquals(0, ToolRunner.run(admin, argv));
    String response =  out.toString();
    assertTrue(response.contains("Read-Only"));
    assertTrue(response.contains("Fault-Tolerant"));
    out.reset();

    // Test list with detail without path.
    argv = new String[] {"-ls", "-d"};
    assertEquals(0, ToolRunner.run(admin, argv));
    response =  out.toString();
    assertTrue("Wrong response: " + response, response.contains("Read-Only"));
    assertTrue("Wrong response: " + response,
        response.contains("Fault-Tolerant"));
  }

  @Test
  public void testListNestedMountTable() throws Exception {
    String dir1 = "/test-ls";
    String dir2 = "/test-ls-longger";
    String[] nsIdList = {"ns0", "ns1", "ns2", "ns3", "ns3"};
    String[] sourceList =
        {dir1, dir1 + "/subdir1", dir2, dir2 + "/subdir1", dir2 + "/subdir2"};
    String[] destList =
        {"/test-ls", "/test-ls/subdir1", "/ls", "/ls/subdir1", "/ls/subdir2"};
    for (int i = 0; i < nsIdList.length; i++) {
      String[] argv =
          new String[] {"-add", sourceList[i], nsIdList[i], destList[i]};
      assertEquals(0, ToolRunner.run(admin, argv));
    }

    // prepare for test
    System.setOut(new PrintStream(out));
    stateStore.loadCache(MountTableStoreImpl.class, true);

    // Test ls dir1
    String[] argv = new String[] {"-ls", dir1};
    assertEquals(0, ToolRunner.run(admin, argv));
    String outStr = out.toString();
    assertTrue(out.toString().contains(dir1 + "/subdir1"));
    assertFalse(out.toString().contains(dir2));

    // Test ls dir2
    argv = new String[] {"-ls", dir2};
    assertEquals(0, ToolRunner.run(admin, argv));
    assertTrue(out.toString().contains(dir2 + "/subdir1"));
    assertTrue(out.toString().contains(dir2 + "/subdir2"));
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
  public void testMultiArgsRemoveMountTable() throws Exception {
    String nsId = "ns0";
    String src1 = "/test-rmmounttable1";
    String src2 = "/test-rmmounttable2";
    String dest1 = "/rmmounttable1";
    String dest2 = "/rmmounttable2";
    // Adding mount table entries
    String[] argv = new String[] {"-add", src1, nsId, dest1};
    assertEquals(0, ToolRunner.run(admin, argv));
    argv = new String[] {"-add", src2, nsId, dest2};
    assertEquals(0, ToolRunner.run(admin, argv));

    stateStore.loadCache(MountTableStoreImpl.class, true);
    // Ensure mount table entries added successfully
    GetMountTableEntriesRequest getRequest =
        GetMountTableEntriesRequest.newInstance(src1);
    GetMountTableEntriesResponse getResponse =
        client.getMountTableManager().getMountTableEntries(getRequest);
    MountTable mountTable = getResponse.getEntries().get(0);
    getRequest = GetMountTableEntriesRequest.newInstance(src2);
    getResponse =
        client.getMountTableManager().getMountTableEntries(getRequest);
    assertEquals(src1, mountTable.getSourcePath());
    mountTable = getResponse.getEntries().get(0);
    assertEquals(src2, mountTable.getSourcePath());
    // Remove multiple mount table entries
    argv = new String[] {"-rm", src1, src2};
    assertEquals(0, ToolRunner.run(admin, argv));

    stateStore.loadCache(MountTableStoreImpl.class, true);
    // Verify successful deletion of mount table entries
    getResponse =
        client.getMountTableManager().getMountTableEntries(getRequest);
    assertEquals(0, getResponse.getEntries().size());
  }

  @Test
  public void testRemoveMountTableNotNormalized() throws Exception {
    String nsId = "ns0";
    String src = "/test-rmmounttable-notnormalized";
    String srcWithSlash = src + "/";
    String dest = "/rmmounttable-notnormalized";
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

    argv = new String[] {"-rm", srcWithSlash};
    assertEquals(0, ToolRunner.run(admin, argv));

    stateStore.loadCache(MountTableStoreImpl.class, true);
    getResponse = client.getMountTableManager()
        .getMountTableEntries(getRequest);
    assertEquals(0, getResponse.getEntries().size());
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
  public void testUpdateMountTableWithoutPermission() throws Exception {
    UserGroupInformation superUser = UserGroupInformation.getCurrentUser();
    String superUserName = superUser.getShortUserName();
    // re-set system out for testing
    System.setOut(new PrintStream(out));

    try {
      stateStore.loadCache(MountTableStoreImpl.class, true);
      // add mount table using super user.
      String[] argv = new String[]{"-add", "/testpath3-1", "ns0", "/testdir3-1",
          "-owner", superUserName, "-group", superUserName, "-mode", "755"};
      assertEquals(0, ToolRunner.run(admin, argv));

      UserGroupInformation remoteUser = UserGroupInformation
          .createRemoteUser(TEST_USER);
      UserGroupInformation.setLoginUser(remoteUser);

      stateStore.loadCache(MountTableStoreImpl.class, true);
      // update mount table using normal user
      argv = new String[]{"-update", "/testpath3-1", "ns0", "/testdir3-2",
          "-owner", TEST_USER, "-group", TEST_USER, "-mode", "777"};
      assertEquals("Normal user update mount table which created by " +
          "superuser unexpected.", -1, ToolRunner.run(admin, argv));
    } finally {
      // set back login user
      UserGroupInformation.setLoginUser(superUser);
    }
  }

  @Test
  public void testOperateMountTableWithGroupPermission() throws Exception {
    UserGroupInformation superUser = UserGroupInformation.getCurrentUser();
    // re-set system out for testing
    System.setOut(new PrintStream(out));

    try {
      String testUserA = "test-user-a";
      String testUserB = "test-user-b";
      String testUserC = "test-user-c";
      String testUserD = "test-user-d";
      String testGroup = "test-group";

      stateStore.loadCache(MountTableStoreImpl.class, true);
      // add mount point with usera.
      UserGroupInformation userA = UserGroupInformation.createUserForTesting(
          testUserA, new String[] {testGroup});
      UserGroupInformation.setLoginUser(userA);

      String[] argv = new String[]{"-add", "/testpath4-1", "ns0", "/testdir4-1",
          "-owner", testUserA, "-group", testGroup, "-mode", "775"};
      assertEquals("Normal user can't add mount table unexpected.", 0,
          ToolRunner.run(admin, argv));

      stateStore.loadCache(MountTableStoreImpl.class, true);
      // update mount point with userb which is same group with owner.
      UserGroupInformation userB = UserGroupInformation.createUserForTesting(
          testUserB, new String[] {testGroup});
      UserGroupInformation.setLoginUser(userB);
      argv = new String[]{"-update", "/testpath4-1", "ns0", "/testdir4-2",
          "-owner", testUserA, "-group", testGroup, "-mode", "775"};
      assertEquals("Another user in same group can't update mount table " +
          "unexpected.", 0, ToolRunner.run(admin, argv));

      stateStore.loadCache(MountTableStoreImpl.class, true);
      // update mount point with userc which is not same group with owner.
      UserGroupInformation userC = UserGroupInformation.createUserForTesting(
          testUserC, new String[] {});
      UserGroupInformation.setLoginUser(userC);
      argv = new String[]{"-update", "/testpath4-1", "ns0", "/testdir4-3",
          "-owner", testUserA, "-group", testGroup, "-mode", "775"};
      assertEquals("Another user not in same group have no permission but " +
              "update mount table successful unexpected.", -1,
          ToolRunner.run(admin, argv));

      stateStore.loadCache(MountTableStoreImpl.class, true);
      // add mount point with userd but immediate parent of mount point
      // does not exist.
      UserGroupInformation userD = UserGroupInformation.createUserForTesting(
          testUserD, new String[] {testGroup});
      UserGroupInformation.setLoginUser(userD);
      argv = new String[]{"-add", "/testpath4-1/foo/bar", "ns0",
          "/testdir4-1/foo/bar", "-owner", testUserD, "-group", testGroup,
          "-mode", "775"};
      assertEquals("Normal user can't add mount table unexpected.", 0,
          ToolRunner.run(admin, argv));

      // test remove mount point with userc.
      UserGroupInformation.setLoginUser(userC);
      argv = new String[]{"-rm", "/testpath4-1"};
      assertEquals(-1, ToolRunner.run(admin, argv));

      // test remove mount point with userb.
      UserGroupInformation.setLoginUser(userB);
      assertEquals("Another user in same group can't remove mount table " +
          "unexpected.", 0, ToolRunner.run(admin, argv));
    } finally {
      // set back login user
      UserGroupInformation.setLoginUser(superUser);
    }
  }

  @Test
  public void testOperateMountTableWithSuperUserPermission() throws Exception {
    UserGroupInformation superUser = UserGroupInformation.getCurrentUser();
    // re-set system out for testing
    System.setOut(new PrintStream(out));

    try {
      String testUserA = "test-user-a";
      String testGroup = "test-group";

      stateStore.loadCache(MountTableStoreImpl.class, true);
      // add mount point with usera.
      UserGroupInformation userA = UserGroupInformation.createUserForTesting(
          testUserA, new String[] {testGroup});
      UserGroupInformation.setLoginUser(userA);
      String[] argv = new String[]{"-add", "/testpath5-1", "ns0", "/testdir5-1",
          "-owner", testUserA, "-group", testGroup, "-mode", "755"};
      assertEquals(0, ToolRunner.run(admin, argv));

      // test update mount point with super user.
      stateStore.loadCache(MountTableStoreImpl.class, true);
      UserGroupInformation.setLoginUser(superUser);
      argv = new String[]{"-update", "/testpath5-1", "ns0", "/testdir5-2",
          "-owner", testUserA, "-group", testGroup, "-mode", "755"};
      assertEquals("Super user can't update mount table unexpected.", 0,
          ToolRunner.run(admin, argv));

      // test remove mount point with super user.
      argv = new String[]{"-rm", "/testpath5-1"};
      assertEquals("Super user can't remove mount table unexpected.", 0,
          ToolRunner.run(admin, argv));
    } finally {
      // set back login user
      UserGroupInformation.setLoginUser(superUser);
    }
  }

  @Test
  public void testAddMountTableIfParentExist() throws Exception {
    UserGroupInformation superUser = UserGroupInformation.getCurrentUser();
    // re-set system out for testing
    System.setOut(new PrintStream(out));

    try {
      String testUserA = "test-user-a";
      String testUserB = "test-user-b";
      String testGroup = "test-group";

      stateStore.loadCache(MountTableStoreImpl.class, true);
      // add mount point with usera.
      UserGroupInformation userA = UserGroupInformation.createUserForTesting(
          testUserA, new String[] {testGroup});
      UserGroupInformation.setLoginUser(userA);
      String[] argv = new String[]{"-add", "/testpath6-1", "ns0", "/testdir6-1",
          "-owner", testUserA, "-group", testGroup, "-mode", "755"};
      assertEquals(0, ToolRunner.run(admin, argv));

      // add mount point with userb will be success since the nearest parent
      // does not exist but have other EXECUTE permission.
      UserGroupInformation userB = UserGroupInformation.createUserForTesting(
          testUserB, new String[] {testGroup});
      UserGroupInformation.setLoginUser(userB);
      argv = new String[]{"-add", "/testpath6-1/parent/foo", "ns0",
          "/testdir6-1/parent/foo", "-owner", testUserA, "-group", testGroup,
          "-mode", "755"};
      assertEquals(0, ToolRunner.run(admin, argv));

      // add mount point with userb will be failure since the nearest parent
      // does exist but no WRITE permission.
      argv = new String[]{"-add", "/testpath6-1/foo", "ns0",
          "/testdir6-1/foo", "-owner", testUserA, "-group", testGroup,
          "-mode", "755"};
      assertEquals(-1, ToolRunner.run(admin, argv));
    } finally {
      // set back login user
      UserGroupInformation.setLoginUser(superUser);
    }
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
    verifyExecutionResult("/testpath2-2", false, -1, 0);

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
   *          whether you can list mount tables under specified mount
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
  public void testInvalidArgumentMessage() throws Exception {
    String nsId = "ns0";
    String src = "/testSource";
    System.setOut(new PrintStream(out));
    String[] argv = new String[] {"-add", src, nsId};
    assertEquals(-1, ToolRunner.run(admin, argv));
    assertTrue("Wrong message: " + out, out.toString().contains(
        "\t[-add <source> <nameservice1, nameservice2, ...> <destination> "
            + "[-readonly] [-faulttolerant] "
            + "[-order HASH|LOCAL|RANDOM|HASH_ALL|SPACE] "
            + "-owner <owner> -group <group> -mode <mode>]"));
    out.reset();

    argv = new String[] {"-update", src, nsId};
    assertEquals(-1, ToolRunner.run(admin, argv));
    assertTrue("Wrong message: " + out, out.toString().contains(
        "\t[-update <source> [<nameservice1, nameservice2, ...> <destination>] "
            + "[-readonly true|false] [-faulttolerant true|false] "
            + "[-order HASH|LOCAL|RANDOM|HASH_ALL|SPACE] "
            + "-owner <owner> -group <group> -mode <mode>]"));
    out.reset();

    argv = new String[] {"-rm"};
    assertEquals(-1, ToolRunner.run(admin, argv));
    assertTrue(out.toString().contains("\t[-rm <source>]"));
    out.reset();

    argv = new String[] {"-setQuota", src};
    assertEquals(-1, ToolRunner.run(admin, argv));
    assertTrue(out.toString()
        .contains("\t[-setQuota <path> -nsQuota <nsQuota> -ssQuota "
            + "<quota in bytes or quota size string>]"));
    out.reset();

    argv = new String[] {"-clrQuota"};
    assertEquals(-1, ToolRunner.run(admin, argv));
    assertTrue(out.toString().contains("\t[-clrQuota <path>]"));
    out.reset();

    argv = new String[] {"-safemode"};
    assertEquals(-1, ToolRunner.run(admin, argv));
    assertTrue(out.toString().contains("\t[-safemode enter | leave | get]"));
    out.reset();

    argv = new String[] {"-nameservice", nsId};
    assertEquals(-1, ToolRunner.run(admin, argv));
    assertTrue(out.toString()
        .contains("\t[-nameservice enable | disable <nameservice>]"));
    out.reset();

    argv = new String[] {"-getDestination"};
    assertEquals(-1, ToolRunner.run(admin, argv));
    assertTrue(out.toString().contains("\t[-getDestination <path>]"));
    out.reset();

    argv = new String[] {"-refreshRouterArgs"};
    assertEquals(-1, ToolRunner.run(admin, argv));
    assertTrue(out.toString().contains("\t[-refreshRouterArgs " +
            "<host:ipc_port> <key> [arg1..argn]]"));
    out.reset();

    argv = new String[] {"-Random"};
    assertEquals(-1, ToolRunner.run(admin, argv));
    String expected = "Usage: hdfs dfsrouteradmin :\n"
        + "\t[-add <source> <nameservice1, nameservice2, ...> <destination> "
        + "[-readonly] [-faulttolerant] "
        + "[-order HASH|LOCAL|RANDOM|HASH_ALL|SPACE] "
        + "-owner <owner> -group <group> -mode <mode>]\n"
        + "\t[-addAll <source1> <nameservice1,nameservice2,...> <destination1> "
        + "[-readonly] [-faulttolerant] [-order HASH|LOCAL|RANDOM|HASH_ALL|SPACE] "
        + "-owner <owner1> -group <group1> -mode <mode1>"
        + " , <source2> <nameservice1,nameservice2,...> <destination2> "
        + "[-readonly] [-faulttolerant] [-order HASH|LOCAL|RANDOM|HASH_ALL|SPACE] "
        + "-owner <owner2> -group <group2> -mode <mode2> , ...]\n"
        + "\t[-update <source> [<nameservice1, nameservice2, ...> "
        + "<destination>] [-readonly true|false]"
        + " [-faulttolerant true|false] "
        + "[-order HASH|LOCAL|RANDOM|HASH_ALL|SPACE] "
        + "-owner <owner> -group <group> -mode <mode>]\n" + "\t[-rm <source>]\n"
        + "\t[-ls [-d] <path>]\n"
        + "\t[-getDestination <path>]\n"
        + "\t[-setQuota <path> -nsQuota <nsQuota> -ssQuota"
        + " <quota in bytes or quota size string>]\n"
        + "\t[-setStorageTypeQuota <path> -storageType <storage type>"
        + " <quota in bytes or quota size string>]\n"
        + "\t[-clrQuota <path>]\n"
        + "\t[-clrStorageTypeQuota <path>]\n"
        + "\t[-dumpState]\n"
        + "\t[-safemode enter | leave | get]\n"
        + "\t[-nameservice enable | disable <nameservice>]\n"
        + "\t[-getDisabledNameservices]\n"
        + "\t[-refresh]\n"
        + "\t[-refreshRouterArgs <host:ipc_port> <key> [arg1..argn]]";
    assertTrue("Wrong message: " + out, out.toString().contains(expected));
    out.reset();
  }

  /**
   * Test command -setStorageTypeQuota with wrong arguments.
   */
  @Test
  public void testWrongArgumentsWhenSetStorageTypeQuota() throws Exception {
    String src = "/type-QuotaMounttable";
    // verify wrong arguments.
    System.setErr(new PrintStream(err));
    String[] argv =
        new String[] {"-setStorageTypeQuota", src, "check", "c2", "c3"};
    ToolRunner.run(admin, argv);
    assertTrue(err.toString().contains("Invalid argument : check"));
  }

  /**
   * Test command -setStorageTypeQuota.
   */
  @Test
  public void testSetStorageTypeQuota() throws Exception {
    String nsId = "ns0";
    String src = "/type-QuotaMounttable";
    String dest = "/type-QuotaMounttable";
    try {
      addMountTable(src, nsId, dest);

      // verify the default quota.
      MountTable mountTable = getMountTable(src).get(0);
      RouterQuotaUsage quotaUsage = mountTable.getQuota();
      for (StorageType t : StorageType.values()) {
        assertEquals(RouterQuotaUsage.QUOTA_USAGE_COUNT_DEFAULT,
            quotaUsage.getTypeConsumed(t));
        assertEquals(HdfsConstants.QUOTA_RESET, quotaUsage.getTypeQuota(t));
      }

      // set storage type quota.
      long ssQuota = 100;
      setStorageTypeQuota(src, ssQuota, StorageType.DISK);

      // verify if the quota is set
      mountTable = getMountTable(src).get(0);
      quotaUsage = mountTable.getQuota();
      assertEquals(ssQuota, quotaUsage.getTypeQuota(StorageType.DISK));
    } finally {
      rmMountTable(src);
    }
  }

  /**
   * Test command -clrStorageTypeQuota.
   */
  @Test
  public void testClearStorageTypeQuota() throws Exception {
    String nsId = "ns0";
    String src = "/type-QuotaMounttable";
    String src1 = "/type-QuotaMounttable1";
    String dest = "/type-QuotaMounttable";
    String dest1 = "/type-QuotaMounttable1";
    long ssQuota = 100;
    try {
      // add mount points.
      addMountTable(src, nsId, dest);
      addMountTable(src1, nsId, dest1);

      // set storage type quota to src and src1.
      setStorageTypeQuota(src, ssQuota, StorageType.DISK);
      assertEquals(ssQuota,
          getMountTable(src).get(0).getQuota().getTypeQuota(StorageType.DISK));
      setStorageTypeQuota(src1, ssQuota, StorageType.DISK);
      assertEquals(ssQuota,
          getMountTable(src1).get(0).getQuota().getTypeQuota(StorageType.DISK));

      // clrQuota of src and src1.
      assertEquals(0, ToolRunner
          .run(admin, new String[] {"-clrStorageTypeQuota", src, src1}));
      stateStore.loadCache(MountTableStoreImpl.class, true);

      // Verify whether the storage type quotas are cleared.
      List<MountTable> mountTables = getMountTable("/");
      for (int i = 0; i < 2; i++) {
        MountTable mountTable = mountTables.get(i);
        RouterQuotaUsage quotaUsage = mountTable.getQuota();
        for (StorageType t : StorageType.values()) {
          assertEquals(RouterQuotaUsage.QUOTA_USAGE_COUNT_DEFAULT,
              quotaUsage.getTypeConsumed(t));
          assertEquals(HdfsConstants.QUOTA_RESET, quotaUsage.getTypeQuota(t));
        }
      }
    } finally {
      rmMountTable(src);
      rmMountTable(src1);
    }
  }

  @Test
  public void testSetAndClearQuota() throws Exception {
    String nsId = "ns0";
    String src = "/test-QuotaMounttable";
    String src1 = "/test-QuotaMounttable1";
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
    assertEquals(HdfsConstants.QUOTA_RESET, quotaUsage.getQuota());
    assertEquals(RouterQuotaUsage.QUOTA_USAGE_COUNT_DEFAULT,
        quotaUsage.getSpaceConsumed());
    assertEquals(HdfsConstants.QUOTA_RESET, quotaUsage.getSpaceQuota());

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
    assertEquals(HdfsConstants.QUOTA_RESET, quotaUsage.getQuota());
    assertEquals(HdfsConstants.QUOTA_RESET, quotaUsage.getSpaceQuota());

    // verify multi args ClrQuota
    String dest1 = "/QuotaMounttable1";
    // Add one more mount table entry.
    argv = new String[] {"-add", src1, nsId, dest1};
    assertEquals(0, ToolRunner.run(admin, argv));

    stateStore.loadCache(MountTableStoreImpl.class, true);
    // SetQuota for the added entries
    argv = new String[] {"-setQuota", src, "-nsQuota", String.valueOf(nsQuota),
        "-ssQuota", String.valueOf(ssQuota)};
    assertEquals(0, ToolRunner.run(admin, argv));
    argv = new String[] {"-setQuota", src1, "-nsQuota",
        String.valueOf(nsQuota), "-ssQuota", String.valueOf(ssQuota)};
    assertEquals(0, ToolRunner.run(admin, argv));
    stateStore.loadCache(MountTableStoreImpl.class, true);
    // Clear quota for the added entries
    argv = new String[] {"-clrQuota", src, src1};
    assertEquals(0, ToolRunner.run(admin, argv));

    stateStore.loadCache(MountTableStoreImpl.class, true);

    getRequest = GetMountTableEntriesRequest.newInstance("/");
    getResponse =
        client.getMountTableManager().getMountTableEntries(getRequest);

    // Verify clear quota for the entries
    for (int i = 0; i < 2; i++) {
      mountTable = getResponse.getEntries().get(i);
      quotaUsage = mountTable.getQuota();
      assertEquals(HdfsConstants.QUOTA_RESET, quotaUsage.getQuota());
      assertEquals(HdfsConstants.QUOTA_RESET, quotaUsage.getSpaceQuota());
    }

    // verify wrong arguments
    System.setErr(new PrintStream(err));
    argv = new String[] {"-setQuota", src, "check", "check2"};
    ToolRunner.run(admin, argv);
    assertTrue(err.toString().contains("Invalid argument : check"));
  }

  @Test
  public void testManageSafeMode() throws Exception {
    // ensure the Router become RUNNING state
    waitState(RouterServiceState.RUNNING);
    assertFalse(routerContext.getRouter().getSafemodeService().isInSafeMode());
    assertEquals(0, ToolRunner.run(admin,
        new String[] {"-safemode", "enter"}));
    // verify state
    assertEquals(RouterServiceState.SAFEMODE,
        routerContext.getRouter().getRouterState());
    assertTrue(routerContext.getRouter().getSafemodeService().isInSafeMode());

    System.setOut(new PrintStream(out));
    System.setErr(new PrintStream(err));
    assertEquals(0, ToolRunner.run(admin,
        new String[] {"-safemode", "get"}));
    assertTrue(out.toString().contains("true"));

    assertEquals(0, ToolRunner.run(admin,
        new String[] {"-safemode", "leave"}));
    // verify state
    assertEquals(RouterServiceState.RUNNING,
        routerContext.getRouter().getRouterState());
    assertFalse(routerContext.getRouter().getSafemodeService().isInSafeMode());

    out.reset();
    assertEquals(0, ToolRunner.run(admin,
        new String[] {"-safemode", "get"}));
    assertTrue(out.toString().contains("false"));

    out.reset();
    assertEquals(-1, ToolRunner.run(admin,
        new String[] {"-safemode", "get", "-random", "check" }));
    assertTrue(err.toString(), err.toString()
        .contains("safemode: Too many arguments, Max=1 argument allowed only"));
    err.reset();

    assertEquals(-1,
        ToolRunner.run(admin, new String[] {"-safemode", "check" }));
    assertTrue(err.toString(),
        err.toString().contains("safemode: Invalid argument: check"));
    err.reset();
  }

  @Test
  public void testSafeModeStatus() throws Exception {
    // ensure the Router become RUNNING state
    waitState(RouterServiceState.RUNNING);
    assertFalse(routerContext.getRouter().getSafemodeService().isInSafeMode());
    final RouterClientProtocol clientProtocol =
        routerContext.getRouter().getRpcServer().getClientProtocolModule();
    assertEquals(HAServiceState.ACTIVE, clientProtocol.getHAServiceState());

    assertEquals(0,
        ToolRunner.run(admin, new String[] {"-safemode", "enter" }));

    RBFMetrics metrics = router.getMetrics();
    String jsonString = metrics.getRouterStatus();
    String result = router.getNamenodeMetrics().getSafemode();
    assertTrue("Wrong safe mode message: " + result,
        result.startsWith("Safe mode is ON."));

    // verify state using RBFMetrics
    assertEquals(RouterServiceState.SAFEMODE.toString(), jsonString);
    assertTrue(routerContext.getRouter().getSafemodeService().isInSafeMode());
    assertEquals(HAServiceState.STANDBY, clientProtocol.getHAServiceState());

    System.setOut(new PrintStream(out));
    assertEquals(0,
        ToolRunner.run(admin, new String[] {"-safemode", "leave" }));
    jsonString = metrics.getRouterStatus();
    result = router.getNamenodeMetrics().getSafemode();
    assertEquals("Wrong safe mode message: " + result, "", result);

    // verify state
    assertEquals(RouterServiceState.RUNNING.toString(), jsonString);
    assertFalse(routerContext.getRouter().getSafemodeService().isInSafeMode());
    assertEquals(HAServiceState.ACTIVE, clientProtocol.getHAServiceState());

    out.reset();
    assertEquals(0, ToolRunner.run(admin, new String[] {"-safemode", "get" }));
    assertTrue(out.toString().contains("false"));
  }

  @Test
  public void testSafeModePermission() throws Exception {
    // ensure the Router become RUNNING state
    waitState(RouterServiceState.RUNNING);
    assertFalse(routerContext.getRouter().getSafemodeService().isInSafeMode());

    UserGroupInformation superUser = UserGroupInformation.createRemoteUser(
        UserGroupInformation.getCurrentUser().getShortUserName());
    UserGroupInformation remoteUser = UserGroupInformation
        .createRemoteUser(TEST_USER);
    try {
      // use normal user as current user to test
      UserGroupInformation.setLoginUser(remoteUser);
      assertEquals(-1,
          ToolRunner.run(admin, new String[]{"-safemode", "enter"}));

      // set back login user
      UserGroupInformation.setLoginUser(superUser);
      assertEquals(0,
          ToolRunner.run(admin, new String[]{"-safemode", "enter"}));

      // use normal user as current user to test
      UserGroupInformation.setLoginUser(remoteUser);
      assertEquals(-1,
          ToolRunner.run(admin, new String[]{"-safemode", "leave"}));

      // set back login user
      UserGroupInformation.setLoginUser(superUser);
      assertEquals(0,
          ToolRunner.run(admin, new String[]{"-safemode", "leave"}));
    } finally {
      // set back login user to make sure it doesn't pollute other unit tests
      // even this one fails.
      UserGroupInformation.setLoginUser(superUser);
    }
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

    err.reset();
    ToolRunner.run(admin,
        new String[] {"-nameservice", "enable", "ns0", "check"});
    assertTrue(
        err.toString().contains("Too many arguments, Max=2 arguments allowed"));
    err.reset();
    ToolRunner.run(admin, new String[] {"-getDisabledNameservices", "check"});
    assertTrue(err.toString().contains("No arguments allowed"));
  }

  @Test
  public void testRefreshMountTableCache() throws Exception {
    String src = "/refreshMount";

    // create mount table entry
    String[] argv = new String[] {"-add", src, "refreshNS0", "/refreshDest"};
    assertEquals(0, ToolRunner.run(admin, argv));

    // refresh the mount table entry cache
    System.setOut(new PrintStream(out));
    argv = new String[] {"-refresh"};
    assertEquals(0, ToolRunner.run(admin, argv));
    assertTrue(
        out.toString().startsWith("Successfully updated mount table cache"));

    // Now ls should return that mount table entry
    out.reset();
    argv = new String[] {"-ls", src};
    assertEquals(0, ToolRunner.run(admin, argv));
    assertTrue(out.toString().contains(src));
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

  @Test
  public void testUpdateNonExistingMountTable() throws Exception {
    System.setErr(new PrintStream(err));
    String nsId = "ns0";
    String src = "/test-updateNonExistingMounttable";
    String dest = "/updateNonExistingMounttable";
    String[] argv = new String[] {"-update", src, nsId, dest};
    // Update shall fail if the mount entry doesn't exist.
    assertEquals(-1, ToolRunner.run(admin, argv));
    assertTrue(err.toString(), err.toString()
        .contains("update: /test-updateNonExistingMounttable doesn't exist."));
  }

  @Test
  public void testUpdateDestinationForExistingMountTable() throws Exception {
    // Add a mount table firstly
    String nsId = "ns0";
    String src = "/test-updateDestinationForExistingMountTable";
    String dest = "/UpdateDestinationForExistingMountTable";
    String[] argv = new String[] {"-add", src, nsId, dest};
    assertEquals(0, ToolRunner.run(admin, argv));

    stateStore.loadCache(MountTableStoreImpl.class, true);
    GetMountTableEntriesRequest getRequest =
        GetMountTableEntriesRequest.newInstance(src);
    GetMountTableEntriesResponse getResponse =
        client.getMountTableManager().getMountTableEntries(getRequest);
    // Ensure mount table added successfully
    MountTable mountTable = getResponse.getEntries().get(0);
    assertEquals(src, mountTable.getSourcePath());
    assertEquals(nsId, mountTable.getDestinations().get(0).getNameserviceId());
    assertEquals(dest, mountTable.getDestinations().get(0).getDest());

    // Update the destination
    String newNsId = "ns1";
    String newDest = "/newDestination";
    argv = new String[] {"-update", src, newNsId, newDest};
    assertEquals(0, ToolRunner.run(admin, argv));

    stateStore.loadCache(MountTableStoreImpl.class, true);
    getResponse = client.getMountTableManager()
        .getMountTableEntries(getRequest);
    // Ensure the destination updated successfully
    mountTable = getResponse.getEntries().get(0);
    assertEquals(src, mountTable.getSourcePath());
    assertEquals(newNsId,
        mountTable.getDestinations().get(0).getNameserviceId());
    assertEquals(newDest, mountTable.getDestinations().get(0).getDest());
  }

  @Test
  public void testUpdateDestinationForExistingMountTableNotNormalized() throws
      Exception {
    // Add a mount table firstly
    String nsId = "ns0";
    String src = "/test-updateDestinationForExistingMountTableNotNormalized";
    String srcWithSlash = src + "/";
    String dest = "/UpdateDestinationForExistingMountTableNotNormalized";
    String[] argv = new String[] {"-add", src, nsId, dest};
    assertEquals(0, ToolRunner.run(admin, argv));

    stateStore.loadCache(MountTableStoreImpl.class, true);
    GetMountTableEntriesRequest getRequest =
        GetMountTableEntriesRequest.newInstance(src);
    GetMountTableEntriesResponse getResponse =
        client.getMountTableManager().getMountTableEntries(getRequest);
    // Ensure mount table added successfully
    MountTable mountTable = getResponse.getEntries().get(0);
    assertEquals(src, mountTable.getSourcePath());
    assertEquals(nsId, mountTable.getDestinations().get(0).getNameserviceId());
    assertEquals(dest, mountTable.getDestinations().get(0).getDest());

    // Update the destination
    String newNsId = "ns1";
    String newDest = "/newDestination";
    argv = new String[] {"-update", srcWithSlash, newNsId, newDest};
    assertEquals(0, ToolRunner.run(admin, argv));

    stateStore.loadCache(MountTableStoreImpl.class, true);
    getResponse = client.getMountTableManager()
        .getMountTableEntries(getRequest);
    // Ensure the destination updated successfully
    mountTable = getResponse.getEntries().get(0);
    assertEquals(src, mountTable.getSourcePath());
    assertEquals(newNsId,
        mountTable.getDestinations().get(0).getNameserviceId());
    assertEquals(newDest, mountTable.getDestinations().get(0).getDest());
  }

  @Test
  public void testUpdateChangeAttributes() throws Exception {
    // Add a mount table firstly
    String nsId = "ns0";
    String src = "/mount";
    String dest = "/dest";
    String[] argv = new String[] {"-add", src, nsId, dest, "-readonly",
        "-order", "HASH_ALL"};
    assertEquals(0, ToolRunner.run(admin, argv));

    stateStore.loadCache(MountTableStoreImpl.class, true);
    GetMountTableEntriesRequest getRequest =
        GetMountTableEntriesRequest.newInstance(src);
    GetMountTableEntriesResponse getResponse =
        client.getMountTableManager().getMountTableEntries(getRequest);
    // Ensure mount table added successfully
    MountTable mountTable = getResponse.getEntries().get(0);
    assertEquals(src, mountTable.getSourcePath());

    // Update the destination
    String newNsId = "ns0";
    String newDest = "/newDestination";
    argv = new String[] {"-update", src, newNsId, newDest};
    assertEquals(0, ToolRunner.run(admin, argv));

    stateStore.loadCache(MountTableStoreImpl.class, true);
    getResponse =
        client.getMountTableManager().getMountTableEntries(getRequest);
    // Ensure the destination updated successfully and other attributes are
    // preserved.
    mountTable = getResponse.getEntries().get(0);
    assertEquals(src, mountTable.getSourcePath());
    assertEquals(newNsId,
        mountTable.getDestinations().get(0).getNameserviceId());
    assertEquals(newDest, mountTable.getDestinations().get(0).getDest());
    assertTrue(mountTable.isReadOnly());
    assertEquals("HASH_ALL", mountTable.getDestOrder().toString());

    // Update the attribute.
    argv = new String[] {"-update", src, "-readonly", "false"};
    assertEquals(0, ToolRunner.run(admin, argv));

    stateStore.loadCache(MountTableStoreImpl.class, true);
    getResponse =
        client.getMountTableManager().getMountTableEntries(getRequest);

    // Ensure the attribute updated successfully and destination and other
    // attributes are preserved.
    mountTable = getResponse.getEntries().get(0);
    assertEquals(src, mountTable.getSourcePath());
    assertEquals(newNsId,
        mountTable.getDestinations().get(0).getNameserviceId());
    assertEquals(newDest, mountTable.getDestinations().get(0).getDest());
    assertFalse(mountTable.isReadOnly());
    assertEquals("HASH_ALL", mountTable.getDestOrder().toString());

  }

  @Test
  public void testUpdateErrorCase() throws Exception {
    // Add a mount table firstly
    String nsId = "ns0";
    String src = "/mount";
    String dest = "/dest";
    String[] argv = new String[] {"-add", src, nsId, dest, "-readonly",
        "-order", "HASH_ALL"};
    assertEquals(0, ToolRunner.run(admin, argv));
    stateStore.loadCache(MountTableStoreImpl.class, true);

    // Check update for non-existent mount entry.
    argv = new String[] {"-update", "/noMount", "-readonly", "false"};
    System.setErr(new PrintStream(err));
    assertEquals(-1, ToolRunner.run(admin, argv));
    assertTrue(err.toString(),
        err.toString().contains("update: /noMount doesn't exist."));
    err.reset();

    // Check update if no true/false value is passed for readonly.
    argv = new String[] {"-update", src, "-readonly", "check"};
    assertEquals(-1, ToolRunner.run(admin, argv));
    assertTrue(err.toString(), err.toString().contains("update: "
        + "Invalid argument: check. Please specify either true or false."));
    err.reset();

    // Check update with missing value is passed for faulttolerant.
    argv = new String[] {"-update", src, "ns1", "/tmp", "-faulttolerant"};
    assertEquals(-1, ToolRunner.run(admin, argv));
    assertTrue(err.toString(),
        err.toString().contains("update: Unable to parse arguments:"
            + " no value provided for -faulttolerant"));
    err.reset();

    // Check update with invalid order.
    argv = new String[] {"-update", src, "ns1", "/tmp", "-order", "Invalid"};
    assertEquals(-1, ToolRunner.run(admin, argv));
    assertTrue(err.toString(), err.toString().contains(
        "update: Unable to parse arguments: Cannot parse order: Invalid"));
    err.reset();
  }

  @Test
  public void testUpdateReadonlyUserGroupPermissionMountable()
      throws Exception {
    // Add a mount table
    String nsId = "ns0";
    String src = "/test-updateReadonlyUserGroupPermissionMountTable";
    String dest = "/UpdateReadonlyUserGroupPermissionMountTable";
    String[] argv = new String[] {"-add", src, nsId, dest};
    assertEquals(0, ToolRunner.run(admin, argv));

    stateStore.loadCache(MountTableStoreImpl.class, true);
    GetMountTableEntriesRequest getRequest =
        GetMountTableEntriesRequest.newInstance(src);
    GetMountTableEntriesResponse getResponse =
        client.getMountTableManager().getMountTableEntries(getRequest);
    // Ensure mount table added successfully
    MountTable mountTable = getResponse.getEntries().get(0);
    assertEquals(src, mountTable.getSourcePath());
    assertEquals(nsId, mountTable.getDestinations().get(0).getNameserviceId());
    assertEquals(dest, mountTable.getDestinations().get(0).getDest());
    assertFalse(mountTable.isReadOnly());

    // Update the readonly, owner, group and permission
    String testOwner = "test_owner";
    String testGroup = "test_group";
    argv = new String[] {"-update", src, nsId, dest, "-readonly", "true",
        "-owner", testOwner, "-group", testGroup, "-mode", "0455"};
    assertEquals(0, ToolRunner.run(admin, argv));

    stateStore.loadCache(MountTableStoreImpl.class, true);
    getResponse = client.getMountTableManager()
        .getMountTableEntries(getRequest);

    // Ensure the destination updated successfully
    mountTable = getResponse.getEntries().get(0);
    assertEquals(src, mountTable.getSourcePath());
    assertEquals(nsId, mountTable.getDestinations().get(0).getNameserviceId());
    assertEquals(dest, mountTable.getDestinations().get(0).getDest());
    assertTrue(mountTable.isReadOnly());
    assertEquals(testOwner, mountTable.getOwnerName());
    assertEquals(testGroup, mountTable.getGroupName());
    assertEquals((short)0455, mountTable.getMode().toShort());
  }

  @Test
  public void testUpdateReadonlyWithQuota() throws Exception {
    // Add a mount table
    String nsId = "ns0";
    String src = "/test-updateReadonlywithQuota";
    String dest = "/UpdateReadonlywithQuota";
    String[] argv = new String[] {"-add", src, nsId, dest };
    assertEquals(0, ToolRunner.run(admin, argv));

    stateStore.loadCache(MountTableStoreImpl.class, true);
    GetMountTableEntriesRequest getRequest = GetMountTableEntriesRequest
        .newInstance(src);
    GetMountTableEntriesResponse getResponse = client.getMountTableManager()
        .getMountTableEntries(getRequest);
    // Ensure mount table added successfully
    MountTable mountTable = getResponse.getEntries().get(0);
    assertEquals(src, mountTable.getSourcePath());
    RemoteLocation localDest = mountTable.getDestinations().get(0);
    assertEquals(nsId, localDest.getNameserviceId());
    assertEquals(dest, localDest.getDest());
    assertFalse(mountTable.isReadOnly());

    argv = new String[] {"-update", src, nsId, dest, "-readonly", "true" };
    assertEquals(0, ToolRunner.run(admin, argv));

    stateStore.loadCache(MountTableStoreImpl.class, true);
    getResponse = client.getMountTableManager()
        .getMountTableEntries(getRequest);
    mountTable = getResponse.getEntries().get(0);
    assertTrue(mountTable.isReadOnly());

    // Update Quota
    long nsQuota = 50;
    long ssQuota = 100;
    argv = new String[] {"-setQuota", src, "-nsQuota", String.valueOf(nsQuota),
        "-ssQuota", String.valueOf(ssQuota) };
    assertEquals(0, ToolRunner.run(admin, argv));

    stateStore.loadCache(MountTableStoreImpl.class, true);
    getResponse = client.getMountTableManager()
        .getMountTableEntries(getRequest);

    mountTable = getResponse.getEntries().get(0);
    RouterQuotaUsage quota = mountTable.getQuota();
    assertEquals(nsQuota, quota.getQuota());
    assertEquals(ssQuota, quota.getSpaceQuota());
    assertTrue(mountTable.isReadOnly());
  }

  @Test
  public void testUpdateOrderMountTable() throws Exception {
    testUpdateOrderMountTable(DestinationOrder.HASH);
    testUpdateOrderMountTable(DestinationOrder.LOCAL);
    testUpdateOrderMountTable(DestinationOrder.RANDOM);
    testUpdateOrderMountTable(DestinationOrder.HASH_ALL);
    testUpdateOrderMountTable(DestinationOrder.SPACE);
  }

  @Test
  public void testOrderErrorMsg() throws Exception {
    String nsId = "ns0";
    DestinationOrder order = DestinationOrder.HASH;
    String src = "/testod" + order.toString();
    String dest = "/testUpd";
    String[] argv = new String[] {"-add", src, nsId, dest};
    assertEquals(0, ToolRunner.run(admin, argv));

    stateStore.loadCache(MountTableStoreImpl.class, true);
    GetMountTableEntriesRequest getRequest = GetMountTableEntriesRequest
        .newInstance(src);
    GetMountTableEntriesResponse getResponse = client.getMountTableManager()
        .getMountTableEntries(getRequest);

    // Ensure mount table added successfully
    MountTable mountTable = getResponse.getEntries().get(0);
    assertEquals(src, mountTable.getSourcePath());
    assertEquals(nsId, mountTable.getDestinations().get(0).getNameserviceId());
    assertEquals(dest, mountTable.getDestinations().get(0).getDest());
    assertEquals(DestinationOrder.HASH, mountTable.getDestOrder());

    argv = new String[] {"-update", src, nsId, dest, "-order",
        order.toString()};
    assertEquals(0, ToolRunner.run(admin, argv));

    // Update the order with wrong command
    argv = new String[] {"-update", src + "a", nsId, dest + "a", "-orde",
        order.toString()};
    assertEquals(-1, ToolRunner.run(admin, argv));

    // Update without order argument
    argv = new String[] {"-update", src, nsId, dest, order.toString()};
    assertEquals(-1, ToolRunner.run(admin, argv));

  }

  private void testUpdateOrderMountTable(DestinationOrder order)
      throws Exception {
    // Add a mount table
    String nsId = "ns0";
    String src = "/test-updateOrderMountTable-"+order.toString();
    String dest = "/UpdateOrderMountTable";
    String[] argv = new String[] {"-add", src, nsId, dest};
    assertEquals(0, ToolRunner.run(admin, argv));

    stateStore.loadCache(MountTableStoreImpl.class, true);
    GetMountTableEntriesRequest getRequest =
        GetMountTableEntriesRequest.newInstance(src);
    GetMountTableEntriesResponse getResponse =
        client.getMountTableManager().getMountTableEntries(getRequest);

    // Ensure mount table added successfully
    MountTable mountTable = getResponse.getEntries().get(0);
    assertEquals(src, mountTable.getSourcePath());
    assertEquals(nsId, mountTable.getDestinations().get(0).getNameserviceId());
    assertEquals(dest, mountTable.getDestinations().get(0).getDest());
    assertEquals(DestinationOrder.HASH, mountTable.getDestOrder());

    // Update the order
    argv = new String[] {"-update", src, nsId, dest, "-order",
        order.toString()};
    assertEquals(0, ToolRunner.run(admin, argv));

    stateStore.loadCache(MountTableStoreImpl.class, true);
    getResponse = client.getMountTableManager()
        .getMountTableEntries(getRequest);

    // Ensure the destination updated successfully
    mountTable = getResponse.getEntries().get(0);
    assertEquals(src, mountTable.getSourcePath());
    assertEquals(nsId, mountTable.getDestinations().get(0).getNameserviceId());
    assertEquals(dest, mountTable.getDestinations().get(0).getDest());
    assertEquals(order, mountTable.getDestOrder());
  }

  @Test
  public void testGetDestination() throws Exception {

    // Test the basic destination feature
    System.setOut(new PrintStream(out));
    String[] argv = new String[] {"-getDestination", "/file.txt"};
    assertEquals(0, ToolRunner.run(admin, argv));
    assertEquals("Destination: ns0" + System.lineSeparator(), out.toString());

    // Add a HASH_ALL entry to check the destination changing
    argv = new String[] {"-add", "/testGetDest", "ns0,ns1",
        "/testGetDestination",
        "-order", DestinationOrder.HASH_ALL.toString()};
    assertEquals(0, ToolRunner.run(admin, argv));
    stateStore.loadCache(MountTableStoreImpl.class, true);
    MountTableResolver resolver =
        (MountTableResolver) router.getSubclusterResolver();
    resolver.loadCache(true);

    // Files should be distributed across ns0 and ns1
    Map<String, AtomicInteger> counter = new TreeMap<>();
    final Pattern p = Pattern.compile("Destination: (.*)");
    for (int i = 0; i < 10; i++) {
      out.reset();
      String filename = "file" + i+ ".txt";
      argv = new String[] {"-getDestination", "/testGetDest/" + filename};
      assertEquals(0, ToolRunner.run(admin, argv));
      String outLine = out.toString();
      Matcher m = p.matcher(outLine);
      assertTrue(m.find());
      String nsId = m.group(1);
      if (counter.containsKey(nsId)) {
        counter.get(nsId).getAndIncrement();
      } else {
        counter.put(nsId, new AtomicInteger(1));
      }
    }
    assertEquals("Wrong counter size: " + counter, 2, counter.size());
    assertTrue(counter + " should contain ns0", counter.containsKey("ns0"));
    assertTrue(counter + " should contain ns1", counter.containsKey("ns1"));

    // Bad cases
    argv = new String[] {"-getDestination"};
    assertEquals(-1, ToolRunner.run(admin, argv));
    argv = new String[] {"-getDestination /file1.txt /file2.txt"};
    assertEquals(-1, ToolRunner.run(admin, argv));
  }

  @Test
  public void testErrorFaultTolerant() throws Exception {

    System.setErr(new PrintStream(err));
    String[] argv = new String[] {"-add", "/mntft", "ns01", "/tmp",
        "-faulttolerant"};
    assertEquals(-1, ToolRunner.run(admin, argv));
    assertTrue(err.toString(), err.toString().contains(
        "Invalid entry, fault tolerance requires multiple destinations"));
    err.reset();

    System.setErr(new PrintStream(err));
    argv = new String[] {"-add", "/mntft", "ns0,ns1", "/tmp",
        "-order", "HASH", "-faulttolerant"};
    assertEquals(-1, ToolRunner.run(admin, argv));
    assertTrue(err.toString(), err.toString().contains(
        "Invalid entry, fault tolerance only supported for ALL order"));
    err.reset();

    argv = new String[] {"-add", "/mntft", "ns0,ns1", "/tmp",
        "-order", "HASH_ALL", "-faulttolerant"};
    assertEquals(0, ToolRunner.run(admin, argv));
  }

  @Test
  public void testRefreshCallQueue() throws Exception {

    System.setOut(new PrintStream(out));
    System.setErr(new PrintStream(err));

    String[] argv = new String[]{"-refreshCallQueue"};
    assertEquals(0, ToolRunner.run(admin, argv));
    assertTrue(out.toString().contains("Refresh call queue successfully"));

    argv = new String[]{};
    assertEquals(-1, ToolRunner.run(admin, argv));
    assertTrue(out.toString().contains("-refreshCallQueue"));

    argv = new String[]{"-refreshCallQueue", "redundant"};
    assertEquals(-1, ToolRunner.run(admin, argv));
    assertTrue(err.toString().contains("No arguments allowed"));
  }

  @Test
  public void testDumpState() throws Exception {
    MockStateStoreDriver driver = new MockStateStoreDriver();
    driver.clearAll();
    // Add two records for block1
    driver.put(MembershipState.newInstance("routerId", "ns1",
        "ns1-ha1", "cluster1", "block1", "rpc1",
        "service1", "lifeline1", "https", "nn01",
        FederationNamenodeServiceState.ACTIVE, false), false, false);
    driver.put(MembershipState.newInstance("routerId", "ns1",
        "ns1-ha2", "cluster1", "block1", "rpc2",
        "service2", "lifeline2", "https", "nn02",
        FederationNamenodeServiceState.STANDBY, false), false, false);
    Configuration conf = new Configuration();
    conf.setClass(RBFConfigKeys.FEDERATION_STORE_DRIVER_CLASS,
        MockStateStoreDriver.class,
        StateStoreDriver.class);
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    try (PrintStream stream = new PrintStream(buffer)) {
      RouterAdmin.dumpStateStore(conf, stream);
    }
    final String expected =
        "---- DisabledNameservice ----\n" +
            "\n" +
            "---- MembershipState ----\n" +
            "  ns1-ha1-ns1-routerId:\n" +
            "    dateCreated: XXX\n" +
            "    dateModified: XXX\n" +
            "    routerId: \"routerId\"\n" +
            "    nameserviceId: \"ns1\"\n" +
            "    namenodeId: \"ns1-ha1\"\n" +
            "    clusterId: \"cluster1\"\n" +
            "    blockPoolId: \"block1\"\n" +
            "    webAddress: \"nn01\"\n" +
            "    rpcAddress: \"rpc1\"\n" +
            "    serviceAddress: \"service1\"\n" +
            "    lifelineAddress: \"lifeline1\"\n" +
            "    state: \"ACTIVE\"\n" +
            "    isSafeMode: false\n" +
            "    webScheme: \"https\"\n" +
            "    \n" +
            "  ns1-ha2-ns1-routerId:\n" +
            "    dateCreated: XXX\n" +
            "    dateModified: XXX\n" +
            "    routerId: \"routerId\"\n" +
            "    nameserviceId: \"ns1\"\n" +
            "    namenodeId: \"ns1-ha2\"\n" +
            "    clusterId: \"cluster1\"\n" +
            "    blockPoolId: \"block1\"\n" +
            "    webAddress: \"nn02\"\n" +
            "    rpcAddress: \"rpc2\"\n" +
            "    serviceAddress: \"service2\"\n" +
            "    lifelineAddress: \"lifeline2\"\n" +
            "    state: \"STANDBY\"\n" +
            "    isSafeMode: false\n" +
            "    webScheme: \"https\"\n" +
            "    \n" +
            "\n" +
            "---- MountTable ----\n" +
            "\n" +
            "---- RouterState ----";
    // Replace the time values with XXX
    assertEquals(expected,
        buffer.toString().trim().replaceAll("[0-9]{4,}+", "XXX"));
  }

  @Test
  public void testAddMultipleMountPointsSuccess() throws Exception {
    String[] argv =
        new String[] {"-addAll", "/testAddMultipleMountPoints-01", "ns01", "/dest01", ",",
            "/testAddMultipleMountPoints-02", "ns02,ns03", "/dest02", "-order", "HASH_ALL",
            "-faulttolerant", ",", "/testAddMultipleMountPoints-03", "ns03", "/dest03"};
    assertEquals(0, ToolRunner.run(admin, argv));

    stateStore.loadCache(MountTableStoreImpl.class, true);

    validateMountEntry("/testAddMultipleMountPoints-01", 1, new String[] {"/dest01"},
        new String[] {"ns01"});
    validateMountEntry("/testAddMultipleMountPoints-02", 2, new String[] {"/dest02", "/dest02"},
        new String[] {"ns02", "ns03"});
    validateMountEntry("/testAddMultipleMountPoints-03", 1, new String[] {"/dest03"},
        new String[] {"ns03"});
  }

  private static void validateMountEntry(String mountName, int numDest, String[] dest, String[] nss)
      throws IOException {
    GetMountTableEntriesRequest request = GetMountTableEntriesRequest.newInstance(mountName);
    GetMountTableEntriesResponse response =
        client.getMountTableManager().getMountTableEntries(request);
    assertEquals(1, response.getEntries().size());
    List<RemoteLocation> destinations = response.getEntries().get(0).getDestinations();
    assertEquals(numDest, destinations.size());
    for (int i = 0; i < numDest; i++) {
      assertEquals(mountName, destinations.get(i).getSrc());
      assertEquals(dest[i], destinations.get(i).getDest());
      assertEquals(nss[i], destinations.get(i).getNameserviceId());
    }
  }

  @Test
  public void testAddMultipleMountPointsFailure() throws Exception {
    System.setErr(new PrintStream(err));

    String[] argv =
        new String[] {"-addAll", "/testAddMultiMountPoints-01", "ns01", ",", "/dest01", ",",
            "/testAddMultiMountPoints-02", "ns02,ns03", "/dest02", "-order", "HASH_ALL",
            "-faulttolerant", ",", "/testAddMultiMountPoints-03", "ns03", "/dest03", ",",
            "/testAddMultiMountPoints-01", "ns02", "/dest02"};
    // syntax issue
    assertNotEquals(0, ToolRunner.run(admin, argv));

    argv =
        new String[] {"-addAll", "/testAddMultiMountPoints-01", "ns01", "/dest01", ",",
            "/testAddMultiMountPoints-02", "ns02,ns03", "/dest02", "-order", "HASH_ALL",
            "-faulttolerant", ",", "/testAddMultiMountPoints-03", "ns03", "/dest03", ",",
            "/testAddMultiMountPoints-01", "ns02", "/dest02"};
    // multiple inputs with same mount
    assertNotEquals(0, ToolRunner.run(admin, argv));

    argv =
        new String[] {"-addAll", "/testAddMultiMountPoints-01", "ns01", "/dest01,/dest02", ",",
            "/testAddMultiMountPoints-02", "ns02,ns03", "/dest02", "-order", "HASH_ALL",
            "-faulttolerant"};
    // multiple dest entries
    assertNotEquals(0, ToolRunner.run(admin, argv));

    argv =
        new String[] {"-addAll", "/testAddMultiMountPoints-01", "ns01", "/dest01", ",",
            "/testAddMultiMountPoints-02", "ns02,ns03", "/dest02", "-order", "HASH_ALL",
            "-faulttolerant"};
    // success
    assertEquals(0, ToolRunner.run(admin, argv));

    argv =
        new String[] {"-addAll", "/testAddMultiMountPoints-01", "ns01", "/dest01", ",",
            "/testAddMultiMountPoints-02", "ns02,ns03", "/dest02", "-order", "HASH_ALL",
            "-faulttolerant"};
    // mount points were already added
    assertNotEquals(0, ToolRunner.run(admin, argv));

    assertTrue("The error message should return failed entries",
        err.toString().contains("Cannot add mount points: [/testAddMultiMountPoints-01"));
  }

  private void addMountTable(String src, String nsId, String dst)
      throws Exception {
    String[] argv = new String[] {"-add", src, nsId, dst};
    assertEquals(0, ToolRunner.run(admin, argv));
    stateStore.loadCache(MountTableStoreImpl.class, true);
  }

  private List<MountTable> getMountTable(String src) throws IOException {
    GetMountTableEntriesRequest getRequest =
        GetMountTableEntriesRequest.newInstance(src);
    GetMountTableEntriesResponse getResponse =
        client.getMountTableManager().getMountTableEntries(getRequest);
    return getResponse.getEntries();
  }

  private void setStorageTypeQuota(String src, long ssQuota, StorageType type)
      throws Exception {
    assertEquals(0, ToolRunner.run(admin,
        new String[] {"-setStorageTypeQuota", src, "-storageType", type.name(),
            String.valueOf(ssQuota)}));
    stateStore.loadCache(MountTableStoreImpl.class, true);
  }

  private void rmMountTable(String src) throws Exception {
    String[] argv = new String[] {"-rm", src};
    assertEquals(0, ToolRunner.run(admin, argv));
    stateStore.loadCache(MountTableStoreImpl.class, true);
  }
}
