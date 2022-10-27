/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.federation.router;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.store.protocol.*;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

/**
 * This is a test through the Router move data to the Trash.
 */
public class TestRouterTrash {

  public static final Logger LOG =
      LoggerFactory.getLogger(TestRouterTrash.class);

  private static StateStoreDFSCluster cluster;
  private static MiniRouterDFSCluster.RouterContext routerContext;
  private static MountTableResolver mountTable;
  private static FileSystem routerFs;
  private static FileSystem nnFs;
  private static final String TEST_USER = "test-trash";
  private static MiniRouterDFSCluster.NamenodeContext nnContext;
  private static String ns0;
  private static String ns1;
  private static final String MOUNT_POINT = "/home/data";
  private static final String FILE = MOUNT_POINT + "/file1";
  private static final String TRASH_ROOT = "/user/" + TEST_USER + "/.Trash";
  private static final String CURRENT = "/Current";

  @BeforeClass
  public static void globalSetUp() throws Exception {
    // Build and start a federated cluster
    cluster = new StateStoreDFSCluster(false, 2);
    Configuration conf = new RouterConfigBuilder()
        .stateStore()
        .admin()
        .rpc()
        .http()
        .build();
    conf.set(FS_TRASH_INTERVAL_KEY, "100");
    cluster.addRouterOverrides(conf);
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp();

    ns0 = cluster.getNameservices().get(0);
    ns1 = cluster.getNameservices().get(1);

    routerContext = cluster.getRandomRouter();
    routerFs = routerContext.getFileSystem();
    nnContext = cluster.getNamenode(ns0, null);
    nnFs = nnContext.getFileSystem();
    Router router = routerContext.getRouter();
    mountTable = (MountTableResolver) router.getSubclusterResolver();
  }

  @AfterClass
  public static void tearDown() {
    if (cluster != null) {
      cluster.stopRouter(routerContext);
      cluster.shutdown();
      cluster = null;
    }
  }

  @After
  public void clearMountTable() throws IOException {
    RouterClient client = routerContext.getAdminClient();
    MountTableManager mountTableManager = client.getMountTableManager();
    GetMountTableEntriesRequest req1 =
        GetMountTableEntriesRequest.newInstance("/");
    GetMountTableEntriesResponse response =
        mountTableManager.getMountTableEntries(req1);
    for (MountTable entry : response.getEntries()) {
      RemoveMountTableEntryRequest req2 =
          RemoveMountTableEntryRequest.newInstance(entry.getSourcePath());
      mountTableManager.removeMountTableEntry(req2);
    }
  }

  @After
  public void clearFile() throws IOException {
    FileStatus[] fileStatuses = nnFs.listStatus(new Path("/"));
    for (FileStatus file : fileStatuses) {
      nnFs.delete(file.getPath(), true);
    }
  }

  private boolean addMountTable(final MountTable entry) throws IOException {
    RouterClient client = routerContext.getAdminClient();
    MountTableManager mountTableManager = client.getMountTableManager();
    AddMountTableEntryRequest addRequest =
        AddMountTableEntryRequest.newInstance(entry);
    AddMountTableEntryResponse addResponse =
        mountTableManager.addMountTableEntry(addRequest);
    // Reload the Router cache
    mountTable.loadCache(true);
    return addResponse.getStatus();
  }

  /**
   * Mock testing the case where all home dirs have already existed. In that
   * case, we shall not call mkdirs/setOwner in CreateUserHomeForTrashPath().
   *
   * This method uses mocked objects and does not talk to routers or minihdfs
   * clusters.
   */
  @Test
  public void mockTestCreateUserHomeForTrashPath()
      throws NoSuchFieldException, IllegalAccessException, IOException {
    // Setup RouterRpcClient mock object
    RouterRpcClient rpcClient = mock(RouterRpcClient.class);
    HdfsFileStatus status = new HdfsFileStatus.Builder().build();
    when(rpcClient.invokeSingle(any(RemoteLocation.class),
        any(RemoteMethod.class), eq(HdfsFileStatus.class))).thenReturn(status);

    // Destination remote locations.
    // Need to test with the identity getRemoteUser().getShortUserName().
    // Otherwise, it won't be considered as a trash path.
    String trashPath =
        "/user/" + RouterRpcServer.getRemoteUser().getShortUserName()
            + "/.Trash" + "/Current/testfile";
    List<RemoteLocation> locs = new ArrayList<>();
    locs.add(new RemoteLocation(ns0, trashPath, trashPath));
    locs.add(new RemoteLocation(ns1, trashPath, trashPath));

    // Setup RouterClientProtocol mock object
    RouterClientProtocol clientProcotol = mock(RouterClientProtocol.class);
    doCallRealMethod().when(clientProcotol)
        .createUserHomeForTrashPath(any(String.class), eq(locs));
    Field rpcClientField =
        RouterClientProtocol.class.getDeclaredField("rpcClient");
    rpcClientField.setAccessible(true);
    // Set to use the mocked RouterRpcClient
    rpcClientField.set(clientProcotol, rpcClient);

    clientProcotol.createUserHomeForTrashPath(trashPath, locs);
    verify(rpcClient, times(2)).invokeSingle(any(RemoteLocation.class),
        any(RemoteMethod.class), eq(HdfsFileStatus.class));
    verify(rpcClient, never()).invokeSingleAsRouter(any(RemoteLocation.class),
        any(RemoteMethod.class), eq(Boolean.class), eq(true));
  }

  /**
   * We set the owner of the root dir to TEST_USER2 and then have TEST_USER
   * to move a file to trash. Router should auto-create the home dir for
   * TEST_USER and moveToTrash should succeed.
   */
  @Test
  public void testMoveToTrashAutoCreateUserHome()
      throws IOException, URISyntaxException, InterruptedException,
             NoSuchFieldException, IllegalAccessException {
    MountTable addEntry = MountTable.newInstance(MOUNT_POINT,
        Collections.singletonMap(ns0, MOUNT_POINT));
    assertTrue(addMountTable(addEntry));
    String testUser2 = "TEST_USER2";

    RouterClientProtocol clientProcotol =
        routerContext.getRouter().getRpcServer().getClientProtocolModule();
    Field autoCreateUserHomeField = RouterClientProtocol.class.getDeclaredField(
        "autoCreateUserHomeForTrash");
    autoCreateUserHomeField.setAccessible(true);
    // Save the original value
    boolean originalAutoCreate =
        autoCreateUserHomeField.getBoolean(clientProcotol);

    // Set owner to TEST_USER for root dir
    DFSClient superUserClient = nnContext.getClient();
    superUserClient.setOwner("/", TEST_USER, TEST_USER);

    // Create MOUNT_POINT
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(TEST_USER);
    DFSClient client = nnContext.getClient(ugi);
    client.mkdirs(MOUNT_POINT, new FsPermission("777"), true);
    assertTrue(client.exists(MOUNT_POINT));
    // Create test file
    client.create(FILE, true);
    Path filePath = new Path(FILE);

    // Set owner to TEST_USER2 for the root dir so that TEST_USER does not have
    // permission to create his home dir under root dir. Instead, the router
    // will create the home dir for TEST_USER.
    superUserClient.setOwner("/", testUser2, testUser2);

    // Test moveToTrash by TEST_USER
    String trashPath = "/user/" + TEST_USER + "/.Trash/Current" + FILE;
    Configuration routerConf = routerContext.getConf();
    FileSystem fs = DFSTestUtil.getFileSystemAs(ugi, routerConf);
    Trash trash = new Trash(fs, routerConf);

    // Should fail when autoCreateUserHomeForTrash flag is off.
    autoCreateUserHomeField.set(clientProcotol, false);
    try {
      trash.moveToTrash(filePath);
    } catch (IOException ignored) {
      // expected.
    }
    // file should still exist
    assertTrue(nnFs.exists(filePath));

    // Turn on the autoCreateUserHomeForTrash flag
    autoCreateUserHomeField.set(clientProcotol, true);
    assertTrue(trash.moveToTrash(filePath));
    assertFalse(nnFs.exists(filePath));
    assertTrue(nnFs.exists(new Path(trashPath)));

    // Restore the autoCreateUserHome flag
    autoCreateUserHomeField.set(clientProcotol, originalAutoCreate);
  }

  @Test
  public void testMoveToTrashNoMountPoint() throws IOException,
      URISyntaxException, InterruptedException {
    MountTable addEntry = MountTable.newInstance(MOUNT_POINT,
        Collections.singletonMap(ns0, MOUNT_POINT));
    assertTrue(addMountTable(addEntry));
    // current user client
    DFSClient client = nnContext.getClient();
    client.setOwner("/", TEST_USER, TEST_USER);
    UserGroupInformation ugi = UserGroupInformation.
        createRemoteUser(TEST_USER);
    // test user client
    client = nnContext.getClient(ugi);
    client.mkdirs(MOUNT_POINT, new FsPermission("777"), true);
    assertTrue(client.exists(MOUNT_POINT));
    // create test file
    client.create(FILE, true);
    Path filePath = new Path(FILE);

    FileStatus[] fileStatuses = routerFs.listStatus(filePath);
    assertEquals(1, fileStatuses.length);
    assertEquals(TEST_USER, fileStatuses[0].getOwner());
    // move to Trash
    Configuration routerConf = routerContext.getConf();
    FileSystem fs =
        DFSTestUtil.getFileSystemAs(ugi, routerConf);
    Trash trash = new Trash(fs, routerConf);
    assertTrue(trash.moveToTrash(filePath));
    fileStatuses = nnFs.listStatus(
        new Path(TRASH_ROOT + CURRENT + MOUNT_POINT));
    assertEquals(1, fileStatuses.length);
    assertTrue(nnFs.exists(new Path(TRASH_ROOT + CURRENT + FILE)));
    assertTrue(nnFs.exists(new Path("/user/" +
        TEST_USER + "/.Trash/Current" + FILE)));
    // When the target path in Trash already exists.
    client.create(FILE, true);
    filePath = new Path(FILE);
    fileStatuses = routerFs.listStatus(filePath);
    assertEquals(1, fileStatuses.length);
    assertTrue(trash.moveToTrash(filePath));
    fileStatuses = routerFs.listStatus(
        new Path(TRASH_ROOT + CURRENT + MOUNT_POINT));
    assertEquals(2, fileStatuses.length);
  }

  @Test
  public void testMoveToTrashWithKerberosUser() throws IOException,
      URISyntaxException, InterruptedException {
    //Constructs the structure of the KerBoers user name
    String kerberosUser = "randomUser/dev@HADOOP.COM";
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(kerberosUser);
    MountTable addEntry = MountTable.newInstance(MOUNT_POINT,
        Collections.singletonMap(ns1, MOUNT_POINT));
    assertTrue(addMountTable(addEntry));
    // current user client
    MiniRouterDFSCluster.NamenodeContext nn1Context = cluster.getNamenode(ns1, null);
    DFSClient currentUserClientNs0 = nnContext.getClient();
    DFSClient currentUserClientNs1 = nn1Context.getClient();

    currentUserClientNs0.setOwner("/", ugi.getShortUserName(), ugi.getShortUserName());
    currentUserClientNs1.setOwner("/", ugi.getShortUserName(), ugi.getShortUserName());

    // test user client
    DFSClient testUserClientNs1 = nn1Context.getClient(ugi);
    testUserClientNs1.mkdirs(MOUNT_POINT, new FsPermission("777"), true);
    assertTrue(testUserClientNs1.exists(MOUNT_POINT));
    // create test file
    testUserClientNs1.create(FILE, true);
    Path filePath = new Path(FILE);

    FileStatus[] fileStatuses = routerFs.listStatus(filePath);
    assertEquals(1, fileStatuses.length);
    assertEquals(ugi.getShortUserName(), fileStatuses[0].getOwner());
    // move to Trash
    Configuration routerConf = routerContext.getConf();
    FileSystem fs = DFSTestUtil.getFileSystemAs(ugi, routerConf);
    Trash trash = new Trash(fs, routerConf);
    assertTrue(trash.moveToTrash(filePath));
    fileStatuses = fs.listStatus(
        new Path("/user/" + ugi.getShortUserName() + "/.Trash/Current" + MOUNT_POINT));
    assertEquals(1, fileStatuses.length);
  }

  @Test
  public void testDeleteToTrashExistMountPoint() throws IOException,
      URISyntaxException, InterruptedException {
    MountTable addEntry = MountTable.newInstance(MOUNT_POINT,
        Collections.singletonMap(ns0, MOUNT_POINT));
    assertTrue(addMountTable(addEntry));
    // add Trash mount point
    addEntry = MountTable.newInstance(TRASH_ROOT,
        Collections.singletonMap(ns1, TRASH_ROOT));
    assertTrue(addMountTable(addEntry));
    // current user client
    DFSClient client = nnContext.getClient();
    client.setOwner("/", TEST_USER, TEST_USER);
    UserGroupInformation ugi = UserGroupInformation.
        createRemoteUser(TEST_USER);
    // test user client
    client = nnContext.getClient(ugi);
    client.mkdirs(MOUNT_POINT, new FsPermission("777"), true);
    assertTrue(client.exists(MOUNT_POINT));
    // create test file
    client.create(FILE, true);
    Path filePath = new Path(FILE);

    FileStatus[] fileStatuses = routerFs.listStatus(filePath);
    assertEquals(1, fileStatuses.length);
    assertEquals(TEST_USER, fileStatuses[0].getOwner());

    // move to Trash
    Configuration routerConf = routerContext.getConf();
    FileSystem fs =
        DFSTestUtil.getFileSystemAs(ugi, routerConf);
    Trash trash = new Trash(fs, routerConf);
    assertTrue(trash.moveToTrash(filePath));
    fileStatuses = nnFs.listStatus(
        new Path(TRASH_ROOT + CURRENT + MOUNT_POINT));
    assertEquals(1, fileStatuses.length);
    assertTrue(nnFs.exists(new Path(TRASH_ROOT + CURRENT + FILE)));
    // When the target path in Trash already exists.
    client.create(FILE, true);
    filePath = new Path(FILE);

    fileStatuses = nnFs.listStatus(filePath);
    assertEquals(1, fileStatuses.length);
    assertTrue(trash.moveToTrash(filePath));
    fileStatuses = nnFs.listStatus(
        new Path(TRASH_ROOT + CURRENT + MOUNT_POINT));
    assertEquals(2, fileStatuses.length);
  }

  @Test
  public void testIsTrashPath() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    assertNotNull(ugi);
    assertTrue(MountTableResolver.isTrashPath(
        "/user/" + ugi.getUserName() + "/.Trash/Current" + MOUNT_POINT));
    assertTrue(MountTableResolver.isTrashPath(
        "/user/" + ugi.getUserName() +
            "/.Trash/" + Time.now() + MOUNT_POINT));
    assertFalse(MountTableResolver.isTrashPath(MOUNT_POINT));

    // Contains TrashCurrent but does not begin with TrashCurrent.
    assertFalse(MountTableResolver.isTrashPath("/home/user/" +
        ugi.getUserName() + "/.Trash/Current" + MOUNT_POINT));
    assertFalse(MountTableResolver.isTrashPath("/home/user/" +
        ugi.getUserName() + "/.Trash/" + Time.now() + MOUNT_POINT));

    // Special cases.
    assertFalse(MountTableResolver.isTrashPath(""));
    assertFalse(MountTableResolver.isTrashPath(
        "/home/user/empty.Trash/Current"));
    assertFalse(MountTableResolver.isTrashPath(
        "/home/user/.Trash"));
    assertFalse(MountTableResolver.isTrashPath(
        "/.Trash/Current"));
  }

  @Test
  public void testSubtractTrashCurrentPath() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    assertNotNull(ugi);
    assertEquals(MOUNT_POINT, MountTableResolver.subtractTrashCurrentPath(
        "/user/" + ugi.getUserName() + "/.Trash/Current" + MOUNT_POINT));
    assertEquals(MOUNT_POINT, MountTableResolver.subtractTrashCurrentPath(
        "/user/" + ugi.getUserName() +
            "/.Trash/" + Time.now() + MOUNT_POINT));

    // Contains TrashCurrent but does not begin with TrashCurrent.
    assertEquals("/home/user/" + ugi.getUserName() +
        "/.Trash/Current" + MOUNT_POINT, MountTableResolver.
        subtractTrashCurrentPath("/home/user/" +
            ugi.getUserName() + "/.Trash/Current" + MOUNT_POINT));
    long time = Time.now();
    assertEquals("/home/user/" + ugi.getUserName() +
        "/.Trash/" + time + MOUNT_POINT, MountTableResolver.
        subtractTrashCurrentPath("/home/user/" + ugi.getUserName() +
            "/.Trash/" + time + MOUNT_POINT));
    // Special cases.
    assertEquals("", MountTableResolver.subtractTrashCurrentPath(""));
    assertEquals("/home/user/empty.Trash/Current", MountTableResolver.
        subtractTrashCurrentPath("/home/user/empty.Trash/Current"));
    assertEquals("/home/user/.Trash", MountTableResolver.
        subtractTrashCurrentPath("/home/user/.Trash"));
    assertEquals("/.Trash/Current", MountTableResolver.
        subtractTrashCurrentPath("/.Trash/Current"));
  }
}