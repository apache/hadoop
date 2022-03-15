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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver;
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