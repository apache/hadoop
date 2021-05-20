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
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver;
import org.apache.hadoop.hdfs.server.federation.store.MembershipStore;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.protocol.*;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipState;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

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
  private static InetSocketAddress webAddress;
  private static List<MembershipState> memberships;
  private static final String TEST_USER = "test-trash";
  private static MiniRouterDFSCluster.NamenodeContext namenode;
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
    
    routerContext = cluster.getRandomRouter();
    routerFs = routerContext.getFileSystem();
    namenode = cluster.getNamenode("ns0", null);
    nnFs = namenode.getFileSystem();
    Router router = routerContext.getRouter();
    mountTable = (MountTableResolver) router.getSubclusterResolver();
    webAddress = router.getHttpServerAddress();
    ns0 = cluster.getNameservices().get(0);
    ns1 = cluster.getNameservices().get(1);
    assertNotNull(webAddress);
    
    StateStoreService stateStore = routerContext.getRouter().getStateStore();
    MembershipStore membership =
        stateStore.getRegisteredRecordStore(MembershipStore.class);
    GetNamenodeRegistrationsRequest request =
        GetNamenodeRegistrationsRequest.newInstance();
    GetNamenodeRegistrationsResponse response =
        membership.getNamenodeRegistrations(request);
    memberships = response.getNamenodeMemberships();
    Collections.sort(memberships);
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
      nnFs.delete(file.getPath());
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
    DFSClient client = namenode.getClient();
    client.setOwner("/", TEST_USER, TEST_USER);
    UserGroupInformation ugi = UserGroupInformation.
        createRemoteUser(TEST_USER);
    // test user client
    client = namenode.getClient(ugi);
    client.mkdirs(MOUNT_POINT);
    assertTrue(client.exists(MOUNT_POINT));
    // crete test file
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
    DFSClient client = namenode.getClient();
    client.setOwner("/", TEST_USER, TEST_USER);
    UserGroupInformation ugi = UserGroupInformation.
        createRemoteUser(TEST_USER);
    // test user client
    client = namenode.getClient(ugi);
    client.mkdirs(MOUNT_POINT);
    assertTrue(client.exists(MOUNT_POINT));
    // crete test file
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
}
