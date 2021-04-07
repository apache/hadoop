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
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.NamenodeContext;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.order.DestinationOrder;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test a router end-to-end including the MountTable.
 */
public class TestRouterMountTable {

  private static StateStoreDFSCluster cluster;
  private static NamenodeContext nnContext0;
  private static NamenodeContext nnContext1;
  private static RouterContext routerContext;
  private static MountTableResolver mountTable;
  private static ClientProtocol routerProtocol;
  private static long startTime;
  private static FileSystem nnFs0;
  private static FileSystem nnFs1;
  private static FileSystem routerFs;

  @BeforeClass
  public static void globalSetUp() throws Exception {
    startTime = Time.now();

    // Build and start a federated cluster
    cluster = new StateStoreDFSCluster(false, 2);
    Configuration conf = new RouterConfigBuilder()
        .stateStore()
        .admin()
        .rpc()
        .build();
    cluster.addRouterOverrides(conf);
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp();

    // Get the end points
    nnContext0 = cluster.getNamenode("ns0", null);
    nnContext1 = cluster.getNamenode("ns1", null);
    nnFs0 = nnContext0.getFileSystem();
    nnFs1 = nnContext1.getFileSystem();
    routerContext = cluster.getRandomRouter();
    routerFs = routerContext.getFileSystem();
    Router router = routerContext.getRouter();
    routerProtocol = routerContext.getClient().getNamenode();
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

  @Test
  public void testReadOnly() throws Exception {

    // Add a read only entry
    MountTable readOnlyEntry = MountTable.newInstance(
        "/readonly", Collections.singletonMap("ns0", "/testdir"));
    readOnlyEntry.setReadOnly(true);
    assertTrue(addMountTable(readOnlyEntry));

    // Add a regular entry
    MountTable regularEntry = MountTable.newInstance(
        "/regular", Collections.singletonMap("ns0", "/testdir"));
    assertTrue(addMountTable(regularEntry));

    // Create a folder which should show in all locations
    assertTrue(routerFs.mkdirs(new Path("/regular/newdir")));

    FileStatus dirStatusNn =
        nnFs0.getFileStatus(new Path("/testdir/newdir"));
    assertTrue(dirStatusNn.isDirectory());
    FileStatus dirStatusRegular =
        routerFs.getFileStatus(new Path("/regular/newdir"));
    assertTrue(dirStatusRegular.isDirectory());
    FileStatus dirStatusReadOnly =
        routerFs.getFileStatus(new Path("/readonly/newdir"));
    assertTrue(dirStatusReadOnly.isDirectory());

    // It should fail writing into a read only path
    try {
      routerFs.mkdirs(new Path("/readonly/newdirfail"));
      fail("We should not be able to write into a read only mount point");
    } catch (IOException ioe) {
      String msg = ioe.getMessage();
      assertTrue(msg.startsWith(
          "/readonly/newdirfail is in a read only mount point"));
    }
  }

  /**
   * Add a mount table entry to the mount table through the admin API.
   * @param entry Mount table entry to add.
   * @return If it was succesfully added.
   * @throws IOException Problems adding entries.
   */
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
   * Verify that the file/dir listing contains correct date/time information.
   */
  @Test
  public void testListFilesTime() throws Exception {
    try {
      // Add mount table entry
      MountTable addEntry = MountTable.newInstance("/testdir",
          Collections.singletonMap("ns0", "/testdir"));
      assertTrue(addMountTable(addEntry));
      addEntry = MountTable.newInstance("/testdir2",
          Collections.singletonMap("ns0", "/testdir2"));
      assertTrue(addMountTable(addEntry));
      addEntry = MountTable.newInstance("/testdir/subdir",
          Collections.singletonMap("ns0", "/testdir/subdir"));
      assertTrue(addMountTable(addEntry));
      addEntry = MountTable.newInstance("/testdir3/subdir1",
          Collections.singletonMap("ns0", "/testdir3"));
      assertTrue(addMountTable(addEntry));
      addEntry = MountTable.newInstance("/testA/testB/testC/testD",
          Collections.singletonMap("ns0", "/test"));
      assertTrue(addMountTable(addEntry));

      // Create test dir in NN
      assertTrue(nnFs0.mkdirs(new Path("/newdir")));

      Map<String, Long> pathModTime = new TreeMap<>();
      for (String mount : mountTable.getMountPoints("/")) {
        if (mountTable.getMountPoint("/" + mount) != null) {
          pathModTime.put(mount,
              mountTable.getMountPoint("/" + mount).getDateModified());
        } else {
          List<MountTable> entries = mountTable.getMounts("/" + mount);
          for (MountTable entry : entries) {
            if (pathModTime.get(mount) == null
                || pathModTime.get(mount) < entry.getDateModified()) {
              pathModTime.put(mount, entry.getDateModified());
            }
          }
        }
      }
      FileStatus[] iterator = nnFs0.listStatus(new Path("/"));
      for (FileStatus file : iterator) {
        pathModTime.put(file.getPath().getName(), file.getModificationTime());
      }
      // Fetch listing
      DirectoryListing listing =
          routerProtocol.getListing("/", HdfsFileStatus.EMPTY_NAME, false);
      Iterator<String> pathModTimeIterator = pathModTime.keySet().iterator();

      // Match date/time for each path returned
      for (HdfsFileStatus f : listing.getPartialListing()) {
        String fileName = pathModTimeIterator.next();
        String currentFile = f.getFullPath(new Path("/")).getName();
        Long currentTime = f.getModificationTime();
        Long expectedTime = pathModTime.get(currentFile);

        assertEquals(currentFile, fileName);
        assertTrue(currentTime > startTime);
        assertEquals(currentTime, expectedTime);
      }
      // Verify the total number of results found/matched
      assertEquals(pathModTime.size(), listing.getPartialListing().length);
    } finally {
      nnFs0.delete(new Path("/newdir"), true);
    }
  }

  /**
   * Verify that the file/dir status with IOException in getMountPointStatus.
   */
  @Test
  public void testGetMountPointStatusWithIOException()
      throws IOException, InterruptedException {
    try {
      // Add mount table entry.
      MountTable addEntry = MountTable.newInstance("/testA",
          Collections.singletonMap("ns0", "/testA"));
      assertTrue(addMountTable(addEntry));
      addEntry = MountTable.newInstance("/testA/testB",
          Collections.singletonMap("ns0", "/testA/testB"));
      addEntry.setOwnerName("userB");
      addEntry.setGroupName("groupB");
      assertTrue(addMountTable(addEntry));
      addEntry = MountTable.newInstance("/testB",
          Collections.singletonMap("ns0", "/test1/testB"));
      assertTrue(addMountTable(addEntry));

      assertTrue(nnFs0.mkdirs(new Path("/test1")));
      nnFs0.setPermission(new Path("/test1"),
          FsPermission.createImmutable((short) 0700));

      // Use mock user to getListing through router.
      UserGroupInformation user = UserGroupInformation.createUserForTesting(
          "mock_user", new String[] {"mock_group"});
      LambdaTestUtils.doAs(user, () -> getListing("/testA"));
    } finally {
      nnFs0.delete(new Path("/test1"), true);
    }
  }

  /**
   * Verify the getMountPointStatus result of passing in different parameters.
   */
  @Test
  public void testGetMountPointStatus() throws IOException {
    MountTable addEntry = MountTable.newInstance("/testA/testB/testC/testD",
        Collections.singletonMap("ns0", "/testA/testB/testC/testD"));
    assertTrue(addMountTable(addEntry));
    RouterClientProtocol clientProtocol = new RouterClientProtocol(
        nnFs0.getConf(), routerContext.getRouter().getRpcServer());
    String src = "/";
    String child = "testA";
    Path childPath = new Path(src, child);
    HdfsFileStatus dirStatus =
        clientProtocol.getMountPointStatus(childPath.toString(), 0, 0);
    assertEquals(child, dirStatus.getLocalName());

    String src1 = "/testA";
    String child1 = "testB";
    Path childPath1 = new Path(src1, child1);
    HdfsFileStatus dirStatus1 =
        clientProtocol.getMountPointStatus(childPath1.toString(), 0, 0);
    assertEquals(child1, dirStatus1.getLocalName());

    String src2 = "/testA/testB";
    String child2 = "testC";
    Path childPath2 = new Path(src2, child2);
    HdfsFileStatus dirStatus2 =
        clientProtocol.getMountPointStatus(childPath2.toString(), 0, 0);
    assertEquals(child2, dirStatus2.getLocalName());
  }
  /**
   * GetListing of testPath through router.
   */
  private void getListing(String testPath)
      throws IOException, URISyntaxException {
    ClientProtocol clientProtocol1 =
        routerContext.getClient().getNamenode();
    DirectoryListing listing = clientProtocol1.getListing(testPath,
        HdfsFileStatus.EMPTY_NAME, false);

    assertEquals(1, listing.getPartialListing().length);
    HdfsFileStatus fileStatus = listing.getPartialListing()[0];
    String currentOwner = fileStatus.getOwner();
    String currentGroup = fileStatus.getGroup();
    String currentFileName =
        fileStatus.getFullPath(new Path("/")).getName();

    assertEquals("testB", currentFileName);
    assertEquals("userB", currentOwner);
    assertEquals("groupB", currentGroup);
  }

  @Test
  public void testListNonExistPath() throws Exception {
    mountTable.setDefaultNSEnable(false);
    LambdaTestUtils.intercept(FileNotFoundException.class,
        "File /base does not exist.",
        "Expect FileNotFoundException.",
        () -> routerFs.listStatus(new Path("/base")));
  }

  @Test
  public void testListWhenDisableDefaultMountTable() throws IOException {
    mountTable.setDefaultNSEnable(false);
    /**
     * /base/dir1 -> ns0:/base/dir1
     * /base/dir2 -> ns0:/base/dir2
     */
    assertTrue(addMountTable(createEntry("/base/dir1", "ns0", "/base/dir1",
        "group2", "owner2", (short) 0750)));
    assertTrue(addMountTable(createEntry("/base/dir2", "ns0", "/base/dir2",
        "group3", "owner3", (short) 0755)));

    FileStatus[] list = routerFs.listStatus(new Path("/base"));
    assertEquals(2, list.length);
    for (FileStatus status : list) {
      if (status.getPath().toUri().getPath().equals("/base/dir1")) {
        assertEquals("group2", status.getGroup());
        assertEquals("owner2", status.getOwner());
        assertEquals((short) 0750, status.getPermission().toShort());
      } else if (status.getPath().toUri().getPath().equals("/base/dir2")) {
        assertEquals("group3", status.getGroup());
        assertEquals("owner3", status.getOwner());
        assertEquals((short) 0755, status.getPermission().toShort());
      } else {
        fail("list result should be either /base/dir1 or /base/dir2.");
      }
    }
  }

  /**
   * Verify permission for a mount point when the actual destination is not
   * present. It returns the permissions of the mount point.
   */
  @Test
  public void testMountTablePermissionsNoDest() throws IOException {
    MountTable addEntry;
    addEntry = MountTable.newInstance("/testdir1",
        Collections.singletonMap("ns0", "/tmp/testdir1"));
    addEntry.setGroupName("group1");
    addEntry.setOwnerName("owner1");
    addEntry.setMode(FsPermission.createImmutable((short) 0775));
    assertTrue(addMountTable(addEntry));
    FileStatus[] list = routerFs.listStatus(new Path("/"));
    assertEquals("group1", list[0].getGroup());
    assertEquals("owner1", list[0].getOwner());
    assertEquals((short) 0775, list[0].getPermission().toShort());
  }

  private MountTable createEntry(String mountPath, String ns, String remotePath,
      String group, String owner, short permission) throws IOException {
    MountTable entry = MountTable
        .newInstance(mountPath, Collections.singletonMap(ns, remotePath));
    entry.setGroupName(group);
    entry.setOwnerName(owner);
    entry.setMode(FsPermission.createImmutable(permission));
    return entry;
  }

  /**
   * Verify permission for a mount point when the actual destination present. It
   * returns the permissions of the actual destination pointed by the mount
   * point.
   */
  @Test
  public void testMountTablePermissionsWithDest() throws IOException {
    try {
      MountTable addEntry = MountTable.newInstance("/testdir",
          Collections.singletonMap("ns0", "/tmp/testdir"));
      assertTrue(addMountTable(addEntry));
      nnFs0.mkdirs(new Path("/tmp/testdir"));
      nnFs0.setOwner(new Path("/tmp/testdir"), "Aowner", "Agroup");
      nnFs0.setPermission(new Path("/tmp/testdir"),
          FsPermission.createImmutable((short) 775));
      FileStatus[] list = routerFs.listStatus(new Path("/"));
      assertEquals("Agroup", list[0].getGroup());
      assertEquals("Aowner", list[0].getOwner());
      assertEquals((short) 775, list[0].getPermission().toShort());
    } finally {
      nnFs0.delete(new Path("/tmp"), true);
    }
  }

  /**
   * Verify permission for a mount point when the multiple destinations are
   * present with both having same permissions. It returns the same actual
   * permissions of the actual destinations pointed by the mount point.
   */
  @Test
  public void testMountTablePermissionsMultiDest() throws IOException {
    try {
      Map<String, String> destMap = new HashMap<>();
      destMap.put("ns0", "/tmp/testdir");
      destMap.put("ns1", "/tmp/testdir01");
      MountTable addEntry = MountTable.newInstance("/testdir", destMap);
      assertTrue(addMountTable(addEntry));
      nnFs0.mkdirs(new Path("/tmp/testdir"));
      nnFs0.setOwner(new Path("/tmp/testdir"), "Aowner", "Agroup");
      nnFs0.setPermission(new Path("/tmp/testdir"),
          FsPermission.createImmutable((short) 775));
      nnFs1.mkdirs(new Path("/tmp/testdir01"));
      nnFs1.setOwner(new Path("/tmp/testdir01"), "Aowner", "Agroup");
      nnFs1.setPermission(new Path("/tmp/testdir01"),
          FsPermission.createImmutable((short) 775));
      FileStatus[] list = routerFs.listStatus(new Path("/"));
      assertEquals("Agroup", list[0].getGroup());
      assertEquals("Aowner", list[0].getOwner());
      assertEquals((short) 775, list[0].getPermission().toShort());
    } finally {
      nnFs0.delete(new Path("/tmp"), true);
      nnFs1.delete(new Path("/tmp"), true);
    }
  }

  /**
   * Verify permission for a mount point when the multiple destinations are
   * present with both having different permissions. It returns the actual
   * permissions of either of the actual destinations pointed by the mount
   * point.
   */
  @Test
  public void testMountTablePermissionsMultiDestDifferentPerm()
      throws IOException {
    try {
      Map<String, String> destMap = new HashMap<>();
      destMap.put("ns0", "/tmp/testdir");
      destMap.put("ns1", "/tmp/testdir01");
      MountTable addEntry = MountTable.newInstance("/testdir", destMap);
      assertTrue(addMountTable(addEntry));
      nnFs0.mkdirs(new Path("/tmp/testdir"));
      nnFs0.setOwner(new Path("/tmp/testdir"), "Aowner", "Agroup");
      nnFs0.setPermission(new Path("/tmp/testdir"),
          FsPermission.createImmutable((short) 775));
      nnFs1.mkdirs(new Path("/tmp/testdir01"));
      nnFs1.setOwner(new Path("/tmp/testdir01"), "Aowner01", "Agroup01");
      nnFs1.setPermission(new Path("/tmp/testdir01"),
          FsPermission.createImmutable((short) 755));
      FileStatus[] list = routerFs.listStatus(new Path("/"));
      assertTrue("Agroup".equals(list[0].getGroup())
          || "Agroup01".equals(list[0].getGroup()));
      assertTrue("Aowner".equals(list[0].getOwner())
          || "Aowner01".equals(list[0].getOwner()));
      assertTrue(((short) 775) == list[0].getPermission().toShort()
          || ((short) 755) == list[0].getPermission().toShort());
    } finally {
      nnFs0.delete(new Path("/tmp"), true);
      nnFs1.delete(new Path("/tmp"), true);
    }
  }

  /**
   * Validate whether mount point name gets resolved or not. On successful
   * resolution the details returned would be the ones actually set on the mount
   * point.
   */
  @Test
  public void testMountPointResolved() throws IOException {
    MountTable addEntry = MountTable.newInstance("/testdir",
        Collections.singletonMap("ns0", "/tmp/testdir"));
    addEntry.setGroupName("group1");
    addEntry.setOwnerName("owner1");
    assertTrue(addMountTable(addEntry));
    HdfsFileStatus finfo = routerProtocol.getFileInfo("/testdir");
    FileStatus[] finfo1 = routerFs.listStatus(new Path("/"));
    assertEquals("owner1", finfo.getOwner());
    assertEquals("owner1", finfo1[0].getOwner());
    assertEquals("group1", finfo.getGroup());
    assertEquals("group1", finfo1[0].getGroup());
  }

  /**
   * Validate the number of children for the mount point.It must be equal to the
   * number of children of the destination pointed by the mount point.
   */
  @Test
  public void testMountPointChildren() throws IOException {
    try {
      MountTable addEntry = MountTable.newInstance("/testdir",
          Collections.singletonMap("ns0", "/tmp/testdir"));
      assertTrue(addMountTable(addEntry));
      nnFs0.mkdirs(new Path("/tmp/testdir"));
      nnFs0.mkdirs(new Path("/tmp/testdir/1"));
      nnFs0.mkdirs(new Path("/tmp/testdir/2"));
      FileStatus[] finfo1 = routerFs.listStatus(new Path("/"));
      assertEquals(2, ((HdfsFileStatus) finfo1[0]).getChildrenNum());
    } finally {
      nnFs0.delete(new Path("/tmp"), true);
    }
  }

  /**
   * Validate the number of children for the mount point pointing to multiple
   * destinations.It must be equal to the sum of number of children of the
   * destinations pointed by the mount point.
   */
  @Test
  public void testMountPointChildrenMultiDest() throws IOException {
    try {
      Map<String, String> destMap = new HashMap<>();
      destMap.put("ns0", "/tmp/testdir");
      destMap.put("ns1", "/tmp/testdir01");
      MountTable addEntry = MountTable.newInstance("/testdir", destMap);
      assertTrue(addMountTable(addEntry));
      nnFs0.mkdirs(new Path("/tmp/testdir"));
      nnFs0.mkdirs(new Path("/tmp/testdir"));
      nnFs1.mkdirs(new Path("/tmp/testdir01"));
      nnFs0.mkdirs(new Path("/tmp/testdir/1"));
      nnFs1.mkdirs(new Path("/tmp/testdir01/1"));
      FileStatus[] finfo1 = routerFs.listStatus(new Path("/"));
      assertEquals(2, ((HdfsFileStatus) finfo1[0]).getChildrenNum());
    } finally {
      nnFs0.delete(new Path("/tmp"), true);
      nnFs0.delete(new Path("/tmp"), true);
    }
  }

  /**
   * Validates the path in the exception. The path should be with respect to the
   * mount not with respect to the sub cluster.
   */
  @Test
  public void testPathInException() throws Exception {
    MountTable addEntry = MountTable.newInstance("/mount",
        Collections.singletonMap("ns0", "/tmp/testdir"));
    addEntry.setDestOrder(DestinationOrder.HASH_ALL);
    assertTrue(addMountTable(addEntry));
    LambdaTestUtils.intercept(FileNotFoundException.class,
        "Directory/File does not exist /mount/file",
        () -> routerFs.setOwner(new Path("/mount/file"), "user", "group"));
  }

  /**
   * Regression test for HDFS-14369.
   * Verify that getListing works with the path with trailing slash.
   */
  @Test
  public void testGetListingWithTrailingSlash() throws IOException {
    try {
      // Add mount table entry
      MountTable addEntry = MountTable.newInstance("/testlist",
          Collections.singletonMap("ns0", "/testlist"));
      assertTrue(addMountTable(addEntry));
      addEntry = MountTable.newInstance("/testlist/tmp0",
          Collections.singletonMap("ns0", "/testlist/tmp0"));
      assertTrue(addMountTable(addEntry));
      addEntry = MountTable.newInstance("/testlist/tmp1",
          Collections.singletonMap("ns1", "/testlist/tmp1"));
      assertTrue(addMountTable(addEntry));

      nnFs0.mkdirs(new Path("/testlist/tmp0"));
      nnFs1.mkdirs(new Path("/testlist/tmp1"));
      // Fetch listing
      DirectoryListing list = routerProtocol.getListing(
          "/testlist/", HdfsFileStatus.EMPTY_NAME, false);
      HdfsFileStatus[] statuses = list.getPartialListing();
      // should return tmp0 and tmp1
      assertEquals(2, statuses.length);
    } finally {
      nnFs0.delete(new Path("/testlist/tmp0"), true);
      nnFs1.delete(new Path("/testlist/tmp1"), true);
    }
  }

  /**
   * Regression test for HDFS-13255.
   * Verify that delete fails if the path is a mount point or
   * there are any mount point under the path.
   */
  @Test
  public void testDeleteMountPoint() throws Exception {
    try {
      MountTable addEntry = MountTable.newInstance("/testdelete/subdir",
          Collections.singletonMap("ns0", "/testdelete/subdir"));
      assertTrue(addMountTable(addEntry));
      nnFs0.mkdirs(new Path("/testdelete/subdir"));
      LambdaTestUtils.intercept(AccessControlException.class,
          "The operation is not allowed because there are mount points: "
              + "subdir under the path: /testdelete",
          () -> routerFs.delete(new Path("/testdelete"), true));
      LambdaTestUtils.intercept(AccessControlException.class,
          "The operation is not allowed because there are mount points: "
              + "subdir under the path: /testdelete",
          () -> routerFs.delete(new Path("/testdelete"), false));
      LambdaTestUtils.intercept(AccessControlException.class,
          "The operation is not allowed because the path: "
              + "/testdelete/subdir is a mount point",
          () -> routerFs.delete(new Path("/testdelete/subdir"), true));
      LambdaTestUtils.intercept(AccessControlException.class,
          "The operation is not allowed because the path: "
              + "/testdelete/subdir is a mount point",
          () -> routerFs.delete(new Path("/testdelete/subdir"), false));
    } finally {
      nnFs0.delete(new Path("/testdelete"), true);
    }
  }

  /**
   * Regression test for HDFS-13255.
   * Verify that rename fails if the src path is a mount point or
   * there are any mount point under the path.
   */
  @Test
  public void testRenameMountPoint() throws Exception {
    try {
      MountTable addEntry = MountTable.newInstance("/testrename1/sub",
          Collections.singletonMap("ns0", "/testrename1/sub"));
      assertTrue(addMountTable(addEntry));
      addEntry = MountTable.newInstance("/testrename2/sub",
          Collections.singletonMap("ns0", "/testrename2/sub"));
      assertTrue(addMountTable(addEntry));
      nnFs0.mkdirs(new Path("/testrename1/sub/sub"));
      nnFs0.mkdirs(new Path("/testrename2"));

      // Success: rename a directory to a mount point
      assertTrue(nnFs0.exists(new Path("/testrename1/sub/sub")));
      assertFalse(nnFs0.exists(new Path("/testrename2/sub")));
      assertTrue(routerFs.rename(new Path("/testrename1/sub/sub"),
          new Path("/testrename2")));
      assertFalse(nnFs0.exists(new Path("/testrename1/sub/sub")));
      assertTrue(nnFs0.exists(new Path("/testrename2/sub")));

      // Fail: the target already exists
      nnFs0.mkdirs(new Path("/testrename1/sub/sub"));
      assertFalse(routerFs.rename(new Path("/testrename1/sub/sub"),
              new Path("/testrename2")));

      // Fail: The src is a mount point
      LambdaTestUtils.intercept(AccessControlException.class,
          "The operation is not allowed because the path: "
              + "/testrename1/sub is a mount point",
          () -> routerFs.rename(new Path("/testrename1/sub"),
              new Path("/testrename2/sub")));

      // Fail: There is a mount point under the src
      LambdaTestUtils.intercept(AccessControlException.class,
          "The operation is not allowed because there are mount points: "
              + "sub under the path: /testrename1",
          () -> routerFs.rename(new Path("/testrename1"),
              new Path("/testrename2/sub")));
    } finally {
      nnFs0.delete(new Path("/testrename1"), true);
      nnFs0.delete(new Path("/testrename2"), true);
    }
  }

  @Test
  public void testListStatusMountPoint() throws Exception {
    try {
      MountTable addEntry = MountTable.newInstance("/mount/testLsMountEntry",
          Collections.singletonMap("ns0", "/testLsMountEntryDest"));
      assertTrue(addMountTable(addEntry));
      nnFs0.mkdirs(new Path("/testLsMountEntryDest"));
      DistributedFileSystem routerDfs = (DistributedFileSystem) routerFs;
      Path mountPath = new Path("/mount/testLsMountEntry");
      routerDfs.setErasureCodingPolicy(mountPath, "RS-6-3-1024k");
      assertTrue(routerDfs.listStatus(new Path("/mount"))[0].isErasureCoded());
    } finally {
      nnFs0.delete(new Path("/testLsMountEntryDest"), true);
    }
  }
}
