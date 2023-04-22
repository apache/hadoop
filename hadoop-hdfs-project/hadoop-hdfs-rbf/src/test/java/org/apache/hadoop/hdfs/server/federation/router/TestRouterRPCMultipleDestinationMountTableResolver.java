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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.SnapshotStatus;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.MultipleDestinationMountTableResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.order.DestinationOrder;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetDestinationRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetDestinationResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.tools.federation.RouterAdmin;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests router rpc with multiple destination mount table resolver.
 */
public class TestRouterRPCMultipleDestinationMountTableResolver {
  private static final List<String> NS_IDS = Arrays.asList("ns0", "ns1", "ns2");

  private static StateStoreDFSCluster cluster;
  private static RouterContext routerContext;
  private static MountTableResolver resolver;
  private static DistributedFileSystem nnFs0;
  private static DistributedFileSystem nnFs1;
  private static DistributedFileSystem nnFs2;
  private static DistributedFileSystem routerFs;
  private static RouterRpcServer rpcServer;

  @BeforeClass
  public static void setUp() throws Exception {

    // Build and start a federated cluster
    cluster = new StateStoreDFSCluster(false, 3,
        MultipleDestinationMountTableResolver.class);
    Configuration routerConf =
        new RouterConfigBuilder().stateStore().admin().quota().rpc().build();

    Configuration hdfsConf = new Configuration(false);
    hdfsConf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);

    cluster.addRouterOverrides(routerConf);
    cluster.addNamenodeOverrides(hdfsConf);
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp();

    routerContext = cluster.getRandomRouter();
    resolver =
        (MountTableResolver) routerContext.getRouter().getSubclusterResolver();
    nnFs0 = (DistributedFileSystem) cluster
        .getNamenode(cluster.getNameservices().get(0), null).getFileSystem();
    nnFs1 = (DistributedFileSystem) cluster
        .getNamenode(cluster.getNameservices().get(1), null).getFileSystem();
    nnFs2 = (DistributedFileSystem) cluster
        .getNamenode(cluster.getNameservices().get(2), null).getFileSystem();
    routerFs = (DistributedFileSystem) routerContext.getFileSystem();
    rpcServer =routerContext.getRouter().getRpcServer();
  }

  @AfterClass
  public static void tearDown() {
    if (cluster != null) {
      cluster.stopRouter(routerContext);
      cluster.shutdown();
      cluster = null;
    }
  }

  /**
   * SetUp the mount entry , directories and file to verify invocation.
   * @param order The order that the mount entry needs to follow.
   * @throws Exception On account of any exception encountered during setting up
   *           the environment.
   */
  public void setupOrderMountPath(DestinationOrder order) throws Exception {
    Map<String, String> destMap = new HashMap<>();
    destMap.put("ns0", "/tmp");
    destMap.put("ns1", "/tmp");
    nnFs0.mkdirs(new Path("/tmp"));
    nnFs1.mkdirs(new Path("/tmp"));
    MountTable addEntry = MountTable.newInstance("/mount", destMap);
    addEntry.setDestOrder(order);
    assertTrue(addMountTable(addEntry));
    routerFs.mkdirs(new Path("/mount/dir/dir"));
    DFSTestUtil.createFile(routerFs, new Path("/mount/dir/file"), 100L, (short) 1,
        1024L);
    DFSTestUtil.createFile(routerFs, new Path("/mount/file"), 100L, (short) 1,
        1024L);
  }

  @After
  public void resetTestEnvironment() throws IOException {
    RouterClient client = routerContext.getAdminClient();
    MountTableManager mountTableManager = client.getMountTableManager();
    RemoveMountTableEntryRequest req2 =
        RemoveMountTableEntryRequest.newInstance("/mount");
    mountTableManager.removeMountTableEntry(req2);
    nnFs0.delete(new Path("/tmp"), true);
    nnFs1.delete(new Path("/tmp"), true);

  }

  @Test
  public void testInvocationSpaceOrder() throws Exception {
    setupOrderMountPath(DestinationOrder.SPACE);
    boolean isDirAll = rpcServer.isPathAll("/mount/dir");
    assertTrue(isDirAll);
    testInvocation(isDirAll);
  }

  @Test
  public void testInvocationHashAllOrder() throws Exception {
    setupOrderMountPath(DestinationOrder.HASH_ALL);
    boolean isDirAll = rpcServer.isPathAll("/mount/dir");
    assertTrue(isDirAll);
    testInvocation(isDirAll);
  }

  @Test
  public void testInvocationRandomOrder() throws Exception {
    setupOrderMountPath(DestinationOrder.RANDOM);
    boolean isDirAll = rpcServer.isPathAll("/mount/dir");
    assertTrue(isDirAll);
    testInvocation(isDirAll);
  }

  @Test
  public void testInvocationHashOrder() throws Exception {
    setupOrderMountPath(DestinationOrder.HASH);
    boolean isDirAll = rpcServer.isPathAll("/mount/dir");
    assertFalse(isDirAll);
    testInvocation(isDirAll);
  }

  @Test
  public void testInvocationLocalOrder() throws Exception {
    setupOrderMountPath(DestinationOrder.LOCAL);
    boolean isDirAll = rpcServer.isPathAll("/mount/dir");
    assertFalse(isDirAll);
    testInvocation(isDirAll);
  }

  /**
   * Verifies the invocation of API's at directory level , file level and at
   * mount level.
   * @param dirAll if true assumes that the mount entry creates directory on all
   *          locations.
   * @throws IOException
   */
  private void testInvocation(boolean dirAll) throws IOException {
    // Verify invocation on nested directory and file.
    Path mountDir = new Path("/mount/dir/dir");
    Path nameSpaceFile = new Path("/tmp/dir/file");
    Path mountFile = new Path("/mount/dir/file");
    Path mountEntry = new Path("/mount");
    Path mountDest = new Path("/tmp");
    Path nameSpaceDir = new Path("/tmp/dir/dir");
    final String name = "user.a1";
    final byte[] value = {0x31, 0x32, 0x33};
    testDirectoryAndFileLevelInvocation(dirAll, mountDir, nameSpaceFile,
        mountFile, nameSpaceDir, name, value);

    // Verify invocation on non nested directory and file.
    mountDir = new Path("/mount/dir");
    nameSpaceFile = new Path("/tmp/file");
    mountFile = new Path("/mount/file");
    nameSpaceDir = new Path("/tmp/dir");
    testDirectoryAndFileLevelInvocation(dirAll, mountDir, nameSpaceFile,
        mountFile, nameSpaceDir, name, value);

    // Check invocation directly for a mount point.
    // Verify owner and permissions.
    routerFs.setOwner(mountEntry, "testuser", "testgroup");
    routerFs.setPermission(mountEntry,
        FsPermission.createImmutable((short) 777));
    assertEquals("testuser", routerFs.getFileStatus(mountEntry).getOwner());
    assertEquals("testuser", nnFs0.getFileStatus(mountDest).getOwner());
    assertEquals("testuser", nnFs1.getFileStatus(mountDest).getOwner());
    assertEquals((short) 777,
        routerFs.getFileStatus(mountEntry).getPermission().toShort());
    assertEquals((short) 777,
        nnFs0.getFileStatus(mountDest).getPermission().toShort());
    assertEquals((short) 777,
        nnFs1.getFileStatus(mountDest).getPermission().toShort());

    //Verify storage policy.
    routerFs.setStoragePolicy(mountEntry, "COLD");
    assertEquals("COLD", routerFs.getStoragePolicy(mountEntry).getName());
    assertEquals("COLD", nnFs0.getStoragePolicy(mountDest).getName());
    assertEquals("COLD", nnFs1.getStoragePolicy(mountDest).getName());
    routerFs.unsetStoragePolicy(mountEntry);
    assertEquals("HOT", routerFs.getStoragePolicy(mountDest).getName());
    assertEquals("HOT", nnFs0.getStoragePolicy(mountDest).getName());
    assertEquals("HOT", nnFs1.getStoragePolicy(mountDest).getName());

    //Verify erasure coding policy.
    routerFs.setErasureCodingPolicy(mountEntry, "RS-6-3-1024k");
    assertEquals("RS-6-3-1024k",
        routerFs.getErasureCodingPolicy(mountEntry).getName());
    assertEquals("RS-6-3-1024k",
        nnFs0.getErasureCodingPolicy(mountDest).getName());
    assertEquals("RS-6-3-1024k",
        nnFs1.getErasureCodingPolicy(mountDest).getName());
    routerFs.unsetErasureCodingPolicy(mountEntry);
    assertNull(routerFs.getErasureCodingPolicy(mountDest));
    assertNull(nnFs0.getErasureCodingPolicy(mountDest));
    assertNull(nnFs1.getErasureCodingPolicy(mountDest));

    //Verify xAttr.
    routerFs.setXAttr(mountEntry, name, value);
    assertArrayEquals(value, routerFs.getXAttr(mountEntry, name));
    assertArrayEquals(value, nnFs0.getXAttr(mountDest, name));
    assertArrayEquals(value, nnFs1.getXAttr(mountDest, name));
    routerFs.removeXAttr(mountEntry, name);
    assertEquals(0, routerFs.getXAttrs(mountEntry).size());
    assertEquals(0, nnFs0.getXAttrs(mountDest).size());
    assertEquals(0, nnFs1.getXAttrs(mountDest).size());
  }

  /**
   * SetUp to verify invocations on directories and file.
   */
  private void testDirectoryAndFileLevelInvocation(boolean dirAll,
      Path mountDir, Path nameSpaceFile, Path mountFile, Path nameSpaceDir,
      final String name, final byte[] value) throws IOException {
    // Check invocation for a directory.
    routerFs.setOwner(mountDir, "testuser", "testgroup");
    routerFs.setPermission(mountDir, FsPermission.createImmutable((short) 777));
    routerFs.setStoragePolicy(mountDir, "COLD");
    routerFs.setErasureCodingPolicy(mountDir, "RS-6-3-1024k");
    routerFs.setXAttr(mountDir, name, value);

    // Verify the directory level invocations were checked in case of mounts not
    // creating directories in all subclusters.
    boolean checkedDir1 = verifyDirectoryLevelInvocations(dirAll, nameSpaceDir,
        nnFs0, name, value);
    boolean checkedDir2 = verifyDirectoryLevelInvocations(dirAll, nameSpaceDir,
        nnFs1, name, value);
    assertTrue("The file didn't existed in either of the subclusters.",
        checkedDir1 || checkedDir2);
    routerFs.unsetStoragePolicy(mountDir);
    routerFs.removeXAttr(mountDir, name);
    routerFs.unsetErasureCodingPolicy(mountDir);

    checkedDir1 =
        verifyDirectoryLevelUnsetInvocations(dirAll, nnFs0, nameSpaceDir);
    checkedDir2 =
        verifyDirectoryLevelUnsetInvocations(dirAll, nnFs1, nameSpaceDir);
    assertTrue("The file didn't existed in either of the subclusters.",
        checkedDir1 || checkedDir2);

    // Check invocation for a file.
    routerFs.setOwner(mountFile, "testuser", "testgroup");
    routerFs.setPermission(mountFile,
        FsPermission.createImmutable((short) 777));
    routerFs.setStoragePolicy(mountFile, "COLD");
    routerFs.setReplication(mountFile, (short) 2);
    routerFs.setXAttr(mountFile, name, value);
    verifyFileLevelInvocations(nameSpaceFile, nnFs0, mountFile, name, value);
    verifyFileLevelInvocations(nameSpaceFile, nnFs1, mountFile, name, value);
  }

  /**
   * Verify invocations of API's unseting values at the directory level.
   * @param dirAll true if the mount entry order creates directory in all
   *          locations.
   * @param nameSpaceDir path of the directory in the namespace.
   * @param nnFs file system where the directory level invocation needs to be
   *          tested.
   * @throws IOException
   */
  private boolean verifyDirectoryLevelUnsetInvocations(boolean dirAll,
      DistributedFileSystem nnFs, Path nameSpaceDir) throws IOException {
    boolean checked = false;
    if (dirAll || nnFs.exists(nameSpaceDir)) {
      checked = true;
      assertEquals("HOT", nnFs.getStoragePolicy(nameSpaceDir).getName());
      assertNull(nnFs.getErasureCodingPolicy(nameSpaceDir));
      assertEquals(0, nnFs.getXAttrs(nameSpaceDir).size());
    }
    return checked;
  }

  /**
   * Verify file level invocations.
   * @param nameSpaceFile path of the file in the namespace.
   * @param nnFs the file system where the file invocation needs to checked.
   * @param mountFile path of the file w.r.t. mount table.
   * @param name name of Xattr.
   * @param value value of Xattr.
   * @throws IOException
   */
  private void verifyFileLevelInvocations(Path nameSpaceFile,
      DistributedFileSystem nnFs, Path mountFile, final String name,
      final byte[] value) throws IOException {
    if (nnFs.exists(nameSpaceFile)) {
      assertEquals("testuser", nnFs.getFileStatus(nameSpaceFile).getOwner());
      assertEquals((short) 777,
          nnFs.getFileStatus(nameSpaceFile).getPermission().toShort());
      assertEquals("COLD", nnFs.getStoragePolicy(nameSpaceFile).getName());
      assertEquals((short) 2,
          nnFs.getFileStatus(nameSpaceFile).getReplication());
      assertArrayEquals(value, nnFs.getXAttr(nameSpaceFile, name));

      routerFs.unsetStoragePolicy(mountFile);
      routerFs.removeXAttr(mountFile, name);
      assertEquals(0, nnFs.getXAttrs(nameSpaceFile).size());

      assertEquals("HOT", nnFs.getStoragePolicy(nameSpaceFile).getName());

    }
  }

  /**
   * Verify invocations at the directory level.
   * @param dirAll true if the mount entry order creates directory in all
   *          locations.
   * @param nameSpaceDir path of the directory in the namespace.
   * @param nnFs file system where the directory level invocation needs to be
   *          tested.
   * @param name name for the Xattr.
   * @param value value for the Xattr.
   * @return true, if directory existed and successful verification of
   *         invocations.
   * @throws IOException
   */
  private boolean verifyDirectoryLevelInvocations(boolean dirAll,
      Path nameSpaceDir, DistributedFileSystem nnFs, final String name,
      final byte[] value) throws IOException {
    boolean checked = false;
    if (dirAll || nnFs.exists(nameSpaceDir)) {
      checked = true;
      assertEquals("testuser", nnFs.getFileStatus(nameSpaceDir).getOwner());
      assertEquals("COLD", nnFs.getStoragePolicy(nameSpaceDir).getName());
      assertEquals("RS-6-3-1024k",
          nnFs.getErasureCodingPolicy(nameSpaceDir).getName());
      assertArrayEquals(value, nnFs.getXAttr(nameSpaceDir, name));
      assertEquals((short) 777,
          nnFs.getFileStatus(nameSpaceDir).getPermission().toShort());
    }
    return checked;
  }

  /**
   * Add a mount table entry to the mount table through the admin API.
   * @param entry Mount table entry to add.
   * @return If it was successfully added.
   * @throws IOException + * Problems adding entries.
   */
  private boolean addMountTable(final MountTable entry) throws IOException {
    RouterClient client = routerContext.getAdminClient();
    MountTableManager mountTableManager = client.getMountTableManager();
    AddMountTableEntryRequest addRequest =
        AddMountTableEntryRequest.newInstance(entry);
    AddMountTableEntryResponse addResponse =
        mountTableManager.addMountTableEntry(addRequest);

    // Reload the Router cache
    resolver.loadCache(true);

    return addResponse.getStatus();
  }

  @Test
  public void testECMultipleDestinations() throws Exception {
    setupOrderMountPath(DestinationOrder.HASH_ALL);
    Path mountPath = new Path("/mount/dir");
    routerFs.setErasureCodingPolicy(mountPath, "RS-6-3-1024k");
    assertTrue(routerFs.getFileStatus(mountPath).isErasureCoded());
  }

  @Test
  public void testACLMultipleDestinations() throws Exception {
    setupOrderMountPath(DestinationOrder.HASH_ALL);
    Path mountPath = new Path("/mount/dir/dir");
    Path nsPath = new Path("/tmp/dir/dir");
    List<AclEntry> aclSpec = Collections.singletonList(
        AclEntry.parseAclEntry("default:USER:TestUser:rwx", true));
    routerFs.setAcl(mountPath, aclSpec);
    assertEquals(5, nnFs0.getAclStatus(nsPath).getEntries().size());
    assertEquals(5, nnFs1.getAclStatus(nsPath).getEntries().size());
    aclSpec = Collections
        .singletonList(AclEntry.parseAclEntry("USER:User:rwx::", true));

    routerFs.modifyAclEntries(mountPath, aclSpec);
    assertEquals(7, nnFs0.getAclStatus(nsPath).getEntries().size());
    assertEquals(7, nnFs1.getAclStatus(nsPath).getEntries().size());

    routerFs.removeAclEntries(mountPath, aclSpec);
    assertEquals(6, nnFs0.getAclStatus(nsPath).getEntries().size());
    assertEquals(6, nnFs1.getAclStatus(nsPath).getEntries().size());

    routerFs.modifyAclEntries(mountPath, aclSpec);
    routerFs.removeDefaultAcl(mountPath);
    assertEquals(2, nnFs0.getAclStatus(nsPath).getEntries().size());
    assertEquals(2, nnFs1.getAclStatus(nsPath).getEntries().size());

    routerFs.removeAcl(mountPath);
    assertEquals(0, nnFs0.getAclStatus(nsPath).getEntries().size());
    assertEquals(0, nnFs1.getAclStatus(nsPath).getEntries().size());

  }

  @Test
  public void testGetDestinationHashAll() throws Exception {
    testGetDestination(DestinationOrder.HASH_ALL,
        Arrays.asList("ns1"),
        Arrays.asList("ns1"),
        Arrays.asList("ns1", "ns0"));
  }

  @Test
  public void testGetDestinationHash() throws Exception {
    testGetDestination(DestinationOrder.HASH,
        Arrays.asList("ns1"),
        Arrays.asList("ns1"),
        Arrays.asList("ns1"));
  }

  @Test
  public void testGetDestinationRandom() throws Exception {
    testGetDestination(DestinationOrder.RANDOM,
        null, null, Arrays.asList("ns0", "ns1"));
  }

  @Test
  public void testIsMultiDestDir() throws Exception {
    RouterClientProtocol client =
        routerContext.getRouter().getRpcServer().getClientProtocolModule();
    setupOrderMountPath(DestinationOrder.HASH_ALL);
    // Should be true only for directory and false for all other cases.
    assertTrue(client.isMultiDestDirectory("/mount/dir"));
    assertFalse(client.isMultiDestDirectory("/mount/nodir"));
    assertFalse(client.isMultiDestDirectory("/mount/dir/file"));
    routerFs.createSymlink(new Path("/mount/dir/file"),
        new Path("/mount/dir/link"), true);
    assertFalse(client.isMultiDestDirectory("/mount/dir/link"));
    routerFs.createSymlink(new Path("/mount/dir/dir"),
        new Path("/mount/dir/linkDir"), true);
    assertFalse(client.isMultiDestDirectory("/mount/dir/linkDir"));
    resetTestEnvironment();
    // Test single directory destination. Should be false for the directory.
    setupOrderMountPath(DestinationOrder.HASH);
    assertFalse(client.isMultiDestDirectory("/mount/dir"));
  }

  /**
   * Verifies the snapshot location returned after snapshot operations is in
   * accordance to the mount path.
   */
  @Test
  public void testSnapshotPathResolution() throws Exception {
    // Create a mount entry with non isPathAll order, to call
    // invokeSequential.
    Map<String, String> destMap = new HashMap<>();
    destMap.put("ns0", "/tmp_ns0");
    destMap.put("ns1", "/tmp_ns1");
    nnFs0.mkdirs(new Path("/tmp_ns0"));
    nnFs1.mkdirs(new Path("/tmp_ns1"));
    MountTable addEntry = MountTable.newInstance("/mountSnap", destMap);
    addEntry.setDestOrder(DestinationOrder.HASH);
    assertTrue(addMountTable(addEntry));
    // Create the actual directory in the destination second in sequence of
    // invokeSequential.
    nnFs0.mkdirs(new Path("/tmp_ns0/snapDir"));
    Path snapDir = new Path("/mountSnap/snapDir");
    Path snapshotPath = new Path("/mountSnap/snapDir/.snapshot/snap");
    routerFs.allowSnapshot(snapDir);
    // Verify the snapshot path returned after createSnapshot is as per mount
    // path.
    Path snapshot = routerFs.createSnapshot(snapDir, "snap");
    assertEquals(snapshotPath, snapshot);
    // Verify the snapshot path returned as part of snapshotListing is as per
    // mount path.
    SnapshotStatus[] snapshots = routerFs.getSnapshotListing(snapDir);
    assertEquals(snapshotPath, snapshots[0].getFullPath());
  }

  @Test
  public void testRenameMultipleDestDirectories() throws Exception {
    // Test renaming directories using rename API.
    verifyRenameOnMultiDestDirectories(DestinationOrder.HASH_ALL, false);
    resetTestEnvironment();
    verifyRenameOnMultiDestDirectories(DestinationOrder.RANDOM, false);
    resetTestEnvironment();
    verifyRenameOnMultiDestDirectories(DestinationOrder.SPACE, false);
    resetTestEnvironment();
    // Test renaming directories using rename2 API.
    verifyRenameOnMultiDestDirectories(DestinationOrder.HASH_ALL, true);
    resetTestEnvironment();
    verifyRenameOnMultiDestDirectories(DestinationOrder.RANDOM, true);
    resetTestEnvironment();
    verifyRenameOnMultiDestDirectories(DestinationOrder.SPACE, true);
  }

  @Test
  public void testClearQuota() throws Exception {
    long nsQuota = 5;
    long ssQuota = 100;
    Path path = new Path("/router_test");
    nnFs0.mkdirs(path);
    nnFs1.mkdirs(path);
    MountTable addEntry = MountTable.newInstance("/router_test",
        Collections.singletonMap("ns0", "/router_test"));
    addEntry.setQuota(new RouterQuotaUsage.Builder().build());
    assertTrue(addMountTable(addEntry));
    RouterQuotaUpdateService updateService =
        routerContext.getRouter().getQuotaCacheUpdateService();
    updateService.periodicInvoke();

    //set quota and validate the quota
    RouterAdmin admin = getRouterAdmin();
    String[] argv = new String[] {"-setQuota", path.toString(), "-nsQuota",
        String.valueOf(nsQuota), "-ssQuota", String.valueOf(ssQuota)};
    assertEquals(0, ToolRunner.run(admin, argv));
    updateService.periodicInvoke();
    resolver.loadCache(true);
    ContentSummary cs = routerFs.getContentSummary(path);
    assertEquals(nsQuota, cs.getQuota());
    assertEquals(ssQuota, cs.getSpaceQuota());

    //clear quota and validate the quota
    argv = new String[] {"-clrQuota", path.toString()};
    assertEquals(0, ToolRunner.run(admin, argv));
    updateService.periodicInvoke();
    resolver.loadCache(true);
    //quota should be cleared
    ContentSummary cs1 = routerFs.getContentSummary(path);
    assertEquals(-1, cs1.getQuota());
    assertEquals(-1, cs1.getSpaceQuota());
  }

  @Test
  public void testContentSummaryWithMultipleDest() throws Exception {
    MountTable addEntry;
    long nsQuota = 5;
    long ssQuota = 100;
    Path path = new Path("/testContentSummaryWithMultipleDest");
    Map<String, String> destMap = new HashMap<>();
    destMap.put("ns0", "/testContentSummaryWithMultipleDest");
    destMap.put("ns1", "/testContentSummaryWithMultipleDest");
    nnFs0.mkdirs(path);
    nnFs1.mkdirs(path);
    addEntry =
        MountTable.newInstance("/testContentSummaryWithMultipleDest", destMap);
    addEntry.setQuota(
        new RouterQuotaUsage.Builder().quota(nsQuota).spaceQuota(ssQuota)
            .build());
    assertTrue(addMountTable(addEntry));
    RouterQuotaUpdateService updateService =
        routerContext.getRouter().getQuotaCacheUpdateService();
    updateService.periodicInvoke();
    ContentSummary cs = routerFs.getContentSummary(path);
    assertEquals(nsQuota, cs.getQuota());
    assertEquals(ssQuota, cs.getSpaceQuota());
    ContentSummary ns0Cs = nnFs0.getContentSummary(path);
    assertEquals(nsQuota, ns0Cs.getQuota());
    assertEquals(ssQuota, ns0Cs.getSpaceQuota());
    ContentSummary ns1Cs = nnFs1.getContentSummary(path);
    assertEquals(nsQuota, ns1Cs.getQuota());
    assertEquals(ssQuota, ns1Cs.getSpaceQuota());
  }

  @Test
  public void testContentSummaryMultipleDestWithMaxValue()
      throws Exception {
    MountTable addEntry;
    long nsQuota = Long.MAX_VALUE - 2;
    long ssQuota = Long.MAX_VALUE - 2;
    Path path = new Path("/testContentSummaryMultipleDestWithMaxValue");
    Map<String, String> destMap = new HashMap<>();
    destMap.put("ns0", "/testContentSummaryMultipleDestWithMaxValue");
    destMap.put("ns1", "/testContentSummaryMultipleDestWithMaxValue");
    nnFs0.mkdirs(path);
    nnFs1.mkdirs(path);
    addEntry = MountTable
        .newInstance("/testContentSummaryMultipleDestWithMaxValue", destMap);
    addEntry.setQuota(
        new RouterQuotaUsage.Builder().quota(nsQuota).spaceQuota(ssQuota)
            .build());
    assertTrue(addMountTable(addEntry));
    RouterQuotaUpdateService updateService =
        routerContext.getRouter().getQuotaCacheUpdateService();
    updateService.periodicInvoke();
    ContentSummary cs = routerFs.getContentSummary(path);
    assertEquals(nsQuota, cs.getQuota());
    assertEquals(ssQuota, cs.getSpaceQuota());
  }

  /**
   * Test RouterRpcServer#invokeAtAvailableNs on mount point with multiple destinations
   * and making a one of the destination's subcluster unavailable.
   */
  @Test
  public void testInvokeAtAvailableNs() throws IOException {
    // Create a mount point with multiple destinations.
    Path path = new Path("/testInvokeAtAvailableNs");
    Map<String, String> destMap = new HashMap<>();
    destMap.put("ns0", "/testInvokeAtAvailableNs");
    destMap.put("ns1", "/testInvokeAtAvailableNs");
    nnFs0.mkdirs(path);
    nnFs1.mkdirs(path);
    MountTable addEntry =
        MountTable.newInstance("/testInvokeAtAvailableNs", destMap);
    addEntry.setQuota(new RouterQuotaUsage.Builder().build());
    addEntry.setDestOrder(DestinationOrder.RANDOM);
    addEntry.setFaultTolerant(true);
    assertTrue(addMountTable(addEntry));

    // Make one subcluster unavailable.
    MiniDFSCluster dfsCluster = cluster.getCluster();
    dfsCluster.shutdownNameNode(0);
    dfsCluster.shutdownNameNode(1);
    try {
      // Verify that #invokeAtAvailableNs works by calling #getServerDefaults.
      RemoteMethod method = new RemoteMethod("getServerDefaults");
      FsServerDefaults serverDefaults =
          rpcServer.invokeAtAvailableNs(method, FsServerDefaults.class);
      assertNotNull(serverDefaults);
    } finally {
      dfsCluster.restartNameNode(0);
      dfsCluster.restartNameNode(1);
    }
  }

  /**
   * Test write on mount point with multiple destinations
   * and making a one of the destination's subcluster unavailable.
   */
  @Test
  public void testWriteWithUnavailableSubCluster() throws IOException {
    //create a mount point with multiple destinations
    Path path = new Path("/testWriteWithUnavailableSubCluster");
    Map<String, String> destMap = new HashMap<>();
    destMap.put("ns0", "/testWriteWithUnavailableSubCluster");
    destMap.put("ns1", "/testWriteWithUnavailableSubCluster");
    nnFs0.mkdirs(path);
    nnFs1.mkdirs(path);
    MountTable addEntry =
        MountTable.newInstance("/testWriteWithUnavailableSubCluster", destMap);
    addEntry.setQuota(new RouterQuotaUsage.Builder().build());
    addEntry.setDestOrder(DestinationOrder.RANDOM);
    addEntry.setFaultTolerant(true);
    assertTrue(addMountTable(addEntry));

    //make one subcluster unavailable and perform write on mount point
    MiniDFSCluster dfsCluster = cluster.getCluster();
    dfsCluster.shutdownNameNode(0);
    FSDataOutputStream out = null;
    Path filePath = new Path(path, "aa");
    try {
      out = routerFs.create(filePath);
      out.write("hello".getBytes());
      out.hflush();
      assertTrue(routerFs.exists(filePath));
    } finally {
      IOUtils.closeStream(out);
      dfsCluster.restartNameNode(0);
    }
  }

  /**
   *  Test rename a dir from src dir (mapped to both ns0 and ns1) to ns0.
   */
  @Test
  public void testRenameWithMultiDestinations() throws Exception {
    //create a mount point with multiple destinations
    String srcDir = "/mount-source-dir";
    Path path = new Path(srcDir);
    Map<String, String> destMap = new HashMap<>();
    destMap.put("ns0", srcDir);
    destMap.put("ns1", srcDir);
    nnFs0.mkdirs(path);
    nnFs1.mkdirs(path);
    MountTable addEntry =
        MountTable.newInstance(srcDir, destMap);
    addEntry.setDestOrder(DestinationOrder.RANDOM);
    assertTrue(addMountTable(addEntry));

    //create a mount point with a single destinations ns0
    String targetDir = "/ns0_test";
    nnFs0.mkdirs(new Path(targetDir));
    MountTable addDstEntry = MountTable.newInstance(targetDir,
        Collections.singletonMap("ns0", targetDir));
    assertTrue(addMountTable(addDstEntry));

    //mkdir sub dirs in srcDir  mapping ns0 & ns1
    routerFs.mkdirs(new Path(srcDir + "/dir1"));
    routerFs.mkdirs(new Path(srcDir + "/dir1/dir_1"));
    routerFs.mkdirs(new Path(srcDir + "/dir1/dir_2"));
    routerFs.mkdirs(new Path(targetDir));

    //try to rename sub dir in srcDir (mapping to ns0 & ns1) to targetDir
    // (mapping ns0)
    LambdaTestUtils.intercept(IOException.class, "The number of" +
            " remote locations for both source and target should be same.",
        () -> {
          routerFs.rename(new Path(srcDir + "/dir1/dir_1"),
              new Path(targetDir));
        });
  }

  /**
   * Test to verify rename operation on directories in case of multiple
   * destinations.
   * @param order order to be followed by the mount entry.
   * @param isRename2 true if the verification is to be done using rename2(..)
   *          method.
   * @throws Exception on account of any exception during test execution.
   */
  private void verifyRenameOnMultiDestDirectories(DestinationOrder order,
      boolean isRename2) throws Exception {
    setupOrderMountPath(order);
    Path src = new Path("/mount/dir/dir");
    Path nnSrc = new Path("/tmp/dir/dir");
    Path dst = new Path("/mount/dir/subdir");
    Path nnDst = new Path("/tmp/dir/subdir");
    Path fileSrc = new Path("/mount/dir/dir/file");
    Path nnFileSrc = new Path("/tmp/dir/dir/file");
    Path fileDst = new Path("/mount/dir/subdir/file");
    Path nnFileDst = new Path("/tmp/dir/subdir/file");
    DFSTestUtil.createFile(routerFs, fileSrc, 100L, (short) 1, 1024L);
    if (isRename2) {
      routerFs.rename(src, dst, Rename.NONE);
    } else {
      assertTrue(routerFs.rename(src, dst));
    }
    assertTrue(nnFs0.exists(nnDst));
    assertTrue(nnFs1.exists(nnDst));
    assertFalse(nnFs0.exists(nnSrc));
    assertFalse(nnFs1.exists(nnSrc));
    assertFalse(routerFs.exists(fileSrc));
    assertTrue(routerFs.exists(fileDst));
    assertTrue(nnFs0.exists(nnFileDst) || nnFs1.exists(nnFileDst));
    assertFalse(nnFs0.exists(nnFileSrc) || nnFs1.exists(nnFileSrc));

    // Verify rename file.
    Path fileRenamed = new Path("/mount/dir/subdir/renamedFile");
    Path nnFileRenamed = new Path("/tmp/dir/subdir/renamedFile");
    if (isRename2) {
      routerFs.rename(fileDst, fileRenamed, Rename.NONE);
    } else {
      assertTrue(routerFs.rename(fileDst, fileRenamed));
    }
    assertTrue(routerFs.exists(fileRenamed));
    assertFalse(routerFs.exists(fileDst));
    assertTrue(nnFs0.exists(nnFileRenamed) || nnFs1.exists(nnFileRenamed));
    assertFalse(nnFs0.exists(nnFileDst) || nnFs1.exists(nnFileDst));

    // Verify rename when one source directory is not present.
    Path dst1 = new Path("/mount/dir/renameddir");
    Path nnDst1 = new Path("/tmp/dir/renameddir");
    nnFs1.delete(nnDst, true);
    if (isRename2) {
      routerFs.rename(dst, dst1, Rename.NONE);
    } else {
      assertTrue(routerFs.rename(dst, dst1));
    }
    assertTrue(nnFs0.exists(nnDst1));
    assertFalse(nnFs0.exists(nnDst));

    // Verify rename when one destination directory is already present.
    Path src1 = new Path("/mount/dir");
    Path dst2 = new Path("/mount/OneDest");
    Path nnDst2 = new Path("/tmp/OneDest");
    nnFs0.mkdirs(nnDst2);
    if (isRename2) {
      routerFs.rename(src1, dst2, Rename.NONE);
    } else {
      assertTrue(routerFs.rename(src1, dst2));
    }
    assertTrue(nnFs0.exists(nnDst2));
    assertTrue(nnFs1.exists(nnDst2));
  }

  /**
   * Generic test for getting the destination subcluster.
   * @param order DestinationOrder of the mount point.
   * @param expectFileLocation Expected subclusters of a file. null for any.
   * @param expectNoFileLocation Expected subclusters of a non-existing file.
   * @param expectDirLocation Expected subclusters of a nested directory.
   * @throws Exception If the test cannot run.
   */
  private void testGetDestination(DestinationOrder order,
      List<String> expectFileLocation,
      List<String> expectNoFileLocation,
      List<String> expectDirLocation) throws Exception {
    setupOrderMountPath(order);

    RouterClient client = routerContext.getAdminClient();
    MountTableManager mountTableManager = client.getMountTableManager();

    // If the file exists, it should be in the expected subcluster
    final String pathFile = "dir/file";
    final Path pathRouterFile = new Path("/mount", pathFile);
    final Path pathLocalFile = new Path("/tmp", pathFile);
    FileStatus fileStatus = routerFs.getFileStatus(pathRouterFile);
    assertTrue(fileStatus + " should be a file", fileStatus.isFile());
    GetDestinationResponse respFile = mountTableManager.getDestination(
        GetDestinationRequest.newInstance(pathRouterFile));
    if (expectFileLocation != null) {
      assertEquals(expectFileLocation, respFile.getDestinations());
      assertPathStatus(expectFileLocation, pathLocalFile, false);
    } else {
      Collection<String> dests = respFile.getDestinations();
      assertPathStatus(dests, pathLocalFile, false);
    }

    // If the file does not exist, it should give us the expected subclusters
    final String pathNoFile = "dir/no-file";
    final Path pathRouterNoFile = new Path("/mount", pathNoFile);
    final Path pathLocalNoFile = new Path("/tmp", pathNoFile);
    LambdaTestUtils.intercept(FileNotFoundException.class,
        () -> routerFs.getFileStatus(pathRouterNoFile));
    GetDestinationResponse respNoFile = mountTableManager.getDestination(
        GetDestinationRequest.newInstance(pathRouterNoFile));
    if (expectNoFileLocation != null) {
      assertEquals(expectNoFileLocation, respNoFile.getDestinations());
    }
    assertPathStatus(Collections.emptyList(), pathLocalNoFile, false);

    // If the folder exists, it should be in the expected subcluster
    final String pathNestedDir = "dir/dir";
    final Path pathRouterNestedDir = new Path("/mount", pathNestedDir);
    final Path pathLocalNestedDir = new Path("/tmp", pathNestedDir);
    FileStatus dirStatus = routerFs.getFileStatus(pathRouterNestedDir);
    assertTrue(dirStatus + " should be a directory", dirStatus.isDirectory());
    GetDestinationResponse respDir = mountTableManager.getDestination(
        GetDestinationRequest.newInstance(pathRouterNestedDir));
    assertEqualsCollection(expectDirLocation, respDir.getDestinations());
    assertPathStatus(expectDirLocation, pathLocalNestedDir, true);
  }

  /**
   * Assert that the status of a file in the subcluster is the expected one.
   * @param expectedLocations Subclusters where the file is expected to exist.
   * @param path Path of the file/directory to check.
   * @param isDir If the path is expected to be a directory.
   * @throws Exception If the file cannot be checked.
   */
  private void assertPathStatus(Collection<String> expectedLocations,
      Path path, boolean isDir) throws Exception {
    for (String nsId : NS_IDS) {
      final FileSystem fs = getFileSystem(nsId);
      if (expectedLocations.contains(nsId)) {
        assertTrue(path + " should exist in " + nsId, fs.exists(path));
        final FileStatus status = fs.getFileStatus(path);
        if (isDir) {
          assertTrue(path + " should be a directory", status.isDirectory());
        } else {
          assertTrue(path + " should be a file", status.isFile());
        }
      } else {
        assertFalse(path + " should not exist in " + nsId, fs.exists(path));
      }
    }
  }

  /**
   * Assert if two collections are equal without checking the order.
   * @param col1 First collection to compare.
   * @param col2 Second collection to compare.
   */
  private static void assertEqualsCollection(
      Collection<String> col1, Collection<String> col2) {
    assertEquals(new TreeSet<>(col1), new TreeSet<>(col2));
  }

  /**
   * Get the filesystem for each subcluster.
   * @param nsId Identifier of the name space (subcluster).
   * @return The FileSystem for
   */
  private static FileSystem getFileSystem(final String nsId) {
    if (nsId.equals("ns0")) {
      return nnFs0;
    }
    if (nsId.equals("ns1")) {
      return nnFs1;
    }
    if (nsId.equals("ns2")) {
      return nnFs2;
    }
    return null;
  }

  private RouterAdmin getRouterAdmin() {
    Router router = routerContext.getRouter();
    Configuration configuration = routerContext.getConf();
    InetSocketAddress routerSocket = router.getAdminServerAddress();
    configuration.setSocketAddr(RBFConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_KEY,
        routerSocket);
    return new RouterAdmin(configuration);
  }
}