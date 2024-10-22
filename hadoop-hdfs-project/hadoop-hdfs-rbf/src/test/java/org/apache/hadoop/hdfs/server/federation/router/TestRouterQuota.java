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

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;
import java.util.Arrays;
import java.util.function.Predicate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.QuotaByStorageTypeExceededException;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.NamenodeContext;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.hdfs.server.federation.store.records.QueryResult;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests quota behaviors in Router-based Federation.
 */
public class TestRouterQuota {
  private static StateStoreDFSCluster cluster;
  private static NamenodeContext nnContext1;
  private static NamenodeContext nnContext2;
  private static RouterContext routerContext;
  private static MountTableResolver resolver;

  private static final int BLOCK_SIZE = 512;

  @Before
  public void setUp() throws Exception {

    // Build and start a federated cluster
    cluster = new StateStoreDFSCluster(false, 2);
    Configuration routerConf = new RouterConfigBuilder()
        .stateStore()
        .admin()
        .quota()
        .rpc()
        .build();
    routerConf.set(RBFConfigKeys.DFS_ROUTER_QUOTA_CACHE_UPDATE_INTERVAL, "2s");

    // override some hdfs settings that used in testing space quota
    Configuration hdfsConf = new Configuration(false);
    hdfsConf.setInt(HdfsClientConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    hdfsConf.setInt(HdfsClientConfigKeys.DFS_REPLICATION_KEY, 1);

    cluster.addRouterOverrides(routerConf);
    cluster.addNamenodeOverrides(hdfsConf);
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp();

    nnContext1 = cluster.getNamenode(cluster.getNameservices().get(0), null);
    nnContext2 = cluster.getNamenode(cluster.getNameservices().get(1), null);
    routerContext = cluster.getRandomRouter();
    Router router = routerContext.getRouter();
    resolver = (MountTableResolver) router.getSubclusterResolver();
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.stopRouter(routerContext);
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testNamespaceQuotaExceed() throws Exception {
    long nsQuota = 4;
    final FileSystem nnFs1 = nnContext1.getFileSystem();
    final FileSystem nnFs2 = nnContext2.getFileSystem();

    // Add two mount tables:
    // /nsquota --> ns0---testdir1
    // /nsquota/subdir --> ns1---testdir2
    nnFs1.mkdirs(new Path("/testdir1"));
    nnFs2.mkdirs(new Path("/testdir2"));
    MountTable mountTable1 = MountTable.newInstance("/nsquota",
        Collections.singletonMap("ns0", "/testdir1"));

    mountTable1.setQuota(new RouterQuotaUsage.Builder().quota(nsQuota).build());
    addMountTable(mountTable1);

    MountTable mountTable2 = MountTable.newInstance("/nsquota/subdir",
        Collections.singletonMap("ns1", "/testdir2"));
    mountTable2.setQuota(new RouterQuotaUsage.Builder().quota(nsQuota).build());
    addMountTable(mountTable2);

    final FileSystem routerFs = routerContext.getFileSystem();
    final List<Path> created = new ArrayList<>();
    GenericTestUtils.waitFor(() -> {
      boolean isNsQuotaViolated = false;
      try {
        // create new directory to trigger NSQuotaExceededException
        Path p = new Path("/nsquota/" + UUID.randomUUID());
        routerFs.mkdirs(p);
        created.add(p);
        p = new Path("/nsquota/subdir/" + UUID.randomUUID());
        routerFs.mkdirs(p);
        created.add(p);
      } catch (NSQuotaExceededException e) {
        isNsQuotaViolated = true;
      } catch (IOException ignored) {
      }
      return isNsQuotaViolated;
    }, 5000, 60000);
    QuotaUsage quota = routerFs.getQuotaUsage(new Path("/nsquota"));
    assertEquals(quota.getQuota(), quota.getFileAndDirectoryCount());

    // mkdir in real FileSystem should be okay
    nnFs1.mkdirs(new Path("/testdir1/" + UUID.randomUUID()));
    nnFs2.mkdirs(new Path("/testdir2/" + UUID.randomUUID()));

    // rename/delete call should be still okay
    assertFalse(created.isEmpty());
    for(Path src: created) {
      final Path dst = new Path(src.toString()+"-renamed");
      routerFs.rename(src, dst);
      routerFs.delete(dst, true);
    }
  }

  @Test
  public void testStorageSpaceQuotaExceed() throws Exception {
    long ssQuota = 3072;
    final FileSystem nnFs1 = nnContext1.getFileSystem();
    final FileSystem nnFs2 = nnContext2.getFileSystem();

    // Add two mount tables:
    // /ssquota --> ns0---testdir3
    // /ssquota/subdir --> ns1---testdir4
    nnFs1.mkdirs(new Path("/testdir3"));
    nnFs2.mkdirs(new Path("/testdir4"));
    MountTable mountTable1 = MountTable.newInstance("/ssquota",
        Collections.singletonMap("ns0", "/testdir3"));

    mountTable1
        .setQuota(new RouterQuotaUsage.Builder().spaceQuota(ssQuota).build());
    addMountTable(mountTable1);

    MountTable mountTable2 = MountTable.newInstance("/ssquota/subdir",
        Collections.singletonMap("ns1", "/testdir4"));
    mountTable2
        .setQuota(new RouterQuotaUsage.Builder().spaceQuota(ssQuota).build());
    addMountTable(mountTable2);

    DFSClient routerClient = routerContext.getClient();
    routerClient.create("/ssquota/file", true).close();
    routerClient.create("/ssquota/subdir/file", true).close();

    GenericTestUtils.waitFor(() -> {
      boolean isDsQuotaViolated = false;
      try {
        // append data to trigger NSQuotaExceededException
        appendData("/ssquota/file", routerClient, BLOCK_SIZE);
        appendData("/ssquota/subdir/file", routerClient, BLOCK_SIZE);
      } catch (DSQuotaExceededException e) {
        isDsQuotaViolated = true;
      } catch (IOException ignored) {
      }
      return isDsQuotaViolated;
    }, 5000, 60000);

    QuotaUsage quota = routerContext.getFileSystem().getQuotaUsage(new Path("/ssquota"));
    assertEquals(quota.getSpaceQuota(), quota.getSpaceConsumed());

    // append data to destination path in real FileSystem should be okay
    appendData("/testdir3/file", nnContext1.getClient(), BLOCK_SIZE);
    appendData("/testdir4/file", nnContext2.getClient(), BLOCK_SIZE);
  }

  @Test
  public void testStorageTypeQuotaExceed() throws Exception {
    long ssQuota = BLOCK_SIZE * 3;
    DFSClient routerClient = routerContext.getClient();
    prepareStorageTypeQuotaTestMountTable(StorageType.DISK, BLOCK_SIZE,
        ssQuota * 2, ssQuota, BLOCK_SIZE + 1, BLOCK_SIZE + 1);

    // Verify quota exceed on Router.
    LambdaTestUtils.intercept(DSQuotaExceededException.class,
        "The DiskSpace quota is exceeded", "Expect quota exceed exception.",
        () -> appendData("/type0/file", routerClient, BLOCK_SIZE));
    LambdaTestUtils.intercept(DSQuotaExceededException.class,
        "The DiskSpace quota is exceeded", "Expect quota exceed exception.",
        () -> appendData("/type0/type1/file", routerClient, BLOCK_SIZE));

    // Verify quota exceed on NN1.
    LambdaTestUtils.intercept(QuotaByStorageTypeExceededException.class,
        "Quota by storage type", "Expect quota exceed exception.",
        () -> appendData("/type0/file", nnContext1.getClient(), BLOCK_SIZE));
    LambdaTestUtils.intercept(QuotaByStorageTypeExceededException.class,
        "Quota by storage type", "Expect quota exceed exception.",
        () -> appendData("/type1/file", nnContext1.getClient(), BLOCK_SIZE));
  }

  /**
   * Add a mount table entry to the mount table through the admin API.
   * @param entry Mount table entry to add.
   * @return If it was successfully added.
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
    resolver.loadCache(true);

    return addResponse.getStatus();
  }

  /**
   * Update a mount table entry to the mount table through the admin API.
   * @param entry Mount table entry to update.
   * @return If it was successfully updated
   * @throws IOException Problems update entries
   */
  private boolean updateMountTable(final MountTable entry) throws IOException {
    RouterClient client = routerContext.getAdminClient();
    MountTableManager mountTableManager = client.getMountTableManager();
    UpdateMountTableEntryRequest updateRequest =
        UpdateMountTableEntryRequest.newInstance(entry);
    UpdateMountTableEntryResponse updateResponse =
        mountTableManager.updateMountTableEntry(updateRequest);

    // Reload the Router cache
    resolver.loadCache(true);
    return updateResponse.getStatus();
  }

  /**
   * Append data in specified file.
   * @param path Path of file.
   * @param client DFS Client.
   * @param dataLen The length of write data.
   * @throws IOException
   */
  private void appendData(String path, DFSClient client, int dataLen)
      throws IOException {
    EnumSet<CreateFlag> createFlag = EnumSet.of(CreateFlag.APPEND);
    HdfsDataOutputStream stream = client.append(path, 1024, createFlag, null,
        null);
    byte[] data = new byte[dataLen];
    stream.write(data);
    stream.close();
  }

  @Test
  public void testSetQuotaToMountTableEntry() throws Exception {
    long nsQuota = 10;
    long ssQuota = 10240;
    long diskQuota = 1024;
    final FileSystem nnFs1 = nnContext1.getFileSystem();
    nnFs1.mkdirs(new Path("/testSetQuotaToFederationPath"));
    nnFs1.mkdirs(new Path("/testSetQuotaToFederationPath/dir0"));

    // Add one mount table:
    // /setquota --> ns0---testSetQuotaToFederationPath
    MountTable mountTable = MountTable.newInstance("/setquota",
        Collections.singletonMap("ns0", "/testSetQuotaToFederationPath"));
    addMountTable(mountTable);

    RouterQuotaUpdateService updateService = routerContext.getRouter()
        .getQuotaCacheUpdateService();
    // ensure mount table is updated to Router.
    updateService.periodicInvoke();

    final FileSystem routerFs = routerContext.getFileSystem();
    // setting quota on a mount point should fail.
    LambdaTestUtils.intercept(AccessControlException.class,
        "is not allowed to change quota of",
        "Expect an AccessControlException.",
        () -> routerFs.setQuota(new Path("/setquota"), nsQuota, ssQuota));
    // setting storage quota on a mount point should fail.
    LambdaTestUtils.intercept(AccessControlException.class,
        "is not allowed to change quota of",
        "Expect an AccessControlException.", () -> routerFs
            .setQuotaByStorageType(new Path("/setquota"), StorageType.DISK,
                diskQuota));
    QuotaUsage quota =
        nnFs1.getQuotaUsage(new Path("/testSetQuotaToFederationPath"));
    // quota should still be unset.
    assertEquals(-1, quota.getQuota());
    assertEquals(-1, quota.getSpaceQuota());

    // setting quota on a non-mount point should succeed.
    routerFs.setQuota(new Path("/setquota/dir0"), nsQuota, ssQuota);
    // setting storage quota on a non-mount point should succeed.
    routerFs.setQuotaByStorageType(new Path("/setquota/dir0"), StorageType.DISK,
        diskQuota);
    quota = nnFs1.getQuotaUsage(new Path("/testSetQuotaToFederationPath/dir0"));
    // quota should be set.
    assertEquals(nsQuota, quota.getQuota());
    assertEquals(ssQuota, quota.getSpaceQuota());
    assertEquals(diskQuota, quota.getTypeQuota(StorageType.DISK));
  }

  @Test
  public void testSetQuota() throws Exception {
    long nsQuota = 5;
    long ssQuota = 100;
    final FileSystem nnFs1 = nnContext1.getFileSystem();
    final FileSystem nnFs2 = nnContext2.getFileSystem();

    // Add two mount tables:
    // /setquota --> ns0---testdir5
    // /setquota/subdir --> ns1---testdir6
    nnFs1.mkdirs(new Path("/testdir5"));
    nnFs2.mkdirs(new Path("/testdir6"));
    MountTable mountTable1 = MountTable.newInstance("/setquota",
        Collections.singletonMap("ns0", "/testdir5"));
    mountTable1
        .setQuota(new RouterQuotaUsage.Builder().quota(nsQuota)
        .spaceQuota(ssQuota).build());
    addMountTable(mountTable1);

    // don't set quota for subpath of mount table
    MountTable mountTable2 = MountTable.newInstance("/setquota/subdir",
        Collections.singletonMap("ns1", "/testdir6"));
    addMountTable(mountTable2);

    RouterQuotaUpdateService updateService = routerContext.getRouter()
        .getQuotaCacheUpdateService();
    // ensure setQuota RPC call was invoked
    updateService.periodicInvoke();

    ClientProtocol client1 = nnContext1.getClient().getNamenode();
    ClientProtocol client2 = nnContext2.getClient().getNamenode();
    final QuotaUsage quota1 = client1.getQuotaUsage("/testdir5");
    final QuotaUsage quota2 = client2.getQuotaUsage("/testdir6");

    assertEquals(nsQuota, quota1.getQuota());
    assertEquals(ssQuota, quota1.getSpaceQuota());
    assertEquals(nsQuota, quota2.getQuota());
    assertEquals(ssQuota, quota2.getSpaceQuota());
  }

  @Test
  public void testStorageTypeQuota() throws Exception {
    long ssQuota = BLOCK_SIZE * 3;
    int fileSize = BLOCK_SIZE;
    prepareStorageTypeQuotaTestMountTable(StorageType.DISK, BLOCK_SIZE,
        ssQuota * 2, ssQuota, fileSize, fileSize);

    // Verify /type0 quota on NN1.
    ClientProtocol client = nnContext1.getClient().getNamenode();
    QuotaUsage usage = client.getQuotaUsage("/type0");
    assertEquals(HdfsConstants.QUOTA_RESET, usage.getQuota());
    assertEquals(HdfsConstants.QUOTA_RESET, usage.getSpaceQuota());
    verifyTypeQuotaAndConsume(new long[] {-1, -1, ssQuota * 2, -1, -1, -1},
        null, usage);
    // Verify /type1 quota on NN1.
    usage = client.getQuotaUsage("/type1");
    assertEquals(HdfsConstants.QUOTA_RESET, usage.getQuota());
    assertEquals(HdfsConstants.QUOTA_RESET, usage.getSpaceQuota());
    verifyTypeQuotaAndConsume(new long[] {-1, -1, ssQuota, -1, -1, -1}, null,
        usage);

    FileSystem routerFs = routerContext.getFileSystem();
    QuotaUsage u0 = routerFs.getQuotaUsage(new Path("/type0"));
    QuotaUsage u1 = routerFs.getQuotaUsage(new Path("/type0/type1"));
    // Verify /type0/type1 storage type quota usage on Router.
    assertEquals(HdfsConstants.QUOTA_RESET, u1.getQuota());
    assertEquals(2, u1.getFileAndDirectoryCount());
    assertEquals(HdfsConstants.QUOTA_RESET, u1.getSpaceQuota());
    assertEquals(fileSize * 3, u1.getSpaceConsumed());
    verifyTypeQuotaAndConsume(new long[] {-1, -1, ssQuota, -1, -1, -1},
        new long[] {0, 0, fileSize * 3, 0, 0, 0}, u1);
    // Verify /type0 storage type quota usage on Router.
    assertEquals(HdfsConstants.QUOTA_RESET, u0.getQuota());
    assertEquals(4, u0.getFileAndDirectoryCount());
    assertEquals(HdfsConstants.QUOTA_RESET, u0.getSpaceQuota());
    assertEquals(fileSize * 3 * 2, u0.getSpaceConsumed());
    verifyTypeQuotaAndConsume(new long[] {-1, -1, ssQuota * 2, -1, -1, -1},
        new long[] {0, 0, fileSize * 3 * 2, 0, 0, 0}, u0);
  }

  @Test
  public void testGetQuota() throws Exception {
    long nsQuota = 10;
    long ssQuota = 100;
    final FileSystem nnFs1 = nnContext1.getFileSystem();
    final FileSystem nnFs2 = nnContext2.getFileSystem();

    // Add two mount tables:
    // /getquota --> ns0---/testdir7
    // /getquota/subdir1 --> ns0---/testdir7/subdir
    // /getquota/subdir2 --> ns1---/testdir8
    // /getquota/subdir3 --> ns1---/testdir8-ext
    nnFs1.mkdirs(new Path("/testdir7"));
    nnFs1.mkdirs(new Path("/testdir7/subdir"));
    nnFs2.mkdirs(new Path("/testdir8"));
    nnFs2.mkdirs(new Path("/testdir8-ext"));
    MountTable mountTable1 = MountTable.newInstance("/getquota",
        Collections.singletonMap("ns0", "/testdir7"));
    mountTable1
        .setQuota(new RouterQuotaUsage.Builder().quota(nsQuota)
        .spaceQuota(ssQuota).build());
    addMountTable(mountTable1);

    MountTable mountTable2 = MountTable.newInstance("/getquota/subdir1",
        Collections.singletonMap("ns0", "/testdir7/subdir"));
    addMountTable(mountTable2);

    MountTable mountTable3 = MountTable.newInstance("/getquota/subdir2",
        Collections.singletonMap("ns1", "/testdir8"));
    addMountTable(mountTable3);

    MountTable mountTable4 = MountTable.newInstance("/getquota/subdir3",
            Collections.singletonMap("ns1", "/testdir8-ext"));
    addMountTable(mountTable4);

    // use router client to create new files
    DFSClient routerClient = routerContext.getClient();
    routerClient.create("/getquota/file", true).close();
    routerClient.create("/getquota/subdir1/file", true).close();
    routerClient.create("/getquota/subdir2/file", true).close();
    routerClient.create("/getquota/subdir3/file", true).close();

    ClientProtocol clientProtocol = routerContext.getClient().getNamenode();
    RouterQuotaUpdateService updateService = routerContext.getRouter()
        .getQuotaCacheUpdateService();
    updateService.periodicInvoke();
    final QuotaUsage quota = clientProtocol.getQuotaUsage("/getquota");
    // the quota should be aggregated
    assertEquals(8, quota.getFileAndDirectoryCount());
  }

  @Test
  public void testStaleQuotaRemoving() throws Exception {
    long nsQuota = 20;
    long ssQuota = 200;
    String stalePath = "/stalequota";
    final FileSystem nnFs1 = nnContext1.getFileSystem();

    // Add one mount tables:
    // /stalequota --> ns0---/testdir9
    nnFs1.mkdirs(new Path("/testdir9"));
    MountTable mountTable = MountTable.newInstance(stalePath,
        Collections.singletonMap("ns0", "/testdir9"));
    mountTable.setQuota(new RouterQuotaUsage.Builder().quota(nsQuota)
        .spaceQuota(ssQuota).build());
    addMountTable(mountTable);

    // Call periodicInvoke to ensure quota for stalePath was
    // loaded into quota manager.
    RouterQuotaUpdateService updateService = routerContext.getRouter()
        .getQuotaCacheUpdateService();
    updateService.periodicInvoke();

    // use quota manager to get its quota usage and do verification
    RouterQuotaManager quotaManager = routerContext.getRouter()
        .getQuotaManager();
    RouterQuotaUsage quota = quotaManager.getQuotaUsage(stalePath);
    assertEquals(nsQuota, quota.getQuota());
    assertEquals(ssQuota, quota.getSpaceQuota());

    // remove stale path entry
    removeMountTable(stalePath);
    updateService.periodicInvoke();
    // the stale entry should be removed and we will get null
    quota = quotaManager.getQuotaUsage(stalePath);
    assertNull(quota);
  }

  /**
   * Remove a mount table entry to the mount table through the admin API.
   * @param path Mount table entry to remove.
   * @return If it was successfully removed.
   * @throws IOException Problems removing entries.
   */
  private boolean removeMountTable(String path) throws IOException {
    RouterClient client = routerContext.getAdminClient();
    MountTableManager mountTableManager = client.getMountTableManager();
    RemoveMountTableEntryRequest removeRequest = RemoveMountTableEntryRequest
        .newInstance(path);
    RemoveMountTableEntryResponse removeResponse = mountTableManager
        .removeMountTableEntry(removeRequest);

    // Reload the Router cache
    resolver.loadCache(true);
    return removeResponse.getStatus();
  }

  /**
   * Test {@link RouterQuotaUpdateService#periodicInvoke()} updates quota usage
   * in RouterQuotaManager.
   */
  @Test
  public void testQuotaUpdating() throws Exception {
    long nsQuota = 30;
    long ssQuota = 1024;
    String path = "/updatequota";
    final FileSystem nnFs1 = nnContext1.getFileSystem();

    // Add one mount table:
    // /updatequota --> ns0---/testdir10
    nnFs1.mkdirs(new Path("/testdir10"));
    MountTable mountTable = MountTable.newInstance(path,
        Collections.singletonMap("ns0", "/testdir10"));
    mountTable.setQuota(new RouterQuotaUsage.Builder().quota(nsQuota)
        .spaceQuota(ssQuota).build());
    addMountTable(mountTable);

    // Call periodicInvoke to ensure quota  updated in quota manager.
    RouterQuotaUpdateService updateService = routerContext.getRouter()
        .getQuotaCacheUpdateService();
    updateService.periodicInvoke();

    // verify initial quota value
    MountTable updatedMountTable = getMountTable(path);
    RouterQuotaUsage quota = updatedMountTable.getQuota();
    assertEquals(nsQuota, quota.getQuota());
    assertEquals(ssQuota, quota.getSpaceQuota());
    assertEquals(1, quota.getFileAndDirectoryCount());
    assertEquals(0, quota.getSpaceConsumed());

    // mkdir and write a new file
    final FileSystem routerFs = routerContext.getFileSystem();
    routerFs.mkdirs(new Path(path + "/" + UUID.randomUUID()));
    DFSClient routerClient = routerContext.getClient();
    routerClient.create(path + "/file", true).close();
    appendData(path + "/file", routerClient, BLOCK_SIZE);

    updateService.periodicInvoke();
    updatedMountTable = getMountTable(path);
    quota = updatedMountTable.getQuota();

    // verify if quota usage has been updated in quota manager.
    assertEquals(nsQuota, quota.getQuota());
    assertEquals(ssQuota, quota.getSpaceQuota());
    assertEquals(3, quota.getFileAndDirectoryCount());
    assertEquals(BLOCK_SIZE, quota.getSpaceConsumed());

    // verify quota sync on adding new destination to mount entry.
    updatedMountTable = getMountTable(path);
    nnFs1.mkdirs(new Path("/newPath"));
    updatedMountTable.setDestinations(
        Collections.singletonList(new RemoteLocation("ns0", "/newPath", path)));
    updateMountTable(updatedMountTable);
    assertEquals(nsQuota, nnFs1.getQuotaUsage(new Path("/newPath")).getQuota());
  }

  /**
   * Get the mount table entries of specified path through the admin API.
   * @param path Mount table entry to get.
   * @return If it was successfully got.
   * @throws IOException Problems getting entries.
   */
  private MountTable getMountTable(String path) throws IOException {
    // Reload the Router cache
    resolver.loadCache(true);
    RouterClient client = routerContext.getAdminClient();
    MountTableManager mountTableManager = client.getMountTableManager();
    GetMountTableEntriesRequest getRequest = GetMountTableEntriesRequest
        .newInstance(path);
    GetMountTableEntriesResponse response = mountTableManager
        .getMountTableEntries(getRequest);
    List<MountTable> results = response.getEntries();

    return !results.isEmpty() ? results.get(0) : null;
  }

  @Test
  public void testQuotaSynchronization() throws IOException {
    long updateNsQuota = 3;
    long updateSsQuota = 4;
    FileSystem nnFs = nnContext1.getFileSystem();
    nnFs.mkdirs(new Path("/testsync"));
    MountTable mountTable = MountTable.newInstance("/quotaSync",
        Collections.singletonMap("ns0", "/testsync"), Time.now(), Time.now());
    mountTable.setQuota(new RouterQuotaUsage.Builder().quota(1)
        .spaceQuota(2).build());
    // Add new mount table
    addMountTable(mountTable);

    // ensure the quota is not set as updated value
    QuotaUsage realQuota = nnContext1.getFileSystem()
        .getQuotaUsage(new Path("/testsync"));
    assertNotEquals(updateNsQuota, realQuota.getQuota());
    assertNotEquals(updateSsQuota, realQuota.getSpaceQuota());

    // Call periodicInvoke to ensure quota  updated in quota manager
    // and state store.
    RouterQuotaUpdateService updateService = routerContext.getRouter()
        .getQuotaCacheUpdateService();
    updateService.periodicInvoke();

    mountTable.setQuota(new RouterQuotaUsage.Builder().quota(updateNsQuota)
        .spaceQuota(updateSsQuota).build());
    updateMountTable(mountTable);

    // verify if the quota is updated in real path
    realQuota = nnContext1.getFileSystem().getQuotaUsage(
        new Path("/testsync"));
    assertEquals(updateNsQuota, realQuota.getQuota());
    assertEquals(updateSsQuota, realQuota.getSpaceQuota());

    // Clear the quota
    mountTable.setQuota(new RouterQuotaUsage.Builder()
        .quota(HdfsConstants.QUOTA_RESET)
        .spaceQuota(HdfsConstants.QUOTA_RESET).build());
    updateMountTable(mountTable);

    // verify if the quota is updated in real path
    realQuota = nnContext1.getFileSystem().getQuotaUsage(
        new Path("/testsync"));
    assertEquals(HdfsConstants.QUOTA_RESET, realQuota.getQuota());
    assertEquals(HdfsConstants.QUOTA_RESET, realQuota.getSpaceQuota());

    // Verify updating mount entry with actual destinations not present.
    mountTable = MountTable.newInstance("/testupdate",
        Collections.singletonMap("ns0", "/testupdate"), Time.now(), Time.now());
    addMountTable(mountTable);
    mountTable.setQuota(new RouterQuotaUsage.Builder().quota(1)
        .spaceQuota(2).build());
    assertTrue(updateMountTable(mountTable));
  }

  @Test
  public void testQuotaRefreshAfterQuotaExceed() throws Exception {
    long nsQuota = 3;
    long ssQuota = 100;
    final FileSystem nnFs1 = nnContext1.getFileSystem();
    final FileSystem nnFs2 = nnContext2.getFileSystem();

    // Add two mount tables:
    // /setquota1 --> ns0---testdir11
    // /setquota2 --> ns1---testdir12
    nnFs1.mkdirs(new Path("/testdir11"));
    nnFs2.mkdirs(new Path("/testdir12"));
    MountTable mountTable1 = MountTable.newInstance("/setquota1",
        Collections.singletonMap("ns0", "/testdir11"));
    mountTable1
        .setQuota(new RouterQuotaUsage.Builder().quota(nsQuota)
        .spaceQuota(ssQuota).build());
    addMountTable(mountTable1);

    MountTable mountTable2 = MountTable.newInstance("/setquota2",
        Collections.singletonMap("ns1", "/testdir12"));
    mountTable2
        .setQuota(new RouterQuotaUsage.Builder().quota(nsQuota)
        .spaceQuota(ssQuota).build());
    addMountTable(mountTable2);

    final FileSystem routerFs = routerContext.getFileSystem();
    // Create directory to make directory count equals to nsQuota
    routerFs.mkdirs(new Path("/setquota1/" + UUID.randomUUID()));
    routerFs.mkdirs(new Path("/setquota1/" + UUID.randomUUID()));

    // create one more directory to exceed the nsQuota
    routerFs.mkdirs(new Path("/setquota1/" + UUID.randomUUID()));

    RouterQuotaUpdateService updateService = routerContext.getRouter()
        .getQuotaCacheUpdateService();
    // Call RouterQuotaUpdateService#periodicInvoke to update quota cache
    updateService.periodicInvoke();
    // Reload the Router cache
    resolver.loadCache(true);

    RouterQuotaManager quotaManager =
        routerContext.getRouter().getQuotaManager();
    ClientProtocol client1 = nnContext1.getClient().getNamenode();
    ClientProtocol client2 = nnContext2.getClient().getNamenode();
    QuotaUsage quota1 = client1.getQuotaUsage("/testdir11");
    QuotaUsage quota2 = client2.getQuotaUsage("/testdir12");
    QuotaUsage cacheQuota1 = quotaManager.getQuotaUsage("/setquota1");
    QuotaUsage cacheQuota2 = quotaManager.getQuotaUsage("/setquota2");

    // Verify quota usage
    assertEquals(4, quota1.getFileAndDirectoryCount());
    assertEquals(4, cacheQuota1.getFileAndDirectoryCount());
    assertEquals(1, quota2.getFileAndDirectoryCount());
    assertEquals(1, cacheQuota2.getFileAndDirectoryCount());

    try {
      // create new directory to trigger NSQuotaExceededException
      routerFs.mkdirs(new Path("/testdir11/" + UUID.randomUUID()));
      fail("Mkdir should be failed under dir /testdir11.");
    } catch (NSQuotaExceededException ignored) {
    }

    // Create directory under the other mount point
    routerFs.mkdirs(new Path("/setquota2/" + UUID.randomUUID()));
    routerFs.mkdirs(new Path("/setquota2/" + UUID.randomUUID()));

    // Call RouterQuotaUpdateService#periodicInvoke to update quota cache
    updateService.periodicInvoke();

    quota1 = client1.getQuotaUsage("/testdir11");
    cacheQuota1 = quotaManager.getQuotaUsage("/setquota1");
    quota2 = client2.getQuotaUsage("/testdir12");
    cacheQuota2 = quotaManager.getQuotaUsage("/setquota2");

    // Verify whether quota usage cache is update by periodicInvoke().
    assertEquals(4, quota1.getFileAndDirectoryCount());
    assertEquals(4, cacheQuota1.getFileAndDirectoryCount());
    assertEquals(3, quota2.getFileAndDirectoryCount());
    assertEquals(3, cacheQuota2.getFileAndDirectoryCount());
  }

  /**
   * Verify whether quota usage cache in RouterQuotaManager is updated properly.
   * {@link RouterQuotaUpdateService#periodicInvoke()} should be able to update
   * the cache even if the destination directory for some mount entry is not
   * present in the filesystem.
   */
  @Test
  public void testQuotaRefreshWhenDestinationNotPresent() throws Exception {
    long nsQuota = 5;
    long ssQuota = 3 * BLOCK_SIZE;
    final FileSystem nnFs = nnContext1.getFileSystem();

    // Add three mount tables:
    // /setdir1 --> ns0---testdir13
    // /setdir2 --> ns0---testdir14
    // Create destination directory
    nnFs.mkdirs(new Path("/testdir13"));
    nnFs.mkdirs(new Path("/testdir14"));

    MountTable mountTable = MountTable.newInstance("/setdir1",
        Collections.singletonMap("ns0", "/testdir13"));
    mountTable
        .setQuota(new RouterQuotaUsage.Builder().quota(nsQuota)
        .spaceQuota(ssQuota).build());
    addMountTable(mountTable);

    mountTable = MountTable.newInstance("/setdir2",
        Collections.singletonMap("ns0", "/testdir14"));
    mountTable
        .setQuota(new RouterQuotaUsage.Builder().quota(nsQuota)
        .spaceQuota(ssQuota).build());
    addMountTable(mountTable);

    final DFSClient routerClient = routerContext.getClient();
    // Create file
    routerClient.create("/setdir1/file1", true).close();
    routerClient.create("/setdir2/file2", true).close();
    // append data to the file
    appendData("/setdir1/file1", routerClient, BLOCK_SIZE);
    appendData("/setdir2/file2", routerClient, BLOCK_SIZE);

    RouterQuotaUpdateService updateService =
        routerContext.getRouter().getQuotaCacheUpdateService();
    // Update quota cache
    updateService.periodicInvoke();
    // Reload the Router cache
    resolver.loadCache(true);

    ClientProtocol client1 = nnContext1.getClient().getNamenode();
    RouterQuotaManager quotaManager =
        routerContext.getRouter().getQuotaManager();
    QuotaUsage quota1 = client1.getQuotaUsage("/testdir13");
    QuotaUsage quota2 = client1.getQuotaUsage("/testdir14");
    QuotaUsage cacheQuota1 = quotaManager.getQuotaUsage("/setdir1");
    QuotaUsage cacheQuota2 = quotaManager.getQuotaUsage("/setdir2");

    // Get quota details in mount table
    MountTable updatedMountTable = getMountTable("/setdir1");
    RouterQuotaUsage mountQuota1 = updatedMountTable.getQuota();
    updatedMountTable = getMountTable("/setdir2");
    RouterQuotaUsage mountQuota2 = updatedMountTable.getQuota();

    // Verify quota usage
    assertEquals(2, quota1.getFileAndDirectoryCount());
    assertEquals(2, cacheQuota1.getFileAndDirectoryCount());
    assertEquals(2, mountQuota1.getFileAndDirectoryCount());
    assertEquals(2, quota2.getFileAndDirectoryCount());
    assertEquals(2, cacheQuota2.getFileAndDirectoryCount());
    assertEquals(2, mountQuota2.getFileAndDirectoryCount());
    assertEquals(BLOCK_SIZE, quota1.getSpaceConsumed());
    assertEquals(BLOCK_SIZE, cacheQuota1.getSpaceConsumed());
    assertEquals(BLOCK_SIZE, mountQuota1.getSpaceConsumed());
    assertEquals(BLOCK_SIZE, quota2.getSpaceConsumed());
    assertEquals(BLOCK_SIZE, cacheQuota2.getSpaceConsumed());
    assertEquals(BLOCK_SIZE, mountQuota2.getSpaceConsumed());

    FileSystem routerFs = routerContext.getFileSystem();
    // Remove file in setdir1. The target directory still exists.
    routerFs.delete(new Path("/setdir1/file1"), true);

    // Create file
    routerClient.create("/setdir2/file3", true).close();
    // append data to the file
    appendData("/setdir2/file3", routerClient, BLOCK_SIZE);
    int updatedSpace = BLOCK_SIZE + BLOCK_SIZE;

    // Update quota cache
    updateService.periodicInvoke();

    quota2 = client1.getQuotaUsage("/testdir14");
    cacheQuota1 = quotaManager.getQuotaUsage("/setdir1");
    cacheQuota2 = quotaManager.getQuotaUsage("/setdir2");

    // Get quota details in mount table
    updatedMountTable = getMountTable("/setdir1");
    mountQuota1 = updatedMountTable.getQuota();
    updatedMountTable = getMountTable("/setdir2");
    mountQuota2 = updatedMountTable.getQuota();

    // The quota usage should be reset.
    assertEquals(1, cacheQuota1.getFileAndDirectoryCount());
    assertEquals(1, mountQuota1.getFileAndDirectoryCount());
    assertEquals(0, cacheQuota1.getSpaceConsumed());
    assertEquals(0, mountQuota1.getSpaceConsumed());

    // Verify current quota usage for other mount entries
    assertEquals(3, quota2.getFileAndDirectoryCount());
    assertEquals(3, cacheQuota2.getFileAndDirectoryCount());
    assertEquals(3, mountQuota2.getFileAndDirectoryCount());
    assertEquals(updatedSpace, quota2.getSpaceConsumed());
    assertEquals(updatedSpace, cacheQuota2.getSpaceConsumed());
    assertEquals(updatedSpace, mountQuota2.getSpaceConsumed());
  }

  @Test
  public void testClearQuotaDefAfterRemovingMountTable() throws Exception {
    long nsQuota = 5;
    long ssQuota = 3 * BLOCK_SIZE;
    final FileSystem nnFs = nnContext1.getFileSystem();

    // Add one mount tables:
    // /setdir --> ns0---testdir15
    // Create destination directory
    nnFs.mkdirs(new Path("/testdir15"));

    MountTable mountTable = MountTable.newInstance("/setdir",
        Collections.singletonMap("ns0", "/testdir15"));
    mountTable.setQuota(new RouterQuotaUsage.Builder().quota(nsQuota)
        .spaceQuota(ssQuota).build());
    addMountTable(mountTable);

    // Update router quota
    RouterQuotaUpdateService updateService =
        routerContext.getRouter().getQuotaCacheUpdateService();
    updateService.periodicInvoke();

    RouterQuotaManager quotaManager =
        routerContext.getRouter().getQuotaManager();
    ClientProtocol client = nnContext1.getClient().getNamenode();
    QuotaUsage routerQuota = quotaManager.getQuotaUsage("/setdir");
    QuotaUsage subClusterQuota = client.getQuotaUsage("/testdir15");

    // Verify current quota definitions
    assertEquals(nsQuota, routerQuota.getQuota());
    assertEquals(ssQuota, routerQuota.getSpaceQuota());
    assertEquals(nsQuota, subClusterQuota.getQuota());
    assertEquals(ssQuota, subClusterQuota.getSpaceQuota());

    // Remove mount table
    removeMountTable("/setdir");
    updateService.periodicInvoke();
    routerQuota = quotaManager.getQuotaUsage("/setdir");
    subClusterQuota = client.getQuotaUsage("/testdir15");

    // Verify quota definitions are cleared after removing the mount table
    assertNull(routerQuota);
    assertEquals(HdfsConstants.QUOTA_RESET, subClusterQuota.getQuota());
    assertEquals(HdfsConstants.QUOTA_RESET, subClusterQuota.getSpaceQuota());

    // Verify removing mount entry with actual destinations not present.
    mountTable = MountTable.newInstance("/mount",
        Collections.singletonMap("ns0", "/testdir16"));
    addMountTable(mountTable);
    assertTrue(removeMountTable("/mount"));
  }

  @Test
  public void testSetQuotaNotMountTable() throws Exception {
    long nsQuota = 5;
    long ssQuota = 100;
    final FileSystem nnFs1 = nnContext1.getFileSystem();

    // setQuota should run for any directory
    MountTable mountTable1 = MountTable.newInstance("/setquotanmt",
        Collections.singletonMap("ns0", "/testdir16"));

    addMountTable(mountTable1);

    // Add a directory not present in mount table.
    nnFs1.mkdirs(new Path("/testdir16/testdir17"));

    routerContext.getRouter().getRpcServer().setQuota("/setquotanmt/testdir17",
        nsQuota, ssQuota, null);

    RouterQuotaUpdateService updateService = routerContext.getRouter()
        .getQuotaCacheUpdateService();
    // ensure setQuota RPC call was invoked
    updateService.periodicInvoke();

    ClientProtocol client1 = nnContext1.getClient().getNamenode();
    final QuotaUsage quota1 = client1.getQuotaUsage("/testdir16/testdir17");

    assertEquals(nsQuota, quota1.getQuota());
    assertEquals(ssQuota, quota1.getSpaceQuota());
  }

  @Test
  public void testNoQuotaaExceptionForUnrelatedOperations() throws Exception {
    FileSystem nnFs = nnContext1.getFileSystem();
    DistributedFileSystem routerFs =
        (DistributedFileSystem) routerContext.getFileSystem();
    Path path = new Path("/quota");
    nnFs.mkdirs(new Path("/dir"));
    MountTable mountTable1 = MountTable.newInstance("/quota",
        Collections.singletonMap("ns0", "/dir"));
    mountTable1.setQuota(new RouterQuotaUsage.Builder().quota(0).build());
    addMountTable(mountTable1);
    routerFs.mkdirs(new Path("/quota/1"));
    routerContext.getRouter().getQuotaCacheUpdateService().periodicInvoke();

    // Quota check for related operation.
    intercept(NSQuotaExceededException.class,
        "The NameSpace quota (directories and files) is exceeded",
        () -> routerFs.mkdirs(new Path("/quota/2")));

    //Quotas shouldn't be checked for unrelated operations.
    routerFs.setStoragePolicy(path, "COLD");
    routerFs.setErasureCodingPolicy(path, "RS-6-3-1024k");
    routerFs.unsetErasureCodingPolicy(path);
    routerFs.setPermission(path, new FsPermission((short) 01777));
    routerFs.setOwner(path, "user", "group");
    routerFs.setTimes(path, 1L, 1L);
    routerFs.listStatus(path);
    routerFs.getContentSummary(path);
  }

  @Test
  public void testGetGlobalQuota() throws Exception {
    long nsQuota = 5;
    long ssQuota = 3 * BLOCK_SIZE;
    prepareGlobalQuotaTestMountTable(nsQuota, ssQuota);

    Quota qModule = routerContext.getRouter().getRpcServer().getQuotaModule();
    QuotaUsage qu = qModule.getGlobalQuota("/dir-1");
    assertEquals(nsQuota, qu.getQuota());
    assertEquals(ssQuota, qu.getSpaceQuota());
    qu = qModule.getGlobalQuota("/dir-1/dir-2");
    assertEquals(nsQuota, qu.getQuota());
    assertEquals(ssQuota * 2, qu.getSpaceQuota());
    qu = qModule.getGlobalQuota("/dir-1/dir-2/dir-3");
    assertEquals(nsQuota, qu.getQuota());
    assertEquals(ssQuota * 2, qu.getSpaceQuota());
    qu = qModule.getGlobalQuota("/dir-4");
    assertEquals(-1, qu.getQuota());
    assertEquals(-1, qu.getSpaceQuota());
  }

  @Test
  public void testFixGlobalQuota() throws Exception {
    long nsQuota = 5;
    long ssQuota = 3 * BLOCK_SIZE;
    final FileSystem nnFs = nnContext1.getFileSystem();
    prepareGlobalQuotaTestMountTable(nsQuota, ssQuota);

    QuotaUsage qu = nnFs.getQuotaUsage(new Path("/dir-1"));
    assertEquals(nsQuota, qu.getQuota());
    assertEquals(ssQuota, qu.getSpaceQuota());
    qu = nnFs.getQuotaUsage(new Path("/dir-2"));
    assertEquals(nsQuota, qu.getQuota());
    assertEquals(ssQuota * 2, qu.getSpaceQuota());
    qu = nnFs.getQuotaUsage(new Path("/dir-3"));
    assertEquals(nsQuota, qu.getQuota());
    assertEquals(ssQuota * 2, qu.getSpaceQuota());
    qu = nnFs.getQuotaUsage(new Path("/dir-4"));
    assertEquals(-1, qu.getQuota());
    assertEquals(-1, qu.getSpaceQuota());
  }

  @Test
  public void testGetQuotaUsageOnMountPoint() throws Exception {
    long nsQuota = 5;
    long ssQuota = 3 * BLOCK_SIZE;
    prepareGlobalQuotaTestMountTable(nsQuota, ssQuota);

    final FileSystem routerFs = routerContext.getFileSystem();
    // Verify getQuotaUsage() of the mount point /dir-1.
    QuotaUsage quotaUsage = routerFs.getQuotaUsage(new Path("/dir-1"));
    assertEquals(nsQuota, quotaUsage.getQuota());
    assertEquals(ssQuota, quotaUsage.getSpaceQuota());
    // Verify getQuotaUsage() of the mount point /dir-1/dir-2.
    quotaUsage = routerFs.getQuotaUsage(new Path("/dir-1/dir-2"));
    assertEquals(nsQuota, quotaUsage.getQuota());
    assertEquals(ssQuota * 2, quotaUsage.getSpaceQuota());
    // Verify getQuotaUsage() of the mount point /dir-1/dir-2/dir-3
    quotaUsage = routerFs.getQuotaUsage(new Path("/dir-1/dir-2/dir-3"));
    assertEquals(nsQuota, quotaUsage.getQuota());
    assertEquals(ssQuota * 2, quotaUsage.getSpaceQuota());
    // Verify getQuotaUsage() of the mount point /dir-4
    quotaUsage = routerFs.getQuotaUsage(new Path("/dir-4"));
    assertEquals(-1, quotaUsage.getQuota());
    assertEquals(-1, quotaUsage.getSpaceQuota());

    routerFs.mkdirs(new Path("/dir-1/dir-normal"));
    try {
      // Verify getQuotaUsage() of the normal path /dir-1/dir-normal.
      quotaUsage = routerFs.getQuotaUsage(new Path("/dir-1/dir-normal"));
      assertEquals(-1, quotaUsage.getQuota());
      assertEquals(-1, quotaUsage.getSpaceQuota());

      routerFs.setQuota(new Path("/dir-1/dir-normal"), 100, 200);
      // Verify getQuotaUsage() of the normal path /dir-1/dir-normal.
      quotaUsage = routerFs.getQuotaUsage(new Path("/dir-1/dir-normal"));
      assertEquals(100, quotaUsage.getQuota());
      assertEquals(200, quotaUsage.getSpaceQuota());
    } finally {
      // clear normal path.
      routerFs.delete(new Path("/dir-1/dir-normal"), true);
    }
  }

  /**
   * RouterQuotaUpdateService.periodicInvoke() should only update usage in
   * cache. The mount table in state store shouldn't be updated.
   */
  @Test
  public void testRouterQuotaUpdateService() throws Exception {
    Router router = routerContext.getRouter();
    StateStoreDriver driver = router.getStateStore().getDriver();
    RouterQuotaUpdateService updateService =
        router.getQuotaCacheUpdateService();
    RouterQuotaManager quotaManager = router.getQuotaManager();
    long nsQuota = 5;
    long ssQuota = 3 * BLOCK_SIZE;
    final FileSystem nnFs = nnContext1.getFileSystem();
    nnFs.mkdirs(new Path("/dir-1"));

    // init mount table.
    MountTable mountTable = MountTable.newInstance("/dir-1",
        Collections.singletonMap("ns0", "/dir-1"));
    mountTable.setQuota(new RouterQuotaUsage.Builder().quota(nsQuota)
        .spaceQuota(ssQuota).build());
    addMountTable(mountTable);
    // verify the mount table in state store is updated.
    QueryResult<MountTable> result = driver.get(MountTable.class);
    RouterQuotaUsage quotaOnStorage = result.getRecords().get(0).getQuota();
    assertEquals(nsQuota, quotaOnStorage.getQuota());
    assertEquals(ssQuota, quotaOnStorage.getSpaceQuota());
    assertEquals(0, quotaOnStorage.getFileAndDirectoryCount());
    assertEquals(0, quotaOnStorage.getSpaceConsumed());

    // test RouterQuotaUpdateService.periodicInvoke().
    updateService.periodicInvoke();

    // RouterQuotaUpdateService should update usage in local cache.
    RouterQuotaUsage quotaUsage = quotaManager.getQuotaUsage("/dir-1");
    assertEquals(nsQuota, quotaUsage.getQuota());
    assertEquals(ssQuota, quotaUsage.getSpaceQuota());
    assertEquals(1, quotaUsage.getFileAndDirectoryCount());
    assertEquals(0, quotaUsage.getSpaceConsumed());
    // RouterQuotaUpdateService shouldn't update mount entry in state store.
    result = driver.get(MountTable.class);
    quotaOnStorage = result.getRecords().get(0).getQuota();
    assertEquals(nsQuota, quotaOnStorage.getQuota());
    assertEquals(ssQuota, quotaOnStorage.getSpaceQuota());
    assertEquals(0, quotaOnStorage.getFileAndDirectoryCount());
    assertEquals(0, quotaOnStorage.getSpaceConsumed());
  }

  /**
   * Verify whether quota is updated properly.
   * {@link RouterQuotaUpdateService#periodicInvoke()} should be able to update
   * the quota even if the destination directory for some mount entry is not
   * present in the filesystem.
   */
  @Test
  public void testQuotaUpdateWhenDestinationNotPresent() throws Exception {
    long nsQuota = 5;
    long ssQuota = 3 * BLOCK_SIZE;
    String path = "/dst-not-present";
    final FileSystem nnFs = nnContext1.getFileSystem();

    // Add one mount table:
    // /dst-not-present --> ns0---/dst-not-present.
    nnFs.mkdirs(new Path(path));
    MountTable mountTable =
        MountTable.newInstance(path, Collections.singletonMap("ns0", path));
    mountTable.setQuota(new RouterQuotaUsage.Builder().quota(nsQuota)
        .spaceQuota(ssQuota).build());
    addMountTable(mountTable);

    Router router = routerContext.getRouter();
    RouterQuotaManager quotaManager = router.getQuotaManager();
    RouterQuotaUpdateService updateService =
        router.getQuotaCacheUpdateService();
    // verify quota usage is updated in RouterQuotaManager.
    updateService.periodicInvoke();
    RouterQuotaUsage quotaUsage = quotaManager.getQuotaUsage(path);
    assertEquals(nsQuota, quotaUsage.getQuota());
    assertEquals(ssQuota, quotaUsage.getSpaceQuota());
    assertEquals(1, quotaUsage.getFileAndDirectoryCount());
    assertEquals(0, quotaUsage.getSpaceConsumed());

    // Update quota to [nsQuota*2, ssQuota*2].
    mountTable.setQuota(new RouterQuotaUsage.Builder().quota(nsQuota * 2)
        .spaceQuota(ssQuota * 2).build());
    updateMountTable(mountTable);
    // Remove /dst-not-present.
    nnFs.delete(new Path(path), true);

    // verify quota is updated in RouterQuotaManager.
    updateService.periodicInvoke();
    quotaUsage = quotaManager.getQuotaUsage(path);
    assertEquals(nsQuota * 2, quotaUsage.getQuota());
    assertEquals(ssQuota * 2, quotaUsage.getSpaceQuota());
    assertEquals(0, quotaUsage.getFileAndDirectoryCount());
    assertEquals(0, quotaUsage.getSpaceConsumed());
  }

  @Test
  public void testAndByStorageType() {
    long[] typeQuota = new long[StorageType.values().length];
    Arrays.fill(typeQuota, HdfsConstants.QUOTA_DONT_SET);

    Predicate<StorageType> predicate = new Predicate<StorageType>() {
      @Override
      public boolean test(StorageType storageType) {
        return typeQuota[storageType.ordinal()] == HdfsConstants.QUOTA_DONT_SET;
      }
    };

    assertTrue(Quota.andByStorageType(predicate));

    // This is a value to test for,
    // as long as it is not equal to HdfsConstants.QUOTA_DONT_SET
    typeQuota[0] = HdfsConstants.QUOTA_RESET;
    assertFalse(Quota.andByStorageType(predicate));

    Arrays.fill(typeQuota, HdfsConstants.QUOTA_DONT_SET);
    typeQuota[1] = HdfsConstants.QUOTA_RESET;
    assertFalse(Quota.andByStorageType(predicate));

    Arrays.fill(typeQuota, HdfsConstants.QUOTA_DONT_SET);
    typeQuota[typeQuota.length-1] = HdfsConstants.QUOTA_RESET;
    assertFalse(Quota.andByStorageType(predicate));
  }

  /**
   * Add three mount tables.
   * /dir-1              --> ns0---/dir-1 [nsQuota, ssQuota]
   * /dir-1/dir-2        --> ns0---/dir-2 [QUOTA_UNSET, ssQuota * 2]
   * /dir-1/dir-2/dir-3  --> ns0---/dir-3 [QUOTA_UNSET, QUOTA_UNSET]
   * /dir-4              --> ns0---/dir-4 [QUOTA_UNSET, QUOTA_UNSET]
   *
   * Expect three remote locations' global quota.
   * ns0---/dir-1 --> [nsQuota, ssQuota]
   * ns0---/dir-2 --> [nsQuota, ssQuota * 2]
   * ns0---/dir-3 --> [nsQuota, ssQuota * 2]
   * ns0---/dir-4 --> [-1, -1]
   */
  private void prepareGlobalQuotaTestMountTable(long nsQuota, long ssQuota)
      throws IOException {
    final FileSystem nnFs = nnContext1.getFileSystem();

    // Create destination directory
    nnFs.mkdirs(new Path("/dir-1"));
    nnFs.mkdirs(new Path("/dir-2"));
    nnFs.mkdirs(new Path("/dir-3"));
    nnFs.mkdirs(new Path("/dir-4"));

    MountTable mountTable = MountTable.newInstance("/dir-1",
        Collections.singletonMap("ns0", "/dir-1"));
    mountTable.setQuota(new RouterQuotaUsage.Builder().quota(nsQuota)
        .spaceQuota(ssQuota).build());
    addMountTable(mountTable);
    mountTable = MountTable.newInstance("/dir-1/dir-2",
        Collections.singletonMap("ns0", "/dir-2"));
    mountTable.setQuota(new RouterQuotaUsage.Builder().spaceQuota(ssQuota * 2)
        .build());
    addMountTable(mountTable);
    mountTable = MountTable.newInstance("/dir-1/dir-2/dir-3",
        Collections.singletonMap("ns0", "/dir-3"));
    addMountTable(mountTable);
    mountTable = MountTable.newInstance("/dir-4",
        Collections.singletonMap("ns0", "/dir-4"));
    addMountTable(mountTable);

    // Ensure setQuota RPC was invoked and mount table was updated.
    RouterQuotaUpdateService updateService = routerContext.getRouter()
        .getQuotaCacheUpdateService();
    updateService.periodicInvoke();
  }

  /**
   * Add two mount tables.
   * /type0              --> ns0---/type0 [-1, -1, {STORAGE_TYPE:quota}]
   * /type0/type1        --> ns0---/type1 [-1, -1, {STORAGE_TYPE:quota}]
   *
   * Add two files with storage policy HOT.
   * /type0/file         --> ns0---/type0/file
   * /type0/type1/file   --> ns0---/type1/file
   */
  private void prepareStorageTypeQuotaTestMountTable(StorageType type,
      long blkSize, long quota0, long quota1, int len0, int len1)
      throws Exception {
    final FileSystem nnFs1 = nnContext1.getFileSystem();

    nnFs1.mkdirs(new Path("/type0"));
    nnFs1.mkdirs(new Path("/type1"));
    ((DistributedFileSystem) nnContext1.getFileSystem())
        .createFile(new Path("/type0/file")).storagePolicyName("HOT")
        .blockSize(blkSize).build().close();
    ((DistributedFileSystem) nnContext1.getFileSystem())
        .createFile(new Path("/type1/file")).storagePolicyName("HOT")
        .blockSize(blkSize).build().close();
    DFSClient client = nnContext1.getClient();
    appendData("/type0/file", client, len0);
    appendData("/type1/file", client, len1);

    MountTable mountTable = MountTable
        .newInstance("/type0", Collections.singletonMap("ns0", "/type0"));
    mountTable.setQuota(
        new RouterQuotaUsage.Builder().typeQuota(type, quota0).build());
    addMountTable(mountTable);
    mountTable = MountTable
        .newInstance("/type0/type1", Collections.singletonMap("ns0", "/type1"));
    mountTable.setQuota(
        new RouterQuotaUsage.Builder().typeQuota(type, quota1).build());
    addMountTable(mountTable);

    // ensure mount table is updated to Router.
    RouterQuotaUpdateService updateService = routerContext.getRouter()
        .getQuotaCacheUpdateService();
    updateService.periodicInvoke();
  }

  private void verifyTypeQuotaAndConsume(long[] quota, long[] consume,
      QuotaUsage usage) {
    for (StorageType t : StorageType.values()) {
      if (quota != null) {
        assertEquals(quota[t.ordinal()], usage.getTypeQuota(t));
      }
      if (consume != null) {
        assertEquals(consume[t.ordinal()], usage.getTypeConsumed(t));
      }
    }
  }
}
